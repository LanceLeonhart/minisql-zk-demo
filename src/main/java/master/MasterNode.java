package master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import util.ZkUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.*;

/**
 * 分布式 Master 节点：
 *  1) DDL (CREATE/DROP) 广播到所有 RegionServer
 *  2) 全表查询 (SELECT * 无 WHERE) 广播
 *  3) 带主键的 DML 操作单点路由
 *  4) 按非主键条件的 DML 操作广播
 */
public class MasterNode {
    private static final int PORT = 8888;
    private static final String ZK_REGION_PATH = "/regions";
    // regionName -> "host:port"
    private static final Map<String, String> regionMap = Collections.synchronizedMap(new TreeMap<>());

    public static void main(String[] args) throws Exception {
        // 1. 连接 ZooKeeper 并监听 /regions 子节点
        CuratorFramework zk = ZkUtils.createZkClient();
        PathChildrenCache cache = new PathChildrenCache(zk, ZK_REGION_PATH, true);
        cache.getListenable().addListener((client, event) -> {
            Map<String, String> tmp = new TreeMap<>();
            for (ChildData d : cache.getCurrentData()) {
                String name = d.getPath().substring(ZK_REGION_PATH.length() + 1);
                String addr = new String(d.getData(), StandardCharsets.UTF_8);
                tmp.put(name, addr);
            }
            synchronized (regionMap) {
                regionMap.clear();
                regionMap.putAll(tmp);
            }
            System.out.println("[Master] Regions = " + regionMap.keySet());
        });
        cache.start();

        // 2. 启动 TCP 服务
        try (ServerSocket server = new ServerSocket(PORT)) {
            System.out.println("[Master] Listening on port " + PORT);
            while (true) {
                Socket client = server.accept();
                handleClient(client);
                client.close();
            }
        }
    }

    private static void handleClient(Socket client) {
        try (
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8)
                );
                PrintWriter out = new PrintWriter(
                        client.getOutputStream(), true, StandardCharsets.UTF_8
                )
        ) {
            String sql = in.readLine();
            System.out.println("[Master] Received SQL: " + sql);
            if (sql == null || sql.isBlank()) {
                out.println("Empty SQL.");
                return;
            }

            // snapshot region list
            List<String> regions;
            synchronized (regionMap) {
                regions = new ArrayList<>(regionMap.keySet());
            }
            if (regions.isEmpty()) {
                out.println("No regions available.");
                return;
            }

            String up = sql.trim().toUpperCase(Locale.ROOT);
            boolean isCreate   = up.startsWith("CREATE TABLE");
            boolean isDrop     = up.startsWith("DROP TABLE");
            boolean isSelect   = up.startsWith("SELECT");
            boolean isSelectAll = isSelect && !up.contains("WHERE");

            if (isCreate || isDrop) {
                // 1) DDL 广播
                for (String region : regions) {
                    String res = forward(region, sql, true);
                    out.printf("[%s] %s%n", region, res);
                }

            } else if (isSelectAll) {
                // 2) 全表查询广播
                for (String region : regions) {
                    List<String> resLines = forwardAll(region, sql);
                    for (String line : resLines) {
                        out.printf("[%s] %s%n", region, line);
                    }
                }

            } else {
                // 3/4) 其余 DML 或带 WHERE
                // 尝试提取主键值
                String pkVal = extractPK(sql, "id");
                if (pkVal != null) {
                    // 按主键单点路由
                    int idx = Math.abs(pkVal.hashCode()) % regions.size();
                    String region = regions.get(idx);
                    String res = forward(region, sql, false);
                    out.println(res);
                } else {
                    // 按非主键条件广播（DELETE/UPDATE/SELECT WHERE non-pk）
                    for (String region : regions) {
                        String res = forward(region, sql, false);
                        out.printf("[%s] %s%n", region, res);
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("[Master] Error handling client: " + e.getMessage());
        }
    }

    /**
     * 单行请求：发送 SQL 并读取第一行响应
     * @param regionName
     * @param sql
     * @param readAllLines if true, read until socket close, then join with \n
     * @return response text (single line or multi-line joined)
     */
    private static String forward(String regionName, String sql, boolean readAllLines) {
        String addr = regionMap.get(regionName);
        String[] hp = addr.split(":");
        try (Socket rs = new Socket(hp[0], Integer.parseInt(hp[1]));
             PrintWriter rout = new PrintWriter(rs.getOutputStream(), true);
             BufferedReader rin = new BufferedReader(
                     new InputStreamReader(rs.getInputStream(), StandardCharsets.UTF_8))
        ) {
            rout.println(sql);
            if (!readAllLines) {
                String line = rin.readLine();
                return line != null ? line : "";
            } else {
                // 读取所有行
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = rin.readLine()) != null) {
                    if (sb.length()>0) sb.append("\n");
                    sb.append(line);
                }
                return sb.toString();
            }
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    /**
     * 广播全表查询：读取所有行，保持行顺序
     */
    private static List<String> forwardAll(String regionName, String sql) {
        String addr = regionMap.get(regionName);
        String[] hp = addr.split(":");
        List<String> list = new ArrayList<>();
        try (Socket rs = new Socket(hp[0], Integer.parseInt(hp[1]));
             PrintWriter rout = new PrintWriter(rs.getOutputStream(), true);
             BufferedReader rin = new BufferedReader(
                     new InputStreamReader(rs.getInputStream(), StandardCharsets.UTF_8))
        ) {
            rout.println(sql);
            String line;
            while ((line = rin.readLine()) != null) {
                list.add(line);
            }
        } catch (IOException e) {
            list.add("Error: " + e.getMessage());
        }
        return list;
    }

    /**
     * 尝试从 SQL 中提取主键列的值（只针对 INSERT 及 WHERE id=...）
     */
    private static String extractPK(String sql, String pkCol) {
        // 1) INSERT INTO tbl (cols) VALUES (vals)
        Pattern pIns = Pattern.compile(
                "INSERT INTO \\w+ \\(([^)]+)\\)\\s+VALUES\\s*\\(([^)]+)\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher mIns = pIns.matcher(sql);
        if (mIns.find()) {
            String[] cols = mIns.group(1).split("\\s*,\\s*");
            String[] vals = mIns.group(2).split("\\s*,\\s*");
            for (int i = 0; i < cols.length; i++) {
                if (cols[i].equalsIgnoreCase(pkCol)) {
                    return vals[i].replaceAll("'", "").trim();
                }
            }
        }
        // 2) WHERE id = val
        Pattern pWhere = Pattern.compile(
                "WHERE\\s+" + pkCol + "\\s*=\\s*('?\\w+'?)",
                Pattern.CASE_INSENSITIVE);
        Matcher mWhere = pWhere.matcher(sql);
        if (mWhere.find()) {
            return mWhere.group(1).replaceAll("'", "").trim();
        }
        return null;
    }
}
