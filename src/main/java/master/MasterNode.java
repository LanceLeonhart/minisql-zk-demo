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

public class MasterNode {
    private static final int PORT = 8888;
    private static final String ZK_REGION_PATH = "/regions";

    // regionName -> "host:port"
    private static final Map<String, String> regionMap = Collections.synchronizedMap(new TreeMap<>());
    // tableName -> primaryKeyColumn
    private static final Map<String, String> tablePKMap = Collections.synchronizedMap(new HashMap<>());

    // 正则：解析 CREATE TABLE 包含 PRIMARY KEY(col)
    private static final Pattern CREATE_P = Pattern.compile(
            "CREATE TABLE\\s+(\\w+)\\s*\\((.+?),\\s*PRIMARY KEY\\((\\w+)\\)\\)", Pattern.CASE_INSENSITIVE);

    // 通用提取表名：INSERT/SELECT/...FROM/DELETE FROM/UPDATE
    private static final Pattern TABLE_P = Pattern.compile(
            "^(?:INSERT INTO|SELECT.+FROM|DELETE FROM|UPDATE)\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) throws Exception {
        // 1. 连接 ZK 并监听 /regions
        CuratorFramework zk = ZkUtils.createZkClient();
        PathChildrenCache cache = new PathChildrenCache(zk, ZK_REGION_PATH, true);
        cache.getListenable().addListener((c, ev) -> {
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

        // 2. 启动服务
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
                        new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8), true
                )
        ) {
            String sql = in.readLine();
            System.out.println("[Master] Received SQL: " + sql);
            if (sql == null || sql.isBlank()) {
                out.println("Empty SQL.");
                return;
            }

            String up = sql.trim().toUpperCase(Locale.ROOT);
            boolean isCreate   = up.startsWith("CREATE TABLE");
            boolean isDrop     = up.startsWith("DROP TABLE");
            boolean isSelect   = up.startsWith("SELECT");
            boolean isSelectAll= isSelect && !up.contains("WHERE");

            // 缓存 PRIMARY KEY 列名
            if (isCreate) {
                Matcher m = CREATE_P.matcher(sql);
                if (m.find()) {
                    String table = m.group(1);
                    String pkCol  = m.group(3);
                    tablePKMap.put(table, pkCol);
                    System.out.printf("[Master] Cached PK for table %s: %s%n", table, pkCol);
                }
            }

            // 快照 region 名称
            List<String> regions;
            synchronized (regionMap) {
                regions = new ArrayList<>(regionMap.keySet());
            }
            if (regions.isEmpty()) {
                out.println("No regions available.");
                return;
            }

            if (isCreate || isDrop) {
                // 1) 广播 DDL
                for (String rg : regions) {
                    String res = forward(rg, sql, true);
                    out.printf("[%s] %s%n", rg, res);
                }

            } else if (isSelectAll) {
                // 2) 全表查询广播
                for (String rg : regions) {
                    for (String line : forwardAll(rg, sql)) {
                        out.printf("[%s] %s%n", rg, line);
                    }
                }

            } else {
                // 3/4) DML 或带 WHERE
                // 3a) 先提取表名
                Matcher tm = TABLE_P.matcher(sql);
                String table = tm.find() ? tm.group(1) : null;
                String pkCol = table != null ? tablePKMap.get(table) : null;

                // 3b) 提取主键值
                String pkVal = pkCol != null ? extractPK(sql, pkCol) : null;
                if (pkVal != null) {
                    // 单点路由
                    int idx = Math.abs(pkVal.hashCode()) % regions.size();
                    String rg = regions.get(idx);
                    String res = forward(rg, sql, false);
                    out.println(res);
                } else {
                    // 非主键条件广播
                    for (String rg : regions) {
                        String res = forward(rg, sql, false);
                        out.printf("[%s] %s%n", rg, res);
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("[Master] Error handling client: " + e.getMessage());
        }
    }

    private static String forward(String region, String sql, boolean allLines) {
        String addr = regionMap.get(region);
        String[] hp = addr.split(":");
        try (Socket rs = new Socket(hp[0], Integer.parseInt(hp[1]));
             PrintWriter rout = new PrintWriter(rs.getOutputStream(), true);
             BufferedReader rin = new BufferedReader(
                     new InputStreamReader(rs.getInputStream(), StandardCharsets.UTF_8))
        ) {
            rout.println(sql);
            if (!allLines) {
                String line = rin.readLine();
                return line != null ? line : "";
            }
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = rin.readLine()) != null) {
                if (sb.length()>0) sb.append("\n");
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    private static List<String> forwardAll(String region, String sql) {
        String addr = regionMap.get(region);
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

    private static String extractPK(String sql, String pkCol) {
        // INSERT 语法
        Pattern pIns = Pattern.compile(
                "INSERT INTO\\s+\\w+\\s*\\(([^)]+)\\)\\s*VALUES\\s*\\(([^)]+)\\)",
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
        // WHERE <pkCol> = value
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
