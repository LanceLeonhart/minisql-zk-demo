package master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import util.ZkUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.regex.*;

public class MasterNode {
    private static final int PORT = 8888;
    private static final String ZK_REGION_PATH = "/regions";
    private static final Map<String, String> regionMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        // 1. 连接 ZooKeeper 并监听 /regions 子节点
        CuratorFramework zk = ZkUtils.createZkClient();
        PathChildrenCache cache = new PathChildrenCache(zk, ZK_REGION_PATH, true);
        cache.getListenable().addListener((client, event) -> {
            regionMap.clear();
            for (ChildData data : cache.getCurrentData()) {
                String name = data.getPath().substring(ZK_REGION_PATH.length() + 1);
                String addr = new String(data.getData());
                regionMap.put(name, addr);
            }
            System.out.println("[Master] regions = " + regionMap.keySet());
        });
        cache.start();

        // 2. 启动 TCP 服务，接收 Client 请求
        try (ServerSocket server = new ServerSocket(PORT)) {
            System.out.println("[Master] Listening on port " + PORT);
            while (true) {
                try (Socket client = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                     PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

                    String sql = in.readLine();
                    System.out.println("[Master] Received SQL: " + sql);

                    // 3. 提取主键值（假设主键列名为 "id"）
                    String pkVal = extractPrimaryKey(sql, "id");

                    // 4. 获取所有 region 名称并排序
                    List<String> regions = new ArrayList<>(regionMap.keySet());
                    Collections.sort(regions);

                    if (pkVal != null && !regions.isEmpty()) {
                        // 单点路由：按主键 hash 到某个 region
                        int idx = Math.abs(pkVal.hashCode()) % regions.size();
                        String target = regions.get(idx);
                        String result = forwardToRegion(target, sql);
                        out.println(result);

                    } else {
                        // 广播路由：发给所有 region，合并结果
                        StringBuilder sb = new StringBuilder();
                        for (String region : regions) {
                            String res = forwardToRegion(region, sql);
                            sb.append("[").append(region).append("] ").append(res).append("\n");
                        }
                        out.println(sb.toString().trim());
                    }
                } catch (Exception e) {
                    // 个别请求处理出错时打印错误但不影响服务
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 将 SQL 发给指定的 RegionServer 并返回它的第一行响应
     */
    private static String forwardToRegion(String regionName, String sql) throws IOException {
        String addr = regionMap.get(regionName);
        String[] parts = addr.split(":");
        try (Socket rs = new Socket(parts[0], Integer.parseInt(parts[1]));
             PrintWriter rout = new PrintWriter(rs.getOutputStream(), true);
             BufferedReader rin = new BufferedReader(new InputStreamReader(rs.getInputStream()))) {
            rout.println(sql);
            return rin.readLine();
        }
    }

    /**
     * 从 INSERT/SELECT/DELETE/UPDATE 语句中提取主键列的值
     * @param sql   原始 SQL
     * @param pkCol 主键列名（如 "id"）
     * @return 提取到的主键值，或 null（如全表查询、按非主键列条件等）
     */
    private static String extractPrimaryKey(String sql, String pkCol) {
        // 1) INSERT INTO users (id, ...) VALUES (42, ...)
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
        // 2) WHERE id = 42
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
