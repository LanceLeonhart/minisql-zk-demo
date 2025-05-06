package master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import util.ZkUtils;

import java.io.OutputStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MasterNode {
    private static final int PORT = 8888;
    private static final String ZK_REGION_PATH = "/regions";
    private static final Map<String, String> regionMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        CuratorFramework zkClient = ZkUtils.createZkClient();

        PathChildrenCache cache = new PathChildrenCache(zkClient, ZK_REGION_PATH, true);
        cache.getListenable().addListener((client, event) -> {
            List<ChildData> children = cache.getCurrentData();
            regionMap.clear();
            for (ChildData child : children) {
                String name = child.getPath().substring(ZK_REGION_PATH.length() + 1);
                String addr = new String(child.getData());
                regionMap.put(name, addr);
            }
            System.out.println("[Master] Region map updated: " + regionMap);
        });
        cache.start();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("[Master] Listening on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                byte[] buf = clientSocket.getInputStream().readNBytes(1024);
                String query = new String(buf, StandardCharsets.UTF_8).trim();
                System.out.println("[Master] Received from client: " + query);

                if (!regionMap.isEmpty()) {
                    int hash = Math.abs(query.hashCode());
                    int regionIndex = hash % regionMap.size();
                    String regionName = regionMap.keySet().stream().sorted().toList().get(regionIndex);
                    String addr = regionMap.get(regionName);

                    String[] parts = addr.split(":");
                    Socket regionSocket = new Socket(parts[0], Integer.parseInt(parts[1]));
                    OutputStream out = regionSocket.getOutputStream();
                    out.write(query.getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    regionSocket.close();
                }
                clientSocket.close();
            }
        }
    }
}
