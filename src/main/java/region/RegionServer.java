package region;

import org.apache.curator.framework.CuratorFramework;
import util.ZkUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class RegionServer {
    private static final String REGION_NAME = "region1";
    private static final int PORT = 9001;

    public static void main(String[] args) throws Exception {
        CuratorFramework zkClient = ZkUtils.createZkClient();
        String path = "/regions/" + REGION_NAME;
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, ("localhost:" + PORT).getBytes());
            System.out.println("[RegionServer] Registered at " + path);
        }

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("[RegionServer] Listening on port " + PORT);
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String input = in.readLine();
                System.out.println("[RegionServer] Received: " + input);
                socket.close();
            }
        }
    }
}