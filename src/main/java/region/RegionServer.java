package region;

import org.apache.curator.framework.CuratorFramework;
import util.ZkUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class RegionServer {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java RegionServer <regionName> <port>");
            System.exit(1);
        }

        String regionName = args[0];
        int port = Integer.parseInt(args[1]);

        CuratorFramework zkClient = ZkUtils.createZkClient();
        String path = "/regions/" + regionName;
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, ("localhost:" + port).getBytes());
            System.out.println("[RegionServer] Registered at " + path);
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[RegionServer] Listening on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String input = in.readLine();
                System.out.println("[" + regionName + "] Received: " + input);
                socket.close();
            }
        }
    }
}
