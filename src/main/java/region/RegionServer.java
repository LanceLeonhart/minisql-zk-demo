package region;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import util.ZkUtils;
import minisql.SimpleSQLExecutor;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class RegionServer {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java RegionServer <regionName> <port>");
            System.exit(1);
        }

        // Get region name and port from args input
        String regionName = args[0];
        int port = Integer.parseInt(args[1]);

        CuratorFramework zkClient = ZkUtils.createZkClient();
        String path = "/regions/" + regionName;
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, ("localhost:" + port).getBytes());

            System.out.println("[RegionServer] Registered at " + path);
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("[" + regionName + "] Listening on port " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String sql = in.readLine();
                System.out.println("[" + regionName + "] Received: " + sql);

                String result = SimpleSQLExecutor.execute(sql);

                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(result);
                socket.close();
            }
        }
    }
}
