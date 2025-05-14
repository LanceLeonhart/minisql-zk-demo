package region;

import org.apache.curator.framework.CuratorFramework;
import util.ZkUtils;

import java.util.List;
import java.util.Scanner;


public class RegionServerLauncher {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入要启动的 RegionServer 数量：");
        int numRegions = scanner.nextInt();

        CuratorFramework zkClient = ZkUtils.createZkClient();
        List<String> existingRegions = zkClient.getChildren().forPath("/regions");
        for (String region : existingRegions) {
            zkClient.delete().forPath("/regions/" + region);  // 清理残留（即使是 EPHEMERAL 也可能还没被清）
        }

        for (int i = 1; i <= numRegions; i++) {
            final String regionName = "region" + i;
            final int port = 9000 + i;
            new Thread(() -> {
                try {
                    RegionServer.main(new String[]{regionName, String.valueOf(port)});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}