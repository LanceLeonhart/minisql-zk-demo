package region;

import java.util.Scanner;

public class RegionServerLauncher {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入要启动的 RegionServer 数量：");
        int numRegions = scanner.nextInt();

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