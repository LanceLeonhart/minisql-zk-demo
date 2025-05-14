package region;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * 用 ProcessBuilder 在多个独立 JVM 中启动 RegionServer
 */
public class RegionServerLauncher {
    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入要启动的 RegionServer 数量：");
        int numRegions = scanner.nextInt();

        // 1. 清理旧的 /regions 临时节点（可选，保持原有逻辑）
        ZooKeeperCleanup.clean("/regions");

        // 2. 逐个启动独立进程
        for (int i = 1; i <= numRegions; i++) {
            String regionName = "region" + i;
            String port       = String.valueOf(9000 + i);

            ProcessBuilder pb = new ProcessBuilder(
                    "java",
                    "-cp", "target/classes:target/dependency/*", // 根据你的 classpath 调整
                    "region.RegionServer",
                    regionName,
                    port
            );
            pb.inheritIO(); // 将子进程的 stdout/stderr 也输出在当前控制台
            pb.start();

            System.out.println("[Launcher] 启动 RegionServer " + regionName + " on port " + port);
        }

    }
}
