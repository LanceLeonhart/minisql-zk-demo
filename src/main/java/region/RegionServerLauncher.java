package region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * 新版 RegionServerLauncher：
 *  - 每个 RegionServer 都启动在独立的 JVM (Process)
 *  - 支持输入 exit/list/help 等命令
 *  - 在 JVM 关闭时自动销毁所有子进程
 */
public class RegionServerLauncher {

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);

        // 1. 读入要启动的 RegionServer 数量
        System.out.print("请输入要启动的 RegionServer 数量：");
        int num = Integer.parseInt(scanner.nextLine().trim());

        // 2. 存放所有子进程
        List<Process> children = new ArrayList<>();

        // 3. 启动每个 RegionServer 作为独立进程
        for (int i = 1; i <= num; i++) {
            String region = "region" + i;
            String port   = String.valueOf(9000 + i);

            ProcessBuilder pb = new ProcessBuilder(
                    "java",
                    "-cp", "target/classes:target/dependency/*",
                    "region.RegionServer",
                    region, port
            );
            // 将子进程的 stdout/stderr 也输出到 Launcher 控制台
            pb.inheritIO();

            Process proc = pb.start();
            children.add(proc);

            System.out.printf("[Launcher] 启动 %s on port %s (pid=%d)%n",
                    region, port, proc.pid());
        }

        // 4. 注册 JVM 退出钩子，保证 CTRL+C / 停止时能销毁子进程
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Launcher] JVM 退出，销毁所有子进程...");
            children.forEach(p -> {
                System.out.printf("  -> 杀掉 pid=%d%n", p.pid());
                p.destroy();
            });
        }));

        // 5. 进入命令循环
        System.out.println("输入命令：exit (停止所有并退出)、list (列出子进程)、help (帮助)");
        while (true) {
            System.out.print("> ");
            String cmd = scanner.nextLine().trim();
            if (cmd.equalsIgnoreCase("exit")) {
                System.out.println("[Launcher] 正在停止所有 RegionServer...");
                children.forEach(p -> {
                    System.out.printf("  -> 销毁 pid=%d%n", p.pid());
                    p.destroy();
                });
                System.out.println("[Launcher] 全部停止，Launcher 退出。");
                break;
            } else if (cmd.equalsIgnoreCase("list")) {
                System.out.println("[Launcher] 当前子进程：");
                children.forEach(p -> {
                    System.out.printf("  pid=%d, alive=%b%n", p.pid(), p.isAlive());
                });
            } else if (cmd.equalsIgnoreCase("help")) {
                System.out.println("可用命令：");
                System.out.println("  exit  - 停止所有 RegionServer 并退出 Launcher");
                System.out.println("  list  - 列出当前所有子进程的 PID 和状态");
                System.out.println("  help  - 显示本帮助");
            } else {
                System.out.println("未知命令。输入 help 查看可用命令。");
            }
        }

        // 6. 退出 Launcher
        System.exit(0);
    }
}
