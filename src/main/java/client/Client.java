package client;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

// java -cp target/classes client.Client < Test.sql > run.log

public class Client {
    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 8888;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Client SQL> ");
            String line = scanner.nextLine();
            if (line.trim().equalsIgnoreCase("exit")) break;

            try (Socket socket = new Socket(MASTER_HOST, MASTER_PORT);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println(line); // 发送 SQL

                // 循环读取所有行，直到对端关闭
                StringBuilder sb = new StringBuilder();
                String respLine;
                while ((respLine = in.readLine()) != null) {
                    sb.append(respLine).append(System.lineSeparator());
                }

                System.out.println("[Client] Got response:\n" + sb.toString().trim());
            } catch (IOException e) {
                System.err.println("Error communicating with master: " + e.getMessage());
            }
        }
    }
}
