package client;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private static final String MASTER_HOST = "localhost";
    private static final int MASTER_PORT = 8888;

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Client SQL> ");
            String line = scanner.nextLine();
            if (line.trim().equalsIgnoreCase("exit")) break;

            Socket socket = new Socket(MASTER_HOST, MASTER_PORT);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(line); // 发送 SQL
            String response = in.readLine(); // 接收响应
            System.out.println("[Client] Got response: " + response);

            socket.close();
        }
    }
}
