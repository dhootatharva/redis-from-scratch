package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Server {

    private static final int PORT = 6379;

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);

        // One store shared across ALL clients
        // Every client reads and writes the same data
        Store store = new Store();

        // One command handler that uses the store
        CommandHandler handler = new CommandHandler(store);

        System.out.println("Redis server started on port " + PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Client connected: " +
                clientSocket.getInetAddress());
            handleClient(clientSocket, handler);
        }
    }

    private static void handleClient(
            Socket clientSocket,
            CommandHandler handler) {
        try {
            RespParser parser = new RespParser(
                clientSocket.getInputStream()
            );
            OutputStream output = clientSocket.getOutputStream();

            while (true) {
                List<String> command = parser.parse();

                if (command == null) {
                    System.out.println("Client disconnected");
                    break;
                }

                System.out.println("Command: " + command);

                byte[] response = handler.handle(command);
                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
            if (!e.getMessage().contains("Connection reset")) {
                System.out.println("Client error: " + e.getMessage());
            }
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing: " + e.getMessage());
            }
        }
    }
}