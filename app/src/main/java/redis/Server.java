package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Server {

    private static final int PORT = 6379;
    private static final int THREAD_POOL_SIZE = 100;

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);

        Store store = new Store();
        CommandHandler handler = new CommandHandler(store);

        ExecutorService threadPool =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        System.out.println("Redis server started on port " + PORT);
        System.out.println("Thread pool size: " + THREAD_POOL_SIZE);

        // Shutdown hook — runs when Ctrl+C is pressed
        // Saves data to disk before the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down server...");

            // Stop accepting new tasks
            threadPool.shutdown();
            try {
                // Wait up to 5 seconds for running tasks to finish
                threadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Save everything to disk
            store.save();

            System.out.println("Server stopped cleanly.");
        }));

        while (true) {
            Socket clientSocket = serverSocket.accept();
            threadPool.submit(() -> {
                handleClient(clientSocket, handler);
            });
        }
    }

    private static void handleClient(
            Socket clientSocket,
            CommandHandler handler) {

        String clientAddress =
            clientSocket.getInetAddress().getHostAddress();
        System.out.println("[Thread "
            + Thread.currentThread().getId()
            + "] Client connected: " + clientAddress);

        try {
            RespParser parser = new RespParser(
                clientSocket.getInputStream()
            );
            OutputStream output = clientSocket.getOutputStream();

            while (true) {
                List<String> command = parser.parse();

                if (command == null) {
                    System.out.println("[Thread "
                        + Thread.currentThread().getId()
                        + "] Client disconnected: "
                        + clientAddress);
                    break;
                }

                System.out.println("[Thread "
                    + Thread.currentThread().getId()
                    + "] Command: " + command);

                byte[] response = handler.handle(command);
                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
            if (!e.getMessage().contains("Connection reset")) {
                System.out.println(
                    "Client error: " + e.getMessage());
            }
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println(
                    "Error closing: " + e.getMessage());
            }
        }
    }
}