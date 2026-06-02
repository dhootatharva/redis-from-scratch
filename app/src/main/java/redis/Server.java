package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server {

    private static final int PORT = 6379;

    // Maximum number of threads in our pool
    // This means at most 100 clients handled simultaneously
    private static final int THREAD_POOL_SIZE = 100;

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);

        // One store shared across ALL threads
        // ConcurrentHashMap inside Store keeps this safe
        Store store = new Store();
        CommandHandler handler = new CommandHandler(store);

        // Create a thread pool
        // newFixedThreadPool(100) = exactly 100 threads, no more
        ExecutorService threadPool =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        System.out.println("Redis server started on port " + PORT);
        System.out.println("Thread pool size: " + THREAD_POOL_SIZE);

        // Shutdown hook — runs when you press Ctrl+C
        // Cleanly shuts down the thread pool instead of killing it
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down server...");
            threadPool.shutdown();
        }));

        // Main loop — only accepts connections, never handles them
        while (true) {
            // Wait for a client
            Socket clientSocket = serverSocket.accept();

            // Immediately hand off to thread pool
            // This line returns instantly — main thread never blocks
            threadPool.submit(() -> {
                handleClient(clientSocket, handler);
            });

            // Main thread is already back here, ready for next client
        }
    }

    // This method runs inside a thread pool thread
    // NOT in the main thread
    // Each client gets their own independent execution here
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
                        + "] Client disconnected: " + clientAddress);
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