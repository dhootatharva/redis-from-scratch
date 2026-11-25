package redis;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Server implements AutoCloseable {

    private static final int DEFAULT_PORT = 6379;
    private static final int THREAD_POOL_SIZE = 100;

    private final ServerSocket serverSocket;
    private final CommandHandler handler;
    private final ExecutorService threadPool;

    // Every connected client, so close() can disconnect them
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private volatile boolean closed = false;

    public static void main(String[] args) throws IOException {

        // Optional port argument: java -jar app.jar 6380
        final int PORT = args.length > 0
            ? Integer.parseInt(args[0])
            : DEFAULT_PORT;

        Store store = new Store();

        // Bind the port first, then load the snapshot and start the
        // background threads (same order as before the refactor)
        Server server = new Server(PORT, store);
        store.load();
        store.startBackgroundThreads();

        System.out.println("Redis server started on port " + PORT);
        System.out.println("Thread pool size: " + THREAD_POOL_SIZE);

        // Shutdown hook — runs when Ctrl+C is pressed
        // Saves data to disk before the JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down server...");

            // Stop accepting new tasks
            server.threadPool.shutdown();
            try {
                // Wait up to 5 seconds for running tasks to finish
                server.threadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Save everything to disk
            store.save();

            System.out.println("Server stopped cleanly.");
        }));

        server.serve();
    }

    // Bind the port and get ready to serve. Pass port 0 to let the
    // operating system pick a free port (used by tests).
    public Server(int port, Store store) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.serverSocket.setReuseAddress(true);
        this.handler = new CommandHandler(store);
        this.threadPool =
            Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    }

    // The port actually bound — differs from the requested one
    // when the server was created with port 0
    public int port() {
        return serverSocket.getLocalPort();
    }

    // The accept loop. Blocks until close() is called.
    public void serve() throws IOException {
        while (true) {
            Socket clientSocket;
            try {
                clientSocket = serverSocket.accept();
            } catch (SocketException e) {
                // close() closed the listening socket
                if (closed) return;
                throw e;
            }
            clients.add(clientSocket);
            threadPool.submit(() -> {
                handleClient(clientSocket);
            });
        }
    }

    // Stop accepting, disconnect every client, stop the workers
    @Override
    public void close() {
        closed = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            // already closed
        }
        for (Socket client : clients) {
            try {
                client.close();
            } catch (IOException e) {
                // already closed
            }
        }
        threadPool.shutdownNow();
    }

    private void handleClient(Socket clientSocket) {

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
            clients.remove(clientSocket);
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println(
                    "Error closing: " + e.getMessage());
            }
        }
    }
}
