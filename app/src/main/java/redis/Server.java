package redis;

import java.io.IOException;
import java.io.InputStream;
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
        try {
            store.load();
        } catch (CorruptSnapshotException e) {
            // Do not start empty: the next automatic save would
            // overwrite the damaged file and destroy what is left.
            System.err.println("[RDB] dump.rdb is damaged ("
                + e.getMessage() + "). Refusing to start.");
            System.err.println("[RDB] Move or delete the file to "
                + "start with an empty database.");
            System.exit(1);
        }
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

    // Send a final reply and close the connection in a way that lets
    // the reply actually arrive.
    //
    // If a socket is closed while bytes from the client are still
    // unread, the operating system sends a reset (RST) instead of a
    // normal close, and the client may never see the reply. So: send
    // the reply, announce "no more data from me", then read and throw
    // away what the client is still sending, for a short time.
    private static void replyAndHangUp(Socket socket,
            OutputStream output, byte[] reply) {
        try {
            output.write(reply);
            output.flush();
            socket.shutdownOutput();
            socket.setSoTimeout(1000);
            InputStream in = socket.getInputStream();
            byte[] scratch = new byte[4096];
            long discarded = 0;
            while (discarded < 1024 * 1024) {
                int n = in.read(scratch);
                if (n == -1) break;
                discarded += n;
            }
        } catch (IOException e) {
            // The client is gone or too slow; closing is all that is left
        }
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
                List<String> command;
                try {
                    command = parser.parse();
                } catch (ProtocolException e) {
                    // Not valid RESP. We can no longer tell where the
                    // next command starts, so say why and hang up.
                    System.out.println("[Thread "
                        + Thread.currentThread().getId()
                        + "] Protocol error from " + clientAddress
                        + ": " + e.getMessage());
                    replyAndHangUp(clientSocket, output,
                        RespSerializer.error(
                            "Protocol error: " + e.getMessage()));
                    break;
                } catch (OutOfMemoryError e) {
                    // A value within the protocol limit but larger
                    // than this JVM's heap. Drop this client only.
                    replyAndHangUp(clientSocket, output,
                        RespSerializer.error("out of memory"));
                    break;
                }

                if (command == null) {
                    System.out.println("[Thread "
                        + Thread.currentThread().getId()
                        + "] Client disconnected: "
                        + clientAddress);
                    break;
                }

                // An empty command (*0, or a blank line) gets no
                // reply at all, as in Redis
                if (command.isEmpty()) {
                    continue;
                }

                System.out.println("[Thread "
                    + Thread.currentThread().getId()
                    + "] Command: " + command);

                byte[] response;
                try {
                    response = handler.handle(command);
                } catch (RuntimeException e) {
                    // A bug in a command must not silently drop the
                    // connection: the client gets an error reply and
                    // the stream stays in step
                    System.out.println("Internal error: " + e);
                    response = RespSerializer.error("internal error");
                }
                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
            String message = String.valueOf(e.getMessage());
            if (!message.contains("Connection reset")) {
                System.out.println("Client error: " + message);
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
