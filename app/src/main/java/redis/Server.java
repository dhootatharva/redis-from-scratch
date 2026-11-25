package redis;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Server implements AutoCloseable {

    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_MAX_CLIENTS = 100;

    // How many finished handshakes the operating system may hold
    // while the accept loop is busy
    private static final int BACKLOG = 511;

    private final ServerSocket serverSocket;
    private final CommandHandler handler;
    private final ExecutorService threadPool;
    private final int maxClients;

    // Every connected client, so close() can disconnect them
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private volatile boolean closed = false;

    // Usage: java -jar app.jar [port] [--verbose]
    public static void main(String[] args) throws IOException {

        int port = DEFAULT_PORT;
        for (String arg : args) {
            if (arg.equals("--verbose")) {
                Log.verbose = true;
            } else {
                port = Integer.parseInt(arg);
            }
        }

        Store store = new Store();

        // Bind the port first, then load the snapshot and start the
        // background threads
        Server server = new Server(port, store);
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

        Log.info("Redis server started on port " + server.port());
        Log.info("Max clients: " + server.maxClients);

        // Shutdown hook — runs when Ctrl+C is pressed.
        // Order matters: stop taking commands, stop the background
        // threads, and only then write the final snapshot, so it
        // contains everything and nothing changes underneath it.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("\nShutting down server...");
            server.close();
            store.close();
            store.save();
            Log.info("Server stopped cleanly.");
        }));

        server.serve();
    }

    public Server(int port, Store store) throws IOException {
        this(port, store, DEFAULT_MAX_CLIENTS);
    }

    // Bind the port and get ready to serve. Pass port 0 to let the
    // operating system pick a free port (used by tests).
    public Server(int port, Store store, int maxClients)
            throws IOException {

        // Options that affect binding must be set BEFORE bind, so the
        // socket is created unbound first.
        this.serverSocket = new ServerSocket();

        // After a server stops, its port stays reserved for a minute
        // or so (TCP's TIME_WAIT state). SO_REUSEADDR lets a restarted
        // server bind it straight away. On Windows the same option
        // means something else — it would let a second process take a
        // port that is in use — and Windows does not need it for a
        // quick restart, so it is left off there.
        if (!isWindows()) {
            this.serverSocket.setReuseAddress(true);
        }
        this.serverSocket.bind(new InetSocketAddress(port), BACKLOG);

        this.handler = new CommandHandler(store);
        this.maxClients = maxClients;
        this.threadPool = Executors.newFixedThreadPool(maxClients);
    }

    private static boolean isWindows() {
        return System.getProperty("os.name", "")
            .toLowerCase().contains("win");
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

            // Every worker thread is tied to one client. With no
            // worker free, a new client would wait in the pool's queue
            // with no reply and no error. Tell it why, and hang up.
            if (clients.size() >= maxClients) {
                reject(clientSocket);
                continue;
            }

            clients.add(clientSocket);
            threadPool.submit(() -> {
                handleClient(clientSocket);
            });
        }
    }

    private static void reject(Socket clientSocket) {
        try {
            OutputStream output = clientSocket.getOutputStream();
            output.write(RespSerializer.error(
                "max number of clients reached"));
            output.flush();
            clientSocket.shutdownOutput();
        } catch (IOException e) {
            // the client went away first
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                // already closed
            }
        }
    }

    // Stop accepting, disconnect every client, stop the workers.
    // A worker blocked in a socket read cannot be interrupted; closing
    // its socket is what wakes it up.
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
        Log.debug(() -> "Client connected: " + clientAddress);

        try {
            // Replies are small and the client is waiting for each
            // one, so send them immediately instead of letting TCP
            // hold them back to combine with later data (Nagle)
            clientSocket.setTcpNoDelay(true);

            // Ask the operating system to probe idle connections, so
            // a client that vanished without closing (power cut,
            // dropped network) is eventually noticed and its worker
            // thread freed
            clientSocket.setKeepAlive(true);

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
                    Log.info("Protocol error from " + clientAddress
                        + ": " + e.getMessage());
                    replyAndHangUp(clientSocket, output,
                        RespSerializer.error(
                            "Protocol error: " + e.getMessage()));
                    break;
                } catch (OutOfMemoryError e) {
                    // A value within the protocol limit but larger
                    // than this JVM's heap. Drop this client only.
                    Log.info("Out of memory reading a command from "
                        + clientAddress);
                    replyAndHangUp(clientSocket, output,
                        RespSerializer.error("out of memory"));
                    break;
                }

                if (command == null) {
                    Log.debug(() ->
                        "Client disconnected: " + clientAddress);
                    break;
                }

                // An empty command (*0, or a blank line) gets no
                // reply at all, as in Redis
                if (command.isEmpty()) {
                    continue;
                }

                Log.debug(() -> "Command: " + command);

                byte[] response;
                try {
                    response = handler.handle(command);
                } catch (RuntimeException e) {
                    // A bug in a command must not silently drop the
                    // connection: the client gets an error reply and
                    // the stream stays in step
                    Log.info("Internal error: " + e);
                    response = RespSerializer.error("internal error");
                }
                output.write(response);
                output.flush();
            }

        } catch (IOException e) {
            // A client that disappears is normal, not an error
            Log.debug(() -> "Client " + clientAddress + ": " + e);
        } finally {
            clients.remove(clientSocket);
            try {
                clientSocket.close();
            } catch (IOException e) {
                // already closed
            }
        }
    }
}
