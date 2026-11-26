package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

// Starting, stopping and limits: what happens at the edges of the
// server's life, as opposed to inside a command.
@Timeout(30)
class ServerLifecycleTest {

    @TempDir
    Path tempDir;

    private Server server;

    private Server start(int port, int maxClients) throws IOException {
        Store store = new Store(
            tempDir.resolve("dump.rdb"), System::currentTimeMillis);
        Server started = new Server(port, store, maxClients);
        Thread acceptThread = new Thread(() -> {
            try {
                started.serve();
            } catch (IOException e) {
                // the test fails on its own when it cannot connect
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();
        return started;
    }

    @AfterEach
    void stop() {
        if (server != null) server.close();
    }

    // ── Client limit ──

    // Without the limit, an extra client would connect, send a
    // command and hang forever with no reply and no error.
    @Test
    void aClientOverTheLimitIsToldWhyAndDisconnected() throws Exception {
        server = start(0, 2);
        try (RespClient first = new RespClient(server.port());
             RespClient second = new RespClient(server.port())) {
            assertEquals("+PONG\r\n", first.command("PING"));
            assertEquals("+PONG\r\n", second.command("PING"));

            try (RespClient third = new RespClient(server.port())) {
                assertEquals("-ERR max number of clients reached\r\n",
                    third.readReply());
                assertThrows(EOFException.class, third::readReply);
            }

            // The clients already connected are not affected
            assertEquals("+PONG\r\n", first.command("PING"));
        }
    }

    @Test
    void aSlotIsFreedWhenAClientLeaves() throws Exception {
        server = start(0, 1);
        try (RespClient only = new RespClient(server.port())) {
            assertEquals("+PONG\r\n", only.command("PING"));
        }

        // The server notices the disconnect a moment later
        long deadline = System.currentTimeMillis() + 5_000;
        while (true) {
            try (RespClient next = new RespClient(server.port())) {
                if (next.command("PING").equals("+PONG\r\n")) return;
            } catch (IOException e) {
                // rejected: the slot is not free yet
            }
            if (System.currentTimeMillis() > deadline) {
                fail("the slot was never freed");
            }
            Thread.sleep(20);
        }
    }

    // ── Stopping ──

    @Test
    void closingTheServerDisconnectsItsClients() throws Exception {
        server = start(0, 10);
        try (RespClient client = new RespClient(server.port())) {
            assertEquals("+PONG\r\n", client.command("PING"));

            server.close();

            assertThrows(IOException.class, () -> {
                client.command("PING");
                client.command("PING");
            });
        }
    }

    // A restarted server must be able to take its old port at once.
    // Repeated, because the failure this guards against is a race:
    // close() returning before the accept loop has let go of the port.
    @Test
    void thePortCanBeReusedImmediatelyAfterClose() throws Exception {
        server = start(0, 10);
        int port = server.port();
        for (int restart = 0; restart < 25; restart++) {
            try (RespClient client = new RespClient(port)) {
                assertEquals("+PONG\r\n", client.command("PING"));
            }
            server.close();
            server = start(port, 10);
        }
        try (RespClient client = new RespClient(port)) {
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    // What the shutdown hook does: stop, then save. Data written just
    // before shutdown must be in the snapshot.
    @Test
    void dataWrittenBeforeShutdownIsInTheFinalSnapshot() throws Exception {
        Path snapshot = tempDir.resolve("final.rdb");
        Store store = new Store(snapshot, System::currentTimeMillis);
        store.startBackgroundThreads();
        server = new Server(0, store, 10);
        Thread acceptThread = new Thread(() -> {
            try {
                server.serve();
            } catch (IOException e) {
                // closed
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();

        try (RespClient client = new RespClient(server.port())) {
            assertEquals("+OK\r\n", client.command("SET", "last", "write"));
        }

        server.close();
        store.close();
        store.save();

        Store restarted = new Store(snapshot, System::currentTimeMillis);
        restarted.load();
        assertEquals("write", restarted.get("last"));
    }
}
