package redis;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

// Starts a real server on a free port before each test and stops it
// afterwards. Tests connect to it with RespClient over TCP.
abstract class ServerTestBase {

    @TempDir
    Path tempDir;

    protected Server server;

    @BeforeEach
    void startServer() throws IOException {
        Store store = new Store(
            tempDir.resolve("dump.rdb"), System::currentTimeMillis);

        // Port 0 = let the operating system choose a free port
        server = new Server(0, store);

        Thread acceptThread = new Thread(() -> {
            try {
                server.serve();
            } catch (IOException e) {
                // the test will fail on its own when it cannot connect
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    protected RespClient connect() throws IOException {
        return new RespClient(server.port());
    }
}
