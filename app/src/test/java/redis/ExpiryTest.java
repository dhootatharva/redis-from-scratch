package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// Expiry tested with a clock the test controls: no sleeping, no
// timing flakiness, and exact assertions on TTL values.
class ExpiryTest {

    @TempDir
    Path tempDir;

    private TestClock clock;
    private Store store;
    private CommandHandler handler;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        store = new Store(tempDir.resolve("dump.rdb"), clock);
        handler = new CommandHandler(store);
    }

    private String run(String... command) {
        return new String(handler.handle(Arrays.asList(command)),
            StandardCharsets.UTF_8);
    }

    // Lazy expiry: the key is removed when someone touches it
    @Test
    void keyIsGoneOnceItsDeadlineHasPassed() {
        run("SET", "k", "v", "PX", "50");

        clock.advance(50);
        assertEquals("$1\r\nv\r\n", run("GET", "k"),
            "still alive exactly at the deadline");

        clock.advance(1);
        assertEquals("$-1\r\n", run("GET", "k"));
        assertEquals(":0\r\n", run("EXISTS", "k"));
        assertEquals(":-2\r\n", run("TTL", "k"));
    }

    // Active expiry: the sweep removes keys nobody reads again
    @Test
    void sweepRemovesExpiredKeysThatAreNeverRead() {
        run("SET", "temporary", "v", "EX", "1");
        run("SET", "permanent", "v");

        clock.advance(1001);
        assertEquals(1, store.removeExpiredKeys());

        assertFalse(store.holdsInMemory("temporary"));
        assertTrue(store.holdsInMemory("permanent"));
        assertEquals(0, store.keysWithDeadline());
    }

    @Test
    void sweepLeavesLiveKeysAlone() {
        run("SET", "k", "v", "EX", "10");

        clock.advance(9_000);
        assertEquals(0, store.removeExpiredKeys());
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
    }

    @Test
    void ttlCountsDownAsTimePasses() {
        run("SET", "k", "v", "EX", "100");
        assertEquals(":100\r\n", run("TTL", "k"));

        clock.advance(30_000);
        assertEquals(":70\r\n", run("TTL", "k"));
        assertEquals(":70000\r\n", run("PTTL", "k"));
    }

    @Test
    void expireAddsADeadlineAndPersistRemovesIt() {
        run("SET", "k", "v");
        assertEquals(":1\r\n", run("EXPIRE", "k", "10"));
        assertEquals(":1\r\n", run("PERSIST", "k"));

        clock.advance(60_000);
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
    }

    // A plain SET replaces the value and clears any old deadline
    @Test
    void setWithoutExpiryClearsAnExistingDeadline() {
        run("SET", "k", "old", "EX", "10");
        run("SET", "k", "new");

        clock.advance(60_000);
        assertEquals("$3\r\nnew\r\n", run("GET", "k"));
    }
}
