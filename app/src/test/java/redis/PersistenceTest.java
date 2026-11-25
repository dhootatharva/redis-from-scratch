package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// Snapshot save and load, using a temp directory and a controlled clock.
// "Restarting the server" is modelled as: save the first store, then
// build a second store on the same file and load it.
class PersistenceTest {

    @TempDir
    Path tempDir;

    private Path snapshot;
    private TestClock clock;

    @BeforeEach
    void setUp() {
        snapshot = tempDir.resolve("dump.rdb");
        clock = new TestClock();
    }

    private Store newStore() {
        return new Store(snapshot, clock);
    }

    private static String run(Store store, String... command) {
        return new String(
            new CommandHandler(store).handle(Arrays.asList(command)),
            StandardCharsets.UTF_8);
    }

    private Store restart(Store before) {
        before.save();
        Store after = newStore();
        after.load();
        return after;
    }

    @Test
    void everyTypeSurvivesARestart() {
        Store before = newStore();
        run(before, "SET", "s", "hello");
        run(before, "RPUSH", "l", "a", "b", "c");
        run(before, "HSET", "h", "field", "value");

        Store after = restart(before);

        assertEquals("$5\r\nhello\r\n", run(after, "GET", "s"));
        assertEquals("*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
            run(after, "LRANGE", "l", "0", "-1"));
        assertEquals("$5\r\nvalue\r\n", run(after, "HGET", "h", "field"));
        assertEquals(":3\r\n", run(after, "DBSIZE"));
    }

    // Deadlines are absolute, so a TTL keeps running while the server
    // is down: 100 s TTL, 40 s of downtime, 60 s left afterwards.
    @Test
    void ttlKeepsCountingWhileTheServerIsDown() {
        Store before = newStore();
        run(before, "SET", "session", "x", "EX", "100");
        before.save();

        clock.advance(40_000);
        Store after = newStore();
        after.load();

        assertEquals(":60\r\n", run(after, "TTL", "session"));
    }

    @Test
    void keysThatExpiredDuringDowntimeAreNotLoaded() {
        Store before = newStore();
        run(before, "SET", "short", "x", "EX", "10");
        run(before, "SET", "forever", "y");
        before.save();

        clock.advance(11_000);
        Store after = newStore();
        after.load();

        assertFalse(after.holdsInMemory("short"));
        assertEquals("$1\r\ny\r\n", run(after, "GET", "forever"));
    }

    @Test
    void alreadyExpiredKeysAreNotWritten() {
        Store before = newStore();
        run(before, "SET", "gone", "x", "PX", "5");
        clock.advance(10);

        Store after = restart(before);

        assertEquals(":0\r\n", run(after, "DBSIZE"));
    }

    @Test
    void startsEmptyWhenThereIsNoSnapshot() {
        Store store = newStore();
        store.load();

        assertEquals(":0\r\n", run(store, "DBSIZE"));
    }

    // A second save must fully replace the first, through the temp
    // file, and leave no temp file behind
    @Test
    void laterSnapshotReplacesEarlierOne() {
        Store before = newStore();
        run(before, "SET", "a", "1");
        before.save();

        run(before, "DEL", "a");
        run(before, "SET", "b", "2");
        Store after = restart(before);

        assertEquals("$-1\r\n", run(after, "GET", "a"));
        assertEquals("$1\r\n2\r\n", run(after, "GET", "b"));
        assertTrue(Files.exists(snapshot));
        assertFalse(Files.exists(tempDir.resolve("dump.rdb.tmp")));
    }
}
