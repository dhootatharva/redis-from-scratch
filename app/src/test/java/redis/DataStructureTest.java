package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

// The two structures chosen for speed: lists as double-ended queues,
// and active expiry by random sampling. These tests check that the
// faster structures still give the right answers, and that the cost
// of one sweep really is bounded.
class DataStructureTest {

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
        return TestWire.run(handler, command);
    }

    // ── Lists ──

    @Test
    void pushesAndPopsAtBothEnds() {
        assertEquals(":3\r\n", run("RPUSH", "l", "b", "c", "d"));
        assertEquals(":4\r\n", run("LPUSH", "l", "a"));
        assertEquals(":5\r\n", run("RPUSH", "l", "e"));

        assertEquals("$1\r\na\r\n", run("LPOP", "l"));
        assertEquals("$1\r\ne\r\n", run("RPOP", "l"));
        assertEquals("*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n",
            run("LRANGE", "l", "0", "-1"));
    }

    @Test
    void lrangeHandlesEveryKindOfIndex() {
        run("RPUSH", "l", "a", "b", "c", "d", "e");

        assertEquals("*2\r\n$1\r\nb\r\n$1\r\nc\r\n",
            run("LRANGE", "l", "1", "2"));
        // negative indexes count from the end
        assertEquals("*2\r\n$1\r\nd\r\n$1\r\ne\r\n",
            run("LRANGE", "l", "-2", "-1"));
        // out-of-range ends are clamped
        assertEquals("*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
            + "$1\r\nd\r\n$1\r\ne\r\n", run("LRANGE", "l", "-100", "100"));
        // an empty or inverted range is empty, not an error
        assertEquals("*0\r\n", run("LRANGE", "l", "3", "1"));
        assertEquals("*0\r\n", run("LRANGE", "l", "10", "20"));
        assertEquals("*0\r\n", run("LRANGE", "missing", "0", "-1"));
    }

    // LINDEX walks in from the nearer end; both directions must agree
    @Test
    void lindexFindsEveryPositionFromEitherEnd() {
        int size = 11;
        for (int i = 0; i < size; i++) {
            run("RPUSH", "l", "item" + i);
        }
        for (int i = 0; i < size; i++) {
            String expected = "item" + i;
            String reply =
                "$" + expected.length() + "\r\n" + expected + "\r\n";
            assertEquals(reply, run("LINDEX", "l", String.valueOf(i)));
            assertEquals(reply,
                run("LINDEX", "l", String.valueOf(i - size)));
        }
        assertEquals("$-1\r\n", run("LINDEX", "l", "11"));
        assertEquals("$-1\r\n", run("LINDEX", "l", "-12"));
    }

    // With a list that copied or shifted on every push, this took
    // time proportional to the square of the list length.
    @Test
    @Timeout(10)
    void aLongListStaysFastAtBothEnds() {
        int n = 200_000;
        for (int i = 0; i < n; i++) {
            run("LPUSH", "l", "x");
        }
        assertEquals(":" + n + "\r\n", run("LLEN", "l"));
        for (int i = 0; i < n; i++) {
            run("LPOP", "l");
        }
        assertEquals(":0\r\n", run("EXISTS", "l"));
    }

    @Test
    void listOrderSurvivesARestart() {
        run("RPUSH", "l", "a", "b", "c");
        run("LPUSH", "l", "first");
        assertTrue(store.save());

        Store after = new Store(tempDir.resolve("dump.rdb"), clock);
        after.load();
        assertEquals(
            "*4\r\n$5\r\nfirst\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n",
            TestWire.run(new CommandHandler(after),
                "LRANGE", "l", "0", "-1"));
    }

    // ── Active expiry by sampling ──

    // The point of sampling: one sweep costs the same whether 100 or
    // 100,000 keys have a deadline.
    @Test
    void oneSweepExaminesAFixedNumberOfKeys() {
        for (int i = 0; i < 10_000; i++) {
            run("SET", "k" + i, "v", "EX", "1000");
        }
        long before = store.keysExaminedBySweeps();

        assertEquals(0, store.removeExpiredKeys());

        long examined = store.keysExaminedBySweeps() - before;
        assertEquals(20, examined);
        assertEquals(10_000, store.keysWithDeadline());
    }

    // When most sampled keys turn out expired, a sweep keeps going
    @Test
    void aSweepContinuesWhileItKeepsFindingExpiredKeys() {
        for (int i = 0; i < 5_000; i++) {
            run("SET", "k" + i, "v", "EX", "1");
        }
        clock.advance(2_000);

        int removed = store.removeExpiredKeys();

        assertTrue(removed > 20,
            "only " + removed + " removed: the sweep stopped early");
    }

    @Test
    void repeatedSweepsRemoveEverythingThatExpired() {
        for (int i = 0; i < 2_000; i++) {
            run("SET", "dead" + i, "v", "EX", "1");
            run("SET", "alive" + i, "v", "EX", "1000");
        }
        clock.advance(2_000);

        // Sampling is random, so allow many passes
        int removed = 0;
        for (int pass = 0; pass < 100_000 && removed < 2_000; pass++) {
            removed += store.removeExpiredKeys();
        }

        assertEquals(2_000, removed);
        assertEquals(2_000, store.keysWithDeadline());
        assertFalse(store.holdsInMemory("dead0"));
        assertFalse(store.holdsInMemory("dead1999"));
        assertTrue(store.holdsInMemory("alive0"));
        assertTrue(store.holdsInMemory("alive1999"));
    }

    // The index of keys with a deadline must follow every command
    // that adds, removes or moves a deadline
    @Test
    void theDeadlineIndexFollowsEveryChange() {
        run("SET", "a", "v", "EX", "100");
        run("SET", "b", "v", "EX", "100");
        run("SET", "c", "v", "EX", "100");
        run("SET", "plain", "v");
        assertEquals(3, store.keysWithDeadline());

        run("PERSIST", "a");                 // deadline removed
        assertEquals(2, store.keysWithDeadline());

        run("DEL", "b");                     // key removed
        assertEquals(1, store.keysWithDeadline());

        run("RENAME", "c", "moved");         // deadline moves with the key
        assertEquals(1, store.keysWithDeadline());
        assertEquals(":100\r\n", run("TTL", "moved"));

        run("SET", "moved", "new");          // plain SET clears it
        assertEquals(0, store.keysWithDeadline());

        run("EXPIRE", "plain", "50");        // deadline added
        assertEquals(1, store.keysWithDeadline());

        run("FLUSHALL");
        assertEquals(0, store.keysWithDeadline());
    }

    // Removing from the middle of the index moves another key into the
    // gap. Every remaining key must still be found afterwards.
    @Test
    void removingKeysInAnyOrderKeepsTheIndexIntact() {
        for (int i = 0; i < 100; i++) {
            run("SET", "k" + i, "v", "PX", String.valueOf(1000 + i));
        }
        // delete every third key, from the middle outwards
        for (int i = 50; i < 100; i += 3) run("DEL", "k" + i);
        for (int i = 49; i >= 0; i -= 3) run("DEL", "k" + i);
        int remaining = store.keysWithDeadline();

        clock.advance(5_000);
        int removed = 0;
        for (int pass = 0; pass < 10_000 && removed < remaining; pass++) {
            removed += store.removeExpiredKeys();
        }

        assertEquals(remaining, removed);
        assertEquals(0, store.keysWithDeadline());
        assertEquals(":0\r\n", run("DBSIZE"));
    }
}
