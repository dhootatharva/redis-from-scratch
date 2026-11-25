package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

// KNOWN BUGS — these tests describe correct behaviour and FAIL today.
// They are excluded from the normal build; run them with
//   ./gradlew knownBugs
// Phase 2 (single keyspace + command lock) is expected to fix all three.
// When a test here passes, move it into a regular test class.
@Tag("known-bug")
@Timeout(60)
class KnownBugsConcurrencyTest {

    private static final int THREADS = 8;

    @TempDir
    Path tempDir;

    private CommandHandler handler;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        Store store = new Store(
            tempDir.resolve("dump.rdb"), System::currentTimeMillis);
        handler = new CommandHandler(store);
        pool = Executors.newFixedThreadPool(THREADS);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
    }

    private String run(String... command) {
        return new String(handler.handle(Arrays.asList(command)),
            StandardCharsets.UTF_8);
    }

    // Lost update: INCR reads, adds one and writes back in three
    // separate steps, so concurrent increments overwrite each other.
    @Test
    void incrNeverLosesAnUpdate() throws Exception {
        int perThread = 20_000;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> workers = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            workers.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    run("INCR", "counter");
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> worker : workers) worker.get();

        String expected = String.valueOf(THREADS * perThread);
        assertEquals(
            "$" + expected.length() + "\r\n" + expected + "\r\n",
            run("GET", "counter"));
    }

    // Check-then-act: SET NX checks "does the key exist?" and then
    // sets it, as two steps. Two clients can both pass the check.
    // This is the command people build locks on, so exactly one
    // client must win.
    @Test
    void setNxHasExactlyOneWinner() throws Exception {
        int rounds = 2_000;
        int roundsWithSeveralWinners = 0;

        for (int round = 0; round < rounds; round++) {
            String key = "lock:" + round;
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> attempts = new ArrayList<>();

            for (int t = 0; t < THREADS; t++) {
                attempts.add(pool.submit(() -> {
                    start.await();
                    return run("SET", key, "owner", "NX");
                }));
            }
            start.countDown();

            int winners = 0;
            for (Future<String> attempt : attempts) {
                if (attempt.get().equals("+OK\r\n")) winners++;
            }
            if (winners > 1) roundsWithSeveralWinners++;
        }

        assertEquals(0, roundsWithSeveralWinners,
            "rounds (out of " + rounds + ") where more than one "
            + "client acquired the same lock");
    }

    // Interleaved multi-map writes: SET and LPUSH on the same key can
    // leave it in both the string map and the list map. Then GET and
    // LLEN both answer WRONGTYPE and the key is unusable.
    @Test
    void aKeyNeverEndsUpWithTwoTypes() throws Exception {
        int keys = 5_000;
        int brokenKeys = 0;

        for (int i = 0; i < keys; i++) {
            String key = "mixed:" + i;
            CountDownLatch start = new CountDownLatch(1);

            Future<?> setter = pool.submit(() -> {
                start.await();
                return run("SET", key, "v");
            });
            Future<?> pusher = pool.submit(() -> {
                start.await();
                return run("LPUSH", key, "x");
            });
            start.countDown();
            setter.get();
            pusher.get();

            boolean notAString = run("GET", key).startsWith("-WRONGTYPE");
            boolean notAList = run("LLEN", key).startsWith("-WRONGTYPE");
            if (notAString && notAList) brokenKeys++;
        }

        assertEquals(0, brokenKeys,
            "keys (out of " + keys + ") left holding two types at once");
    }
}
