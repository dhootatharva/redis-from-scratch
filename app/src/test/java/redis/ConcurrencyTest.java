package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

// Many threads running commands at the same time. Each test releases
// its threads together through a latch, to make them overlap as much
// as possible.
//
// The first three tests failed before commands were made atomic
// (one keyspace, one lock): increments were lost, two clients could
// both win a SET NX, and a key could end up with two types.
@Timeout(60)
class ConcurrencyTest {

    private static final int THREADS = 8;

    @TempDir
    Path tempDir;

    private Store store;
    private CommandHandler handler;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        store = new Store(
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

    // INCR reads, adds one and writes back. Without atomic commands,
    // concurrent increments overwrite each other (a "lost update").
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

    // SET NX is "set only if the key does not exist". It is the
    // command people build locks on, so exactly one client must win.
    // The check and the write have to be one atomic step.
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

    // SET and LPUSH racing on one key: whichever runs second decides.
    // The key must end up as a string or as a list, never as both.
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

    // Snapshots are taken while clients keep writing. The save copies
    // the keyspace under the lock, so it must never fail or see a
    // list or hash changing underneath it, and what it writes must
    // load back cleanly.
    @Test
    void savingWhileClientsWriteIsSafe() throws Exception {
        int perWriter = 2_000;
        AtomicInteger writersRunning = new AtomicInteger(THREADS);
        List<Future<?>> writers = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            int id = t;
            writers.add(pool.submit(() -> {
                try {
                    for (int i = 0; i < perWriter; i++) {
                        run("RPUSH", "list:" + id, "item" + i);
                        run("HSET", "hash:" + id,
                            "field" + (i % 50), "v" + i);
                        run("SET", "string:" + id + ":" + (i % 100),
                            "v" + i);
                    }
                } finally {
                    writersRunning.decrementAndGet();
                }
                return null;
            }));
        }

        // Keep taking snapshots for as long as anyone is writing
        int saves = 0;
        while (writersRunning.get() > 0) {
            store.save();
            saves++;
        }
        for (Future<?> writer : writers) writer.get();
        assertTrue(saves > 0, "no snapshot overlapped the writers");

        // One last save now that the writers are done, then reload
        store.save();
        Store reloaded = new Store(
            tempDir.resolve("dump.rdb"), System::currentTimeMillis);
        reloaded.load();
        CommandHandler reloadedHandler = new CommandHandler(reloaded);

        for (int t = 0; t < THREADS; t++) {
            String listKey = "list:" + t;
            assertEquals(
                run("LLEN", listKey),
                new String(reloadedHandler.handle(
                    Arrays.asList("LLEN", listKey)),
                    StandardCharsets.UTF_8),
                "list length after reload for " + listKey);
        }
        assertEquals(run("DBSIZE"),
            new String(reloadedHandler.handle(Arrays.asList("DBSIZE")),
                StandardCharsets.UTF_8));
    }

    // SAVE and BGSAVE take the snapshot lock and then the Store lock.
    // Ordinary commands take only the Store lock. Running them all
    // together must not deadlock (the test would hit its timeout).
    @Test
    void saveCommandsDoNotDeadlockWithOtherCommands() throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> workers = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            boolean saver = t % 2 == 0;
            workers.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < 10; i++) {
                    if (saver) {
                        run("SAVE");
                    } else {
                        run("INCR", "n");
                        run("LPUSH", "l", "x");
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> worker : workers) worker.get();

        assertEquals(":" + (THREADS / 2 * 10) + "\r\n", run("LLEN", "l"));
    }
}
