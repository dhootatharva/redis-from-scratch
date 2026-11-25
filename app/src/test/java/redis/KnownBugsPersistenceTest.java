package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// KNOWN BUG — describes correct behaviour and FAILS today.
// Excluded from the normal build; run with  ./gradlew knownBugs
// Phase 5 (persistence) is expected to fix it.
@Tag("known-bug")
class KnownBugsPersistenceTest {

    @TempDir
    Path tempDir;

    private static String run(Store store, String... command) {
        return TestWire.run(new CommandHandler(store), command);
    }

    // Snapshots use DataOutputStream.writeUTF, which cannot write more
    // than 65,535 bytes. One larger value makes the WHOLE save fail,
    // so every other key stops being persisted too.
    @Test
    void aValueOver64KbDoesNotBreakTheSnapshot() {
        Path snapshot = tempDir.resolve("dump.rdb");
        TestClock clock = new TestClock();
        String large = "x".repeat(70_000);

        Store before = new Store(snapshot, clock);
        run(before, "SET", "small", "1");
        run(before, "SET", "large", large);
        before.save();

        Store after = new Store(snapshot, clock);
        after.load();

        assertEquals("$1\r\n1\r\n", run(after, "GET", "small"));
        assertEquals(":70000\r\n", run(after, "STRLEN", "large"));
    }
}
