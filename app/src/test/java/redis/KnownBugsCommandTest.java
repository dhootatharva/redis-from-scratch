package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// KNOWN BUGS — these tests describe what real Redis does and FAIL today.
// They are excluded from the normal build; run them with
//   ./gradlew knownBugs
// Each test names the phase expected to fix it. When a test here
// passes, move it into CommandHandlerTest.
@Tag("known-bug")
class KnownBugsCommandTest {

    @TempDir
    Path tempDir;

    private TestClock clock;
    private CommandHandler handler;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        handler = new CommandHandler(
            new Store(tempDir.resolve("dump.rdb"), clock));
    }

    private String run(String... command) {
        return new String(handler.handle(Arrays.asList(command)),
            StandardCharsets.UTF_8);
    }

    // ── Phase 2: keyspace ──

    // Today the key is deleted: it is removed as the "destination",
    // re-added, then removed again as the "source".
    @Test
    void renamingAKeyToItselfKeepsIt() {
        run("SET", "k", "v");
        assertEquals("+OK\r\n", run("RENAME", "k", "k"));
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
    }

    // Today SETNX skips the type check and writes a string next to
    // the list, leaving the key with two types.
    @Test
    void setnxDoesNotTouchAKeyOfAnotherType() {
        run("LPUSH", "l", "a");
        assertEquals(":0\r\n", run("SETNX", "l", "x"));
        assertEquals("+list\r\n", run("TYPE", "l"));
        assertEquals(":1\r\n", run("LLEN", "l"));
    }

    // Today DEL counts a key that has expired but not been swept yet.
    @Test
    void delDoesNotCountAnExpiredKey() {
        run("SET", "k", "v", "PX", "5");
        clock.advance(10);
        assertEquals(":0\r\n", run("DEL", "k"));
    }

    // ── Phase 4: command table and validation ──

    @Test
    void setRejectsAZeroExpiry() {
        assertTrue(run("SET", "k", "v", "EX", "0").startsWith("-ERR"),
            "EX 0 must be an error, not a key that never expires");
    }

    @Test
    void setRejectsANegativeExpiry() {
        assertTrue(run("SET", "k", "v", "EX", "-5").startsWith("-ERR"));
    }

    @Test
    void nonNumericArgumentGetsTheRedisErrorMessage() {
        assertEquals(
            "-ERR value is not an integer or out of range\r\n",
            run("SET", "k", "v", "EX", "abc"));
    }

    @Test
    void extraArgumentsAreRejected() {
        run("SET", "k", "v");
        assertTrue(run("GET", "k", "extra")
            .startsWith("-ERR wrong number of arguments"));
    }

    @Test
    void incrAtTheMaximumValueIsAnErrorNotAWrapAround() {
        run("SET", "n", String.valueOf(Long.MAX_VALUE));
        assertTrue(run("INCR", "n").startsWith("-ERR"));
    }

    @Test
    void keysFiltersByPattern() {
        run("SET", "foo", "1");
        run("SET", "bar", "2");
        assertEquals("*1\r\n$3\r\nfoo\r\n", run("KEYS", "foo*"));
    }

    @Test
    void existsCountsEveryKeyGiven() {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("EXISTS", "a", "b", "missing"));
    }

    // An empty command must not throw out of the handler: an uncaught
    // exception kills the client's connection.
    @Test
    void emptyCommandDoesNotThrow() {
        assertTrue(run().startsWith("-ERR"));
    }
}
