package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CommandHandlerTest {

    @TempDir
    Path tempDir;

    private TestClock clock;
    private CommandHandler handler;

    // Each test gets a fresh, empty store: its own temp directory,
    // a clock the test controls, and no background threads
    @BeforeEach
    void setUp() {
        clock = new TestClock();
        Store store = new Store(tempDir.resolve("dump.rdb"), clock);
        handler = new CommandHandler(store);
    }

    private String run(String... command) {
        return new String(handler.handle(Arrays.asList(command)),
            StandardCharsets.UTF_8);
    }

    @Test
    void setAndGet() {
        assertEquals("+OK\r\n", run("SET", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
        assertEquals("$-1\r\n", run("GET", "missing"));
    }

    @Test
    void setNxAndXx() {
        assertEquals("+OK\r\n", run("SET", "k", "1", "NX"));
        assertEquals("$-1\r\n", run("SET", "k", "2", "NX"));
        assertEquals("$1\r\n1\r\n", run("GET", "k"));
        assertEquals("$-1\r\n", run("SET", "other", "x", "XX"));
    }

    @Test
    void counters() {
        assertEquals(":1\r\n", run("INCR", "n"));
        assertEquals(":11\r\n", run("INCRBY", "n", "10"));
        assertEquals(":10\r\n", run("DECR", "n"));
    }

    @Test
    void lpushPrependsEachValueInTurn() {
        assertEquals(":3\r\n", run("LPUSH", "l", "a", "b", "c"));
        assertEquals("*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n",
            run("LRANGE", "l", "0", "-1"));
        assertEquals(":4\r\n", run("RPUSH", "l", "z"));
        assertEquals("$1\r\nz\r\n", run("LINDEX", "l", "-1"));
    }

    @Test
    void hashes() {
        assertEquals(":2\r\n", run("HSET", "h", "f1", "v1", "f2", "v2"));
        assertEquals("$2\r\nv2\r\n", run("HGET", "h", "f2"));
        assertEquals(":2\r\n", run("HLEN", "h"));
        assertEquals("+hash\r\n", run("TYPE", "h"));
    }

    @Test
    void wrongTypeIsRejected() {
        run("LPUSH", "l", "a");
        assertTrue(run("GET", "l").startsWith("-WRONGTYPE "));
        assertTrue(run("HGET", "l", "f").startsWith("-WRONGTYPE "));
    }

    @Test
    void strlenCountsBytes() {
        run("SET", "k", "héllo");
        assertEquals(":6\r\n", run("STRLEN", "k"));
    }

    @Test
    void ttlSemantics() {
        assertEquals(":-2\r\n", run("TTL", "missing"));
        run("SET", "k", "v");
        assertEquals(":-1\r\n", run("TTL", "k"));
        run("EXPIRE", "k", "100");
        String ttl = run("TTL", "k");
        assertTrue(ttl.equals(":100\r\n") || ttl.equals(":99\r\n"), ttl);
        assertEquals(":1\r\n", run("PERSIST", "k"));
        assertEquals(":-1\r\n", run("TTL", "k"));
    }

    @Test
    void expiredKeysDisappear() {
        run("SET", "k", "v", "PX", "50");
        clock.advance(120);
        assertEquals("$-1\r\n", run("GET", "k"));
        assertEquals(":0\r\n", run("EXISTS", "k"));
    }

    // ── One key, one entry: type, value and deadline together ──

    // Used to delete the key: it was removed as the "destination",
    // re-added, then removed again as the "source".
    @Test
    void renamingAKeyToItselfKeepsIt() {
        run("SET", "k", "v", "EX", "100");
        assertEquals("+OK\r\n", run("RENAME", "k", "k"));
        assertEquals("$1\r\nv\r\n", run("GET", "k"));
        assertEquals(":100\r\n", run("TTL", "k"));
    }

    @Test
    void renameCarriesTheDeadlineToTheNewName() {
        run("SET", "old", "v", "EX", "100");
        assertEquals("+OK\r\n", run("RENAME", "old", "new"));
        assertEquals(":0\r\n", run("EXISTS", "old"));
        assertEquals("$1\r\nv\r\n", run("GET", "new"));
        assertEquals(":100\r\n", run("TTL", "new"));
    }

    // The destination's old value, type and deadline are all replaced
    @Test
    void renameOverwritesTheDestinationCompletely() {
        run("LPUSH", "source", "a");
        run("SET", "target", "old", "EX", "100");
        assertEquals("+OK\r\n", run("RENAME", "source", "target"));
        assertEquals("+list\r\n", run("TYPE", "target"));
        assertEquals(":-1\r\n", run("TTL", "target"));
    }

    @Test
    void renamingAMissingKeyIsAnError() {
        assertEquals("-ERR no such key\r\n", run("RENAME", "nope", "x"));
    }

    // Used to skip the type check and write a string next to the
    // list, leaving the key with two types.
    @Test
    void setnxDoesNotTouchAKeyOfAnotherType() {
        run("LPUSH", "l", "a");
        assertEquals(":0\r\n", run("SETNX", "l", "x"));
        assertEquals("+list\r\n", run("TYPE", "l"));
        assertEquals(":1\r\n", run("LLEN", "l"));
    }

    // Used to count a key that had expired but not been swept yet.
    @Test
    void delDoesNotCountAnExpiredKey() {
        run("SET", "k", "v", "PX", "5");
        clock.advance(10);
        assertEquals(":0\r\n", run("DEL", "k"));
    }

    // SET replaces a key of any type, and its deadline
    @Test
    void setReplacesAKeyOfAnotherType() {
        run("HSET", "k", "f", "v");
        run("EXPIRE", "k", "100");
        assertEquals("+OK\r\n", run("SET", "k", "now-a-string"));
        assertEquals("+string\r\n", run("TYPE", "k"));
        assertEquals(":-1\r\n", run("TTL", "k"));
    }

    // Commands that modify a value in place keep its deadline
    @Test
    void incrAndAppendKeepTheDeadline() {
        run("SET", "n", "1", "EX", "100");
        assertEquals(":2\r\n", run("INCR", "n"));
        assertEquals(":100\r\n", run("TTL", "n"));

        run("SET", "s", "ab", "EX", "100");
        assertEquals(":4\r\n", run("APPEND", "s", "cd"));
        assertEquals(":100\r\n", run("TTL", "s"));
    }

    // An expired key is gone: PERSIST must not bring it back
    @Test
    void persistDoesNotReviveAnExpiredKey() {
        run("SET", "k", "v", "PX", "5");
        clock.advance(10);
        assertEquals(":0\r\n", run("PERSIST", "k"));
        assertEquals("$-1\r\n", run("GET", "k"));
    }

    // An expired key of one type must not block a command of another
    @Test
    void expiredKeyCanBeReusedWithAnotherType() {
        run("SET", "k", "v", "PX", "5");
        clock.advance(10);
        assertEquals(":1\r\n", run("LPUSH", "k", "a"));
        assertEquals("+list\r\n", run("TYPE", "k"));
    }

    // Lists and hashes disappear when their last element is removed
    @Test
    void emptyContainersAreRemoved() {
        run("RPUSH", "l", "only");
        assertEquals("$4\r\nonly\r\n", run("LPOP", "l"));
        assertEquals(":0\r\n", run("EXISTS", "l"));

        run("HSET", "h", "f", "v");
        assertEquals(":1\r\n", run("HDEL", "h", "f"));
        assertEquals(":0\r\n", run("EXISTS", "h"));
    }

    @Test
    void mgetReturnsNullForMissingAndWrongTypeKeys() {
        run("SET", "s", "v");
        run("LPUSH", "l", "a");
        assertEquals("*3\r\n$1\r\nv\r\n$-1\r\n$-1\r\n",
            run("MGET", "s", "l", "missing"));
    }
}
