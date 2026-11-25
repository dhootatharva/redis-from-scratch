package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// What the command table enforces for every command: argument counts,
// argument values, option syntax, and the exact error text Redis uses.
class CommandValidationTest {

    private static final String NOT_AN_INTEGER =
        "-ERR value is not an integer or out of range\r\n";
    private static final String SYNTAX_ERROR = "-ERR syntax error\r\n";

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
        return TestWire.run(handler, command);
    }

    private static String wrongArgs(String name) {
        return "-ERR wrong number of arguments for '"
            + name + "' command\r\n";
    }

    // ── Argument counts ──

    @Test
    void tooFewArgumentsAreRejected() {
        assertEquals(wrongArgs("get"), run("GET"));
        assertEquals(wrongArgs("set"), run("SET", "k"));
        assertEquals(wrongArgs("lrange"), run("LRANGE", "l", "0"));
        assertEquals(wrongArgs("hset"), run("HSET", "h", "f"));
    }

    // A fixed-size command must reject extras too, not ignore them
    @Test
    void extraArgumentsAreRejected() {
        run("SET", "k", "v");
        assertEquals(wrongArgs("get"), run("GET", "k", "extra"));
        assertEquals(wrongArgs("ttl"), run("TTL", "k", "extra"));
        assertEquals(wrongArgs("dbsize"), run("DBSIZE", "extra"));
        assertEquals(wrongArgs("ping"), run("PING", "a", "b"));
    }

    @Test
    void incompletePairsAreRejected() {
        assertEquals(wrongArgs("mset"), run("MSET", "a", "1", "b"));
        assertEquals(wrongArgs("hset"), run("HSET", "h", "f", "v", "g"));
    }

    @Test
    void commandNamesAreCaseInsensitive() {
        assertEquals("+OK\r\n", run("set", "k", "v"));
        assertEquals("$1\r\nv\r\n", run("GeT", "k"));
    }

    @Test
    void unknownCommandKeepsTheNameAsTyped() {
        assertEquals("-ERR unknown command 'FooBar'\r\n", run("FooBar"));
    }

    // ── Numbers ──

    // The client must see Redis's message, not a Java exception text
    @Test
    void nonNumericArgumentsGetTheRedisErrorMessage() {
        run("SET", "k", "v");
        assertEquals(NOT_AN_INTEGER, run("SET", "k", "v", "EX", "abc"));
        assertEquals(NOT_AN_INTEGER, run("EXPIRE", "k", "soon"));
        assertEquals(NOT_AN_INTEGER, run("INCRBY", "n", "1.5"));
        assertEquals(NOT_AN_INTEGER, run("LRANGE", "l", "a", "b"));
        assertEquals(NOT_AN_INTEGER, run("LINDEX", "l", "x"));
    }

    @Test
    void incrOnANonNumberIsAnError() {
        run("SET", "s", "abc");
        assertEquals(NOT_AN_INTEGER, run("INCR", "s"));
    }

    // Counters are 64-bit. Going past the limit must be an error,
    // not a silent wrap-around to a negative number.
    @Test
    void counterOverflowIsAnError() {
        run("SET", "n", String.valueOf(Long.MAX_VALUE));
        assertEquals("-ERR increment or decrement would overflow\r\n",
            run("INCR", "n"));
        assertEquals("$19\r\n" + Long.MAX_VALUE + "\r\n", run("GET", "n"));

        run("SET", "m", String.valueOf(Long.MIN_VALUE));
        assertEquals("-ERR increment or decrement would overflow\r\n",
            run("DECR", "m"));
    }

    @Test
    void hugeListIndexesAreSimplyOutOfRange() {
        run("RPUSH", "l", "a", "b");
        assertEquals("*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            run("LRANGE", "l", "0", "99999999999"));
        assertEquals("$-1\r\n", run("LINDEX", "l", "99999999999"));
    }

    // ── SET options ──

    @Test
    void setRejectsAZeroOrNegativeExpiry() {
        String invalid = "-ERR invalid expire time in 'set' command\r\n";
        assertEquals(invalid, run("SET", "k", "v", "EX", "0"));
        assertEquals(invalid, run("SET", "k", "v", "EX", "-5"));
        assertEquals(invalid, run("SET", "k", "v", "PX", "0"));
        // and nothing was written
        assertEquals(":0\r\n", run("EXISTS", "k"));
    }

    @Test
    void setRejectsAnExpiryTooLargeToRepresent() {
        String huge = String.valueOf(Long.MAX_VALUE);
        assertTrue(run("SET", "k", "v", "EX", huge)
            .startsWith("-ERR invalid expire time"));
        assertTrue(run("SET", "k", "v", "PX", huge)
            .startsWith("-ERR invalid expire time"));
    }

    @Test
    void setRejectsBadOptionSyntax() {
        assertEquals(SYNTAX_ERROR, run("SET", "k", "v", "BOGUS"));
        assertEquals(SYNTAX_ERROR, run("SET", "k", "v", "EX"));
        assertEquals(SYNTAX_ERROR, run("SET", "k", "v", "NX", "XX"));
        assertEquals(SYNTAX_ERROR,
            run("SET", "k", "v", "EX", "10", "PX", "10"));
    }

    @Test
    void setOptionsWorkInAnyOrderAndCase() {
        assertEquals("+OK\r\n", run("SET", "k", "v", "nx", "ex", "100"));
        assertEquals(":100\r\n", run("TTL", "k"));
        assertEquals("+OK\r\n", run("SET", "k", "w", "PX", "5000", "XX"));
        assertEquals(":5\r\n", run("TTL", "k"));
    }

    // ── EXPIRE ──

    // A deadline that is not in the future deletes the key
    @Test
    void expireWithANonPositiveTimeDeletesTheKey() {
        run("SET", "k", "v");
        assertEquals(":1\r\n", run("EXPIRE", "k", "0"));
        assertEquals(":0\r\n", run("EXISTS", "k"));

        run("SET", "k", "v");
        assertEquals(":1\r\n", run("EXPIRE", "k", "-10"));
        assertEquals(":0\r\n", run("EXISTS", "k"));
    }

    // ── EXISTS and KEYS ──

    @Test
    void existsCountsEveryKeyGiven() {
        run("SET", "a", "1");
        run("SET", "b", "2");
        assertEquals(":2\r\n", run("EXISTS", "a", "b", "missing"));
        // a key named twice is counted twice, as in Redis
        assertEquals(":2\r\n", run("EXISTS", "a", "a"));
    }

    @Test
    void keysFiltersByPattern() {
        run("SET", "foo", "1");
        run("SET", "bar", "2");
        assertEquals("*1\r\n$3\r\nfoo\r\n", run("KEYS", "foo*"));
        assertEquals("*1\r\n$3\r\nbar\r\n", run("KEYS", "?a?"));
        assertEquals("*0\r\n", run("KEYS", "nothing*"));
        assertTrue(run("KEYS", "*").startsWith("*2\r\n"));
    }

    @Test
    void keysDoesNotListExpiredKeys() {
        run("SET", "gone", "v", "PX", "5");
        run("SET", "here", "v");
        clock.advance(10);
        assertEquals("*1\r\n$4\r\nhere\r\n", run("KEYS", "*"));
        assertEquals(":1\r\n", run("DBSIZE"));
    }

    // ── Commands real clients send while connecting ──

    @Test
    void connectionHousekeepingCommandsAreAccepted() {
        assertEquals("+OK\r\n", run("SELECT", "0"));
        assertEquals("-ERR DB index is out of range\r\n", run("SELECT", "1"));
        assertEquals("+OK\r\n",
            run("CLIENT", "SETINFO", "LIB-NAME", "test"));
        assertEquals("+OK\r\n", run("CLIENT", "SETNAME", "me"));
        assertEquals("*0\r\n", run("COMMAND"));
        assertEquals("*0\r\n", run("COMMAND", "DOCS"));
        assertEquals("*0\r\n", run("CONFIG", "GET", "save"));
        assertTrue(run("INFO").contains("db0:keys=0"));
    }

    // ── Errors do not change data ──

    @Test
    void aRejectedCommandLeavesTheDataUntouched() {
        run("RPUSH", "l", "a");
        assertTrue(run("INCR", "l").startsWith("-WRONGTYPE"));
        assertTrue(run("SET", "l", "v", "EX", "abc").startsWith("-ERR"));
        assertEquals("+list\r\n", run("TYPE", "l"));
        assertFalse(run("LRANGE", "l", "0", "-1").startsWith("-"));
    }
}
