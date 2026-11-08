package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CommandHandlerTest {

    private CommandHandler handler;

    @BeforeEach
    void setUp() {
        Store store = new Store();
        handler = new CommandHandler(store);
        run("FLUSHALL");
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
    void expiredKeysDisappear() throws InterruptedException {
        run("SET", "k", "v", "PX", "50");
        Thread.sleep(120);
        assertEquals("$-1\r\n", run("GET", "k"));
        assertEquals(":0\r\n", run("EXISTS", "k"));
    }
}
