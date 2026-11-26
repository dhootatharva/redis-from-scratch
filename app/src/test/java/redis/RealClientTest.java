package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

// Drives the server with Jedis, a widely used Redis client library,
// instead of our own test client. This is the proof behind "real
// Redis clients can talk to it": Jedis performs its normal connection
// handshake and encodes and decodes every command itself.
@Timeout(30)
class RealClientTest extends ServerTestBase {

    private Jedis jedis() {
        return new Jedis("127.0.0.1", server.port());
    }

    @Test
    void connectsAndPings() {
        try (Jedis jedis = jedis()) {
            assertEquals("PONG", jedis.ping());
        }
    }

    @Test
    void strings() {
        try (Jedis jedis = jedis()) {
            assertEquals("OK", jedis.set("name", "alice"));
            assertEquals("alice", jedis.get("name"));
            assertNull(jedis.get("missing"));

            assertEquals(1L, jedis.incr("counter"));
            assertEquals(11L, jedis.incrBy("counter", 10));
            assertEquals(5L, jedis.append("name", ""));
            assertEquals(List.of("alice", "11"),
                jedis.mget("name", "counter"));
        }
    }

    @Test
    void setWithOptionsAndExpiry() {
        try (Jedis jedis = jedis()) {
            SetParams onlyIfAbsent = SetParams.setParams().nx().ex(100);
            assertEquals("OK", jedis.set("lock", "me", onlyIfAbsent));
            assertNull(jedis.set("lock", "you", onlyIfAbsent));
            assertEquals("me", jedis.get("lock"));

            long ttl = jedis.ttl("lock");
            assertTrue(ttl > 90 && ttl <= 100, "ttl was " + ttl);
            assertEquals(1L, jedis.persist("lock"));
            assertEquals(-1L, jedis.ttl("lock"));
        }
    }

    @Test
    void lists() {
        try (Jedis jedis = jedis()) {
            assertEquals(3L, jedis.rpush("jobs", "a", "b", "c"));
            assertEquals(4L, jedis.lpush("jobs", "first"));
            assertEquals(List.of("first", "a", "b", "c"),
                jedis.lrange("jobs", 0, -1));
            assertEquals("first", jedis.lpop("jobs"));
            assertEquals("c", jedis.rpop("jobs"));
            assertEquals(2L, jedis.llen("jobs"));
        }
    }

    @Test
    void hashes() {
        try (Jedis jedis = jedis()) {
            assertEquals(2L,
                jedis.hset("user:1", Map.of("name", "alice", "age", "30")));
            assertEquals("alice", jedis.hget("user:1", "name"));
            assertEquals(Map.of("name", "alice", "age", "30"),
                jedis.hgetAll("user:1"));
            assertEquals(1L, jedis.hdel("user:1", "age"));
            assertEquals(1L, jedis.hlen("user:1"));
        }
    }

    @Test
    void keysTypeAndDelete() {
        try (Jedis jedis = jedis()) {
            jedis.set("user:1", "a");
            jedis.set("user:2", "b");
            jedis.rpush("queue", "x");

            assertEquals(Set.of("user:1", "user:2"), jedis.keys("user:*"));
            assertEquals("list", jedis.type("queue"));
            assertEquals(2L, jedis.exists("user:1", "queue", "nope"));
            assertEquals(2L, jedis.del("user:1", "user:2"));
            assertEquals(1L, jedis.dbSize());
        }
    }

    @Test
    void binaryValues() {
        byte[] key = "blob".getBytes(StandardCharsets.UTF_8);
        byte[] value = {0, (byte) 0xFF, 13, 10, (byte) 0x80};
        try (Jedis jedis = jedis()) {
            jedis.set(key, value);
            assertArrayEquals(value, jedis.get(key));
        }
    }

    // A command error arrives as the client library's own exception
    @Test
    void errorsReachTheClientAsExceptions() {
        try (Jedis jedis = jedis()) {
            jedis.rpush("list", "a");
            JedisDataException e = assertThrows(
                JedisDataException.class, () -> jedis.get("list"));
            assertTrue(e.getMessage().startsWith("WRONGTYPE"));
            // and the connection is still usable afterwards
            assertEquals("PONG", jedis.ping());
        }
    }
}
