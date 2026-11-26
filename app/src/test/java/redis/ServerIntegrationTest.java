package redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// End-to-end tests: a real server, real TCP sockets, real RESP bytes.
// These cover what the unit tests cannot: the accept loop, the
// per-client loop, framing across reads, and several clients at once.
@Timeout(30)
class ServerIntegrationTest extends ServerTestBase {

    @Test
    void pingOverARealSocket() throws Exception {
        try (RespClient client = connect()) {
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    @Test
    void setThenGet() throws Exception {
        try (RespClient client = connect()) {
            assertEquals("+OK\r\n", client.command("SET", "name", "alice"));
            assertEquals("$5\r\nalice\r\n", client.command("GET", "name"));
            assertEquals("$-1\r\n", client.command("GET", "missing"));
        }
    }

    // Pipelining: several commands arrive in one TCP write.
    // The server must answer each one, in order.
    @Test
    void pipelinedCommandsInOneWrite() throws Exception {
        try (RespClient client = connect()) {
            ByteArrayOutputStream batch = new ByteArrayOutputStream();
            batch.write(RespClient.encode("SET", "k", "v"));
            batch.write(RespClient.encode("GET", "k"));
            batch.write(RespClient.encode("PING"));
            client.sendRaw(batch.toByteArray());

            assertEquals("+OK\r\n", client.readReply());
            assertEquals("$1\r\nv\r\n", client.readReply());
            assertEquals("+PONG\r\n", client.readReply());
        }
    }

    // TCP is a byte stream: one command can arrive in two pieces.
    // The parser must wait for the rest instead of failing.
    @Test
    void commandSplitAcrossTwoWrites() throws Exception {
        try (RespClient client = connect()) {
            byte[] command = RespClient.encode("SET", "k", "hello");
            int cut = 9; // in the middle of the command

            client.sendRaw(Arrays.copyOfRange(command, 0, cut));
            Thread.sleep(100);
            client.sendRaw(Arrays.copyOfRange(command, cut, command.length));

            assertEquals("+OK\r\n", client.readReply());
            assertEquals("$5\r\nhello\r\n", client.command("GET", "k"));
        }
    }

    @Test
    void twoClientsSeeTheSameData() throws Exception {
        try (RespClient writer = connect(); RespClient reader = connect()) {
            assertEquals("+OK\r\n", writer.command("SET", "shared", "1"));
            assertEquals("$1\r\n1\r\n", reader.command("GET", "shared"));
        }
    }

    @Test
    void serverKeepsRunningAfterAClientDisconnects() throws Exception {
        try (RespClient first = connect()) {
            assertEquals("+OK\r\n", first.command("SET", "k", "v"));
        }
        try (RespClient second = connect()) {
            assertEquals("$1\r\nv\r\n", second.command("GET", "k"));
        }
    }

    // A command name is a bulk string, so it may contain a line break.
    // The error that quotes it must still be exactly one reply, or
    // every later reply on this connection is matched to the wrong
    // command.
    @Test
    void aLineBreakInACommandNameDoesNotSplitTheErrorReply()
            throws Exception {
        try (RespClient client = connect()) {
            assertEquals("-ERR unknown command 'FOO  +FAKE'\r\n",
                client.command("FOO\r\n+FAKE"));
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    @Test
    void unknownCommandIsAnErrorButTheConnectionStaysOpen()
            throws Exception {
        try (RespClient client = connect()) {
            assertEquals("-ERR unknown command 'FOO'\r\n",
                client.command("FOO"));
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    @Test
    void manyClientsAtOnceEachGetTheirOwnReplies() throws Exception {
        int clientCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < clientCount; i++) {
                String key = "key" + i;
                String value = "value" + i;
                results.add(pool.submit(() -> {
                    try (RespClient client = connect()) {
                        client.command("SET", key, value);
                        return client.command("GET", key);
                    }
                }));
            }
            for (int i = 0; i < clientCount; i++) {
                String value = "value" + i;
                assertEquals(
                    "$" + value.length() + "\r\n" + value + "\r\n",
                    results.get(i).get());
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
