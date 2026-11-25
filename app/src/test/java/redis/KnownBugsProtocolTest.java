package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// KNOWN BUGS — these tests describe what real Redis does and FAIL today.
// They are excluded from the normal build; run them with
//   ./gradlew knownBugs
// Phase 3 (protocol hardening) is expected to fix all of them.
// They run over a real socket so they stay valid when the parser's
// internal types change.
@Tag("known-bug")
@Timeout(30)
class KnownBugsProtocolTest extends ServerTestBase {

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    // Redis values are arbitrary bytes. Today they pass through a Java
    // String, so bytes that are not valid UTF-8 are replaced.
    @Test
    void valuesAreBinarySafe() throws Exception {
        byte[] payload = {(byte) 0xFF, (byte) 0xFE};

        ByteArrayOutputStream command = new ByteArrayOutputStream();
        command.write(ascii("*2\r\n$4\r\nECHO\r\n$2\r\n"));
        command.write(payload);
        command.write(ascii("\r\n"));

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.write(ascii("$2\r\n"));
        expected.write(payload);
        expected.write(ascii("\r\n"));

        try (RespClient client = connect()) {
            client.sendRaw(command.toByteArray());
            assertArrayEquals(expected.toByteArray(),
                client.readReplyBytes());
        }
    }

    // Redis ignores an empty array. Today it throws inside the worker
    // and the connection is dropped.
    @Test
    void emptyArrayIsIgnored() throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw("*0\r\n");
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    // Today a non-numeric count throws NumberFormatException, which
    // nothing catches: the client is disconnected with no explanation.
    @Test
    void malformedArrayLengthGetsAProtocolError() throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw("*abc\r\n");
            assertTrue(client.readReply().startsWith("-ERR Protocol error"));
        }
    }

    // Today the server tries to allocate whatever length the client
    // claims. Redis caps a bulk string at 512 MB.
    @Test
    void oversizedBulkLengthIsRejectedWithoutAllocating() throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw("*1\r\n$2000000000\r\n");
            assertTrue(client.readReply().startsWith("-ERR Protocol error"));
        }
    }
}
