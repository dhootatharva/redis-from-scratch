package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// Protocol behaviour over a real socket: binary safety, inline
// commands, and what the server does with input that is not valid
// RESP. The parser's rules are unit-tested in RespTest; these tests
// check what a client actually experiences.
@Timeout(30)
class ProtocolTest extends ServerTestBase {

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.write(part, 0, part.length);
        }
        return out.toByteArray();
    }

    // ── Binary safety ──

    // These two bytes are not valid UTF-8. A server that treats
    // values as text replaces them; a binary-safe one returns them.
    @Test
    void bytesThatAreNotValidTextComeBackUnchanged() throws Exception {
        byte[] payload = {(byte) 0xFF, (byte) 0xFE};
        try (RespClient client = connect()) {
            client.sendRaw(RespClient.encodeBytes(ascii("ECHO"), payload));
            assertArrayEquals(
                concat(ascii("$2\r\n"), payload, ascii("\r\n")),
                client.readReplyBytes());
        }
    }

    @Test
    void everyByteValueSurvivesSetAndGet() throws Exception {
        byte[] payload = new byte[256];
        for (int i = 0; i < 256; i++) payload[i] = (byte) i;

        try (RespClient client = connect()) {
            client.sendRaw(RespClient.encodeBytes(
                ascii("SET"), ascii("blob"), payload));
            assertEquals("+OK\r\n", client.readReply());

            assertEquals(":256\r\n", client.command("STRLEN", "blob"));

            client.sendRaw(RespClient.encode("GET", "blob"));
            assertArrayEquals(
                concat(ascii("$256\r\n"), payload, ascii("\r\n")),
                client.readReplyBytes());
        }
    }

    // Keys and hash fields are bytes too, not only values
    @Test
    void keysAndFieldsAreBinarySafe() throws Exception {
        byte[] key = {(byte) 0x00, (byte) 0xFF, (byte) 0x80};
        byte[] field = {(byte) 0xC3, (byte) 0x28};
        try (RespClient client = connect()) {
            client.sendRaw(RespClient.encodeBytes(
                ascii("HSET"), key, field, ascii("v")));
            assertEquals(":1\r\n", client.readReply());

            client.sendRaw(RespClient.encodeBytes(ascii("HKEYS"), key));
            assertArrayEquals(
                concat(ascii("*1\r\n$2\r\n"), field, ascii("\r\n")),
                client.readReplyBytes());
        }
    }

    @Test
    void textIsStoredAsTheBytesTheClientSent() throws Exception {
        try (RespClient client = connect()) {
            assertEquals("+OK\r\n", client.command("SET", "k", "héllo"));
            // 5 characters, 6 bytes in UTF-8
            assertEquals(":6\r\n", client.command("STRLEN", "k"));
            assertEquals("$6\r\nhéllo\r\n", client.command("GET", "k"));
        }
    }

    @Test
    void lineBreaksInsideAValueArePreserved() throws Exception {
        try (RespClient client = connect()) {
            assertEquals("+OK\r\n", client.command("SET", "k", "a\r\nb"));
            assertEquals("$4\r\na\r\nb\r\n", client.command("GET", "k"));
        }
    }

    @Test
    void aOneMegabyteValueRoundTrips() throws Exception {
        byte[] payload = new byte[1024 * 1024];
        new Random(42).nextBytes(payload);

        try (RespClient client = connect()) {
            client.sendRaw(RespClient.encodeBytes(
                ascii("SET"), ascii("big"), payload));
            assertEquals("+OK\r\n", client.readReply());

            client.sendRaw(RespClient.encode("GET", "big"));
            assertArrayEquals(
                concat(ascii("$1048576\r\n"), payload, ascii("\r\n")),
                client.readReplyBytes());
        }
    }

    // ── Empty and inline commands ──

    // Redis ignores an empty array: no reply, connection stays open
    @Test
    void emptyArrayIsIgnored() throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw("*0\r\n");
            client.sendRaw("*-1\r\n");
            assertEquals("+PONG\r\n", client.command("PING"));
        }
    }

    // Plain text followed by a newline, as typed into telnet or nc
    @Test
    void inlineCommandsWork() throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw("PING\r\n");
            assertEquals("+PONG\r\n", client.readReply());

            client.sendRaw("SET greeting hello\r\n");
            assertEquals("+OK\r\n", client.readReply());

            client.sendRaw("\r\n");   // a blank line is ignored
            client.sendRaw("GET greeting\r\n");
            assertEquals("$5\r\nhello\r\n", client.readReply());
        }
    }

    // ── Malformed input ──

    private void assertProtocolErrorThenHangUp(byte[] wire,
            String expectedMessage) throws Exception {
        try (RespClient client = connect()) {
            client.sendRaw(wire);
            assertEquals(
                "-ERR Protocol error: " + expectedMessage + "\r\n",
                client.readReply());
            // After a protocol error the server closes the connection
            assertThrows(EOFException.class, client::readReply);
        }
    }

    @Test
    void malformedArrayLengthGetsAProtocolError() throws Exception {
        assertProtocolErrorThenHangUp(ascii("*abc\r\n"),
            "invalid multibulk length");
    }

    @Test
    void oversizedBulkLengthIsRejectedWithoutAllocating() throws Exception {
        assertProtocolErrorThenHangUp(ascii("*1\r\n$2000000000\r\n"),
            "invalid bulk length");
    }

    @Test
    void negativeBulkLengthGetsAProtocolError() throws Exception {
        assertProtocolErrorThenHangUp(ascii("*1\r\n$-5\r\n"),
            "invalid bulk length");
    }

    // The error is sent with unread bytes still pending ("5\r\n" and
    // more). The reply must still reach the client.
    @Test
    void protocolErrorArrivesEvenWithUnreadInput() throws Exception {
        assertProtocolErrorThenHangUp(
            concat(ascii("*1\r\n:5\r\n"), new byte[8192]),
            "expected '$', got ':'");
    }

    @Test
    void missingCrlfAfterAValueGetsAProtocolError() throws Exception {
        assertProtocolErrorThenHangUp(ascii("*1\r\n$4\r\nPINGxx"),
            "expected CRLF after bulk string");
    }

    @Test
    void aLineThatNeverEndsGetsAProtocolError() throws Exception {
        assertProtocolErrorThenHangUp(ascii("A".repeat(70_000)),
            "too big inline request");
    }

    // One client's bad input must not affect anyone else
    @Test
    void otherClientsAreUnaffectedByAProtocolError() throws Exception {
        try (RespClient good = connect(); RespClient bad = connect()) {
            assertEquals("+OK\r\n", good.command("SET", "k", "v"));

            bad.sendRaw("*abc\r\n");
            bad.readReply();

            assertEquals("$1\r\nv\r\n", good.command("GET", "k"));
        }
        try (RespClient later = connect()) {
            assertEquals("+PONG\r\n", later.command("PING"));
        }
    }
}
