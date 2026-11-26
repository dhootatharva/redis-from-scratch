package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

// Unit tests for the parser and serializer, with no socket involved:
// the parser reads from an in-memory byte array.
class RespTest {

    private static RespParser parserFor(String wire) {
        return parserFor(wire.getBytes(StandardCharsets.UTF_8));
    }

    private static RespParser parserFor(byte[] wire) {
        return new RespParser(new ByteArrayInputStream(wire));
    }

    private static String str(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String protocolError(String wire) {
        ProtocolException e = assertThrows(ProtocolException.class,
            () -> parserFor(wire).parse());
        return e.getMessage();
    }

    // ── Well-formed input ──

    @Test
    void parsesCommandArray() throws IOException {
        RespParser p = parserFor("*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n");
        assertEquals(List.of("SET", "name", "alice"), p.parse());
    }

    @Test
    void parsesPipelinedCommandsThenEndOfStream() throws IOException {
        RespParser p = parserFor("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assertEquals(List.of("PING"), p.parse());
        assertEquals(List.of("GET", "k"), p.parse());
        assertNull(p.parse());
    }

    @Test
    void bulkStringLengthIsInBytes() throws IOException {
        // "é" is one character but two bytes in UTF-8. The parser
        // must read two bytes and keep both.
        RespParser p = parserFor("*2\r\n$4\r\nECHO\r\n$2\r\né\r\n");
        List<String> command = p.parse();
        assertEquals(2, command.get(1).length());
        assertEquals(Binary.fromText("é"), command.get(1));
        assertEquals("$2\r\né\r\n",
            str(RespSerializer.bulkString(command.get(1))));
    }

    // Every possible byte value must come back out exactly as it
    // went in. This is what "binary-safe" means.
    @Test
    void everyByteValueRoundTrips() throws IOException {
        byte[] payload = new byte[256];
        for (int i = 0; i < 256; i++) payload[i] = (byte) i;

        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        wire.write("*1\r\n$256\r\n".getBytes(StandardCharsets.US_ASCII));
        wire.write(payload);
        wire.write("\r\n".getBytes(StandardCharsets.US_ASCII));

        String value = parserFor(wire.toByteArray()).parse().get(0);
        assertEquals(256, value.length());
        assertArrayEquals(payload, Binary.encode(value));
    }

    // \r\n inside a value is data, not a separator: the length
    // prefix says how many bytes belong to the value.
    @Test
    void crlfInsideAValueIsNotASeparator() throws IOException {
        RespParser p = parserFor("*1\r\n$4\r\na\r\nb\r\n");
        assertEquals(List.of("a\r\nb"), p.parse());
    }

    @Test
    void emptyBulkStringIsAnEmptyArgument() throws IOException {
        RespParser p = parserFor("*2\r\n$3\r\nSET\r\n$0\r\n\r\n");
        assertEquals(List.of("SET", ""), p.parse());
    }

    // ── Empty commands ──

    @Test
    void emptyAndNullArraysAreEmptyCommands() throws IOException {
        RespParser p = parserFor("*0\r\n*-1\r\n*1\r\n$4\r\nPING\r\n");
        assertTrue(p.parse().isEmpty());
        assertTrue(p.parse().isEmpty());
        assertEquals(List.of("PING"), p.parse());
    }

    // ── Inline commands ──

    @Test
    void inlineCommandIsSplitOnSpaces() throws IOException {
        RespParser p = parserFor("SET  key value\r\nPING\n");
        assertEquals(List.of("SET", "key", "value"), p.parse());
        assertEquals(List.of("PING"), p.parse());
    }

    @Test
    void blankLineIsAnEmptyCommand() throws IOException {
        RespParser p = parserFor("\r\n\nPING\r\n");
        assertTrue(p.parse().isEmpty());
        assertTrue(p.parse().isEmpty());
        assertEquals(List.of("PING"), p.parse());
    }

    // ── Malformed input ──

    @Test
    void nonNumericArrayLengthIsAProtocolError() {
        assertEquals("invalid multibulk length", protocolError("*abc\r\n"));
    }

    @Test
    void hugeArrayLengthIsAProtocolError() {
        assertEquals("invalid multibulk length",
            protocolError("*99999999\r\n"));
    }

    @Test
    void nonNumericBulkLengthIsAProtocolError() {
        assertEquals("invalid bulk length", protocolError("*1\r\n$x\r\n"));
    }

    @Test
    void negativeBulkLengthIsAProtocolError() {
        assertEquals("invalid bulk length", protocolError("*1\r\n$-5\r\n"));
    }

    // Rejected from the header alone: nothing is allocated or read
    @Test
    void oversizedBulkLengthIsAProtocolError() {
        assertEquals("invalid bulk length",
            protocolError("*1\r\n$2000000000\r\n"));
    }

    @Test
    void elementThatIsNotABulkStringIsAProtocolError() {
        assertEquals("expected '$', got ':'", protocolError("*1\r\n:5\r\n"));
    }

    @Test
    void bulkStringWithoutCrlfAfterItIsAProtocolError() {
        assertEquals("expected CRLF after bulk string",
            protocolError("*1\r\n$4\r\nPINGxx"));
    }

    @Test
    void lineThatNeverEndsIsAProtocolError() {
        assertEquals("too big inline request",
            protocolError("A".repeat(70_000)));
    }

    // A stream that ends mid-command is a disconnect, not bad syntax
    @Test
    void truncatedCommandIsAnIoErrorNotAProtocolError() {
        IOException e = assertThrows(IOException.class,
            () -> parserFor("*1\r\n$10\r\nabc").parse());
        assertTrue(!(e instanceof ProtocolException));
    }

    // ── Replies ──

    @Test
    void serializesReplies() {
        assertEquals("+OK\r\n", str(RespSerializer.simpleString("OK")));
        assertEquals(":42\r\n", str(RespSerializer.integer(42)));
        assertEquals("$-1\r\n", str(RespSerializer.bulkString(null)));
        assertEquals("$0\r\n\r\n", str(RespSerializer.bulkString("")));
        assertEquals("-ERR unknown command 'FOO'\r\n",
            str(RespSerializer.error("unknown command 'FOO'")));
    }

    // An error or simple string is one line. Text that came from the
    // client can contain a line break, which would split the reply.
    @Test
    void lineBreaksInOneLineRepliesBecomeSpaces() {
        assertEquals("-ERR unknown command 'a  +OK'\r\n",
            str(RespSerializer.error("unknown command 'a\r\n+OK'")));
        assertEquals("-ERR a b\r\n",
            str(RespSerializer.error("a\nb")));
        assertEquals("+a b\r\n",
            str(RespSerializer.simpleString("a\rb")));
    }

    // A bulk string is protected by its length, so it keeps them
    @Test
    void lineBreaksInBulkStringsAreKept() {
        assertEquals("$4\r\na\r\nb\r\n",
            str(RespSerializer.bulkString("a\r\nb")));
    }

    @Test
    void wrongTypeErrorKeepsItsOwnCode() {
        byte[] reply = RespSerializer.error(
            "WRONGTYPE Operation against a key holding the wrong kind of value");
        assertArrayEquals(
            "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                .getBytes(StandardCharsets.UTF_8),
            reply);
    }
}
