package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RespTest {

    private static RespParser parserFor(String wire) {
        return new RespParser(new ByteArrayInputStream(
            wire.getBytes(StandardCharsets.UTF_8)));
    }

    private static String str(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

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
        // "é" is one char but two bytes in UTF-8
        RespParser p = parserFor("*2\r\n$4\r\nECHO\r\n$2\r\né\r\n");
        assertEquals(List.of("ECHO", "é"), p.parse());
        assertEquals("$2\r\né\r\n", str(RespSerializer.bulkString("é")));
    }

    @Test
    void serializesReplies() {
        assertEquals("+OK\r\n", str(RespSerializer.simpleString("OK")));
        assertEquals(":42\r\n", str(RespSerializer.integer(42)));
        assertEquals("$-1\r\n", str(RespSerializer.bulkString(null)));
        assertEquals("-ERR unknown command 'FOO'\r\n",
            str(RespSerializer.error("unknown command 'FOO'")));
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
