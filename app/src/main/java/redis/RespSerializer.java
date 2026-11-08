package redis;

import java.nio.charset.StandardCharsets;

public class RespSerializer {

    // Simple string response — +OK\r\n or +PONG\r\n
    // Used for success responses
    public static byte[] simpleString(String value) {
        return ("+" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    // Error response — -ERR message\r\n
    // Redis sends some errors with their own code instead of ERR,
    // e.g. -WRONGTYPE Operation against a key holding the wrong kind of value
    public static byte[] error(String message) {
        String line = message.startsWith("WRONGTYPE ")
            ? message
            : "ERR " + message;
        return ("-" + line + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    // Integer response — :42\r\n
    // Used for INCR, DEL, LLEN etc
    public static byte[] integer(long value) {
        return (":" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    // Bulk string response — $6\r\nfoobar\r\n
    // The length prefix is the number of bytes, not characters
    public static byte[] bulkString(String value) {
        if (value == null) {
            // Null bulk string — key does not exist
            return nullBulkString();
        }
        byte[] body = value.getBytes(StandardCharsets.UTF_8);
        byte[] header = ("$" + body.length + "\r\n")
            .getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[header.length + body.length + 2];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(body, 0, out, header.length, body.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    // Null response — when GET finds no key
    public static byte[] nullBulkString() {
        return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    }
}
