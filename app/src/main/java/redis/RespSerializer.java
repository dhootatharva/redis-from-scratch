package redis;

// Builds replies. All text is turned into bytes with Binary.encode,
// so a stored value goes back to the client byte for byte.
public class RespSerializer {

    // Simple string response — +OK\r\n or +PONG\r\n
    // Used for success responses
    public static byte[] simpleString(String value) {
        return Binary.encode("+" + value + "\r\n");
    }

    // Error response — -ERR message\r\n
    // Redis sends some errors with their own code instead of ERR,
    // e.g. -WRONGTYPE Operation against a key holding the wrong kind of value
    public static byte[] error(String message) {
        String line = message.startsWith("WRONGTYPE ")
            ? message
            : "ERR " + message;
        return Binary.encode("-" + line + "\r\n");
    }

    // Integer response — :42\r\n
    // Used for INCR, DEL, LLEN etc
    public static byte[] integer(long value) {
        return Binary.encode(":" + value + "\r\n");
    }

    // Bulk string response — $6\r\nfoobar\r\n
    // The length prefix is the number of bytes. For a binary string
    // that is simply its length: one char per byte.
    public static byte[] bulkString(String value) {
        if (value == null) {
            // Null bulk string — key does not exist
            return nullBulkString();
        }
        byte[] body = Binary.encode(value);
        byte[] header = Binary.encode("$" + body.length + "\r\n");
        byte[] out = new byte[header.length + body.length + 2];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(body, 0, out, header.length, body.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    // Null response — when GET finds no key
    public static byte[] nullBulkString() {
        return Binary.encode("$-1\r\n");
    }
}
