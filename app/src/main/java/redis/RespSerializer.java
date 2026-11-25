package redis;

import java.util.List;

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
        return Binary.encode(
            "$" + value.length() + "\r\n" + value + "\r\n");
    }

    // Null response — when GET finds no key
    public static byte[] nullBulkString() {
        return Binary.encode("$-1\r\n");
    }

    // Array of bulk strings — *2\r\n$1\r\na\r\n$1\r\nb\r\n
    // A null item becomes a null bulk string (used by MGET).
    public static byte[] array(List<String> items) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(items.size()).append("\r\n");
        for (String item : items) {
            if (item == null) {
                sb.append("$-1\r\n");
            } else {
                sb.append("$").append(item.length()).append("\r\n")
                  .append(item).append("\r\n");
            }
        }
        return Binary.encode(sb.toString());
    }
}
