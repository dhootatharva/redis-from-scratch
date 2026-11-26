package redis;

import java.util.List;

// Builds replies. All text is turned into bytes with Binary.encode,
// so a stored value goes back to the client byte for byte.
public class RespSerializer {

    // Simple string response — +OK\r\n or +PONG\r\n
    // Used for success responses
    public static byte[] simpleString(String value) {
        return Binary.encode("+" + oneLine(value) + "\r\n");
    }

    // A simple string or an error ends at the first line break, so the
    // text itself must not contain one. Error text can include what
    // the client sent (an unknown command's name, say), and a client
    // may send any bytes. A line break in there would end the reply
    // early and the client would read the rest as a second reply.
    // Redis replaces line breaks with spaces; so does this.
    private static String oneLine(String text) {
        if (text.indexOf('\r') < 0 && text.indexOf('\n') < 0) {
            return text;
        }
        return text.replace('\r', ' ').replace('\n', ' ');
    }

    // Error response — -ERR message\r\n
    // Redis sends some errors with their own code instead of ERR,
    // e.g. -WRONGTYPE Operation against a key holding the wrong kind of value
    public static byte[] error(String message) {
        String line = message.startsWith("WRONGTYPE ")
            ? message
            : "ERR " + message;
        return Binary.encode("-" + oneLine(line) + "\r\n");
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
