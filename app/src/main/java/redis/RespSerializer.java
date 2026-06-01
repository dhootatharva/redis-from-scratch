package redis;

public class RespSerializer {

    // Simple string response — +OK\r\n or +PONG\r\n
    // Used for success responses
    public static byte[] simpleString(String value) {
        return ("+" + value + "\r\n").getBytes();
    }

    // Error response — -ERR message\r\n
    // Used when a command fails
    public static byte[] error(String message) {
        return ("-ERR " + message + "\r\n").getBytes();
    }

    // Integer response — :42\r\n
    // Used for INCR, DEL, LLEN etc
    public static byte[] integer(long value) {
        return (":" + value + "\r\n").getBytes();
    }

    // Bulk string response — $6\r\nfoobar\r\n
    // Used for GET responses
    public static byte[] bulkString(String value) {
        if (value == null) {
            // Null bulk string — key does not exist
            return "$-1\r\n".getBytes();
        }
        return ("$" + value.length() + "\r\n" + value + "\r\n").getBytes();
    }

    // Null response — when GET finds no key
    public static byte[] nullBulkString() {
        return "$-1\r\n".getBytes();
    }
}