package redis;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Reads commands from a client.
//
// Everything a client sends is untrusted. A length or a count is only
// a claim, so each one is checked against a limit BEFORE the parser
// acts on it. Input that is not valid RESP raises ProtocolException.
//
// Values are returned as binary strings (see Binary): one char per
// byte, so arbitrary bytes pass through unchanged.
public class RespParser {

    // Same limit as Redis: one bulk string is at most 512 MB
    static final int MAX_BULK_LENGTH = 512 * 1024 * 1024;

    // Same limit as Redis: at most 1,048,576 arguments per command
    static final int MAX_ARRAY_LENGTH = 1024 * 1024;

    // Same limit as Redis: 64 KB for a header line or inline command
    static final int MAX_LINE_LENGTH = 64 * 1024;

    // The stream we read bytes from
    // This is the data coming from the client over the network
    private final InputStream input;

    // Constructor — give the parser an input stream to read from
    // Buffered so each read() does not become a separate socket call
    public RespParser(InputStream input) {
        this.input = new BufferedInputStream(input);
    }

    // Parse one complete command from the stream.
    // Returns a List of Strings like ["SET", "name", "Atharva"].
    // Returns an empty list for an empty command (the caller skips it).
    // Returns null if the client disconnected between commands.
    public List<String> parse() throws IOException {

        // Read the very first byte — this tells us what type is coming
        int firstByte = input.read();

        // -1 means the client closed the connection
        if (firstByte == -1) {
            return null;
        }

        // Every command from a real client is an array (*)
        if (firstByte == '*') {
            return parseArray();
        }

        // Anything else is an inline command: plain text followed by
        // a newline, as typed into telnet or nc.  PING\r\n
        return parseInline(firstByte);
    }

    // Parse an array — called after we already read the '*'
    private List<String> parseArray() throws IOException {

        // Read the number of elements in the array
        // For *3\r\n this reads "3"
        long count = parseNumber(readLine(),
            "invalid multibulk length");

        if (count > MAX_ARRAY_LENGTH) {
            throw new ProtocolException("invalid multibulk length");
        }

        // *0 and *-1 are empty commands; Redis silently ignores them
        if (count <= 0) {
            return Collections.emptyList();
        }

        List<String> parts = new ArrayList<>();

        // Loop exactly 'count' times
        // Each iteration reads one element
        for (long i = 0; i < count; i++) {

            // Read the type byte of this element
            int typeByte = input.read();
            if (typeByte == -1) {
                throw new IOException("Connection closed while reading");
            }

            // Every element in a command array is a bulk string ($)
            if (typeByte != '$') {
                throw new ProtocolException(
                    "expected '$', got '" + (char) typeByte + "'");
            }
            parts.add(parseBulkString());
        }

        return parts;
    }

    // Parse a bulk string — called after we already read the '$'
    private String parseBulkString() throws IOException {

        // Read the length — for $4\r\n this reads "4"
        long length = parseNumber(readLine(), "invalid bulk length");

        // The length is the client's claim. Refuse negative and
        // oversized values before reading (or allocating) anything.
        if (length < 0 || length > MAX_BULK_LENGTH) {
            throw new ProtocolException("invalid bulk length");
        }

        // readNBytes keeps reading until it has 'length' bytes or the
        // stream ends. It grows its buffer as data actually arrives,
        // so a client that claims 500 MB and sends nothing does not
        // make the server reserve 500 MB.
        byte[] body = input.readNBytes((int) length);
        if (body.length < length) {
            throw new IOException("Connection closed while reading");
        }

        // After the bytes there must be exactly \r\n
        expectCrlf();

        return Binary.decode(body);
    }

    private void expectCrlf() throws IOException {
        int cr = input.read();
        int lf = input.read();
        if (cr == -1 || lf == -1) {
            throw new IOException("Connection closed while reading");
        }
        if (cr != '\r' || lf != '\n') {
            throw new ProtocolException(
                "expected CRLF after bulk string");
        }
    }

    // Parse an inline command — the first byte is already read.
    // Arguments are separated by spaces; there is no quoting.
    private List<String> parseInline(int firstByte) throws IOException {

        // A bare newline is an empty command
        if (firstByte == '\n') {
            return Collections.emptyList();
        }

        StringBuilder sb = new StringBuilder();
        sb.append((char) firstByte);
        String line = finishLine(sb).trim();

        if (line.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> parts = new ArrayList<>();
        for (String part : line.split(" +")) {
            parts.add(part);
        }
        return parts;
    }

    // Read one line and return it without its line ending.
    // For example: reads "3\r\n" and returns "3"
    private String readLine() throws IOException {
        return finishLine(new StringBuilder());
    }

    // Keep reading into sb until \n, then strip the trailing \r
    private String finishLine(StringBuilder sb) throws IOException {

        while (true) {
            int b = input.read();

            // -1 means disconnected
            if (b == -1) {
                throw new IOException("Connection closed");
            }

            if (b == '\n') {
                break;
            }

            sb.append((char) b);

            // A line that never ends must not grow without limit
            if (sb.length() > MAX_LINE_LENGTH) {
                throw new ProtocolException("too big inline request");
            }
        }

        int end = sb.length();
        if (end > 0 && sb.charAt(end - 1) == '\r') {
            sb.setLength(end - 1);
        }
        return sb.toString();
    }

    // A count or length must be a plain integer. Anything else is a
    // protocol error, reported with the message Redis uses.
    private static long parseNumber(String text, String error)
            throws ProtocolException {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new ProtocolException(error);
        }
    }
}
