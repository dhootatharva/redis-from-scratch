package redis;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class RespParser {

    // The stream we read bytes from
    // This is the data coming from the client over the network
    private final InputStream input;

    // Constructor — give the parser an input stream to read from
    public RespParser(InputStream input) {
        this.input = input;
    }

    // Main method — parse one complete command from the stream
    // Returns a List of Strings like ["SET", "name", "Atharva"]
    // Returns null if the client disconnected
    public List<String> parse() throws IOException {

        // Read the very first byte — this tells us what type is coming
        int firstByte = input.read();

        // -1 means the client closed the connection
        if (firstByte == -1) {
            return null;
        }

        // Convert the int to a char so we can compare it
        char type = (char) firstByte;

        // Every command from redis-cli is an array (*)
        // So we handle that case here
        if (type == '*') {
            return parseArray();
        }

        // If we see something else, we do not handle it yet
        throw new IOException("Unexpected first byte: " + type);
    }

    // Parse an array — called after we already read the '*'
    private List<String> parseArray() throws IOException {

        // Read the number of elements in the array
        // For *3\r\n this reads "3"
        int count = Integer.parseInt(readLine());

        // Create a list to hold all the parsed strings
        List<String> parts = new ArrayList<>();

        // Loop exactly 'count' times
        // Each iteration reads one element
        for (int i = 0; i < count; i++) {

            // Read the type byte of this element
            int typeByte = input.read();
            char elementType = (char) typeByte;

            // Every element in a command array is a bulk string ($)
            if (elementType == '$') {
                parts.add(parseBulkString());
            } else {
                throw new IOException(
                    "Expected bulk string, got: " + elementType
                );
            }
        }

        return parts;
    }

    // Parse a bulk string — called after we already read the '$'
    private String parseBulkString() throws IOException {

        // Read the length — for $4\r\n this reads "4"
        int length = Integer.parseInt(readLine());

        // Handle null bulk string ($-1\r\n)
        // This means "no value" — used in GET response when key missing
        if (length == -1) {
            return null;
        }

        // Read exactly 'length' bytes into a buffer
        byte[] buffer = new byte[length];

        // read() might not give us all bytes at once
        // so we keep reading until we have all of them
        int totalRead = 0;
        while (totalRead < length) {
            int read = input.read(buffer, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Connection closed while reading");
            }
            totalRead += read;
        }

        // After the string bytes, there is always \r\n
        // We must read and discard these two bytes
        input.read(); // \r
        input.read(); // \n

        // Convert the byte array to a String and return it
        return new String(buffer);
    }

    // Helper — read bytes until we hit \r\n
    // Returns everything before the \r\n as a String
    // For example: reads "3\r\n" and returns "3"
    private String readLine() throws IOException {

        StringBuilder sb = new StringBuilder();

        while (true) {
            int b = input.read();

            // -1 means disconnected
            if (b == -1) {
                throw new IOException("Connection closed");
            }

            // \r signals end of line is coming
            if (b == '\r') {
                // Read and discard the \n
                input.read();
                break;
            }

            // Otherwise add this character to our string
            sb.append((char) b);
        }

        return sb.toString();
    }
}