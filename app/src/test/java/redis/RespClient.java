package redis;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

// A minimal Redis client for tests. It talks to the server over a real
// TCP socket and returns each reply exactly as it appeared on the wire,
// so tests assert on the protocol bytes, not on a parsed value.
final class RespClient implements AutoCloseable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    RespClient(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        // Fail the test instead of hanging if the server never replies
        socket.setSoTimeout(3000);
        // Send small writes immediately (needed by the split-write test)
        socket.setTcpNoDelay(true);
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
    }

    // Encode a command the way redis-cli does: an array of bulk strings
    static byte[] encode(String... args) {
        byte[][] raw = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            raw[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }
        return encodeBytes(raw);
    }

    // The same, for arguments that are raw bytes instead of text
    static byte[] encodeBytes(byte[]... args) {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        write(buf, "*" + args.length + "\r\n");
        for (byte[] body : args) {
            write(buf, "$" + body.length + "\r\n");
            buf.write(body, 0, body.length);
            write(buf, "\r\n");
        }
        return buf.toByteArray();
    }

    private static void write(ByteArrayOutputStream buf, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        buf.write(bytes, 0, bytes.length);
    }

    // Send one command and return its reply as text
    String command(String... args) throws IOException {
        sendRaw(encode(args));
        return readReply();
    }

    void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    void sendRaw(String text) throws IOException {
        sendRaw(text.getBytes(StandardCharsets.UTF_8));
    }

    // Read exactly one complete reply and return it as text
    String readReply() throws IOException {
        return new String(readReplyBytes(), StandardCharsets.UTF_8);
    }

    // Read exactly one complete reply and return its raw bytes
    byte[] readReplyBytes() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        readReplyInto(buf);
        return buf.toByteArray();
    }

    private void readReplyInto(ByteArrayOutputStream buf)
            throws IOException {
        String line = readLineInto(buf);
        char type = line.charAt(0);
        if (type == '$') {
            int length = Integer.parseInt(line.substring(1));
            if (length >= 0) {
                // the body plus its trailing \r\n
                for (int i = 0; i < length + 2; i++) {
                    buf.write(readByte());
                }
            }
        } else if (type == '*') {
            int count = Integer.parseInt(line.substring(1));
            for (int i = 0; i < count; i++) {
                readReplyInto(buf);
            }
        }
        // '+', '-' and ':' replies are a single line: nothing more to read
    }

    // Reads up to and including \r\n, copies it into buf,
    // and returns the line without the \r\n
    private String readLineInto(ByteArrayOutputStream buf)
            throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = readByte();
            buf.write(b);
            if (b == '\n') break;
            if (b != '\r') line.append((char) b);
        }
        return line.toString();
    }

    private int readByte() throws IOException {
        int b = in.read();
        if (b == -1) {
            throw new EOFException("server closed the connection");
        }
        return b;
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
