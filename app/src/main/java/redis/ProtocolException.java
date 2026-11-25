package redis;

import java.io.IOException;

// The client sent bytes that are not valid RESP.
//
// This is different from a command error such as WRONGTYPE. After a
// command error the stream is still in step and the next command can
// be read. After a protocol error the server no longer knows where
// one command ends and the next begins, so the only safe response is
// to tell the client and close the connection.
public class ProtocolException extends IOException {

    public ProtocolException(String message) {
        super(message);
    }
}
