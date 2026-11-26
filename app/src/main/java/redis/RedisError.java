package redis;

// An error that is the client's fault and is reported back to it as an
// error reply: wrong type, bad argument, no such key.
//
// Having its own type separates "the client asked for something
// invalid" from "the server has a bug". CommandHandler turns a
// RedisError into "-ERR ..."; any other exception is a server bug and
// is reported as an internal error.
public class RedisError extends RuntimeException {

    public RedisError(String message) {
        super(message);
    }
}
