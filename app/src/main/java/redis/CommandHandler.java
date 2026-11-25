package redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// Turns a parsed command into a reply.
//
// Every command is described once, in a table: its name, how many
// arguments it takes, and the code that runs it. Looking a command up,
// checking its argument count, taking the lock and converting errors
// then happen in ONE place (handle) for all commands, instead of being
// repeated inside each one. Real Redis has the same kind of table.
public class CommandHandler {

    // The code behind one command. args.get(0) is the command name.
    @FunctionalInterface
    private interface Action {
        byte[] run(List<String> args);
    }

    private static final class Spec {
        final String name;

        // The Redis convention for argument counts, name included:
        //   positive  = exactly this many      GET key        ->  2
        //   negative  = at least this many     DEL key [key]  -> -2
        final int arity;

        // true: runs while holding the Store lock (see handle)
        final boolean usesStore;

        final Action action;

        Spec(String name, int arity, boolean usesStore, Action action) {
            this.name = name;
            this.arity = arity;
            this.usesStore = usesStore;
            this.action = action;
        }
    }

    private final Store store;
    private final Map<String, Spec> commands = new HashMap<>();

    public CommandHandler(Store store) {
        this.store = store;
        registerConnectionCommands();
        registerStringCommands();
        registerKeyCommands();
        registerListCommands();
        registerHashCommands();
        registerServerCommands();
    }

    // ─────────────────────────────────────────
    // Dispatch — the same steps for every command
    // ─────────────────────────────────────────

    public byte[] handle(List<String> command) {

        // The server skips empty commands before they get here, but
        // this method must never throw on any input
        if (command.isEmpty()) {
            return RespSerializer.error("empty command");
        }

        Spec spec = commands.get(
            command.get(0).toUpperCase(Locale.ROOT));
        if (spec == null) {
            return RespSerializer.error(
                "unknown command '" + command.get(0) + "'");
        }

        if (!arityMatches(spec.arity, command.size())) {
            return RespSerializer.error(
                "wrong number of arguments for '"
                + spec.name.toLowerCase(Locale.ROOT) + "' command");
        }

        try {
            if (!spec.usesStore) {
                return spec.action.run(command);
            }
            // A keyspace command runs while holding the Store lock,
            // so only one command touches the data at a time. This is
            // what makes each command atomic — the same guarantee real
            // Redis gets from executing all commands on a single
            // thread. Reading from and writing to sockets happens
            // outside this lock, so network work still runs in parallel.
            synchronized (store) {
                return spec.action.run(command);
            }
        } catch (RedisError e) {
            // The client asked for something invalid. Anything else
            // that is thrown is a server bug and is left to the caller.
            return RespSerializer.error(e.getMessage());
        }
    }

    private static boolean arityMatches(int arity, int actual) {
        return arity >= 0 ? actual == arity : actual >= -arity;
    }

    // ─────────────────────────────────────────
    // Registration helpers
    // ─────────────────────────────────────────

    // A command that reads or writes the keyspace
    private void keyspace(String name, int arity, Action action) {
        commands.put(name, new Spec(name, arity, true, action));
    }

    // A command that does not touch the keyspace (or, for SAVE,
    // must NOT hold the Store lock when it runs)
    private void plain(String name, int arity, Action action) {
        commands.put(name, new Spec(name, arity, false, action));
    }

    // ─────────────────────────────────────────
    // Argument and reply helpers
    // ─────────────────────────────────────────

    private static long parseLong(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new RedisError(
                "value is not an integer or out of range");
        }
    }

    // List indexes: any integer is accepted, and values beyond what a
    // list can hold are clamped (they are out of range either way)
    private static int parseIndex(String text) {
        long value = parseLong(text);
        if (value > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        if (value < Integer.MIN_VALUE) return Integer.MIN_VALUE;
        return (int) value;
    }

    private static List<String> from(List<String> args, int start) {
        return args.subList(start, args.size());
    }

    private static byte[] ok() {
        return RespSerializer.simpleString("OK");
    }

    private static byte[] integer(long value) {
        return RespSerializer.integer(value);
    }

    private static byte[] bulk(String value) {
        return RespSerializer.bulkString(value);
    }

    private static byte[] array(List<String> items) {
        return RespSerializer.array(items);
    }

    // ─────────────────────────────────────────
    // Connection
    // ─────────────────────────────────────────

    private void registerConnectionCommands() {

        plain("PING", -1, a -> {
            if (a.size() > 2) {
                throw new RedisError(
                    "wrong number of arguments for 'ping' command");
            }
            return a.size() == 1
                ? RespSerializer.simpleString("PONG")
                : bulk(a.get(1));
        });

        plain("ECHO", 2, a -> bulk(a.get(1)));

        // Only database 0 exists
        plain("SELECT", 2, a -> {
            if (parseLong(a.get(1)) != 0) {
                throw new RedisError("DB index is out of range");
            }
            return ok();
        });

        // Client libraries send these while connecting. Answering
        // them plainly lets real clients talk to this server.
        plain("CLIENT", -2, a -> {
            switch (a.get(1).toUpperCase(Locale.ROOT)) {
                case "SETINFO":
                case "SETNAME":
                    return ok();
                case "GETNAME":
                    return RespSerializer.nullBulkString();
                case "ID":
                    return integer(1);
                default:
                    throw new RedisError(
                        "unknown subcommand '" + a.get(1) + "'");
            }
        });

        // redis-cli asks for the command list on startup and works
        // without one
        plain("COMMAND", -1,
            a -> array(Collections.emptyList()));
    }

    // ─────────────────────────────────────────
    // Strings
    // ─────────────────────────────────────────

    private void registerStringCommands() {

        keyspace("SET", -3, this::set);

        keyspace("GET", 2, a -> bulk(store.get(a.get(1))));

        keyspace("GETDEL", 2, a -> bulk(store.getdel(a.get(1))));

        keyspace("SETNX", 3,
            a -> integer(store.setnx(a.get(1), a.get(2))));

        keyspace("MSET", -3, a -> {
            // key value [key value ...] — the pairs must be complete
            if (a.size() % 2 == 0) {
                throw new RedisError(
                    "wrong number of arguments for 'mset' command");
            }
            store.mset(from(a, 1));
            return ok();
        });

        keyspace("MGET", -2, a -> array(store.mget(from(a, 1))));

        keyspace("APPEND", 3,
            a -> integer(store.append(a.get(1), a.get(2))));

        keyspace("STRLEN", 2, a -> integer(store.strlen(a.get(1))));

        keyspace("INCR", 2, a -> integer(store.incr(a.get(1))));

        keyspace("DECR", 2, a -> integer(store.decr(a.get(1))));

        keyspace("INCRBY", 3,
            a -> integer(store.incrby(a.get(1), parseLong(a.get(2)))));

        keyspace("DECRBY", 3,
            a -> integer(store.decrby(a.get(1), parseLong(a.get(2)))));
    }

    // SET key value [EX seconds | PX milliseconds] [NX | XX]
    private byte[] set(List<String> a) {
        String key = a.get(1);
        String value = a.get(2);
        long expiryMs = -1;
        boolean nx = false;
        boolean xx = false;

        for (int i = 3; i < a.size(); i++) {
            String option = a.get(i).toUpperCase(Locale.ROOT);
            switch (option) {
                case "NX":
                    nx = true;
                    break;
                case "XX":
                    xx = true;
                    break;
                case "EX":
                case "PX":
                    // only one expiry, and it needs a value after it
                    if (expiryMs != -1 || i + 1 >= a.size()) {
                        throw new RedisError("syntax error");
                    }
                    long amount = parseLong(a.get(++i));
                    if (amount <= 0) {
                        throw new RedisError(
                            "invalid expire time in 'set' command");
                    }
                    try {
                        expiryMs = option.equals("EX")
                            ? Math.multiplyExact(amount, 1000L)
                            : amount;
                    } catch (ArithmeticException e) {
                        throw new RedisError(
                            "invalid expire time in 'set' command");
                    }
                    break;
                default:
                    throw new RedisError("syntax error");
            }
        }

        // "only if absent" and "only if present" contradict each other
        if (nx && xx) {
            throw new RedisError("syntax error");
        }

        // The NX / XX check and the write are one Store call,
        // so they cannot be separated
        boolean written = store.set(key, value, expiryMs, nx, xx);
        return written ? ok() : RespSerializer.nullBulkString();
    }

    // ─────────────────────────────────────────
    // Keys and expiry
    // ─────────────────────────────────────────

    private void registerKeyCommands() {

        keyspace("DEL", -2, a -> integer(store.del(from(a, 1))));

        keyspace("EXISTS", -2, a -> integer(store.exists(from(a, 1))));

        keyspace("KEYS", 2, a -> array(store.keys(a.get(1))));

        keyspace("TYPE", 2,
            a -> RespSerializer.simpleString(store.type(a.get(1))));

        keyspace("RENAME", 3, a -> {
            store.rename(a.get(1), a.get(2));
            return ok();
        });

        keyspace("DBSIZE", 1, a -> integer(store.dbsize()));

        keyspace("FLUSHALL", -1, a -> {
            store.flushall();
            return ok();
        });

        keyspace("EXPIRE", 3,
            a -> integer(store.expire(a.get(1), parseLong(a.get(2)))));

        keyspace("TTL", 2, a -> integer(store.ttl(a.get(1))));

        keyspace("PTTL", 2, a -> integer(store.pttl(a.get(1))));

        keyspace("PERSIST", 2, a -> integer(store.persist(a.get(1))));
    }

    // ─────────────────────────────────────────
    // Lists
    // ─────────────────────────────────────────

    private void registerListCommands() {

        keyspace("LPUSH", -3,
            a -> integer(store.lpush(a.get(1), from(a, 2))));

        keyspace("RPUSH", -3,
            a -> integer(store.rpush(a.get(1), from(a, 2))));

        keyspace("LPOP", 2, a -> bulk(store.lpop(a.get(1))));

        keyspace("RPOP", 2, a -> bulk(store.rpop(a.get(1))));

        keyspace("LLEN", 2, a -> integer(store.llen(a.get(1))));

        keyspace("LRANGE", 4, a -> array(store.lrange(
            a.get(1), parseIndex(a.get(2)), parseIndex(a.get(3)))));

        keyspace("LINDEX", 3,
            a -> bulk(store.lindex(a.get(1), parseIndex(a.get(2)))));
    }

    // ─────────────────────────────────────────
    // Hashes
    // ─────────────────────────────────────────

    private void registerHashCommands() {

        keyspace("HSET", -4, a -> {
            // key field value [field value ...]
            if (a.size() % 2 != 0) {
                throw new RedisError(
                    "wrong number of arguments for 'hset' command");
            }
            return integer(store.hset(a.get(1), from(a, 2)));
        });

        keyspace("HGET", 3, a -> bulk(store.hget(a.get(1), a.get(2))));

        keyspace("HGETALL", 2, a -> array(store.hgetall(a.get(1))));

        keyspace("HDEL", -3,
            a -> integer(store.hdel(a.get(1), from(a, 2))));

        keyspace("HEXISTS", 3,
            a -> integer(store.hexists(a.get(1), a.get(2))));

        keyspace("HLEN", 2, a -> integer(store.hlen(a.get(1))));

        keyspace("HKEYS", 2, a -> array(store.hkeys(a.get(1))));

        keyspace("HVALS", 2, a -> array(store.hvals(a.get(1))));
    }

    // ─────────────────────────────────────────
    // Server
    // ─────────────────────────────────────────

    private void registerServerCommands() {

        // SAVE and BGSAVE are "plain": Store.save() takes its own
        // locks in a fixed order (see Store.save), so it must be
        // called WITHOUT already holding the Store lock.
        plain("SAVE", 1, a -> {
            store.save();
            return ok();
        });

        plain("BGSAVE", -1, a -> {
            // Run save in a background thread
            // so it does not block the client
            new Thread(() -> store.save()).start();
            return RespSerializer.simpleString(
                "Background saving started");
        });

        keyspace("INFO", -1, a -> bulk(
            "# Server\r\n"
            + "redis_mode:standalone\r\n"
            + "# Keyspace\r\n"
            + "db0:keys=" + store.dbsize() + "\r\n"));

        // Benchmark and admin tools read settings with CONFIG GET.
        // There are none to report.
        plain("CONFIG", -2, a -> {
            if (a.get(1).equalsIgnoreCase("GET")) {
                return array(Collections.emptyList());
            }
            throw new RedisError(
                "unknown subcommand '" + a.get(1) + "'");
        });
    }
}
