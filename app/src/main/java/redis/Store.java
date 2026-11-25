package redis;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

// The keyspace: every key the server holds, with its type, value and
// expiry deadline.
//
// Thread safety: every public method is synchronized on this Store,
// and CommandHandler holds the same lock for the whole of each command.
// So only one command touches the keyspace at a time, which makes every
// command atomic — the guarantee real Redis gets from executing all
// commands on a single thread.
public class Store {

    enum Type { STRING, LIST, HASH }

    // Everything the server knows about one key lives in one Entry.
    // Because a key maps to exactly one Entry, it can only ever have
    // one type, and its value and deadline can never disagree.
    static final class Entry {
        final Type type;
        Object value;     // String, List<String> or Map<String, String>
        long expireAt;    // absolute time in ms; 0 means "never expires"

        Entry(Type type, Object value, long expireAt) {
            this.type = type;
            this.value = value;
            this.expireAt = expireAt;
        }

        String string() {
            return (String) value;
        }

        @SuppressWarnings("unchecked")
        List<String> list() {
            return (List<String>) value;
        }

        @SuppressWarnings("unchecked")
        Map<String, String> hash() {
            return (Map<String, String>) value;
        }
    }

    private static final String WRONGTYPE =
        "WRONGTYPE Operation against a key holding " +
        "the wrong kind of value";

    // The one map that holds all data. A plain HashMap is safe here
    // because every access happens while holding the Store lock.
    private final Map<String, Entry> keyspace = new HashMap<>();

    // Index of the keys that have a deadline, so the active sweep only
    // looks at those instead of at every key. Always updated together
    // with the keyspace, through put(), remove() and setDeadline().
    private final Set<String> expiring = new HashSet<>();

    // Where the snapshot lives, and where "now" comes from.
    // Both are injected so tests can use a temp directory and a
    // clock they control instead of the real file and real time.
    private final Path snapshotFile;
    private final LongSupplier clock;

    // Held for the whole of a save, so two saves cannot overlap.
    // Lock order is always saveLock first, then the Store lock.
    private final Object saveLock = new Object();

    private Thread expiryThread;
    private Thread saveThread;

    // Production defaults: dump.rdb in the working directory, system clock
    public Store() {
        this(Paths.get("dump.rdb"), System::currentTimeMillis);
    }

    // The constructor has no side effects: no disk I/O, no threads.
    // The server calls load() and startBackgroundThreads() explicitly.
    public Store(Path snapshotFile, LongSupplier clock) {
        this.snapshotFile = snapshotFile;
        this.clock = clock;
    }

    // ─────────────────────────────────────────
    // The only places that change the keyspace.
    // Keeping them in one spot keeps the 'expiring' index correct.
    // ─────────────────────────────────────────

    private void put(String key, Entry entry) {
        keyspace.put(key, entry);
        if (entry.expireAt != 0) {
            expiring.add(key);
        } else {
            expiring.remove(key);
        }
    }

    private void remove(String key) {
        keyspace.remove(key);
        expiring.remove(key);
    }

    private void setDeadline(String key, Entry entry, long expireAt) {
        entry.expireAt = expireAt;
        if (expireAt != 0) {
            expiring.add(key);
        } else {
            expiring.remove(key);
        }
    }

    // ─────────────────────────────────────────
    // Lookups
    // ─────────────────────────────────────────

    private static boolean isExpired(Entry entry, long now) {
        return entry.expireAt != 0 && now > entry.expireAt;
    }

    // Lazy expiry, in one place: returns the entry for a key, or null
    // if the key is missing. A key whose deadline has passed is deleted
    // here and reported as missing, so no command ever sees it.
    private Entry live(String key) {
        Entry entry = keyspace.get(key);
        if (entry == null) return null;
        if (isExpired(entry, clock.getAsLong())) {
            remove(key);
            return null;
        }
        return entry;
    }

    // Like live(), but also insists on the type. Returns null if the
    // key is missing; throws WRONGTYPE if it holds another type.
    private Entry typed(String key, Type type) {
        Entry entry = live(key);
        if (entry != null && entry.type != type) {
            throw new RedisError(WRONGTYPE);
        }
        return entry;
    }

    // ─────────────────────────────────────────
    // Persistence
    // ─────────────────────────────────────────

    // Load persisted data from disk — called once on startup
    public synchronized void load() {
        Map<String, Entry> loaded = new HashMap<>();
        Persistence.load(snapshotFile, clock.getAsLong(), loaded);
        for (Map.Entry<String, Entry> item : loaded.entrySet()) {
            put(item.getKey(), item.getValue());
        }
    }

    // Save a snapshot to disk.
    //
    // The keyspace is copied while holding the Store lock (fast, in
    // memory), and the copy is written to disk after releasing it
    // (slow). So the snapshot shows the data at one single instant,
    // and clients are not blocked while the disk write happens.
    //
    // Must NOT be called while holding the Store lock: it takes
    // saveLock first and then the Store lock, and taking them in the
    // opposite order elsewhere could deadlock.
    public void save() {
        synchronized (saveLock) {
            Map<String, Entry> snapshot;
            long now;
            synchronized (this) {
                now = clock.getAsLong();
                snapshot = copyKeyspace();
            }
            Persistence.save(snapshotFile, now, snapshot);
        }
    }

    // A deep copy: lists and hashes are duplicated, so later commands
    // cannot change what the snapshot writer sees. Strings are
    // immutable in Java and can be shared.
    private Map<String, Entry> copyKeyspace() {
        Map<String, Entry> copy = new HashMap<>(keyspace.size() * 2);
        for (Map.Entry<String, Entry> item : keyspace.entrySet()) {
            Entry entry = item.getValue();
            Object value;
            switch (entry.type) {
                case LIST:
                    value = new ArrayList<>(entry.list());
                    break;
                case HASH:
                    value = new LinkedHashMap<>(entry.hash());
                    break;
                default:
                    value = entry.value;
            }
            copy.put(item.getKey(),
                new Entry(entry.type, value, entry.expireAt));
        }
        return copy;
    }

    // ─────────────────────────────────────────
    // Background threads
    // ─────────────────────────────────────────

    public void startBackgroundThreads() {
        startActiveExpiry();
        startBackgroundSave();
    }

    // Stop the background threads (used by tests and clean shutdown)
    public void close() {
        if (expiryThread != null) expiryThread.interrupt();
        if (saveThread != null) saveThread.interrupt();
    }

    // Saves to disk every 60 seconds automatically, so data survives
    // even if the server is killed without a clean shutdown
    private void startBackgroundSave() {
        saveThread = new Thread(() -> {
            while (true) {
                try {
                    // Wait 60 seconds between saves
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    break;
                }
                save();
            }
        });
        saveThread.setDaemon(true);
        saveThread.setName("background-save");
        saveThread.start();
        System.out.println("Background save thread started" +
            " (saves every 60 seconds)");
    }

    // One pass of the active sweep: delete every key whose deadline
    // has passed. Takes the Store lock, so it never runs in the middle
    // of a client's command. Package-private so tests can run a pass
    // directly instead of waiting for the background thread.
    synchronized int removeExpiredKeys() {
        if (expiring.isEmpty()) return 0;
        long now = clock.getAsLong();
        int deleted = 0;
        Iterator<String> keys = expiring.iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            Entry entry = keyspace.get(key);
            if (entry != null && isExpired(entry, now)) {
                keyspace.remove(key);
                keys.remove();
                deleted++;
            }
        }
        return deleted;
    }

    private void startActiveExpiry() {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
                int deleted = removeExpiredKeys();
                if (deleted > 0) {
                    System.out.println(
                        "[Expiry] Cleaned up "
                        + deleted + " expired keys");
                }
            }
        });
        cleanupThread.setDaemon(true);
        cleanupThread.setName("expiry-cleanup");
        cleanupThread.start();
        expiryThread = cleanupThread;
        System.out.println("Active expiry thread started");
    }

    // Test hooks: look at raw state without triggering lazy expiry
    synchronized boolean holdsInMemory(String key) {
        return keyspace.containsKey(key);
    }

    synchronized int keysWithDeadline() {
        return expiring.size();
    }

    // ─────────────────────────────────────────
    // STRING COMMANDS
    // ─────────────────────────────────────────

    // SET, including its NX ("only if absent") and XX ("only if
    // present") options. The existence check and the write happen
    // under one lock, so two clients can never both win an NX.
    // Returns false if NX or XX prevented the write.
    public synchronized boolean set(String key, String value,
            long expiryMs, boolean nx, boolean xx) {
        Entry existing = live(key);
        if (nx && existing != null) return false;
        if (xx && existing == null) return false;

        long expireAt = 0;
        if (expiryMs > 0) {
            try {
                expireAt = Math.addExact(clock.getAsLong(), expiryMs);
            } catch (ArithmeticException e) {
                throw new RedisError(
                    "invalid expire time in 'set' command");
            }
        }
        // SET replaces whatever was there: any type, any old deadline
        put(key, new Entry(Type.STRING, value, expireAt));
        return true;
    }

    public synchronized String get(String key) {
        Entry entry = typed(key, Type.STRING);
        return entry == null ? null : entry.string();
    }

    public synchronized String getdel(String key) {
        Entry entry = typed(key, Type.STRING);
        if (entry == null) return null;
        remove(key);
        return entry.string();
    }

    public synchronized int setnx(String key, String value) {
        if (live(key) != null) return 0;
        put(key, new Entry(Type.STRING, value, 0));
        return 1;
    }

    public synchronized String mset(List<String> args) {
        for (int i = 0; i < args.size(); i += 2) {
            put(args.get(i),
                new Entry(Type.STRING, args.get(i + 1), 0));
        }
        return "OK";
    }

    public synchronized List<String> mget(List<String> keys) {
        List<String> results = new ArrayList<>();
        for (String key : keys) {
            Entry entry = live(key);
            // MGET returns null for a missing key AND for a key of
            // another type; it never fails with WRONGTYPE
            if (entry == null || entry.type != Type.STRING) {
                results.add(null);
            } else {
                results.add(entry.string());
            }
        }
        return results;
    }

    public synchronized int append(String key, String value) {
        Entry entry = typed(key, Type.STRING);
        String newValue;
        if (entry == null) {
            newValue = value;
            put(key, new Entry(Type.STRING, newValue, 0));
        } else {
            // Changing the value in place keeps the key's deadline
            newValue = entry.string() + value;
            entry.value = newValue;
        }
        // Redis reports lengths in bytes. Values are binary strings
        // (see Binary), so length() is the number of bytes.
        return newValue.length();
    }

    public synchronized int strlen(String key) {
        Entry entry = typed(key, Type.STRING);
        return entry == null ? 0 : entry.string().length();
    }

    // Read, add and write back all happen under the Store lock,
    // so concurrent increments can no longer overwrite each other
    public synchronized long incrby(String key, long amount) {
        Entry entry = typed(key, Type.STRING);
        String current = entry == null ? "0" : entry.string();
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RedisError(
                "value is not an integer or out of range");
        }
        // addExact throws instead of silently wrapping around
        try {
            value = Math.addExact(value, amount);
        } catch (ArithmeticException e) {
            throw new RedisError(
                "increment or decrement would overflow");
        }
        if (entry == null) {
            put(key, new Entry(Type.STRING, String.valueOf(value), 0));
        } else {
            // Changing the value in place keeps the key's deadline
            entry.value = String.valueOf(value);
        }
        return value;
    }

    public long incr(String key) {
        return incrby(key, 1);
    }

    public long decr(String key) {
        return incrby(key, -1);
    }

    public long decrby(String key, long amount) {
        // -Long.MIN_VALUE does not fit in a long
        if (amount == Long.MIN_VALUE) {
            throw new RedisError("decrement would overflow");
        }
        return incrby(key, -amount);
    }

    // ─────────────────────────────────────────
    // KEY COMMANDS
    // ─────────────────────────────────────────

    public synchronized int del(List<String> keys) {
        int deleted = 0;
        for (String key : keys) {
            // live() means an already-expired key is not counted
            if (live(key) != null) {
                remove(key);
                deleted++;
            }
        }
        return deleted;
    }

    // Counts how many of the given keys exist. A key named twice is
    // counted twice, as in Redis.
    public synchronized int exists(List<String> keys) {
        int count = 0;
        for (String key : keys) {
            if (live(key) != null) count++;
        }
        return count;
    }

    public synchronized String type(String key) {
        Entry entry = live(key);
        if (entry == null) return "none";
        switch (entry.type) {
            case STRING: return "string";
            case LIST:   return "list";
            default:     return "hash";
        }
    }

    public synchronized String rename(String key, String newKey) {
        Entry entry = live(key);
        if (entry == null) {
            throw new RedisError("no such key");
        }
        // Renaming a key to itself changes nothing
        if (key.equals(newKey)) return "OK";

        // Move the same Entry to the new name. Its value and its
        // deadline travel together, and whatever the destination
        // held before (value and deadline) is replaced.
        remove(key);
        put(newKey, entry);
        return "OK";
    }

    // All live keys matching a glob pattern such as  user:*
    public synchronized List<String> keys(String pattern) {
        long now = clock.getAsLong();
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, Entry> item : keyspace.entrySet()) {
            if (!isExpired(item.getValue(), now)
                    && Glob.matches(pattern, item.getKey())) {
                result.add(item.getKey());
            }
        }
        return result;
    }

    public synchronized int dbsize() {
        long now = clock.getAsLong();
        int count = 0;
        for (Entry entry : keyspace.values()) {
            if (!isExpired(entry, now)) count++;
        }
        return count;
    }

    public synchronized String flushall() {
        keyspace.clear();
        expiring.clear();
        return "OK";
    }

    // ─────────────────────────────────────────
    // EXPIRY COMMANDS
    // ─────────────────────────────────────────

    public synchronized int expire(String key, long seconds) {
        Entry entry = live(key);
        if (entry == null) return 0;
        // A deadline that is not in the future deletes the key now
        if (seconds <= 0) {
            remove(key);
            return 1;
        }
        try {
            setDeadline(key, entry, Math.addExact(clock.getAsLong(),
                Math.multiplyExact(seconds, 1000L)));
        } catch (ArithmeticException e) {
            throw new RedisError(
                "invalid expire time in 'expire' command");
        }
        return 1;
    }

    public synchronized long ttl(String key) {
        Entry entry = live(key);
        if (entry == null) return -2;
        if (entry.expireAt == 0) return -1;
        return Math.max(1,
            (entry.expireAt - clock.getAsLong()) / 1000);
    }

    public synchronized long pttl(String key) {
        Entry entry = live(key);
        if (entry == null) return -2;
        if (entry.expireAt == 0) return -1;
        return Math.max(1,
            entry.expireAt - clock.getAsLong());
    }

    public synchronized int persist(String key) {
        Entry entry = live(key);
        if (entry == null || entry.expireAt == 0) return 0;
        setDeadline(key, entry, 0);
        return 1;
    }

    // ─────────────────────────────────────────
    // LIST COMMANDS
    // ─────────────────────────────────────────

    // Returns the list at key, creating an empty one if the key is
    // missing. Throws WRONGTYPE if the key holds another type.
    private List<String> listForWrite(String key) {
        Entry entry = typed(key, Type.LIST);
        if (entry == null) {
            entry = new Entry(Type.LIST, new ArrayList<String>(), 0);
            put(key, entry);
        }
        return entry.list();
    }

    public synchronized long lpush(String key, List<String> values) {
        List<String> list = listForWrite(key);
        // Like Redis, each value is pushed onto the head in turn,
        // so LPUSH k a b c leaves the list as c, b, a
        for (String value : values) {
            list.add(0, value);
        }
        return list.size();
    }

    public synchronized long rpush(String key, List<String> values) {
        List<String> list = listForWrite(key);
        list.addAll(values);
        return list.size();
    }

    public synchronized String lpop(String key) {
        Entry entry = typed(key, Type.LIST);
        if (entry == null) return null;
        List<String> list = entry.list();
        String value = list.remove(0);
        // An empty list does not exist: remove the key with it
        if (list.isEmpty()) remove(key);
        return value;
    }

    public synchronized String rpop(String key) {
        Entry entry = typed(key, Type.LIST);
        if (entry == null) return null;
        List<String> list = entry.list();
        String value = list.remove(list.size() - 1);
        if (list.isEmpty()) remove(key);
        return value;
    }

    public synchronized long llen(String key) {
        Entry entry = typed(key, Type.LIST);
        return entry == null ? 0 : entry.list().size();
    }

    public synchronized List<String> lrange(
            String key, int start, int stop) {
        Entry entry = typed(key, Type.LIST);
        if (entry == null) return new ArrayList<>();
        List<String> list = entry.list();
        int size = list.size();
        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;
        stop = Math.min(stop, size - 1);
        if (start > stop) return new ArrayList<>();
        return new ArrayList<>(list.subList(start, stop + 1));
    }

    public synchronized String lindex(String key, int index) {
        Entry entry = typed(key, Type.LIST);
        if (entry == null) return null;
        List<String> list = entry.list();
        int size = list.size();
        if (index < 0) index = size + index;
        if (index < 0 || index >= size) return null;
        return list.get(index);
    }

    // ─────────────────────────────────────────
    // HASH COMMANDS
    // ─────────────────────────────────────────

    public synchronized int hset(String key, List<String> fieldValues) {
        Entry entry = typed(key, Type.HASH);
        if (entry == null) {
            entry = new Entry(Type.HASH,
                new LinkedHashMap<String, String>(), 0);
            put(key, entry);
        }
        Map<String, String> hash = entry.hash();
        int added = 0;
        for (int i = 0; i < fieldValues.size(); i += 2) {
            String field = fieldValues.get(i);
            String value = fieldValues.get(i + 1);
            if (hash.put(field, value) == null) {
                added++;
            }
        }
        return added;
    }

    public synchronized String hget(String key, String field) {
        Entry entry = typed(key, Type.HASH);
        return entry == null ? null : entry.hash().get(field);
    }

    public synchronized List<String> hgetall(String key) {
        Entry entry = typed(key, Type.HASH);
        List<String> result = new ArrayList<>();
        if (entry == null) return result;
        for (Map.Entry<String, String> field
                : entry.hash().entrySet()) {
            result.add(field.getKey());
            result.add(field.getValue());
        }
        return result;
    }

    public synchronized int hdel(String key, List<String> fields) {
        Entry entry = typed(key, Type.HASH);
        if (entry == null) return 0;
        Map<String, String> hash = entry.hash();
        int deleted = 0;
        for (String field : fields) {
            if (hash.remove(field) != null) {
                deleted++;
            }
        }
        // An empty hash does not exist: remove the key with it
        if (hash.isEmpty()) remove(key);
        return deleted;
    }

    public synchronized int hexists(String key, String field) {
        Entry entry = typed(key, Type.HASH);
        if (entry == null) return 0;
        return entry.hash().containsKey(field) ? 1 : 0;
    }

    public synchronized int hlen(String key) {
        Entry entry = typed(key, Type.HASH);
        return entry == null ? 0 : entry.hash().size();
    }

    public synchronized List<String> hkeys(String key) {
        Entry entry = typed(key, Type.HASH);
        if (entry == null) return new ArrayList<>();
        return new ArrayList<>(entry.hash().keySet());
    }

    public synchronized List<String> hvals(String key) {
        Entry entry = typed(key, Type.HASH);
        if (entry == null) return new ArrayList<>();
        return new ArrayList<>(entry.hash().values());
    }
}
