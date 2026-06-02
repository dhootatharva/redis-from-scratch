package redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Store {

    // Main data storage — key → value
    private final ConcurrentHashMap<String, String> data
        = new ConcurrentHashMap<>();

    // Expiry storage — key → expiry time in milliseconds
    // Only keys with an expiry appear here
    private final ConcurrentHashMap<String, Long> expiry
        = new ConcurrentHashMap<>();

    // Constructor — starts the active expiry background thread
    public Store() {
        startActiveExpiry();
    }

    // ─────────────────────────────────────────
    // Core expiry helper — is this key expired?
    // Returns true if the key has an expiry AND
    // that expiry time has already passed
    // ─────────────────────────────────────────
    private boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) {
            // No expiry set — never expires
            return false;
        }
        // Compare expiry time to current time
        return System.currentTimeMillis() > expiryTime;
    }

    // ─────────────────────────────────────────
    // Delete a key from both maps
    // Used internally when expiry is detected
    // ─────────────────────────────────────────
    private void deleteKey(String key) {
        data.remove(key);
        expiry.remove(key);
    }

    // ─────────────────────────────────────────
    // SET key value
    // Optional: EX seconds or PX milliseconds
    // expiryMs = -1 means no expiry
    // ─────────────────────────────────────────
    public String set(String key, String value, long expiryMs) {
        data.put(key, value);

        if (expiryMs > 0) {
            // Store the absolute expiry time
            // current time + how long it should live
            long expiryTime = System.currentTimeMillis() + expiryMs;
            expiry.put(key, expiryTime);
        } else {
            // No expiry — remove any existing expiry for this key
            // Important: SET without EX clears any previous expiry
            expiry.remove(key);
        }

        return "OK";
    }

    // Overload — SET without expiry (backward compatible)
    public String set(String key, String value) {
        return set(key, value, -1);
    }

    // ─────────────────────────────────────────
    // GET key
    // Lazy expiry check here — before returning
    // ─────────────────────────────────────────
    public String get(String key) {
        // Lazy expiry — check on every access
        if (isExpired(key)) {
            deleteKey(key);
            return null;
        }
        return data.get(key);
    }

    // ─────────────────────────────────────────
    // DEL key [key ...]
    // ─────────────────────────────────────────
    public int del(List<String> keys) {
        int deleted = 0;
        for (String key : keys) {
            if (data.remove(key) != null) {
                expiry.remove(key);
                deleted++;
            }
        }
        return deleted;
    }

    // ─────────────────────────────────────────
    // EXISTS key
    // Must check expiry before answering
    // ─────────────────────────────────────────
    public int exists(String key) {
        if (isExpired(key)) {
            deleteKey(key);
            return 0;
        }
        return data.containsKey(key) ? 1 : 0;
    }

    // ─────────────────────────────────────────
    // EXPIRE key seconds
    // Sets expiry on an existing key
    // Returns 1 if key exists, 0 if not
    // ─────────────────────────────────────────
    public int expire(String key, long seconds) {
        // Cannot expire a key that does not exist
        // or is already expired
        if (!data.containsKey(key) || isExpired(key)) {
            return 0;
        }
        long expiryTime = System.currentTimeMillis()
            + (seconds * 1000);
        expiry.put(key, expiryTime);
        return 1;
    }

    // ─────────────────────────────────────────
    // TTL key
    // Returns seconds remaining until expiry
    // -1 = no expiry set
    // -2 = key does not exist or already expired
    // ─────────────────────────────────────────
    public long ttl(String key) {
        if (!data.containsKey(key)) {
            return -2; // key does not exist
        }
        if (isExpired(key)) {
            deleteKey(key);
            return -2; // expired
        }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) {
            return -1; // exists but no expiry
        }
        // Convert milliseconds remaining to seconds
        // Math.max ensures we never return 0 or negative
        long remaining = expiryTime - System.currentTimeMillis();
        return Math.max(1, remaining / 1000);
    }

    // ─────────────────────────────────────────
    // PTTL key
    // Same as TTL but returns milliseconds
    // ─────────────────────────────────────────
    public long pttl(String key) {
        if (!data.containsKey(key)) {
            return -2;
        }
        if (isExpired(key)) {
            deleteKey(key);
            return -2;
        }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) {
            return -1;
        }
        long remaining = expiryTime - System.currentTimeMillis();
        return Math.max(1, remaining);
    }

    // ─────────────────────────────────────────
    // PERSIST key
    // Removes expiry from a key
    // Returns 1 if expiry was removed, 0 if no expiry existed
    // ─────────────────────────────────────────
    public int persist(String key) {
        if (!data.containsKey(key)) {
            return 0;
        }
        // remove() returns null if key was not in expiry map
        Long removed = expiry.remove(key);
        return removed != null ? 1 : 0;
    }

    // ─────────────────────────────────────────
    // INCR key
    // ─────────────────────────────────────────
    public long incr(String key) {
        if (isExpired(key)) {
            deleteKey(key);
        }
        String current = data.getOrDefault(key, "0");
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                "value is not an integer or out of range"
            );
        }
        value++;
        data.put(key, String.valueOf(value));
        return value;
    }

    // ─────────────────────────────────────────
    // DECR key
    // ─────────────────────────────────────────
    public long decr(String key) {
        if (isExpired(key)) {
            deleteKey(key);
        }
        String current = data.getOrDefault(key, "0");
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                "value is not an integer or out of range"
            );
        }
        value--;
        data.put(key, String.valueOf(value));
        return value;
    }

    // ─────────────────────────────────────────
    // MSET key value [key value ...]
    // ─────────────────────────────────────────
    public String mset(List<String> args) {
        for (int i = 0; i < args.size(); i += 2) {
            data.put(args.get(i), args.get(i + 1));
            // MSET clears expiry for all keys it sets
            expiry.remove(args.get(i));
        }
        return "OK";
    }

    // ─────────────────────────────────────────
    // MGET key [key ...]
    // ─────────────────────────────────────────
    public List<String> mget(List<String> keys) {
        List<String> results = new ArrayList<>();
        for (String key : keys) {
            if (isExpired(key)) {
                deleteKey(key);
                results.add(null);
            } else {
                results.add(data.get(key));
            }
        }
        return results;
    }

    // ─────────────────────────────────────────
    // KEYS *
    // Only returns non-expired keys
    // ─────────────────────────────────────────
    public List<String> keys() {
        List<String> result = new ArrayList<>();
        for (String key : data.keySet()) {
            if (!isExpired(key)) {
                result.add(key);
            }
        }
        return result;
    }

    // ─────────────────────────────────────────
    // DBSIZE — count non-expired keys
    // ─────────────────────────────────────────
    public int dbsize() {
        // Use keys() so expired ones are not counted
        return keys().size();
    }

    // ─────────────────────────────────────────
    // FLUSHALL
    // ─────────────────────────────────────────
    public String flushall() {
        data.clear();
        expiry.clear();
        return "OK";
    }

    // ─────────────────────────────────────────
    // APPEND key value
    // ─────────────────────────────────────────
    public int append(String key, String value) {
        if (isExpired(key)) {
            deleteKey(key);
        }
        String current = data.getOrDefault(key, "");
        String newValue = current + value;
        data.put(key, newValue);
        return newValue.length();
    }

    // ─────────────────────────────────────────
    // Active expiry — background thread
    // Wakes every 100ms, samples expired keys,
    // deletes them to free memory
    // ─────────────────────────────────────────
    private void startActiveExpiry() {

        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    // Sleep 100 milliseconds between each scan
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }

                // Only scan if there are keys with expiry
                if (expiry.isEmpty()) {
                    continue;
                }

                // Get all keys that have an expiry set
                List<String> expiryKeys =
                    new ArrayList<>(expiry.keySet());

                int deleted = 0;

                // Check each key
                for (String key : expiryKeys) {
                    if (isExpired(key)) {
                        deleteKey(key);
                        deleted++;
                    }
                }

                // Only log if something was actually deleted
                if (deleted > 0) {
                    System.out.println("[Expiry] Cleaned up "
                        + deleted + " expired keys");
                }
            }
        });

        // Daemon thread = dies automatically when main program exits
        // Without this, the cleanup thread would keep the JVM alive
        // even after you press Ctrl+C
        cleanupThread.setDaemon(true);
        cleanupThread.setName("expiry-cleanup");
        cleanupThread.start();

        System.out.println("Active expiry thread started");
    }
}