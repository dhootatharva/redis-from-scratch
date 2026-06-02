package redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Store {

    // String storage — key → string value
    private final ConcurrentHashMap<String, String> data
        = new ConcurrentHashMap<>();

    // List storage — key → list of strings
    // CopyOnWriteArrayList is thread-safe for reads
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>>
        lists = new ConcurrentHashMap<>();

    // Expiry storage — key → expiry time in milliseconds
    // Shared across both strings and lists
    private final ConcurrentHashMap<String, Long> expiry
        = new ConcurrentHashMap<>();

    public Store() {
        startActiveExpiry();
    }

    // ─────────────────────────────────────────
    // Type checking helpers
    // ─────────────────────────────────────────

    // Is this key a string type?
    private boolean isString(String key) {
        return data.containsKey(key);
    }

    // Is this key a list type?
    private boolean isList(String key) {
        return lists.containsKey(key);
    }

    // Throws if key exists but is a string, not a list
    private void assertList(String key) {
        if (isString(key)) {
            throw new RuntimeException(
                "WRONGTYPE Operation against a key holding " +
                "the wrong kind of value"
            );
        }
    }

    // Throws if key exists but is a list, not a string
    private void assertString(String key) {
        if (isList(key)) {
            throw new RuntimeException(
                "WRONGTYPE Operation against a key holding " +
                "the wrong kind of value"
            );
        }
    }

    // ─────────────────────────────────────────
    // Expiry helpers — same as before
    // ─────────────────────────────────────────

    private boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return false;
        return System.currentTimeMillis() > expiryTime;
    }

    private void deleteKey(String key) {
        data.remove(key);
        lists.remove(key);
        expiry.remove(key);
    }

    // ─────────────────────────────────────────
    // STRING COMMANDS
    // ─────────────────────────────────────────

    public String set(String key, String value, long expiryMs) {
        // If key was a list before, clear it
        lists.remove(key);
        data.put(key, value);
        if (expiryMs > 0) {
            expiry.put(key,
                System.currentTimeMillis() + expiryMs);
        } else {
            expiry.remove(key);
        }
        return "OK";
    }

    public String set(String key, String value) {
        return set(key, value, -1);
    }

    public String get(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertString(key);
        return data.get(key);
    }

    public int del(List<String> keys) {
        int deleted = 0;
        for (String key : keys) {
            boolean existed = data.containsKey(key)
                || lists.containsKey(key);
            if (existed) {
                deleteKey(key);
                deleted++;
            }
        }
        return deleted;
    }

    public int exists(String key) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        return (data.containsKey(key)
            || lists.containsKey(key)) ? 1 : 0;
    }

    public int expire(String key, long seconds) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key);
        if (!keyExists || isExpired(key)) return 0;
        expiry.put(key,
            System.currentTimeMillis() + (seconds * 1000));
        return 1;
    }

    public long ttl(String key) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key);
        if (!keyExists) return -2;
        if (isExpired(key)) { deleteKey(key); return -2; }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return -1;
        return Math.max(1,
            (expiryTime - System.currentTimeMillis()) / 1000);
    }

    public long pttl(String key) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key);
        if (!keyExists) return -2;
        if (isExpired(key)) { deleteKey(key); return -2; }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return -1;
        return Math.max(1,
            expiryTime - System.currentTimeMillis());
    }

    public int persist(String key) {
        if (!data.containsKey(key)
                && !lists.containsKey(key)) return 0;
        Long removed = expiry.remove(key);
        return removed != null ? 1 : 0;
    }

    public long incr(String key) {
        if (isExpired(key)) deleteKey(key);
        assertString(key);
        String current = data.getOrDefault(key, "0");
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                "value is not an integer or out of range");
        }
        value++;
        data.put(key, String.valueOf(value));
        return value;
    }

    public long decr(String key) {
        if (isExpired(key)) deleteKey(key);
        assertString(key);
        String current = data.getOrDefault(key, "0");
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                "value is not an integer or out of range");
        }
        value--;
        data.put(key, String.valueOf(value));
        return value;
    }

    public String mset(List<String> args) {
        for (int i = 0; i < args.size(); i += 2) {
            String key = args.get(i);
            lists.remove(key);
            data.put(key, args.get(i + 1));
            expiry.remove(key);
        }
        return "OK";
    }

    public List<String> mget(List<String> keys) {
        List<String> results = new ArrayList<>();
        for (String key : keys) {
            if (isExpired(key)) {
                deleteKey(key);
                results.add(null);
            } else if (isList(key)) {
                // MGET on a list key returns null
                results.add(null);
            } else {
                results.add(data.get(key));
            }
        }
        return results;
    }

    public List<String> keys() {
        List<String> result = new ArrayList<>();
        for (String key : data.keySet()) {
            if (!isExpired(key)) result.add(key);
        }
        for (String key : lists.keySet()) {
            if (!isExpired(key)) result.add(key);
        }
        return result;
    }

    public int dbsize() {
        return keys().size();
    }

    public String flushall() {
        data.clear();
        lists.clear();
        expiry.clear();
        return "OK";
    }

    public int append(String key, String value) {
        if (isExpired(key)) deleteKey(key);
        assertString(key);
        String current = data.getOrDefault(key, "");
        String newValue = current + value;
        data.put(key, newValue);
        return newValue.length();
    }

    // ─────────────────────────────────────────
    // LIST COMMANDS
    // ─────────────────────────────────────────

    // ─────────────────────────────────────────
    // LPUSH key value [value ...]
    // Adds values to the LEFT (front) of the list
    // Creates the list if it does not exist
    // Returns new length of the list
    // ─────────────────────────────────────────
    public long lpush(String key, List<String> values) {
        if (isExpired(key)) deleteKey(key);
        assertList(key);

        // Get existing list or create a new one
        CopyOnWriteArrayList<String> list =
            lists.computeIfAbsent(key,
                k -> new CopyOnWriteArrayList<>());

        // Add each value to the front
        // We add in reverse so that LPUSH a b c
        // results in [c, b, a] — same as real Redis
        List<String> reversed = new ArrayList<>(values);
        Collections.reverse(reversed);
        for (String value : reversed) {
            list.add(0, value);
        }

        return list.size();
    }

    // ─────────────────────────────────────────
    // RPUSH key value [value ...]
    // Adds values to the RIGHT (back) of the list
    // Returns new length of the list
    // ─────────────────────────────────────────
    public long rpush(String key, List<String> values) {
        if (isExpired(key)) deleteKey(key);
        assertList(key);

        CopyOnWriteArrayList<String> list =
            lists.computeIfAbsent(key,
                k -> new CopyOnWriteArrayList<>());

        // Add each value to the back — order preserved
        list.addAll(values);

        return list.size();
    }

    // ─────────────────────────────────────────
    // LPOP key
    // Removes and returns the leftmost element
    // Returns null if list is empty or does not exist
    // ─────────────────────────────────────────
    public String lpop(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);

        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;

        String value = list.remove(0);

        // If list is now empty, delete the key entirely
        // Real Redis does this automatically
        if (list.isEmpty()) deleteKey(key);

        return value;
    }

    // ─────────────────────────────────────────
    // RPOP key
    // Removes and returns the rightmost element
    // ─────────────────────────────────────────
    public String rpop(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);

        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;

        String value = list.remove(list.size() - 1);

        if (list.isEmpty()) deleteKey(key);

        return value;
    }

    // ─────────────────────────────────────────
    // LLEN key
    // Returns number of elements in the list
    // Returns 0 if key does not exist
    // ─────────────────────────────────────────
    public long llen(String key) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        return list == null ? 0 : list.size();
    }

    // ─────────────────────────────────────────
    // LRANGE key start stop
    // Returns elements from start to stop index
    // Supports negative indexes
    // ─────────────────────────────────────────
    public List<String> lrange(String key, int start, int stop) {
        if (isExpired(key)) { deleteKey(key); return new ArrayList<>(); }
        assertList(key);

        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return new ArrayList<>();

        int size = list.size();

        // Convert negative indexes to positive
        // -1 means last element = size - 1
        // -2 means second to last = size - 2
        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;

        // Clamp stop to valid range
        stop = Math.min(stop, size - 1);

        // Invalid range — return empty list
        if (start > stop) return new ArrayList<>();

        // subList is exclusive on the end so we add 1
        return new ArrayList<>(list.subList(start, stop + 1));
    }

    // ─────────────────────────────────────────
    // LINDEX key index
    // Returns element at given index
    // Returns null if out of range
    // ─────────────────────────────────────────
    public String lindex(String key, int index) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);

        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null) return null;

        int size = list.size();

        // Convert negative index
        if (index < 0) index = size + index;

        // Out of range
        if (index < 0 || index >= size) return null;

        return list.get(index);
    }

    // ─────────────────────────────────────────
    // Active expiry background thread
    // ─────────────────────────────────────────
    private void startActiveExpiry() {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
                if (expiry.isEmpty()) continue;
                List<String> expiryKeys =
                    new ArrayList<>(expiry.keySet());
                int deleted = 0;
                for (String key : expiryKeys) {
                    if (isExpired(key)) {
                        deleteKey(key);
                        deleted++;
                    }
                }
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
        System.out.println("Active expiry thread started");
    }
}