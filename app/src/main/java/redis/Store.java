package redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Store {

    // Package-private so Persistence can access them
    final ConcurrentHashMap<String, String> data
        = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, CopyOnWriteArrayList<String>>
        lists = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String,
        ConcurrentHashMap<String, String>> hashes
        = new ConcurrentHashMap<>();

    final ConcurrentHashMap<String, Long> expiry
        = new ConcurrentHashMap<>();

    public Store() {
        // Load persisted data from disk on startup
        Persistence.load(data, lists, hashes, expiry);
        startActiveExpiry();
        startBackgroundSave();
    }

    // ─────────────────────────────────────────
    // Save snapshot to disk
    // Called by Server on shutdown
    // ─────────────────────────────────────────
    public void save() {
        Persistence.save(data, lists, hashes, expiry);
    }

    // ─────────────────────────────────────────
    // Background save thread
    // Saves to disk every 60 seconds automatically
    // This ensures data survives even if the server
    // is killed without a clean shutdown
    // ─────────────────────────────────────────
    private void startBackgroundSave() {
        Thread saveThread = new Thread(() -> {
            while (true) {
                try {
                    // Wait 60 seconds between saves
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    break;
                }
                Persistence.save(data, lists, hashes, expiry);
            }
        });
        saveThread.setDaemon(true);
        saveThread.setName("background-save");
        saveThread.start();
        System.out.println("Background save thread started" +
            " (saves every 60 seconds)");
    }

    private boolean isString(String key) {
        return data.containsKey(key);
    }

    private boolean isList(String key) {
        return lists.containsKey(key);
    }

    private boolean isHash(String key) {
        return hashes.containsKey(key);
    }

    private void assertList(String key) {
        if (isString(key) || isHash(key)) {
            throw new RuntimeException(
                "WRONGTYPE Operation against a key holding " +
                "the wrong kind of value"
            );
        }
    }

    private void assertString(String key) {
        if (isList(key) || isHash(key)) {
            throw new RuntimeException(
                "WRONGTYPE Operation against a key holding " +
                "the wrong kind of value"
            );
        }
    }

    private void assertHash(String key) {
        if (isString(key) || isList(key)) {
            throw new RuntimeException(
                "WRONGTYPE Operation against a key holding " +
                "the wrong kind of value"
            );
        }
    }

    private boolean isExpired(String key) {
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return false;
        return System.currentTimeMillis() > expiryTime;
    }

    private void deleteKey(String key) {
        data.remove(key);
        lists.remove(key);
        hashes.remove(key);
        expiry.remove(key);
    }

    public String set(String key, String value, long expiryMs) {
        lists.remove(key);
        hashes.remove(key);
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
                || lists.containsKey(key)
                || hashes.containsKey(key);
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
            || lists.containsKey(key)
            || hashes.containsKey(key)) ? 1 : 0;
    }

    public int expire(String key, long seconds) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key)
            || hashes.containsKey(key);
        if (!keyExists || isExpired(key)) return 0;
        expiry.put(key,
            System.currentTimeMillis() + (seconds * 1000));
        return 1;
    }

    public long ttl(String key) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key)
            || hashes.containsKey(key);
        if (!keyExists) return -2;
        if (isExpired(key)) { deleteKey(key); return -2; }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return -1;
        return Math.max(1,
            (expiryTime - System.currentTimeMillis()) / 1000);
    }

    public long pttl(String key) {
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key)
            || hashes.containsKey(key);
        if (!keyExists) return -2;
        if (isExpired(key)) { deleteKey(key); return -2; }
        Long expiryTime = expiry.get(key);
        if (expiryTime == null) return -1;
        return Math.max(1,
            expiryTime - System.currentTimeMillis());
    }

    public int persist(String key) {
        if (!data.containsKey(key)
                && !lists.containsKey(key)
                && !hashes.containsKey(key)) return 0;
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
            hashes.remove(key);
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
            } else if (isList(key) || isHash(key)) {
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
        for (String key : hashes.keySet()) {
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
        hashes.clear();
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

    public int strlen(String key) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertString(key);
        String value = data.get(key);
        return value == null ? 0 : value.length();
    }

    public long incrby(String key, long amount) {
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
        value += amount;
        data.put(key, String.valueOf(value));
        return value;
    }

    public long decrby(String key, long amount) {
        return incrby(key, -amount);
    }

    public String getdel(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertString(key);
        String value = data.remove(key);
        if (value != null) expiry.remove(key);
        return value;
    }

    public int setnx(String key, String value) {
        if (isExpired(key)) deleteKey(key);
        String existing = data.putIfAbsent(key, value);
        return existing == null ? 1 : 0;
    }

    public String type(String key) {
        if (isExpired(key)) { deleteKey(key); return "none"; }
        if (data.containsKey(key)) return "string";
        if (lists.containsKey(key)) return "list";
        if (hashes.containsKey(key)) return "hash";
        return "none";
    }

    public String rename(String key, String newKey) {
        if (isExpired(key)) deleteKey(key);
        boolean keyExists = data.containsKey(key)
            || lists.containsKey(key)
            || hashes.containsKey(key);
        if (!keyExists) {
            throw new RuntimeException("no such key");
        }
        if (data.containsKey(key)) {
            String value = data.get(key);
            deleteKey(newKey);
            data.put(newKey, value);
            Long expiryTime = expiry.get(key);
            if (expiryTime != null) {
                expiry.put(newKey, expiryTime);
            }
            deleteKey(key);
        } else if (lists.containsKey(key)) {
            CopyOnWriteArrayList<String> list = lists.get(key);
            deleteKey(newKey);
            lists.put(newKey,
                new CopyOnWriteArrayList<>(list));
            Long expiryTime = expiry.get(key);
            if (expiryTime != null) {
                expiry.put(newKey, expiryTime);
            }
            deleteKey(key);
        } else if (hashes.containsKey(key)) {
            ConcurrentHashMap<String, String> hash =
                hashes.get(key);
            deleteKey(newKey);
            hashes.put(newKey,
                new ConcurrentHashMap<>(hash));
            Long expiryTime = expiry.get(key);
            if (expiryTime != null) {
                expiry.put(newKey, expiryTime);
            }
            deleteKey(key);
        }
        return "OK";
    }

    // ─────────────────────────────────────────
    // LIST COMMANDS
    // ─────────────────────────────────────────

    public long lpush(String key, List<String> values) {
        if (isExpired(key)) deleteKey(key);
        assertList(key);
        CopyOnWriteArrayList<String> list =
            lists.computeIfAbsent(key,
                k -> new CopyOnWriteArrayList<>());
        List<String> reversed = new ArrayList<>(values);
        Collections.reverse(reversed);
        for (String value : reversed) {
            list.add(0, value);
        }
        return list.size();
    }

    public long rpush(String key, List<String> values) {
        if (isExpired(key)) deleteKey(key);
        assertList(key);
        CopyOnWriteArrayList<String> list =
            lists.computeIfAbsent(key,
                k -> new CopyOnWriteArrayList<>());
        list.addAll(values);
        return list.size();
    }

    public String lpop(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;
        String value = list.remove(0);
        if (list.isEmpty()) deleteKey(key);
        return value;
    }

    public String rpop(String key) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;
        String value = list.remove(list.size() - 1);
        if (list.isEmpty()) deleteKey(key);
        return value;
    }

    public long llen(String key) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        return list == null ? 0 : list.size();
    }

    public List<String> lrange(String key, int start, int stop) {
        if (isExpired(key)) {
            deleteKey(key);
            return new ArrayList<>();
        }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return new ArrayList<>();
        int size = list.size();
        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;
        stop = Math.min(stop, size - 1);
        if (start > stop) return new ArrayList<>();
        return new ArrayList<>(list.subList(start, stop + 1));
    }

    public String lindex(String key, int index) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertList(key);
        CopyOnWriteArrayList<String> list = lists.get(key);
        if (list == null) return null;
        int size = list.size();
        if (index < 0) index = size + index;
        if (index < 0 || index >= size) return null;
        return list.get(index);
    }

    // ─────────────────────────────────────────
    // HASH COMMANDS
    // ─────────────────────────────────────────

    public int hset(String key, List<String> fieldValues) {
        if (isExpired(key)) deleteKey(key);
        assertHash(key);
        ConcurrentHashMap<String, String> hash =
            hashes.computeIfAbsent(key,
                k -> new ConcurrentHashMap<>());
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

    public String hget(String key, String field) {
        if (isExpired(key)) { deleteKey(key); return null; }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return null;
        return hash.get(field);
    }

    public List<String> hgetall(String key) {
        if (isExpired(key)) {
            deleteKey(key);
            return new ArrayList<>();
        }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return new ArrayList<>();
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, String> entry : hash.entrySet()) {
            result.add(entry.getKey());
            result.add(entry.getValue());
        }
        return result;
    }

    public int hdel(String key, List<String> fields) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return 0;
        int deleted = 0;
        for (String field : fields) {
            if (hash.remove(field) != null) {
                deleted++;
            }
        }
        if (hash.isEmpty()) deleteKey(key);
        return deleted;
    }

    public int hexists(String key, String field) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return 0;
        return hash.containsKey(field) ? 1 : 0;
    }

    public int hlen(String key) {
        if (isExpired(key)) { deleteKey(key); return 0; }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        return hash == null ? 0 : hash.size();
    }

    public List<String> hkeys(String key) {
        if (isExpired(key)) {
            deleteKey(key);
            return new ArrayList<>();
        }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return new ArrayList<>();
        return new ArrayList<>(hash.keySet());
    }

    public List<String> hvals(String key) {
        if (isExpired(key)) {
            deleteKey(key);
            return new ArrayList<>();
        }
        assertHash(key);
        ConcurrentHashMap<String, String> hash = hashes.get(key);
        if (hash == null) return new ArrayList<>();
        return new ArrayList<>(hash.values());
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