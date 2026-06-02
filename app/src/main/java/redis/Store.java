package redis;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Store {

    // This is the actual storage — a thread-safe HashMap
    // Key is always a String (Redis keys are always strings)
    // Value is always a String (we handle only strings in Phase 3)
    private final ConcurrentHashMap<String, String> data
        = new ConcurrentHashMap<>();

    // ─────────────────────────────────────────
    // SET key value
    // Stores the value. Always succeeds.
    // Returns "OK"
    // ─────────────────────────────────────────
    public String set(String key, String value) {
        data.put(key, value);
        return "OK";
    }

    // ─────────────────────────────────────────
    // GET key
    // Returns the value, or null if key missing
    // ─────────────────────────────────────────
    public String get(String key) {
        return data.get(key);
    }

    // ─────────────────────────────────────────
    // DEL key [key ...]
    // Deletes one or more keys
    // Returns how many keys were actually deleted
    // ─────────────────────────────────────────
    public int del(List<String> keys) {
        int deleted = 0;
        for (String key : keys) {
            // remove() returns the old value if key existed
            // or null if it did not exist
            if (data.remove(key) != null) {
                deleted++;
            }
        }
        return deleted;
    }

    // ─────────────────────────────────────────
    // EXISTS key
    // Returns 1 if key exists, 0 if not
    // ─────────────────────────────────────────
    public int exists(String key) {
        return data.containsKey(key) ? 1 : 0;
    }

    // ─────────────────────────────────────────
    // INCR key
    // Adds 1 to the value stored at key
    // If key does not exist, starts from 0 then adds 1
    // Returns the new value as a long
    // Throws exception if value is not a number
    // ─────────────────────────────────────────
    public long incr(String key) {
        // getOrDefault returns 0 if key does not exist
        String current = data.getOrDefault(key, "0");

        // Try to parse as a number — throws if not numeric
        long value;
        try {
            value = Long.parseLong(current);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                "value is not an integer or out of range"
            );
        }

        // Increment and store back
        value++;
        data.put(key, String.valueOf(value));
        return value;
    }

    // ─────────────────────────────────────────
    // DECR key
    // Subtracts 1 from the value
    // Same logic as INCR
    // ─────────────────────────────────────────
    public long decr(String key) {
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
    // Sets multiple keys at once
    // Returns "OK"
    // ─────────────────────────────────────────
    public String mset(List<String> args) {
        // args comes in pairs: [key1, val1, key2, val2, ...]
        for (int i = 0; i < args.size(); i += 2) {
            data.put(args.get(i), args.get(i + 1));
        }
        return "OK";
    }

    // ─────────────────────────────────────────
    // MGET key [key ...]
    // Returns a list of values
    // null for any key that does not exist
    // ─────────────────────────────────────────
    public List<String> mget(List<String> keys) {
        List<String> results = new ArrayList<>();
        for (String key : keys) {
            // Returns null if key missing — that is correct
            results.add(data.get(key));
        }
        return results;
    }

    // ─────────────────────────────────────────
    // KEYS pattern
    // We support only KEYS * for now (return all keys)
    // ─────────────────────────────────────────
    public List<String> keys() {
        return new ArrayList<>(data.keySet());
    }

    // ─────────────────────────────────────────
    // DBSIZE
    // Returns total number of keys
    // ─────────────────────────────────────────
    public int dbsize() {
        return data.size();
    }

    // ─────────────────────────────────────────
    // FLUSHALL
    // Deletes everything
    // Returns "OK"
    // ─────────────────────────────────────────
    public String flushall() {
        data.clear();
        return "OK";
    }

    // ─────────────────────────────────────────
    // APPEND key value
    // Appends value to existing string
    // If key does not exist, creates it
    // Returns new length of the string
    // ─────────────────────────────────────────
    public int append(String key, String value) {
        // getOrDefault gives "" if key missing
        String current = data.getOrDefault(key, "");
        String newValue = current + value;
        data.put(key, newValue);
        return newValue.length();
    }
}