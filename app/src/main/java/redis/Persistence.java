package redis;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Persistence {

    // The file where we save the snapshot
    private static final String RDB_FILE = "dump.rdb";

    // Magic header — identifies this as our RDB file
    // Like a file signature so we know it is valid
    private static final String MAGIC = "REDIS0001";

    // Type bytes — one byte at the start of each entry
    // tells us what kind of data follows
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_LIST   = 2;
    private static final byte TYPE_HASH   = 3;

    // End of file marker
    private static final byte TYPE_EOF    = 0;

    // ─────────────────────────────────────────
    // SAVE — write everything to disk
    // Called when server shuts down (Ctrl+C)
    // or periodically in background
    // ─────────────────────────────────────────
    public static void save(
            ConcurrentHashMap<String, String> data,
            ConcurrentHashMap<String,
                CopyOnWriteArrayList<String>> lists,
            ConcurrentHashMap<String,
                ConcurrentHashMap<String, String>> hashes,
            ConcurrentHashMap<String, Long> expiry) {

        // Write to a temp file first
        // Only rename to dump.rdb when fully written
        // This prevents a corrupt file if we crash mid-write
        String tempFile = RDB_FILE + ".tmp";

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(tempFile)))) {

            // Write magic header
            out.writeUTF(MAGIC);

            // Current time — used to skip expired keys
            long now = System.currentTimeMillis();

            // ── Write all string keys ──
            for (Map.Entry<String, String> entry
                    : data.entrySet()) {

                String key = entry.getKey();
                Long expiryTime = expiry.get(key);

                // Skip keys that are already expired
                if (expiryTime != null && now > expiryTime) {
                    continue;
                }

                out.writeByte(TYPE_STRING);
                out.writeUTF(key);
                out.writeUTF(entry.getValue());

                // Write expiry — 0 means no expiry
                out.writeLong(expiryTime != null
                    ? expiryTime : 0L);
            }

            // ── Write all list keys ──
            for (Map.Entry<String,
                    CopyOnWriteArrayList<String>> entry
                    : lists.entrySet()) {

                String key = entry.getKey();
                Long expiryTime = expiry.get(key);

                if (expiryTime != null && now > expiryTime) {
                    continue;
                }

                CopyOnWriteArrayList<String> list =
                    entry.getValue();

                out.writeByte(TYPE_LIST);
                out.writeUTF(key);

                // Write number of elements
                out.writeInt(list.size());

                // Write each element
                for (String element : list) {
                    out.writeUTF(element);
                }

                out.writeLong(expiryTime != null
                    ? expiryTime : 0L);
            }

            // ── Write all hash keys ──
            for (Map.Entry<String,
                    ConcurrentHashMap<String, String>> entry
                    : hashes.entrySet()) {

                String key = entry.getKey();
                Long expiryTime = expiry.get(key);

                if (expiryTime != null && now > expiryTime) {
                    continue;
                }

                ConcurrentHashMap<String, String> hash =
                    entry.getValue();

                out.writeByte(TYPE_HASH);
                out.writeUTF(key);

                // Write number of fields
                out.writeInt(hash.size());

                // Write each field-value pair
                for (Map.Entry<String, String> field
                        : hash.entrySet()) {
                    out.writeUTF(field.getKey());
                    out.writeUTF(field.getValue());
                }

                out.writeLong(expiryTime != null
                    ? expiryTime : 0L);
            }

            // Write end of file marker
            out.writeByte(TYPE_EOF);

            System.out.println("[RDB] Saved "
                + data.size() + " strings, "
                + lists.size() + " lists, "
                + hashes.size() + " hashes to "
                + RDB_FILE);

        } catch (IOException e) {
            System.out.println("[RDB] Save failed: "
                + e.getMessage());
            return;
        }

        // Rename temp file to actual RDB file
        // This is atomic on most operating systems
        new File(tempFile).renameTo(new File(RDB_FILE));
    }


    
    // ─────────────────────────────────────────
    // LOAD — read everything from disk
    // Called when server starts up
    // ─────────────────────────────────────────
    public static void load(
            ConcurrentHashMap<String, String> data,
            ConcurrentHashMap<String,
                CopyOnWriteArrayList<String>> lists,
            ConcurrentHashMap<String,
                ConcurrentHashMap<String, String>> hashes,
            ConcurrentHashMap<String, Long> expiry) {

        File rdbFile = new File(RDB_FILE);

        // No file yet — first time running
        if (!rdbFile.exists()) {
            System.out.println(
                "[RDB] No dump.rdb found — starting fresh");
            return;
        }

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(
                    new FileInputStream(rdbFile)))) {

            // Read and verify magic header
            String magic = in.readUTF();
            if (!magic.equals(MAGIC)) {
                System.out.println(
                    "[RDB] Invalid file format — ignoring");
                return;
            }

            long now = System.currentTimeMillis();
            int loaded = 0;
            int skipped = 0;

            // Read entries until we hit EOF marker
            while (true) {
                byte type = in.readByte();

                // EOF marker — done reading
                if (type == TYPE_EOF) {
                    break;
                }

                if (type == TYPE_STRING) {
                    String key   = in.readUTF();
                    String value = in.readUTF();
                    long expiryTime = in.readLong();

                    // Skip if expired
                    if (expiryTime > 0 && now > expiryTime) {
                        skipped++;
                        continue;
                    }

                    data.put(key, value);
                    if (expiryTime > 0) {
                        expiry.put(key, expiryTime);
                    }
                    loaded++;

                } else if (type == TYPE_LIST) {
                    String key = in.readUTF();
                    int size = in.readInt();

                    CopyOnWriteArrayList<String> list =
                        new CopyOnWriteArrayList<>();

                    for (int i = 0; i < size; i++) {
                        list.add(in.readUTF());
                    }

                    long expiryTime = in.readLong();

                    if (expiryTime > 0 && now > expiryTime) {
                        skipped++;
                        continue;
                    }

                    lists.put(key, list);
                    if (expiryTime > 0) {
                        expiry.put(key, expiryTime);
                    }
                    loaded++;

                } else if (type == TYPE_HASH) {
                    String key = in.readUTF();
                    int size = in.readInt();

                    ConcurrentHashMap<String, String> hash =
                        new ConcurrentHashMap<>();

                    for (int i = 0; i < size; i++) {
                        String field = in.readUTF();
                        String value = in.readUTF();
                        hash.put(field, value);
                    }

                    long expiryTime = in.readLong();

                    if (expiryTime > 0 && now > expiryTime) {
                        skipped++;
                        continue;
                    }

                    hashes.put(key, hash);
                    if (expiryTime > 0) {
                        expiry.put(key, expiryTime);
                    }
                    loaded++;

                } else {
                    System.out.println(
                        "[RDB] Unknown type byte: " + type);
                    break;
                }
            }

            System.out.println("[RDB] Loaded " + loaded
                + " keys, skipped " + skipped
                + " expired keys from " + RDB_FILE);

        } catch (IOException e) {
            System.out.println(
                "[RDB] Load failed: " + e.getMessage());
        }
    }
}