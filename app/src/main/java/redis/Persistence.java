package redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import redis.Store.Entry;
import redis.Store.Type;

public class Persistence {

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
    // SAVE — write a snapshot to disk
    //
    // 'snapshot' is a private copy of the keyspace made by the Store
    // under its lock. Nothing else can change it, so this method can
    // take as long as the disk needs without blocking any client and
    // without seeing a half-finished command.
    // 'now' is the time the copy was taken, used to skip expired keys.
    // ─────────────────────────────────────────
    // synchronized: the 60 s background save, SAVE, BGSAVE and the
    // shutdown hook can all fire at once and share the same temp file
    public static synchronized void save(
            Path file,
            long now,
            Map<String, Entry> snapshot) {

        // Write to a temp file first
        // Only rename to dump.rdb when fully written
        // This prevents a corrupt file if we crash mid-write
        Path temp = file.resolveSibling(
            file.getFileName() + ".tmp");

        int strings = 0;
        int lists = 0;
        int hashes = 0;

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(temp.toFile())))) {

            // Write magic header
            out.writeUTF(MAGIC);

            for (Map.Entry<String, Entry> item : snapshot.entrySet()) {

                String key = item.getKey();
                Entry entry = item.getValue();

                // Skip keys that are already expired
                if (entry.expireAt != 0 && now > entry.expireAt) {
                    continue;
                }

                switch (entry.type) {
                    case STRING:
                        out.writeByte(TYPE_STRING);
                        out.writeUTF(key);
                        out.writeUTF(entry.string());
                        strings++;
                        break;

                    case LIST:
                        List<String> list = entry.list();
                        out.writeByte(TYPE_LIST);
                        out.writeUTF(key);
                        // Number of elements, then each element
                        out.writeInt(list.size());
                        for (String element : list) {
                            out.writeUTF(element);
                        }
                        lists++;
                        break;

                    case HASH:
                        Map<String, String> hash = entry.hash();
                        out.writeByte(TYPE_HASH);
                        out.writeUTF(key);
                        // Number of fields, then each field-value pair
                        out.writeInt(hash.size());
                        for (Map.Entry<String, String> field
                                : hash.entrySet()) {
                            out.writeUTF(field.getKey());
                            out.writeUTF(field.getValue());
                        }
                        hashes++;
                        break;
                }

                // Every entry ends with its deadline — 0 means no expiry
                out.writeLong(entry.expireAt);
            }

            // Write end of file marker
            out.writeByte(TYPE_EOF);

            System.out.println("[RDB] Saved "
                + strings + " strings, "
                + lists + " lists, "
                + hashes + " hashes to "
                + file.getFileName());

        } catch (IOException e) {
            System.out.println("[RDB] Save failed: "
                + e.getMessage());
            return;
        }

        // Replace dump.rdb with the finished temp file.
        // File.renameTo fails silently on Windows when the target
        // already exists, so use Files.move with REPLACE_EXISTING
        // (and an atomic move where the filesystem supports it).
        try {
            Files.move(temp, file,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(temp, file,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException ex) {
                System.out.println("[RDB] Rename failed: "
                    + ex.getMessage());
            }
        } catch (IOException e) {
            System.out.println("[RDB] Rename failed: "
                + e.getMessage());
        }
    }

    // ─────────────────────────────────────────
    // LOAD — read a snapshot from disk into 'target'
    // Called when server starts up
    // ─────────────────────────────────────────
    public static void load(
            Path file,
            long now,
            Map<String, Entry> target) {

        File rdbFile = file.toFile();

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

            int loaded = 0;
            int skipped = 0;

            // Read entries until we hit EOF marker
            while (true) {
                byte typeByte = in.readByte();

                // EOF marker — done reading
                if (typeByte == TYPE_EOF) {
                    break;
                }

                String key;
                Type type;
                Object value;

                if (typeByte == TYPE_STRING) {
                    key = in.readUTF();
                    type = Type.STRING;
                    value = in.readUTF();

                } else if (typeByte == TYPE_LIST) {
                    key = in.readUTF();
                    type = Type.LIST;
                    int size = in.readInt();
                    List<String> list = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        list.add(in.readUTF());
                    }
                    value = list;

                } else if (typeByte == TYPE_HASH) {
                    key = in.readUTF();
                    type = Type.HASH;
                    int size = in.readInt();
                    Map<String, String> hash = new LinkedHashMap<>();
                    for (int i = 0; i < size; i++) {
                        String field = in.readUTF();
                        hash.put(field, in.readUTF());
                    }
                    value = hash;

                } else {
                    System.out.println(
                        "[RDB] Unknown type byte: " + typeByte);
                    break;
                }

                long expireAt = in.readLong();

                // Skip keys whose deadline passed while we were down
                if (expireAt > 0 && now > expireAt) {
                    skipped++;
                    continue;
                }

                target.put(key, new Entry(type, value, expireAt));
                loaded++;
            }

            System.out.println("[RDB] Loaded " + loaded
                + " keys, skipped " + skipped
                + " expired keys from " + file.getFileName());

        } catch (IOException e) {
            System.out.println(
                "[RDB] Load failed: " + e.getMessage());
        }
    }
}
