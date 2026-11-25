package redis;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import redis.Store.Entry;
import redis.Store.Type;

// Reads and writes the snapshot file.
//
// File layout (version 3):
//
//   header     "REDIS0003"
//   entries    one per key:
//                type byte   1 = string, 2 = list, 3 = hash
//                key         string
//                value       string | count + strings | count + pairs
//                deadline    8 bytes, 0 = never expires
//   end marker one 0 byte
//   checksum   8 bytes, CRC-32 of everything before it
//
// A string is a 4-byte length followed by that many raw bytes, so any
// length up to the protocol's 512 MB limit and any byte values work.
//
// A save gives four guarantees:
//   consistent  the Store hands over a copy made at one instant
//   atomic      written to a temp file, then renamed over the old one;
//               a reader sees the old snapshot or the new, never a mix
//   durable     flushed to the disk itself (fsync) before the rename
//   verifiable  the checksum exposes any later corruption
public class Persistence {

    private static final String MAGIC = "REDIS0003";

    // Older formats, still readable:
    //   2: strings written with writeUTF (64 KB limit), no checksum
    //   1: as 2, but holding decoded text instead of binary strings
    private static final String MAGIC_V2 = "REDIS0002";
    private static final String MAGIC_V1 = "REDIS0001";

    // Type bytes — one byte at the start of each entry
    // tells us what kind of data follows
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_LIST   = 2;
    private static final byte TYPE_HASH   = 3;

    // End of file marker
    private static final byte TYPE_EOF    = 0;

    // ─────────────────────────────────────────
    // SAVE
    //
    // 'snapshot' is a private copy of the keyspace made by the Store
    // under its lock, and 'now' is when it was taken.
    // Throws IOException if the snapshot could not be written; the
    // previous snapshot file is then left exactly as it was.
    // ─────────────────────────────────────────
    // synchronized: the 60 s background save, SAVE, BGSAVE and the
    // shutdown hook can all fire at once and share the same temp file
    public static synchronized void save(
            Path file,
            long now,
            Map<String, Entry> snapshot) throws IOException {

        Path temp = file.resolveSibling(
            file.getFileName() + ".tmp");

        int strings = 0;
        int lists = 0;
        int hashes = 0;

        // Every byte written through 'out' also updates the checksum
        CRC32 crc = new CRC32();

        try (FileOutputStream fileOut =
                 new FileOutputStream(temp.toFile());
             DataOutputStream out = new DataOutputStream(
                 new CheckedOutputStream(
                     new BufferedOutputStream(fileOut), crc))) {

            out.writeUTF(MAGIC);

            for (Map.Entry<String, Entry> item : snapshot.entrySet()) {

                Entry entry = item.getValue();

                // Skip keys that are already expired
                if (entry.expireAt != 0 && now > entry.expireAt) {
                    continue;
                }

                switch (entry.type) {
                    case STRING:
                        out.writeByte(TYPE_STRING);
                        writeString(out, item.getKey());
                        writeString(out, entry.string());
                        strings++;
                        break;

                    case LIST:
                        out.writeByte(TYPE_LIST);
                        writeString(out, item.getKey());
                        out.writeInt(entry.list().size());
                        for (String element : entry.list()) {
                            writeString(out, element);
                        }
                        lists++;
                        break;

                    case HASH:
                        out.writeByte(TYPE_HASH);
                        writeString(out, item.getKey());
                        out.writeInt(entry.hash().size());
                        for (Map.Entry<String, String> field
                                : entry.hash().entrySet()) {
                            writeString(out, field.getKey());
                            writeString(out, field.getValue());
                        }
                        hashes++;
                        break;
                }

                // Every entry ends with its deadline — 0 means no expiry
                out.writeLong(entry.expireAt);
            }

            out.writeByte(TYPE_EOF);

            // The checksum covers everything written so far
            out.writeLong(crc.getValue());

            // flush() hands the bytes to the operating system, which
            // may keep them in memory for a while. sync() makes it
            // write them to the disk itself and waits until it has.
            // Without it, a power cut after the rename could leave a
            // new file name pointing at data that never arrived.
            out.flush();
            fileOut.getFD().sync();

        } catch (IOException e) {
            // Remove our half-written temp file, but only if it is a
            // file: never delete something this method did not create
            if (Files.isRegularFile(temp)) {
                Files.deleteIfExists(temp);
            }
            throw e;
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
            Files.move(temp, file,
                StandardCopyOption.REPLACE_EXISTING);
        }
        syncDirectory(file);

        System.out.println("[RDB] Saved "
            + strings + " strings, "
            + lists + " lists, "
            + hashes + " hashes to "
            + file.getFileName());
    }

    private static void writeString(DataOutputStream out, String value)
            throws IOException {
        byte[] bytes = Binary.encode(value);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    // The rename changed the directory, which is also data on disk.
    // Syncing the directory makes the rename itself durable. Linux
    // supports this; Windows does not allow opening a directory this
    // way, so there it is skipped.
    private static void syncDirectory(Path file) {
        Path directory = file.toAbsolutePath().getParent();
        if (directory == null) return;
        try (FileChannel channel = FileChannel.open(
                directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // not supported on this platform
        }
    }

    // ─────────────────────────────────────────
    // LOAD
    //
    // Returns the keys in the snapshot (an empty map if there is no
    // snapshot file). Loading is all or nothing: if the file is
    // damaged in any way, CorruptSnapshotException is thrown and
    // nothing from it is used.
    // ─────────────────────────────────────────
    public static Map<String, Entry> load(Path file, long now) {

        Map<String, Entry> loaded = new HashMap<>();

        // No file yet — first time running
        if (!Files.exists(file)) {
            System.out.println(
                "[RDB] No dump.rdb found — starting fresh");
            return loaded;
        }

        int skipped = 0;
        CRC32 crc = new CRC32();

        try (DataInputStream in = new DataInputStream(
                new CheckedInputStream(
                    new BufferedInputStream(
                        new FileInputStream(file.toFile())), crc))) {

            int version = versionOf(in.readUTF());

            // Read entries until we hit EOF marker
            while (true) {
                byte typeByte = in.readByte();

                if (typeByte == TYPE_EOF) {
                    break;
                }

                String key = readString(in, version);
                Type type;
                Object value;

                if (typeByte == TYPE_STRING) {
                    type = Type.STRING;
                    value = readString(in, version);

                } else if (typeByte == TYPE_LIST) {
                    type = Type.LIST;
                    int size = readCount(in);
                    List<String> list = new ArrayList<>();
                    for (int i = 0; i < size; i++) {
                        list.add(readString(in, version));
                    }
                    value = list;

                } else if (typeByte == TYPE_HASH) {
                    type = Type.HASH;
                    int size = readCount(in);
                    Map<String, String> hash = new LinkedHashMap<>();
                    for (int i = 0; i < size; i++) {
                        String field = readString(in, version);
                        hash.put(field, readString(in, version));
                    }
                    value = hash;

                } else {
                    throw new CorruptSnapshotException(
                        "unknown type byte " + typeByte);
                }

                long expireAt = in.readLong();

                // Skip keys whose deadline passed while we were down
                if (expireAt > 0 && now > expireAt) {
                    skipped++;
                    continue;
                }

                loaded.put(key, new Entry(type, value, expireAt));
            }

            // Compare the checksum of what we just read with the one
            // stored when the file was written. Read our own value
            // first: reading the stored one would change it.
            if (version >= 3) {
                long computed = crc.getValue();
                long stored = in.readLong();
                if (computed != stored) {
                    throw new CorruptSnapshotException(
                        "checksum mismatch");
                }
            }

        } catch (EOFException e) {
            throw new CorruptSnapshotException(
                "file ends before the snapshot is complete");
        } catch (IOException e) {
            throw new CorruptSnapshotException(
                "cannot be read: " + e.getMessage());
        }

        System.out.println("[RDB] Loaded " + loaded.size()
            + " keys, skipped " + skipped
            + " expired keys from " + file.getFileName());
        return loaded;
    }

    private static int versionOf(String magic) {
        switch (magic) {
            case MAGIC:    return 3;
            case MAGIC_V2: return 2;
            case MAGIC_V1: return 1;
            default:
                throw new CorruptSnapshotException(
                    "not a snapshot file");
        }
    }

    // Like the parser, the loader treats every number in the file as
    // a claim to be checked before acting on it
    private static int readCount(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0) {
            throw new CorruptSnapshotException("negative count");
        }
        return count;
    }

    private static String readString(DataInputStream in, int version)
            throws IOException {
        if (version == 1) {
            // Version 1 stored the text as Java saw it after decoding
            // the client's bytes as UTF-8; re-encoding gives back the
            // bytes the client originally sent
            return Binary.fromText(in.readUTF());
        }
        if (version == 2) {
            return in.readUTF();
        }
        int length = in.readInt();
        if (length < 0 || length > RespParser.MAX_BULK_LENGTH) {
            throw new CorruptSnapshotException("invalid string length");
        }
        byte[] bytes = in.readNBytes(length);
        if (bytes.length < length) {
            throw new EOFException();
        }
        return Binary.decode(bytes);
    }
}
