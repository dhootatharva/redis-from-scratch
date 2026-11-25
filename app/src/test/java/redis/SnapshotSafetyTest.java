package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// What a snapshot promises: no size limit below the protocol's own,
// damage is detected instead of loaded, and a failed save neither
// lies about it nor harms the previous snapshot.
class SnapshotSafetyTest {

    @TempDir
    Path tempDir;

    private Path snapshot;
    private TestClock clock;

    @BeforeEach
    void setUp() {
        snapshot = tempDir.resolve("dump.rdb");
        clock = new TestClock();
    }

    private Store newStore() {
        return new Store(snapshot, clock);
    }

    private static String run(Store store, String... command) {
        return TestWire.run(new CommandHandler(store), command);
    }

    // A store with a few keys of each type, already saved
    private Store savedStore() {
        Store store = newStore();
        run(store, "SET", "s", "hello");
        run(store, "RPUSH", "l", "a", "b", "c");
        run(store, "HSET", "h", "field", "value");
        assertTrue(store.save());
        return store;
    }

    // ── Size ──

    // The old format could not hold a string over 65,535 bytes, and
    // one such value made the whole save fail.
    @Test
    void aValueOver64KbDoesNotBreakTheSnapshot() {
        Store before = newStore();
        run(before, "SET", "small", "1");
        run(before, "SET", "large", "x".repeat(70_000));
        assertTrue(before.save());

        Store after = newStore();
        after.load();

        assertEquals("$1\r\n1\r\n", run(after, "GET", "small"));
        assertEquals(":70000\r\n", run(after, "STRLEN", "large"));
    }

    @Test
    void aOneMegabyteBinaryValueSurvivesARestart() {
        byte[] payload = new byte[1024 * 1024];
        new Random(7).nextBytes(payload);

        Store before = newStore();
        new CommandHandler(before).handle(
            Arrays.asList("SET", "blob", Binary.decode(payload)));
        assertTrue(before.save());

        Store after = newStore();
        after.load();

        assertArrayEquals(payload, Binary.encode(after.get("blob")));
    }

    // ── Damage is detected ──

    // One flipped bit anywhere in the file must be noticed
    @Test
    void aSingleChangedByteIsDetected() throws IOException {
        savedStore();
        byte[] file = Files.readAllBytes(snapshot);
        file[file.length / 2] ^= 0x01;
        Files.write(snapshot, file);

        Store after = newStore();
        assertThrows(CorruptSnapshotException.class, after::load);
        // all or nothing: no key from the damaged file was kept
        assertEquals(":0\r\n", run(after, "DBSIZE"));
    }

    @Test
    void everySingleByteCorruptionIsDetected() throws IOException {
        savedStore();
        byte[] original = Files.readAllBytes(snapshot);

        for (int i = 0; i < original.length; i++) {
            byte[] damaged = original.clone();
            damaged[i] ^= (byte) 0xFF;
            Files.write(snapshot, damaged);

            int position = i;
            assertThrows(CorruptSnapshotException.class,
                () -> newStore().load(),
                "corruption at byte " + position + " was not detected");
        }
    }

    // As after a crash or a full disk while copying the file
    @Test
    void aTruncatedFileIsRejected() throws IOException {
        savedStore();
        byte[] file = Files.readAllBytes(snapshot);

        for (int length = 0; length < file.length; length++) {
            Files.write(snapshot, Arrays.copyOf(file, length));

            int cut = length;
            assertThrows(CorruptSnapshotException.class,
                () -> newStore().load(),
                "file cut to " + cut + " bytes was accepted");
        }
    }

    @Test
    void aFileThatIsNotASnapshotIsRejected() throws IOException {
        Files.writeString(snapshot, "this is not a snapshot file");
        assertThrows(CorruptSnapshotException.class,
            () -> newStore().load());
    }

    // ── A failed save ──

    // The temp file's name is taken by a directory, so the snapshot
    // cannot be written.
    @Test
    void aFailedSaveIsReportedAndKeepsTheOldSnapshot() throws IOException {
        Store store = savedStore();
        byte[] goodSnapshot = Files.readAllBytes(snapshot);

        Files.createDirectory(tempDir.resolve("dump.rdb.tmp"));
        run(store, "SET", "newer", "data");

        assertFalse(store.save());
        assertTrue(run(store, "SAVE").startsWith("-ERR"),
            "SAVE must not answer OK when nothing was written");

        // The previous snapshot is byte for byte what it was
        assertArrayEquals(goodSnapshot, Files.readAllBytes(snapshot));
        Store after = newStore();
        after.load();
        assertEquals("$5\r\nhello\r\n", run(after, "GET", "s"));
    }

    @Test
    void saveReportsSuccessWhenItWorks() {
        Store store = newStore();
        run(store, "SET", "k", "v");
        assertEquals("+OK\r\n", run(store, "SAVE"));
        assertTrue(Files.exists(snapshot));
        assertFalse(Files.exists(tempDir.resolve("dump.rdb.tmp")));
    }

    // ── Older files ──

    // Version 2: strings written with writeUTF, no checksum
    @Test
    void versionTwoSnapshotStillLoads() throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                Files.newOutputStream(snapshot))) {
            out.writeUTF("REDIS0002");
            out.writeByte(2);          // type: list
            out.writeUTF("jobs");
            out.writeInt(2);
            out.writeUTF("a");
            out.writeUTF("b");
            out.writeLong(0);
            out.writeByte(0);          // end of file
        }

        Store store = newStore();
        store.load();

        assertEquals("*2\r\n$1\r\na\r\n$1\r\nb\r\n",
            run(store, "LRANGE", "jobs", "0", "-1"));
    }

    // Loading an old file and saving rewrites it in the new format
    @Test
    void anOldSnapshotIsUpgradedOnTheNextSave() throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                Files.newOutputStream(snapshot))) {
            out.writeUTF("REDIS0002");
            out.writeByte(1);
            out.writeUTF("k");
            out.writeUTF("v");
            out.writeLong(0);
            out.writeByte(0);
        }

        Store store = newStore();
        store.load();
        assertTrue(store.save());

        byte[] file = Files.readAllBytes(snapshot);
        assertEquals("REDIS0003", new String(file, 2, 9));
        Store again = newStore();
        again.load();
        assertEquals("$1\r\nv\r\n", run(again, "GET", "k"));
    }
}
