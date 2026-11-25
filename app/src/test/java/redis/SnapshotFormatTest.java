package redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

// The snapshot file across versions, and binary data in snapshots.
class SnapshotFormatTest {

    @TempDir
    Path tempDir;

    // Version 1 files were written before values became binary
    // strings: they hold text as Java decoded it. Loading one must
    // give back the bytes the client originally sent.
    @Test
    void versionOneSnapshotIsConvertedWhileLoading() throws IOException {
        Path snapshot = tempDir.resolve("dump.rdb");
        try (DataOutputStream out = new DataOutputStream(
                Files.newOutputStream(snapshot))) {
            out.writeUTF("REDIS0001");   // old magic header
            out.writeByte(1);            // type: string
            out.writeUTF("clé");         // key, as old versions stored it
            out.writeUTF("héllo");       // value
            out.writeLong(0);            // no deadline
            out.writeByte(0);            // end of file
        }

        Store store = new Store(snapshot, new TestClock());
        store.load();
        CommandHandler handler = new CommandHandler(store);

        // "héllo" is 6 bytes in UTF-8, and comes back as that text
        assertEquals(":6\r\n", TestWire.run(handler, "STRLEN", "clé"));
        assertEquals("$6\r\nhéllo\r\n", TestWire.run(handler, "GET", "clé"));
    }

    // Bytes that are not valid text must survive save and reload
    @Test
    void binaryValuesSurviveARestart() {
        Path snapshot = tempDir.resolve("dump.rdb");
        TestClock clock = new TestClock();
        byte[] payload = new byte[256];
        for (int i = 0; i < 256; i++) payload[i] = (byte) i;

        Store before = new Store(snapshot, clock);
        new CommandHandler(before).handle(Arrays.asList(
            "SET", "blob", Binary.decode(payload)));
        before.save();

        Store after = new Store(snapshot, clock);
        after.load();

        assertArrayEquals(payload, Binary.encode(after.get("blob")));
    }

    // A snapshot written now starts with the current header
    @Test
    void newSnapshotsUseTheCurrentHeader() throws IOException {
        Path snapshot = tempDir.resolve("dump.rdb");
        Store store = new Store(snapshot, new TestClock());
        new CommandHandler(store).handle(Arrays.asList("SET", "k", "v"));
        store.save();

        byte[] file = Files.readAllBytes(snapshot);
        // writeUTF stores a 2-byte length, then the characters
        String header = new String(file, 2, 9, StandardCharsets.US_ASCII);
        assertEquals("REDIS0003", header);
    }
}
