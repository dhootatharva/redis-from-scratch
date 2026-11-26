package redis;

// The snapshot file exists but cannot be trusted: it is truncated,
// fails its checksum, or is not a snapshot at all.
//
// The server refuses to start when this happens. Starting empty would
// be worse: the next automatic save would overwrite the damaged file,
// and whatever could still have been recovered from it would be gone.
public class CorruptSnapshotException extends RuntimeException {

    public CorruptSnapshotException(String message) {
        super(message);
    }
}
