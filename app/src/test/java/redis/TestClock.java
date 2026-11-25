package redis;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

// A clock the test controls. Time only moves when advance() is called,
// so expiry tests are exact and never need Thread.sleep.
final class TestClock implements LongSupplier {

    private final AtomicLong now = new AtomicLong(1_000_000L);

    @Override
    public long getAsLong() {
        return now.get();
    }

    void advance(long millis) {
        now.addAndGet(millis);
    }
}
