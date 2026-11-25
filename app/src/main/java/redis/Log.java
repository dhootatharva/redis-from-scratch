package redis;

import java.util.function.Supplier;

// Minimal logging with two levels.
//
//   info   always printed: startup, shutdown, errors
//   debug  printed only with --verbose: every connection and command
//
// Per-command logging is off by default for two reasons. Printing is
// slow (System.out is synchronized, so every worker would queue on
// it), and commands contain user data, which can be binary and can
// be secret.
final class Log {

    private Log() {}

    static volatile boolean verbose = false;

    static void info(String message) {
        System.out.println(message);
    }

    // Takes a Supplier so the message is only built when it will be
    // printed. With verbose off, a debug call costs one boolean check.
    static void debug(Supplier<String> message) {
        if (verbose) {
            System.out.println(message.get());
        }
    }
}
