package redis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

// Runs a command against a CommandHandler the way a real client
// would, minus the socket.
//
// Tests write arguments as ordinary Java text. A real client would
// send that text as UTF-8 bytes, and the parser would hand the server
// a binary string (see Binary). This helper does the same conversion,
// so a test with "héllo" exercises the 6 bytes a client would send.
// The reply bytes are decoded as UTF-8 so tests can compare text.
final class TestWire {

    private TestWire() {}

    static String run(CommandHandler handler, String... command) {
        List<String> args = new ArrayList<>();
        for (String arg : command) {
            args.add(Binary.fromText(arg));
        }
        return new String(handler.handle(args), StandardCharsets.UTF_8);
    }
}
