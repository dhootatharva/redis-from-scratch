package redis;

import java.nio.charset.StandardCharsets;

// How this server holds arbitrary bytes in Java Strings.
//
// Redis keys and values are bytes, not text: a value can be a JPEG or
// a compressed blob. Decoding such bytes as UTF-8 would damage them,
// because byte sequences that are not valid UTF-8 get replaced.
//
// ISO-8859-1 (Latin-1) maps every byte 0..255 to exactly one char
// 0..255 and back, so nothing is ever lost or replaced. Inside the
// server, a String is therefore a "binary string": one char per byte.
//
//   wire bytes  --decode-->  String (1 char = 1 byte)  --encode-->  wire bytes
//
// Consequences:
//   - string.length() IS the length in bytes
//   - never call getBytes(UTF_8) on a stored value; use encode()
//   - text typed as a Java literal (in tests, say) must go through
//     fromText() to become what a real client would have sent
//
// Java stores Latin-1 Strings with one byte per char internally
// (compact strings, JDK 9+), so this costs no extra memory.
final class Binary {

    private Binary() {}

    // Bytes from the network -> binary string
    static String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    // Binary string -> the exact bytes it stands for
    static byte[] encode(String binary) {
        return binary.getBytes(StandardCharsets.ISO_8859_1);
    }

    // Ordinary text -> the binary string a client sending that text
    // as UTF-8 would produce. "é" becomes two chars: 0xC3, 0xA9.
    static String fromText(String text) {
        return decode(text.getBytes(StandardCharsets.UTF_8));
    }
}
