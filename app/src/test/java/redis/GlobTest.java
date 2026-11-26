package redis;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class GlobTest {

    @Test
    void literalTextMustMatchExactly() {
        assertTrue(Glob.matches("hello", "hello"));
        assertFalse(Glob.matches("hello", "hell"));
        assertFalse(Glob.matches("hello", "hello!"));
        assertTrue(Glob.matches("", ""));
        assertFalse(Glob.matches("", "x"));
    }

    @Test
    void starMatchesAnyRunIncludingNothing() {
        assertTrue(Glob.matches("*", ""));
        assertTrue(Glob.matches("*", "anything"));
        assertTrue(Glob.matches("user:*", "user:"));
        assertTrue(Glob.matches("user:*", "user:42"));
        assertFalse(Glob.matches("user:*", "session:42"));
        assertTrue(Glob.matches("*:42", "user:42"));
        assertTrue(Glob.matches("a*b*c", "aXXbYYc"));
        assertFalse(Glob.matches("a*b*c", "aXXbYY"));
        assertTrue(Glob.matches("a**c", "abc"));
    }

    @Test
    void questionMarkMatchesExactlyOneCharacter() {
        assertTrue(Glob.matches("h?llo", "hello"));
        assertTrue(Glob.matches("h?llo", "hallo"));
        assertFalse(Glob.matches("h?llo", "hllo"));
        assertFalse(Glob.matches("?", ""));
    }

    @Test
    void bracketsMatchOneOfASet() {
        assertTrue(Glob.matches("h[ae]llo", "hello"));
        assertTrue(Glob.matches("h[ae]llo", "hallo"));
        assertFalse(Glob.matches("h[ae]llo", "hillo"));
    }

    @Test
    void bracketsSupportRangesAndNegation() {
        assertTrue(Glob.matches("key[0-9]", "key7"));
        assertFalse(Glob.matches("key[0-9]", "keyx"));
        assertTrue(Glob.matches("h[^e]llo", "hallo"));
        assertFalse(Glob.matches("h[^e]llo", "hello"));
        assertTrue(Glob.matches("[a-c][x-z]", "by"));
    }

    @Test
    void backslashMakesASpecialCharacterLiteral() {
        assertTrue(Glob.matches("what\\?", "what?"));
        assertFalse(Glob.matches("what\\?", "whatx"));
        assertTrue(Glob.matches("a\\*b", "a*b"));
        assertFalse(Glob.matches("a\\*b", "aXb"));
    }
}
