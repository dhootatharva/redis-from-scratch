package redis;

// Glob-style pattern matching, as used by the KEYS command.
//
//   *        any sequence of characters, including none
//   ?        exactly one character
//   [abc]    one of the listed characters
//   [a-z]    one character in the range
//   [^abc]   one character NOT listed
//   \x       the character x itself (to match a literal * or ?)
final class Glob {

    private Glob() {}

    static boolean matches(String pattern, String text) {
        return match(pattern, 0, text, 0);
    }

    // Does pattern[pi..] match text[ti..] ?
    private static boolean match(String p, int pi, String t, int ti) {
        while (pi < p.length()) {
            char c = p.charAt(pi);

            if (c == '*') {
                // Several stars in a row mean the same as one
                while (pi + 1 < p.length() && p.charAt(pi + 1) == '*') {
                    pi++;
                }
                // A trailing star matches whatever is left
                if (pi + 1 == p.length()) return true;
                // Otherwise try every possible length for the star
                for (int k = ti; k <= t.length(); k++) {
                    if (match(p, pi + 1, t, k)) return true;
                }
                return false;
            }

            // Every other pattern element consumes one character
            if (ti >= t.length()) return false;
            char ch = t.charAt(ti);

            if (c == '?') {
                pi++;

            } else if (c == '[') {
                int j = pi + 1;
                boolean negate = false;
                if (j < p.length() && p.charAt(j) == '^') {
                    negate = true;
                    j++;
                }
                boolean found = false;
                while (j < p.length() && p.charAt(j) != ']') {
                    if (p.charAt(j) == '\\' && j + 1 < p.length()) {
                        // escaped character inside the brackets
                        if (p.charAt(j + 1) == ch) found = true;
                        j += 2;
                    } else if (j + 2 < p.length()
                            && p.charAt(j + 1) == '-'
                            && p.charAt(j + 2) != ']') {
                        // a range such as a-z
                        char low = p.charAt(j);
                        char high = p.charAt(j + 2);
                        if (low > high) {
                            char swap = low;
                            low = high;
                            high = swap;
                        }
                        if (ch >= low && ch <= high) found = true;
                        j += 3;
                    } else {
                        if (p.charAt(j) == ch) found = true;
                        j++;
                    }
                }
                if (found == negate) return false;
                // step past the closing bracket, if there is one
                pi = j < p.length() ? j + 1 : j;

            } else {
                // A backslash makes the next character literal
                if (c == '\\' && pi + 1 < p.length()) {
                    pi++;
                    c = p.charAt(pi);
                }
                if (c != ch) return false;
                pi++;
            }
            ti++;
        }
        // The pattern is used up: it matched only if the text is too
        return ti == t.length();
    }
}
