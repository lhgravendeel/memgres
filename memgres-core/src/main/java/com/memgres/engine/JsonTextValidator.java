package com.memgres.engine;

/**
 * Checks that text really is one JSON document before it is stored as json or jsonb.
 *
 * <p>PostgreSQL parses json input properly, so it refuses things that look close enough to pass a
 * bracket-balancing check: a second value after the first ({@code [1,2] [3]}), an unquoted object
 * key, a string with no closing quote, and the number forms JSON does not have ({@code 007},
 * {@code +1}, {@code 1.}, {@code .5}). Accepting those stored a value that could never be read
 * back the same way — {@code '[1,2] [3]'} came out as {@code [1, 2, , []]}.
 */
final class JsonTextValidator {

    /** What PostgreSQL says under a \\u that is not followed by four hexadecimal digits. */
    private static final String BAD_UNICODE_ESCAPE =
            "\"\\u\" must be followed by four hexadecimal digits.";

    private final String text;
    private int pos;
    private int depth;

    private JsonTextValidator(String text) {
        this.text = text;
    }

    /** @throws MemgresException 22P02 when {@code text} is not exactly one JSON document */
    static void validate(String text) {
        JsonTextValidator v = new JsonTextValidator(text);
        v.skipWhitespace();
        v.readValue();
        v.skipWhitespace();
        // Anything after the first document is a second value, which json input does not take
        if (v.pos != text.length()) throw invalid();
    }

    private static MemgresException invalid() {
        return new MemgresException("invalid input syntax for type json", "22P02");
    }

    /**
     * The same error carrying the DETAIL line PostgreSQL writes under it. The primary message is
     * the same for every way a document can be malformed, so the detail is the only part that
     * says which one it was.
     */
    private static MemgresException invalid(String detail) {
        MemgresException e = invalid();
        e.setDetail(detail);
        return e;
    }

    private void readValue() {
        if (pos >= text.length()) throw invalid();
        char c = text.charAt(pos);
        switch (c) {
            case '{': readObject(); return;
            case '[': readArray(); return;
            case '"': readString(); return;
            case 't': expect("true"); return;
            case 'f': expect("false"); return;
            case 'n': expect("null"); return;
            default: readNumber();
        }
    }

    private void readObject() {
        enter();
        pos++; // {
        skipWhitespace();
        if (peek() == '}') { pos++; depth--; return; }
        while (true) {
            skipWhitespace();
            // A key is always a quoted string; {a: 1} is not JSON
            if (peek() != '"') throw invalid();
            readString();
            skipWhitespace();
            if (peek() != ':') throw invalid();
            pos++;
            skipWhitespace();
            readValue();
            skipWhitespace();
            char c = peek();
            if (c == ',') { pos++; continue; }
            if (c == '}') { pos++; depth--; return; }
            throw invalid();
        }
    }

    private void readArray() {
        enter();
        pos++; // [
        skipWhitespace();
        if (peek() == ']') { pos++; depth--; return; }
        while (true) {
            skipWhitespace();
            readValue();
            skipWhitespace();
            char c = peek();
            if (c == ',') { pos++; continue; }
            if (c == ']') { pos++; depth--; return; }
            throw invalid();
        }
    }

    private void readString() {
        pos++; // opening quote
        while (true) {
            if (pos >= text.length()) throw invalid();   // no closing quote
            char c = text.charAt(pos++);
            if (c == '"') return;
            if (c == '\\') {
                if (pos >= text.length()) throw invalid();
                char esc = text.charAt(pos++);
                if ("\"\\/bfnrt".indexOf(esc) >= 0) continue;
                if (esc == 'u') {
                    if (pos + 4 > text.length()) throw invalid(BAD_UNICODE_ESCAPE);
                    for (int i = 0; i < 4; i++) {
                        if (Character.digit(text.charAt(pos + i), 16) < 0) {
                            throw invalid(BAD_UNICODE_ESCAPE);
                        }
                    }
                    pos += 4;
                    continue;
                }
                throw invalid("Escape sequence \"\\" + esc + "\" is invalid.");
            }
            if (c < 0x20) throw invalid();  // a raw control character is not allowed in a string
        }
    }

    /** JSON's number grammar: no leading plus, no leading zeros, digits either side of the point. */
    private void readNumber() {
        int start = pos;
        if (peek() == '-') pos++;
        int intStart = pos;
        while (pos < text.length() && isDigit(text.charAt(pos))) pos++;
        int intLen = pos - intStart;
        if (intLen == 0) throw invalid();                       // ".5" has no integer part
        if (intLen > 1 && text.charAt(intStart) == '0') throw invalid();   // "007"
        if (pos < text.length() && text.charAt(pos) == '.') {
            pos++;
            int fracStart = pos;
            while (pos < text.length() && isDigit(text.charAt(pos))) pos++;
            if (pos == fracStart) throw invalid();              // "1." has no fraction
        }
        if (pos < text.length() && (text.charAt(pos) == 'e' || text.charAt(pos) == 'E')) {
            pos++;
            if (pos < text.length() && (text.charAt(pos) == '+' || text.charAt(pos) == '-')) pos++;
            int expStart = pos;
            while (pos < text.length() && isDigit(text.charAt(pos))) pos++;
            if (pos == expStart) throw invalid();
        }
        if (pos == start) throw invalid();
    }

    private void enter() {
        // Nesting this deep cannot be walked recursively later on, so refuse it at input time
        if (++depth > PgErrors.MAX_RECURSION_DEPTH) throw PgErrors.stackDepthExceeded();
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private char peek() {
        return pos < text.length() ? text.charAt(pos) : '\0';
    }

    private void expect(String word) {
        if (!text.startsWith(word, pos)) throw invalid();
        pos += word.length();
    }

    private void skipWhitespace() {
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
            else break;
        }
    }
}
