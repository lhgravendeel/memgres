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
    // The token last read. Punctuation stands for itself; the rest are named below. PostgreSQL
    // reports the text of the token it stopped at, so where that begins and ends is kept too.
    private int token;
    private int tokenStart;
    private int tokenEnd;

    private static final int INVALID = 0;
    private static final int END = 1;
    private static final int STRING = 2;
    /** A number, or one of true, false and null: a value that stands on its own. */
    private static final int SCALAR = 3;

    private JsonTextValidator(String text) {
        this.text = text;
    }

    /** @throws MemgresException 22P02 when {@code text} is not exactly one JSON document */
    static void validate(String text) {
        JsonTextValidator v = new JsonTextValidator(text);
        v.lex();
        v.readValue();
        v.lex();
        // Anything after the first document is a second value, which json input does not take
        if (v.token != END) throw v.wanted("end of input");
    }

    /**
     * The error carrying the DETAIL line PostgreSQL writes under it. The primary message is the
     * same for every way a document can be malformed, so the detail is the only part that says
     * which one it was.
     */
    private static MemgresException invalid(String detail) {
        MemgresException e = new MemgresException("invalid input syntax for type json", "22P02");
        e.setDetail(detail);
        return e;
    }

    /**
     * What PostgreSQL says about the token the reader stopped at. Running out of text is its own
     * complaint; otherwise the reader names what it wanted in that place and what it found.
     */
    private MemgresException wanted(String what) {
        if (token == END) return invalid("The input string ended unexpectedly.");
        return invalid("Expected " + what + ", but found \"" + tokenText() + "\".");
    }

    private String tokenText() {
        return text.substring(tokenStart, tokenEnd);
    }

    private void readValue() {
        switch (token) {
            case '{': readObject(); return;
            case '[': readArray(); return;
            case STRING: case SCALAR: return;
            default: throw wanted("JSON value");
        }
    }

    private void readObject() {
        enter();
        lex();
        if (token != '}') {
            boolean firstKey = true;
            for (;;) {
                // A key is always a quoted string; {a: 1} is not JSON
                if (token != STRING) throw wanted(firstKey ? "string or \"}\"" : "string");
                firstKey = false;
                lex();
                if (token != ':') throw wanted("\":\"");
                lex();
                readValue();
                lex();
                if (token == ',') { lex(); continue; }
                if (token == '}') break;
                throw wanted("\",\" or \"}\"");
            }
        }
        depth--;
    }

    private void readArray() {
        enter();
        lex();
        if (token != ']') {
            for (;;) {
                readValue();
                lex();
                if (token == ',') { lex(); continue; }
                if (token == ']') break;
                throw wanted("\",\" or \"]\"");
            }
        }
        depth--;
    }

    /**
     * Read the next token. Text that is no token of JSON's is refused here rather than handed on,
     * because PostgreSQL's lexer runs ahead of its parser: {@code [1,2] x} is a bad token, not a
     * document that should have ended.
     */
    private void lex() {
        skipWhitespace();
        tokenStart = pos;
        if (pos >= text.length()) { tokenEnd = pos; token = END; return; }
        char c = text.charAt(pos);
        if (c == '{' || c == '}' || c == '[' || c == ']' || c == ',' || c == ':') {
            pos++;
            tokenEnd = pos;
            token = c;
            return;
        }
        if (c == '"') lexString();
        else if (c == '-' || isDigit(c)) lexNumber();
        else lexWord();
        if (token == INVALID) throw invalid("Token \"" + tokenText() + "\" is invalid.");
    }

    private void lexString() {
        pos++; // opening quote
        while (pos < text.length()) {
            char c = text.charAt(pos++);
            if (c == '"') {
                tokenEnd = pos;
                token = STRING;
                return;
            }
            if (c == '\\') readEscape();
            else if (c < 0x20) throw invalid("Character with value 0x" + hex(c) + " must be escaped.");
        }
        // A string with no closing quote is no token at all, and PostgreSQL names all of it
        tokenEnd = pos;
        token = INVALID;
    }

    private void readEscape() {
        if (pos >= text.length()) return;   // the unfinished string is the complaint
        char esc = text.charAt(pos++);
        if ("\"\\/bfnrt".indexOf(esc) >= 0) return;
        if (esc == 'u') {
            if (pos + 4 > text.length()) throw invalid(BAD_UNICODE_ESCAPE);
            for (int i = 0; i < 4; i++) {
                if (Character.digit(text.charAt(pos + i), 16) < 0) throw invalid(BAD_UNICODE_ESCAPE);
            }
            pos += 4;
            return;
        }
        throw invalid("Escape sequence \"\\" + esc + "\" is invalid.");
    }

    /** JSON's number grammar: no leading plus, no leading zeros, digits either side of the point. */
    private void lexNumber() {
        boolean bad = false;
        if (text.charAt(pos) == '-') pos++;
        if (pos < text.length() && text.charAt(pos) == '0') pos++;
        else if (!skipDigits()) bad = true;                          // "-" has no digits at all
        if (pos < text.length() && text.charAt(pos) == '.') {
            pos++;
            if (!skipDigits()) bad = true;                           // "1." has no fraction
        }
        if (pos < text.length() && (text.charAt(pos) == 'e' || text.charAt(pos) == 'E')) {
            pos++;
            if (pos < text.length() && (text.charAt(pos) == '+' || text.charAt(pos) == '-')) pos++;
            if (!skipDigits()) bad = true;
        }
        // Whatever runs on from a number with no delimiter between belongs to the same bad token,
        // which is how "007" is named whole rather than as a zero followed by something else.
        while (pos < text.length() && isWordChar(text.charAt(pos))) {
            pos++;
            bad = true;
        }
        tokenEnd = pos;
        token = bad ? INVALID : SCALAR;
    }

    private boolean skipDigits() {
        int from = pos;
        while (pos < text.length() && isDigit(text.charAt(pos))) pos++;
        return pos > from;
    }

    /** The only bare words JSON has are true, false and null; anything else is named and refused. */
    private void lexWord() {
        while (pos < text.length() && isWordChar(text.charAt(pos))) pos++;
        if (pos == tokenStart) pos++;   // punctuation of its own: name that one character
        tokenEnd = pos;
        String word = tokenText();
        token = word.equals("true") || word.equals("false") || word.equals("null")
                ? SCALAR : INVALID;
    }

    private void enter() {
        // Nesting this deep cannot be walked recursively later on, so refuse it at input time
        if (++depth > PgErrors.MAX_RECURSION_DEPTH) throw PgErrors.stackDepthExceeded();
    }

    private static String hex(char c) {
        String s = Integer.toHexString(c);
        return s.length() < 2 ? "0" + s : s;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    /** PostgreSQL's JSON_ALPHANUMERIC_CHAR: how far a bare word or a bad number reaches. */
    private static boolean isWordChar(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || c == '_' || c > 127;
    }

    private void skipWhitespace() {
        while (pos < text.length()) {
            char c = text.charAt(pos);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') pos++;
            else break;
        }
    }
}
