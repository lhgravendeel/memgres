package com.memgres.engine;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Reads JSON text, either to check that it is one document or to build the value it stands for.
 *
 * <p>PostgreSQL parses json input properly, so it refuses things that look close enough to pass a
 * bracket-balancing check: a second value after the first ({@code [1,2] [3]}), an unquoted object
 * key, a string with no closing quote, and the number forms JSON does not have ({@code 007},
 * {@code +1}, {@code 1.}, {@code .5}). Accepting those stored a value that could never be read
 * back the same way — {@code '[1,2] [3]'} came out as {@code [1, 2, , []]}.
 *
 * <p>The document is assembled with a stack rather than by recursion. PostgreSQL bounds nesting by
 * how much C stack the parse takes, which in a default installation is somewhere past sixteen
 * thousand levels; a Java parse that recursed once per level would give out an order of magnitude
 * earlier, so no depth this accepts may cost a frame.
 *
 * <p>Three readings are offered, and they differ in how much of the document has to mean something:
 * <ul>
 *   <li>{@link #validate} only asks whether the text is one document. It is what the json type and
 *       the IS JSON predicate ask, and it decodes nothing — {@code "\u0000"} is a valid json
 *       document even though no text can hold what it names.
 *   <li>{@link #parse} builds the document as written, with its members in their original order,
 *       duplicate keys kept and numbers spelled as the document spelled them. Strings are decoded,
 *       which is where an escape naming no character is refused.
 *   <li>{@link #parseJsonb} builds what jsonb stores: members sorted and deduplicated, and numbers
 *       converted to numerics, which is where one too large for a numeric is refused.
 * </ul>
 */
final class JsonParser {

    /** What PostgreSQL says under a \\u that is not followed by four hexadecimal digits. */
    private static final String BAD_UNICODE_ESCAPE =
            "\"\\u\" must be followed by four hexadecimal digits.";

    private final String text;
    private final boolean building;
    private final boolean jsonb;
    /** Whether keys are read for their own sake, when nothing else about the document is wanted. */
    private boolean checkKeys;
    private boolean uniqueKeys = true;
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

    private JsonParser(String text, boolean building, boolean jsonb) {
        this.text = text;
        this.building = building;
        this.jsonb = jsonb;
    }

    /** @throws MemgresException 22P02 when {@code text} is not exactly one JSON document */
    static void validate(String text) {
        new JsonParser(text, false, false).readDocument();
    }

    /** The document as written. @throws MemgresException 22P02, or 22P05 for an unusable escape */
    static JsonValue parse(String text) {
        return new JsonParser(text, true, false).readDocument();
    }

    /** The document as jsonb stores it. @throws MemgresException 22003 for an unusable number */
    static JsonValue parseJsonb(String text) {
        return new JsonParser(text, true, true).readDocument();
    }

    /**
     * What the IS JSON predicate asks of a text: whether it is one document, what kind of value
     * that document is, and whether every object in it writes each of its keys once.
     *
     * <p>Nothing but the keys is decoded. IS JSON asks about the document and not about what could
     * be made of it, so {@code '"\u005cu0000"' IS JSON} is true even though no text can hold what
     * that escape names.
     *
     * @return null when the text is not one JSON document
     */
    static Shape shapeOf(String text) {
        JsonParser p = new JsonParser(text, false, false);
        p.checkKeys = true;
        try {
            p.readDocument();
        } catch (MemgresException e) {
            return null;
        }
        return new Shape(p.rootKind, p.uniqueKeys);
    }

    /**
     * Whether this text is a JSON number, and can therefore stand in a document unquoted.
     *
     * <p>Asked of every number about to be written, because a number may print as a word: NaN and
     * the two infinities are values that numeric and the floats hold and JSON has no spelling for,
     * so they go in as strings. Writing them bare made a document that could not be read back.
     */
    static boolean isNumberText(String text) {
        Shape shape = shapeOf(text);
        return shape != null && shape.kind == JsonValue.NUMBER;
    }

    /** What a document is, for a predicate that asks about the document rather than its value. */
    static final class Shape {
        /** One of {@link JsonValue}'s kind constants. */
        final int kind;
        /** False when some object in the document writes one key twice. */
        final boolean uniqueKeys;

        Shape(int kind, boolean uniqueKeys) {
            this.kind = kind;
            this.uniqueKeys = uniqueKeys;
        }
    }

    /**
     * The members of a container, each with the source text of its value.
     *
     * <p>The text is what the document wrote, so a member of a json document keeps the spelling and
     * the spacing json keeps, and a member of a jsonb document is already in jsonb's shape. That is
     * what the operators that hand back part of a document want: {@code ->} on a json value gives
     * the text as written, and normalising it would be handing back a different document.
     *
     * @return null when the text is not a container
     */
    static List<Member> membersOf(String text) {
        JsonParser p = new JsonParser(text, false, false);
        p.lex();
        if (p.token != '{' && p.token != '[') return null;
        return p.members();
    }

    /**
     * The kind of value a document holds, read off its first token. The functions that fill a
     * record from a document name the kind they were handed when they were handed the wrong one,
     * and asking a reader that builds the whole value would be reading the document twice.
     *
     * @return one of {@link JsonValue}'s kind constants
     * @throws MemgresException 22P02 when the text does not begin a JSON value
     */
    static int kindOf(String text) {
        JsonParser p = new JsonParser(text, false, false);
        p.lex();
        int kind = p.kindOfToken();
        if (kind < 0) throw p.wanted("json value");
        return kind;
    }

    /** One member of a container: an array element, or an object key and the value under it. */
    static final class Member {
        /** The key, decoded, or null for an array element. */
        final String key;
        /** The value, as the document wrote it. */
        final String text;

        Member(String key, String text) {
            this.key = key;
            this.text = text;
        }
    }

    /**
     * The members of the container the reader is sitting on, leaving it on the closing bracket.
     *
     * <p>Each value is skipped by reading it, so where one ends is decided by the same reader that
     * decides whether the document is valid at all. Counting brackets instead ended a value at the
     * first {@code }} inside a string.
     */
    private List<Member> members() {
        boolean object = token == '{';
        char end = object ? '}' : ']';
        List<Member> out = new ArrayList<Member>();
        lex();
        if (token == end) return out;
        for (;;) {
            String key = null;
            if (object) {
                if (token != STRING) throw wanted(out.isEmpty() ? "string or \"}\"" : "string");
                key = decodeString();
                lex();
                if (token != ':') throw wanted("\":\"");
                lex();
            }
            int from = tokenStart;
            readValue();
            out.add(new Member(key, text.substring(from, tokenEnd)));
            lex();
            if (token == ',') {
                lex();
                continue;
            }
            if (token == end) return out;
            throw wanted(object ? "\",\" or \"}\"" : "\",\" or \"]\"");
        }
    }

    /**
     * The error carrying the DETAIL line PostgreSQL writes under it. The primary message is the
     * same for every way a document can be malformed, so the detail is the only part that says
     * which one it was.
     */
    private MemgresException invalid(String detail) {
        MemgresException e = new MemgresException("invalid input syntax for type json", "22P02");
        e.setDetail(detail);
        e.setPgContext(context());
        return e;
    }

    /**
     * The document as far as the reader got, which PostgreSQL reports under the error as its
     * context. Only the line stopped in is shown, and only the last fifty or so characters of it,
     * with an ellipsis at whichever end the line was cut -- a document is often one long line, and
     * the part worth reading is the part just before the reader gave up.
     */
    private String context() {
        int end = pos;
        int lineStart = end == 0 ? 0 : text.lastIndexOf('\n', end - 1) + 1;
        int line = 1;
        for (int i = 0; i < lineStart; i++) {
            if (text.charAt(i) == '\n') line++;
        }
        int from = lineStart;
        while (end - from >= 50) from++;
        // Three characters are not worth an ellipsis three characters long.
        if (from - lineStart <= 3) from = lineStart;
        boolean more = token != END && end < text.length()
                && text.charAt(end) != '\n' && text.charAt(end) != '\r';
        return "JSON data, line " + line + ": " + (from > lineStart ? "..." : "")
                + text.substring(from, end) + (more ? "..." : "");
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

    /** The kind of the document's top-level value, read off its first token. */
    private int rootKind;

    private JsonValue readDocument() {
        lex();
        rootKind = kindOfToken();
        JsonValue value = readValue();
        lex();
        // Anything after the first document is a second value, which json input does not take
        if (token != END) throw wanted("end of input");
        return value;
    }

    /**
     * One value, and everything inside it. The containers still open are held on a stack, so the
     * whole document is read in one loop: the outer half reads a value, and the inner half hands a
     * finished value to whatever is waiting for it and reads the separator that follows.
     */
    private JsonValue readValue() {
        Deque<Frame> open = new ArrayDeque<Frame>();
        for (;;) {
            JsonValue done;
            if (token == '{') {
                enter();
                Frame frame = new Frame(true);
                open.push(frame);
                lex();
                if (token != '}') {
                    readKey(frame);
                    continue;
                }
                done = close(open.pop());
            } else if (token == '[') {
                enter();
                open.push(new Frame(false));
                lex();
                if (token != ']') continue;
                done = close(open.pop());
            } else {
                done = leaf();
            }
            // The value is finished. Give it to the container waiting for it, and read on until
            // one of them is not finished by it.
            boolean waiting = false;
            while (!waiting) {
                if (open.isEmpty()) return done;
                Frame frame = open.peek();
                frame.add(done);
                lex();
                if (frame.object) {
                    if (token == ',') {
                        lex();
                        readKey(frame);
                        waiting = true;
                    } else if (token == '}') {
                        done = close(open.pop());
                    } else {
                        throw wanted("\",\" or \"}\"");
                    }
                } else {
                    if (token == ',') {
                        lex();
                        waiting = true;
                    } else if (token == ']') {
                        done = close(open.pop());
                    } else {
                        throw wanted("\",\" or \"]\"");
                    }
                }
            }
        }
    }

    /** The kind the token the reader is on opens, for a token that opens a value. */
    private int kindOfToken() {
        if (token == '{') return JsonValue.OBJECT;
        if (token == '[') return JsonValue.ARRAY;
        if (token == STRING) return JsonValue.STRING;
        if (token != SCALAR) return -1;
        String word = tokenText();
        if (word.equals("true") || word.equals("false")) return JsonValue.BOOLEAN;
        return word.equals("null") ? JsonValue.NULL : JsonValue.NUMBER;
    }

    /** A key and the colon after it, leaving the reader on the first token of the member's value. */
    private void readKey(Frame frame) {
        // A key is always a quoted string; {a: 1} is not JSON
        if (token != STRING) throw wanted(frame.keys.isEmpty() ? "string or \"}\"" : "string");
        frame.pendingKey = building || checkKeys ? decodeString() : null;
        lex();
        if (token != ':') throw wanted("\":\"");
        lex();
    }

    private JsonValue leaf() {
        if (token == STRING) return building ? JsonValue.string(decodeString()) : null;
        if (token != SCALAR) throw wanted("JSON value");
        if (!building) return null;
        String word = tokenText();
        if (word.equals("true")) return JsonValue.TRUE;
        if (word.equals("false")) return JsonValue.FALSE;
        if (word.equals("null")) return JsonValue.JSON_NULL;
        BigDecimal value = new BigDecimal(word);
        if (jsonb) {
            JsonNormalizer.requireNumericRange(value);
            return JsonValue.number(value);
        }
        return JsonValue.number(word, value);
    }

    private JsonValue close(Frame frame) {
        depth--;
        // Uniqueness is asked of every object in the document and not only of the outermost one,
        // so it is settled as each object closes
        if (checkKeys && frame.object && uniqueKeys) {
            uniqueKeys = new java.util.HashSet<String>(frame.keys).size() == frame.keys.size();
        }
        if (!building) return null;
        if (!frame.object) return JsonValue.array(frame.values);
        return jsonb ? JsonNormalizer.sortedObject(frame.keys, frame.values)
                : JsonValue.object(frame.keys, frame.values);
    }

    /** A container being read: an array's elements, or an object's keys and the values under them. */
    private static final class Frame {
        final boolean object;
        final List<String> keys;
        final List<JsonValue> values;
        String pendingKey;

        Frame(boolean object) {
            this.object = object;
            this.keys = object ? new ArrayList<String>() : null;
            this.values = new ArrayList<JsonValue>();
        }

        void add(JsonValue value) {
            if (object) keys.add(pendingKey);
            values.add(value);
        }
    }

    /**
     * The characters the string token stands for. The lexer has already checked that every escape
     * is one JSON has; what is left is what those escapes name, which is where the two an escape
     * can name but text cannot hold are refused.
     */
    private String decodeString() {
        String body = text.substring(tokenStart + 1, tokenEnd - 1);
        if (body.indexOf('\\') < 0) return body;
        StringBuilder out = new StringBuilder(body.length());
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char esc = body.charAt(++i);
            switch (esc) {
                case '"': out.append('"'); break;
                case '\\': out.append('\\'); break;
                case '/': out.append('/'); break;
                case 'b': out.append('\b'); break;
                case 'f': out.append('\f'); break;
                case 'n': out.append('\n'); break;
                case 'r': out.append('\r'); break;
                case 't': out.append('\t'); break;
                default: {
                    int cp = Integer.parseInt(body.substring(i + 1, i + 5), 16);
                    i += 4;
                    if (cp == 0) {
                        MemgresException e = new MemgresException(
                                "unsupported Unicode escape sequence", "22P05");
                        e.setDetail("\\u0000 cannot be converted to text.");
                        throw e;
                    }
                    if (Character.isHighSurrogate((char) cp)) {
                        out.append((char) cp).append((char) readLowSurrogate(body, i));
                        i += 6;
                    } else if (Character.isLowSurrogate((char) cp)) {
                        throw invalidSurrogate();
                    } else {
                        out.append((char) cp);
                    }
                }
            }
        }
        return out.toString();
    }

    /** The second half of a surrogate pair has to follow immediately, as its own \\u escape. */
    private int readLowSurrogate(String body, int i) {
        if (i + 7 > body.length() || body.charAt(i + 1) != '\\' || body.charAt(i + 2) != 'u') {
            throw invalidSurrogate();
        }
        int low;
        try {
            low = Integer.parseInt(body.substring(i + 3, i + 7), 16);
        } catch (NumberFormatException e) {
            throw invalidSurrogate();
        }
        if (!Character.isLowSurrogate((char) low)) throw invalidSurrogate();
        return low;
    }

    private MemgresException invalidSurrogate() {
        // Every way a surrogate escape can go wrong -- a high half with nothing after it, a low
        // half on its own, a pair whose second half is not one -- is the same complaint in PG.
        return invalid("Unicode low surrogate must follow a high surrogate.");
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
        // Nesting this deep is past what PostgreSQL itself will read, and past what any later
        // walk of the document could follow
        if (++depth > MAX_DEPTH) throw PgErrors.stackDepthExceeded();
    }

    /**
     * How deep a document may nest. PostgreSQL has no fixed limit — it stops when its C stack runs
     * low — and a default installation stops between sixteen and seventeen thousand levels, which
     * is where this sits.
     */
    static final int MAX_DEPTH = 16000;

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
