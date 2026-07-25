package com.memgres.engine.fts;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL's default text-search parser.
 *
 * <p>PG does not split text on "everything that is not a letter or a digit": it runs a
 * state machine that assigns each token one of a fixed set of <em>types</em>, and the
 * text-search configuration then routes each type to a different dictionary. That is why
 * {@code john.doe@example.com} survives as one lexeme (type {@code email}, routed to
 * {@code simple}) while {@code well-known} yields three ({@code asciihword} plus its two
 * parts, all stemmed).
 *
 * <p>The types produced here are the ones PG's parser emits for ordinary text:
 * {@code asciiword}, {@code word}, {@code numword}, the hyphenated-compound family,
 * {@code email}, {@code protocol}, {@code url}, {@code host}, {@code url_path},
 * {@code file}, {@code uint}, {@code int}, {@code float}, {@code sfloat},
 * {@code version} and {@code blank}. Markup-specific types ({@code tag},
 * {@code entity}) are not recognised; their text falls through to the word rules rather
 * than being mis-typed.
 */
public final class TsParser {

    private TsParser() {
    }

    /** Token types PG's default parser assigns. */
    public enum Type {
        ASCIIWORD, WORD, NUMWORD,
        ASCIIHWORD, HWORD, NUMHWORD,
        HWORD_ASCIIPART, HWORD_PART, HWORD_NUMPART,
        EMAIL, PROTOCOL, URL, HOST, URL_PATH, FILE,
        UINT, INT, FLOAT, SFLOAT, VERSION,
        BLANK
    }

    /** Which dictionary the default {@code english} configuration routes a type to. */
    public enum Dict { STEM, SIMPLE, NONE }

    public static final class Token {
        public final Type type;
        public final String text;

        Token(Type type, String text) {
            this.type = type;
            this.text = text;
        }

        public Type type() { return type; }

        public String text() { return text; }

        @Override
        public String toString() {
            return type + "(" + text + ")";
        }
    }

    /** Dictionary mapping of the stock {@code english} configuration. */
    public static Dict dictionaryFor(Type type) {
        switch (type) {
            case ASCIIWORD:
            case WORD:
            case ASCIIHWORD:
            case HWORD:
            case HWORD_ASCIIPART:
            case HWORD_PART:
                return Dict.STEM;
            case NUMWORD:
            case NUMHWORD:
            case HWORD_NUMPART:
            case EMAIL:
            case URL:
            case HOST:
            case URL_PATH:
            case FILE:
            case UINT:
            case INT:
            case FLOAT:
            case SFLOAT:
            case VERSION:
                return Dict.SIMPLE;
            default:
                // blank and protocol have no dictionary, so they take no position either.
                return Dict.NONE;
        }
    }

    public static List<Token> parse(String text) {
        List<Token> out = new ArrayList<Token>();
        if (text == null) return out;
        int i = 0;
        int n = text.length();
        StringBuilder blank = new StringBuilder();
        while (i < n) {
            char c = text.charAt(i);
            int consumed = -1;
            if (isDigit(c) || ((c == '-' || c == '+') && i + 1 < n && isDigit(text.charAt(i + 1)))) {
                consumed = scanNumber(text, i, out, blank);
                if (consumed < 0 && isDigit(c)) consumed = scanWordish(text, i, out, blank);
            } else if (isWordChar(c)) {
                consumed = scanWordish(text, i, out, blank);
            } else if (c == '/') {
                int end = scanPath(text, i);
                if (end > i + 1) {
                    flush(blank, out);
                    out.add(new Token(Type.FILE, text.substring(i, end)));
                    consumed = end;
                }
            }
            if (consumed < 0) {
                blank.append(c);
                i++;
            } else {
                i = consumed;
            }
        }
        flush(blank, out);
        return out;
    }

    private static void flush(StringBuilder blank, List<Token> out) {
        if (blank.length() > 0) {
            out.add(new Token(Type.BLANK, blank.toString()));
            blank.setLength(0);
        }
    }

    // ------------------------------------------------------------------
    // Scanners
    // ------------------------------------------------------------------

    /** Scans a token that starts on a letter or digit; returns the new offset. */
    private static int scanWordish(String text, int start, List<Token> out, StringBuilder blank) {
        int n = text.length();
        int core = start;
        while (core < n && isWordChar(text.charAt(core))) core++;

        // scheme:// — emitted on its own; whatever follows is scanned as usual, which is
        // why http://localhost:5432/db yields an asciiword rather than a host.
        if (core + 2 < n && text.charAt(core) == ':' && text.charAt(core + 1) == '/'
                && text.charAt(core + 2) == '/' && isAllAlpha(text, start, core)) {
            flush(blank, out);
            out.add(new Token(Type.PROTOCOL, text.substring(start, core + 3)));
            return core + 3;
        }

        // local@host
        int at = scanEmail(text, start);
        if (at > start) {
            flush(blank, out);
            out.add(new Token(Type.EMAIL, text.substring(start, at)));
            return at;
        }

        // A dotted sequence: host, or a file when it is not a usable host name.
        int dotted = scanDotted(text, core);
        if (dotted > core) {
            // A :port suffix belongs to the host, as in Foo.java:597.
            if (dotted + 1 < n && text.charAt(dotted) == ':' && isDigit(text.charAt(dotted + 1))) {
                int k = dotted + 1;
                while (k < n && isDigit(text.charAt(k))) k++;
                dotted = k;
            }
            String whole = text.substring(start, dotted);
            boolean host = isHostName(whole);
            int path = scanPath(text, dotted, host);
            flush(blank, out);
            if (host) {
                if (path > dotted) {
                    out.add(new Token(Type.URL, text.substring(start, path)));
                    out.add(new Token(Type.HOST, whole));
                    out.add(new Token(Type.URL_PATH, text.substring(dotted, path)));
                    return path;
                }
                out.add(new Token(Type.HOST, whole));
                return dotted;
            }
            out.add(new Token(Type.FILE, text.substring(start, Math.max(path, dotted))));
            return Math.max(path, dotted);
        }

        // A bare word followed by a path is a file name, as in 5432/db.
        int wordPath = scanPath(text, core);
        if (wordPath > core) {
            flush(blank, out);
            out.add(new Token(Type.FILE, text.substring(start, wordPath)));
            return wordPath;
        }

        String word = text.substring(start, core);

        // A hyphenated compound only forms when the first part contains a letter;
        // after a bare number a hyphen is a sign, as in 2024-01-15.
        if (hasLetter(word)) {
            List<String> parts = new ArrayList<String>();
            parts.add(word);
            int p = core;
            while (p + 1 < n && text.charAt(p) == '-' && isWordChar(text.charAt(p + 1))) {
                int q = p + 1;
                while (q < n && isWordChar(text.charAt(q))) q++;
                String part = text.substring(p + 1, q);
                // A purely numeric part is a signed number, not a compound part: row-3.
                if (!hasLetter(part)) break;
                parts.add(part);
                p = q;
            }
            if (parts.size() > 1) {
                flush(blank, out);
                out.add(new Token(compoundType(parts), text.substring(start, p)));
                int off = start;
                for (int k = 0; k < parts.size(); k++) {
                    if (k > 0) out.add(new Token(Type.BLANK, "-"));
                    out.add(new Token(partType(parts.get(k)), parts.get(k)));
                    off += parts.get(k).length() + 1;
                }
                return p;
            }
        }

        flush(blank, out);
        out.add(new Token(classifyWord(word), word));
        return core;
    }

    /**
     * Scans a number: {@code uint}, {@code int} (signed), {@code float} ({@code 3.14},
     * {@code +2.5}), {@code sfloat} ({@code 1.5e-3}, {@code -1.5E+10}) or {@code version}
     * ({@code 192.168.1.1}). Returns -1 when the run is not a number after all, e.g.
     * {@code 123abc}, which is a numword.
     */
    private static int scanNumber(String text, int start, List<Token> out, StringBuilder blank) {
        int n = text.length();
        int j = start;
        boolean signed = false;
        if (text.charAt(j) == '+' || text.charAt(j) == '-') { signed = true; j++; }
        if (j >= n || !isDigit(text.charAt(j))) return -1;
        while (j < n && isDigit(text.charAt(j))) j++;

        boolean fraction = false;
        if (j + 1 < n && text.charAt(j) == '.' && isDigit(text.charAt(j + 1))) {
            j++;
            while (j < n && isDigit(text.charAt(j))) j++;
            fraction = true;
        }

        // Three or more dot-separated numeric groups is a version, not a decimal.
        if (!signed && fraction && j + 1 < n && text.charAt(j) == '.' && isDigit(text.charAt(j + 1))) {
            while (j + 1 < n && text.charAt(j) == '.' && isDigit(text.charAt(j + 1))) {
                j++;
                while (j < n && isDigit(text.charAt(j))) j++;
            }
            if (j >= n || !isWordChar(text.charAt(j))) {
                flush(blank, out);
                out.add(new Token(Type.VERSION, text.substring(start, j)));
                return j;
            }
            return -1;
        }

        if (j < n && (text.charAt(j) == 'e' || text.charAt(j) == 'E')) {
            int k = j + 1;
            if (k < n && (text.charAt(k) == '+' || text.charAt(k) == '-')) k++;
            int expStart = k;
            while (k < n && isDigit(text.charAt(k))) k++;
            if (k > expStart && (k >= n || !isWordChar(text.charAt(k)))) {
                flush(blank, out);
                out.add(new Token(Type.SFLOAT, text.substring(start, k)));
                return k;
            }
        }

        // Letters immediately after the digits make this a word, not a number.
        if (j < n && Character.isLetter(text.charAt(j))) return -1;

        // A number followed by a path is a file name, as in the 5432/db of a URL.
        int path = scanPath(text, j);
        if (path > j) {
            flush(blank, out);
            out.add(new Token(Type.FILE, text.substring(start, path)));
            return path;
        }

        flush(blank, out);
        out.add(new Token(fraction ? Type.FLOAT : (signed ? Type.INT : Type.UINT),
                text.substring(start, j)));
        return j;
    }

    /** Consumes {@code (.label)+} starting at {@code i}; returns {@code i} when there is none. */
    private static int scanDotted(String text, int i) {
        int n = text.length();
        int end = i;
        while (end + 1 < n && text.charAt(end) == '.' && isWordChar(text.charAt(end + 1))) {
            int j = end + 1;
            while (j < n && (isWordChar(text.charAt(j)) || text.charAt(j) == '-')) j++;
            // A trailing hyphen is not part of the label.
            while (j > end + 1 && text.charAt(j - 1) == '-') j--;
            end = j;
        }
        return end;
    }

    /** Consumes a {@code /path} suffix; returns {@code i} when there is none. */
    private static int scanPath(String text, int i) {
        return scanPath(text, i, false);
    }

    /**
     * Consumes a {@code /path} suffix; returns {@code i} when there is none. A path that
     * follows a real host name may carry query syntax ({@code ?}, {@code =}, {@code &}),
     * which a plain file name may not.
     */
    private static int scanPath(String text, int i, boolean urlPath) {
        int n = text.length();
        if (i >= n || text.charAt(i) != '/') return i;
        int end = i;
        boolean sawAlnum = false;
        while (end < n && (urlPath ? isUrlPathChar(text.charAt(end)) : isFilePathChar(text.charAt(end)))) {
            if (Character.isLetterOrDigit(text.charAt(end))) sawAlnum = true;
            end++;
        }
        while (end > i && isTrailingPunctuation(text.charAt(end - 1))) end--;
        return sawAlnum && end > i + 1 ? end : i;
    }

    /** Scans {@code local@host}; returns the start offset when this is not an address. */
    private static int scanEmail(String text, int start) {
        int n = text.length();
        int i = start;
        while (i < n && (isWordChar(text.charAt(i)) || text.charAt(i) == '.'
                || text.charAt(i) == '_' || text.charAt(i) == '-' || text.charAt(i) == '+')) {
            i++;
        }
        if (i >= n || text.charAt(i) != '@' || i == start) return start;
        int hostStart = i + 1;
        int j = hostStart;
        while (j < n && (isWordChar(text.charAt(j)) || text.charAt(j) == '-')) j++;
        if (j == hostStart) return start;
        int dotted = scanDotted(text, j);
        if (dotted == j) return start;   // an address needs a dotted host
        return dotted;
    }

    // ------------------------------------------------------------------
    // Classification
    // ------------------------------------------------------------------


    /**
     * A dotted run is a host name only when no label is empty or purely numeric and the
     * last label is at least two characters. Everything else — v1.2.3, a.b — is a file.
     */
    private static boolean isHostName(String s) {
        int colon = s.indexOf(':');
        String host = colon >= 0 ? s.substring(0, colon) : s;
        String[] parts = host.split("\\.", -1);
        if (parts.length < 2) return false;
        for (String p : parts) {
            if (p.isEmpty() || isNumeric(p)) return false;
        }
        return parts[parts.length - 1].length() >= 2;
    }

    private static Type classifyWord(String w) {
        if (isNumeric(w)) return Type.UINT;
        if (isScientific(w)) return Type.SFLOAT;
        boolean digits = hasDigit(w);
        boolean nonAscii = !isAscii(w);
        if (digits) return Type.NUMWORD;
        return nonAscii ? Type.WORD : Type.ASCIIWORD;
    }

    private static Type compoundType(List<String> parts) {
        boolean digits = false, nonAscii = false;
        for (String p : parts) {
            digits |= hasDigit(p);
            nonAscii |= !isAscii(p);
        }
        if (digits) return Type.NUMHWORD;
        return nonAscii ? Type.HWORD : Type.ASCIIHWORD;
    }

    private static Type partType(String p) {
        if (hasDigit(p)) return Type.HWORD_NUMPART;
        return isAscii(p) ? Type.HWORD_ASCIIPART : Type.HWORD_PART;
    }

    // ------------------------------------------------------------------
    // Character classes
    // ------------------------------------------------------------------

    private static boolean isWordChar(char c) {
        // Underscore is not a word character to PG's parser: foo_bar is two tokens.
        return Character.isLetterOrDigit(c);
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isFilePathChar(char c) {
        return Character.isLetterOrDigit(c) || "/._-~".indexOf(c) >= 0;
    }

    private static boolean isUrlPathChar(char c) {
        return Character.isLetterOrDigit(c) || "/._-~%&=?+#".indexOf(c) >= 0;
    }

    private static boolean isTrailingPunctuation(char c) {
        return c == '.' || c == ',' || c == ';' || c == ':' || c == '!' || c == '?' || c == '/';
    }

    private static boolean isAllAlpha(String s, int from, int to) {
        for (int i = from; i < to; i++) {
            if (!Character.isLetter(s.charAt(i))) return false;
        }
        return to > from;
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (!isDigit(s.charAt(i))) return false;
        }
        return true;
    }

    private static boolean hasDigit(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (isDigit(s.charAt(i))) return true;
        }
        return false;
    }

    private static boolean hasLetter(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isLetter(s.charAt(i))) return true;
        }
        return false;
    }

    private static boolean isAscii(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > 127) return false;
        }
        return true;
    }

    /** {@code 1e10}, {@code 1.5e-3}: PG's sfloat. */
    private static boolean isScientific(String s) {
        int i = 0, n = s.length();
        int digits = 0;
        while (i < n && isDigit(s.charAt(i))) { i++; digits++; }
        if (digits == 0 || i >= n) return false;
        char e = s.charAt(i);
        if (e != 'e' && e != 'E') return false;
        i++;
        if (i < n && (s.charAt(i) == '+' || s.charAt(i) == '-')) i++;
        int expDigits = 0;
        while (i < n && isDigit(s.charAt(i))) { i++; expDigits++; }
        return expDigits > 0 && i == n;
    }
}
