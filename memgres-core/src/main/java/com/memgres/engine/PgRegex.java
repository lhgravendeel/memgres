package com.memgres.engine;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * PostgreSQL's advanced regular expressions expressed as {@link java.util.regex} patterns.
 *
 * The two engines disagree in four places. PostgreSQL matches newline-insensitively by
 * default, so a dot matches a newline and {@code ^}/{@code $} anchor the whole string;
 * Java's default is the opposite for the dot. PostgreSQL spells its newline-sensitivity
 * choice as one of the option letters {@code n}, {@code p}, {@code s} and {@code w}, which
 * may appear either in a function's flags argument or in an embedded option director at the
 * front of the pattern — a spelling Java's inline modifiers do not share. Bracket
 * expressions name their character classes the POSIX way. And an option letter PostgreSQL
 * does not know is an error rather than something to ignore.
 */
final class PgRegex {

    private PgRegex() {
    }

    /** The ARE option letters, valid both in a flags argument and in an embedded director. */
    private static final String OPTION_LETTERS = "bceimnpqstwx";

    /** The options one pattern is compiled under. */
    static final class Options {
        boolean ignoreCase;
        boolean expanded;
        boolean literal;
        /** PG's default is newline-insensitive matching, where a dot matches a newline too. */
        boolean dotMatchesNewline = true;
        boolean lineAnchors;
        boolean global;
    }

    /**
     * Read a function's flags argument. {@code allowGlobal} is false for the functions that
     * take flags but have no use for {@code g}; PostgreSQL names the caller when it complains.
     */
    static Options parseFlags(String flags, boolean allowGlobal, String functionName) {
        Options opts = new Options();
        if (flags == null) return opts;
        for (int i = 0; i < flags.length(); i++) {
            char c = flags.charAt(i);
            if (c == 'g') {
                if (!allowGlobal) {
                    throw new MemgresException(
                            functionName + "() does not support the \"global\" option", "22023");
                }
                opts.global = true;
            } else if (!applyOption(opts, c)) {
                throw new MemgresException(
                        "invalid regular expression option: \"" + c + "\"", "22023");
            }
        }
        return opts;
    }

    static Options caseInsensitive() {
        Options opts = new Options();
        opts.ignoreCase = true;
        return opts;
    }

    static Pattern compile(String pattern) {
        return compile(pattern, new Options());
    }

    static Pattern compile(String pattern, Options opts) {
        String body = pattern;
        // The *** directors are the only way to ask for a literal or an ARE up front
        if (body.startsWith("***=")) {
            return Pattern.compile(Pattern.quote(body.substring(4)), javaFlags(opts));
        }
        if (body.startsWith("***:")) {
            body = body.substring(4);
        }
        body = applyEmbeddedOptions(body, opts);
        if (opts.literal) {
            return Pattern.compile(Pattern.quote(body), javaFlags(opts));
        }
        try {
            return Pattern.compile(translate(body, opts), javaFlags(opts));
        } catch (PatternSyntaxException e) {
            throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B");
        }
    }

    private static boolean applyOption(Options opts, char c) {
        switch (c) {
            case 'b': // basic RE
            case 'e': // extended RE
                // Accepted and matched as an ARE: the constructs the older syntaxes spell
                // differently are rare, and rejecting the letter would be worse.
                return true;
            case 'c': opts.ignoreCase = false; return true;
            case 'i': opts.ignoreCase = true; return true;
            case 'm': // historical synonym for n
            case 'n': opts.dotMatchesNewline = false; opts.lineAnchors = true; return true;
            case 'p': opts.dotMatchesNewline = false; opts.lineAnchors = false; return true;
            case 'q': opts.literal = true; return true;
            case 's': opts.dotMatchesNewline = true; opts.lineAnchors = false; return true;
            case 't': opts.expanded = false; return true;
            case 'w': opts.dotMatchesNewline = true; opts.lineAnchors = true; return true;
            case 'x': opts.expanded = true; return true;
            default: return false;
        }
    }

    /**
     * Consume an embedded option director. PostgreSQL only recognises one, and only at the
     * very front; anything else beginning {@code (?} is an ordinary Java-style construct
     * such as {@code (?:...)} or a lookahead, which is passed through untouched.
     */
    private static String applyEmbeddedOptions(String body, Options opts) {
        if (!body.startsWith("(?")) return body;
        int close = body.indexOf(')', 2);
        if (close < 0) return body;
        String letters = body.substring(2, close);
        if (letters.isEmpty()) return body;
        for (int i = 0; i < letters.length(); i++) {
            char c = letters.charAt(i);
            if (OPTION_LETTERS.indexOf(c) >= 0) continue;
            if (Character.isLetter(c)) {
                throw new MemgresException(
                        "invalid regular expression: invalid embedded option", "2201B");
            }
            return body; // not a director at all
        }
        for (int i = 0; i < letters.length(); i++) {
            applyOption(opts, letters.charAt(i));
        }
        return body.substring(close + 1);
    }

    private static String translate(String p, Options opts) {
        StringBuilder out = new StringBuilder(p.length() + 8);
        int i = 0;
        while (i < p.length()) {
            char c = p.charAt(i);
            if (c == '\\') {
                if (i + 1 >= p.length()) {
                    out.append("\\\\");
                    i++;
                    continue;
                }
                char next = p.charAt(i + 1);
                // PG's word-boundary escapes have no Java spelling of their own
                if (next == 'm' || next == 'M' || next == 'y') out.append("\\b");
                else if (next == 'Y') out.append("\\B");
                else out.append(c).append(next);
                i += 2;
            } else if (c == '[') {
                i = appendBracketExpression(p, i, out);
            } else if (c == '$' && !opts.lineAnchors) {
                // Java's $ also matches before a trailing newline; PG's anchors the very end
                out.append("\\z");
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** Translate one bracket expression; returns the index just past its closing bracket. */
    private static int appendBracketExpression(String p, int start, StringBuilder out) {
        // These two are word boundaries wearing a bracket expression's clothes
        if (p.startsWith("[[:<:]]", start) || p.startsWith("[[:>:]]", start)) {
            out.append("\\b");
            return start + 7;
        }
        StringBuilder cls = new StringBuilder("[");
        int i = start + 1;
        if (i < p.length() && p.charAt(i) == '^') {
            cls.append('^');
            i++;
        }
        if (i < p.length() && p.charAt(i) == ']') {
            cls.append("\\]"); // a bracket in first position is a literal one
            i++;
        }
        boolean closed = false;
        while (i < p.length()) {
            char c = p.charAt(i);
            if (c == ']') {
                closed = true;
                i++;
                break;
            }
            if (c == '[' && i + 1 < p.length() && ":.=".indexOf(p.charAt(i + 1)) >= 0) {
                char kind = p.charAt(i + 1);
                int end = p.indexOf(String.valueOf(kind) + ']', i + 2);
                if (end < 0) throw bracketsNotBalanced();
                String inner = p.substring(i + 2, end);
                // A collating element or equivalence class stands for the character itself
                cls.append(kind == ':' ? posixClass(inner) : Pattern.quote(inner));
                i = end + 2;
                continue;
            }
            if (c == '\\' && i + 1 < p.length()) {
                cls.append(c).append(p.charAt(i + 1));
                i += 2;
                continue;
            }
            // Java reads these as nesting and intersection where PG reads plain characters
            if (c == '[' || c == '&') cls.append('\\');
            cls.append(c);
            i++;
        }
        if (!closed) throw bracketsNotBalanced();
        cls.append(']');
        out.append(cls);
        return i;
    }

    private static String posixClass(String name) {
        if (name.equals("alpha")) return "\\p{Alpha}";
        if (name.equals("digit")) return "\\p{Digit}";
        if (name.equals("alnum")) return "\\p{Alnum}";
        if (name.equals("upper")) return "\\p{Upper}";
        if (name.equals("lower")) return "\\p{Lower}";
        if (name.equals("space")) return "\\p{Space}";
        if (name.equals("print")) return "\\p{Print}";
        if (name.equals("punct")) return "\\p{Punct}";
        if (name.equals("cntrl")) return "\\p{Cntrl}";
        if (name.equals("xdigit")) return "\\p{XDigit}";
        if (name.equals("graph")) return "\\p{Graph}";
        if (name.equals("blank")) return "\\p{Blank}";
        if (name.equals("ascii")) return "\\p{ASCII}";
        if (name.equals("word")) return "\\w";
        throw new MemgresException("invalid regular expression: invalid character class", "2201B");
    }

    private static MemgresException bracketsNotBalanced() {
        return new MemgresException("invalid regular expression: brackets [] not balanced", "2201B");
    }

    private static int javaFlags(Options opts) {
        int flags = 0;
        if (opts.ignoreCase) flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
        if (opts.expanded) flags |= Pattern.COMMENTS;
        if (opts.dotMatchesNewline) flags |= Pattern.DOTALL;
        if (opts.lineAnchors) flags |= Pattern.MULTILINE;
        return flags;
    }

    /**
     * Split as {@code regexp_split_to_table} does. PostgreSQL ignores a zero-length match
     * that sits where the previous match ended or at the very end of the string, so an
     * empty pattern splits into the characters themselves and nothing more.
     */
    static java.util.List<String> split(String input, Pattern pattern) {
        java.util.List<String> fields = new java.util.ArrayList<>();
        java.util.regex.Matcher m = pattern.matcher(input);
        int fieldStart = 0;
        while (m.find()) {
            boolean degenerate = m.start() == m.end();
            if (degenerate && (m.start() == fieldStart || m.start() == input.length())) {
                continue;
            }
            fields.add(input.substring(fieldStart, m.start()));
            fieldStart = m.end();
        }
        fields.add(input.substring(fieldStart));
        return fields;
    }
}
