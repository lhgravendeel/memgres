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

    /**
     * The advanced regular expression a {@code SIMILAR TO} pattern stands for.
     *
     * <p>SQL's pattern language is a regular expression with a smaller vocabulary, not a LIKE
     * pattern with extras: {@code %} and {@code _} stand for any run and any one character, the
     * alternation, grouping and repetition operators mean what they mean in a regular
     * expression, and a bracket expression is one. Everything else stands for itself -- a dot is
     * a dot, and a backslash before a letter is a backslash followed by that letter. Building
     * the pattern by quoting some characters and passing others through raw let both halves of
     * that go wrong at once: a quoted bracket expression stopped naming a class, and a raw dot
     * started matching anything.
     *
     * @param escape the character that makes the next one stand for itself, or {@code null}
     *     where the pattern has none
     */
    static String fromSimilarTo(String pattern, Character escape) {
        StringBuilder out = new StringBuilder(pattern.length() + 8);
        out.append("^(?:");
        int i = 0;
        while (i < pattern.length()) {
            char c = pattern.charAt(i);
            if (escape != null && c == escape.charValue()) {
                // An escape at the very end has nothing to make literal and is dropped.
                if (i + 1 < pattern.length()) appendLiteral(out, pattern.charAt(i + 1));
                i += 2;
                continue;
            }
            if (c == '%') {
                out.append(".*");
                i++;
            } else if (c == '_') {
                out.append('.');
                i++;
            } else if (c == '{' && isQuantifier(pattern, i)) {
                // A repetition count is copied whole: its digits and comma are part of the
                // count and not characters to be matched.
                int end = pattern.indexOf('}', i);
                out.append(pattern, i, end + 1);
                i = end + 1;
            } else if (c == '|' || c == '(' || c == ')' || c == '*' || c == '+' || c == '?'
                    || c == '{' || c == '}') {
                out.append(c);
                i++;
            } else if (c == '[') {
                // A bracket expression is copied through as it stands, so that the class names
                // in it are read by the same reader that reads them everywhere else.
                int end = bracketEnd(pattern, i);
                out.append(pattern, i, end);
                i = end;
            } else {
                appendLiteral(out, c);
                i++;
            }
        }
        return out.append(")$").toString();
    }

    /** One character, spelled so that it stands for itself. */
    private static void appendLiteral(StringBuilder out, char c) {
        if (!Character.isLetterOrDigit(c) && c != '_') out.append('\\');
        out.append(c);
    }

    /** The index just past the bracket expression beginning here. */
    private static int bracketEnd(String pattern, int start) {
        int i = start + 1;
        if (i < pattern.length() && pattern.charAt(i) == '^') i++;
        if (i < pattern.length() && pattern.charAt(i) == ']') i++;
        while (i < pattern.length()) {
            char c = pattern.charAt(i);
            if (c == '[' && i + 1 < pattern.length() && ":.=".indexOf(pattern.charAt(i + 1)) >= 0) {
                int inner = pattern.indexOf(String.valueOf(pattern.charAt(i + 1)) + "]", i + 2);
                if (inner < 0) throw bracketsNotBalanced();
                i = inner + 2;
                continue;
            }
            if (c == ']') return i + 1;
            i++;
        }
        throw bracketsNotBalanced();
    }

    /**
     * The escape a {@code SIMILAR TO} or {@code LIKE} clause names.
     *
     * @return the character, or {@code null} where the clause names none
     */
    static Character escapeCharacter(String given) {
        if (given == null || given.isEmpty()) return null;
        if (given.length() != 1) {
            MemgresException e = new MemgresException("invalid escape string", "22025");
            e.setHint("Escape string must be empty or one character.");
            throw e;
        }
        return Character.valueOf(given.charAt(0));
    }

    /** An escape PostgreSQL does not define is an error rather than something to guess at. */
    private static MemgresException invalidEscape() {
        return new MemgresException(
                "invalid regular expression: invalid escape \\ sequence", "2201B");
    }

    /**
     * The escapes PostgreSQL gives a meaning to, other than the class shorthands and the
     * constraint escapes handled separately. Everything else that is a letter or a digit after a
     * backslash is one it does not define.
     */
    private static final String KNOWN_ESCAPES = "abBefnrtuUvxcdDsSwWAmMyYZ0123456789";

    private static String translate(String p, Options opts) {
        StringBuilder out = new StringBuilder(p.length() + 8);
        int i = 0;
        while (i < p.length()) {
            char c = p.charAt(i);
            if (c == '\\') {
                // A backslash at the very end escapes nothing. Passing it through as a literal
                // backslash accepted a pattern PostgreSQL refuses.
                if (i + 1 >= p.length()) throw invalidEscape();
                char next = p.charAt(i + 1);
                i += 2;
                // The word-boundary escapes have no Java spelling of their own.
                if (next == 'm' || next == 'M' || next == 'y') {
                    out.append("\\b");
                } else if (next == 'Y') {
                    out.append("\\B");
                } else if (next == 'Z') {
                    // PostgreSQL's end-of-string anchor is the end; Java's \Z also matches
                    // before a final newline, which is a different place.
                    out.append("\\z");
                } else if (next == 'd' || next == 'D' || next == 's' || next == 'S'
                        || next == 'w' || next == 'W') {
                    out.append(classEscape(next));
                } else if (next >= '0' && next <= '9') {
                    i = appendDigitEscape(p, i - 2, out);
                } else if (Character.isLetterOrDigit(next) && KNOWN_ESCAPES.indexOf(next) < 0) {
                    throw invalidEscape();
                } else {
                    out.append(c).append(next);
                }
            } else if (c == '[') {
                i = appendBracketExpression(p, i, out);
            } else if (c == '(' && p.startsWith("(?#", i)) {
                // A comment says nothing about what matches, so it is removed rather than
                // handed to Java, which has no such construct and refuses the whole pattern.
                int close = p.indexOf(')', i + 3);
                if (close < 0) throw new MemgresException(
                        "invalid regular expression: parentheses () not balanced", "2201B");
                i = close + 1;
            } else if (c == '(' && p.startsWith("(?", i) && !isKnownGroupPrefix(p, i)) {
                // (?: (?= and (?! are the only ones PostgreSQL has. Java has a dozen more, and
                // letting them through accepted patterns a real server refuses.
                throw new MemgresException(
                        "invalid regular expression: quantifier operand invalid", "2201B");
            } else if (c == '{' && !isQuantifier(p, i)) {
                // A brace that does not open a repetition count is an ordinary character.
                out.append("\\{");
                i++;
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

    /** The class a shorthand escape stands for, as a whole bracket expression. */
    private static String classEscape(char letter) {
        switch (letter) {
            case 'd': return "[" + DIGIT + "]";
            case 'D': return "[^" + DIGIT + "]";
            case 's': return "[" + SPACE + "]";
            case 'S': return "[^" + SPACE + "]";
            case 'w': return "[" + WORD + "]";
            default: return "[^" + WORD + "]";
        }
    }

    /**
     * A backslash followed by digits: either a back reference or a character named in octal.
     *
     * <p>Three octal digits are always the character, which is the spelling
     * {@code '\141'} uses for {@code a}; Java writes that with a leading zero and read the same
     * text as a reference to group 141. One or two digits stay a back reference.
     */
    private static int appendDigitEscape(String p, int at, StringBuilder out) {
        int end = at + 1;
        while (end < p.length() && end - at <= 3 && p.charAt(end) >= '0' && p.charAt(end) <= '7') {
            end++;
        }
        if (end - at == 4) {
            out.append("\\0").append(p, at + 1, end);
            return end;
        }
        out.append('\\').append(p.charAt(at + 1));
        return at + 2;
    }

    /** Whether {@code (?} at this point opens one of the groups PostgreSQL knows. */
    private static boolean isKnownGroupPrefix(String p, int at) {
        if (at + 2 >= p.length()) return false;
        char kind = p.charAt(at + 2);
        if (kind == ':' || kind == '=' || kind == '!') return true;
        // Lookbehind is written (?<= or (?<! . A name after the bracket is Java's, not
        // PostgreSQL's, and a pattern using one is refused rather than quietly accepted.
        return kind == '<' && at + 3 < p.length()
                && (p.charAt(at + 3) == '=' || p.charAt(at + 3) == '!');
    }

    /**
     * Whether a brace opens a repetition count, which is {@code {m}}, {@code {m,}} or
     * {@code {m,n}} -- always with a number first. {@code {,3}} names no count and is three
     * ordinary characters.
     */
    private static boolean isQuantifier(String p, int at) {
        int i = at + 1;
        int digits = 0;
        while (i < p.length() && Character.isDigit(p.charAt(i))) {
            i++;
            digits++;
        }
        if (digits == 0) return false;
        if (i < p.length() && p.charAt(i) == ',') {
            i++;
            while (i < p.length() && Character.isDigit(p.charAt(i))) i++;
        }
        return i < p.length() && p.charAt(i) == '}';
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
                char next = p.charAt(i + 1);
                if (next == 'd' || next == 'D' || next == 's' || next == 'S'
                        || next == 'w' || next == 'W') {
                    // A shorthand inside a bracket expression contributes its members. A
                    // negated one stays a class of its own, which Java reads as a union.
                    String members = classEscape(next);
                    if (members.startsWith("[^")) cls.append(members);
                    else cls.append(members, 1, members.length() - 1);
                } else if (next == 'b') {
                    // \b is a word boundary outside brackets and a backspace inside one, which
                    // is a character Java refuses to spell that way.
                    cls.append("\\x08");
                } else {
                    cls.append(c).append(next);
                }
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

    // ------------------------------------------------------------------ classes
    //
    // What each named class holds, written as the contents of a Java character class so that it
    // can be dropped into a bracket expression or wrapped in one on its own.
    //
    // Java's own \p{Alpha}, \p{Digit} and friends are the ASCII ones unless the whole pattern is
    // compiled with UNICODE_CHARACTER_CLASS, and that switch is too blunt: it would make
    // [[:digit:]] hold every decimal digit in Unicode, where PostgreSQL's holds 0 to 9 alone.
    // These are the memberships the reference server answers, class by class.

    /** Digits: the ten, and no others -- an Arabic-Indic five is not one of these. */
    private static final String DIGIT = "0-9";

    /**
     * Letters: everything Unicode calls alphabetic, and every decimal digit that is not one of
     * the ten. The second half is not an accident of the tables -- a digit outside ASCII is a
     * letter to PostgreSQL and not a digit, which is why {@code [[:alnum:]]} holds it either way.
     */
    private static final String ALPHA = "\\p{IsAlphabetic}[\\p{Nd}&&[^0-9]]";

    private static final String ALNUM = ALPHA + DIGIT;

    /** A word character: what a letter or digit is, and the underscore. */
    private static final String WORD = ALNUM + "_";

    /** Both cases hold the titlecase letters, which are at once upper and lower. */
    private static final String UPPER = "\\p{IsUppercase}\\p{Lt}";
    private static final String LOWER = "\\p{IsLowercase}\\p{Lt}";

    /**
     * Space: Unicode's whitespace less the non-breaking kinds, which are there precisely so as
     * not to be treated as a place where text may be broken.
     */
    private static final String SPACE = "[\\p{IsWhite_Space}&&[^\\u00a0\\u2007\\u202f]]";

    private static final String BLANK = " \\t";
    private static final String CNTRL = "\\p{Cntrl}";
    private static final String XDIGIT = "0-9A-Fa-f";
    /** Everything that is not a control character prints; the space among them is not a graph. */
    private static final String PRINT = "\\P{Cntrl}";
    private static final String GRAPH = "[\\P{Cntrl}&&[^" + SPACE + "]]";
    /** Punctuation is what is left of the graphs once the letters and digits are taken out. */
    private static final String PUNCT = "[" + GRAPH + "&&[^" + ALNUM + "]]";

    private static String posixClass(String name) {
        if (name.equals("alpha")) return ALPHA;
        if (name.equals("digit")) return DIGIT;
        if (name.equals("alnum")) return ALNUM;
        if (name.equals("upper")) return UPPER;
        if (name.equals("lower")) return LOWER;
        if (name.equals("space")) return SPACE;
        if (name.equals("print")) return PRINT;
        if (name.equals("punct")) return PUNCT;
        if (name.equals("cntrl")) return CNTRL;
        if (name.equals("xdigit")) return XDIGIT;
        if (name.equals("graph")) return GRAPH;
        if (name.equals("blank")) return BLANK;
        if (name.equals("ascii")) return "\\p{ASCII}";
        if (name.equals("word")) return WORD;
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
