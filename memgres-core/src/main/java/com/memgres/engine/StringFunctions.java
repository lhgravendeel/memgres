package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;
import java.util.*;

/**
 * String function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class StringFunctions {

    /**
     * PostgreSQL builds each of these results in a single allocation, so it refuses up front any
     * request that could not fit in one rather than trying and running the backend out of memory.
     * The ceiling is MaxAllocSize less the varlena header; a pad length is in characters, which
     * UTF-8 can widen fourfold, while repeat() counts the bytes it is about to write.
     */
    private static final int MAX_RESULT_BYTES = 0x3fffffff - 4;
    private static final int MAX_PAD_LENGTH = MAX_RESULT_BYTES / 4;

    private static MemgresException requestedLengthTooLarge() {
        return new MemgresException("requested length too large", "54000");
    }

    /** Bytes {@code s} occupies in UTF-8, which is the length PostgreSQL budgets against. */
    private static long utf8Length(String s) {
        long n = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x80) n += 1;
            else if (c < 0x800) n += 2;
            else if (Character.isHighSurrogate(c)) { n += 4; i++; }
            else n += 3;
        }
        return n;
    }

    /**
     * PostgreSQL declares these routines with an {@code integer} count and has no {@code bigint}
     * overload, so a wider count is a function that does not exist rather than a value to narrow.
     * Narrowing it silently turned a count of four billion into an empty string.
     *
     * @param index which argument carries the count
     * @return the count once it is known to fit, or null when the argument itself is null
     */
    private Integer countArgument(FunctionCallExpr fn, RowContext ctx, int index, String name) {
        Object raw = executor.evalExpr(fn.args().get(index), ctx);
        if (raw == null) return null; // strict: the whole call is null
        long value;
        try {
            value = executor.toLong(raw);
        } catch (NumberFormatException e) {
            // A count that is not a number at all is bad input, not a missing overload.
            throw new MemgresException(
                    "invalid input syntax for type integer: \"" + raw + "\"", "22P02");
        }
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new MemgresException("function " + name + "("
                    + argumentTypeList(fn, ctx, index) + ") does not exist", "42883");
        }
        return (int) value;
    }

    /**
     * Render the argument types the way PostgreSQL reports them when no overload matches. An
     * unadorned literal is still {@code unknown} at that point, which is what it calls it.
     */
    private String argumentTypeList(FunctionCallExpr fn, RowContext ctx, int wideIndex) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fn.args().size(); i++) {
            if (i > 0) sb.append(", ");
            if (i == wideIndex) {
                sb.append("bigint");
                continue;
            }
            Expression arg = fn.args().get(i);
            if (arg instanceof Literal
                    && ((Literal) arg).literalType() == Literal.LiteralType.STRING) {
                sb.append("unknown");
                continue;
            }
            Object val;
            try {
                val = executor.evalExpr(arg, ctx);
            } catch (RuntimeException e) {
                val = null;
            }
            sb.append(runtimeTypeName(val));
        }
        return sb.toString();
    }

    private static String runtimeTypeName(Object val) {
        if (val instanceof Integer || val instanceof Short) return "integer";
        if (val instanceof Long) return "bigint";
        if (val instanceof java.math.BigDecimal) return "numeric";
        if (val instanceof Double || val instanceof Float) return "double precision";
        if (val instanceof Boolean) return "boolean";
        if (val instanceof byte[]) return "bytea";
        return "text";
    }

    /** The four Unicode normalization forms the grammar accepts as bare keywords. */
    private static final java.util.Set<String> NORMALIZATION_FORMS = new java.util.HashSet<>(
            java.util.Arrays.asList("NFC", "NFD", "NFKC", "NFKD"));
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /**
     * Split a qualified name into its parts, taking the quoting off each. In strict mode anything
     * left over after the last part is an error; otherwise the trailing text is simply ignored.
     */
    private static List<Object> parseIdent(String input, boolean strict) {
        List<Object> parts = new ArrayList<Object>();
        int i = 0;
        int n = input.length();
        boolean expectingPart = false;
        while (true) {
            while (i < n && Character.isWhitespace(input.charAt(i))) i++;
            if (i >= n) break;
            if (input.charAt(i) == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                boolean closed = false;
                while (i < n) {
                    char c = input.charAt(i);
                    if (c == '"') {
                        // "" inside a quoted name is one literal quote
                        if (i + 1 < n && input.charAt(i + 1) == '"') { sb.append('"'); i += 2; continue; }
                        i++;
                        closed = true;
                        break;
                    }
                    sb.append(c);
                    i++;
                }
                if (!closed) {
                    throw new MemgresException("string is not a valid identifier: \"" + input + "\"",
                            "22023");
                }
                parts.add(sb.toString());
            } else {
                int start = i;
                while (i < n) {
                    char c = input.charAt(i);
                    if (Character.isLetterOrDigit(c) || c == '_' || c == '$') { i++; continue; }
                    break;
                }
                if (i == start) break;
                parts.add(input.substring(start, i).toLowerCase());
            }
            expectingPart = false;
            while (i < n && Character.isWhitespace(input.charAt(i))) i++;
            // A dot promises another part; if none follows, the name is unfinished.
            if (i < n && input.charAt(i) == '.') { i++; expectingPart = true; continue; }
            break;
        }
        while (i < n && Character.isWhitespace(input.charAt(i))) i++;
        if (parts.isEmpty() || expectingPart || (strict && i < n)) {
            throw new MemgresException("string is not a valid identifier: \"" + input + "\"", "22023");
        }
        return parts;
    }

    private static final Set<String> RESERVED_WORDS = Cols.setOf(
            "select", "from", "where", "insert", "update", "delete", "create", "drop", "alter",
            "table", "index", "view", "and", "or", "not", "null", "true", "false", "in", "is",
            "like", "between", "exists", "case", "when", "then", "else", "end", "as", "on",
            "join", "left", "right", "inner", "outer", "cross", "group", "order", "by", "having",
            "limit", "offset", "union", "intersect", "except", "all", "distinct", "set", "values",
            "into", "primary", "key", "foreign", "references", "constraint", "check", "default",
            "unique", "cascade", "restrict", "grant", "revoke", "begin", "commit", "rollback"
    );

    private static final Set<String> VALID_ENCODINGS = Cols.setOf(
            "UTF8", "UTF-8", "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5",
            "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10",
            "SQL_ASCII", "WIN1250", "WIN1251", "WIN1252", "WIN1253", "WIN1254",
            "WIN1255", "WIN1256", "WIN1257", "WIN1258",
            "EUC_JP", "EUC_CN", "EUC_KR", "EUC_TW",
            "SJIS", "BIG5", "GBK", "GB18030", "JOHAB", "UHC",
            "ISO_8859_5", "ISO_8859_6", "ISO_8859_7", "ISO_8859_8",
            "KOI8R", "KOI8U", "MULE_INTERNAL"
    );

    private final AstExecutor executor;

    StringFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "length":
            case "char_length":
            case "character_length": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof byte[]) return ((byte[]) arg).length; // bytea length = byte count
                // length(text, encoding): PG resolves two-text-arg call to encoding-aware overload
                // which then rejects invalid encoding name with 22023 (invalid_parameter_value)
                if (fn.args().size() > 1 && !(arg instanceof byte[])) {
                    Object enc = executor.evalExpr(fn.args().get(1), ctx);
                    throw new MemgresException("\"" + enc + "\" is not a valid encoding name", "22023");
                }
                if (arg instanceof TsVector) return ((TsVector) arg).length();
                String argStr = arg.toString();
                if (GeometricOperations.isGeometricString(argStr)) {
                    return GeometricOperations.length(argStr);
                }
                // Use codePointCount for proper Unicode character counting (PG counts codepoints, not UTF-16 units)
                return argStr.codePointCount(0, argStr.length());
            }
            case "upper": {
                Expression argExpr = fn.args().get(0);
                // Detect COLLATE wrapping: apply locale-specific uppercase
                boolean asciiOnly = false;
                java.util.Locale collLocale = null;
                if (argExpr instanceof CollateExpr) {
                    CollateExpr ce = (CollateExpr) argExpr;
                    String coll = ce.collation().toLowerCase().replace("\"", "");
                    asciiOnly = coll.equals("c") || coll.equals("posix")
                            || coll.equals("pg_catalog.c") || coll.equals("pg_catalog.posix");
                    if (!asciiOnly) {
                        // Look up user-defined collation for locale-aware upper()
                        Database.CollationDef collDef = executor.database.getCollation(ce.collation());
                        if (collDef != null && collDef.locale != null) {
                            collLocale = parseLocale(collDef.locale);
                        }
                    }
                }
                Object arg = executor.evalExpr(argExpr, ctx);
                if (arg == null) return null;
                if (arg instanceof String && RangeOperations.isMultirangeOrEmpty(((String) arg))) {
                    java.util.List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(((String) arg));
                    if (ranges.isEmpty()) return null;
                    RangeOperations.PgRange last = ranges.get(ranges.size() - 1);
                    return last.isEmpty() ? null : last.upper();
                }
                if (arg instanceof String && RangeOperations.isRangeString(((String) arg))) {
                    String s = (String) arg;
                    RangeOperations.PgRange r = RangeOperations.parse(s);
                    return r.isEmpty() ? null : r.upper();
                }
                if (arg instanceof Number) throw new MemgresException("function upper(integer) does not exist", "42883");
                if (asciiOnly) {
                    // ASCII-only uppercase: only uppercase A-Z range, leave non-ASCII characters unchanged
                    String str = arg.toString();
                    StringBuilder sb = new StringBuilder(str.length());
                    for (int i = 0; i < str.length(); i++) {
                        char c = str.charAt(i);
                        if (c >= 'a' && c <= 'z') {
                            sb.append((char)(c - 32));
                        } else {
                            sb.append(c);
                        }
                    }
                    return sb.toString();
                }
                if (collLocale != null) {
                    return arg.toString().toUpperCase(collLocale);
                }
                return arg.toString().toUpperCase();
            }
            // casefold is lower() done for comparison rather than for display: it folds the
            // characters whose case-insensitive match differs from a simple lowercasing.
            case "casefold": {
                if (fn.args().isEmpty()) {
                    throw new MemgresException("function casefold() does not exist"
                            + "\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object cfArg = executor.evalExpr(fn.args().get(0), ctx);
                if (cfArg == null) return null;
                return cfArg.toString().toLowerCase();
            }
            // parse_ident splits a qualified name into its parts, unquoting each one.
            case "parse_ident": {
                if (fn.args().isEmpty()) {
                    throw new MemgresException("function parse_ident() does not exist"
                            + "\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object piArg = executor.evalExpr(fn.args().get(0), ctx);
                if (piArg == null) return null;
                boolean strict = true;
                if (fn.args().size() > 1) {
                    Object strictArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (strictArg != null) strict = executor.isTruthy(strictArg);
                }
                return parseIdent(piArg.toString(), strict);
            }
            case "lower": {
                if (fn.args().isEmpty()) {
                    throw new MemgresException("function lower() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // Multirange lower() function — returns lower bound of the first sub-range
                if (arg instanceof String && RangeOperations.isMultirangeOrEmpty(((String) arg))) {
                    java.util.List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(((String) arg));
                    if (ranges.isEmpty()) return null;
                    RangeOperations.PgRange first = ranges.get(0);
                    return first.isEmpty() ? null : first.lower();
                }
                // Range lower() function
                if (arg instanceof String && RangeOperations.isRangeString(((String) arg))) {
                    String s = (String) arg;
                    RangeOperations.PgRange r = RangeOperations.parse(s);
                    return r.isEmpty() ? null : r.lower();
                }
                if (arg instanceof Number) throw new MemgresException("function lower(integer) does not exist", "42883");
                {
                    String original = arg.toString();
                    String lowered = original.toLowerCase();
                    // PG does not lowercase U+1E9E (capital sharp S) to U+00DF (ß)
                    if (original.indexOf('\u1E9E') >= 0) {
                        StringBuilder sb = new StringBuilder(lowered.length());
                        int oi = 0;
                        for (int li = 0; li < lowered.length(); li++) {
                            if (lowered.charAt(li) == '\u00DF' && oi < original.length() && original.charAt(oi) == '\u1E9E') {
                                sb.append('\u1E9E');
                            } else {
                                sb.append(lowered.charAt(li));
                            }
                            oi++;
                        }
                        return sb.toString();
                    }
                    return lowered;
                }
            }
            case "trim":
            case "btrim": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // bytea btrim
                if (arg instanceof byte[]) {
                    byte[] data = (byte[]) arg;
                    byte[] trimBytes = fn.args().size() > 1 ? toBytea(executor.evalExpr(fn.args().get(1), ctx)) : new byte[]{0x20};
                    return byteaTrim(data, trimBytes, true, true);
                }
                if (fn.args().size() > 1) {
                    String chars = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                    String s = arg.toString();
                    int start = 0;
                    while (start < s.length() && chars.indexOf(s.charAt(start)) >= 0) start++;
                    int end = s.length() - 1;
                    while (end >= start && chars.indexOf(s.charAt(end)) >= 0) end--;
                    return s.substring(start, end + 1);
                }
                return arg.toString().trim();
            }
            case "concat": {
                StringBuilder sb = new StringBuilder();
                for (Expression arg : fn.args()) {
                    Object val = executor.evalExpr(arg, ctx);
                    if (val != null) sb.append(pgTextOutput(val));
                }
                return sb.toString();
            }
            case "concat_ws": {
                // Arity is validated pre-expansion in FunctionEvaluator (PG has no concat_ws(text)
                // signature). A separator-only arg list reaching here means a VARIADIC empty array
                // expanded to no values → return the empty string, matching PG.
                if (fn.args().isEmpty()) return null;
                Object sep = executor.evalExpr(fn.args().get(0), ctx);
                if (sep == null) return null;
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (int i = 1; i < fn.args().size(); i++) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    if (val != null) {
                        if (!first) sb.append(sep);
                        sb.append(pgTextOutput(val));
                        first = false;
                    }
                }
                return sb.toString();
            }
            case "replace": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object from = executor.evalExpr(fn.args().get(1), ctx);
                Object to = executor.evalExpr(fn.args().get(2), ctx);
                // PG is strict here: a NULL in any argument makes the whole call NULL.
                if (str == null || from == null || to == null) return null;
                // There is nothing to find in an empty needle, so PG returns the input as it
                // stands rather than wedging the replacement between every character.
                if (from.toString().isEmpty()) return str.toString();
                return str.toString().replace(from.toString(), to.toString());
            }
            case "substring":
            case "substr": {
                if (fn.args().size() < 2) {
                    throw new MemgresException("function substring(text) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                // PG rejects non-text types (e.g., substring(1 FROM 1 FOR 1))
                if (str instanceof Number || str instanceof Boolean) {
                    throw new MemgresException("function substring(integer, integer, integer) does not exist", "42883");
                }
                if (str instanceof byte[]) {
                    byte[] bytes = (byte[]) str;
                    int bStart = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                    if (fn.args().size() > 2) {
                        int bLen = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                        if (bLen < 0) {
                            throw new MemgresException("negative substring length not allowed", "22011");
                        }
                        return ByteaOperations.substring(bytes, bStart, bLen);
                    }
                    return ByteaOperations.substring(bytes, bStart, bytes.length);
                }
                // SQL-standard substring(text FROM similar_pattern FOR escape_char): three args
                // where the pattern is a (non-numeric) string, e.g.
                //   substring('foobar' from '%#"o_b#"%' for '#') -> 'oob'.
                if (fn.args().size() == 3) {
                    Object patArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (patArg instanceof String && !((String) patArg).matches("\\s*[+-]?\\d+\\s*")) {
                        Object escArg = executor.evalExpr(fn.args().get(2), ctx);
                        return sqlSimilarSubstring(str.toString(), (String) patArg,
                                escArg == null ? "\\" : escArg.toString());
                    }
                }
                Object arg1 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null) return null;
                // If the second arg is a string (not a number), treat as regex substring
                if (arg1 instanceof String && fn.args().size() == 2) {
                    String pattern = (String) arg1;
                    try {
                        executor.toInt(arg1); // try as int first
                    } catch (Exception e) {
                        // Not an int, treat as regex pattern
                        java.util.regex.Matcher m = PgRegex.compile(pattern).matcher(str.toString());
                        // PG returns group 1 if it exists, else whole match
                        return m.find() ? (m.groupCount() >= 1 && m.group(1) != null ? m.group(1) : m.group()) : null;
                    }
                }
                Integer startBox = countArgument(fn, ctx, 1, name); // PG 1-based position
                if (startBox == null) return null;
                int start = startBox;
                String strVal = str.toString();
                if (fn.args().size() > 2) {
                    Integer countBox = countArgument(fn, ctx, 2, name);
                    if (countBox == null) return null;
                    int count = countBox;
                    if (count < 0) {
                        throw new MemgresException("negative substring length not allowed", "22011");
                    }
                    // PG semantics: end = start + count (1-based exclusive).
                    // Clip start to [1, len+1], clip end to [start, len+1].
                    int end = start + count; // 1-based exclusive
                    int from = Math.max(1, start) - 1; // 0-based inclusive
                    int to = Math.min(strVal.length(), Math.max(0, end - 1)); // 0-based exclusive
                    if (from >= strVal.length() || to <= from) return "";
                    return strVal.substring(from, to);
                }
                int from = Math.max(1, start) - 1; // 0-based
                if (from >= strVal.length()) return "";
                return strVal.substring(from);
            }
            case "substring_similar": {
                // substring(str SIMILAR pattern ESCAPE escape)
                // Extracts the portion of str matched between the two occurrences of escape+"
                if (fn.args().size() < 3) return null;
                Object strObj = executor.evalExpr(fn.args().get(0), ctx);
                Object patObj = executor.evalExpr(fn.args().get(1), ctx);
                Object escObj = executor.evalExpr(fn.args().get(2), ctx);
                if (strObj == null || patObj == null) return null;
                return sqlSimilarSubstring(strObj.toString(), patObj.toString(),
                        escObj == null ? "\\" : escObj.toString());
            }
            case "ltrim": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                // bytea ltrim
                if (str instanceof byte[]) {
                    byte[] data = (byte[]) str;
                    byte[] trimBytes = fn.args().size() > 1 ? toBytea(executor.evalExpr(fn.args().get(1), ctx)) : new byte[]{0x20};
                    return byteaTrim(data, trimBytes, true, false);
                }
                String chars = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : " ";
                String s = str.toString();
                int i = 0;
                while (i < s.length() && chars.indexOf(s.charAt(i)) >= 0) i++;
                return s.substring(i);
            }
            case "rtrim": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                // bytea rtrim
                if (str instanceof byte[]) {
                    byte[] data = (byte[]) str;
                    byte[] trimBytes = fn.args().size() > 1 ? toBytea(executor.evalExpr(fn.args().get(1), ctx)) : new byte[]{0x20};
                    return byteaTrim(data, trimBytes, false, true);
                }
                String chars = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : " ";
                String s = str.toString();
                int i2 = s.length() - 1;
                while (i2 >= 0 && chars.indexOf(s.charAt(i2)) >= 0) i2--;
                return s.substring(0, i2 + 1);
            }
            case "lpad": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Integer lenBox = countArgument(fn, ctx, 1, "lpad");
                if (lenBox == null) return null;
                int len = lenBox;
                if (len < 0) return "";
                String fill = " ";
                if (fn.args().size() > 2) {
                    Object fillVal = executor.evalExpr(fn.args().get(2), ctx);
                    if (fillVal == null) return null;
                    fill = fillVal.toString();
                }
                String s = str.toString();
                if (s.length() >= len) return s.substring(0, len);
                // Nothing to pad with, so PG shortens the request to the input itself — which is
                // also why an outsized length with an empty fill is not a length error.
                if (fill.isEmpty()) return s;
                if (len > MAX_PAD_LENGTH) throw requestedLengthTooLarge();
                StringBuilder sb = new StringBuilder();
                while (sb.length() + s.length() < len) {
                    sb.append(fill);
                }
                return sb.substring(0, len - s.length()) + s;
            }
            case "rpad": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Integer lenBox = countArgument(fn, ctx, 1, "rpad");
                if (lenBox == null) return null;
                int len = lenBox;
                if (len < 0) return "";
                String fill = " ";
                if (fn.args().size() > 2) {
                    Object fillVal = executor.evalExpr(fn.args().get(2), ctx);
                    if (fillVal == null) return null;
                    fill = fillVal.toString();
                }
                String s = str.toString();
                if (s.length() >= len) return s.substring(0, len);
                // Nothing to pad with: PG shortens the request to the input itself, and without
                // that the loop below appends an empty string forever.
                if (fill.isEmpty()) return s;
                if (len > MAX_PAD_LENGTH) throw requestedLengthTooLarge();
                StringBuilder sb = new StringBuilder(s);
                while (sb.length() < len) {
                    sb.append(fill);
                }
                return sb.substring(0, len);
            }
            case "position":
            case "strpos": {
                // position(substring IN string) or strpos(string, substring)
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null || arg2 == null) return null;
                // bytea variants: position(sub bytea IN str bytea) / strpos(str bytea, sub bytea)
                if (arg1 instanceof byte[] || arg2 instanceof byte[]) {
                    byte[] sub;
                    byte[] hay;
                    if (name.equals("position")) { sub = toBytea(arg1); hay = toBytea(arg2); }
                    else { hay = toBytea(arg1); sub = toBytea(arg2); }
                    return byteaIndexOf(hay, sub) + 1;
                }
                if (name.equals("position")) {
                    // POSITION: arg1=substring, arg2=string
                    return arg2.toString().indexOf(arg1.toString()) + 1;
                }
                // strpos: arg1=string, arg2=substring
                return arg1.toString().indexOf(arg2.toString()) + 1;
            }
            case "left": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Integer nBox = countArgument(fn, ctx, 1, "left");
                if (nBox == null) return null;
                int n = nBox;
                String s = str.toString();
                if (n >= 0) return s.substring(0, Math.min(n, s.length()));
                return n + s.length() > 0 ? s.substring(0, s.length() + n) : "";
            }
            case "right": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Integer nBox = countArgument(fn, ctx, 1, "right");
                if (nBox == null) return null;
                int n = nBox;
                String s = str.toString();
                if (n >= 0) return s.substring(Math.max(0, s.length() - n));
                return -n < s.length() ? s.substring(-n) : "";
            }
            case "repeat": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Integer nBox = countArgument(fn, ctx, 1, "repeat");
                if (nBox == null) return null;
                int n = nBox;
                String body = str.toString();
                if ((long) Math.max(0, n) * utf8Length(body) > MAX_RESULT_BYTES) {
                    throw requestedLengthTooLarge();
                }
                return Strs.repeat(body, Math.max(0, n));
            }
            case "reverse": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                return str == null ? null : new StringBuilder(str.toString()).reverse().toString();
            }
            case "split_part": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object delim = executor.evalExpr(fn.args().get(1), ctx);
                Object fieldVal = executor.evalExpr(fn.args().get(2), ctx);
                // PG is strict here: a NULL in any argument makes the whole call NULL, and that
                // is decided before the field position is range-checked.
                if (str == null || delim == null || fieldVal == null) return null;
                Integer fieldBox = countArgument(fn, ctx, 2, "split_part");
                if (fieldBox == null) return null;
                int field = fieldBox;
                if (field == 0) throw new MemgresException("field position must not be zero", "22023");
                String ds = delim.toString();
                // PG: empty delimiter → return whole string for any field position
                if (ds.isEmpty()) {
                    return (field == 1 || field == -1) ? str.toString() : "";
                }
                String[] parts = str.toString().split(java.util.regex.Pattern.quote(ds), -1);
                if (field < 0) {
                    // Negative indexing: count from the end
                    int idx = parts.length + field;
                    return (idx >= 0 && idx < parts.length) ? parts[idx] : "";
                }
                return (field >= 1 && field <= parts.length) ? parts[field - 1] : "";
            }
            case "regexp_replace": {
                // Two forms:
                // Old: regexp_replace(string, pattern, replacement [, flags])
                // PG15+: regexp_replace(string, pattern, replacement, start, N [, flags])
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                Object replacementVal = executor.evalExpr(fn.args().get(2), ctx);
                if (patternVal == null || replacementVal == null) return null;
                String pattern = patternVal.toString();
                String replacement = replacementVal.toString();
                // PG's replacement text only gives meaning to \N and \&; the conversion to
                // Java's syntax happens below, once the pattern's group count is known.
                String flags = "";
                int startPos = 1;
                int nth = 1; // PG15+ form default: replace first match only (0 = all)
                boolean pg15Form = false;
                if (fn.args().size() > 3) {
                    Object arg3 = executor.evalExpr(fn.args().get(3), ctx);
                    // Distinguish old form (flags string) from PG15+ form (start int)
                    if (arg3 instanceof Number) {
                        pg15Form = true;
                        startPos = executor.toInt(arg3);
                        if (fn.args().size() > 4) {
                            nth = executor.toInt(executor.evalExpr(fn.args().get(4), ctx));
                        }
                        if (fn.args().size() > 5) {
                            Object flagsVal = executor.evalExpr(fn.args().get(5), ctx);
                            if (flagsVal == null) return null;
                            flags = flagsVal.toString();
                        }
                    } else {
                        if (arg3 == null) return null;
                        flags = arg3.toString();
                    }
                }
                PgRegex.Options opts = PgRegex.parseFlags(flags, true, name);
                try {
                    String s = str.toString();
                    java.util.regex.Pattern p = PgRegex.compile(pattern, opts);
                    // Convert \N backrefs now that the group count is known: a valid group
                    // becomes $N, a reference to a non-existent group substitutes empty (PG).
                    int groupCount = p.matcher("").groupCount();
                    replacement = convertRegexpReplaceBackrefs(replacement, groupCount);
                    if (pg15Form) {
                        if (startPos < 1) {
                            throw new MemgresException("invalid value for parameter \"start\": " + startPos, "22023");
                        }
                        // PG15+ form: start position and nth match
                        String prefix = s.substring(0, Math.min(startPos - 1, s.length()));
                        String searchPart = s.substring(Math.min(startPos - 1, s.length()));
                        java.util.regex.Matcher m = p.matcher(searchPart);
                        if (nth == 0) {
                            // Replace all matches in the search region
                            return prefix + m.replaceAll(replacement);
                        } else {
                            // Replace only the Nth match in the search region
                            StringBuffer sb = new StringBuffer();
                            int found = 0;
                            while (m.find()) {
                                found++;
                                if (found == nth) {
                                    m.appendReplacement(sb, replacement);
                                    break;
                                }
                            }
                            m.appendTail(sb);
                            return prefix + sb.toString();
                        }
                    } else {
                        // Old form
                        if (opts.global) {
                            return p.matcher(s).replaceAll(replacement);
                        }
                        return p.matcher(s).replaceFirst(replacement);
                    }
                } catch (java.util.regex.PatternSyntaxException e) {
                    throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B");
                } catch (IndexOutOfBoundsException e) {
                    // Invalid backref (e.g., \1 with no group) — PG treats gracefully as empty
                    return str.toString();
                }
            }
            case "regexp_match":
            case "regexp_matches": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                Object flagsVal = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx) : "";
                if (flagsVal == null) return null;
                PgRegex.Options opts = PgRegex.parseFlags(flagsVal.toString(),
                        name.equals("regexp_matches"), name);
                java.util.regex.Matcher m =
                        PgRegex.compile(patternVal.toString(), opts).matcher(str.toString());
                if (opts.global) {
                    // Global flag: return List of all matches (SRF, one row per match)
                    List<Object> allMatches = new ArrayList<>();
                    while (m.find()) {
                        allMatches.add(matchGroupArray(m));
                    }
                    return allMatches.isEmpty() ? null : allMatches;
                }
                if (m.find()) {
                    return matchGroupArray(m);
                }
                return null;
            }
            case "regexp_count": {
                // regexp_count(string, pattern [, start [, flags]])
                // start is 1-based position to begin searching from
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                int start = 1;
                String flags = "";
                if (fn.args().size() > 2) {
                    Object arg2 = executor.evalExpr(fn.args().get(2), ctx);
                    // 3rd arg is start position (int), not flags
                    start = executor.toInt(arg2);
                }
                if (fn.args().size() > 3) {
                    Object flagsVal = executor.evalExpr(fn.args().get(3), ctx);
                    if (flagsVal == null) return null;
                    flags = flagsVal.toString();
                }
                String s = str.toString();
                String searchStr = start > 1 ? s.substring(Math.min(start - 1, s.length())) : s;
                java.util.regex.Matcher m = PgRegex.compile(patternVal.toString(),
                        PgRegex.parseFlags(flags, false, name)).matcher(searchStr);
                int count = 0;
                while (m.find()) count++;
                return count;
            }
            case "regexp_like": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                Object flagsVal = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx) : "";
                if (flagsVal == null) return null;
                return PgRegex.compile(patternVal.toString(),
                        PgRegex.parseFlags(flagsVal.toString(), false, name))
                        .matcher(str.toString()).find();
            }
            case "regexp_substr": {
                // regexp_substr(string, pattern [, start [, N [, flags [, subexpr]]]])
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                int start = 1;
                int nthMatch = 1;
                String flags = "";
                int subexpr = 0;
                if (fn.args().size() > 2) {
                    start = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                }
                if (fn.args().size() > 3) {
                    nthMatch = executor.toInt(executor.evalExpr(fn.args().get(3), ctx));
                }
                if (fn.args().size() > 4) {
                    Object flagsVal = executor.evalExpr(fn.args().get(4), ctx);
                    if (flagsVal == null) return null;
                    flags = flagsVal.toString();
                }
                if (fn.args().size() > 5) {
                    subexpr = executor.toInt(executor.evalExpr(fn.args().get(5), ctx));
                }
                String s = str.toString();
                int offset = Math.min(start - 1, s.length());
                java.util.regex.Matcher m = PgRegex.compile(patternVal.toString(),
                        PgRegex.parseFlags(flags, false, name)).matcher(s);
                int found = 0;
                int regionStart = offset;
                while (m.find(regionStart)) {
                    found++;
                    if (found == nthMatch) {
                        return m.group(subexpr);
                    }
                    regionStart = m.end();
                }
                return null;
            }
            case "regexp_instr": {
                // regexp_instr(string, pattern [, start [, N [, endoption [, flags [, subexpr]]]]])
                // start: 1-based position to begin searching
                // N: which match to return (1 = first)
                // endoption: 0 = return start of match (default), 1 = return position after end of match
                // flags: regex flags string
                // subexpr: which capture group to return position of
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                int start = 1;
                int nthMatch = 1;
                int endOption = 0;
                String flags = "";
                int subexpr = 0;
                if (fn.args().size() > 2) {
                    start = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                }
                if (fn.args().size() > 3) {
                    nthMatch = executor.toInt(executor.evalExpr(fn.args().get(3), ctx));
                }
                if (fn.args().size() > 4) {
                    endOption = executor.toInt(executor.evalExpr(fn.args().get(4), ctx));
                }
                if (fn.args().size() > 5) {
                    Object flagsVal = executor.evalExpr(fn.args().get(5), ctx);
                    if (flagsVal == null) return null;
                    flags = flagsVal.toString();
                }
                if (fn.args().size() > 6) {
                    subexpr = executor.toInt(executor.evalExpr(fn.args().get(6), ctx));
                }
                String s = str.toString();
                int offset = Math.min(start - 1, s.length());
                java.util.regex.Matcher m = PgRegex.compile(patternVal.toString(),
                        PgRegex.parseFlags(flags, false, name)).matcher(s);
                int found = 0;
                int regionStart = offset;
                while (m.find(regionStart)) {
                    found++;
                    if (found == nthMatch) {
                        int grp = subexpr;
                        if (endOption == 1) {
                            return m.end(grp) + 1; // position after the match end, 1-based
                        }
                        return m.start(grp) + 1; // 1-based position in original string
                    }
                    regionStart = m.end();
                }
                return 0;
            }
            case "regexp_split_to_array": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object patternVal = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || patternVal == null) return null;
                String flags = "";
                if (fn.args().size() > 2) {
                    Object flagsVal = executor.evalExpr(fn.args().get(2), ctx);
                    if (flagsVal == null) return null;
                    flags = flagsVal.toString();
                }
                List<String> parts = PgRegex.split(str.toString(),
                        PgRegex.compile(patternVal.toString(), PgRegex.parseFlags(flags, false, name)));
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < parts.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(quoteArrayElement(parts.get(i)));
                }
                sb.append("}");
                return sb.toString();
            }
            case "format": {
                // format(formatstr, arg1, arg2, ...): PG-style %s, %I, %L with width/flags
                Object fmt = executor.evalExpr(fn.args().get(0), ctx);
                if (fmt == null) return null;
                String fmtStr = fmt.toString();
                List<Object> fmtArgs = new ArrayList<>();
                for (int i = 1; i < fn.args().size(); i++) {
                    fmtArgs.add(executor.evalExpr(fn.args().get(i), ctx));
                }
                StringBuilder result = new StringBuilder();
                int argIdx = 0;
                for (int i = 0; i < fmtStr.length(); i++) {
                    if (fmtStr.charAt(i) != '%') {
                        result.append(fmtStr.charAt(i));
                        continue;
                    }
                    if (i + 1 >= fmtStr.length()) {
                        throw new MemgresException("unterminated format() type specifier", "22023");
                    }
                    // Parse format specifier: %[flags][width][position$]type
                    int j = i + 1;
                    char next = fmtStr.charAt(j);
                    if (next == '%') { result.append('%'); i++; continue; }
                    // Parse optional '-' flag (left-align)
                    boolean leftAlign = false;
                    if (next == '-') { leftAlign = true; j++; }
                    // Parse optional width (digits or '*' for width from arg)
                    int width = 0;
                    boolean hasWidth = false;
                    if (j < fmtStr.length() && fmtStr.charAt(j) == '*') {
                        // Width from next argument
                        if (argIdx >= fmtArgs.size()) throw new MemgresException("too few arguments for format()", "22023");
                        Object wArg = fmtArgs.get(argIdx++);
                        width = wArg instanceof Number ? ((Number) wArg).intValue() : Integer.parseInt(wArg.toString());
                        if (width < 0) { leftAlign = true; width = -width; }
                        hasWidth = true;
                        j++;
                    } else {
                        while (j < fmtStr.length() && Character.isDigit(fmtStr.charAt(j))) {
                            width = width * 10 + (fmtStr.charAt(j) - '0');
                            hasWidth = true;
                            j++;
                        }
                    }
                    if (j >= fmtStr.length()) throw new MemgresException("unterminated format() type specifier", "22023");
                    // Check for positional: N$type
                    int useArgIdx = -1;
                    if (fmtStr.charAt(j) == '$' && hasWidth && !leftAlign) {
                        // The "width" digits were actually a position number
                        useArgIdx = width - 1; // 1-based → 0-based
                        width = 0;
                        hasWidth = false;
                        j++;
                        // Re-parse optional flags and width after position
                        if (j < fmtStr.length() && fmtStr.charAt(j) == '-') { leftAlign = true; j++; }
                        while (j < fmtStr.length() && Character.isDigit(fmtStr.charAt(j))) {
                            width = width * 10 + (fmtStr.charAt(j) - '0');
                            hasWidth = true;
                            j++;
                        }
                    }
                    if (j >= fmtStr.length()) throw new MemgresException("unterminated format() type specifier", "22023");
                    char spec = fmtStr.charAt(j);
                    if (spec != 's' && spec != 'I' && spec != 'L') {
                        throw new MemgresException("unrecognized format() type specifier \"" + spec + "\"", "22023");
                    }
                    // Get the argument value
                    int aIdx = useArgIdx >= 0 ? useArgIdx : argIdx++;
                    if (aIdx < 0 || aIdx >= fmtArgs.size()) {
                        throw new MemgresException("too few arguments for format()", "22023");
                    }
                    Object argVal = fmtArgs.get(aIdx);
                    // Format the value
                    String formatted;
                    if (spec == 'L') {
                        if (argVal == null) formatted = "NULL";
                        else formatted = "'" + argVal.toString().replace("'", "''") + "'";
                    } else if (spec == 'I') {
                        if (argVal == null) throw new MemgresException("null values cannot be formatted as an SQL identifier", "22004");
                        formatted = formatIdentifier(argVal.toString());
                    } else {
                        formatted = argVal == null ? "" : argVal.toString();
                    }
                    // Apply width padding
                    if (hasWidth && formatted.length() < width) {
                        int pad = width - formatted.length();
                        StringBuilder padSb = new StringBuilder();
                        for (int p = 0; p < pad; p++) padSb.append(' ');
                        if (leftAlign) {
                            formatted = formatted + padSb;
                        } else {
                            formatted = padSb + formatted;
                        }
                    }
                    result.append(formatted);
                    i = j;
                }
                return result.toString();
            }
            case "chr": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                int codepoint = executor.toInt(arg);
                if (codepoint == 0) throw new MemgresException("null character not permitted", "54000");
                if (codepoint < 0) throw new MemgresException("requested character too large for encoding: " + codepoint, "22023");
                if (codepoint > 1114111) throw new MemgresException("requested character too large for encoding: " + codepoint, "54000");
                return new String(Character.toChars(codepoint));
            }
            case "ascii": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                return s.isEmpty() ? 0 : (int) s.charAt(0);
            }
            case "md5": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof byte[]) return ByteaOperations.md5bytes((byte[]) arg);
                return ByteaOperations.md5(arg.toString());
            }
            case "translate": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object from = executor.evalExpr(fn.args().get(1), ctx);
                Object to = executor.evalExpr(fn.args().get(2), ctx);
                // PG is strict here: a NULL in any argument makes the whole call NULL.
                if (str == null || from == null || to == null) return null;
                String s = str.toString(), f = from.toString(), t = to.toString();
                StringBuilder sb = new StringBuilder();
                for (char c : s.toCharArray()) {
                    int idx = f.indexOf(c);
                    if (idx < 0) sb.append(c);
                    else if (idx < t.length()) sb.append(t.charAt(idx));
                    // else character is deleted (PG behavior)
                }
                return sb.toString();
            }
            case "initcap": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().toLowerCase();
                StringBuilder sb = new StringBuilder();
                boolean capitalize = true;
                for (char c : s.toCharArray()) {
                    if (Character.isWhitespace(c) || !Character.isLetterOrDigit(c)) {
                        capitalize = true;
                        sb.append(c);
                    } else if (capitalize) {
                        sb.append(Character.toUpperCase(c));
                        capitalize = false;
                    } else {
                        sb.append(c);
                    }
                }
                return sb.toString();
            }
            case "starts_with": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object prefix = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null || prefix == null) return null;
                return str.toString().startsWith(prefix.toString());
            }
            case "string_to_array": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object delim = executor.evalExpr(fn.args().get(1), ctx);
                if (str == null) return null;
                if (str.toString().isEmpty()) {
                    return new ArrayList<>();
                }
                if (delim == null) {
                    // NULL delimiter: split into individual characters
                    String s = str.toString();
                    List<Object> chars = new ArrayList<>();
                    for (int i = 0; i < s.length(); i++) {
                        chars.add(String.valueOf(s.charAt(i)));
                    }
                    return chars;
                }
                List<Object> result;
                if (delim.toString().isEmpty()) {
                    // An empty delimiter tells PG not to split at all
                    result = new ArrayList<>();
                    result.add(str.toString());
                } else {
                    String[] parts = str.toString().split(java.util.regex.Pattern.quote(delim.toString()), -1);
                    result = new ArrayList<>(Arrays.asList((Object[]) parts));
                }
                if (fn.args().size() > 2) {
                    Object nullStr = executor.evalExpr(fn.args().get(2), ctx);
                    if (nullStr != null) {
                        String ns = nullStr.toString();
                        for (int i = 0; i < result.size(); i++) {
                            if (ns.equals(result.get(i))) {
                                result.set(i, null);
                            }
                        }
                    }
                }
                return result;
            }
            case "encode": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object fmt = executor.evalExpr(fn.args().get(1), ctx);
                if (data == null) return null;
                String format = fmt.toString().toLowerCase();
                byte[] bytes;
                if (data instanceof byte[]) {
                    bytes = (byte[]) data;
                } else {
                    bytes = data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                if (format.equals("base64")) {
                    return java.util.Base64.getEncoder().encodeToString(bytes);
                } else if (format.equals("hex")) {
                    StringBuilder hex = new StringBuilder();
                    for (byte b : bytes) hex.append(String.format("%02x", b));
                    return hex.toString();
                } else if (format.equals("escape")) {
                    // PG encode(bytea, 'escape'):
                    // - Printable ASCII (32-126 except \): as-is
                    // - Backslash (92): \\ (single escape pair)
                    // - All other bytes: \NNN (single backslash + octal)
                    StringBuilder sb = new StringBuilder();
                    for (byte b : bytes) {
                        int unsigned = b & 0xFF;
                        if (unsigned >= 32 && unsigned <= 126 && unsigned != 92) {
                            sb.append((char) unsigned);
                        } else if (unsigned == 92) {
                            sb.append("\\\\");
                        } else {
                            sb.append('\\').append(String.format("%03o", unsigned));
                        }
                    }
                    return sb.toString();
                }
                return data.toString();
            }
            case "decode": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object fmt = executor.evalExpr(fn.args().get(1), ctx);
                if (data == null) return null;
                String format = fmt.toString().toLowerCase();
                if (format.equals("base64")) {
                    try {
                        return java.util.Base64.getDecoder().decode(data.toString());
                    } catch (IllegalArgumentException e) {
                        throw new MemgresException("invalid input for decoding: \"" + data + "\"", "22023");
                    }
                } else if (format.equals("hex")) {
                    String hexStr = data.toString();
                    if (hexStr.length() % 2 != 0 || !hexStr.matches("[0-9a-fA-F]*")) {
                        throw new MemgresException("invalid hexadecimal data: odd number of digits", "22023");
                    }
                    return ByteaOperations.hexToBytes(hexStr);
                } else if (format.equals("escape")) {
                    // decode(text, 'escape') -> bytea: plain ASCII bytes, with \NNN for non-printable
                    String s = data.toString();
                    java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
                    for (int ci = 0; ci < s.length(); ci++) {
                        char c = s.charAt(ci);
                        if (c == '\\' && ci + 3 < s.length()
                                && s.charAt(ci + 1) >= '0' && s.charAt(ci + 1) <= '3'
                                && s.charAt(ci + 2) >= '0' && s.charAt(ci + 2) <= '7'
                                && s.charAt(ci + 3) >= '0' && s.charAt(ci + 3) <= '7') {
                            int val = (s.charAt(ci + 1) - '0') * 64 + (s.charAt(ci + 2) - '0') * 8 + (s.charAt(ci + 3) - '0');
                            bos.write(val);
                            ci += 3;
                        } else if (c == '\\' && ci + 1 < s.length() && s.charAt(ci + 1) == '\\') {
                            bos.write('\\');
                            ci++;
                        } else {
                            bos.write((byte) c);
                        }
                    }
                    return bos.toByteArray();
                } else {
                    throw new MemgresException("unrecognized encoding: \"" + fmt + "\"", "22023");
                }
            }
            case "overlay": {
                // overlay(string PLACING replacement FROM start FOR count)
                // PG: overlay(s, r, p, n) = left(s, p-1) || r || substr(s, p+n)
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Object replacementObj = executor.evalExpr(fn.args().get(1), ctx);
                // PG is strict here: a NULL replacement makes the whole call NULL.
                if (replacementObj == null) return null;
                if (str instanceof byte[] || replacementObj instanceof byte[]) {
                    // bytea overlay
                    byte[] bStr = (str instanceof byte[]) ? (byte[]) str : str.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] bReplacement = (replacementObj instanceof byte[]) ? (byte[]) replacementObj : replacementObj.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    Object startObj = executor.evalExpr(fn.args().get(2), ctx);
                    Object countObj = fn.args().size() > 3
                            ? executor.evalExpr(fn.args().get(3), ctx) : null;
                    if (startObj == null || (fn.args().size() > 3 && countObj == null)) return null;
                    int startPos = executor.toInt(startObj);
                    int count = countObj != null ? executor.toInt(countObj) : bReplacement.length;
                    if (count < 0 || startPos <= 0) {
                        throw new MemgresException("negative substring length not allowed", "22011");
                    }
                    int start = startPos - 1; // 1-based to 0-based
                    int prefixLen = Math.min(start, bStr.length);
                    int suffixStart = start + count;
                    int suffixLen = suffixStart < bStr.length ? bStr.length - suffixStart : 0;
                    byte[] result = new byte[prefixLen + bReplacement.length + suffixLen];
                    System.arraycopy(bStr, 0, result, 0, prefixLen);
                    System.arraycopy(bReplacement, 0, result, prefixLen, bReplacement.length);
                    if (suffixLen > 0) {
                        System.arraycopy(bStr, suffixStart, result, prefixLen + bReplacement.length, suffixLen);
                    }
                    return result;
                }
                String replacement = String.valueOf(replacementObj);
                Object startObj = executor.evalExpr(fn.args().get(2), ctx);
                Object countObj = fn.args().size() > 3
                        ? executor.evalExpr(fn.args().get(3), ctx) : null;
                if (startObj == null || (fn.args().size() > 3 && countObj == null)) return null;
                int startPos = executor.toInt(startObj);
                int count = countObj != null ? executor.toInt(countObj) : replacement.length();
                if (count < 0 || startPos <= 0) {
                    throw new MemgresException("negative substring length not allowed", "22011");
                }
                int start = startPos - 1; // 1-based to 0-based
                String s = str.toString();
                return s.substring(0, Math.min(start, s.length()))
                        + replacement
                        + (start + count < s.length() ? s.substring(start + count) : "");
            }
            case "octet_length": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof byte[]) return ((byte[]) arg).length;
                // Bit string: octet_length = ceil(bits / 8)
                if (arg instanceof AstExecutor.PgBitString) {
                    int bits = ((AstExecutor.PgBitString) arg).bits().length();
                    return (bits + 7) / 8;
                }
                return arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            }
            case "bit_length": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof byte[]) return ((byte[]) arg).length * 8;
                // Bit string: bit_length(B'1010') = 4
                if (arg instanceof AstExecutor.PgBitString)
                    return ((AstExecutor.PgBitString) arg).bits().length();
                String bs = arg.toString();
                return bs.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 8;
            }
            case "quote_literal": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                String escaped = s.replace("'", "''").replace("\\", "\\\\");
                // PG uses E'' prefix when string contains backslashes
                if (s.contains("\\")) {
                    return "E'" + escaped + "'";
                }
                return "'" + escaped + "'";
            }
            case "quote_ident": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String ident = arg.toString();
                // Only quote if it contains special chars, is mixed case, or is a keyword
                if (ident.matches("[a-z_][a-z0-9_]*") && !isReservedWord(ident)) {
                    return ident;
                }
                return "\"" + ident.replace("\"", "\"\"") + "\"";
            }
            case "quote_nullable": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return "NULL";
                String qs = arg.toString();
                String qescaped = qs.replace("'", "''").replace("\\", "\\\\");
                if (qs.contains("\\")) return "E'" + qescaped + "'";
                return "'" + qescaped + "'";
            }
            case "to_hex": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return Long.toHexString(executor.toLong(arg));
            }
            case "normalize": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String form = "NFC";
                if (fn.args().size() > 1) {
                    // The form is a bare keyword in the grammar, so it arrives looking like a
                    // column reference and must be read by name rather than evaluated.
                    Expression formExpr = fn.args().get(1);
                    String keyword = formExpr instanceof ColumnRef
                            && ((ColumnRef) formExpr).table() == null
                            ? ((ColumnRef) formExpr).column() : null;
                    if (keyword != null && NORMALIZATION_FORMS.contains(keyword.toUpperCase())) {
                        form = keyword.toUpperCase();
                    } else {
                        form = String.valueOf(executor.evalExpr(formExpr, ctx)).toUpperCase();
                    }
                }
                if (!NORMALIZATION_FORMS.contains(form)) {
                    throw new MemgresException("invalid normalization form: " + form.toLowerCase(), "22023");
                }
                return java.text.Normalizer.normalize(arg.toString(), java.text.Normalizer.Form.valueOf(form));
            }
            case "unicode": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                return s.isEmpty() ? 0 : s.codePointAt(0);
            }
            case "unistr": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < s.length(); i++) {
                    if (s.charAt(i) == '\\' && i + 1 < s.length()) {
                        // \+NNNNNN — 6-digit codepoint
                        if (s.charAt(i + 1) == '+' && i + 7 < s.length()) {
                            String hex = s.substring(i + 2, i + 8);
                            try {
                                sb.appendCodePoint(Integer.parseInt(hex, 16));
                                i += 7;
                                continue;
                            } catch (NumberFormatException e) { /* fall through */ }
                        }
                        // \NNNN — 4-digit codepoint
                        if (i + 4 < s.length()) {
                            String hex = s.substring(i + 1, i + 5);
                            try {
                                sb.appendCodePoint(Integer.parseInt(hex, 16));
                                i += 4;
                                continue;
                            } catch (NumberFormatException e) { /* fall through */ }
                        }
                        // \\ — literal backslash
                        if (s.charAt(i + 1) == '\\') {
                            sb.append('\\');
                            i++;
                            continue;
                        }
                    }
                    sb.append(s.charAt(i));
                }
                return sb.toString();
            }
            default:
                return NOT_HANDLED;
        }
    }

    // ---- Helper methods ----

    private static void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types."), "42883");
        }
    }

    /** Convert value to PG text output form. Booleans → "t"/"f". */
    private static String pgTextOutput(Object val) {
        if (val instanceof Boolean) return ((Boolean) val) ? "t" : "f";
        // bytea and arrays have no useful toString(); render them as PG's output functions do
        if (val instanceof byte[]) return TypeCoercion.byteaToText((byte[]) val);
        if (val instanceof java.util.List) return TypeCoercion.formatPgArray((java.util.List<?>) val);
        return val.toString();
    }

    /**
     * The array one regexp match contributes: the capture groups when the pattern has any,
     * otherwise the whole match — which is still an element when it is the empty string.
     */
    private static String matchGroupArray(java.util.regex.Matcher m) {
        StringBuilder sb = new StringBuilder("{");
        if (m.groupCount() > 0) {
            for (int gi = 1; gi <= m.groupCount(); gi++) {
                if (gi > 1) sb.append(",");
                sb.append(quoteArrayElement(m.group(gi)));
            }
        } else {
            sb.append(quoteArrayElement(m.group(0)));
        }
        return sb.append("}").toString();
    }

    /** Render one array element the way PG's array_out does, quoting only where it must. */
    private static String quoteArrayElement(String value) {
        if (value == null) return "NULL";
        boolean needsQuotes = value.isEmpty() || value.equalsIgnoreCase("NULL");
        for (int i = 0; i < value.length() && !needsQuotes; i++) {
            char c = value.charAt(i);
            needsQuotes = c == '{' || c == '}' || c == ',' || c == '"' || c == '\\'
                    || Character.isWhitespace(c);
        }
        if (!needsQuotes) return value;
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\');
            sb.append(c);
        }
        return sb.append('"').toString();
    }

    /**
     * Convert PG {@code \N} numbered backrefs in a regexp_replace replacement string into
     * Java {@code $N} group references. A reference to a group that does not exist is
     * dropped (PG substitutes the empty string). Non-backref characters pass through
     * unchanged (matching the previous behaviour for {@code $0}, literal {@code $}, etc.).
     */
    private static String convertRegexpReplaceBackrefs(String repl, int groupCount) {
        StringBuilder sb = new StringBuilder(repl.length());
        for (int i = 0; i < repl.length(); i++) {
            char c = repl.charAt(i);
            if (c == '\\' && i + 1 < repl.length()) {
                char next = repl.charAt(i + 1);
                i++;
                if (Character.isDigit(next)) {
                    int g = next - '0';
                    if (g <= groupCount) sb.append('$').append(next);
                    // else: backref to a non-existent group -> empty substitution
                } else if (next == '&') {
                    sb.append("$0"); // PG's whole-match reference
                } else if (next == '\\') {
                    sb.append("\\\\"); // a literal backslash, escaped for Java
                } else {
                    sb.append(java.util.regex.Matcher.quoteReplacement(String.valueOf(next)));
                }
                continue;
            }
            // '$' carries no meaning in PG's replacement text, so Java must see it as a literal
            if (c == '$') {
                sb.append("\\$");
                continue;
            }
            // A trailing backslash has nothing to escape; PG emits it as itself, while Java
            // would reject a replacement string that ends mid-escape
            if (c == '\\') {
                sb.append("\\\\");
                continue;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * SQL-standard {@code substring(text FROM pattern FOR escape)}: the substring bracketed
     * by the two {@code <escape>"} markers in the SIMILAR-style pattern is returned.
     */
    static Object sqlSimilarSubstring(String str, String pat, String escArg) {
        String esc = (escArg == null || escArg.isEmpty()) ? "\\" : escArg;
        // The escape char + '"' marks the start/end of the capture group.
        String delimiter = java.util.regex.Pattern.quote(esc) + "\"";
        String[] parts = pat.split(delimiter, -1);
        if (parts.length < 3) return null; // need exactly two delimiters
        String regexBefore = similarToRegex(parts[0], esc);
        String regexCapture = similarToRegex(parts[1], esc);
        String regexAfter = similarToRegex(parts[2], esc);
        String fullRegex = "(?s)" + regexBefore + "(" + regexCapture + ")" + regexAfter;
        try {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile(fullRegex).matcher(str);
            if (m.matches()) return m.group(1);
            return null;
        } catch (java.util.regex.PatternSyntaxException e) {
            throw new MemgresException("invalid regular expression: " + e.getMessage(), "2201B");
        }
    }

    private static String similarToRegex(String pattern, String escapeChar) {
        StringBuilder sb = new StringBuilder();
        String esc = escapeChar != null && !escapeChar.isEmpty() ? escapeChar : "\\";
        int i = 0;
        while (i < pattern.length()) {
            char ch = pattern.charAt(i);
            String chStr = String.valueOf(ch);
            if (chStr.equals(esc) && i + 1 < pattern.length()) {
                // Escaped character, treat next char as literal
                sb.append(java.util.regex.Pattern.quote(pattern.substring(i + 1, i + 2)));
                i += 2;
            } else if (chStr.equals(esc)) {
                // An escape with nothing left to escape is dropped, as PG's similar_escape does
                i++;
            } else if (ch == '%') {
                sb.append(".*");
                i++;
            } else if (ch == '_') {
                sb.append(".");
                i++;
            } else if (ch == '|') {
                sb.append("|");
                i++;
            } else if (ch == '(') {
                sb.append("(");
                i++;
            } else if (ch == ')') {
                sb.append(")");
                i++;
            } else if (ch == '+') {
                sb.append("+");
                i++;
            } else if (ch == '*') {
                sb.append("*");
                i++;
            } else if (ch == '?') {
                sb.append("?");
                i++;
            } else if (ch == '{') {
                // Pass through bounded quantifier like {2}, {1,3} as-is
                int end = pattern.indexOf('}', i);
                if (end >= 0) {
                    sb.append(pattern, i, end + 1);
                    i = end + 1;
                } else {
                    sb.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
                    i++;
                }
            } else if (ch == '[') {
                // Pass character class through to regex, converting POSIX classes to Java equivalents
                // Find closing ']' that isn't part of a POSIX class like [:alpha:]
                int end = -1;
                {
                    int depth = 0;
                    for (int j = i + 1; j < pattern.length(); j++) {
                        if (pattern.charAt(j) == '[' && j + 1 < pattern.length() && pattern.charAt(j + 1) == ':') {
                            depth++;
                        } else if (pattern.charAt(j) == ']') {
                            if (depth > 0 && j > 0 && pattern.charAt(j - 1) == ':') {
                                depth--;
                            } else {
                                end = j;
                                break;
                            }
                        }
                    }
                }
                if (end >= 0) {
                    String cls = pattern.substring(i, end + 1);
                    cls = cls.replace("[:alpha:]", "\\p{Alpha}");
                    cls = cls.replace("[:digit:]", "\\p{Digit}");
                    cls = cls.replace("[:alnum:]", "\\p{Alnum}");
                    cls = cls.replace("[:upper:]", "\\p{Upper}");
                    cls = cls.replace("[:lower:]", "\\p{Lower}");
                    cls = cls.replace("[:space:]", "\\p{Space}");
                    cls = cls.replace("[:print:]", "\\p{Print}");
                    cls = cls.replace("[:punct:]", "\\p{Punct}");
                    cls = cls.replace("[:cntrl:]", "\\p{Cntrl}");
                    cls = cls.replace("[:xdigit:]", "\\p{XDigit}");
                    cls = cls.replace("[:graph:]", "\\p{Graph}");
                    cls = cls.replace("[:blank:]", "\\p{Blank}");
                    sb.append(cls);
                    i = end + 1;
                } else {
                    sb.append(java.util.regex.Pattern.quote(chStr));
                    i++;
                }
            } else {
                sb.append(java.util.regex.Pattern.quote(chStr));
                i++;
            }
        }
        return sb.toString();
    }

    private boolean isReservedWord(String word) {
        return RESERVED_WORDS.contains(word.toLowerCase());
    }

    /** Format an identifier using PG's quote_ident logic: only quote when needed. */
    private String formatIdentifier(String ident) {
        if (ident.matches("[a-z_][a-z0-9_]*") && !isReservedWord(ident)) {
            return ident;
        }
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    private void validateEncoding(String encoding) {
        String upper = encoding.toUpperCase();
        if (!VALID_ENCODINGS.contains(upper) && !VALID_ENCODINGS.contains(upper.replace("-", ""))) {
            throw new MemgresException("encoding \"" + encoding + "\" does not exist", "22023");
        }
    }

    /**
     * Parse an ICU/POSIX locale string into a Java Locale.
     * Handles formats like "tr-TR", "tr_TR", "und-u-ks-level2", "en_US.UTF-8".
     */
    static java.util.Locale parseLocale(String locale) {
        if (locale == null) return java.util.Locale.ROOT;
        // Strip encoding suffix (e.g., ".UTF-8")
        String loc = locale;
        int dotIdx = loc.indexOf('.');
        if (dotIdx >= 0) loc = loc.substring(0, dotIdx);
        // Replace hyphens with underscores for splitting
        String[] parts = loc.replace('-', '_').split("_", 3);
        if (parts.length >= 2) {
            return new java.util.Locale(parts[0], parts[1]);
        }
        return new java.util.Locale(parts[0]);
    }

    /** Convert value to byte[] for bytea operations. */
    private static byte[] toBytea(Object val) {
        if (val instanceof byte[]) return (byte[]) val;
        return val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 0-based index of the first occurrence of {@code sub} in {@code hay}, or -1 (PG position semantics). */
    private static int byteaIndexOf(byte[] hay, byte[] sub) {
        if (sub.length == 0) return 0; // PG: empty substring matches at position 1
        outer:
        for (int i = 0; i + sub.length <= hay.length; i++) {
            for (int j = 0; j < sub.length; j++) {
                if (hay[i + j] != sub[j]) continue outer;
            }
            return i;
        }
        return -1;
    }

    /** Trim bytes from bytea: remove leading/trailing bytes that appear in trimBytes set. */
    private static byte[] byteaTrim(byte[] data, byte[] trimBytes, boolean trimLeft, boolean trimRight) {
        java.util.Set<Byte> trimSet = new java.util.HashSet<>();
        for (byte b : trimBytes) trimSet.add(b);
        int start = 0;
        int end = data.length;
        if (trimLeft) {
            while (start < end && trimSet.contains(data[start])) start++;
        }
        if (trimRight) {
            while (end > start && trimSet.contains(data[end - 1])) end--;
        }
        return java.util.Arrays.copyOfRange(data, start, end);
    }
}
