package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;
import java.util.*;

/**
 * String function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class StringFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

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
                return arg.toString().toLowerCase();
            }
            case "trim":
            case "btrim": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
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
                    if (val != null) sb.append(val);
                }
                return sb.toString();
            }
            case "concat_ws": {
                Object sep = executor.evalExpr(fn.args().get(0), ctx);
                if (sep == null) return null;
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (int i = 1; i < fn.args().size(); i++) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    if (val != null) {
                        if (!first) sb.append(sep);
                        sb.append(val);
                        first = false;
                    }
                }
                return sb.toString();
            }
            case "replace": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object from = executor.evalExpr(fn.args().get(1), ctx);
                Object to = executor.evalExpr(fn.args().get(2), ctx);
                return str == null ? null : str.toString().replace(from.toString(), to.toString());
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
                    int bLen = fn.args().size() > 2 ? executor.toInt(executor.evalExpr(fn.args().get(2), ctx)) : bytes.length;
                    return ByteaOperations.substring(bytes, bStart, bLen);
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
                        try {
                            java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern).matcher(str.toString());
                            return m.find() ? m.group() : null;
                        } catch (java.util.regex.PatternSyntaxException pse) {
                            throw new MemgresException("invalid regular expression: " + pse.getMessage(), "2201B");
                        }
                    }
                }
                int start = executor.toInt(arg1) - 1; // PG is 1-based
                String strVal = str.toString();
                if (fn.args().size() > 2) {
                    int len = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                    if (len < 0) {
                        throw new MemgresException("negative substring length not allowed", "22023");
                    }
                    int from = Math.max(0, start);
                    int to = Math.min(strVal.length(), Math.max(0, start) + len);
                    if (from >= strVal.length()) return "";
                    return strVal.substring(from, to);
                }
                int from = Math.max(0, start);
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
                String str2 = strObj.toString();
                String pat = patObj.toString();
                String esc = escObj == null ? "\\" : escObj.toString();
                if (esc.isEmpty()) esc = "\\";
                // Convert SQL SIMILAR pattern to POSIX regex
                // The escape char + '"' marks the start/end of the capture group
                String delimiter = java.util.regex.Pattern.quote(esc) + "\"";
                // Split pattern on delimiter to find the capture portion
                String[] parts2 = pat.split(delimiter, -1);
                if (parts2.length < 3) return null; // need two delimiters
                String before = parts2[0];
                String capture = parts2[1];
                String after = parts2.length > 2 ? parts2[2] : "";
                // Convert SIMILAR TO parts to regex
                String regexBefore = similarToRegex(before, esc);
                String regexCapture = similarToRegex(capture, esc);
                String regexAfter = similarToRegex(after, esc);
                String fullRegex = "(?s)" + regexBefore + "(" + regexCapture + ")" + regexAfter;
                try {
                    java.util.regex.Matcher m = java.util.regex.Pattern.compile(fullRegex).matcher(str2);
                    if (m.matches()) return m.group(1);
                    return null;
                } catch (java.util.regex.PatternSyntaxException e) {
                    throw new MemgresException("invalid regular expression: " + e.getMessage(), "2201B");
                }
            }
            case "ltrim": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                String chars = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : " ";
                String s = str.toString();
                int i = 0;
                while (i < s.length() && chars.indexOf(s.charAt(i)) >= 0) i++;
                return s.substring(i);
            }
            case "rtrim": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                String chars = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : " ";
                String s = str.toString();
                int i = s.length() - 1;
                while (i >= 0 && chars.indexOf(s.charAt(i)) >= 0) i--;
                return s.substring(0, i + 1);
            }
            case "lpad": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                int len = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                String fill = fn.args().size() > 2 ? String.valueOf(executor.evalExpr(fn.args().get(2), ctx)) : " ";
                String s = str.toString();
                if (s.length() >= len) return s.substring(0, len);
                StringBuilder sb = new StringBuilder();
                while (sb.length() + s.length() < len) {
                    sb.append(fill);
                }
                return sb.substring(0, len - s.length()) + s;
            }
            case "rpad": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                int len = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                String fill = fn.args().size() > 2 ? String.valueOf(executor.evalExpr(fn.args().get(2), ctx)) : " ";
                String s = str.toString();
                if (s.length() >= len) return s.substring(0, len);
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
                int n = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                String s = str.toString();
                if (n >= 0) return s.substring(0, Math.min(n, s.length()));
                return n + s.length() > 0 ? s.substring(0, s.length() + n) : "";
            }
            case "right": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                int n = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                String s = str.toString();
                if (n >= 0) return s.substring(Math.max(0, s.length() - n));
                return -n < s.length() ? s.substring(-n) : "";
            }
            case "repeat": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                Object countArg = executor.evalExpr(fn.args().get(1), ctx);
                if (countArg == null) return null;
                int n = executor.toInt(countArg);
                return Strs.repeat(str.toString(), Math.max(0, n));
            }
            case "reverse": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                return str == null ? null : new StringBuilder(str.toString()).reverse().toString();
            }
            case "split_part": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object delim = executor.evalExpr(fn.args().get(1), ctx);
                int field = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                if (field == 0) throw new MemgresException("field position must not be zero", "22023");
                if (str == null) return null;
                String[] parts = str.toString().split(java.util.regex.Pattern.quote(delim.toString()), -1);
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
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                String replacement = String.valueOf(executor.evalExpr(fn.args().get(2), ctx));
                // Convert PG-style backreferences (\1, \2) to Java-style ($1, $2)
                replacement = replacement.replaceAll("\\\\(\\d)", "\\$$1");
                String flags = "";
                int startPos = 1;
                int nth = 0; // 0 means replace all when using PG15+ form; in old form, default is replaceFirst
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
                            flags = String.valueOf(executor.evalExpr(fn.args().get(5), ctx));
                        }
                    } else {
                        flags = String.valueOf(arg3);
                    }
                }
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                try {
                    String s = str.toString();
                    java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern, jflags);
                    if (pg15Form) {
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
                        if (flags.contains("g")) {
                            return p.matcher(s).replaceAll(replacement);
                        }
                        return p.matcher(s).replaceFirst(replacement);
                    }
                } catch (java.util.regex.PatternSyntaxException e) {
                    throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B");
                }
            }
            case "regexp_match":
            case "regexp_matches": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                String flags = fn.args().size() > 2 ? String.valueOf(executor.evalExpr(fn.args().get(2), ctx)) : "";
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                boolean global = flags.contains("g");
                java.util.regex.Matcher m;
                try {
                    m = java.util.regex.Pattern.compile(pattern, jflags).matcher(str.toString());
                } catch (java.util.regex.PatternSyntaxException pse) {
                    throw new MemgresException("invalid regular expression: " + pse.getDescription(), "2201B");
                }
                if (global && name.equals("regexp_matches")) {
                    // Global flag: return List of all matches (SRF, one row per match)
                    List<Object> allMatches = new ArrayList<>();
                    while (m.find()) {
                        StringBuilder sb = new StringBuilder("{");
                        if (m.groupCount() > 0) {
                            for (int gi = 1; gi <= m.groupCount(); gi++) {
                                if (gi > 1) sb.append(",");
                                sb.append(m.group(gi));
                            }
                        } else {
                            sb.append(m.group(0));
                        }
                        sb.append("}");
                        allMatches.add(sb.toString());
                    }
                    return allMatches.isEmpty() ? null : allMatches;
                }
                if (m.find()) {
                    StringBuilder sb = new StringBuilder("{");
                    if (m.groupCount() > 0) {
                        for (int gi = 1; gi <= m.groupCount(); gi++) {
                            if (gi > 1) sb.append(",");
                            sb.append(m.group(gi));
                        }
                    } else {
                        sb.append(m.group(0));
                    }
                    sb.append("}");
                    return sb.toString();
                }
                return null;
            }
            case "regexp_count": {
                // regexp_count(string, pattern [, start [, flags]])
                // start is 1-based position to begin searching from
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                int start = 1;
                String flags = "";
                if (fn.args().size() > 2) {
                    Object arg2 = executor.evalExpr(fn.args().get(2), ctx);
                    // 3rd arg is start position (int), not flags
                    start = executor.toInt(arg2);
                }
                if (fn.args().size() > 3) {
                    flags = String.valueOf(executor.evalExpr(fn.args().get(3), ctx));
                }
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                String s = str.toString();
                String searchStr = start > 1 ? s.substring(Math.min(start - 1, s.length())) : s;
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern, jflags).matcher(searchStr);
                int count = 0;
                while (m.find()) count++;
                return count;
            }
            case "regexp_like": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return false;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                String flags = fn.args().size() > 2 ? String.valueOf(executor.evalExpr(fn.args().get(2), ctx)) : "";
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                return java.util.regex.Pattern.compile(pattern, jflags).matcher(str.toString()).find();
            }
            case "regexp_substr": {
                // regexp_substr(string, pattern [, start [, N [, flags [, subexpr]]]])
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                if (str == null) return null;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
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
                    flags = String.valueOf(executor.evalExpr(fn.args().get(4), ctx));
                }
                if (fn.args().size() > 5) {
                    subexpr = executor.toInt(executor.evalExpr(fn.args().get(5), ctx));
                }
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                String s = str.toString();
                int offset = Math.min(start - 1, s.length());
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern, jflags).matcher(s);
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
                if (str == null) return 0;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
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
                    flags = String.valueOf(executor.evalExpr(fn.args().get(5), ctx));
                }
                if (fn.args().size() > 6) {
                    subexpr = executor.toInt(executor.evalExpr(fn.args().get(6), ctx));
                }
                int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
                String s = str.toString();
                int offset = Math.min(start - 1, s.length());
                java.util.regex.Matcher m = java.util.regex.Pattern.compile(pattern, jflags).matcher(s);
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
                if (str == null) return null;
                String pattern = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                String[] parts = str.toString().split(pattern);
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < parts.length; i++) {
                    if (i > 0) sb.append(",");
                    sb.append(parts[i]);
                }
                sb.append("}");
                return sb.toString();
            }
            case "format": {
                // format(formatstr, arg1, arg2, ...): PG-style %s, %I, %L
                Object fmt = executor.evalExpr(fn.args().get(0), ctx);
                if (fmt == null) return null;
                String fmtStr = fmt.toString();
                // Validate format string for invalid specifiers and trailing %
                for (int vi = 0; vi < fmtStr.length(); vi++) {
                    if (fmtStr.charAt(vi) == '%') {
                        if (vi + 1 >= fmtStr.length()) {
                            throw new MemgresException("unterminated format() type specifier", "22023");
                        }
                        char vc = fmtStr.charAt(vi + 1);
                        if (vc == '%' || vc == 's' || vc == 'I' || vc == 'L') {
                            vi++; // skip known specifier
                        } else if (Character.isDigit(vc)) {
                            // Positional: skip digits and $
                            int vj = vi + 1;
                            while (vj < fmtStr.length() && Character.isDigit(fmtStr.charAt(vj))) vj++;
                            if (vj < fmtStr.length() && fmtStr.charAt(vj) == '$' && vj + 1 < fmtStr.length()) {
                                char spec = fmtStr.charAt(vj + 1);
                                if (spec != 's' && spec != 'I' && spec != 'L') {
                                    throw new MemgresException("unrecognized format() type specifier \"" + spec + "\"", "22023");
                                }
                                vi = vj + 1;
                            } else {
                                throw new MemgresException("unrecognized format() type specifier \"" + vc + "\"", "22023");
                            }
                        } else {
                            throw new MemgresException("unrecognized format() type specifier \"" + vc + "\"", "22023");
                        }
                    }
                }
                List<Object> fmtArgs = new ArrayList<>();
                for (int i = 1; i < fn.args().size(); i++) {
                    fmtArgs.add(executor.evalExpr(fn.args().get(i), ctx));
                }
                // PG-style format: %s, %I, %L, and positional %N$s, %N$I, %N$L
                StringBuilder result = new StringBuilder();
                int argIdx = 0;
                for (int i = 0; i < fmtStr.length(); i++) {
                    if (fmtStr.charAt(i) == '%' && i + 1 < fmtStr.length()) {
                        char next = fmtStr.charAt(i + 1);
                        // Check for positional: %N$X (e.g., %1$I, %2$L)
                        if (Character.isDigit(next)) {
                            int j = i + 1;
                            while (j < fmtStr.length() && Character.isDigit(fmtStr.charAt(j))) j++;
                            if (j < fmtStr.length() && fmtStr.charAt(j) == '$' && j + 1 < fmtStr.length()) {
                                int posArgIdx = Integer.parseInt(fmtStr.substring(i + 1, j)) - 1; // 1-based -> 0-based
                                char spec = fmtStr.charAt(j + 1);
                                if (spec == 's' || spec == 'I' || spec == 'L') {
                                    if (posArgIdx < 0 || posArgIdx >= fmtArgs.size()) {
                                        throw new MemgresException("too few arguments for format()", "22023");
                                    }
                                    Object argVal = fmtArgs.get(posArgIdx);
                                    if (spec == 'L') {
                                        if (argVal == null) result.append("NULL");
                                        else result.append('\'').append(argVal.toString().replace("'", "''")).append('\'');
                                    } else if (spec == 'I') {
                                        if (argVal == null) throw new MemgresException("null values cannot be formatted as an SQL identifier", "22004");
                                        result.append(formatIdentifier(argVal.toString()));
                                    } else {
                                        result.append(argVal == null ? "" : argVal);
                                    }
                                    i = j + 1;
                                    continue;
                                }
                            }
                        }
                        if ((next == 's' || next == 'I' || next == 'L') && argIdx < fmtArgs.size()) {
                            Object argVal = fmtArgs.get(argIdx++);
                            if (next == 'L') {
                                if (argVal == null) result.append("NULL");
                                else result.append('\'').append(argVal.toString().replace("'", "''")).append('\'');
                            } else if (next == 'I') {
                                if (argVal == null) throw new MemgresException("null values cannot be formatted as an SQL identifier", "22004");
                                result.append(formatIdentifier(argVal.toString()));
                            } else {
                                // %s: NULL -> empty string
                                result.append(argVal == null ? "" : argVal);
                            }
                            i++;
                            continue;
                        } else if (next == '%') {
                            result.append('%');
                            i++;
                            continue;
                        }
                    }
                    result.append(fmtStr.charAt(i));
                }
                return result.toString();
            }
            case "chr": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                int codepoint = executor.toInt(arg);
                if (codepoint == 0) throw new MemgresException("null character not permitted", "22023");
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
                return arg == null ? null : ByteaOperations.md5(arg.toString());
            }
            case "translate": {
                Object str = executor.evalExpr(fn.args().get(0), ctx);
                Object from = executor.evalExpr(fn.args().get(1), ctx);
                Object to = executor.evalExpr(fn.args().get(2), ctx);
                if (str == null) return null;
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
                String[] parts = str.toString().split(java.util.regex.Pattern.quote(delim.toString()), -1);
                List<Object> result = new ArrayList<>(Arrays.asList((Object[]) parts));
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
                    // PG bytea escape format:
                    // - Printable ASCII (32-126 except \): as-is
                    // - Backslash (92): \\
                    // - Zero byte and high bytes (0, 128-255): \\NNN (double backslash + octal)
                    // - Control characters (1-31, 127): \NNN (single backslash + octal)
                    StringBuilder sb = new StringBuilder();
                    for (byte b : bytes) {
                        int unsigned = b & 0xFF;
                        if (unsigned >= 32 && unsigned <= 126 && unsigned != 92) {
                            sb.append((char) unsigned);
                        } else if (unsigned == 92) {
                            sb.append((char) 92).append((char) 92);
                        } else if (unsigned == 0 || unsigned > 127) {
                            sb.append((char) 92).append((char) 92).append(String.format("%03o", unsigned));
                        } else {
                            sb.append((char) 92).append(String.format("%03o", unsigned));
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
                        return new String(java.util.Base64.getDecoder().decode(data.toString()), java.nio.charset.StandardCharsets.UTF_8);
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
                if (str instanceof byte[] || replacementObj instanceof byte[]) {
                    // bytea overlay
                    byte[] bStr = (str instanceof byte[]) ? (byte[]) str : str.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    byte[] bReplacement = (replacementObj instanceof byte[]) ? (byte[]) replacementObj : replacementObj.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    int startPos = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                    int count = fn.args().size() > 3 ? executor.toInt(executor.evalExpr(fn.args().get(3), ctx)) : bReplacement.length;
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
                int startPos = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                int count = fn.args().size() > 3 ? executor.toInt(executor.evalExpr(fn.args().get(3), ctx)) : replacement.length();
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
                return arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            }
            case "bit_length": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof byte[]) return ((byte[]) arg).length * 8;
                // Bit string: bit_length(B'1010') = 4
                String bs = arg.toString();
                if (bs.matches("[01]+")) return bs.length();
                return bs.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * 8;
            }
            case "quote_literal": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return "'" + arg.toString().replace("'", "''") + "'";
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
                return "'" + arg.toString().replace("'", "''") + "'";
            }
            case "to_hex": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return Long.toHexString(executor.toLong(arg));
            }
            case "normalize": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String form = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toUpperCase() : "NFC";
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
                    if (s.charAt(i) == '\\' && i + 4 < s.length()) {
                        String hex = s.substring(i + 1, i + 5);
                        try {
                            sb.appendCodePoint(Integer.parseInt(hex, 16));
                            i += 4;
                            continue;
                        } catch (NumberFormatException e) { /* fall through */ }
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
}
