package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * The text form of an array, read the way PostgreSQL's array input reads it.
 *
 * <p>PostgreSQL decides an array's shape from its braces before it looks at a single element, and
 * refuses anything whose braces do not describe a rectangle. Reading the text loosely instead did
 * not merely accept what PostgreSQL rejects — it produced a <em>different</em> array from the same
 * text, so {@code {"a""b"}} loaded as two elements and {@code {foo,,bar}} gained an empty one.
 *
 * <p>The optional {@code [lb:ub]=} prefix in front of the braces gives the array explicit lower
 * bounds; the shape it states must agree with the shape the braces describe.
 */
final class ArrayLiteral {

    /** PostgreSQL is built with MAXDIM 6, and says so when an array is nested deeper. */
    private static final int MAX_DIM = 6;

    // Where the reader stands within one brace level. PostgreSQL rejects a literal by finding a
    // character that cannot follow what came before it, so the shape check is a walk over these.
    private static final int NO_LEVEL = 0;
    private static final int LEVEL_STARTED = 1;
    private static final int ELEM_STARTED = 2;
    private static final int QUOTED_ELEM_STARTED = 3;
    private static final int QUOTED_ELEM_COMPLETED = 4;
    private static final int LEVEL_COMPLETED = 5;
    private static final int LEVEL_DELIMITED = 6;
    private static final int ELEM_DELIMITED = 7;

    private final List<Object> elements;
    private final int[] lowerBounds;
    private final int[] dims;

    private ArrayLiteral(List<Object> elements, int[] lowerBounds, int[] dims) {
        this.elements = elements;
        this.lowerBounds = lowerBounds;
        this.dims = dims;
    }

    /** The elements, nested one list per dimension. Each leaf is the element's raw text or null. */
    List<Object> elements() {
        return elements;
    }

    /** True when the array states a lower bound other than 1, which its text form has to carry. */
    boolean hasCustomLowerBounds() {
        for (int i = 0; i < lowerBounds.length; i++) {
            if (lowerBounds[i] != 1) return true;
        }
        return false;
    }

    /** The {@code [lb:ub]...=} prefix PostgreSQL writes in front of such an array. */
    String boundsPrefix() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lowerBounds.length; i++) {
            sb.append('[').append(lowerBounds[i]).append(':')
                    .append(lowerBounds[i] + dims[i] - 1).append(']');
        }
        return sb.append('=').toString();
    }

    /**
     * The bounds an array's text form states in front of its braces, as {@code {lower, upper}} with
     * one entry per dimension, or null when it states none. An array with a lower bound other than
     * 1 is kept in that form, so every function that reports a dimension has to read it back.
     */
    static int[][] statedBounds(String text) {
        String s = text.trim();
        if (!s.startsWith("[")) return null;
        int assign = s.indexOf("]=");
        if (assign < 0) return null;
        List<Integer> lower = new ArrayList<Integer>();
        List<Integer> upper = new ArrayList<Integer>();
        int p = 0;
        while (p < s.length() && s.charAt(p) == '[') {
            int close = s.indexOf(']', p);
            if (close < 0) return null;
            String item = s.substring(p + 1, close);
            int colon = item.indexOf(':');
            try {
                if (colon < 0) {
                    lower.add(1);
                    upper.add(Integer.parseInt(item.trim()));
                } else {
                    lower.add(Integer.parseInt(item.substring(0, colon).trim()));
                    upper.add(Integer.parseInt(item.substring(colon + 1).trim()));
                }
            } catch (NumberFormatException e) {
                return null;
            }
            p = close + 1;
        }
        if (p >= s.length() || s.charAt(p) != '=') return null;
        int[][] bounds = new int[2][lower.size()];
        for (int i = 0; i < lower.size(); i++) {
            bounds[0][i] = lower.get(i);
            bounds[1][i] = upper.get(i);
        }
        return bounds;
    }

    /** The {@code {...}} part of an array's text form, with any {@code [lb:ub]=} prefix removed. */
    static String body(String text) {
        String s = text.trim();
        if (s.startsWith("[")) {
            int assign = s.indexOf("]=");
            if (assign > 0) return s.substring(assign + 2).trim();
        }
        return s;
    }

    static ArrayLiteral parse(String text) {
        int p = 0;
        List<int[]> declared = new ArrayList<int[]>();
        // Dimension items may be separated by whitespace but never contain any
        for (;;) {
            while (p < text.length() && isSpace(text.charAt(p))) p++;
            if (p >= text.length() || text.charAt(p) != '[') break;
            p++;
            if (declared.size() >= MAX_DIM) throw dimensionOverflow(declared.size() + 1);
            int q = skipSignedDigits(text, p);
            if (q == p) throw malformed(text);
            int lb = 1;
            if (q < text.length() && text.charAt(q) == ':') {
                lb = toBound(text.substring(p, q), text);
                p = q + 1;
                q = skipSignedDigits(text, p);
                if (q == p) throw malformed(text);
            }
            if (q >= text.length() || text.charAt(q) != ']') throw malformed(text);
            int ub = toBound(text.substring(p, q), text);
            p = q + 1;
            if (ub < lb) {
                throw new MemgresException("upper bound cannot be less than lower bound", "2202E");
            }
            declared.add(new int[]{lb, ub - lb + 1});
        }

        if (!declared.isEmpty()) {
            if (p >= text.length() || text.charAt(p) != '=') throw malformed(text);
            p++;
            while (p < text.length() && isSpace(text.charAt(p))) p++;
        }
        if (p >= text.length() || text.charAt(p) != '{') throw malformed(text);

        int[] dims = countDimensions(text, p);
        int[] lowerBounds = new int[dims.length];
        for (int i = 0; i < dims.length; i++) lowerBounds[i] = 1;
        if (!declared.isEmpty()) {
            if (declared.size() != dims.length) throw malformed(text);
            for (int i = 0; i < dims.length; i++) {
                if (declared.get(i)[1] != dims[i]) throw malformed(text);
                lowerBounds[i] = declared.get(i)[0];
            }
        }
        if (dims.length == 0) {
            return new ArrayLiteral(new ArrayList<Object>(), new int[0], new int[0]);
        }
        List<Object> flat = readElements(text, p);
        return new ArrayLiteral(reshape(flat, dims, 0, new int[]{0}), lowerBounds, dims);
    }

    /**
     * The shape the braces describe, one entry per dimension, or an empty array for a literal with
     * no elements at all. Everything PostgreSQL calls a malformed literal is refused here.
     */
    private static int[] countDimensions(String str, int start) {
        int[] temp = new int[MAX_DIM];
        int[] nelems = new int[MAX_DIM];
        int[] nelemsLast = new int[MAX_DIM];
        for (int i = 0; i < MAX_DIM; i++) {
            nelems[i] = 1;
            nelemsLast[i] = -1;
        }
        int state = NO_LEVEL;
        int nestLevel = 0;
        int ndim = 0;
        boolean inQuotes = false;
        boolean eoArray = false;
        boolean emptyArray = true;
        int ptr = start;
        while (!eoArray) {
            boolean itemdone = false;
            while (!itemdone) {
                if (state == ELEM_STARTED || state == QUOTED_ELEM_STARTED) emptyArray = false;
                char c = ptr < str.length() ? str.charAt(ptr) : '\0';
                if (c == '\0') {
                    throw malformed(str);
                } else if (c == '\\') {
                    if (state != LEVEL_STARTED && state != ELEM_STARTED
                            && state != QUOTED_ELEM_STARTED && state != ELEM_DELIMITED) {
                        throw malformed(str);
                    }
                    if (state != QUOTED_ELEM_STARTED) state = ELEM_STARTED;
                    if (ptr + 1 >= str.length()) throw malformed(str);
                    ptr++;
                } else if (c == '"') {
                    if (state != LEVEL_STARTED && state != QUOTED_ELEM_STARTED
                            && state != ELEM_DELIMITED) {
                        throw malformed(str);
                    }
                    inQuotes = !inQuotes;
                    state = inQuotes ? QUOTED_ELEM_STARTED : QUOTED_ELEM_COMPLETED;
                } else if (c == '{' && !inQuotes) {
                    if (state != NO_LEVEL && state != LEVEL_STARTED && state != LEVEL_DELIMITED) {
                        throw malformed(str);
                    }
                    state = LEVEL_STARTED;
                    if (nestLevel >= MAX_DIM) throw dimensionOverflow(nestLevel + 1);
                    temp[nestLevel] = 0;
                    nestLevel++;
                    if (ndim < nestLevel) ndim = nestLevel;
                } else if (c == '}' && !inQuotes) {
                    // A level closed straight after it opened holds nothing; every sibling level
                    // then has to hold nothing too, which is how {{},{1}} is caught.
                    boolean levelEmpty = state == LEVEL_STARTED;
                    if (!levelEmpty && state != ELEM_STARTED && state != QUOTED_ELEM_COMPLETED
                            && state != LEVEL_COMPLETED) {
                        throw malformed(str);
                    }
                    if (nestLevel == 0) throw malformed(str);
                    state = LEVEL_COMPLETED;
                    nestLevel--;
                    int count = levelEmpty ? 0 : nelems[nestLevel];
                    if (nelemsLast[nestLevel] >= 0 && count != nelemsLast[nestLevel]) {
                        throw malformed(str);
                    }
                    nelemsLast[nestLevel] = count;
                    nelems[nestLevel] = 1;
                    if (nestLevel == 0) {
                        eoArray = true;
                        itemdone = true;
                    } else {
                        temp[nestLevel - 1]++;
                    }
                } else if (!inQuotes && c == ',') {
                    if (state != ELEM_STARTED && state != QUOTED_ELEM_COMPLETED
                            && state != LEVEL_COMPLETED) {
                        throw malformed(str);
                    }
                    state = state == LEVEL_COMPLETED ? LEVEL_DELIMITED : ELEM_DELIMITED;
                    itemdone = true;
                    if (nestLevel > 0) nelems[nestLevel - 1]++;
                } else if (!inQuotes && !isSpace(c)) {
                    if (state != LEVEL_STARTED && state != ELEM_STARTED
                            && state != ELEM_DELIMITED) {
                        throw malformed(str);
                    }
                    state = ELEM_STARTED;
                }
                if (!itemdone) ptr++;
            }
            temp[ndim - 1]++;
            ptr++;
        }
        while (ptr < str.length()) {
            if (!isSpace(str.charAt(ptr))) throw malformed(str);
            ptr++;
        }
        if (emptyArray) return new int[0];
        int[] dims = new int[ndim];
        System.arraycopy(temp, 0, dims, 0, ndim);
        return dims;
    }

    /**
     * The elements in row-major order, each as the text PostgreSQL would hand to the element type's
     * input function. Only an unquoted, unescaped NULL is the SQL null; {@code {"NULL"}} is the
     * three-letter string, which is why the quoting has to be tracked rather than the text alone.
     */
    private static List<Object> readElements(String str, int start) {
        List<Object> flat = new ArrayList<Object>();
        int ptr = start;
        int nest = 0;
        boolean inQuotes = false;
        boolean eoArray = false;
        while (!eoArray) {
            StringBuilder sb = new StringBuilder();
            int kept = 0;
            boolean leadingSpace = true;
            boolean quoted = false;
            boolean captured = false;
            boolean itemdone = false;
            while (!itemdone) {
                char c = str.charAt(ptr);
                if (c == '\\') {
                    sb.append(str.charAt(ptr + 1));
                    ptr += 2;
                    leadingSpace = false;
                    kept = sb.length();
                    quoted = true;
                } else if (c == '"') {
                    inQuotes = !inQuotes;
                    if (inQuotes) leadingSpace = false;
                    else kept = sb.length();
                    quoted = true;
                    ptr++;
                } else if (c == '{' && !inQuotes) {
                    nest++;
                    ptr++;
                } else if (c == '}' && !inQuotes) {
                    // The closing brace of an inner level ends the element but not the item: the
                    // delimiter after it does, which is why the element is captured only once.
                    if (!captured) {
                        flat.add(finish(sb, kept, quoted));
                        captured = true;
                    }
                    nest--;
                    ptr++;
                    if (nest == 0) {
                        eoArray = true;
                        itemdone = true;
                    }
                } else if (c == ',' && !inQuotes) {
                    if (!captured) flat.add(finish(sb, kept, quoted));
                    itemdone = true;
                    ptr++;
                } else if (isSpace(c) && !inQuotes) {
                    // Leading whitespace is dropped outright; interior whitespace is kept but does
                    // not extend the element, so trailing whitespace falls away with it.
                    if (!leadingSpace) sb.append(c);
                    ptr++;
                } else {
                    sb.append(c);
                    ptr++;
                    leadingSpace = false;
                    kept = sb.length();
                }
            }
        }
        return flat;
    }

    private static Object finish(StringBuilder sb, int kept, boolean quoted) {
        String text = sb.substring(0, kept);
        if (!quoted && text.equalsIgnoreCase("NULL")) return null;
        return text;
    }

    /** Fold the row-major elements back into one list per dimension. */
    private static List<Object> reshape(List<Object> flat, int[] dims, int level, int[] cursor) {
        List<Object> out = new ArrayList<Object>(dims[level]);
        for (int i = 0; i < dims[level]; i++) {
            if (level == dims.length - 1) {
                out.add(cursor[0] < flat.size() ? flat.get(cursor[0]) : null);
                cursor[0]++;
            } else {
                out.add(reshape(flat, dims, level + 1, cursor));
            }
        }
        return out;
    }

    private static int skipSignedDigits(String text, int from) {
        int q = from;
        while (q < text.length()) {
            char c = text.charAt(q);
            if (c >= '0' && c <= '9') q++;
            else if (c == '-' || c == '+') q++;
            else break;
        }
        return q;
    }

    private static int toBound(String text, String source) {
        try {
            return Integer.parseInt(text.startsWith("+") ? text.substring(1) : text);
        } catch (NumberFormatException e) {
            throw malformed(source);
        }
    }

    /** PostgreSQL's scanner_isspace, which is what array input skips between tokens. */
    private static boolean isSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == 0x0b;
    }

    private static MemgresException malformed(String text) {
        return new MemgresException("malformed array literal: \"" + text + "\"", "22P02");
    }

    private static MemgresException dimensionOverflow(int ndim) {
        return new MemgresException("number of array dimensions (" + ndim
                + ") exceeds the maximum allowed (" + MAX_DIM + ")", "54000");
    }
}
