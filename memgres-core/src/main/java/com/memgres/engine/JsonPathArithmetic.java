package com.memgres.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Arithmetic in a jsonpath, which is written around the path rather than inside it:
 * {@code 7 - $[0]}, {@code - $.x}, {@code (1 + 2) * 3}. The path walker cannot see these because
 * an operand may come before the {@code $}, so the expression is parsed first and each path it
 * names is walked as an operand.
 *
 * <p>The two operand rules differ, and PG reports them with different SQLSTATEs: a binary operator
 * wants exactly one numeric value on each side, while a unary operator unwraps an array operand
 * and applies itself to every element.
 */
final class JsonPathArithmetic {

    /** How a path operand written inside the expression is resolved against the document. */
    interface PathEvaluator {
        List<String> evaluate(String pathExpr);
    }

    /** PG divides to at least this many significant digits. */
    private static final int MIN_SIG_DIGITS = 16;

    /** numeric stores its digits in groups of four, and the division scale is chosen per group. */
    private static final int NBASE_DIGITS = 4;

    private final String expr;
    private final PathEvaluator paths;
    private int pos;

    private JsonPathArithmetic(String expr, PathEvaluator paths) {
        this.expr = expr;
        this.paths = paths;
    }

    /**
     * True when the expression applies an arithmetic operator outside any bracket, quote or
     * filter. A comparison anywhere at that level makes it a predicate instead, which is a
     * different thing to evaluate.
     */
    static boolean isArithmetic(String expr) {
        String trimmed = expr.trim();
        if (trimmed.startsWith("(") && isGroup(trimmed)) {
            return isArithmetic(trimmed.substring(1, trimmed.length() - 1));
        }
        int depth = 0;
        boolean inQuote = false;
        boolean found = false;
        for (int i = 0; i < trimmed.length(); i++) {
            char c = trimmed.charAt(i);
            if (inQuote) {
                if (c == '\\') i++;
                else if (c == '"') inQuote = false;
                continue;
            }
            if (c == '"') { inQuote = true; continue; }
            if (c == '(' || c == '[') { depth++; continue; }
            if (c == ')' || c == ']') { depth--; continue; }
            if (depth != 0) continue;
            if (c == '=' || c == '<' || c == '>' || c == '!' || c == '&' || c == '|') return false;
            if (c == '*' && isWildcard(trimmed, i)) continue;
            if (c == '+' || c == '-' || c == '*' || c == '/' || c == '%') found = true;
        }
        return found;
    }

    /** True when the whole string is one parenthesised group, as in {@code (1 + 2)}. */
    private static boolean isGroup(String s) {
        int depth = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')' && --depth == 0) return i == s.length() - 1;
        }
        return false;
    }

    /** The star in {@code $.*} and {@code $.**} names every member, it does not multiply. */
    private static boolean isWildcard(String s, int at) {
        char prev = at > 0 ? s.charAt(at - 1) : '\0';
        return prev == '.' || prev == '*';
    }

    /** @return every value the expression produces, as JSON text */
    static List<String> evaluate(String expr, PathEvaluator paths) {
        JsonPathArithmetic p = new JsonPathArithmetic(expr, paths);
        List<String> result = p.parseAdditive();
        p.skipSpace();
        if (p.pos < expr.length()) throw endOfInput();
        return result;
    }

    private List<String> parseAdditive() {
        List<String> left = parseMultiplicative();
        while (true) {
            skipSpace();
            char c = peek();
            if (c != '+' && c != '-') return left;
            pos++;
            left = binary(c, left, parseMultiplicative());
        }
    }

    private List<String> parseMultiplicative() {
        List<String> left = parseUnary();
        while (true) {
            skipSpace();
            char c = peek();
            if (c != '*' && c != '/' && c != '%') return left;
            pos++;
            left = binary(c, left, parseUnary());
        }
    }

    private List<String> parseUnary() {
        skipSpace();
        char c = peek();
        if (c == '+' || c == '-') {
            pos++;
            return unary(c, parseUnary());
        }
        return parsePrimary();
    }

    private List<String> parsePrimary() {
        skipSpace();
        if (pos >= expr.length()) throw endOfInput();
        char c = expr.charAt(pos);
        if (c == '(') {
            pos++;
            List<String> inner = parseAdditive();
            skipSpace();
            if (peek() != ')') throw endOfInput();
            pos++;
            return inner;
        }
        if (c == '$' || c == '@') {
            return paths.evaluate(readPath());
        }
        List<String> single = new ArrayList<>();
        if (c == '"') {
            single.add(readQuoted());
            return single;
        }
        single.add(readNumber());
        return single;
    }

    /** A path operand runs until an operator that is not inside a bracket, filter or string. */
    private String readPath() {
        int start = pos;
        int depth = 0;
        boolean inQuote = false;
        while (pos < expr.length()) {
            char c = expr.charAt(pos);
            if (inQuote) {
                if (c == '\\') pos++;
                else if (c == '"') inQuote = false;
                pos++;
                continue;
            }
            if (c == '"') { inQuote = true; pos++; continue; }
            if (c == '(' || c == '[') depth++;
            else if (c == ')' || c == ']') {
                if (depth == 0) break;   // the closing paren of an enclosing group
                depth--;
            } else if (depth == 0 && (c == '+' || c == '-' || c == '/' || c == '%'
                    || (c == '*' && !isWildcard(expr, pos)))) {
                break;
            }
            pos++;
        }
        return expr.substring(start, pos).trim();
    }

    private String readQuoted() {
        int start = pos++;
        while (pos < expr.length() && expr.charAt(pos) != '"') {
            if (expr.charAt(pos) == '\\') pos++;
            pos++;
        }
        if (pos >= expr.length()) throw endOfInput();
        pos++;
        return expr.substring(start, pos);
    }

    private String readNumber() {
        int start = pos;
        while (pos < expr.length() && Character.isDigit(expr.charAt(pos))) pos++;
        if (pos < expr.length() && expr.charAt(pos) == '.') {
            pos++;
            while (pos < expr.length() && Character.isDigit(expr.charAt(pos))) pos++;
        }
        if (pos < expr.length() && (expr.charAt(pos) == 'e' || expr.charAt(pos) == 'E')) {
            pos++;
            if (pos < expr.length() && (expr.charAt(pos) == '+' || expr.charAt(pos) == '-')) pos++;
            while (pos < expr.length() && Character.isDigit(expr.charAt(pos))) pos++;
        }
        if (pos == start) throw endOfInput();
        return expr.substring(start, pos);
    }

    private List<String> binary(char op, List<String> left, List<String> right) {
        BigDecimal l = singleNumeric(left, op, "left");
        BigDecimal r = singleNumeric(right, op, "right");
        BigDecimal result;
        switch (op) {
            case '+': result = l.add(r); break;
            case '-': result = l.subtract(r); break;
            case '*': result = l.multiply(r); break;
            case '/':
                if (r.signum() == 0) throw new MemgresException("division by zero", "22012");
                result = divide(l, r);
                break;
            default:
                if (r.signum() == 0) throw new MemgresException("division by zero", "22012");
                result = l.remainder(r);
        }
        List<String> out = new ArrayList<>();
        out.add(result.toPlainString());
        return out;
    }

    /**
     * A unary operator is the one place lax mode unwraps: an array operand contributes each of
     * its elements, so {@code - $.x} over {@code [2,3,4]} yields three values.
     */
    private List<String> unary(char op, List<String> operand) {
        List<String> out = new ArrayList<>();
        for (String value : operand) {
            if (JsonOperations.isArray(value)) {
                for (String elem : JsonOperations.parseArrayElements(value)) out.add(applyUnary(op, elem));
            } else {
                out.add(applyUnary(op, value));
            }
        }
        return out;
    }

    private String applyUnary(char op, String value) {
        BigDecimal n = numericOrNull(value);
        if (n == null) {
            throw new MemgresException(
                    "operand of unary jsonpath operator " + op + " is not a numeric value", "2203B");
        }
        return (op == '-' ? n.negate() : n).toPlainString();
    }

    private BigDecimal singleNumeric(List<String> values, char op, String side) {
        BigDecimal n = values.size() == 1 ? numericOrNull(values.get(0)) : null;
        if (n == null) {
            throw new MemgresException(side + " operand of jsonpath operator " + op
                    + " is not a single numeric value", "22038");
        }
        return n;
    }

    private static BigDecimal numericOrNull(String value) {
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * PG picks a division scale giving at least sixteen significant digits, counted in the
     * four-digit groups numeric stores its value in, and keeps the trailing zeros that produces:
     * 10/4 comes out as 2.5000000000000000 while 1/3 gets four more places.
     */
    private static BigDecimal divide(BigDecimal a, BigDecimal b) {
        int qweight = nbaseWeight(a) - nbaseWeight(b);
        if (leadingGroup(a).compareTo(leadingGroup(b)) < 0) qweight--;
        int rscale = Math.max(MIN_SIG_DIGITS - qweight * NBASE_DIGITS,
                Math.max(a.scale(), Math.max(b.scale(), 0)));
        return a.divide(b, rscale, RoundingMode.HALF_UP);
    }

    /** Which four-digit group holds the leading digit, counted from the decimal point. */
    private static int nbaseWeight(BigDecimal v) {
        if (v.signum() == 0) return 0;
        int intDigits = v.precision() - v.scale();
        return Math.floorDiv(intDigits - 1, NBASE_DIGITS);
    }

    /** The value of that leading group, which decides whether the quotient loses a digit. */
    private static BigInteger leadingGroup(BigDecimal v) {
        if (v.signum() == 0) return BigInteger.ZERO;
        return v.abs().movePointLeft(nbaseWeight(v) * NBASE_DIGITS).toBigInteger();
    }

    private static MemgresException endOfInput() {
        return new MemgresException("syntax error at end of jsonpath input", "42601");
    }

    private void skipSpace() {
        while (pos < expr.length() && Character.isWhitespace(expr.charAt(pos))) pos++;
    }

    private char peek() {
        return pos < expr.length() ? expr.charAt(pos) : '\0';
    }
}
