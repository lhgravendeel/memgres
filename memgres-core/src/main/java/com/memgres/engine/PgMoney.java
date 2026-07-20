package com.memgres.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * Wrapper for PostgreSQL money type values.
 * Stores the underlying value as a BigDecimal with scale 2 (cents precision).
 * PG money range: -92233720368547758.08 to +92233720368547758.07 (int64 cents).
 */
public class PgMoney implements Comparable<PgMoney> {

    // PG stores money as int64 cents
    private static final BigDecimal MAX_MONEY = new BigDecimal("92233720368547758.07");
    private static final BigDecimal MIN_MONEY = new BigDecimal("-92233720368547758.08");

    private final BigDecimal value;

    public PgMoney(BigDecimal value) {
        BigDecimal scaled = value.setScale(2, RoundingMode.HALF_UP);
        if (scaled.compareTo(MAX_MONEY) > 0 || scaled.compareTo(MIN_MONEY) < 0) {
            throw new MemgresException("value \"" + value.toPlainString() + "\" is out of range for type money", "22003");
        }
        this.value = scaled;
    }

    public BigDecimal getValue() {
        return value;
    }

    /** Parse money from string with PG-compatible formats. */
    public static PgMoney parse(String input) {
        String s = input.trim();
        boolean negative = false;
        // Handle parenthetical negation: ($123.45) or (123.45)
        if (s.startsWith("(") && s.endsWith(")")) {
            s = s.substring(1, s.length() - 1).trim();
            negative = true;
        }
        // Handle leading/trailing minus or negative sign
        if (s.startsWith("-")) {
            negative = !negative;
            s = s.substring(1).trim();
        } else if (s.endsWith("-")) {
            negative = !negative;
            s = s.substring(0, s.length() - 1).trim();
        }
        // Strip currency symbol
        if (s.startsWith("$")) s = s.substring(1).trim();
        // Strip thousands separators
        s = s.replace(",", "");
        if (s.isEmpty()) {
            throw new MemgresException("invalid input syntax for type money: \"" + input + "\"", "22P02");
        }
        try {
            BigDecimal bd = new BigDecimal(s);
            if (negative) bd = bd.negate();
            return new PgMoney(bd);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type money: \"" + input + "\"", "22P02");
        }
    }

    public PgMoney add(PgMoney other) {
        return new PgMoney(value.add(other.value));
    }

    public PgMoney subtract(PgMoney other) {
        return new PgMoney(value.subtract(other.value));
    }

    public PgMoney multiply(Number factor) {
        return new PgMoney(value.multiply(BigDecimal.valueOf(factor.doubleValue())));
    }

    /** money / money → float8 (PG behavior). */
    public double divideByMoney(PgMoney other) {
        if (other.value.signum() == 0) {
            throw new MemgresException("division by zero", "22012");
        }
        return value.doubleValue() / other.value.doubleValue();
    }

    /** money / numeric → money. */
    public PgMoney divideByNumber(Number divisor) {
        if (divisor.doubleValue() == 0) {
            throw new MemgresException("division by zero", "22012");
        }
        return new PgMoney(value.divide(BigDecimal.valueOf(divisor.doubleValue()), 2, RoundingMode.HALF_UP));
    }

    /** Returns the money-formatted string with '$' prefix and thousands separators. */
    @Override
    public String toString() {
        DecimalFormat df = new DecimalFormat("#,##0.00", DecimalFormatSymbols.getInstance(Locale.US));
        String formatted = df.format(value.abs());
        if (value.signum() < 0) {
            return "-$" + formatted;
        }
        return "$" + formatted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PgMoney)) return false;
        return value.compareTo(((PgMoney) o).value) == 0;
    }

    @Override
    public int hashCode() {
        return value.stripTrailingZeros().hashCode();
    }

    @Override
    public int compareTo(PgMoney other) {
        return value.compareTo(other.value);
    }
}
