package com.memgres.engine;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * PostgreSQL's {@code float4out} / {@code float8out}.
 *
 * <p>With the default {@code extra_float_digits}, PG emits the <em>shortest</em> decimal
 * string that reads back as the same binary value (its bundled Ryū implementation), then
 * formats it with its own notation rules rather than C's {@code %g}: scientific notation
 * when the leading digit's decimal exponent is below -4 or at least the type's decimal
 * digit count (6 for {@code real}, 15 for {@code double precision}), and a signed
 * exponent padded to at least two digits.
 *
 * <p>The digits are found by rounding the value's exact binary expansion to successively
 * more places until it reads back unchanged. Java's own conversions are no help here:
 * {@code Double.toString} is not shortest-round-trip before JDK 19, and {@code %e} pads
 * with zeros past the shortest representation instead of giving the true expansion.
 */
public final class PgFloatFormat {

    private PgFloatFormat() {
    }

    /** PG's {@code float8out}. */
    public static String float8out(double d) {
        if (Double.isNaN(d)) return "NaN";
        if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
        boolean negative = (Double.doubleToRawLongBits(d) & 0x8000000000000000L) != 0;
        if (d == 0.0) return negative ? "-0" : "0";

        double a = Math.abs(d);
        BigDecimal exact = new BigDecimal(a);
        BigDecimal low = midpoint(exact, Math.nextDown(a));
        BigDecimal high = midpoint(exact, Math.nextUp(a));
        for (int precision = 1; precision <= 17; precision++) {
            BigDecimal candidate = exact.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (Double.parseDouble(candidate.toString()) == a
                    && strictlyInside(candidate, low, high)) {
                return render(negative, candidate, 15);
            }
        }
        return render(negative, exact, 15);
    }

    /** PG's {@code float4out}. */
    public static String float4out(float f) {
        if (Float.isNaN(f)) return "NaN";
        if (Float.isInfinite(f)) return f > 0 ? "Infinity" : "-Infinity";
        boolean negative = (Float.floatToRawIntBits(f) & 0x80000000) != 0;
        if (f == 0.0f) return negative ? "-0" : "0";

        float a = Math.abs(f);
        BigDecimal exact = new BigDecimal((double) a);
        BigDecimal low = midpoint(exact, Math.nextDown(a));
        BigDecimal high = midpoint(exact, Math.nextUp(a));
        for (int precision = 1; precision <= 9; precision++) {
            BigDecimal candidate = exact.round(new MathContext(precision, RoundingMode.HALF_EVEN));
            if (Float.parseFloat(candidate.toString()) == a
                    && strictlyInside(candidate, low, high)) {
                return render(negative, candidate, 6);
            }
        }
        return render(negative, exact, 6);
    }

    private static BigDecimal midpoint(BigDecimal exact, double neighbour) {
        if (Double.isInfinite(neighbour)) return null;
        return exact.add(new BigDecimal(neighbour)).divide(BigDecimal.valueOf(2));
    }

    /**
     * A decimal sitting exactly on the midpoint to a neighbouring binary value still
     * reads back unchanged under round-half-even, but Ryū — and therefore PG — will not
     * use it, so accepting it would print one digit fewer than PG does.
     */
    private static boolean strictlyInside(BigDecimal candidate, BigDecimal low, BigDecimal high) {
        if (low != null && candidate.compareTo(low) <= 0) return false;
        return high == null || candidate.compareTo(high) < 0;
    }

    /**
     * @param sciThreshold decimal digit count of the type: exponents at or above this use
     *                     scientific notation (FLT_DIG = 6, DBL_DIG = 15)
     */
    private static String render(boolean negative, BigDecimal value, int sciThreshold) {
        BigDecimal v = value.stripTrailingZeros();
        String digits = v.unscaledValue().abs().toString();
        int k = digits.length();
        int exponent = (k - 1) - v.scale();

        StringBuilder sb = new StringBuilder();
        if (negative) sb.append('-');
        if (exponent < -4 || exponent >= sciThreshold) {
            sb.append(digits.charAt(0));
            if (k > 1) sb.append('.').append(digits.substring(1));
            sb.append('e').append(exponent < 0 ? '-' : '+');
            String exp = Integer.toString(Math.abs(exponent));
            if (exp.length() < 2) sb.append('0');
            sb.append(exp);
        } else if (exponent >= k - 1) {
            sb.append(digits);
            for (int i = 0; i < exponent - (k - 1); i++) sb.append('0');
        } else if (exponent >= 0) {
            sb.append(digits, 0, exponent + 1).append('.').append(digits.substring(exponent + 1));
        } else {
            sb.append("0.");
            for (int i = 0; i < -exponent - 1; i++) sb.append('0');
            sb.append(digits);
        }
        return sb.toString();
    }
}
