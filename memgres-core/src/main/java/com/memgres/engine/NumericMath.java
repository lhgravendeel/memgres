package com.memgres.engine;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/**
 * The transcendental functions of PostgreSQL's {@code numeric}, computed in {@link BigDecimal}.
 *
 * <p>Routing {@code numeric} through {@code double} costs both range and scale. Range, because
 * {@code exp(-1000::numeric)} is a perfectly ordinary numeric that {@code double} rounds to zero,
 * and {@code ln(1e-400::numeric)} then reports a logarithm of zero PostgreSQL never raises. Scale,
 * because PostgreSQL picks a result scale from the input — {@code sqrt(2::numeric)} answers with
 * fifteen decimals and {@code exp(1::numeric)} with sixteen — while a {@code double} answers with
 * whatever seventeen significant digits it happens to hold.
 *
 * <p>The scale rules mirror PostgreSQL's own: aim for {@code NUMERIC_MIN_SIG_DIGITS} significant
 * digits by subtracting the estimated decimal weight of the result, then never go below the
 * argument's own display scale.
 */
final class NumericMath {

    /** PG's NUMERIC_MIN_SIG_DIGITS: the significant digits a transcendental result aims for. */
    private static final int MIN_SIG_DIGITS = 16;

    /** PG's NUMERIC_MAX_DISPLAY_SCALE — the widest scale a computed result is given. */
    private static final int MAX_DISPLAY_SCALE = 1000;

    /** PG's NUMERIC_MAX_RESULT_SCALE, which bounds the weight estimate before it is used. */
    private static final int MAX_RESULT_SCALE = 2000;

    private static final double LOG10_E = 0.434294481903252;
    private static final double LN10 = 2.302585092994046;

    /** Digits of headroom carried through an intermediate so the last kept digit is right. */
    private static final int GUARD = 24;

    private NumericMath() {}

    // ---- weights ----

    /** {@code floor(log10(|v|))} — PG's decimal weight — with zero reported as weight 0. */
    static int decimalWeight(BigDecimal v) {
        if (v.signum() == 0) return 0;
        return v.precision() - v.scale() - 1;
    }

    /**
     * {@code ln(|v|)} as a double, computed from the decimal weight rather than by converting
     * {@code v}: {@code 1e-400} has no double form, and converting it first turns its logarithm
     * into negative infinity.
     */
    private static double lnAsDouble(BigDecimal v) {
        if (v.signum() == 0) return Double.NEGATIVE_INFINITY;
        int w = decimalWeight(v);
        double mantissa = v.abs().movePointLeft(w).doubleValue();
        return Math.log(mantissa) + w * LN10;
    }

    /** PG's estimate_ln_dweight: the decimal weight {@code ln(v)} is expected to have. */
    private static int estimateLnDweight(BigDecimal v) {
        // Near one the logarithm is tiny and its weight comes from ln(1+x) ~= x instead.
        if (v.compareTo(new BigDecimal("0.9")) >= 0 && v.compareTo(new BigDecimal("1.1")) <= 0) {
            BigDecimal x = v.subtract(BigDecimal.ONE);
            return x.signum() == 0 ? 0 : decimalWeight(x);
        }
        double ln = lnAsDouble(v);
        if (ln == 0 || Double.isInfinite(ln) || Double.isNaN(ln)) return 0;
        // PG truncates towards zero here, so a logarithm below 1 in magnitude estimates as 0.
        return (int) Math.log10(Math.abs(ln));
    }

    /** Clamp a computed result scale the way every one of PG's rscale selections ends. */
    private static int clampScale(int rscale, int minimum) {
        int r = Math.max(rscale, minimum);
        if (r < 0) r = 0;
        return Math.min(r, MAX_DISPLAY_SCALE);
    }

    // ---- the constants, to as many digits as asked for ----

    /** {@code 2*atanh(1/n)} by its odd-power series, which is how ln 2 and ln 10 are built. */
    private static BigDecimal twoAtanhReciprocal(int n, MathContext mc) {
        BigDecimal z = BigDecimal.ONE.divide(BigDecimal.valueOf(n), mc);
        BigDecimal zSq = z.multiply(z, mc);
        BigDecimal term = z;
        BigDecimal sum = z;
        for (int k = 3; ; k += 2) {
            term = term.multiply(zSq, mc);
            BigDecimal add = term.divide(BigDecimal.valueOf(k), mc);
            if (add.signum() == 0) break;
            BigDecimal next = sum.add(add, mc);
            if (next.compareTo(sum) == 0) break;
            sum = next;
        }
        return sum.multiply(BigDecimal.valueOf(2), mc);
    }

    private static BigDecimal ln2(MathContext mc) {
        return twoAtanhReciprocal(3, mc);
    }

    private static BigDecimal ln10(MathContext mc) {
        // 10 = 8 * 1.25, so ln 10 = 3 ln 2 + 2 atanh(1/9).
        return ln2(mc).multiply(BigDecimal.valueOf(3), mc).add(twoAtanhReciprocal(9, mc), mc);
    }

    // ---- natural logarithm ----

    /** {@code ln(x)} for a positive {@code x}, to {@code prec} significant digits. */
    static BigDecimal lnTo(BigDecimal x, int prec) {
        MathContext mc = new MathContext(prec + GUARD, RoundingMode.HALF_EVEN);
        int w = decimalWeight(x);
        BigDecimal m = x.movePointLeft(w);              // m in [1, 10)
        // Halve until m is in [1, 2), where the atanh series converges in about one digit a term.
        int halvings = 0;
        BigDecimal two = BigDecimal.valueOf(2);
        while (m.compareTo(two) >= 0) {
            m = m.divide(two, mc);
            halvings++;
        }
        BigDecimal z = m.subtract(BigDecimal.ONE).divide(m.add(BigDecimal.ONE), mc);
        BigDecimal zSq = z.multiply(z, mc);
        BigDecimal term = z;
        BigDecimal sum = z;
        for (int k = 3; ; k += 2) {
            term = term.multiply(zSq, mc);
            BigDecimal add = term.divide(BigDecimal.valueOf(k), mc);
            if (add.signum() == 0) break;
            BigDecimal next = sum.add(add, mc);
            if (next.compareTo(sum) == 0) break;
            sum = next;
        }
        BigDecimal lnM = sum.multiply(two, mc);
        BigDecimal result = lnM;
        if (halvings != 0) result = result.add(ln2(mc).multiply(BigDecimal.valueOf(halvings), mc), mc);
        if (w != 0) result = result.add(ln10(mc).multiply(BigDecimal.valueOf(w), mc), mc);
        return result;
    }

    /** {@code exp(x)} to {@code prec} significant digits, for an {@code x} of any magnitude. */
    private static BigDecimal expTo(BigDecimal x, int prec) {
        MathContext mc = new MathContext(prec + GUARD, RoundingMode.HALF_EVEN);
        BigDecimal intPart = new BigDecimal(x.toBigInteger());
        BigDecimal frac = x.subtract(intPart);
        BigDecimal result = expSeries(frac, mc);
        int n = intPart.intValueExact();
        if (n != 0) {
            BigDecimal e = expSeries(BigDecimal.ONE, mc);
            BigDecimal p = e.pow(Math.abs(n), mc);
            result = n > 0 ? result.multiply(p, mc) : result.divide(p, mc);
        }
        return result;
    }

    /** The Taylor series for {@code exp}, used only where {@code |x| <= 1} keeps it short. */
    private static BigDecimal expSeries(BigDecimal x, MathContext mc) {
        if (x.signum() == 0) return BigDecimal.ONE;
        BigDecimal term = BigDecimal.ONE;
        BigDecimal sum = BigDecimal.ONE;
        for (int k = 1; ; k++) {
            term = term.multiply(x, mc).divide(BigDecimal.valueOf(k), mc);
            if (term.signum() == 0) break;
            BigDecimal next = sum.add(term, mc);
            if (next.compareTo(sum) == 0) break;
            sum = next;
        }
        return sum;
    }

    // ---- the SQL-visible entry points ----

    /** {@code sqrt(numeric)}, with PG's result scale. */
    static BigDecimal sqrt(BigDecimal arg) {
        if (arg.signum() < 0) {
            throw new MemgresException("cannot take square root of a negative number", "2201F");
        }
        // PG works in base-10000 groups: the result's weight is half the argument's, so the scale
        // that leaves 16 significant digits is 16 minus that.
        int weight = Math.floorDiv(decimalWeight(arg), 4);
        int sweight = (weight + 1) * 2 - 1;
        int rscale = clampScale(MIN_SIG_DIGITS - sweight, arg.scale());
        if (arg.signum() == 0) return BigDecimal.ZERO.setScale(rscale);
        return sqrtTo(arg, rscale + GUARD).setScale(rscale, RoundingMode.HALF_UP);
    }

    /** Newton's method for a square root — {@code BigDecimal.sqrt} arrived after Java 8. */
    private static BigDecimal sqrtTo(BigDecimal x, int prec) {
        MathContext mc = new MathContext(prec + GUARD, RoundingMode.HALF_EVEN);
        int w = decimalWeight(x);
        // Start from a double estimate of the mantissa, scaled back by half the weight.
        int half = Math.floorDiv(w, 2);
        double mantissa = x.movePointLeft(2 * half).doubleValue();
        BigDecimal guess = new BigDecimal(Math.sqrt(mantissa)).movePointRight(half);
        BigDecimal two = BigDecimal.valueOf(2);
        for (int i = 0; i < 200; i++) {
            BigDecimal next = guess.add(x.divide(guess, mc), mc).divide(two, mc);
            if (next.compareTo(guess) == 0) break;
            guess = next;
        }
        return guess;
    }

    /** {@code exp(numeric)}, with PG's result scale and PG's overflow bound. */
    static BigDecimal exp(BigDecimal arg) {
        double val = arg.doubleValue() * LOG10_E;
        val = Math.max(val, -MAX_RESULT_SCALE);
        val = Math.min(val, MAX_RESULT_SCALE);
        int rscale = clampScale(MIN_SIG_DIGITS - (int) val, arg.scale());
        // PG refuses past exp(6000) rather than build a number numeric could not hold, and
        // underflows the other end to a zero carrying the same scale.
        double raw = arg.doubleValue();
        if (Math.abs(raw) >= 6000.0) {
            if (raw > 0) throw NumericLimits.valueOverflowsNumeric();
            return BigDecimal.ZERO.setScale(rscale);
        }
        int weight = (int) val;
        int prec = Math.max(rscale + weight, MIN_SIG_DIGITS);
        return expTo(arg, prec).setScale(rscale, RoundingMode.HALF_UP);
    }

    /** {@code ln(numeric)}, with PG's result scale. */
    static BigDecimal ln(BigDecimal arg) {
        checkLogArgument(arg);
        int lnDweight = estimateLnDweight(arg);
        int rscale = clampScale(MIN_SIG_DIGITS - lnDweight, arg.scale());
        int prec = Math.max(rscale + lnDweight + 1, MIN_SIG_DIGITS);
        return lnTo(arg, prec).setScale(rscale, RoundingMode.HALF_UP);
    }

    /** {@code log(base, num)} — and, with base ten, the one-argument {@code log}. */
    static BigDecimal log(BigDecimal base, BigDecimal num) {
        checkLogArgument(base);
        checkLogArgument(num);
        int lnBaseDweight = estimateLnDweight(base);
        int lnNumDweight = estimateLnDweight(num);
        int resultDweight = lnNumDweight - lnBaseDweight;
        int rscale = clampScale(MIN_SIG_DIGITS - resultDweight,
                Math.max(base.scale(), num.scale()));
        if (base.compareTo(BigDecimal.ONE) == 0) {
            throw new MemgresException("division by zero", "22012");
        }
        int prec = Math.max(rscale + Math.max(resultDweight, 0) + 1, MIN_SIG_DIGITS) + GUARD;
        BigDecimal lnNum = lnTo(num, prec);
        BigDecimal lnBase = lnTo(base, prec);
        MathContext mc = new MathContext(prec + GUARD, RoundingMode.HALF_EVEN);
        return lnNum.divide(lnBase, mc).setScale(rscale, RoundingMode.HALF_UP);
    }

    /** {@code power(numeric, numeric)}, with PG's result scale. */
    static BigDecimal power(BigDecimal base, BigDecimal exponent) {
        if (base.signum() == 0 && exponent.signum() < 0) {
            throw new MemgresException("zero raised to a negative power is undefined", "2201F");
        }
        boolean integerExponent = exponent.stripTrailingZeros().scale() <= 0;
        if (base.signum() < 0 && !integerExponent) {
            throw new MemgresException(
                    "a negative number raised to a non-integer power yields a complex result", "2201F");
        }
        if (integerExponent) {
            java.math.BigInteger n = exponent.toBigInteger();
            if (n.bitLength() < 31) {
                // The scale still aims for 16 significant digits, so 10^20 comes out as an
                // integer while 2^3 keeps sixteen decimals.
                double weight = base.signum() == 0
                        ? 0 : lnAsDouble(base) * LOG10_E * n.intValue();
                weight = Math.max(-MAX_RESULT_SCALE, Math.min(MAX_RESULT_SCALE, weight));
                int rscale = clampScale(MIN_SIG_DIGITS - (int) weight,
                        Math.max(base.scale(), exponent.scale()));
                return integerPower(base, n.intValue(), rscale);
            }
        }
        if (base.signum() == 0) return BigDecimal.ZERO.setScale(clampScale(MIN_SIG_DIGITS, 0));
        double lnNum = lnAsDouble(base) * exponent.doubleValue();
        if (Math.abs(lnNum) > MAX_RESULT_SCALE * 3.01) {
            if (lnNum > 0) throw NumericLimits.valueOverflowsNumeric();
            return BigDecimal.ZERO.setScale(clampScale(MIN_SIG_DIGITS, 0));
        }
        double val = Math.max(-MAX_RESULT_SCALE, Math.min(MAX_RESULT_SCALE, lnNum * LOG10_E));
        int rscale = clampScale(MIN_SIG_DIGITS - (int) val,
                Math.max(base.scale(), exponent.scale()));
        int prec = Math.max(rscale + (int) val, MIN_SIG_DIGITS) + GUARD;
        MathContext mc = new MathContext(prec + GUARD, RoundingMode.HALF_EVEN);
        BigDecimal product = lnTo(base, prec).multiply(exponent, mc);
        return expTo(product, prec).setScale(rscale, RoundingMode.HALF_UP);
    }

    private static BigDecimal integerPower(BigDecimal base, int n, int rscale) {
        if (n == 0) return BigDecimal.ONE.setScale(rscale);
        // An exact power of a wide base can be far wider than numeric could ever hold, so the
        // size is checked before it is built rather than after the heap has grown to fit it.
        // What decides that is how many figures stand before the point, which is the base's own
        // logarithm times the exponent: counting the base's digits instead made 10^100000 twice
        // as wide as it is and refused a power PostgreSQL answers.
        double digits = n <= 0 ? 0
                : (double) n * lnAsDouble(base.abs()) * LOG10_E + 1.0;
        if (digits > 131072) throw NumericLimits.valueOverflowsNumeric();
        if (n > 0) {
            BigDecimal exact = base.pow(n);
            if (exact.scale() > rscale) return exact.setScale(rscale, RoundingMode.HALF_UP);
            return exact.setScale(rscale);
        }
        BigDecimal denominator = base.pow(-n);
        return BigDecimal.ONE.divide(denominator, rscale, RoundingMode.HALF_UP);
    }

    private static void checkLogArgument(BigDecimal v) {
        if (v.signum() == 0) throw new MemgresException("cannot take logarithm of zero", "2201E");
        if (v.signum() < 0) {
            throw new MemgresException("cannot take logarithm of a negative number", "2201E");
        }
    }
    /**
     * How many decimal places a division answers with.
     *
     * <p>PostgreSQL chooses the scale from the operands rather than fixing one: enough places for
     * sixteen significant digits of the quotient, and never fewer than either operand already
     * had. Fixing a scale of twenty instead made 10.00 / 4 answer with twenty places where
     * PostgreSQL gives sixteen, and 1e-10 / 3 with twenty where PostgreSQL gives twenty-eight.
     *
     * <p>The weights are counted the way PostgreSQL stores a numeric, in groups of four decimal
     * digits, because that is what its own estimate of the quotient's weight is made of.
     */
    static int divisionScale(java.math.BigDecimal dividend, java.math.BigDecimal divisor) {
        int[] left = weightAndLeadingGroup(dividend);
        int[] right = weightAndLeadingGroup(divisor);
        int quotientWeight = left[0] - right[0];
        // Where the leading groups are equal the quotient may still be smaller, so PostgreSQL
        // assumes it is and keeps one group more.
        if (left[1] <= right[1]) quotientWeight--;
        int scale = 16 - quotientWeight * 4;
        scale = Math.max(scale, Math.max(dividend.scale(), 0));
        scale = Math.max(scale, Math.max(divisor.scale(), 0));
        return Math.max(0, Math.min(scale, 1000));
    }

    /** A value's weight in groups of four decimal digits, and the leading group itself. */
    private static int[] weightAndLeadingGroup(java.math.BigDecimal value) {
        if (value == null || value.signum() == 0) return new int[]{0, 0};
        java.math.BigDecimal magnitude = value.abs().stripTrailingZeros();
        int mostSignificant = magnitude.precision() - magnitude.scale() - 1;
        int weight = Math.floorDiv(mostSignificant, 4);
        java.math.BigDecimal leading = magnitude.movePointLeft(4 * weight);
        return new int[]{weight, leading.setScale(0, java.math.RoundingMode.FLOOR).intValue()};
    }

}
