package com.memgres.engine;

import java.math.BigDecimal;

/**
 * The bounds of PostgreSQL's {@code numeric} storage format.
 *
 * <p>{@code numeric} is stored as base-10000 digit groups with a signed weight and a display
 * scale, which caps it at 131072 digits before the decimal point and 16383 after it. memgres is
 * backed by {@link BigDecimal}, which has no such ceiling, so a value PostgreSQL refuses outright
 * was instead being built — a cast of {@code 1e200000} produced a 200001-digit number, and
 * {@code factorial(50000)} a 213237-digit one, each of which grows the heap rather than erroring.
 */
final class NumericLimits {

    /** Digits allowed before the decimal point (PG's NUMERIC_WEIGHT_MAX, in base-10 digits). */
    private static final int MAX_INTEGRAL_DIGITS = 131072;

    /** Digits allowed after the decimal point (PG's NUMERIC_DSCALE_MAX). */
    private static final int MAX_SCALE = 16383;

    private NumericLimits() {}

    static MemgresException valueOverflowsNumeric() {
        return new MemgresException("value overflows numeric format", "22003");
    }

    /**
     * Returns {@code value} unchanged when {@code numeric} could hold it, otherwise raises the
     * overflow PostgreSQL raises. Cheap for ordinary values: the scale is a field, and
     * {@link BigDecimal#precision()} is only consulted once the scale alone is inconclusive.
     */
    static BigDecimal check(BigDecimal value) {
        if (value == null) return null;
        int scale = value.scale();
        if (scale > MAX_SCALE) throw valueOverflowsNumeric();
        // Integral digits = precision - scale; a scale that is merely small cannot overflow,
        // so only reach for precision() when the exponent is already out on its own.
        if (-scale > MAX_INTEGRAL_DIGITS
                || (long) value.precision() - scale > MAX_INTEGRAL_DIGITS) {
            throw valueOverflowsNumeric();
        }
        return value;
    }

    // ---- numeric NaN and the infinities ----
    //
    // PostgreSQL's numeric carries NaN and, since 14, both infinities. BigDecimal has no form
    // for any of them, so memgres carries them as the matching Double: it prints, orders and
    // computes exactly the way PG's numeric does for these values.

    /** True when the value is a numeric NaN or infinity, whichever float width holds it. */
    static boolean isSpecial(Object val) {
        if (val instanceof Double) {
            Double d = (Double) val;
            return d.isNaN() || d.isInfinite();
        }
        if (val instanceof Float) {
            Float f = (Float) val;
            return f.isNaN() || f.isInfinite();
        }
        return false;
    }

    /**
     * The special numeric a value denotes, or null when it is an ordinary number. Recognises
     * the spellings PG's numeric input accepts, which are case-insensitive and allow a sign.
     */
    static Double specialNumericOrNull(Object val) {
        if (val instanceof Double || val instanceof Float) {
            return isSpecial(val) ? Double.valueOf(((Number) val).doubleValue()) : null;
        }
        if (!(val instanceof String)) return null;
        String s = ((String) val).trim().toLowerCase(java.util.Locale.ROOT);
        if (s.startsWith("+")) s = s.substring(1);
        if (s.equals("nan")) return Double.valueOf(Double.NaN);
        if (s.equals("infinity") || s.equals("inf")) return Double.valueOf(Double.POSITIVE_INFINITY);
        if (s.equals("-infinity") || s.equals("-inf")) return Double.valueOf(Double.NEGATIVE_INFINITY);
        return null;
    }

    // ---- two's-complement boundaries ----

    /** The out-of-range error PG raises, tagged with the type so ERROR fields carry it too. */
    static MemgresException integerOutOfRange(String typeName) {
        MemgresException e = new MemgresException(typeName + " out of range", "22003");
        e.setDatatype(typeName);
        return e;
    }

    /**
     * Absolute value, in the argument's own type, or null when the argument is not one of the
     * numeric types. The minimum of a two's-complement integer has no positive counterpart, so
     * PG raises there rather than handing back the minimum again.
     */
    static Object absExact(Object val) {
        if (val instanceof Short) {
            short s = (Short) val;
            if (s == Short.MIN_VALUE) throw integerOutOfRange("smallint");
            return (short) Math.abs(s);
        }
        if (val instanceof Integer) {
            int i = (Integer) val;
            if (i == Integer.MIN_VALUE) throw integerOutOfRange("integer");
            return Math.abs(i);
        }
        if (val instanceof Long) {
            long l = (Long) val;
            if (l == Long.MIN_VALUE) throw integerOutOfRange("bigint");
            return Math.abs(l);
        }
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).abs();
        if (val instanceof Double) return Math.abs((Double) val);
        if (val instanceof Float) return Math.abs((Float) val);
        return null; // caller keeps its own handling for the non-numeric types
    }

    /** Negation in the argument's own type, raising at the minimum for the same reason. */
    static Object negateExact(Object val) {
        if (val instanceof Short) {
            short s = (Short) val;
            if (s == Short.MIN_VALUE) throw integerOutOfRange("smallint");
            return (short) -s;
        }
        if (val instanceof Integer) {
            int i = (Integer) val;
            if (i == Integer.MIN_VALUE) throw integerOutOfRange("integer");
            return -i;
        }
        if (val instanceof Long) {
            long l = (Long) val;
            if (l == Long.MIN_VALUE) throw integerOutOfRange("bigint");
            return -l;
        }
        return null; // caller keeps its own handling for the non-integer types
    }

    /**
     * The integer type gcd/lcm return for these arguments — the wider of the two. Anything that
     * is not a narrower integer resolves to bigint, which is the widest overload PG offers.
     */
    static String widestIntegerType(Object a, Object b) {
        if (a instanceof Short && b instanceof Short) return "smallint";
        boolean narrowA = a instanceof Short || a instanceof Integer;
        boolean narrowB = b instanceof Short || b instanceof Integer;
        return narrowA && narrowB ? "integer" : "bigint";
    }

    /** |v| as a long, reporting the argument type's own range when the minimum has no |v|. */
    static long absExactLong(long v, Object a, Object b) {
        if (v == Long.MIN_VALUE) throw integerOutOfRange(widestIntegerType(a, b));
        return Math.abs(v);
    }

    /** Reject a gcd/lcm result that does not fit the integer type the arguments imply. */
    static Object narrowToIntegerType(long v, Object a, Object b) {
        String type = widestIntegerType(a, b);
        if ("smallint".equals(type)) {
            if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) throw integerOutOfRange(type);
            return (short) v;
        }
        if ("integer".equals(type)) {
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) throw integerOutOfRange(type);
            return (int) v;
        }
        return v;
    }

    // ---- mathematical domain errors ----

    /** PG raises rather than returning the IEEE special a complex or undefined power would give. */
    static void checkPowerDomain(double base, double exponent) {
        if (base == 0 && exponent < 0) {
            throw new MemgresException("zero raised to a negative power is undefined", "2201F");
        }
        if (base < 0 && !Double.isInfinite(exponent) && !Double.isNaN(exponent)
                && exponent != Math.floor(exponent)) {
            throw new MemgresException(
                    "a negative number raised to a non-integer power yields a complex result", "2201F");
        }
    }

    /** Logarithms are defined only on the positive reals; PG raises on the rest. */
    static void checkLogDomain(double value) {
        if (value == 0) throw new MemgresException("cannot take logarithm of zero", "2201E");
        if (value < 0) throw new MemgresException("cannot take logarithm of a negative number", "2201E");
    }

    /** The domain error PG raises for an argument outside a function's definition. */
    static MemgresException inputOutOfRange() {
        return new MemgresException("input is out of range", "22003");
    }

    /** asin/acos are defined on [-1, 1]; PG raises outside it but passes NaN straight through. */
    static void checkUnitInterval(double value) {
        if (Double.isNaN(value)) return;
        if (value < -1 || value > 1) throw inputOutOfRange();
    }

    /**
     * The result of a float8 {@code exp}. PG reports both ends rather than handing back the
     * infinity or the zero the hardware produced: {@code exp(1000)} overflows and
     * {@code exp(-1000)} underflows, while {@code exp(-Infinity)} is a legitimate zero.
     */
    static double checkExp(double result, double argument) {
        if (Double.isInfinite(result) && !Double.isInfinite(argument)) throw floatOverflow();
        if (result == 0 && argument != Double.NEGATIVE_INFINITY) throw floatUnderflow();
        return result;
    }

    /**
     * Narrow a float8 computation the way PG's float8 operators do: an infinity that neither
     * operand brought with it is an overflow, and a zero that neither operand explains an
     * underflow.
     */
    static double checkFloat8(double result, double left, double right) {
        if (Double.isInfinite(result) && !Double.isInfinite(left) && !Double.isInfinite(right)) {
            throw floatOverflow();
        }
        if (result == 0 && left != 0 && !Double.isInfinite(left) && !Double.isInfinite(right)) {
            throw floatUnderflow();
        }
        return result;
    }

    // ---- float overflow and underflow ----

    static MemgresException floatOverflow() {
        return new MemgresException("value out of range: overflow", "22003");
    }

    static MemgresException floatUnderflow() {
        return new MemgresException("value out of range: underflow", "22003");
    }

    /**
     * Narrow a float4 computation back to float4, the way PG's float4 operators do. Computing
     * in double and keeping the wide result lets a {@code real} column hold a value
     * {@code real} cannot represent, which is how an overflow turns into a plausible number.
     */
    static float checkFloat4(double result, double left, double right) {
        float f = (float) result;
        if (Float.isInfinite(f) && !Double.isInfinite(left) && !Double.isInfinite(right)) {
            throw floatOverflow();
        }
        // A non-zero result that vanishes on the way down has underflowed; an exactly zero
        // one (1.0 - 1.0, say) has not.
        if (f == 0 && result != 0) throw floatUnderflow();
        return f;
    }

    /**
     * Narrow a float4 aggregate's accumulator back to float4. A running total may legitimately
     * reach zero, so only the overflow half of the check applies here.
     */
    static float checkFloat4Total(double result) {
        float f = (float) result;
        if (Float.isInfinite(f) && !Double.isInfinite(result)) throw floatOverflow();
        return f;
    }

    /**
     * Narrow a float8 aggregate's accumulator, which PG keeps in float8: a total that leaves
     * float8's range is reported rather than stored as an infinity nobody put there.
     */
    static double checkFloat8Total(double result) {
        if (Double.isInfinite(result)) throw floatOverflow();
        return result;
    }

    /**
     * How PostgreSQL writes the offending value in an out-of-range message: a numeric prints in
     * plain decimal, so {@code 1e39} is reported as its forty digits rather than as {@code 1E+39}.
     */
    static String plainText(Object value) {
        if (value instanceof BigDecimal) return ((BigDecimal) value).toPlainString();
        return String.valueOf(value);
    }

    /** The message PG gives when an input value has no representation in the target float type. */
    static MemgresException outOfRangeForType(Object value, String typeName) {
        MemgresException e = new MemgresException(
                "\"" + plainText(value) + "\" is out of range for type " + typeName, "22003");
        e.setDatatype(typeName);
        return e;
    }

    /**
     * True when a non-zero input collapsed to zero converting into a float type. PG reports that
     * as out of range rather than storing a zero the caller never wrote.
     */
    static boolean underflowedToZero(Object value, double converted) {
        if (converted != 0) return false;
        if (value instanceof BigDecimal) return ((BigDecimal) value).signum() != 0;
        if (value instanceof Number) return false;   // a Number that reads as 0 really is 0
        try {
            return new BigDecimal(String.valueOf(value).trim()).signum() != 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
