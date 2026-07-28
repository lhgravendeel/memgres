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
}
