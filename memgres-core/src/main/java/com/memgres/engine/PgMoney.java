package com.memgres.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Wrapper for PostgreSQL money type values.
 * Stores the underlying value as a BigDecimal with scale 2,
 * and formats with '$' prefix when converted to text (matching PG behavior).
 */
public class PgMoney implements Comparable<PgMoney> {

    private final BigDecimal value;

    public PgMoney(BigDecimal value) {
        this.value = value.setScale(2, RoundingMode.HALF_UP);
    }

    public BigDecimal getValue() {
        return value;
    }

    /** Returns the money-formatted string with '$' prefix, e.g. "$12.34" or "-$12.34". */
    @Override
    public String toString() {
        if (value.signum() < 0) {
            return "-$" + value.negate().toPlainString();
        }
        return "$" + value.toPlainString();
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
