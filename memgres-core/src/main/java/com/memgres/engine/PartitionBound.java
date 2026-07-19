package com.memgres.engine;

/**
 * Sentinel values for open-ended RANGE partition bounds (MINVALUE / MAXVALUE).
 *
 * <p>PostgreSQL treats MINVALUE as smaller than every value of the partition key type and
 * MAXVALUE as larger than every value, regardless of the key's data type. Representing the
 * sentinels as dedicated singletons (instead of Long.MIN_VALUE / Long.MAX_VALUE) lets the
 * routing comparator implement that "always below / always above" behavior for non-numeric
 * keys (text, date, ...) as well.</p>
 */
public final class PartitionBound {

    /** Smaller than every partition key value. */
    public static final PartitionBound MINVALUE = new PartitionBound("MINVALUE");

    /** Larger than every partition key value. */
    public static final PartitionBound MAXVALUE = new PartitionBound("MAXVALUE");

    private final String label;

    private PartitionBound(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }
}
