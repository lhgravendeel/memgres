package com.memgres.engine;

/**
 * The field qualifier and fractional-seconds precision carried by a written interval type:
 * {@code INTERVAL DAY TO SECOND(2)}, {@code interval(3)}, {@code CAST(x AS interval hour)}.
 *
 * <p>PostgreSQL packs both into the type modifier, and both change the value. The qualifier
 * decides which unit an unlabelled number in the literal means -- {@code INTERVAL '3' DAY} is
 * three days where a bare {@code INTERVAL '3'} is three seconds -- and which fields survive
 * afterwards: everything less significant than the qualifier's last field is dropped. The
 * precision decides how many fractional digits of the seconds field are kept, rounding the rest
 * away from zero.
 */
public final class IntervalTypmod {

    public static final int YEAR = 1 << 0;
    public static final int MONTH = 1 << 1;
    public static final int DAY = 1 << 2;
    public static final int HOUR = 1 << 3;
    public static final int MINUTE = 1 << 4;
    public static final int SECOND = 1 << 5;

    /** No field restriction: every field the literal names survives. */
    public static final int FULL_RANGE = 0;
    /** No fractional-seconds restriction: all six digits survive. */
    public static final int FULL_PRECISION = -1;

    /** PG stores microseconds, so six digits is the most a precision can ask for. */
    public static final int MAX_PRECISION = 6;

    private static final long US_PER_HOUR = 3_600_000_000L;
    private static final long US_PER_MINUTE = 60_000_000L;

    /** A plain {@code interval}: no qualifier, no precision. */
    public static final IntervalTypmod PLAIN = new IntervalTypmod(FULL_RANGE, FULL_PRECISION);

    private final int range;
    private final int precision;

    private IntervalTypmod(int range, int precision) {
        this.range = range;
        this.precision = precision;
    }

    public int range() { return range; }

    public int precision() { return precision; }

    /** True when this modifier neither restricts fields nor rounds the seconds. */
    public boolean isPlain() {
        return range == FULL_RANGE && precision == FULL_PRECISION;
    }

    /**
     * Reads the qualifier out of a written type name -- "interval", "interval hour",
     * "interval day to second(2)", "interval(3)".
     *
     * @return the modifier, or null when the name is not an interval type at all (including
     *         {@code interval[]}, which is an array of them rather than one)
     */
    public static IntervalTypmod fromTypeSpec(String spec) {
        if (spec == null) return null;
        String s = spec.toLowerCase(java.util.Locale.ROOT).trim();
        if (!s.startsWith("interval")) return null;
        String rest = s.substring("interval".length()).trim();
        if (rest.isEmpty()) return PLAIN;

        int precision = FULL_PRECISION;
        int open = rest.indexOf('(');
        if (open >= 0) {
            int close = rest.indexOf(')', open);
            if (close < 0) return null;
            String digits = rest.substring(open + 1, close).trim();
            try {
                precision = Integer.parseInt(digits);
            } catch (NumberFormatException e) {
                return null;
            }
            // PG warns and clamps rather than refusing a precision it cannot store
            if (precision < 0) precision = FULL_PRECISION;
            else if (precision > MAX_PRECISION) precision = MAX_PRECISION;
            rest = (rest.substring(0, open) + rest.substring(close + 1)).trim();
        }

        int range = FULL_RANGE;
        if (!rest.isEmpty()) {
            String[] parts = rest.split("\\s+");
            if (parts.length == 1) {
                range = fieldBit(parts[0]);
            } else if (parts.length == 3 && "to".equals(parts[1])) {
                range = spanBits(fieldBit(parts[0]), fieldBit(parts[2]));
            } else {
                return null;
            }
            if (range == FULL_RANGE) return null;
        }
        return new IntervalTypmod(range, precision);
    }

    /** The bit for one field word, or 0 when the word does not name an interval field. */
    private static int fieldBit(String name) {
        if ("year".equals(name)) return YEAR;
        if ("month".equals(name)) return MONTH;
        if ("day".equals(name)) return DAY;
        if ("hour".equals(name)) return HOUR;
        if ("minute".equals(name)) return MINUTE;
        if ("second".equals(name)) return SECOND;
        return 0;
    }

    /** Every field from {@code from} down to {@code to} inclusive; 0 when the span is invalid. */
    private static int spanBits(int from, int to) {
        if (from == 0 || to == 0) return 0;
        int fromIdx = bitIndex(from);
        int toIdx = bitIndex(to);
        if (fromIdx >= toIdx) return 0;
        int mask = 0;
        for (int i = fromIdx; i <= toIdx; i++) mask |= (1 << i);
        return mask;
    }

    private static int bitIndex(int bit) {
        for (int i = 0; i < 6; i++) if (bit == (1 << i)) return i;
        return -1;
    }

    /**
     * The unit an unlabelled number in the literal takes: the least significant field of the
     * qualifier, so {@code INTERVAL '5' DAY TO HOUR} is five hours and not five days.
     */
    public int defaultUnit() {
        for (int i = 5; i >= 0; i--) {
            if ((range & (1 << i)) != 0) return 1 << i;
        }
        return SECOND;
    }

    /** The canonical unit word for one field bit, as {@link PgInterval} names its units. */
    public static String unitName(int bit) {
        if (bit == YEAR) return "year";
        if (bit == MONTH) return "month";
        if (bit == DAY) return "day";
        if (bit == HOUR) return "hour";
        if (bit == MINUTE) return "minute";
        return "second";
    }

    /**
     * Drops the fields the qualifier does not reach and rounds the seconds to the precision.
     * An infinite interval has no fields to drop, so it passes through untouched.
     */
    public PgInterval apply(PgInterval value) {
        if (value == null || value.isInfinite() || isPlain()) return value;
        int months = value.getMonths();
        int days = value.getDays();
        long micros = value.getMicroseconds();

        if (range != FULL_RANGE) {
            int last = defaultUnit();
            if (last == YEAR || last == MONTH) {
                days = 0;
                micros = 0;
                // A qualifier that stops at YEAR cannot hold the leftover months
                if (last == YEAR) months = (months / 12) * 12;
            } else if (last == DAY) {
                micros = 0;
            } else if (last == HOUR) {
                micros = (micros / US_PER_HOUR) * US_PER_HOUR;
            } else if (last == MINUTE) {
                micros = (micros / US_PER_MINUTE) * US_PER_MINUTE;
            }
        }

        if (precision != FULL_PRECISION && precision < MAX_PRECISION) {
            long scale = 1;
            for (int i = precision; i < MAX_PRECISION; i++) scale *= 10;
            long half = scale / 2;
            // PG rounds the dropped digits away from zero, so -1.5 second(0) is -2 seconds
            micros = micros >= 0
                    ? ((micros + half) / scale) * scale
                    : -(((-micros + half) / scale) * scale);
        }

        return new PgInterval(months, days, micros);
    }
}
