package com.memgres.engine;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

/**
 * The rows of a series, worked out when they are read rather than built in advance.
 *
 * <p>A series is not stored anywhere: every row of it follows from the first one and the step, so
 * the <em>i</em>th row can be worked out on its own and the number of rows can be counted without
 * producing any of them. Building the whole list first meant a five-million-row series cost five
 * million boxed values and five million row arrays before the query had looked at one of them —
 * {@code LIMIT 1} paid for all five million — and a series past the size memgres was willing to
 * hold had to be refused outright, where PostgreSQL simply answers it.
 *
 * <p>Read as a list, so nothing downstream has to know: a query that walks the rows walks them one
 * at a time and holds none, and {@code count(*)} is the size, which is arithmetic.
 *
 * <p>Only the forms whose rows really are a function of their position are built this way. A step
 * measured in months is not one of them — a month added twice is not two months added once, since
 * the length of a month depends on where you start — so a series stepping by months is still
 * produced a row at a time.
 */
abstract class SeriesRows extends AbstractList<Object[]> implements RandomAccess {

    /** Poll for cancellation once per 1024 rows read, as generating them used to. */
    private static final int CANCEL_POLL_MASK = 1023;

    private final int size;

    private SeriesRows(int size) {
        this.size = size;
    }

    @Override
    public final int size() {
        return size;
    }

    @Override
    public final Object[] get(int index) {
        if (index < 0 || index >= size) throw new IndexOutOfBoundsException("row " + index);
        if ((index & CANCEL_POLL_MASK) == 0) StatementCancel.check();
        return new Object[]{valueAt(index)};
    }

    /** The value the series holds at this position. */
    abstract Object valueAt(int index);

    /**
     * How many rows a series holds, or -1 when it holds more than a list can address.
     *
     * <p>A count is what decides whether a series can be answered at all: past {@link
     * Integer#MAX_VALUE} rows there is no list that can carry them, and the request is refused
     * rather than answered short.
     */
    private static int countOf(BigInteger span, BigInteger step) {
        if (span.signum() < 0) return 0;
        BigInteger count = span.divide(step.abs()).add(BigInteger.ONE);
        if (count.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) return -1;
        return count.intValue();
    }

    private static MemgresException tooLarge() {
        return new MemgresException("generate_series would produce more rows than can be held",
                "54000");
    }

    // ---- integer ----

    /**
     * {@code generate_series(int, int[, int])}. The value is int4 while the whole series is, and
     * int8 as soon as any argument was, which is the overload PostgreSQL would have chosen.
     */
    static List<Object[]> ofIntegers(final long start, long stop, final long step, final boolean asLong) {
        if (step == 0) return new IntegerSeries(0, 0, 0, asLong);   // the caller refuses this first
        BigInteger span = BigInteger.valueOf(stop).subtract(BigInteger.valueOf(start));
        if (step < 0) span = span.negate();
        int count = countOf(span, BigInteger.valueOf(step));
        if (count < 0) throw tooLarge();
        return new IntegerSeries(start, count, step, asLong);
    }

    private static final class IntegerSeries extends SeriesRows {
        private final long start;
        private final long step;
        private final boolean asLong;

        IntegerSeries(long start, int size, long step, boolean asLong) {
            super(size);
            this.start = start;
            this.step = step;
            this.asLong = asLong;
        }

        @Override
        Object valueAt(int index) {
            long value = start + step * (long) index;
            if (asLong) return Long.valueOf(value);
            return (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
                    ? (Object) Integer.valueOf((int) value) : (Object) Long.valueOf(value);
        }
    }

    // ---- numeric ----

    /**
     * {@code generate_series(numeric, numeric[, numeric])}. Each value is the start plus so many
     * steps, which carries the same scale as adding the step that many times.
     */
    static List<Object[]> ofNumerics(BigDecimal start, BigDecimal stop, BigDecimal step) {
        BigDecimal span = stop.subtract(start);
        if (step.signum() < 0) span = span.negate();
        if (span.signum() < 0) return new NumericSeries(start, 0, step);
        BigDecimal steps = span.divide(step.abs(), 0, RoundingMode.FLOOR);
        BigInteger count = steps.toBigInteger().add(BigInteger.ONE);
        if (count.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) throw tooLarge();
        return new NumericSeries(start, count.intValue(), step);
    }

    private static final class NumericSeries extends SeriesRows {
        private final BigDecimal start;
        private final BigDecimal step;

        NumericSeries(BigDecimal start, int size, BigDecimal step) {
            super(size);
            this.start = start;
            this.step = step;
        }

        @Override
        Object valueAt(int index) {
            if (index == 0) return start;
            return start.add(step.multiply(BigDecimal.valueOf(index)));
        }
    }

    // ---- timestamp and timestamptz ----

    /**
     * The microseconds one step covers, or 0 when the step is not a fixed length of time. Days are
     * a fixed length here for the same reason they are in the addition this replaces: a timestamp
     * carries no zone, and a timestamptz is stepped at its own offset.
     */
    static long fixedStepMicros(PgInterval step) {
        if (step == null || step.getMonths() != 0 || step.isInfinite()) return 0;
        try {
            return Math.addExact(Math.multiplyExact((long) step.getDays(), 86_400_000_000L),
                    step.getMicroseconds());
        } catch (ArithmeticException e) {
            return 0;
        }
    }

    /**
     * {@code generate_series(timestamp, timestamp, interval)} with a step of fixed length. A
     * series written over dates answers with instants, as PostgreSQL's does, so it is the same
     * arithmetic read out at UTC.
     */
    static List<Object[]> ofTimestamps(LocalDateTime start, LocalDateTime stop, long stepMicros,
                                       boolean asInstants) {
        int count = countOf(microsBetween(start, stop, stepMicros), BigInteger.valueOf(stepMicros));
        if (count < 0) throw tooLarge();
        return new TimestampSeries(start, count, stepMicros, asInstants);
    }

    /**
     * The span a series covers, counted the way it is stepped. The timestamp type reaches further
     * than a count of microseconds does, so a span that cannot be counted at all is a span no
     * series over it could be held for either.
     */
    private static BigInteger microsBetween(java.time.temporal.Temporal from,
                                            java.time.temporal.Temporal to, long stepMicros) {
        long span;
        try {
            span = ChronoUnit.MICROS.between(from, to);
        } catch (ArithmeticException e) {
            throw tooLarge();
        }
        return BigInteger.valueOf(stepMicros < 0 ? -span : span);
    }

    private static final class TimestampSeries extends SeriesRows {
        private final LocalDateTime start;
        private final long stepMicros;
        private final boolean asInstants;

        TimestampSeries(LocalDateTime start, int size, long stepMicros, boolean asInstants) {
            super(size);
            this.start = start;
            this.stepMicros = stepMicros;
            this.asInstants = asInstants;
        }

        @Override
        Object valueAt(int index) {
            LocalDateTime value = TypeCoercion.requireTimestampInRange(
                    advance(start, stepMicros, index));
            return asInstants
                    ? (Object) value.atZone(java.time.ZoneOffset.UTC).toOffsetDateTime()
                    : (Object) value;
        }
    }

    /** {@code generate_series(timestamptz, timestamptz, interval)} with a step of fixed length. */
    static List<Object[]> ofTimestampTzs(OffsetDateTime start, OffsetDateTime stop, long stepMicros) {
        int count = countOf(microsBetween(start, stop, stepMicros), BigInteger.valueOf(stepMicros));
        if (count < 0) throw tooLarge();
        return new TimestampTzSeries(start, count, stepMicros);
    }

    private static final class TimestampTzSeries extends SeriesRows {
        private final OffsetDateTime start;
        private final long stepMicros;

        TimestampTzSeries(OffsetDateTime start, int size, long stepMicros) {
            super(size);
            this.start = start;
            this.stepMicros = stepMicros;
        }

        @Override
        Object valueAt(int index) {
            OffsetDateTime value = advance(start.toLocalDateTime(), stepMicros, index)
                    .atOffset(start.getOffset());
            TypeCoercion.requireTimestampInRange(
                    value.withOffsetSameInstant(java.time.ZoneOffset.UTC).toLocalDateTime());
            return value;
        }
    }

    /**
     * {@code start} moved on by {@code index} steps, split into whole days and the rest so that a
     * long series does not run a nanosecond count past what a long holds.
     */
    private static LocalDateTime advance(LocalDateTime start, long stepMicros, int index) {
        if (index == 0) return start;
        try {
            long micros = Math.multiplyExact(stepMicros, (long) index);
            long days = micros / 86_400_000_000L;
            long rest = micros % 86_400_000_000L;
            return start.plusDays(days).plusNanos(Math.multiplyExact(rest, 1000L));
        } catch (MemgresException e) {
            throw e;
        } catch (RuntimeException e) {
            // java.time runs out of year long before the type does; either way there is no answer.
            throw new MemgresException("timestamp out of range", "22008");
        }
    }

    /**
     * The rows a series produces, read as the row contexts a FROM item hands on. Each is built
     * when it is asked for, so a query that looks at one row of a long series builds one.
     */
    static List<RowContext> contextsOver(final Table table, final String alias,
                                         final List<Object[]> rows) {
        return new AbstractList<RowContext>() {
            @Override
            public RowContext get(int index) {
                return new RowContext(table, alias, rows.get(index));
            }

            @Override
            public int size() {
                return rows.size();
            }
        };
    }
}
