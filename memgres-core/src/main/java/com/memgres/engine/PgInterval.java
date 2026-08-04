package com.memgres.engine;

import com.memgres.engine.util.Strs;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgreSQL INTERVAL type. Stores months, days, and microseconds separately
 * (matching PG's internal representation).
 */
public class PgInterval implements Comparable<PgInterval> {

    private final int months;
    private final int days;
    private final long microseconds;

    /**
     * PG stores an infinite interval as a reserved field pattern rather than a magnitude, so
     * the sentinels below are the whole value: days and microseconds carry no meaning for them.
     */
    public static final PgInterval INFINITY = new PgInterval(Integer.MAX_VALUE, 0, 0);
    public static final PgInterval NEG_INFINITY = new PgInterval(Integer.MIN_VALUE, 0, 0);

    public PgInterval(int months, int days, long microseconds) {
        this.months = months;
        this.days = days;
        this.microseconds = microseconds;
    }

    /** True for an interval of exactly zero length in every field. */
    public boolean isZero() { return months == 0 && days == 0 && microseconds == 0; }

    /** True when the interval steps backwards; the most significant non-zero field decides. */
    public boolean isNegative() {
        if (months != 0) return months < 0;
        if (days != 0) return days < 0;
        return microseconds < 0;
    }

    public int getMonths() { return months; }
    public int getDays() { return days; }
    public long getMicroseconds() { return microseconds; }

    public boolean isInfinite() {
        return months == Integer.MAX_VALUE || months == Integer.MIN_VALUE;
    }

    public boolean isPositiveInfinity() { return months == Integer.MAX_VALUE; }

    public boolean isNegativeInfinity() { return months == Integer.MIN_VALUE; }

    /** PG rejects an operation whose result would be an indeterminate infinity. */
    private static MemgresException intervalOutOfRange() {
        return new MemgresException("interval out of range", "22008");
    }

    public PgInterval plus(PgInterval other) {
        if (isInfinite() || other.isInfinite()) {
            // infinity + -infinity has no value, so PG raises rather than picking one
            if (isInfinite() && other.isInfinite() && isPositiveInfinity() != other.isPositiveInfinity()) {
                throw intervalOutOfRange();
            }
            return isInfinite() ? this : other;
        }
        try {
            return checked(Math.addExact((long) months, other.months),
                    Math.addExact((long) days, other.days),
                    Math.addExact(microseconds, other.microseconds));
        } catch (ArithmeticException e) {
            throw intervalOutOfRange();
        }
    }

    public PgInterval minus(PgInterval other) {
        if (isInfinite() || other.isInfinite()) {
            if (isInfinite() && other.isInfinite() && isPositiveInfinity() == other.isPositiveInfinity()) {
                throw intervalOutOfRange();
            }
            return isInfinite() ? this : other.negate();
        }
        try {
            return checked(Math.subtractExact((long) months, other.months),
                    Math.subtractExact((long) days, other.days),
                    Math.subtractExact(microseconds, other.microseconds));
        } catch (ArithmeticException e) {
            throw intervalOutOfRange();
        }
    }

    public PgInterval negate() {
        if (isPositiveInfinity()) return NEG_INFINITY;
        if (isNegativeInfinity()) return INFINITY;
        return new PgInterval(-months, -days, -microseconds);
    }

    public PgInterval multiply(double factor) {
        if (isInfinite()) {
            // infinity * 0 is indeterminate; any other factor keeps or flips the sign
            if (factor == 0) throw intervalOutOfRange();
            boolean positive = isPositiveInfinity() == (factor > 0);
            return positive ? INFINITY : NEG_INFINITY;
        }
        // PG cascades fractional parts: fractional months → days, fractional days → microseconds
        double totalMonths = months * factor;
        int newMonths = (int) totalMonths;
        double fracMonths = totalMonths - newMonths;
        // 1 month = 30 days in PG interval arithmetic
        double totalDays = days * factor + fracMonths * 30.0;
        int newDays = (int) totalDays;
        double fracDays = totalDays - newDays;
        // 1 day = 24 hours = 86400000000 microseconds
        double newMicros = microseconds * factor + fracDays * 86400_000_000L;
        if (!Double.isFinite(totalMonths) || !Double.isFinite(totalDays) || !Double.isFinite(newMicros)) {
            throw intervalOutOfRange();
        }
        return checked((long) totalMonths, (long) totalDays, Math.round(newMicros));
    }

    /**
     * Build an interval from widened fields, rejecting anything that no longer fits. The
     * infinity sentinels occupy the extreme month values, so a finite result may not reach
     * them either.
     */
    private static PgInterval checked(long months, long days, long micros) {
        if (months >= Integer.MAX_VALUE || months <= Integer.MIN_VALUE
                || days > Integer.MAX_VALUE || days < Integer.MIN_VALUE) {
            throw intervalOutOfRange();
        }
        return new PgInterval((int) months, (int) days, micros);
    }

    /**
     * Add this interval to a LocalDate.
     */
    public LocalDate addTo(LocalDate date) {
        if (isInfinite()) {
            return isPositiveInfinity()
                    ? TypeCoercion.DATE_INFINITY : TypeCoercion.DATE_NEG_INFINITY;
        }
        LocalDate result = date;
        if (months != 0) result = result.plusMonths(months);
        if (days != 0) result = result.plusDays(days);
        // Microseconds don't affect dates
        return result;
    }

    /**
     * Add this interval to a LocalDateTime.
     */
    public LocalDateTime addTo(LocalDateTime dateTime) {
        if (isInfinite()) {
            return isPositiveInfinity()
                    ? TypeCoercion.TIMESTAMP_INFINITY : TypeCoercion.TIMESTAMP_NEG_INFINITY;
        }
        LocalDateTime result = dateTime;
        if (months != 0) result = result.plusMonths(months);
        if (days != 0) result = result.plusDays(days);
        if (microseconds != 0) result = result.plusNanos(microseconds * 1000);
        return result;
    }

    /**
     * Add this interval to an OffsetDateTime.
     */
    public OffsetDateTime addTo(OffsetDateTime dateTime) {
        if (isInfinite()) {
            LocalDateTime bound = isPositiveInfinity()
                    ? TypeCoercion.TIMESTAMP_INFINITY : TypeCoercion.TIMESTAMP_NEG_INFINITY;
            return bound.atOffset(dateTime.getOffset());
        }
        OffsetDateTime result = dateTime;
        if (months != 0) result = result.plusMonths(months);
        if (days != 0) result = result.plusDays(days);
        if (microseconds != 0) result = result.plusNanos(microseconds * 1000);
        return result;
    }

    // ---- Parsing ----

    private static final Pattern POSTGRES_INTERVAL = Pattern.compile(
            "(?:(-?\\d+)\\s+years?)?\\s*" +
            "(?:(-?\\d+)\\s+mons?(?:ths?)?)?\\s*" +
            "(?:(-?\\d+)\\s+days?)?\\s*" +
            "(?:(-?)(\\d+):(\\d+):(\\d+)(?:\\.(\\d+))?)?",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern VERBOSE_INTERVAL = Pattern.compile(
            "(?:(-?\\d+)\\s+years?)?\\s*" +
            "(?:(-?\\d+)\\s+mon(?:th)?s?)?\\s*" +
            "(?:(-?\\d+)\\s+weeks?)?\\s*" +
            "(?:(-?\\d+)\\s+days?)?\\s*" +
            "(?:(-?\\d+)\\s+hours?)?\\s*" +
            "(?:(-?\\d+)\\s+minutes?)?\\s*" +
            "(?:(-?\\d+(?:\\.\\d+)?)\\s+seconds?)?",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Parse an interval literal written with a field qualifier, e.g. {@code INTERVAL '5' DAY TO
     * HOUR}. The qualifier decides what an unlabelled number in the literal counts, then decides
     * which of the parsed fields survive; a literal that names its own units is only trimmed.
     */
    public static PgInterval parse(String input, IntervalTypmod typmod) {
        if (typmod == null) return parse(input);
        if (typmod.range() != IntervalTypmod.FULL_RANGE) {
            PgInterval ranged;
            try {
                ranged = parseRanged(input, typmod);
            } catch (NumberFormatException | ArithmeticException | IsoFieldOverflow e) {
                throw PgErrors.intervalFieldOutOfRange(input == null ? "" : input.trim());
            }
            if (ranged != null) return typmod.apply(ranged);
        }
        return typmod.apply(parse(input));
    }

    /**
     * Parse a PostgreSQL interval string like '1 year 2 months 3 days 04:05:06'.
     */
    public static PgInterval parse(String input) {
        try {
            return parseInternal(input);
        } catch (NumberFormatException | ArithmeticException | IsoFieldOverflow e) {
            // A field wider than its Java type is a value-range problem, not an engine defect.
            throw PgErrors.intervalFieldOutOfRange(input == null ? "" : input.trim());
        }
    }

    private static PgInterval parseInternal(String input) {
        if (input == null) {
            return new PgInterval(0, 0, 0);
        }
        String s = input.trim();
        // PG has no spelling of the empty interval: text with nothing in it is a syntax error,
        // not a zero. Callers that want zero build it directly.
        if (s.isEmpty()) throw invalidIntervalSyntax(input);
        if (s.equalsIgnoreCase("infinity") || s.equalsIgnoreCase("+infinity")) return INFINITY;
        if (s.equalsIgnoreCase("-infinity")) return NEG_INFINITY;

        // ISO 8601 durations are read from the untrimmed text: PG only takes this branch when the
        // string itself opens with an upper-case 'P', so ' P1Y' and 'p1Y' are not durations at all.
        PgInterval iso = parseIso8601(input);
        if (iso != null) return iso;

        // Try verbose format first: '1 year 2 months 3 weeks 3 days 4 hours 5 minutes 6 seconds'
        Matcher vm = VERBOSE_INTERVAL.matcher(s);
        if (vm.matches() && !s.isEmpty()) {
            int years = vm.group(1) != null ? Integer.parseInt(vm.group(1)) : 0;
            int mons = vm.group(2) != null ? Integer.parseInt(vm.group(2)) : 0;
            int weeks = vm.group(3) != null ? Integer.parseInt(vm.group(3)) : 0;
            int days = vm.group(4) != null ? Integer.parseInt(vm.group(4)) : 0;
            int hours = vm.group(5) != null ? Integer.parseInt(vm.group(5)) : 0;
            int minutes = vm.group(6) != null ? Integer.parseInt(vm.group(6)) : 0;
            long secondMicros = secondsToMicros(vm.group(7));

            days += weeks * 7;
            long totalMonths = (long) years * 12 + mons;
            long totalMicros = (hours * 3600L + minutes * 60L) * 1_000_000L + secondMicros;

            // Return if we actually matched something, or if any group was present (even if zero)
            if (years != 0 || mons != 0 || weeks != 0 || days != 0 || hours != 0 || minutes != 0 || secondMicros != 0
                    || vm.group(1) != null || vm.group(2) != null || vm.group(3) != null
                    || vm.group(4) != null || vm.group(5) != null || vm.group(6) != null || vm.group(7) != null) {
                return checked(totalMonths, days, totalMicros);
            }
        }

        // Try PG output format: '1 year 2 mons 3 days 04:05:06'
        Matcher pm = POSTGRES_INTERVAL.matcher(s);
        if (pm.matches()) {
            int years = pm.group(1) != null ? Integer.parseInt(pm.group(1)) : 0;
            int mons = pm.group(2) != null ? Integer.parseInt(pm.group(2)) : 0;
            int days = pm.group(3) != null ? Integer.parseInt(pm.group(3)) : 0;
            int totalMonths = years * 12 + mons;
            long totalMicros = 0;
            if (pm.group(5) != null) {
                // The time of day is read by the one decoder that knows PG's rules for it, so
                // '04:70:00' is as much out of range here as it is anywhere else.
                String ss = pm.group(7) + (pm.group(8) == null ? "" : "." + pm.group(8));
                totalMicros = timeFieldMicros(
                        ("-".equals(pm.group(4)) ? "-" : "") + pm.group(5),
                        pm.group(6), ss, IntervalTypmod.FULL_RANGE);
            }
            if (totalMicros != NOT_A_TIME_FIELD) return new PgInterval(totalMonths, days, totalMicros);
        }

        // The sql_standard shapes 'Y-M [D] [H:M:S]' and 'D H:M:S' are left to the general reader
        // below: a bare number between a year-month field and a time of day is a day count, but
        // the same number with no time of day after it is a count of seconds, and only a reader
        // that looks at what follows can tell the two apart.

        // A bare number is a count of seconds. Only plain decimal digits qualify: Java would also
        // read '1d' and '1f' as numbers, but to PG the trailing letter is a unit word, so '1d' is
        // a day. Anything else falls through to the unit list, which knows what the letters mean.
        if (PLAIN_NUMBER.matcher(s).matches()) {
            return new PgInterval(0, 0, secondsToMicros(s));
        }

        // An unqualified literal is decoded by the same reader a qualified one uses, with nothing
        // restricted: PG has one DecodeInterval, and the qualifier only supplies the unit for an
        // unlabelled number. Reading both through it is what makes '1-2 3 4:05.678' a year, two
        // months, three days and four minutes rather than a heap of seconds, and what makes
        // '1 hour 2 hour' the syntax error PG says it is.
        PgInterval ranged = parseRanged(s, IntervalTypmod.PLAIN);
        if (ranged != null) return ranged;

        PgInterval units = parseUnitList(s);
        if (units != null) return units;

        throw invalidIntervalSyntax(input);
    }

    /**
     * PG's DecodeInterval reads a sequence of signed, possibly fractional quantities, each with
     * its own unit, mixed freely with bare {@code HH:MM[:SS]} time fields. That grammar covers
     * the shapes PG itself emits — {@code '-1 mons +3 days'}, {@code '1.5 years'} — as well as
     * the extended unit names, so anything the earlier fixed-shape patterns miss lands here.
     *
     * @return the parsed interval, or null when the text is not a unit list at all
     */
    private static PgInterval parseUnitList(String s) {
        String written = s;
        // PG's traditional output opened with '@'; it carries no meaning of its own
        if (s.startsWith("@")) s = s.substring(1).trim();
        Matcher tok = java.util.regex.Pattern.compile(
                "\\G\\s*(?:([+-]?\\d+):(\\d+)(?::(\\d+(?:\\.\\d+)?))?"
                + "|([+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+))\\s*([A-Za-z]+)?"
                + "|([A-Za-z]+))\\s*").matcher(s);
        long months = 0;
        long days = 0;
        long micros = 0;
        boolean matchedAny = false;
        boolean ago = false;
        boolean lastWasUnitless = false;
        int end = 0;
        while (tok.find()) {
            end = tok.end();
            if (tok.group(1) != null) {
                if (ago) throw invalidIntervalSyntax(written);
                // A bare time field: the sign on the hours carries across the whole field
                boolean neg = tok.group(1).startsWith("-");
                long h = Math.abs(Long.parseLong(tok.group(1)));
                long m = Long.parseLong(tok.group(2));
                long field = (h * 3600L + m * 60L) * 1_000_000L + secondsToMicros(tok.group(3));
                micros += neg ? -field : field;
                matchedAny = true;
                lastWasUnitless = false;
                continue;
            }
            if (tok.group(6) != null) {
                if (!"ago".equalsIgnoreCase(tok.group(6))) return null;
                // 'ago' stands where the unit for the number to its left would have been, so a
                // number that had no unit of its own has none at all: '1 ago' is a syntax error.
                if (lastWasUnitless || ago) throw invalidIntervalSyntax(written);
                ago = true;
                continue;
            }
            // 'ago' is the last word of a literal; anything after it is a syntax error
            if (ago) throw invalidIntervalSyntax(written);
            Quantity value = quantity(tok.group(4));
            lastWasUnitless = tok.group(5) == null;
            String unit = tok.group(5) == null ? "second" : normalizeUnit(tok.group(5));
            if (unit == null) return null;
            Accum one = new Accum();
            if (!addUnit(one, value, unit)) return null;
            months += one.months;
            days += one.days;
            micros += one.micros;
            matchedAny = true;
        }
        if (!matchedAny || end != s.length()) return null;
        if (ago) return checked(-months, -days, -micros);
        return checked(months, days, micros);
    }

    /** Running totals while a list of quantity-and-unit fields is decoded. */
    private static final class Accum {
        long months;
        long days;
        long micros;
    }

    /** A number in an ISO 8601 duration, and where reading it stopped. */
    private static final class IsoNumber {
        final int whole;
        final double frac;
        final int end;
        IsoNumber(int whole, double frac, int end) { this.whole = whole; this.frac = frac; this.end = end; }
    }

    /** A bare seconds count: decimal digits and nothing else, no exponent and no type suffix. */
    private static final Pattern PLAIN_NUMBER = Pattern.compile("[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)");

    /** What an ISO 8601 duration may hold where a number is expected — strtod's own grammar. */
    private static final Pattern ISO_NUMBER =
            Pattern.compile("-?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?");

    /**
     * Read an ISO 8601 duration the way PG's DecodeISO8601Interval does.
     *
     * <p>Three shapes share the grammar. The usual one names each field with a designator letter
     * ({@code P1Y2M3DT4H5M6S}), and PG simply loops over number-and-letter pairs, so a designator
     * may repeat and a quantity may be fractional. The alternative extended form writes the fields
     * positionally instead ({@code P0001-02-03T04:05:06}), and the alternative basic form runs
     * them together ({@code P00010203T040506}) — both are recognised by the shape of the very
     * first number, and neither may follow a designator field.
     *
     * <p>The text is read exactly as given: the duration must start at the first character and
     * end at the last, and every letter is upper case. That is why {@code ' P1Y'}, {@code 'P1Y '}
     * and {@code 'p1Y'} are not durations, and why a bare {@code 'P'} is nothing at all.
     *
     * @return the decoded interval, or null when the text is not an ISO 8601 duration
     */
    private static PgInterval parseIso8601(String str) {
        if (str.length() < 2 || str.charAt(0) != 'P') return null;
        Accum acc = new Accum();
        boolean datepart = true;
        boolean havefield = false;
        int n = str.length();
        int i = 1;
        while (i < n) {
            if (str.charAt(i) == 'T') {
                datepart = false;
                havefield = false;
                i++;
                continue;
            }
            int fieldStart = i;
            IsoNumber num = isoNumber(str, i);
            if (num == null) return null;
            i = num.end;
            char unit = i < n ? str.charAt(i) : '\0';
            i++;
            if (datepart) {
                switch (unit) {
                    case 'Y':
                        addYears(acc, num.whole, num.frac * 12.0);
                        break;
                    case 'M':
                        acc.months += num.whole;
                        adjustFractDays(acc, num.frac, 30);
                        break;
                    case 'W':
                        acc.days += num.whole * 7L;
                        adjustFractDays(acc, num.frac, 7);
                        break;
                    case 'D':
                        acc.days += num.whole;
                        adjustFractSeconds(acc, num.frac, 86_400);
                        break;
                    case 'T':
                    case '\0':
                        if (isoDigitWidth(str, fieldStart) == 8 && !havefield) {
                            // Alternative basic form: PYYYYMMDD, the whole date in one number
                            acc.months += (num.whole / 10000) * 12L + (num.whole / 100) % 100;
                            acc.days += num.whole % 100;
                            adjustFractSeconds(acc, num.frac, 86_400);
                            if (unit == '\0') return checked(acc.months, acc.days, acc.micros);
                            datepart = false;
                            havefield = false;
                            continue;
                        }
                        // falls through to the extended form, which reads the same first number
                    case '-': {
                        // Alternative extended form: PYYYY-MM-DD, only ever the first field
                        if (havefield) return null;
                        addYears(acc, num.whole, num.frac * 12.0);
                        if (unit == '\0') return checked(acc.months, acc.days, acc.micros);
                        if (unit == 'T') {
                            datepart = false;
                            havefield = false;
                            continue;
                        }
                        IsoNumber mon = isoNumber(str, i);
                        if (mon == null) return null;
                        i = mon.end;
                        acc.months += mon.whole;
                        adjustFractDays(acc, mon.frac, 30);
                        if (i >= n) return checked(acc.months, acc.days, acc.micros);
                        if (str.charAt(i) == 'T') {
                            datepart = false;
                            havefield = false;
                            i++;
                            continue;
                        }
                        if (str.charAt(i) != '-') return null;
                        i++;
                        IsoNumber day = isoNumber(str, i);
                        if (day == null) return null;
                        i = day.end;
                        acc.days += day.whole;
                        adjustFractSeconds(acc, day.frac, 86_400);
                        if (i >= n) return checked(acc.months, acc.days, acc.micros);
                        if (str.charAt(i) != 'T') return null;
                        datepart = false;
                        havefield = false;
                        i++;
                        continue;
                    }
                    default:
                        return null;
                }
            } else {
                switch (unit) {
                    case 'H':
                        acc.micros += num.whole * 3_600_000_000L;
                        adjustFractSeconds(acc, num.frac, 3_600);
                        break;
                    case 'M':
                        acc.micros += num.whole * 60_000_000L;
                        adjustFractSeconds(acc, num.frac, 60);
                        break;
                    case 'S':
                        acc.micros += num.whole * 1_000_000L;
                        adjustFractSeconds(acc, num.frac, 1);
                        break;
                    case '\0':
                        if (isoDigitWidth(str, fieldStart) == 6 && !havefield) {
                            // Alternative basic form: Thhmmss. PG reads what trails the six digits
                            // as microseconds outright, not as a fraction of a second.
                            acc.micros += (num.whole / 10000) * 3_600_000_000L
                                    + ((num.whole / 100) % 100) * 60_000_000L
                                    + (num.whole % 100) * 1_000_000L
                                    + roundToMicrosecond(num.frac);
                            return checked(acc.months, acc.days, acc.micros);
                        }
                        // falls through to the extended form, which reads the same first number
                    case ':': {
                        // Alternative extended form: Thh:mm:ss, only ever the first field
                        if (havefield) return null;
                        acc.micros += num.whole * 3_600_000_000L;
                        adjustFractSeconds(acc, num.frac, 3_600);
                        if (unit == '\0') return checked(acc.months, acc.days, acc.micros);
                        IsoNumber min = isoNumber(str, i);
                        if (min == null) return null;
                        i = min.end;
                        acc.micros += min.whole * 60_000_000L;
                        adjustFractSeconds(acc, min.frac, 60);
                        if (i >= n) return checked(acc.months, acc.days, acc.micros);
                        if (str.charAt(i) != ':') return null;
                        i++;
                        IsoNumber sec = isoNumber(str, i);
                        if (sec == null) return null;
                        i = sec.end;
                        acc.micros += sec.whole * 1_000_000L;
                        adjustFractSeconds(acc, sec.frac, 1);
                        if (i >= n) return checked(acc.months, acc.days, acc.micros);
                        return null;
                    }
                    default:
                        return null;
                }
            }
            havefield = true;
        }
        return checked(acc.months, acc.days, acc.micros);
    }

    /** Read one number of an ISO 8601 duration; null when nothing there looks like a number. */
    private static IsoNumber isoNumber(String str, int from) {
        if (from >= str.length()) return null;
        Matcher m = ISO_NUMBER.matcher(str);
        m.region(from, str.length());
        if (!m.lookingAt()) return null;
        double v = Double.parseDouble(m.group());
        // An unrepresentable magnitude is a malformed duration to PG, an oversized one a bad value
        if (Double.isInfinite(v) || Double.isNaN(v)) return null;
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) throw new IsoFieldOverflow();
        int whole = (int) v;
        return new IsoNumber(whole, v - whole, m.end());
    }

    /** A field of an ISO 8601 duration that will not fit the int PG decodes it into. */
    private static final class IsoFieldOverflow extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /** How many digits open the field at {@code from}; the alternative forms are 8 and 6 wide. */
    private static int isoDigitWidth(String str, int from) {
        int i = from;
        if (i < str.length() && str.charAt(i) == '-') i++;
        int start = i;
        while (i < str.length() && str.charAt(i) >= '0' && str.charAt(i) <= '9') i++;
        return i - start;
    }

    /**
     * A quantity as PG reads one: the integer digits and the fraction are decoded separately,
     * because every unit spills its fractional part into a different smaller field.
     */
    private static final class Quantity {
        final long whole;
        final double frac;
        Quantity(long whole, double frac) { this.whole = whole; this.frac = frac; }
    }

    /** Split a signed decimal into its integer digits and its fraction, as PG's decoder does. */
    private static Quantity quantity(String text) {
        String t = stripPlus(text.trim());
        int dot = t.indexOf('.');
        if (dot < 0) return new Quantity(Long.parseLong(t), 0);
        String ip = t.substring(0, dot);
        long whole = (ip.isEmpty() || "-".equals(ip)) ? 0 : Long.parseLong(ip);
        String fp = t.substring(dot);
        double frac = fp.length() > 1 ? Double.parseDouble("0" + fp) : 0;
        if (t.startsWith("-")) frac = -frac;
        return new Quantity(whole, frac);
    }

    /**
     * Add a quantity of {@code unit} to the running totals, spilling a fractional quantity into
     * the next smaller field the way PG does: a fraction of a year becomes whole months, a
     * fraction of a month or a week becomes whole days and then part of a day, and a fraction of
     * anything smaller becomes microseconds.
     *
     * @return false when the unit is not one this type can hold
     */
    private static boolean addUnit(Accum acc, Quantity q, String unit) {
        long val = q.whole;
        double fval = q.frac;
        if ("millennium".equals(unit)) return addYears(acc, val * 1000L, fval * 12_000.0);
        if ("century".equals(unit))    return addYears(acc, val * 100L, fval * 1_200.0);
        if ("decade".equals(unit))     return addYears(acc, val * 10L, fval * 120.0);
        switch (unit) {
            case "year":
                return addYears(acc, val, fval * 12.0);
            case "month":
                acc.months += val;
                adjustFractDays(acc, fval, 30);
                return true;
            case "week":
                acc.days += Math.multiplyExact(val, 7L);
                adjustFractDays(acc, fval, 7);
                return true;
            case "day":
                acc.days += val;
                adjustFractSeconds(acc, fval, 86_400);
                return true;
            case "hour":
                acc.micros += Math.multiplyExact(val, 3_600_000_000L);
                adjustFractSeconds(acc, fval, 3_600);
                return true;
            case "minute":
                acc.micros += Math.multiplyExact(val, 60_000_000L);
                adjustFractSeconds(acc, fval, 60);
                return true;
            case "second":
                acc.micros += Math.multiplyExact(val, 1_000_000L);
                adjustFractSeconds(acc, fval, 1);
                return true;
            case "millisecond":
                acc.micros += Math.multiplyExact(val, 1_000L) + roundToMicrosecond(fval * 1_000.0);
                return true;
            case "microsecond":
                acc.micros += val + roundToMicrosecond(fval);
                return true;
            default: return false;
        }
    }

    /** Years plus a fraction already scaled to months; PG rounds that fraction to a whole month. */
    private static boolean addYears(Accum acc, long years, double fractionalMonths) {
        acc.months += Math.multiplyExact(years, 12L);
        if (fractionalMonths != 0) acc.months += (long) Math.rint(fractionalMonths);
        return true;
    }

    /**
     * PG's AdjustFractSeconds: the fraction is scaled to seconds, the whole seconds are taken as
     * they stand, and only what is left is rounded to microseconds.
     */
    private static void adjustFractSeconds(Accum acc, double frac, int scale) {
        if (frac == 0) return;
        double f = frac * scale;
        long sec = (long) f;
        acc.micros += sec * 1_000_000L;
        f -= sec;
        acc.micros += roundToMicrosecond(f * 1_000_000.0);
    }

    /**
     * Round a count of microseconds to a whole one. PG rounds to the nearer, and a quantity that
     * lands exactly half way keeps the smaller magnitude — 1.5us is 1us and 3.5us is 3us, where
     * ordinary IEEE rounding would send both to the even neighbour instead. The fraction of a
     * year is the one place that does round to even, and it is rounded on its own.
     */
    private static long roundToMicrosecond(double micros) {
        double magnitude = Math.abs(micros);
        double whole = Math.floor(magnitude);
        if (magnitude - whole > 0.5) whole += 1.0;
        return (long) (micros < 0 ? -whole : whole);
    }

    /**
     * PG's AdjustFractDays: the fraction is scaled to days, the whole days are taken as they
     * stand, and the remainder becomes a fraction of one day.
     */
    private static void adjustFractDays(Accum acc, double frac, int scale) {
        if (frac == 0) return;
        double f = frac * scale;
        long extraDays = (long) f;
        acc.days += extraDays;
        f -= extraDays;
        adjustFractSeconds(acc, f, 86_400);
    }

    /**
     * Seconds to microseconds the way PG does it: whole seconds exactly, the rest rounded. The
     * text is split before it is converted, because reading '1.0000005' as one double and
     * subtracting the 1 afterwards does not leave the same fraction as reading '.0000005' alone.
     */
    private static long secondsToMicros(String text) {
        if (text == null) return 0;
        Quantity q = quantity(text);
        Accum acc = new Accum();
        acc.micros = Math.multiplyExact(q.whole, 1_000_000L);
        adjustFractSeconds(acc, q.frac, 1);
        return acc.micros;
    }

    /**
     * One field of a qualified interval literal. PG splits the text on whitespace and reads the
     * fields right to left, which is what lets the unit word in '2 hours' name the number to its
     * left and what lets '1 2' DAY TO HOUR mean a day and an hour.
     */
    private static final Pattern QUALIFIED_FIELD = Pattern.compile(
            "\\G\\s*(?:"
          + "([+-]?\\d+):(\\d+(?:\\.\\d*)?)(?::(\\d+(?:\\.\\d*)?))?"  // 1,2,3 time HH:MM[:SS]
          + "|([+-]?\\d+)-(\\d+)"                              // 4,5   sql_standard years-months
          + "|([+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+))"             // 6     bare quantity
          + "|([A-Za-z]+)"                                     // 7     unit word, or "ago"
          + "|(@)"                                             // 8     PG's traditional lead-in
          + ")\\s*");

    /**
     * Decode a literal against a field qualifier. A number the literal does not label takes the
     * qualifier's least significant field, so INTERVAL '5' DAY TO HOUR is five hours; and PG
     * refuses a literal that fills the same field twice rather than adding the two together.
     *
     * @return the decoded interval, or null when the text is not a shape this reader recognises
     *         (an ISO 8601 duration, say) and the unqualified reader should have it instead
     */
    private static PgInterval parseRanged(String input, IntervalTypmod typmod) {
        if (input == null || Strs.isBlank(input)) return null;
        String s = input.trim();
        if (s.equalsIgnoreCase("infinity") || s.equalsIgnoreCase("+infinity")) return INFINITY;
        if (s.equalsIgnoreCase("-infinity")) return NEG_INFINITY;
        // An ISO 8601 duration names every unit itself, so the qualifier has nothing to assign
        if (s.startsWith("P") || s.startsWith("p")) return null;

        List<String[]> fields = new ArrayList<String[]>();
        Matcher tok = QUALIFIED_FIELD.matcher(s);
        int end = 0;
        while (tok.find()) {
            end = tok.end();
            if (tok.group(1) != null) {
                fields.add(new String[]{"time", tok.group(1), tok.group(2), tok.group(3)});
            } else if (tok.group(4) != null) {
                fields.add(new String[]{"ym", tok.group(4), tok.group(5)});
            } else if (tok.group(6) != null) {
                fields.add(new String[]{"num", tok.group(6)});
            } else if (tok.group(7) != null) {
                fields.add(new String[]{"word", tok.group(7)});
            } else {
                fields.add(new String[]{"at"});
            }
        }
        if (fields.isEmpty() || end != s.length()) return null;

        Accum acc = new Accum();
        int filled = 0;
        String pendingUnit = null;
        boolean ago = false;
        boolean agoIsTheUnit = false;
        boolean sawField = false;
        for (int i = fields.size() - 1; i >= 0; i--) {
            String[] f = fields.get(i);
            // '@' is PG's traditional lead-in and carries nothing, wherever it stands
            if ("at".equals(f[0])) continue;
            if ("word".equals(f[0])) {
                if ("ago".equalsIgnoreCase(f[1])) {
                    // 'ago' turns the whole interval around and PG only takes it as the last word
                    // of the literal: 'ago 1 day' and '1 day ago 2 hours' are syntax errors, and
                    // so is a second 'ago' with nothing left to turn.
                    if (sawField) throw invalidIntervalSyntax(input);
                    // PG reads the fields right to left and lets a unit word name the number to
                    // its left. 'ago' is not a unit, and PG leaves it standing where the unit
                    // would have been, so a number with nothing but 'ago' to its right has no
                    // unit at all — which is why '1 ago' is a syntax error and '1 day ago' is not.
                    ago = true;
                    sawField = true;
                    agoIsTheUnit = true;
                    pendingUnit = null;
                    continue;
                }
                String unit = normalizeUnit(f[1]);
                if (unit == null) return null;
                pendingUnit = unit;
                sawField = true;
                agoIsTheUnit = false;
                continue;
            }
            sawField = true;
            if ("time".equals(f[0])) {
                int timeFields = U_HOUR | U_MINUTE | U_SECOND;
                if ((filled & timeFields) != 0) throw invalidIntervalSyntax(input);
                long micros = timeFieldMicros(f[1], f[2], f[3], typmod.range());
                if (micros == NOT_A_TIME_FIELD) return null;
                filled |= timeFields;
                acc.micros += micros;
                // PG reads whatever stands to the left of a time-of-day as a day count
                pendingUnit = "day";
                agoIsTheUnit = false;
                continue;
            }
            if ("ym".equals(f[0])) {
                int ymFields = U_YEAR | U_MONTH;
                if ((filled & ymFields) != 0) throw invalidIntervalSyntax(input);
                filled |= ymFields;
                long sign = f[1].startsWith("-") ? -1 : 1;
                long years = Math.abs(Long.parseLong(stripPlus(f[1])));
                long mons = Long.parseLong(f[2]);
                acc.months += sign * (years * 12 + mons);
                pendingUnit = null;
                agoIsTheUnit = false;
                continue;
            }
            if (agoIsTheUnit) throw invalidIntervalSyntax(input);
            String unit = pendingUnit != null
                    ? pendingUnit : IntervalTypmod.unitName(typmod.defaultUnit());
            int bit = fieldBitOf(unit);
            if ((filled & bit) != 0) throw invalidIntervalSyntax(input);
            filled |= bit;
            Quantity value;
            try {
                value = quantity(f[1]);
            } catch (NumberFormatException e) {
                return null;
            }
            if (!addUnit(acc, value, unit)) return null;
            // A unit keeps naming the numbers further left until another unit word replaces it,
            // which is what makes '1 2 days' the duplicate-field error PG reports rather than a
            // day and a stray second. The one exception is PG's own: an hour hands DAY leftwards,
            // because 'D H' is the SQL standard spelling of DAY TO HOUR.
            pendingUnit = "hour".equals(unit) ? "day" : unit;
        }
        // A literal that filled no field at all is not an interval of zero: PG has no spelling for
        // the empty interval, so 'day', 'ago', '@' and '@ ago' are all syntax errors rather than
        // 00:00:00. Handing them back as a zero let a column store a quantity nobody wrote.
        if (filled == 0) return null;
        if (ago) return checked(-acc.months, -acc.days, -acc.micros);
        return checked(acc.months, acc.days, acc.micros);
    }

    private static String stripPlus(String s) {
        return s.startsWith("+") ? s.substring(1) : s;
    }

    /**
     * Microseconds held by one HH:MM[:SS] field; the sign on the hours covers the whole field.
     *
     * <p>Two parts are not always hours and minutes. PG reads {@code a:b} as MINUTE:SECOND when
     * the qualifier is MINUTE TO SECOND, and — whatever the qualifier — whenever the second part
     * carries a fraction, because a fraction can only belong to the seconds field. That is why
     * {@code '3:04'} is three hours and four minutes but {@code '3:04.5'} is three minutes and
     * four and a half seconds.
     *
     * <p>The minutes may not exceed 59 and the seconds may not exceed 60, exactly as PG's
     * DecodeTimeForInterval requires; the hours are unbounded because an interval is a duration
     * rather than a time of day.
     */
    private static long timeFieldMicros(String hh, String mm, String ss, int range) {
        boolean negative = hh.startsWith("-");
        long hours = Math.abs(Long.parseLong(stripPlus(hh)));
        long minutes;
        String secondsText;
        if (mm.indexOf('.') >= 0) {
            if (ss != null) return NOT_A_TIME_FIELD;   // 'a:b.c:d' is no shape PG accepts
            secondsText = mm;
            minutes = hours;
            hours = 0;
        } else if (ss != null) {
            minutes = Long.parseLong(mm);
            secondsText = ss;
        } else if (range == (IntervalTypmod.MINUTE | IntervalTypmod.SECOND)) {
            secondsText = mm;
            minutes = hours;
            hours = 0;
        } else {
            minutes = Long.parseLong(mm);
            secondsText = null;
        }
        // PG counts the whole seconds and the fraction separately, and bounds each on its own:
        // 60 whole seconds is allowed (it is what makes '3:60.5' four minutes) and so is a
        // fraction that rounds to a whole second, but 61 is out of range.
        Quantity sec = secondsText == null ? new Quantity(0, 0) : quantity(secondsText);
        long fracMicros = (long) Math.rint(sec.frac * 1_000_000.0);
        if (minutes > 59 || sec.whole > 60 || Math.abs(fracMicros) > 1_000_000L) {
            throw new IsoFieldOverflow();
        }
        long field = (hours * 3600L + minutes * 60L + sec.whole) * 1_000_000L + fracMicros;
        return negative ? -field : field;
    }

    /** Sentinel for a time field whose shape PG's decoder would refuse outright. */
    private static final long NOT_A_TIME_FIELD = Long.MIN_VALUE;

    // One bit per unit word, for detecting a literal that fills the same field twice. PG keeps a
    // separate mask bit for every unit it knows rather than one per interval field, so
    // '1 week 2 days' and '1 second 500 milliseconds' are both fine while '1 day 1 day' is not.
    private static final int U_MILLENNIUM = 1 << 0;
    private static final int U_CENTURY = 1 << 1;
    private static final int U_DECADE = 1 << 2;
    private static final int U_YEAR = 1 << 3;
    private static final int U_MONTH = 1 << 4;
    private static final int U_WEEK = 1 << 5;
    private static final int U_DAY = 1 << 6;
    private static final int U_HOUR = 1 << 7;
    private static final int U_MINUTE = 1 << 8;
    private static final int U_SECOND = 1 << 9;
    private static final int U_MILLISECOND = 1 << 10;
    private static final int U_MICROSECOND = 1 << 11;

    /** The mask bit a unit word fills, for detecting a literal that fills one twice. */
    private static int fieldBitOf(String unit) {
        if ("millennium".equals(unit)) return U_MILLENNIUM;
        if ("century".equals(unit)) return U_CENTURY;
        if ("decade".equals(unit)) return U_DECADE;
        if ("year".equals(unit)) return U_YEAR;
        if ("month".equals(unit)) return U_MONTH;
        if ("week".equals(unit)) return U_WEEK;
        if ("day".equals(unit)) return U_DAY;
        if ("hour".equals(unit)) return U_HOUR;
        if ("minute".equals(unit)) return U_MINUTE;
        if ("millisecond".equals(unit)) return U_MILLISECOND;
        if ("microsecond".equals(unit)) return U_MICROSECOND;
        return U_SECOND;
    }

    private static MemgresException invalidIntervalSyntax(String input) {
        return new MemgresException(
                "invalid input syntax for type interval: \"" + input + "\"", "22007");
    }

    /**
     * PG holds each interval unit in a keyword table under at most ten characters, and truncates
     * the word it is given before looking it up. That single rule decides the whole accepted set:
     * 'microseconds' and 'microsecon' name the same field because both shorten to the same key,
     * while 'cents' and 'millenium' name nothing at all because neither is a key.
     */
    private static final int UNIT_TOKEN_MAX_LEN = 10;

    /** Map a PG interval unit word (any accepted abbreviation or plural) to its canonical name. */
    private static String normalizeUnit(String raw) {
        String u = raw.toLowerCase(java.util.Locale.ROOT);
        if (u.length() > UNIT_TOKEN_MAX_LEN) u = u.substring(0, UNIT_TOKEN_MAX_LEN);
        switch (u) {
            case "us": case "usec": case "usecond": case "useconds": case "usecs":
            case "microsecon":
                return "microsecond";
            case "ms": case "msec": case "msecond": case "mseconds": case "msecs":
            case "millisecon":
                return "millisecond";
            case "s": case "sec": case "second": case "seconds": case "secs":
                return "second";
            case "m": case "min": case "mins": case "minute": case "minutes":
                return "minute";
            case "h": case "hr": case "hrs": case "hour": case "hours":
                return "hour";
            case "d": case "day": case "days":
                return "day";
            case "w": case "week": case "weeks":
                return "week";
            case "mon": case "mons": case "month": case "months":
                return "month";
            case "y": case "yr": case "yrs": case "year": case "years":
                return "year";
            case "dec": case "decs": case "decade": case "decades":
                return "decade";
            case "c": case "cent": case "century": case "centuries":
                return "century";
            case "mil": case "mils": case "millennia": case "millennium":
                return "millennium";
            default: return null;
        }
    }

    @Override
    public String toString() {
        return toString("postgres");
    }

    /**
     * Format this interval according to the given intervalstyle.
     * Supported styles: "postgres" (default), "iso_8601", "sql_standard".
     */
    public String toString(String intervalStyle) {
        if (isPositiveInfinity()) return "infinity";
        if (isNegativeInfinity()) return "-infinity";
        if (intervalStyle != null && intervalStyle.equalsIgnoreCase("iso_8601")) {
            return toIso8601();
        } else if (intervalStyle != null && intervalStyle.equalsIgnoreCase("sql_standard")) {
            return toSqlStandard();
        } else if (intervalStyle != null && intervalStyle.equalsIgnoreCase("postgres_verbose")) {
            return toPostgresVerbose();
        }
        return toPostgres();
    }

    private String toPostgresVerbose() {
        StringBuilder sb = new StringBuilder("@ ");
        int years = months / 12;
        int mons = months % 12;
        if (years != 0) sb.append(years).append(years == 1 || years == -1 ? " year " : " years ");
        if (mons != 0) sb.append(mons).append(mons == 1 || mons == -1 ? " mon " : " mons ");
        if (days != 0) sb.append(days).append(days == 1 || days == -1 ? " day " : " days ");

        long absMicros = Math.abs(microseconds);
        long totalSecs = absMicros / 1_000_000;
        long fracMicros = absMicros % 1_000_000;
        long hours = totalSecs / 3600;
        long mins = (totalSecs % 3600) / 60;
        long secs = totalSecs % 60;
        if (microseconds < 0) {
            if (hours != 0) sb.append(-hours).append(hours == 1 ? " hour " : " hours ");
            if (mins != 0) sb.append(-mins).append(mins == 1 ? " min " : " mins ");
            if (secs != 0 || fracMicros != 0) {
                sb.append(-secs);
                if (fracMicros > 0) sb.append(String.format(".%06d", fracMicros).replaceAll("0+$", ""));
                sb.append(secs == 1 && fracMicros == 0 ? " sec " : " secs ");
            }
        } else {
            if (hours != 0) sb.append(hours).append(hours == 1 ? " hour " : " hours ");
            if (mins != 0) sb.append(mins).append(mins == 1 ? " min " : " mins ");
            if (secs != 0 || fracMicros != 0) {
                sb.append(secs);
                if (fracMicros > 0) sb.append(String.format(".%06d", fracMicros).replaceAll("0+$", ""));
                sb.append(secs == 1 && fracMicros == 0 ? " sec " : " secs ");
            }
        }

        String result = sb.toString().trim();
        if (result.equals("@")) return "@ 0";
        return result;
    }

    private String toPostgres() {
        StringBuilder sb = new StringBuilder();
        int years = months / 12;
        int mons = months % 12;

        // PG marks a positive field that directly follows a negative one with an explicit '+',
        // which is what makes '-1 mons +3 days' the printed form -- and what has to read back
        // in. Only the field before decides, so '-1 years +2 mons 3 days' has one plus, not two.
        boolean sawNegative = false;
        if (years != 0) {
            if (sawNegative && years > 0) sb.append('+');
            sb.append(years).append(years == 1 ? " year " : " years ");
            sawNegative = years < 0;
        }
        if (mons != 0) {
            if (sawNegative && mons > 0) sb.append('+');
            sb.append(mons).append(mons == 1 ? " mon " : " mons ");
            sawNegative = mons < 0;
        }
        if (days != 0) {
            if (sawNegative && days > 0) sb.append('+');
            sb.append(days).append(days == 1 ? " day " : " days ");
            sawNegative = days < 0;
        }

        long absMicros = Math.abs(microseconds);
        if (absMicros > 0 || sb.length() == 0) {
            long totalSecs = absMicros / 1_000_000;
            long fracMicros = absMicros % 1_000_000;
            long hours = totalSecs / 3600;
            long mins = (totalSecs % 3600) / 60;
            long secs = totalSecs % 60;
            if (microseconds < 0) sb.append("-");
            else if (sawNegative && microseconds > 0) sb.append("+");
            sb.append(String.format("%02d:%02d:%02d", hours, mins, secs));
            if (fracMicros > 0) {
                sb.append(String.format(".%06d", fracMicros).replaceAll("0+$", ""));
            }
        }

        return sb.toString().trim();
    }

    private String toIso8601() {
        StringBuilder sb = new StringBuilder("P");
        int years = months / 12;
        int mons = months % 12;
        if (years != 0) sb.append(years).append("Y");
        if (mons != 0) sb.append(mons).append("M");
        if (days != 0) sb.append(days).append("D");

        long absMicros = Math.abs(microseconds);
        if (absMicros > 0 || (years == 0 && mons == 0 && days == 0)) {
            sb.append("T");
            long totalSecs = absMicros / 1_000_000;
            long fracMicros = absMicros % 1_000_000;
            long hours = totalSecs / 3600;
            long mins = (totalSecs % 3600) / 60;
            long secs = totalSecs % 60;
            boolean neg = microseconds < 0;
            if (hours != 0) sb.append(neg ? -hours : hours).append("H");
            if (mins != 0) sb.append(neg ? -mins : mins).append("M");
            if (secs != 0 || fracMicros != 0 || (hours == 0 && mins == 0)) {
                long displaySecs = neg ? -secs : secs;
                sb.append(displaySecs);
                if (fracMicros > 0) {
                    sb.append(String.format(".%06d", fracMicros).replaceAll("0+$", ""));
                }
                sb.append("S");
            }
        }
        return sb.toString();
    }

    private String toSqlStandard() {
        StringBuilder sb = new StringBuilder();
        int years = months / 12;
        int mons = months % 12;

        if (years != 0 || mons != 0) {
            sb.append(years).append("-").append(Math.abs(mons));
        }

        if (days != 0) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(days);
        }

        long absMicros = Math.abs(microseconds);
        if (absMicros > 0 || sb.length() == 0) {
            if (sb.length() > 0) sb.append(" ");
            long totalSecs = absMicros / 1_000_000;
            long fracMicros = absMicros % 1_000_000;
            long hours = totalSecs / 3600;
            long mins = (totalSecs % 3600) / 60;
            long secs = totalSecs % 60;
            if (microseconds < 0) sb.append("-");
            sb.append(String.format("%d:%02d:%02d", hours, mins, secs));
            if (fracMicros > 0) {
                sb.append(String.format(".%06d", fracMicros).replaceAll("0+$", ""));
            }
        }

        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PgInterval)) return false;
        PgInterval that = (PgInterval) o;
        return months == that.months && days == that.days && microseconds == that.microseconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(months, days, microseconds);
    }

    @Override
    public int compareTo(PgInterval other) {
        if (isInfinite() || other.isInfinite()) {
            int mine = isPositiveInfinity() ? 1 : isNegativeInfinity() ? -1 : 0;
            int theirs = other.isPositiveInfinity() ? 1 : other.isNegativeInfinity() ? -1 : 0;
            return Integer.compare(mine, theirs);
        }
        // Approximate comparison: 1 month = 30 days, 1 day = 24 hours
        long thisTotalMicros = months * 30L * 24 * 3600 * 1_000_000L + days * 24L * 3600 * 1_000_000L + microseconds;
        long otherTotalMicros = other.months * 30L * 24 * 3600 * 1_000_000L + other.days * 24L * 3600 * 1_000_000L + other.microseconds;
        return Long.compare(thisTotalMicros, otherTotalMicros);
    }
}
