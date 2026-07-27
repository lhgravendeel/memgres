package com.memgres.engine;

import com.memgres.engine.util.Strs;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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
     * Parse a PostgreSQL interval string like '1 year 2 months 3 days 04:05:06'.
     */
    public static PgInterval parse(String input) {
        try {
            return parseInternal(input);
        } catch (NumberFormatException | ArithmeticException e) {
            // A field wider than its Java type is a value-range problem, not an engine defect.
            throw PgErrors.intervalFieldOutOfRange(input == null ? "" : input.trim());
        }
    }

    private static PgInterval parseInternal(String input) {
        if (input == null || Strs.isBlank(input)) {
            return new PgInterval(0, 0, 0);
        }
        String s = input.trim();
        if (s.equalsIgnoreCase("infinity") || s.equalsIgnoreCase("+infinity")) return INFINITY;
        if (s.equalsIgnoreCase("-infinity")) return NEG_INFINITY;

        // Try ISO 8601 duration format: P[nY][nM][nD][T[nH][nM][nS]]
        if (s.startsWith("P") || s.startsWith("p")) {
            Matcher iso = java.util.regex.Pattern.compile(
                    "^[Pp](?:(-?\\d+)[Yy])?(?:(-?\\d+)[Mm])?(?:(-?\\d+)[Ww])?(?:(-?\\d+)[Dd])?" +
                    "(?:[Tt](?:(-?\\d+)[Hh])?(?:(-?\\d+)[Mm])?(?:(-?\\d+(?:\\.\\d+)?)[Ss])?)?$"
            ).matcher(s);
            if (iso.matches()) {
                int years = iso.group(1) != null ? Integer.parseInt(iso.group(1)) : 0;
                int mons = iso.group(2) != null ? Integer.parseInt(iso.group(2)) : 0;
                int weeks = iso.group(3) != null ? Integer.parseInt(iso.group(3)) : 0;
                int days = iso.group(4) != null ? Integer.parseInt(iso.group(4)) : 0;
                int hours = iso.group(5) != null ? Integer.parseInt(iso.group(5)) : 0;
                int minutes = iso.group(6) != null ? Integer.parseInt(iso.group(6)) : 0;
                double seconds = iso.group(7) != null ? Double.parseDouble(iso.group(7)) : 0;
                days += weeks * 7;
                long totalMonths = (long) years * 12 + mons;
                long totalMicros = (hours * 3600L + minutes * 60L) * 1_000_000L + Math.round(seconds * 1_000_000L);
                return checked(totalMonths, days, totalMicros);
            }
        }

        // Try verbose format first: '1 year 2 months 3 weeks 3 days 4 hours 5 minutes 6 seconds'
        Matcher vm = VERBOSE_INTERVAL.matcher(s);
        if (vm.matches() && !s.isEmpty()) {
            int years = vm.group(1) != null ? Integer.parseInt(vm.group(1)) : 0;
            int mons = vm.group(2) != null ? Integer.parseInt(vm.group(2)) : 0;
            int weeks = vm.group(3) != null ? Integer.parseInt(vm.group(3)) : 0;
            int days = vm.group(4) != null ? Integer.parseInt(vm.group(4)) : 0;
            int hours = vm.group(5) != null ? Integer.parseInt(vm.group(5)) : 0;
            int minutes = vm.group(6) != null ? Integer.parseInt(vm.group(6)) : 0;
            double seconds = vm.group(7) != null ? Double.parseDouble(vm.group(7)) : 0;

            days += weeks * 7;
            long totalMonths = (long) years * 12 + mons;
            long totalMicros = (hours * 3600L + minutes * 60L) * 1_000_000L + Math.round(seconds * 1_000_000L);

            // Return if we actually matched something, or if any group was present (even if zero)
            if (years != 0 || mons != 0 || weeks != 0 || days != 0 || hours != 0 || minutes != 0 || seconds != 0
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
            String sign = pm.group(4);
            int hours = pm.group(5) != null ? Integer.parseInt(pm.group(5)) : 0;
            int minutes = pm.group(6) != null ? Integer.parseInt(pm.group(6)) : 0;
            int secs = pm.group(7) != null ? Integer.parseInt(pm.group(7)) : 0;
            int fracMicros = 0;
            if (pm.group(8) != null) {
                String frac = pm.group(8);
                // Pad to 6 digits
                frac = (frac + "000000").substring(0, 6);
                fracMicros = Integer.parseInt(frac);
            }

            int totalMonths = years * 12 + mons;
            long totalMicros = (hours * 3600L + minutes * 60L + secs) * 1_000_000L + fracMicros;
            if ("-".equals(sign)) totalMicros = -totalMicros;

            return new PgInterval(totalMonths, days, totalMicros);
        }

        // Try sql_standard year-month format: 'Y-M' (e.g. '1-2' = 1 year 2 months)
        // Also handles optional sign and optional day/time parts: [+|-]Y-M [D] [H:M:S]
        {
            java.util.regex.Matcher sqm = java.util.regex.Pattern.compile(
                    "^(-?)(\\d+)-(\\d+)(?:\\s+(-?\\d+))?(?:\\s+(-?)(\\d+):(\\d+)(?::(\\d+(?:\\.\\d+)?))?)?$"
            ).matcher(s);
            if (sqm.matches()) {
                int sign = "-".equals(sqm.group(1)) ? -1 : 1;
                int years = Integer.parseInt(sqm.group(2));
                int mons = Integer.parseInt(sqm.group(3));
                int totalMonths = sign * (years * 12 + mons);
                int days = sqm.group(4) != null ? Integer.parseInt(sqm.group(4)) : 0;
                long totalMicros = 0;
                if (sqm.group(6) != null) {
                    int tsign = "-".equals(sqm.group(5)) ? -1 : 1;
                    int hours = Integer.parseInt(sqm.group(6));
                    int minutes = Integer.parseInt(sqm.group(7));
                    double secs = sqm.group(8) != null ? Double.parseDouble(sqm.group(8)) : 0;
                    totalMicros = tsign * ((hours * 3600L + minutes * 60L) * 1_000_000L + Math.round(secs * 1_000_000L));
                }
                return new PgInterval(totalMonths, days, totalMicros);
            }
        }

        // Try sql_standard day-time format: 'D HH:MM:SS' (e.g. '2 04:05:06' = 2 days 04:05:06)
        {
            java.util.regex.Matcher dtm = java.util.regex.Pattern.compile(
                    "^(-?)(\\d+)\\s+(-?)(\\d+):(\\d+)(?::(\\d+(?:\\.\\d+)?))?$"
            ).matcher(s);
            if (dtm.matches()) {
                int dsign = "-".equals(dtm.group(1)) ? -1 : 1;
                int days = dsign * Integer.parseInt(dtm.group(2));
                int tsign = "-".equals(dtm.group(3)) ? -1 : 1;
                int hours = Integer.parseInt(dtm.group(4));
                int minutes = Integer.parseInt(dtm.group(5));
                double secs = dtm.group(6) != null ? Double.parseDouble(dtm.group(6)) : 0;
                long totalMicros = tsign * ((hours * 3600L + minutes * 60L) * 1_000_000L + Math.round(secs * 1_000_000L));
                return new PgInterval(0, days, totalMicros);
            }
        }

        // Try simple number (treated as seconds)
        try {
            double secs = Double.parseDouble(s);
            return new PgInterval(0, 0, Math.round(secs * 1_000_000));
        } catch (NumberFormatException e) {
            // ignore
        }

        PgInterval units = parseUnitList(s);
        if (units != null) return units;

        throw new MemgresException("invalid input syntax for type interval: \"" + input + "\"", "22007");
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
        int end = 0;
        while (tok.find()) {
            end = tok.end();
            if (tok.group(1) != null) {
                // A bare time field: the sign on the hours carries across the whole field
                boolean neg = tok.group(1).startsWith("-");
                long h = Math.abs(Long.parseLong(tok.group(1)));
                long m = Long.parseLong(tok.group(2));
                double sec = tok.group(3) != null ? Double.parseDouble(tok.group(3)) : 0;
                long field = (h * 3600L + m * 60L) * 1_000_000L + Math.round(sec * 1_000_000L);
                micros += neg ? -field : field;
                matchedAny = true;
                continue;
            }
            if (tok.group(6) != null) {
                if (!"ago".equalsIgnoreCase(tok.group(6))) return null;
                ago = true;
                continue;
            }
            double value = Double.parseDouble(tok.group(4));
            String unit = tok.group(5) == null ? "second" : normalizeUnit(tok.group(5));
            if (unit == null) return null;
            // Scale the larger units down to years first, so one fractional rule covers them all
            if ("millennium".equals(unit)) { value *= 1000; unit = "year"; }
            else if ("century".equals(unit)) { value *= 100; unit = "year"; }
            else if ("decade".equals(unit)) { value *= 10; unit = "year"; }
            long whole = (long) value;
            double frac = value - whole;
            switch (unit) {
                case "year":
                    // A fractional year spills into whole months, as PG does
                    months += whole * 12 + Math.round(frac * 12);
                    break;
                case "month":
                    months += whole;
                    days += Math.round(frac * 30);
                    break;
                case "week":
                    days += whole * 7;
                    micros += Math.round(frac * 7 * 86_400_000_000L);
                    break;
                case "day":
                    days += whole;
                    micros += Math.round(frac * 86_400_000_000L);
                    break;
                case "hour":        micros += Math.round(value * 3_600_000_000L); break;
                case "minute":      micros += Math.round(value * 60_000_000L); break;
                case "second":      micros += Math.round(value * 1_000_000L); break;
                case "millisecond": micros += Math.round(value * 1_000L); break;
                case "microsecond": micros += Math.round(value); break;
                default: return null;
            }
            matchedAny = true;
        }
        if (!matchedAny || end != s.length()) return null;
        if (ago) return checked(-months, -days, -micros);
        return checked(months, days, micros);
    }

    /** Map a PG interval unit word (any accepted abbreviation or plural) to its canonical name. */
    private static String normalizeUnit(String raw) {
        String u = raw.toLowerCase();
        if (u.length() > 1 && u.endsWith("s")) u = u.substring(0, u.length() - 1);
        switch (u) {
            case "microsecond": case "usec": case "us": return "microsecond";
            case "millisecond": case "msec": case "ms": return "millisecond";
            case "second": case "sec": case "s": return "second";
            case "minute": case "min": case "m": return "minute";
            case "hour": case "hr": case "h": return "hour";
            case "day": case "d": return "day";
            case "week": case "w": return "week";
            case "month": case "mon": return "month";
            case "year": case "yr": case "y": return "year";
            case "decade": case "dec": return "decade";
            case "century": case "centurie": case "cent": return "century";
            case "millennium": case "millennia": case "millenium": return "millennium";
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

        // PG marks a positive field that follows a negative one with an explicit '+', which is
        // what makes '-1 mons +3 days' the printed form -- and what has to read back in.
        boolean sawNegative = false;
        if (years != 0) {
            if (sawNegative && years > 0) sb.append('+');
            sb.append(years).append(years == 1 ? " year " : " years ");
            sawNegative |= years < 0;
        }
        if (mons != 0) {
            if (sawNegative && mons > 0) sb.append('+');
            sb.append(mons).append(mons == 1 ? " mon " : " mons ");
            sawNegative |= mons < 0;
        }
        if (days != 0) {
            if (sawNegative && days > 0) sb.append('+');
            sb.append(days).append(days == 1 ? " day " : " days ");
            sawNegative |= days < 0;
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
