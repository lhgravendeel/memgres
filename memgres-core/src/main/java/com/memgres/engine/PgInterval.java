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

    public PgInterval(int months, int days, long microseconds) {
        this.months = months;
        this.days = days;
        this.microseconds = microseconds;
    }

    public int getMonths() { return months; }
    public int getDays() { return days; }
    public long getMicroseconds() { return microseconds; }

    public PgInterval plus(PgInterval other) {
        return new PgInterval(months + other.months, days + other.days, microseconds + other.microseconds);
    }

    public PgInterval minus(PgInterval other) {
        return new PgInterval(months - other.months, days - other.days, microseconds - other.microseconds);
    }

    public PgInterval negate() {
        return new PgInterval(-months, -days, -microseconds);
    }

    public PgInterval multiply(double factor) {
        return new PgInterval(
                (int) Math.round(months * factor),
                (int) Math.round(days * factor),
                Math.round(microseconds * factor)
        );
    }

    /**
     * Add this interval to a LocalDate.
     */
    public LocalDate addTo(LocalDate date) {
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
        if (input == null || Strs.isBlank(input)) {
            return new PgInterval(0, 0, 0);
        }
        String s = input.trim();

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
            int totalMonths = years * 12 + mons;
            long totalMicros = (hours * 3600L + minutes * 60L) * 1_000_000L + Math.round(seconds * 1_000_000L);

            // Return if we actually matched something, or if any group was present (even if zero)
            if (years != 0 || mons != 0 || weeks != 0 || days != 0 || hours != 0 || minutes != 0 || seconds != 0
                    || vm.group(1) != null || vm.group(2) != null || vm.group(3) != null
                    || vm.group(4) != null || vm.group(5) != null || vm.group(6) != null || vm.group(7) != null) {
                return new PgInterval(totalMonths, days, totalMicros);
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

        // Try simple number (treated as seconds)
        try {
            double secs = Double.parseDouble(s);
            return new PgInterval(0, 0, Math.round(secs * 1_000_000));
        } catch (NumberFormatException e) {
            // ignore
        }

        throw new MemgresException("invalid input syntax for type interval: \"" + input + "\"", "22007");
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
        if (intervalStyle != null && intervalStyle.equalsIgnoreCase("iso_8601")) {
            return toIso8601();
        } else if (intervalStyle != null && intervalStyle.equalsIgnoreCase("sql_standard")) {
            return toSqlStandard();
        }
        return toPostgres();
    }

    private String toPostgres() {
        StringBuilder sb = new StringBuilder();
        int years = months / 12;
        int mons = months % 12;

        if (years != 0) sb.append(years).append(years == 1 ? " year " : " years ");
        if (mons != 0) sb.append(mons).append(mons == 1 ? " mon " : " mons ");
        if (days != 0) sb.append(days).append(days == 1 ? " day " : " days ");

        long absMicros = Math.abs(microseconds);
        if (absMicros > 0 || sb.length() == 0) {
            long totalSecs = absMicros / 1_000_000;
            long fracMicros = absMicros % 1_000_000;
            long hours = totalSecs / 3600;
            long mins = (totalSecs % 3600) / 60;
            long secs = totalSecs % 60;
            if (microseconds < 0) sb.append("-");
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
        // Approximate comparison: 1 month = 30 days, 1 day = 24 hours
        long thisTotalMicros = months * 30L * 24 * 3600 * 1_000_000L + days * 24L * 3600 * 1_000_000L + microseconds;
        long otherTotalMicros = other.months * 30L * 24 * 3600 * 1_000_000L + other.days * 24L * 3600 * 1_000_000L + other.microseconds;
        return Long.compare(thisTotalMicros, otherTotalMicros);
    }
}
