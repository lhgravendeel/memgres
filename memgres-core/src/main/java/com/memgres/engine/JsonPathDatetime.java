package com.memgres.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * The date and time values a jsonpath can make out of a string.
 *
 * <p>jsonpath has no date literal, so {@code .datetime()} and its typed relatives are the only way
 * a path reaches one: the string is read, the shape it turned out to have is remembered, and the
 * method then asks for that shape to be cast to the one it names. Both halves matter. Reading
 * {@code "2020-01-02"} as a date rather than as a timestamp at midnight is what makes
 * {@code .time()} of it a mistake rather than {@code 00:00:00}, and it is the cast that decides
 * whether the answer can be given at all: a value that carries an offset and one that does not
 * cannot be turned into each other without saying which zone the one without is in, so the
 * plain functions refuse and only the {@code _tz} ones do it.
 */
final class JsonPathDatetime {

    static final int DATE = 0;
    static final int TIME = 1;
    static final int TIME_TZ = 2;
    static final int TIMESTAMP = 3;
    static final int TIMESTAMP_TZ = 4;

    private static final String[] TYPE_NAMES = {"date", "time", "timetz", "timestamp",
            "timestamptz"};

    /** Which of the five shapes this value has. */
    final int type;
    /** The day, or null for a bare time. */
    final LocalDate date;
    /** The time of day, or null for a bare date. */
    final LocalTime time;
    /** The offset from UTC, or null where the value carries none. */
    final ZoneOffset offset;

    private JsonPathDatetime(int type, LocalDate date, LocalTime time, ZoneOffset offset) {
        this.type = type;
        this.date = date;
        this.time = time;
        this.offset = offset;
    }

    // ------------------------------------------------------------------ reading

    /**
     * Reads a string as whichever of the five shapes it spells, or null when it spells none of
     * them. The shapes are told apart by what is present rather than tried one parser at a time:
     * a day, a time of day, an offset, and the two of the first three that appear decide the type.
     */
    static JsonPathDatetime parse(String text) {
        String s = text.trim();
        if (s.isEmpty()) return null;
        int at = 0;
        LocalDate date = null;
        if (looksLikeDate(s)) {
            int end = s.indexOf('T');
            if (end < 0) end = s.indexOf(' ');
            if (end < 0) end = s.length();
            try {
                date = LocalDate.parse(s.substring(0, end));
            } catch (RuntimeException e) {
                return null;
            }
            at = end;
            if (at < s.length()) at++;                    // the T or the space
            if (at >= s.length()) return new JsonPathDatetime(DATE, date, null, null);
        }
        int offsetAt = offsetStart(s, at);
        LocalTime time;
        try {
            time = LocalTime.parse(s.substring(at, offsetAt));
        } catch (RuntimeException e) {
            return null;
        }
        ZoneOffset offset = null;
        if (offsetAt < s.length()) {
            offset = parseOffset(s.substring(offsetAt));
            if (offset == null) return null;
        }
        if (date != null) {
            return new JsonPathDatetime(offset == null ? TIMESTAMP : TIMESTAMP_TZ, date, time,
                    offset);
        }
        return new JsonPathDatetime(offset == null ? TIME : TIME_TZ, null, time, offset);
    }

    /** Reads a string through a to_char-style template, as {@code .datetime(template)} does. */
    static JsonPathDatetime parse(String text, String template) {
        LocalDateTime parsed;
        try {
            parsed = DateTimeTemplate.parse(text.trim(), template);
        } catch (MemgresException e) {
            return null;
        }
        // A template that names no time field describes a day, so the midnight the parser fills
        // in is not part of the value and the result is a date.
        return mentionsTime(template)
                ? new JsonPathDatetime(TIMESTAMP, parsed.toLocalDate(), parsed.toLocalTime(), null)
                : new JsonPathDatetime(DATE, parsed.toLocalDate(), null, null);
    }

    private static boolean mentionsTime(String template) {
        String upper = template.toUpperCase(java.util.Locale.ROOT);
        return upper.contains("HH") || upper.contains("MI") || upper.contains("SS")
                || upper.contains("US") || upper.contains("MS");
    }

    private static boolean looksLikeDate(String s) {
        if (s.length() < 10) return false;
        for (int i = 0; i < 10; i++) {
            char c = s.charAt(i);
            boolean dash = i == 4 || i == 7;
            if (dash ? c != '-' : c < '0' || c > '9') return false;
        }
        return true;
    }

    /** Where the offset starts in a time, or the end of the string where there is none. */
    private static int offsetStart(String s, int from) {
        for (int i = from; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == 'Z' || c == 'z' || c == '+' || (c == '-' && i > from)) return i;
        }
        return s.length();
    }

    private static ZoneOffset parseOffset(String text) {
        if (text.equalsIgnoreCase("Z")) return ZoneOffset.UTC;
        try {
            // A bare hour offset is the common spelling in SQL and is not one java.time reads.
            String t = text.length() == 3 ? text + ":00" : text;
            return ZoneOffset.of(t);
        } catch (RuntimeException e) {
            return null;
        }
    }

    // ------------------------------------------------------------------ casting

    /**
     * This value seen as {@code target}.
     *
     * @param tzAllowed whether the caller is one of the {@code _tz} functions, which may use the
     *                  session time zone to bridge a value that carries an offset and one that
     *                  does not
     * @return the converted value, or null when the shapes are not convertible at all
     * @throws MemgresException where the conversion is possible but needs a time zone the plain
     *                          functions are not allowed to consult
     */
    JsonPathDatetime cast(int target, boolean tzAllowed) {
        if (type == target) return this;
        switch (target) {
            case DATE:
                if (type == TIMESTAMP) return new JsonPathDatetime(DATE, date, null, null);
                if (type == TIMESTAMP_TZ) return atSessionZone(tzAllowed, DATE);
                return null;
            case TIME:
                if (type == TIMESTAMP) return new JsonPathDatetime(TIME, null, time, null);
                if (type == TIME_TZ || type == TIMESTAMP_TZ) return atSessionZone(tzAllowed, TIME);
                return null;
            case TIME_TZ:
                if (type == TIME) return atSessionZone(tzAllowed, TIME_TZ);
                // A timestamptz already carries an offset, so the time of day inside it is read
                // in the session zone whether or not the caller was allowed to ask for one.
                if (type == TIMESTAMP_TZ) {
                    LocalTime utc = time.minusSeconds(offset.getTotalSeconds());
                    return new JsonPathDatetime(TIME_TZ, null, utc, ZoneOffset.UTC);
                }
                return null;
            case TIMESTAMP:
                if (type == DATE) {
                    return new JsonPathDatetime(TIMESTAMP, date, LocalTime.MIDNIGHT, null);
                }
                if (type == TIMESTAMP_TZ) return atSessionZone(tzAllowed, TIMESTAMP);
                return null;
            case TIMESTAMP_TZ:
                if (type == DATE) {
                    return withSessionOffset(tzAllowed, date, LocalTime.MIDNIGHT);
                }
                if (type == TIMESTAMP) return withSessionOffset(tzAllowed, date, time);
                return null;
            default:
                return null;
        }
    }

    private JsonPathDatetime atSessionZone(boolean tzAllowed, int target) {
        requireTz(tzAllowed, target);
        // The session zone is UTC, so dropping the offset is shifting the value onto it.
        long shift = -offset.getTotalSeconds();
        if (date == null) {
            return new JsonPathDatetime(target, null, time.plusSeconds(shift), null);
        }
        LocalDateTime shifted = LocalDateTime.of(date, time).plusSeconds(shift);
        if (target == DATE) return new JsonPathDatetime(DATE, shifted.toLocalDate(), null, null);
        if (target == TIME) return new JsonPathDatetime(TIME, null, shifted.toLocalTime(), null);
        return new JsonPathDatetime(TIMESTAMP, shifted.toLocalDate(), shifted.toLocalTime(), null);
    }

    private JsonPathDatetime withSessionOffset(boolean tzAllowed, LocalDate day, LocalTime clock) {
        requireTz(tzAllowed, TIMESTAMP_TZ);
        return new JsonPathDatetime(TIMESTAMP_TZ, day, clock, ZoneOffset.UTC);
    }

    private void requireTz(boolean tzAllowed, int target) {
        if (tzAllowed) return;
        MemgresException e = new MemgresException("cannot convert value from " + TYPE_NAMES[type]
                + " to " + TYPE_NAMES[target] + " without time zone usage", "0A000");
        e.setHint("Use *_tz() function for time zone support.");
        throw e;
    }

    // ----------------------------------------------------------------- printing

    /** The value rounded to {@code digits} fractional second digits, as a typmod would. */
    JsonPathDatetime rounded(int digits) {
        if (time == null || digits < 0 || digits >= 9) return this;
        BigDecimal nanos = BigDecimal.valueOf(time.getNano())
                .movePointLeft(9).setScale(digits, RoundingMode.HALF_UP).movePointRight(9);
        long carried = nanos.longValue();
        LocalTime rounded = time.withNano(0);
        LocalDate day = date;
        if (carried >= 1000000000L) {
            LocalTime bumped = rounded.plusSeconds(1);
            if (day != null && bumped.isBefore(rounded)) day = day.plusDays(1);
            rounded = bumped;
        } else {
            rounded = rounded.withNano((int) carried);
        }
        return new JsonPathDatetime(type, day, rounded, offset);
    }

    /** The value written the way PostgreSQL writes it inside a jsonb string. */
    String text() {
        StringBuilder sb = new StringBuilder();
        if (date != null) {
            sb.append(String.format("%04d-%02d-%02d", date.getYear(), date.getMonthValue(),
                    date.getDayOfMonth()));
        }
        if (time != null) {
            if (date != null) sb.append('T');
            sb.append(String.format("%02d:%02d:%02d", time.getHour(), time.getMinute(),
                    time.getSecond()));
            appendFraction(sb, time.getNano());
        }
        if (offset != null) appendOffset(sb, offset.getTotalSeconds());
        return sb.toString();
    }

    private static void appendFraction(StringBuilder sb, int nano) {
        if (nano == 0) return;
        String digits = String.format("%09d", nano);
        int end = digits.length();
        while (end > 1 && digits.charAt(end - 1) == '0') end--;
        sb.append('.').append(digits, 0, end);
    }

    private static void appendOffset(StringBuilder sb, int seconds) {
        sb.append(seconds < 0 ? '-' : '+');
        int abs = Math.abs(seconds);
        sb.append(String.format("%02d:%02d", abs / 3600, (abs % 3600) / 60));
        if (abs % 60 != 0) sb.append(String.format(":%02d", abs % 60));
    }

    /** The name a diagnostic gives this shape. */
    static String typeName(int type) {
        return TYPE_NAMES[type];
    }

    /** The name {@code .type()} gives this shape, which is the SQL type's own name. */
    String sqlTypeName() {
        return SQL_TYPE_NAMES[type];
    }

    private static final String[] SQL_TYPE_NAMES = {"date", "time without time zone",
            "time with time zone", "timestamp without time zone", "timestamp with time zone"};

    // -------------------------------------------------------------- comparison

    /**
     * The two values compared, or null where they are of shapes that do not compare at all. A day
     * and a time of day are two different questions rather than two answers to one, so neither is
     * less than the other and neither equals it; a day and a moment in a day do compare, with the
     * day taken as its midnight.
     *
     * @throws MemgresException where the two shapes could be brought together only by consulting
     *                          the session time zone and the caller is not allowed to
     */
    static Integer compare(JsonPathDatetime a, JsonPathDatetime b, boolean tzAllowed) {
        if (isTimeOfDay(a.type) != isTimeOfDay(b.type)) return null;
        int target = Math.max(a.type, b.type);
        return a.cast(target, tzAllowed).compareSameType(b.cast(target, tzAllowed));
    }

    /** Whether this shape is a time of day, which is the half of the shapes that carries no day. */
    private static boolean isTimeOfDay(int type) {
        return type == TIME || type == TIME_TZ;
    }

    private int compareSameType(JsonPathDatetime other) {
        switch (type) {
            case DATE:
                return date.compareTo(other.date);
            case TIME:
                return time.compareTo(other.time);
            case TIME_TZ:
                // A time with a zone is compared at the moment of day it names in UTC, which may
                // fall outside the day it was written in -- so the shift is not wrapped. Two that
                // name the same moment in different zones are still two values rather than one,
                // and the zone further west is the greater: a timetz remembers where it was
                // written, and equality means both halves agree.
                int moment = Long.compare(utcNanoOfDay(), other.utcNanoOfDay());
                return moment != 0 ? moment
                        : Integer.compare(-offset.getTotalSeconds(),
                                -other.offset.getTotalSeconds());
            case TIMESTAMP:
                return LocalDateTime.of(date, time)
                        .compareTo(LocalDateTime.of(other.date, other.time));
            default:
                return LocalDateTime.of(date, time).toInstant(offset)
                        .compareTo(LocalDateTime.of(other.date, other.time).toInstant(other.offset));
        }
    }

    private long utcNanoOfDay() {
        return time.toNanoOfDay() - offset.getTotalSeconds() * 1000000000L;
    }
}
