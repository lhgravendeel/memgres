package com.memgres.engine;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PostgreSQL's field reader for date and time literals.
 *
 * <p>PostgreSQL does not match a literal against a list of layouts. It cuts the text into fields,
 * decides what each field is from its own shape — a month name, a clock reading, a displacement, a
 * bare number — and only then works out which of the numbers is the day and which is the year. That
 * is why {@code 'Jan 8 99'}, {@code '8 Jan 99'}, {@code 'Mon Jan 8 1999'} and
 * {@code '04:05:06 Jan 8 1999'} all name the same day: the order the fields come in barely matters,
 * and a weekday name does not matter at all.
 *
 * <p>This reads the forms the layout lists could not, and only those: it is reached after the
 * ordinary parsing has already failed, so it can widen what is accepted but never change an answer
 * already being given. Text it cannot make a date of returns null, and the caller reports the
 * literal as bad input; a field that is written plainly but names nothing real — a 32nd day, a
 * 99th — throws, because PostgreSQL calls that a field out of range rather than bad input.
 */
final class PgDateTimeDecoder {

    private PgDateTimeDecoder() {
    }

    /** What a literal was found to say. Any of the parts may be absent. */
    static final class Fields {
        /** The proleptic year, with BC already folded in, or null when no year was written. */
        Integer year;
        Integer month;
        Integer day;
        LocalTime time;
        /** The displacement the literal named, or null when it named no zone at all. */
        ZoneOffset offset;
        boolean bc;

        /** The year exactly as written, and how wide it was written; both decide the real one. */
        private Integer writtenYear;
        private int writtenYearDigits;

        boolean hasDate() {
            return year != null && month != null && day != null;
        }

        LocalDate date() {
            return LocalDate.of(year, month, day);
        }
    }

    private static final Map<String, Integer> MONTHS = new HashMap<>();
    private static final List<String> WEEKDAYS = new ArrayList<>();

    static {
        String[] full = {"january", "february", "march", "april", "may", "june", "july",
                "august", "september", "october", "november", "december"};
        for (int i = 0; i < full.length; i++) {
            MONTHS.put(full[i], i + 1);
            // PostgreSQL matches a month by its first three letters, so every longer spelling of
            // the name reaches the same month as the abbreviation does.
            MONTHS.put(full[i].substring(0, 3), i + 1);
        }
        String[] days = {"sunday", "monday", "tuesday", "wednesday", "thursday", "friday",
                "saturday"};
        for (String d : days) {
            WEEKDAYS.add(d);
            WEEKDAYS.add(d.substring(0, 3));
        }
    }

    /** A clock reading, on its own or with a displacement written straight onto it. */
    private static final java.util.regex.Pattern CLOCK = java.util.regex.Pattern.compile(
            "(\\d{1,2}):(\\d{1,2})(?::(\\d{1,2})(\\.\\d+)?)?");

    /** A displacement: a sign, an hour, and optionally minutes and seconds after it. */
    private static final java.util.regex.Pattern DISPLACEMENT = java.util.regex.Pattern.compile(
            "([+-])(\\d{1,2})(?::?(\\d{2}))?(?::(\\d{2}))?");

    private static final java.util.regex.Pattern DIGITS = java.util.regex.Pattern.compile("\\d+");

    /**
     * Read the literal, or return null when it is not one this understands.
     *
     * @param original the literal as written, for the message of any error raised
     */
    static Fields decode(String text, String original) {
        String trimmed = text.trim();
        if (trimmed.isEmpty()) return null;
        Fields out = new Fields();
        if (readDayOfYear(trimmed, out)) return out;
        // The numbers whose meaning the shape of the literal does not settle on its own: which of
        // them is the day and which the year is decided once every field has been seen.
        List<int[]> loose = new ArrayList<>();   // {value, digits, cameBeforeTheMonthName}
        boolean meridiemPm = false;
        boolean sawMeridiem = false;

        for (String token : trimmed.split("[\\s,]+")) {
            if (token.isEmpty()) continue;
            String lower = token.toLowerCase(Locale.ROOT);
            if (lower.equals("bc") || lower.equals("b.c.")) {
                out.bc = true;
                continue;
            }
            if (lower.equals("ad") || lower.equals("a.d.")) continue;
            if (lower.equals("am") || lower.equals("pm")) {
                sawMeridiem = true;
                meridiemPm = lower.equals("pm");
                continue;
            }
            if (WEEKDAYS.contains(lower)) continue;   // which weekday it was adds nothing
            if (readClockToken(token, out, original)) continue;
            if (readDisplacementToken(token, out)) continue;
            if (readZoneName(token, out)) continue;
            if (MONTHS.containsKey(lower)) {
                if (out.month != null) return null;   // two months is not a date
                out.month = MONTHS.get(lower);
                continue;
            }
            if (DIGITS.matcher(token).matches()) {
                if (!readBareNumber(token, out, loose)) return null;
                continue;
            }
            if (readCompoundDate(token, out, loose, original)) continue;
            return null;   // a field that is nothing this knows: not a date at all
        }

        if (sawMeridiem && out.time != null) out.time = applyMeridiem(out.time, meridiemPm);
        if (!assignLooseNumbers(out, loose)) return null;
        if (!settleYearAndDay(out, original)) return null;
        return out;
    }

    /** A clock reading, possibly with a displacement written straight onto its end. */
    private static boolean readClockToken(String token, Fields out, String original) {
        java.util.regex.Matcher m = CLOCK.matcher(token);
        if (!m.lookingAt() || m.start() != 0) return false;
        String rest = token.substring(m.end());
        if (!rest.isEmpty() && !readDisplacementToken(rest, out)) return false;
        if (out.time != null) return false;
        int hour = Integer.parseInt(m.group(1));
        int minute = Integer.parseInt(m.group(2));
        int second = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
        if (hour > 24 || minute > 59 || second > 60) throw fieldOutOfRange(original);
        long nanos = m.group(4) == null ? 0
                : new java.math.BigDecimal(m.group(4)).movePointRight(6)
                        .setScale(0, java.math.RoundingMode.HALF_EVEN).longValue() * 1000L;
        out.time = LocalTime.MIDNIGHT.plusHours(hour).plusMinutes(minute)
                .plusSeconds(second).plusNanos(nanos);
        return true;
    }

    private static boolean readDisplacementToken(String token, Fields out) {
        if (token.isEmpty() || (token.charAt(0) != '+' && token.charAt(0) != '-')) return false;
        java.util.regex.Matcher m = DISPLACEMENT.matcher(token);
        if (!m.matches()) return false;
        if (out.offset != null) return false;
        int seconds = Integer.parseInt(m.group(2)) * 3600
                + (m.group(3) == null ? 0 : Integer.parseInt(m.group(3))) * 60
                + (m.group(4) == null ? 0 : Integer.parseInt(m.group(4)));
        if (m.group(1).equals("-")) seconds = -seconds;
        if (seconds < -16 * 3600 || seconds > 16 * 3600) return false;
        out.offset = ZoneOffset.ofTotalSeconds(seconds);
        return true;
    }

    private static boolean readZoneName(String token, Fields out) {
        PgTimeZones.Abbrev abbrev = PgTimeZones.lookup(token);
        if (abbrev == null) return false;
        if (out.offset != null) return false;
        out.offset = ZoneOffset.ofTotalSeconds(abbrev.offsetSeconds);
        return true;
    }

    /**
     * A number standing on its own. Six or eight digits are a whole date written without
     * separators; three digits following a year already read are the day of that year; anything
     * else is a field whose meaning waits until the rest of the literal has been seen.
     */
    private static boolean readBareNumber(String token, Fields out, List<int[]> loose) {
        int value = parseOrOverflow(token);
        if (value < 0) return false;
        if (out.month == null && out.day == null && out.writtenYear == null) {
            if (token.length() == 8) return readCompact(token, out, 4);
            if (token.length() == 6) return readCompact(token, out, 2);
        }
        loose.add(new int[]{value, token.length(), out.month == null ? 1 : 0});
        return true;
    }

    /** {@code 19990108} and {@code 990108}: a year of the given width, then a month and a day. */
    private static boolean readCompact(String token, Fields out, int yearDigits) {
        int year = Integer.parseInt(token.substring(0, yearDigits));
        int month = Integer.parseInt(token.substring(yearDigits, yearDigits + 2));
        int day = Integer.parseInt(token.substring(yearDigits + 2));
        if (month < 1 || month > 12 || day < 1 || day > 31) return false;
        out.writtenYear = year;
        out.writtenYearDigits = yearDigits;
        out.month = month;
        out.day = day;
        return true;
    }

    /** A four-digit year, then the day counted from the first of it. */
    private static final java.util.regex.Pattern DAY_OF_YEAR =
            java.util.regex.Pattern.compile("(\\d{4})[-. ](\\d{3})");

    /**
     * {@code 1999.008}, {@code 1999-008} and {@code 1999 008}: the day of the year rather than of
     * a month. The count may run past the year's own end, and the date it names is then in the
     * next one — {@code '1999.366'} is the first of January 2000.
     */
    private static boolean readDayOfYear(String text, Fields out) {
        java.util.regex.Matcher m = DAY_OF_YEAR.matcher(text);
        if (!m.matches()) return false;
        int dayOfYear = Integer.parseInt(m.group(2));
        if (dayOfYear < 1 || dayOfYear > 366) return false;
        LocalDate resolved =
                LocalDate.of(Integer.parseInt(m.group(1)), 1, 1).plusDays(dayOfYear - 1L);
        out.year = resolved.getYear();
        out.month = resolved.getMonthValue();
        out.day = resolved.getDayOfMonth();
        return true;
    }

    /**
     * A date written as one field with separators inside it, as {@code 08-Jan-99} is.
     *
     * <p>This is the one place where position alone settles the numbers: whichever of them is
     * written first is the day unless it is four digits wide, and the other is the year. That is
     * what makes {@code '99-Jan-08'} a 99th day — a field out of range — where the same numbers
     * spaced apart are read differently.
     */
    private static boolean readCompoundDate(String token, Fields out, List<int[]> loose,
                                            String original) {
        if (out.writtenYear != null || out.day != null) return false;
        String[] parts = token.split("[-/.]");
        if (parts.length != 3) return false;
        Integer month = null;
        List<String> numbers = new ArrayList<>();
        for (String part : parts) {
            String lower = part.toLowerCase(Locale.ROOT);
            if (MONTHS.containsKey(lower)) {
                if (month != null) return false;
                month = MONTHS.get(lower);
            } else if (DIGITS.matcher(part).matches()) {
                numbers.add(part);
            } else {
                return false;
            }
        }
        if (out.month != null) return false;
        // Three numbers with the year written out in front need no field order to be read, so a
        // date of that shape is taken here; any other all-numeric date has a reading of its own
        // that the DateStyle decides, and this must not step in front of it.
        if (month == null) {
            // A month is written with one digit or two. Written wider it is read as a year, and a
            // date claiming two of those is no date: '2020-006-005' is refused where
            // '2020-06-005' is the fifth of June.
            if (numbers.size() != 3 || numbers.get(0).length() != 4
                    || numbers.get(1).length() > 2) {
                return false;
            }
            int month0 = parseOrOverflow(numbers.get(1));
            int day0 = parseOrOverflow(numbers.get(2));
            if (month0 < 1 || month0 > 12 || day0 < 1 || day0 > 31) return false;
            out.writtenYear = parseOrOverflow(numbers.get(0));
            out.writtenYearDigits = 4;
            out.month = month0;
            out.day = day0;
            loose.clear();
            return true;
        }
        if (numbers.size() != 2) return false;
        int first = parseOrOverflow(numbers.get(0));
        int second = parseOrOverflow(numbers.get(1));
        if (first < 0 || second < 0) return false;
        out.month = month;
        if (numbers.get(0).length() >= 4) {
            out.writtenYear = first;
            out.writtenYearDigits = numbers.get(0).length();
            out.day = second;
        } else {
            out.day = first;
            out.writtenYear = second;
            out.writtenYearDigits = numbers.get(1).length();
        }
        if (out.day < 1 || out.day > 31) throw fieldOutOfRange(original);
        loose.clear();
        return true;
    }

    /**
     * Settle the numbers whose meaning the literal left open.
     *
     * <p>With a month named, one number is the day and the other the year. A number written before
     * the month name can only be the day if it could be one — a wider number there is a year, and
     * a number that is neither is not part of a date PostgreSQL reads at all.
     */
    private static boolean assignLooseNumbers(Fields out, List<int[]> loose) {
        if (loose.isEmpty()) return true;
        if (out.month == null || loose.size() > 2) return false;
        for (int[] item : loose) {
            int value = item[0];
            int digits = item[1];
            boolean beforeMonth = item[2] == 1;
            if (out.day == null && (digits <= 2 || !beforeMonth)) {
                // A number in front of the month name is the day; one too large to be a day is
                // not the year either, and a literal reading that way is no date at all.
                if (beforeMonth && value > 31) return false;
                out.day = value;
            } else if (out.writtenYear == null) {
                out.writtenYear = value;
                out.writtenYearDigits = digits;
            } else {
                return false;
            }
        }
        return out.day != null && out.writtenYear != null;
    }

    /**
     * Turn the year as written into the year it names, and check the day against it.
     *
     * <p>Both wait until every field has been read because both depend on fields that may come
     * last: a two-digit year is widened to a century only in the common era, so the {@code BC} at
     * the end of {@code 'January 8, 99 BC'} is what keeps it the year 99; and how long a month is
     * depends on the year the month is in.
     */
    private static boolean settleYearAndDay(Fields out, String original) {
        if (out.writtenYear != null) {
            if (out.bc) {
                if (out.writtenYear < 1) throw fieldOutOfRange(original);
                out.year = 1 - out.writtenYear;
            } else {
                out.year = out.writtenYearDigits <= 2
                        ? widenTwoDigitYear(out.writtenYear) : out.writtenYear;
            }
        }
        if (out.day == null || out.year == null || out.month == null) return true;
        if (out.day < 1 || out.day > LocalDate.of(out.year, out.month, 1).lengthOfMonth()) {
            throw fieldOutOfRange(original);
        }
        return true;
    }

    /**
     * A year written with two digits belongs to the century that puts it within fifty years or so
     * of now: 70 through 99 are the 1900s and everything below 70 is the 2000s.
     */
    private static int widenTwoDigitYear(int year) {
        if (year >= 100) return year;
        return year < 70 ? year + 2000 : year + 1900;
    }

    private static LocalTime applyMeridiem(LocalTime time, boolean pm) {
        int hour = time.getHour();
        if (pm && hour < 12) return time.plusHours(12);
        if (!pm && hour == 12) return time.minusHours(12);
        return time;
    }

    /** The number a field of digits holds, or -1 when it holds more than a year could be. */
    private static int parseOrOverflow(String digits) {
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static MemgresException fieldOutOfRange(String original) {
        return new MemgresException(
                "date/time field value out of range: \"" + original + "\"", "22008");
    }
}
