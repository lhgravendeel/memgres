package com.memgres.engine;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL's date/time formatting templates, shared by to_char, to_timestamp and to_date.
 *
 * <p>The templates are a small language, not a {@link java.time.format.DateTimeFormatter} pattern:
 * keywords are looked up in a fixed table scanned in order so that {@code MON} wins over {@code M},
 * they carry prefix and postfix modifiers ({@code FM}, {@code TH}, {@code SP}), double-quoted runs
 * are copied out or skipped over, and on the reading side a separator matches any separator — or
 * nothing at all — unless {@code FX} pins the input to the template. Handing the template to Java
 * cannot express any of that, so both directions are written out here.
 */
final class DateTimeTemplate {

    private DateTimeTemplate() {
    }

    // ------------------------------------------------------------------ table

    /**
     * The keyword table, in PostgreSQL's order. The scan is a plain walk from the top, which
     * reproduces PostgreSQL's per-initial-letter search: longer keywords precede the shorter ones
     * they start with, and the two letter cases are separate entries because the case of the
     * keyword decides the case of the output.
     */
    private static final String[] KEYWORDS = {
            "A.D.", "A.M.", "AD", "AM",
            "B.C.", "BC",
            "CC",
            "DAY", "DDD", "DD", "DY", "Day", "Dy", "D",
            "FF1", "FF2", "FF3", "FF4", "FF5", "FF6", "FX",
            "HH24", "HH12", "HH",
            "IDDD", "ID", "IW", "IYYY", "IYY", "IY", "I",
            "J",
            "MI", "MM", "MONTH", "MON", "MS", "Month", "Mon",
            "OF",
            "P.M.", "PM",
            "Q",
            "RM",
            "SSSSS", "SSSS", "SS",
            "TZH", "TZM", "TZ",
            "US",
            "WW", "W",
            "Y,YYY", "YYYY", "YYY", "YY", "Y",
            "a.d.", "a.m.", "ad", "am",
            "b.c.", "bc",
            "cc",
            "day", "ddd", "dd", "dy", "d",
            "ff1", "ff2", "ff3", "ff4", "ff5", "ff6", "fx",
            "hh24", "hh12", "hh",
            "iddd", "id", "iw", "iyyy", "iyy", "iy", "i",
            "j",
            "mi", "mm", "month", "mon", "ms",
            "of",
            "p.m.", "pm",
            "q",
            "rm",
            "sssss", "ssss", "ss",
            "tzh", "tzm", "tz",
            "us",
            "ww", "w",
            "y,yyy", "yyyy", "yyy", "yy", "y",
    };

    /**
     * The keywords an interval cannot answer. An interval is a length, not a point, so it has no
     * weekday, no month name, no era and no zone; PostgreSQL refuses the whole template rather
     * than printing a field it has nothing to put in.
     */
    private static final java.util.Set<String> CALENDAR_ONLY = new java.util.HashSet<String>(
            java.util.Arrays.asList("A.D.", "AD", "B.C.", "BC", "DAY", "DY", "D", "ID",
                    "MONTH", "MON", "OF", "TZ", "TZH", "TZM"));

    /**
     * The keywords whose padding counts the sign, the way {@code %02d} of −1 is {@code -1}.
     * Every other numeric keyword pads the digits and writes the sign in front of them, so
     * {@code MM} of −1 is {@code -01} where {@code DD} of −1 is {@code -1}.
     */
    private static final java.util.Set<String> SIGN_INSIDE_WIDTH = new java.util.HashSet<String>(
            java.util.Arrays.asList("DD", "DDD", "IDDD"));

    /** Keywords whose output is digits: only these take a TH suffix and a fixed parse width. */
    private static final java.util.Set<String> DIGIT_KEYS = new java.util.HashSet<String>(
            java.util.Arrays.asList("CC", "DDD", "DD", "D", "HH24", "HH12", "HH", "IDDD", "ID",
                    "IW", "IYYY", "IYY", "IY", "I", "J", "MI", "MM", "MS", "Q", "SSSSS", "SSSS",
                    "SS", "TZM", "US", "WW", "W", "Y,YYY", "YYYY", "YYY", "YY", "Y"));

    private static final String[] MONTHS_FULL = {"January", "February", "March", "April", "May",
            "June", "July", "August", "September", "October", "November", "December"};
    private static final String[] MONTHS_SHORT = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    /** Day names indexed by PostgreSQL's day-of-week, which starts at Sunday. */
    private static final String[] DAYS_FULL = {"Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday"};
    private static final String[] DAYS_SHORT = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    private static final String[] ROMAN_MONTHS = {"I", "II", "III", "IV", "V", "VI", "VII",
            "VIII", "IX", "X", "XI", "XII"};

    private static final int CASE_UPPER = 0, CASE_CAP = 1, CASE_LOWER = 2;

    private static final String NOT_AN_ALLOWED_VALUE =
            "\n  Detail: The given value did not match any of the allowed values for this field.";

    // ------------------------------------------------------------- tokenizing

    /** One template element: either a keyword with its modifiers, or a literal character. */
    private static final class Node {
        final String key;
        final char ch;
        boolean fm;
        boolean tm;
        boolean th;
        boolean thUpper;

        Node(String key, char ch) {
            this.key = key;
            this.ch = ch;
        }

        boolean isAction() {
            return key != null;
        }
    }

    private static List<Node> parseFormat(String fmt) {
        List<Node> out = new ArrayList<Node>();
        int i = 0;
        while (i < fmt.length()) {
            boolean fm = false;
            boolean tm = false;
            // Prefix modifiers. TM asks for a localised name; the C locale memgres reports gives
            // the same English name, but a localised name is never blank-padded.
            int prefixLen = 0;
            while (i + 1 < fmt.length()) {
                String p = fmt.substring(i, i + 2);
                if (p.equals("FM") || p.equals("fm")) {
                    fm = true;
                    i += 2;
                    prefixLen += 2;
                } else if (p.equals("TM") || p.equals("tm")) {
                    tm = true;
                    i += 2;
                    prefixLen += 2;
                } else {
                    break;
                }
            }
            String kw = matchKeyword(fmt, i);
            if (kw != null) {
                Node n = new Node(kw, '\0');
                n.fm = fm;
                n.tm = tm;
                i += kw.length();
                if (i + 2 <= fmt.length()) {
                    String suf = fmt.substring(i, i + 2);
                    if (suf.equals("TH")) {
                        n.th = true;
                        n.thUpper = true;
                        i += 2;
                    } else if (suf.equals("th")) {
                        n.th = true;
                        i += 2;
                    } else if (suf.equals("SP")) {
                        // Spell mode: PostgreSQL accepts it and prints the digits anyway.
                        i += 2;
                    }
                }
                out.add(n);
                continue;
            }
            // A prefix modifier that turned out not to introduce a keyword is literal text.
            i -= prefixLen;
            char c = fmt.charAt(i);
            if (c == '"') {
                i++;
                while (i < fmt.length() && fmt.charAt(i) != '"') {
                    if (fmt.charAt(i) == '\\' && i + 1 < fmt.length()) i++;
                    out.add(new Node(null, fmt.charAt(i)));
                    i++;
                }
                if (i < fmt.length()) i++; // closing quote
                continue;
            }
            // Outside a quoted run a backslash is only special before a quote.
            if (c == '\\' && i + 1 < fmt.length() && fmt.charAt(i + 1) == '"') {
                i++;
                c = '"';
            }
            out.add(new Node(null, c));
            i++;
        }
        return out;
    }

    private static String matchKeyword(String fmt, int at) {
        for (int k = 0; k < KEYWORDS.length; k++) {
            String kw = KEYWORDS[k];
            if (fmt.startsWith(kw, at)) return kw;
        }
        return null;
    }

    private static String canon(String key) {
        return key.toUpperCase(java.util.Locale.ROOT);
    }

    private static int caseOf(String key) {
        if (key.equals(key.toLowerCase(java.util.Locale.ROOT))) return CASE_LOWER;
        if (key.equals(key.toUpperCase(java.util.Locale.ROOT))) return CASE_UPPER;
        return CASE_CAP;
    }

    // ---------------------------------------------------------------- to_char

    /** The broken-out fields PostgreSQL's template renderer works from. */
    private static final class Tm {
        int year;          // always positive: the year as an era names it
        boolean bc;
        int mon, mday, hour, min, sec, micros;
        int doy, dow, isoDow, isoWeek, isoYear, isoDoy, weekOfMonth, weekOfYear, quarter;
        long julian;
        boolean interval;
        ZoneOffset offset; // null when the value carries no zone
        java.time.Instant instant; // the instant behind an offset value, for its zone's name
    }

    static String toChar(Object source, String fmt) {
        if (fmt.isEmpty()) return null; // PostgreSQL answers NULL for an empty datetime template
        Tm tm = broken(source);
        List<Node> nodes = parseFormat(fmt);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            Node n = nodes.get(i);
            if (!n.isAction()) {
                sb.append(n.ch);
                continue;
            }
            if (tm.interval && CALENDAR_ONLY.contains(canon(n.key))) {
                throw new MemgresException("invalid format specification for an interval value"
                        + "\n  Hint: Intervals are not tied to specific calendar dates.", "22007");
            }
            String out = render(n, tm);
            if (out == null) continue;
            if (n.th && DIGIT_KEYS.contains(canon(n.key))) out = out + ordinal(out, n.thUpper);
            sb.append(out);
        }
        return sb.toString();
    }

    private static Tm broken(Object source) {
        Tm tm = new Tm();
        if (source instanceof LocalTime) {
            // There is no to_char over a time: the call reaches to_char(interval, text) through
            // the cast time carries, so a time is broken out as the length it stands for and no
            // calendar field of the template has anything to answer with.
            LocalTime lt = (LocalTime) source;
            source = new PgInterval(0, 0, lt.toNanoOfDay() / 1000L);
        }
        if (source instanceof PgInterval) {
            PgInterval iv = (PgInterval) source;
            tm.interval = true;
            tm.year = iv.getMonths() / 12;
            tm.mon = iv.getMonths() % 12;
            tm.mday = iv.getDays();
            long micros = iv.getMicroseconds();
            tm.hour = (int) (micros / 3_600_000_000L);
            tm.min = (int) ((micros / 60_000_000L) % 60);
            tm.sec = (int) ((micros / 1_000_000L) % 60);
            tm.micros = (int) (micros % 1_000_000L);
            // The calendar keys an interval still answers are computed on the fields as they
            // stand, which is a date of month nought and year nought. PostgreSQL runs its own
            // calendar arithmetic over them, so the same arithmetic is run here.
            tm.quarter = tm.mon == 0 ? 0 : (tm.mon - 1) / 3 + 1;
            tm.doy = 0;
            tm.weekOfMonth = (tm.mday - 1) / 7 + 1;
            tm.weekOfYear = (tm.doy - 1) / 7 + 1;
            tm.julian = julianDay(tm.year, tm.mon, tm.mday);
            tm.isoYear = isoYearOf(tm.year, tm.mon, tm.mday);
            tm.isoWeek = isoWeekOf(tm.year, tm.mon, tm.mday);
            tm.isoDow = 0;
            tm.isoDoy = (int) (julianDay(tm.year, tm.mon, tm.mday)
                    - isoWeekStart(tm.isoYear, 1) + 1);
            return tm;
        }
        LocalDateTime dt;
        if (source instanceof LocalDate) {
            dt = ((LocalDate) source).atStartOfDay();
        } else if (source instanceof LocalDateTime) {
            dt = (LocalDateTime) source;
        } else if (source instanceof OffsetDateTime) {
            // A timestamptz is rendered in the session's zone, the way PostgreSQL renders it.
            OffsetDateTime odt = ((OffsetDateTime) source).atZoneSameInstant(
                    TypeCoercion.sessionZone()).toOffsetDateTime();
            dt = odt.toLocalDateTime();
            tm.offset = odt.getOffset();
            tm.instant = odt.toInstant();
        } else {
            dt = TypeCoercion.toLocalDateTime(source);
        }
        int y = dt.getYear();
        tm.bc = y <= 0;
        tm.year = tm.bc ? 1 - y : y;
        tm.mon = dt.getMonthValue();
        tm.mday = dt.getDayOfMonth();
        tm.hour = dt.getHour();
        tm.min = dt.getMinute();
        tm.sec = dt.getSecond();
        tm.micros = dt.getNano() / 1000;
        tm.doy = dt.getDayOfYear();
        tm.isoDow = dt.getDayOfWeek().getValue();
        tm.dow = tm.isoDow % 7 + 1;
        tm.isoWeek = dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        tm.isoYear = dt.get(IsoFields.WEEK_BASED_YEAR);
        tm.isoDoy = (tm.isoWeek - 1) * 7 + tm.isoDow;
        tm.weekOfMonth = (tm.mday - 1) / 7 + 1;
        tm.weekOfYear = (tm.doy - 1) / 7 + 1;
        tm.quarter = (tm.mon - 1) / 3 + 1;
        tm.julian = dt.toLocalDate().getLong(JulianFields.JULIAN_DAY);
        return tm;
    }

    private static String render(Node n, Tm tm) {
        String key = n.key;
        boolean fm = n.fm;
        boolean bare = n.fm || n.tm; // a localised name is never blank-padded either
        int cs = caseOf(key);
        switch (canon(key)) {
            case "FX":
                return null;
            case "HH":
            case "HH12":
                return num((tm.hour + 11) % 12 + 1, 2, fm);
            case "HH24":
                return num(tm.hour, 2, fm);
            case "MI":
                return num(tm.min, 2, fm);
            case "SS":
                return num(tm.sec, 2, fm);
            case "SSSS":
            case "SSSSS":
                return Integer.toString(tm.hour * 3600 + tm.min * 60 + tm.sec);
            case "MS":
                return num(tm.micros / 1000, 3, false);
            case "US":
                return num(tm.micros, 6, false);
            case "FF1":
            case "FF2":
            case "FF3":
            case "FF4":
            case "FF5":
            case "FF6": {
                int digits = key.charAt(2) - '0';
                int scale = (int) Math.pow(10, 6 - digits);
                return num(tm.micros / scale, digits, false);
            }
            case "Y,YYY": {
                int y = tm.year;
                return Integer.toString(y / 1000) + "," + num(y % 1000, 3, false);
            }
            case "YYYY":
                return num(tm.year, 4, fm);
            case "YYY":
                return num(tm.year % 1000, 3, fm);
            case "YY":
                return num(tm.year % 100, 2, fm);
            case "Y":
                return num(tm.year % 10, 1, fm);
            case "IYYY":
                return numC(isoYear(tm), 4, fm);
            case "IYY":
                return numC(isoYear(tm) % 1000, 3, fm);
            case "IY":
                return numC(isoYear(tm) % 100, 2, fm);
            case "I":
                return numC(isoYear(tm) % 10, 1, fm);
            case "CC": {
                int cc;
                if (tm.interval) cc = tm.year / 100;
                else if (!tm.bc) cc = (tm.year - 1) / 100 + 1;
                else cc = -((tm.year - 1) / 100 + 1);
                return num(cc, 2, fm);
            }
            case "AD":
            case "A.D.":
            case "BC":
            case "B.C.": {
                boolean dots = key.indexOf('.') >= 0;
                String s = tm.bc ? (dots ? "B.C." : "BC") : (dots ? "A.D." : "AD");
                return cs == CASE_LOWER ? s.toLowerCase(java.util.Locale.ROOT) : s;
            }
            case "AM":
            case "A.M.":
            case "PM":
            case "P.M.": {
                boolean dots = key.indexOf('.') >= 0;
                boolean afternoon = tm.hour >= 12;
                String s = afternoon ? (dots ? "P.M." : "PM") : (dots ? "A.M." : "AM");
                return cs == CASE_LOWER ? s.toLowerCase(java.util.Locale.ROOT) : s;
            }
            case "MONTH":
                if (tm.mon < 1 || tm.mon > 12) return "";
                return name(MONTHS_FULL[tm.mon - 1], cs, bare, 9);
            case "MON":
                if (tm.mon < 1 || tm.mon > 12) return "";
                return name(MONTHS_SHORT[tm.mon - 1], cs, true, 0);
            case "MM":
                return num(tm.mon, 2, fm);
            case "DAY":
                if (tm.dow < 1 || tm.dow > 7) return "";
                return name(DAYS_FULL[tm.dow - 1], cs, bare, 9);
            case "DY":
                if (tm.dow < 1 || tm.dow > 7) return "";
                return name(DAYS_SHORT[tm.dow - 1], cs, true, 0);
            case "DDD":
                // An interval has no day of the year, so its day count answers for it.
                return numC(tm.interval ? tm.mday : tm.doy, 3, fm);
            case "IDDD":
                return numC(tm.isoDoy, 3, fm);
            case "DD":
                return numC(tm.mday, 2, fm);
            case "D":
                return Integer.toString(tm.dow);
            case "ID":
                return Integer.toString(tm.isoDow);
            case "W":
                return Integer.toString(tm.weekOfMonth);
            case "WW":
                return num(tm.weekOfYear, 2, fm);
            case "IW":
                return num(tm.isoWeek, 2, fm);
            case "Q":
                // An interval with no months has no quarter to be in.
                if (tm.interval && tm.mon == 0) return "";
                return Integer.toString(tm.quarter);
            case "RM": {
                if (tm.mon < 1 || tm.mon > 12) return "";
                String r = ROMAN_MONTHS[tm.mon - 1];
                if (cs == CASE_LOWER) r = r.toLowerCase(java.util.Locale.ROOT);
                return fm ? r : padRight(r, 4);
            }
            case "J":
                return Long.toString(tm.julian);
            case "TZ": {
                if (tm.offset == null || tm.instant == null) return "";
                String abbrev = PgTimeZones.sessionAbbreviationAt(tm.instant);
                return cs == CASE_LOWER ? abbrev.toLowerCase(java.util.Locale.ROOT) : abbrev;
            }
            case "TZH": {
                int secs = tm.offset == null ? 0 : tm.offset.getTotalSeconds();
                return (secs < 0 ? "-" : "+") + num(Math.abs(secs) / 3600, 2, false);
            }
            case "TZM": {
                int secs = tm.offset == null ? 0 : tm.offset.getTotalSeconds();
                return num((Math.abs(secs) % 3600) / 60, 2, false);
            }
            case "OF": {
                int secs = tm.offset == null ? 0 : tm.offset.getTotalSeconds();
                String s = (secs < 0 ? "-" : "+") + num(Math.abs(secs) / 3600, 2, fm);
                int mins = (Math.abs(secs) % 3600) / 60;
                return mins == 0 ? s : s + ":" + num(mins, 2, false);
            }
            default:
                return "";
        }
    }

    private static int isoYear(Tm tm) {
        if (tm.interval) return tm.isoYear;
        return tm.isoYear <= 0 ? 1 - tm.isoYear : tm.isoYear;
    }

    /**
     * The Julian day of a date, by PostgreSQL's own arithmetic — which takes a month of nought
     * as readily as it takes a real one, and is what an interval's J is computed with.
     */
    private static long julianDay(int year, int month, int day) {
        int y = year;
        int m = month;
        if (m > 2) {
            m += 1;
            y += 4800;
        } else {
            m += 13;
            y += 4799;
        }
        int century = y / 100;
        long julian = y * 365L - 32167L;
        julian += y / 4 - century + century / 4;
        julian += 7834L * m / 256 + day;
        return julian;
    }

    /** The weekday of a Julian day, counted from Sunday, the way PostgreSQL counts it. */
    private static long weekdayOf(long julian) {
        long day = (julian + 1) % 7;
        return day < 0 ? day + 7 : day;
    }

    /** The Julian day the given ISO week of the given ISO year begins on. */
    private static long isoWeekStart(int isoYear, int week) {
        long day4 = julianDay(isoYear, 1, 4);
        return (week - 1) * 7L + (day4 - weekdayOf(day4 - 1));
    }

    /** The ISO week number of a date, by the same arithmetic. */
    private static int isoWeekOf(int year, int month, int day) {
        long dayn = julianDay(year, month, day);
        long day4 = julianDay(year, 1, 4);
        long day0 = weekdayOf(day4 - 1);
        double weeks = (double) (dayn - (day4 - day0)) / 7 + 1;
        if (weeks >= 52) {
            day4 = julianDay(year + 1, 1, 4);
            day0 = weekdayOf(day4 - 1);
            if (dayn >= day4 - day0) weeks = (double) (dayn - (day4 - day0)) / 7 + 1;
        }
        if (weeks <= 0) {
            day4 = julianDay(year - 1, 1, 4);
            day0 = weekdayOf(day4 - 1);
            weeks = (double) (dayn - (day4 - day0)) / 7 + 1;
        }
        return (int) weeks;
    }

    /** The ISO week-numbering year of a date, by the same arithmetic. */
    private static int isoYearOf(int year, int month, int day) {
        long dayn = julianDay(year, month, day);
        long day4 = julianDay(year, 1, 4);
        long day0 = (day4 - 1 + 1) % 7;
        if (dayn < day4 - day0) {
            day4 = julianDay(year - 1, 1, 4);
            day0 = (day4 - 1 + 1) % 7;
            return year - 1;
        }
        long weeks = (dayn - (day4 - day0)) / 7;
        if (weeks >= 52) {
            day4 = julianDay(year + 1, 1, 4);
            day0 = (day4 - 1 + 1) % 7;
            if (dayn >= day4 - day0) return year + 1;
        }
        return year;
    }

    private static String name(String base, int cs, boolean fm, int width) {
        String s;
        if (cs == CASE_UPPER) s = base.toUpperCase(java.util.Locale.ROOT);
        else if (cs == CASE_LOWER) s = base.toLowerCase(java.util.Locale.ROOT);
        else s = base;
        return fm ? s : padRight(s, width);
    }

    private static String padRight(String s, int width) {
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < width) sb.append(' ');
        return sb.toString();
    }

    private static String num(int v, int width, boolean fm) {
        if (fm) return Integer.toString(v);
        String s = Integer.toString(Math.abs(v));
        StringBuilder sb = new StringBuilder();
        for (int i = s.length(); i < width; i++) sb.append('0');
        sb.append(s);
        return (v < 0 ? "-" : "") + sb;
    }

    /** The same, for the keywords whose field width has the sign inside it. */
    private static String numC(int v, int width, boolean fm) {
        if (fm) return Integer.toString(v);
        String s = Integer.toString(v);
        StringBuilder sb = new StringBuilder();
        int digits = v < 0 ? width - 1 : width;
        for (int i = s.length() - (v < 0 ? 1 : 0); i < digits; i++) sb.append('0');
        if (v < 0) return "-" + sb + s.substring(1);
        return sb + s;
    }

    /** The English ordinal for the digits already rendered — 11th and 12th, but 21st. */
    static String ordinal(String digits, boolean upper) {
        int len = digits.length();
        char last = len == 0 ? ' ' : digits.charAt(len - 1);
        if (last < '0' || last > '9') return "";
        if (digits.charAt(0) == '-') return "";
        if (len > 1 && digits.charAt(len - 2) == '1') last = 0;
        String s;
        switch (last) {
            case '1': s = "st"; break;
            case '2': s = "nd"; break;
            case '3': s = "rd"; break;
            default: s = "th"; break;
        }
        return upper ? s.toUpperCase(java.util.Locale.ROOT) : s;
    }

    // ------------------------------------------------------------------ parse

    /** Cursor over the input being read, so the field readers can share a position. */
    private static final class Cursor {
        final String s;
        int i;

        Cursor(String s) {
            this.s = s;
        }

        boolean has() {
            return i < s.length();
        }

        char peek() {
            return s.charAt(i);
        }

        void skipSpaces() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }

        String rest(int max) {
            int end = Math.min(s.length(), i + max);
            return s.substring(i, end);
        }
    }

    /** Everything a template can say about the value being read. */
    private static final class Fields {
        int year, cc, mon, mday, doy, hh, mi, ss, ms, us;
        int yysz;
        boolean bc, pm, clock12, hasYear, hasJulian;
        long julian;
        int tzHours, tzMinutes;
        boolean tzNegative, hasZone;
    }

    /** What a template read: the fields it named, and the zone it named if it named one. */
    static final class Read {
        final LocalDateTime local;
        /** The displacement the template read, or null when it read no zone at all. */
        final ZoneOffset offset;

        Read(LocalDateTime local, ZoneOffset offset) {
            this.local = local;
            this.offset = offset;
        }
    }

    static LocalDateTime parse(String input, String fmt) {
        return read(input, fmt).local;
    }

    static Read read(String input, String fmt) {
        List<Node> nodes = parseFormat(fmt);
        Cursor s = new Cursor(input);
        Fields f = new Fields();
        boolean fx = false;
        for (int idx = 0; idx < nodes.size(); idx++) {
            Node n = nodes.get(idx);
            boolean isFx = n.isAction() && canon(n.key).equals("FX");
            if (!fx && !isFx) s.skipSpaces();
            if (!n.isAction()) {
                if (!s.has()) continue;
                if (fx) {
                    s.i++;
                } else if (isSeparator(n.ch) || Character.isWhitespace(n.ch)) {
                    // Without FX a separator matches any separator, or nothing at all — except
                    // the sign in front of a displacement, which belongs to the field that reads
                    // it and would leave that field pointing the wrong way if it were eaten here.
                    boolean signOfDisplacement = (s.peek() == '+' || s.peek() == '-')
                            && readsDisplacement(nodes, idx);
                    if (!signOfDisplacement
                            && (isSeparator(s.peek()) || Character.isWhitespace(s.peek()))) {
                        s.i++;
                    }
                } else {
                    // A letter or digit in the template stands for one character of input.
                    s.i++;
                }
                continue;
            }
            if (isFx) {
                fx = true;
                continue;
            }
            readField(n, nodes, idx, s, f);
        }
        ZoneOffset offset = null;
        if (f.hasZone) {
            int secs = f.tzHours * 3600 + f.tzMinutes * 60;
            offset = ZoneOffset.ofTotalSeconds(f.tzNegative ? -secs : secs);
        }
        return new Read(assemble(f, input), offset);
    }

    /** Whether the next keyword in the template reads a displacement, sign and all. */
    private static boolean readsDisplacement(List<Node> nodes, int idx) {
        for (int i = idx + 1; i < nodes.size(); i++) {
            Node next = nodes.get(i);
            if (!next.isAction()) {
                if (Character.isWhitespace(next.ch)) continue;
                return false;
            }
            String key = canon(next.key);
            return key.equals("OF") || key.equals("TZH");
        }
        return false;
    }

    private static void readField(Node n, List<Node> nodes, int idx, Cursor s, Fields f) {
        String key = n.key;
        boolean slurp = n.fm || nextIsSeparator(nodes, idx);
        switch (canon(key)) {
            case "YYYY":
            case "IYYY": {
                f.year = readInt(s, 4, key, slurp)[0];
                f.yysz = 4;
                f.hasYear = true;
                break;
            }
            case "YYY":
            case "IYY": {
                int[] r = readInt(s, 3, key, slurp);
                f.year = r[1] < 4 ? adjustYear(r[0]) : r[0];
                f.yysz = 3;
                f.hasYear = true;
                break;
            }
            case "YY":
            case "IY": {
                int[] r = readInt(s, 2, key, slurp);
                f.year = r[1] < 4 ? adjustYear(r[0]) : r[0];
                f.yysz = 2;
                f.hasYear = true;
                break;
            }
            case "Y":
            case "I": {
                int[] r = readInt(s, 1, key, slurp);
                f.year = r[1] < 4 ? adjustYear(r[0]) : r[0];
                f.yysz = 1;
                f.hasYear = true;
                break;
            }
            case "Y,YYY": {
                int[] r = readInt(s, 5, key, true);
                f.year = r[0];
                f.yysz = 4;
                f.hasYear = true;
                break;
            }
            case "CC":
                f.cc = readInt(s, 2, key, slurp)[0];
                break;
            case "MM":
                f.mon = readInt(s, 2, key, slurp)[0];
                break;
            case "DD":
                f.mday = readInt(s, 2, key, slurp)[0];
                break;
            case "DDD":
            case "IDDD":
                f.doy = readInt(s, 3, key, slurp)[0];
                break;
            case "HH":
            case "HH12":
                f.hh = readInt(s, 2, key, slurp)[0];
                f.clock12 = true;
                break;
            case "HH24":
                f.hh = readInt(s, 2, key, slurp)[0];
                break;
            case "MI":
                f.mi = readInt(s, 2, key, slurp)[0];
                break;
            case "SS":
                f.ss = readInt(s, 2, key, slurp)[0];
                break;
            case "SSSS":
            case "SSSSS": {
                int secs = readInt(s, 5, key, slurp)[0];
                f.hh = secs / 3600;
                f.mi = (secs / 60) % 60;
                f.ss = secs % 60;
                break;
            }
            case "MS": {
                // The digits are a decimal fraction, so '3' is 300ms and '003' is 3ms.
                int[] r = readInt(s, 3, key, slurp);
                f.ms = r[0] * (r[1] == 1 ? 100 : r[1] == 2 ? 10 : 1);
                break;
            }
            case "US": {
                int[] r = readInt(s, 6, key, slurp);
                f.us = r[0] * scaleForFraction(r[1], 6);
                break;
            }
            case "FF1":
            case "FF2":
            case "FF3":
            case "FF4":
            case "FF5":
            case "FF6": {
                int digits = key.charAt(2) - '0';
                int[] r = readInt(s, digits, key, slurp);
                f.us = r[0] * scaleForFraction(r[1], 6);
                break;
            }
            case "J":
                f.julian = readInt(s, 7, key, true)[0];
                f.hasJulian = true;
                break;
            case "Q":
                readInt(s, 1, key, slurp);
                break;
            case "W":
                readInt(s, 1, key, slurp);
                break;
            case "WW":
            case "IW":
                readInt(s, 2, key, slurp);
                break;
            case "D":
            case "ID":
                readInt(s, 1, key, slurp);
                break;
            case "TZH":
                if (s.has() && s.peek() == '-') {
                    f.tzNegative = true;
                    s.i++;
                } else if (s.has() && s.peek() == '+') {
                    s.i++;
                }
                f.tzHours = readInt(s, 2, key, slurp)[0];
                f.hasZone = true;
                break;
            case "TZM":
                f.tzMinutes = readInt(s, 2, key, slurp)[0];
                f.hasZone = true;
                break;
            case "MONTH":
                f.mon = seqSearch(s, MONTHS_FULL, key) + 1;
                break;
            case "MON":
                f.mon = seqSearch(s, MONTHS_SHORT, key) + 1;
                break;
            case "DAY":
                seqSearch(s, DAYS_FULL, key);
                break;
            case "DY":
                seqSearch(s, DAYS_SHORT, key);
                break;
            case "RM":
                f.mon = seqSearch(s, ROMAN_MONTHS, key) + 1;
                break;
            case "AM":
            case "A.M.":
            case "PM":
            case "P.M.": {
                boolean dots = key.indexOf('.') >= 0;
                String[] names = dots ? new String[]{"A.M.", "P.M."} : new String[]{"AM", "PM"};
                f.pm = seqSearchLiteral(s, names, key) == 1;
                break;
            }
            case "AD":
            case "A.D.":
            case "BC":
            case "B.C.": {
                boolean dots = key.indexOf('.') >= 0;
                String[] names = dots ? new String[]{"A.D.", "B.C."} : new String[]{"AD", "BC"};
                f.bc = seqSearchLiteral(s, names, key) == 1;
                break;
            }
            case "OF": {
                if (s.has() && (s.peek() == '+' || s.peek() == '-')) {
                    f.tzNegative = s.peek() == '-';
                    s.i++;
                }
                f.tzHours = readInt(s, 2, key, false)[0];
                if (s.has() && s.peek() == ':') {
                    s.i++;
                    f.tzMinutes = readInt(s, 2, key, false)[0];
                }
                f.hasZone = true;
                break;
            }
            case "TZ": {
                // Only an abbreviation stands here: a zone's own name is not one of the names
                // this field reads, which is what PostgreSQL says when it is handed one.
                int end = s.i;
                while (end < s.s.length() && !Character.isWhitespace(s.s.charAt(end))) end++;
                String named = s.s.substring(s.i, end);
                ZoneOffset read = PgTimeZones.offsetOf(named);
                if (read == null) {
                    throw new MemgresException("invalid value \"" + named + "\" for \"" + key
                            + "\"\n  Detail: Time zone abbreviation is not recognized.", "22007");
                }
                int secs = read.getTotalSeconds();
                f.tzNegative = secs < 0;
                f.tzHours = Math.abs(secs) / 3600;
                f.tzMinutes = (Math.abs(secs) % 3600) / 60;
                f.hasZone = true;
                s.i = end;
                break;
            }
            default:
                break;
        }
    }

    /** Digits read as a decimal fraction scale up to the unit's own width. */
    private static int scaleForFraction(int digitsRead, int digitsInUnit) {
        int scale = 1;
        for (int i = digitsRead; i < digitsInUnit; i++) scale *= 10;
        return scale;
    }

    /**
     * Whether the next template element stops a run of digits. When it does, a numeric field
     * takes as many digits as the input offers rather than exactly its own width.
     */
    private static boolean nextIsSeparator(List<Node> nodes, int idx) {
        if (idx + 1 >= nodes.size()) return true;
        Node next = nodes.get(idx + 1);
        if (next.isAction()) return !DIGIT_KEYS.contains(canon(next.key));
        return !(next.ch >= '0' && next.ch <= '9');
    }

    /** Reads one integer; answers its value and the number of characters it consumed. */
    private static int[] readInt(Cursor s, int len, String key, boolean slurp) {
        s.skipSpaces();
        // A template longer than its input is not an error: the fields left over keep their
        // defaults, which is how to_date('2020-06', 'YYYY-MM-DD') lands on the first of the month.
        if (!s.has()) return new int[]{0, 0};
        String window = slurp ? s.s.substring(s.i) : s.rest(len);
        int p = 0;
        boolean neg = false;
        if (p < window.length() && (window.charAt(p) == '+' || window.charAt(p) == '-')) {
            neg = window.charAt(p) == '-';
            p++;
        }
        int start = p;
        long value = 0;
        while (p < window.length() && Character.isDigit(window.charAt(p))) {
            value = value * 10 + (window.charAt(p) - '0');
            if (value > Integer.MAX_VALUE) value = Integer.MAX_VALUE;
            p++;
        }
        if (p == start) {
            throw new MemgresException("invalid value \"" + s.rest(len) + "\" for \""
                    + key + "\"\n  Detail: Value must be an integer.", "22007");
        }
        s.i += p;
        return new int[]{(int) (neg ? -value : value), p - start};
    }

    /** PostgreSQL pulls short years toward the present: 95 is 1995 and 5 is 2005. */
    private static int adjustYear(int year) {
        if (year < 0) return year;
        if (year < 70) return year + 2000;
        if (year < 100) return year + 1900;
        if (year < 520) return year + 2000;
        if (year < 1000) return year + 1000;
        return year;
    }

    /** Matches the leading letters against a name table, answering the zero-based index. */
    private static int seqSearch(Cursor s, String[] names, String key) {
        int end = s.i;
        while (end < s.s.length() && Character.isLetter(s.s.charAt(end))) end++;
        String run = s.s.substring(s.i, end);
        for (int k = 0; k < names.length; k++) {
            if (run.length() >= names[k].length()
                    && run.regionMatches(true, 0, names[k], 0, names[k].length())) {
                s.i += names[k].length();
                return k;
            }
        }
        throw new MemgresException("invalid value \"" + run + "\" for \"" + key + "\""
                + NOT_AN_ALLOWED_VALUE, "22007");
    }

    /** The same, for tables whose entries carry punctuation such as {@code A.M.}. */
    private static int seqSearchLiteral(Cursor s, String[] names, String key) {
        for (int k = 0; k < names.length; k++) {
            if (s.s.regionMatches(true, s.i, names[k], 0, names[k].length())) {
                s.i += names[k].length();
                return k;
            }
        }
        throw new MemgresException("invalid value \"" + s.rest(names[0].length())
                + "\" for \"" + key + "\"" + NOT_AN_ALLOWED_VALUE, "22007");
    }

    private static boolean isSeparator(char c) {
        return c > 0x20 && c < 0x7F
                && !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9');
    }

    private static LocalDateTime assemble(Fields f, String input) {
        int year;
        int mon = f.mon == 0 ? 1 : f.mon;
        int mday = f.mday == 0 ? 1 : f.mday;
        if (f.cc != 0 && f.yysz <= 2) {
            // A century number is one ahead of its hundreds — the 21st century starts in 2001 —
            // and a year that lands on the hundred belongs to the top of the century, not the foot.
            int within = 1;
            if (f.hasYear) {
                within = Math.abs(f.year) % 100;
                if (within == 0) within = 100;
            }
            int cc = f.bc ? -f.cc : f.cc;
            if (cc >= 0) year = (cc - 1) * 100 + within;
            else year = cc * 100 - (within - 1);
        } else if (f.year != 0) {
            year = f.bc ? -f.year : f.year;
            if (year < 0) year++; // there is no year zero, so 1 BC is the proleptic year 0
        } else {
            year = 0; // no year in the template at all: the proleptic year zero, 1 BC
        }

        int hour = f.hh;
        if (f.clock12) {
            if (hour < 1 || hour > 12) {
                throw new MemgresException("hour \"" + hour + "\" is invalid for the 12-hour clock"
                        + "\n  Hint: Use the 24-hour clock, or give an hour between 1 and 12.",
                        "22007");
            }
            if (f.pm && hour < 12) hour += 12;
            else if (!f.pm && hour == 12) hour = 0;
        }
        long micros = f.ms * 1000L + f.us;

        LocalDate date;
        try {
            if (f.hasJulian) {
                date = LocalDate.ofEpochDay(f.julian - 2440588L);
            } else if (f.doy != 0 && (f.mon == 0 || f.mday == 0)) {
                // A day of the year is a day of that year: 366 is a day only in a leap year, and
                // 400 is a day in none. Added on regardless, the date ran into the year after.
                int daysInYear = java.time.Year.of(year).length();
                if (f.doy < 1 || f.doy > daysInYear) {
                    throw new MemgresException("date/time field value out of range: \""
                            + input + "\"", "22008");
                }
                date = LocalDate.of(year, 1, 1).plusDays(f.doy - 1L);
            } else {
                date = LocalDate.of(year, mon, mday);
            }
            return LocalDateTime.of(date, LocalTime.of(hour, f.mi, f.ss))
                    .plusNanos(micros * 1000L);
        } catch (RuntimeException e) {
            throw new MemgresException(
                    "date/time field value out of range: \"" + input + "\"", "22008");
        }
    }
}
