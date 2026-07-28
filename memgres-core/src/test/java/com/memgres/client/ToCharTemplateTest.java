package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * to_char, to_timestamp and to_date share PostgreSQL's formatting templates, which are a language
 * of their own rather than a {@code java.time} pattern. Handing the template to Java made the
 * reading direction ignore it altogether — a separator had to match exactly, MS meant absolute
 * milliseconds rather than a decimal fraction, a two-digit year was refused instead of being pulled
 * toward the present, and CC or a month name surfaced "Unknown pattern letter" as an internal
 * error. Every expected value here was measured against PostgreSQL 18.
 */
class ToCharTemplateTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) { st.execute("SET TIME ZONE 'UTC'"); }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertScalar(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertFails(String state, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
        assertEquals(state, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ------------------------------------------------------------------ to_char, temporal

    @Test
    void theTimeOfDayPatterns() throws Exception {
        assertScalar("03 03 15 04 05 54245 54245", "SELECT to_char(timestamp"
                + " '2020-06-06 15:04:05.123456', 'HH HH12 HH24 MI SS SSSS SSSSS')");
        assertScalar("123 123456 1 12 123 1234 12345 123456", "SELECT to_char(timestamp"
                + " '2020-06-06 15:04:05.123456', 'MS US FF1 FF2 FF3 FF4 FF5 FF6')");
        assertScalar("5 5 4 5 18245", "SELECT to_char(timestamp '2020-06-06 05:04:05',"
                + " 'FMHH FMHH24 FMMI FMSS FMSSSS')");
        assertScalar("PM PM P.M. P.M. pm pm p.m. p.m.", "SELECT to_char(timestamp"
                + " '2020-06-06 15:04:05', 'AM PM A.M. P.M. am pm a.m. p.m.')");
        assertScalar("AM AM A.M. A.M. am am a.m. a.m.", "SELECT to_char(timestamp"
                + " '2020-06-06 05:04:05', 'AM PM A.M. P.M. am pm a.m. p.m.')");
        // midnight and noon are both twelve on the 12-hour clock
        assertScalar("12 AM", "SELECT to_char(timestamp '2020-01-01 00:00:00', 'HH12 AM')");
        assertScalar("12 PM", "SELECT to_char(timestamp '2020-01-01 12:00:00', 'HH12 AM')");
    }

    @Test
    void theYearMonthAndDayPatterns() throws Exception {
        assertScalar("2,020 2020 020 20 0 21", "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                + " 'Y,YYY YYYY YYY YY Y CC')");
        assertScalar("2,020 2020 20 20 0 21", "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                + " 'FMY,YYY FMYYYY FMYYY FMYY FMY FMCC')");
        assertScalar("2020 020 20 0 23 160 6", "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                + " 'IYYY IYY IY I IW IDDD ID')");
        assertScalar("JUNE      June      june      JUN Jun jun 06",
                "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                        + " 'MONTH Month month MON Mon mon MM')");
        assertScalar("JUNE June june JUN Jun jun 6",
                "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                        + " 'FMMONTH FMMonth FMmonth FMMON FMMon FMmon FMMM')");
        assertScalar("SATURDAY  Saturday  saturday  SAT Sat sat 158 06 7",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'DAY Day day DY Dy dy DDD DD D')");
        assertScalar("SATURDAY Saturday saturday SAT Sat sat 158 6 7",
                "SELECT to_char(timestamp '2020-06-06 15:04:05',"
                        + " 'FMDAY FMDay FMday FMDY FMDy FMdy FMDDD FMDD FMD')");
        assertScalar("1 23 2 VI   vi   2459007",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'W WW Q RM rm J')");
        assertScalar("1 23 2 VI vi 2459007",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'FMW FMWW FMQ FMRM FMrm FMJ')");
        assertScalar("VIII viii VIII",
                "SELECT to_char(timestamp '2020-08-06 15:04:05', 'RM rm FMRM')");
        assertScalar("XII ", "SELECT to_char(timestamp '2020-12-06 15:04:05', 'RM')");
        assertScalar("AD AD A.D. A.D. ad ad a.d. a.d.",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'AD BC A.D. B.C. ad bc a.d. b.c.')");
        // the ISO week counters disagree with the calendar ones at the turn of the year
        assertScalar("1 01 001 01 003 3 4",
                "SELECT to_char(timestamp '2020-01-01 00:00:00', 'W WW DDD IW IDDD ID D')");
        assertScalar("5 53 366 53 2020 4 5",
                "SELECT to_char(timestamp '2020-12-31 00:00:00', 'W WW DDD IW IYYY ID D')");
        assertScalar("53 2020 369",
                "SELECT to_char(timestamp '2021-01-01 00:00:00', 'IW IYYY IDDD')");
    }

    @Test
    void theSuffixesAndQuotedRuns() throws Exception {
        assertScalar("06th 06th 06TH 77th",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'DDth ddth DDTH Ddth')");
        assertScalar("01st 01ST 1st",
                "SELECT to_char(timestamp '2020-06-01 15:04:05', 'DDth DDTH FMDDth')");
        assertScalar("02nd", "SELECT to_char(timestamp '2020-06-02 15:04:05', 'DDth')");
        assertScalar("03rd", "SELECT to_char(timestamp '2020-06-03 15:04:05', 'DDth')");
        // the teens are all "th", but twenty-one is "st" again
        assertScalar("11th", "SELECT to_char(timestamp '2020-06-11 15:04:05', 'DDth')");
        assertScalar("12th", "SELECT to_char(timestamp '2020-06-12 15:04:05', 'DDth')");
        assertScalar("13th", "SELECT to_char(timestamp '2020-06-13 15:04:05', 'DDth')");
        assertScalar("21st", "SELECT to_char(timestamp '2020-06-21 15:04:05', 'DDth')");
        assertScalar("2020th 06th 15th",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'YYYYth MMth HH24th')");
        // only one postfix is consumed, so the SP in MMTHSP is plain text
        assertScalar("06 06TH 06THSP",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'MMSP MMSPTH MMTHSP')");
        assertScalar("June Saturday JUN",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'TMMonth TMDay TMMON')");
        assertScalar("Hello 2020",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', '\"Hello\" YYYY')");
        assertScalar("15:04:05 o'clock",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'HH24:MI:SS \"o''clock\"')");
        assertScalar("ZZZ", "SELECT to_char(timestamp '2020-06-06 15:04:05', 'ZZZ')");
        // a keyword only matches in its own letter case, and what is left over is text
        assertScalar("MOn Junt JUNt",
                "SELECT to_char(timestamp '2020-06-06 15:04:05', 'MOn Mont MONt')");
    }

    @Test
    void theOtherTemporalTypes() throws Exception {
        assertScalar("2020-06-06", "SELECT to_char(timestamp '2020-06-06 15:04:05', 'YYYY-MM-DD')");
        assertScalar("2020-06-06 00:00:00",
                "SELECT to_char(date '2020-06-06', 'YYYY-MM-DD HH24:MI:SS')");
        assertScalar("15:04:05", "SELECT to_char(time '15:04:05', 'HH24:MI:SS')");
        // a timestamptz is rendered in the session zone, not in the offset the literal carried
        assertScalar("2020-06-06 13:04:05",
                "SELECT to_char(timestamptz '2020-06-06 15:04:05+02', 'YYYY-MM-DD HH24:MI:SS')");
        assertScalar("+00 00 +00",
                "SELECT to_char(timestamptz '2020-06-06 15:04:05+02', 'TZH TZM OF')");
        // an interval has no calendar, so each field stands on its own
        assertScalar("04:00:00 03 00 0000",
                "SELECT to_char(interval '3 days 4 hours', 'HH24:MI:SS DD MM YYYY')");
        assertScalar("0001 02 00 00",
                "SELECT to_char(interval '1 year 2 months', 'YYYY MM DD HH24')");
    }

    @Test
    void anEmptyOrNullTemplateAnswersNull() throws Exception {
        assertNull(scalar("SELECT to_char(timestamp '2020-06-06 15:04:05', '')"));
        assertNull(scalar("SELECT to_char(timestamp '2020-06-06 15:04:05', NULL)"));
        assertNull(scalar("SELECT to_char(NULL::timestamp, 'YYYY')"));
        assertNull(scalar("SELECT to_char(NULL::numeric, '999')"));
        assertNull(scalar("SELECT to_char(485, NULL)"));
        assertNull(scalar("SELECT to_date('2020-06-06', NULL)"));
        assertNull(scalar("SELECT to_date(NULL, 'YYYY-MM-DD')"));
        // a numeric template is the exception: an empty one gives an empty string
        assertScalar("", "SELECT to_char(1.5, '')");
        assertScalar("", "SELECT to_char(1.5::float8, '')");
    }

    // ------------------------------------------------------------- to_timestamp / to_date

    @Test
    void aSeparatorMatchesLooselyWithoutFx() throws Exception {
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp(' 2000    JUN', 'YYYY MON')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000 - JUN', 'YYYY-MON')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000JUN', 'YYYY///MON')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000/JUN', 'YYYY MON')");
        // FX pins the template to the input one character at a time
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000/JUN', 'FXYYYY MON')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000 JUN', 'FXYYYY MON')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000-JUN', 'FXYYYY MON')");
        assertFails("22007", "invalid value \"\" for \"MON\"",
                "SELECT to_timestamp('2000  JUN', 'FXYYYY MON')");
        assertScalar("2020-06-06", "SELECT to_date('2020-06-06', 'FXYYYY-MM-DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020-6-6', 'FXYYYY-MM-DD')");
    }

    @Test
    void msUsAndFfReadADecimalFraction() throws Exception {
        // MS is a fraction of a second, so a single 3 is 300ms rather than 3ms
        assertScalar("0001-01-01 00:00:12.3+00 BC", "SELECT to_timestamp('12.3', 'SS.MS')");
        assertScalar("0001-01-01 00:00:12.03+00 BC", "SELECT to_timestamp('12.03', 'SS.MS')");
        assertScalar("0001-01-01 00:00:12.003+00 BC", "SELECT to_timestamp('12.003', 'SS.MS')");
        assertScalar("0001-01-01 00:00:12.003+00 BC", "SELECT to_timestamp('12.0003', 'SS.MS')");
        assertScalar("0001-01-01 15:12:02.02123+00 BC",
                "SELECT to_timestamp('15:12:02.020.001230', 'HH24:MI:SS.MS.US')");
        assertScalar("0001-01-01 00:00:01.2+00 BC", "SELECT to_timestamp('1.2', 'SS.US')");
        assertScalar("0001-01-01 00:00:01.2+00 BC", "SELECT to_timestamp('1.2', 'SS.FF3')");
        assertScalar("2020-06-06 15:04:05.123+00",
                "SELECT to_timestamp('2020-06-06 15:04:05.123', 'YYYY-MM-DD HH24:MI:SS.FF3')");
    }

    @Test
    void aShortYearIsPulledTowardThePresent() throws Exception {
        assertScalar("1995-01-01", "SELECT to_date('95', 'YY')");
        assertScalar("1995-01-01", "SELECT to_date('095', 'YYY')");
        assertScalar("2005-01-01", "SELECT to_date('5', 'Y')");
        assertScalar("2020-01-01", "SELECT to_date('20', 'YY')");
        assertScalar("2069-01-01", "SELECT to_date('69', 'YY')");
        assertScalar("1970-01-01", "SELECT to_date('70', 'YY')");
        assertScalar("2000-01-01", "SELECT to_date('00', 'YY')");
        assertScalar("2001-01-01", "SELECT to_date('1', 'YYY')");
        assertScalar("2000-01-01", "SELECT to_date('0', 'Y')");
        assertScalar("1995-01-01", "SELECT to_date('995', 'YYY')");
        assertScalar("2519-01-01", "SELECT to_date('519', 'YYY')");
        assertScalar("1520-01-01", "SELECT to_date('520', 'YYY')");
        // four digits are taken at face value whichever template reads them
        assertScalar("1995-01-01", "SELECT to_date('1995', 'YYYY')");
        assertScalar("0095-01-01", "SELECT to_date('95', 'YYYY')");
        assertScalar("1995-01-01", "SELECT to_date('1995', 'YYY')");
    }

    @Test
    void centuriesAndWideYears() throws Exception {
        assertScalar("2020-06-06", "SELECT to_date('20 2020 06 06', 'CC YYYY MM DD')");
        assertScalar("2095-06-06", "SELECT to_date('21 95 06 06', 'CC YY MM DD')");
        assertScalar("2100-06-06", "SELECT to_date('21 00 06 06', 'CC YY MM DD')");
        assertScalar("1895-06-06", "SELECT to_date('19 95 06 06', 'CC YY MM DD')");
        // a century on its own starts at its first year, which is 1901 for the twentieth
        assertScalar("1901-06-06", "SELECT to_date('20 06 06', 'CC MM DD')");
        assertScalar("2001-06-06", "SELECT to_date('21 06 06', 'CC MM DD')");
        assertScalar("0001-06-06", "SELECT to_date('1 06 06', 'CC MM DD')");
        assertScalar("20000-11-30", "SELECT to_date('20000-1130', 'YYYY-MMDD')");
        assertScalar("20000-11-30", "SELECT to_date('20000Nov30', 'YYYYMonDD')");
    }

    @Test
    void quotedRunsAndLettersSkipInput() throws Exception {
        assertScalar("2020-06-06", "SELECT to_date('2020XX06XX06', 'YYYY\"XX\"MM\"XX\"DD')");
        // the quoted characters need not be the ones in the input, only as many of them
        assertScalar("2020-06-06", "SELECT to_date('2020ab06cd06', 'YYYY\"XX\"MM\"XX\"DD')");
        assertScalar("2020-01-06", "SELECT to_date('2020ab0cd06', 'YYYY\"XX\"MM\"XX\"DD')");
        assertScalar("2000-06-01 00:00:00+00", "SELECT to_timestamp('2000y6m1d', 'yyyytMMtDDt')");
        assertScalar("2000-06-01 00:00:00+00",
                "SELECT to_timestamp('2000y6m1d', 'yyyy\"y\"MM\"m\"DD\"d\"')");
    }

    @Test
    void theOrdinaryShapesKeepWorking() throws Exception {
        assertScalar("2020-06-06", "SELECT to_date('2020-06-06', 'YYYY-MM-DD')");
        assertScalar("2020-06-06", "SELECT to_date('06/06/2020', 'MM/DD/YYYY')");
        assertScalar("2020-06-06", "SELECT to_date('20200606', 'YYYYMMDD')");
        assertScalar("2020-06-06", "SELECT to_date('2020-6-6', 'YYYY-MM-DD')");
        assertScalar("2020-06-06", "SELECT to_date('  2020-6-6', 'YYYY-MM-DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020-06-06extra', 'YYYY-MM-DD')");
        // a field the input runs out of keeps its default rather than failing
        assertScalar("2020-06-01", "SELECT to_date('2020-06', 'YYYY-MM-DD')");
        assertScalar("2020-01-01", "SELECT to_date('2020-00-01', 'YYYY-MM-DD')");
        assertScalar("2020-01-01", "SELECT to_date('2020', 'YYYY')");
        assertScalar("2020-06-06", "SELECT to_date('2020 Jun 06', 'YYYY Mon DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 JUN 06', 'YYYY Mon DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 jun 06', 'YYYY MON DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 June 06', 'YYYY Month DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 JUNE 06', 'YYYY MONTH DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 june 06', 'YYYY month DD')");
        // a day name is read and then ignored: the date comes from the numeric fields
        assertScalar("2020-06-06", "SELECT to_date('2020 Saturday 06 06', 'YYYY Day MM DD')");
        assertScalar("2020-06-06", "SELECT to_date('2020 Sat 06 06', 'YYYY Dy MM DD')");
        assertScalar("2020-06-08", "SELECT to_date('2020-160', 'YYYY-DDD')");
        assertScalar("2020-12-31", "SELECT to_date('2020-366', 'YYYY-DDD')");
        assertScalar("2020-01-01", "SELECT to_date('2020-000', 'YYYY-DDD')");
        assertScalar("1999-01-08", "SELECT to_date('2451187', 'J')");
        assertScalar("2020-01-01 00:00:00+00", "SELECT to_timestamp('2020', 'YYYY')");
        assertScalar("2020-06-06 15:04:05+00",
                "SELECT to_timestamp('2020-06-06 15:04:05', 'YYYY-MM-DD HH24:MI:SS')");
        assertScalar("2020-06-06 15:04:05+00",
                "SELECT to_timestamp('2020-06-06 03:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM')");
        assertScalar("2020-06-06 15:04:05+00",
                "SELECT to_timestamp('2020-06-06 03:04:05 pm', 'YYYY-MM-DD HH:MI:SS pm')");
        assertScalar("2020-06-06 03:04:05+00",
                "SELECT to_timestamp('2020-06-06 03:04:05 AM', 'YYYY-MM-DD HH12:MI:SS AM')");
        assertScalar("2020-06-06 00:04:05+00",
                "SELECT to_timestamp('2020-06-06 12:04:05 AM', 'YYYY-MM-DD HH12:MI:SS AM')");
        assertScalar("2020-06-06 12:04:05+00",
                "SELECT to_timestamp('2020-06-06 12:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM')");
        assertScalar("t", "SELECT to_date('2020-06-06', 'YYYY-MM-DD') = date '2020-06-06'");
        // to_timestamp of a number is seconds since the epoch, untouched by any of this
        assertScalar("1970-01-01 00:00:00+00", "SELECT to_timestamp(0)");
        assertScalar("2001-09-09 01:46:40+00", "SELECT to_timestamp(1000000000)");
    }

    @Test
    void erasBeforeTheCommonEra() throws Exception {
        // there is no year zero, so an unset year is the first year before the common era
        assertScalar("0001-01-01 BC", "SELECT to_date('', 'YYYY')");
        assertScalar("0001-06-06 BC", "SELECT to_date('0000-06-06', 'YYYY-MM-DD')");
        assertScalar("2020-06-06 BC", "SELECT to_date('2020-06-06 BC', 'YYYY-MM-DD BC')");
        assertScalar("2020-06-06", "SELECT to_date('2020-06-06 AD', 'YYYY-MM-DD BC')");
    }

    @Test
    void whatTheReaderRefuses() {
        assertFails("22007", "invalid value \"abc\" for \"YYYY\"",
                "SELECT to_date('abc', 'YYYY')");
        assertFails("22007", "invalid value \"Xyz\" for \"Mon\"",
                "SELECT to_date('2020 Xyz 06', 'YYYY Mon DD')");
        // a short month name is not a full one
        assertFails("22007", "invalid value \"Jun\" for \"Month\"",
                "SELECT to_date('2020 Jun 06', 'YYYY Month DD')");
        assertFails("22008", "date/time field value out of range: \"2020606\"",
                "SELECT to_date('2020606', 'YYYYMMDD')");
        assertFails("22008", "date/time field value out of range: \"2020-13-01\"",
                "SELECT to_date('2020-13-01', 'YYYY-MM-DD')");
        assertFails("22008", "date/time field value out of range: \"2020-02-30\"",
                "SELECT to_date('2020-02-30', 'YYYY-MM-DD')");
        assertFails("22008", "date/time field value out of range: \"2020-06-31\"",
                "SELECT to_date('2020-06-31', 'YYYY-MM-DD')");
        assertFails("22008", "date/time field value out of range: \"Feb 30 2020\"",
                "SELECT to_date('Feb 30 2020', 'Mon DD YYYY')");
        assertFails("22007", "hour \"13\" is invalid for the 12-hour clock",
                "SELECT to_timestamp('2020-06-06 13:04:05 PM', 'YYYY-MM-DD HH12:MI:SS AM')");
    }

    // ------------------------------------------------------------------ to_char, numeric

    @Test
    void digitsPaddingAndOverflow() throws Exception {
        assertScalar(" 485", "SELECT to_char(485, '999')");
        assertScalar("-485", "SELECT to_char(-485, '999')");
        assertScalar(" 4 8 5", "SELECT to_char(485, '9 9 9')");
        assertScalar(" 1,485", "SELECT to_char(1485, '9,999')");
        // a group separator ahead of every digit prints as a blank
        assertScalar("   148", "SELECT to_char(148, '9,999')");
        assertScalar(" 148.500", "SELECT to_char(148.5, '999.999')");
        assertScalar("148.5", "SELECT to_char(148.5, 'FM999.999')");
        // a 0 in the fraction is asked for, so fill mode keeps it
        assertScalar("148.500", "SELECT to_char(148.5, 'FM999.990')");
        // a value wider than its field fills the digit positions rather than widening
        assertScalar(" ###", "SELECT to_char(12345, '999')");
        assertScalar("-###", "SELECT to_char(-12345, '999')");
        assertScalar("###", "SELECT to_char(12345, 'FM999')");
        assertScalar(" ##.##", "SELECT to_char(485, '99.99')");
        assertScalar(" .#", "SELECT to_char(0.5, '.9')");
        assertScalar(" 1234", "SELECT to_char(1234, '9999')");
        assertScalar("-1234", "SELECT to_char(-1234, '9999')");
        assertScalar("  4.85", "SELECT to_char(4.85, '99.99')");
        assertScalar(" 1234567", "SELECT to_char(1234567, '9999999')");
        assertScalar("     123.46", "SELECT to_char(123.456, '9999999.99')");
        assertScalar("1234.568", "SELECT to_char(1234.5678, 'FM9999.999')");
        assertScalar("-485", "SELECT to_char(-485, 'FM999')");
    }

    @Test
    void zeroFillAndTheLeadingZeroOfAFraction() throws Exception {
        assertScalar(" 0012", "SELECT to_char(12, '0000')");
        assertScalar("0012", "SELECT to_char(12, 'FM0000')");
        assertScalar("   012", "SELECT to_char(12, '99099')");
        assertScalar("  0012", "SELECT to_char(12, '90009')");
        assertScalar("-0012", "SELECT to_char(-12, '0000')");
        assertScalar(" 0", "SELECT to_char(0, '9')");
        assertScalar("  0", "SELECT to_char(0, '99')");
        assertScalar(" 0", "SELECT to_char(0, '0')");
        assertScalar("0", "SELECT to_char(0, 'FM9')");
        assertScalar(" 0", "SELECT to_char(0, 'B9')");
        assertScalar("   .00", "SELECT to_char(0, 'B99.99')");
        // the lone leading zero of a fraction-only value prints as a blank, and the sign follows it
        assertScalar("  .0", "SELECT to_char(0.0, '9.9')");
        assertScalar(" 0.0", "SELECT to_char(0.0, '0.9')");
        assertScalar("  .1", "SELECT to_char(0.1, '9.9')");
        assertScalar(" -.1", "SELECT to_char(-0.1, '9.9')");
        assertScalar("-.1", "SELECT to_char(-0.1, 'FM9.9')");
        assertScalar(".1", "SELECT to_char(0.1, 'FM9.9')");
        assertScalar("    .000", "SELECT to_char(0, '999.999')");
        assertScalar("0.", "SELECT to_char(0, 'FM999.999')");
        assertScalar("  .00001", "SELECT to_char(1e-5::numeric, '9.99999')");
        // a decimal point with nothing significant after it is dropped
        assertScalar(" 485", "SELECT to_char(485, '999.')");
        assertScalar("485", "SELECT to_char(485, 'FM999.')");
    }

    @Test
    void theDigitShiftTheExponentRomanAndOrdinals() throws Exception {
        assertScalar(" 14850", "SELECT to_char(148.5, '999V99')");
        assertScalar(" ####", "SELECT to_char(485, '9V999')");
        assertScalar("  485", "SELECT to_char(0.485, '9V999')");
        assertScalar("49", "SELECT to_char(0.0485, 'FM9V999')");
        assertScalar(" 12000", "SELECT to_char(12, '99V999')");
        assertScalar(" 125", "SELECT to_char(12.45, '99V9')");
        assertScalar("-125", "SELECT to_char(-12.45, '99V9')");
        assertScalar(" 4.86e-04", "SELECT to_char(0.0004859, '9.99EEEE')");
        assertScalar(" 1.23e+03", "SELECT to_char(1234.5, '9.99EEEE')");
        assertScalar(" 0.00e+00", "SELECT to_char(0, '9.99EEEE')");
        assertScalar("-1.2e+03", "SELECT to_char(-1234.5, '9.9EEEE')");
        assertScalar(" 1e+03", "SELECT to_char(1234.5, '99EEEE')");
        assertScalar("        CDLXXXV", "SELECT to_char(485, 'RN')");
        assertScalar("        cdlxxxv", "SELECT to_char(485, 'rn')");
        assertScalar("CDLXXXV", "SELECT to_char(485, 'FMRN')");
        assertScalar("cdlxxxv", "SELECT to_char(485, 'FMrn')");
        assertScalar("              I", "SELECT to_char(1, 'RN')");
        assertScalar("      MMMCMXCIX", "SELECT to_char(3999, 'RN')");
        // roman numerals run from I to MMMCMXCIX and nothing else can be written
        assertScalar("###############", "SELECT to_char(0, 'RN')");
        assertScalar("###############", "SELECT to_char(4000, 'RN')");
        assertScalar("###############", "SELECT to_char(-5, 'RN')");
        assertScalar("V", "SELECT to_char(5.2, 'FMRN')");
        assertScalar("VI", "SELECT to_char(5.6, 'FMRN')");
        assertScalar(" 481st", "SELECT to_char(481, '999th')");
        assertScalar(" 482nd", "SELECT to_char(482, '999th')");
        assertScalar(" 483rd", "SELECT to_char(483, '999th')");
        assertScalar(" 411TH", "SELECT to_char(411, '999TH')");
        assertScalar(" 485TH", "SELECT to_char(485, '999TH')");
        // a negative number takes no ordinal at all
        assertScalar("-481", "SELECT to_char(-481, '999th')");
    }

    @Test
    void whereTheSignGoes() throws Exception {
        assertScalar("+485", "SELECT to_char(485, 'S999')");
        assertScalar("-485", "SELECT to_char(-485, 'S999')");
        // an S after every digit position is a trailing sign
        assertScalar("485+", "SELECT to_char(485, '999S')");
        assertScalar("485-", "SELECT to_char(-485, '999S')");
        assertScalar("-485", "SELECT to_char(-485, '99S9')");
        assertScalar("+485", "SELECT to_char(485, 'FMS999')");
        assertScalar("485-", "SELECT to_char(-485, 'FM999S')");
        assertScalar(" 485", "SELECT to_char(485, 'MI999')");
        assertScalar("-485", "SELECT to_char(-485, 'MI999')");
        assertScalar("485 ", "SELECT to_char(485, '999MI')");
        assertScalar("485-", "SELECT to_char(-485, '999MI')");
        assertScalar("485", "SELECT to_char(485, 'FM999MI')");
        // PL does not take the place of the ordinary sign, but MI and SG do
        assertScalar("+ 485", "SELECT to_char(485, 'PL999')");
        assertScalar(" -485", "SELECT to_char(-485, 'PL999')");
        assertScalar(" 485+", "SELECT to_char(485, '999PL')");
        assertScalar("-485 ", "SELECT to_char(-485, '999PL')");
        assertScalar("+485", "SELECT to_char(485, 'SG999')");
        assertScalar("-485", "SELECT to_char(-485, 'SG999')");
        assertScalar("485+", "SELECT to_char(485, '999SG')");
        assertScalar("485-", "SELECT to_char(-485, '999SG')");
        assertScalar("4-85", "SELECT to_char(-485, '9SG99')");
        assertScalar(" 485 ", "SELECT to_char(485, '999PR')");
        assertScalar("<485>", "SELECT to_char(-485, '999PR')");
        assertScalar("<485>", "SELECT to_char(-485, 'FM999PR')");
        assertScalar("485", "SELECT to_char(485, 'FM999PR')");
        // MI and PL may share a template, each writing at its own position
        assertScalar(" 2+", "SELECT to_char(1.5, 'MI9PL')");
        assertScalar("-2 ", "SELECT to_char(-1.5, 'MI9PL')");
        assertScalar("+2 ", "SELECT to_char(1.5, 'PL9MI')");
        assertScalar(" 2-", "SELECT to_char(-1.5, 'PL9MI')");
        assertScalar("+2 ", "SELECT to_char(1.5, 'SG9MI')");
        assertScalar("-2-", "SELECT to_char(-1.5, 'SG9MI')");
    }

    @Test
    void literalsRoundingAndTheSourceType() throws Exception {
        assertScalar("Good number: 485", "SELECT to_char(485, '\"Good number:\"999')");
        assertScalar("Pre: 485 Post: .800",
                "SELECT to_char(485.8, '\"Pre:\"999\" Post:\" .999')");
        assertScalar(" 485xyz", "SELECT to_char(485, '999xyz')");
        assertScalar("xyz 485", "SELECT to_char(485, 'xyz999')");
        assertScalar("a", "SELECT to_char(1.5, 'abc')");
        assertScalar(" 2", "SELECT to_char(1.5, '9SP')");
        // a numeric rounds half away from zero, a float half to even
        assertScalar(" 1", "SELECT to_char(0.5, '9')");
        assertScalar(" 2", "SELECT to_char(1.5, '9')");
        assertScalar(" 3", "SELECT to_char(2.5, '9')");
        assertScalar("-3", "SELECT to_char(-2.5, '9')");
        assertScalar(" 0", "SELECT to_char(0.5::float8, '9')");
        assertScalar(" 2", "SELECT to_char(2.5::float8, '9')");
        assertScalar(" 485", "SELECT to_char(485::int, '999')");
        assertScalar(" 485", "SELECT to_char(485::bigint, '999')");
        assertScalar(" 485.5", "SELECT to_char(485.5::float8, '999.9')");
        assertScalar("  1.50", "SELECT to_char(1.5::float4, '99.99')");
    }

    @Test
    void theNumericTemplatesThatAreRejected() {
        assertFails("42601", "cannot use \"V\" and decimal point together",
                "SELECT to_char(1.5, '99.9V99')");
        assertFails("42601", "multiple decimal points", "SELECT to_char(1.5, '9.9.9')");
        assertFails("42601", "cannot use \"S\" twice", "SELECT to_char(1.5, 'S9S9')");
        assertFails("42601", "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                "SELECT to_char(1.5, 'S9PR')");
        assertFails("42601", "cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together",
                "SELECT to_char(1.5, '9MI9PR')");
        assertFails("42601", "\"9\" must be ahead of \"PR\"", "SELECT to_char(1.5, '9PR9')");
        assertFails("42601", "\"9\" must be ahead of \"PR\"", "SELECT to_char(1.5, 'PR9')");
        assertFails("42601", "cannot use \"S\" and \"MI\" together", "SELECT to_char(1.5, 'S9MI')");
        assertFails("42601", "cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together",
                "SELECT to_char(1.5, 'MI9S')");
        assertFails("42601", "\"EEEE\" is incompatible with other formats",
                "SELECT to_char(1234.5, 'FM9.99EEEE')");
        assertFails("42601", "\"EEEE\" is incompatible with other formats",
                "SELECT to_char(1234.5, 'S9.99EEEE')");
        assertFails("42601", "\"EEEE\" must be the last pattern used",
                "SELECT to_char(1234.5, '9.99EEEEV9')");
    }
}
