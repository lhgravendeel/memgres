package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A template is a language, and a zone is a place.
 *
 * <p>A date/time template is not a {@code java.time} pattern. Its keywords are looked up in a
 * table of its own, so {@code MON} wins over {@code M} and the case of the keyword decides the
 * case of the output. A field's width counts the sign for some keywords and not for others, so
 * {@code DD} of minus one day is {@code -1} where {@code MM} of minus one month is {@code -01}.
 * And an interval — a length, not a point — is refused the keywords that ask about the calendar,
 * while the ones it does answer are computed by PostgreSQL's own calendar arithmetic over fields
 * that are not a real date at all. Reading is the same language backwards, and a template that
 * names a displacement reads the value against it rather than against the session.
 *
 * <p>A zone is a second subject. A name is looked up in PostgreSQL's own abbreviation table
 * before the zone database is asked, so {@code CET} is the fixed hour that table says it is and
 * not the zone of that name with its summer time. A bare number is read the POSIX way round,
 * where west of Greenwich is positive. And the day a zone puts its clocks back is twenty-five
 * hours long for everything that counts days: adding an interval of days, walking a series, and
 * reading a clock that came round twice.
 */
class DateTimeTemplateAndZoneTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        exec("SET TimeZone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    // ---- the template as a language ------------------------------------------------------

    /** Every keyword the calendar half of the language has, in each of its spellings. */
    @Test
    void aTemplateKeywordIsLookedUpInItsOwnTable() throws SQLException {
        assertEquals("2 3 24 167", one("SELECT to_char(date '2020-06-15', 'Q W WW DDD')"));
        assertEquals("MONDAY    Monday    monday   ",
                one("SELECT to_char(date '2020-06-15', 'DAY Day day')"));
        assertEquals("MON Mon mon", one("SELECT to_char(date '2020-06-15', 'DY Dy dy')"));
        assertEquals("JUNE      June      june     ",
                one("SELECT to_char(date '2020-06-15', 'MONTH Month month')"));
        assertEquals("VI   vi  ", one("SELECT to_char(date '2020-06-15', 'RM rm')"));
        assertEquals("2,020 020 20 0", one("SELECT to_char(date '2020-06-15', 'Y,YYY YYY YY Y')"));
        assertEquals("2020 020 20 0", one("SELECT to_char(date '2020-06-15', 'IYYY IYY IY I')"));
        assertEquals("21 2459016", one("SELECT to_char(date '2020-06-15', 'CC J')"));
    }

    /** A localised name is never blank-padded, and FM takes the padding off the rest. */
    @Test
    void aNameIsPaddedUnlessTheTemplateSaysNot() throws SQLException {
        assertEquals("Monday June", one("SELECT to_char(date '2020-06-15', 'TMDay TMMonth')"));
        assertEquals("June the 15th", one("SELECT to_char(date '2020-06-15', 'FMMonth \"the\" DDth')"));
        assertEquals("15thth", one("SELECT to_char(date '2020-06-15', 'DDthth')"));
    }

    /** The half of the day, and every width of a fraction of a second. */
    @Test
    void theClockHalfOfTheLanguage() throws SQLException {
        assertEquals("01:45 PM", one("SELECT to_char(timestamp '2020-06-15 13:45:00', 'HH12:MI AM')"));
        assertEquals("01:45 P.M.",
                one("SELECT to_char(timestamp '2020-06-15 13:45:00', 'HH12:MI P.M.')"));
        assertEquals("49500 00 123 123456 1 123 123456",
                one("SELECT to_char(timestamp '2020-06-15 13:45:00.123456',"
                        + " 'SSSS SS MS US FF1 FF3 FF6')"));
    }

    /** An empty date/time template answers with nothing at all, which is NULL. */
    @Test
    void anEmptyTemplateAnswersWithNull() throws SQLException {
        assertEquals("true", one("SELECT (to_char(date '2020-06-15', '') IS NULL)::text"));
    }

    // ---- an interval is a length ---------------------------------------------------------

    /**
     * An interval has no weekday, no month name, no era and no zone, so a template asking for
     * one of those is refused outright rather than printing a field with nothing in it.
     */
    @Test
    void anIntervalRefusesTheKeywordsThatAskAboutTheCalendar() throws SQLException {
        for (String key : new String[]{"AD", "A.D.", "BC", "B.C.", "DAY", "Day", "day",
                "DY", "Dy", "D", "ID", "MONTH", "Month", "month", "MON", "Mon",
                "OF", "TZ", "TZH", "TZM"}) {
            String sql = "SELECT to_char(interval '1 day 2 hours', '" + key + "')";
            assertEquals("22007", stateOf(sql), sql);
            assertTrue(messageOf(sql).contains("invalid format specification for an interval value"),
                    sql);
        }
    }

    /**
     * The rest it does answer, by running PostgreSQL's calendar arithmetic over fields that are
     * not a date: month nought of year nought, which has a Julian day and an ISO year of its own.
     */
    @Test
    void anIntervalAnswersTheRestFromTheFieldsItHas() throws SQLException {
        assertEquals("1721029", one("SELECT to_char(interval '1 day 2 hours', 'J')"));
        assertEquals("-001", one("SELECT to_char(interval '1 day 2 hours', 'IYYY')"));
        assertEquals("-1", one("SELECT to_char(interval '1 day 2 hours', 'I')"));
        assertEquals("332", one("SELECT to_char(interval '1 day 2 hours', 'IDDD')"));
        assertEquals("48", one("SELECT to_char(interval '1 day 2 hours', 'IW')"));
        assertEquals("01", one("SELECT to_char(interval '1 day 2 hours', 'WW')"));
        assertEquals("1", one("SELECT to_char(interval '1 day 2 hours', 'W')"));
        assertEquals("00", one("SELECT to_char(interval '1 day 2 hours', 'CC')"));
        assertEquals("AM", one("SELECT to_char(interval '1 day 2 hours', 'AM')"));
        // An interval with no months is in no quarter, so the field prints nothing.
        assertEquals("", one("SELECT to_char(interval '1 day 2 hours', 'Q')"));
        assertEquals("2", one("SELECT to_char(interval '4 mons', 'Q')"));
    }

    /** DDD and DD both answer with an interval's days, and neither is cut to its width. */
    @Test
    void anIntervalsDaysAreItsDaysHoweverManyThereAre() throws SQLException {
        assertEquals("003", one("SELECT to_char(interval '3 days', 'DDD')"));
        assertEquals("400 400", one("SELECT to_char(interval '400 days', 'DDD DD')"));
        assertEquals("0001 02 03", one("SELECT to_char(interval '1 year 2 mons 3 days',"
                + " 'YYYY MM DD')"));
    }

    /**
     * The day fields count the sign inside their width and the rest write it in front of theirs,
     * so minus one day is {@code -1} under DD and minus one month is {@code -01} under MM.
     */
    @Test
    void aFieldsWidthCountsTheSignOrDoesNot() throws SQLException {
        assertEquals("-1", one("SELECT to_char(interval '-1 day', 'DD')"));
        assertEquals("-01", one("SELECT to_char(interval '-1 day', 'DDD')"));
        assertEquals("-01", one("SELECT to_char(interval '-1 mon', 'MM')"));
        assertEquals("-01", one("SELECT to_char(interval '-1 hour', 'HH24')"));
        assertEquals("-0001 -001 -01 -1",
                one("SELECT to_char(interval '-1 year', 'YYYY YYY YY Y')"));
        assertEquals("-01 -500 -500000",
                one("SELECT to_char(interval '-1.5 sec', 'SS MS US')"));
        // SSSS is a plain count of seconds and carries no width at all.
        assertEquals("-1", one("SELECT to_char(interval '-1 sec', 'SSSS')"));
    }

    /**
     * There is no to_char over a time: the call reaches the interval form through the cast the
     * type carries, so the calendar fields have nothing to answer with.
     */
    @Test
    void aTimeReachesToCharAsTheLengthItStandsFor() throws SQLException {
        assertEquals("12:34:56", one("SELECT to_char(time '12:34:56', 'HH24:MI:SS')"));
        assertEquals("500 500000 500 45296",
                one("SELECT to_char(time '12:34:56.5', 'MS US FF3 SSSS')"));
        // Its calendar fields are the interval's, which are nought.
        assertEquals("0000-00-00 12:34:56",
                one("SELECT to_char(time '12:34:56', 'YYYY-MM-DD HH24:MI:SS')"));
        assertEquals("22007", stateOf("SELECT to_char(time '12:34:56', 'DAY')"));
        // And no to_char over a timetz at all.
        assertEquals("42883", stateOf("SELECT to_char(timetz '12:34:56+05:30', 'HH24')"));
    }

    // ---- reading -------------------------------------------------------------------------

    /** The reading side takes the same keywords, in the same table order. */
    @Test
    void theSameLanguageReadsAValueBack() throws SQLException {
        assertEquals("2020-06-15 13:45:00+00",
                one("SELECT to_timestamp('2020-06-15 13:45', 'YYYY-MM-DD HH24:MI')::text"));
        assertEquals("2020-06-15 00:00:00+00",
                one("SELECT to_timestamp('15-Jun-2020', 'DD-Mon-YYYY')::text"));
        assertEquals("2020-04-09 00:00:00+00",
                one("SELECT to_timestamp('2020 100', 'YYYY DDD')::text"));
        assertEquals("2020-06-15 00:00:00+00",
                one("SELECT to_timestamp('20 6 15', 'YY MM DD')::text"));
        assertEquals("2000-01-01", one("SELECT to_date('2451545', 'J')::text"));
        assertEquals("2020-06-15 13:45:00.123+00",
                one("SELECT to_timestamp('2020-06-15 13:45:00.123',"
                        + " 'YYYY-MM-DD HH24:MI:SS.MS')::text"));
        // The twelve-hour clock takes an hour it has, and thirteen is not one of them.
        assertEquals("22007", stateOf("SELECT to_timestamp('13:45 PM', 'HH12:MI PM')"));
    }

    /** Without FX a separator matches any separator, or nothing at all. */
    @Test
    void aSeparatorIsLooseUnlessTheTemplateIsPinned() throws SQLException {
        assertEquals("2020-06-15", one("SELECT to_date('  2020-06-15', 'YYYY-MM-DD')::text"));
        assertEquals("2020-06-15", one("SELECT to_date('2020/06/15', 'YYYY-MM-DD')::text"));
        assertEquals("2020-06-15", one("SELECT to_date('20200615', 'YYYYMMDD')::text"));
        assertEquals("2020-06-15", one("SELECT to_timestamp('2020-6-15', 'FXYYYY-MM-DD')::text")
                .substring(0, 10));
    }

    /** A template with no year at all lands on the year before the first, which is 1 BC. */
    @Test
    void aTemplateWithNoYearLandsOnTheYearBeforeTheFirst() throws SQLException {
        assertEquals("0001-01-01 00:00:00+00 BC",
                one("SELECT to_timestamp('01 01', 'MM DD')::text"));
    }

    /**
     * A template that names a displacement reads the value against it. Only an abbreviation
     * stands under TZ: a zone's own name is not one of the names that field reads.
     */
    @Test
    void aTemplateThatNamesADisplacementReadsAgainstIt() throws SQLException {
        assertEquals("2020-06-14 18:30:00+00",
                one("SELECT to_timestamp('2020-06-15 +05:30', 'YYYY-MM-DD OF')::text"));
        assertEquals("2020-06-15 07:00:00+00",
                one("SELECT to_timestamp('2020-06-15 -07', 'YYYY-MM-DD OF')::text"));
        assertEquals("2020-06-14 18:30:00+00",
                one("SELECT to_timestamp('2020-06-15 05 30', 'YYYY-MM-DD TZH TZM')::text"));
        assertEquals("2020-06-15 05:30:00+00",
                one("SELECT to_timestamp('2020-06-15 -05 30', 'YYYY-MM-DD TZH TZM')::text"));
        assertEquals("2020-06-15 05:00:00+00",
                one("SELECT to_timestamp('2020-06-15 EST', 'YYYY-MM-DD TZ')::text"));
        assertEquals("22007",
                stateOf("SELECT to_timestamp('2020-06-15 America/New_York', 'YYYY-MM-DD TZ')"));
        // A date has no time of day for a displacement to move, so it reads it and drops it.
        assertEquals("2020-06-15", one("SELECT to_date('2020-06-15 +05:30', 'YYYY-MM-DD OF')::text"));
    }

    /** These three answer with a date/time type, not with the text of one. */
    @Test
    void theReadingFunctionsAnswerWithTheTypeTheyAreDeclaredWith() throws SQLException {
        assertEquals("timestamp with time zone", one("SELECT pg_typeof(to_timestamp(1))::text"));
        assertEquals("timestamp with time zone",
                one("SELECT pg_typeof(to_timestamp('2020','YYYY'))::text"));
        assertEquals("date", one("SELECT pg_typeof(to_date('2020','YYYY'))::text"));
        assertEquals("numeric", one("SELECT pg_typeof(to_number('1','9'))::text"));
    }

    /**
     * The seconds of the epoch are a float8 and the fraction is part of the instant. The count is
     * taken to microseconds from PostgreSQL's own epoch, which is where the ties are settled.
     */
    @Test
    void theSecondsOfTheEpochCarryTheirFraction() throws SQLException {
        assertEquals("2009-02-13 23:31:30.123456+00",
                one("SELECT to_timestamp(1234567890.123456)::text"));
        assertEquals("1970-01-01 00:00:00.5+00", one("SELECT to_timestamp(0.5)::text"));
        assertEquals("1969-12-31 23:59:59.5+00", one("SELECT to_timestamp(-0.5)::text"));
        assertEquals("1970-01-01 00:00:01+00", one("SELECT to_timestamp(1.0000005)::text"));
        assertEquals("1970-01-01 00:00:01.000002+00", one("SELECT to_timestamp(1.0000015)::text"));
        assertEquals("1970-01-01 00:00:00+00", one("SELECT to_timestamp(0.0000005)::text"));
    }

    /** An instant too far out is out of range, and the seconds are quoted back as C writes them. */
    @Test
    void anInstantTooFarOutIsOutOfRange() throws SQLException {
        assertEquals("22008", stateOf("SELECT to_timestamp(1e20)"));
        assertTrue(messageOf("SELECT to_timestamp(1e20)").contains("\"1e+20\""));
        assertTrue(messageOf("SELECT to_timestamp(123456789012345.0)").contains("\"1.23457e+14\""));
        assertEquals("22008", stateOf("SELECT to_timestamp(9223372036854775807)"));
    }

    // ---- a zone is a place ---------------------------------------------------------------

    /**
     * PostgreSQL's abbreviation table is asked first, so CET is the fixed hour the table says
     * rather than the zone of that name observing summer time.
     */
    @Test
    void anAbbreviationIsLookedUpBeforeTheZoneDatabase() throws SQLException {
        assertEquals("2020-01-01 17:00:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'EST')::text"));
        assertEquals("2020-01-01 17:00:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'est')::text"));
        assertEquals("2020-07-01 11:00:00+00",
                one("SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'CET')::text"));
        assertEquals("2020-07-01 11:00:00+00",
                one("SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'MET')::text"));
        assertEquals("2020-07-01 10:00:00+00",
                one("SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'IST')::text"));
        // A name the table does not carry is left to the zone database, and keeps its rules.
        assertEquals("2020-07-01 16:00:00+00",
                one("SELECT (timestamp '2020-07-01 12:00' AT TIME ZONE 'EST5EDT')::text"));
    }

    /** Written as a number a zone is POSIX, where west of Greenwich is the positive direction. */
    @Test
    void aNumberedZoneIsReadThePosixWayRound() throws SQLException {
        assertEquals("2020-01-01 17:00:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '+05')::text"));
        assertEquals("2020-01-01 06:30:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '-05:30')::text"));
        assertEquals("2020-01-01 17:30:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE '05:30')::text"));
        assertEquals("2020-01-01 17:00:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE 'UTC+5')::text"));
        // An interval is not POSIX: it names the displacement it says it names.
        assertEquals("2020-01-01 06:30:00+00",
                one("SELECT (timestamp '2020-01-01 12:00:00' AT TIME ZONE INTERVAL '5:30')::text"));
    }

    /** What may stand for a zone is what the two-argument timezone() takes, and nothing else. */
    @Test
    void whatMayStandForAZone() throws SQLException {
        assertEquals("22023",
                stateOf("SELECT timestamp '2020-01-01 12:00:00' AT TIME ZONE INTERVAL '1 day'"));
        assertEquals("42883", stateOf("SELECT timestamp '2020-01-01 12:00:00' AT TIME ZONE 5"));
        assertEquals("22023", stateOf("SELECT timestamp '2020-01-01 12:00:00' AT TIME ZONE ''"));
        // A zone that is nothing names no zone, and a conversion into no zone is nothing.
        assertEquals("true",
                one("SELECT (timezone(NULL, timestamp '2020-01-01 12:00:00') IS NULL)::text"));
    }

    /** The function spelling takes the same operands as the operator. */
    @Test
    void theFunctionSpellingTakesTheSameOperands() throws SQLException {
        assertEquals("2020-01-01 07:00:00+00",
                one("SELECT timezone(interval '5 hours', timestamp '2020-01-01 12:00:00')::text"));
        assertEquals("2020-01-01 17:00:00+00",
                one("SELECT timezone('EST', timestamp '2020-01-01 12:00:00')::text"));
    }

    /** A time takes the session's displacement first, and a date is an instant at midnight. */
    @Test
    void aTimeAndADateGoThroughTheOperatorToo() throws SQLException {
        assertEquals("07:00:00-05", one("SELECT (time '12:00:00' AT TIME ZONE 'EST')::text"));
        assertEquals("07:00:00-05", one("SELECT (timetz '12:00:00+00' AT TIME ZONE 'EST')::text"));
        assertEquals("time with time zone",
                one("SELECT pg_typeof(time '12:00:00' AT TIME ZONE 'EST')::text"));
        assertEquals("2019-12-31 19:00:00",
                one("SELECT (date '2020-01-01' AT TIME ZONE 'EST')::text"));
    }

    /** The clock reading an hour puts back came round twice, and the second time is standard. */
    @Test
    void anAmbiguousReadingIsTheSecondOfTheTwo() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            assertEquals("2020-11-01 01:30:00-05",
                    one("SELECT (timestamp '2020-11-01 01:30:00'"
                            + " AT TIME ZONE 'America/New_York')::text"));
            // A reading in the hour a zone skips is the same instant either way round.
            assertEquals("2020-03-08 03:30:00-04",
                    one("SELECT (timestamp '2020-03-08 02:30:00'"
                            + " AT TIME ZONE 'America/New_York')::text"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    /**
     * A day is a calendar length and the time of day survives it, so a day added across the end
     * of summer time is twenty-five hours. Hours are elapsed time and are twenty-four of them.
     */
    @Test
    void aDayIsACalendarDayAndAnHourIsAnHour() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            assertEquals("2020-03-08 12:00:00-04",
                    one("SELECT (timestamptz '2020-03-07 12:00:00-05' + interval '1 day')::text"));
            assertEquals("2020-03-08 13:00:00-04",
                    one("SELECT (timestamptz '2020-03-07 12:00:00-05' + interval '24 hours')::text"));
            assertEquals("2020-11-02 00:00:00-05",
                    one("SELECT (timestamptz '2020-11-01 00:00:00-04' + interval '1 day')::text"));
            assertEquals("2020-11-01 01:30:00-05",
                    one("SELECT (timestamptz '2020-10-31 01:30:00-04' + interval '1 day')::text"));
            assertEquals("2020-11-01 00:00:00-04",
                    one("SELECT (timestamptz '2020-11-02 00:00:00-05' - interval '1 day')::text"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    /** A series over instants is walked a step at a time, so it lands on the same days. */
    @Test
    void aSeriesOfDaysWalksTheCalendar() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            List<String> walked = rows("SELECT g::text FROM generate_series("
                    + "timestamptz '2020-03-07 00:00:00-05',"
                    + " timestamptz '2020-03-09 00:00:00-04', interval '1 day') g");
            assertEquals(3, walked.size());
            assertEquals("2020-03-07 00:00:00-05", walked.get(0));
            assertEquals("2020-03-08 00:00:00-05", walked.get(1));
            assertEquals("2020-03-09 00:00:00-04", walked.get(2));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    /** date_add and date_subtract are the same arithmetic, in the zone they are given. */
    @Test
    void aDayMayBeAddedInAZoneOfItsOwn() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            assertEquals("2020-11-01 01:30:00-05",
                    one("SELECT date_add(timestamptz '2020-10-31 01:30:00-04',"
                            + " interval '1 day')::text"));
            assertEquals("2020-11-01 01:30:00-04",
                    one("SELECT date_add(timestamptz '2020-10-31 01:30:00-04',"
                            + " interval '1 day', 'UTC')::text"));
            assertEquals("2020-10-31 01:30:00-04",
                    one("SELECT date_subtract(timestamptz '2020-11-01 01:30:00-05',"
                            + " interval '1 day')::text"));
            assertEquals("timestamp with time zone",
                    one("SELECT pg_typeof(date_add(now(), interval '1 day'))::text"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    /** A zone writes its own abbreviation, which changes with the season. */
    @Test
    void aZoneWritesItsOwnAbbreviation() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            assertEquals("EST",
                    one("SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'TZ')"));
            assertEquals("EDT",
                    one("SELECT to_char(timestamptz '2020-07-01 12:00:00+00', 'TZ')"));
            assertEquals("-05 -5",
                    one("SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'OF FMOF')"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
        exec("SET TimeZone = 'Asia/Kolkata'");
        try {
            assertEquals("IST", one("SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'TZ')"));
            assertEquals("+05:30 +5:30",
                    one("SELECT to_char(timestamptz '2020-01-01 12:00:00+00', 'OF FMOF')"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    /** The catalogue reports PostgreSQL's own table, and the names the zone database writes. */
    @Test
    void theZoneCatalogueReportsWhatPostgresqlCarries() throws SQLException {
        assertEquals("195", one("SELECT count(*)::text FROM pg_timezone_abbrevs"));
        assertEquals("02:00:00",
                one("SELECT utc_offset::text FROM pg_timezone_abbrevs WHERE abbrev = 'IST'"));
        assertEquals("-06:00:00",
                one("SELECT utc_offset::text FROM pg_timezone_abbrevs WHERE abbrev = 'CST'"));
        assertEquals("IST",
                one("SELECT abbrev FROM pg_timezone_names WHERE name = 'Asia/Kolkata'"));
        assertEquals("-03",
                one("SELECT abbrev FROM pg_timezone_names WHERE name = 'America/Sao_Paulo'"));
        assertEquals("+0545",
                one("SELECT abbrev FROM pg_timezone_names WHERE name = 'Asia/Kathmandu'"));
        assertEquals("CAT",
                one("SELECT abbrev FROM pg_timezone_names WHERE name = 'Africa/Windhoek'"));
        assertEquals("-00", one("SELECT abbrev FROM pg_timezone_names WHERE name = 'Factory'"));
        // The zone database drops the SystemV names Java still carries.
        assertEquals("0",
                one("SELECT count(*)::text FROM pg_timezone_names WHERE name LIKE 'SystemV/%'"));
    }
}
