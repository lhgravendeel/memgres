package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Field extraction and the unit vocabulary behind it. Every extraction from a time or timetz
 * failed, because the value was routed through timestamp parsing; and the unit table knew only
 * the singular spellings, so date_trunc('days', …) and date_part('yrs', …) — both ordinary SQL —
 * were rejected. PostgreSQL matches a unit on the first ten characters of a fixed token table,
 * which is why "microseconds" is a unit and "microsecs" is not, and it distinguishes a word it
 * knows but cannot answer for this type (0A000) from a word it does not know at all (22023).
 */
class DateTimeUnitsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("SET TimeZone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
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

    private static void assertFails(String sqlState, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql);
        assertTrue(e.getMessage().contains(message),
                sql + " -> " + e.getMessage() + " (wanted " + message + ")");
    }

    // ---- extract() from time and timetz ------------------------------------------------

    @Test
    void extractReadsTheFieldsOfATime() throws Exception {
        assertScalar("36000.000000", "SELECT extract(epoch from time '10:00:00')");
        assertScalar("10", "SELECT extract(hour from time '10:20:30')");
        assertScalar("20", "SELECT extract(minute from time '10:20:30')");
        assertScalar("30.500000", "SELECT extract(second from time '10:20:30.5')");
        assertScalar("30.000000", "SELECT extract(second from time '10:20:30')");
        assertScalar("30500000", "SELECT extract(microseconds from time '10:20:30.5')");
        assertScalar("30500.000", "SELECT extract(milliseconds from time '10:20:30.5')");
        assertScalar("30000.000", "SELECT extract(milliseconds from time '10:20:30')");
    }

    @Test
    void extractReadsTheFieldsOfATimetz() throws Exception {
        assertScalar("28800.000000", "SELECT extract(epoch from timetz '10:00:00+02')");
        assertScalar("2", "SELECT extract(timezone_hour from timetz '10:00:00+02')");
        assertScalar("-5", "SELECT extract(timezone_hour from timetz '10:00:00-05')");
        assertScalar("30", "SELECT extract(timezone_minute from timetz '10:00:00+02:30')");
        assertScalar("-30", "SELECT extract(timezone_minute from timetz '10:00:00-02:30')");
        assertScalar("7200", "SELECT extract(timezone from timetz '10:00:00+02')");
        assertScalar("-18000", "SELECT extract(timezone from timetz '10:00:00-05')");
        assertScalar("10", "SELECT extract(hour from timetz '10:20:30+02')");
    }

    @Test
    void extractReadsTimeColumnsToo() throws Exception {
        exec("DROP TABLE IF EXISTS dtu_cols");
        exec("CREATE TABLE dtu_cols (t time, tz timetz)");
        exec("INSERT INTO dtu_cols VALUES ('10:20:30.5', '10:20:30.5+02')");
        try {
            assertScalar("10", "SELECT extract(hour from t) FROM dtu_cols");
            assertScalar("30.500000", "SELECT extract(second from t) FROM dtu_cols");
            assertScalar("30030.500000", "SELECT extract(epoch from tz) FROM dtu_cols");
            assertScalar("2", "SELECT extract(timezone_hour from tz) FROM dtu_cols");
            assertScalar("10:00:00", "SELECT date_trunc('hour', t)::text FROM dtu_cols");
        } finally {
            exec("DROP TABLE dtu_cols");
        }
    }

    @Test
    void aTimeHasNoCalendarFields() {
        assertFails("0A000", "unit \"year\" not supported for type time without time zone",
                "SELECT extract(year from time '10:20:30')");
        assertFails("0A000", "unit \"dow\" not supported for type time without time zone",
                "SELECT extract(dow from time '10:20:30')");
        assertFails("0A000", "unit \"julian\" not supported for type time without time zone",
                "SELECT extract(julian from time '10:20:30')");
        assertFails("0A000", "unit \"timezone_hour\" not supported for type time without time zone",
                "SELECT extract(timezone_hour from time '10:20:30')");
        assertFails("0A000", "unit \"year\" not supported for type time with time zone",
                "SELECT extract(year from timetz '10:20:30+02')");
        assertFails("22023", "unit \"bogus\" not recognized for type time without time zone",
                "SELECT extract('bogus' from time '10:20:30')");
        assertFails("22023", "unit \"bogus\" not recognized for type time with time zone",
                "SELECT extract('bogus' from timetz '10:20:30+02')");
        // "now" is a word PG knows, but not one a time can be asked about
        assertFails("22023", "unit \"now\" not recognized for type time without time zone",
                "SELECT extract('now' from time '10:20:30')");
    }

    // ---- the unit vocabulary -----------------------------------------------------------

    @Test
    void pluralAndAbbreviatedUnitsAreUnits() throws Exception {
        assertScalar("2026-07-28 00:00:00",
                "SELECT date_trunc('days', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2026-07-28 10:00:00",
                "SELECT date_trunc('hrs', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2026-07-01 00:00:00",
                "SELECT date_trunc('qtr', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2026-07-27 00:00:00",
                "SELECT date_trunc('w', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2026-01-01 00:00:00",
                "SELECT date_trunc('years', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2026-07-28 10:20:00",
                "SELECT date_trunc('minutes', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("7", "SELECT date_part('mons', timestamp '2026-07-28 10:20:30')");
        assertScalar("2026", "SELECT date_part('yrs', timestamp '2026-07-28 10:20:30')");
        assertScalar("30123456", "SELECT date_part('usec', timestamp '2026-07-28 10:20:30.123456')");
        assertScalar("2026", "SELECT date_part('y', timestamp '2026-07-28 10:20:30')");
        assertScalar("28", "SELECT date_part('d', timestamp '2026-07-28 10:20:30')");
        assertScalar("10", "SELECT date_part('h', timestamp '2026-07-28 10:20:30')");
        assertScalar("20", "SELECT date_part('min', timestamp '2026-07-28 10:20:30')");
        assertScalar("30", "SELECT date_part('s', timestamp '2026-07-28 10:20:30')");
        assertScalar("31", "SELECT date_part('w', timestamp '2026-07-28 10:20:30')");
        assertScalar("2461250.4309027777", "SELECT date_part('j', timestamp '2026-07-28 10:20:30')");
    }

    @Test
    void unitsAreMatchedOnTheirFirstTenCharactersOnly() throws Exception {
        // "microsecon" is a ten-character token, so anything longer that starts with it matches
        assertScalar("30123456",
                "SELECT date_part('microsecondsfoo', timestamp '2026-07-28 10:20:30.123456')");
        assertScalar("30123.456",
                "SELECT date_part('millisecondfoo', timestamp '2026-07-28 10:20:30.123456')");
        assertScalar("3", "SELECT date_part('millenniums', timestamp '2026-07-28 10:20:30')");
        // …while a shorter spelling that is not itself a token is simply unknown
        assertFails("22023", "unit \"microsecs\" not recognized",
                "SELECT date_part('microsecs', timestamp '2026-07-28 10:20:30')");
        assertFails("22023", "unit \"millisecs\" not recognized",
                "SELECT date_part('millisecs', timestamp '2026-07-28 10:20:30')");
        assertFails("22023", "unit \"quarters\" not recognized",
                "SELECT date_part('quarters', timestamp '2026-07-28 10:20:30')");
        assertFails("22023", "unit \"yy\" not recognized",
                "SELECT date_part('yy', timestamp '2026-07-28 10:20:30')");
        assertFails("22023", "unit \"wk\" not recognized",
                "SELECT date_part('wk', timestamp '2026-07-28 10:20:30')");
    }

    @Test
    void unitsAreCaseInsensitiveAndReportedInLowerCase() throws Exception {
        assertScalar("2026-01-01 00:00:00",
                "SELECT date_trunc('YEAR', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("10", "SELECT date_part('Hour', timestamp '2026-07-28 10:20:30')");
        assertFails("0A000", "unit \"timezone_hour\" not supported for type timestamp without time zone",
                "SELECT date_part('TimeZone_Hour', timestamp '2026-07-28 10:20:30')");
    }

    @Test
    void theErrorNamesTheTypeThatCouldNotAnswer() {
        assertFails("22023", "unit \"bogus\" not recognized for type timestamp without time zone",
                "SELECT date_part('bogus', timestamp '2026-07-28 10:20:30')");
        assertFails("22023", "unit \"bogus\" not recognized for type timestamp with time zone",
                "SELECT date_part('bogus', timestamptz '2026-07-28 10:20:30+00')");
        assertFails("22023", "unit \"bogus\" not recognized for type interval",
                "SELECT date_part('bogus', interval '13 days')");
        assertFails("22023", "unit \"bogus\" not recognized for type date",
                "SELECT extract('bogus' from date '2026-07-28')");
        assertFails("22023", "unit \"bogus\" not recognized for type timestamp without time zone",
                "SELECT date_trunc('bogus', timestamp '2026-07-28 10:20:30')");
        // date_trunc does not read the field-only units at all, so epoch is simply unknown to it
        assertFails("22023", "unit \"epoch\" not recognized for type timestamp without time zone",
                "SELECT date_trunc('epoch', timestamp '2026-07-28 10:20:30')");
        assertFails("0A000", "unit \"timezone\" not supported for type timestamp without time zone",
                "SELECT date_trunc('timezone', timestamp '2026-07-28 10:20:30')");
    }

    // ---- date and time resolve to other functions --------------------------------------

    @Test
    void dateTruncOverADateAnswersATimestamptz() throws Exception {
        assertScalar("2026-07-28 00:00:00+00",
                "SELECT date_trunc('day', date '2026-07-28')::text");
        assertScalar("2026-07-27 00:00:00+00",
                "SELECT date_trunc('week', date '2026-07-28')::text");
        assertFails("22023", "unit \"bogus\" not recognized for type timestamp with time zone",
                "SELECT date_trunc('bogus', date '2026-07-28')");
    }

    @Test
    void dateTruncOverATimeAnswersAnInterval() throws Exception {
        assertScalar("10:00:00", "SELECT date_trunc('hour', time '10:20:30.5')::text");
        assertScalar("10:20:00", "SELECT date_trunc('minute', time '10:20:30.5')::text");
        assertScalar("00:00:00", "SELECT date_trunc('year', time '10:20:30.5')::text");
        assertFails("0A000", "unit \"week\" not supported for type interval",
                "SELECT date_trunc('week', time '10:20:30.5')");
        assertFails("42883", "function date_trunc(unknown, time with time zone) does not exist",
                "SELECT date_trunc('hour', timetz '10:20:30+02')");
    }

    @Test
    void extractAndDatePartDisagreeAboutADate() throws Exception {
        // extract() has its own entry point for date and refuses every sub-day unit …
        assertFails("0A000", "unit \"hour\" not supported for type date",
                "SELECT extract(hour from date '2026-07-28')");
        assertFails("0A000", "unit \"second\" not supported for type date",
                "SELECT extract(second from date '2026-07-28')");
        // … while date_part() has none, so the date reaches the timestamp code and answers zero
        assertScalar("0", "SELECT date_part('hour', date '2026-07-28')");
        assertScalar("0", "SELECT date_part('second', date '2026-07-28')");
        assertScalar("2026", "SELECT extract(year from date '2026-07-28')");
        assertScalar("1785196800", "SELECT extract(epoch from date '2026-07-28')");
    }

    // ---- interval units ----------------------------------------------------------------

    @Test
    void anIntervalAnswersTheLargerUnitsToo() throws Exception {
        assertScalar("3", "SELECT extract(quarter from interval '7 months')");
        assertScalar("20", "SELECT extract(century from interval '2001 years')");
        assertScalar("2", "SELECT extract(millennium from interval '2001 years')");
        assertScalar("200", "SELECT extract(decade from interval '2001 years')");
        assertScalar("1", "SELECT extract(week from interval '13 days 24 hours')");
        assertScalar("-1", "SELECT extract(week from interval '-13 days')");
        assertScalar("-2", "SELECT extract(quarter from interval '-5 months')");
        assertScalar("-1", "SELECT extract(quarter from interval '-12 months')");
        assertScalar("30500000", "SELECT extract(microseconds from interval '5 days 10:20:30.5')");
        assertScalar("30500.000", "SELECT extract(milliseconds from interval '5 days 10:20:30.5')");
        assertFails("0A000", "unit \"dow\" not supported for type interval",
                "SELECT extract(dow from interval '13 days')");
        assertFails("0A000", "unit \"julian\" not supported for type interval",
                "SELECT extract(julian from interval '13 days')");
    }

    // ---- date_bin ----------------------------------------------------------------------

    @Test
    void dateBinChecksItsStride() throws Exception {
        assertScalar("2026-07-28 10:15:00", "SELECT date_bin(interval '15 minutes',"
                + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00')::text");
        assertFails("22008", "stride must be greater than zero", "SELECT date_bin(interval '-2 hours',"
                + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00')");
        assertFails("22008", "stride must be greater than zero", "SELECT date_bin(interval '0 hours',"
                + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00')");
        assertFails("0A000", "timestamps cannot be binned into intervals containing months or years",
                "SELECT date_bin(interval '1 mon 1 day',"
                        + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00')");
        assertFails("0A000", "timestamps cannot be binned into intervals containing months or years",
                "SELECT date_bin(interval '1 year',"
                        + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 00:00:00')");
    }

    @Test
    void dateBinBinsBeforeItsOriginAndKeepsATimestamptz() throws Exception {
        assertScalar("2026-07-28 10:15:00", "SELECT date_bin(interval '10 minutes',"
                + " timestamp '2026-07-28 10:20:30', timestamp '2026-07-28 10:25:00')::text");
        assertScalar("2026-07-28 10:15:00+00", "SELECT date_bin(interval '15 minutes',"
                + " timestamptz '2026-07-28 10:20:30+00', timestamptz '2026-07-28 00:00:00+00')::text");
    }

    @Test
    void aNullArgumentMakesTheWholeCallNull() throws Exception {
        assertNull(scalar("SELECT date_part(NULL, timestamp '2026-07-28 10:00:00')"));
        assertNull(scalar("SELECT date_trunc(NULL, timestamp '2026-07-28 10:00:00')"));
        assertNull(scalar("SELECT date_part('hour', NULL::timestamp)"));
        assertNull(scalar("SELECT extract(hour from NULL::time)"));
        assertNull(scalar("SELECT date_bin(interval '15 minutes', NULL,"
                + " timestamp '2026-07-28 00:00:00')"));
        assertNull(scalar("SELECT date_bin(NULL, timestamp '2026-07-28 10:20:30',"
                + " timestamp '2026-07-28 00:00:00')"));
        assertNull(scalar("SELECT date_bin(interval '15 minutes',"
                + " timestamp '2026-07-28 10:20:30', NULL)"));
    }

    // ---- the session zone still decides for a timestamptz ------------------------------

    @Test
    void aTimestamptzIsResolvedInTheSessionZone() throws Exception {
        // +14 and -11 are a full calendar day apart, so a zone-blind answer cannot pass both
        exec("SET TimeZone = 'Pacific/Kiritimati'");
        assertScalar("2026-07-28 00:00:00+14",
                "SELECT date_trunc('day', timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("15", "SELECT extract(hour from timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("50400", "SELECT extract(timezone from timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("14", "SELECT extract(timezone_hour from timestamptz '2026-07-28 01:00:00+00')");

        exec("SET TimeZone = 'Pacific/Niue'");
        assertScalar("2026-07-27 00:00:00-11",
                "SELECT date_trunc('day', timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("14", "SELECT extract(hour from timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("27", "SELECT extract(day from timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("-39600", "SELECT extract(timezone from timestamptz '2026-07-28 01:00:00+00')");
        // the instant itself does not move with the zone
        assertScalar("1785200400.000000",
                "SELECT extract(epoch from timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("2", "SELECT extract(timezone_hour from timetz '10:00:00+02')");

        exec("SET TimeZone = 'UTC'");
        assertScalar("2026-07-28 00:00:00+00",
                "SELECT date_trunc('day', timestamptz '2026-07-28 01:00:00+00')");
        assertScalar("0", "SELECT extract(timezone from timestamptz '2026-07-28 01:00:00+00')");
    }

    // ---- neighbours that must keep working ---------------------------------------------

    @Test
    void theSingularUnitsAndIntervalTruncationAreUnchanged() throws Exception {
        assertScalar("2026-07-27 00:00:00",
                "SELECT date_trunc('week', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2001-01-01 00:00:00",
                "SELECT date_trunc('century', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2001-01-01 00:00:00",
                "SELECT date_trunc('millennium', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("2020-01-01 00:00:00",
                "SELECT date_trunc('decade', timestamp '2026-07-28 10:20:30')::text");
        assertScalar("3 days 04:00:00",
                "SELECT date_trunc('hour', interval '3 days 4 hours 30 minutes')::text");
        assertScalar("infinity", "SELECT date_trunc('day', interval 'infinity')::text");
        assertScalar("infinity", "SELECT date_trunc('day', timestamp 'infinity')::text");
        assertScalar("-44", "SELECT extract(year from timestamp '0044-03-15 BC')");
        assertScalar("-1", "SELECT extract(century from timestamp '0044-03-15 BC')");
    }

    @Test
    void anInfiniteValueAnswersOnlyTheFieldsThatKeepGrowing() throws Exception {
        assertScalar("Infinity", "SELECT extract(year from timestamp 'infinity')");
        assertScalar("-Infinity", "SELECT extract(year from timestamp '-infinity')");
        assertScalar("Infinity", "SELECT extract(epoch from timestamp 'infinity')");
        assertScalar("Infinity", "SELECT extract(year from interval 'infinity')");
        assertScalar("-Infinity", "SELECT extract(day from interval '-infinity')");
        assertNull(scalar("SELECT extract(day from timestamp 'infinity')"));
        assertNull(scalar("SELECT extract(month from interval 'infinity')"));
        assertFails("0A000", "unit \"dow\" not supported for type interval",
                "SELECT extract(dow from interval 'infinity')");
        assertFails("22023", "unit \"bogus\" not recognized for type interval",
                "SELECT extract('bogus' from interval 'infinity')");
    }

    // ---- interval text: sign carry and the ISO 8601 alternative form -------------------

    @Test
    void onlyTheFieldAfterANegativeOneIsSigned() throws Exception {
        assertScalar("-10 mons +3 days 04:05:06",
                "SELECT (interval '-10 mons 3 days 4:05:06')::text");
        assertScalar("-1 years -10 mons +3 days 04:05:06",
                "SELECT (interval '-2 years 2 mons 3 days 4:05:06')::text");
        // a negative day is the last signed field here, so the time part carries a plus
        assertScalar("1 year -3 days +04:05:06",
                "SELECT (interval '1 year -3 days 4:05:06')::text");
        assertScalar("1 day -04:05:06", "SELECT (interval '1 day -4:05:06')::text");
    }

    @Test
    void intervalReadsIso8601sAlternativeForm() throws Exception {
        assertScalar("1 year 2 mons 3 days 04:05:06",
                "SELECT (interval 'P0001-02-03T04:05:06')::text");
        assertScalar("1 year 2 mons 3 days 04:05:06.5",
                "SELECT (interval 'P0001-02-03T04:05:06.5')::text");
        assertScalar("1 year 2 mons 3 days 04:05:06",
                "SELECT (interval 'P1Y2M3DT4H5M6S')::text");
    }
}
