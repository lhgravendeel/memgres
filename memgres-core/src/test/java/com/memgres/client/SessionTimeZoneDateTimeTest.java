package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * "Now" belongs to the session, not to the server. CURRENT_DATE, LOCALTIMESTAMP, the 'today' and
 * 'now' literals, date_trunc and EXTRACT were all reading the JVM's clock and zone, so a server in
 * one zone and a session in another disagreed about which day it was — for fourteen hours a day at
 * the extremes, and every day for the two hours between Amsterdam midnight and UTC midnight.
 */
class SessionTimeZoneDateTimeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @AfterEach
    void resetZone() throws Exception {
        exec("RESET TimeZone");
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

    private static void assertTrueIn(String zone, String predicate) throws Exception {
        exec("SET TimeZone = '" + zone + "'");
        assertEquals("true", scalar("SELECT (" + predicate + ")::text"),
                zone + ": " + predicate);
    }

    /** +14 and -11 are a full calendar day apart, so a zone-blind answer cannot pass both. */
    private static final String[] ZONES = {
        "UTC", "Pacific/Kiritimati", "Pacific/Midway", "Europe/Amsterdam", "America/Los_Angeles",
    };

    @Test
    void theTodayLiteralIsTodayInTheSessionZone() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "TIMESTAMP 'today' = date_trunc('day', now())::timestamp");
            assertTrueIn(zone, "DATE 'today' = current_date");
        }
    }

    @Test
    void yesterdayAndTomorrowFollowToday() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "TIMESTAMP 'yesterday' = TIMESTAMP 'today' - interval '1 day'");
            assertTrueIn(zone, "TIMESTAMP 'tomorrow' = TIMESTAMP 'today' + interval '1 day'");
            assertTrueIn(zone, "DATE 'tomorrow' - DATE 'today' = 1");
        }
    }

    @Test
    void currentDateAndLocaltimestampAgreeWithNow() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "current_date = (now() AT TIME ZONE current_setting('TimeZone'))::date");
            assertTrueIn(zone, "localtimestamp = (now() AT TIME ZONE current_setting('TimeZone'))");
            assertTrueIn(zone, "localtime = (now() AT TIME ZONE current_setting('TimeZone'))::time");
            assertTrueIn(zone, "current_date = localtimestamp::date");
        }
    }

    @Test
    void theNowLiteralIsTheStatementInstantNotAFreshReading() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "TIMESTAMP 'now' = localtimestamp");
            assertTrueIn(zone, "TIME 'now' = localtime");
        }
    }

    @Test
    void aTimestamptzReadsItsFieldsInTheSessionZone() throws Exception {
        exec("SET TimeZone = 'Pacific/Kiritimati'");
        assertEquals("2024-06-16 13:00:00",
                scalar("SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::timestamp::text"));
        assertEquals("2024-06-16", scalar("SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::date::text"));
        assertEquals("16", scalar("SELECT extract(day from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text"));
        assertEquals("13", scalar("SELECT extract(hour from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text"));

        exec("SET TimeZone = 'Pacific/Midway'");
        assertEquals("2024-06-15 12:00:00",
                scalar("SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::timestamp::text"));
        assertEquals("2024-06-15", scalar("SELECT (TIMESTAMPTZ '2024-06-15 23:00:00+00')::date::text"));
        assertEquals("12", scalar("SELECT extract(hour from TIMESTAMPTZ '2024-06-15 23:00:00+00')::text"));
    }

    @Test
    void dateTruncFindsMidnightInTheSessionZone() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "date_trunc('day', now()) = current_date::timestamptz");
            assertTrueIn(zone, "extract(day from now()) = extract(day from current_date)");
        }
    }

    @Test
    void dateTruncTakesAnExplicitZone() throws Exception {
        exec("SET TimeZone = 'UTC'");
        assertEquals("true", scalar("SELECT (date_trunc('day',"
                + " TIMESTAMPTZ '2024-06-15 23:30:00+00', 'Pacific/Kiritimati')"
                + " = TIMESTAMPTZ '2024-06-15 10:00:00+00')::text"));
        assertEquals("true", scalar("SELECT (date_trunc('day',"
                + " TIMESTAMPTZ '2024-06-15 00:30:00+00', 'Pacific/Midway')"
                + " = TIMESTAMPTZ '2024-06-14 11:00:00+00')::text"));
        // a half-hour zone shifts the hour boundary too
        assertEquals("true", scalar("SELECT (date_trunc('hour',"
                + " TIMESTAMPTZ '2024-06-15 23:45:00+00', 'Asia/Kolkata')"
                + " = TIMESTAMPTZ '2024-06-15 23:30:00+00')::text"));
        assertNull(scalar("SELECT date_trunc('day', NULL::timestamptz, 'UTC')"));
        assertNull(scalar("SELECT date_trunc('day', TIMESTAMPTZ '2024-06-15 12:00:00+00', NULL)"));
    }

    @Test
    void aColumnDefaultReadsTheSameClock() throws Exception {
        exec("SET TimeZone = 'Pacific/Kiritimati'");
        exec("DROP TABLE IF EXISTS stz_t CASCADE");
        exec("CREATE TABLE stz_t (i int, d date DEFAULT current_date,"
                + " ts timestamp DEFAULT localtimestamp)");
        exec("INSERT INTO stz_t (i) VALUES (1)");
        assertEquals("true", scalar("SELECT (d = current_date)::text FROM stz_t WHERE i = 1"));
        assertEquals("true", scalar("SELECT (ts::date = current_date)::text FROM stz_t WHERE i = 1"));
        exec("DROP TABLE stz_t CASCADE");
    }

    @Test
    void everyCurrentReadingAgreesWithinOneStatement() throws Exception {
        for (String zone : ZONES) {
            assertTrueIn(zone, "current_date = localtimestamp::date"
                    + " AND localtimestamp::date = (TIMESTAMP 'today')::date"
                    + " AND (TIMESTAMP 'today')::date = (DATE 'today')");
        }
    }

    @Test
    void aTransactionFreezesTheClockForEveryReading() throws Exception {
        exec("SET TimeZone = 'Pacific/Midway'");
        conn.setAutoCommit(false);
        try {
            assertEquals("true", scalar("SELECT (current_date = now()::date)::text"));
            assertEquals("true", scalar("SELECT (localtimestamp"
                    + " = (now() AT TIME ZONE current_setting('TimeZone')))::text"));
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void anUnrecognisedZoneNameInDateTruncIsRejected() {
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT date_trunc('day', now(), 'Mars/Olympus')"));
        assertEquals("22023", e.getSQLState());
    }
}
