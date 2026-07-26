package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PostgreSQL counts centuries and millennia from year 1, has no year zero, and lets date_trunc
 * work on an interval. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N41 date_trunc/extract gaps, N64 BC dates and infinity comparisons.
 */
class DateTruncAndBcTimestampsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String state(String sql) {
        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        });
        return e.getSQLState();
    }

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    // ------------------------------------------------------------------
    // N41 — the wide truncation units and interval truncation
    // ------------------------------------------------------------------

    @Test
    void decadeCenturyAndMillenniumTruncate() throws Exception {
        assertEquals("2020-01-01 00:00:00",
                expr("SELECT date_trunc('decade', TIMESTAMP '2026-06-25 13:04:05')::text"));
        assertEquals("2001-01-01 00:00:00",
                expr("SELECT date_trunc('century', TIMESTAMP '2026-06-25 13:04:05')::text"));
        assertEquals("2001-01-01 00:00:00",
                expr("SELECT date_trunc('millennium', TIMESTAMP '2026-06-25 13:04:05')::text"));
    }

    /** A century begins in year 1, so 2000 belongs to the century starting 1901. */
    @Test
    void centuriesAreCountedFromYearOne() throws Exception {
        assertEquals("1901-01-01 00:00:00",
                expr("SELECT date_trunc('century', TIMESTAMP '2000-06-25')::text"));
        assertEquals("2001-01-01 00:00:00",
                expr("SELECT date_trunc('century', TIMESTAMP '2001-01-01')::text"));
    }

    @Test
    void dateTruncWorksOnAnInterval() throws Exception {
        assertEquals("3 days 04:00:00",
                expr("SELECT date_trunc('hour', INTERVAL '3 days 4 hours 30 minutes')::text"));
        assertEquals("3 days",
                expr("SELECT date_trunc('day', INTERVAL '3 days 4 hours 30 minutes')::text"));
        assertEquals("5 years",
                expr("SELECT date_trunc('year', INTERVAL '5 years 3 mons 2 days')::text"));
    }

    @Test
    void epochOfAnIntervalUsesAThirtyDayMonth() throws Exception {
        assertEquals("2592000.000000", expr("SELECT extract(epoch FROM INTERVAL '1 mon')::text"));
    }

    // ------------------------------------------------------------------
    // N64 — BC dates and infinity
    // ------------------------------------------------------------------

    @Test
    void bcTimestampsReadAndPrintWithTheirEra() throws Exception {
        assertEquals("0044-03-15 00:00:00 BC", expr("SELECT (TIMESTAMP '0044-03-15 BC')::text"));
        assertEquals("0044-03-15 BC", expr("SELECT (DATE '0044-03-15 BC')::text"));
    }

    /** A negative year argument names the same BC year. */
    @Test
    void makeDateAcceptsANegativeYear() throws Exception {
        assertEquals("0044-03-15 BC", expr("SELECT make_date(-44,3,15)::text"));
    }

    @Test
    void extractYearReportsTheBcYearAsNegative() throws Exception {
        assertEquals("-44", expr("SELECT extract(year FROM TIMESTAMP '0044-03-15 BC')::text"));
    }

    @Test
    void bcDatesOrderBeforeChristianEraDates() throws Exception {
        assertEquals("t", expr("SELECT (DATE '0044-03-15 BC' < DATE '0001-01-01')"));
    }

    // ------------------------------------------------------------------
    // N64 — an interval can be infinite too
    // ------------------------------------------------------------------

    @Test
    void anIntervalLiteralMayBeInfinite() throws Exception {
        assertEquals("infinity", expr("SELECT (INTERVAL 'infinity')::text"));
        assertEquals("-infinity", expr("SELECT (INTERVAL '-infinity')::text"));
    }

    /** The difference of two timestamps is infinite when either endpoint is. */
    @Test
    void anInfiniteEndpointMakesTheDifferenceInfinite() throws Exception {
        assertEquals("infinity", expr("SELECT (TIMESTAMP 'infinity' - TIMESTAMP '-infinity')::text"));
        assertEquals("infinity", expr("SELECT (TIMESTAMP 'infinity' - TIMESTAMP '2026-01-01')::text"));
        assertEquals("-infinity", expr("SELECT (TIMESTAMP '2026-01-01' - TIMESTAMP 'infinity')::text"));
    }

    @Test
    void arithmeticKeepsAnInfiniteInterval() throws Exception {
        assertEquals("infinity", expr("SELECT (INTERVAL 'infinity' + INTERVAL '1 day')::text"));
        assertEquals("-infinity", expr("SELECT (INTERVAL '-infinity' + INTERVAL '1 day')::text"));
        assertEquals("infinity", expr("SELECT (INTERVAL 'infinity' * 2)::text"));
        assertEquals("infinity", expr("SELECT (TIMESTAMP '2026-01-01' + INTERVAL 'infinity')::text"));
    }

    /** An indeterminate result is an error, not a guess. */
    @Test
    void indeterminateInfiniteArithmeticIsRejected() {
        assertEquals("22008", state("SELECT (INTERVAL 'infinity' - INTERVAL 'infinity')"));
        assertEquals("22008", state("SELECT (INTERVAL 'infinity' * 0)"));
    }

    @Test
    void anInfiniteIntervalOrdersOutsideEveryFiniteOne() throws Exception {
        assertEquals("t", expr("SELECT (INTERVAL 'infinity' > INTERVAL '1000 years')"));
        assertEquals("t", expr("SELECT (INTERVAL '-infinity' < INTERVAL '-1000 years')"));
        assertEquals("t", expr("SELECT (INTERVAL 'infinity' = INTERVAL 'infinity')"));
    }

    @Test
    void intervalFunctionsHandleAnInfiniteInterval() throws Exception {
        assertEquals("f", expr("SELECT isfinite(INTERVAL 'infinity')"));
        assertEquals("t", expr("SELECT isfinite(INTERVAL '1 day')"));
        assertEquals("Infinity", expr("SELECT extract(epoch FROM INTERVAL 'infinity')::text"));
        assertEquals("infinity", expr("SELECT justify_hours(INTERVAL 'infinity')::text"));
        assertEquals("infinity", expr("SELECT date_trunc('day', INTERVAL 'infinity')::text"));
    }

    /** A BC date has to be a date, not text carrying a suffix, or it cannot be used. */
    @Test
    void aBcDateSupportsArithmeticAndOrdering() throws Exception {
        assertEquals("366", expr("SELECT (DATE '0001-01-01' - DATE '0001-01-01 BC')::text"));
        assertEquals("0044-03-16 BC", expr("SELECT (DATE '0044-03-15 BC' + 1)::text"));
        assertEquals("t", expr("SELECT (DATE '0044-03-15 BC' < DATE '0043-01-01 BC')"));
    }

    @Test
    void infinityComparesAsTheExtremeValueForDateAndTimestamp() throws Exception {
        assertEquals("t", expr("SELECT (TIMESTAMP 'infinity' > TIMESTAMP '2026-01-01')"));
        assertEquals("t", expr("SELECT ('infinity'::date > DATE '2026-01-01')"));
        assertEquals("t", expr("SELECT ('-infinity'::date < DATE '2026-01-01')"));
    }
}
