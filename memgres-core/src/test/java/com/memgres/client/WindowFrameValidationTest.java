package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Window evaluation was already strong, but nothing checked the specification it evaluated: an
 * undefined window name, a frame whose start is after its end and a window function in GROUP BY
 * all produced a plausible answer from a specification PostgreSQL refuses to guess at. A negative
 * lag offset — which PostgreSQL reads as looking forward — went further and crashed.
 */
class WindowFrameValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE wfv_t (i int, g text, v int)");
        exec("INSERT INTO wfv_t VALUES (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',30),(5,'b',40)");
        exec("CREATE TABLE wfv_n (i int, v int)");
        exec("INSERT INTO wfv_n VALUES (1,10),(2,NULL),(3,30),(4,NULL),(5,50)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per line, columns joined by '|', NULL rendered as "null". */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
            return String.join(",", out);
        }
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- named windows ----

    @Test
    void anUndefinedWindowNameIsRejected() {
        assertFails("42704", "window \"nosuchwindow\" does not exist",
                "SELECT sum(v) OVER nosuchwindow FROM wfv_t");
        assertFails("42704", "window \"nosuch\" does not exist",
                "SELECT sum(v) OVER (nosuch ORDER BY i) FROM wfv_t");
        assertFails("42704", "window \"w\" does not exist",
                "SELECT sum(v) OVER w FROM wfv_t");
        // a WINDOW entry that names a window of its own must resolve too
        assertFails("42704", "window \"nosuch\" does not exist",
                "SELECT 1 FROM wfv_t WINDOW w AS (nosuch)");
        // a base window has to be defined before the entry using it, so a chain cannot loop
        assertFails("42704", "window \"w\" does not exist",
                "SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (w)");
        assertFails("42704", "window \"w\" does not exist",
                "SELECT sum(v) OVER w2 FROM wfv_t WINDOW w2 AS (w), w AS (ORDER BY i)");
    }

    @Test
    void aWindowNameMayNotBeDefinedTwice() {
        assertFails("42P20", "window \"w\" is already defined",
                "SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (), w AS ()");
        assertFails("42P20", "window \"w\" is already defined",
                "SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (PARTITION BY g), w AS (ORDER BY i)");
    }

    @Test
    void aCopiedWindowMayNotOverrideWhatTheOriginalFixed() {
        assertFails("42P20", "cannot override PARTITION BY clause of window \"w\"",
                "SELECT sum(v) OVER (w PARTITION BY g) FROM wfv_t WINDOW w AS (PARTITION BY g)");
        // PARTITION BY is refused even when the named window does not have one
        assertFails("42P20", "cannot override PARTITION BY clause of window \"w\"",
                "SELECT sum(v) OVER (w PARTITION BY i) FROM wfv_t WINDOW w AS (ORDER BY i)");
        assertFails("42P20", "cannot override ORDER BY clause of window \"w\"",
                "SELECT sum(v) OVER (w ORDER BY i) FROM wfv_t WINDOW w AS (ORDER BY v)");
        assertFails("42P20", "cannot copy window \"w\" because it has a frame clause",
                "SELECT sum(v) OVER (w) FROM wfv_t"
                        + " WINDOW w AS (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
        // the same three rules govern WINDOW w2 AS (w ...)
        assertFails("42P20", "cannot override ORDER BY clause of window \"w\"",
                "SELECT sum(v) OVER w2 FROM wfv_t WINDOW w AS (ORDER BY i), w2 AS (w ORDER BY v)");
        assertFails("42P20", "cannot override PARTITION BY clause of window \"w\"",
                "SELECT sum(v) OVER w2 FROM wfv_t WINDOW w AS (ORDER BY i), w2 AS (w PARTITION BY g)");
        assertFails("42P20", "cannot copy window \"w\" because it has a frame clause",
                "SELECT sum(v) OVER w2 FROM wfv_t"
                        + " WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), w2 AS (w)");
    }

    @Test
    void validNamedWindowsStillWork() throws Exception {
        // OVER w takes the named window whole, frame included
        assertEquals("10,30,40,50,70", rows("SELECT sum(v) OVER w FROM wfv_t"
                + " WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY i"));
        // adding what the original left open is allowed
        assertEquals("1|10,2|30,3|50,4|30,5|70",
                rows("SELECT i, sum(v) OVER (w ORDER BY i) FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g) ORDER BY i"));
        assertEquals("1|10,2|30,3|40,4|30,5|70",
                rows("SELECT i, sum(v) OVER (w ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g ORDER BY i) ORDER BY i"));
        // a window defined in terms of another inherits its partitioning
        assertEquals("1|10,2|30,3|50,4|30,5|70",
                rows("SELECT i, sum(v) OVER w2 FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY i) ORDER BY i"));
        // window names are case-insensitive, and an unreferenced entry is harmless
        assertEquals("1|10,2|30,3|50,4|80,5|120",
                rows("SELECT i, sum(v) OVER W FROM wfv_t WINDOW w AS (ORDER BY i), x AS () ORDER BY i"));
        // a chain of base windows flattens
        assertEquals("1|10,2|30,3|50,4|30,5|70",
                rows("SELECT i, sum(v) OVER w3 FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g), w2 AS (w), w3 AS (w2 ORDER BY i) ORDER BY i"));
    }

    // ---- frame clauses ----

    @Test
    void aRangeOffsetNeedsExactlyOneOrderByColumn() {
        String message = "RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column";
        assertFails("42P20", message,
                "SELECT sum(v) OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", message,
                "SELECT sum(v) OVER (ORDER BY g, v RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM wfv_t");
        assertFails("42P20", message,
                "SELECT sum(v) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", message,
                "SELECT sum(v) OVER (PARTITION BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        // the ORDER BY may come from the named window the frame is attached to
        assertFails("42P20", message,
                "SELECT sum(v) OVER (w RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g)");
        assertFails("42P20", message,
                "SELECT sum(v) OVER w FROM wfv_t WINDOW w AS (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)");
    }

    @Test
    void groupsModeNeedsAnOrderBy() {
        assertFails("42P20", "GROUPS mode requires an ORDER BY clause",
                "SELECT sum(v) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", "GROUPS mode requires an ORDER BY clause",
                "SELECT sum(v) OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", "GROUPS mode requires an ORDER BY clause",
                "SELECT sum(v) OVER (w GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t"
                        + " WINDOW w AS (PARTITION BY g)");
    }

    @Test
    void aFrameStartingAfterItsEndIsRejected() {
        assertFails("42P20", "frame starting from current row cannot have preceding rows",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM wfv_t");
        assertFails("42P20", "frame starting from following row cannot have preceding rows",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", "frame start cannot be UNBOUNDED FOLLOWING",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM wfv_t");
        assertFails("42P20", "frame end cannot be UNBOUNDED PRECEDING",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM wfv_t");
        assertFails("42P20", "frame end cannot be UNBOUNDED PRECEDING",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM wfv_t");
        // and inside a WINDOW clause, where nothing references it
        assertFails("42P20", "frame starting from current row cannot have preceding rows",
                "SELECT 1 FROM wfv_t WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)");
    }

    @Test
    void aFrameOffsetMustNotBeNull() {
        assertFails("22004", "frame starting offset must not be null",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("22004", "frame ending offset must not be null",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND NULL FOLLOWING) FROM wfv_t");
        assertFails("22004", "frame starting offset must not be null",
                "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("22004", "frame starting offset must not be null",
                "SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wfv_t");
    }

    @Test
    void aFrameOffsetMustNotBeNegative() {
        assertFails("22013", "frame starting offset must not be negative",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("22013", "frame ending offset must not be negative",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND -1 FOLLOWING) FROM wfv_t");
        assertFails("22013", "frame starting offset must not be negative",
                "SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        // RANGE reports a bad size in its own words
        assertFails("22013", "invalid preceding or following size in window function",
                "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wfv_t");
    }

    @Test
    void aRangeOffsetMustSuitTheSortColumnType() {
        assertFails("0A000",
                "RANGE with offset PRECEDING/FOLLOWING is not supported for column type text",
                "SELECT sum(v) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wfv_t");
        assertFails("0A000",
                "is not supported for column type integer and offset type numeric",
                "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM wfv_t");
    }

    @Test
    void frameOffsetsAreBigintsSoTheyRoundRatherThanTruncate() throws Exception {
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i GROUPS BETWEEN 1.5 PRECEDING AND CURRENT ROW)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2::bigint PRECEDING AND CURRENT ROW)"
                        + " FROM wfv_t ORDER BY i"));
        // a signed offset parses; a positive one behaves as the unsigned form
        assertEquals("1|10,2|30,3|40,4|50,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN +1 PRECEDING AND CURRENT ROW)"
                        + " FROM wfv_t ORDER BY i"));
    }

    // ---- placement ----

    @Test
    void aWindowFunctionIsRefusedWhereItCannotAppear() {
        assertFails("42P20", "window functions are not allowed in GROUP BY",
                "SELECT g FROM wfv_t GROUP BY rank() OVER (ORDER BY v)");
        assertFails("42P20", "window functions are not allowed in GROUP BY",
                "SELECT count(*) FROM wfv_t GROUP BY row_number() OVER ()");
        assertFails("42P20", "window function calls cannot be nested",
                "SELECT sum(rank() OVER (ORDER BY v)) OVER () FROM wfv_t");
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT rank() OVER (ORDER BY row_number() OVER ()) FROM wfv_t");
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT rank() OVER (PARTITION BY rank() OVER ()) FROM wfv_t");
        // the clauses that were already refused stay refused
        assertFails("42P20", "window functions are not allowed in WHERE",
                "SELECT i FROM wfv_t WHERE rank() OVER (ORDER BY v) = 1");
        assertFails("42P20", "window functions are not allowed in HAVING",
                "SELECT g FROM wfv_t GROUP BY g HAVING rank() OVER (ORDER BY g) = 1");
    }

    @Test
    void filterIsNotImplementedForNonAggregateWindowFunctions() {
        String message = "FILTER is not implemented for non-aggregate window functions";
        assertFails("0A000", message,
                "SELECT rank() FILTER (WHERE v > 15) OVER (ORDER BY v) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT row_number() FILTER (WHERE v > 15) OVER (ORDER BY v) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT lag(v) FILTER (WHERE v > 15) OVER (ORDER BY v) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT first_value(v) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT ntile(2) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT nth_value(v, 2) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t");
        assertFails("0A000", message,
                "SELECT rank() FILTER (WHERE v > 15) OVER w FROM wfv_t WINDOW w AS (ORDER BY v)");
    }

    @Test
    void filterOnAWindowAggregateStillWorks() throws Exception {
        assertEquals("1|null,2|20,3|40,4|70,5|110",
                rows("SELECT i, sum(v) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|0,2|1,3|2,4|3,5|4",
                rows("SELECT i, count(*) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        // and a window function in ORDER BY is allowed
        assertEquals("1,2,3,4,5", rows("SELECT i FROM wfv_t ORDER BY rank() OVER (ORDER BY v), i"));
    }

    // ---- lag / lead / nth_value / ntile offsets ----

    @Test
    void aNegativeLagLooksForwardInsteadOfCrashing() throws Exception {
        assertEquals("1|20,2|20,3|30,4|40,5|null",
                rows("SELECT i, lag(v, -1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|20,2|30,3|40,4|null,5|null",
                rows("SELECT i, lag(v, -2) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lag(v, -10) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|20,2|20,3|30,4|40,5|0",
                rows("SELECT i, lag(v, -1, 0) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|-1,2|-1,3|-1,4|-1,5|-1",
                rows("SELECT i, lag(v, -10, -1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        // it stays inside its own partition
        assertEquals("1|20,2|20,3|null,4|40,5|null",
                rows("SELECT i, lag(v, -1) OVER (PARTITION BY g ORDER BY i) FROM wfv_t ORDER BY i"));
    }

    @Test
    void aNegativeLeadLooksBackward() throws Exception {
        assertEquals("1|null,2|10,3|20,4|20,5|30",
                rows("SELECT i, lead(v, -1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|10,4|20,5|20",
                rows("SELECT i, lead(v, -2) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|0,2|10,3|20,4|20,5|30",
                rows("SELECT i, lead(v, -1, 0) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|10,3|20,4|null,5|30",
                rows("SELECT i, lead(v, -1) OVER (PARTITION BY g ORDER BY i) FROM wfv_t ORDER BY i"));
    }

    @Test
    void anOffsetAtTheIntegerLimitsDoesNotWrapAround() throws Exception {
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lead(v, 2147483647) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lag(v, 2147483647) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lag(v, -2147483648) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
    }

    @Test
    void aNullOffsetYieldsNullRatherThanTheDefault() throws Exception {
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lag(v, NULL) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, lead(v, NULL) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, nth_value(v, NULL) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|null,3|null,4|null,5|null",
                rows("SELECT i, ntile(NULL) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
    }

    @Test
    void aFractionalOffsetMatchesNoFunction() {
        assertFails("42883", "function lag(integer, numeric) does not exist",
                "SELECT lag(v, 1.5) OVER (ORDER BY i) FROM wfv_t");
        assertFails("42883", "function lead(integer, numeric) does not exist",
                "SELECT lead(v, 1.5) OVER (ORDER BY i) FROM wfv_t");
        assertFails("42883", "function nth_value(integer, numeric) does not exist",
                "SELECT nth_value(v, 2.5) OVER (ORDER BY i) FROM wfv_t");
    }

    @Test
    void positionArgumentsOutsideTheirRangeKeepTheirOwnErrors() {
        assertFails("22016", "argument of nth_value must be greater than zero",
                "SELECT nth_value(v, 0) OVER (ORDER BY i) FROM wfv_t");
        assertFails("22016", "argument of nth_value must be greater than zero",
                "SELECT nth_value(v, -1) OVER (ORDER BY i) FROM wfv_t");
        assertFails("22014", "argument of ntile must be greater than zero",
                "SELECT ntile(0) OVER (ORDER BY i) FROM wfv_t");
        assertFails("22014", "argument of ntile must be greater than zero",
                "SELECT ntile(-1) OVER (ORDER BY i) FROM wfv_t");
    }

    @Test
    void ordinaryOffsetsAreUnaffected() throws Exception {
        assertEquals("1|10,2|20,3|20,4|30,5|40",
                rows("SELECT i, lag(v, 0) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|10,3|20,4|20,5|30",
                rows("SELECT i, lag(v, 1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|-1,2|-1,3|10,4|20,5|20",
                rows("SELECT i, lag(v, 2, -1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|20,2|30,3|40,4|-1,5|-1",
                rows("SELECT i, lead(v, 2, -1) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|10,3|null,4|30,5|null",
                rows("SELECT i, lag(v) OVER (ORDER BY i) FROM wfv_n ORDER BY i"));
        assertEquals("1|null,2|20,3|20,4|20,5|20",
                rows("SELECT i, nth_value(v, 2) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
        assertEquals("1|1,2|1,3|1,4|2,5|2",
                rows("SELECT i, ntile(2) OVER (ORDER BY i) FROM wfv_t ORDER BY i"));
    }

    // ---- the frames themselves, which must keep working ----

    @Test
    void rowsRangeAndGroupsFramesStillCompute() throws Exception {
        assertEquals("1|10,2|30,3|40,4|50,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS 1 PRECEDING) FROM wfv_t ORDER BY i"));
        assertEquals("1|null,2|10,3|30,4|40,5|50",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|40,2|50,3|70,4|40,5|null",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)"
                        + " FROM wfv_t ORDER BY i"));
        // count over an empty frame is 0, not NULL
        assertEquals("1|2,2|2,3|1,4|0,5|0",
                rows("SELECT i, count(*) OVER (ORDER BY i ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|50,2|80,3|80,4|110,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|50,2|80,3|80,4|110,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY v GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
                        + " FROM wfv_t ORDER BY i"));
        assertEquals("1|10|20,2|10|20,3|20|30,4|20|40,5|30|40",
                rows("SELECT i,"
                        + " first_value(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),"
                        + " last_value(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
                        + " FROM wfv_t ORDER BY i"));
    }

    @Test
    void allFourExcludeVariantsStillCompute() throws Exception {
        String frame = "ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE ";
        assertEquals("1|110,2|100,3|100,4|90,5|80",
                rows("SELECT i, sum(v) OVER (" + frame + "CURRENT ROW) FROM wfv_t ORDER BY i"));
        assertEquals("1|110,2|80,3|80,4|90,5|80",
                rows("SELECT i, sum(v) OVER (" + frame + "GROUP) FROM wfv_t ORDER BY i"));
        assertEquals("1|120,2|100,3|100,4|120,5|120",
                rows("SELECT i, sum(v) OVER (" + frame + "TIES) FROM wfv_t ORDER BY i"));
        assertEquals("1|120,2|120,3|120,4|120,5|120",
                rows("SELECT i, sum(v) OVER (" + frame + "NO OTHERS) FROM wfv_t ORDER BY i"));
        assertEquals("1|110,2|80,3|80,4|90,5|80",
                rows("SELECT i, sum(v) OVER (ORDER BY v GROUPS BETWEEN UNBOUNDED PRECEDING"
                        + " AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM wfv_t ORDER BY i"));
        assertEquals("1|120,2|100,3|100,4|120,5|120",
                rows("SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING"
                        + " AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM wfv_t ORDER BY i"));
    }

    @Test
    void theRankingFunctionsStillCompute() throws Exception {
        assertEquals("1|1|1,2|2|2,3|2|2,4|4|3,5|5|4",
                rows("SELECT i, rank() OVER w, dense_rank() OVER w FROM wfv_t"
                        + " WINDOW w AS (ORDER BY v) ORDER BY i"));
        assertEquals("1|0|0.2,2|0.25|0.6,3|0.25|0.6,4|0.75|0.8,5|1|1",
                rows("SELECT i, percent_rank() OVER w, cume_dist() OVER w FROM wfv_t"
                        + " WINDOW w AS (ORDER BY v) ORDER BY i"));
    }
}
