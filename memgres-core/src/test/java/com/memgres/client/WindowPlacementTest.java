package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a window function may stand, and what the parts of a window specification may contain.
 * Frame validation covered the shape of a frame; this covers the calls around it: DISTINCT and an
 * aggregate's ORDER BY inside a window aggregate, what a FILTER condition may hold, an aggregate
 * that contains a window function, and a frame offset — which was parsed as a single token, so
 * {@code 1+1 PRECEDING} was a syntax error while a column, an aggregate or a window function in
 * the same position was accepted and evaluated to nothing. ORDER BY over a window function was
 * accepted too, and then ignored: the rows came back in whatever order they were read in.
 *
 * <p>It also covers the one direction that was missing: a window function <em>over</em> a grouped
 * result. A window in a grouped query runs on the grouped rows, so {@code sum(v)} is an ordinary
 * value to it — {@code sum(sum(v)) OVER ()} is legal, and was refused as a nested aggregate, while
 * {@code rank() OVER (ORDER BY sum(v))} was accepted and then evaluated against a result whose
 * columns are the output aliases, where {@code sum(v)} resolves to nothing and every group ranks
 * 1. A nested aggregate with no OVER is still no query at all and is still refused.
 */
class WindowPlacementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE wpl_t (i int, g text, v int)");
        exec("INSERT INTO wpl_t VALUES (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',30),(5,'b',40)");
        exec("CREATE VIEW wpl_v AS SELECT i, g, rank() OVER (ORDER BY v) rn FROM wpl_t");
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

    // ---- DISTINCT and aggregate ORDER BY inside a window aggregate ----

    @Test
    void distinctInsideAWindowAggregateIsNotImplemented() {
        String message = "DISTINCT is not implemented for window functions";
        assertFails("0A000", message, "SELECT count(DISTINCT v) OVER (PARTITION BY g) FROM wpl_t");
        assertFails("0A000", message, "SELECT sum(DISTINCT v) OVER () FROM wpl_t");
        assertFails("0A000", message, "SELECT avg(DISTINCT v) OVER (ORDER BY i) FROM wpl_t");
        assertFails("0A000", message, "SELECT string_agg(DISTINCT g, ',') OVER () FROM wpl_t");
        assertFails("0A000", message,
                "SELECT sum(DISTINCT v) OVER w FROM wpl_t WINDOW w AS (PARTITION BY g)");
        assertFails("0A000", message,
                "SELECT i, count(DISTINCT v) OVER (PARTITION BY g) + 1 FROM wpl_t");
        // decided before any row is read, so an empty result refuses it too
        assertFails("0A000", message,
                "SELECT array_agg(DISTINCT v) OVER (ORDER BY i) FROM wpl_t WHERE 1 = 0");
        assertFails("0A000", message, "SELECT count(DISTINCT v) OVER () FROM wpl_t GROUP BY v");
    }

    @Test
    void anAggregateOrderByInsideAWindowAggregateIsNotImplemented() {
        assertFails("0A000", "aggregate ORDER BY is not implemented for window functions",
                "SELECT array_agg(v ORDER BY v) OVER () FROM wpl_t");
        assertFails("0A000", "aggregate ORDER BY is not implemented for window functions",
                "SELECT string_agg(g, ',' ORDER BY g) OVER (PARTITION BY g) FROM wpl_t");
        assertFails("0A000", "OVER is not supported for ordered-set aggregate percentile_cont",
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) OVER () FROM wpl_t");
    }

    @Test
    void distinctAndOrderByInAPlainAggregateAreUnaffected() throws Exception {
        assertEquals("4", rows("SELECT count(DISTINCT v) FROM wpl_t"));
        assertEquals("100", rows("SELECT sum(DISTINCT v) FROM wpl_t"));
        assertEquals("a|2,b|2", rows("SELECT g, count(DISTINCT v) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("{10,20,20,30,40}", rows("SELECT array_agg(v ORDER BY v) FROM wpl_t"));
        assertEquals("20", rows("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM wpl_t"));
    }

    // ---- FILTER ----

    @Test
    void aFilterConditionMayNotContainAnAggregateOrAWindowFunction() {
        assertFails("42803", "aggregate functions are not allowed in FILTER",
                "SELECT sum(v) FILTER (WHERE sum(v) > 15) OVER () FROM wpl_t");
        assertFails("42803", "aggregate functions are not allowed in FILTER",
                "SELECT sum(v) FILTER (WHERE sum(v) > 15) FROM wpl_t");
        assertFails("42803", "aggregate functions are not allowed in FILTER",
                "SELECT g, sum(v) FILTER (WHERE count(*) > 1) FROM wpl_t GROUP BY g");
        assertFails("42P20", "window functions are not allowed in FILTER",
                "SELECT sum(v) FILTER (WHERE rank() OVER (ORDER BY i) > 1) OVER () FROM wpl_t");
        assertFails("42P20", "window functions are not allowed in FILTER",
                "SELECT array_agg(v) FILTER (WHERE rank() OVER () > 1) FROM wpl_t");
    }

    @Test
    void filterOnAWindowAggregateStillFiltersTheFrame() throws Exception {
        assertEquals("1|null,2|20,3|40,4|70,5|110",
                rows("SELECT i, sum(v) FILTER (WHERE v > 15) OVER (ORDER BY i) FROM wpl_t ORDER BY i"));
        assertEquals("1|1,2|2,3|3,4|3,5|3",
                rows("SELECT i, count(*) FILTER (WHERE g = 'a') OVER (ORDER BY i) FROM wpl_t ORDER BY i"));
        assertEquals("1|3,2|3,3|3,4|0,5|0",
                rows("SELECT i, count(*) FILTER (WHERE v < 25) OVER (PARTITION BY g) FROM wpl_t ORDER BY i"));
        // a sub-select in the condition is a constant, not an aggregate
        assertEquals("1|110,2|110,3|110,4|110,5|110",
                rows("SELECT i, sum(v) FILTER (WHERE v > (SELECT 15)) OVER () FROM wpl_t ORDER BY i"));
        assertEquals("110", rows("SELECT sum(v) FILTER (WHERE v > 15) FROM wpl_t"));
    }

    // ---- an aggregate may not contain a window function ----

    @Test
    void anAggregateMayNotContainAWindowFunction() {
        String message = "aggregate function calls cannot contain window function calls";
        assertFails("42803", message, "SELECT sum(rank() OVER (ORDER BY v)) FROM wpl_t");
        assertFails("42803", message, "SELECT max(row_number() OVER ()) FROM wpl_t");
        assertFails("42803", message, "SELECT sum(1 + rank() OVER (ORDER BY v)) FROM wpl_t");
        assertFails("42803", message, "SELECT sum(v) + max(rank() OVER ()) FROM wpl_t");
        assertFails("42803", message, "SELECT g, sum(rank() OVER (ORDER BY v)) FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT sum(v ORDER BY rank() OVER ()) FROM wpl_t");
        // a window function whose argument is a window function is a nested call instead
        assertFails("42P20", "window function calls cannot be nested",
                "SELECT sum(rank() OVER (ORDER BY v)) OVER () FROM wpl_t");
    }

    @Test
    void anOrdinaryFunctionAroundAWindowFunctionIsFine() throws Exception {
        assertEquals("1,2,2,4,5", rows("SELECT abs(rank() OVER (ORDER BY v)) FROM wpl_t ORDER BY 1"));
        assertEquals("1,2,2,4,5", rows("SELECT rank() OVER (ORDER BY abs(v)) FROM wpl_t ORDER BY 1"));
        assertEquals("120", rows("SELECT sum(v) FROM wpl_t"));
        assertEquals("125", rows("SELECT sum(v + 1) FROM wpl_t"));
    }

    // ---- frame offsets ----

    @Test
    void aFrameOffsetMayNotDependOnTheRowTheGroupOrTheFrame() {
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN rank() OVER () PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t");
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN rank() OVER () PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t");
        // the error names the frame's own mode
        assertFails("42803", "aggregate functions are not allowed in window ROWS",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42803", "aggregate functions are not allowed in window RANGE",
                "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42803", "aggregate functions are not allowed in window GROUPS",
                "SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN count(*) PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42803", "aggregate functions are not allowed in window ROWS",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND count(*) FOLLOWING) FROM wpl_t");
        assertFails("42P10", "argument of ROWS must not contain variables",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42P10", "argument of RANGE must not contain variables",
                "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42P10", "argument of GROUPS must not contain variables",
                "SELECT sum(v) OVER (ORDER BY i GROUPS BETWEEN v PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42P10", "argument of ROWS must not contain variables",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND v FOLLOWING) FROM wpl_t");
        assertFails("42P10", "argument of ROWS must not contain variables",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN v + 1 PRECEDING AND CURRENT ROW) FROM wpl_t");
    }

    @Test
    void aWindowClauseEntryNothingReferencesIsCheckedTheSameWay() {
        assertFails("42803", "aggregate functions are not allowed in window ROWS",
                "SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i ROWS BETWEEN count(*) PRECEDING AND CURRENT ROW)");
        assertFails("42P10", "argument of ROWS must not contain variables",
                "SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i ROWS BETWEEN v PRECEDING AND CURRENT ROW)");
        assertFails("42P10", "argument of RANGE must not contain variables",
                "SELECT 1 FROM wpl_t WINDOW w AS (ORDER BY i RANGE BETWEEN v PRECEDING AND CURRENT ROW)");
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT 1 FROM wpl_t"
                        + " WINDOW w AS (ORDER BY i ROWS BETWEEN rank() OVER () PRECEDING AND CURRENT ROW)");
    }

    @Test
    void aFrameOffsetIsAnOrdinaryExpression() throws Exception {
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1+1 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|30,2|50,3|80,4|110,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2*1 PRECEDING AND 3-2 FOLLOWING)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|40,4|50,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN -1+2 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN abs(-2) PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN (1+1) PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|50,3|50,4|70,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY v RANGE BETWEEN 5+5 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        // a sub-select reads its own rows, so it is a constant as far as the frame is concerned
        assertEquals("1|10,2|30,3|50,4|80,5|120",
                rows("SELECT i, sum(v) OVER (ORDER BY i"
                        + " ROWS BETWEEN (SELECT max(v) FROM wpl_t)/10 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|70,5|90",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 2::bigint PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
    }

    @Test
    void theOffsetErrorsAlreadyReportedStillAre() {
        assertFails("22013", "frame starting offset must not be negative",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN -1 PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("22004", "frame starting offset must not be null",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN NULL PRECEDING AND CURRENT ROW) FROM wpl_t");
        assertFails("42P20", "frame starting from following row cannot have preceding rows",
                "SELECT sum(v) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM wpl_t");
        assertFails("0A000", "is not supported for column type integer and offset type numeric",
                "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM wpl_t");
    }

    // ---- LIMIT and OFFSET ----

    @Test
    void limitAndOffsetAreReadBeforeAnyRowIsFramed() {
        assertFails("42P20", "window functions are not allowed in LIMIT",
                "SELECT i FROM wpl_t LIMIT rank() OVER ()");
        assertFails("42P20", "window functions are not allowed in OFFSET",
                "SELECT i FROM wpl_t OFFSET rank() OVER ()");
    }

    // ---- ORDER BY over a window function ----

    @Test
    void orderByAWindowFunctionThatIsNotSelected() throws Exception {
        assertEquals("5|40,4|30,2|20,3|20,1|10",
                rows("SELECT i, v FROM wpl_t ORDER BY row_number() OVER (ORDER BY v DESC)"));
        assertEquals("5,4,2,3,1",
                rows("SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v) DESC, i"));
        assertEquals("5,4,2,3,1",
                rows("SELECT i FROM wpl_t ORDER BY 1 + rank() OVER (ORDER BY v DESC)"));
        assertEquals("5,4,3,2,1",
                rows("SELECT i FROM wpl_t ORDER BY sum(v) OVER (ORDER BY i) DESC"));
        assertEquals("1,2,3,4,5",
                rows("SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v), i"));
    }

    @Test
    void orderByAWindowFunctionBesideOneThatIsSelected() throws Exception {
        assertEquals("5|5,4|4,3|2,2|2,1|1",
                rows("SELECT i, rank() OVER (ORDER BY v) FROM wpl_t"
                        + " ORDER BY row_number() OVER (ORDER BY i DESC)"));
        assertEquals("1|1,2|2,3|2,4|4,5|5",
                rows("SELECT i, rank() OVER (ORDER BY v) r FROM wpl_t ORDER BY r, i"));
        assertEquals("5|5,4|4,3|2,2|2,1|1",
                rows("SELECT i, rank() OVER (ORDER BY v) r FROM wpl_t ORDER BY r DESC, i DESC"));
    }

    @Test
    void theOtherClausesKeepTheirMeaningAroundIt() throws Exception {
        assertEquals("4,2",
                rows("SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC) LIMIT 2 OFFSET 1"));
        assertEquals("2,3,1",
                rows("SELECT i FROM wpl_t WHERE g = 'a' ORDER BY rank() OVER (ORDER BY v DESC)"));
        assertEquals("5|b|40,4|b|30,2|a|20,3|a|20,1|a|10",
                rows("SELECT * FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC)"));
        assertEquals("5|b|40,4|b|30,2|a|20,3|a|20,1|a|10",
                rows("SELECT t.* FROM wpl_t t ORDER BY row_number() OVER (ORDER BY v DESC)"));
        assertEquals("5,4,2,3,1",
                rows("SELECT i FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC) NULLS FIRST"));
        // DISTINCT keeps the rule that the ordering expression must be selected
        assertFails("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                "SELECT DISTINCT v FROM wpl_t ORDER BY rank() OVER (ORDER BY v DESC)");
        assertEquals("1,2,4,5",
                rows("SELECT DISTINCT rank() OVER (ORDER BY v) FROM wpl_t ORDER BY 1"));
        assertEquals("1,4",
                rows("SELECT DISTINCT ON (g) i FROM wpl_t ORDER BY g, rank() OVER (ORDER BY v)"));
        // a set-returning target still expands
        assertEquals("1,2,1,2,1,2,1,2,1,2",
                rows("SELECT generate_series(1,2) AS s FROM wpl_t ORDER BY row_number() OVER ()"));
    }

    // ---- the placements already refused, and the shapes that must keep working ----

    @Test
    void theClausesThatRefusedAWindowFunctionStillDo() {
        assertFails("42P20", "window functions are not allowed in GROUP BY",
                "SELECT g FROM wpl_t GROUP BY rank() OVER (ORDER BY v)");
        assertFails("42P20", "window functions are not allowed in HAVING",
                "SELECT g FROM wpl_t GROUP BY g HAVING rank() OVER (ORDER BY g) = 1");
        assertFails("42P20", "window functions are not allowed in WHERE",
                "SELECT i FROM wpl_t WHERE rank() OVER (ORDER BY v) = 1");
        assertFails("42P20", "window functions are not allowed in window definitions",
                "SELECT rank() OVER (PARTITION BY rank() OVER ()) FROM wpl_t");
    }

    @Test
    void windowFunctionsThroughSubqueriesViewsAndJoinsAreUnaffected() throws Exception {
        assertEquals("1|1,2|2,3|2,4|4,5|5",
                rows("SELECT * FROM (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) s"
                        + " WHERE s.rn >= 1 ORDER BY i"));
        assertEquals("1|1,2|2,3|2,4|4,5|5",
                rows("WITH c AS (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t)"
                        + " SELECT * FROM c ORDER BY i"));
        assertEquals("1|1,2|2,3|2,4|4,5|5",
                rows("SELECT a.i, b.rn FROM wpl_t a"
                        + " JOIN (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) b ON a.i = b.i"
                        + " ORDER BY a.i"));
        assertEquals("1|1,2|1,3|1,4|1,5|1",
                rows("SELECT i, x.rn FROM wpl_t t, LATERAL (SELECT rank() OVER (ORDER BY t.v) rn) x"
                        + " ORDER BY i"));
        // through a view, including one grouped afterwards
        assertEquals("1|1,2|2,3|2,4|4,5|5", rows("SELECT i, rn FROM wpl_v ORDER BY i"));
        assertEquals("a|1,b|4", rows("SELECT g, min(rn) FROM wpl_v GROUP BY g ORDER BY g"));
    }

    @Test
    void ordinaryWindowFunctionsAndFramesStillCompute() throws Exception {
        assertEquals("1|10,2|30,3|40,4|50,5|70",
                rows("SELECT i, sum(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|30,5|70",
                rows("SELECT i, sum(v) OVER (PARTITION BY g ORDER BY i) FROM wpl_t ORDER BY i"));
        assertEquals("1|1,2|1,3|1,4|2,5|2",
                rows("SELECT i, ntile(2) OVER (ORDER BY i) FROM wpl_t ORDER BY i"));
        assertEquals("1|null,2|10,3|20,4|20,5|30",
                rows("SELECT i, lag(v) OVER (ORDER BY i) FROM wpl_t ORDER BY i"));
        assertEquals("1|10,2|30,3|50,4|80,5|120",
                rows("SELECT i, sum(v) OVER w FROM wpl_t WINDOW w AS (ORDER BY i) ORDER BY i"));
        assertEquals("a|50,b|70",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|1,b|70|2",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g"));
    }

    // ---- a window function over a grouped result ----

    @Test
    void anAggregateUnderAWindowFunctionIsNotANestedCall() throws Exception {
        assertEquals("120,120", rows("SELECT sum(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("2,2", rows("SELECT count(count(*)) OVER () FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("a|70|50,b|70|50",
                rows("SELECT g, max(sum(v)) OVER (), min(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|60.0000000000000000,b|70|60.0000000000000000",
                rows("SELECT g, sum(v), avg(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|{50,70},b|{50,70}",
                rows("SELECT g, array_agg(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|10|30,b|30|30",
                rows("SELECT g, min(v), max(min(v)) OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("125,125",
                rows("SELECT sum(sum(v) + count(*)) OVER () FROM wpl_t GROUP BY g ORDER BY 1"));
        // one group, written without GROUP BY
        assertEquals("120", rows("SELECT sum(sum(v)) OVER () FROM wpl_t"));
        assertEquals("null", rows("SELECT sum(sum(v)) OVER () FROM wpl_t WHERE 1 = 0"));
        // an aggregate only under the window still makes the query grouped
        assertEquals("1", rows("SELECT rank() OVER (ORDER BY sum(v)) FROM wpl_t"));
        assertEquals("1", rows("SELECT rank() OVER (ORDER BY count(*)) FROM wpl_t"));
    }

    @Test
    void aWindowOrdersAndPartitionsByTheGroupedValue() throws Exception {
        assertEquals("a|50|1,b|70|2",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|2,b|1",
                rows("SELECT g, rank() OVER (ORDER BY sum(v) DESC) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|1,b|70|2",
                rows("SELECT g, sum(v), dense_rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|0,b|70|1",
                rows("SELECT g, sum(v), percent_rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|0.5,b|70|1",
                rows("SELECT g, sum(v), cume_dist() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|1,b|2", rows("SELECT g, ntile(2) OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        // an expression over the aggregate, and a CASE over it
        assertEquals("a|1,b|2",
                rows("SELECT g, rank() OVER (ORDER BY sum(v) * 2 + count(*)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|2,b|70|1",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY -sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|2,b|70|1",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY CASE WHEN sum(v) > 60 THEN 0 ELSE 1 END)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        // PARTITION BY reads the grouped value too
        assertEquals("a|50|1,b|70|1",
                rows("SELECT g, sum(v), rank() OVER (PARTITION BY sum(v) ORDER BY g) FROM wpl_t"
                        + " GROUP BY g ORDER BY g"));
        assertEquals("a|3|50,b|2|70",
                rows("SELECT g, count(*), sum(sum(v)) OVER (PARTITION BY count(*)) FROM wpl_t"
                        + " GROUP BY g ORDER BY g"));
        assertEquals("a|50|1,b|70|1",
                rows("SELECT g, sum(v), count(*) OVER (PARTITION BY sum(v) > 60) FROM wpl_t"
                        + " GROUP BY g ORDER BY g"));
        assertEquals("a|50|50,b|70|70",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (PARTITION BY g) FROM wpl_t GROUP BY g ORDER BY g"));
        // a grouping column the select list does not carry
        assertEquals("1,2", rows("SELECT rank() OVER (ORDER BY g DESC) FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("70,50",
                rows("SELECT sum(v) FROM wpl_t GROUP BY g ORDER BY rank() OVER (ORDER BY g DESC)"));
        assertEquals("1|1|2,2|2|1,3|1|3,4|1|4",
                rows("SELECT v/10 d, count(*), rank() OVER (ORDER BY count(*) DESC, v/10) FROM wpl_t"
                        + " GROUP BY v/10 ORDER BY 1"));
    }

    @Test
    void aFrameOverGroupedRowsFramesTheGroups() throws Exception {
        assertEquals("a|50|50,b|70|120",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|120,b|120",
                rows("SELECT g, sum(sum(v)) OVER (ORDER BY g"
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|50,b|70|120",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY sum(v)"
                        + " RANGE BETWEEN 20 PRECEDING AND CURRENT ROW) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50,b|120",
                rows("SELECT g, sum(sum(v)) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50,b|120",
                rows("SELECT g, sum(sum(v)) OVER (ORDER BY sum(v)"
                        + " RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        // the default frame runs to the end of the current row's peers, which are grouped rows
        assertEquals("a|10|10,a|20|50,a|20|50,b|30|80,b|40|120",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY g, i ORDER BY g, i"));
        assertEquals("a|10|10,a|20|50,a|20|50,b|30|30,b|40|70",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (PARTITION BY g ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY g, i ORDER BY g, i"));
        // the functions that read a particular row of the frame read the grouped row
        assertEquals("a|50|null,b|70|50",
                rows("SELECT g, sum(v), lag(sum(v)) OVER (ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|70,b|70|-1",
                rows("SELECT g, sum(v), lead(sum(v), 1, -1) OVER (ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50,b|50",
                rows("SELECT g, first_value(sum(v)) OVER (ORDER BY g) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|70,b|70|70",
                rows("SELECT g, sum(v), last_value(sum(v)) OVER (ORDER BY sum(v)"
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|70,b|70|70",
                rows("SELECT g, sum(v), nth_value(sum(v), 2) OVER (ORDER BY g"
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
    }

    @Test
    void severalWindowsInOneGroupedQuery() throws Exception {
        assertEquals("a|50|1|2|120,b|70|2|1|120",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)),"
                        + " row_number() OVER (ORDER BY sum(v) DESC), sum(sum(v)) OVER ()"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|50|3,b|70|120|2,null|120|240|1",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY g), rank() OVER (ORDER BY sum(v) DESC)"
                        + " FROM wpl_t GROUP BY ROLLUP(g) ORDER BY 1,2"));
        // a window beside an aggregate, and one inside a larger expression
        assertEquals("a|51,b|72",
                rows("SELECT g, sum(v) + rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|151,b|141",
                rows("SELECT g, sum(v) * count(*) + rank() OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|11,b|12",
                rows("SELECT g, rank() OVER (ORDER BY sum(v)) + 10 FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|low,b|high",
                rows("SELECT g, CASE WHEN rank() OVER (ORDER BY sum(v)) = 1 THEN 'low' ELSE 'high' END"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        // a named window carries the aggregate just as an inline one does
        assertEquals("a|50,b|120",
                rows("SELECT g, sum(sum(v)) OVER w FROM wpl_t GROUP BY g"
                        + " WINDOW w AS (ORDER BY sum(v)) ORDER BY g"));
        assertEquals("a|50,b|50",
                rows("SELECT g, sum(sum(v)) FILTER (WHERE g = 'a') OVER () FROM wpl_t GROUP BY g ORDER BY g"));
    }

    @Test
    void aWindowRunsOnTheGroupsHavingKept() throws Exception {
        assertEquals("b|70|70",
                rows("SELECT g, sum(v), sum(sum(v)) OVER () FROM wpl_t GROUP BY g"
                        + " HAVING sum(v) > 60 ORDER BY g"));
        assertEquals("a|50|120,b|70|120",
                rows("SELECT g, sum(v), sum(sum(v)) OVER () FROM wpl_t GROUP BY g"
                        + " HAVING count(*) > 1 ORDER BY g"));
        assertEquals("b|70,a|50",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING sum(v) > 0"
                        + " ORDER BY rank() OVER (ORDER BY sum(v) DESC)"));
        assertEquals("b|70|1,null|120|2",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY CUBE(g)"
                        + " HAVING sum(v) > 60 ORDER BY 1"));
    }

    @Test
    void aWindowOverGroupingSetsRunsOnEveryRowTheySpell() throws Exception {
        assertEquals("a|50|1,b|70|2,null|120|3",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY ROLLUP(g) ORDER BY 1,2,3"));
        assertEquals("a|50|240,b|70|240,null|120|240",
                rows("SELECT g, sum(v), sum(sum(v)) OVER () FROM wpl_t"
                        + " GROUP BY GROUPING SETS ((g),()) ORDER BY 1,2,3"));
        assertEquals("a|50|50,b|70|120,null|120|240",
                rows("SELECT g, sum(v), sum(sum(v)) OVER (ORDER BY g) FROM wpl_t"
                        + " GROUP BY CUBE(g) ORDER BY 1,2,3"));
        assertEquals("a|1|10|1,a|2|20|3,a|3|20|3,a|null|50|11,b|4|30|7,b|5|40|9,b|null|70|12,"
                        + "null|1|10|1,null|2|20|3,null|3|20|3,null|4|30|7,null|5|40|9,null|null|120|13",
                rows("SELECT g, i, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY CUBE(g,i) ORDER BY 1,2,3,4"));
        assertEquals("a|0|1,b|0|2,null|1|3",
                rows("SELECT g, grouping(g), rank() OVER (ORDER BY grouping(g), sum(v)) FROM wpl_t"
                        + " GROUP BY ROLLUP(g) ORDER BY 1,2,3"));
        assertEquals("null|120,b|70,a|50",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY ROLLUP(g)"
                        + " ORDER BY rank() OVER (ORDER BY sum(v) DESC)"));
        assertEquals("a|50|1,b|70|2",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY GROUPING SETS ((g)) ORDER BY g"));
    }

    @Test
    void distinctAndOrderByReadTheWindowsAlreadyComputed() throws Exception {
        assertEquals("120", rows("SELECT DISTINCT sum(sum(v)) OVER () FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("50,70",
                rows("SELECT DISTINCT sum(sum(v)) OVER (PARTITION BY g) FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("a|1,b|2",
                rows("SELECT DISTINCT g, rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY 1"));
        assertEquals("a|50|1,b|70|2",
                rows("SELECT DISTINCT ON (g) g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t"
                        + " GROUP BY g ORDER BY g"));
        // ordering by the window's output column, by an alias for it, and by a window not selected
        assertEquals("b|70|2,a|50|1",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY 3 DESC, 1"));
        assertEquals("50|1,70|2",
                rows("SELECT sum(v) AS s, rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g ORDER BY s"));
        assertEquals("5,4,3,2,1",
                rows("SELECT i FROM wpl_t GROUP BY i ORDER BY rank() OVER (ORDER BY i DESC)"));
        assertEquals("b|70,a|50",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY g"
                        + " ORDER BY row_number() OVER (ORDER BY sum(v) DESC)"));
        assertEquals("b,a", rows("SELECT g FROM wpl_t GROUP BY g"
                + " ORDER BY sum(sum(v)) OVER (ORDER BY g) DESC"));
        // LIMIT counts rows the window has already been computed over
        assertEquals("b|70|2",
                rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t GROUP BY g LIMIT 1 OFFSET 1"));
    }

    @Test
    void anOrderedSetAggregateUnderAWindowIsAValueLikeAnyOther() throws Exception {
        assertEquals("a|20|1,b|35|2",
                rows("SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) p,"
                        + " rank() OVER (ORDER BY percentile_cont(0.5) WITHIN GROUP (ORDER BY v))"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|20|1,b|30|2",
                rows("SELECT g, mode() WITHIN GROUP (ORDER BY v) m,"
                        + " rank() OVER (ORDER BY mode() WITHIN GROUP (ORDER BY v))"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50,b|50",
                rows("SELECT g, sum(percentile_disc(0.5) WITHIN GROUP (ORDER BY v)) OVER ()"
                        + " FROM wpl_t GROUP BY g ORDER BY g"));
    }

    @Test
    void aNestedAggregateWithNoOverIsStillNoQuery() {
        String message = "aggregate function calls cannot be nested";
        assertFails("42803", message, "SELECT sum(sum(v)) FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT count(count(*)) FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT sum(1 + sum(v)) FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT g, max(sum(v)) FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT sum(sum(v)) FROM wpl_t");
        assertFails("42803", message, "SELECT g FROM wpl_t GROUP BY g HAVING sum(sum(v)) > 0");
        assertFails("42803", message, "SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING max(count(*)) > 0");
        assertFails("42803", message, "SELECT g FROM wpl_t GROUP BY g ORDER BY sum(sum(v))");
        // refused while the statement is read, so an empty result refuses it too
        assertFails("42803", message, "SELECT sum(sum(v)) FROM wpl_t WHERE 1 = 0 GROUP BY g");
        // an ordered-set aggregate reads its WITHIN GROUP ORDER BY per input row, like any other
        assertFails("42803", message,
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum(v)) FROM wpl_t GROUP BY g");
        assertFails("42803", message,
                "SELECT sum(percentile_disc(0.5) WITHIN GROUP (ORDER BY v)) FROM wpl_t GROUP BY g");
        // a window lifts exactly one level: two aggregates under it are still nested
        assertFails("42803", message, "SELECT sum(sum(sum(v))) OVER () FROM wpl_t GROUP BY g");
        assertFails("42803", message, "SELECT rank() OVER (ORDER BY sum(sum(v))) FROM wpl_t GROUP BY g");
    }

    @Test
    void aWindowSpecificationIsJudgedAgainstTheGroupingLikeAnythingElse() {
        String ungrouped = "must appear in the GROUP BY clause or be used in an aggregate function";
        assertFails("42803", ungrouped, "SELECT g, rank() OVER (ORDER BY v) FROM wpl_t GROUP BY g");
        assertFails("42803", ungrouped, "SELECT g, sum(v) OVER () FROM wpl_t GROUP BY g");
        assertFails("42803", ungrouped, "SELECT max(v) OVER () FROM wpl_t GROUP BY g");
        // an aggregate under the window groups the query, so the other targets must be grouped too
        assertFails("42803", ungrouped, "SELECT i, rank() OVER (ORDER BY sum(v)) FROM wpl_t");
        // a WINDOW clause entry is judged even when nothing names it
        assertFails("42803", ungrouped, "SELECT 1 FROM wpl_t GROUP BY g WINDOW w AS (ORDER BY v)");
        // and the placements that were already refused stay refused
        assertFails("42P20", "window functions are not allowed in HAVING",
                "SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING rank() OVER (ORDER BY sum(v)) = 1");
        assertFails("42P20", "window functions are not allowed in WHERE",
                "SELECT i FROM wpl_t WHERE rank() OVER (ORDER BY sum(v)) = 1");
        assertFails("42803", "aggregate functions are not allowed in FILTER",
                "SELECT g, sum(sum(v)) FILTER (WHERE sum(v) > 60) OVER () FROM wpl_t GROUP BY g");
        assertFails("42803", "aggregate function calls cannot contain window function calls",
                "SELECT sum(sum(v) OVER ()) FROM wpl_t GROUP BY g");
    }

    @Test
    void ordinaryGroupedQueriesAreUnaffected() throws Exception {
        assertEquals("a|50,b|70", rows("SELECT g, sum(v) FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50,b|70", rows("SELECT g, sum(v) s FROM wpl_t GROUP BY g ORDER BY s"));
        assertEquals("b|70,a|50", rows("SELECT g, sum(v) FROM wpl_t GROUP BY g ORDER BY sum(v) DESC"));
        assertEquals("b|70", rows("SELECT g, sum(v) FROM wpl_t GROUP BY g HAVING sum(v) > 60 ORDER BY g"));
        assertEquals("b|2,a|3", rows("SELECT g, count(*) FROM wpl_t GROUP BY g ORDER BY count(*), g"));
        assertEquals("a|50,b|70,null|120",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY ROLLUP(g) ORDER BY 1,2"));
        assertEquals("b|70,null|120",
                rows("SELECT g, sum(v) FROM wpl_t GROUP BY CUBE(g) HAVING sum(v) > 60 ORDER BY 1"));
        assertEquals("a,b", rows("SELECT DISTINCT g FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("120", rows("SELECT sum(v) FROM wpl_t"));
        assertEquals("120,120", rows("SELECT (SELECT sum(x.v) FROM wpl_t x) FROM wpl_t GROUP BY g ORDER BY 1"));
        // window functions on an ungrouped query, and through a derived column
        assertEquals("1|1,2|2,3|2,4|4,5|5", rows("SELECT i, rank() OVER (ORDER BY v) FROM wpl_t ORDER BY i"));
        assertEquals("1|1,2|2,3|2,4|4,5|5",
                rows("SELECT * FROM (SELECT i, rank() OVER (ORDER BY v) rn FROM wpl_t) sub"
                        + " WHERE sub.rn >= 1 ORDER BY i"));
        assertEquals("5|1", rows("SELECT count(*), rank() OVER () FROM wpl_t"));
        // a window over grouped rows that reads nothing of the group is unchanged too
        assertEquals("a|2,b|2", rows("SELECT g, count(*) OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|a,b,b|a,b", rows("SELECT g, string_agg(g, ',') OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        assertEquals("a|50|1,b|70|2", rows("SELECT g, sum(v), row_number() OVER () FROM wpl_t GROUP BY g ORDER BY g"));
        // an empty grouped result stays empty, and an empty single group still answers
        assertEquals("", rows("SELECT g, sum(v), rank() OVER (ORDER BY sum(v)) FROM wpl_t"
                + " WHERE 1 = 0 GROUP BY g ORDER BY g"));
        assertEquals("0|1", rows("SELECT count(*), rank() OVER (ORDER BY count(*)) FROM wpl_t WHERE 1 = 0"));
    }
}
