package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for window frame EXCLUDE, nth_value errors, RANGE interval,
 * EXECUTE USING string literal, and 42702 subquery scoping (M2-M4, M26-M27).
 */
class WindowPlpgsqlTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE wt (id int, val int)");
            s.execute("INSERT INTO wt VALUES (1,10),(2,20),(3,30),(4,40),(5,50)");

            s.execute("CREATE TABLE wt_dates (id int, d date, val int)");
            s.execute("INSERT INTO wt_dates VALUES (1, '2024-01-01', 10), (2, '2024-01-02', 20), (3, '2024-01-04', 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private List<String> qAll(String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) results.add(rs.getString(1));
        }
        return results;
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // M2: first_value/last_value/nth_value should respect EXCLUDE clauses
    // ========================================================================

    @Test
    void m2_firstValueExcludeCurrentRow() throws SQLException {
        // first_value with EXCLUDE CURRENT ROW: on row 1, current row IS the first,
        // so result should be the second row's value
        List<String> results = qAll(
                "SELECT first_value(val) OVER (ORDER BY id " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM wt ORDER BY id");
        // Row 1: frame excludes row 1, first remaining = 20
        assertEquals("20", results.get(0));
        // Row 2: frame excludes row 2, first remaining = 10
        assertEquals("10", results.get(1));
    }

    @Test
    void m2_lastValueExcludeCurrentRow() throws SQLException {
        List<String> results = qAll(
                "SELECT last_value(val) OVER (ORDER BY id " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) " +
                "FROM wt ORDER BY id");
        // Row 5: frame excludes row 5, last remaining = 40
        assertEquals("40", results.get(4));
        // Row 1: frame excludes row 1, last remaining = 50
        assertEquals("50", results.get(0));
    }

    @Test
    void m2_nthValueExcludeGroup() throws SQLException {
        // With EXCLUDE GROUP, all peer rows are excluded
        List<String> results = qAll(
                "SELECT nth_value(val, 1) OVER (ORDER BY id " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) " +
                "FROM wt ORDER BY id");
        // Row 1: peers = {1}, exclude group → first remaining = 20
        assertEquals("20", results.get(0));
    }

    // ========================================================================
    // M26: nth_value(x, 0) and negative n should error with 22016
    // ========================================================================

    @Test
    void m26_nthValueZero() {
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT nth_value(val, 0) OVER () FROM wt"));
        assertEquals("22016", ex.getSQLState(),
                "nth_value(x, 0) should error 22016");
    }

    @Test
    void m26_nthValueNegative() {
        SQLException ex = assertThrows(SQLException.class,
                () -> q("SELECT nth_value(val, -1) OVER () FROM wt"));
        assertEquals("22016", ex.getSQLState(),
                "nth_value(x, -1) should error 22016");
    }

    // ========================================================================
    // M27: RANGE with interval offsets should parse and execute
    // ========================================================================

    @Test
    void m27_rangeIntervalOffset() throws SQLException {
        // RANGE BETWEEN '1 day'::interval PRECEDING AND CURRENT ROW
        List<String> results = qAll(
                "SELECT sum(val) OVER (ORDER BY d " +
                "RANGE BETWEEN '1 day'::interval PRECEDING AND CURRENT ROW) " +
                "FROM wt_dates ORDER BY d");
        // 2024-01-01: only itself (10)
        assertEquals("10", results.get(0));
        // 2024-01-02: includes 01-01 and 01-02 (10+20=30)
        assertEquals("30", results.get(1));
        // 2024-01-04: only itself, 01-03 doesn't exist and 01-02 is 2 days back (30)
        assertEquals("30", results.get(2));
    }

    // ========================================================================
    // M3: EXECUTE...USING should not replace $N inside string literals
    // ========================================================================

    @Test
    void m3_executeUsingNoLiteralSplice() throws SQLException {
        // The $1 inside the string literal should NOT be replaced
        exec("DO $$ DECLARE r text; BEGIN "
                + "EXECUTE 'SELECT ''costs $1'' || $1' INTO r USING '!'; "
                + "RAISE NOTICE '%', r; END $$");
        // If working correctly, we should NOT get a parse error
    }

    @Test
    void m3_executeUsingMultipleParams() throws SQLException {
        // Verify $1 inside single-quoted literal is preserved while $1/$2 outside are replaced
        exec("DO $$ DECLARE r text; BEGIN "
                + "EXECUTE 'SELECT $1 || '' got $1 '' || $2' INTO r USING 'A', 'B'; "
                + "RAISE NOTICE '%', r; END $$");
    }

    // ========================================================================
    // M4: 42702 false positive for subquery-only table columns
    // ========================================================================

    @Test
    void m4_noFalsePositiveSubqueryColumn() throws SQLException {
        // Variable 'val' and column emp.val — but emp is only in a subquery
        exec("CREATE TABLE m4_emp (id int, val int)");
        exec("INSERT INTO m4_emp VALUES (1, 100)");
        exec("DO $$ DECLARE val int := 5; r int; BEGIN "
                + "r := (SELECT count(*) FROM m4_emp) + val; "
                + "RAISE NOTICE 'result=%', r; END $$");
    }

    @Test
    void m4_topLevelTableStillAmbiguous() {
        // Variable 'val' with top-level FROM m4_emp should still be 42702
        assertThrows(SQLException.class, () ->
                q("DO $$ DECLARE val int := 5; BEGIN "
                        + "PERFORM val FROM m4_emp; END $$"));
    }
}
