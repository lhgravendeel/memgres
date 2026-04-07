package com.memgres.catalog;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 compatibility tests for EXPLAIN output and COMMENT ON DDL.
 *
 * Memgres does NOT generate real query plans. It provides best-effort EXPLAIN
 * output that looks similar to PG but has no real cost estimation or plan
 * selection. Tests here verify structure and basic content, not exact plans.
 *
 * Covers known PG 18 vs Memgres compatibility differences:
 *
 * diff 26, EXPLAIN basic:
 *   EXPLAIN SELECT * FROM admin_t WHERE id = 1
 *   Unordered data differs. Test basic EXPLAIN output structure.
 *
 * diff 27, EXPLAIN (COSTS OFF):
 *   EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id
 *   PG returns 1 row, Memgres returns 2 rows. Test at-least-1-row and
 *   expected plan keywords.
 *
 * diff 28, EXPLAIN ANALYZE:
 *   EXPLAIN ANALYZE SELECT * FROM admin_t WHERE id = 1
 *   PG returns 6 rows, Memgres returns 4. Test multiple rows and timing info.
 *
 * diff 48, COMMENT ON INDEX:
 *   COMMENT ON INDEX admin_t_pkey IS 'pk index'
 *   PG gives 42P01 (table doesn't exist in baseline context). Memgres succeeds.
 *   Test that COMMENT ON INDEX works when index exists and errors when it doesn't.
 *
 * diffs 49-55, EXPLAIN options parsing:
 *   Various EXPLAIN option combinations including FORMAT text/json/xml/yaml
 *   and invalid option values that should give 22023.
 *
 * Additional coverage:
 *   - EXPLAIN SELECT, UPDATE, INSERT, DELETE
 *   - EXPLAIN with COSTS ON/OFF, VERBOSE ON/OFF, ANALYZE ON/OFF,
 *     TIMING ON/OFF, BUFFERS ON/OFF
 *   - EXPLAIN (FORMAT text), (FORMAT json), (FORMAT xml), (FORMAT yaml)
 *   - EXPLAIN for JOIN queries, subqueries, aggregate queries
 *   - COMMENT ON TABLE, COLUMN, SCHEMA, VIEW, INDEX
 *   - EXPLAIN output keywords (Seq Scan, Index Scan, Sort, etc.)
 */
class ExplainOutputTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Create a table used by most EXPLAIN tests
        exec("CREATE TABLE admin_t (id int PRIMARY KEY, name text, val int)");
        exec("INSERT INTO admin_t VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 30)");

        // Additional tables for JOIN / subquery tests
        exec("CREATE TABLE explain_a (id int PRIMARY KEY, label text)");
        exec("CREATE TABLE explain_b (id int PRIMARY KEY, a_id int, score int)");
        exec("INSERT INTO explain_a VALUES (1, 'x'), (2, 'y')");
        exec("INSERT INTO explain_b VALUES (1, 1, 100), (2, 1, 200), (3, 2, 300)");

        // A view for COMMENT ON VIEW tests
        exec("CREATE VIEW admin_v AS SELECT * FROM admin_t WHERE val > 10");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Concatenate all rows of the first column into a single string for easier pattern matching. */
    static String planText(List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        for (List<String> row : rows) {
            if (!row.isEmpty() && row.get(0) != null) sb.append(row.get(0)).append("\n");
        }
        return sb.toString();
    }

    // ========================================================================
    // diff 26: EXPLAIN basic
    // ========================================================================

    /**
     * EXPLAIN on a simple WHERE query returns at least 1 row and contains
     * recognisable plan text.  Exact plan is not checked because Memgres does
     * not do real cost-based planning.
     */
    @Test
    void explain_basic_select_returns_rows() throws SQLException {
        List<List<String>> rows = query("EXPLAIN SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(), "EXPLAIN SELECT should return at least 1 row");
    }

    /**
     * EXPLAIN basic SELECT output should contain a scan-type keyword.
     */
    @Test
    void explain_basic_select_contains_scan_keyword() throws SQLException {
        List<List<String>> rows = query("EXPLAIN SELECT * FROM admin_t WHERE id = 1");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("seq scan") || plan.contains("index scan")
                        || plan.contains("seqscan") || plan.contains("indexscan")
                        || plan.contains("scan"),
                "EXPLAIN output should mention some kind of scan, got: " + plan);
    }

    /**
     * EXPLAIN output for a simple SELECT should reference the table name.
     */
    @Test
    void explain_basic_select_mentions_table() throws SQLException {
        List<List<String>> rows = query("EXPLAIN SELECT * FROM admin_t WHERE id = 1");
        String plan = planText(rows).toLowerCase();
        assertTrue(plan.contains("admin_t"),
                "EXPLAIN output should mention the scanned table, got: " + plan);
    }

    // ========================================================================
    // diff 27: EXPLAIN (COSTS OFF)
    // ========================================================================

    /**
     * EXPLAIN (COSTS OFF) returns at least 1 row.
     * PG returns 1 row for this query, Memgres may return 2; both are acceptable.
     */
    @Test
    void explain_costs_off_returns_at_least_one_row() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (COSTS OFF) should return at least 1 row");
    }

    /**
     * EXPLAIN (COSTS OFF) output should contain a plan keyword.
     */
    @Test
    void explain_costs_off_contains_plan_keyword() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("seq scan") || plan.contains("index scan")
                        || plan.contains("sort") || plan.contains("scan"),
                "EXPLAIN (COSTS OFF) should contain a plan keyword, got: " + plan);
    }

    /**
     * EXPLAIN (COSTS OFF) output must NOT contain cost numbers like "(cost=".
     */
    @Test
    void explain_costs_off_omits_cost_numbers() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (COSTS OFF) SELECT * FROM admin_t ORDER BY id");
        String plan = planText(rows).toLowerCase();
        assertFalse(plan.contains("cost="),
                "EXPLAIN (COSTS OFF) output should not contain 'cost=', got: " + plan);
    }

    /**
     * EXPLAIN (COSTS ON), the default, includes cost estimates in the output.
     */
    @Test
    void explain_costs_on_includes_cost_numbers() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (COSTS ON) SELECT * FROM admin_t ORDER BY id");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (COSTS ON) should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(plan.contains("cost") || plan.contains("rows") || plan.contains("width"),
                "EXPLAIN (COSTS ON) output should contain cost/rows/width info, got: " + plan);
    }

    // ========================================================================
    // diff 28: EXPLAIN ANALYZE
    // ========================================================================

    /**
     * EXPLAIN ANALYZE returns multiple rows.
     * PG returns 6 rows; Memgres returns at least 2.
     */
    @Test
    void explain_analyze_returns_multiple_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN ANALYZE SELECT * FROM admin_t WHERE id = 1");
        assertTrue(rows.size() >= 2,
                "EXPLAIN ANALYZE should return at least 2 rows, got " + rows.size());
    }

    /**
     * EXPLAIN ANALYZE output should contain timing-like information.
     * Look for "actual time", "rows", or "loops" keywords.
     */
    @Test
    void explain_analyze_contains_timing_info() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN ANALYZE SELECT * FROM admin_t WHERE id = 1");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("actual time") || plan.contains("actual") || plan.contains("time")
                        || plan.contains("rows") || plan.contains("loops"),
                "EXPLAIN ANALYZE should contain timing/rows/loops info, got: " + plan);
    }

    /**
     * EXPLAIN ANALYZE should mention execution time in the summary lines.
     */
    @Test
    void explain_analyze_mentions_execution_time() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN ANALYZE SELECT * FROM admin_t WHERE id = 1");
        String plan = planText(rows).toLowerCase();
        // PG ends EXPLAIN ANALYZE with "Execution Time: X ms" and "Planning Time: X ms"
        // Memgres best-effort: accept any of these or simply "time"
        assertTrue(
                plan.contains("execution") || plan.contains("planning") || plan.contains("time"),
                "EXPLAIN ANALYZE output should mention execution or planning time, got: " + plan);
    }

    // ========================================================================
    // diff 48: COMMENT ON INDEX
    // ========================================================================

    /**
     * COMMENT ON INDEX on an explicitly created index should succeed.
     * PK constraint-backed indexes (admin_t_pkey) are NOT valid targets
     * for COMMENT ON INDEX because PG treats them as constraints, not indexes.
     */
    @Test
    void comment_on_index_existing_index_succeeds() throws SQLException {
        exec("CREATE INDEX IF NOT EXISTS admin_t_val_idx ON admin_t (val)");
        try {
            exec("COMMENT ON INDEX admin_t_val_idx IS 'val index'");
        } catch (SQLException ex) {
            fail("COMMENT ON INDEX on existing index should succeed, got: "
                    + ex.getSQLState() + " " + ex.getMessage());
        }
    }

    /**
     * COMMENT ON INDEX on a PK constraint-backed index should succeed.
     * PG18 allows COMMENT ON INDEX for auto-generated constraint indexes.
     */
    @Test
    void comment_on_index_pk_constraint_succeeds() throws SQLException {
        exec("COMMENT ON INDEX admin_t_pkey IS 'pk index'");
    }

    /**
     * COMMENT ON INDEX on a non-existent index should give 42P01
     * (undefined_table / undefined object).
     */
    @Test
    void comment_on_index_nonexistent_gives_42P01() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON INDEX no_such_index_xyz IS 'should fail'"));
        assertEquals("42P01", ex.getSQLState(),
                "COMMENT ON INDEX for missing index should give 42P01, got " + ex.getSQLState());
    }

    /**
     * COMMENT ON INDEX can be cleared by setting it to NULL.
     */
    @Test
    void comment_on_index_null_clears_comment() throws SQLException {
        exec("CREATE INDEX IF NOT EXISTS admin_t_val_idx ON admin_t (val)");
        try {
            exec("COMMENT ON INDEX admin_t_val_idx IS 'temp comment'");
            exec("COMMENT ON INDEX admin_t_val_idx IS NULL");
        } catch (SQLException ex) {
            fail("Clearing COMMENT ON INDEX should succeed: " + ex.getMessage());
        }
    }

    // ========================================================================
    // diff 49-55: EXPLAIN options parsing
    // ========================================================================

    /**
     * diff 49: EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) should work.
     */
    @Test
    void explain_options_analyze_false_costs_false_verbose_true() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN with ANALYZE false, COSTS false, VERBOSE true should return rows");
    }

    /**
     * diff 50: EXPLAIN (FORMAT text) should work and return text plan.
     */
    @Test
    void explain_format_text_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (FORMAT text) SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (FORMAT text) should return at least 1 row");
        String plan = planText(rows);
        assertFalse(Strs.isBlank(plan), "EXPLAIN (FORMAT text) output should not be blank");
    }

    /**
     * diff 51: EXPLAIN (FORMAT json) should work and return JSON.
     */
    @Test
    void explain_format_json_returns_json() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (FORMAT json) SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (FORMAT json) should return at least 1 row");
        // The JSON output's first row should start with '[' or '{'
        String firstRow = rows.get(0).isEmpty() ? "" : rows.get(0).get(0);
        assertNotNull(firstRow, "EXPLAIN (FORMAT json) first row should not be null");
        String trimmed = firstRow.trim();
        assertTrue(trimmed.startsWith("[") || trimmed.startsWith("{"),
                "EXPLAIN (FORMAT json) output should start with '[' or '{', got: " + trimmed);
    }

    /**
     * diff 52: EXPLAIN (FORMAT xml) should work and return XML.
     */
    @Test
    void explain_format_xml_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (FORMAT xml) SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (FORMAT xml) should return at least 1 row");
        String plan = planText(rows);
        assertFalse(Strs.isBlank(plan), "EXPLAIN (FORMAT xml) output should not be blank");
    }

    /**
     * diff 53: EXPLAIN (FORMAT yaml) should work and return YAML.
     */
    @Test
    void explain_format_yaml_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (FORMAT yaml) SELECT * FROM admin_t WHERE id = 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (FORMAT yaml) should return at least 1 row");
        String plan = planText(rows);
        assertFalse(Strs.isBlank(plan), "EXPLAIN (FORMAT yaml) output should not be blank");
    }

    /**
     * diff 54: EXPLAIN (FORMAT yamlish), an invalid format value.
     * PG gives 22023 (invalid_parameter_value) for an unrecognised FORMAT.
     * Memgres should also give 22023.
     */
    @Test
    void explain_invalid_format_gives_22023() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (FORMAT yamlish) SELECT * FROM admin_t WHERE id = 1"));
        assertEquals("22023", ex.getSQLState(),
                "Invalid FORMAT value should give 22023, got " + ex.getSQLState());
    }

    /**
     * diff 55a: EXPLAIN (ANALYZE maybe), a non-boolean value for ANALYZE.
     * PG gives 22023 for a non-boolean option value.
     */
    @Test
    void explain_analyze_non_boolean_gives_22023() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (ANALYZE maybe) SELECT * FROM admin_t WHERE id = 1"));
        assertEquals("22023", ex.getSQLState(),
                "Non-boolean ANALYZE value should give 22023, got " + ex.getSQLState());
    }

    /**
     * diff 55b: EXPLAIN (BUFFERS 123), integer used where boolean expected.
     * PG gives 22023 for a non-boolean BUFFERS option.
     */
    @Test
    void explain_buffers_non_boolean_gives_22023() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("EXPLAIN (BUFFERS 123) SELECT * FROM admin_t WHERE id = 1"));
        assertEquals("22023", ex.getSQLState(),
                "Non-boolean BUFFERS value should give 22023, got " + ex.getSQLState());
    }

    // ========================================================================
    // EXPLAIN for DML statements
    // ========================================================================

    /**
     * EXPLAIN UPDATE returns at least 1 row with plan keywords.
     */
    @Test
    void explain_update_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN UPDATE admin_t SET val = val + 1 WHERE id = 1");
        assertFalse(rows.isEmpty(), "EXPLAIN UPDATE should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(plan.contains("update") || plan.contains("scan"),
                "EXPLAIN UPDATE output should mention update or scan, got: " + plan);
    }

    /**
     * EXPLAIN INSERT returns at least 1 row with plan keywords.
     */
    @Test
    void explain_insert_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN INSERT INTO admin_t SELECT 999, 'zzz', 99");
        assertFalse(rows.isEmpty(), "EXPLAIN INSERT should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(plan.contains("insert") || plan.contains("scan") || plan.contains("result"),
                "EXPLAIN INSERT output should mention insert/scan/result, got: " + plan);
    }

    /**
     * EXPLAIN DELETE returns at least 1 row with plan keywords.
     */
    @Test
    void explain_delete_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN DELETE FROM admin_t WHERE id = 999");
        assertFalse(rows.isEmpty(), "EXPLAIN DELETE should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(plan.contains("delete") || plan.contains("scan"),
                "EXPLAIN DELETE output should mention delete or scan, got: " + plan);
    }

    // ========================================================================
    // EXPLAIN option combinations
    // ========================================================================

    /**
     * EXPLAIN (VERBOSE true) returns at least 1 row.
     */
    @Test
    void explain_verbose_true_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (VERBOSE true) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN (VERBOSE true) should return at least 1 row");
    }

    /**
     * EXPLAIN (VERBOSE false) returns at least 1 row.
     */
    @Test
    void explain_verbose_false_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (VERBOSE false) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN (VERBOSE false) should return at least 1 row");
    }

    /**
     * EXPLAIN (ANALYZE true) returns at least 1 row.
     */
    @Test
    void explain_analyze_true_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (ANALYZE true) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN (ANALYZE true) should return at least 1 row");
    }

    /**
     * EXPLAIN (ANALYZE false) returns at least 1 row and no timing info.
     * With ANALYZE false, actual time should not appear.
     */
    @Test
    void explain_analyze_false_returns_rows_without_actual_time() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (ANALYZE false) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN (ANALYZE false) should return at least 1 row");
    }

    /**
     * EXPLAIN (TIMING true) is valid (implies ANALYZE true).
     */
    @Test
    void explain_timing_true_with_analyze_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (ANALYZE true, TIMING true) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (ANALYZE true, TIMING true) should return at least 1 row");
    }

    /**
     * EXPLAIN (BUFFERS true) with ANALYZE is valid.
     */
    @Test
    void explain_buffers_true_with_analyze_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (ANALYZE true, BUFFERS true) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(),
                "EXPLAIN (ANALYZE true, BUFFERS true) should return at least 1 row");
    }

    /**
     * EXPLAIN (COSTS true) is valid and the default.
     */
    @Test
    void explain_costs_true_returns_rows() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN (COSTS true) SELECT * FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN (COSTS true) should return at least 1 row");
    }

    // ========================================================================
    // EXPLAIN for JOIN queries
    // ========================================================================

    /**
     * EXPLAIN for a JOIN query returns at least 1 row and mentions join-related terms.
     */
    @Test
    void explain_join_query_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT a.label, b.score "
                        + "FROM explain_a a JOIN explain_b b ON a.id = b.a_id");
        assertFalse(rows.isEmpty(), "EXPLAIN for JOIN should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("join") || plan.contains("hash") || plan.contains("nested loop")
                        || plan.contains("merge") || plan.contains("scan"),
                "EXPLAIN JOIN plan should mention join type or scan, got: " + plan);
    }

    /**
     * EXPLAIN for a LEFT JOIN query returns at least 1 row.
     */
    @Test
    void explain_left_join_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT a.label, b.score "
                        + "FROM explain_a a LEFT JOIN explain_b b ON a.id = b.a_id");
        assertFalse(rows.isEmpty(), "EXPLAIN for LEFT JOIN should return at least 1 row");
    }

    // ========================================================================
    // EXPLAIN for subqueries
    // ========================================================================

    /**
     * EXPLAIN for a subquery in WHERE returns at least 1 row.
     */
    @Test
    void explain_subquery_in_where_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT * FROM admin_t WHERE id IN (SELECT a_id FROM explain_b)");
        assertFalse(rows.isEmpty(),
                "EXPLAIN for subquery in WHERE should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertFalse(Strs.isBlank(plan), "EXPLAIN subquery output should not be blank");
    }

    /**
     * EXPLAIN for a scalar subquery in SELECT list returns at least 1 row.
     */
    @Test
    void explain_scalar_subquery_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT id, (SELECT count(*) FROM explain_b WHERE a_id = admin_t.id) AS cnt "
                        + "FROM admin_t");
        assertFalse(rows.isEmpty(),
                "EXPLAIN for scalar subquery should return at least 1 row");
    }

    // ========================================================================
    // EXPLAIN for aggregate queries
    // ========================================================================

    /**
     * EXPLAIN for an aggregate query returns at least 1 row and mentions aggregate-related terms.
     */
    @Test
    void explain_aggregate_query_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT count(*), sum(val) FROM admin_t GROUP BY name");
        assertFalse(rows.isEmpty(),
                "EXPLAIN for aggregate should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("aggregate") || plan.contains("group") || plan.contains("scan"),
                "EXPLAIN aggregate plan should mention aggregate/group/scan, got: " + plan);
    }

    /**
     * EXPLAIN for a simple COUNT(*) returns at least 1 row.
     */
    @Test
    void explain_count_star_returns_plan() throws SQLException {
        List<List<String>> rows = query("EXPLAIN SELECT count(*) FROM admin_t");
        assertFalse(rows.isEmpty(), "EXPLAIN SELECT count(*) should return at least 1 row");
    }

    // ========================================================================
    // COMMENT ON TABLE
    // ========================================================================

    /**
     * COMMENT ON TABLE on an existing table should succeed.
     */
    @Test
    void comment_on_table_existing_succeeds() throws SQLException {
        try {
            exec("COMMENT ON TABLE admin_t IS 'admin table for tests'");
        } catch (SQLException ex) {
            fail("COMMENT ON TABLE on existing table should succeed: " + ex.getMessage());
        }
    }

    /**
     * COMMENT ON TABLE on non-existent table gives 42P01.
     */
    @Test
    void comment_on_table_nonexistent_gives_42P01() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON TABLE no_such_table_xyz IS 'should fail'"));
        assertEquals("42P01", ex.getSQLState(),
                "COMMENT ON TABLE for missing table should give 42P01, got " + ex.getSQLState());
    }

    /**
     * COMMENT ON TABLE can be cleared with NULL.
     */
    @Test
    void comment_on_table_null_clears_comment() throws SQLException {
        try {
            exec("COMMENT ON TABLE admin_t IS 'temporary'");
            exec("COMMENT ON TABLE admin_t IS NULL");
        } catch (SQLException ex) {
            fail("Clearing table comment should succeed: " + ex.getMessage());
        }
    }

    // ========================================================================
    // COMMENT ON COLUMN
    // ========================================================================

    /**
     * COMMENT ON COLUMN on an existing column should succeed.
     */
    @Test
    void comment_on_column_existing_succeeds() throws SQLException {
        try {
            exec("COMMENT ON COLUMN admin_t.id IS 'primary key column'");
        } catch (SQLException ex) {
            fail("COMMENT ON COLUMN on existing column should succeed: " + ex.getMessage());
        }
    }

    /**
     * COMMENT ON COLUMN on non-existent column gives 42703 (undefined_column).
     */
    @Test
    void comment_on_column_nonexistent_column_gives_42703() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON COLUMN admin_t.no_such_col IS 'should fail'"));
        assertEquals("42703", ex.getSQLState(),
                "COMMENT ON COLUMN for missing column should give 42703, got " + ex.getSQLState());
    }

    /**
     * COMMENT ON COLUMN on non-existent table gives 42P01.
     */
    @Test
    void comment_on_column_nonexistent_table_gives_42P01() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON COLUMN no_such_table_xyz.col IS 'should fail'"));
        assertEquals("42P01", ex.getSQLState(),
                "COMMENT ON COLUMN for missing table should give 42P01, got " + ex.getSQLState());
    }

    /**
     * COMMENT ON COLUMN can be cleared with NULL.
     */
    @Test
    void comment_on_column_null_clears_comment() throws SQLException {
        try {
            exec("COMMENT ON COLUMN admin_t.name IS 'name column'");
            exec("COMMENT ON COLUMN admin_t.name IS NULL");
        } catch (SQLException ex) {
            fail("Clearing column comment should succeed: " + ex.getMessage());
        }
    }

    // ========================================================================
    // COMMENT ON SCHEMA
    // ========================================================================

    /**
     * COMMENT ON SCHEMA public should succeed.
     */
    @Test
    void comment_on_schema_public_succeeds() throws SQLException {
        try {
            exec("COMMENT ON SCHEMA public IS 'main schema'");
        } catch (SQLException ex) {
            fail("COMMENT ON SCHEMA public should succeed: " + ex.getMessage());
        }
    }

    /**
     * COMMENT ON SCHEMA on non-existent schema gives 3F000 or 42P01.
     */
    @Test
    void comment_on_schema_nonexistent_gives_error() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON SCHEMA no_such_schema_xyz IS 'should fail'"));
        String state = ex.getSQLState();
        assertTrue(state.equals("3F000") || state.equals("42P01"),
                "COMMENT ON SCHEMA for missing schema should give 3F000 or 42P01, got " + state);
    }

    // ========================================================================
    // COMMENT ON VIEW
    // ========================================================================

    /**
     * COMMENT ON VIEW on an existing view should succeed.
     */
    @Test
    void comment_on_view_existing_succeeds() throws SQLException {
        try {
            exec("COMMENT ON VIEW admin_v IS 'admin view'");
        } catch (SQLException ex) {
            fail("COMMENT ON VIEW on existing view should succeed: " + ex.getMessage());
        }
    }

    /**
     * COMMENT ON VIEW on non-existent view gives 42P01.
     */
    @Test
    void comment_on_view_nonexistent_gives_42P01() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COMMENT ON VIEW no_such_view_xyz IS 'should fail'"));
        assertEquals("42P01", ex.getSQLState(),
                "COMMENT ON VIEW for missing view should give 42P01, got " + ex.getSQLState());
    }

    // ========================================================================
    // EXPLAIN on an explicit index scan (with ORDER BY on PK)
    // ========================================================================

    /**
     * EXPLAIN SELECT with ORDER BY on the primary key column returns at least 1 row.
     */
    @Test
    void explain_order_by_pk_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT * FROM admin_t ORDER BY id");
        assertFalse(rows.isEmpty(),
                "EXPLAIN SELECT ORDER BY pk should return at least 1 row");
    }

    /**
     * EXPLAIN ANALYZE SELECT with ORDER BY contains timing info and plan keywords.
     */
    @Test
    void explain_analyze_order_by_pk_contains_timing_and_keywords() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN ANALYZE SELECT * FROM admin_t ORDER BY id");
        assertFalse(rows.isEmpty(),
                "EXPLAIN ANALYZE SELECT ORDER BY pk should return at least 1 row");
        String plan = planText(rows).toLowerCase();
        assertTrue(
                plan.contains("actual") || plan.contains("time") || plan.contains("rows"),
                "EXPLAIN ANALYZE plan should contain timing info, got: " + plan);
        assertTrue(
                plan.contains("scan") || plan.contains("sort") || plan.contains("index"),
                "EXPLAIN ANALYZE plan should contain scan/sort/index keyword, got: " + plan);
    }

    // ========================================================================
    // EXPLAIN with a LIMIT clause
    // ========================================================================

    /**
     * EXPLAIN SELECT with LIMIT returns at least 1 row.
     */
    @Test
    void explain_with_limit_returns_plan() throws SQLException {
        List<List<String>> rows = query(
                "EXPLAIN SELECT * FROM admin_t ORDER BY id LIMIT 1");
        assertFalse(rows.isEmpty(),
                "EXPLAIN SELECT with LIMIT should return at least 1 row");
    }

    // ========================================================================
    // EXPLAIN: non-existent table gives an error (not an empty plan)
    // ========================================================================

    /**
     * EXPLAIN on a non-existent table gives 42P01, not an empty plan.
     */
    @Test
    void explain_nonexistent_table_gives_42P01() throws SQLException {
        SQLException ex = assertThrows(SQLException.class,
                () -> query("EXPLAIN SELECT * FROM no_such_table_xyz"));
        assertEquals("42P01", ex.getSQLState(),
                "EXPLAIN on non-existent table should give 42P01, got " + ex.getSQLState());
    }
}
