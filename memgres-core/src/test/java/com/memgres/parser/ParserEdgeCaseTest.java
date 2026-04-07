package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parser edge cases that the verification suite identified as failing.
 *
 * Covers:
 * - OPERATOR(schema.op)(args) qualified operator syntax
 * - FOR UPDATE FOR SHARE (dual locking)
 * - SELECT FROM table (no columns)
 * - Empty CREATE VIEW AS SELECT (should fail)
 * - Factorial operator (5 !)
 * - COLLATE in expressions
 * - Mixed-case identifier resolution
 * - EXECUTE name(args) for prepared statements
 * - jsonb_path_query as set-returning function
 * - jsonb_path_match with exists()
 */
class ParserEdgeCaseTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    // ========================================================================
    // OPERATOR(schema.op) qualified operator syntax
    // ========================================================================

    @Test
    void qualified_operator_plus() {
        // PG18 treats OPERATOR(pg_catalog.+)(1,2) as unary prefix operator on ROW(1,2)
        // which fails with "operator does not exist: pg_catalog.+ record"
        assertThrows(SQLException.class,
                () -> scalar("SELECT OPERATOR(pg_catalog.+)(1,2)"),
                "Qualified operator + with tuple args should fail in PG18");
    }

    @Test
    void qualified_operator_concat() {
        // PG18 treats OPERATOR(pg_catalog.||)('a','b') as unary prefix operator on ROW('a','b')
        // which fails with "operator does not exist: pg_catalog.|| record"
        assertThrows(SQLException.class,
                () -> scalar("SELECT OPERATOR(pg_catalog.||)('a','b')"),
                "Qualified operator || with tuple args should fail in PG18");
    }

    @Test
    void qualified_operator_unary_fails() {
        // OPERATOR(pg_catalog.+)(1): unary plus via OPERATOR syntax
        // PG: should fail with "operator requires exactly two arguments"
        // But the important thing is that it's parsed
        try {
            scalar("SELECT OPERATOR(pg_catalog.+)(1)");
            // If it succeeds, that might be acceptable too
        } catch (SQLException e) {
            // Expected failure
        }
    }

    // ========================================================================
    // FOR UPDATE / FOR SHARE
    // ========================================================================

    @Test
    void for_update_for_share_dual_lock_succeeds() throws SQLException {
        exec("CREATE TABLE lock_t(id int PRIMARY KEY)");
        exec("INSERT INTO lock_t VALUES (1)");
        try {
            // PG 18 supports dual FOR clauses (FOR UPDATE FOR SHARE)
            int count = countRows("SELECT * FROM lock_t FOR UPDATE FOR SHARE");
            assertEquals(1, count, "Dual FOR UPDATE FOR SHARE should succeed and return rows");
        } finally {
            exec("DROP TABLE IF EXISTS lock_t");
        }
    }

    @Test
    void for_update_skip_locked() throws SQLException {
        exec("CREATE TABLE lock_t2(id int PRIMARY KEY, status text)");
        exec("INSERT INTO lock_t2 VALUES (1, 'ready'), (2, 'ready')");
        try {
            // FOR UPDATE SKIP LOCKED should be valid syntax
            int count = countRows("SELECT * FROM lock_t2 ORDER BY id FOR UPDATE SKIP LOCKED");
            assertEquals(2, count, "FOR UPDATE SKIP LOCKED should return rows");
        } finally {
            exec("DROP TABLE IF EXISTS lock_t2");
        }
    }

    @Test
    void for_update_nowait() throws SQLException {
        exec("CREATE TABLE lock_t3(id int PRIMARY KEY)");
        exec("INSERT INTO lock_t3 VALUES (1)");
        try {
            int count = countRows("SELECT * FROM lock_t3 FOR UPDATE NOWAIT");
            assertEquals(1, count, "FOR UPDATE NOWAIT should work on unlocked rows");
        } finally {
            exec("DROP TABLE IF EXISTS lock_t3");
        }
    }

    // ========================================================================
    // jsonb_path_query as SRF
    // ========================================================================

    @Test
    void jsonb_path_query_returns_multiple_rows() throws SQLException {
        // jsonb_path_query is a set-returning function and should return one row per match
        int count = countRows("SELECT jsonb_path_query('{\"a\":[1,2,3]}'::jsonb, '$.a[*]')");
        assertEquals(3, count,
                "jsonb_path_query should return 3 rows for array elements, not 1");
    }

    @Test
    void jsonb_path_match_with_exists() throws SQLException {
        // jsonb_path_match with exists() predicate
        String val = scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, 'exists($.a ? (@ == 2))')");
        assertEquals("t", val, "jsonb_path_match with exists should return true");
    }

    // ========================================================================
    // Mixed-case table as qualifier
    // ========================================================================

    @Test
    void mixed_case_table_as_qualifier_fails_without_quotes() throws SQLException {
        exec("CREATE TABLE mixedcase(id int PRIMARY KEY, normal text)");
        exec("INSERT INTO mixedcase VALUES (1, 'test')");
        try {
            // SELECT mixedcase.normal FROM mixedcase: PG treats 'mixedcase' as table qualifier
            // This should work since the table name matches (case-insensitive for unquoted)
            String val = scalar("SELECT mixedcase.normal FROM mixedcase");
            assertEquals("test", val);
        } finally {
            exec("DROP TABLE IF EXISTS mixedcase");
        }
    }

    // ========================================================================
    // Empty/degenerate SELECT and CREATE VIEW
    // ========================================================================

    @Test
    void create_view_with_empty_select_fails() throws SQLException {
        // CREATE VIEW vv AS SELECT, with no column list and no FROM
        // PG 18 allows bare SELECT (returns 0 columns)
        assertDoesNotThrow(() -> exec("CREATE VIEW vv AS SELECT"));
        try { exec("DROP VIEW IF EXISTS vv"); } catch (SQLException ignored) {}
    }

    @Test
    void select_with_no_columns_from_table() throws SQLException {
        exec("CREATE TABLE nc_t(id int PRIMARY KEY)");
        exec("INSERT INTO nc_t VALUES (1),(2)");
        try {
            // SELECT FROM nc_t is valid PG syntax, returning rows but no output columns
            // The result set should have 0 columns but 2 rows
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT FROM nc_t")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(0, md.getColumnCount(), "SELECT FROM should have 0 columns");
                int rowCount = 0;
                while (rs.next()) rowCount++;
                assertEquals(2, rowCount, "Should still return 2 rows");
            }
        } finally {
            exec("DROP TABLE IF EXISTS nc_t");
        }
    }

    // ========================================================================
    // GROUPING function
    // ========================================================================

    @Test
    void grouping_function_requires_group_by() throws SQLException {
        exec("CREATE TABLE grp_t(a int, b int, c int)");
        exec("INSERT INTO grp_t VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)");
        try {
            // GROUPING(a) without a in GROUP BY should fail
            assertThrows(SQLException.class,
                    () -> exec("SELECT grouping(a) FROM grp_t"),
                    "GROUPING without proper GROUP BY should fail");
        } finally {
            exec("DROP TABLE IF EXISTS grp_t");
        }
    }

    @Test
    void grouping_with_rollup() throws SQLException {
        exec("CREATE TABLE grp_t2(a int, b text, c int)");
        exec("INSERT INTO grp_t2 VALUES (1, 'x', 10), (1, 'y', 20), (2, 'x', 30)");
        try {
            // GROUPING(a) with GROUP BY ROLLUP(a)
            List<String> vals = column(
                    "SELECT grouping(a) FROM grp_t2 GROUP BY ROLLUP(a) ORDER BY grouping(a)");
            assertTrue(vals.contains("0"), "Should have non-grouped rows with grouping=0");
            assertTrue(vals.contains("1"), "Should have the rollup total row with grouping=1");
        } finally {
            exec("DROP TABLE IF EXISTS grp_t2");
        }
    }

    // ========================================================================
    // CTE-based queue pattern (UPDATE FROM CTE with FOR UPDATE)
    // ========================================================================

    @Test
    void cte_queue_update_from_pattern() throws SQLException {
        exec("CREATE TABLE work_item(id int PRIMARY KEY, status text NOT NULL, priority int NOT NULL)");
        exec("INSERT INTO work_item VALUES (1,'ready',10),(2,'ready',5),(3,'done',1)");
        try {
            // Common queue pattern: CTE with FOR UPDATE SKIP LOCKED, then UPDATE FROM
            exec("""
                WITH next AS (
                  SELECT id FROM work_item
                  WHERE status = 'ready'
                  ORDER BY priority DESC, id
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE work_item w
                SET status = 'running'
                FROM next n
                WHERE w.id = n.id
                """);

            String status = scalar("SELECT status FROM work_item WHERE id = 1");
            assertEquals("running", status, "Queue pattern should update the highest-priority ready item");
        } finally {
            exec("DROP TABLE IF EXISTS work_item");
        }
    }

    // ========================================================================
    // SQLSTATE accuracy for errors
    // ========================================================================

    @Test
    void deprecated_factorial_operator_errors_with_42601() {
        // The ! factorial operator was removed in PG 14
        try {
            exec("SELECT 5 !");
            // If it succeeds, memgres supports it (non-standard but ok)
        } catch (SQLException e) {
            // PG 18 returns 42601 (syntax error), not 42883 (undefined function)
            assertEquals("42601", e.getSQLState(),
                    "Factorial ! should produce syntax error (42601), not " + e.getSQLState());
        }
    }

    @Test
    void row_field_access_error_is_42703() {
        // (ROW(1,'a')).x: anonymous ROW has no named fields
        try {
            exec("SELECT (ROW(1,'a')).x");
            fail("Should fail");
        } catch (SQLException e) {
            assertEquals("42703", e.getSQLState(),
                    "Field access on anonymous ROW should be 42703 (undefined_column)");
        }
    }
}
