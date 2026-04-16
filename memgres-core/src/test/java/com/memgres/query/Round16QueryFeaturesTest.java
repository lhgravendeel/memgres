package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category H: Query features.
 *
 * Covers:
 *  - CTE MATERIALIZED / NOT MATERIALIZED
 *  - CTE SEARCH / CYCLE pseudo-columns
 *  - FOR UPDATE OF table list honored (only listed rel's rows locked)
 *  - FILTER on window aggregates
 *  - ORDER BY ... USING operator
 *  - SOME as ANY synonym
 *  - EXPLAIN (BUFFERS|WAL|TIMING|SUMMARY|SETTINGS) options
 *  - TABLESAMPLE SYSTEM vs BERNOULLI
 */
class Round16QueryFeaturesTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // H1. CTE MATERIALIZED / NOT MATERIALIZED
    // =========================================================================

    @Test
    void cte_materialized_clause_parses_and_executes() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH w AS MATERIALIZED (SELECT 1 AS v) SELECT v FROM w")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1),
                    "WITH ... AS MATERIALIZED must parse and return rows");
        }
    }

    @Test
    void cte_not_materialized_clause_parses_and_executes() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH w AS NOT MATERIALIZED (SELECT 2 AS v) SELECT v FROM w")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1),
                    "WITH ... AS NOT MATERIALIZED must parse and return rows");
        }
    }

    // =========================================================================
    // H2. CTE SEARCH / CYCLE pseudo-columns
    // =========================================================================

    @Test
    void recursive_cte_search_breadth_first_pseudo_column() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_edges");
        exec("CREATE TABLE r16_edges (src int, dst int)");
        exec("INSERT INTO r16_edges VALUES (1,2),(2,3)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH RECURSIVE t(n, p) AS (" +
                             "  SELECT src, ARRAY[src] FROM r16_edges WHERE src=1 " +
                             "  UNION ALL " +
                             "  SELECT e.dst, t.p || e.dst FROM t JOIN r16_edges e ON t.n = e.src" +
                             ") SEARCH BREADTH FIRST BY n SET ord " +
                             "SELECT n, ord FROM t ORDER BY ord")) {
            assertTrue(rs.next());
            // Must have ord column projected (PG pseudo-column)
            assertNotNull(rs.getObject("ord"),
                    "CTE SEARCH BREADTH FIRST must project the `ord` pseudo-column");
        }
    }

    @Test
    void recursive_cte_cycle_pseudo_columns_projected() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_edges2");
        exec("CREATE TABLE r16_edges2 (src int, dst int)");
        exec("INSERT INTO r16_edges2 VALUES (1,2),(2,1)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH RECURSIVE t(n) AS (" +
                             "  SELECT src FROM r16_edges2 WHERE src=1 " +
                             "  UNION ALL " +
                             "  SELECT e.dst FROM t JOIN r16_edges2 e ON t.n=e.src" +
                             ") CYCLE n SET is_cycle USING path " +
                             "SELECT n, is_cycle, path FROM t")) {
            assertTrue(rs.next());
            // is_cycle + path pseudo-columns must appear
            rs.getBoolean("is_cycle");
            assertNotNull(rs.getObject("path"),
                    "CTE CYCLE must project `is_cycle` and `path` pseudo-columns");
        }
    }

    // =========================================================================
    // H3. FOR UPDATE OF table list
    // =========================================================================

    @Test
    void for_update_of_locks_only_named_relation() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_a");
        exec("DROP TABLE IF EXISTS r16_b");
        exec("CREATE TABLE r16_a (id int primary key, v int)");
        exec("CREATE TABLE r16_b (id int primary key, v int)");
        exec("INSERT INTO r16_a VALUES (1, 10)");
        exec("INSERT INTO r16_b VALUES (1, 20)");
        // Parse/plan must accept `OF r16_a` (not lock rows of r16_b).
        // We cannot directly introspect lock sets; assert query at least executes.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT r16_a.v FROM r16_a JOIN r16_b USING (id) FOR UPDATE OF r16_a")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1),
                    "SELECT ... FOR UPDATE OF <rel> must parse and return data");
        }
    }

    // =========================================================================
    // H4. FILTER on window aggregates
    // =========================================================================

    @Test
    void window_filter_applies_to_aggregate() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_wf");
        exec("CREATE TABLE r16_wf (id int, x int)");
        exec("INSERT INTO r16_wf VALUES (1,10),(2,20),(3,30)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, count(*) FILTER (WHERE x > 15) OVER () AS c " +
                             "FROM r16_wf ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong("c"),
                    "count(*) FILTER (WHERE x>15) OVER () must equal 2 (only 20 and 30 qualify)");
        }
    }

    // =========================================================================
    // H5. ORDER BY ... USING operator
    // =========================================================================

    @Test
    void order_by_using_operator_accepted() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT v FROM (VALUES (3),(1),(2)) AS t(v) ORDER BY v USING <")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "ORDER BY v USING < must sort ascending");
        }
    }

    // =========================================================================
    // H6. SOME as ANY synonym
    // =========================================================================

    @Test
    void some_is_synonym_of_any() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT 1 = SOME (SELECT unnest(ARRAY[1,2,3]))")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "`= SOME (subq)` must be accepted as ANY synonym");
        }
    }

    // =========================================================================
    // H7. EXPLAIN option flags parsed (BUFFERS / WAL / TIMING / SUMMARY / SETTINGS)
    // =========================================================================

    @Test
    void explain_buffers_option_parses() throws SQLException {
        // Should not raise a parse error; at minimum must execute.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (BUFFERS) SELECT 1")) {
            assertTrue(rs.next(), "EXPLAIN (BUFFERS) must parse");
        }
    }

    @Test
    void explain_wal_option_parses() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (WAL) SELECT 1")) {
            assertTrue(rs.next(), "EXPLAIN (WAL) must parse");
        }
    }

    @Test
    void explain_settings_option_parses_and_emits_settings_block() throws SQLException {
        boolean sawSettings = false;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (SETTINGS) SELECT 1")) {
            while (rs.next()) {
                String line = rs.getString(1);
                if (line != null && line.toLowerCase().contains("settings")) {
                    sawSettings = true;
                }
            }
        }
        assertTrue(sawSettings,
                "EXPLAIN (SETTINGS) must emit a Settings: line when non-defaults present");
    }

    // =========================================================================
    // H8. TABLESAMPLE SYSTEM vs BERNOULLI
    // =========================================================================

    @Test
    void tablesample_system_keyword_accepted() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_samp");
        exec("CREATE TABLE r16_samp (id int)");
        for (int i = 1; i <= 100; i++) {
            exec("INSERT INTO r16_samp VALUES (" + i + ")");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM r16_samp TABLESAMPLE SYSTEM(100) REPEATABLE(1)")) {
            assertTrue(rs.next());
            // SYSTEM(100) with repeatable must return all rows
            assertEquals(100L, rs.getLong(1),
                    "TABLESAMPLE SYSTEM(100) REPEATABLE(1) must return all 100 rows");
        }
    }
}
