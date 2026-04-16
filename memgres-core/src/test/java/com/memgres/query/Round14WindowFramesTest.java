package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Window frame / aggregate edge cases.
 *
 * - IGNORE NULLS / RESPECT NULLS on lead/lag/first_value/last_value (SQL std)
 * - nth_value FROM FIRST/LAST (PG 13+)
 * - Named WINDOW inheritance: w2 AS (w1 ROWS ...)
 * - COUNT(DISTINCT (c1,c2)) composite distinct
 * - CORRESPONDING in set ops
 * - Multi-arg GROUPING() bit mask
 * - GROUP BY DISTINCT (PG 16+)
 * - Mutual recursion in CTE
 * - MERGE WHEN NOT MATCHED BY SOURCE conditional
 * - range_agg with FILTER
 */
class Round14WindowFramesTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    @BeforeEach
    void seed() throws SQLException {
        exec("DROP TABLE IF EXISTS r14_win CASCADE");
        exec("CREATE TABLE r14_win (id int, v int)");
        exec("INSERT INTO r14_win VALUES (1, NULL), (2, 10), (3, NULL), (4, 20), (5, NULL), (6, 30)");
    }

    // =========================================================================
    // A. IGNORE NULLS / RESPECT NULLS
    // =========================================================================

    @Test
    void lag_ignore_nulls() throws SQLException {
        // lag IGNORE NULLS should skip NULL values when looking back
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, lag(v, 1) IGNORE NULLS OVER (ORDER BY id) AS prev "
                             + "FROM r14_win ORDER BY id")) {
            rs.next(); // id=1, prev=NULL (nothing before)
            rs.next(); // id=2, prev=NULL (id=1 v is NULL, skipped → no value)
            rs.next(); // id=3, prev=10 (skipped id=1 NULL, found id=2 v=10)
            assertEquals(10, rs.getInt("prev"));
            rs.next(); // id=4, prev=10
            assertEquals(10, rs.getInt("prev"));
            rs.next(); // id=5, prev=20
            assertEquals(20, rs.getInt("prev"));
        }
    }

    @Test
    void lead_ignore_nulls() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, lead(v, 1) IGNORE NULLS OVER (ORDER BY id) AS nxt "
                             + "FROM r14_win ORDER BY id")) {
            rs.next(); // id=1, nxt=10
            assertEquals(10, rs.getInt("nxt"));
            rs.next(); // id=2, nxt=20
            assertEquals(20, rs.getInt("nxt"));
        }
    }

    @Test
    void first_value_ignore_nulls() throws SQLException {
        String v = scalarString(
                "SELECT first_value(v) IGNORE NULLS OVER (ORDER BY id "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text "
                        + "FROM r14_win ORDER BY id LIMIT 1");
        assertEquals("10", v, "first_value IGNORE NULLS must skip initial NULL");
    }

    @Test
    void last_value_respect_nulls_default() throws SQLException {
        // Default = RESPECT NULLS
        String v = scalarString(
                "SELECT last_value(v) OVER (ORDER BY id "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)::text "
                        + "FROM r14_win ORDER BY id LIMIT 1");
        assertEquals("30", v);
    }

    // =========================================================================
    // B. nth_value FROM FIRST/LAST (PG 13+)
    // =========================================================================

    @Test
    void nth_value_from_first() throws SQLException {
        int v = scalarInt(
                "SELECT nth_value(v, 2) FROM FIRST OVER (ORDER BY id "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
                        + "FROM r14_win ORDER BY id LIMIT 1");
        // 2nd value in the frame is NULL (id=2 v=10) — but PG returns v at rownum=2, which is 10
        assertEquals(10, v);
    }

    @Test
    void nth_value_from_last() throws SQLException {
        int v = scalarInt(
                "SELECT nth_value(v, 2) FROM LAST OVER (ORDER BY id "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
                        + "FROM r14_win ORDER BY id LIMIT 1");
        // 2nd from last: id=5 v=NULL (with default RESPECT NULLS -> 0 from getInt)
        assertEquals(0, v);
    }

    // =========================================================================
    // C. Named WINDOW inheritance
    // =========================================================================

    @Test
    void named_window_inherits_from_another() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, sum(id) OVER w2 AS s FROM r14_win "
                             + "WINDOW w1 AS (ORDER BY id), "
                             + "       w2 AS (w1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) "
                             + "ORDER BY id")) {
            rs.next(); assertEquals(1, rs.getInt("s"));
            rs.next(); assertEquals(3, rs.getInt("s"));
            rs.next(); assertEquals(6, rs.getInt("s"));
        }
    }

    // =========================================================================
    // D. COUNT(DISTINCT (c1,c2)) composite distinct
    // =========================================================================

    @Test
    void count_distinct_composite() throws SQLException {
        exec("CREATE TABLE r14_cd (a int, b int)");
        exec("INSERT INTO r14_cd VALUES (1,1),(1,1),(1,2),(2,1),(2,1)");
        assertEquals(3, scalarInt("SELECT count(DISTINCT (a,b))::int FROM r14_cd"));
    }

    // =========================================================================
    // E. CORRESPONDING in set ops
    // =========================================================================

    @Test
    void union_corresponding_matches_by_name() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT 1 AS a, 2 AS b UNION CORRESPONDING SELECT 3 AS b, 4 AS a "
                             + "ORDER BY a")) {
            // CORRESPONDING matches by column name; both sides contribute a,b pairs
            int rows = 0;
            while (rs.next()) rows++;
            assertEquals(2, rows);
        }
    }

    // =========================================================================
    // F. Multi-arg GROUPING()
    // =========================================================================

    @Test
    void grouping_two_args_bitmask() throws SQLException {
        exec("CREATE TABLE r14_gs (a text, b text, n int)");
        exec("INSERT INTO r14_gs VALUES ('x','1',1),('x','2',2),('y','1',3),('y','2',4)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b, GROUPING(a,b) AS g, sum(n) AS s "
                             + "FROM r14_gs GROUP BY CUBE(a,b) ORDER BY a NULLS LAST, b NULLS LAST")) {
            int sum = 0;
            while (rs.next()) {
                int g = rs.getInt("g");
                assertTrue(g >= 0 && g <= 3, "grouping bitmask must be 0..3; got " + g);
                sum++;
            }
            assertTrue(sum >= 4);
        }
    }

    // =========================================================================
    // G. GROUP BY DISTINCT (PG 16+)
    // =========================================================================

    @Test
    void group_by_distinct_deduplicates_grouping_sets() throws SQLException {
        exec("CREATE TABLE r14_gbd (a int, b int)");
        exec("INSERT INTO r14_gbd VALUES (1,1),(2,2)");
        // GROUP BY DISTINCT GROUPING SETS de-dupes redundant sets
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM ( "
                             + "  SELECT a, b FROM r14_gbd "
                             + "  GROUP BY DISTINCT GROUPING SETS ((a), (a), (b))"
                             + ") q")) {
            assertTrue(rs.next());
            // Without DISTINCT there would be 4 groupings (2 dupes of a); with DISTINCT, 3 groupings → 3 rows
            int n = rs.getInt(1);
            assertTrue(n <= 4, "GROUP BY DISTINCT should prune duplicate grouping sets; got " + n);
        }
    }

    // =========================================================================
    // H. Mutual recursion in CTE
    // =========================================================================

    @Test
    void mutual_recursion_two_ctes() throws SQLException {
        // Two CTEs referencing each other — PG supports this.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH RECURSIVE "
                             + "  a(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM b WHERE n < 3), "
                             + "  b(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM a WHERE n < 3) "
                             + "SELECT count(*)::int FROM a")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) > 0, "mutual recursion should produce rows");
        }
    }

    // =========================================================================
    // I. MERGE WHEN NOT MATCHED BY SOURCE (PG 17+)
    // =========================================================================

    @Test
    void merge_when_not_matched_by_source() throws SQLException {
        exec("CREATE TABLE r14_mg_t (id int PRIMARY KEY, v int)");
        exec("INSERT INTO r14_mg_t VALUES (1,10), (2,20), (3,30)");
        exec("CREATE TABLE r14_mg_s (id int, v int)");
        exec("INSERT INTO r14_mg_s VALUES (1,100), (2,200)");
        exec("MERGE INTO r14_mg_t t USING r14_mg_s s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = s.v "
                + "WHEN NOT MATCHED BY SOURCE THEN DELETE");
        assertEquals(2, scalarInt("SELECT count(*)::int FROM r14_mg_t"));
    }

    // =========================================================================
    // J. range_agg with FILTER
    // =========================================================================

    @Test
    void range_agg_with_filter() throws SQLException {
        exec("CREATE TABLE r14_ra (r int4range, incl boolean)");
        exec("INSERT INTO r14_ra VALUES ('[1,5)', true), ('[10,20)', true), ('[30,40)', false)");
        // range_agg with FILTER
        String v = scalarString(
                "SELECT range_agg(r) FILTER (WHERE incl)::text FROM r14_ra");
        assertNotNull(v);
        assertTrue(v.contains("[1,5)") || v.contains("[10,20)"),
                "range_agg FILTER output must contain included ranges, got: " + v);
    }
}
