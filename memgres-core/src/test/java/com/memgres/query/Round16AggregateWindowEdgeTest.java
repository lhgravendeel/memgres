package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category L: Aggregate / window edges.
 *
 * Covers:
 *  - FILTER on window aggregates (also H4; duplicated here for area owner)
 *  - array_agg DISTINCT dedup on actual value, not toString()
 *  - percentile_disc/cont array with NaN raises SQLSTATE 22023 (not 22003)
 *  - nth_value(col, n) FROM FIRST / FROM LAST / IGNORE NULLS
 */
class Round16AggregateWindowEdgeTest {

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
    // L1. Window FILTER
    // =========================================================================

    @Test
    void window_count_filter_equals_filtered_count() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_wfx");
        exec("CREATE TABLE r16_wfx (id int, x int)");
        exec("INSERT INTO r16_wfx VALUES (1,5),(2,15),(3,25)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, count(*) FILTER (WHERE x>=15) OVER () AS c " +
                             "FROM r16_wfx ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong("c"),
                    "count(*) FILTER (WHERE x>=15) OVER () must be 2");
        }
    }

    // =========================================================================
    // L2. array_agg DISTINCT dedup key uses actual value, not toString
    // =========================================================================

    @Test
    void array_agg_distinct_dedup_respects_value_identity() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_aad");
        exec("CREATE TABLE r16_aad (a int, b int)");
        // Two distinct composite (a,b) rows
        exec("INSERT INTO r16_aad VALUES (1, NULL)");
        exec("INSERT INTO r16_aad VALUES (NULL, 1)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT array_length(array_agg(DISTINCT ROW(a,b)), 1) FROM r16_aad")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1),
                    "array_agg(DISTINCT ROW(a,b)) must dedup by value, not toString() — " +
                            "both (1,NULL) and (NULL,1) are distinct");
        }
    }

    // =========================================================================
    // L3. percentile_disc / percentile_cont with NaN → SQLSTATE 22023
    // =========================================================================

    @Test
    void percentile_disc_with_nan_raises_22023() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT percentile_disc(ARRAY['nan'::float8, 0.5]) " +
                             "WITHIN GROUP (ORDER BY x) " +
                             "FROM (VALUES (1), (2), (3)) AS t(x)")) {
            if (rs.next()) {
                fail("percentile_disc with NaN must raise SQLSTATE 22023");
            }
        } catch (SQLException e) {
            assertEquals("22023", e.getSQLState(),
                    "percentile with NaN must raise 22023 invalid_parameter_value; got " +
                            e.getSQLState());
        }
    }

    // =========================================================================
    // L4. nth_value FROM FIRST / FROM LAST / IGNORE NULLS
    // =========================================================================

    @Test
    void nth_value_from_last_returns_nth_from_end_of_frame() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_nv");
        exec("CREATE TABLE r16_nv (id int, v int)");
        exec("INSERT INTO r16_nv VALUES (1,10),(2,20),(3,30),(4,40)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, nth_value(v, 2) FROM LAST OVER " +
                             "(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                             "FROM r16_nv ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(2),
                    "nth_value(v, 2) FROM LAST over full frame must be v at second-to-last row = 30");
        }
    }

    @Test
    void nth_value_ignore_nulls_skips_null_rows() throws SQLException {
        exec("DROP TABLE IF EXISTS r16_nvn");
        exec("CREATE TABLE r16_nvn (id int, v int)");
        exec("INSERT INTO r16_nvn VALUES (1, NULL), (2, 10), (3, NULL), (4, 20)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT nth_value(v, 2) IGNORE NULLS OVER " +
                             "(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) " +
                             "FROM r16_nvn ORDER BY id LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1),
                    "nth_value(v,2) IGNORE NULLS must skip null v's — 2nd non-null is 20");
        }
    }
}
