package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category E: Set operators + WITH ORDINALITY.
 *
 * Covers:
 *  - INTERSECT ALL / EXCEPT ALL semantics (bag vs set)
 *  - DISTINCT ON + GROUP BY combined (should error per PG)
 *  - ROWS FROM (…) WITH ORDINALITY — emits ordinal column
 *  - WITH ORDINALITY on a single SRF (LATERAL context)
 */
class Round15SetOpsOrdinalityTest {

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

    // =========================================================================
    // A. INTERSECT ALL / EXCEPT ALL
    // =========================================================================

    @Test
    void intersect_all_keeps_duplicate_rows_counted_min() throws SQLException {
        // {1,1,1,2} INTERSECT ALL {1,1,2,3} → {1,1,2}
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT x FROM (VALUES (1),(1),(1),(2)) v(x) "
                             + "INTERSECT ALL "
                             + "SELECT x FROM (VALUES (1),(1),(2),(3)) w(x)")) {
            int count1 = 0, count2 = 0;
            while (rs.next()) {
                int x = rs.getInt(1);
                if (x == 1) count1++;
                if (x == 2) count2++;
            }
            assertEquals(2, count1, "INTERSECT ALL must keep min(3,2)=2 copies of 1");
            assertEquals(1, count2, "INTERSECT ALL must keep min(1,1)=1 copy of 2");
        }
    }

    @Test
    void intersect_distinct_removes_duplicates() throws SQLException {
        int n = scalarInt(
                "SELECT count(*)::int FROM ("
                        + "SELECT x FROM (VALUES (1),(1),(2)) v(x) "
                        + "INTERSECT "
                        + "SELECT x FROM (VALUES (1),(2),(3)) w(x)"
                        + ") sub");
        assertEquals(2, n, "INTERSECT (no ALL) should dedup to {1,2}");
    }

    @Test
    void except_all_counts_diff() throws SQLException {
        // {1,1,1,2} EXCEPT ALL {1,2} → {1,1}
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT x FROM (VALUES (1),(1),(1),(2)) v(x) "
                             + "EXCEPT ALL "
                             + "SELECT x FROM (VALUES (1),(2)) w(x)")) {
            int count1 = 0, count2 = 0;
            while (rs.next()) {
                int x = rs.getInt(1);
                if (x == 1) count1++;
                if (x == 2) count2++;
            }
            assertEquals(2, count1, "EXCEPT ALL must keep max(3-1,0)=2 copies of 1");
            assertEquals(0, count2, "EXCEPT ALL must remove 2");
        }
    }

    @Test
    void except_distinct_dedups() throws SQLException {
        int n = scalarInt(
                "SELECT count(*)::int FROM ("
                        + "SELECT x FROM (VALUES (1),(1),(1),(2)) v(x) "
                        + "EXCEPT "
                        + "SELECT x FROM (VALUES (2)) w(x)"
                        + ") sub");
        assertEquals(1, n, "EXCEPT dedups → just {1}");
    }

    // =========================================================================
    // B. DISTINCT ON + GROUP BY — error per PG
    // =========================================================================

    @Test
    void distinct_on_with_group_by_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r15_doonly (a int, b int)");
            s.execute("INSERT INTO r15_doonly VALUES (1,1),(1,2),(2,3)");
        }
        try {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT DISTINCT ON (a) a, count(*) FROM r15_doonly GROUP BY a")) {
                // consume
                while (rs.next()) { /* drain */ }
            }
            fail("DISTINCT ON + GROUP BY combination must error per PG");
        } catch (SQLException e) {
            // expected: error about GROUP BY / aggregate with DISTINCT ON
            assertNotNull(e.getMessage());
        }
    }

    // =========================================================================
    // C. ROWS FROM (…) WITH ORDINALITY
    // =========================================================================

    @Test
    void rows_from_with_ordinality_emits_ordinal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM ROWS FROM (generate_series(10,12)) WITH ORDINALITY AS t(val, ord)")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("val"));
            assertEquals(1, rs.getInt("ord"));
            assertTrue(rs.next());
            assertEquals(11, rs.getInt("val"));
            assertEquals(2, rs.getInt("ord"));
            assertTrue(rs.next());
            assertEquals(12, rs.getInt("val"));
            assertEquals(3, rs.getInt("ord"));
        }
    }

    @Test
    void srf_with_ordinality_standalone() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM generate_series(100, 102) WITH ORDINALITY AS t(val, ord)")) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                int ord = rs.getInt("ord");
                assertEquals(rowCount, ord,
                        "ordinal column must be 1-based sequential");
            }
            assertEquals(3, rowCount);
        }
    }

    @Test
    void rows_from_two_srfs() throws SQLException {
        // ROWS FROM with multiple SRFs runs them side-by-side (padded with NULL)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM ROWS FROM "
                             + "(generate_series(1,3), generate_series(10,11)) AS t(a, b)")) {
            int rowCount = 0;
            while (rs.next()) rowCount++;
            assertEquals(3, rowCount,
                    "ROWS FROM with different-length SRFs pads to max length (3)");
        }
    }

    // =========================================================================
    // D. Set op parenthesization
    // =========================================================================

    @Test
    void union_all_in_subquery() throws SQLException {
        int n = scalarInt(
                "SELECT count(*)::int FROM ("
                        + "(SELECT 1 UNION ALL SELECT 2) "
                        + "UNION ALL "
                        + "(SELECT 3 UNION ALL SELECT 4)"
                        + ") sub");
        assertEquals(4, n, "Parenthesized UNION ALL should total 4");
    }

    @Test
    void union_with_order_by_and_limit() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT x FROM (VALUES (3),(1),(2)) v(x) "
                             + "UNION ALL SELECT x FROM (VALUES (2)) w(x) "
                             + "ORDER BY x LIMIT 2")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next(), "LIMIT 2 should cut off after 2 rows");
        }
    }
}
