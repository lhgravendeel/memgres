package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests window function EXCLUDE clause support.
 *
 * PG 18 supports four EXCLUDE modes in window frame specifications:
 *   EXCLUDE CURRENT ROW   - excludes the current row from the frame
 *   EXCLUDE GROUP          - excludes the current row and its ORDER BY peers
 *   EXCLUDE TIES           - excludes peers of the current row but keeps current row
 *   EXCLUDE NO OTHERS      - excludes nothing (default)
 *
 * Memgres: The EXCLUDE clause is not implemented. The FrameClause AST has no
 * exclude field, and the window evaluator does not filter frame rows by
 * exclusion mode.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class WindowFrameExcludeTest {

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

    @BeforeEach
    void setup() throws SQLException {
        exec("DROP TABLE IF EXISTS wfe_data");
        exec("CREATE TABLE wfe_data (id int, grp text, val int)");
        exec("INSERT INTO wfe_data VALUES "
                + "(1, 'a', 10), (2, 'a', 20), (3, 'b', 30), "
                + "(4, 'b', 40), (5, 'b', 50)");
    }

    // -------------------------------------------------------------------------
    // EXCLUDE CURRENT ROW
    // -------------------------------------------------------------------------

    @Test
    void excludeCurrentRow_sumShouldExcludeSelf() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, val, "
                             + "sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
                             + "AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS sum_excl "
                             + "FROM wfe_data ORDER BY id")) {
            // Total sum = 10+20+30+40+50 = 150
            // For each row, sum_excl = 150 - val
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(140, rs.getInt("sum_excl"), "150 - 10 = 140");

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(130, rs.getInt("sum_excl"), "150 - 20 = 130");

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals(120, rs.getInt("sum_excl"), "150 - 30 = 120");

            assertTrue(rs.next());
            assertEquals(4, rs.getInt("id"));
            assertEquals(110, rs.getInt("sum_excl"), "150 - 40 = 110");

            assertTrue(rs.next());
            assertEquals(5, rs.getInt("id"));
            assertEquals(100, rs.getInt("sum_excl"), "150 - 50 = 100");
        }
    }

    // -------------------------------------------------------------------------
    // EXCLUDE GROUP (excludes current row and its ORDER BY peers)
    // -------------------------------------------------------------------------

    @Test
    void excludeGroup_shouldExcludeAllPeers() throws SQLException {
        // Using grp as ORDER BY so peers exist
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, grp, val, "
                             + "sum(val) OVER (ORDER BY grp ROWS BETWEEN UNBOUNDED PRECEDING "
                             + "AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS sum_excl "
                             + "FROM wfe_data ORDER BY id")) {
            // Group 'a': ids 1,2 (vals 10,20) -> excluded sum = 30+40+50 = 120
            // Group 'b': ids 3,4,5 (vals 30,40,50) -> excluded sum = 10+20 = 30
            assertTrue(rs.next());
            assertEquals(120, rs.getInt("sum_excl"), "Row 1 (grp=a): exclude group a (10+20)");

            assertTrue(rs.next());
            assertEquals(120, rs.getInt("sum_excl"), "Row 2 (grp=a): exclude group a");

            assertTrue(rs.next());
            assertEquals(30, rs.getInt("sum_excl"), "Row 3 (grp=b): exclude group b (30+40+50)");

            assertTrue(rs.next());
            assertEquals(30, rs.getInt("sum_excl"), "Row 4 (grp=b): exclude group b");

            assertTrue(rs.next());
            assertEquals(30, rs.getInt("sum_excl"), "Row 5 (grp=b): exclude group b");
        }
    }

    // -------------------------------------------------------------------------
    // EXCLUDE TIES (excludes peers but keeps current row)
    // -------------------------------------------------------------------------

    @Test
    void excludeTies_shouldKeepCurrentRowButExcludePeers() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, grp, val, "
                             + "sum(val) OVER (ORDER BY grp ROWS BETWEEN UNBOUNDED PRECEDING "
                             + "AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS sum_excl "
                             + "FROM wfe_data ORDER BY id")) {
            // Group 'a' has 2 members. For id=1 (val=10): exclude id=2 (20) -> 150-20=130
            // For id=2 (val=20): exclude id=1 (10) -> 150-10=140
            // Group 'b' has 3 members. For id=3 (val=30): exclude 4,5 (40+50=90) -> 150-90=60
            assertTrue(rs.next());
            assertEquals(130, rs.getInt("sum_excl"), "Row 1: exclude tie (id=2, val=20)");

            assertTrue(rs.next());
            assertEquals(140, rs.getInt("sum_excl"), "Row 2: exclude tie (id=1, val=10)");

            assertTrue(rs.next());
            assertEquals(60, rs.getInt("sum_excl"), "Row 3: exclude ties (ids 4,5)");

            assertTrue(rs.next());
            assertEquals(70, rs.getInt("sum_excl"), "Row 4: exclude ties (ids 3,5)");

            assertTrue(rs.next());
            assertEquals(80, rs.getInt("sum_excl"), "Row 5: exclude ties (ids 3,4)");
        }
    }

    // -------------------------------------------------------------------------
    // EXCLUDE NO OTHERS (default, should include everything)
    // -------------------------------------------------------------------------

    @Test
    void excludeNoOthers_shouldIncludeEverything() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, "
                             + "sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
                             + "AND UNBOUNDED FOLLOWING EXCLUDE NO OTHERS) AS total "
                             + "FROM wfe_data ORDER BY id")) {
            // Should be identical to no EXCLUDE clause: every row sees sum = 150
            while (rs.next()) {
                assertEquals(150, rs.getInt("total"),
                        "EXCLUDE NO OTHERS should include all rows in frame");
            }
        }
    }

    // -------------------------------------------------------------------------
    // EXCLUDE with count()
    // -------------------------------------------------------------------------

    @Test
    void excludeCurrentRow_withCount() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, "
                             + "count(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
                             + "AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS cnt "
                             + "FROM wfe_data ORDER BY id")) {
            // Total 5 rows, exclude current = 4 for each row
            while (rs.next()) {
                assertEquals(4, rs.getInt("cnt"),
                        "count(*) with EXCLUDE CURRENT ROW should be 4 for each row");
            }
        }
    }
}
