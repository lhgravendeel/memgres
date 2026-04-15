package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests system column behavior differences between Memgres and PostgreSQL 18.
 *
 * PG 18 provides system columns on every table: ctid, xmin, xmax, cmin, cmax, tableoid.
 * Memgres gaps:
 *   - ctid: returns fixed "(0,0)" for all rows instead of actual physical tuple ID
 *   - xmin: throws error 42703 (column not found) instead of returning inserting txn ID
 *   - xmax: throws error 42703 instead of returning deleting/locking txn ID
 *   - cmin: throws error 42703 instead of returning command ID within inserting txn
 *   - cmax: throws error 42703 instead of returning command ID within deleting txn
 *   - tableoid: works correctly
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class SystemColumnCompatTest {

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
    void createTable() throws SQLException {
        exec("DROP TABLE IF EXISTS syscol_test");
        exec("CREATE TABLE syscol_test (id integer PRIMARY KEY, val text)");
        exec("INSERT INTO syscol_test VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
    }

    // -------------------------------------------------------------------------
    // ctid should return distinct physical tuple IDs, not a fixed value
    // -------------------------------------------------------------------------

    @Test
    void ctid_shouldReturnDistinctValuesPerRow() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT ctid, id FROM syscol_test ORDER BY id")) {
            assertTrue(rs.next());
            String ctid1 = rs.getString("ctid");
            assertTrue(rs.next());
            String ctid2 = rs.getString("ctid");
            assertTrue(rs.next());
            String ctid3 = rs.getString("ctid");

            // In PG, each row has a distinct ctid like (0,1), (0,2), (0,3)
            // Memgres returns "(0,0)" for all rows
            assertNotEquals(ctid1, ctid2,
                    "Each row should have a distinct ctid; got " + ctid1 + " and " + ctid2);
            assertNotEquals(ctid2, ctid3,
                    "Each row should have a distinct ctid; got " + ctid2 + " and " + ctid3);
        }
    }

    @Test
    void ctid_shouldChangeAfterUpdate() throws SQLException {
        String ctidBefore;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT ctid FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            ctidBefore = rs.getString("ctid");
        }

        exec("UPDATE syscol_test SET val = 'ALPHA' WHERE id = 1");

        String ctidAfter;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT ctid FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            ctidAfter = rs.getString("ctid");
        }

        // In PG, an UPDATE creates a new tuple so ctid changes (HOT updates aside)
        // Memgres always returns "(0,0)"
        assertNotEquals(ctidBefore, ctidAfter,
                "ctid should change after UPDATE; both are " + ctidBefore);
    }

    // -------------------------------------------------------------------------
    // xmin should be queryable and return a transaction ID
    // -------------------------------------------------------------------------

    @Test
    void xmin_shouldBeQueryable() throws SQLException {
        // PG: SELECT xmin FROM table returns the inserting transaction's ID
        // Memgres: throws 42703 "column xmin does not exist"
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT xmin, id FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            long xmin = rs.getLong("xmin");
            assertTrue(xmin > 0, "xmin should be a positive transaction ID, got " + xmin);
        }
    }

    @Test
    void xmin_shouldBeSameWithinTransaction() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO syscol_test VALUES (10, 'ten')");
            exec("INSERT INTO syscol_test VALUES (11, 'eleven')");
            conn.commit();

            // Rows inserted in same transaction should have same xmin
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT xmin FROM syscol_test WHERE id IN (10, 11) ORDER BY id")) {
                assertTrue(rs.next());
                long xmin1 = rs.getLong("xmin");
                assertTrue(rs.next());
                long xmin2 = rs.getLong("xmin");
                assertEquals(xmin1, xmin2,
                        "Rows inserted in same transaction should have same xmin");
            }
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // -------------------------------------------------------------------------
    // xmax should be queryable
    // -------------------------------------------------------------------------

    @Test
    void xmax_shouldBeZeroForLiveRows() throws SQLException {
        // PG: xmax = 0 for rows that haven't been deleted or locked
        // Memgres: throws 42703
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT xmax, id FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            long xmax = rs.getLong("xmax");
            assertEquals(0, xmax, "xmax should be 0 for live, unlocked rows");
        }
    }

    // -------------------------------------------------------------------------
    // cmin and cmax should be queryable
    // -------------------------------------------------------------------------

    @Test
    void cmin_shouldBeQueryable() throws SQLException {
        // PG: cmin is the command ID within the inserting transaction
        // Memgres: throws 42703
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT cmin, id FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            int cmin = rs.getInt("cmin");
            assertTrue(cmin >= 0, "cmin should be a non-negative command ID, got " + cmin);
        }
    }

    @Test
    void cmax_shouldBeQueryable() throws SQLException {
        // PG: cmax is the command ID within the deleting transaction
        // Memgres: throws 42703
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT cmax, id FROM syscol_test WHERE id = 1")) {
            assertTrue(rs.next());
            int cmax = rs.getInt("cmax");
            assertTrue(cmax >= 0, "cmax should be a non-negative command ID, got " + cmax);
        }
    }

    // -------------------------------------------------------------------------
    // System columns in WHERE clauses
    // -------------------------------------------------------------------------

    @Test
    void ctid_shouldBeUsableInWhere() throws SQLException {
        // PG: SELECT * FROM t WHERE ctid = '(0,1)' returns that specific row
        // Memgres: may work but returns wrong results since ctid is always "(0,0)"
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT ctid FROM syscol_test ORDER BY id LIMIT 1")) {
            assertTrue(rs.next());
            String firstCtid = rs.getString(1);

            // Now use that ctid to fetch exactly one row
            try (ResultSet rs2 = s.executeQuery(
                    "SELECT id FROM syscol_test WHERE ctid = '" + firstCtid + "'")) {
                int count = 0;
                while (rs2.next()) count++;
                assertEquals(1, count,
                        "WHERE ctid = '<specific>' should return exactly 1 row, got " + count);
            }
        }
    }
}
