package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests RETURNING OLD.* / RETURNING NEW.* (PG 18 feature).
 *
 * PG 18 enhances RETURNING for INSERT/UPDATE/DELETE to support:
 *   RETURNING OLD.*      - pre-modification values
 *   RETURNING NEW.*      - post-modification values
 *   RETURNING OLD.col, NEW.col  - mixed access
 *
 * For INSERT: OLD.* is all NULLs, NEW.* is the inserted row.
 * For UPDATE: OLD.* is the pre-update row, NEW.* is the post-update row.
 * For DELETE: OLD.* is the deleted row, NEW.* is all NULLs.
 *
 * Memgres: Standard RETURNING works, but OLD/NEW qualifiers are not implemented.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class ReturningOldNewTest {

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
        exec("DROP TABLE IF EXISTS ron_test");
        exec("CREATE TABLE ron_test (id int PRIMARY KEY, val text, score int)");
        exec("INSERT INTO ron_test VALUES (1, 'alpha', 10), (2, 'beta', 20)");
    }

    // -------------------------------------------------------------------------
    // INSERT RETURNING NEW.*
    // -------------------------------------------------------------------------

    @Test
    void insert_returningNew_shouldReturnInsertedRow() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "INSERT INTO ron_test VALUES (3, 'gamma', 30) RETURNING NEW.*")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("gamma", rs.getString("val"));
            assertEquals(30, rs.getInt("score"));
            assertFalse(rs.next());
        }
    }

    // -------------------------------------------------------------------------
    // INSERT RETURNING OLD.* (should be all NULLs)
    // -------------------------------------------------------------------------

    @Test
    void insert_returningOld_shouldReturnNulls() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "INSERT INTO ron_test VALUES (4, 'delta', 40) RETURNING OLD.*")) {
            assertTrue(rs.next());
            assertNull(rs.getObject("id"), "OLD.id should be NULL for INSERT");
            assertNull(rs.getObject("val"), "OLD.val should be NULL for INSERT");
            assertNull(rs.getObject("score"), "OLD.score should be NULL for INSERT");
        }
    }

    // -------------------------------------------------------------------------
    // UPDATE RETURNING OLD.*
    // -------------------------------------------------------------------------

    @Test
    void update_returningOld_shouldReturnPreUpdateValues() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "UPDATE ron_test SET val = 'ALPHA', score = 100 "
                             + "WHERE id = 1 RETURNING OLD.*")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("val"), "OLD.val should be pre-update value");
            assertEquals(10, rs.getInt("score"), "OLD.score should be pre-update value");
        }
    }

    // -------------------------------------------------------------------------
    // UPDATE RETURNING NEW.*
    // -------------------------------------------------------------------------

    @Test
    void update_returningNew_shouldReturnPostUpdateValues() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "UPDATE ron_test SET val = 'BETA', score = 200 "
                             + "WHERE id = 2 RETURNING NEW.*")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("BETA", rs.getString("val"), "NEW.val should be post-update value");
            assertEquals(200, rs.getInt("score"), "NEW.score should be post-update value");
        }
    }

    // -------------------------------------------------------------------------
    // UPDATE RETURNING mixed OLD and NEW columns
    // -------------------------------------------------------------------------

    @Test
    void update_returningMixed_oldAndNew() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "UPDATE ron_test SET val = 'ALPHA', score = 999 WHERE id = 1 "
                             + "RETURNING OLD.val AS old_val, NEW.val AS new_val, "
                             + "OLD.score AS old_score, NEW.score AS new_score")) {
            assertTrue(rs.next());
            assertEquals("alpha", rs.getString("old_val"));
            assertEquals("ALPHA", rs.getString("new_val"));
            assertEquals(10, rs.getInt("old_score"));
            assertEquals(999, rs.getInt("new_score"));
        }
    }

    // -------------------------------------------------------------------------
    // DELETE RETURNING OLD.*
    // -------------------------------------------------------------------------

    @Test
    void delete_returningOld_shouldReturnDeletedRow() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "DELETE FROM ron_test WHERE id = 1 RETURNING OLD.*")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("val"));
            assertEquals(10, rs.getInt("score"));
        }
    }

    // -------------------------------------------------------------------------
    // DELETE RETURNING NEW.* (should be all NULLs)
    // -------------------------------------------------------------------------

    @Test
    void delete_returningNew_shouldReturnNulls() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "DELETE FROM ron_test WHERE id = 2 RETURNING NEW.*")) {
            assertTrue(rs.next());
            assertNull(rs.getObject("id"), "NEW.id should be NULL for DELETE");
            assertNull(rs.getObject("val"), "NEW.val should be NULL for DELETE");
            assertNull(rs.getObject("score"), "NEW.score should be NULL for DELETE");
        }
    }
}
