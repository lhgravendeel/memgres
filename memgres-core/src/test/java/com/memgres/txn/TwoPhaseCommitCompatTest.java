package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests two-phase commit (PREPARE TRANSACTION / COMMIT PREPARED) behavior.
 *
 * PG 18: PREPARE TRANSACTION persists the transaction state so it survives
 * crashes. The prepared transaction is visible in pg_prepared_xacts and can
 * be committed or rolled back by any session. The preparing session's
 * connection is released.
 *
 * Memgres: PREPARE TRANSACTION, COMMIT PREPARED, and ROLLBACK PREPARED are
 * silently no-op'd (parsed and accepted but have no effect). The transaction
 * is not actually prepared, and pg_prepared_xacts is empty.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class TwoPhaseCommitCompatTest {

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

    // -------------------------------------------------------------------------
    // PREPARE TRANSACTION should make data visible in pg_prepared_xacts
    // -------------------------------------------------------------------------

    @Test
    void prepareTransaction_shouldBeVisibleInCatalog() throws SQLException {
        exec("DROP TABLE IF EXISTS tpc_test");
        exec("CREATE TABLE tpc_test (id int PRIMARY KEY, val text)");

        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO tpc_test VALUES (1, 'prepared')");
            exec("PREPARE TRANSACTION 'tpc_test_txn'");

            // In PG, after PREPARE TRANSACTION the connection is no longer in a transaction
            // and the prepared txn is visible in pg_prepared_xacts
            conn.setAutoCommit(true);

            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT gid FROM pg_prepared_xacts WHERE gid = 'tpc_test_txn'")) {
                assertTrue(rs.next(),
                        "PREPARE TRANSACTION should make txn visible in pg_prepared_xacts. "
                                + "Memgres silently no-ops this command.");
                assertEquals("tpc_test_txn", rs.getString(1));
            }

            // Commit the prepared transaction
            exec("COMMIT PREPARED 'tpc_test_txn'");

            // Data should now be visible
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT val FROM tpc_test WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("prepared", rs.getString(1));
            }
        } catch (SQLException e) {
            // If PREPARE TRANSACTION failed, try to rollback normally
            try { conn.rollback(); } catch (SQLException ignored) {}
            conn.setAutoCommit(true);
            throw e;
        }

        exec("DROP TABLE tpc_test");
    }

    // -------------------------------------------------------------------------
    // COMMIT PREPARED from a different session
    // -------------------------------------------------------------------------

    @Test
    void commitPrepared_fromDifferentSession_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS tpc_cross");
        exec("CREATE TABLE tpc_cross (id int PRIMARY KEY, val text)");

        // Session 1: prepare the transaction
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO tpc_cross VALUES (1, 'cross-session')");
            exec("PREPARE TRANSACTION 'tpc_cross_txn'");
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        }
        conn.setAutoCommit(true);

        // Session 2: commit the prepared transaction
        try (Connection c2 = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            c2.setAutoCommit(true);
            c2.createStatement().execute("COMMIT PREPARED 'tpc_cross_txn'");
        }

        // Data should be committed
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT val FROM tpc_cross WHERE id = 1")) {
            assertTrue(rs.next(),
                    "Data should be visible after COMMIT PREPARED from another session");
            assertEquals("cross-session", rs.getString(1));
        }

        exec("DROP TABLE tpc_cross");
    }

    // -------------------------------------------------------------------------
    // ROLLBACK PREPARED should discard the data
    // -------------------------------------------------------------------------

    @Test
    void rollbackPrepared_shouldDiscardData() throws SQLException {
        exec("DROP TABLE IF EXISTS tpc_rb");
        exec("CREATE TABLE tpc_rb (id int PRIMARY KEY, val text)");
        exec("INSERT INTO tpc_rb VALUES (1, 'existing')");

        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO tpc_rb VALUES (2, 'to_be_rolled_back')");
            exec("PREPARE TRANSACTION 'tpc_rb_txn'");
        } catch (SQLException e) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            throw e;
        }
        conn.setAutoCommit(true);

        exec("ROLLBACK PREPARED 'tpc_rb_txn'");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM tpc_rb")) {
            rs.next();
            assertEquals(1, rs.getInt(1),
                    "ROLLBACK PREPARED should discard the inserted row");
        }

        exec("DROP TABLE tpc_rb");
    }
}
