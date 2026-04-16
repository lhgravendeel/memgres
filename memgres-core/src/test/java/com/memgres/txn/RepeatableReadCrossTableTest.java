package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests REPEATABLE READ snapshot semantics differences.
 *
 * PG 18: REPEATABLE READ takes a transaction-wide snapshot at the first query.
 * All subsequent reads (including reads of different tables) see data as of that
 * single snapshot point. No phantoms are possible within the transaction.
 *
 * Memgres: REPEATABLE READ takes per-table snapshots on first read of each table.
 * This means reading table A, then table B after another transaction inserts into B,
 * will show the new rows in B (phantom across table boundaries).
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class RepeatableReadCrossTableTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // -------------------------------------------------------------------------
    // Cross-table phantom: RR should not see inserts into other tables
    // -------------------------------------------------------------------------

    @Test
    void repeatableRead_shouldNotSeePhantomInOtherTable() throws SQLException {
        // Setup
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS rr_orders");
            setup.createStatement().execute("DROP TABLE IF EXISTS rr_items");
            setup.createStatement().execute(
                    "CREATE TABLE rr_orders (id int PRIMARY KEY, status text)");
            setup.createStatement().execute(
                    "CREATE TABLE rr_items (id int PRIMARY KEY, order_id int, name text)");
            setup.createStatement().execute(
                    "INSERT INTO rr_orders VALUES (1, 'open')");
            setup.createStatement().execute(
                    "INSERT INTO rr_items VALUES (1, 1, 'widget')");
            setup.commit();
        }

        Connection rrTxn = connect();
        Connection writer = connect();

        try {
            rrTxn.createStatement().execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // RR transaction reads orders table first (establishes snapshot)
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_orders")) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }

            // Another transaction inserts into items and commits
            writer.createStatement().execute(
                    "INSERT INTO rr_items VALUES (2, 1, 'gadget')");
            writer.commit();

            // RR transaction reads items table for the FIRST TIME
            // PG: snapshot was taken at first query, so this sees 1 item (pre-insert)
            // Memgres: per-table snapshot taken now, sees 2 items (post-insert)
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_items")) {
                rs.next();
                int count = rs.getInt(1);
                assertEquals(1, count,
                        "REPEATABLE READ should see consistent snapshot across tables. "
                                + "Expected 1 item (snapshot before insert), got " + count
                                + ". This suggests per-table snapshots instead of transaction-wide.");
            }

            rrTxn.commit();
        } finally {
            rrTxn.close();
            writer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Same-table repeated read should be consistent (this should pass)
    // -------------------------------------------------------------------------

    @Test
    void repeatableRead_sameTable_shouldBeConsistent() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS rr_stable");
            setup.createStatement().execute(
                    "CREATE TABLE rr_stable (id int PRIMARY KEY, val text)");
            setup.createStatement().execute(
                    "INSERT INTO rr_stable VALUES (1, 'a'), (2, 'b')");
            setup.commit();
        }

        Connection rrTxn = connect();
        Connection writer = connect();

        try {
            rrTxn.createStatement().execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // First read
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_stable")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // Writer inserts and commits
            writer.createStatement().execute("INSERT INTO rr_stable VALUES (3, 'c')");
            writer.commit();

            // Second read of same table should still see 2 rows
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_stable")) {
                rs.next();
                assertEquals(2, rs.getInt(1),
                        "Repeated read of same table should be consistent");
            }

            rrTxn.commit();
        } finally {
            rrTxn.close();
            writer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Cross-table join should use consistent snapshot
    // -------------------------------------------------------------------------

    @Test
    void repeatableRead_crossTableJoin_shouldUseConsistentSnapshot() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS rr_parent CASCADE");
            setup.createStatement().execute("DROP TABLE IF EXISTS rr_child CASCADE");
            setup.createStatement().execute(
                    "CREATE TABLE rr_parent (id int PRIMARY KEY, name text)");
            setup.createStatement().execute(
                    "CREATE TABLE rr_child (id int PRIMARY KEY, parent_id int, label text)");
            setup.createStatement().execute("INSERT INTO rr_parent VALUES (1, 'P1')");
            setup.createStatement().execute(
                    "INSERT INTO rr_child VALUES (1, 1, 'C1')");
            setup.commit();
        }

        Connection rrTxn = connect();
        Connection writer = connect();

        try {
            rrTxn.createStatement().execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

            // Read parent (establishes snapshot in PG)
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_parent")) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }

            // Another transaction adds a parent and child, commits
            writer.createStatement().execute("INSERT INTO rr_parent VALUES (2, 'P2')");
            writer.createStatement().execute(
                    "INSERT INTO rr_child VALUES (2, 2, 'C2')");
            writer.commit();

            // Cross-table join should use the snapshot from first query
            // PG: sees 1 parent + 1 child = 1 joined row
            // Memgres: might see 2 parents + 2 children if child table gets fresh snapshot
            try (ResultSet rs = rrTxn.createStatement().executeQuery(
                    "SELECT count(*) FROM rr_parent p JOIN rr_child c ON p.id = c.parent_id")) {
                rs.next();
                int count = rs.getInt(1);
                assertEquals(1, count,
                        "REPEATABLE READ cross-table join should use consistent snapshot. "
                                + "Expected 1 joined row, got " + count);
            }

            rrTxn.commit();
        } finally {
            rrTxn.close();
            writer.close();
        }
    }
}
