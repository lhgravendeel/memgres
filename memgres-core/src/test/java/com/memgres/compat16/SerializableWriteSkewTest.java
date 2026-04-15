package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests SERIALIZABLE isolation write-skew detection.
 *
 * PG 18 implements true Serializable Snapshot Isolation (SSI) which detects
 * write-skew anomalies and aborts one of the conflicting transactions with
 * SQLSTATE 40001 (serialization_failure).
 *
 * Memgres implements SERIALIZABLE as equivalent to REPEATABLE READ (snapshot
 * isolation) with NO write-skew detection. Both transactions commit successfully,
 * producing an anomalous result that would be impossible under any serial execution.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class SerializableWriteSkewTest {

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
    // Classic write-skew: two doctors on call, both try to take leave
    // -------------------------------------------------------------------------

    @Test
    void writeSkew_doctorsOnCall_shouldDetectConflict() throws SQLException {
        // Setup: two doctors on call
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS doctors");
            setup.createStatement().execute(
                    "CREATE TABLE doctors (name text PRIMARY KEY, on_call boolean)");
            setup.createStatement().execute(
                    "INSERT INTO doctors VALUES ('Alice', true), ('Bob', true)");
            setup.commit();
        }

        Connection txn1 = connect();
        Connection txn2 = connect();

        try {
            // Both transactions start SERIALIZABLE
            txn1.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            txn2.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

            // T1: reads both doctors, sees 2 on call
            try (ResultSet rs = txn1.createStatement().executeQuery(
                    "SELECT count(*) FROM doctors WHERE on_call = true")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // T2: reads both doctors, sees 2 on call
            try (ResultSet rs = txn2.createStatement().executeQuery(
                    "SELECT count(*) FROM doctors WHERE on_call = true")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // T1: takes Alice off call (sees 2 on-call, so 1 remaining is OK)
            txn1.createStatement().execute(
                    "UPDATE doctors SET on_call = false WHERE name = 'Alice'");

            // T2: takes Bob off call (sees 2 on-call, so 1 remaining is OK)
            txn2.createStatement().execute(
                    "UPDATE doctors SET on_call = false WHERE name = 'Bob'");

            // T1 commits first
            txn1.commit();

            // T2 should FAIL with serialization_failure (40001) in PG
            // because the result (0 doctors on call) is impossible under serial execution
            boolean t2Failed = false;
            try {
                txn2.commit();
            } catch (SQLException e) {
                t2Failed = true;
                assertEquals("40001", e.getSQLState(),
                        "Write-skew should produce serialization_failure (40001), got: "
                                + e.getSQLState() + " - " + e.getMessage());
            }

            assertTrue(t2Failed,
                    "T2 commit should have failed with serialization_failure (40001) "
                            + "but both transactions committed successfully, leaving 0 doctors on call");

        } finally {
            txn1.close();
            txn2.close();
        }
    }

    // -------------------------------------------------------------------------
    // Write-skew: balance transfer between two accounts
    // -------------------------------------------------------------------------

    @Test
    void writeSkew_balanceInvariant_shouldDetectConflict() throws SQLException {
        // Setup: two accounts, invariant is sum >= 0
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS accounts");
            setup.createStatement().execute(
                    "CREATE TABLE accounts (name text PRIMARY KEY, balance integer)");
            setup.createStatement().execute(
                    "INSERT INTO accounts VALUES ('checking', 100), ('savings', 100)");
            setup.commit();
        }

        Connection txn1 = connect();
        Connection txn2 = connect();

        try {
            txn1.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            txn2.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");

            // T1: reads total balance (200), withdraws 200 from checking
            try (ResultSet rs = txn1.createStatement().executeQuery(
                    "SELECT sum(balance) FROM accounts")) {
                rs.next();
                assertEquals(200, rs.getInt(1));
            }
            txn1.createStatement().execute(
                    "UPDATE accounts SET balance = balance - 200 WHERE name = 'checking'");

            // T2: reads total balance (200), withdraws 200 from savings
            try (ResultSet rs = txn2.createStatement().executeQuery(
                    "SELECT sum(balance) FROM accounts")) {
                rs.next();
                assertEquals(200, rs.getInt(1));
            }
            txn2.createStatement().execute(
                    "UPDATE accounts SET balance = balance - 200 WHERE name = 'savings'");

            // T1 commits
            txn1.commit();

            // T2 should fail: result would be checking=-100, savings=-100 (sum=-200)
            // which violates the invariant that was checked
            boolean t2Failed = false;
            try {
                txn2.commit();
            } catch (SQLException e) {
                t2Failed = true;
                assertEquals("40001", e.getSQLState(),
                        "Expected serialization_failure, got: " + e.getSQLState());
            }

            assertTrue(t2Failed,
                    "T2 commit should fail with 40001 to prevent write-skew anomaly");

        } finally {
            txn1.close();
            txn2.close();
        }
    }

    // -------------------------------------------------------------------------
    // Read-only transaction should not cause serialization failure
    // -------------------------------------------------------------------------

    @Test
    void serializable_readOnlyTransactions_shouldNotConflict() throws SQLException {
        try (Connection setup = connect()) {
            setup.createStatement().execute("DROP TABLE IF EXISTS serial_ro");
            setup.createStatement().execute(
                    "CREATE TABLE serial_ro (id int PRIMARY KEY, val text)");
            setup.createStatement().execute(
                    "INSERT INTO serial_ro VALUES (1, 'a'), (2, 'b')");
            setup.commit();
        }

        Connection txn1 = connect();
        Connection txn2 = connect();

        try {
            txn1.createStatement().execute(
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");
            txn2.createStatement().execute(
                    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY");

            // Both read the same data
            try (ResultSet rs = txn1.createStatement().executeQuery(
                    "SELECT count(*) FROM serial_ro")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }
            try (ResultSet rs = txn2.createStatement().executeQuery(
                    "SELECT count(*) FROM serial_ro")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // Both should commit successfully (no writes = no conflict)
            txn1.commit();
            txn2.commit();
        } finally {
            txn1.close();
            txn2.close();
        }
    }
}
