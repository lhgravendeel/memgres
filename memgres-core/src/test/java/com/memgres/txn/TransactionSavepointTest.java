package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 4 (Java/JDBC): Transaction and savepoint recovery.
 * Tests manual commit, statement failure in transaction, setSavepoint,
 * rollback(savepoint), releaseSavepoint, DDL in transaction + rollback,
 * temp-table behavior across commit/rollback.
 */
class TransactionSavepointTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // --- Basic transaction commit/rollback ---

    @Test void commit_persists_data() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_c1(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO tx_c1 VALUES (1)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_c1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            c.createStatement().execute("DROP TABLE tx_c1");
            c.commit();
        }
    }

    @Test void rollback_discards_data() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_r1(id int PRIMARY KEY)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_r1 VALUES (1)");
            c.rollback();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_r1")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Rolled back insert should not persist");
            }
            c.createStatement().execute("DROP TABLE tx_r1");
            c.commit();
        }
    }

    // --- Statement failure in transaction ---

    @Test void error_aborts_transaction_in_pg() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_err(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO tx_err VALUES (1)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_err VALUES (2)");
            // This should fail (duplicate key)
            try {
                c.createStatement().execute("INSERT INTO tx_err VALUES (1)");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }
            // In PG, after an error the transaction is aborted; any statement should fail
            try {
                c.createStatement().executeQuery("SELECT 1");
            } catch (SQLException e) {
                // PG returns 25P02 "current transaction is aborted"
                assertEquals("25P02", e.getSQLState(),
                        "Transaction should be aborted after error");
            }
            c.rollback();
            c.createStatement().execute("DROP TABLE tx_err");
            c.commit();
        }
    }

    // --- Savepoints ---

    @Test void savepoint_basic_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_sp1(id int)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_sp1 VALUES (1)");
            Savepoint sp = c.setSavepoint("sp1");
            c.createStatement().execute("INSERT INTO tx_sp1 VALUES (2)");
            c.rollback(sp);
            c.commit();
            // Row 1 should exist, row 2 should not
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT id FROM tx_sp1 ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next(), "Row 2 should have been rolled back");
            }
            c.createStatement().execute("DROP TABLE tx_sp1");
            c.commit();
        }
    }

    @Test void savepoint_release_and_commit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_sp2(id int)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_sp2 VALUES (1)");
            Savepoint sp = c.setSavepoint("sp2");
            c.createStatement().execute("INSERT INTO tx_sp2 VALUES (2)");
            c.releaseSavepoint(sp);
            c.commit();
            // Both rows should exist after release + commit
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_sp2")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            c.createStatement().execute("DROP TABLE tx_sp2");
            c.commit();
        }
    }

    @Test void nested_savepoints() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_sp3(id int)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_sp3 VALUES (1)");
            Savepoint sp1 = c.setSavepoint("sp1");
            c.createStatement().execute("INSERT INTO tx_sp3 VALUES (2)");
            Savepoint sp2 = c.setSavepoint("sp2");
            c.createStatement().execute("INSERT INTO tx_sp3 VALUES (3)");
            c.rollback(sp2); // rolls back row 3
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_sp3")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Only rows 1 and 2 should remain");
            }
            c.createStatement().execute("DROP TABLE tx_sp3");
            c.commit();
        }
    }

    @Test void savepoint_error_recovery() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_sp4(id int PRIMARY KEY)");
            c.commit();
            c.createStatement().execute("INSERT INTO tx_sp4 VALUES (1)");
            Savepoint sp = c.setSavepoint("sp_err");
            try {
                c.createStatement().execute("INSERT INTO tx_sp4 VALUES (1)"); // duplicate
            } catch (SQLException ignored) {}
            c.rollback(sp);
            // After rolling back to savepoint, should be able to continue
            c.createStatement().execute("INSERT INTO tx_sp4 VALUES (2)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_sp4")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            c.createStatement().execute("DROP TABLE tx_sp4");
            c.commit();
        }
    }

    // --- DDL in transaction + rollback ---

    @Test void ddl_in_transaction_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_ddl1(id int)");
            c.commit();
            c.createStatement().execute("ALTER TABLE tx_ddl1 ADD COLUMN v text");
            c.rollback();
            // After rollback, the column should not exist
            try (ResultSet rs = c.createStatement().executeQuery("SELECT * FROM tx_ddl1")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(1, md.getColumnCount(), "Rolled-back ALTER should leave only 1 column");
            }
            c.createStatement().execute("DROP TABLE tx_ddl1");
            c.commit();
        }
    }

    @Test void create_table_in_transaction_rollback() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE tx_ddl2(id int)");
            c.rollback();
            // Table should not exist after rollback
            assertThrows(SQLException.class, () -> {
                c.createStatement().executeQuery("SELECT * FROM tx_ddl2");
            });
            c.rollback();
        }
    }

    // --- Temp tables across commit/rollback ---

    @Test void temp_table_survives_commit() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TEMP TABLE tx_temp1(id int)");
            c.createStatement().execute("INSERT INTO tx_temp1 VALUES (1)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_temp1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Temp table and data should survive commit");
            }
            c.createStatement().execute("DROP TABLE tx_temp1");
            c.commit();
        }
    }

    @Test void temp_table_on_commit_delete_rows() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TEMP TABLE tx_temp2(id int) ON COMMIT DELETE ROWS");
            c.createStatement().execute("INSERT INTO tx_temp2 VALUES (1),(2),(3)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM tx_temp2")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "ON COMMIT DELETE ROWS should clear data");
            }
            c.createStatement().execute("DROP TABLE tx_temp2");
            c.commit();
        }
    }

    @Test void temp_table_on_commit_drop() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TEMP TABLE tx_temp3(id int) ON COMMIT DROP");
            c.createStatement().execute("INSERT INTO tx_temp3 VALUES (1)");
            c.commit();
            assertThrows(SQLException.class, () -> {
                c.createStatement().executeQuery("SELECT * FROM tx_temp3");
            });
            c.rollback();
        }
    }

    // --- Autocommit behavior ---

    @Test void autocommit_each_statement_committed() throws Exception {
        try (Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            c.setAutoCommit(true);
            c.createStatement().execute("CREATE TABLE tx_auto(id int)");
            c.createStatement().execute("INSERT INTO tx_auto VALUES (1)");
            // Open new connection to verify persistence
            try (Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
                c2.setAutoCommit(true);
                try (ResultSet rs = c2.createStatement().executeQuery("SELECT count(*) FROM tx_auto")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
            c.createStatement().execute("DROP TABLE tx_auto");
        }
    }
}
