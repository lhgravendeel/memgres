package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that COMMIT in an aborted (FAILED) transaction acts as ROLLBACK,
 * matching PostgreSQL behavior. PG issues: "WARNING: transaction was committed
 * but contained errors; COMMIT processed as ROLLBACK".
 */
class AbortedTransactionCommitTest {

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

    @Test void commit_after_error_rolls_back_insert() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t1(id int PRIMARY KEY)");
            c.commit();

            c.createStatement().execute("INSERT INTO atc_t1 VALUES (1)");
            // Cause an error to put txn in FAILED state
            try {
                c.createStatement().execute("SELECT 1/0");
                fail("Expected division by zero error");
            } catch (SQLException e) {
                // expected — txn is now aborted
            }
            // COMMIT should act as ROLLBACK
            c.commit();

            // The insert should NOT have persisted
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM atc_t1")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "COMMIT after error should roll back; insert must not persist");
            }
            c.createStatement().execute("DROP TABLE atc_t1");
            c.commit();
        }
    }

    @Test void commit_after_error_rolls_back_update() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t2(id int PRIMARY KEY, val text)");
            c.createStatement().execute("INSERT INTO atc_t2 VALUES (1, 'original')");
            c.commit();

            c.createStatement().execute("UPDATE atc_t2 SET val = 'modified' WHERE id = 1");
            try {
                c.createStatement().execute("SELECT 1/0");
            } catch (SQLException ignored) {}
            c.commit();

            try (ResultSet rs = c.createStatement().executeQuery("SELECT val FROM atc_t2 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("original", rs.getString(1), "Update should be rolled back after error + COMMIT");
            }
            c.createStatement().execute("DROP TABLE atc_t2");
            c.commit();
        }
    }

    @Test void commit_after_error_rolls_back_delete() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t3(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO atc_t3 VALUES (1), (2), (3)");
            c.commit();

            c.createStatement().execute("DELETE FROM atc_t3 WHERE id = 2");
            try {
                c.createStatement().execute("SELECT 1/0");
            } catch (SQLException ignored) {}
            c.commit();

            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM atc_t3")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "Delete should be rolled back after error + COMMIT");
            }
            c.createStatement().execute("DROP TABLE atc_t3");
            c.commit();
        }
    }

    @Test void commit_after_constraint_violation_rolls_back() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t4(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO atc_t4 VALUES (1)");
            c.commit();

            c.createStatement().execute("INSERT INTO atc_t4 VALUES (2)");
            // Trigger PK violation to abort txn
            try {
                c.createStatement().execute("INSERT INTO atc_t4 VALUES (1)");
                fail("Expected unique violation");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }
            c.commit();

            // Only the original row should exist
            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM atc_t4")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Both inserts should be rolled back after constraint error + COMMIT");
            }
            c.createStatement().execute("DROP TABLE atc_t4");
            c.commit();
        }
    }

    @Test void commit_after_error_rolls_back_ddl() throws Exception {
        try (Connection c = newConn()) {
            // Create table inside txn, then error, then COMMIT — table should not exist
            c.createStatement().execute("CREATE TABLE atc_t5_should_not_exist(id int)");
            try {
                c.createStatement().execute("SELECT 1/0");
            } catch (SQLException ignored) {}
            c.commit();

            // Table should not exist
            try {
                c.createStatement().executeQuery("SELECT 1 FROM atc_t5_should_not_exist LIMIT 1");
                fail("Table should not exist after aborted transaction COMMIT");
            } catch (SQLException e) {
                // expected: relation does not exist
                assertTrue(e.getMessage().contains("does not exist") || e.getMessage().contains("not found"),
                        "Expected 'does not exist' error, got: " + e.getMessage());
            }
            c.rollback(); // clean up failed state
        }
    }

    @Test void multiple_statements_before_error_all_rolled_back() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t6(id int PRIMARY KEY, val int)");
            c.commit();

            c.createStatement().execute("INSERT INTO atc_t6 VALUES (1, 10)");
            c.createStatement().execute("INSERT INTO atc_t6 VALUES (2, 20)");
            c.createStatement().execute("INSERT INTO atc_t6 VALUES (3, 30)");
            try {
                c.createStatement().execute("SELECT 1/0");
            } catch (SQLException ignored) {}
            c.commit();

            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM atc_t6")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "All pre-error inserts should be rolled back");
            }
            c.createStatement().execute("DROP TABLE atc_t6");
            c.commit();
        }
    }

    @Test void normal_commit_still_works() throws Exception {
        // Sanity check: commit without error should persist data
        try (Connection c = newConn()) {
            c.createStatement().execute("CREATE TABLE atc_t7(id int PRIMARY KEY)");
            c.createStatement().execute("INSERT INTO atc_t7 VALUES (1)");
            c.commit();

            try (ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM atc_t7")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            c.createStatement().execute("DROP TABLE atc_t7");
            c.commit();
        }
    }
}
