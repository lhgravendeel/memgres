package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that a failed ON CONFLICT DO UPDATE does not permanently corrupt the row.
 * Bug: DmlExecutor mutates the row in place, then runs constraint validation
 * outside the restore try-block. A unique violation during the update leaves
 * the table with corrupted data that even ROLLBACK can't repair.
 */
class OnConflictUpdateCorruptionTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    @Test void failed_upsert_does_not_corrupt_unique_column() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t1 (id int PRIMARY KEY, email text UNIQUE, val text)");
            s.execute("INSERT INTO ocu_t1 VALUES (1, 'a@test.com', 'first')");
            s.execute("INSERT INTO ocu_t1 VALUES (2, 'b@test.com', 'second')");

            // Upsert on id=1 tries to set email to 'b@test.com' — violates UNIQUE on email
            try {
                s.execute("INSERT INTO ocu_t1 VALUES (1, 'a@test.com', 'first') "
                        + "ON CONFLICT (id) DO UPDATE SET email = 'b@test.com'");
                fail("Expected unique violation on email");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }

            // Row id=1 should still have its original email
            try (ResultSet rs = s.executeQuery("SELECT email, val FROM ocu_t1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("a@test.com", rs.getString("email"),
                        "Row should retain original email after failed upsert");
                assertEquals("first", rs.getString("val"));
            }

            // Row id=2 should be untouched
            try (ResultSet rs = s.executeQuery("SELECT email FROM ocu_t1 WHERE id = 2")) {
                assertTrue(rs.next());
                assertEquals("b@test.com", rs.getString("email"));
            }

            s.execute("DROP TABLE ocu_t1");
        }
    }

    @Test void failed_upsert_row_still_findable_by_original_pk() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t2 (id int PRIMARY KEY, code text UNIQUE)");
            s.execute("INSERT INTO ocu_t2 VALUES (1, 'AAA')");
            s.execute("INSERT INTO ocu_t2 VALUES (2, 'BBB')");

            try {
                s.execute("INSERT INTO ocu_t2 VALUES (1, 'x') "
                        + "ON CONFLICT (id) DO UPDATE SET code = 'BBB'");
            } catch (SQLException ignored) {}

            // The row must still be findable via PK lookup
            try (ResultSet rs = s.executeQuery("SELECT code FROM ocu_t2 WHERE id = 1")) {
                assertTrue(rs.next(), "Row id=1 must still be findable by PK after failed upsert");
                assertEquals("AAA", rs.getString(1));
            }

            // Total row count should be unchanged
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM ocu_t2")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }

            s.execute("DROP TABLE ocu_t2");
        }
    }

    @Test void failed_upsert_in_transaction_rollback_restores() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t3 (id int PRIMARY KEY, name text UNIQUE)");
            s.execute("INSERT INTO ocu_t3 VALUES (1, 'alice')");
            s.execute("INSERT INTO ocu_t3 VALUES (2, 'bob')");
            c.commit();

            try {
                s.execute("INSERT INTO ocu_t3 VALUES (1, 'alice') "
                        + "ON CONFLICT (id) DO UPDATE SET name = 'bob'");
            } catch (SQLException ignored) {}
            c.rollback();

            // After rollback, both rows should be pristine
            try (ResultSet rs = s.executeQuery("SELECT name FROM ocu_t3 ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("alice", rs.getString(1));
                assertTrue(rs.next()); assertEquals("bob", rs.getString(1));
            }

            s.execute("DROP TABLE ocu_t3");
            c.commit();
        }
    }

    @Test void successful_upsert_still_works() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t4 (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO ocu_t4 VALUES (1, 'old')");

            // This upsert should succeed (no constraint violation)
            s.execute("INSERT INTO ocu_t4 VALUES (1, 'old') "
                    + "ON CONFLICT (id) DO UPDATE SET val = 'new'");

            try (ResultSet rs = s.executeQuery("SELECT val FROM ocu_t4 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("new", rs.getString(1));
            }

            s.execute("DROP TABLE ocu_t4");
        }
    }

    @Test void failed_upsert_index_remains_consistent() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t5 (id int PRIMARY KEY, tag text UNIQUE)");
            s.execute("INSERT INTO ocu_t5 VALUES (1, 'x')");
            s.execute("INSERT INTO ocu_t5 VALUES (2, 'y')");

            try {
                s.execute("INSERT INTO ocu_t5 VALUES (1, 'x') "
                        + "ON CONFLICT (id) DO UPDATE SET tag = 'y'");
            } catch (SQLException ignored) {}

            // After failed upsert, inserting a new row with 'x' tag should fail (still taken)
            try {
                s.execute("INSERT INTO ocu_t5 VALUES (3, 'x')");
                fail("Tag 'x' should still be in use by row 1");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }

            // But inserting with a truly new tag should work
            s.execute("INSERT INTO ocu_t5 VALUES (3, 'z')");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM ocu_t5")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }

            s.execute("DROP TABLE ocu_t5");
        }
    }

    @Test void failed_upsert_check_constraint_does_not_corrupt() throws Exception {
        try (Connection c = newConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ocu_t6 (id int PRIMARY KEY, score int CHECK (score >= 0))");
            s.execute("INSERT INTO ocu_t6 VALUES (1, 50)");

            try {
                s.execute("INSERT INTO ocu_t6 VALUES (1, 50) "
                        + "ON CONFLICT (id) DO UPDATE SET score = -1");
                fail("Expected check constraint violation");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("check") || e.getMessage().contains("constraint")
                        || "23514".equals(e.getSQLState()) || "23503".equals(e.getSQLState()),
                        "Expected constraint violation, got: " + e.getMessage());
            }

            // Row should retain original value
            try (ResultSet rs = s.executeQuery("SELECT score FROM ocu_t6 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1), "Score should be unchanged after failed upsert");
            }

            s.execute("DROP TABLE ocu_t6");
        }
    }
}
