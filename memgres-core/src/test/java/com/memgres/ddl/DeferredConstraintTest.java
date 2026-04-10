package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for deferred constraint validation matching PG 18 behavior.
 * Covers DEFERRABLE INITIALLY DEFERRED/IMMEDIATE, SET CONSTRAINTS, and catalog reflection.
 */
class DeferredConstraintTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false); // transactions needed for deferred constraints
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void cleanUp() throws SQLException {
        try { conn.rollback(); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
        for (String t : List.of("t1", "t2", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
        conn.setAutoCommit(false);
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        try { conn.rollback(); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
        for (String t : List.of("t1", "t2", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // FK DEFERRABLE INITIALLY DEFERRED
    // ========================================================================

    @Test
    void fk_deferred_allows_insert_before_parent() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, pid int, " +
             "CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
        // Insert child before parent — deferred FK allows this within transaction
        exec("INSERT INTO child VALUES (1, 100)");
        exec("INSERT INTO parent VALUES (100)");
        conn.commit(); // Should succeed — parent exists at commit time
    }

    @Test
    void fk_deferred_fails_at_commit_if_parent_missing() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, pid int, " +
             "CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO child VALUES (1, 999)");
        // Commit should fail — parent 999 doesn't exist
        assertThrows(SQLException.class, () -> conn.commit());
    }

    // ========================================================================
    // UNIQUE DEFERRABLE INITIALLY DEFERRED
    // ========================================================================

    @Test
    void unique_deferred_allows_temporary_duplicates() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        exec("INSERT INTO t1 VALUES (2, 200)");
        conn.commit();
        // Now swap codes — temporarily creates duplicates
        exec("UPDATE t1 SET code = 200 WHERE id = 1");
        exec("UPDATE t1 SET code = 100 WHERE id = 2");
        conn.commit(); // Should succeed — no duplicates at commit time
        // Verify swap happened
        conn.setAutoCommit(true);
        assertEquals("200", scalar("SELECT code FROM t1 WHERE id = 1"));
        assertEquals("100", scalar("SELECT code FROM t1 WHERE id = 2"));
        conn.setAutoCommit(false);
    }

    @Test
    void unique_deferred_fails_at_commit_with_duplicates() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        exec("INSERT INTO t1 VALUES (2, 100)"); // duplicate, but deferred
        assertThrows(SQLException.class, () -> conn.commit());
    }

    // ========================================================================
    // CHECK DEFERRABLE INITIALLY DEFERRED
    // ========================================================================

    @Test
    void check_deferred_allows_temporary_violation() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int, " +
             "CONSTRAINT chk_positive CHECK (val > 0) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO t1 VALUES (1, -5)"); // violates check, but deferred
        exec("UPDATE t1 SET val = 10 WHERE id = 1"); // fix before commit
        conn.commit(); // Should succeed — constraint satisfied at commit time
    }

    @Test
    void check_deferred_fails_at_commit_with_violation() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int, " +
             "CONSTRAINT chk_positive CHECK (val > 0) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO t1 VALUES (1, -5)"); // violates check
        assertThrows(SQLException.class, () -> conn.commit());
    }

    // ========================================================================
    // DEFERRABLE INITIALLY IMMEDIATE
    // ========================================================================

    @Test
    void unique_initially_immediate_checks_at_statement() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY IMMEDIATE)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        // Duplicate should fail immediately (INITIALLY IMMEDIATE)
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 100)"));
        conn.rollback();
    }

    // ========================================================================
    // SET CONSTRAINTS
    // ========================================================================

    @Test
    void set_constraints_all_deferred() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY IMMEDIATE)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        conn.commit();
        // Switch to deferred mode
        exec("SET CONSTRAINTS ALL DEFERRED");
        exec("INSERT INTO t1 VALUES (2, 100)"); // duplicate, but now deferred
        exec("DELETE FROM t1 WHERE id = 1"); // remove duplicate before commit
        conn.commit(); // Should succeed
    }

    @Test
    void set_constraints_named_deferred() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY IMMEDIATE)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        conn.commit();
        exec("SET CONSTRAINTS uq_code DEFERRED");
        exec("INSERT INTO t1 VALUES (2, 100)"); // duplicate, but uq_code is now deferred
        exec("DELETE FROM t1 WHERE id = 1");
        conn.commit();
    }

    @Test
    void set_constraints_immediate_restores_checking() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)");
        exec("INSERT INTO t1 VALUES (1, 100)");
        conn.commit();
        // Override to immediate
        exec("SET CONSTRAINTS ALL IMMEDIATE");
        // Now it should check immediately
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 100)"));
        conn.rollback();
    }

    // ========================================================================
    // pg_constraint catalog reflection
    // ========================================================================

    @Test
    void pg_constraint_condeferrable_true_for_deferrable() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, " +
             "CONSTRAINT uq_code UNIQUE (code) DEFERRABLE INITIALLY DEFERRED)");
        conn.commit();
        conn.setAutoCommit(true);
        assertEquals("t", scalar("SELECT condeferrable FROM pg_constraint WHERE conname = 'uq_code'"));
        assertEquals("t", scalar("SELECT condeferred FROM pg_constraint WHERE conname = 'uq_code'"));
        conn.setAutoCommit(false);
    }

    @Test
    void pg_constraint_condeferrable_false_for_non_deferrable() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int UNIQUE)");
        conn.commit();
        conn.setAutoCommit(true);
        // Find the unique constraint name
        String name = scalar("SELECT conname FROM pg_constraint WHERE contype = 'u' AND conrelid = (SELECT oid FROM pg_class WHERE relname = 't1')");
        if (name != null) {
            assertEquals("f", scalar("SELECT condeferrable FROM pg_constraint WHERE conname = '" + name + "'"));
        }
        conn.setAutoCommit(false);
    }

    // ========================================================================
    // Non-deferrable constraints remain immediate
    // ========================================================================

    @Test
    void non_deferrable_constraint_not_affected_by_set_constraints() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int UNIQUE)"); // NOT deferrable
        exec("INSERT INTO t1 VALUES (1, 100)");
        conn.commit();
        exec("SET CONSTRAINTS ALL DEFERRED");
        // Non-deferrable constraint should still fail immediately
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 100)"));
        conn.rollback();
    }
}
