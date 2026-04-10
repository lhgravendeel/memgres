package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NOT VALID constraints and VALIDATE CONSTRAINT (PG 18 compatible).
 */
class NotValidConstraintTest {

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

    @BeforeEach
    void cleanUp() throws SQLException {
        for (String t : List.of("t1", "t2", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        for (String t : List.of("t1", "t2", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // CHECK constraint NOT VALID
    // ========================================================================

    @Test
    void check_not_valid_skips_existing_rows() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)"); // violates val > 0
        // Adding CHECK NOT VALID should succeed even though existing data violates it
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        // The constraint is stored
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_constraint WHERE conname = 'chk_val'"));
    }

    @Test
    void check_not_valid_still_validates_new_rows() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        // New inserts must still satisfy the constraint
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, -10)"));
        assertEquals("23514", ex.getSQLState());
        // Valid inserts still work
        exec("INSERT INTO t1 VALUES (3, 10)");
    }

    @Test
    void check_not_valid_pg_constraint_convalidated_false() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        assertEquals("f", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'"));
    }

    @Test
    void check_validated_pg_constraint_convalidated_true() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        exec("ALTER TABLE t1 VALIDATE CONSTRAINT chk_val");
        assertEquals("t", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'"));
    }

    @Test
    void validate_constraint_fails_if_data_violates() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        // VALIDATE CONSTRAINT should fail because existing row violates
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 VALIDATE CONSTRAINT chk_val"));
        assertEquals("23514", ex.getSQLState());
    }

    @Test
    void validate_constraint_succeeds_after_fixing_data() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID");
        // Fix the violating row
        exec("UPDATE t1 SET val = 5 WHERE id = 1");
        // Now VALIDATE CONSTRAINT should succeed
        exec("ALTER TABLE t1 VALIDATE CONSTRAINT chk_val");
        assertEquals("t", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'"));
    }

    @Test
    void validate_already_valid_is_noop() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0)");
        // Already valid constraint — VALIDATE should be a no-op
        exec("ALTER TABLE t1 VALIDATE CONSTRAINT chk_val");
        assertEquals("t", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'"));
    }

    @Test
    void validate_nonexistent_constraint_errors() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY)");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 VALIDATE CONSTRAINT no_such_constraint"));
        assertEquals("42704", ex.getSQLState());
    }

    // ========================================================================
    // FK constraint NOT VALID
    // ========================================================================

    @Test
    void fk_not_valid_skips_existing_rows() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int)");
        exec("INSERT INTO t1 VALUES (1, 999)"); // violates FK
        // Adding FK NOT VALID should succeed
        exec("ALTER TABLE t1 ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) NOT VALID");
        assertEquals("f", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'fk_parent'"));
    }

    @Test
    void fk_not_valid_validates_new_rows() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int)");
        exec("INSERT INTO t1 VALUES (1, 999)");
        exec("ALTER TABLE t1 ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) NOT VALID");
        // New inserts must still satisfy FK
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 888)"));
        assertEquals("23503", ex.getSQLState());
    }

    @Test
    void fk_validate_constraint_fails_with_violations() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int)");
        exec("INSERT INTO t1 VALUES (1, 999)");
        exec("ALTER TABLE t1 ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) NOT VALID");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 VALIDATE CONSTRAINT fk_parent"));
        assertEquals("23503", ex.getSQLState());
    }

    @Test
    void fk_validate_constraint_succeeds_after_fix() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1), (999)");
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int)");
        exec("INSERT INTO t1 VALUES (1, 999)");
        exec("ALTER TABLE t1 ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id) NOT VALID");
        exec("ALTER TABLE t1 VALIDATE CONSTRAINT fk_parent");
        assertEquals("t", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'fk_parent'"));
    }

    // ========================================================================
    // Without NOT VALID — regular behavior preserved
    // ========================================================================

    @Test
    void check_without_not_valid_validates_immediately() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0)"));
        assertEquals("23514", ex.getSQLState());
    }

    @Test
    void check_without_not_valid_convalidated_true() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_val CHECK (val > 0)");
        assertEquals("t", scalar("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'"));
    }
}
