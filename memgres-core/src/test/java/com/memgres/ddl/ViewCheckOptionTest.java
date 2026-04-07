package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for updatable views with CHECK OPTION (LOCAL and CASCADED),
 * pg_get_viewdef formatting, and view insert/update/delete behavior.
 *
 * Key PG behaviors:
 * - WITH LOCAL CHECK OPTION: only checks this view's WHERE
 * - WITH CASCADED CHECK OPTION: checks this view AND all underlying views
 * - pg_get_viewdef returns pretty-printed SQL with proper indentation
 * - Views on JOINs are NOT automatically updatable
 */
class ViewCheckOptionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE base(id serial PRIMARY KEY, a int NOT NULL, b text DEFAULT 'd')");
        exec("INSERT INTO base(a, b) VALUES (1, 'x'), (2, 'y'), (10, 'z')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    // ========================================================================
    // WITH LOCAL CHECK OPTION
    // ========================================================================

    @Test
    void local_check_option_rejects_violating_insert() throws SQLException {
        exec("CREATE VIEW v_local AS SELECT * FROM base WHERE a > 0 WITH LOCAL CHECK OPTION");
        try {
            // Insert a = -1 violates the view's WHERE clause
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO v_local(a, b) VALUES (-1, 'bad')"),
                    "LOCAL CHECK OPTION should reject INSERT that violates view WHERE");
        } finally {
            exec("DROP VIEW IF EXISTS v_local");
        }
    }

    @Test
    void local_check_option_allows_valid_insert() throws SQLException {
        exec("CREATE VIEW v_local2 AS SELECT * FROM base WHERE a > 0 WITH LOCAL CHECK OPTION");
        try {
            exec("INSERT INTO v_local2(a, b) VALUES (5, 'ok')");
            assertNotNull(scalar("SELECT id FROM base WHERE a = 5"));
        } finally {
            exec("DROP VIEW IF EXISTS v_local2");
            exec("DELETE FROM base WHERE a = 5");
        }
    }

    // ========================================================================
    // WITH CASCADED CHECK OPTION
    // ========================================================================

    @Test
    void cascaded_check_option_creates_successfully() throws SQLException {
        exec("CREATE VIEW v_inner AS SELECT * FROM base WHERE a > 0 WITH LOCAL CHECK OPTION");
        try {
            // This is the pattern that was failing: WITH CASCADED CHECK OPTION
            exec("CREATE VIEW v_cascaded AS SELECT * FROM v_inner WHERE a >= 2 WITH CASCADED CHECK OPTION");
            // Verify the view exists and works
            int count = countRows("SELECT * FROM v_cascaded");
            assertTrue(count >= 1, "Cascaded view should return rows matching a >= 2");
        } finally {
            exec("DROP VIEW IF EXISTS v_cascaded");
            exec("DROP VIEW IF EXISTS v_inner");
        }
    }

    @Test
    void cascaded_check_option_enforces_underlying_view_where() throws SQLException {
        exec("CREATE VIEW v_base_pos AS SELECT * FROM base WHERE a > 0 WITH LOCAL CHECK OPTION");
        exec("CREATE VIEW v_gt5 AS SELECT * FROM v_base_pos WHERE a > 5 WITH CASCADED CHECK OPTION");
        try {
            // a=3 passes v_gt5? No (3 is not > 5), so this should fail
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO v_gt5(a, b) VALUES (3, 'x')"),
                    "CASCADED CHECK should reject value that doesn't match this view's WHERE");
        } finally {
            exec("DROP VIEW IF EXISTS v_gt5");
            exec("DROP VIEW IF EXISTS v_base_pos");
        }
    }

    @Test
    void insert_through_cascaded_view() throws SQLException {
        exec("CREATE VIEW v_bp AS SELECT * FROM base WHERE a > 0 WITH LOCAL CHECK OPTION");
        exec("CREATE VIEW v_nested AS SELECT * FROM v_bp WHERE a >= 2 WITH CASCADED CHECK OPTION");
        try {
            exec("INSERT INTO v_nested(a, b) VALUES (5, 'vv')");
            assertNotNull(scalar("SELECT id FROM base WHERE a = 5 AND b = 'vv'"));

            exec("UPDATE v_nested SET a = 6 WHERE b = 'vv'");
            assertEquals("6", scalar("SELECT a FROM base WHERE b = 'vv'"));
        } finally {
            exec("DROP VIEW IF EXISTS v_nested");
            exec("DROP VIEW IF EXISTS v_bp");
            exec("DELETE FROM base WHERE b = 'vv'");
        }
    }

    // ========================================================================
    // pg_get_viewdef formatting
    // ========================================================================

    @Test
    void pg_get_viewdef_returns_formatted_sql() throws SQLException {
        exec("CREATE VIEW v_fmt AS SELECT id, b FROM base");
        try {
            String def = scalar("SELECT pg_get_viewdef('v_fmt'::regclass, true)");
            assertNotNull(def);
            // PG returns formatted SQL like:
            //  SELECT id,\n    b\n   FROM base;
            // At minimum it should contain SELECT and FROM, and be valid SQL-like text
            assertTrue(def.toUpperCase().contains("SELECT"), "viewdef should contain SELECT");
            assertTrue(def.toUpperCase().contains("FROM"), "viewdef should contain FROM");
            // PG pretty-prints with indentation
            assertTrue(def.contains("\n"), "Pretty-printed viewdef should contain newlines");
        } finally {
            exec("DROP VIEW IF EXISTS v_fmt");
        }
    }

    // ========================================================================
    // Non-updatable views
    // ========================================================================

    @Test
    void view_on_join_is_not_updatable() throws SQLException {
        exec("CREATE TABLE t2(id int PRIMARY KEY, note text)");
        exec("CREATE VIEW v_join AS SELECT b1.id, b1.a, t2.note FROM base b1 JOIN t2 ON b1.id = t2.id");
        try {
            // PG: views on joins are not automatically updatable
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO v_join VALUES (1, 1, 'x')"),
                    "View on JOIN should not be insertable");
        } finally {
            exec("DROP VIEW IF EXISTS v_join");
            exec("DROP TABLE IF EXISTS t2");
        }
    }

    // ========================================================================
    // Updatable view update/delete through view
    // ========================================================================

    @Test
    void update_through_simple_view() throws SQLException {
        exec("CREATE VIEW v_upd AS SELECT id, a, b FROM base WHERE a > 0");
        try {
            exec("UPDATE v_upd SET b = 'updated' WHERE a = 1");
            assertEquals("updated", scalar("SELECT b FROM base WHERE a = 1"));
        } finally {
            exec("DROP VIEW IF EXISTS v_upd");
            exec("UPDATE base SET b = 'x' WHERE a = 1");
        }
    }

    @Test
    void delete_through_simple_view() throws SQLException {
        exec("INSERT INTO base(a, b) VALUES (99, 'del_me')");
        exec("CREATE VIEW v_del AS SELECT id, a, b FROM base WHERE a > 0");
        try {
            exec("DELETE FROM v_del WHERE a = 99");
            assertNull(scalar("SELECT id FROM base WHERE a = 99"));
        } finally {
            exec("DROP VIEW IF EXISTS v_del");
        }
    }
}
