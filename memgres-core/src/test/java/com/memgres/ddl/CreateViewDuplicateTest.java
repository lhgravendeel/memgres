package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE VIEW edge cases related to duplicate views and
 * the difference between CREATE VIEW, CREATE OR REPLACE VIEW,
 * and CREATE VIEW IF NOT EXISTS.
 *
 * pg_dump can output the same view definition twice (once in the schema
 * section and once when recreating dependent views), causing "already exists"
 * errors if the engine doesn't handle this gracefully.
 *
 * Also tests CREATE OR REPLACE VIEW where the replacement adds columns.
 */
class CreateViewDuplicateTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE base_entries (id serial PRIMARY KEY, name text, category text, val int)");
            s.execute("INSERT INTO base_entries (name, category, val) VALUES ('a', 'x', 10), ('b', 'y', 20)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // CREATE VIEW: duplicate without OR REPLACE should error
    // =========================================================================

    @Test
    void testCreateViewDuplicateErrors() throws SQLException {
        exec("CREATE VIEW dup_view1 AS SELECT id, name FROM base_entries");
        assertThrows(SQLException.class, () ->
                exec("CREATE VIEW dup_view1 AS SELECT id, name FROM base_entries"));
    }

    // =========================================================================
    // CREATE OR REPLACE VIEW: should succeed on duplicate
    // =========================================================================

    @Test
    void testCreateOrReplaceViewIdempotent() throws SQLException {
        exec("CREATE VIEW repl_view1 AS SELECT id, name FROM base_entries");
        exec("CREATE OR REPLACE VIEW repl_view1 AS SELECT id, name FROM base_entries");
        // Should not error
        assertEquals("a", query1("SELECT name FROM repl_view1 ORDER BY id LIMIT 1"));
    }

    @Test
    void testCreateOrReplaceViewAddsColumns() throws SQLException {
        exec("CREATE VIEW repl_view2 AS SELECT id, name FROM base_entries");
        exec("CREATE OR REPLACE VIEW repl_view2 AS SELECT id, name, category, val FROM base_entries");
        // New columns should be available
        assertEquals("x", query1("SELECT category FROM repl_view2 ORDER BY id LIMIT 1"));
    }

    @Test
    void testCreateOrReplaceViewChangesQuery() throws SQLException {
        exec("CREATE VIEW repl_view3 AS SELECT id, name FROM base_entries WHERE category = 'x'");
        exec("CREATE OR REPLACE VIEW repl_view3 AS SELECT id, name FROM base_entries WHERE category = 'y'");
        assertEquals("b", query1("SELECT name FROM repl_view3 LIMIT 1"));
    }

    // =========================================================================
    // Simulated pg_dump duplicate: view created, then created again
    // =========================================================================

    @Test
    void testPgDumpStyleDuplicateViewsWithOrReplace() throws SQLException {
        // pg_dump sometimes outputs the same view twice. The second time should
        // use CREATE OR REPLACE to be idempotent.
        exec("CREATE VIEW pgd_view AS SELECT id, name FROM base_entries");
        exec("CREATE OR REPLACE VIEW pgd_view AS SELECT id, name FROM base_entries");
        exec("CREATE OR REPLACE VIEW pgd_view AS SELECT id, name FROM base_entries");
        assertEquals("a", query1("SELECT name FROM pgd_view ORDER BY id LIMIT 1"));
    }

    // =========================================================================
    // CREATE MATERIALIZED VIEW: duplicate handling
    // =========================================================================

    @Test
    void testCreateMaterializedViewDuplicateErrors() throws SQLException {
        exec("CREATE MATERIALIZED VIEW dup_mv AS SELECT COUNT(*) AS cnt FROM base_entries");
        assertThrows(SQLException.class, () ->
                exec("CREATE MATERIALIZED VIEW dup_mv AS SELECT COUNT(*) AS cnt FROM base_entries"));
    }

    @Test
    void testCreateMaterializedViewIfNotExists() throws SQLException {
        exec("CREATE MATERIALIZED VIEW IF NOT EXISTS ine_mv AS SELECT COUNT(*) AS cnt FROM base_entries");
        exec("CREATE MATERIALIZED VIEW IF NOT EXISTS ine_mv AS SELECT COUNT(*) AS cnt FROM base_entries");
        // Second call should not error
        assertEquals("2", query1("SELECT cnt FROM ine_mv"));
    }

    // =========================================================================
    // Views depending on other views (pg_dump outputs them in dependency order)
    // =========================================================================

    @Test
    void testViewDependingOnView() throws SQLException {
        exec("CREATE VIEW dep_base AS SELECT id, name, category FROM base_entries");
        exec("CREATE VIEW dep_derived AS SELECT name, category FROM dep_base WHERE category = 'x'");
        assertEquals("a", query1("SELECT name FROM dep_derived"));
    }

    @Test
    void testReplaceDependentView() throws SQLException {
        exec("CREATE VIEW dep2_base AS SELECT id, name, val FROM base_entries");
        exec("CREATE VIEW dep2_child AS SELECT name, val FROM dep2_base");
        // Replace the base view; dependent view should still work if columns are compatible
        exec("CREATE OR REPLACE VIEW dep2_base AS SELECT id, name, val, category FROM base_entries");
        assertEquals("a", query1("SELECT name FROM dep2_child ORDER BY name LIMIT 1"));
    }

    // =========================================================================
    // DROP VIEW IF EXISTS + recreate (another pg_dump pattern)
    // =========================================================================

    @Test
    void testDropAndRecreateView() throws SQLException {
        exec("CREATE VIEW drop_recreate AS SELECT id, name FROM base_entries");
        exec("DROP VIEW IF EXISTS drop_recreate");
        exec("CREATE VIEW drop_recreate AS SELECT id, name, val FROM base_entries");
        assertNotNull(query1("SELECT val FROM drop_recreate LIMIT 1"));
    }

    @Test
    void testDropViewIfExistsNonexistent() throws SQLException {
        exec("DROP VIEW IF EXISTS nonexistent_view_name");
        // Should not error
    }

    @Test
    void testDropMaterializedViewIfExists() throws SQLException {
        exec("CREATE MATERIALIZED VIEW drop_mv AS SELECT 1 AS n");
        exec("DROP MATERIALIZED VIEW IF EXISTS drop_mv");
        exec("DROP MATERIALIZED VIEW IF EXISTS drop_mv"); // second call ok
    }
}
