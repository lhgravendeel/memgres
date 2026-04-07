package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2: System catalog completeness. Validates that information_schema and
 * pg_catalog tables correctly report all constraints, indexes, and objects,
 * matching PG18 behavior.
 *
 * Covers: table_constraints, pg_constraint, pg_class, pg_index, pg_get_indexdef,
 * pg_get_constraintdef, pg_get_functiondef, obj_description, col_description,
 * adrelid::regclass, column_default for enum types.
 */
class SystemCatalogCompletenessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        // Create a schema with a realistic set of objects
        exec("CREATE SCHEMA IF NOT EXISTS cattest");
        exec("SET search_path TO cattest, public, pg_catalog");
        exec("CREATE TABLE parent_t (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE)");
        exec("CREATE TABLE child_t (id INT PRIMARY KEY, parent_id INT REFERENCES parent_t(id), " +
             "qty INT CHECK (qty >= 0), note TEXT DEFAULT 'x')");
        exec("CREATE INDEX child_note_idx ON child_t (note)");
        exec("CREATE UNIQUE INDEX child_parent_uq ON child_t (parent_id)");
        exec("CREATE VIEW child_v AS SELECT id, note FROM child_t");
        exec("CREATE FUNCTION cat_fn(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + 1 $$");
        exec("COMMENT ON TABLE parent_t IS 'parent table comment'");
        exec("COMMENT ON COLUMN child_t.note IS 'note column comment'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try { exec("SET search_path TO public, pg_catalog"); } catch (SQLException ignored) {}
            try { exec("DROP SCHEMA cattest CASCADE"); } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        }
    }

    static Set<String> queryColumn(String sql) throws SQLException {
        Set<String> result = new LinkedHashSet<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getString(1));
        }
        return result;
    }

    // ========================================================================
    // information_schema.tables: should list tables AND views
    // ========================================================================

    @Test
    void info_schema_tables_includes_view() throws SQLException {
        Set<String> tables = queryColumn(
            "SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = 'cattest' ORDER BY table_name");
        assertTrue(tables.contains("parent_t"), "Should contain parent_t: " + tables);
        assertTrue(tables.contains("child_t"), "Should contain child_t: " + tables);
        assertTrue(tables.contains("child_v"), "Should contain view child_v: " + tables);
    }

    // ========================================================================
    // information_schema.table_constraints: all constraint types
    // ========================================================================

    @Test
    void table_constraints_includes_pk() throws SQLException {
        Set<String> types = queryColumn(
            "SELECT constraint_type FROM information_schema.table_constraints " +
            "WHERE table_schema = 'cattest' AND table_name = 'parent_t'");
        assertTrue(types.contains("PRIMARY KEY"), "Should include PK: " + types);
    }

    @Test
    void table_constraints_includes_unique() throws SQLException {
        Set<String> types = queryColumn(
            "SELECT constraint_type FROM information_schema.table_constraints " +
            "WHERE table_schema = 'cattest' AND table_name = 'parent_t'");
        assertTrue(types.contains("UNIQUE"), "Should include UNIQUE: " + types);
    }

    @Test
    void table_constraints_includes_fk() throws SQLException {
        Set<String> types = queryColumn(
            "SELECT constraint_type FROM information_schema.table_constraints " +
            "WHERE table_schema = 'cattest' AND table_name = 'child_t'");
        assertTrue(types.contains("FOREIGN KEY"), "Should include FK: " + types);
    }

    @Test
    void table_constraints_includes_check() throws SQLException {
        Set<String> types = queryColumn(
            "SELECT constraint_type FROM information_schema.table_constraints " +
            "WHERE table_schema = 'cattest' AND table_name = 'child_t'");
        assertTrue(types.contains("CHECK"), "Should include CHECK: " + types);
    }

    @Test
    void table_constraints_count_for_child() throws SQLException {
        // child_t has: PK, FK, CHECK = at least 3 constraints
        int count = countRows(
            "SELECT constraint_name FROM information_schema.table_constraints " +
            "WHERE table_schema = 'cattest' AND table_name = 'child_t'");
        assertTrue(count >= 3, "child_t should have >= 3 constraints, got " + count);
    }

    // ========================================================================
    // pg_constraint: all constraint types visible
    // ========================================================================

    @Test
    void pg_constraint_shows_pk_unique_fk_check() throws SQLException {
        Set<String> types = queryColumn(
            "SELECT contype FROM pg_constraint WHERE conrelid = 'cattest.child_t'::regclass");
        assertTrue(types.contains("p") || types.contains("u") || types.contains("f") || types.contains("c"),
                "Should have some constraint types: " + types);
        // At minimum, PK (p), FK (f), CHECK (c)
        int count = countRows(
            "SELECT conname FROM pg_constraint WHERE conrelid = 'cattest.child_t'::regclass");
        assertTrue(count >= 3, "child_t should have >= 3 pg_constraints, got " + count);
    }

    @Test
    void pg_constraint_shows_parent_pk_and_unique() throws SQLException {
        int count = countRows(
            "SELECT conname FROM pg_constraint WHERE conrelid = 'cattest.parent_t'::regclass");
        assertTrue(count >= 2, "parent_t should have PK + UNIQUE >= 2 constraints, got " + count);
    }

    // ========================================================================
    // pg_class: tables, views, indexes, sequences
    // ========================================================================

    @Test
    void pg_class_includes_tables_and_views() throws SQLException {
        Set<String> relnames = queryColumn(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = 'cattest' AND c.relkind IN ('r','v','i','S')");
        assertTrue(relnames.contains("parent_t"), "Should contain parent_t: " + relnames);
        assertTrue(relnames.contains("child_t"), "Should contain child_t: " + relnames);
        assertTrue(relnames.contains("child_v"), "Should contain child_v (view): " + relnames);
        assertTrue(relnames.contains("child_note_idx"), "Should contain index: " + relnames);
    }

    // ========================================================================
    // pg_index: explicit and implicit indexes
    // ========================================================================

    @Test
    void pg_index_includes_explicit_indexes() throws SQLException {
        int count = countRows(
            "SELECT i.indexrelid::regclass FROM pg_index i " +
            "WHERE i.indrelid = 'cattest.child_t'::regclass");
        // child_t has: PK index, child_note_idx, child_parent_uq = at least 3
        assertTrue(count >= 3, "child_t should have >= 3 indexes, got " + count);
    }

    @Test
    void pg_index_marks_primary_correctly() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT i.indisprimary FROM pg_index i " +
                 "WHERE i.indrelid = 'cattest.parent_t'::regclass AND i.indisprimary = true")) {
            assertTrue(rs.next(), "parent_t should have a primary key index");
        }
    }

    // ========================================================================
    // pg_get_indexdef: index definition text
    // ========================================================================

    @Test
    void pg_get_indexdef_returns_definition() throws SQLException {
        String def = q("SELECT pg_get_indexdef('cattest.child_note_idx'::regclass)");
        assertNotNull(def, "pg_get_indexdef should return non-null for existing index");
        assertTrue(def.toLowerCase().contains("index"), "Should contain INDEX: " + def);
        assertTrue(def.toLowerCase().contains("child_t") || def.toLowerCase().contains("note"),
                "Should reference table or column: " + def);
    }

    @Test
    void pg_get_indexdef_unique_index() throws SQLException {
        String def = q("SELECT pg_get_indexdef('cattest.child_parent_uq'::regclass)");
        assertNotNull(def, "pg_get_indexdef should return non-null");
        assertTrue(def.toLowerCase().contains("unique"), "Should contain UNIQUE: " + def);
    }

    // ========================================================================
    // pg_get_constraintdef: constraint definition text
    // ========================================================================

    @Test
    void pg_get_constraintdef_returns_check() throws SQLException {
        String def = q(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
            "WHERE conrelid = 'cattest.child_t'::regclass AND contype = 'c' LIMIT 1");
        assertNotNull(def, "pg_get_constraintdef should return CHECK definition");
        assertTrue(def.toLowerCase().contains("qty") || def.toLowerCase().contains("check"),
                "Should reference check expression: " + def);
    }

    // ========================================================================
    // pg_get_functiondef: function definition with schema
    // ========================================================================

    @Test
    void pg_get_functiondef_with_schema() throws SQLException {
        String def = q("SELECT pg_get_functiondef('cattest.cat_fn(int)'::regprocedure)");
        assertNotNull(def, "pg_get_functiondef should return non-null");
        assertTrue(def.toLowerCase().contains("cat_fn"), "Should contain function name: " + def);
        assertTrue(def.toLowerCase().contains("select"), "Should contain body: " + def);
    }

    // ========================================================================
    // obj_description / col_description: schema-qualified
    // ========================================================================

    @Test
    void obj_description_returns_table_comment() throws SQLException {
        String desc = q("SELECT obj_description('cattest.parent_t'::regclass)");
        assertEquals("parent table comment", desc);
    }

    @Test
    void col_description_returns_column_comment() throws SQLException {
        String desc = q("SELECT col_description('cattest.child_t'::regclass, 4)");
        // column 4 = note (id=1, parent_id=2, qty=3, note=4)
        assertEquals("note column comment", desc);
    }

    // ========================================================================
    // adrelid::regclass in pg_attrdef: should show table name, not OID
    // ========================================================================

    @Test
    void pg_attrdef_adrelid_shows_table_name() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT adrelid::regclass FROM pg_attrdef " +
                 "WHERE adrelid = 'cattest.child_t'::regclass LIMIT 1")) {
            if (rs.next()) {
                String relname = rs.getString(1);
                assertFalse(relname.matches("\\d+"),
                        "adrelid::regclass should show table name, not OID: " + relname);
            }
        }
    }

    // ========================================================================
    // column_default for enum types: should include type cast
    // ========================================================================

    @Test
    void column_default_enum_type_uses_proper_cast() throws SQLException {
        exec("CREATE TYPE cattest_status AS ENUM ('new', 'done')");
        exec("CREATE TABLE enum_def_t (id INT, status cattest_status DEFAULT 'new')");
        try {
            String def = q(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_schema = 'cattest' AND table_name = 'enum_def_t' AND column_name = 'status'");
            assertNotNull(def, "Default should not be null");
            // PG shows 'new'::cattest_status (with the enum type name, not USER-DEFINED)
            assertTrue(def.contains("cattest_status") || def.contains("::"),
                    "Enum default should have proper type cast: " + def);
        } finally {
            exec("DROP TABLE IF EXISTS enum_def_t");
            exec("DROP TYPE IF EXISTS cattest_status");
        }
    }

    // ========================================================================
    // pg_class oid lookup for views
    // ========================================================================

    @Test
    void pg_class_view_oid_resolves() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT c.relname, pg_get_viewdef(c.oid, true) FROM pg_class c " +
                 "WHERE c.oid = 'cattest.child_v'::regclass")) {
            assertTrue(rs.next(), "Should find view in pg_class by oid");
            assertEquals("child_v", rs.getString(1));
            assertNotNull(rs.getString(2), "pg_get_viewdef should return definition");
        }
    }
}
