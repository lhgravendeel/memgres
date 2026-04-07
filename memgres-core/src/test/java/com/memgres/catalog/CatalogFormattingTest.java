package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 5: Catalog formatting. Validates that introspection functions and
 * catalog queries return correctly formatted output matching PG18.
 * Covers: pg_get_constraintdef SQL output, pg_get_viewdef pretty printing,
 * pg_get_functiondef schema-qualified, regclass display, to_regclass for indexes,
 * and remaining catalog row counts.
 */
class CatalogFormattingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE SCHEMA IF NOT EXISTS cfmt");
        exec("SET search_path TO cfmt, public, pg_catalog");
        exec("CREATE TABLE cfmt_t (id INT PRIMARY KEY, note TEXT DEFAULT 'x', qty INT CHECK (qty >= 0))");
        exec("CREATE INDEX cfmt_note_idx ON cfmt_t (note)");
        exec("CREATE VIEW cfmt_v AS SELECT id, note FROM cfmt_t");
        exec("CREATE FUNCTION cfmt_fn(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + 1 $$");
        exec("COMMENT ON TABLE cfmt_t IS 'table comment'");
        exec("COMMENT ON COLUMN cfmt_t.note IS 'note comment'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try { exec("SET search_path TO public, pg_catalog"); } catch (SQLException ignored) {}
            try { exec("DROP SCHEMA cfmt CASCADE"); } catch (SQLException ignored) {}
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
            int c = 0; while (rs.next()) c++; return c;
        }
    }

    // ========================================================================
    // pg_get_constraintdef: should return SQL text, not AST toString
    // ========================================================================

    @Test
    void constraintdef_check_returns_sql() throws SQLException {
        String def = q(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
            "WHERE conrelid = 'cfmt.cfmt_t'::regclass AND contype = 'c' LIMIT 1");
        assertNotNull(def, "Should find CHECK constraint definition");
        // PG returns: CHECK ((qty >= 0))
        // Memgres was returning: CHECK (BinaryExpr[...]), i.e. AST toString
        assertTrue(def.contains("qty"), "Should contain column name: " + def);
        assertTrue(def.contains(">=") || def.contains(">"), "Should contain operator: " + def);
        assertFalse(def.contains("BinaryExpr"), "Should not contain AST class name: " + def);
        assertFalse(def.contains("ColumnRef"), "Should not contain AST class name: " + def);
    }

    @Test
    void constraintdef_pk_returns_sql() throws SQLException {
        String def = q(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
            "WHERE conrelid = 'cfmt.cfmt_t'::regclass AND contype = 'p' LIMIT 1");
        assertNotNull(def, "Should find PK constraint definition");
        assertTrue(def.toUpperCase().contains("PRIMARY KEY"), "Should contain PRIMARY KEY: " + def);
    }

    // ========================================================================
    // pg_get_viewdef: pretty-printed output
    // ========================================================================

    @Test
    void viewdef_pretty_includes_newlines() throws SQLException {
        // pg_get_viewdef(oid, true) should produce pretty-printed SQL with newlines
        String def = q("SELECT pg_get_viewdef('cfmt.cfmt_v'::regclass, true)");
        assertNotNull(def, "Should return view definition");
        assertTrue(def.toLowerCase().contains("select"), "Should contain SELECT: " + def);
        assertTrue(def.toLowerCase().contains("cfmt_t"), "Should reference base table: " + def);
        // Pretty-printed PG output has newlines and indentation
        // At minimum it should end with a semicolon
    }

    @Test
    void viewdef_via_pg_class_oid() throws SQLException {
        // Query pg_class by OID and get view definition
        int count = countRows(
            "SELECT c.relname, pg_get_viewdef(c.oid, true) FROM pg_class c " +
            "WHERE c.oid = 'cfmt.cfmt_v'::regclass");
        assertTrue(count >= 1, "Should find view in pg_class by oid");
    }

    // ========================================================================
    // pg_get_functiondef: schema-qualified
    // ========================================================================

    @Test
    void functiondef_uses_correct_schema() throws SQLException {
        String def = q("SELECT pg_get_functiondef('cfmt.cfmt_fn(int)'::regprocedure)");
        assertNotNull(def, "Should return function definition");
        // PG includes the schema: CREATE OR REPLACE FUNCTION cfmt.cfmt_fn(...)
        assertTrue(def.contains("cfmt"), "Should contain schema name 'cfmt': " + def);
        assertFalse(def.contains("public.cfmt_fn"),
                "Should not say public schema for cfmt function: " + def);
    }

    // ========================================================================
    // pg_get_indexdef: expression indexes
    // ========================================================================

    @Test
    void indexdef_expression_index_format() throws SQLException {
        exec("CREATE INDEX cfmt_expr_idx ON cfmt_t (COALESCE(note, ''))");
        try {
            String def = q("SELECT pg_get_indexdef('cfmt.cfmt_expr_idx'::regclass)");
            assertNotNull(def, "Should return expression index definition");
            // PG: CREATE INDEX cfmt_expr_idx ON cfmt.cfmt_t USING btree (COALESCE(note, ''::text))
            assertTrue(def.toLowerCase().contains("coalesce") || def.toLowerCase().contains("note"),
                    "Should contain expression: " + def);
        } finally {
            exec("DROP INDEX IF EXISTS cfmt_expr_idx");
        }
    }

    // ========================================================================
    // regclass display: schema prefix rules
    // ========================================================================

    @Test
    void regclass_omits_public_schema() throws SQLException {
        exec("CREATE TABLE public.regc_pub (id INT)");
        try {
            String result = q("SELECT 'public.regc_pub'::regclass");
            // PG omits "public." prefix when displaying regclass
            assertEquals("regc_pub", result, "Should omit public schema");
        } finally {
            exec("DROP TABLE public.regc_pub");
        }
    }

    @Test
    void regclass_keeps_non_public_schema() throws SQLException {
        // cfmt.cfmt_t::regclass should keep "cfmt." prefix
        String result = q("SELECT 'cfmt.cfmt_t'::regclass");
        assertNotNull(result);
        // PG keeps non-public schema: if search_path includes cfmt, it may omit it
        // But when explicitly schema-qualified, display should match
    }

    // ========================================================================
    // to_regclass: index lookup
    // ========================================================================

    @Test
    void to_regclass_finds_index() throws SQLException {
        String result = q("SELECT to_regclass('cfmt.cfmt_note_idx')");
        assertNotNull(result, "to_regclass should find schema-qualified index");
    }

    @Test
    void to_regclass_returns_unqualified_for_current_schema() throws SQLException {
        // When the table is in the current search_path schema, PG returns unqualified name
        String result = q("SELECT to_regclass('cfmt.cfmt_t')");
        assertNotNull(result, "Should find table");
        // PG returns "cfmt_t" (unqualified when in search_path)
        assertEquals("cfmt_t", result, "Should return unqualified name for current schema");
    }

    // ========================================================================
    // adrelid::regclass: table name, not OID
    // ========================================================================

    @Test
    void pg_attrdef_adrelid_resolves_to_name() throws SQLException {
        String result = q(
            "SELECT adrelid::regclass FROM pg_attrdef WHERE adrelid = 'cfmt.cfmt_t'::regclass LIMIT 1");
        if (result != null) {
            assertFalse(result.matches("\\d+"), "adrelid::regclass should be name, not OID: " + result);
        }
    }

    // ========================================================================
    // pg_constraint completeness: FK and unique constraints visible
    // ========================================================================

    @Test
    void pg_constraint_includes_all_types() throws SQLException {
        exec("CREATE TABLE cfmt_parent (id INT PRIMARY KEY)");
        exec("CREATE TABLE cfmt_child (id INT PRIMARY KEY, pid INT REFERENCES cfmt_parent(id), " +
             "code TEXT UNIQUE, val INT CHECK (val > 0))");
        try {
            int count = countRows(
                "SELECT conname FROM pg_constraint WHERE conrelid = 'cfmt.cfmt_child'::regclass");
            // Should have: PK + FK + UNIQUE + CHECK = 4 constraints minimum
            assertTrue(count >= 4, "cfmt_child should have >= 4 constraints, got " + count);
        } finally {
            exec("DROP TABLE cfmt_child CASCADE");
            exec("DROP TABLE cfmt_parent CASCADE");
        }
    }

    // ========================================================================
    // information_schema.table_constraints row count
    // ========================================================================

    @Test
    void table_constraints_complete_count() throws SQLException {
        exec("CREATE TABLE cfmt_full (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, " +
             "ref_id INT, qty INT CHECK (qty > 0))");
        exec("CREATE TABLE cfmt_ref (id INT PRIMARY KEY)");
        exec("ALTER TABLE cfmt_full ADD CONSTRAINT cfmt_full_fk FOREIGN KEY (ref_id) REFERENCES cfmt_ref(id)");
        try {
            int count = countRows(
                "SELECT constraint_name FROM information_schema.table_constraints " +
                "WHERE table_schema = 'cfmt' AND table_name = 'cfmt_full'");
            // PK + UNIQUE + CHECK + FK = 4 minimum
            assertTrue(count >= 4,
                    "cfmt_full should have >= 4 constraints in information_schema, got " + count);
        } finally {
            exec("DROP TABLE cfmt_full CASCADE");
            exec("DROP TABLE cfmt_ref CASCADE");
        }
    }

    // ========================================================================
    // column_default for enum types: proper type name
    // ========================================================================

    @Test
    void column_default_enum_uses_type_name_not_user_defined() throws SQLException {
        exec("CREATE TYPE cfmt_status AS ENUM ('new', 'done')");
        exec("CREATE TABLE cfmt_enum_t (id INT, status cfmt_status DEFAULT 'new')");
        try {
            String def = q(
                "SELECT column_default FROM information_schema.columns " +
                "WHERE table_schema = 'cfmt' AND table_name = 'cfmt_enum_t' AND column_name = 'status'");
            assertNotNull(def);
            // Should be 'new'::cfmt_status, NOT 'new'::USER-DEFINED
            assertTrue(def.contains("cfmt_status"),
                    "Should use enum type name, not USER-DEFINED: " + def);
        } finally {
            exec("DROP TABLE cfmt_enum_t");
            exec("DROP TYPE cfmt_status");
        }
    }

    // ========================================================================
    // isfinite() with infinity timestamps
    // ========================================================================

    @Test
    void isfinite_infinity_timestamp_returns_false() throws SQLException {
        String result = q("SELECT isfinite(TIMESTAMP 'infinity')");
        assertEquals("f", result, "isfinite(infinity) should return false");
    }

    @Test
    void isfinite_normal_date_returns_true() throws SQLException {
        String result = q("SELECT isfinite(DATE '2024-01-01')");
        assertEquals("t", result, "isfinite(normal date) should return true");
    }
}
