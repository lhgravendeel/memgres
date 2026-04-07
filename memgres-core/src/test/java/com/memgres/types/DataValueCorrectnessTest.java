package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 8: Data value correctness. Validates that query results return
 * the correct values matching PG18 behavior. Covers: JSONB arrow type/null handling,
 * composite array formatting, trigger/rule data propagation, COLLATE behavior,
 * DO block dynamic SQL, regclass/to_regclass display, ts_rank accuracy.
 */
class DataValueCorrectnessTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // JSONB arrow → returns jsonb type, not text
    // ========================================================================

    @Test
    void jsonb_arrow_returns_jsonb_type() throws SQLException {
        // ('{"b":[10,20,null]}'::jsonb)->'b' should return jsonb array, not text
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT ('{\"a\":1,\"b\":[10,20,null]}'::jsonb)->'b'")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val, "jsonb -> should return non-null for existing key");
            assertTrue(val.contains("[10") || val.contains("[10,"),
                    "Should return JSON array value: " + val);
        }
    }

    @Test
    void jsonb_arrow_text_on_array_element() throws SQLException {
        // jsonb->>1 on array should return text of element at index 1
        String result = q("SELECT ('{\"a\":1,\"b\":[10,20,null]}'::jsonb)->'b'->>1");
        assertNotNull(result);
        assertEquals("20", result, "Array element at index 1 should be 20");
    }

    // ========================================================================
    // Composite array formatting: quoted elements
    // ========================================================================

    @Test
    void composite_array_elements_are_quoted() throws SQLException {
        exec("CREATE TYPE dv_pair AS (x INT, y TEXT)");
        try {
            String result = q("SELECT ARRAY[ROW(1,'a')::dv_pair, ROW(2,'b')::dv_pair]");
            assertNotNull(result);
            // PG formats: {"(1,a)","(2,b)"}, with double quotes around each element
            assertTrue(result.contains("\"("), "Composite array elements should be quoted: " + result);
        } finally {
            exec("DROP TYPE dv_pair CASCADE");
        }
    }

    // ========================================================================
    // COLLATE with upper()
    // ========================================================================

    @Test
    void upper_respects_collate_c() throws SQLException {
        // In COLLATE "C", upper('ä') should produce 'ä' (unchanged; C collation
        // doesn't know about locale-specific case mapping in most implementations)
        // Actually PG's upper() with COLLATE "C" does uppercase, 'ä' → 'Ä'
        // But the behavior depends on the OS locale. The key thing is that
        // COLLATE "C" should be accepted and not change the function behavior
        // in a surprising way.
        String result = q("SELECT upper('abc' COLLATE \"C\")");
        assertEquals("ABC", result, "upper with COLLATE C should work");
    }

    // ========================================================================
    // DO block with EXECUTE dynamic SQL
    // ========================================================================

    @Test
    void do_block_execute_inserts_data() throws SQLException {
        exec("CREATE TABLE dv_dyn (id INT, val TEXT)");
        try {
            exec("DO $$ BEGIN EXECUTE 'INSERT INTO dv_dyn VALUES (1, ''x'')'; END $$");
            assertEquals("x", q("SELECT val FROM dv_dyn WHERE id = 1"),
                    "EXECUTE in DO block should insert data");
        } finally {
            exec("DROP TABLE dv_dyn");
        }
    }

    // ========================================================================
    // to_regclass display: PG omits schema for objects in search_path
    // ========================================================================

    @Test
    void to_regclass_current_schema_returns_unqualified() throws SQLException {
        exec("CREATE TABLE dv_reg (id INT)");
        try {
            // to_regclass for a table in the current schema should return unqualified name
            String result = q("SELECT to_regclass('dv_reg')");
            assertNotNull(result);
            assertEquals("dv_reg", result);
        } finally {
            exec("DROP TABLE dv_reg");
        }
    }

    @Test
    void to_regclass_other_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS dv_other");
        exec("CREATE TABLE dv_other.dv_t (id INT)");
        try {
            String result = q("SELECT to_regclass('dv_other.dv_t')");
            assertNotNull(result, "Should find table in other schema");
            // PG returns unqualified if schema is in search_path, qualified otherwise
        } finally {
            exec("DROP SCHEMA dv_other CASCADE");
        }
    }

    // ========================================================================
    // Trigger data propagation: upper() in trigger function
    // ========================================================================

    @Test
    void trigger_function_modifies_row_data() throws SQLException {
        exec("CREATE TABLE dv_trig_t (id INT, a INT, b TEXT)");
        exec("CREATE FUNCTION dv_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$ " +
             "BEGIN NEW.b := upper(NEW.b); RETURN NEW; END $$");
        exec("CREATE TRIGGER dv_trig BEFORE INSERT ON dv_trig_t " +
             "FOR EACH ROW EXECUTE FUNCTION dv_trig_fn()");
        try {
            exec("INSERT INTO dv_trig_t VALUES (1, 10, 'hello')");
            assertEquals("HELLO", q("SELECT b FROM dv_trig_t WHERE id = 1"),
                    "Trigger should have uppercased the value");
        } finally {
            exec("DROP TABLE dv_trig_t CASCADE");
            exec("DROP FUNCTION IF EXISTS dv_trig_fn()");
        }
    }

    // ========================================================================
    // INSTEAD rule INSERT propagates data
    // ========================================================================

    @Test
    void instead_rule_insert_applies_values() throws SQLException {
        exec("CREATE TABLE dv_base (id INT, a INT, b TEXT)");
        exec("CREATE VIEW dv_rule_v AS SELECT * FROM dv_base");
        exec("CREATE RULE dv_ins AS ON INSERT TO dv_rule_v DO INSTEAD " +
             "INSERT INTO dv_base VALUES (NEW.id, NEW.a, 'rule')");
        try {
            exec("INSERT INTO dv_rule_v VALUES (4, 40, 'ignored')");
            assertEquals("rule", q("SELECT b FROM dv_base WHERE id = 4"),
                    "INSTEAD rule should have set b to 'rule'");
        } finally {
            exec("DROP VIEW dv_rule_v CASCADE");
            exec("DROP TABLE dv_base CASCADE");
        }
    }

    // ========================================================================
    // Generated column value in UPDATE through view
    // ========================================================================

    @Test
    void updatable_view_update_recomputes_generated() throws SQLException {
        exec("CREATE TABLE dv_gen (id INT PRIMARY KEY, a INT, d INT GENERATED ALWAYS AS (a * 10) STORED)");
        exec("CREATE VIEW dv_gen_v AS SELECT id, a, d FROM dv_gen");
        exec("INSERT INTO dv_gen (id, a) VALUES (1, 5)");
        try {
            exec("UPDATE dv_gen_v SET a = 7 WHERE id = 1");
            assertEquals("70", q("SELECT d FROM dv_gen WHERE id = 1"),
                    "Generated column should be recomputed after UPDATE through view");
        } finally {
            exec("DROP VIEW dv_gen_v");
            exec("DROP TABLE dv_gen");
        }
    }

    // ========================================================================
    // Identity column sequence: consistent numbering
    // ========================================================================

    @Test
    void identity_column_sequences_correctly() throws SQLException {
        exec("CREATE TABLE dv_ident (id INT GENERATED ALWAYS AS IDENTITY, val TEXT)");
        try {
            exec("INSERT INTO dv_ident (val) VALUES ('a')");
            exec("INSERT INTO dv_ident (val) VALUES ('b')");
            exec("INSERT INTO dv_ident (val) VALUES ('c')");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id FROM dv_ident ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            }
        } finally {
            exec("DROP TABLE dv_ident");
        }
    }

    // ========================================================================
    // JSON -> null value semantics
    // ========================================================================

    @Test
    void jsonb_arrow_null_returns_json_null_string() throws SQLException {
        // jsonb->'key' where value is JSON null → returns string "null" (not SQL NULL)
        String result = q("SELECT ('{\"a\":null}'::jsonb)->'a'");
        assertEquals("null", result, "-> on null value should return JSON null string");
    }

    @Test
    void jsonb_arrow_text_null_returns_sql_null() throws SQLException {
        // jsonb->>'key' where value is JSON null → returns SQL NULL
        String result = q("SELECT ('{\"a\":null}'::jsonb)->>'a'");
        assertNull(result, "->> on null value should return SQL NULL");
    }

    @Test
    void jsonb_arrow_missing_key_returns_sql_null() throws SQLException {
        // jsonb->'missing' → returns SQL NULL (key doesn't exist)
        String result = q("SELECT ('{\"a\":1}'::jsonb)->'missing'");
        assertNull(result, "-> on missing key should return SQL NULL");
    }
}
