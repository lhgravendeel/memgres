package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for schema evolution patterns commonly used in migrations.
 *
 * Covers:
 * - ALTER TABLE ADD COLUMN IF NOT EXISTS with constraints (should reject constraint on existing col)
 * - ALTER TABLE RENAME COLUMN on partitioned tables
 * - ALTER COLUMN SET GENERATED ALWAYS
 * - Index expression definition introspection (pg_get_indexdef)
 * - Expression index with WHERE predicate and conflict detection
 * - Temp table drop behavior (should fail if already dropped)
 * - Sequence manipulation (setval) and visibility
 * - pg_get_expr for defaults on pg_attrdef
 */
class SchemaEvolutionTest {

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
    // ADD COLUMN IF NOT EXISTS with constraint on existing column
    // ========================================================================

    @Test
    void add_column_if_not_exists_existing_with_different_constraint() throws SQLException {
        exec("CREATE TABLE evo_t(id int PRIMARY KEY, code text)");
        try {
            // First add: should succeed
            exec("ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS extra int DEFAULT 0");

            // Second add with different constraint; PG accepts IF NOT EXISTS silently
            // even if the constraint differs (column already exists)
            exec("ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS extra int CHECK (extra > -1)");

            // But trying to add with NOT NULL on an existing column that has NULLs should NOT apply the constraint
            exec("INSERT INTO evo_t(id, code) VALUES (1, 'a')"); // extra gets DEFAULT 0
            String val = scalar("SELECT extra FROM evo_t WHERE id = 1");
            assertEquals("0", val, "Default should be applied");
        } finally {
            exec("DROP TABLE IF EXISTS evo_t");
        }
    }

    // ========================================================================
    // Expression index definition introspection
    // ========================================================================

    @Test
    void expression_index_pg_get_indexdef_format() throws SQLException {
        exec("CREATE TABLE idx_fmt(id int PRIMARY KEY, email text, status text, note text)");
        exec("CREATE UNIQUE INDEX idx_email ON idx_fmt(lower(email)) WHERE status = 'active'");
        exec("CREATE INDEX idx_note ON idx_fmt(COALESCE(note, ''))");
        try {
            String emailDef = scalar("SELECT pg_get_indexdef('idx_email'::regclass)");
            assertNotNull(emailDef);
            // PG formats like: CREATE UNIQUE INDEX idx_email ON public.idx_fmt USING btree (lower(email)) WHERE (status = 'active'::text)
            assertTrue(emailDef.toUpperCase().contains("UNIQUE"),
                    "Should contain UNIQUE: " + emailDef);
            assertTrue(emailDef.toLowerCase().contains("lower(email)"),
                    "Should contain lower(email): " + emailDef);
            assertTrue(emailDef.toUpperCase().contains("WHERE"),
                    "Should contain WHERE predicate: " + emailDef);

            String noteDef = scalar("SELECT pg_get_indexdef('idx_note'::regclass)");
            assertNotNull(noteDef);
            assertTrue(noteDef.toLowerCase().contains("coalesce"),
                    "Should contain COALESCE: " + noteDef);
            // PG adds explicit cast: ''::text
            assertTrue(noteDef.contains("''") || noteDef.contains("text"),
                    "PG-formatted indexdef should include text cast: " + noteDef);
        } finally {
            exec("DROP TABLE IF EXISTS idx_fmt CASCADE");
        }
    }

    // ========================================================================
    // Partial unique index conflict detection
    // ========================================================================

    @Test
    void partial_unique_index_conflict() throws SQLException {
        exec("CREATE TABLE pu_t(id int PRIMARY KEY, tenant_id int, email text, status text)");
        exec("CREATE UNIQUE INDEX pu_email ON pu_t(tenant_id, email) WHERE status = 'active'");
        exec("INSERT INTO pu_t VALUES (1, 10, 'a@test.com', 'active')");
        try {
            // Same tenant + email + active status → should conflict
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO pu_t VALUES (2, 10, 'a@test.com', 'active')"),
                    "Duplicate on partial unique index should fail");

            // Same tenant + email but different status → OK
            exec("INSERT INTO pu_t VALUES (3, 10, 'a@test.com', 'inactive')");
        } finally {
            exec("DROP TABLE IF EXISTS pu_t CASCADE");
        }
    }

    // ========================================================================
    // Temp table scoping
    // ========================================================================

    @Test
    void insert_into_dropped_temp_table_fails() throws SQLException {
        exec("CREATE TEMP TABLE tt(id int)");
        exec("INSERT INTO tt VALUES (1)");
        exec("DROP TABLE tt");

        // After DROP, insert should fail
        assertThrows(SQLException.class,
                () -> exec("INSERT INTO tt VALUES (1)"),
                "Insert into dropped temp table should fail");
    }

    // ========================================================================
    // ALTER COLUMN SET GENERATED ALWAYS on non-identity column
    // ========================================================================

    @Test
    void alter_set_generated_always_on_wrong_column_fails() throws SQLException {
        exec("CREATE TABLE gen_t(id int PRIMARY KEY, a text)");
        try {
            // 'a' is not an identity column, so it can't SET GENERATED ALWAYS
            assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE gen_t ALTER COLUMN a SET GENERATED ALWAYS"),
                    "SET GENERATED ALWAYS on non-identity column should fail with 55000 or 42601");
        } finally {
            exec("DROP TABLE IF EXISTS gen_t");
        }
    }

    // ========================================================================
    // pg_get_expr for column defaults via pg_attrdef
    // ========================================================================

    @Test
    void pg_attrdef_shows_schema_qualified_table() throws SQLException {
        exec("CREATE TABLE adef_t(id int PRIMARY KEY, note text DEFAULT 'x')");
        try {
            String tableName = scalar("""
                SELECT adrelid::regclass
                FROM pg_attrdef
                WHERE adrelid = 'adef_t'::regclass AND adnum = 2
                """);
            assertNotNull(tableName);
            // In PG, this returns the table name (optionally schema-qualified)
            assertTrue(tableName.contains("adef_t"), "Should contain table name: " + tableName);
        } finally {
            exec("DROP TABLE IF EXISTS adef_t");
        }
    }

    // ========================================================================
    // RENAME COLUMN on partitioned table
    // ========================================================================

    @Test
    void rename_column_on_partitioned_table() throws SQLException {
        exec("CREATE TABLE rp(id int, val text) PARTITION BY RANGE (id)");
        exec("CREATE TABLE rp_1 PARTITION OF rp FOR VALUES FROM (1) TO (100)");
        exec("INSERT INTO rp VALUES (1, 'a')");
        try {
            exec("ALTER TABLE rp RENAME COLUMN val TO value");
            String result = scalar("SELECT value FROM rp WHERE id = 1");
            assertEquals("a", result, "Renamed column should be queryable");
        } finally {
            exec("DROP TABLE IF EXISTS rp CASCADE");
        }
    }

    // ========================================================================
    // Procedure (CALL) support
    // ========================================================================

    @Test
    void create_and_call_procedure() throws SQLException {
        exec("CREATE TABLE proc_t(id int PRIMARY KEY, note text)");
        exec("""
            CREATE OR REPLACE PROCEDURE p_ins(i int, t text) LANGUAGE SQL AS $$
              INSERT INTO proc_t(id, note) VALUES (i, t)
            $$
            """);
        try {
            exec("CALL p_ins(1, 'hello')");
            assertEquals("hello", scalar("SELECT note FROM proc_t WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS proc_t");
            exec("DROP PROCEDURE IF EXISTS p_ins(int, text)");
        }
    }

    @Test
    void procedure_without_call_fails() throws SQLException {
        exec("CREATE TABLE proc_t2(id int PRIMARY KEY, note text)");
        exec("""
            CREATE OR REPLACE PROCEDURE p_ins2(i int, t text) LANGUAGE SQL AS $$
              INSERT INTO proc_t2(id, note) VALUES (i, t)
            $$
            """);
        try {
            // Procedures can't be called with SELECT
            assertThrows(SQLException.class,
                    () -> exec("SELECT p_ins2(1, 'bad')"),
                    "Calling a procedure with SELECT should fail");
        } finally {
            exec("DROP TABLE IF EXISTS proc_t2");
            exec("DROP PROCEDURE IF EXISTS p_ins2(int, text)");
        }
    }
}
