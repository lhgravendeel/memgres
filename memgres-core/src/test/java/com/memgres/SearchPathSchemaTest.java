package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that search_path and current_schema() work correctly,
 * matching PostgreSQL 18 behavior.
 */
class SearchPathSchemaTest {

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

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // 1. current_schema() respects search_path
    // ========================================================================

    @Test
    void current_schema_default_is_public() throws SQLException {
        String schema = querySingle("SELECT current_schema()");
        assertEquals("public", schema);
    }

    @Test
    void current_schema_follows_search_path() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS myschema");
        exec("SET search_path TO myschema, public");
        try {
            String schema = querySingle("SELECT current_schema()");
            assertEquals("myschema", schema);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS myschema CASCADE");
        }
    }

    @Test
    void current_schema_skips_nonexistent_schema() throws SQLException {
        exec("SET search_path TO no_such_schema, public");
        try {
            String schema = querySingle("SELECT current_schema()");
            assertEquals("public", schema,
                    "Should skip nonexistent schema and return first valid one");
        } finally {
            exec("SET search_path TO public, pg_catalog");
        }
    }

    @Test
    void current_schema_skips_dollar_user() throws SQLException {
        // Default search_path is "$user", public; $user is skipped
        exec("SET search_path TO \"$user\", public");
        try {
            String schema = querySingle("SELECT current_schema()");
            assertEquals("public", schema,
                    "$user should be skipped, falling through to public");
        } finally {
            exec("SET search_path TO public, pg_catalog");
        }
    }

    // ========================================================================
    // 2. current_schemas() respects search_path
    // ========================================================================

    @Test
    void current_schemas_without_implicit() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS s1");
        exec("SET search_path TO s1, public");
        try {
            String schemas = querySingle("SELECT current_schemas(false)");
            assertNotNull(schemas);
            assertTrue(schemas.contains("s1"), "Should contain s1: " + schemas);
            assertTrue(schemas.contains("public"), "Should contain public: " + schemas);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS s1 CASCADE");
        }
    }

    @Test
    void current_schemas_with_implicit() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS s2");
        exec("SET search_path TO s2, public");
        try {
            String schemas = querySingle("SELECT current_schemas(true)");
            assertNotNull(schemas);
            assertTrue(schemas.contains("pg_catalog"), "Should include pg_catalog: " + schemas);
            assertTrue(schemas.contains("s2"), "Should contain s2: " + schemas);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS s2 CASCADE");
        }
    }

    // ========================================================================
    // 3. DDL goes to effective schema
    // ========================================================================

    @Test
    void create_table_uses_search_path_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS ddl_test");
        exec("SET search_path TO ddl_test, public");
        try {
            exec("CREATE TABLE sp_table (id INT, name TEXT)");

            // Table should be in ddl_test schema, not public
            String schema = querySingle(
                    "SELECT table_schema FROM information_schema.tables WHERE table_name = 'sp_table'");
            assertEquals("ddl_test", schema, "Table should be created in ddl_test schema");

            // Can still access it unqualified
            exec("INSERT INTO sp_table VALUES (1, 'test')");
            String name = querySingle("SELECT name FROM sp_table WHERE id = 1");
            assertEquals("test", name);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS ddl_test CASCADE");
        }
    }

    @Test
    void schema_qualified_overrides_search_path() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS override_test");
        exec("SET search_path TO override_test, public");
        try {
            // Create table explicitly in public schema
            exec("CREATE TABLE public.explicit_table (val TEXT)");
            exec("INSERT INTO public.explicit_table VALUES ('in_public')");

            String val = querySingle("SELECT val FROM public.explicit_table");
            assertEquals("in_public", val);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP TABLE IF EXISTS public.explicit_table CASCADE");
            exec("DROP SCHEMA IF EXISTS override_test CASCADE");
        }
    }

    // ========================================================================
    // 4. DML finds tables via search_path
    // ========================================================================

    @Test
    void insert_update_delete_via_search_path() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS dml_test");
        exec("SET search_path TO dml_test, public");
        try {
            exec("CREATE TABLE dml_t (id INT PRIMARY KEY, val TEXT)");
            exec("INSERT INTO dml_t VALUES (1, 'original')");

            String val = querySingle("SELECT val FROM dml_t WHERE id = 1");
            assertEquals("original", val);

            exec("UPDATE dml_t SET val = 'updated' WHERE id = 1");
            val = querySingle("SELECT val FROM dml_t WHERE id = 1");
            assertEquals("updated", val);

            exec("DELETE FROM dml_t WHERE id = 1");
            String count = querySingle("SELECT COUNT(*) FROM dml_t");
            assertEquals("0", count);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS dml_test CASCADE");
        }
    }

    @Test
    void search_path_resolution_order() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS first_schema");
        exec("CREATE SCHEMA IF NOT EXISTS second_schema");
        exec("SET search_path TO first_schema, second_schema, public");
        try {
            // Create same-named table in different schemas with different data
            exec("CREATE TABLE first_schema.lookup (val TEXT)");
            exec("INSERT INTO first_schema.lookup VALUES ('from_first')");
            exec("CREATE TABLE second_schema.lookup (val TEXT)");
            exec("INSERT INTO second_schema.lookup VALUES ('from_second')");

            // Unqualified should resolve to first_schema (first in search_path)
            String val = querySingle("SELECT val FROM lookup");
            assertEquals("from_first", val,
                    "Should resolve to first_schema.lookup (first in search_path)");

            // Qualified should override
            String val2 = querySingle("SELECT val FROM second_schema.lookup");
            assertEquals("from_second", val2);
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS first_schema CASCADE");
            exec("DROP SCHEMA IF EXISTS second_schema CASCADE");
        }
    }

    // ========================================================================
    // 5. System catalog respects current_schema
    // ========================================================================

    @Test
    void information_schema_tables_filters_by_current_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS cat_test");
        exec("SET search_path TO cat_test, public");
        try {
            exec("CREATE TABLE cat_table (id INT)");

            // Query using current_schema() to filter; should find the table
            String found = querySingle(
                    "SELECT table_name FROM information_schema.tables " +
                    "WHERE table_schema = current_schema() AND table_name = 'cat_table'");
            assertEquals("cat_table", found,
                    "information_schema should find table via current_schema()");
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS cat_test CASCADE");
        }
    }

    // ========================================================================
    // 6. Default behavior preserved
    // ========================================================================

    @Test
    void default_search_path_still_uses_public() throws SQLException {
        // Without any SET, default should be public
        exec("CREATE TABLE default_sp_test (x INT)");
        try {
            String schema = querySingle(
                    "SELECT table_schema FROM information_schema.tables WHERE table_name = 'default_sp_test'");
            assertEquals("public", schema);
        } finally {
            exec("DROP TABLE IF EXISTS default_sp_test CASCADE");
        }
    }

    // ========================================================================
    // 7. Transactions work with non-default schema
    // ========================================================================

    @Test
    void transaction_rollback_in_custom_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS tx_test");
        exec("SET search_path TO tx_test, public");
        try {
            exec("CREATE TABLE tx_t (id INT)");
            exec("BEGIN");
            exec("INSERT INTO tx_t VALUES (1)");
            exec("ROLLBACK");

            String count = querySingle("SELECT COUNT(*) FROM tx_t");
            assertEquals("0", count, "Rollback should undo insert in non-public schema");
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS tx_test CASCADE");
        }
    }

    // ========================================================================
    // 8. DROP TABLE uses search_path
    // ========================================================================

    @Test
    void drop_table_uses_search_path() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS drop_test");
        exec("SET search_path TO drop_test, public");
        try {
            exec("CREATE TABLE drop_me (id INT)");
            // Table is in drop_test schema
            String schema = querySingle(
                    "SELECT table_schema FROM information_schema.tables WHERE table_name = 'drop_me'");
            assertEquals("drop_test", schema);

            // DROP without qualification should find it via search_path
            exec("DROP TABLE drop_me");

            String gone = querySingle(
                    "SELECT table_name FROM information_schema.tables WHERE table_name = 'drop_me'");
            assertNull(gone, "Table should be dropped");
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS drop_test CASCADE");
        }
    }

    // ========================================================================
    // 9. ALTER TABLE uses search_path
    // ========================================================================

    @Test
    void alter_table_uses_search_path() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS alter_test");
        exec("SET search_path TO alter_test, public");
        try {
            exec("CREATE TABLE alter_me (id INT)");
            exec("ALTER TABLE alter_me ADD COLUMN name TEXT");

            String col = querySingle(
                    "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'alter_test' AND table_name = 'alter_me' AND column_name = 'name'");
            assertEquals("name", col, "ALTER TABLE should work on table found via search_path");
        } finally {
            exec("SET search_path TO public, pg_catalog");
            exec("DROP SCHEMA IF EXISTS alter_test CASCADE");
        }
    }
}
