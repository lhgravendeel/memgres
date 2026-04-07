package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for pg_catalog function calls and system function patterns
 * used in pg_dump output and migration scripts.
 *
 * Covers:
 * - SELECT pg_catalog.set_config(), used by pg_dump to set search_path
 * - ::regclass casts in function arguments
 * - pg_catalog qualified function calls
 * - Implicit joins with pg_catalog tables (pg_index, pg_attribute, etc.)
 * - textin/int2vectorout functions used in pg_dump views
 */
class PgCatalogFunctionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
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
    // SELECT pg_catalog.set_config()
    // =========================================================================

    @Test
    void testSetConfigSearchPath() throws SQLException {
        exec("SELECT pg_catalog.set_config('search_path', 'public', false)");
        // After setting search_path, unqualified table names should resolve to public
        exec("CREATE TABLE after_setconfig (id serial PRIMARY KEY)");
        exec("INSERT INTO after_setconfig DEFAULT VALUES");
    }

    @Test
    void testSetConfigEmptySearchPath() throws SQLException {
        exec("SELECT pg_catalog.set_config('search_path', '', false)");
        // With empty search_path, must use schema-qualified names
        exec("CREATE TABLE public.qualified_after_empty (id serial PRIMARY KEY)");
        // Reset for other tests
        exec("SET search_path TO public");
    }

    @Test
    void testSetConfigLocalTrue() throws SQLException {
        // local=true means the setting only lasts for the current transaction
        exec("BEGIN");
        exec("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)");
        exec("COMMIT");
        // After commit, should revert to previous search_path
    }

    // =========================================================================
    // pg_catalog qualified function calls
    // =========================================================================

    @Test
    void testPgCatalogCurrentSchema() throws SQLException {
        assertNotNull(query1("SELECT pg_catalog.current_schema()"));
    }

    @Test
    void testPgCatalogCurrentUser() throws SQLException {
        assertNotNull(query1("SELECT pg_catalog.current_user"));
    }

    @Test
    void testPgCatalogArrayToString() throws SQLException {
        assertEquals("a,b,c", query1("SELECT pg_catalog.array_to_string(ARRAY['a','b','c'], ',')"));
    }

    // =========================================================================
    // ::regclass cast
    // =========================================================================

    @Test
    void testRegclassCast() throws SQLException {
        exec("CREATE TABLE regclass_test (id serial PRIMARY KEY, name text)");
        assertNotNull(query1("SELECT 'regclass_test'::regclass"));
    }

    @Test
    void testRegclassInNextval() throws SQLException {
        exec("CREATE TABLE regclass_seq_test (id bigint NOT NULL)");
        exec("CREATE SEQUENCE regclass_seq_test_id START WITH 1");
        exec("ALTER TABLE regclass_seq_test ALTER COLUMN id SET DEFAULT nextval('regclass_seq_test_id'::regclass)");
        exec("INSERT INTO regclass_seq_test DEFAULT VALUES");
        assertEquals("1", query1("SELECT id FROM regclass_seq_test"));
    }

    // =========================================================================
    // Implicit join with pg_catalog tables
    // =========================================================================

    @Test
    void testImplicitJoinPgIndexPgAttribute() throws SQLException {
        exec("CREATE TABLE idx_lookup (id serial PRIMARY KEY, name text, email text)");
        exec("CREATE INDEX idx_lookup_name ON idx_lookup (name)");
        // Old-style implicit join querying index columns
        assertNotNull(query1("""
            SELECT array_to_string(array_agg(a.attname), ', ')
            FROM pg_index ix, pg_attribute a
            WHERE ix.indexrelid = 'idx_lookup_name'::regclass
              AND a.attrelid = ix.indrelid
              AND a.attnum = ANY(ix.indkey)
        """));
    }

    @Test
    void testImplicitJoinPgClassPgNamespace() throws SQLException {
        assertNotNull(query1("""
            SELECT c.relname
            FROM pg_class c, pg_namespace n
            WHERE c.relnamespace = n.oid
              AND n.nspname = 'public'
              AND c.relkind = 'r'
            LIMIT 1
        """));
    }

    @Test
    void testImplicitJoinInDoBlock() throws SQLException {
        exec("CREATE TABLE do_idx_test (id serial PRIMARY KEY, val text)");
        exec("CREATE INDEX do_idx_val ON do_idx_test (val)");
        exec("""
            DO $$
            DECLARE
                col_names text;
            BEGIN
                SELECT array_to_string(array_agg(a.attname), ', ')
                INTO col_names
                FROM pg_index ix, pg_attribute a
                WHERE ix.indexrelid = 'do_idx_val'::regclass
                  AND a.attrelid = ix.indrelid
                  AND a.attnum = ANY(ix.indkey);
                RAISE NOTICE 'Index columns: %', col_names;
            END;
            $$
        """);
    }

    // =========================================================================
    // current_schema() in WHERE clause
    // =========================================================================

    @Test
    void testCurrentSchemaInWhere() throws SQLException {
        exec("CREATE TABLE cs_test (id serial PRIMARY KEY)");
        assertNotNull(query1("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = current_schema()
            AND tablename = 'cs_test'
        """));
    }

    @Test
    void testQuotedCurrentSchema() throws SQLException {
        // pg_dump uses "current_schema"() with double quotes
        assertNotNull(query1("SELECT \"current_schema\"()"));
    }

    // =========================================================================
    // pg_catalog.obj_description / col_description
    // =========================================================================

    @Test
    void testObjDescription() throws SQLException {
        exec("CREATE TABLE desc_test (id serial PRIMARY KEY)");
        exec("COMMENT ON TABLE desc_test IS 'A test table'");
        // obj_description returns the comment
        assertNotNull(query1("SELECT obj_description('desc_test'::regclass, 'pg_class')"));
    }

    // =========================================================================
    // Multiple pg_catalog tables in implicit join (3+ tables)
    // =========================================================================

    @Test
    void testThreeWayImplicitJoinCatalog() throws SQLException {
        exec("CREATE TABLE three_way (id serial PRIMARY KEY, ref_id int REFERENCES three_way(id))");
        // Query foreign keys using 3-way implicit join
        exec("""
            DO $$
            DECLARE
                fk_count int;
            BEGIN
                SELECT COUNT(*) INTO fk_count
                FROM pg_constraint c, pg_class t, pg_namespace n
                WHERE c.conrelid = t.oid
                  AND t.relnamespace = n.oid
                  AND n.nspname = 'public'
                  AND c.contype = 'f';
            END;
            $$
        """);
    }
}
