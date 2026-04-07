package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for schema-qualified identifiers and cross-schema operations
 * found in real-world schemas. Many projects use custom schemas (auth, app, etc.)
 * and pg_dump always qualifies with public.
 */
class SchemaQualificationCompatTest {

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
    // CREATE SCHEMA
    // =========================================================================

    @Test
    void testCreateSchema() throws SQLException {
        exec("CREATE SCHEMA app_schema");
        exec("CREATE TABLE app_schema.config (key text PRIMARY KEY, val text)");
        exec("INSERT INTO app_schema.config VALUES ('version', '1.0')");
        assertEquals("1.0", query1("SELECT val FROM app_schema.config WHERE key = 'version'"));
    }

    @Test
    void testCreateSchemaIfNotExists() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS idempotent_schema");
        exec("CREATE SCHEMA IF NOT EXISTS idempotent_schema");
    }

    @Test
    void testCreateSchemaAuthorization() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS owned_schema AUTHORIZATION CURRENT_USER");
    }

    // =========================================================================
    // Schema-qualified table references in DML
    // =========================================================================

    @Test
    void testInsertIntoSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sq_dml");
        exec("CREATE TABLE sq_dml.items (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO sq_dml.items (name) VALUES ('qualified')");
        assertEquals("qualified", query1("SELECT name FROM sq_dml.items"));
    }

    @Test
    void testUpdateSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sq_upd");
        exec("CREATE TABLE sq_upd.entries (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO sq_upd.entries (val) VALUES ('old')");
        exec("UPDATE sq_upd.entries SET val = 'new' WHERE id = 1");
        assertEquals("new", query1("SELECT val FROM sq_upd.entries"));
    }

    @Test
    void testDeleteSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sq_del");
        exec("CREATE TABLE sq_del.temp (id serial PRIMARY KEY)");
        exec("INSERT INTO sq_del.temp DEFAULT VALUES");
        exec("DELETE FROM sq_del.temp WHERE id = 1");
        assertEquals("0", query1("SELECT COUNT(*) FROM sq_del.temp"));
    }

    // =========================================================================
    // public. prefix (pg_dump always qualifies with public.)
    // =========================================================================

    @Test
    void testPublicDotCreateTable() throws SQLException {
        exec("CREATE TABLE public.pg_dump_style (id bigint NOT NULL, data text)");
        exec("INSERT INTO public.pg_dump_style VALUES (1, 'test')");
        assertEquals("test", query1("SELECT data FROM public.pg_dump_style WHERE id = 1"));
    }

    @Test
    void testPublicDotSequence() throws SQLException {
        exec("CREATE SEQUENCE public.my_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        assertNotNull(query1("SELECT nextval('public.my_seq')"));
    }

    @Test
    void testPublicDotAlterSequenceOwnedBy() throws SQLException {
        exec("CREATE TABLE public.owned_seq_tbl (id bigint NOT NULL, name text)");
        exec("CREATE SEQUENCE public.owned_seq_tbl_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        exec("ALTER SEQUENCE public.owned_seq_tbl_id_seq OWNED BY public.owned_seq_tbl.id");
    }

    // =========================================================================
    // Cross-schema foreign keys
    // =========================================================================

    @Test
    void testCrossSchemaForeignKey() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS core");
        exec("CREATE SCHEMA IF NOT EXISTS billing");
        exec("CREATE TABLE core.customers (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE billing.invoices (id serial PRIMARY KEY, customer_id int REFERENCES core.customers(id), amount numeric)");
        exec("INSERT INTO core.customers (name) VALUES ('Acme')");
        exec("INSERT INTO billing.invoices (customer_id, amount) VALUES (1, 100.00)");
    }

    // =========================================================================
    // SET search_path
    // =========================================================================

    @Test
    void testSetSearchPath() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sp_test");
        exec("CREATE TABLE sp_test.data (id serial PRIMARY KEY, val text)");
        exec("SET search_path TO sp_test, public");
        exec("INSERT INTO data (val) VALUES ('found via search path')");
        assertEquals("found via search path", query1("SELECT val FROM data"));
        // Reset
        exec("SET search_path TO public");
    }

    // =========================================================================
    // Schema-qualified functions
    // =========================================================================

    @Test
    void testSchemaQualifiedFunctionCreation() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS funcs");
        exec("""
            CREATE FUNCTION funcs.add(a int, b int)
            RETURNS int LANGUAGE sql AS $$ SELECT a + b; $$
        """);
        assertEquals("5", query1("SELECT funcs.add(2, 3)"));
    }

    @Test
    void testSchemaQualifiedTriggerFunction() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS hooks");
        exec("""
            CREATE FUNCTION hooks.set_updated()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN NEW.updated_at := now(); RETURN NEW; END;
            $$
        """);
        exec("CREATE TABLE hook_target (id serial PRIMARY KEY, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE TRIGGER auto_update
            BEFORE UPDATE ON hook_target
            FOR EACH ROW EXECUTE FUNCTION hooks.set_updated()
        """);
    }

    // =========================================================================
    // Schema-qualified type references
    // =========================================================================

    @Test
    void testSchemaQualifiedEnum() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS types");
        exec("CREATE TYPE types.status AS ENUM ('active', 'inactive')");
        exec("CREATE TABLE typed_entries (id serial PRIMARY KEY, status types.status)");
        exec("INSERT INTO typed_entries (status) VALUES ('active')");
        assertEquals("active", query1("SELECT status FROM typed_entries"));
    }

    // =========================================================================
    // DROP SCHEMA CASCADE
    // =========================================================================

    @Test
    void testDropSchemaCascade() throws SQLException {
        exec("CREATE SCHEMA drop_me");
        exec("CREATE TABLE drop_me.t1 (id int)");
        exec("CREATE TABLE drop_me.t2 (id int)");
        exec("DROP SCHEMA drop_me CASCADE");
        // Schema and all contents should be gone
    }

    @Test
    void testDropSchemaRestrict() throws SQLException {
        exec("CREATE SCHEMA drop_restrict");
        exec("CREATE TABLE drop_restrict.t1 (id int)");
        // Should fail because schema is not empty
        assertThrows(SQLException.class, () -> exec("DROP SCHEMA drop_restrict RESTRICT"));
    }

    @Test
    void testDropSchemaIfExists() throws SQLException {
        exec("DROP SCHEMA IF EXISTS nonexistent_schema");
    }

    // =========================================================================
    // Schema-qualified CREATE INDEX
    // =========================================================================

    @Test
    void testIndexOnSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS indexed");
        exec("CREATE TABLE indexed.records (id serial PRIMARY KEY, name text, ts timestamp)");
        exec("CREATE INDEX idx_indexed_name ON indexed.records (name)");
        exec("CREATE INDEX idx_indexed_ts ON indexed.records USING btree (ts DESC NULLS LAST)");
    }
}
