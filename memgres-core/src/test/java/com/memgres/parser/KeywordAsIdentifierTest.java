package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL keywords used as column names or identifiers.
 *
 * PostgreSQL allows many keywords to be used as column names without quoting.
 * Some like INTERVAL, TYPE, STATUS, ACTION, KEY, VALUE are commonly used
 * in real-world schemas as column names.
 *
 * Also covers implicit (old-style) joins with comma syntax in FROM clause,
 * and SELECT pg_catalog.set_config() function calls.
 */
class KeywordAsIdentifierTest {

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
    // INTERVAL as column name
    // =========================================================================

    @Test
    void testIntervalAsColumnName() throws SQLException {
        exec("CREATE TABLE plans (id serial PRIMARY KEY, name text, interval text NOT NULL DEFAULT 'month')");
        exec("INSERT INTO plans (name, interval) VALUES ('basic', 'month')");
        assertEquals("month", query1("SELECT interval FROM plans WHERE id = 1"));
    }

    @Test
    void testIntervalInCaseExpression() throws SQLException {
        // The exact failing pattern: UPDATE ... SET col = CASE WHEN interval = 'month' THEN ...
        exec("CREATE TABLE subscriptions (id serial PRIMARY KEY, interval text, interval_enum text)");
        exec("INSERT INTO subscriptions (interval) VALUES ('month'), ('year'), ('week')");
        exec("""
            UPDATE subscriptions SET interval_enum = CASE
                WHEN interval = 'month' THEN 'monthly'
                WHEN interval = 'year' THEN 'yearly'
                ELSE NULL
            END
        """);
        assertEquals("monthly", query1("SELECT interval_enum FROM subscriptions WHERE interval = 'month'"));
    }

    @Test
    void testIntervalInWhereClause() throws SQLException {
        exec("CREATE TABLE billing (id serial PRIMARY KEY, interval text, amount numeric)");
        exec("INSERT INTO billing (interval, amount) VALUES ('month', 9.99), ('year', 99.99)");
        assertEquals("99.99", query1("SELECT amount FROM billing WHERE interval = 'year'"));
    }

    // =========================================================================
    // TYPE as column name
    // =========================================================================

    @Test
    void testTypeAsColumnName() throws SQLException {
        exec("CREATE TABLE entries (id serial PRIMARY KEY, type text NOT NULL)");
        exec("INSERT INTO entries (type) VALUES ('article')");
        assertEquals("article", query1("SELECT type FROM entries WHERE id = 1"));
    }

    @Test
    void testTypeInCaseExpression() throws SQLException {
        exec("CREATE TABLE items (id serial PRIMARY KEY, type text, category text)");
        exec("INSERT INTO items (type) VALUES ('widget'), ('gadget')");
        exec("UPDATE items SET category = CASE WHEN type = 'widget' THEN 'hardware' ELSE 'other' END");
    }

    // =========================================================================
    // ACTION, KEY, VALUE as column names
    // =========================================================================

    @Test
    void testActionAsColumnName() throws SQLException {
        exec("CREATE TABLE audit (id serial PRIMARY KEY, action text NOT NULL, ts timestamp DEFAULT now())");
        exec("INSERT INTO audit (action) VALUES ('login')");
        assertEquals("login", query1("SELECT action FROM audit WHERE id = 1"));
    }

    @Test
    void testKeyValueAsColumnNames() throws SQLException {
        exec("CREATE TABLE settings (key text PRIMARY KEY, value text)");
        exec("INSERT INTO settings (key, value) VALUES ('theme', 'dark')");
        assertEquals("dark", query1("SELECT value FROM settings WHERE key = 'theme'"));
    }

    @Test
    void testStatusAsColumnName() throws SQLException {
        exec("CREATE TABLE jobs (id serial PRIMARY KEY, status text DEFAULT 'pending')");
        exec("INSERT INTO jobs DEFAULT VALUES");
        assertEquals("pending", query1("SELECT status FROM jobs WHERE id = 1"));
    }

    // =========================================================================
    // NAME, ROLE, USER as column names (reserved/non-reserved keywords)
    // =========================================================================

    @Test
    void testNameAsColumnName() throws SQLException {
        exec("CREATE TABLE labels (id serial PRIMARY KEY, name text NOT NULL)");
        exec("INSERT INTO labels (name) VALUES ('test')");
        assertEquals("test", query1("SELECT name FROM labels WHERE id = 1"));
    }

    @Test
    void testRoleAsColumnNameQuoted() throws SQLException {
        // ROLE is a reserved keyword and must be quoted
        exec("CREATE TABLE memberships (id serial PRIMARY KEY, \"role\" text)");
        exec("INSERT INTO memberships (\"role\") VALUES ('admin')");
        assertEquals("admin", query1("SELECT \"role\" FROM memberships WHERE id = 1"));
    }

    // =========================================================================
    // Implicit (old-style) comma join with aliases
    // =========================================================================

    @Test
    void testImplicitCommaJoin() throws SQLException {
        exec("CREATE TABLE dept (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE emp (id serial PRIMARY KEY, name text, dept_id int)");
        exec("INSERT INTO dept (name) VALUES ('Engineering')");
        exec("INSERT INTO emp (name, dept_id) VALUES ('Alice', 1)");
        assertEquals("Engineering", query1("""
            SELECT d.name FROM dept d, emp e WHERE e.dept_id = d.id AND e.name = 'Alice'
        """));
    }

    @Test
    void testImplicitCommaJoinThreeTables() throws SQLException {
        exec("CREATE TABLE t_a (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE t_b (id serial PRIMARY KEY, a_id int, info text)");
        exec("CREATE TABLE t_c (id serial PRIMARY KEY, b_id int, detail text)");
        exec("INSERT INTO t_a (val) VALUES ('root')");
        exec("INSERT INTO t_b (a_id, info) VALUES (1, 'mid')");
        exec("INSERT INTO t_c (b_id, detail) VALUES (1, 'leaf')");
        assertEquals("leaf", query1("""
            SELECT c.detail FROM t_a a, t_b b, t_c c
            WHERE b.a_id = a.id AND c.b_id = b.id
        """));
    }

    @Test
    void testImplicitJoinInDoBlock() throws SQLException {
        // Real-world pattern: DO block querying pg_catalog with implicit joins
        exec("CREATE TABLE test_tab (id serial PRIMARY KEY, val text)");
        exec("CREATE INDEX idx_test_val ON test_tab (val)");
        exec("""
            DO $$
            DECLARE
                col_name text;
            BEGIN
                SELECT array_to_string(array_agg(a.attname), ', ')
                INTO col_name
                FROM pg_index ix, pg_attribute a
                WHERE ix.indexrelid = 'idx_test_val'::regclass
                  AND a.attrelid = ix.indrelid
                  AND a.attnum = ANY(ix.indkey);
            END;
            $$
        """);
    }

    @Test
    void testImplicitJoinWithSchemaQualifiedTable() throws SQLException {
        exec("CREATE TABLE ref_a (id serial PRIMARY KEY, code text)");
        exec("CREATE TABLE ref_b (id serial PRIMARY KEY, a_code text, label text)");
        exec("INSERT INTO ref_a (code) VALUES ('X')");
        exec("INSERT INTO ref_b (a_code, label) VALUES ('X', 'found')");
        assertEquals("found", query1("""
            SELECT b.label FROM public.ref_a a, public.ref_b b WHERE b.a_code = a.code
        """));
    }

    // =========================================================================
    // SELECT pg_catalog.set_config()
    // =========================================================================

    @Test
    void testPgCatalogSetConfig() throws SQLException {
        // pg_dump uses this to set search_path
        exec("SELECT pg_catalog.set_config('search_path', 'public', false)");
    }

    @Test
    void testPgCatalogSetConfigEmptySearchPath() throws SQLException {
        exec("SELECT pg_catalog.set_config('search_path', '', false)");
        // After empty search_path, schema-qualified names should still work
        exec("CREATE TABLE public.after_empty_sp (id serial PRIMARY KEY)");
        exec("INSERT INTO public.after_empty_sp DEFAULT VALUES");
        // Reset
        exec("SET search_path TO public");
    }

    // =========================================================================
    // UPDATE with keyword column name in CASE
    // =========================================================================

    @Test
    void testUpdateWithKeywordColumnInCase() throws SQLException {
        exec("CREATE TYPE billing_interval AS ENUM ('monthly', 'yearly')");
        exec("CREATE TABLE subs (id serial PRIMARY KEY, interval text, interval_type billing_interval)");
        exec("INSERT INTO subs (interval) VALUES ('month'), ('year')");
        exec("""
            UPDATE subs SET interval_type = CASE
                WHEN interval = 'month' THEN 'monthly'::billing_interval
                WHEN interval = 'year' THEN 'yearly'::billing_interval
                ELSE NULL
            END
        """);
        assertEquals("monthly", query1("SELECT interval_type FROM subs WHERE interval = 'month'"));
    }

    // =========================================================================
    // Multiple keyword columns in WHERE
    // =========================================================================

    @Test
    void testMultipleKeywordColumnsInWhere() throws SQLException {
        exec("CREATE TABLE config (key text, value text, type text, action text, status text)");
        exec("INSERT INTO config VALUES ('k1', 'v1', 'string', 'create', 'active')");
        assertEquals("v1", query1("""
            SELECT value FROM config WHERE key = 'k1' AND type = 'string' AND action = 'create' AND status = 'active'
        """));
    }
}
