package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for advanced DO block / PL/pgSQL features that still fail.
 *
 * v1 compat tests covered basic labeled blocks and FOREACH.
 * These tests cover the edge cases that still fail:
 * - Labeled blocks with space around << >> markers
 * - CTE (WITH ... AS) used inside a DO block body
 * - DELETE with alias inside DO block
 * - Multiple DECLARE blocks in a single DO
 * - EXECUTE with dynamic SQL containing CTE
 * - GRANT/REVOKE inside DO block
 * - ALTER ROLE ... SET inside DO block
 * - IF current_setting() pattern
 */
class DoBlockAdvancedCompatTest {

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
    // Labeled blocks: various spacing patterns
    // =========================================================================

    @Test
    void testLabeledBlockNoSpaces() throws SQLException {
        exec("""
            DO $$
            <<myblock>>
            DECLARE x int := 0;
            BEGIN
                x := x + 1;
            END myblock;
            $$
        """);
    }

    @Test
    void testLabeledBlockWithSpaces() throws SQLException {
        // Some codebases have spaces around the label: << label >>
        exec("""
            DO $$
            << my_migration_block >>
            DECLARE x int := 0;
            BEGIN
                x := x + 1;
            END my_migration_block;
            $$
        """);
    }

    @Test
    void testLabeledBlockWithUnderscoresInName() throws SQLException {
        exec("""
            DO $$
            <<migrate_if_version_below_500>>
            DECLARE
                current_version VARCHAR(100) := '';
            BEGIN
                current_version := '100';
            END migrate_if_version_below_500;
            $$
        """);
    }

    @Test
    void testLabeledBlockWithComplexBody() throws SQLException {
        exec("CREATE TABLE lb_complex (id serial PRIMARY KEY, val text, score int)");
        exec("INSERT INTO lb_complex (val, score) VALUES ('a', 10), ('b', 20), ('c', 30)");
        exec("""
            DO $$
            <<data_check>>
            DECLARE
                wrong_count integer := 0;
                total_count integer := 0;
            BEGIN
                SELECT COALESCE(
                    SUM(CASE WHEN CHAR_LENGTH(val) > 255 THEN 1 ELSE 0 END),
                    0
                ) INTO wrong_count FROM lb_complex;

                SELECT COUNT(*) INTO total_count FROM lb_complex;

                IF wrong_count > 0 THEN
                    RAISE NOTICE 'Found % invalid rows out of %', wrong_count, total_count;
                END IF;
            END data_check;
            $$
        """);
    }

    // =========================================================================
    // CTE (WITH clause) inside DO block
    // =========================================================================

    @Test
    void testDoBlockWithCte() throws SQLException {
        exec("CREATE TABLE cte_src (id serial PRIMARY KEY, token text, active boolean DEFAULT true)");
        exec("CREATE TABLE cte_archive (id int, token text)");
        exec("INSERT INTO cte_src (token, active) VALUES ('keep', true), ('remove', false)");
        exec("""
            DO $$
            BEGIN
                WITH deleted AS (
                    DELETE FROM cte_src WHERE active = false
                    RETURNING id, token
                )
                INSERT INTO cte_archive SELECT id, token FROM deleted;
            END;
            $$
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM cte_archive"));
    }

    @Test
    void testDoBlockWithCteDelete() throws SQLException {
        // Pattern from migrations: CTE-based cascade delete
        exec("CREATE TABLE session_data (id serial PRIMARY KEY, token text, user_id int)");
        exec("CREATE TABLE access_tokens (id serial PRIMARY KEY, token text, client_id text, user_id int)");
        exec("INSERT INTO access_tokens (token, client_id, user_id) VALUES ('tok1', 'app1', 1)");
        exec("INSERT INTO session_data (token, user_id) VALUES ('tok1', 1)");
        exec("""
            DO $$
            BEGIN
                WITH removed_tokens AS (
                    DELETE FROM access_tokens a
                    WHERE NOT EXISTS (
                        SELECT 1 FROM session_data s
                        WHERE a.user_id = s.user_id
                        AND a.client_id IS NOT NULL
                    )
                    RETURNING a.token
                )
                DELETE FROM session_data s
                WHERE s.token IN (SELECT token FROM removed_tokens);
            END;
            $$
        """);
    }

    // =========================================================================
    // DELETE with alias inside DO block
    // =========================================================================

    @Test
    void testDoBlockDeleteWithAlias() throws SQLException {
        exec("CREATE TABLE del_alias (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO del_alias (val) VALUES ('keep'), ('remove')");
        exec("""
            DO $$
            BEGIN
                DELETE FROM del_alias d WHERE d.val = 'remove';
            END;
            $$
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM del_alias"));
    }

    // =========================================================================
    // Multiple DECLARE sections
    // =========================================================================

    @Test
    void testMultipleDeclareBlocks() throws SQLException {
        exec("""
            DO $$
            DECLARE
                count_a integer := 0;
            DECLARE
                count_b integer := 0;
            DECLARE
                tmp_val integer := 0;
            BEGIN
                count_a := 1;
                count_b := 2;
                tmp_val := count_a + count_b;
            END;
            $$
        """);
    }

    // =========================================================================
    // EXECUTE with complex dynamic SQL
    // =========================================================================

    @Test
    void testExecuteWithDynamicCte() throws SQLException {
        exec("CREATE TABLE exec_cte_tbl (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO exec_cte_tbl (name) VALUES ('Alice'), ('Bob')");
        exec("""
            DO $$
            BEGIN
                EXECUTE 'WITH names AS (SELECT name FROM exec_cte_tbl) SELECT COUNT(*) FROM names';
            END;
            $$
        """);
    }

    @Test
    void testExecuteFormatWithDollarQuotedBody() throws SQLException {
        exec("""
            DO $$
            DECLARE
                tbl_name text := 'dynamic_exec_tbl';
            BEGIN
                EXECUTE FORMAT(
                    'CREATE TABLE %I (id serial PRIMARY KEY, data text)',
                    tbl_name
                );
            END;
            $$
        """);
        exec("INSERT INTO dynamic_exec_tbl (data) VALUES ('ok')");
        assertEquals("ok", query1("SELECT data FROM dynamic_exec_tbl"));
    }

    // =========================================================================
    // EXECUTE FORMAT with %1$I positional args (PG format specifier)
    // =========================================================================

    @Test
    void testExecuteFormatWithPositionalArgs() throws SQLException {
        exec("CREATE TABLE fmt_pos_tbl (id serial PRIMARY KEY, val text)");
        exec("""
            DO $$
            DECLARE
                tbl text := 'fmt_pos_tbl';
            BEGIN
                EXECUTE FORMAT('INSERT INTO %1$I (val) VALUES (%2$L)', tbl, 'test_value');
            END;
            $$
        """);
        assertEquals("test_value", query1("SELECT val FROM fmt_pos_tbl"));
    }

    @Test
    void testExecuteFormatWithEnableRls() throws SQLException {
        // Pattern: dynamic RLS setup across multiple tables
        exec("CREATE TABLE rls_dynamic_a (id serial PRIMARY KEY, owner_name text)");
        exec("CREATE TABLE rls_dynamic_b (id serial PRIMARY KEY, owner_name text)");
        exec("""
            DO $$
            DECLARE
                tbl text;
                tables text[] := array['rls_dynamic_a', 'rls_dynamic_b'];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    EXECUTE FORMAT('ALTER TABLE %1$I ENABLE ROW LEVEL SECURITY', tbl);
                    EXECUTE FORMAT(
                        'CREATE POLICY owner_policy ON %1$I FOR ALL USING (owner_name = current_user)',
                        tbl
                    );
                END LOOP;
            END;
            $$
        """);
    }

    // =========================================================================
    // IF with current_setting() and server_version_num
    // =========================================================================

    @Test
    void testIfCurrentSettingServerVersion() throws SQLException {
        exec("""
            DO $$
            BEGIN
                IF (SELECT current_setting('server_version_num')::INT >= 150000) THEN
                    RAISE NOTICE 'Server version is 15+';
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testIfCurrentSettingCustomVar() throws SQLException {
        exec("""
            DO $$
            BEGIN
                IF current_setting('app.mode', true) IS NULL THEN
                    RAISE NOTICE 'app.mode is not set';
                END IF;
            END;
            $$
        """);
    }

    // =========================================================================
    // GRANT / REVOKE inside DO block
    // =========================================================================

    @Test
    void testGrantInsideDoBlock() throws SQLException {
        exec("CREATE TABLE grant_tbl (id serial PRIMARY KEY)");
        exec("""
            DO $$
            BEGIN
                EXECUTE 'GRANT SELECT ON grant_tbl TO PUBLIC';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Grant failed: %', SQLERRM;
            END;
            $$
        """);
    }

    // =========================================================================
    // ALTER ROLE ... SET inside DO block
    // =========================================================================

    @Test
    void testAlterRoleSetInsideDoBlock() throws SQLException {
        exec("""
            DO $$
            BEGIN
                ALTER ROLE CURRENT_USER SET statement_timeout = '30s';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Skipped: %', SQLERRM;
            END;
            $$
        """);
    }

    // =========================================================================
    // Nested blocks
    // =========================================================================

    @Test
    void testNestedBlocks() throws SQLException {
        exec("""
            DO $$
            <<outer_block>>
            DECLARE
                x int := 0;
            BEGIN
                <<inner_block>>
                DECLARE
                    y int := 0;
                BEGIN
                    y := 10;
                    x := x + y;
                END inner_block;
                RAISE NOTICE 'Result: %', x;
            END outer_block;
            $$
        """);
    }

    // =========================================================================
    // RETURN inside IF in DO block
    // =========================================================================

    @Test
    void testEarlyReturnInDoBlock() throws SQLException {
        exec("CREATE TABLE early_ret (id serial PRIMARY KEY, done boolean DEFAULT false)");
        exec("INSERT INTO early_ret (done) VALUES (true)");
        exec("""
            DO $$
            DECLARE
                is_done boolean;
            BEGIN
                SELECT done INTO is_done FROM early_ret WHERE id = 1;
                IF is_done THEN
                    RETURN;
                END IF;
                -- This should not execute
                UPDATE early_ret SET done = false WHERE id = 1;
            END;
            $$
        """);
        // done should still be true since we returned early
        assertEquals("true", query1("SELECT done::text FROM early_ret WHERE id = 1"));
    }

    // =========================================================================
    // string_to_array / array_to_string in DO block
    // =========================================================================

    @Test
    void testStringToArrayInDoBlock() throws SQLException {
        exec("CREATE TABLE ver_test (id serial PRIMARY KEY, version text)");
        exec("INSERT INTO ver_test (version) VALUES ('5.1.2')");
        exec("""
            DO $$
            DECLARE
                ver text;
                parts text[];
                major int;
            BEGIN
                SELECT version INTO ver FROM ver_test WHERE id = 1;
                parts := string_to_array(ver, '.');
                major := parts[1]::int;
                IF major >= 5 THEN
                    RAISE NOTICE 'Version % is supported', ver;
                END IF;
            END;
            $$
        """);
    }
}
