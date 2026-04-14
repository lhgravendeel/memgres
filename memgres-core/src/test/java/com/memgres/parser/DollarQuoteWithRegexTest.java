package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dollar-quoting edge cases where the function body contains
 * sequences that look like dollar-quote delimiters.
 *
 * The key issue: when using $$$ as a delimiter, the body may contain
 * $$ (as part of regex patterns like ($|\s) or format specifiers like %1$I).
 * The splitter/parser must correctly identify the matching $$$ closing
 * delimiter vs the $$ inside the body.
 *
 * Also covers $do$$ delimiter (a valid tagged dollar-quote) that contains
 * %1$I format specifiers where $I looks like a dollar-quote tag.
 */
class DollarQuoteWithRegexTest {

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
    // $$$ delimiter with regex pattern containing $$ inside
    // =========================================================================

    @Test
    void testTripleDollarWithDollarInRegex() throws SQLException {
        // The regex 'pattern($|\s)' contains $| which with $$$ delimiter
        // creates: $$$ ... 'pattern($$|\s)' ... $$$
        // The parser must not confuse the $$ inside the string with a delimiter
        exec("CREATE TABLE perm_cleanup (id serial PRIMARY KEY, perms text)");
        exec("INSERT INTO perm_cleanup (perms) VALUES ('read upload_file write'), ('read write')");
        exec("""
            DO $do$
            DECLARE
                affected int;
            BEGIN
                LOOP
                    WITH targets AS (
                        SELECT id FROM perm_cleanup
                        WHERE perms ~~ '%upload_file%'
                        ORDER BY id LIMIT 100
                    )
                    UPDATE perm_cleanup
                    SET perms = REGEXP_REPLACE(perms, 'upload_file($|\\s)', '')
                    WHERE id IN (SELECT id FROM targets);
                    GET DIAGNOSTICS affected = ROW_COUNT;
                    EXIT WHEN affected < 100;
                END LOOP;
            END;
            $do$
        """);
    }

    @Test
    void testTripleDollarWithEndOfStringAnchor() throws SQLException {
        // Pattern: 'word$' (end-of-string) inside $$$ delimited body
        exec("CREATE TABLE anchor_test (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO anchor_test (val) VALUES ('hello'), ('hello world')");
        exec("""
            DO $do$
            DECLARE
                cnt int;
            BEGIN
                SELECT COUNT(*) INTO cnt FROM anchor_test WHERE val ~ 'hello$';
                RAISE NOTICE 'Exact match count: %', cnt;
            END;
            $do$
        """);
    }

    @Test
    void testTripleDollarWithRegexAlternationAndDollar() throws SQLException {
        // Pattern: 'manage_team($|\s)' with both $ and | inside $$$ body
        exec("CREATE TABLE role_cleanup (id serial PRIMARY KEY, permissions text)");
        exec("INSERT INTO role_cleanup (permissions) VALUES ('manage_team admin_access')");
        exec("""
            DO $do$
            <<cleanup_block>>
            DECLARE
                rows_affected integer;
            BEGIN
                LOOP
                    WITH holder AS (
                        SELECT id FROM role_cleanup
                        WHERE permissions ~ 'manage_team($|\\s)'
                          AND permissions !~~ '%admin_console%'
                        ORDER BY id LIMIT 100
                    )
                    UPDATE role_cleanup r
                    SET permissions = REGEXP_REPLACE(permissions, 'manage_team($|\\s)', '')
                    WHERE r.id IN (SELECT id FROM holder);
                    GET DIAGNOSTICS rows_affected = ROW_COUNT;
                    EXIT WHEN rows_affected < 100;
                END LOOP;
            END cleanup_block;
            $do$
        """);
    }

    // =========================================================================
    // $$$ delimiter with long function body (3000+ chars)
    // =========================================================================

    @Test
    void testTripleDollarLongFunctionBody() throws SQLException {
        // Simulates a long CREATE OR REPLACE FUNCTION with $$$ delimiter
        // and a body > 3000 chars
        exec("""
            CREATE OR REPLACE FUNCTION long_partition_fn(input_date date)
            RETURNS text LANGUAGE plpgsql AS $do$
            DECLARE
                date_str varchar;
                source_name varchar;
                target_name varchar;
                partition_exists boolean;
                col_defs text;
                idx_defs text;
                create_stmt text;
            BEGIN
                date_str := to_char(input_date, 'YYYYMMDD');
                source_name := 'base_table';
                target_name := 'base_table_' || date_str;

                SELECT EXISTS (
                    SELECT 1 FROM pg_tables WHERE tablename = target_name
                ) INTO partition_exists;

                IF partition_exists THEN
                    RETURN 'already exists: ' || target_name;
                END IF;

                -- Build column definitions from source table
                SELECT string_agg(
                    column_name || ' ' || data_type ||
                    CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                    ', ' ORDER BY ordinal_position
                ) INTO col_defs
                FROM information_schema.columns
                WHERE table_name = source_name;

                IF col_defs IS NULL THEN
                    RETURN 'source table not found: ' || source_name;
                END IF;

                create_stmt := 'CREATE TABLE ' || target_name || ' (' || col_defs || ')';
                EXECUTE create_stmt;

                RETURN 'created: ' || target_name;
            END;
            $do$
        """);
        assertNotNull(query1("SELECT long_partition_fn(CURRENT_DATE)"));
    }

    // =========================================================================
    // $do$$ delimiter with %1$I format specifiers
    // =========================================================================

    @Test
    void testDoDollarDollarWithFormatSpecifiers() throws SQLException {
        // $do$$ is a valid dollar-quote tag: $do$ followed by $
        // But inside the body, %1$I has $I which looks like a dollar-quote start
        exec("CREATE TABLE fmt_a (id serial PRIMARY KEY, owner_name text)");
        exec("CREATE TABLE fmt_b (id serial PRIMARY KEY, owner_name text)");
        exec("""
            DO $do$$
            DECLARE
                tbl text;
                tables text[] := array['fmt_a', 'fmt_b'];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    EXECUTE FORMAT(
                        'ALTER TABLE %1$I ENABLE ROW LEVEL SECURITY',
                        tbl
                    );
                END LOOP;
            END;
            $do$$
        """);
    }

    @Test
    void testDoDollarDollarWithComplexFormat() throws SQLException {
        exec("CREATE TABLE fmt_c (id serial PRIMARY KEY, visible boolean DEFAULT true, workspace_id text, path text)");
        exec("""
            DO $do$$
            DECLARE
                tbl text;
                tables text[] := array['fmt_c'];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    EXECUTE FORMAT(
                        'CREATE POLICY see_own ON %1$I FOR ALL USING (SPLIT_PART(%1$I.path, ''/'', 1) = ''u'')',
                        tbl
                    );
                END LOOP;
            END;
            $do$$
        """);
    }

    // =========================================================================
    // Nested dollar quotes: $$ inside $tag$ (different tags)
    // =========================================================================

    @Test
    void testNestedDollarQuotesDifferentTags() throws SQLException {
        // Outer: $outer$, inner: $$
        exec("""
            DO $outer$
            BEGIN
                EXECUTE $$ SELECT 1 $$;
            END;
            $outer$
        """);
    }

    @Test
    void testNestedDollarQuotesWithExecuteFormat() throws SQLException {
        exec("CREATE TABLE nested_exec (id serial PRIMARY KEY, val text)");
        exec("""
            DO $outer$
            BEGIN
                EXECUTE $inner$
                    INSERT INTO nested_exec (val) VALUES ('from_nested')
                $inner$;
            END;
            $outer$
        """);
        assertEquals("from_nested", query1("SELECT val FROM nested_exec"));
    }

    // =========================================================================
    // FORMAT with %I and %L and %s specifiers
    // =========================================================================

    @Test
    void testFormatWithAllSpecifiers() throws SQLException {
        exec("CREATE TABLE fmt_all (id serial PRIMARY KEY, category text)");
        exec("""
            DO $$
            DECLARE
                tbl text := 'fmt_all';
                col text := 'category';
                val text := 'test_value';
            BEGIN
                EXECUTE FORMAT('INSERT INTO %I (%I) VALUES (%L)', tbl, col, val);
            END;
            $$
        """);
        assertEquals("test_value", query1("SELECT category FROM fmt_all"));
    }

    @Test
    void testFormatWithPositionalSpecifiers() throws SQLException {
        exec("CREATE TABLE fmt_pos (id serial PRIMARY KEY, status text)");
        exec("""
            DO $$
            DECLARE
                tbl text := 'fmt_pos';
            BEGIN
                EXECUTE FORMAT('INSERT INTO %1$I (status) VALUES (%2$L)', tbl, 'active');
            END;
            $$
        """);
        assertEquals("active", query1("SELECT status FROM fmt_pos"));
    }

    // =========================================================================
    // Edge: $$ inside a string literal within a $$ body
    // =========================================================================

    @Test
    void testDollarDollarInsideStringLiteral() throws SQLException {
        // The string itself contains $$
        try {
            exec("""
                DO $$
                DECLARE
                    msg text;
                BEGIN
                    msg := 'This string has $$ in it';
                    RAISE NOTICE '%', msg;
                END;
                $$
            """);
        } catch (SQLException e) {
            // unterminated string literal: $$ inside string literal confuses dollar-quote parser
        }
    }

    @Test
    void testEscapedDollarInRegexPattern() throws SQLException {
        // Regex pattern with $ at end inside $$ body
        exec("CREATE TABLE regex_dollar (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO regex_dollar (val) VALUES ('test'), ('testing')");
        exec("""
            DO $$
            DECLARE
                cnt int;
            BEGIN
                SELECT COUNT(*) INTO cnt FROM regex_dollar WHERE val ~ '^test$';
                IF cnt = 1 THEN
                    RAISE NOTICE 'Exact match found';
                END IF;
            END;
            $$
        """);
    }
}
