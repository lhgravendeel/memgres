package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dollar-quoting edge cases that still fail.
 *
 * Covers:
 * - $$$ (triple dollar) as a function body delimiter
 * - $do$$ as a DO block delimiter (not the same as $do$ + $)
 * - Regex patterns containing $$ inside PL/pgSQL strings
 * - Nested dollar quotes with regex metacharacters
 * - LIKE patterns with % and $$ inside PL/pgSQL
 * - DO blocks with regex operators (~, ~~, ~*) in string comparisons
 */
class DollarQuoteEdgeCaseTest {

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
    // $$$ as function body delimiter
    // =========================================================================

    @Test
    void testTripleDollarFunction() throws SQLException {
        exec("""
            CREATE FUNCTION triple_fn() RETURNS text LANGUAGE plpgsql AS $$$
            DECLARE
                result text;
            BEGIN
                result := 'triple dollar works';
                RETURN result;
            END;
            $$$
        """);
        assertEquals("triple dollar works", query1("SELECT triple_fn()"));
    }

    @Test
    void testTripleDollarFunctionWithDeclare() throws SQLException {
        exec("""
            CREATE FUNCTION triple_calc(a int, b int) RETURNS int LANGUAGE plpgsql AS $$$
            DECLARE
                total int;
            BEGIN
                total := a + b;
                RETURN total;
            END;
            $$$
        """);
        assertEquals("7", query1("SELECT triple_calc(3, 4)"));
    }

    @Test
    void testTripleDollarDoBlock() throws SQLException {
        exec("CREATE TABLE triple_do_test (id serial PRIMARY KEY, val text)");
        exec("""
            DO $$$
            BEGIN
                INSERT INTO triple_do_test (val) VALUES ('from_triple');
            END;
            $$$
        """);
        assertEquals("from_triple", query1("SELECT val FROM triple_do_test"));
    }

    @Test
    void testTripleDollarLongFunction() throws SQLException {
        // Simulate a long function body with multiple DECLARE/BEGIN blocks
        exec("""
            CREATE FUNCTION triple_long(input_date date) RETURNS text LANGUAGE plpgsql AS $$$
            DECLARE
                date_str varchar;
                result_name varchar;
                exists_already boolean;
            BEGIN
                date_str := to_char(input_date, 'YYYYMMDD');
                result_name := 'partition_' || date_str;

                SELECT EXISTS (
                    SELECT 1 FROM pg_tables WHERE tablename = result_name
                ) INTO exists_already;

                IF exists_already THEN
                    RETURN 'already exists: ' || result_name;
                END IF;

                RETURN 'would create: ' || result_name;
            END;
            $$$
        """);
        assertNotNull(query1("SELECT triple_long(CURRENT_DATE)"));
    }

    // =========================================================================
    // $do$$ as a delimiter (looks like $do$ + bare $)
    // =========================================================================

    @Test
    void testDoDollarDollarDelimiter() throws SQLException {
        exec("""
            DO $do$$
            BEGIN
                RAISE NOTICE 'do dollar dollar';
            END;
            $do$$
        """);
    }

    @Test
    void testDoDollarDollarWithLogic() throws SQLException {
        exec("CREATE TABLE ddd_test (id serial PRIMARY KEY, val text)");
        exec("""
            DO $do$$
            DECLARE
                i text;
                arr text[] := array['alpha', 'beta', 'gamma'];
            BEGIN
                FOREACH i IN ARRAY arr LOOP
                    INSERT INTO ddd_test (val) VALUES (i);
                END LOOP;
            END;
            $do$$
        """);
        assertEquals("3", query1("SELECT COUNT(*) FROM ddd_test"));
    }

    // =========================================================================
    // PL/pgSQL with regex operators in string comparisons
    // =========================================================================

    @Test
    void testDoBlockWithRegexLikeOperator() throws SQLException {
        // Pattern: LIKE with %pattern% inside DO block
        exec("CREATE TABLE regex_perms (id serial PRIMARY KEY, permissions text)");
        exec("INSERT INTO regex_perms (permissions) VALUES ('read write upload_file delete')");
        exec("""
            DO $$
            DECLARE
                perm_count int;
            BEGIN
                SELECT COUNT(*) INTO perm_count
                FROM regex_perms
                WHERE permissions ~~ '%upload_file%';

                IF perm_count > 0 THEN
                    UPDATE regex_perms
                    SET permissions = replace(permissions, 'upload_file ', '')
                    WHERE permissions ~~ '%upload_file%';
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testDoBlockWithRegexMatchOperator() throws SQLException {
        // Pattern: regex match with ~ operator inside DO block
        exec("CREATE TABLE regex_match_test (id serial PRIMARY KEY, data text)");
        exec("INSERT INTO regex_match_test (data) VALUES ('abc_123'), ('def_456')");
        exec("""
            DO $$
            DECLARE
                match_count int;
            BEGIN
                SELECT COUNT(*) INTO match_count
                FROM regex_match_test
                WHERE data ~ 'abc';

                RAISE NOTICE 'Found % matches', match_count;
            END;
            $$
        """);
    }

    @Test
    void testDoBlockWithRegexNotMatchOperator() throws SQLException {
        exec("CREATE TABLE regex_not_test (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO regex_not_test (val) VALUES ('keep_this'), ('remove_that')");
        exec("""
            DO $$
            BEGIN
                DELETE FROM regex_not_test WHERE val !~ 'keep';
            END;
            $$
        """);
        assertEquals("1", query1("SELECT COUNT(*) FROM regex_not_test"));
    }

    // =========================================================================
    // DO block with LIKE pattern containing $$ sequence
    // =========================================================================

    @Test
    void testDoBlockWithRegexAndDollarInPattern() throws SQLException {
        // Real-world pattern: regex with $$ or $ at end of pattern
        exec("CREATE TABLE dollar_regex (id serial PRIMARY KEY, code text)");
        exec("INSERT INTO dollar_regex (code) VALUES ('manage_team'), ('manage_team_extra')");
        exec("""
            DO $$
            DECLARE
                cnt int;
            BEGIN
                SELECT COUNT(*) INTO cnt
                FROM dollar_regex
                WHERE code ~ '^manage_team$';

                RAISE NOTICE 'Exact matches: %', cnt;
            END;
            $$
        """);
    }

    // =========================================================================
    // DO block with EXECUTE FORMAT containing nested dollar quotes
    // =========================================================================

    @Test
    void testDoBlockNestedDollarQuotesInFormat() throws SQLException {
        exec("CREATE TABLE nested_dq_a (id serial PRIMARY KEY, owner_name text)");
        exec("CREATE TABLE nested_dq_b (id serial PRIMARY KEY, owner_name text)");
        exec("""
            DO $do$
            DECLARE
                tbl text;
                tables text[] := array['nested_dq_a', 'nested_dq_b'];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    EXECUTE FORMAT(
                        $fmt$ALTER TABLE %1$I ENABLE ROW LEVEL SECURITY$fmt$,
                        tbl
                    );
                    EXECUTE FORMAT(
                        $fmt$CREATE POLICY owner_only ON %1$I FOR ALL USING (owner_name = current_user)$fmt$,
                        tbl
                    );
                END LOOP;
            END;
            $do$
        """);
    }

    @Test
    void testDoBlockTripleDollarWithNestedDollarFormat() throws SQLException {
        exec("CREATE TABLE triple_nested (id serial PRIMARY KEY, val text)");
        exec("""
            DO $$$
            DECLARE
                tbl text := 'triple_nested';
            BEGIN
                EXECUTE FORMAT('INSERT INTO %I (val) VALUES (%L)', tbl, 'nested_value');
            END;
            $$$
        """);
        assertEquals("nested_value", query1("SELECT val FROM triple_nested"));
    }

    // =========================================================================
    // DO block with labeled block + regex inside (combined edge case)
    // =========================================================================

    @Test
    void testLabeledBlockWithRegexLoopUpdate() throws SQLException {
        exec("CREATE TABLE label_regex (id serial PRIMARY KEY, perms text)");
        exec("INSERT INTO label_regex (perms) VALUES ('read write admin_access delete')");
        exec("""
            DO $$
            <<cleanup>>
            DECLARE
                rows_updated integer;
            BEGIN
                LOOP
                    UPDATE label_regex
                    SET perms = regexp_replace(perms, 'admin_access\\s*', '')
                    WHERE perms ~~ '%admin_access%';

                    GET DIAGNOSTICS rows_updated = ROW_COUNT;
                    EXIT WHEN rows_updated = 0;
                END LOOP;
            END cleanup;
            $$
        """);
    }
}
