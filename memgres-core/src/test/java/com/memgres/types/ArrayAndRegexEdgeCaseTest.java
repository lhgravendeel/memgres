package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for array comparison edge cases and regex patterns in PL/pgSQL
 * that still cause failures.
 *
 * Covers:
 * - Array comparison with mismatched element counts
 * - string_to_array comparison (text[] < text[])
 * - Regex patterns with alternation pipe (|) in PL/pgSQL string literals
 * - Regex patterns with $ anchor + alternation inside string
 * - CREATE DOMAIN with schema-qualified base types (citext)
 */
class ArrayAndRegexEdgeCaseTest {

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
    // Array comparison: text arrays
    // =========================================================================

    @Test
    void testTextArrayLessThan() throws SQLException {
        // Comparing arrays of same size
        String result = query1("SELECT ARRAY['1','0','0'] < ARRAY['2','0','0']");
        assertTrue("t".equals(result) || "true".equals(result));
    }

    @Test
    void testTextArrayLessThanMismatchedSize() throws SQLException {
        // Comparing arrays with different element counts; this is valid in PG
        // PG uses lexicographic comparison: shorter array is "less" if all elements match
        assertNotNull(query1("""
            SELECT ARRAY['5'] < ARRAY['5','0','0']
        """));
    }

    @Test
    void testStringToArrayComparison() throws SQLException {
        // The exact pattern from migrations: comparing version strings as arrays
        assertNotNull(query1("""
            SELECT string_to_array('5.12.0', '.') < string_to_array('5.0.0', '.')
        """));
    }

    @Test
    void testStringToArrayComparisonDifferentLengths() throws SQLException {
        // Different length version strings
        assertNotNull(query1("""
            SELECT string_to_array('5', '.') < string_to_array('5.0.0', '.')
        """));
    }

    @Test
    void testStringToArrayInDoBlockCondition() throws SQLException {
        // The actual migration pattern
        exec("CREATE TABLE version_store (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO version_store (val) VALUES ('5.12.0')");
        exec("""
            DO $$
            DECLARE
                current_ver VARCHAR(100) := '';
            BEGIN
                SELECT val INTO current_ver FROM version_store WHERE id = 1;
                IF (string_to_array(current_ver, '.') < string_to_array('6.0.0', '.')) THEN
                    RAISE NOTICE 'Version % is below 6.0.0', current_ver;
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testArrayComparisonGreaterThan() throws SQLException {
        String result = query1("SELECT ARRAY[2,0,0] > ARRAY[1,9,9]");
        assertTrue("t".equals(result) || "true".equals(result));
    }

    @Test
    void testArrayComparisonEquality() throws SQLException {
        String result = query1("SELECT ARRAY['5','0','0'] = ARRAY['5','0','0']");
        assertTrue("t".equals(result) || "true".equals(result));
    }

    @Test
    void testArrayComparisonWithNulls() throws SQLException {
        // Array comparison with NULL elements
        // Array comparison with NULL elements
        assertNotNull(query1("""
            SELECT ARRAY[1, NULL, 3] < ARRAY[1, 2, 3]
        """));
    }

    // =========================================================================
    // Regex with alternation pipe in PL/pgSQL strings
    // =========================================================================

    @Test
    void testRegexWithPipeAlternation() throws SQLException {
        // Basic regex with | (alternation)
        exec("CREATE TABLE perm_data (id serial PRIMARY KEY, perms text)");
        exec("INSERT INTO perm_data (perms) VALUES ('read write upload_file delete')");
        assertNotNull(query1("""
            SELECT perms FROM perm_data WHERE perms ~ 'upload_file|download_file'
        """));
    }

    @Test
    void testRegexWithDollarAndPipe() throws SQLException {
        // The exact failing pattern: regex with ($|\s), meaning end-of-string OR whitespace
        exec("CREATE TABLE regex_pipe (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO regex_pipe (val) VALUES ('manage_team extra')");
        exec("INSERT INTO regex_pipe (val) VALUES ('manage_team')");
        assertEquals("2", query1("""
            SELECT COUNT(*) FROM regex_pipe WHERE val ~ 'manage_team($|\\s)'
        """));
    }

    @Test
    void testRegexWithDollarPipeInDoBlock() throws SQLException {
        // The actual migration pattern: DO block with regex containing ($|\s)
        exec("CREATE TABLE do_regex (id serial PRIMARY KEY, permissions text)");
        exec("INSERT INTO do_regex (permissions) VALUES ('read manage_team write')");
        exec("""
            DO $$
            DECLARE
                rows_updated integer;
            BEGIN
                LOOP
                    UPDATE do_regex
                    SET permissions = REGEXP_REPLACE(permissions, 'manage_team($|\\s)', '')
                    WHERE permissions ~ 'manage_team($|\\s)';
                    GET DIAGNOSTICS rows_updated = ROW_COUNT;
                    EXIT WHEN rows_updated = 0;
                END LOOP;
            END;
            $$
        """);
    }

    @Test
    void testRegexWithPipeInLikeAndRegex() throws SQLException {
        // Combined ~~ (LIKE) and ~ (regex) in same query
        exec("CREATE TABLE mixed_regex (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO mixed_regex (val) VALUES ('abc_upload_file_def')");
        assertNotNull(query1("""
            SELECT val FROM mixed_regex
            WHERE val ~~ '%upload_file%' AND val !~ 'create_post($|\\s)'
        """));
    }

    @Test
    void testDoBlockWithLabelAndRegexPipe() throws SQLException {
        // Labeled DO block with CTE + regex alternation: the exact combined pattern
        exec("CREATE TABLE label_regex_pipe (id serial PRIMARY KEY, perms text)");
        exec("INSERT INTO label_regex_pipe (perms) VALUES ('read upload_file write'), ('read write')");
        exec("""
            DO $$
            <<cleanup_perms>>
            DECLARE
                rows_updated integer;
            BEGIN
                LOOP
                    WITH targets AS (
                        SELECT id FROM label_regex_pipe
                        WHERE perms ~~ '%upload_file%'
                            AND perms !~ 'create_post($|\\s)'
                        ORDER BY id LIMIT 100
                    )
                    UPDATE label_regex_pipe r
                    SET perms = REGEXP_REPLACE(perms, 'upload_file($|\\s)', '')
                    WHERE r.id IN (SELECT id FROM targets);
                    GET DIAGNOSTICS rows_updated = ROW_COUNT;
                    EXIT WHEN rows_updated < 100;
                END LOOP;
            END cleanup_perms;
            $$
        """);
    }

    // =========================================================================
    // CREATE DOMAIN with schema-qualified base types
    // =========================================================================

    @Test
    void testCreateDomainWithSchemaQualifiedBaseType() throws SQLException {
        // Pattern: CREATE DOMAIN schema.name AS public.type CHECK(...)
        exec("CREATE SCHEMA IF NOT EXISTS app_auth");
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        exec("""
            CREATE DOMAIN app_auth.email_addr AS public.citext
            CHECK (value ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
        """);
        exec("CREATE TABLE app_auth.accounts (id serial PRIMARY KEY, email app_auth.email_addr NOT NULL UNIQUE)");
        exec("INSERT INTO app_auth.accounts (email) VALUES ('test@example.com')");
        assertEquals("test@example.com", query1("SELECT email FROM app_auth.accounts"));
    }

    @Test
    void testCreateDomainWithCitextBaseType() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        exec("CREATE DOMAIN ci_email AS citext CHECK (value ~ '@')");
        exec("CREATE TABLE ci_email_test (id serial PRIMARY KEY, email ci_email)");
        exec("INSERT INTO ci_email_test (email) VALUES ('Test@Example.COM')");
        // citext is case-insensitive
        assertNotNull(query1("SELECT email FROM ci_email_test WHERE email = 'test@example.com'"));
    }

    @Test
    void testSchemaQualifiedDomainAsColumnType() throws SQLException {
        // Use a domain defined in one schema as a column type in another
        exec("CREATE SCHEMA IF NOT EXISTS domain_defs");
        exec("CREATE DOMAIN domain_defs.short_text AS varchar(100) CHECK (length(value) > 0)");
        exec("CREATE TABLE domain_user (id serial PRIMARY KEY, name domain_defs.short_text)");
        exec("INSERT INTO domain_user (name) VALUES ('valid')");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO domain_user (name) VALUES ('')"));
    }

    // =========================================================================
    // REGEXP_REPLACE with complex patterns
    // =========================================================================

    @Test
    void testRegexpReplaceWithAlternation() throws SQLException {
        assertEquals("read write", query1("""
            SELECT REGEXP_REPLACE('read upload_file write', 'upload_file($|\\s)', '')
        """));
    }

    @Test
    void testRegexpReplaceWithGroupCapture() throws SQLException {
        assertEquals("hello_world", query1("""
            SELECT REGEXP_REPLACE('hello world', '(\\w+) (\\w+)', '\\1_\\2')
        """));
    }

    @Test
    void testRegexpReplaceGlobal() throws SQLException {
        assertEquals("x-x-x", query1("""
            SELECT REGEXP_REPLACE('a-b-c', '[a-c]', 'x', 'g')
        """));
    }
}
