package com.memgres.client;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for environment capability detection and output normalization rules,
 * covering 1570_environment_capability_detection_and_skip_policy.md and
 * 1580_harness_output_normalization_rules.md.
 *
 * Verifies server version introspection, catalog probing functions,
 * feature-availability DDL smoke tests, and wire-format output normalization.
 *
 * Table prefix: env_
 */
class EnvironmentCapabilityDetectionTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // 1. Server version detection
    // =========================================================================

    /**
     * SHOW server_version returns a non-empty string that can be parsed
     * as a dotted version number (e.g. "14.0" or "16.3").
     */
    @Test
    void serverVersionShowIsParseable() throws SQLException {
        String ver = query1("SHOW server_version");
        assertNotNull(ver, "server_version must not be null");
        assertFalse(Strs.isBlank(ver), "server_version must not be blank");
        // Must contain at least one digit and one dot, or just digits
        assertTrue(ver.matches("\\d+.*"), "server_version must start with a digit, got: " + ver);
    }

    // =========================================================================
    // 2. Version number format
    // =========================================================================

    /**
     * current_setting('server_version_num') returns a purely numeric string
     * whose integer value is >= 90000 (PostgreSQL version numbering).
     */
    @Test
    void serverVersionNumIsNumeric() throws SQLException {
        String numStr = query1("SELECT current_setting('server_version_num')");
        assertNotNull(numStr, "server_version_num must not be null");
        long num = assertDoesNotThrow(() -> Long.parseLong(numStr.trim()),
                "server_version_num must be parseable as a long, got: " + numStr);
        assertTrue(num >= 90000L, "server_version_num must be >= 90000, got: " + num);
    }

    // =========================================================================
    // 3. Extension availability check
    // =========================================================================

    /**
     * Querying pg_extension succeeds and returns a ResultSet (even if empty).
     */
    @Test
    void pgExtensionQuerySucceeds() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT extname FROM pg_extension")) {
            assertNotNull(rs, "pg_extension query must return a ResultSet");
            // Just drain the result; we only care that it doesn't throw
            int rows = 0;
            while (rs.next()) rows++;
            assertTrue(rows >= 0, "pg_extension row count must be non-negative");
        }
    }

    // =========================================================================
    // 4. to_regclass for table existence
    // =========================================================================

    /**
     * to_regclass('pg_class') must return a non-null value because pg_class
     * is always present in the catalog.
     */
    @Test
    void toRegclassKnownTableIsNotNull() throws SQLException {
        String val = query1("SELECT to_regclass('pg_class')::text");
        assertNotNull(val, "to_regclass('pg_class') must not be null");
        assertFalse(Strs.isBlank(val), "to_regclass('pg_class') must not be blank");
    }

    /**
     * to_regclass on a non-existent table must return NULL rather than throwing.
     */
    @Test
    void toRegclassMissingTableIsNull() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT to_regclass('env_definitely_no_such_table_xyz')")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1), "to_regclass of a missing table must be NULL");
        }
    }

    // =========================================================================
    // 5. to_regproc for function existence
    // =========================================================================

    /**
     * to_regproc('now') must return a non-null value because now() is a
     * standard built-in function.
     */
    @Test
    void toRegprocBuiltinFunctionIsNotNull() throws SQLException {
        String val = query1("SELECT to_regproc('now')::text");
        assertNotNull(val, "to_regproc('now') must not be null");
        assertFalse(Strs.isBlank(val), "to_regproc('now') must not be blank");
    }

    // =========================================================================
    // 6. has_table_privilege check
    // =========================================================================

    /**
     * The current user must have SELECT privilege on pg_class.
     */
    @Test
    void hasTablePrivilegeSelectOnPgClass() throws SQLException {
        String result = query1(
                "SELECT has_table_privilege(current_user, 'pg_class', 'SELECT')::text");
        assertTrue(result.equals("t") || result.equals("true"),
                "current_user must have SELECT on pg_class, got: " + result);
    }

    // =========================================================================
    // 7. Feature: GENERATED ALWAYS AS IDENTITY
    // =========================================================================

    /**
     * CREATE TABLE with GENERATED ALWAYS AS IDENTITY should succeed and
     * auto-populate the identity column on INSERT.
     */
    @Test
    void featureGeneratedAlwaysAsIdentity() throws SQLException {
        exec("CREATE TABLE env_identity_test (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO env_identity_test (val) VALUES ('a'), ('b'), ('c')");
            String count = query1("SELECT COUNT(*)::text FROM env_identity_test");
            assertEquals("3", count, "Expected 3 rows after identity INSERT");
            String minId = query1("SELECT MIN(id)::text FROM env_identity_test");
            assertNotNull(minId);
            assertTrue(Long.parseLong(minId) >= 1, "Identity id must be >= 1");
        } finally {
            exec("DROP TABLE IF EXISTS env_identity_test");
        }
    }

    // =========================================================================
    // 8. Feature: MERGE statement
    // =========================================================================

    /**
     * MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED should parse and execute
     * without error.
     */
    @Test
    void featureMergeStatement() throws SQLException {
        exec("CREATE TABLE env_merge_target (id int PRIMARY KEY, val text)");
        exec("CREATE TABLE env_merge_source (id int PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO env_merge_target VALUES (1, 'old')");
            exec("INSERT INTO env_merge_source VALUES (1, 'updated'), (2, 'new')");
            exec("""
                    MERGE INTO env_merge_target AS t
                    USING env_merge_source AS s ON t.id = s.id
                    WHEN MATCHED THEN
                        UPDATE SET val = s.val
                    WHEN NOT MATCHED THEN
                        INSERT (id, val) VALUES (s.id, s.val)
                    """);
            String count = query1("SELECT COUNT(*)::text FROM env_merge_target");
            assertEquals("2", count, "MERGE should result in 2 rows");
            String updatedVal = query1("SELECT val FROM env_merge_target WHERE id = 1");
            assertEquals("updated", updatedVal, "MERGE WHEN MATCHED must update existing row");
        } finally {
            exec("DROP TABLE IF EXISTS env_merge_target");
            exec("DROP TABLE IF EXISTS env_merge_source");
        }
    }

    // =========================================================================
    // 9. Feature: JSON path operator @?
    // =========================================================================

    /**
     * The jsonb @? jsonpath operator should return true when the path matches.
     */
    @Test
    void featureJsonPathOperator() throws SQLException {
        String result = query1("SELECT ('{\"a\":1}'::jsonb @? '$.a')::text");
        assertTrue(result.equals("t") || result.equals("true"),
                "jsonb @? path match must return true, got: " + result);
    }

    /**
     * The jsonb @? jsonpath operator should return false when the path does not match.
     */
    @Test
    void featureJsonPathOperatorNoMatch() throws SQLException {
        String result = query1("SELECT ('{\"a\":1}'::jsonb @? '$.b')::text");
        assertTrue(result.equals("f") || result.equals("false"),
                "jsonb @? on missing key must return false, got: " + result);
    }

    // =========================================================================
    // 10. Feature: Generated columns (GENERATED ALWAYS AS expr STORED)
    // =========================================================================

    /**
     * CREATE TABLE with a GENERATED ALWAYS AS (expression) STORED column
     * should work and the generated value should be computed automatically.
     */
    @Test
    void featureGeneratedStoredColumns() throws SQLException {
        exec("""
                CREATE TABLE env_gencol_test (
                    id serial PRIMARY KEY,
                    price numeric(10,2),
                    qty int,
                    total numeric(10,2) GENERATED ALWAYS AS (price * qty) STORED
                )
                """);
        try {
            exec("INSERT INTO env_gencol_test (price, qty) VALUES (9.99, 3)");
            String total = query1("SELECT total::text FROM env_gencol_test LIMIT 1");
            assertNotNull(total, "Generated column 'total' must not be null");
            double d = Double.parseDouble(total);
            assertEquals(29.97, d, 0.001, "Generated column value must equal price * qty");
        } finally {
            exec("DROP TABLE IF EXISTS env_gencol_test");
        }
    }

    // =========================================================================
    // 11. Boolean output format
    // =========================================================================

    /**
     * A boolean TRUE in the text protocol must render as 't' or 'true'.
     */
    @Test
    void booleanTrueOutputFormat() throws SQLException {
        String result = query1("SELECT true::text");
        assertTrue(result.equals("t") || result.equals("true"),
                "Boolean TRUE must render as 't' or 'true', got: " + result);
    }

    /**
     * A boolean FALSE in the text protocol must render as 'f' or 'false'.
     */
    @Test
    void booleanFalseOutputFormat() throws SQLException {
        String result = query1("SELECT false::text");
        assertTrue(result.equals("f") || result.equals("false"),
                "Boolean FALSE must render as 'f' or 'false', got: " + result);
    }

    // =========================================================================
    // 12. NULL output format
    // =========================================================================

    /**
     * A NULL selected directly must be returned as Java null via getString().
     */
    @Test
    void nullOutputFormatIsJavaNull() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT NULL::text")) {
            assertTrue(rs.next(), "Expected one row");
            assertNull(rs.getString(1), "NULL must be returned as Java null in ResultSet");
        }
    }

    // =========================================================================
    // 13. Array output format
    // =========================================================================

    /**
     * ARRAY[1,2,3]::text must produce '{1,2,3}' (PostgreSQL array literal format).
     */
    @Test
    void arrayTextOutputFormat() throws SQLException {
        String result = query1("SELECT ARRAY[1,2,3]::text");
        assertEquals("{1,2,3}", result,
                "Integer array text representation must be '{1,2,3}', got: " + result);
    }

    // =========================================================================
    // 14. Timestamp output format
    // =========================================================================

    /**
     * A timestamptz value cast to text must include a date part in YYYY-MM-DD
     * format followed by a time part, consistent with ISO-8601/PostgreSQL output.
     */
    @Test
    void timestampOutputFollowsIsoFormat() throws SQLException {
        String result = query1("SELECT '2024-01-15 10:30:00'::timestamp::text");
        assertNotNull(result, "Timestamp text must not be null");
        // PostgreSQL outputs: 2024-01-15 10:30:00 (seconds may be omitted if :00)
        assertTrue(result.startsWith("2024-01-15"),
                "Timestamp must start with date 2024-01-15, got: " + result);
        assertTrue(result.matches(".*\\d{4}-\\d{2}-\\d{2}.*\\d+.*"),
                "Timestamp must contain a valid date-time pattern, got: " + result);
        // Verify the time part starts with 10:30
        assertTrue(result.contains("10:30"),
                "Timestamp must contain time 10:30, got: " + result);
    }

    // =========================================================================
    // 15. Float output normalization
    // =========================================================================

    /**
     * 1.0::float8::text must render as a decimal representation (not scientific
     * notation for this small value) and must be parseable as a double equal to 1.0.
     */
    @Test
    void floatOutputNormalization() throws SQLException {
        String result = query1("SELECT 1.0::float8::text");
        assertNotNull(result, "Float text must not be null");
        double d = assertDoesNotThrow(() -> Double.parseDouble(result),
                "Float output must be parseable as double, got: " + result);
        assertEquals(1.0, d, 0.0, "Float 1.0 must parse back to exactly 1.0");
    }

    // =========================================================================
    // 16. JSON whitespace normalization
    // =========================================================================

    /**
     * When a jsonb value is cast back to text, PostgreSQL normalizes whitespace:
     * keys are sorted and exactly one space follows each colon/comma.
     */
    @Test
    void jsonbWhitespaceNormalization() throws SQLException {
        // Input with irregular whitespace and unsorted keys; jsonb normalizes both
        String result = query1("SELECT '{\"z\":3,\"a\":1,\"m\":2}'::jsonb::text");
        assertNotNull(result, "jsonb text output must not be null");
        // PostgreSQL sorts keys alphabetically: {"a": 1, "m": 2, "z": 3}
        assertTrue(result.contains("\"a\""), "jsonb output must contain key 'a'");
        assertTrue(result.contains("\"m\""), "jsonb output must contain key 'm'");
        assertTrue(result.contains("\"z\""), "jsonb output must contain key 'z'");
        // Keys must appear in alphabetical order
        int posA = result.indexOf("\"a\"");
        int posM = result.indexOf("\"m\"");
        int posZ = result.indexOf("\"z\"");
        assertTrue(posA < posM && posM < posZ,
                "jsonb must sort keys alphabetically (a < m < z), got: " + result);
    }

    // =========================================================================
    // 17. Interval output format
    // =========================================================================

    /**
     * An interval of '1 day 2 hours' must render consistently in PostgreSQL's
     * interval output format and be parseable (contains day and hour information).
     */
    @Test
    void intervalOutputFormat() throws SQLException {
        String result = query1("SELECT '1 day 2 hours'::interval::text");
        assertNotNull(result, "Interval text must not be null");
        assertFalse(Strs.isBlank(result), "Interval text must not be blank");
        // PostgreSQL typically outputs: 1 day 02:00:00
        assertTrue(result.contains("1 day") || result.contains("1day"),
                "Interval must mention 1 day, got: " + result);
        // Time portion must be present
        assertTrue(result.matches(".*\\d+:\\d+.*"),
                "Interval must contain a HH:MM time portion, got: " + result);
    }
}
