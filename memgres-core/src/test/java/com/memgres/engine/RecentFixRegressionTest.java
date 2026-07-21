package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for recent fix regressions: H38, H39, M1, M24, M28, L1.
 */
class RecentFixRegressionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // H38: jsonb column || jsonb column should use jsonb concat, not text
    // ========================================================================

    @Test
    void h38_jsonbColumnConcatScalars() throws SQLException {
        exec("CREATE TABLE h38_j (a jsonb, b jsonb)");
        exec("INSERT INTO h38_j VALUES ('1', '2')");
        String result = q("SELECT a || b FROM h38_j");
        assertEquals("[1, 2]", result);
    }

    @Test
    void h38_jsonbColumnConcatArrayAndScalar() throws SQLException {
        exec("CREATE TABLE h38_j2 (a jsonb, b jsonb)");
        exec("INSERT INTO h38_j2 VALUES ('[1]', '\"x\"')");
        String result = q("SELECT a || b FROM h38_j2");
        assertEquals("[1, \"x\"]", result);
    }

    @Test
    void h38_jsonbColumnConcatObjects() throws SQLException {
        // Objects already worked, but verify they still do
        exec("CREATE TABLE h38_j3 (a jsonb, b jsonb)");
        exec("INSERT INTO h38_j3 VALUES ('{\"x\":1}', '{\"y\":2}')");
        String result = q("SELECT a || b FROM h38_j3");
        assertEquals("{\"x\": 1, \"y\": 2}", result);
    }

    @Test
    void h38_jsonbCastConcatStillWorks() throws SQLException {
        // Explicit casts should still work
        assertEquals("[1, 2]", q("SELECT '1'::jsonb || '2'::jsonb"));
    }

    // ========================================================================
    // H39: pg_advisory_xact_lock in autocommit releases at statement end
    // ========================================================================

    @Test
    void h39_advisoryXactLockReleasedInAutocommit() throws SQLException {
        // Acquire xact lock in autocommit mode
        exec("SELECT pg_advisory_xact_lock(99999)");
        // Try to acquire it again — should succeed since first was released
        String result = q("SELECT pg_try_advisory_xact_lock(99999)");
        assertEquals("t", result);
    }

    // ========================================================================
    // M1: name-subscript heuristic should not hijack jsonb scalars
    // ========================================================================

    @Test
    void m1_jsonbScalarArrow() throws SQLException {
        // Verified against real PG 18: a jsonb scalar is treated as a one-element array for
        // `-> 0`, so '123'::jsonb -> 0 returns the scalar 123 (IS NULL is false).
        String result = q("SELECT '123'::jsonb -> 0");
        assertEquals("123", result, "'123'::jsonb -> 0 should echo the scalar (PG one-element-array rule)");
    }

    @Test
    void m1_jsonbStringScalarArrow() throws SQLException {
        // PG 18: '"abc"'::jsonb -> 0 returns the jsonb string "abc" (with quotes).
        String result = q("SELECT '\"abc\"'::jsonb -> 0");
        assertEquals("\"abc\"", result, "'\"abc\"'::jsonb -> 0 should echo the scalar string");
    }

    @Test
    void m1_textSubscriptReject() throws SQLException {
        // ('hello'::text) -> 0 should error in PG (no -> operator for text)
        assertThrows(SQLException.class, () -> q("SELECT ('hello'::text) -> 0"));
    }

    // ========================================================================
    // M24: COPY CSV should not quote fields just because they contain ESCAPE char
    // ========================================================================

    @Test
    void m24_copyEscapeNoQuote() throws SQLException {
        exec("CREATE TABLE m24_esc (val text)");
        exec("INSERT INTO m24_esc VALUES ('back\\slash')");
        // Use CopyManager to execute COPY TO STDOUT
        org.postgresql.copy.CopyManager cm = new org.postgresql.copy.CopyManager(
                conn.unwrap(org.postgresql.core.BaseConnection.class));
        java.io.StringWriter sw = new java.io.StringWriter();
        try {
            cm.copyOut("COPY m24_esc TO STDOUT WITH (FORMAT csv, ESCAPE '\\\\')", sw);
        } catch (java.io.IOException e) {
            throw new SQLException(e);
        }
        String result = sw.toString().trim();
        // PG does NOT quote "back\slash" — escape char alone doesn't trigger quoting
        assertFalse(result.startsWith("\""),
                "COPY CSV should not quote field containing only escape char, got: " + result);
    }

    // ========================================================================
    // M28: PREPARE should reject DDL (utility statements)
    // ========================================================================

    @Test
    void m28_prepareRejectCreateTable() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("PREPARE p_ddl AS CREATE TABLE m28_t (id int)"));
        assertEquals("42601", ex.getSQLState(),
                "PREPARE with DDL should fail with 42601");
    }

    @Test
    void m28_prepareRejectDrop() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("PREPARE p_ddl2 AS DROP TABLE IF EXISTS m28_t"));
        assertEquals("42601", ex.getSQLState(),
                "PREPARE with DROP should fail with 42601");
    }

    @Test
    void m28_prepareAllowSelect() throws SQLException {
        // SELECT should still be allowed in PREPARE
        exec("PREPARE p_sel AS SELECT 1");
        String result = q("EXECUTE p_sel");
        assertEquals("1", result);
    }

    @Test
    void m28_prepareAllowInsert() throws SQLException {
        exec("CREATE TABLE m28_ins (id int)");
        exec("PREPARE p_ins AS INSERT INTO m28_ins VALUES (1)");
        exec("EXECUTE p_ins");
        assertEquals("1", q("SELECT count(*) FROM m28_ins"));
    }

    // ========================================================================
    // L1: alias-hiding error message should match PG
    // ========================================================================

    @Test
    void l1_aliasHidingErrorMessage() throws SQLException {
        exec("CREATE TABLE l1_t (id int)");
        try {
            q("SELECT l1_t.id FROM l1_t AS x");
            fail("Should have thrown");
        } catch (SQLException ex) {
            // PG message: "invalid reference to FROM-clause entry for table \"l1_t\""
            // with hint: "Perhaps you meant to reference the table alias \"x\"."
            assertTrue(ex.getMessage().contains("invalid reference to FROM-clause entry"),
                    "Error should say 'invalid reference to FROM-clause entry', got: " + ex.getMessage());
        }
    }
}
