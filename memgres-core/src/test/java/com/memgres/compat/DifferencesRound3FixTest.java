package com.memgres.compat;

import com.memgres.Pg18SampleSql5Test;
import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for round-3 differences fixes.
 *
 * S3: parseFile must handle BEGIN ATOMIC...END blocks without splitting on semicolons
 * D4: JSON_SERIALIZE without RETURNING should return bytea (matching PG 18 wire format)
 */
class DifferencesRound3FixTest {

    // ========================================================================
    // S3: parseFile BEGIN ATOMIC handling
    // ========================================================================

    @Test
    void parseFileHandlesBeginAtomicWithMultipleStatements() {
        String sql = "CREATE TABLE t1 (id int);\n"
                + "\n"
                + "CREATE FUNCTION f1(x text) RETURNS text\n"
                + "LANGUAGE sql\n"
                + "BEGIN ATOMIC\n"
                + "  INSERT INTO t1 (id) VALUES (1);\n"
                + "  SELECT 'done: ' || x;\n"
                + "END;\n"
                + "\n"
                + "SELECT f1('hello');\n";

        List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(sql);

        // Should produce 3 blocks:
        // 1. CREATE TABLE t1 (id int)
        // 2. CREATE FUNCTION f1(x text) ... BEGIN ATOMIC ... END
        // 3. SELECT f1('hello')
        assertEquals(3, blocks.size(),
                "BEGIN ATOMIC body should NOT be split at internal semicolons; expected 3 blocks but got "
                        + blocks.size() + ": " + blocks.stream().map(Pg18SampleSql5Test.ParsedBlock::sql).toList());

        String funcSql = blocks.get(1).sql();
        assertTrue(funcSql.contains("BEGIN ATOMIC"), "Function SQL should contain BEGIN ATOMIC");
        assertTrue(funcSql.contains("INSERT INTO"), "Function SQL should contain INSERT statement");
        assertTrue(funcSql.contains("SELECT 'done:"), "Function SQL should contain SELECT statement");
        assertTrue(funcSql.contains("END"), "Function SQL should end with END");
    }

    @Test
    void parseFileHandlesBeginAtomicSingleStatement() {
        String sql = "CREATE FUNCTION f2(x int) RETURNS int\n"
                + "LANGUAGE sql\n"
                + "BEGIN ATOMIC\n"
                + "  SELECT x * 10;\n"
                + "END;\n"
                + "\n"
                + "SELECT f2(5);\n";

        List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(sql);

        assertEquals(2, blocks.size(),
                "Expected 2 blocks but got " + blocks.size());
        assertTrue(blocks.get(0).sql().contains("BEGIN ATOMIC"));
        assertTrue(blocks.get(0).sql().contains("SELECT x * 10"));
    }

    @Test
    void parseFileDollarQuotingStillWorks() {
        // Verify dollar quoting is not broken by BEGIN ATOMIC changes
        String sql = "CREATE FUNCTION f3() RETURNS int LANGUAGE plpgsql AS $$\n"
                + "BEGIN\n"
                + "  RETURN 1;\n"
                + "END;\n"
                + "$$;\n"
                + "\n"
                + "SELECT f3();\n";

        List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(sql);

        assertEquals(2, blocks.size(), "Dollar-quoted body should produce 2 blocks");
        assertTrue(blocks.get(0).sql().contains("$$"));
    }

    // ========================================================================
    // D4: JSON_SERIALIZE should return bytea (matching PG 18 wire format)
    // ========================================================================

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

    @Test
    void jsonSerializeWithoutReturningReturnsText() throws SQLException {
        // JSON_SERIALIZE without RETURNING returns text (SQL standard default)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb) AS result")) {
            ResultSetMetaData md = rs.getMetaData();
            String typeName = md.getColumnTypeName(1);
            assertTrue(rs.next());
            assertEquals("text", typeName,
                    "JSON_SERIALIZE without RETURNING should return text type");
            String val = rs.getString(1);
            assertEquals("{\"a\": 1}", val,
                    "getString() should return the JSON text directly");
        }
    }

    @Test
    void jsonSerializeWithReturningTextReturnsText() throws SQLException {
        // JSON_SERIALIZE with explicit RETURNING text should return text
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb RETURNING text) AS result")) {
            ResultSetMetaData md = rs.getMetaData();
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("{\"a\": 1}", val, "JSON_SERIALIZE RETURNING text should return JSON text");
        }
    }

    @Test
    void jsonSerializeNullReturnsText() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_SERIALIZE(NULL::jsonb) AS result")) {
            ResultSetMetaData md = rs.getMetaData();
            String typeName = md.getColumnTypeName(1);
            assertTrue(rs.next());
            assertNull(rs.getString(1), "JSON_SERIALIZE of NULL should return NULL");
            assertEquals("text", typeName, "JSON_SERIALIZE NULL should still report text type");
        }
    }

    // ========================================================================
    // D2-3: FOREACH SLICE should be rejected (matching PG 18)
    // ========================================================================

    @Test
    void foreachSliceRejectedAtCreation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CREATE FUNCTION test_foreach_slice() RETURNS text LANGUAGE plpgsql AS $$ "
                            + "DECLARE arr integer[] := ARRAY[[1,2],[3,4]]; slice integer[]; result text := ''; "
                            + "BEGIN FOREACH slice SLICE 1 IN ARRAY arr LOOP result := result || slice::text; END LOOP; RETURN result; END; $$"));
            assertEquals("42601", ex.getSQLState(),
                    "FOREACH SLICE should be rejected with syntax error 42601");
        }
    }

    // ========================================================================
    // S1-2: SAVEPOINT and CURSOR WITH HOLD rejected in plpgsql (matching PG 18)
    // ========================================================================

    @Test
    void savepointInProcedureRejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CREATE PROCEDURE test_sp() LANGUAGE plpgsql AS $$ "
                            + "BEGIN SAVEPOINT sp1; END; $$"));
            assertEquals("42601", ex.getSQLState(),
                    "SAVEPOINT in plpgsql should be rejected with 42601");
        }
    }

    @Test
    void rollbackToSavepointInProcedureRejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CREATE PROCEDURE test_rbsp() LANGUAGE plpgsql AS $$ "
                            + "BEGIN ROLLBACK TO SAVEPOINT sp1; END; $$"));
            assertEquals("42601", ex.getSQLState(),
                    "ROLLBACK TO SAVEPOINT in plpgsql should be rejected with 42601");
        }
    }

    @Test
    void cursorWithHoldInProcedureRejected() throws SQLException {
        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("CREATE PROCEDURE test_cwh() LANGUAGE plpgsql AS $$ "
                            + "DECLARE cur CURSOR WITH HOLD FOR SELECT 1; "
                            + "BEGIN OPEN cur; CLOSE cur; END; $$"));
            assertEquals("42601", ex.getSQLState(),
                    "CURSOR WITH HOLD in plpgsql should be rejected with 42601");
        }
    }
}
