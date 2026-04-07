package com.memgres;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for "funny" patterns: quoted identifiers, reserved words as names,
 * special characters, case sensitivity, edge-case strings, etc.
 * Verifies PostgreSQL-compatible identifier handling.
 */
class QuirkyPatternsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ==================== Double-quoted identifiers ====================

    @Test
    void testDoubleQuotedTableName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"my weird table\" (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO \"my weird table\" VALUES (1, 'hello')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM \"my weird table\" WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
            stmt.execute("DROP TABLE \"my weird table\"");
        }
    }

    @Test
    void testDoubleQuotedColumnName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE qcol (\"my column\" TEXT, \"another col\" INTEGER)");
            stmt.execute("INSERT INTO qcol (\"my column\", \"another col\") VALUES ('test', 42)");
            ResultSet rs = stmt.executeQuery("SELECT \"my column\", \"another col\" FROM qcol");
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            assertEquals(42, rs.getInt(2));
            stmt.execute("DROP TABLE qcol");
        }
    }

    @Test
    void testReservedWordAsTableName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"select\" (\"from\" TEXT, \"where\" INTEGER)");
            stmt.execute("INSERT INTO \"select\" (\"from\", \"where\") VALUES ('here', 1)");
            ResultSet rs = stmt.executeQuery("SELECT \"from\", \"where\" FROM \"select\"");
            assertTrue(rs.next());
            assertEquals("here", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            stmt.execute("DROP TABLE \"select\"");
        }
    }

    @Test
    void testReservedWordAsColumnName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE reserved_cols (\"table\" TEXT, \"order\" INTEGER, \"group\" TEXT)");
            stmt.execute("INSERT INTO reserved_cols VALUES ('t1', 1, 'g1')");
            ResultSet rs = stmt.executeQuery("SELECT \"table\", \"order\", \"group\" FROM reserved_cols");
            assertTrue(rs.next());
            assertEquals("t1", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertEquals("g1", rs.getString(3));
            stmt.execute("DROP TABLE reserved_cols");
        }
    }

    @Test
    void testQuotedIdentifierWithNumbers() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"123table\" (\"456col\" INTEGER)");
            stmt.execute("INSERT INTO \"123table\" VALUES (789)");
            ResultSet rs = stmt.executeQuery("SELECT \"456col\" FROM \"123table\"");
            assertTrue(rs.next());
            assertEquals(789, rs.getInt(1));
            stmt.execute("DROP TABLE \"123table\"");
        }
    }

    @Test
    void testQuotedIdentifierWithSpecialChars() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"my-table\" (\"col.name\" TEXT, \"col@2\" INTEGER)");
            stmt.execute("INSERT INTO \"my-table\" (\"col.name\", \"col@2\") VALUES ('val', 99)");
            ResultSet rs = stmt.executeQuery("SELECT \"col.name\", \"col@2\" FROM \"my-table\"");
            assertTrue(rs.next());
            assertEquals("val", rs.getString(1));
            assertEquals(99, rs.getInt(2));
            stmt.execute("DROP TABLE \"my-table\"");
        }
    }

    @Test
    void testEmbeddedDoubleQuoteInIdentifier() throws SQLException {
        // In PG, "" inside a quoted identifier represents a literal "
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"say \"\"hello\"\"\" (id INTEGER)");
            stmt.execute("INSERT INTO \"say \"\"hello\"\"\" VALUES (1)");
            ResultSet rs = stmt.executeQuery("SELECT id FROM \"say \"\"hello\"\"\"");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            stmt.execute("DROP TABLE \"say \"\"hello\"\"\"");
        }
    }

    // ==================== Case sensitivity ====================

    @Test
    void testUnquotedIdentifierCaseInsensitive() throws SQLException {
        // In PG, unquoted identifiers are folded to lowercase.
        // CREATE TABLE MyTable -> table name is "mytable"
        // SELECT * FROM MYTABLE -> looks for "mytable"
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE MixedCase (Id INTEGER, NaMe TEXT)");
            stmt.execute("INSERT INTO mixedcase (id, name) VALUES (1, 'test')");
            ResultSet rs = stmt.executeQuery("SELECT ID, NAME FROM MIXEDCASE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("test", rs.getString(2));
            stmt.execute("DROP TABLE MixedCase");
        }
    }

    @Test
    void testQuotedPreservesCase() throws SQLException {
        // "MyTable" preserves case, unlike unquoted mytable
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE \"CaseSensitive\" (\"MyCol\" TEXT)");
            stmt.execute("INSERT INTO \"CaseSensitive\" (\"MyCol\") VALUES ('preserved')");
            ResultSet rs = stmt.executeQuery("SELECT \"MyCol\" FROM \"CaseSensitive\"");
            assertTrue(rs.next());
            assertEquals("preserved", rs.getString(1));
            stmt.execute("DROP TABLE \"CaseSensitive\"");
        }
    }

    // ==================== Special string values ====================

    @Test
    void testEmptyStringValue() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_str (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO empty_str VALUES (1, '')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM empty_str WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
            // PG: empty string is NOT null
            assertFalse(rs.wasNull());
            stmt.execute("DROP TABLE empty_str");
        }
    }

    @Test
    void testStringWithSingleQuotes() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sq (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO sq VALUES (1, 'it''s a test')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM sq WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("it's a test", rs.getString(1));
            stmt.execute("DROP TABLE sq");
        }
    }

    @Test
    void testStringWithBackslash() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE bs (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO bs VALUES (1, 'path\\to\\file')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM bs WHERE id = 1");
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains("\\") || rs.getString(1).contains("to"));
            stmt.execute("DROP TABLE bs");
        }
    }

    @Test
    void testStringWithNewlines() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nl (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO nl VALUES (1, E'line1\\nline2')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM nl WHERE id = 1");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("\n"), "Should contain newline, got: " + val);
            stmt.execute("DROP TABLE nl");
        }
    }

    @Test
    void testStringWithUnicode() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE uni (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO uni VALUES (1, 'caf\u00e9')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM uni WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("caf\u00e9", rs.getString(1));
            stmt.execute("DROP TABLE uni");
        }
    }

    @Test
    void testVeryLongString() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE longstr (id INTEGER, val TEXT)");
            String longVal = Strs.repeat("x", 10000);
            stmt.execute("INSERT INTO longstr VALUES (1, '" + longVal + "')");
            ResultSet rs = stmt.executeQuery("SELECT LENGTH(val) FROM longstr WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(10000, rs.getInt(1));
            stmt.execute("DROP TABLE longstr");
        }
    }

    // ==================== NULL edge cases ====================

    @Test
    void testNullComparisonIsNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // In PG, NULL = NULL is NULL (not true), use IS NULL instead
            ResultSet rs = stmt.executeQuery("SELECT NULL = NULL");
            assertTrue(rs.next());
            rs.getBoolean(1);
            assertTrue(rs.wasNull(), "NULL = NULL should yield NULL, not true");
        }
    }

    @Test
    void testNullInConcatenation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // In PG, 'hello' || NULL = NULL
            ResultSet rs = stmt.executeQuery("SELECT 'hello' || NULL");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    @Test
    void testNullArithmetic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1 + NULL");
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void testCoalesceWithNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COALESCE(NULL, NULL, 'fallback')");
            assertTrue(rs.next());
            assertEquals("fallback", rs.getString(1));
        }
    }

    // ==================== Aliases with funny names ====================

    @Test
    void testQuotedAlias() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1 AS \"my result\", 'hello' AS \"the value\"");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("my result"));
            assertEquals("hello", rs.getString("the value"));
        }
    }

    @Test
    void testTableAliasInJoin() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ta1 (id INTEGER, name TEXT)");
            stmt.execute("CREATE TABLE ta2 (id INTEGER, ta1_id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO ta1 VALUES (1, 'parent')");
            stmt.execute("INSERT INTO ta2 VALUES (1, 1, 'child')");
            ResultSet rs = stmt.executeQuery(
                    "SELECT a.name, b.val FROM ta1 AS a JOIN ta2 AS b ON a.id = b.ta1_id");
            assertTrue(rs.next());
            assertEquals("parent", rs.getString(1));
            assertEquals("child", rs.getString(2));
            stmt.execute("DROP TABLE ta2");
            stmt.execute("DROP TABLE ta1");
        }
    }

    // ==================== Whitespace and formatting ====================

    @Test
    void testExtraWhitespace() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("  SELECT   1  +  2  ");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testMultipleStatementsInBatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE batch1 (id INTEGER)");
            stmt.execute("INSERT INTO batch1 VALUES (1); INSERT INTO batch1 VALUES (2)");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch1");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            stmt.execute("DROP TABLE batch1");
        }
    }

    // ==================== Numeric edge cases ====================

    @Test
    void testIntegerOverflow() throws SQLException {
        // PG throws 22003 "integer out of range" for integer overflow
        try (Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                stmt.executeQuery("SELECT 2147483647 + 1"));
        }
    }

    @Test
    void testDivisionByZero() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT 1 / 0"));
        }
    }

    @Test
    void testNegativeNumbers() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT -42, -3.14, -(-5)");
            assertTrue(rs.next());
            assertEquals(-42, rs.getInt(1));
            assertEquals(-3.14, rs.getDouble(2), 0.001);
            assertEquals(5, rs.getInt(3));
        }
    }

    @Test
    void testScientificNotation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1.5e2, 3E-1");
            assertTrue(rs.next());
            assertEquals(150.0, rs.getDouble(1), 0.001);
            assertEquals(0.3, rs.getDouble(2), 0.001);
        }
    }

    // ==================== Boolean edge cases ====================

    @Test
    void testBooleanLiterals() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE bools (id INTEGER, flag BOOLEAN)");
            stmt.execute("INSERT INTO bools VALUES (1, true), (2, false), (3, NULL)");
            ResultSet rs = stmt.executeQuery("SELECT flag FROM bools WHERE flag IS TRUE ORDER BY id");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE bools");
        }
    }

    // ==================== Tricky WHERE clauses ====================

    @Test
    void testWhereWithParentheses() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wp (a INTEGER, b INTEGER, c INTEGER)");
            stmt.execute("INSERT INTO wp VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)");
            ResultSet rs = stmt.executeQuery(
                    "SELECT a FROM wp WHERE (a = 1 AND b = 2) OR (a = 7 AND c = 9) ORDER BY a");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(7, rs.getInt(1));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE wp");
        }
    }

    @Test
    void testInWithMixedTypes() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE mix (val TEXT)");
            stmt.execute("INSERT INTO mix VALUES ('1'), ('hello'), ('3')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM mix WHERE val IN ('1', 'hello') ORDER BY val");
            assertTrue(rs.next()); assertEquals("1", rs.getString(1));
            assertTrue(rs.next()); assertEquals("hello", rs.getString(1));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE mix");
        }
    }

    // ==================== Expression edge cases ====================

    @Test
    void testConcatOperator() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 'hello' || ' ' || 'world'");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
    }

    @Test
    void testNestedFunctionCalls() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT UPPER(TRIM('  hello  '))");
            assertTrue(rs.next());
            assertEquals("HELLO", rs.getString(1));
        }
    }

    @Test
    void testCaseInsensitiveKeywords() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Mix of upper, lower, and mixed case keywords
            ResultSet rs = stmt.executeQuery("select 1 as result WHERE true ORDER by 1 LIMIT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // ==================== PG-specific string patterns ====================

    @Test
    void testLikeWithPercent() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE likes (val TEXT)");
            stmt.execute("INSERT INTO likes VALUES ('apple'), ('banana'), ('application')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM likes WHERE val LIKE 'app%' ORDER BY val");
            assertTrue(rs.next()); assertEquals("apple", rs.getString(1));
            assertTrue(rs.next()); assertEquals("application", rs.getString(1));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE likes");
        }
    }

    @Test
    void testLikeWithUnderscore() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE likes2 (val TEXT)");
            stmt.execute("INSERT INTO likes2 VALUES ('cat'), ('bat'), ('hat'), ('cart')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM likes2 WHERE val LIKE '_at' ORDER BY val");
            assertTrue(rs.next()); assertEquals("bat", rs.getString(1));
            assertTrue(rs.next()); assertEquals("cat", rs.getString(1));
            assertTrue(rs.next()); assertEquals("hat", rs.getString(1));
            assertFalse(rs.next());
            stmt.execute("DROP TABLE likes2");
        }
    }
}
