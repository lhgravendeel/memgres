package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 8: Built-in Functions tests.
 * String, math, date/time, JSON, conditional, system functions.
 */
class BuiltinFunctionsTest {

    private static Memgres memgres;
    private static Connection conn;

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

    // ==== String functions ====

    @Test
    void testLtrim() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ltrim('   hello')");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void testLtrimChars() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ltrim('xxyhello', 'xy')");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void testRtrim() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT rtrim('hello   ')");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void testLpad() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT lpad('42', 5, '0')");
            assertTrue(rs.next());
            assertEquals("00042", rs.getString(1));
        }
    }

    @Test
    void testRpad() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT rpad('hi', 5, '!')");
            assertTrue(rs.next());
            assertEquals("hi!!!", rs.getString(1));
        }
    }

    @Test
    void testPosition() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT POSITION('lo' IN 'hello')");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void testStrpos() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT strpos('hello world', 'world')");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    @Test
    void testLeft() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT left('hello world', 5)");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void testRight() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT right('hello world', 5)");
            assertTrue(rs.next());
            assertEquals("world", rs.getString(1));
        }
    }

    @Test
    void testRepeat() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT repeat('ab', 3)");
            assertTrue(rs.next());
            assertEquals("ababab", rs.getString(1));
        }
    }

    @Test
    void testReverse() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT reverse('hello')");
            assertTrue(rs.next());
            assertEquals("olleh", rs.getString(1));
        }
    }

    @Test
    void testSplitPart() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT split_part('a.b.c', '.', 2)");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
    }

    @Test
    void testRegexpReplace() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT regexp_replace('hello 123 world', '[0-9]+', 'NUM')");
            assertTrue(rs.next());
            assertEquals("hello NUM world", rs.getString(1));
        }
    }

    @Test
    void testRegexpReplaceGlobal() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT regexp_replace('a1b2c3', '[0-9]', 'X', 'g')");
            assertTrue(rs.next());
            assertEquals("aXbXcX", rs.getString(1));
        }
    }

    @Test
    void testRegexpMatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT regexp_match('abc123def', '([0-9]+)')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("123"), "Expected 123 in: " + val);
        }
    }

    @Test
    void testFormat() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT format('Hello %s, you are %s', 'World', 42)");
            assertTrue(rs.next());
            assertEquals("Hello World, you are 42", rs.getString(1));
        }
    }

    @Test
    void testChr() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT chr(65)");
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
        }
    }

    @Test
    void testAscii() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ascii('A')");
            assertTrue(rs.next());
            assertEquals(65, rs.getInt(1));
        }
    }

    @Test
    void testMd5() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT md5('hello')");
            assertTrue(rs.next());
            assertEquals("5d41402abc4b2a76b9719d911017c592", rs.getString(1));
        }
    }

    @Test
    void testTranslate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT translate('12345', '143', 'ax')");
            assertTrue(rs.next());
            assertEquals("a2x5", rs.getString(1));
        }
    }

    @Test
    void testInitcap() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT initcap('hello world')");
            assertTrue(rs.next());
            assertEquals("Hello World", rs.getString(1));
        }
    }

    @Test
    void testStartsWith() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT starts_with('hello world', 'hello')");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    @Test
    void testOverlay() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT OVERLAY('Txxxxas' PLACING 'hom' FROM 2 FOR 4)");
            assertTrue(rs.next());
            assertEquals("Thomas", rs.getString(1));
        }
    }

    @Test
    void testEncode() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT encode('hello', 'base64')");
            assertTrue(rs.next());
            assertEquals("aGVsbG8=", rs.getString(1));
        }
    }

    @Test
    void testOctetLength() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT octet_length('hello')");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void testQuoteLiteral() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT quote_literal('hello')");
            assertTrue(rs.next());
            assertEquals("'hello'", rs.getString(1));
        }
    }

    @Test
    void testStringToArray() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT array_to_string(string_to_array('a,b,c', ','), '-')");
            assertTrue(rs.next());
            assertEquals("a-b-c", rs.getString(1));
        }
    }

    // ==== Math functions ====

    @Test
    void testTrunc() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT trunc(42.8)");
            assertTrue(rs.next());
            assertEquals(42, rs.getLong(1));
        }
    }

    @Test
    void testTruncScale() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT trunc(42.4382, 2)");
            assertTrue(rs.next());
            assertEquals(42.43, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testMod() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT mod(10, 3)");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testPower() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT power(2, 10)");
            assertTrue(rs.next());
            assertEquals(1024.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testSqrt() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT sqrt(144)");
            assertTrue(rs.next());
            assertEquals(12.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testCbrt() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT cbrt(27)");
            assertTrue(rs.next());
            assertEquals(3.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testLog() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT log(100)");
            assertTrue(rs.next());
            assertEquals(2.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testLn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ln(2.718281828)");
            assertTrue(rs.next());
            assertEquals(1.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testExp() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT exp(1)");
            assertTrue(rs.next());
            assertEquals(2.718, rs.getDouble(1), 0.01);
        }
    }

    @Test
    void testSign() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT sign(-42)");
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(1));

            rs = stmt.executeQuery("SELECT sign(0)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            rs = stmt.executeQuery("SELECT sign(99)");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testPi() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pi()");
            assertTrue(rs.next());
            assertEquals(3.14159, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testDegrees() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT degrees(3.14159265358979)");
            assertTrue(rs.next());
            assertEquals(180.0, rs.getDouble(1), 0.01);
        }
    }

    @Test
    void testRadians() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT radians(180)");
            assertTrue(rs.next());
            assertEquals(3.14159, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testTrigFunctions() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT sin(0), cos(0), tan(0)");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), 0.001);
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertEquals(0.0, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testDiv() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT div(7, 2)");
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
        }
    }

    @Test
    void testGcd() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT gcd(12, 8)");
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
        }
    }

    @Test
    void testLcm() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT lcm(12, 8)");
            assertTrue(rs.next());
            assertEquals(24, rs.getLong(1));
        }
    }

    @Test
    void testWidthBucket() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT width_bucket(5.35, 0.024, 10.06, 5)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    // ==== JSON functions ====

    @Test
    void testJsonbBuildObject() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT jsonb_build_object('name', 'Alice', 'age', 30)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("\"name\""), "Expected name key in: " + val);
            assertTrue(val.contains("Alice"), "Expected Alice in: " + val);
            assertTrue(val.contains("30"), "Expected 30 in: " + val);
        }
    }

    @Test
    void testJsonbBuildArray() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT jsonb_build_array(1, 2, 'three')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.startsWith("["), "Expected array in: " + val);
            assertTrue(val.contains("1"), "Expected 1 in: " + val);
            assertTrue(val.contains("three"), "Expected three in: " + val);
        }
    }

    @Test
    void testToJson() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_json(42)");
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
        }
    }

    @Test
    void testToJsonText() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_json('hello')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("hello"), "Expected hello in: " + val);
        }
    }

    @Test
    void testJsonbTypeof() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT jsonb_typeof('{\"a\":1}')");
            assertTrue(rs.next());
            assertEquals("object", rs.getString(1));

            rs = stmt.executeQuery("SELECT jsonb_typeof('[1,2]')");
            assertTrue(rs.next());
            assertEquals("array", rs.getString(1));
        }
    }

    @Test
    void testJsonbArrayLength() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT jsonb_array_length('[1, 2, 3]')");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testJsonbExtractPathText() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT jsonb_extract_path_text('{\"name\": \"Alice\", \"age\": 30}', 'name')");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void testJsonArrow() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT '{\"name\": \"Bob\"}'::jsonb ->> 'name'");
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString(1));
        }
    }

    // ==== Conditional functions (already mostly tested, add edge cases) ====

    @Test
    void testCoalesceMultiple() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT coalesce(NULL, NULL, 'found', 'not this')");
            assertTrue(rs.next());
            assertEquals("found", rs.getString(1));
        }
    }

    @Test
    void testNullif() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT nullif(1, 1)");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));

            rs = stmt.executeQuery("SELECT nullif(1, 2)");
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
        }
    }

    // ==== System functions ====

    @Test
    void testVersion() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT version()");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("PostgreSQL") && val.contains("18.0"));
        }
    }

    @Test
    void testCurrentUser() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_user");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    // ==== Combined/edge case tests ====

    @Test
    void testNestedStringFunctions() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT upper(reverse('hello'))");
            assertTrue(rs.next());
            assertEquals("OLLEH", rs.getString(1));
        }
    }

    @Test
    void testMathExpressionCombined() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT round(sqrt(power(3, 2) + power(4, 2)))");
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));
        }
    }

    @Test
    void testNullPropagation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT upper(NULL), abs(NULL), length(NULL)");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(3));
        }
    }
}
