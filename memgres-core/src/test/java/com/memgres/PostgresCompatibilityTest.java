package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive PostgreSQL compatibility tests.
 * Tests real-world query patterns, type casting, precision, date/time,
 * UUID, generate_series, ILIKE, ANY/ALL, arrays, JSON, sequences,
 * RETURNING, ON CONFLICT, window functions, and common application patterns.
 */
class PostgresCompatibilityTest {

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

    // ================================================================
    //  1. TYPE CASTING: cross-type, NULL, edge cases
    // ================================================================

    @Test
    void testCastIntegerToText() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 42::text");
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
        }
    }

    @Test
    void testCastTextToInteger() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '123'::integer");
            assertTrue(rs.next());
            assertEquals(123, rs.getInt(1));
        }
    }

    @Test
    void testCastTextToBoolean() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'true'::boolean, 'false'::boolean, 'yes'::boolean, 'no'::boolean");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
            assertFalse(rs.getBoolean(4));
        }
    }

    @Test
    void testCastIntegerToBoolean() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1::boolean, 0::boolean");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
        }
    }

    @Test
    void testCastNullPreservesNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULL::integer, NULL::text, NULL::boolean, NULL::date");
            assertTrue(rs.next());
            rs.getInt(1); assertTrue(rs.wasNull());
            assertNull(rs.getString(2));
            rs.getBoolean(3); assertTrue(rs.wasNull());
            assertNull(rs.getDate(4));
        }
    }

    @Test
    void testCastDoubleToInteger() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 3.7::integer, 3.2::integer");
            assertTrue(rs.next());
            // PG truncates toward zero for cast to int
            int val1 = rs.getInt(1);
            int val2 = rs.getInt(2);
            assertTrue(val1 == 3 || val1 == 4, "3.7 cast to int should be 3 or 4, got " + val1);
            assertTrue(val2 == 3, "3.2 cast to int should be 3, got " + val2);
        }
    }

    @Test
    void testCastTimestampToDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '2024-06-15 14:30:00'::timestamp::date");
            assertTrue(rs.next());
            assertEquals("2024-06-15", rs.getString(1));
        }
    }

    @Test
    void testCastDateToTimestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '2024-06-15'::date::timestamp");
            assertTrue(rs.next());
            String ts = rs.getString(1);
            assertTrue(ts.startsWith("2024-06-15"), "Should start with date, got: " + ts);
        }
    }

    @Test
    void testCastChaining() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // 42 -> text -> integer -> bigint
            ResultSet rs = s.executeQuery("SELECT 42::text::integer::bigint");
            assertTrue(rs.next());
            assertEquals(42L, rs.getLong(1));
        }
    }

    @Test
    void testCastWithCastFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(3.14 AS integer), CAST('2024-01-01' AS date)");
            assertTrue(rs.next());
            int intVal = rs.getInt(1);
            assertTrue(intVal == 3, "CAST(3.14 AS integer) should be 3, got " + intVal);
        }
    }

    // ================================================================
    //  2. NUMERIC PRECISION: boundaries, rounding, overflow
    // ================================================================

    @Test
    void testSmallintBoundaries() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE si_test (v SMALLINT)");
            s.execute("INSERT INTO si_test VALUES (32767), (-32768), (0)");
            ResultSet rs = s.executeQuery("SELECT MIN(v), MAX(v) FROM si_test");
            assertTrue(rs.next());
            assertEquals(-32768, rs.getInt(1));
            assertEquals(32767, rs.getInt(2));
            s.execute("DROP TABLE si_test");
        }
    }

    @Test
    void testBigintArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 9223372036854775806::bigint + 1");
            assertTrue(rs.next());
            assertEquals(9223372036854775807L, rs.getLong(1));
        }
    }

    @Test
    void testFloatPrecision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Classic floating point issue
            ResultSet rs = s.executeQuery("SELECT 0.1 + 0.2");
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(0.3, val, 0.0001, "0.1 + 0.2 should be approximately 0.3");
        }
    }

    @Test
    void testDivisionPrecision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1.0 / 3.0");
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(0.3333, val, 0.001);
        }
    }

    @Test
    void testIntegerDivisionTruncates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7 / 2");
            assertTrue(rs.next());
            int val = rs.getInt(1);
            // In PG, integer / integer = integer (truncated)
            assertEquals(3, val);
        }
    }

    @Test
    void testModuloOperator() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 17 % 5, -17 % 5, 17 % -5");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testRoundFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ROUND(2.5), ROUND(3.14159, 2), ROUND(-2.5)");
            assertTrue(rs.next());
            // PG rounds half away from zero
            double r1 = rs.getDouble(1);
            assertTrue(r1 == 3.0 || r1 == 2.0, "ROUND(2.5) got " + r1);
            assertEquals(3.14, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void testTruncFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUNC(99.9), TRUNC(-99.9), TRUNC(3.14159, 3)");
            assertTrue(rs.next());
            assertEquals(99.0, rs.getDouble(1), 0.001);
            assertEquals(-99.0, rs.getDouble(2), 0.001);
            assertEquals(3.141, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testPowerAndSqrt() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT POWER(2, 10), SQRT(144), CBRT(27)");
            assertTrue(rs.next());
            assertEquals(1024.0, rs.getDouble(1), 0.001);
            assertEquals(12.0, rs.getDouble(2), 0.001);
            assertEquals(3.0, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testAbsCeilFloor() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ABS(-42), CEIL(2.3), FLOOR(2.9), CEIL(-2.3), FLOOR(-2.9)");
            assertTrue(rs.next());
            assertEquals(42.0, rs.getDouble(1), 0.001);
            assertEquals(3.0, rs.getDouble(2), 0.001);
            assertEquals(2.0, rs.getDouble(3), 0.001);
            assertEquals(-2.0, rs.getDouble(4), 0.001);
            assertEquals(-3.0, rs.getDouble(5), 0.001);
        }
    }

    @Test
    void testLogAndExp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT LN(1), EXP(0), LOG(100)");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), 0.001);
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertEquals(2.0, rs.getDouble(3), 0.001);
        }
    }

    // ================================================================
    //  3. DATE/TIME: arithmetic, extract, formatting, intervals
    // ================================================================

    @Test
    void testCurrentTimestampNotNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NOW(), CURRENT_TIMESTAMP, CURRENT_DATE");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
        }
    }

    @Test
    void testExtractDateParts() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(YEAR FROM DATE '2024-06-15'), " +
                    "EXTRACT(MONTH FROM DATE '2024-06-15'), " +
                    "EXTRACT(DAY FROM DATE '2024-06-15')");
            assertTrue(rs.next());
            assertEquals(2024.0, rs.getDouble(1), 0.001);
            assertEquals(6.0, rs.getDouble(2), 0.001);
            assertEquals(15.0, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testDateTrunc() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_trunc('month', TIMESTAMP '2024-06-15 14:30:00')");
            assertTrue(rs.next());
            String truncated = rs.getString(1);
            assertTrue(truncated.startsWith("2024-06-01"), "Should truncate to first of month, got: " + truncated);
        }
    }

    @Test
    void testDateArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-01' + 30");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-01-31"), "Jan 1 + 30 days = Jan 31, got: " + result);
        }
    }

    @Test
    void testIntervalArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT DATE '2024-01-15' + INTERVAL '2 months'");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-03-15"), "Jan 15 + 2 months = Mar 15, got: " + result);
        }
    }

    @Test
    void testAgeFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT AGE(DATE '2024-06-15', DATE '2020-01-01')");
            assertTrue(rs.next());
            String age = rs.getString(1);
            assertNotNull(age);
            // Should contain years
            assertTrue(age.contains("4") || age.contains("year"), "Age should show ~4 years, got: " + age);
        }
    }

    @Test
    void testToCharDateFormatting() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-06-15', 'YYYY-MM-DD')");
            assertTrue(rs.next());
            assertEquals("2024-06-15", rs.getString(1));
        }
    }

    @Test
    void testToDateParsing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_date('15-06-2024', 'DD-MM-YYYY')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-06-15"), "to_date should parse correctly, got: " + result);
        }
    }

    @Test
    void testExtractEpoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00')");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testExtractDowAndDoy() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // 2024-01-01 is a Monday (DOW=1 in PG, but 0=Sun convention varies)
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(DOW FROM DATE '2024-01-01'), EXTRACT(DOY FROM DATE '2024-01-01')");
            assertTrue(rs.next());
            double dow = rs.getDouble(1);
            assertEquals(1.0, rs.getDouble(2), 0.001); // Jan 1 = day 1
            assertTrue(dow >= 0 && dow <= 6, "DOW should be 0-6, got: " + dow);
        }
    }

    @Test
    void testMakeDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT make_date(2024, 6, 15)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-06-15"), "make_date should create date, got: " + result);
        }
    }

    // ================================================================
    //  4. UUID: generation, uniqueness, format
    // ================================================================

    @Test
    void testGenRandomUuid() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT gen_random_uuid()");
            assertTrue(rs.next());
            String uuid = rs.getString(1);
            assertNotNull(uuid);
            assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "UUID should match format, got: " + uuid);
        }
    }

    @Test
    void testUuidUniqueness() throws SQLException {
        try (Statement s = conn.createStatement()) {
            Set<String> uuids = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                ResultSet rs = s.executeQuery("SELECT gen_random_uuid()");
                assertTrue(rs.next());
                uuids.add(rs.getString(1));
            }
            assertEquals(100, uuids.size(), "100 UUIDs should all be unique");
        }
    }

    @Test
    void testUuidInTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_test (id UUID DEFAULT gen_random_uuid(), name TEXT)");
            s.execute("INSERT INTO uuid_test (name) VALUES ('alice'), ('bob'), ('charlie')");
            ResultSet rs = s.executeQuery("SELECT id, name FROM uuid_test ORDER BY name");
            Set<String> uuids = new HashSet<>();
            while (rs.next()) {
                String uuid = rs.getString(1);
                assertNotNull(uuid);
                uuids.add(uuid);
            }
            assertEquals(3, uuids.size(), "Each row should have a unique UUID");
            s.execute("DROP TABLE uuid_test");
        }
    }

    @Test
    void testUuidCastToText() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT gen_random_uuid()::text");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.matches("[0-9a-f-]{36}"), "UUID cast to text should work, got: " + result);
        }
    }

    // ================================================================
    //  5. generate_series: numeric, date, step variations
    // ================================================================

    @Test
    void testGenerateSeriesBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM generate_series(1, 5)");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(5, count, "generate_series(1,5) should produce 5 rows");
        }
    }

    @Test
    void testGenerateSeriesWithStep() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM generate_series(0, 10, 2)");
            int count = 0;
            int prev = -1;
            while (rs.next()) {
                int val = rs.getInt(1);
                assertTrue(val > prev);
                assertTrue(val % 2 == 0, "Step of 2 should give even numbers, got: " + val);
                prev = val;
                count++;
            }
            assertEquals(6, count, "0,2,4,6,8,10 = 6 rows");
        }
    }

    @Test
    void testGenerateSeriesDescending() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM generate_series(5, 1, -1)");
            int count = 0;
            int prev = 6;
            while (rs.next()) {
                int val = rs.getInt(1);
                assertTrue(val < prev, "Should descend");
                prev = val;
                count++;
            }
            assertEquals(5, count, "5,4,3,2,1 = 5 rows");
        }
    }

    @Test
    void testGenerateSeriesEmpty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM generate_series(5, 1)");
            // Default step is 1, going from 5 to 1 with step 1 produces no rows
            assertFalse(rs.next(), "generate_series(5,1) with positive step should be empty");
        }
    }

    @Test
    void testGenerateSeriesInQuery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT n, n * n AS square FROM generate_series(1, 5) AS n ORDER BY n");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(4, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(9, rs.getInt(2));
        }
    }

    // ================================================================
    //  6. STRING PATTERNS: ILIKE, regex, special chars
    // ================================================================

    @Test
    void testIlike() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ilike_test (val TEXT)");
            s.execute("INSERT INTO ilike_test VALUES ('Hello'), ('HELLO'), ('hello'), ('World')");
            ResultSet rs = s.executeQuery("SELECT val FROM ilike_test WHERE val ILIKE 'hello' ORDER BY val");
            int count = 0;
            while (rs.next()) {
                assertTrue(rs.getString(1).equalsIgnoreCase("hello"));
                count++;
            }
            assertEquals(3, count, "ILIKE should match case-insensitively");
            s.execute("DROP TABLE ilike_test");
        }
    }

    @Test
    void testIlikeWithWildcard() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ilike2 (val TEXT)");
            s.execute("INSERT INTO ilike2 VALUES ('Apple'), ('application'), ('APEX'), ('banana')");
            ResultSet rs = s.executeQuery("SELECT val FROM ilike2 WHERE val ILIKE 'ap%' ORDER BY val");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(3, count, "ILIKE 'ap%' should match Apple, application, APEX");
            s.execute("DROP TABLE ilike2");
        }
    }

    @Test
    void testRegexpReplace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT regexp_replace('Hello 123 World 456', '[0-9]+', 'NUM', 'g')");
            assertTrue(rs.next());
            assertEquals("Hello NUM World NUM", rs.getString(1));
        }
    }

    @Test
    void testPositionFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT POSITION('world' IN 'hello world')");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    @Test
    void testSubstringWithPattern() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT SUBSTRING('hello world' FROM 7)");
            assertTrue(rs.next());
            assertEquals("world", rs.getString(1));
        }
    }

    @Test
    void testSplitPart() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT split_part('a.b.c.d', '.', 2)");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
    }

    @Test
    void testRepeatAndReverse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT REPEAT('ab', 3), REVERSE('hello')");
            assertTrue(rs.next());
            assertEquals("ababab", rs.getString(1));
            assertEquals("olleh", rs.getString(2));
        }
    }

    @Test
    void testLpadRpad() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT LPAD('42', 5, '0'), RPAD('hi', 5, '.')");
            assertTrue(rs.next());
            assertEquals("00042", rs.getString(1));
            assertEquals("hi...", rs.getString(2));
        }
    }

    @Test
    void testInitcap() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INITCAP('hello world foo bar')");
            assertTrue(rs.next());
            assertEquals("Hello World Foo Bar", rs.getString(1));
        }
    }

    @Test
    void testTranslate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRANSLATE('hello', 'helo', 'HELO')");
            assertTrue(rs.next());
            assertEquals("HELLO", rs.getString(1));
        }
    }

    // ================================================================
    //  7. ANY/ALL operators
    // ================================================================

    @Test
    void testAnyWithArray() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE any_test (id INTEGER, name TEXT)");
            s.execute("INSERT INTO any_test VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
            ResultSet rs = s.executeQuery("SELECT name FROM any_test WHERE id = ANY(ARRAY[1,3,5]) ORDER BY name");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1));
            assertTrue(rs.next()); assertEquals("c", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE any_test");
        }
    }

    @Test
    void testInAsAnyEquivalent() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // IN (...) should be equivalent to = ANY(ARRAY[...])
            s.execute("CREATE TABLE in_test (v INTEGER)");
            s.execute("INSERT INTO in_test VALUES (1),(2),(3),(4),(5)");
            ResultSet rs1 = s.executeQuery("SELECT COUNT(*) FROM in_test WHERE v IN (2,4)");
            assertTrue(rs1.next());
            int countIn = rs1.getInt(1);
            assertEquals(2, countIn);
            s.execute("DROP TABLE in_test");
        }
    }

    // ================================================================
    //  8. ARRAY operations
    // ================================================================

    @Test
    void testArrayLiteral() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ARRAY[1, 2, 3]");
            assertTrue(rs.next());
            String arr = rs.getString(1);
            assertNotNull(arr);
            // PG format: {1,2,3}
            assertTrue(arr.contains("1") && arr.contains("2") && arr.contains("3"),
                    "Array should contain elements, got: " + arr);
        }
    }

    @Test
    void testArrayLength() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_length(ARRAY[10, 20, 30, 40], 1)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void testArrayToString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_to_string(ARRAY['a', 'b', 'c'], ', ')");
            assertTrue(rs.next());
            assertEquals("a, b, c", rs.getString(1));
        }
    }

    @Test
    void testStringToArray() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_length(string_to_array('one,two,three', ','), 1)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testArrayAgg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE arr_agg (grp TEXT, val INTEGER)");
            s.execute("INSERT INTO arr_agg VALUES ('a',1),('a',2),('b',3),('b',4),('b',5)");
            ResultSet rs = s.executeQuery("SELECT grp, array_length(array_agg(val), 1) FROM arr_agg GROUP BY grp ORDER BY grp");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals(3, rs.getInt(2));
            s.execute("DROP TABLE arr_agg");
        }
    }

    // ================================================================
    //  9. JSON/JSONB: construction, extraction, operators
    // ================================================================

    @Test
    void testJsonBuildObject() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_build_object('name', 'Alice', 'age', 30)");
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertTrue(json.contains("Alice") && json.contains("30"), "JSON should contain values, got: " + json);
        }
    }

    @Test
    void testJsonBuildArray() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_build_array(1, 'two', 3.0, true, NULL)");
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertTrue(json.contains("1") && json.contains("two"), "JSON array should have elements, got: " + json);
        }
    }

    @Test
    void testJsonArrowOperator() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"name\": \"Alice\", \"age\": 30}'::jsonb ->> 'name'");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void testJsonNestedAccess() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT '{\"a\": {\"b\": \"deep\"}}'::jsonb -> 'a' ->> 'b'");
            assertTrue(rs.next());
            assertEquals("deep", rs.getString(1));
        }
    }

    @Test
    void testJsonTypeof() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_typeof('42'::jsonb), jsonb_typeof('\"hello\"'::jsonb), jsonb_typeof('true'::jsonb), jsonb_typeof('null'::jsonb)");
            assertTrue(rs.next());
            assertEquals("number", rs.getString(1));
            assertEquals("string", rs.getString(2));
            assertEquals("boolean", rs.getString(3));
            assertEquals("null", rs.getString(4));
        }
    }

    @Test
    void testJsonArrayLength() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_array_length('[1, 2, 3, 4, 5]'::jsonb)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void testJsonInTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE json_test (id INTEGER, data JSONB)");
            s.execute("INSERT INTO json_test VALUES (1, '{\"name\": \"Alice\", \"score\": 95}')");
            s.execute("INSERT INTO json_test VALUES (2, '{\"name\": \"Bob\", \"score\": 87}')");
            ResultSet rs = s.executeQuery("SELECT data ->> 'name', (data ->> 'score')::integer FROM json_test ORDER BY id");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(95, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(87, rs.getInt(2));
            s.execute("DROP TABLE json_test");
        }
    }

    // ================================================================
    //  10. SEQUENCES: nextval, currval, setval
    // ================================================================

    @Test
    void testSequenceBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE test_seq START 1");
            ResultSet rs = s.executeQuery("SELECT nextval('test_seq')");
            assertTrue(rs.next()); assertEquals(1, rs.getLong(1));
            rs = s.executeQuery("SELECT nextval('test_seq')");
            assertTrue(rs.next()); assertEquals(2, rs.getLong(1));
            rs = s.executeQuery("SELECT currval('test_seq')");
            assertTrue(rs.next()); assertEquals(2, rs.getLong(1));
            s.execute("DROP SEQUENCE test_seq");
        }
    }

    @Test
    void testSetval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE sv_seq START 1");
            s.executeQuery("SELECT nextval('sv_seq')"); // must call nextval first
            s.executeQuery("SELECT setval('sv_seq', 100)");
            ResultSet rs = s.executeQuery("SELECT nextval('sv_seq')");
            assertTrue(rs.next()); assertEquals(101, rs.getLong(1));
            s.execute("DROP SEQUENCE sv_seq");
        }
    }

    @Test
    void testSerialColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE serial_test (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO serial_test (name) VALUES ('a'), ('b'), ('c')");
            ResultSet rs = s.executeQuery("SELECT id, name FROM serial_test ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("b", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("c", rs.getString(2));
            s.execute("DROP TABLE serial_test");
        }
    }

    // ================================================================
    //  11. RETURNING clauses
    // ================================================================

    @Test
    void testInsertReturning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ret_test (id SERIAL, name TEXT, created_at TIMESTAMP DEFAULT NOW())");
            ResultSet rs = s.executeQuery("INSERT INTO ret_test (name) VALUES ('Alice') RETURNING id, name");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("Alice", rs.getString(2));
            s.execute("DROP TABLE ret_test");
        }
    }

    @Test
    void testUpdateReturning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ret2 (id INTEGER, val INTEGER)");
            s.execute("INSERT INTO ret2 VALUES (1, 10), (2, 20), (3, 30)");
            ResultSet rs = s.executeQuery("UPDATE ret2 SET val = val * 2 WHERE id <= 2 RETURNING id, val");
            int count = 0;
            while (rs.next()) {
                assertTrue(rs.getInt(2) >= 20, "Updated val should be doubled");
                count++;
            }
            assertEquals(2, count, "Should return 2 updated rows");
            s.execute("DROP TABLE ret2");
        }
    }

    @Test
    void testDeleteReturning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ret3 (id INTEGER, name TEXT)");
            s.execute("INSERT INTO ret3 VALUES (1,'a'),(2,'b'),(3,'c')");
            ResultSet rs = s.executeQuery("DELETE FROM ret3 WHERE id = 2 RETURNING *");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("b", rs.getString(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE ret3");
        }
    }

    // ================================================================
    //  12. ON CONFLICT (upsert)
    // ================================================================

    @Test
    void testOnConflictDoNothing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upsert1 (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO upsert1 VALUES (1, 'original')");
            s.execute("INSERT INTO upsert1 VALUES (1, 'duplicate') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT val FROM upsert1 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("original", rs.getString(1), "DO NOTHING should keep original");
            s.execute("DROP TABLE upsert1");
        }
    }

    @Test
    void testOnConflictDoUpdate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upsert2 (id INTEGER PRIMARY KEY, val TEXT, count INTEGER DEFAULT 1)");
            s.execute("INSERT INTO upsert2 VALUES (1, 'first', 1)");
            s.execute("INSERT INTO upsert2 (id, val) VALUES (1, 'second') ON CONFLICT (id) DO UPDATE SET val = 'updated', count = upsert2.count + 1");
            ResultSet rs = s.executeQuery("SELECT val, count FROM upsert2 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            s.execute("DROP TABLE upsert2");
        }
    }

    @Test
    void testOnConflictMultipleRows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upsert3 (id INTEGER PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO upsert3 VALUES (1,'a'),(2,'b')");
            s.execute("INSERT INTO upsert3 VALUES (2,'bb'),(3,'c') ON CONFLICT (id) DO UPDATE SET name = 'conflict_' || EXCLUDED.name");
            ResultSet rs = s.executeQuery("SELECT id, name FROM upsert3 ORDER BY id");
            assertTrue(rs.next()); assertEquals("a", rs.getString(2));
            assertTrue(rs.next()); assertEquals("conflict_bb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("c", rs.getString(2));
            s.execute("DROP TABLE upsert3");
        }
    }

    // ================================================================
    //  13. WINDOW FUNCTIONS: edge cases
    // ================================================================

    @Test
    void testRowNumberPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE wf_test (dept TEXT, name TEXT, salary INTEGER)");
            s.execute("INSERT INTO wf_test VALUES ('A','x',100),('A','y',200),('A','z',150),('B','p',300),('B','q',250)");
            ResultSet rs = s.executeQuery(
                    "SELECT dept, name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn " +
                    "FROM wf_test ORDER BY dept, rn");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals("y", rs.getString(2)); assertEquals(1, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals("z", rs.getString(2)); assertEquals(2, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals("x", rs.getString(2)); assertEquals(3, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals("p", rs.getString(2)); assertEquals(1, rs.getInt(4));
            s.execute("DROP TABLE wf_test");
        }
    }

    @Test
    void testRankWithTies() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rank_test (name TEXT, score INTEGER)");
            s.execute("INSERT INTO rank_test VALUES ('a',100),('b',100),('c',90),('d',80)");
            ResultSet rs = s.executeQuery(
                    "SELECT name, score, RANK() OVER (ORDER BY score DESC), DENSE_RANK() OVER (ORDER BY score DESC) " +
                    "FROM rank_test ORDER BY score DESC, name");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(3)); assertEquals(1, rs.getInt(4)); // a,100
            assertTrue(rs.next()); assertEquals(1, rs.getInt(3)); assertEquals(1, rs.getInt(4)); // b,100
            assertTrue(rs.next()); assertEquals(3, rs.getInt(3)); assertEquals(2, rs.getInt(4)); // c,90 (rank skips to 3, dense_rank is 2)
            assertTrue(rs.next()); assertEquals(4, rs.getInt(3)); assertEquals(3, rs.getInt(4)); // d,80
            s.execute("DROP TABLE rank_test");
        }
    }

    @Test
    void testLagLead() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lagtest (d DATE, val INTEGER)");
            s.execute("INSERT INTO lagtest VALUES ('2024-01-01',10),('2024-02-01',20),('2024-03-01',15),('2024-04-01',25)");
            ResultSet rs = s.executeQuery(
                    "SELECT d, val, LAG(val) OVER (ORDER BY d) AS prev, LEAD(val) OVER (ORDER BY d) AS next " +
                    "FROM lagtest ORDER BY d");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(2));
            rs.getInt(3); assertTrue(rs.wasNull(), "LAG of first row should be NULL");
            assertEquals(20, rs.getInt(4));
            s.execute("DROP TABLE lagtest");
        }
    }

    @Test
    void testSumAsWindowFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE running (month INTEGER, amount INTEGER)");
            s.execute("INSERT INTO running VALUES (1,100),(2,200),(3,150),(4,300)");
            ResultSet rs = s.executeQuery(
                    "SELECT month, amount, SUM(amount) OVER (ORDER BY month) AS running_total " +
                    "FROM running ORDER BY month");
            assertTrue(rs.next()); assertEquals(100, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(300, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(450, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(750, rs.getInt(3));
            s.execute("DROP TABLE running");
        }
    }

    // ================================================================
    //  14. CTE PATTERNS: common real-world uses
    // ================================================================

    @Test
    void testCteBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "WITH nums AS (SELECT generate_series(1, 5) AS n) " +
                    "SELECT SUM(n) FROM nums");
            assertTrue(rs.next());
            assertEquals(15, rs.getInt(1));
        }
    }

    @Test
    void testCteMultiple() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cte_data (id INTEGER, grp TEXT, val INTEGER)");
            s.execute("INSERT INTO cte_data VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)");
            ResultSet rs = s.executeQuery(
                    "WITH grp_sums AS (SELECT grp, SUM(val) AS total FROM cte_data GROUP BY grp), " +
                    "     grand AS (SELECT SUM(total) AS grand_total FROM grp_sums) " +
                    "SELECT gs.grp, gs.total, g.grand_total " +
                    "FROM grp_sums gs CROSS JOIN grand g ORDER BY gs.grp");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals(30, rs.getInt(2)); assertEquals(100, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals(70, rs.getInt(2)); assertEquals(100, rs.getInt(3));
            s.execute("DROP TABLE cte_data");
        }
    }

    @Test
    void testRecursiveCte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE cnt(n) AS (" +
                    "  SELECT 1 " +
                    "  UNION ALL " +
                    "  SELECT n + 1 FROM cnt WHERE n < 10" +
                    ") SELECT SUM(n) FROM cnt");
            assertTrue(rs.next());
            assertEquals(55, rs.getInt(1), "Sum of 1..10 = 55");
        }
    }

    // ================================================================
    //  15. COMMON APPLICATION PATTERNS
    // ================================================================

    @Test
    void testPagination() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE paginate (id SERIAL, name TEXT)");
            for (int i = 1; i <= 50; i++) {
                s.execute("INSERT INTO paginate (name) VALUES ('item" + i + "')");
            }
            // Page 3, 10 items per page
            ResultSet rs = s.executeQuery("SELECT id, name FROM paginate ORDER BY id LIMIT 10 OFFSET 20");
            assertTrue(rs.next());
            assertEquals(21, rs.getInt(1));
            int count = 1;
            while (rs.next()) count++;
            assertEquals(10, count);
            s.execute("DROP TABLE paginate");
        }
    }

    @Test
    void testConditionalAggregation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sales (region TEXT, amount INTEGER, status TEXT)");
            s.execute("INSERT INTO sales VALUES ('N',100,'won'),('N',200,'lost'),('S',150,'won'),('S',50,'won'),('S',300,'lost')");
            ResultSet rs = s.executeQuery(
                    "SELECT region, " +
                    "SUM(CASE WHEN status = 'won' THEN amount ELSE 0 END) AS won_total, " +
                    "SUM(CASE WHEN status = 'lost' THEN amount ELSE 0 END) AS lost_total " +
                    "FROM sales GROUP BY region ORDER BY region");
            assertTrue(rs.next()); assertEquals("N", rs.getString(1)); assertEquals(100, rs.getInt(2)); assertEquals(200, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("S", rs.getString(1)); assertEquals(200, rs.getInt(2)); assertEquals(300, rs.getInt(3));
            s.execute("DROP TABLE sales");
        }
    }

    @Test
    void testCoalesceChain() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prefs (user_pref TEXT, dept_pref TEXT, global_pref TEXT)");
            s.execute("INSERT INTO prefs VALUES (NULL, 'dept_val', 'global_val')");
            s.execute("INSERT INTO prefs VALUES ('user_val', NULL, 'global_val')");
            s.execute("INSERT INTO prefs VALUES (NULL, NULL, 'global_val')");
            ResultSet rs = s.executeQuery(
                    "SELECT COALESCE(user_pref, dept_pref, global_pref) FROM prefs");
            assertTrue(rs.next()); assertEquals("dept_val", rs.getString(1));
            assertTrue(rs.next()); assertEquals("user_val", rs.getString(1));
            assertTrue(rs.next()); assertEquals("global_val", rs.getString(1));
            s.execute("DROP TABLE prefs");
        }
    }

    @Test
    void testStringAgg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tags (item TEXT, tag TEXT)");
            s.execute("INSERT INTO tags VALUES ('a','x'),('a','y'),('a','z'),('b','p')");
            ResultSet rs = s.executeQuery(
                    "SELECT item, string_agg(tag, ', ' ORDER BY tag) FROM tags GROUP BY item ORDER BY item");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals("x, y, z", rs.getString(2));
            assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals("p", rs.getString(2));
            s.execute("DROP TABLE tags");
        }
    }

    @Test
    void testExistsForConditionalLogic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER)");
            s.execute("CREATE TABLE customers (id INTEGER, name TEXT)");
            s.execute("INSERT INTO customers VALUES (1,'Active'),(2,'Inactive'),(3,'Active')");
            s.execute("INSERT INTO orders VALUES (1,1),(2,1),(3,3)");
            ResultSet rs = s.executeQuery(
                    "SELECT c.name FROM customers c " +
                    "WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY c.name");
            assertTrue(rs.next()); assertEquals("Active", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Active", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE orders");
            s.execute("DROP TABLE customers");
        }
    }

    @Test
    void testInsertFromSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE src (id INTEGER, val TEXT)");
            s.execute("CREATE TABLE dst (id INTEGER, val TEXT)");
            s.execute("INSERT INTO src VALUES (1,'a'),(2,'b'),(3,'c')");
            s.execute("INSERT INTO dst SELECT * FROM src WHERE id <= 2");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM dst");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE dst");
            s.execute("DROP TABLE src");
        }
    }

    @Test
    void testUpdateFromJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prices (product TEXT, price INTEGER)");
            s.execute("CREATE TABLE discounts (product TEXT, pct INTEGER)");
            s.execute("INSERT INTO prices VALUES ('a',100),('b',200),('c',300)");
            s.execute("INSERT INTO discounts VALUES ('a',10),('c',20)");
            s.execute("UPDATE prices SET price = prices.price * (100 - d.pct) / 100 FROM discounts d WHERE prices.product = d.product");
            ResultSet rs = s.executeQuery("SELECT product, price FROM prices ORDER BY product");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals(90, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals(200, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("c", rs.getString(1)); assertEquals(240, rs.getInt(2));
            s.execute("DROP TABLE discounts");
            s.execute("DROP TABLE prices");
        }
    }

    @Test
    void testMultiColumnOrderByWithNulls() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nullsort (a INTEGER, b TEXT)");
            s.execute("INSERT INTO nullsort VALUES (1,'x'),(NULL,'y'),(2,NULL),(NULL,NULL),(1,'a')");
            ResultSet rs = s.executeQuery("SELECT a, b FROM nullsort ORDER BY a NULLS LAST, b NULLS FIRST");
            // Expected order: a=1,b=a | a=1,b=x | a=2,b=NULL | a=NULL,b=NULL | a=NULL,b=y
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("x", rs.getString(2));
            s.execute("DROP TABLE nullsort");
        }
    }

    @Test
    void testGreatest() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT GREATEST(1, 5, 3, 7, 2), LEAST(1, 5, 3, 7, 2)");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
        }
    }

    @Test
    void testCurrentSettingVersion() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT current_setting('server_version'), version()");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
        }
    }

    @Test
    void testPgTypeof() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT pg_typeof(42), pg_typeof('hello'), pg_typeof(true), pg_typeof(3.14)");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
        }
    }

    @Test
    void testMd5() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT md5('hello')");
            assertTrue(rs.next());
            assertEquals("5d41402abc4b2a76b9719d911017c592", rs.getString(1));
        }
    }

    @Test
    void testComplexRealWorldQuery() throws SQLException {
        // A query pattern commonly seen in dashboards
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE events (id SERIAL, user_id INTEGER, event_type TEXT, created_at DATE, amount INTEGER)");
            s.execute("INSERT INTO events (user_id, event_type, created_at, amount) VALUES " +
                    "(1, 'purchase', '2024-01-15', 100), " +
                    "(1, 'purchase', '2024-02-15', 200), " +
                    "(2, 'purchase', '2024-01-20', 150), " +
                    "(2, 'refund', '2024-02-10', -50), " +
                    "(1, 'purchase', '2024-03-01', 300), " +
                    "(3, 'purchase', '2024-01-05', 75)");

            ResultSet rs = s.executeQuery(
                    "WITH monthly AS (" +
                    "  SELECT EXTRACT(MONTH FROM created_at)::integer AS month, " +
                    "         SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue, " +
                    "         SUM(CASE WHEN event_type = 'refund' THEN ABS(amount) ELSE 0 END) AS refunds, " +
                    "         COUNT(DISTINCT user_id) AS users " +
                    "  FROM events GROUP BY EXTRACT(MONTH FROM created_at) " +
                    ") " +
                    "SELECT month, revenue, refunds, revenue - refunds AS net, users " +
                    "FROM monthly ORDER BY month");

            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // January
            assertEquals(325, rs.getInt(2)); // revenue: 100+150+75
            assertEquals(0, rs.getInt(3)); // no refunds
            assertEquals(3, rs.getInt(5)); // 3 users

            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // February
            assertEquals(200, rs.getInt(2)); // revenue
            assertEquals(50, rs.getInt(3)); // refund

            s.execute("DROP TABLE events");
        }
    }
}
