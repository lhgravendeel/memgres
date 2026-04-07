package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 7: Type System & Coercion tests.
 * Implicit coercion, date/time arithmetic, INTERVAL, arrays, domains.
 */
class TypeSystemTest {

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

    // ---- Implicit type coercion ----

    @Test
    void testIntegerBigintCoercion() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1 + 2147483648");
            assertTrue(rs.next());
            assertEquals(2147483649L, rs.getLong(1));
        }
    }

    @Test
    void testIntegerDoubleCoercion() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1 + 2.5");
            assertTrue(rs.next());
            assertEquals(3.5, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testStringToIntegerComparison() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE coerce_test (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO coerce_test (id, val) VALUES (42, 'hello')");
            ResultSet rs = stmt.executeQuery("SELECT * FROM coerce_test WHERE id = 42");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("id"));
        }
    }

    @Test
    void testBooleanCoercion() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CAST('true' AS BOOLEAN)");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));

            rs = stmt.executeQuery("SELECT CAST('f' AS BOOLEAN)");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1));
        }
    }

    @Test
    void testNumericCast() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CAST(3.14 AS INTEGER)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testNumericPrecision() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE numeric_test (amount NUMERIC(10,2))");
            stmt.execute("INSERT INTO numeric_test (amount) VALUES (123.456)");
            ResultSet rs = stmt.executeQuery("SELECT amount FROM numeric_test");
            assertTrue(rs.next());
            // Should be rounded to 2 decimal places
            double val = rs.getDouble("amount");
            assertEquals(123.46, val, 0.001);
        }
    }

    // ---- VARCHAR length enforcement ----

    @Test
    void testVarcharLengthValid() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE varchar_test (name VARCHAR(5))");
            stmt.execute("INSERT INTO varchar_test (name) VALUES ('hello')");
            ResultSet rs = stmt.executeQuery("SELECT name FROM varchar_test");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString("name"));
        }
    }

    @Test
    void testVarcharLengthViolation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE varchar_test2 (name VARCHAR(3))");
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO varchar_test2 (name) VALUES ('toolong')"));
        }
    }

    // ---- Date/Time types ----

    @Test
    void testDateLiteral() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DATE '2024-01-15'");
            assertTrue(rs.next());
            assertEquals("2024-01-15", rs.getString(1));
        }
    }

    @Test
    void testDateCast() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CAST('2024-06-15' AS DATE)");
            assertTrue(rs.next());
            assertEquals("2024-06-15", rs.getString(1));
        }
    }

    @Test
    void testTimestampLiteral() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT TIMESTAMP '2024-01-15 10:30:00'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("2024-01-15"));
            assertTrue(val.contains("10:30"));
        }
    }

    @Test
    void testDateColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE date_test (id INTEGER, created DATE)");
            stmt.execute("INSERT INTO date_test (id, created) VALUES (1, '2024-01-15')");
            ResultSet rs = stmt.executeQuery("SELECT created FROM date_test WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("2024-01-15", rs.getString("created"));
        }
    }

    @Test
    void testTimestampColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ts_test (id INTEGER, created TIMESTAMP)");
            stmt.execute("INSERT INTO ts_test (id, created) VALUES (1, '2024-01-15 10:30:00')");
            ResultSet rs = stmt.executeQuery("SELECT created FROM ts_test WHERE id = 1");
            assertTrue(rs.next());
            String val = rs.getString("created");
            assertTrue(val.contains("2024-01-15"));
        }
    }

    @Test
    void testCurrentDateFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_date");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should be a valid date format
            assertTrue(val.matches("\\d{4}-\\d{2}-\\d{2}"), "Expected date format, got: " + val);
        }
    }

    @Test
    void testNowFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT now()");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain a date
            assertTrue(val.matches("\\d{4}-\\d{2}-.*"));
        }
    }

    // ---- Date arithmetic ----

    @Test
    void testDatePlusInteger() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DATE '2024-01-01' + 10");
            assertTrue(rs.next());
            assertEquals("2024-01-11", rs.getString(1));
        }
    }

    @Test
    void testDateMinusInteger() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DATE '2024-01-15' - 5");
            assertTrue(rs.next());
            assertEquals("2024-01-10", rs.getString(1));
        }
    }

    @Test
    void testDateMinusDate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DATE '2024-01-15' - DATE '2024-01-10'");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    // ---- INTERVAL ----

    @Test
    void testIntervalLiteral() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT INTERVAL '1 day'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("1 day"), "Expected '1 day' in: " + val);
        }
    }

    @Test
    void testIntervalArithmetic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT DATE '2024-01-01' + INTERVAL '1 month'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("2024-02-01"), "Expected date in Feb, got: " + val);
        }
    }

    @Test
    void testIntervalMultiply() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT INTERVAL '1 hour' * 3");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("03:00:00"), "Expected 3 hours in: " + val);
        }
    }

    @Test
    void testIntervalAddition() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT INTERVAL '1 day' + INTERVAL '2 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("1 day"), "Expected 1 day in: " + val);
            assertTrue(val.contains("02:00:00"), "Expected 2 hours in: " + val);
        }
    }

    @Test
    void testTimestampPlusInterval() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL '2 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("12:00"), "Expected 12:00 in: " + val);
        }
    }

    @Test
    void testTimestampMinusInterval() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT TIMESTAMP '2024-01-15 10:00:00' - INTERVAL '3 days'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("2024-01-12"), "Expected Jan 12 in: " + val);
        }
    }

    // ---- Date functions ----

    @Test
    void testExtract() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT extract('year' FROM TIMESTAMP '2024-06-15 10:30:00')");
            assertTrue(rs.next());
            assertEquals(2024.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testDatePart() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT date_part('month', TIMESTAMP '2024-06-15 10:30:00')");
            assertTrue(rs.next());
            assertEquals(6.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testDateTrunc() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT date_trunc('month', TIMESTAMP '2024-06-15 10:30:00')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("2024-06-01"), "Expected June 1 in: " + val);
        }
    }

    @Test
    void testMakeDate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT make_date(2024, 3, 15)");
            assertTrue(rs.next());
            assertEquals("2024-03-15", rs.getString(1));
        }
    }

    @Test
    void testAge() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT age(TIMESTAMP '2024-06-15', TIMESTAMP '2024-01-15')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("5 mons"), "Expected 5 months in: " + val);
        }
    }

    // ---- Date comparison ----

    @Test
    void testDateComparison() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE date_cmp (id INTEGER, d DATE)");
            stmt.execute("INSERT INTO date_cmp (id, d) VALUES (1, '2024-01-01')");
            stmt.execute("INSERT INTO date_cmp (id, d) VALUES (2, '2024-06-15')");
            stmt.execute("INSERT INTO date_cmp (id, d) VALUES (3, '2024-12-31')");
            ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM date_cmp WHERE d > '2024-06-01' ORDER BY id");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testDateBetween() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE date_btwn (id INTEGER, d DATE)");
            stmt.execute("INSERT INTO date_btwn (id, d) VALUES (1, '2024-01-01')");
            stmt.execute("INSERT INTO date_btwn (id, d) VALUES (2, '2024-06-15')");
            stmt.execute("INSERT INTO date_btwn (id, d) VALUES (3, '2024-12-31')");
            ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM date_btwn WHERE d BETWEEN '2024-01-01' AND '2024-06-30'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testDateOrderBy() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE date_ord (id INTEGER, d DATE)");
            stmt.execute("INSERT INTO date_ord (id, d) VALUES (1, '2024-12-31')");
            stmt.execute("INSERT INTO date_ord (id, d) VALUES (2, '2024-01-01')");
            stmt.execute("INSERT INTO date_ord (id, d) VALUES (3, '2024-06-15')");
            ResultSet rs = stmt.executeQuery("SELECT id FROM date_ord ORDER BY d");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }
    }

    // ---- Array operations ----

    @Test
    void testArrayConstructor() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ARRAY[1, 2, 3]");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void testArrayLength() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT array_length(ARRAY[1, 2, 3], 1)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testArrayToString() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT array_to_string(ARRAY[1, 2, 3], ',')");
            assertTrue(rs.next());
            assertEquals("1,2,3", rs.getString(1));
        }
    }

    @Test
    void testCardinality() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT cardinality(ARRAY[10, 20, 30, 40])");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    // ---- CREATE DOMAIN ----

    @Test
    void testCreateDomainBasic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DOMAIN positive_int AS INTEGER");
            stmt.execute("CREATE TABLE domain_test (id positive_int, name TEXT)");
            stmt.execute("INSERT INTO domain_test (id, name) VALUES (42, 'test')");
            ResultSet rs = stmt.executeQuery("SELECT id FROM domain_test");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("id"));
        }
    }

    @Test
    void testCreateDomainNotNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DOMAIN nn_text AS TEXT NOT NULL");
            stmt.execute("CREATE TABLE domain_nn_test (val nn_text)");
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO domain_nn_test (val) VALUES (NULL)"));
        }
    }

    @Test
    void testDropDomain() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DOMAIN temp_domain AS TEXT");
            stmt.execute("DROP DOMAIN temp_domain");
            // Should error on re-use
            assertThrows(SQLException.class, () ->
                    stmt.execute("CREATE TABLE drop_test (val temp_domain)"));
        }
    }

    // ---- pg_typeof ----

    @Test
    void testPgTypeofInteger() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pg_typeof(42)");
            assertTrue(rs.next());
            assertEquals("integer", rs.getString(1));
        }
    }

    @Test
    void testPgTypeofText() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pg_typeof('hello')");
            assertTrue(rs.next());
            assertEquals("unknown", rs.getString(1));
        }
    }

    @Test
    void testPgTypeofBoolean() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pg_typeof(true)");
            assertTrue(rs.next());
            assertEquals("boolean", rs.getString(1));
        }
    }

    // ---- Type coercion in INSERT ----

    @Test
    void testInsertStringToInteger() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE coerce_insert (id INTEGER, val BIGINT)");
            // String literal '42' should be coerced to integer
            stmt.execute("INSERT INTO coerce_insert (id, val) VALUES (1, 100)");
            ResultSet rs = stmt.executeQuery("SELECT val FROM coerce_insert WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(100, rs.getLong("val"));
        }
    }

    @Test
    void testInsertDateString() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE coerce_date (id INTEGER, d DATE)");
            stmt.execute("INSERT INTO coerce_date (id, d) VALUES (1, '2024-03-15')");
            ResultSet rs = stmt.executeQuery("SELECT d FROM coerce_date WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("2024-03-15", rs.getString("d"));
        }
    }

    // ---- Mixed-type equality ----

    @Test
    void testIntegerLongEquality() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT CASE WHEN 1 = CAST(1 AS BIGINT) THEN 'yes' ELSE 'no' END");
            assertTrue(rs.next());
            assertEquals("yes", rs.getString(1));
        }
    }

    // ---- to_timestamp ----

    @Test
    void testToTimestamp() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Unix epoch 0 = 1970-01-01
            ResultSet rs = stmt.executeQuery("SELECT to_timestamp(0)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue(val.contains("1970"), "Expected 1970 in: " + val);
        }
    }
}
