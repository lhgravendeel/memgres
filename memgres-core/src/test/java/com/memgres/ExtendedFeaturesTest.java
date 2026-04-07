package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extended feature tests covering bytea, bitwise ops, inet/cidr, partitioning,
 * SIMILAR TO, date formatting, math operations, array operations, and more.
 */
class ExtendedFeaturesTest {

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
    //  1. BYTEA / BYTE BLOBS
    // ================================================================

    @Test
    void testByteaColumnStore() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bytea_test (id INTEGER, data BYTEA)");
            s.execute("INSERT INTO bytea_test VALUES (1, '\\x48656c6c6f')");
            ResultSet rs = s.executeQuery("SELECT data FROM bytea_test WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE bytea_test");
        }
    }

    @Test
    void testByteaOctetLength() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT octet_length('hello'::bytea)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void testByteaConcat() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello'::text || ' world'::text");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
    }

    @Test
    void testEncodeDecode() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT encode('hello'::bytea, 'hex')");
            assertTrue(rs.next());
            assertEquals("68656c6c6f", rs.getString(1));
        }
    }

    @Test
    void testDecodeHex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('68656c6c6f', 'hex')");
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));
        }
    }

    // ================================================================
    //  2. BITWISE OPERATIONS
    // ================================================================

    @Test
    void testBitwiseAnd() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 & 10");
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1)); // 1100 & 1010 = 1000
        }
    }

    @Test
    void testBitwiseOr() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 | 10");
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1)); // 1100 | 1010 = 1110
        }
    }

    @Test
    void testBitwiseXor() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 # 10");
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1)); // 1100 ^ 1010 = 0110
        }
    }

    @Test
    void testBitwiseNot() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ~1");
            assertTrue(rs.next());
            assertEquals(-2, rs.getInt(1)); // ~1 = -2 in two's complement
        }
    }

    @Test
    void testBitwiseShiftLeft() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 << 4");
            assertTrue(rs.next());
            assertEquals(16, rs.getInt(1));
        }
    }

    @Test
    void testBitwiseShiftRight() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 16 >> 2");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void testBitwiseCombined() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (255 & 15) | 240");
            assertTrue(rs.next());
            assertEquals(255, rs.getInt(1)); // (0xFF & 0x0F) | 0xF0 = 0xFF
        }
    }

    @Test
    void testPowerOperator() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // In PG, ^ is power operator
            ResultSet rs = s.executeQuery("SELECT 2 ^ 10");
            assertTrue(rs.next());
            assertEquals(1024, rs.getInt(1));
        }
    }

    // ================================================================
    //  3. INET / CIDR
    // ================================================================

    @Test
    void testInetStore() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_test (id INTEGER, ip INET)");
            s.execute("INSERT INTO inet_test VALUES (1, '192.168.1.1')");
            s.execute("INSERT INTO inet_test VALUES (2, '10.0.0.50')");
            ResultSet rs = s.executeQuery("SELECT ip FROM inet_test WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("192.168.1.1", rs.getString(1));
            s.execute("DROP TABLE inet_test");
        }
    }

    @Test
    void testCidrStore() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cidr_test (id INTEGER, network CIDR)");
            s.execute("INSERT INTO cidr_test VALUES (1, '192.168.1.0/24')");
            ResultSet rs = s.executeQuery("SELECT network FROM cidr_test WHERE id = 1");
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains("192.168.1"));
            s.execute("DROP TABLE cidr_test");
        }
    }

    @Test
    void testInetComparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_cmp (ip INET)");
            s.execute("INSERT INTO inet_cmp VALUES ('192.168.1.1'),('10.0.0.1'),('172.16.0.1')");
            ResultSet rs = s.executeQuery("SELECT ip FROM inet_cmp ORDER BY ip");
            assertTrue(rs.next()); // ordering should work on text comparison at minimum
            int count = 1;
            while (rs.next()) count++;
            assertEquals(3, count);
            s.execute("DROP TABLE inet_cmp");
        }
    }

    // ================================================================
    //  4. PARTITIONING (comprehensive)
    // ================================================================

    @Test
    void testRangePartitionComplete() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sales (id INTEGER, sale_date DATE, amount INTEGER) PARTITION BY RANGE (sale_date)");
            s.execute("CREATE TABLE sales_q1 PARTITION OF sales FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')");
            s.execute("CREATE TABLE sales_q2 PARTITION OF sales FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')");
            s.execute("INSERT INTO sales VALUES (1, '2024-02-15', 100)");
            s.execute("INSERT INTO sales VALUES (2, '2024-05-20', 200)");
            ResultSet rs = s.executeQuery("SELECT SUM(amount) FROM sales");
            assertTrue(rs.next());
            assertEquals(300, rs.getInt(1));
            // Query specific partition
            ResultSet rs2 = s.executeQuery("SELECT amount FROM ONLY sales_q1");
            assertTrue(rs2.next());
            assertEquals(100, rs2.getInt(1));
            s.execute("DROP TABLE sales_q1");
            s.execute("DROP TABLE sales_q2");
            s.execute("DROP TABLE sales");
        }
    }

    @Test
    void testListPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE logs (id INTEGER, region TEXT, msg TEXT) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE logs_us PARTITION OF logs FOR VALUES IN ('us-east', 'us-west')");
            s.execute("CREATE TABLE logs_eu PARTITION OF logs FOR VALUES IN ('eu-west', 'eu-central')");
            s.execute("INSERT INTO logs VALUES (1, 'us-east', 'hello')");
            s.execute("INSERT INTO logs VALUES (2, 'eu-west', 'world')");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM logs");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE logs_us");
            s.execute("DROP TABLE logs_eu");
            s.execute("DROP TABLE logs");
        }
    }

    @Test
    void testHashPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE hash_data (id INTEGER, val TEXT) PARTITION BY HASH (id)");
            s.execute("CREATE TABLE hash_data_0 PARTITION OF hash_data FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            s.execute("CREATE TABLE hash_data_1 PARTITION OF hash_data FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
            s.execute("INSERT INTO hash_data VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM hash_data");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            s.execute("DROP TABLE hash_data_0");
            s.execute("DROP TABLE hash_data_1");
            s.execute("DROP TABLE hash_data");
        }
    }

    @Test
    void testPartitionNoMatchThrows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE strict_part (id INTEGER, region TEXT) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE strict_us PARTITION OF strict_part FOR VALUES IN ('us')");
            // Insert data that doesn't match any partition should error
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO strict_part VALUES (1, 'eu')"));
            s.execute("DROP TABLE strict_us");
            s.execute("DROP TABLE strict_part");
        }
    }

    @Test
    void testDefaultPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE def_part (id INTEGER, cat TEXT) PARTITION BY LIST (cat)");
            s.execute("CREATE TABLE def_part_a PARTITION OF def_part FOR VALUES IN ('a')");
            s.execute("CREATE TABLE def_part_default PARTITION OF def_part DEFAULT");
            s.execute("INSERT INTO def_part VALUES (1, 'a')");
            s.execute("INSERT INTO def_part VALUES (2, 'b')"); // goes to default
            s.execute("INSERT INTO def_part VALUES (3, 'c')"); // goes to default
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM def_part");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            ResultSet rs2 = s.executeQuery("SELECT COUNT(*) FROM ONLY def_part_default");
            assertTrue(rs2.next());
            assertEquals(2, rs2.getInt(1));
            s.execute("DROP TABLE def_part_a");
            s.execute("DROP TABLE def_part_default");
            s.execute("DROP TABLE def_part");
        }
    }

    // ================================================================
    //  5. SIMILAR TO
    // ================================================================

    @Test
    void testSimilarToBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sim_test (val TEXT)");
            s.execute("INSERT INTO sim_test VALUES ('abc'),('aXc'),('def'),('abbc')");
            ResultSet rs = s.executeQuery("SELECT val FROM sim_test WHERE val SIMILAR TO 'a%c' ORDER BY val");
            assertTrue(rs.next()); assertEquals("aXc", rs.getString(1));
            assertTrue(rs.next()); assertEquals("abbc", rs.getString(1));
            assertTrue(rs.next()); assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE sim_test");
        }
    }

    @Test
    void testSimilarToAlternation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sim_alt (val TEXT)");
            s.execute("INSERT INTO sim_alt VALUES ('cat'),('dog'),('car'),('bat')");
            ResultSet rs = s.executeQuery("SELECT val FROM sim_alt WHERE val SIMILAR TO '(cat|dog)' ORDER BY val");
            assertTrue(rs.next()); assertEquals("cat", rs.getString(1));
            assertTrue(rs.next()); assertEquals("dog", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE sim_alt");
        }
    }

    @Test
    void testNotSimilarTo() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nsim (val TEXT)");
            s.execute("INSERT INTO nsim VALUES ('abc'),('def'),('ghi')");
            ResultSet rs = s.executeQuery("SELECT val FROM nsim WHERE val NOT SIMILAR TO 'a%' ORDER BY val");
            assertTrue(rs.next()); assertEquals("def", rs.getString(1));
            assertTrue(rs.next()); assertEquals("ghi", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE nsim");
        }
    }

    // ================================================================
    //  6. LIKE/ILIKE patterns
    // ================================================================

    @Test
    void testLikePercentMiddle() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE like_mid (val TEXT)");
            s.execute("INSERT INTO like_mid VALUES ('foobar'),('foobaz'),('foo'),('bar')");
            ResultSet rs = s.executeQuery("SELECT val FROM like_mid WHERE val LIKE 'foo%' ORDER BY val");
            assertTrue(rs.next()); assertEquals("foo", rs.getString(1));
            assertTrue(rs.next()); assertEquals("foobar", rs.getString(1));
            assertTrue(rs.next()); assertEquals("foobaz", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE like_mid");
        }
    }

    @Test
    void testIlikePattern() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ilike_p (val TEXT)");
            s.execute("INSERT INTO ilike_p VALUES ('Hello'),('HELLO'),('world')");
            ResultSet rs = s.executeQuery("SELECT val FROM ilike_p WHERE val ILIKE '%ello' ORDER BY val");
            assertTrue(rs.next()); assertEquals("HELLO", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Hello", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE ilike_p");
        }
    }

    @Test
    void testLikeUnderscore() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'abc' LIKE 'a_c'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    @Test
    void testLikeExact() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'abc' LIKE 'abc'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    @Test
    void testNotLike() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'abc' NOT LIKE 'xyz'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    // ================================================================
    //  7. TIMESTAMPS, to_char, to_date, to_timestamp, to_number
    // ================================================================

    @Test
    void testToCharDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-03-15', 'YYYY-MM-DD')");
            assertTrue(rs.next());
            assertEquals("2024-03-15", rs.getString(1));
        }
    }

    @Test
    void testToCharDayMonth() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-03-15', 'DD/MM/YYYY')");
            assertTrue(rs.next());
            assertEquals("15/03/2024", rs.getString(1));
        }
    }

    @Test
    void testToCharMonthName() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-01-15', 'Month')");
            assertTrue(rs.next());
            assertTrue(rs.getString(1).trim().startsWith("January") || rs.getString(1).trim().startsWith("january"));
        }
    }

    @Test
    void testToCharDayName() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-03-15', 'Day')");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1)); // Should return the day name (Friday)
        }
    }

    @Test
    void testToDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_date('15-03-2024', 'DD-MM-YYYY')");
            assertTrue(rs.next());
            assertEquals("2024-03-15", rs.getString(1));
        }
    }

    @Test
    void testToTimestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_timestamp(0)");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1)); // Unix epoch
        }
    }

    @Test
    void testToNumber() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_number('12,345.67', '99G999D99')");
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(12345.67, val, 0.01);
        }
    }

    @Test
    void testDateTruncMonth() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT date_trunc('month', TIMESTAMP '2024-03-15 14:30:00')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("2024-03-01"), "Expected month truncation, got: " + result);
        }
    }

    @Test
    void testDateTruncYear() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT date_trunc('year', DATE '2024-06-15')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("2024-01-01"), "Expected year truncation, got: " + result);
        }
    }

    @Test
    void testDateTruncDay() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT date_trunc('day', TIMESTAMP '2024-03-15 14:30:45')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("2024-03-15"), "Expected day truncation, got: " + result);
        }
    }

    @Test
    void testDateTruncHour() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT date_trunc('hour', TIMESTAMP '2024-03-15 14:30:45')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("14:00:00") || result.contains("14:00"), "Expected hour truncation, got: " + result);
        }
    }

    @Test
    void testExtractFields() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(YEAR FROM DATE '2024-03-15'), " +
                    "EXTRACT(MONTH FROM DATE '2024-03-15'), " +
                    "EXTRACT(DAY FROM DATE '2024-03-15')");
            assertTrue(rs.next());
            assertEquals(2024, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(15, rs.getInt(3));
        }
    }

    @Test
    void testDateArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-03-15' + 10");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-03-25"), "Expected 10 days added, got: " + result);
        }
    }

    @Test
    void testCurrentDateFunctions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT current_date, current_timestamp, now()");
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));
            assertNotNull(rs.getObject(2));
            assertNotNull(rs.getObject(3));
        }
    }

    // ================================================================
    //  8. MATH OPERATIONS
    // ================================================================

    @Test
    void testMathAbs() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT abs(-42), abs(42)");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals(42, rs.getInt(2));
        }
    }

    @Test
    void testMathCeil() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ceil(4.2), ceil(-4.2)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(-4, rs.getInt(2));
        }
    }

    @Test
    void testMathFloor() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT floor(4.8), floor(-4.2)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals(-5, rs.getInt(2));
        }
    }

    @Test
    void testMathRound() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT round(4.5), round(4.4), round(3.14159, 2)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(3.14, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testMathTrunc() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(4.8), trunc(-4.8), trunc(3.14159, 2)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals(-4, rs.getInt(2));
            assertEquals(3.14, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testMathMod() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT mod(10, 3), mod(10, 5)");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        }
    }

    @Test
    void testMathPower() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT power(2, 10), power(3, 3)");
            assertTrue(rs.next());
            assertEquals(1024.0, rs.getDouble(1), 0.001);
            assertEquals(27.0, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void testMathSqrtCbrt() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT sqrt(144), cbrt(27)");
            assertTrue(rs.next());
            assertEquals(12.0, rs.getDouble(1), 0.001);
            assertEquals(3.0, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void testMathLogLnExp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ln(1), exp(0), log(100)");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), 0.001);
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertEquals(2.0, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testMathSign() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT sign(-5), sign(0), sign(5)");
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
        }
    }

    @Test
    void testMathTrig() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT sin(0), cos(0), tan(0)");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), 0.001);
            assertEquals(1.0, rs.getDouble(2), 0.001);
            assertEquals(0.0, rs.getDouble(3), 0.001);
        }
    }

    @Test
    void testMathPi() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT pi()");
            assertTrue(rs.next());
            assertEquals(Math.PI, rs.getDouble(1), 0.0001);
        }
    }

    @Test
    void testMathDegreesRadians() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT degrees(pi()), radians(180)");
            assertTrue(rs.next());
            assertEquals(180.0, rs.getDouble(1), 0.001);
            assertEquals(Math.PI, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void testMathDivGcdLcm() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT div(7, 2), gcd(12, 8), lcm(4, 6)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(12, rs.getInt(3));
        }
    }

    @Test
    void testUnaryPlus() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT +42, +(-5)");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals(-5, rs.getInt(2));
        }
    }

    @Test
    void testIntegerDivision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 17 / 5, 17 % 5");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
        }
    }

    @Test
    void testCompositeArithmeticPrecedence() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 2 + 3 * 4, (2 + 3) * 4");
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1)); // 2 + 12
            assertEquals(20, rs.getInt(2)); // 5 * 4
        }
    }

    // ================================================================
    //  9. COMPOSITE BOOLEAN COMPARISONS
    // ================================================================

    @Test
    void testNestedAndOr() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bool_nest (a INTEGER, b INTEGER, c INTEGER)");
            s.execute("INSERT INTO bool_nest VALUES (1,2,3),(4,5,6),(7,8,9),(1,5,9)");
            ResultSet rs = s.executeQuery(
                    "SELECT a FROM bool_nest WHERE (a = 1 AND (b = 2 OR c = 9)) ORDER BY a, b");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE bool_nest");
        }
    }

    @Test
    void testNotWithAndOr() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NOT (TRUE AND FALSE), NOT TRUE OR TRUE");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1)); // NOT FALSE = TRUE
            assertEquals("t", rs.getString(2)); // (NOT TRUE) OR TRUE = FALSE OR TRUE = TRUE
        }
    }

    @Test
    void testBooleanPrecedence() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // AND binds tighter than OR
            ResultSet rs = s.executeQuery("SELECT TRUE OR FALSE AND FALSE");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1)); // TRUE OR (FALSE AND FALSE) = TRUE
        }
    }

    // ================================================================
    //  10. ARRAY OPERATIONS
    // ================================================================

    @Test
    void testArrayLength() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_length(ARRAY[1,2,3,4,5], 1)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void testArrayToString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_to_string(ARRAY['a','b','c'], ', ')");
            assertTrue(rs.next());
            assertEquals("a, b, c", rs.getString(1));
        }
    }

    @Test
    void testStringToArray() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_length(string_to_array('a,b,c,d', ','), 1)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void testCardinality() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT cardinality(ARRAY[10,20,30])");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testArrayCat() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_cat(ARRAY[1,2], ARRAY[3,4])");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // Should contain all 4 elements
            assertTrue(result.contains("1") && result.contains("4"), "Got: " + result);
        }
    }

    @Test
    void testArrayAppend() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_append(ARRAY[1,2,3], 4)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertTrue(result.contains("4"), "Should contain appended element, got: " + result);
        }
    }

    @Test
    void testArrayPrepend() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_prepend(0, ARRAY[1,2,3])");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertTrue(result.contains("0"), "Should contain prepended element, got: " + result);
        }
    }

    @Test
    void testArrayRemove() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_remove(ARRAY[1,2,3,2,1], 2)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertFalse(result.contains("2"), "Should not contain removed element, got: " + result);
        }
    }

    @Test
    void testArrayPosition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_position(ARRAY['a','b','c','d'], 'c')");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // 1-based
        }
    }

    @Test
    void testArrayReplace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT array_replace(ARRAY[1,2,3,2], 2, 9)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertFalse(result.contains("2"), "Should not contain old value, got: " + result);
            assertTrue(result.contains("9"), "Should contain new value, got: " + result);
        }
    }

    @Test
    void testUnnest() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT * FROM unnest(ARRAY[10,20,30]) AS t(val)");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    // ================================================================
    //  11. GEOMETRIC TYPES (basic support)
    // ================================================================

    @Test
    void testPointType() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE geo_points (id INTEGER, location POINT)");
            s.execute("INSERT INTO geo_points VALUES (1, '(1.5, 2.5)')");
            s.execute("INSERT INTO geo_points VALUES (2, '(3.0, 4.0)')");
            ResultSet rs = s.executeQuery("SELECT location FROM geo_points WHERE id = 1");
            assertTrue(rs.next());
            String loc = rs.getString(1);
            assertNotNull(loc);
            assertTrue(loc.contains("1.5") && loc.contains("2.5"), "Got: " + loc);
            s.execute("DROP TABLE geo_points");
        }
    }

    @Test
    void testBoxType() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE geo_boxes (id INTEGER, area BOX)");
            s.execute("INSERT INTO geo_boxes VALUES (1, '((0,0),(1,1))')");
            ResultSet rs = s.executeQuery("SELECT area FROM geo_boxes WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE geo_boxes");
        }
    }

    @Test
    void testCircleType() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE geo_circles (id INTEGER, c CIRCLE)");
            s.execute("INSERT INTO geo_circles VALUES (1, '<(0,0),5>')");
            ResultSet rs = s.executeQuery("SELECT c FROM geo_circles WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE geo_circles");
        }
    }

    // ================================================================
    //  12. TO_CHAR FORMAT OPTIONS
    // ================================================================

    @Test
    void testToCharYYYY() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-07-04', 'YYYY')");
            assertTrue(rs.next());
            assertEquals("2024", rs.getString(1));
        }
    }

    @Test
    void testToCharMM() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-07-04', 'MM')");
            assertTrue(rs.next());
            assertEquals("07", rs.getString(1));
        }
    }

    @Test
    void testToCharDD() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-07-04', 'DD')");
            assertTrue(rs.next());
            assertEquals("04", rs.getString(1));
        }
    }

    @Test
    void testToCharHH24MISS() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(TIMESTAMP '2024-07-04 15:30:45', 'HH24:MI:SS')");
            assertTrue(rs.next());
            assertEquals("15:30:45", rs.getString(1));
        }
    }

    @Test
    void testToCharDayOfWeek() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-07-04', 'D')");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1)); // Day of week number
        }
    }

    @Test
    void testToCharQuarter() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-07-04', 'Q')");
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1)); // July = Q3
        }
    }

    // ================================================================
    //  13. ADDITIONAL DATE OPERATIONS
    // ================================================================

    @Test
    void testAgeFunctionDates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT age(DATE '2024-06-15', DATE '2024-01-01')");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // Should show something like "5 mons 14 days"
        }
    }

    @Test
    void testDatePartFunction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT date_part('year', DATE '2024-03-15')");
            assertTrue(rs.next());
            assertEquals(2024.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void testMakeDate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT make_date(2024, 12, 25)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("2024-12-25"), "Got: " + result);
        }
    }

    // ================================================================
    //  14. ADDITIONAL PARTITION SCENARIOS
    // ================================================================

    @Test
    void testDetachPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dp (id INTEGER, cat TEXT) PARTITION BY LIST (cat)");
            s.execute("CREATE TABLE dp_a PARTITION OF dp FOR VALUES IN ('a')");
            s.execute("CREATE TABLE dp_b PARTITION OF dp FOR VALUES IN ('b')");
            s.execute("INSERT INTO dp VALUES (1, 'a'), (2, 'b')");
            // Detach partition
            s.execute("ALTER TABLE dp DETACH PARTITION dp_b");
            // Parent should only have partition a data
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM dp");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Detached table should still exist standalone
            ResultSet rs2 = s.executeQuery("SELECT COUNT(*) FROM dp_b");
            assertTrue(rs2.next());
            assertEquals(1, rs2.getInt(1));
            s.execute("DROP TABLE dp_a");
            s.execute("DROP TABLE dp_b");
            s.execute("DROP TABLE dp");
        }
    }

    @Test
    void testAttachPartition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ap (id INTEGER, cat TEXT) PARTITION BY LIST (cat)");
            s.execute("CREATE TABLE ap_a PARTITION OF ap FOR VALUES IN ('a')");
            // Create standalone table
            s.execute("CREATE TABLE ap_b (id INTEGER, cat TEXT)");
            s.execute("INSERT INTO ap_b VALUES (1, 'b')");
            // Attach as partition
            s.execute("ALTER TABLE ap ATTACH PARTITION ap_b FOR VALUES IN ('b')");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ap");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // should include ap_b data
            s.execute("DROP TABLE ap_a");
            s.execute("DROP TABLE ap_b");
            s.execute("DROP TABLE ap");
        }
    }
}
