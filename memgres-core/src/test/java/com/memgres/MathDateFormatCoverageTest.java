package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class MathDateFormatCoverageTest {
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

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private double queryDouble(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getDouble(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private long queryLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    // ========================================================================
    // Item 51: Mathematical Operators & Functions
    // ========================================================================

    @Test void testAbsNegativeInt() throws SQLException {
        assertEquals(5, queryInt("SELECT abs(-5)"));
    }

    @Test void testAbsPositiveInt() throws SQLException {
        assertEquals(5, queryInt("SELECT abs(5)"));
    }

    @Test void testAbsNegativeDecimal() throws SQLException {
        assertEquals(3.14, queryDouble("SELECT abs(-3.14)"), 0.001);
    }

    @Test void testCeil() throws SQLException {
        assertEquals(5, queryInt("SELECT ceil(4.1)"));
    }

    @Test void testCeilingNegative() throws SQLException {
        assertEquals(-4, queryInt("SELECT ceiling(-4.1)"));
    }

    @Test void testFloorPositive() throws SQLException {
        assertEquals(4, queryInt("SELECT floor(4.9)"));
    }

    @Test void testFloorNegative() throws SQLException {
        assertEquals(-5, queryInt("SELECT floor(-4.1)"));
    }

    @Test void testRoundNoScale() throws SQLException {
        assertEquals(5, queryInt("SELECT round(4.5)"));
    }

    @Test void testRoundWithScale() throws SQLException {
        assertEquals(3.14, queryDouble("SELECT round(3.14159, 2)"), 0.001);
    }

    @Test void testTruncNoScale() throws SQLException {
        assertEquals(4, queryInt("SELECT trunc(4.9)"));
    }

    @Test void testTruncWithScale() throws SQLException {
        assertEquals(3.14, queryDouble("SELECT trunc(3.14159, 2)"), 0.001);
    }

    @Test void testSignNegative() throws SQLException {
        assertEquals(-1, queryInt("SELECT sign(-5)"));
    }

    @Test void testSignZero() throws SQLException {
        assertEquals(0, queryInt("SELECT sign(0)"));
    }

    @Test void testSignPositive() throws SQLException {
        assertEquals(1, queryInt("SELECT sign(5)"));
    }

    @Test void testMod() throws SQLException {
        assertEquals(1, queryInt("SELECT mod(9, 4)"));
    }

    @Test void testDiv() throws SQLException {
        assertEquals(2, queryInt("SELECT div(9, 4)"));
    }

    @Test void testPower() throws SQLException {
        assertEquals(1024.0, queryDouble("SELECT power(2, 10)"), 0.001);
    }

    @Test void testSqrt() throws SQLException {
        assertEquals(4.0, queryDouble("SELECT sqrt(16)"), 0.001);
    }

    @Test void testCbrt() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT cbrt(27)"), 0.001);
    }

    @Test void testExp() throws SQLException {
        assertEquals(2.718, queryDouble("SELECT exp(1)"), 0.01);
    }

    @Test void testLnOfExp() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT ln(exp(1))"), 0.001);
    }

    @Test void testLog10() throws SQLException {
        assertEquals(2.0, queryDouble("SELECT log(100)"), 0.001);
    }

    @Test void testLogArbitraryBase() throws SQLException {
        assertEquals(6.0, queryDouble("SELECT log(2, 64)"), 0.001);
    }

    @Test void testFactorial5() throws SQLException {
        assertEquals(120, queryLong("SELECT factorial(5)"));
    }

    @Test void testFactorial0() throws SQLException {
        assertEquals(1, queryLong("SELECT factorial(0)"));
    }

    @Test void testPi() throws SQLException {
        assertEquals(3.14159, queryDouble("SELECT pi()"), 0.001);
    }

    @Test void testDegrees() throws SQLException {
        assertEquals(180.0, queryDouble("SELECT degrees(pi())"), 0.001);
    }

    @Test void testRadians() throws SQLException {
        assertEquals(3.14159, queryDouble("SELECT radians(180)"), 0.001);
    }

    @Test void testRandom() throws SQLException {
        double val = queryDouble("SELECT random()");
        assertTrue(val >= 0.0 && val < 1.0, "random() should be in [0, 1)");
    }

    @Test void testSetseed() throws SQLException {
        // setseed returns empty/void; just ensure it doesn't error
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT setseed(0.5)");
        }
    }

    @Test void testWidthBucketMiddle() throws SQLException {
        assertEquals(3, queryInt("SELECT width_bucket(5.0, 0.0, 10.0, 5)"));
    }

    @Test void testWidthBucketLow() throws SQLException {
        assertEquals(1, queryInt("SELECT width_bucket(0, 0, 10, 5)"));
    }

    @Test void testWidthBucketOverflow() throws SQLException {
        assertEquals(6, queryInt("SELECT width_bucket(10, 0, 10, 5)"));
    }

    @Test void testGcd() throws SQLException {
        assertEquals(4, queryInt("SELECT gcd(12, 8)"));
    }

    @Test void testLcm() throws SQLException {
        assertEquals(24, queryInt("SELECT lcm(12, 8)"));
    }

    @Test void testPowerOperator() throws SQLException {
        assertEquals(8.0, queryDouble("SELECT 2 ^ 3"), 0.001);
    }

    @Test void testModuloOperator() throws SQLException {
        assertEquals(1, queryInt("SELECT 9 % 4"));
    }

    // ========================================================================
    // Item 52: Trigonometric Functions
    // ========================================================================

    @Test void testSinZero() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT sin(0)"), 0.001);
    }

    @Test void testSinPiOver2() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT sin(pi()/2)"), 0.001);
    }

    @Test void testCosZero() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT cos(0)"), 0.001);
    }

    @Test void testCosPi() throws SQLException {
        assertEquals(-1.0, queryDouble("SELECT cos(pi())"), 0.001);
    }

    @Test void testTanZero() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT tan(0)"), 0.001);
    }

    @Test void testAsin() throws SQLException {
        assertEquals(1.5708, queryDouble("SELECT asin(1)"), 0.001);
    }

    @Test void testAcos() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT acos(1)"), 0.001);
    }

    @Test void testAtan() throws SQLException {
        assertEquals(0.7854, queryDouble("SELECT atan(1)"), 0.001);
    }

    @Test void testAtan2() throws SQLException {
        assertEquals(0.7854, queryDouble("SELECT atan2(1, 1)"), 0.001);
    }

    @Test void testSind() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT sind(90)"), 0.001);
    }

    @Test void testCosdZero() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT cosd(0)"), 0.001);
    }

    @Test void testCosd180() throws SQLException {
        assertEquals(-1.0, queryDouble("SELECT cosd(180)"), 0.001);
    }

    @Test void testTand() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT tand(45)"), 0.001);
    }

    @Test void testAsind() throws SQLException {
        assertEquals(90.0, queryDouble("SELECT asind(1)"), 0.001);
    }

    @Test void testAcosd() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT acosd(1)"), 0.001);
    }

    @Test void testAtand() throws SQLException {
        assertEquals(45.0, queryDouble("SELECT atand(1)"), 0.001);
    }

    @Test void testAtand2() throws SQLException {
        assertEquals(45.0, queryDouble("SELECT atan2d(1, 1)"), 0.001);
    }

    @Test void testSinh() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT sinh(0)"), 0.001);
    }

    @Test void testCosh() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT cosh(0)"), 0.001);
    }

    @Test void testTanh() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT tanh(0)"), 0.001);
    }

    @Test void testAsinh() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT asinh(0)"), 0.001);
    }

    @Test void testAcosh() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT acosh(1)"), 0.001);
    }

    @Test void testAtanh() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT atanh(0)"), 0.001);
    }

    // ========================================================================
    // Item 53: Date/Time Functions
    // ========================================================================

    @Test void testAgeOneYear() throws SQLException {
        String result = query1("SELECT age(timestamp '2024-01-15', timestamp '2023-01-15')");
        assertTrue(result.contains("1 year"), "Expected '1 year' but got: " + result);
    }

    @Test void testAgeFiveMonths() throws SQLException {
        String result = query1("SELECT age(timestamp '2024-06-15', timestamp '2024-01-15')");
        assertTrue(result.contains("5 mon"), "Expected '5 mons' but got: " + result);
    }

    @Test void testClockTimestamp() throws SQLException {
        assertTrue(queryBool("SELECT clock_timestamp() IS NOT NULL"));
    }

    @Test void testDatePartYear() throws SQLException {
        assertEquals(2024.0, queryDouble("SELECT date_part('year', timestamp '2024-06-15 14:30:00')"), 0.001);
    }

    @Test void testDatePartMonth() throws SQLException {
        assertEquals(6.0, queryDouble("SELECT date_part('month', timestamp '2024-06-15')"), 0.001);
    }

    @Test void testDatePartDay() throws SQLException {
        assertEquals(15.0, queryDouble("SELECT date_part('day', timestamp '2024-06-15')"), 0.001);
    }

    @Test void testDatePartHour() throws SQLException {
        assertEquals(14.0, queryDouble("SELECT date_part('hour', timestamp '2024-06-15 14:30:00')"), 0.001);
    }

    @Test void testDatePartMinute() throws SQLException {
        assertEquals(30.0, queryDouble("SELECT date_part('minute', timestamp '2024-06-15 14:30:00')"), 0.001);
    }

    @Test void testDatePartSecond() throws SQLException {
        assertEquals(45.0, queryDouble("SELECT date_part('second', timestamp '2024-06-15 14:30:45')"), 0.001);
    }

    @Test void testDatePartDow() throws SQLException {
        // 2024-06-15 is a Saturday, dow=6
        double dow = queryDouble("SELECT date_part('dow', timestamp '2024-06-15')");
        assertEquals(6.0, dow, 0.001);
    }

    @Test void testDatePartDoy() throws SQLException {
        assertEquals(15.0, queryDouble("SELECT date_part('doy', timestamp '2024-01-15')"), 0.001);
    }

    @Test void testDatePartQuarter() throws SQLException {
        assertEquals(2.0, queryDouble("SELECT date_part('quarter', timestamp '2024-06-15')"), 0.001);
    }

    @Test void testDatePartWeek() throws SQLException {
        double week = queryDouble("SELECT date_part('week', timestamp '2024-06-15')");
        assertTrue(week > 0 && week <= 53, "Week should be between 1 and 53, got: " + week);
    }

    @Test void testDatePartEpoch() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT date_part('epoch', timestamp '1970-01-01 00:00:00')"), 0.001);
    }

    @Test void testExtractYear() throws SQLException {
        assertEquals(2024.0, queryDouble("SELECT EXTRACT(year FROM timestamp '2024-06-15')"), 0.001);
    }

    @Test void testExtractMonth() throws SQLException {
        assertEquals(6.0, queryDouble("SELECT EXTRACT(month FROM timestamp '2024-06-15')"), 0.001);
    }

    @Test void testExtractEpochFromInterval() throws SQLException {
        assertEquals(86400.0, queryDouble("SELECT EXTRACT(epoch FROM interval '1 day')"), 0.001);
    }

    @Test void testDateTruncMonth() throws SQLException {
        String result = query1("SELECT date_trunc('month', timestamp '2024-06-15 14:30:00')");
        assertTrue(result.startsWith("2024-06-01"), "Expected start with '2024-06-01' but got: " + result);
    }

    @Test void testDateTruncYear() throws SQLException {
        String result = query1("SELECT date_trunc('year', timestamp '2024-06-15')");
        assertTrue(result.startsWith("2024-01-01"), "Expected start with '2024-01-01' but got: " + result);
    }

    @Test void testDateTruncHour() throws SQLException {
        String result = query1("SELECT date_trunc('hour', timestamp '2024-06-15 14:30:45')");
        assertTrue(result.contains("14:00:00"), "Expected '14:00:00' but got: " + result);
    }

    @Test void testDateTruncDay() throws SQLException {
        String result = query1("SELECT date_trunc('day', timestamp '2024-06-15 14:30:00')");
        assertTrue(result.startsWith("2024-06-15"), "Expected start with '2024-06-15' but got: " + result);
    }

    @Test void testDateBin15Min() throws SQLException {
        String result = query1("SELECT date_bin(interval '15 minutes', timestamp '2024-06-15 14:22:00', timestamp '2024-06-15 00:00:00')");
        assertTrue(result.contains("14:15:00"), "Expected '14:15:00' but got: " + result);
    }

    @Test void testDateBin1Hour() throws SQLException {
        String result = query1("SELECT date_bin(interval '1 hour', timestamp '2024-06-15 14:22:00', timestamp '2024-06-15 00:00:00')");
        assertTrue(result.contains("14:00:00"), "Expected '14:00:00' but got: " + result);
    }

    @Test void testIsfiniteTimestamp() throws SQLException {
        assertTrue(queryBool("SELECT isfinite(timestamp '2024-01-01')"));
    }

    @Test void testIsfiniteDate() throws SQLException {
        assertTrue(queryBool("SELECT isfinite(date '2024-01-01')"));
    }

    @Test void testIsfiniteInterval() throws SQLException {
        assertTrue(queryBool("SELECT isfinite(interval '1 day')"));
    }

    @Test void testMakeDate() throws SQLException {
        String result = query1("SELECT make_date(2024, 6, 15)");
        assertEquals("2024-06-15", result);
    }

    @Test void testMakeTime() throws SQLException {
        String result = query1("SELECT make_time(14, 30, 45)");
        assertEquals("14:30:45", result);
    }

    @Test void testMakeTimestamp() throws SQLException {
        String result = query1("SELECT make_timestamp(2024, 6, 15, 14, 30, 0)");
        assertTrue(result.startsWith("2024-06-15") && result.contains("14:30:00"),
                "Expected '2024-06-15 14:30:00' but got: " + result);
    }

    @Test void testMakeTimestamptz() throws SQLException {
        String result = query1("SELECT make_timestamptz(2024, 6, 15, 14, 30, 0, 'UTC')");
        assertNotNull(result);
        assertTrue(result.contains("2024-06-15"), "Expected date in result but got: " + result);
    }

    @Test void testMakeIntervalYearsMonths() throws SQLException {
        String result = query1("SELECT make_interval(years => 1, months => 2)");
        assertTrue(result.contains("1 year") && result.contains("2 mon"),
                "Expected '1 year 2 mons' but got: " + result);
    }

    @Test void testMakeIntervalDays() throws SQLException {
        String result = query1("SELECT make_interval(days => 10)");
        assertTrue(result.contains("10 day"), "Expected '10 days' but got: " + result);
    }

    @Test void testStatementTimestamp() throws SQLException {
        assertTrue(queryBool("SELECT statement_timestamp() IS NOT NULL"));
    }

    @Test void testTransactionTimestamp() throws SQLException {
        assertTrue(queryBool("SELECT transaction_timestamp() IS NOT NULL"));
    }

    @Test void testTimeofday() throws SQLException {
        String result = query1("SELECT timeofday()");
        assertNotNull(result);
        assertTrue(result.length() > 0, "timeofday() should return a non-empty string");
    }

    @Test void testJustifyHours() throws SQLException {
        String result = query1("SELECT justify_hours(interval '30 hours')");
        assertTrue(result.contains("1 day") && result.contains("06:00:00"),
                "Expected '1 day 06:00:00' but got: " + result);
    }

    @Test void testJustifyDays() throws SQLException {
        String result = query1("SELECT justify_days(interval '40 days')");
        assertTrue(result.contains("1 mon") && result.contains("10 day"),
                "Expected '1 mon 10 days' but got: " + result);
    }

    @Test void testJustifyInterval() throws SQLException {
        String result = query1("SELECT justify_interval(interval '1 mon -1 hour')");
        assertTrue(result.contains("29 day") && result.contains("23:00:00"),
                "Expected '29 days 23:00:00' but got: " + result);
    }

    @Test void testNow() throws SQLException {
        String result = query1("SELECT now()");
        assertNotNull(result);
    }

    @Test void testCurrentDate() throws SQLException {
        assertTrue(queryBool("SELECT CURRENT_DATE IS NOT NULL"));
    }

    @Test void testCurrentTime() throws SQLException {
        assertTrue(queryBool("SELECT CURRENT_TIME IS NOT NULL"));
    }

    @Test void testCurrentTimestamp() throws SQLException {
        assertTrue(queryBool("SELECT CURRENT_TIMESTAMP IS NOT NULL"));
    }

    // ========================================================================
    // Item 54: Date/Time Formatting
    // ========================================================================

    @Test void testToCharTimestampYMD() throws SQLException {
        assertEquals("2024-06-15",
                query1("SELECT to_char(timestamp '2024-06-15 14:30:00', 'YYYY-MM-DD')"));
    }

    @Test void testToCharTimestampHMS() throws SQLException {
        assertEquals("14:30:00",
                query1("SELECT to_char(timestamp '2024-06-15 14:30:00', 'HH24:MI:SS')"));
    }

    @Test void testToCharYear() throws SQLException {
        assertEquals("2024",
                query1("SELECT to_char(timestamp '2024-06-15', 'YYYY')"));
    }

    @Test void testToCharMonth2Digit() throws SQLException {
        assertEquals("06",
                query1("SELECT to_char(timestamp '2024-06-15', 'MM')"));
    }

    @Test void testToCharDay2Digit() throws SQLException {
        assertEquals("15",
                query1("SELECT to_char(timestamp '2024-06-15', 'DD')"));
    }

    @Test void testToCharDayName() throws SQLException {
        String result = query1("SELECT to_char(timestamp '2024-06-15', 'Day')");
        assertTrue(result.trim().equals("Saturday"),
                "Expected 'Saturday' but got: '" + result + "'");
    }

    @Test void testToCharDayAbbrev() throws SQLException {
        String result = query1("SELECT to_char(timestamp '2024-06-15', 'Dy')");
        assertTrue(result.trim().equals("Sat"),
                "Expected 'Sat' but got: '" + result + "'");
    }

    @Test void testToCharMonthName() throws SQLException {
        String result = query1("SELECT to_char(timestamp '2024-06-15', 'Month')");
        assertTrue(result.trim().equals("June"),
                "Expected 'June' but got: '" + result + "'");
    }

    @Test void testToCharMonthAbbrev() throws SQLException {
        String result = query1("SELECT to_char(timestamp '2024-06-15', 'Mon')");
        assertTrue(result.trim().equals("Jun"),
                "Expected 'Jun' but got: '" + result + "'");
    }

    @Test void testToCharHH12AM() throws SQLException {
        String result = query1("SELECT to_char(timestamp '2024-06-15 14:30:00', 'HH12:MI AM')");
        assertTrue(result.contains("02") && result.contains("30") && result.contains("PM"),
                "Expected '02:30 PM' but got: '" + result + "'");
    }

    @Test void testToCharQuarter() throws SQLException {
        assertEquals("2",
                query1("SELECT to_char(timestamp '2024-06-15', 'Q')"));
    }

    @Test void testToDateYMD() throws SQLException {
        String result = query1("SELECT to_date('2024-06-15', 'YYYY-MM-DD')");
        assertEquals("2024-06-15", result);
    }

    @Test void testToDateDMY() throws SQLException {
        String result = query1("SELECT to_date('15/06/2024', 'DD/MM/YYYY')");
        assertEquals("2024-06-15", result);
    }

    @Test void testToTimestamp() throws SQLException {
        String result = query1("SELECT to_timestamp('2024-06-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertTrue(result.contains("2024-06-15") && result.contains("14:30:00"),
                "Expected timestamp with '2024-06-15 14:30:00' but got: '" + result + "'");
    }

    // ========================================================================
    // Item 55: Numeric Formatting
    // ========================================================================

    @Test void testToCharInt999() throws SQLException {
        String result = query1("SELECT to_char(123, '999')");
        assertTrue(result.contains("123"), "Expected '123' in result but got: '" + result + "'");
    }

    @Test void testToCharInt0000() throws SQLException {
        String result = query1("SELECT to_char(123, '0000')");
        assertTrue(result.contains("0123"), "Expected '0123' in result but got: '" + result + "'");
    }

    @Test void testToCharDecimal() throws SQLException {
        String result = query1("SELECT to_char(123.45, '999.99')");
        assertTrue(result.contains("123.45"), "Expected '123.45' in result but got: '" + result + "'");
    }

    @Test void testToCharWithComma() throws SQLException {
        String result = query1("SELECT to_char(1234, '9,999')");
        assertTrue(result.contains("1,234"), "Expected '1,234' in result but got: '" + result + "'");
    }

    @Test void testToCharNegativeMI() throws SQLException {
        String result = query1("SELECT to_char(-123, '999MI')");
        assertTrue(result.contains("123") && result.contains("-"),
                "Expected '123-' but got: '" + result + "'");
    }

    @Test void testToCharPositivePR() throws SQLException {
        String result = query1("SELECT to_char(123, '999PR')");
        assertTrue(result.contains("123"), "Expected '123' in result but got: '" + result + "'");
        assertFalse(result.contains("<"), "Positive numbers should not have angle brackets");
    }

    @Test void testToCharNegativePR() throws SQLException {
        String result = query1("SELECT to_char(-123, '999PR')");
        assertTrue(result.contains("<") && result.contains("123") && result.contains(">"),
                "Expected '<123>' but got: '" + result + "'");
    }

    @Test void testToCharFillMode() throws SQLException {
        String result = query1("SELECT to_char(123, 'FM999')");
        assertEquals("123", result);
    }

    @Test void testToNumber() throws SQLException {
        assertEquals(1234.56, queryDouble("SELECT to_number('1,234.56', '9,999.99')"), 0.001);
    }

    @Test void testToNumberSimple() throws SQLException {
        assertEquals(123.0, queryDouble("SELECT to_number('123', '999')"), 0.001);
    }
}
