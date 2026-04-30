package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category D: Numeric / decimal edge cases.
 *
 * Covers:
 *  - numeric(p, -s) negative scale (rounds to 10^|s|)
 *  - NaN equality (NaN = NaN → true for numeric)
 *  - information_schema.columns typmod → numeric_precision/scale
 *  - width_bucket(value, ARRAY[...]) two-arg variant
 *  - money formatting ($prefix, lc_monetary)
 *  - float8 parsing: 'inf', 'infinity', '+Infinity' (case-insensitive)
 *  - round(num, -n) / trunc(num, -n) negative precision (exact)
 *  - mod(numeric, numeric) returns numeric (not Long-cast)
 *  - gcd/lcm overflow → 22003
 *  - factorial() overflow promotion to numeric
 *  - random_normal(mean, stddev)
 */
class Round16NumericEdgeTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private static BigDecimal dec(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBigDecimal(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // D1. numeric(p, -s) negative scale
    // =========================================================================

    @Test
    void numeric_negative_scale_rounds_to_hundreds() throws SQLException {
        BigDecimal v = dec("SELECT 1234.56::numeric(5,-2)");
        assertEquals(0, new BigDecimal("1200").compareTo(v),
                "numeric(5,-2) must round 1234.56 to 1200");
    }

    // =========================================================================
    // D2. NaN equality
    // =========================================================================

    @Test
    void numeric_nan_equals_nan() throws SQLException {
        assertTrue(bool("SELECT 'NaN'::numeric = 'NaN'::numeric"),
                "PG 18: NaN::numeric = NaN::numeric returns true");
    }

    @Test
    void numeric_nan_orders_greatest() throws SQLException {
        assertTrue(bool("SELECT 'NaN'::numeric > 1e308::numeric"),
                "PG 18: NaN::numeric orders greater than any finite numeric");
    }

    // =========================================================================
    // D3. information_schema.columns typmod
    // =========================================================================

    @Test
    void information_schema_numeric_precision_and_scale() throws SQLException {
        exec("CREATE TABLE r16_num_typmod (a numeric(10,2), b numeric(5))");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT column_name, numeric_precision, numeric_scale "
                             + "FROM information_schema.columns "
                             + "WHERE table_name = 'r16_num_typmod' ORDER BY ordinal_position")) {
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(10, rs.getInt(2), "numeric(10,2) precision must be 10");
            assertEquals(2,  rs.getInt(3), "numeric(10,2) scale must be 2");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(5, rs.getInt(2), "numeric(5) precision must be 5");
            assertEquals(0, rs.getInt(3), "numeric(5) scale must be 0");
        }
    }

    // =========================================================================
    // D4. width_bucket with array thresholds (2-arg)
    // =========================================================================

    @Test
    void width_bucket_with_array_thresholds() throws SQLException {
        // thresholds {10, 20, 30} — value 15 falls into bucket 1
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT width_bucket(15, ARRAY[10, 20, 30])")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // =========================================================================
    // D5. Money formatting
    // =========================================================================

    @Test
    void money_formatting_has_currency_prefix() throws SQLException {
        String v = str("SELECT 12.34::money::text");
        assertNotNull(v);
        // PG 18 with default lc_monetary uses "$12.34"
        assertTrue(v.contains("$") || v.contains("€") || v.contains("£"),
                "money text output must contain a currency symbol; got: " + v);
    }

    // =========================================================================
    // D6. Float8 infinity parsing
    // =========================================================================

    @Test
    void float8_accepts_lowercase_inf() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'inf'::float8 > 1e308")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "'inf'::float8 must represent positive infinity");
        }
    }

    @Test
    void float8_accepts_lowercase_infinity() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'infinity'::float8 > 1e308")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void float8_accepts_plus_infinity() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '+Infinity'::float8 > 1e308")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // =========================================================================
    // D7. round/trunc with negative precision
    // =========================================================================

    @Test
    void round_negative_precision_is_exact_not_float_drift() throws SQLException {
        BigDecimal v = dec("SELECT round(1234.56, -2)");
        assertEquals(0, new BigDecimal("1200").compareTo(v),
                "round(1234.56, -2) must be exactly 1200");
    }

    @Test
    void trunc_negative_precision_is_exact() throws SQLException {
        BigDecimal v = dec("SELECT trunc(1234.56, -2)");
        assertEquals(0, new BigDecimal("1200").compareTo(v),
                "trunc(1234.56, -2) must be exactly 1200 (no float drift)");
    }

    // =========================================================================
    // D8. mod() on numeric
    // =========================================================================

    @Test
    void mod_numeric_returns_numeric() throws SQLException {
        BigDecimal v = dec("SELECT mod(3.7::numeric, 2::numeric)");
        assertEquals(0, new BigDecimal("1.7").compareTo(v),
                "mod(3.7, 2) must return numeric 1.7");
    }

    // =========================================================================
    // D9. gcd/lcm overflow → SQLSTATE 22003
    // =========================================================================

    @Test
    void lcm_overflow_raises_22003() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT lcm(9223372036854775807::bigint, 2::bigint)")) {
            // Either raises immediately or returns a wrong value
            if (rs.next()) {
                long got = rs.getLong(1);
                fail("lcm(BIGINT_MAX, 2) must raise 22003 'bigint out of range'; got " + got);
            }
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState(),
                    "lcm overflow must raise SQLSTATE 22003; got " + e.getSQLState());
        }
    }

    // =========================================================================
    // D10. factorial — promote on overflow or raise
    // =========================================================================

    @Test
    void factorial_overflow_promotes_or_errors() throws SQLException {
        // 25! = 15511210043330985984000000 — exceeds int64; PG 18 returns numeric.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT factorial(25)")) {
            assertTrue(rs.next());
            BigDecimal v = rs.getBigDecimal(1);
            assertNotNull(v);
            assertEquals(0, new BigDecimal("15511210043330985984000000").compareTo(v),
                    "factorial(25) must return the exact numeric 25!");
        }
    }

    // =========================================================================
    // D11. random_normal (PG 16+)
    // =========================================================================

    @Test
    void random_normal_function_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT random_normal(0, 1)")) {
            assertTrue(rs.next());
            double v = rs.getDouble(1);
            // Just sanity-bound: must be finite
            assertTrue(Double.isFinite(v), "random_normal(0,1) must return a finite double");
        }
    }
}
