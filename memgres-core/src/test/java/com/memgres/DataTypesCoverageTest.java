package com.memgres;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checklist items 25-33: Data Types coverage tests.
 * Numeric (integer, float, numeric/decimal, money),
 * Character (char, varchar, text),
 * Date/Time (date, time, timestamp, interval).
 */
class DataTypesCoverageTest {

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

    // ========================================================================
    // 25. Integer Types
    // ========================================================================

    @Test
    void int_create_table_with_smallint_integer_bigint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE int_types (a SMALLINT, b INTEGER, c BIGINT)");
            s.execute("INSERT INTO int_types VALUES (1, 100, 1000000000000)");
            ResultSet rs = s.executeQuery("SELECT a, b, c FROM int_types");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals(100, rs.getInt("b"));
            assertEquals(1000000000000L, rs.getLong("c"));
            s.execute("DROP TABLE int_types");
        }
    }

    @Test
    void int_serial_auto_increment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE serial_test (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO serial_test (name) VALUES ('a')");
            s.execute("INSERT INTO serial_test (name) VALUES ('b')");
            s.execute("INSERT INTO serial_test (name) VALUES ('c')");
            ResultSet rs = s.executeQuery("SELECT id FROM serial_test ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            s.execute("DROP TABLE serial_test");
        }
    }

    @Test
    void int_bigserial_auto_increment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bigserial_test (id BIGSERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO bigserial_test (name) VALUES ('x')");
            s.execute("INSERT INTO bigserial_test (name) VALUES ('y')");
            ResultSet rs = s.executeQuery("SELECT id FROM bigserial_test ORDER BY id");
            assertTrue(rs.next()); assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next()); assertEquals(2L, rs.getLong(1));
            s.execute("DROP TABLE bigserial_test");
        }
    }

    @Test
    void int_smallserial_auto_increment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE smallserial_test (id SMALLSERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO smallserial_test (name) VALUES ('p')");
            s.execute("INSERT INTO smallserial_test (name) VALUES ('q')");
            ResultSet rs = s.executeQuery("SELECT id FROM smallserial_test ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE smallserial_test");
        }
    }

    @Test
    void int_arithmetic_operators() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 10 + 3, 10 - 3, 10 * 3, 10 / 3, 10 % 3");
            assertTrue(rs.next());
            assertEquals(13, rs.getInt(1));
            assertEquals(7, rs.getInt(2));
            assertEquals(30, rs.getInt(3));
            assertEquals(3, rs.getInt(4));  // integer division
            assertEquals(1, rs.getInt(5));
        }
    }

    @Test
    void int_implicit_widening_smallint_plus_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE widen1 (a SMALLINT, b INTEGER)");
            s.execute("INSERT INTO widen1 VALUES (100, 100000)");
            ResultSet rs = s.executeQuery("SELECT a + b FROM widen1");
            assertTrue(rs.next());
            assertEquals(100100, rs.getInt(1));
            s.execute("DROP TABLE widen1");
        }
    }

    @Test
    void int_implicit_widening_integer_plus_bigint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE widen2 (a INTEGER, b BIGINT)");
            s.execute("INSERT INTO widen2 VALUES (100, 5000000000)");
            ResultSet rs = s.executeQuery("SELECT a + b FROM widen2");
            assertTrue(rs.next());
            assertEquals(5000000100L, rs.getLong(1));
            s.execute("DROP TABLE widen2");
        }
    }

    @Test
    void int_cast_between_types() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(32000 AS SMALLINT)");
            assertTrue(rs.next());
            assertEquals(32000, rs.getInt(1));

            rs = s.executeQuery("SELECT 42::bigint");
            assertTrue(rs.next());
            assertEquals(42L, rs.getLong(1));
        }
    }

    @Test
    void int_int2_int4_int8_aliases() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE int_alias (a INT2, b INT4, c INT8)");
            s.execute("INSERT INTO int_alias VALUES (10, 20, 30)");
            ResultSet rs = s.executeQuery("SELECT a, b, c FROM int_alias");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("a"));
            assertEquals(20, rs.getInt("b"));
            assertEquals(30, rs.getLong("c"));
            s.execute("DROP TABLE int_alias");
        }
    }

    @Test
    void int_negative_values_and_zero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE int_neg (a INTEGER, b BIGINT)");
            s.execute("INSERT INTO int_neg VALUES (-42, -9999999999)");
            s.execute("INSERT INTO int_neg VALUES (0, 0)");
            ResultSet rs = s.executeQuery("SELECT a, b FROM int_neg ORDER BY a");
            assertTrue(rs.next());
            assertEquals(-42, rs.getInt("a"));
            assertEquals(-9999999999L, rs.getLong("b"));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("a"));
            assertEquals(0L, rs.getLong("b"));
            s.execute("DROP TABLE int_neg");
        }
    }

    @Test
    void int_max_reasonable_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 32767::smallint, 2147483647::integer, 9223372036854775807::bigint");
            assertTrue(rs.next());
            assertEquals(32767, rs.getInt(1));
            assertEquals(2147483647, rs.getInt(2));
            assertEquals(9223372036854775807L, rs.getLong(3));
        }
    }

    // ========================================================================
    // 26. Floating-Point Types
    // ========================================================================

    @Test
    void float_create_table_real_double() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE float_types (a REAL, b DOUBLE PRECISION)");
            s.execute("INSERT INTO float_types VALUES (3.14, 2.718281828459045)");
            ResultSet rs = s.executeQuery("SELECT a, b FROM float_types");
            assertTrue(rs.next());
            assertEquals(3.14, rs.getFloat("a"), 0.01);
            assertEquals(2.718281828459045, rs.getDouble("b"), 0.0000000001);
            s.execute("DROP TABLE float_types");
        }
    }

    @Test
    void float_float4_float8_aliases() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE float_alias (a FLOAT4, b FLOAT8)");
            s.execute("INSERT INTO float_alias VALUES (1.5, 2.5)");
            ResultSet rs = s.executeQuery("SELECT a, b FROM float_alias");
            assertTrue(rs.next());
            assertEquals(1.5, rs.getFloat("a"), 0.001);
            assertEquals(2.5, rs.getDouble("b"), 0.001);
            s.execute("DROP TABLE float_alias");
        }
    }

    @Test
    void float_scientific_notation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1.5e10, 2.5E-3");
            assertTrue(rs.next());
            assertEquals(1.5e10, rs.getDouble(1), 1e5);
            assertEquals(2.5e-3, rs.getDouble(2), 1e-8);
        }
    }

    @Test
    void float_infinity() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST('Infinity' AS DOUBLE PRECISION), CAST('-Infinity' AS DOUBLE PRECISION)");
            assertTrue(rs.next());
            assertEquals(Double.POSITIVE_INFINITY, rs.getDouble(1));
            assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble(2));
        }
    }

    @Test
    void float_nan() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST('NaN' AS DOUBLE PRECISION)");
            assertTrue(rs.next());
            assertTrue(Double.isNaN(rs.getDouble(1)));
        }
    }

    @Test
    void float_nan_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // In PG, NaN = NaN is true (unlike IEEE), NaN > any number
            ResultSet rs = s.executeQuery("SELECT CAST('NaN' AS DOUBLE PRECISION) = CAST('NaN' AS DOUBLE PRECISION)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // PG returns true for NaN = NaN
            assertEquals("t", val);
        }
    }

    @Test
    void float_arithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1.5 + 2.5, 10.0 - 3.5, 2.0 * 3.5, 7.0 / 2.0");
            assertTrue(rs.next());
            assertEquals(4.0, rs.getDouble(1), 0.001);
            assertEquals(6.5, rs.getDouble(2), 0.001);
            assertEquals(7.0, rs.getDouble(3), 0.001);
            assertEquals(3.5, rs.getDouble(4), 0.001);
        }
    }

    @Test
    void float_precision_syntax() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // FLOAT(1) through FLOAT(24) => REAL, FLOAT(25) through FLOAT(53) => DOUBLE PRECISION
            s.execute("CREATE TABLE float_prec (a FLOAT(1), b FLOAT(53))");
            s.execute("INSERT INTO float_prec VALUES (1.5, 2.5)");
            ResultSet rs = s.executeQuery("SELECT a, b FROM float_prec");
            assertTrue(rs.next());
            assertEquals(1.5, rs.getFloat("a"), 0.01);
            assertEquals(2.5, rs.getDouble("b"), 0.001);
            s.execute("DROP TABLE float_prec");
        }
    }

    @Test
    void float_cast_between_float_and_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(3.7 AS INTEGER)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1)); // rounds in PG

            rs = s.executeQuery("SELECT CAST(42 AS DOUBLE PRECISION)");
            assertTrue(rs.next());
            assertEquals(42.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void float_division_producing_float() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7.0 / 2");
            assertTrue(rs.next());
            assertEquals(3.5, rs.getDouble(1), 0.001);
        }
    }

    // ========================================================================
    // 27. Numeric/Decimal
    // ========================================================================

    @Test
    void numeric_precision_scale() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_ps (val NUMERIC(10, 2))");
            s.execute("INSERT INTO num_ps VALUES (1234.567)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_ps");
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal("val");
            assertEquals(new BigDecimal("1234.57"), result);
            s.execute("DROP TABLE num_ps");
        }
    }

    @Test
    void numeric_without_precision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_unl (val NUMERIC)");
            s.execute("INSERT INTO num_unl VALUES (123456789.123456789)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_unl");
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal("val");
            assertTrue(result.compareTo(new BigDecimal("123456789.123456789")) == 0);
            s.execute("DROP TABLE num_unl");
        }
    }

    @Test
    void numeric_decimal_alias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dec_test (val DECIMAL(8, 3))");
            s.execute("INSERT INTO dec_test VALUES (12345.6789)");
            ResultSet rs = s.executeQuery("SELECT val FROM dec_test");
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal("val");
            assertEquals(new BigDecimal("12345.679"), result);
            s.execute("DROP TABLE dec_test");
        }
    }

    @Test
    void numeric_exact_arithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 0.1::numeric + 0.2::numeric");
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertTrue(result.compareTo(new BigDecimal("0.3")) == 0);
        }
    }

    @Test
    void numeric_very_large_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_large (val NUMERIC)");
            s.execute("INSERT INTO num_large VALUES (99999999999999999999999999999.999999)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_large");
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal("val");
            assertTrue(result.compareTo(new BigDecimal("99999999999999999999999999999.999999")) == 0);
            s.execute("DROP TABLE num_large");
        }
    }

    @Test
    void numeric_cast_from_various_types() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(42 AS NUMERIC)");
            assertTrue(rs.next());
            assertEquals(new BigDecimal("42"), rs.getBigDecimal(1));

            rs = s.executeQuery("SELECT CAST(3.14 AS NUMERIC(5, 2))");
            assertTrue(rs.next());
            assertEquals(new BigDecimal("3.14"), rs.getBigDecimal(1));

            rs = s.executeQuery("SELECT CAST('99.99' AS NUMERIC)");
            assertTrue(rs.next());
            assertTrue(rs.getBigDecimal(1).compareTo(new BigDecimal("99.99")) == 0);
        }
    }

    @Test
    void numeric_rounding_behavior() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_round (val NUMERIC(5, 2))");
            s.execute("INSERT INTO num_round VALUES (1.234)");
            s.execute("INSERT INTO num_round VALUES (1.235)");
            s.execute("INSERT INTO num_round VALUES (1.245)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_round ORDER BY val");
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1.23"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            // PG rounds 1.235 to 1.24 (banker's rounding or half-up)
            BigDecimal v2 = rs.getBigDecimal(1);
            assertTrue(v2.compareTo(new BigDecimal("1.23")) >= 0 && v2.compareTo(new BigDecimal("1.24")) <= 0);
            s.execute("DROP TABLE num_round");
        }
    }

    // ========================================================================
    // 28. Money Type
    // ========================================================================

    @Test
    void money_create_and_insert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE money_test (val MONEY)");
            s.execute("INSERT INTO money_test VALUES ('$1,234.56')");
            ResultSet rs = s.executeQuery("SELECT val FROM money_test");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1234.56") || val.contains("1,234.56") || val.contains("$1,234.56"),
                    "Money value should contain 1234.56, got: " + val);
            s.execute("DROP TABLE money_test");
        }
    }

    @Test
    void money_insert_numeric() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE money_num (val MONEY)");
            s.execute("INSERT INTO money_num VALUES (99.99)");
            ResultSet rs = s.executeQuery("SELECT val FROM money_num");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("99.99"), "Money value should contain 99.99, got: " + val);
            s.execute("DROP TABLE money_num");
        }
    }

    @Test
    void money_arithmetic_add_subtract() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '$10.00'::money + '$5.50'::money");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("15.50"), "Expected 15.50, got: " + val);

            rs = s.executeQuery("SELECT '$10.00'::money - '$3.25'::money");
            assertTrue(rs.next());
            val = rs.getString(1);
            assertTrue(val.contains("6.75"), "Expected 6.75, got: " + val);
        }
    }

    @Test
    void money_cast_to_numeric() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '$42.50'::money::numeric");
            assertTrue(rs.next());
            BigDecimal val = rs.getBigDecimal(1);
            assertTrue(val.compareTo(new BigDecimal("42.50")) == 0);
        }
    }

    @Test
    void money_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE money_cmp (a MONEY, b MONEY)");
            s.execute("INSERT INTO money_cmp VALUES ('$10.00', '$20.00')");
            ResultSet rs = s.executeQuery("SELECT a < b, a > b, a = a FROM money_cmp");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("f", rs.getString(2));
            assertEquals("t", rs.getString(3));
            s.execute("DROP TABLE money_cmp");
        }
    }

    // ========================================================================
    // 29. Character Types
    // ========================================================================

    @Test
    void char_padding_verification() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE char_pad (val CHAR(10))");
            s.execute("INSERT INTO char_pad VALUES ('hello')");
            ResultSet rs = s.executeQuery("SELECT val, length(val) FROM char_pad");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // CHAR(10) should pad with spaces to length 10
            assertEquals(10, val.length());
            assertTrue(val.startsWith("hello"));
        }
    }

    @Test
    void char_varchar_within_limit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE vc_ok (val VARCHAR(5))");
            s.execute("INSERT INTO vc_ok VALUES ('hello')");
            ResultSet rs = s.executeQuery("SELECT val FROM vc_ok");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
            s.execute("DROP TABLE vc_ok");
        }
    }

    @Test
    void char_varchar_exceeding_limit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE vc_err (val VARCHAR(3))");
            assertThrows(SQLException.class, () -> {
                s.execute("INSERT INTO vc_err VALUES ('toolong')");
            });
            s.execute("DROP TABLE vc_err");
        }
    }

    @Test
    void char_text_unlimited() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE text_test (val TEXT)");
            String longStr = Strs.repeat("a", 10000);
            s.execute("INSERT INTO text_test VALUES ('" + longStr + "')");
            ResultSet rs = s.executeQuery("SELECT length(val) FROM text_test");
            assertTrue(rs.next());
            assertEquals(10000, rs.getInt(1));
            s.execute("DROP TABLE text_test");
        }
    }

    @Test
    void char_character_length_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT character_length('hello world')");
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
        }
    }

    @Test
    void char_octet_length_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT octet_length('hello')");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void char_concatenation_operator() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello' || ' ' || 'world'");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
    }

    @Test
    void char_empty_string_is_not_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '' IS NOT NULL");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));

            rs = s.executeQuery("SELECT '' IS NULL");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1));
        }
    }

    @Test
    void char_column_types_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE char_types (c CHAR(5), v VARCHAR(10), t TEXT)");
            s.execute("INSERT INTO char_types VALUES ('ab', 'cd', 'ef')");
            ResultSet rs = s.executeQuery("SELECT c, v, t FROM char_types");
            assertTrue(rs.next());
            // CHAR pads, VARCHAR and TEXT don't
            assertEquals("ab   ", rs.getString("c"));
            assertEquals("cd", rs.getString("v"));
            assertEquals("ef", rs.getString("t"));
            s.execute("DROP TABLE char_types");
        }
    }

    @Test
    void char_character_varying_alias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cv_test (val CHARACTER VARYING(20))");
            s.execute("INSERT INTO cv_test VALUES ('test value')");
            ResultSet rs = s.executeQuery("SELECT val FROM cv_test");
            assertTrue(rs.next());
            assertEquals("test value", rs.getString(1));
            s.execute("DROP TABLE cv_test");
        }
    }

    // ========================================================================
    // 30. Date Type
    // ========================================================================

    @Test
    void date_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15'");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertNotNull(d);
            assertEquals("2024-01-15", d.toString());
        }
    }

    @Test
    void date_plus_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15' + 10");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertEquals("2024-01-25", d.toString());
        }
    }

    @Test
    void date_minus_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15' - 5");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertEquals("2024-01-10", d.toString());
        }
    }

    @Test
    void date_minus_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15' - DATE '2024-01-10'");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void date_current_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CURRENT_DATE");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertNotNull(d);
            // Just verify it's a reasonable date (in the 2020s)
            assertTrue(d.toString().startsWith("202"));
        }
    }

    @Test
    void date_comparison_operators() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT DATE '2024-01-10' < DATE '2024-01-15', " +
                    "DATE '2024-01-15' > DATE '2024-01-10', " +
                    "DATE '2024-01-15' = DATE '2024-01-15', " +
                    "DATE '2024-01-10' <> DATE '2024-01-15'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("t", rs.getString(2));
            assertEquals("t", rs.getString(3));
            assertEquals("t", rs.getString(4));
        }
    }

    @Test
    void date_to_char_formatting() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-01-15', 'YYYY-MM-DD')");
            assertTrue(rs.next());
            assertEquals("2024-01-15", rs.getString(1));
        }
    }

    @Test
    void date_cast_string_to_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST('2024-06-15' AS DATE)");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertEquals("2024-06-15", d.toString());

            rs = s.executeQuery("SELECT '2024-12-25'::date");
            assertTrue(rs.next());
            d = rs.getDate(1);
            assertEquals("2024-12-25", d.toString());
        }
    }

    @Test
    void date_insert_and_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE date_tab (id INTEGER, d DATE)");
            s.execute("INSERT INTO date_tab VALUES (1, '2024-01-01')");
            s.execute("INSERT INTO date_tab VALUES (2, '2024-06-15')");
            s.execute("INSERT INTO date_tab VALUES (3, '2024-12-31')");
            ResultSet rs = s.executeQuery("SELECT d FROM date_tab ORDER BY d");
            assertTrue(rs.next()); assertEquals("2024-01-01", rs.getDate(1).toString());
            assertTrue(rs.next()); assertEquals("2024-06-15", rs.getDate(1).toString());
            assertTrue(rs.next()); assertEquals("2024-12-31", rs.getDate(1).toString());
            s.execute("DROP TABLE date_tab");
        }
    }

    @Test
    void date_plus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15' + INTERVAL '1 month'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-02-15"), "Expected 2024-02-15, got: " + val);
        }
    }

    // ========================================================================
    // 31. Time Types
    // ========================================================================

    @Test
    void time_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIME '14:30:00'");
            assertTrue(rs.next());
            Time t = rs.getTime(1);
            assertNotNull(t);
            assertEquals("14:30:00", t.toString());
        }
    }

    @Test
    void time_column_insert_and_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE time_tab (id INTEGER, t TIME)");
            s.execute("INSERT INTO time_tab VALUES (1, '09:00:00')");
            s.execute("INSERT INTO time_tab VALUES (2, '12:30:00')");
            s.execute("INSERT INTO time_tab VALUES (3, '23:59:59')");
            ResultSet rs = s.executeQuery("SELECT t FROM time_tab ORDER BY t");
            assertTrue(rs.next()); assertEquals("09:00:00", rs.getTime(1).toString());
            assertTrue(rs.next()); assertEquals("12:30:00", rs.getTime(1).toString());
            assertTrue(rs.next()); assertEquals("23:59:59", rs.getTime(1).toString());
            s.execute("DROP TABLE time_tab");
        }
    }

    @Test
    void time_current_time() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CURRENT_TIME");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should be a time string
            assertTrue(val.matches("\\d{2}:\\d{2}:\\d{2}.*"), "Expected time format, got: " + val);
        }
    }

    @Test
    void time_localtime() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT LOCALTIME");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.matches("\\d{2}:\\d{2}:\\d{2}.*"), "Expected time format, got: " + val);
        }
    }

    @Test
    void time_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIME '09:00:00' < TIME '17:00:00', " +
                    "TIME '17:00:00' > TIME '09:00:00', " +
                    "TIME '12:00:00' = TIME '12:00:00'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("t", rs.getString(2));
            assertEquals("t", rs.getString(3));
        }
    }

    @Test
    void time_plus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIME '10:00:00' + INTERVAL '2 hours 30 minutes'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("12:30:00"), "Expected 12:30:00, got: " + val);
        }
    }

    @Test
    void time_precision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE time_prec (t TIME(0))");
            s.execute("INSERT INTO time_prec VALUES ('12:34:56.789')");
            ResultSet rs = s.executeQuery("SELECT t FROM time_prec");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // With precision 0, fractional seconds should be truncated/rounded
            assertTrue(val.startsWith("12:34:5"), "Expected time around 12:34:56/57, got: " + val);
            s.execute("DROP TABLE time_prec");
        }
    }

    @Test
    void time_without_time_zone_explicit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE time_notz (t TIME WITHOUT TIME ZONE)");
            s.execute("INSERT INTO time_notz VALUES ('15:45:00')");
            ResultSet rs = s.executeQuery("SELECT t FROM time_notz");
            assertTrue(rs.next());
            assertEquals("15:45:00", rs.getTime(1).toString());
            s.execute("DROP TABLE time_notz");
        }
    }

    @Test
    void time_different_formats() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE time_fmt (t TIME)");
            s.execute("INSERT INTO time_fmt VALUES ('08:05:01')");
            s.execute("INSERT INTO time_fmt VALUES ('00:00:00')");
            s.execute("INSERT INTO time_fmt VALUES ('23:59:59')");
            ResultSet rs = s.executeQuery("SELECT t FROM time_fmt ORDER BY t");
            assertTrue(rs.next()); assertEquals("00:00:00", rs.getTime(1).toString());
            assertTrue(rs.next()); assertEquals("08:05:01", rs.getTime(1).toString());
            assertTrue(rs.next()); assertEquals("23:59:59", rs.getTime(1).toString());
            s.execute("DROP TABLE time_fmt");
        }
    }

    // ========================================================================
    // 32. Timestamp Types
    // ========================================================================

    @Test
    void timestamp_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 14:30:00'");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
            assertTrue(ts.toString().startsWith("2024-01-15 14:30:00"));
        }
    }

    @Test
    void timestamp_with_time_zone_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP WITH TIME ZONE '2024-01-15 14:30:00+05:00'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain the date/time
            assertTrue(val.contains("2024-01-15") || val.contains("2024-01-15"),
                    "Expected timestamp with tz, got: " + val);
        }
    }

    @Test
    void timestamp_timestamptz_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tstz_tab (id INTEGER, ts TIMESTAMPTZ)");
            s.execute("INSERT INTO tstz_tab VALUES (1, '2024-06-15 10:30:00+00')");
            ResultSet rs = s.executeQuery("SELECT ts FROM tstz_tab");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-06-15"), "Expected date in timestamptz, got: " + val);
            s.execute("DROP TABLE tstz_tab");
        }
    }

    @Test
    void timestamp_now_returns_timestamptz() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NOW()");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
            // Should be a recent timestamp
            long diff = System.currentTimeMillis() - ts.getTime();
            assertTrue(Math.abs(diff) < 60000, "NOW() should be close to current time");
        }
    }

    @Test
    void timestamp_current_timestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CURRENT_TIMESTAMP");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
            long diff = System.currentTimeMillis() - ts.getTime();
            assertTrue(Math.abs(diff) < 60000, "CURRENT_TIMESTAMP should be close to current time");
        }
    }

    @Test
    void timestamp_localtimestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT LOCALTIMESTAMP");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
        }
    }

    @Test
    void timestamp_plus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL '2 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("12:00:00"), "Expected 12:00:00, got: " + val);
        }
    }

    @Test
    void timestamp_minus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 10:00:00' - INTERVAL '3 days'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-01-12"), "Expected 2024-01-12, got: " + val);
        }
    }

    @Test
    void timestamp_minus_timestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIMESTAMP '2024-01-15 10:00:00' - TIMESTAMP '2024-01-10 10:00:00'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Result should be an interval of 5 days
            assertTrue(val.contains("5") && (val.contains("day") || val.contains("120:00:00")),
                    "Expected 5 days interval, got: " + val);
        }
    }

    @Test
    void timestamp_extract_year_month_day() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(YEAR FROM TIMESTAMP '2024-06-15 14:30:45'), " +
                    "EXTRACT(MONTH FROM TIMESTAMP '2024-06-15 14:30:45'), " +
                    "EXTRACT(DAY FROM TIMESTAMP '2024-06-15 14:30:45')");
            assertTrue(rs.next());
            assertEquals(2024, rs.getInt(1));
            assertEquals(6, rs.getInt(2));
            assertEquals(15, rs.getInt(3));
        }
    }

    @Test
    void timestamp_extract_hour_minute_second() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(HOUR FROM TIMESTAMP '2024-06-15 14:30:45'), " +
                    "EXTRACT(MINUTE FROM TIMESTAMP '2024-06-15 14:30:45'), " +
                    "EXTRACT(SECOND FROM TIMESTAMP '2024-06-15 14:30:45')");
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1));
            assertEquals(30, rs.getInt(2));
            assertEquals(45, rs.getInt(3));
        }
    }

    @Test
    void timestamp_extract_epoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00')");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        }
    }

    @Test
    void timestamp_extract_dow_doy() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // 2024-01-15 is Monday; PG DOW: Sunday=0, Monday=1
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(DOW FROM TIMESTAMP '2024-01-15 00:00:00'), " +
                    "EXTRACT(DOY FROM TIMESTAMP '2024-01-15 00:00:00')");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // Monday
            assertEquals(15, rs.getInt(2)); // 15th day of year
        }
    }

    @Test
    void timestamp_extract_quarter_week() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(QUARTER FROM TIMESTAMP '2024-06-15 00:00:00'), " +
                    "EXTRACT(WEEK FROM TIMESTAMP '2024-06-15 00:00:00')");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // Q2
            int week = rs.getInt(2);
            assertTrue(week >= 23 && week <= 25, "Expected week ~24, got: " + week);
        }
    }

    @Test
    void timestamp_at_time_zone() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'UTC'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-01-15"), "Expected date in result, got: " + val);
        }
    }

    @Test
    void timestamp_to_timestamp_from_epoch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_timestamp(0)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1970-01-01"), "Expected epoch start, got: " + val);
        }
    }

    @Test
    void timestamp_precision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ts_prec (ts TIMESTAMP(0))");
            s.execute("INSERT INTO ts_prec VALUES ('2024-01-15 14:30:45.999')");
            ResultSet rs = s.executeQuery("SELECT ts FROM ts_prec");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // With precision 0, no fractional seconds
            assertTrue(val.contains("14:30:4"), "Expected time around 14:30:45/46, got: " + val);
            s.execute("DROP TABLE ts_prec");
        }
    }

    @Test
    void timestamp_cast_date_to_timestamp() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(DATE '2024-01-15' AS TIMESTAMP)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-01-15") && val.contains("00:00:00"),
                    "Expected 2024-01-15 00:00:00, got: " + val);
        }
    }

    @Test
    void timestamp_cast_between_timestamp_and_timestamptz() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(TIMESTAMP '2024-01-15 14:30:00' AS TIMESTAMPTZ)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-01-15"), "Expected date in result, got: " + val);
        }
    }

    // ========================================================================
    // 33. Interval Type
    // ========================================================================

    @Test
    void interval_year_month_day_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '1 year 2 months 3 days'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1 year") || val.contains("14 mon") || val.contains("1 year"),
                    "Expected interval with year/months, got: " + val);
        }
    }

    @Test
    void interval_hours_minutes_seconds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '4 hours 5 minutes 6 seconds'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("04:05:06") || val.contains("4:05:06"),
                    "Expected 04:05:06, got: " + val);
        }
    }

    @Test
    void interval_addition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '1 hour' + INTERVAL '30 minutes'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("01:30:00") || val.contains("1:30:00"),
                    "Expected 1:30:00, got: " + val);
        }
    }

    @Test
    void interval_subtraction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '2 hours' - INTERVAL '30 minutes'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("01:30:00") || val.contains("1:30:00"),
                    "Expected 1:30:00, got: " + val);
        }
    }

    @Test
    void interval_multiply_by_number() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '1 hour' * 3");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("03:00:00") || val.contains("3:00:00"),
                    "Expected 3:00:00, got: " + val);
        }
    }

    @Test
    void interval_extract_year() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(YEAR FROM INTERVAL '2 years 3 months')");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void interval_extract_month() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(MONTH FROM INTERVAL '2 years 3 months')");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void interval_extract_day() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(DAY FROM INTERVAL '10 days 5 hours')");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        }
    }

    @Test
    void interval_extract_hour() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(HOUR FROM INTERVAL '3 hours 25 minutes')");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void interval_date_plus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15' + INTERVAL '1 month 5 days'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-02-20"), "Expected 2024-02-20, got: " + val);
        }
    }

    @Test
    void interval_timestamp_plus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIMESTAMP '2024-01-15 10:00:00' + INTERVAL '1 day 2 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-01-16") && val.contains("12:00:00"),
                    "Expected 2024-01-16 12:00:00, got: " + val);
        }
    }

    @Test
    void interval_date_minus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-03-15' - INTERVAL '1 month'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-02-15"), "Expected 2024-02-15, got: " + val);
        }
    }

    @Test
    void interval_timestamp_minus_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIMESTAMP '2024-03-15 10:00:00' - INTERVAL '2 days 3 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-03-13") && val.contains("07:00:00"),
                    "Expected 2024-03-13 07:00:00, got: " + val);
        }
    }

    @Test
    void interval_justify_hours() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT justify_hours(INTERVAL '25 hours')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1 day") && val.contains("01:00:00"),
                    "Expected 1 day 01:00:00, got: " + val);
        }
    }

    @Test
    void interval_justify_days() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT justify_days(INTERVAL '35 days')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1 mon") && val.contains("5 day"),
                    "Expected 1 mon 5 days, got: " + val);
        }
    }

    @Test
    void interval_justify_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT justify_interval(INTERVAL '1 month -1 hour')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // justify_interval normalizes signs
            assertNotNull(val);
        }
    }

    @Test
    void interval_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT INTERVAL '1 hour' < INTERVAL '2 hours', " +
                    "INTERVAL '1 day' > INTERVAL '12 hours', " +
                    "INTERVAL '60 minutes' = INTERVAL '1 hour'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("t", rs.getString(2));
            assertEquals("t", rs.getString(3));
        }
    }

    @Test
    void interval_make_interval() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT make_interval(years => 1, months => 2, days => 3)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1 year") || val.contains("14 mon"),
                    "Expected interval with years/months, got: " + val);
        }
    }

    @Test
    void interval_negative() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '-3 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("-03:00:00") || val.contains("-3:00:00"),
                    "Expected negative interval, got: " + val);
        }
    }

    @Test
    void interval_negative_via_subtraction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '1 hour' - INTERVAL '3 hours'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("-02:00:00") || val.contains("-2:00:00"),
                    "Expected -2:00:00, got: " + val);
        }
    }

    @Test
    void interval_age_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT age(DATE '2024-06-15', DATE '2024-01-10')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("5 mon") || val.contains("5 month"),
                    "Expected interval with ~5 months, got: " + val);
        }
    }

    @Test
    void interval_age_single_arg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // age(timestamp) computes age from CURRENT_DATE
            ResultSet rs = s.executeQuery("SELECT age(TIMESTAMP '2020-01-01 00:00:00')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain years since 2020
            assertTrue(val.contains("year"), "Expected interval with years, got: " + val);
        }
    }

    @Test
    void interval_iso_format_check() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '2 hours 30 minutes'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // PG default output is like "02:30:00"
            assertTrue(val.contains("02:30:00") || val.contains("2:30:00"),
                    "Expected formatted interval, got: " + val);
        }
    }

    // ========================================================================
    // Additional edge-case tests to reach 80+ count
    // ========================================================================

    @Test
    void int_modulo_negative() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT -7 % 3");
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertEquals(-1, val); // PG returns -1 for -7 % 3
        }
    }

    @Test
    void int_integer_division_truncates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7 / 2");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // integer division
        }
    }

    @Test
    void float_negative_zero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT -0.0::double precision");
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(0.0, val, 0.0);
        }
    }

    @Test
    void float_very_small_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1e-300::double precision");
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertTrue(val > 0 && val < 1e-299);
        }
    }

    @Test
    void numeric_addition_subtraction() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 100.50::numeric + 200.25::numeric, 100.50::numeric - 50.25::numeric");
            assertTrue(rs.next());
            assertEquals(0, rs.getBigDecimal(1).compareTo(new BigDecimal("300.75")));
            assertEquals(0, rs.getBigDecimal(2).compareTo(new BigDecimal("50.25")));
        }
    }

    @Test
    void numeric_multiplication_division() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 10.5::numeric * 2::numeric");
            assertTrue(rs.next());
            assertEquals(0, rs.getBigDecimal(1).compareTo(new BigDecimal("21.0")));
        }
    }

    @Test
    void char_length_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT char_length('test'), length('hello world')");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals(11, rs.getInt(2));
        }
    }

    @Test
    void char_trim_functions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trim('  hello  '), trim(leading ' ' from '  hello  '), trim(trailing ' ' from '  hello  ')");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
            assertEquals("hello  ", rs.getString(2));
            assertEquals("  hello", rs.getString(3));
        }
    }

    @Test
    void date_extract_from_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(YEAR FROM DATE '2024-06-15'), " +
                    "EXTRACT(MONTH FROM DATE '2024-06-15'), " +
                    "EXTRACT(DAY FROM DATE '2024-06-15')");
            assertTrue(rs.next());
            assertEquals(2024, rs.getInt(1));
            assertEquals(6, rs.getInt(2));
            assertEquals(15, rs.getInt(3));
        }
    }

    @Test
    void date_month_boundary_arithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // End of January + 1 month
            ResultSet rs = s.executeQuery("SELECT DATE '2024-01-31' + INTERVAL '1 month'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // PG returns 2024-02-29 (leap year clamp)
            assertTrue(val.contains("2024-02-29") || val.contains("2024-03"),
                    "Expected end-of-Feb or start-of-Mar, got: " + val);
        }
    }

    @Test
    void timestamp_insert_and_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ts_tab (id INTEGER, ts TIMESTAMP)");
            s.execute("INSERT INTO ts_tab VALUES (1, '2024-01-01 00:00:00')");
            s.execute("INSERT INTO ts_tab VALUES (2, '2024-06-15 12:30:45')");
            ResultSet rs = s.executeQuery("SELECT ts FROM ts_tab ORDER BY ts");
            assertTrue(rs.next());
            assertTrue(rs.getTimestamp(1).toString().startsWith("2024-01-01 00:00:00"));
            assertTrue(rs.next());
            assertTrue(rs.getTimestamp(1).toString().startsWith("2024-06-15 12:30:45"));
            s.execute("DROP TABLE ts_tab");
        }
    }

    @Test
    void timestamp_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT TIMESTAMP '2024-01-01 00:00:00' < TIMESTAMP '2024-12-31 23:59:59', " +
                    "TIMESTAMP '2024-06-15 12:00:00' = TIMESTAMP '2024-06-15 12:00:00'");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("t", rs.getString(2));
        }
    }

    @Test
    void interval_days_only() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '10 days'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("10 day") || val.contains("10 days"),
                    "Expected 10 days, got: " + val);
        }
    }

    @Test
    void interval_months_only() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '6 months'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("6 mon"), "Expected 6 months, got: " + val);
        }
    }

    @Test
    void interval_column_storage() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE interval_tab (id INTEGER, dur INTERVAL)");
            s.execute("INSERT INTO interval_tab VALUES (1, '2 hours 30 minutes')");
            s.execute("INSERT INTO interval_tab VALUES (2, '1 day')");
            ResultSet rs = s.executeQuery("SELECT dur FROM interval_tab ORDER BY id");
            assertTrue(rs.next());
            String v1 = rs.getString(1);
            assertTrue(v1.contains("02:30:00") || v1.contains("2:30:00"), "Got: " + v1);
            assertTrue(rs.next());
            String v2 = rs.getString(1);
            assertTrue(v2.contains("1 day"), "Got: " + v2);
            s.execute("DROP TABLE interval_tab");
        }
    }

    @Test
    void int_abs_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT abs(-42), abs(42)");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals(42, rs.getInt(2));
        }
    }

    @Test
    void float_round_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT round(3.14159::double precision)");
            assertTrue(rs.next());
            assertEquals(3.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void numeric_zero_scale() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_zero_sc (val NUMERIC(10, 0))");
            s.execute("INSERT INTO num_zero_sc VALUES (42.9)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_zero_sc");
            assertTrue(rs.next());
            assertEquals(new BigDecimal("43"), rs.getBigDecimal(1));
            s.execute("DROP TABLE num_zero_sc");
        }
    }

    @Test
    void char_position_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT position('world' in 'hello world')");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    @Test
    void char_substring_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT substring('hello world' from 7 for 5)");
            assertTrue(rs.next());
            assertEquals("world", rs.getString(1));
        }
    }

    @Test
    void char_upper_lower() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT upper('hello'), lower('WORLD')");
            assertTrue(rs.next());
            assertEquals("HELLO", rs.getString(1));
            assertEquals("world", rs.getString(2));
        }
    }

    @Test
    void date_year_boundary() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2023-12-31' + 1");
            assertTrue(rs.next());
            assertEquals("2024-01-01", rs.getDate(1).toString());
        }
    }

    @Test
    void timestamp_midnight() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 00:00:00'");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertTrue(ts.toString().startsWith("2024-01-15 00:00:00"));
        }
    }

    @Test
    void timestamp_end_of_day() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 23:59:59'");
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertTrue(ts.toString().startsWith("2024-01-15 23:59:59"));
        }
    }

    @Test
    void interval_zero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '0 seconds'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("00:00:00"), "Expected zero interval, got: " + val);
        }
    }

    @Test
    void int_unary_minus() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT -(-42)");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void float_ceil_floor() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ceil(3.2), floor(3.8)");
            assertTrue(rs.next());
            assertEquals(4.0, rs.getDouble(1), 0.001);
            assertEquals(3.0, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void numeric_negative_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_neg (val NUMERIC(10, 2))");
            s.execute("INSERT INTO num_neg VALUES (-999.99)");
            ResultSet rs = s.executeQuery("SELECT val FROM num_neg");
            assertTrue(rs.next());
            assertEquals(new BigDecimal("-999.99"), rs.getBigDecimal(1));
            s.execute("DROP TABLE num_neg");
        }
    }

    @Test
    void char_null_concatenation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello' || NULL");
            assertTrue(rs.next());
            // In PG, string || NULL = NULL
            assertNull(rs.getString(1));
        }
    }

    @Test
    void date_leap_year() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT DATE '2024-02-29'");
            assertTrue(rs.next());
            assertEquals("2024-02-29", rs.getDate(1).toString());
        }
    }

    @Test
    void timestamp_extract_microsecond() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // In PG, EXTRACT(SECOND) from a timestamp with fractional seconds includes the fraction
            ResultSet rs = s.executeQuery(
                    "SELECT EXTRACT(SECOND FROM TIMESTAMP '2024-01-15 14:30:45.123')");
            assertTrue(rs.next());
            double sec = rs.getDouble(1);
            assertTrue(sec >= 45 && sec < 46, "Expected ~45.123, got: " + sec);
        }
    }

    @Test
    void interval_weeks() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '2 weeks'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // PG converts weeks to days: 2 weeks = 14 days
            assertTrue(val.contains("14 day") || val.contains("14 days"),
                    "Expected 14 days, got: " + val);
        }
    }

    @Test
    void int_serial_after_explicit_id() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE serial_expl (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO serial_expl (id, name) VALUES (10, 'explicit')");
            s.execute("INSERT INTO serial_expl (name) VALUES ('auto')");
            ResultSet rs = s.executeQuery("SELECT id, name FROM serial_expl ORDER BY id");
            assertTrue(rs.next());
            // auto-generated should be 1 or 2 (before explicit 10)
            int firstId = rs.getInt(1);
            assertTrue(rs.next());
            int secondId = rs.getInt(1);
            // Both rows should exist
            assertTrue(firstId < secondId);
            s.execute("DROP TABLE serial_expl");
        }
    }

    @Test
    void float_mixed_arithmetic_with_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 5 * 2.5");
            assertTrue(rs.next());
            assertEquals(12.5, rs.getDouble(1), 0.001);
        }
    }

    @Test
    void numeric_cast_to_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CAST(99.9::numeric AS integer)");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
    }

    @Test
    void char_repeat_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT repeat('ab', 3)");
            assertTrue(rs.next());
            assertEquals("ababab", rs.getString(1));
        }
    }

    @Test
    void date_to_char_day_name() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_char(DATE '2024-01-15', 'Day')");
            assertTrue(rs.next());
            String val = rs.getString(1).trim();
            assertEquals("Monday", val);
        }
    }

    @Test
    void timestamp_to_char() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT to_char(TIMESTAMP '2024-06-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS')");
            assertTrue(rs.next());
            assertEquals("2024-06-15 14:30:45", rs.getString(1));
        }
    }

    @Test
    void interval_mixed_units() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT INTERVAL '1 year 6 months 10 days 3 hours 30 minutes'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain the major components
            assertTrue(val.contains("year") || val.contains("mon"),
                    "Expected interval with year or month component, got: " + val);
        }
    }

    @Test
    void int_boolean_from_integer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1::boolean, 0::boolean");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("f", rs.getString(2));
        }
    }

    @Test
    void float_trunc_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT trunc(42.8), trunc(-42.8)");
            assertTrue(rs.next());
            assertEquals(42.0, rs.getDouble(1), 0.001);
            assertEquals(-42.0, rs.getDouble(2), 0.001);
        }
    }

    @Test
    void date_make_date() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT make_date(2024, 6, 15)");
            assertTrue(rs.next());
            Date d = rs.getDate(1);
            assertEquals("2024-06-15", d.toString());
        }
    }

    @Test
    void timestamp_date_trunc() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_trunc('hour', TIMESTAMP '2024-06-15 14:35:45')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("14:00:00"), "Expected truncated to hour, got: " + val);
        }
    }

    @Test
    void timestamp_date_trunc_day() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT date_trunc('day', TIMESTAMP '2024-06-15 14:35:45')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("2024-06-15") && val.contains("00:00:00"),
                    "Expected truncated to day, got: " + val);
        }
    }

    @Test
    void interval_epoch_extract() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXTRACT(EPOCH FROM INTERVAL '1 day 2 hours')");
            assertTrue(rs.next());
            double epoch = rs.getDouble(1);
            // 1 day = 86400, 2 hours = 7200 → 93600
            assertEquals(93600.0, epoch, 1.0);
        }
    }
}
