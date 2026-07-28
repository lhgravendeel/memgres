package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A value that leaves its type's range must be reported, not rounded into a plausible wrong
 * number. Covers numeric's NaN and infinities, the two's-complement boundary where negation and
 * absolute value have no result, real and double arithmetic that overflows or underflows,
 * numeric's typmod bounds, the mathematical domain errors, and the declared-size limits on
 * char/varchar/bit and on an array's dimensions.
 *
 * <p>Every expectation here was measured against PostgreSQL 18.
 */
class NumericLimitsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers ----

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Asserts the statement fails with the given SQLSTATE, and returns the message. */
    private static String state(String expected, String sql) {
        try {
            exec(sql);
            return fail("expected " + expected + " for " + sql);
        } catch (SQLException e) {
            assertEquals(expected, e.getSQLState(), sql + " -> " + e.getMessage());
            return e.getMessage();
        }
    }

    // ---- numeric NaN and the infinities ----

    @Test void numeric_reads_the_special_values() throws Exception {
        assertEquals("Infinity", one("SELECT 'Infinity'::numeric"));
        assertEquals("-Infinity", one("SELECT '-Infinity'::numeric"));
        assertEquals("Infinity", one("SELECT 'inf'::numeric"));
        assertEquals("-Infinity", one("SELECT '-inf'::numeric"));
        assertEquals("Infinity", one("SELECT 'INFINITY'::numeric"));
        assertEquals("Infinity", one("SELECT '+Infinity'::numeric"));
        assertEquals("NaN", one("SELECT 'NaN'::numeric"));
        assertEquals("NaN", one("SELECT 'nan'::numeric"));
    }

    @Test void numeric_still_rejects_anything_else() {
        state("22P02", "SELECT 'infin'::numeric");
        state("22P02", "SELECT 'abc'::numeric");
    }

    @Test void arithmetic_follows_the_ieee_rules() throws Exception {
        assertEquals("Infinity", one("SELECT 'Infinity'::numeric + 1"));
        assertEquals("Infinity", one("SELECT 'Infinity'::numeric / 2"));
        assertEquals("NaN", one("SELECT 'Infinity'::numeric - 'Infinity'::numeric"));
        assertEquals("NaN", one("SELECT 'Infinity'::numeric * 0"));
        assertEquals("NaN", one("SELECT 'NaN'::numeric + 1"));
        assertEquals("NaN", one("SELECT 'Infinity'::numeric % 2"));
        assertEquals("NaN", one("SELECT mod('NaN'::numeric, 2)"));
        assertEquals("NaN", one("SELECT sign('NaN'::numeric)"));
        assertEquals("-1", one("SELECT sign('-Infinity'::numeric)"));
        assertEquals("Infinity", one("SELECT abs('-Infinity'::numeric)"));
    }

    @Test void comparison_puts_nan_above_everything() throws Exception {
        assertEquals("t", one("SELECT '-Infinity'::numeric < 'NaN'::numeric"));
        assertEquals("t", one("SELECT 'Infinity'::numeric < 'NaN'::numeric"));
        assertEquals("t", one("SELECT 'NaN'::numeric = 'NaN'::numeric"));
        assertEquals("t", one("SELECT '-Infinity'::numeric < 1"));
        assertEquals("t", one("SELECT 'Infinity'::numeric > 1"));
    }

    @Test void aggregation_carries_the_special_values_through() throws Exception {
        assertEquals("NaN", one("SELECT sum(v) FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v)"));
        assertEquals("Infinity",
                one("SELECT sum(v) FROM (VALUES ('Infinity'::numeric),(1::numeric)) x(v)"));
        assertEquals("NaN",
                one("SELECT sum(v) FROM (VALUES ('Infinity'::numeric),('-Infinity'::numeric)) x(v)"));
        assertEquals("Infinity",
                one("SELECT avg(v) FROM (VALUES ('Infinity'::numeric),(1::numeric)) x(v)"));
        assertEquals("NaN", one("SELECT max(v) FROM (VALUES ('NaN'::numeric),(1::numeric)) x(v)"));
    }

    @Test void a_typmod_takes_nan_but_not_an_infinity() throws Exception {
        assertEquals("NaN", one("SELECT 'NaN'::numeric(10,2)"));
        assertTrue(state("22003", "SELECT 'Infinity'::numeric(10,2)").contains("numeric field overflow"));
    }

    @Test void no_special_numeric_has_an_integer_form() {
        assertTrue(state("0A000", "SELECT 'NaN'::numeric::int").contains("cannot convert NaN to integer"));
        assertTrue(state("0A000", "SELECT 'NaN'::numeric::int8").contains("cannot convert NaN to bigint"));
        assertTrue(state("0A000", "SELECT 'NaN'::numeric::int2").contains("cannot convert NaN to smallint"));
        assertTrue(state("0A000", "SELECT 'Infinity'::numeric::int").contains("cannot convert infinity to integer"));
        assertTrue(state("0A000", "SELECT '-Infinity'::numeric::bigint").contains("cannot convert infinity to bigint"));
        state("0A000", "SELECT ('Infinity'::numeric + 1)::int");
        state("0A000", "SELECT sum(v)::int FROM (VALUES ('NaN'::numeric)) x(v)");
    }

    @Test void the_same_value_from_float8_is_a_range_error_instead() {
        state("22003", "SELECT 'NaN'::float8::int");
        state("22003", "SELECT 'Infinity'::float8::int");
    }

    @Test void ordinary_numeric_casts_are_untouched() throws Exception {
        assertEquals("2", one("SELECT 1.9::numeric::int"));
        assertEquals("3", one("SELECT 2.5::numeric::int"));
        assertEquals("2", one("SELECT 1.9::float8::int"));
    }

    // ---- the two's-complement boundary ----

    @Test void abs_and_negation_overflow_at_the_minimum() {
        assertTrue(state("22003", "SELECT abs('-9223372036854775808'::int8)").contains("bigint out of range"));
        assertTrue(state("22003", "SELECT -('-9223372036854775808'::int8)").contains("bigint out of range"));
        assertTrue(state("22003", "SELECT abs('-2147483648'::int4)").contains("integer out of range"));
        assertTrue(state("22003", "SELECT -('-2147483648'::int4)").contains("integer out of range"));
        assertTrue(state("22003", "SELECT abs('-32768'::int2)").contains("smallint out of range"));
        assertTrue(state("22003", "SELECT -('-32768'::int2)").contains("smallint out of range"));
        assertTrue(state("22003", "SELECT @ '-2147483648'::int4").contains("integer out of range"));
    }

    @Test void one_step_inside_the_boundary_is_unaffected() throws Exception {
        assertEquals("32767", one("SELECT abs('-32767'::int2)"));
        assertEquals("2147483647", one("SELECT abs('-2147483647'::int4)"));
        assertEquals("9223372036854775807", one("SELECT abs(-9223372036854775807::int8)"));
        assertEquals("5", one("SELECT abs(-5)"));
        assertEquals("5", one("SELECT -(-5)"));
        assertEquals("5", one("SELECT @ (-5)"));
        assertEquals("2.5", one("SELECT abs(-2.5::numeric)"));
        assertEquals("2.5", one("SELECT abs(-2.5::float8)"));
    }

    @Test void gcd_and_lcm_carry_the_same_boundary() {
        assertTrue(state("22003", "SELECT gcd('-9223372036854775808'::int8, 0::int8)")
                .contains("bigint out of range"));
        assertTrue(state("22003", "SELECT gcd('-2147483648'::int4, 0::int4)")
                .contains("integer out of range"));
        assertTrue(state("22003", "SELECT lcm('-9223372036854775808'::int8, 1::int8)")
                .contains("bigint out of range"));
    }

    @Test void ordinary_gcd_and_lcm_still_answer() throws Exception {
        assertEquals("4", one("SELECT gcd(12, 8)"));
        assertEquals("4", one("SELECT gcd(-12, 8)"));
        assertEquals("0", one("SELECT gcd(0, 0)"));
        assertEquals("12", one("SELECT lcm(4, 6)"));
        assertEquals("12", one("SELECT lcm(-4, 6)"));
        assertEquals("0", one("SELECT lcm(0, 5)"));
    }

    // ---- real and double range ----

    @Test void real_arithmetic_overflows_rather_than_widening() {
        state("22003", "SELECT 3.4e38::real * 2::real");
        state("22003", "SELECT 3.0e38::real + 3.0e38::real");
        state("22003", "SELECT (-3.0e38)::real - 3.0e38::real");
        state("22003", "SELECT 3.4e38::real / 0.5::real");
        state("22003", "SELECT 1.0e38::real * 1.0e38::real");
    }

    @Test void a_result_that_vanishes_on_the_way_down_underflows() {
        assertTrue(state("22003", "SELECT 1.0e-38::real * 1.0e-38::real")
                .contains("value out of range: underflow"));
        assertTrue(state("22003", "SELECT 1e-308::float8 * 1e-308::float8")
                .contains("value out of range: underflow"));
    }

    @Test void a_real_result_keeps_reals_precision() throws Exception {
        assertEquals("0.33333334", one("SELECT (1.0::real / 3.0::real)::text"));
        assertEquals("0.3", one("SELECT (0.1::real + 0.2::real)::text"));
        assertEquals("3.4e+38", one("SELECT (3.4e38::real + 1::real)::text"));
        assertEquals("2", one("SELECT 1.0::real * 2.0::real"));
    }

    @Test void an_infinite_operand_is_not_an_overflow() throws Exception {
        assertEquals("Infinity", one("SELECT 'Infinity'::real * 2::real"));
        assertEquals("Infinity", one("SELECT 'Infinity'::float8 + 1"));
    }

    @Test void an_exactly_zero_result_has_not_underflowed() throws Exception {
        assertEquals("0", one("SELECT 1.0::real - 1.0::real"));
        assertEquals("0", one("SELECT 1.0::float8 - 1.0::float8"));
        assertEquals("0", one("SELECT 0.0::float8 * 5.0::float8"));
        assertEquals("0", one("SELECT 0.0::real * 5.0::real"));
    }

    @Test void float_division_reports_a_zero_divisor() {
        assertTrue(state("22012", "SELECT 2::float8 / 0").contains("division by zero"));
        state("22012", "SELECT 1.0::real / 0.0::real");
        state("22012", "SELECT 'Infinity'::float8 / 0");
        state("22012", "SELECT 2::numeric / 0");
        state("22012", "SELECT 2 / 0");
        state("22012", "SELECT 2 % 0");
    }

    @Test void a_nan_dividend_still_yields_nan() throws Exception {
        assertEquals("NaN", one("SELECT 'NaN'::float8 / 0"));
    }

    @Test void a_real_total_is_a_real_and_can_overflow() throws Exception {
        exec("CREATE TABLE nlt_real (r real)");
        try {
            exec("INSERT INTO nlt_real VALUES (3.0e38), (3.0e38)");
            state("22003", "SELECT sum(r) FROM nlt_real");
            // The average of the same column is a double and stays in range
            assertEquals("3.0000000054977558e+38", one("SELECT avg(r)::text FROM nlt_real"));
        } finally {
            exec("DROP TABLE nlt_real");
        }
    }

    @Test void ordinary_real_aggregates_are_unaffected() throws Exception {
        assertEquals("3", one("SELECT sum(x) FROM (VALUES (1::real),(2::real)) t(x)"));
        assertEquals("1.5", one("SELECT avg(x) FROM (VALUES (1::real),(2::real)) t(x)"));
        assertEquals("2", one("SELECT max(x) FROM (VALUES (1::real),(2::real)) t(x)"));
        assertEquals("4", one("SELECT sum(x) FROM (VALUES (1.5::float8),(2.5::float8)) t(x)"));
    }

    // ---- numeric typmod bounds ----

    @Test void numeric_precision_and_scale_have_bounds() {
        assertTrue(state("22023", "SELECT 1::numeric(1001,0)")
                .contains("NUMERIC precision 1001 must be between 1 and 1000"));
        assertTrue(state("22023", "SELECT 1::numeric(0,0)")
                .contains("NUMERIC precision 0 must be between 1 and 1000"));
        assertTrue(state("22023", "SELECT 1::numeric(5,1001)")
                .contains("NUMERIC scale 1001 must be between -1000 and 1000"));
    }

    @Test void a_scale_wider_than_the_precision_leaves_a_fractional_field() throws Exception {
        state("22003", "SELECT 1::numeric(5,10)");
        state("22003", "SELECT 0.00001::numeric(5,10)");
        assertEquals("0.0000010000", one("SELECT 0.000001::numeric(5,10)"));
    }

    @Test void the_numeric_bounds_themselves_are_accepted() throws Exception {
        assertEquals("1", one("SELECT 1::numeric(1000,0)"));
        assertEquals("1.00", one("SELECT 1::numeric(5,2)"));
        assertEquals("0", one("SELECT 1::numeric(5,-2)"));
        assertEquals("12300", one("SELECT 12345::numeric(5,-2)"));
        state("22003", "SELECT 12345.6::numeric(5,2)");
    }

    // ---- mathematical domain errors ----

    @Test void a_zero_base_with_a_negative_power_is_undefined() {
        assertTrue(state("2201F", "SELECT power(0::numeric, -1)")
                .contains("zero raised to a negative power is undefined"));
        state("2201F", "SELECT power(0::float8, -1)");
        state("2201F", "SELECT 0::float8 ^ -1");
        state("2201F", "SELECT 0::numeric ^ -1");
    }

    @Test void a_negative_base_with_a_fractional_power_is_complex() {
        assertTrue(state("2201F", "SELECT power(-1::numeric, 0.5)")
                .contains("a negative number raised to a non-integer power"));
        state("2201F", "SELECT power(-1::float8, 0.5)");
    }

    @Test void logarithms_are_defined_only_on_the_positive_reals() {
        assertTrue(state("2201E", "SELECT ln(0::numeric)").contains("cannot take logarithm of zero"));
        assertTrue(state("2201E", "SELECT ln(-1::numeric)")
                .contains("cannot take logarithm of a negative number"));
        state("2201E", "SELECT ln(0::float8)");
        state("2201E", "SELECT ln(-1::float8)");
        state("2201E", "SELECT log(0::numeric)");
        state("2201E", "SELECT log(-1::numeric)");
        state("2201E", "SELECT log10(0::numeric)");
        state("2201E", "SELECT log10(-1::numeric)");
        state("2201E", "SELECT log(0.0, 10.0)");
        state("2201E", "SELECT log(10.0, 0.0)");
    }

    @Test void a_base_of_one_makes_log_divide_by_zero() {
        assertTrue(state("22012", "SELECT log(1.0, 10.0)").contains("division by zero"));
    }

    @Test void sqrt_and_the_inverse_trig_functions_have_domains() {
        state("2201F", "SELECT sqrt(-1::numeric)");
        state("2201F", "SELECT sqrt(-1::float8)");
        assertTrue(state("22003", "SELECT asin(2::float8)").contains("input is out of range"));
        state("22003", "SELECT acos(2::float8)");
        state("22003", "SELECT asin('Infinity'::float8)");
    }

    @Test void everything_inside_the_domain_still_answers() throws Exception {
        assertEquals("1", one("SELECT power(0::numeric, 0)"));
        assertEquals("8", one("SELECT power(2::numeric, 3)"));
        assertEquals("-8", one("SELECT power(-2::numeric, 3)"));
        assertEquals("1", one("SELECT power('NaN'::float8, 0::float8)"));
        assertEquals("1024", one("SELECT 2 ^ 10"));
        assertEquals("0", one("SELECT ln(1)"));
        assertEquals("2", one("SELECT log(100)"));
        assertEquals("2", one("SELECT log10(100)"));
        assertEquals("10", one("SELECT log(2, 1024)"));
        assertEquals("3", one("SELECT sqrt(9)"));
        assertEquals("0", one("SELECT asin(0)"));
        assertEquals("0", one("SELECT acos(1)"));
        assertEquals("NaN", one("SELECT acos('NaN'::float8)"));
    }

    // ---- declared-size limits ----

    @Test void a_character_length_outside_the_typmod_range_is_refused() {
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a varchar(10485761))")
                .contains("length for type varchar cannot exceed 10485760"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a varchar(0))")
                .contains("length for type varchar must be at least 1"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a char(0))")
                .contains("length for type char must be at least 1"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a char(10485761))")
                .contains("length for type char cannot exceed 10485760"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a bit(0))")
                .contains("length for type bit must be at least 1"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a bit varying(0))")
                .contains("length for type varbit must be at least 1"));
    }

    @Test void a_declared_numeric_or_float_precision_is_bounded_too() {
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a numeric(1001,2))")
                .contains("NUMERIC precision 1001 must be between 1 and 1000"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a float(54))")
                .contains("precision for type float must be less than 54 bits"));
        assertTrue(state("22023", "CREATE TABLE nlt_bad (a float(0))")
                .contains("precision for type float must be at least 1 bit"));
    }

    @Test void a_casts_type_modifier_is_checked_the_same_way() {
        state("22023", "SELECT '1'::varchar(10485761)");
        state("22023", "SELECT '1'::varchar(0)");
        state("22023", "SELECT '1'::char(0)");
        state("22023", "SELECT '1'::bit(0)");
        state("22023", "SELECT 1::float(54)");
        state("22023", "SELECT 1::numeric(1001,2)");
    }

    @Test void every_modifier_inside_the_limits_still_declares() throws Exception {
        exec("CREATE TABLE nlt_ok (a varchar(10), b char(5), c bit(4), d bit varying(8),"
                + " e numeric(1000,0), f numeric(5,2), g float(53), h float(24))");
        try {
            exec("INSERT INTO nlt_ok VALUES ('abc', 'xy', '1010', '1100', 7, 1.25, 1.5, 2.5)");
            assertEquals("abc", one("SELECT a FROM nlt_ok"));
            assertEquals("xy   ", one("SELECT b FROM nlt_ok"));
            assertEquals("1010", one("SELECT c FROM nlt_ok"));
            assertEquals("1.25", one("SELECT f FROM nlt_ok"));
            // The declared length still bounds the value, as it always did
            assertTrue(state("22001", "INSERT INTO nlt_ok VALUES ('abcdefghijk', 'xy', '1010',"
                    + " '1100', 7, 1.25, 1.5, 2.5)").contains("value too long"));
        } finally {
            exec("DROP TABLE nlt_ok");
        }
        assertEquals("1", one("SELECT '1'::varchar(5)"));
        assertEquals("1", one("SELECT 1::float(53)"));
        assertEquals("1", one("SELECT 1::float(24)"));
    }

    // ---- array dimensions ----

    @Test void an_array_carries_at_most_six_dimensions() {
        assertTrue(state("54000", "SELECT array_ndims(ARRAY[[[[[[[1]]]]]]])")
                .contains("number of array dimensions (7) exceeds the maximum allowed (6)"));
    }

    @Test void six_dimensions_and_fewer_still_build() throws Exception {
        assertEquals("6", one("SELECT array_ndims(ARRAY[[[[[[1]]]]]])"));
        assertEquals("1", one("SELECT array_ndims(ARRAY[1,2,3])"));
        assertEquals("2", one("SELECT array_ndims(ARRAY[[1,2],[3,4]])"));
    }
}
