package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * numeric's NaN and infinities as stored and aggregated values, the float overflow checks on the
 * paths that store rather than compute, the mathematical domain errors, and the overload an
 * untyped literal resolves to.
 *
 * <p>The last of those decides answers, not only types: PostgreSQL resolves {@code round('2.5')}
 * to {@code round(double precision)}, which rounds half to even and gives 2, while
 * {@code round(2.5::numeric)} rounds half away from zero and gives 3.
 *
 * <p>Every expectation here was measured against PostgreSQL 18.
 */
class NumericSpecialsTest {

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

    /** The value of every column of the single row, joined with '|'. */
    private static String row(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for " + sql);
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                sb.append(rs.getString(i));
            }
            return sb.toString();
        }
    }

    /** The declared type of the first result column, as the wire advertises it. */
    private static String columnType(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
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

    // ================================================================
    // numeric NaN and the infinities as stored values
    // ================================================================

    @Test void a_numeric_column_stores_every_spelling_of_the_specials() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_store");
        exec("CREATE TABLE nsm_store (v numeric)");
        exec("INSERT INTO nsm_store VALUES ('Infinity')");
        exec("INSERT INTO nsm_store VALUES ('-Infinity')");
        exec("INSERT INTO nsm_store VALUES ('inf')");
        exec("INSERT INTO nsm_store VALUES ('-inf')");
        exec("INSERT INTO nsm_store VALUES ('Infinity'::numeric)");
        exec("INSERT INTO nsm_store VALUES (CAST('NaN' AS numeric))");
        exec("INSERT INTO nsm_store SELECT 'Infinity'");
        exec("INSERT INTO nsm_store SELECT 'Infinity'::numeric");
        exec("INSERT INTO nsm_store SELECT 'NaN'::numeric");
        assertEquals("5", one("SELECT count(*)::text FROM nsm_store WHERE v = 'Infinity'"));
        assertEquals("2", one("SELECT count(*)::text FROM nsm_store WHERE v = '-Infinity'"));
        assertEquals("2", one("SELECT count(*)::text FROM nsm_store WHERE v = 'NaN'"));
        exec("DROP TABLE nsm_store");
    }

    @Test void updating_a_numeric_column_to_a_special_leaves_the_row_usable() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_upd");
        exec("CREATE TABLE nsm_upd (id int primary key, v numeric)");
        exec("INSERT INTO nsm_upd VALUES (1, 1)");
        exec("UPDATE nsm_upd SET v = 'Infinity' WHERE id = 1");
        assertEquals("Infinity", one("SELECT v::text FROM nsm_upd WHERE id = 1"));
        exec("UPDATE nsm_upd SET v = 'NaN' WHERE id = 1");
        assertEquals("NaN", one("SELECT v::text FROM nsm_upd WHERE id = 1"));
        // The row is not left locked by a statement that used to fail here
        exec("UPDATE nsm_upd SET v = 2 WHERE id = 1");
        assertEquals("2", one("SELECT v::text FROM nsm_upd WHERE id = 1"));
        exec("DROP TABLE nsm_upd");
    }

    @Test void a_numeric_column_still_refuses_anything_that_is_not_a_number() {
        try {
            exec("DROP TABLE IF EXISTS nsm_bad");
            exec("CREATE TABLE nsm_bad (v numeric)");
        } catch (SQLException e) {
            fail(e);
        }
        state("22P02", "INSERT INTO nsm_bad VALUES ('infin')");
        state("22P02", "INSERT INTO nsm_bad VALUES ('abc')");
        try {
            exec("DROP TABLE nsm_bad");
        } catch (SQLException e) {
            fail(e);
        }
    }

    // ================================================================
    // the numeric functions over the specials
    // ================================================================

    @Test void scale_and_min_scale_have_no_answer_for_a_special() throws Exception {
        assertNull(one("SELECT scale('NaN'::numeric)"));
        assertNull(one("SELECT scale('Infinity'::numeric)"));
        assertNull(one("SELECT min_scale('NaN'::numeric)"));
        assertNull(one("SELECT min_scale('Infinity'::numeric)"));
        assertEquals("NaN", one("SELECT trim_scale('NaN'::numeric)::text"));
        assertEquals("Infinity", one("SELECT trim_scale('Infinity'::numeric)::text"));
        assertEquals("-Infinity", one("SELECT trim_scale('-Infinity'::numeric)::text"));
    }

    @Test void scale_and_min_scale_keep_answering_for_ordinary_numbers() throws Exception {
        assertEquals("3|2|1.23", row("SELECT scale(1.230)::text, min_scale(1.230)::text,"
                + " trim_scale(1.230)::text"));
        assertEquals("1|0|1.1", row("SELECT scale(1.0)::text, min_scale(1.00)::text,"
                + " trim_scale(1.100)::text"));
        assertEquals("0", one("SELECT scale(1)::text"));
        assertEquals("int4", columnType("SELECT scale(1.5)"));
        assertEquals("numeric", columnType("SELECT trim_scale(1.5)"));
        assertEquals("null|null|null",
                row("SELECT scale(NULL::numeric), min_scale(NULL::numeric), trim_scale(NULL::numeric)"));
    }

    @Test void the_variance_of_a_set_holding_a_special_is_not_a_number() throws Exception {
        String values = " FROM (VALUES ('NaN'::numeric),(1::numeric),(2::numeric)) x(v)";
        assertEquals("NaN", one("SELECT var_pop(v)::text" + values));
        assertEquals("NaN", one("SELECT var_samp(v)::text" + values));
        assertEquals("NaN", one("SELECT stddev_pop(v)::text" + values));
        assertEquals("NaN", one("SELECT stddev(v)::text" + values));
        assertEquals("NaN", one("SELECT stddev_samp(v)::text" + values));
        String infinite = " FROM (VALUES ('Infinity'::numeric),(1::numeric),(2::numeric)) x(v)";
        assertEquals("NaN", one("SELECT var_pop(v)::text" + infinite));
        assertEquals("NaN", one("SELECT stddev_pop(v)::text" + infinite));
    }

    @Test void the_variance_aggregates_answer_in_their_argument_type() throws Exception {
        assertEquals("numeric", columnType(
                "SELECT stddev(v) FROM (VALUES ('NaN'::numeric),(1::numeric),(2::numeric)) x(v)"));
        assertEquals("numeric", columnType(
                "SELECT var_pop(v) FROM (VALUES (1::numeric),(2::numeric)) x(v)"));
        assertEquals("float8", columnType(
                "SELECT var_pop(v) FROM (VALUES (1::float8),(2::float8)) x(v)"));
        assertEquals("float8", columnType(
                "SELECT stddev(v) FROM (VALUES (1::float8),(2::float8)) x(v)"));
        assertEquals("numeric",
                one("SELECT pg_typeof(stddev(v))::text"
                        + " FROM (VALUES ('NaN'::numeric),(1::numeric),(2::numeric)) x(v)"));
    }

    @Test void the_variance_aggregates_still_answer_for_ordinary_sets() throws Exception {
        String values = " FROM (VALUES (1::numeric),(2::numeric),(4::numeric)) t(x)";
        assertEquals("1.5555555555555556", one("SELECT var_pop(x)::text" + values));
        assertEquals("2.3333333333333333", one("SELECT var_samp(x)::text" + values));
        assertEquals("1.2472191289246471", one("SELECT stddev_pop(x)::text" + values));
        assertEquals("1.5275252316519467", one("SELECT stddev(x)::text" + values));
    }

    @Test void integer_division_carries_the_specials_through() throws Exception {
        assertEquals("Infinity", one("SELECT div('Infinity'::numeric, 2::numeric)::text"));
        assertEquals("-Infinity", one("SELECT div('-Infinity'::numeric, 2::numeric)::text"));
        assertEquals("NaN", one("SELECT div('NaN'::numeric, 2::numeric)::text"));
        assertEquals("NaN", one("SELECT div(2::numeric, 'NaN'::numeric)::text"));
        assertEquals("0", one("SELECT div(2::numeric, 'Infinity'::numeric)::text"));
        assertEquals("NaN", one("SELECT div('Infinity'::numeric, 'Infinity'::numeric)::text"));
    }

    @Test void integer_division_is_unchanged_for_ordinary_numbers() throws Exception {
        assertEquals("2|-2|-2|0",
                row("SELECT div(9,4)::text, div(-9,4)::text, div(9,-4)::text, div(0,5)::text"));
        assertEquals("numeric", columnType("SELECT div(9,4)"));
        assertEquals("numeric", columnType("SELECT div(9.0,4.0)"));
        state("22012", "SELECT div(1, 0)");
        assertNull(one("SELECT div(NULL::numeric, 2)"));
    }

    @Test void to_char_writes_the_word_for_nan_and_a_full_field_for_an_infinity() throws Exception {
        assertEquals(" NaN", one("SELECT to_char('NaN'::numeric, '999')"));
        assertEquals(" NaN", one("SELECT to_char('NaN'::float8, '999')"));
        assertEquals("  NaN", one("SELECT to_char('NaN'::numeric, '9999')"));
        assertEquals("NaN", one("SELECT to_char('NaN'::numeric, 'FM999')"));
        assertEquals(" NaN", one("SELECT to_char('NaN'::numeric, '999.99')"));
        assertEquals(" ###", one("SELECT to_char('Infinity'::numeric, '999')"));
        assertEquals(" ###", one("SELECT to_char('Infinity'::float8, '999')"));
        assertEquals("-###", one("SELECT to_char('-Infinity'::numeric, '999')"));
        assertEquals("-###", one("SELECT to_char('-Infinity'::float8, '999')"));
        assertEquals(" ###.##", one("SELECT to_char('Infinity'::numeric, '999.99')"));
        assertEquals("###", one("SELECT to_char('Infinity'::numeric, 'FM999')"));
    }

    @Test void to_char_is_unchanged_for_ordinary_numbers() throws Exception {
        assertEquals("  42", one("SELECT to_char(42::numeric, '999')"));
        assertEquals(" -1.5", one("SELECT to_char(-1.5, '99.9')"));
        assertEquals(" ###", one("SELECT to_char(1234::numeric, '999')"));
        assertNull(one("SELECT to_char(NULL::numeric, '999')"));
    }

    @Test void a_bucket_cannot_be_chosen_for_a_nan() {
        assertTrue(state("2201G", "SELECT width_bucket('NaN'::numeric, 0, 10, 5)")
                .contains("operand, lower bound, and upper bound cannot be NaN"));
        state("2201G", "SELECT width_bucket('NaN'::float8, 0, 10, 5)");
        state("2201G", "SELECT width_bucket(1, 'NaN'::numeric, 10, 5)");
        state("2201G", "SELECT width_bucket(1, 0, 'NaN'::numeric, 5)");
    }

    @Test void width_bucket_still_places_ordinary_values() throws Exception {
        assertEquals("2|0|4", row("SELECT width_bucket(5.0,1.0,10.0,3)::text,"
                + " width_bucket(0,1,10,3)::text, width_bucket(11,1,10,3)::text"));
        assertEquals("int4", columnType("SELECT width_bucket(5.0,1.0,10.0,3)"));
        assertEquals("int4", columnType("SELECT width_bucket('5','1','10',3)"));
        assertNull(one("SELECT width_bucket(NULL::float8, 0, 10, 5)"));
    }

    @Test void a_special_has_no_money_to_become() {
        assertTrue(state("0A000", "SELECT 'NaN'::numeric::money").contains("cannot convert NaN to bigint"));
        assertTrue(state("0A000", "SELECT 'Infinity'::numeric::money")
                .contains("cannot convert infinity to bigint"));
    }

    @Test void a_declared_numeric_field_cannot_hold_an_infinity() {
        String message = state("22003", "SELECT 'Infinity'::numeric(10,2)");
        assertTrue(message.contains("numeric field overflow"), message);
        assertTrue(state("22003", "SELECT 1::numeric(5,5)")
                .contains("must round to an absolute value less than 1."));
        assertTrue(state("22023", "SELECT 1::numeric(-1)")
                .contains("NUMERIC precision -1 must be between 1 and 1000"));
        assertTrue(state("22023", "SELECT 1::numeric(1001)")
                .contains("NUMERIC precision 1001 must be between 1 and 1000"));
    }

    @Test void a_numeric_power_of_a_special_answers_in_numeric() throws Exception {
        assertEquals("1", one("SELECT ('Infinity'::numeric ^ 0)::text"));
        assertEquals("numeric", columnType("SELECT 'Infinity'::numeric ^ 0"));
        assertEquals("1|1", row("SELECT power('NaN'::numeric,0)::text,"
                + " power(1::numeric,'NaN'::numeric)::text"));
    }

    // ================================================================
    // real and double overflow on the storing paths
    // ================================================================

    @Test void a_real_column_refuses_a_value_real_cannot_hold() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_real");
        exec("CREATE TABLE nsm_real (id int primary key, r real)");
        assertTrue(state("22003", "INSERT INTO nsm_real VALUES (1, 1e39)")
                .contains("\"1000000000000000000000000000000000000000\" is out of range for type real"));
        state("22003", "INSERT INTO nsm_real VALUES (2, 3.5e38)");
        state("22003", "INSERT INTO nsm_real VALUES (3, '1e39')");
        assertTrue(state("22003", "INSERT INTO nsm_real SELECT 4, 3.4e38::float8*2")
                .contains("value out of range: overflow"));
        // The values real can hold are stored as before
        exec("INSERT INTO nsm_real VALUES (5, 1.5), (6, 3.4e38), (7, 'Infinity'), (8, NULL)");
        assertEquals("1.5|3.4e+38|Infinity|null",
                row("SELECT (SELECT r::text FROM nsm_real WHERE id=5),"
                        + " (SELECT r::text FROM nsm_real WHERE id=6),"
                        + " (SELECT r::text FROM nsm_real WHERE id=7),"
                        + " (SELECT r::text FROM nsm_real WHERE id=8)"));
        exec("DROP TABLE nsm_real");
    }

    @Test void altering_a_column_to_real_refuses_a_value_it_could_not_keep() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_alter");
        exec("CREATE TABLE nsm_alter (d float8)");
        exec("INSERT INTO nsm_alter VALUES (1e39)");
        assertTrue(state("22003", "ALTER TABLE nsm_alter ALTER COLUMN d TYPE real")
                .contains("value out of range: overflow"));
        // The column keeps its type and its value
        assertEquals("1e+39", one("SELECT d::text FROM nsm_alter"));
        exec("DELETE FROM nsm_alter");
        exec("INSERT INTO nsm_alter VALUES (1.5)");
        exec("ALTER TABLE nsm_alter ALTER COLUMN d TYPE real");
        assertEquals("1.5", one("SELECT d::text FROM nsm_alter"));
        exec("DROP TABLE nsm_alter");
    }

    @Test void a_float8_total_that_leaves_the_range_is_reported() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_big");
        exec("CREATE TABLE nsm_big (d float8)");
        exec("INSERT INTO nsm_big VALUES (1.5e308),(1.5e308)");
        assertTrue(state("22003", "SELECT sum(d) FROM nsm_big").contains("value out of range: overflow"));
        state("22003", "SELECT avg(d) FROM nsm_big");
        exec("DROP TABLE nsm_big");
    }

    @Test void ordinary_float_aggregates_answer_in_float8() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_agg");
        exec("CREATE TABLE nsm_agg (d float8, r real, i int, n numeric)");
        exec("INSERT INTO nsm_agg VALUES (1.5, 1.5, 1, 1.5), (2.5, 2.5, 2, 2.5)");
        assertEquals("4|2", row("SELECT sum(d)::text, avg(d)::text FROM nsm_agg"));
        assertEquals("float8", columnType("SELECT sum(d) FROM nsm_agg"));
        assertEquals("float8", columnType("SELECT avg(d) FROM nsm_agg"));
        assertEquals("float4", columnType("SELECT sum(r) FROM nsm_agg"));
        assertEquals("float8", columnType("SELECT avg(r) FROM nsm_agg"));
        assertEquals("int8", columnType("SELECT sum(i) FROM nsm_agg"));
        assertEquals("numeric", columnType("SELECT avg(i) FROM nsm_agg"));
        assertEquals("numeric", columnType("SELECT sum(n) FROM nsm_agg"));
        exec("DROP TABLE nsm_agg");
    }

    @Test void real_averages_answer_in_float8() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_r");
        exec("CREATE TABLE nsm_r (r real)");
        exec("INSERT INTO nsm_r VALUES (3.0e38),(3.0e38)");
        assertEquals("3.0000000054977558e+38", one("SELECT avg(r)::text FROM nsm_r"));
        assertEquals("float8", columnType("SELECT avg(r) FROM nsm_r"));
        assertEquals("1.1559999674581679e+77", one("SELECT power(3.4e38::real, 2)::text"));
        assertEquals("float8", columnType("SELECT power(3.4e38::real, 2)"));
        exec("DROP TABLE nsm_r");
    }

    @Test void a_literal_that_underflows_a_float_type_is_reported() {
        assertTrue(state("22003", "SELECT 1e-400::float8").contains("is out of range for type double precision"));
        assertTrue(state("22003", "SELECT '1e-400'::float8").contains("\"1e-400\" is out of range"));
        assertTrue(state("22003", "SELECT 1e-46::real")
                .contains("\"0.0000000000000000000000000000000000000000000001\" is out of range for type real"));
        assertTrue(state("22003", "SELECT '1e-46'::real").contains("\"1e-46\" is out of range for type real"));
        state("22003", "SELECT 1e-46::numeric::real");
    }

    @Test void the_out_of_range_value_is_written_in_plain_decimal() {
        assertTrue(state("22003", "SELECT 1e39::real")
                .contains("\"1000000000000000000000000000000000000000\" is out of range for type real"));
        assertTrue(state("22003", "SELECT 3.5e38::real")
                .contains("\"350000000000000000000000000000000000000\""));
        assertTrue(state("22003", "SELECT 1e39::numeric::real")
                .contains("\"1000000000000000000000000000000000000000\""));
        assertTrue(state("22003", "SELECT ARRAY[1e39]::real[]")
                .contains("\"1000000000000000000000000000000000000000\""));
        // A float8 that no longer fits float4 is a narrowing, which PG names by the operation
        assertTrue(state("22003", "SELECT 1e39::float8::real").contains("value out of range: overflow"));
        assertTrue(state("22003", "SELECT (1e308::float8 * 10)::real").contains("value out of range: overflow"));
    }

    @Test void the_float_values_inside_the_range_are_untouched() throws Exception {
        assertEquals("1e-46", one("SELECT 1e-46::float8::text"));
        assertEquals("1e-45|1e-44", row("SELECT 1e-45::float4::text, 1e-44::float4::text"));
        assertEquals("1e-05|1e-06", row("SELECT 0.00001::float8::text, 0.000001::float8::text"));
        assertEquals("3.4e+38", one("SELECT 3.4e38::real::text"));
        assertEquals("Infinity|-Infinity|NaN",
                row("SELECT 'Infinity'::real::text, '-Infinity'::real::text, 'NaN'::real::text"));
    }

    // ================================================================
    // the mathematical domain errors
    // ================================================================

    @Test void the_inverse_functions_report_an_argument_outside_their_domain() {
        for (String sql : new String[]{
                "SELECT asind(2)", "SELECT asind(-2)", "SELECT acosd(2)", "SELECT acosd(-2)",
                "SELECT asind(2::float8)", "SELECT acosd(2::float8)",
                "SELECT acosh(0.5)", "SELECT acosh(0.5::float8)", "SELECT acosh(0.999999)",
                "SELECT atanh(2.0)", "SELECT atanh(2.0::float8)", "SELECT atanh(1.0000001)"}) {
            assertTrue(state("22003", sql).contains("input is out of range"), sql);
        }
    }

    @Test void the_inverse_functions_still_answer_inside_their_domain() throws Exception {
        assertEquals("30|60|90|0|45", row("SELECT asind(0.5)::text, acosd(0.5)::text,"
                + " asind(1)::text, acosd(1)::text, atand(1)::text"));
        assertEquals("-90|180|-45|45", row("SELECT asind(-1)::text, acosd(-1)::text,"
                + " atand(-1)::text, atan2d(1,1)::text"));
        assertEquals("1.3169578969248166", one("SELECT acosh(2.0)::text"));
        assertEquals("0.5493061443340549", one("SELECT atanh(0.5)::text"));
        assertEquals("0|0|0", row("SELECT asinh(0)::text, acosh(1)::text, atanh(0)::text"));
        // atanh is infinite at the ends of its domain rather than undefined there
        assertEquals("Infinity|-Infinity", row("SELECT atanh(1.0)::text, atanh(-1.0)::text"));
        assertEquals("float8", columnType("SELECT atanh(1.0)"));
        assertEquals("float8", columnType("SELECT acosh(1::float8)"));
    }

    @Test void cot_and_cotd_exist_and_are_infinite_at_zero() throws Exception {
        assertEquals("Infinity", one("SELECT cot(0)::text"));
        assertEquals("Infinity", one("SELECT cot(0.0)::text"));
        assertEquals("Infinity", one("SELECT cot(0::float8)::text"));
        assertEquals("Infinity", one("SELECT cotd(0)::text"));
        assertEquals("Infinity", one("SELECT cotd(0::float8)::text"));
        assertEquals("0", one("SELECT cotd(90)::text"));
        assertEquals("1", one("SELECT cotd(45)::text"));
        assertEquals("-Infinity", one("SELECT cotd(180)::text"));
        assertEquals("-1", one("SELECT cotd(135)::text"));
        assertEquals("0.6420926159343306", one("SELECT cot(1.0)::text"));
        assertEquals("float8", columnType("SELECT cot(1.0)"));
    }

    @Test void the_degree_trig_functions_are_exact_at_the_quarter_turns() throws Exception {
        assertEquals("Infinity|-Infinity|1|0|0",
                row("SELECT tand(90)::text, tand(270)::text, tand(45)::text,"
                        + " tand(180)::text, tand(360)::text"));
        assertEquals("Infinity", one("SELECT tand(90::float8)::text"));
        assertEquals("0.5|1|0|0.5|0",
                row("SELECT sind(30)::text, sind(90)::text, sind(180)::text,"
                        + " cosd(60)::text, cosd(90)::text"));
        assertEquals("0|1|-1", row("SELECT sind(0)::text, cosd(0)::text, tand(135)::text"));
        assertEquals("NaN|NaN", row("SELECT cot('NaN'::float8)::text, tand('NaN'::float8)::text"));
        assertTrue(state("22003", "SELECT tand('Infinity'::float8)").contains("input is out of range"));
    }

    @Test void float8_exp_reports_both_ends_of_the_range() {
        assertTrue(state("22003", "SELECT exp(1000::float8)").contains("value out of range: overflow"));
        state("22003", "SELECT exp(710::float8)");
        state("22003", "SELECT exp(1000::real)");
        assertTrue(state("22003", "SELECT exp(-1000::float8)").contains("value out of range: underflow"));
        state("22003", "SELECT exp(-746::float8)");
        state("22003", "SELECT power(10::float8, 400)");
        state("22003", "SELECT power(10::float8, -400)");
    }

    @Test void float8_exp_still_answers_inside_the_range() throws Exception {
        assertEquals("8.218407461554972e+307", one("SELECT exp(709::float8)::text"));
        assertEquals("1", one("SELECT exp(0::float8)::text"));
        assertEquals("2.718281828459045", one("SELECT exp(1)::text"));
        assertEquals("Infinity|NaN",
                row("SELECT exp('Infinity'::float8)::text, exp('NaN'::float8)::text"));
    }

    @Test void the_numeric_transcendentals_are_computed_in_numeric() throws Exception {
        assertEquals("2.7182818284590452", one("SELECT exp(1::numeric)::text"));
        assertEquals("22026.465794806717", one("SELECT exp(10::numeric)::text"));
        assertEquals("1.414213562373095", one("SELECT sqrt(2::numeric)::text"));
        assertEquals("0.50000000000000000", one("SELECT sqrt(0.25::numeric)::text"));
        assertEquals("1.4142135623730950", one("SELECT power(2::numeric,0.5)::text"));
        assertEquals("10.0000000000000000", one("SELECT log(2.0,1024.0)::text"));
        assertEquals("3.0000000000000000", one("SELECT log(2.0,8.0)::text"));
        assertEquals("0.000000000000000", one("SELECT sqrt(0.0)::text"));
        assertEquals("1.0000000000000000", one("SELECT power(0::numeric,0)::text"));
        assertEquals("4.0000000000000000", one("SELECT power(-2::numeric,2)::text"));
        assertEquals("100000000000000000000", one("SELECT power(10::numeric,20::numeric)::text"));
        assertEquals("0.6931471805599453", one("SELECT ln(2::numeric)::text"));
        assertEquals("2.3025850929940457", one("SELECT ln(10::numeric)::text"));
        assertEquals("2.0000000000000000", one("SELECT log(100::numeric)::text"));
    }

    @Test void a_numeric_exponential_that_no_numeric_could_hold_is_reported() {
        assertTrue(state("22003", "SELECT exp(6000::numeric)").contains("value overflows numeric format"));
    }

    @Test void a_numeric_logarithm_reaches_below_where_double_underflows() throws Exception {
        // 1e-400 is an ordinary numeric; taking its logarithm in double would first round it
        // to zero and then report a logarithm of zero PG never raises.
        assertEquals("t", one("SELECT ln(1e-400::numeric)::text LIKE '-921.03403719761827%'"));
        assertEquals("t", one("SELECT log(1e-400::numeric)::text = '-400.'||repeat('0',400)"));
        assertEquals("t", one("SELECT exp(-1000::numeric)::text LIKE '0.%5075958897549457'"));
        assertEquals("1002", one("SELECT length(exp(-6000::numeric)::text)::text"));
    }

    @Test void the_logarithm_domain_errors_are_unchanged() {
        assertTrue(state("2201E", "SELECT ln(0::numeric)").contains("cannot take logarithm of zero"));
        state("2201E", "SELECT ln(-1::numeric)");
        state("2201E", "SELECT log(0.0, 10.0)");
        state("2201E", "SELECT log(10.0, 0.0)");
        assertTrue(state("22012", "SELECT log(1.0, 10.0)").contains("division by zero"));
        state("2201F", "SELECT sqrt(-1::numeric)");
        state("2201F", "SELECT sqrt(-1::float8)");
    }

    @Test void the_float8_specials_pass_through_the_logarithms() throws Exception {
        assertEquals("NaN|NaN|Infinity|Infinity",
                row("SELECT ln('NaN'::float8)::text, sqrt('NaN'::float8)::text,"
                        + " sqrt('Infinity'::float8)::text, ln('Infinity'::float8)::text"));
        assertEquals("float8", columnType("SELECT ln('NaN'::float8)"));
        assertEquals("float8", columnType("SELECT sqrt('Infinity'::float8)"));
    }

    @Test void a_float8_of_that_size_prints_in_exponential_notation() throws Exception {
        assertEquals("1.633123935319537e+16", one("SELECT tan(pi()/2)::text"));
        assertEquals("1e+16|1e+15", row("SELECT 1e16::float8::text, 1e15::float8::text"));
        assertEquals("1.2345678901234568e+17", one("SELECT 123456789012345678::float8::text"));
        assertEquals("float8", columnType("SELECT tan(pi()/2)"));
    }

    @Test void the_root_operators_answer_in_float8() throws Exception {
        assertEquals("-2", one("SELECT (||/ -8.0)::text"));
        assertEquals("float8", columnType("SELECT ||/ -8.0"));
        assertEquals("2", one("SELECT (|/ 4.0)::text"));
        assertEquals("float8", columnType("SELECT |/ 4.0"));
        assertEquals("int4", columnType("SELECT @ -5"));
        assertEquals("5", one("SELECT (@ -5)::text"));
        assertEquals("float8", columnType("SELECT 2 ^ 3"));
        assertEquals("float8", columnType("SELECT 2::int8 ^ 63"));
        assertEquals("9.223372036854776e+18", one("SELECT (2::int8 ^ 63)::text"));
    }

    // ================================================================
    // the overload an untyped literal resolves to
    // ================================================================

    @Test void an_untyped_literal_resolves_a_math_function_to_float8() throws Exception {
        assertEquals("2|-2|0|2", row("SELECT round('2.5')::text, round('-2.5')::text,"
                + " round('0.5')::text, round('1.5')::text"));
        assertEquals("float8", columnType("SELECT round('2.5')"));
        assertEquals("double precision", one("SELECT pg_typeof(round('2.5'))::text"));
        assertEquals("5|5|2|8", row("SELECT abs('-5')::text, ceil('4.2')::text,"
                + " sqrt('4')::text, power('2','3')::text"));
        for (String fn : new String[]{"abs('-5')", "ceil('4.2')", "floor('4.2')", "sqrt('4')",
                "power('2','3')", "sign('-4.2')", "exp('1')", "ln('1')", "log('100')", "cbrt('8')"}) {
            assertEquals("double precision", one("SELECT pg_typeof(" + fn + ")::text"), fn);
            assertEquals("float8", columnType("SELECT " + fn), fn);
        }
    }

    @Test void round_on_a_float8_rounds_half_to_even() throws Exception {
        assertEquals("2|4|-2|0", row("SELECT round(2.5::float8)::text, round(3.5::float8)::text,"
                + " round(-2.5::float8)::text, round(0.5::float8)::text"));
        assertEquals("float8", columnType("SELECT round(2.5::float8)"));
        assertEquals("2", one("SELECT round(2.5::float4)::text"));
        assertEquals("float8", columnType("SELECT round(2.5::float4)"));
    }

    @Test void round_on_a_numeric_still_rounds_half_away_from_zero() throws Exception {
        assertEquals("3|4|-3", row("SELECT round(2.5::numeric)::text, round(3.5::numeric)::text,"
                + " round(-2.5::numeric)::text"));
        assertEquals("numeric", columnType("SELECT round(2.5::numeric)"));
        assertEquals("numeric", columnType("SELECT round(2.5)"));
        assertEquals("3", one("SELECT round(2.5)::text"));
        assertEquals("2.68", one("SELECT round(2.675::numeric, 2)::text"));
        assertEquals("numeric", columnType("SELECT round(2.567, 2)"));
        assertNull(one("SELECT round(NULL::numeric)"));
    }

    @Test void an_all_untyped_call_with_no_single_reading_is_refused() {
        assertTrue(state("42725", "SELECT trunc('2.9')").contains("function trunc(unknown) is not unique"));
        assertTrue(state("42725", "SELECT mod('5', '2')")
                .contains("function mod(unknown, unknown) is not unique"));
        assertTrue(state("42725", "SELECT gcd('12', '8')")
                .contains("function gcd(unknown, unknown) is not unique"));
        assertTrue(state("42725", "SELECT lcm('12', '8')")
                .contains("function lcm(unknown, unknown) is not unique"));
    }

    @Test void one_typed_argument_is_enough_to_settle_the_call() throws Exception {
        assertEquals("1|4|2|24", row("SELECT mod('5', 2)::text, gcd('12', 8)::text,"
                + " trunc('2.9'::numeric)::text, lcm('12', 8)::text"));
        assertEquals("int4", columnType("SELECT mod('5', 2)"));
        assertEquals("2|-2|4", row("SELECT trunc(2.9)::text, trunc(-2.9::float8)::text, trunc(4.7)::text"));
        assertEquals("numeric", columnType("SELECT trunc(2.9)"));
        assertEquals("float8", columnType("SELECT trunc(2.9::float8)"));
        assertEquals("1|1|1.0", row("SELECT mod(5,2)::text, mod(5::int8,2::int8)::text,"
                + " mod(5.5::numeric,1.5::numeric)::text"));
        assertEquals("int4", columnType("SELECT mod(5,2)"));
        assertEquals("int8", columnType("SELECT mod(5::int8,2::int8)"));
        assertEquals("numeric", columnType("SELECT mod(5.0,2.0)"));
    }

    @Test void abs_answers_in_the_argument_type_at_every_width() throws Exception {
        assertEquals("9223372036854775807", one("SELECT abs('-9223372036854775807'::int8)::text"));
        assertEquals("int8", columnType("SELECT abs('-9223372036854775807'::int8)"));
        assertEquals("int4", columnType("SELECT abs('-2147483647'::int4)"));
        assertEquals("int2", columnType("SELECT abs('-32767'::int2)"));
        assertEquals("numeric", columnType("SELECT abs(1.5::numeric)"));
        assertEquals("float8", columnType("SELECT abs(1.5::float8)"));
        assertEquals("float4", columnType("SELECT abs(1.5::float4)"));
        assertEquals("bigint", one("SELECT pg_typeof(abs(NULL::int8))::text"));
        assertEquals("smallint", one("SELECT pg_typeof(abs(NULL::int2))::text"));
        assertEquals("double precision", one("SELECT pg_typeof(abs(NULL))::text"));
        state("22003", "SELECT abs(-2147483648::int4)");
    }

    @Test void a_table_built_from_abs_keeps_the_argument_type() throws Exception {
        exec("DROP TABLE IF EXISTS nsm_abs2");
        exec("DROP TABLE IF EXISTS nsm_abs");
        exec("CREATE TABLE nsm_abs (i int)");
        exec("INSERT INTO nsm_abs VALUES (-5)");
        exec("CREATE TABLE nsm_abs2 AS SELECT abs(i) AS x FROM nsm_abs");
        assertEquals("integer", one("SELECT pg_typeof(x)::text FROM nsm_abs2"));
        assertEquals("int4", columnType("SELECT x FROM nsm_abs2"));
        exec("DROP TABLE nsm_abs2");
        exec("DROP TABLE nsm_abs");
    }

    @Test void the_numeric_gcd_has_no_int8_ceiling() throws Exception {
        assertEquals("9223372036854775808",
                one("SELECT gcd('-9223372036854775808'::numeric, 0::numeric)::text"));
        assertEquals("0.5", one("SELECT gcd(1.5::numeric, 0.5::numeric)::text"));
        assertEquals("12", one("SELECT lcm(4::numeric, 6::numeric)::text"));
        assertEquals("numeric", columnType("SELECT gcd(12::numeric, 8::numeric)"));
        // The integer forms keep their own ceilings
        assertEquals("1", one("SELECT gcd('-9223372036854775808'::int8, 1::int8)::text"));
        assertEquals("int8", columnType("SELECT gcd('-9223372036854775808'::int8, 1::int8)"));
        assertEquals("int8", columnType("SELECT lcm(0::int8,0::int8)"));
        assertEquals("int4", columnType("SELECT gcd(12,8)"));
        assertEquals("0|4|0", row("SELECT gcd(0,0)::text, gcd(-12,8)::text, lcm(0,5)::text"));
    }

    @Test void factorial_and_the_scale_functions_answer_in_their_own_types() throws Exception {
        assertEquals("120", one("SELECT factorial('5')::text"));
        assertEquals("numeric", columnType("SELECT factorial('5')"));
        assertEquals("51090942171709440000", one("SELECT factorial(21)::text"));
        assertEquals("numeric", columnType("SELECT factorial(21)"));
    }

    // ================================================================
    // regression guard: the shapes around every new rule must keep working
    // ================================================================

    @Test void the_math_functions_work_over_real_columns_views_and_subqueries() throws Exception {
        exec("DROP VIEW IF EXISTS nsm_v");
        exec("DROP TABLE IF EXISTS nsm_g");
        exec("CREATE TABLE nsm_g (id int, n numeric, d float8, r real, i int)");
        exec("INSERT INTO nsm_g VALUES (1, 2.5, 2.5, 2.5, 3), (2, -2.5, -2.5, -2.5, -3),"
                + " (3, NULL, NULL, NULL, NULL)");

        assertEquals("3|2|2", row("SELECT round(n)::text, round(d)::text, round(r)::text"
                + " FROM nsm_g WHERE id = 1"));
        assertEquals("-3|-2|-2", row("SELECT round(n)::text, round(d)::text, round(r)::text"
                + " FROM nsm_g WHERE id = 2"));
        assertEquals("null|null|null", row("SELECT round(n)::text, round(d)::text, round(r)::text"
                + " FROM nsm_g WHERE id = 3"));
        assertEquals("numeric", columnType("SELECT round(n) FROM nsm_g"));
        assertEquals("float8", columnType("SELECT round(d) FROM nsm_g"));
        assertEquals("float8", columnType("SELECT round(r) FROM nsm_g"));
        assertEquals("int4", columnType("SELECT abs(i) FROM nsm_g"));

        // in WHERE, in ORDER BY, in GROUP BY
        assertEquals("1", one("SELECT id::text FROM nsm_g WHERE round(n) = 3"));
        assertEquals("1", one("SELECT id::text FROM nsm_g WHERE abs(i) >= 3 ORDER BY id"));
        assertEquals("-3", one("SELECT round(n)::text FROM nsm_g ORDER BY round(n) NULLS LAST"));
        assertEquals("-2", one("SELECT round(d)::text FROM nsm_g GROUP BY round(d) ORDER BY 1"));

        // through a derived table, and through a view
        assertEquals("3", one("SELECT sub.rn::text FROM (SELECT round(n) AS rn FROM nsm_g) sub"
                + " WHERE sub.rn >= 3"));
        assertEquals("1", one("SELECT count(*)::text FROM (SELECT row_number() OVER (ORDER BY id) AS rn"
                + " FROM nsm_g) sub WHERE sub.rn >= 3"));
        exec("CREATE VIEW nsm_v AS SELECT id, round(d) AS rd, abs(i) AS ai FROM nsm_g");
        assertEquals("2|3", row("SELECT rd::text, ai::text FROM nsm_v WHERE id = 1"));
        assertEquals("double precision|integer",
                row("SELECT pg_typeof(rd)::text, pg_typeof(ai)::text FROM nsm_v LIMIT 1"));

        // aggregates over the same columns
        assertEquals("0.0|0|0|0", row("SELECT sum(n)::text, sum(d)::text, sum(i)::text,"
                + " avg(d)::text FROM nsm_g"));
        assertEquals("-2.5|2.5|2", row("SELECT min(n)::text, max(n)::text, count(n)::text FROM nsm_g"));

        // plain arithmetic on the same columns
        assertEquals("3.5|5|1.25|1", row("SELECT (n + 1)::text, (d * 2)::text, (r / 2)::text,"
                + " (i % 2)::text FROM nsm_g WHERE id = 1"));
        assertEquals("float8", columnType("SELECT r * 2 FROM nsm_g"));
        assertEquals("float4", columnType("SELECT r * r FROM nsm_g"));

        exec("DROP VIEW nsm_v");
        exec("DROP TABLE nsm_g");
    }

    @Test void the_untouched_neighbours_still_behave() throws Exception {
        assertEquals("4|4.000000000000000|3|-2",
                row("SELECT sqrt(16)::text, sqrt(16.0)::text, cbrt(27)::text, cbrt(-8)::text"));
        assertEquals("-4|-5|-4|-5", row("SELECT ceil(-4.2)::text, floor(-4.2)::text,"
                + " ceil(-4.2::float8)::text, floor(-4.2::float8)::text"));
        assertEquals("0|0|0", row("SELECT sign(0)::text, sign(0.0)::text, sign(-0.0::float8)::text"));
        assertEquals("2|1|-2", row("SELECT round(1.5)::text, round(0.5)::text, round(-1.5)::text"));
        assertEquals("0.3333333333333333", one("SELECT (1::float8/3::float8)::text"));
        assertEquals("0.30000000000000004", one("SELECT (0.1::float8 + 0.2::float8)::text"));
        assertEquals("6", one("SELECT ('5'::int + 1)::text"));
        assertEquals("6", one("SELECT ('5' + 1)::text"));
        assertEquals("5x", one("SELECT '5' || 'x'"));
        assertEquals("null|null|null",
                row("SELECT mod(NULL::int, 2), gcd(NULL::int, 2), lcm(NULL::int, 2)"));
        assertEquals("null|null", row("SELECT abs(NULL::numeric), trunc(NULL::numeric)"));
        state("22003", "SELECT 1e308::float8 * 10");
        state("2201F", "SELECT (-1)::float8 ^ 0.5");
    }
}
