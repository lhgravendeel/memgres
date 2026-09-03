package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What ANY takes, what a constant beside it becomes, and what numeric can hold.
 *
 * <p>{@code = ANY(x)} takes an array: a value that is not one is not a set of anything to compare
 * against. A constant with no type of its own takes the array's element type, and one that does
 * not read as that type is refused rather than compared as text.
 *
 * <p>A float becomes a numeric through the text it prints as, which carries no trailing zero; and
 * a constant whose exponent is past the widest numeric there is names no number at all.
 */
class WhatAValueIsComparedAsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** What the ANY spelling takes on its right is an array. */
    @Test
    void whatAnyTakesOnItsRight() throws SQLException {
        assertEquals("42809", stateOf("SELECT 1 = ANY(1)"));
        assertTrue(messageOf("SELECT 1 = ANY(1)")
                .contains("op ANY/ALL (array) requires array on right side"));
        assertEquals("true", one("SELECT (1 = ANY(ARRAY[1,2]))::text"));
        assertEquals("true", one("SELECT (1 = ANY(ARRAY[1]))::text"));
        // A constant with no type of its own becomes the array it is asked for.
        assertEquals("true", one("SELECT (1 = ANY('{1,2}'))::text"));
        exec("CREATE TABLE zwv_a (a int[])");
        exec("INSERT INTO zwv_a VALUES (ARRAY[1,2])");
        assertEquals("true", one("SELECT (1 = ANY(a))::text FROM zwv_a"));
        assertEquals("false", one("SELECT (5 = ANY(a))::text FROM zwv_a"));
        exec("DROP TABLE zwv_a");
    }

    /** A constant beside an array of numbers is read as one of them. */
    @Test
    void whatAConstantBesideAnArrayBecomes() {
        assertEquals("22P02", stateOf("SELECT 'a' = ANY(ARRAY[1])"));
        assertTrue(messageOf("SELECT 'a' = ANY(ARRAY[1])")
                .contains("invalid input syntax for type integer: \"a\""));
    }

    /** A float becomes a numeric through the text it prints as. */
    @Test
    void whatAFloatBecomesAsANumeric() throws SQLException {
        assertEquals("1", one("SELECT (1::real)::numeric::text"));
        assertEquals("2.5", one("SELECT (2.5::real)::numeric::text"));
        assertEquals("0.1", one("SELECT (0.1::real)::numeric::text"));
        assertEquals("10000000000", one("SELECT (1e10::real)::numeric::text"));
        assertEquals("1", one("SELECT (1::float8)::numeric::text"));
    }

    /** What a CASE or a COALESCE answers with is settled from all its branches. */
    @Test
    void theTypeTheBranchesSettleOn() throws SQLException {
        assertEquals("numeric", one("SELECT pg_typeof(COALESCE(1, 1.5))::text"));
        assertEquals("bigint",
                one("SELECT pg_typeof(CASE WHEN true THEN 1::int2 ELSE 1::int8 END)::text"));
        assertEquals("numeric", one("SELECT pg_typeof(CASE WHEN true THEN 1 ELSE 2.5 END)::text"));
        // A branch with no type of its own says nothing, and where none says anything it is text.
        assertEquals("text", one("SELECT pg_typeof(COALESCE(NULL, NULL))::text"));
        assertEquals("date", one("SELECT pg_typeof(COALESCE(NULL::date, NULL::date))::text"));
        assertEquals("integer", one("SELECT pg_typeof(GREATEST('10', 9))::text"));
    }

    /** How many places a division answers with is taken from what it divided. */
    @Test
    void howManyPlacesADivisionAnswersWith() throws SQLException {
        // Sixteen significant digits of the quotient, and never fewer places than an operand had.
        assertEquals("2.5000000000000000", one("SELECT (10.00 / 4)::text"));
        assertEquals("3333333333.33333333", one("SELECT (1e10::numeric / 3)::text"));
        assertEquals("0.0000000000333333333333333333",
                one("SELECT (1e-10::numeric / 3)::text"));
        assertEquals("0.33333333333333333333", one("SELECT (1::numeric / 3)::text"));
        assertEquals("14.2857142857142857", one("SELECT (100::numeric / 7)::text"));
        // A division of whole numbers is a whole number, and is not this.
        assertEquals("0", one("SELECT (2 / 3)::text"));
    }

    /** A value of a composite type is compared as the row it is. */
    @Test
    void whatARowIsComparedWith() throws SQLException {
        exec("CREATE TYPE zwv_ct AS (a int, b text)");
        exec("CREATE TABLE zwv_t (v zwv_ct)");
        exec("INSERT INTO zwv_t VALUES (ROW(1,'a')::zwv_ct)");
        assertEquals("true", one("SELECT (v = ROW(1,'a')::zwv_ct)::text FROM zwv_t"));
        assertEquals("false", one("SELECT (v = ROW(2,'a')::zwv_ct)::text FROM zwv_t"));
        assertEquals("true", one("SELECT (v < ROW(2,'a')::zwv_ct)::text FROM zwv_t"));
        assertEquals("true",
                one("SELECT ('(1,a)'::zwv_ct = ROW(1,'a')::zwv_ct)::text"));
        // Text written as text is text, and has no operator against a row.
        assertEquals("42883", stateOf("SELECT '(1,a)'::text = ROW(1,'a')::zwv_ct"));
        assertTrue(messageOf("SELECT '(1,a)'::text = ROW(1,'a')::zwv_ct")
                .contains("operator does not exist: text = zwv_ct"));
        exec("DROP TABLE zwv_t");
        exec("DROP TYPE zwv_ct");
    }

    /** A constant beside a row is asked to read itself as one, which a bare row has no name for. */
    @Test
    void whatAConstantBesideARowBecomes() {
        assertEquals("0A000", stateOf("SELECT 'x' = ROW(1,2)"));
        assertTrue(messageOf("SELECT 'x' = ROW(1,2)")
                .contains("input of anonymous composite types is not implemented"));
        assertEquals("0A000", stateOf("SELECT ROW(1,2) < 'x'"));
        assertEquals("0A000", stateOf("SELECT ROW(1,2) IS DISTINCT FROM 'x'"));
        // A constant of a type of its own names no operator at all.
        assertEquals("42883", stateOf("SELECT 'x'::text = ROW(1,2)"));
        assertTrue(messageOf("SELECT 'x'::text = ROW(1,2)")
                .contains("operator does not exist: text = record"));
        assertTrue(messageOf("SELECT 1 = ROW(1)")
                .contains("operator does not exist: integer = record"));
    }

    /** What a value is reported as: money is money, and the standard domains are their bases. */
    @Test
    void whatAValueIsReportedAs() throws SQLException {
        assertEquals("money", one("SELECT pg_typeof('1.00'::money + '1.00'::money)::text"));
        assertEquals("money", one("SELECT pg_typeof('1.00'::money * 2)::text"));
        assertEquals("name", columnTypeOf("SELECT 'x'::information_schema.sql_identifier"));
        assertEquals("varchar", columnTypeOf("SELECT 'x'::information_schema.character_data"));
        assertEquals("int4", columnTypeOf("SELECT 1::information_schema.cardinal_number"));
    }

    private static String columnTypeOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
        }
    }

    /** Running two values together answers with text, and a comparison is resolved against it. */
    @Test
    void whatAConcatenationIsComparedAs() {
        assertEquals("42883", stateOf("SELECT 1 = 1 || 'x'"));
        assertTrue(messageOf("SELECT 1 = 1 || 'x'")
                .contains("operator does not exist: integer = text"));
        assertEquals("42883", stateOf("SELECT 2 > 1 || 'x'"));
        assertEquals("42883", stateOf("SELECT 1 <> 1 || 'x'"));
    }

    /** A number numeric cannot hold is an overflow, whatever it was written as. */
    @Test
    void whatNumericCannotHold() {
        assertEquals("22003", stateOf("SELECT 1e1000000"));
        assertEquals("22003", stateOf("SELECT 1e99999999999999999999"));
        assertEquals("22003", stateOf("SELECT '1e99999999999999999999'::numeric"));
        assertTrue(messageOf("SELECT 1e1000000").contains("value overflows numeric format"));
        // What it can hold, it holds.
        assertEquals("131072", messageOfLength("SELECT length((1e131071::numeric)::text)"));
    }

    private static String messageOfLength(String sql) {
        try {
            return one(sql);
        } catch (SQLException e) {
            return e.getMessage();
        }
    }
}
