package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A branch and an operand are settled by their types, not by the values they turn out to hold.
 *
 * <p>PostgreSQL resolves an expression before it runs one: a prefix operator is chosen from the
 * type its operand is written with, a simple CASE builds one equality per WHEN out of the
 * operand's type and the value's, a CASE settles one result type across its branches, and an IN
 * list and an ANY or ALL settle one type across both sides. memgres was deciding all of these
 * from the values as they came out, so {@code -'abc'::text} answered abc, {@code CASE 1 WHEN 'a'}
 * answered, and the two mismatch messages named a type nobody wrote and an operator spelled out
 * of an enum constant.
 *
 * <p>Only a type the query itself writes down takes part: a column's type here is whatever the
 * engine settled on, and refusing an operator on the strength of that would reject working SQL.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class BranchAndOperandTypeResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_ty (id int, v int, t text)");
            st.execute("INSERT INTO zz_ty VALUES (1, 10, 'a'), (2, 20, 'b')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The one value of the one row a statement answers with, rendered as text. */
    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            String v = rs.getString(1);
            return rs.wasNull() ? null : v;
        }
    }

    /** The first column of every row, rendered as text, in the order they came back. */
    private static List<String> col(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                out.add(rs.wasNull() ? null : v);
            }
        }
        return out;
    }

    private static PSQLException refusalOf(String sql) {
        return assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
    }

    private static String stateOf(String sql) {
        return refusalOf(sql).getSQLState();
    }

    private static String messageOf(String sql) {
        return refusalOf(sql).getServerErrorMessage().getMessage();
    }

    @Test
    void prefixOperatorIsResolvedFromTheTypeItsOperandIsWrittenWith() throws Exception {
        assertEquals("-1", one("SELECT -(1::smallint)"));
        assertEquals("-1", one("SELECT -(1::bigint)"));
        assertEquals("-1", one("SELECT -(1::numeric)"));
        assertEquals("-1.5", one("SELECT -('1.5'::float8)"));
        assertEquals("1", one("SELECT +(1::bigint)"));
        assertEquals("1", one("SELECT @('-1'::int)"));
        assertEquals("2", one("SELECT |/(4::int)"));
        assertEquals("2", one("SELECT ||/(8::int)"));
        assertNull(one("SELECT -(NULL::int)"));
    }

    /** Minus is also declared over a span of time; the others are not. */
    @Test
    void minusIsTheOnePrefixOperatorOverASpanOfTime() throws Exception {
        assertEquals("-1 days", one("SELECT -(interval '1 day')"));
        assertEquals("42883", stateOf("SELECT +(interval '1 day')"));
        assertEquals("42883", stateOf("SELECT @('1 day'::interval)"));
    }

    @Test
    void aTypeThePrefixOperatorHasNoDeclarationOverIsRefused() throws Exception {
        assertEquals("42883", stateOf("SELECT -('abc'::text)"));
        assertEquals("42883", stateOf("SELECT -(NULL::text)"));
        assertEquals("42883", stateOf("SELECT -(true)"));
        assertEquals("42883", stateOf("SELECT -(date '2020-01-01')"));
        assertEquals("42883", stateOf("SELECT -('2020-01-01'::timestamp)"));
        assertEquals("42883",
                stateOf("SELECT -('11111111-1111-1111-1111-111111111111'::uuid)"));
        assertEquals("42883", stateOf("SELECT -('{1,2}'::int[])"));
        assertEquals("42883", stateOf("SELECT -('$1'::money)"));
        assertEquals("42883", stateOf("SELECT -(B'101')"));
        assertEquals("42883", stateOf("SELECT +('abc'::text)"));
        assertEquals("42883", stateOf("SELECT @(true)"));
        assertEquals("42883", stateOf("SELECT |/('4'::text)"));
    }

    /** The complaint names the operand's type, and asks for one cast because there is one operand. */
    @Test
    void thePrefixComplaintNamesTheOperandTypeInTheSingular() throws Exception {
        assertEquals("operator does not exist: - text", messageOf("SELECT -('abc'::text)"));
        assertEquals("operator does not exist: - character varying",
                messageOf("SELECT -('x'::varchar)"));
        assertEquals("operator does not exist: - timestamp with time zone",
                messageOf("SELECT -('2020-01-01'::timestamptz)"));
        assertEquals("operator does not exist: - integer[]", messageOf("SELECT -('{1,2}'::int[])"));
        assertEquals("No operator matches the given name and argument type."
                        + " You might need to add an explicit type cast.",
                refusalOf("SELECT -('abc'::text)").getServerErrorMessage().getHint());
    }

    /**
     * An operand that says nothing about its type leaves the operator to be chosen from the
     * candidates alone, and minus has two families to choose between.
     */
    @Test
    void anUnknownOperandLeavesMinusUndecided() throws Exception {
        assertEquals("42725", stateOf("SELECT -'1'"));
        assertEquals("42725", stateOf("SELECT -'abc'"));
        assertEquals("42725", stateOf("SELECT -(NULL)"));
        assertEquals("operator is not unique: - unknown", messageOf("SELECT -'1'"));
        assertEquals("1", one("SELECT +'1'"));
        assertNull(one("SELECT @(NULL)"));
    }

    /** A column's type is not a type the query wrote down, so it settles nothing. */
    @Test
    void aColumnsTypeSettlesNoOperator() throws Exception {
        assertEquals(java.util.Arrays.asList("-20", "-10"), col("SELECT -v FROM zz_ty ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1", "1"),
                col("SELECT -length(t) * -1 FROM zz_ty ORDER BY 1"));
    }

    @Test
    void simpleCaseComparesItsOperandWithEachValueByType() throws Exception {
        assertEquals("y", one("SELECT CASE 1 WHEN 1 THEN 'y' ELSE 'n' END"));
        assertEquals("n", one("SELECT CASE 1 WHEN 1.5 THEN 'y' ELSE 'n' END"));
        assertEquals("y", one("SELECT CASE 1 WHEN '1' THEN 'y' ELSE 'n' END"));
        assertEquals("n", one("SELECT CASE 'a' WHEN NULL THEN 'y' ELSE 'n' END"));
        assertEquals("22P02", stateOf("SELECT CASE 1 WHEN 'a' THEN 1 ELSE 2 END"));
        assertEquals("invalid input syntax for type bigint: \"a\"",
                messageOf("SELECT CASE 1::bigint WHEN 'a' THEN 1 ELSE 2 END"));
        assertEquals("22P02", stateOf("SELECT CASE true WHEN 'x' THEN 1 ELSE 2 END"));
    }

    /** An operand that says nothing about its type is text, not what the first WHEN turns out to be. */
    @Test
    void anUnknownCaseOperandIsText() throws Exception {
        assertEquals("operator does not exist: text = integer",
                messageOf("SELECT CASE 'a' WHEN 1 THEN 1 ELSE 2 END"));
        assertEquals("operator does not exist: text = numeric",
                messageOf("SELECT CASE 'a' WHEN 1.5 THEN 1 ELSE 2 END"));
        assertEquals("operator does not exist: integer = boolean",
                messageOf("SELECT CASE 1 WHEN true THEN 1 ELSE 2 END"));
        assertEquals("42883", stateOf("SELECT CASE 'a'::text WHEN 1 THEN 1 ELSE 2 END"));
    }

    /** A CASE over a column is compared as it always was: the column's type wrote nothing down. */
    @Test
    void aCaseOverAColumnIsLeftAlone() throws Exception {
        assertEquals(java.util.Arrays.asList("1", "2"),
                col("SELECT CASE t WHEN 'a' THEN 1 ELSE 2 END FROM zz_ty ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1", "2"),
                col("SELECT CASE v WHEN 10 THEN 1 ELSE 2 END FROM zz_ty ORDER BY 1"));
    }

    /** A subquery hands its answer out as a value of some type, so an unknown in it is text. */
    @Test
    void aScalarSubqueryBranchIsSettledByItsSelectList() throws Exception {
        assertEquals("22P02", stateOf("SELECT CASE WHEN 1=1 THEN 'a' ELSE (SELECT 1/0) END"));
        assertEquals("invalid input syntax for type integer: \"a\"",
                messageOf("SELECT CASE WHEN true THEN 'a' ELSE (SELECT 1) END"));
        assertEquals("CASE types text and integer cannot be matched",
                messageOf("SELECT CASE WHEN true THEN 1 ELSE (SELECT 'a') END"));
        assertEquals("CASE types text and integer cannot be matched",
                messageOf("SELECT CASE WHEN true THEN 1 ELSE (SELECT NULL) END"));
        assertEquals("1", one("SELECT CASE WHEN true THEN 1 ELSE NULL END"));
    }

    @Test
    void twoArraysAreMatchedByWhatTheyHold() throws Exception {
        assertEquals("CASE/WHEN could not convert type integer[] to text[]",
                messageOf("SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY['a'] END"));
        assertEquals("CASE/WHEN could not convert type text[] to integer[]",
                messageOf("SELECT CASE WHEN false THEN ARRAY['a'] ELSE ARRAY[1] END"));
        assertEquals("42846",
                stateOf("SELECT CASE WHEN false THEN ARRAY[1] ELSE ARRAY[NULL] END"));
        assertEquals("COALESCE could not convert type text[] to integer[]",
                messageOf("SELECT COALESCE(ARRAY[1], ARRAY['a'])"));
        assertEquals("GREATEST could not convert type text[] to integer[]",
                messageOf("SELECT GREATEST(ARRAY[1], ARRAY['a'])"));
        // An array and a value of its element type are not two arrays, and do not match at all.
        assertEquals("CASE types integer and integer[] cannot be matched",
                messageOf("SELECT CASE WHEN false THEN ARRAY[1] ELSE 1 END"));
    }

    @Test
    void everyEntryOfAnInListIsReadAsTheSettledType() throws Exception {
        assertEquals("22P02", stateOf("SELECT 1 IN ('x')"));
        assertEquals("invalid input syntax for type numeric: \"x\"",
                messageOf("SELECT 1.5 IN ('x')"));
        assertEquals("invalid input syntax for type bigint: \"x\"",
                messageOf("SELECT 1::bigint IN ('x')"));
        // An entry is at fault whether or not an earlier entry already matched.
        assertEquals("22P02", stateOf("SELECT 1 IN (1, 'x')"));
        assertEquals("22P02", stateOf("SELECT 1 NOT IN (1, 'x')"));
        assertEquals("22P02", stateOf("SELECT NULL IN (1, 'x')"));
        // An untyped left side is read as the list's type rather than kept as text.
        assertEquals("invalid input syntax for type integer: \"x\"", messageOf("SELECT 'x' IN (1)"));
        assertEquals("t", one("SELECT '1' IN (1)"));
        assertEquals("f", one("SELECT '1' IN (2)"));
    }

    @Test
    void anInListThatReadsAsTheSettledTypeAnswersAsItAlwaysDid() throws Exception {
        assertEquals("t", one("SELECT 1 IN (1, 2)"));
        assertEquals("f", one("SELECT 3 IN (1, 2)"));
        assertNull(one("SELECT 3 IN (1, NULL)"));
        assertEquals(java.util.Arrays.asList("10", "20"),
                col("SELECT v FROM zz_ty WHERE v IN (10, 20) ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("a", "b"),
                col("SELECT t FROM zz_ty WHERE t IN ('a', 'b') ORDER BY 1"));
    }

    /** The subquery's select list settles what it produces, whether or not it produces a row. */
    @Test
    void anyAndAllSettleBothSidesBeforeComparingAnyOfThem() throws Exception {
        assertEquals("operator does not exist: integer = text",
                messageOf("SELECT 1 = ANY (SELECT 'x')"));
        assertEquals("operator does not exist: integer = text",
                messageOf("SELECT 1 = ANY (SELECT 'x' WHERE false)"));
        assertEquals("operator does not exist: integer = text",
                messageOf("SELECT 1 = ANY (SELECT '1')"));
        assertEquals("operator does not exist: integer = text",
                messageOf("SELECT 1 = ANY (SELECT NULL)"));
        assertEquals("operator does not exist: numeric > text",
                messageOf("SELECT 1.5 > ANY (SELECT 'x')"));
        assertEquals("operator does not exist: integer < text",
                messageOf("SELECT 1 < ALL (SELECT 'x')"));
        assertEquals("operator does not exist: integer <> text",
                messageOf("SELECT 1 <> ANY (SELECT 'x')"));
        assertEquals("operator does not exist: integer >= text",
                messageOf("SELECT 1 >= ANY (SELECT 'x')"));
        assertEquals("operator does not exist: boolean = integer",
                messageOf("SELECT true = ANY (SELECT 1)"));
        assertEquals("invalid input syntax for type integer: \"a\"",
                messageOf("SELECT 'a' = ANY (SELECT 1)"));
    }

    @Test
    void anyAndAllOverOneFamilyCompareAsTheyAlwaysDid() throws Exception {
        assertEquals("f", one("SELECT 1 = ANY (SELECT 1.5)"));
        assertEquals("f", one("SELECT 1 = ANY (SELECT 1 WHERE false)"));
        assertEquals("t", one("SELECT 1 = ALL (SELECT 1 WHERE false)"));
        assertEquals("t", one("SELECT 10 = ANY (SELECT v FROM zz_ty)"));
        assertEquals("f", one("SELECT 30 = ANY (SELECT v FROM zz_ty)"));
        assertEquals("t", one("SELECT 30 > ALL (SELECT v FROM zz_ty)"));
    }
}
