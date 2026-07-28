package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An operator and a cast are chosen from the types the query declares, not from the shape of the
 * values that turn up. Reading the values instead answered {@code '5'::text = 5} with true, gave
 * {@code '2.5'::int} a value at all, and read the literal beside an inet as a point — so a query
 * that is an error in PostgreSQL returned a plausible number here.
 */
class TypeResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE DOMAIN tres_pos AS int CHECK (VALUE > 0)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertState(String expected, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expected, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    // ---- an operator is resolved from the declared types ----

    @Test
    void thereIsNoOperatorAcrossTwoTypeFamilies() {
        assertState("42883", "SELECT '5'::text = 5");
        assertState("42883", "SELECT '5'::text > 4");
        assertState("42883", "SELECT 1::int = true");
        assertState("42883", "SELECT '10'::text + 5");
        assertState("42883", "SELECT '3'::varchar * 2");
        assertState("42883", "SELECT '7'::text / 2");
        assertState("42883", "SELECT 5 - '2'::text");
        assertState("42883", "SELECT '1'::json > '2'::json");
        // json has no comparison operator at all, not even equality; jsonb has the full set
        assertState("42883", "SELECT '1'::json = '1'::json");
    }

    @Test
    void anUndeclaredLiteralStillResolvesAgainstTheOtherSide() throws Exception {
        // '5' is unknown, not text, so PG resolves it as an integer and so must this
        assertEquals("true", scalar("SELECT ('5' = 5)::text"));
        assertEquals("true", scalar("SELECT ('5'::text = '5')::text"));
        assertEquals("true", scalar("SELECT (1 = 1.0)::text"));
        assertEquals("true", scalar("SELECT (1::int8 = 1::int4)::text"));
        assertEquals("2", scalar("SELECT (1 + 1)::text"));
        assertEquals("2024-01-02", scalar("SELECT ('2024-01-01'::date + 1)::text"));
        assertEquals("true", scalar("SELECT ('1'::jsonb = '1'::jsonb)::text"));
        assertEquals("true", scalar("SELECT ('1'::jsonb < '2'::jsonb)::text"));
    }

    @Test
    void aColumnsTypeDoesNotDecideTheOperator() throws Exception {
        // a derived column carries whatever type the engine defaulted it to; refusing an operator
        // on the strength of that would reject SQL PostgreSQL runs
        exec("DROP TABLE IF EXISTS tres_t CASCADE");
        exec("CREATE TABLE tres_t (id int, a text)");
        exec("INSERT INTO tres_t VALUES (1,'x'),(2,'y')");
        assertEquals("2", scalar("SELECT count(*)::text FROM ("
                + " SELECT t.id, row_number() OVER (ORDER BY t.id) AS rn FROM tres_t t) sub"
                + " WHERE sub.rn >= 1"));
        exec("DROP TABLE tres_t CASCADE");
    }

    // ---- an untyped literal is read as the type on the other side ----

    @Test
    void anUntypedLiteralTakesTheOtherOperandsType() throws Exception {
        assertEquals("true", scalar("SELECT ('192.168.1.1'::inet && '192.168.1.0/24')::text"));
        assertEquals("true", scalar("SELECT ('192.168.1.1'::inet << '192.168.1.0/24')::text"));
        assertEquals("true", scalar("SELECT ('10.0.0.0/8'::cidr >> '10.1.2.3')::text"));
        assertEquals("2", scalar("SELECT ('10.1.2.5'::inet - '10.1.2.3')::text"));
        assertEquals("1 day 01:00:00", scalar("SELECT (interval '1 day' + '1 hour')::text"));
    }

    @Test
    void aLiteralTheOtherTypeCannotReadIsRejected() {
        // reading it as that type is also what produces PG's error, rather than a quiet true
        assertState("22P02", "SELECT '((0,0),(2,2))'::box @> '(1,1)'");
        assertState("22P02", "SELECT '<(0,0),5>'::circle @> '(1,1)'");
        assertState("22P02", "SELECT '[2020-01-01,2020-06-01)'::daterange @> '2020-03-01'");
    }

    // ---- integer input ----

    @Test
    void integerInputIsWhatPostgresAccepts() throws Exception {
        assertState("22P02", "SELECT '2.5'::integer");
        assertState("22P02", "SELECT '2.9'::bigint");
        assertState("22P02", "SELECT 'null'::integer");
        assertState("22P02", "SELECT ''::integer");
        assertEquals("42", scalar("SELECT '0x2a'::int::text"));
        assertEquals("42", scalar("SELECT '0o52'::int::text"));
        assertEquals("42", scalar("SELECT '0b101010'::int::text"));
        assertEquals("1000", scalar("SELECT '1_000'::int::text"));
        assertEquals("100", scalar("SELECT '1_0_0'::int::text"));
        assertEquals("42", scalar("SELECT '0X2A'::bigint::text"));
        assertEquals("42", scalar("SELECT ' 42 '::integer::text"));
        assertEquals("42", scalar("SELECT '+42'::integer::text"));
    }

    @Test
    void booleanInputTakesAnyPrefixThatNamesOneWord() throws Exception {
        for (String yes : new String[]{"t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"}) {
            assertEquals("true", scalar("SELECT '" + yes + "'::boolean::text"), yes);
        }
        for (String no : new String[]{"f", "fa", "fals", "false", "n", "no", "of", "off", "0"}) {
            assertEquals("false", scalar("SELECT '" + no + "'::boolean::text"), no);
        }
        // "o" starts both "on" and "off", so it names neither
        assertState("22P02", "SELECT 'o'::boolean");
        assertState("22P02", "SELECT 'tx'::boolean");
        assertState("22P02", "SELECT 'null'::boolean");
    }

    // ---- casts ----

    @Test
    void onlyIntegerConvertsToBoolean() {
        assertState("42846", "SELECT (0.5::numeric)::boolean");
        assertState("42846", "SELECT (1.5::float8)::boolean");
        assertState("42846", "SELECT (1::int8)::boolean");
        assertState("42846", "SELECT (1::int2)::boolean");
    }

    @Test
    void aFloatRoundsHalfToEvenAndIsRangeChecked() throws Exception {
        assertEquals("0", scalar("SELECT (0.5::float8)::int::text"));
        assertEquals("2", scalar("SELECT (1.5::float8)::int::text"));
        assertEquals("2", scalar("SELECT (1.6::float8)::int::text"));
        assertEquals("2", scalar("SELECT (2.5::float8)::int::text"));
        assertEquals("-2", scalar("SELECT (-1.5::float8)::int::text"));
        assertState("22003", "SELECT (1e30::float8)::bigint");
        assertState("22003", "SELECT ('Infinity'::float8)::bigint");
        assertState("22003", "SELECT ('NaN'::float8)::bigint");
        assertState("22003", "SELECT (2147483647.6::float8)::int");
        assertEquals("2147483647", scalar("SELECT (2147483647.4::float8)::int::text"));
        // a numeric still rounds away from zero, which is a different rule from a float's
        assertEquals("1", scalar("SELECT (0.5::numeric)::int::text"));
        assertEquals("3", scalar("SELECT (2.5::numeric)::int::text"));
    }

    // ---- json input ----

    @Test
    void jsonInputIsParsedNotBracketCounted() {
        assertState("22P02", "SELECT '{\"a\": 1} trailing'::json");
        assertState("22P02", "SELECT '{a: 1}'::json");
        assertState("22P02", "SELECT '\"abc'::json");
        assertState("22P02", "SELECT '007'::json");
        assertState("22P02", "SELECT '+1'::json");
        assertState("22P02", "SELECT '1.'::json");
        assertState("22P02", "SELECT '.5'::json");
        assertState("22P02", "SELECT '[1,2] [3]'::jsonb");
        assertState("22P02", "SELECT '{\"a\":1,}'::json");
    }

    @Test
    void theJsonItStillAccepts() throws Exception {
        assertEquals("1e5", scalar("SELECT '1e5'::json::text"));
        assertEquals("-0.5", scalar("SELECT '-0.5'::json::text"));
        assertEquals("true", scalar("SELECT 'true'::json::text"));
        assertEquals("null", scalar("SELECT 'null'::json::text"));
        assertEquals("[]", scalar("SELECT '[]'::json::text"));
        assertEquals("{\"a\": 1}", scalar("SELECT '{\"a\": 1}'::json::text"));
    }

    // ---- IS UNKNOWN and friends read a boolean ----

    @Test
    void aThreeValuedTestNeedsABoolean() throws Exception {
        assertState("42804", "SELECT 1 IS UNKNOWN");
        assertState("42804", "SELECT 1 IS TRUE");
        assertState("42804", "SELECT 1 IS NOT FALSE");
        assertEquals("false", scalar("SELECT (true IS UNKNOWN)::text"));
        assertEquals("true", scalar("SELECT (NULL::boolean IS UNKNOWN)::text"));
        assertEquals("true", scalar("SELECT (true IS TRUE)::text"));
    }

    @Test
    void aDomainCheckStillAppliesOnCast() {
        assertState("23514", "SELECT (-1)::tres_pos");
    }

    // ---- the rest of the branch: what the four follow-up fixes closed ----

    @Test
    void isDistinctFromResolvesTheSameEquality() {
        // IS DISTINCT FROM is a NULL-safe "=", not an operator of its own
        assertState("42883", "SELECT 1 IS DISTINCT FROM 'a'::text");
        assertState("42883", "SELECT 1 IS NOT DISTINCT FROM 'a'::text");
    }

    @Test
    void aTypeWithNoEqualityCannotBeCompared() {
        // point has no "=" at all, so an untyped literal cannot resolve against it either
        assertState("42883", "SELECT '(1,2)' = '(1,2)'::point");
    }

    @Test
    void aBranchListSettlesOnOneType() {
        assertState("42804", "SELECT CASE WHEN true THEN 1 ELSE '2'::text END");
        assertState("42804", "SELECT GREATEST('10'::text, 9)");
        // an untyped literal is coerced to the type already established, and fails on its input
        assertState("22P02", "SELECT COALESCE(1::int, 'x')");
    }

    @Test
    void rowComparisonNeedsEqualArity() {
        assertState("42601", "SELECT ROW(1,2) < ROW(1,2,3)");
        assertState("42601", "SELECT ROW(1,2) = ROW(1,2,3)");
    }

    @Test
    void nullifTakesANullSecondArgument() throws Exception {
        assertEquals("1", scalar("SELECT NULLIF(1, NULL)::text"));
        assertEquals("1.5", scalar("SELECT NULLIF(1.5, NULL)::text"));
        assertEquals("2", scalar("SELECT (NULLIF(1, NULL) + 1)::text"));
    }

    @Test
    void isnullAndNotnullArePostfixSpellings() throws Exception {
        assertEquals("false", scalar("SELECT (1 ISNULL)::text"));
        assertEquals("true", scalar("SELECT (1 NOTNULL)::text"));
        assertEquals("true", scalar("SELECT (NULL::int ISNULL)::text"));
        assertEquals("false", scalar("SELECT (NULL::int NOTNULL)::text"));
    }

    @Test
    void anArrayLiteralHasToBeBraced() throws Exception {
        assertState("22P02", "SELECT ARRAY[1,2] || '3'");
        assertState("22P02", "SELECT ARRAY['a','b'] || 'c'");
        // a braced one still concatenates, including two-dimensionally
        assertEquals("{{1,2},{3,4},{5,6}}", scalar("SELECT (ARRAY[[1,2],[3,4]] || '{5,6}')::text"));
        assertEquals("{1,2,3}", scalar("SELECT (ARRAY[1,2] || 3)::text"));
        assertEquals("{1,2,3}", scalar("SELECT (ARRAY[1,2] || ARRAY[3])::text"));
    }

    @Test
    void aPolymorphicArgumentCannotComeFromAnUnknownLiteral() throws Exception {
        assertState("42804", "SELECT array_to_string('{1,2,3}', '-')");
        assertState("42804", "SELECT array_length('{1,2,3}', 1)");
        assertState("42804", "SELECT cardinality('{1,2,3}')");
        // the same calls with a real array keep working
        assertEquals("1-2-3", scalar("SELECT array_to_string(ARRAY[1,2,3], '-')"));
        assertEquals("3", scalar("SELECT array_length(ARRAY[1,2,3], 1)::text"));
        assertEquals("1-2-3", scalar("SELECT array_to_string('{1,2,3}'::int[], '-')"));
    }

    @Test
    void aCallIsResolvedFromTheTypesTheQueryWrote() throws Exception {
        exec("DROP FUNCTION IF EXISTS tres_num(int)");
        exec("DROP FUNCTION IF EXISTS tres_num(double precision)");
        exec("CREATE FUNCTION tres_num(int) RETURNS text LANGUAGE sql AS $$ SELECT 'int' $$");
        exec("CREATE FUNCTION tres_num(double precision) RETURNS text"
                + " LANGUAGE sql AS $$ SELECT 'float' $$");
        try {
            // an unknown literal and a numeric literal both prefer the float candidate
            assertEquals("float", scalar("SELECT tres_num('6')"));
            assertEquals("float", scalar("SELECT tres_num(6.0)"));
            assertEquals("int", scalar("SELECT tres_num(6)"));
        } finally {
            exec("DROP FUNCTION IF EXISTS tres_num(int)");
            exec("DROP FUNCTION IF EXISTS tres_num(double precision)");
        }
    }

    @Test
    void rangeOperatorsResolveFromTheDeclaredRangeType() throws Exception {
        assertEquals("[2.0,3.0)", scalar("SELECT ('[1.0,3.0)'::numrange * '[2.0,5.0)')::text"));
        assertEquals("true", scalar("SELECT ('[1,3)'::int4range && '[2,5)')::text"));
        // a multirange operand needs a multirange literal beside it
        assertState("22P02", "SELECT '{[1,3),[5,7)}'::int4multirange @> '[1,2)'");
        assertEquals("true", scalar("SELECT ('{[1,3)}'::int4multirange @> '{[1,2)}')::text"));
    }

    @Test
    void tsqueryMatchesAVectorEitherWayRound() throws Exception {
        assertEquals("true", scalar("SELECT ('a'::tsquery @@ to_tsvector('simple','a b'))::text"));
        assertEquals("true", scalar("SELECT (to_tsvector('simple','a b') @@ 'a'::tsquery)::text"));
        assertEquals("true",
                scalar("SELECT ('a & b'::tsquery @@ to_tsvector('simple','a b'))::text"));
    }

    @Test
    void jsonbKeyExistenceFunctionsExist() throws Exception {
        assertEquals("true",
                scalar("SELECT jsonb_exists_any('{\"a\":1,\"b\":2}'::jsonb, '{a,z}')::text"));
    }

    @Test
    void aCheckAddedByAlterDomainAppliesOnCast() throws Exception {
        exec("DROP DOMAIN IF EXISTS tres_d3 CASCADE");
        exec("CREATE DOMAIN tres_d3 AS int");
        exec("ALTER DOMAIN tres_d3 ADD CHECK (VALUE > 0)");
        try {
            assertState("23514", "SELECT (-5)::tres_d3");
            assertEquals("5", scalar("SELECT (5)::tres_d3::text"));
        } finally {
            exec("DROP DOMAIN IF EXISTS tres_d3 CASCADE");
        }
    }

    @Test
    void textThatIsNoDateAtAllIsAnInputSyntaxError() {
        // a field can only be out of range if there was a field; "null" never had one
        assertState("22007", "SELECT 'null'::date");
    }

    @Test
    void inAndAnyResolveTheSameEqualityToo() throws Exception {
        // "= ANY(array)" is parsed as an IN, and IN resolves the operand type's "=",
        // so a type without one cannot be written any of these ways
        assertState("42883", "SELECT '(1,2)'::point = ANY(ARRAY['(1,2)'::point])");
        assertState("42883", "SELECT '(1,2)'::point = ANY('{\"(1,2)\"}'::point[])");
        assertState("42883", "SELECT '(1,2)'::point IN ('(1,2)'::point)");
        assertState("42883", "SELECT '(1,2)'::point = ALL(ARRAY['(1,2)'::point])");
        assertState("42883", "SELECT polygon '((0,0),(1,1))' IN (polygon '((0,0),(1,1))')");
        // NOT IN is not the negation of IN: PG expands it to "<> ALL", and a point has "<>"
        assertEquals("false", scalar("SELECT ('(1,2)'::point NOT IN ('(1,2)'::point))::text"));
        assertEquals("false", scalar("SELECT ('(1,2)'::point <> ANY(ARRAY['(1,2)'::point]))::text"));
    }

    @Test
    void ordinaryInAndAnyAreUntouched() throws Exception {
        assertEquals("true", scalar("SELECT (1 = ANY(ARRAY[1,2]))::text"));
        assertEquals("true", scalar("SELECT (1 IN (1,2))::text"));
        assertEquals("true", scalar("SELECT (3 NOT IN (1,2))::text"));
        assertEquals("true", scalar("SELECT ('a'::text IN ('a','b'))::text"));
        assertEquals("true", scalar("SELECT (1 = ALL(ARRAY[1,1]))::text"));
        assertEquals("true",
                scalar("SELECT ('2024-01-01'::date IN ('2024-01-01'::date))::text"));
        assertEquals("2", scalar("SELECT count(*)::text FROM (VALUES (1),(2)) v(x)"
                + " WHERE v.x IN (1,2)"));
    }
}
