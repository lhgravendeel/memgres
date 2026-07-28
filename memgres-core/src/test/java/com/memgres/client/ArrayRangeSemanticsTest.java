package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL decides an array's shape from its braces before it reads a single element, and refuses
 * anything whose braces do not describe a rectangle. Reading the text loosely did not merely accept
 * what PostgreSQL rejects: {@code {"a""b"}} loaded as two elements and {@code {foo,,bar}} gained an
 * empty one, so the array in the database was not the array in the file.
 *
 * <p>On the range side the operators are resolved from the declared types, so {@code *}, {@code +}
 * and {@code -} answer in the range type rather than in an integer the driver could not read, and a
 * containment test takes exactly the type its range is built over.
 */
class ArrayRangeSemanticsTest {

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

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertValue(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class,
                () -> { try (Statement st = conn.createStatement()) { st.execute(sql); } }, sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage() + " for " + sql);
    }

    private static void assertMalformedArray(String sql) {
        assertFails("22P02", "malformed array literal", sql);
    }

    // ---- array literal: the dimension prefix ----

    @Test
    void anArrayCarriesTheLowerBoundsItWasWrittenWith() throws Exception {
        assertValue("[0:2]={10,20,30}", "SELECT '[0:2]={10,20,30}'::int[]");
        assertValue("[-2:0]={10,20,30}", "SELECT '[-2:0]={10,20,30}'::int[]");
        assertValue("[0:2]={10,20,30}", "SELECT ' [0:2] = {10,20,30} '::int[]");
        assertValue("[0:2]={10,20,30}", "SELECT '[0:2]=  {10,20,30}'::int[]");
        // A lower bound of 1 is the default, so the prefix is not written back
        assertValue("{1,2,3}", "SELECT '[3]={1,2,3}'::int[]");
        assertValue("{{1,2},{3,4}}", "SELECT '[1:2][1:2]={{1,2},{3,4}}'::int[]");
        assertValue("{{1,2},{3,4}}", "SELECT '[2][2]={{1,2},{3,4}}'::int[]");
    }

    @Test
    void theStatedBoundsAreWhatTheDimensionFunctionsReport() throws Exception {
        assertValue("[0:2]", "SELECT array_dims('[0:2]={10,20,30}'::int[])");
        assertValue("1", "SELECT array_ndims('[0:2]={10,20,30}'::int[])");
        assertValue("0", "SELECT array_lower('[0:2]={10,20,30}'::int[], 1)");
        assertValue("2", "SELECT array_upper('[0:2]={10,20,30}'::int[], 1)");
        assertValue("3", "SELECT array_length('[0:2]={10,20,30}'::int[], 1)");
        assertValue("3", "SELECT cardinality('[0:2]={10,20,30}'::int[])");
        assertValue("10", "SELECT ('[0:2]={10,20,30}'::int[])[0]");
        // The bounds are part of the value's shape, so the same elements are a different array
        assertValue("f", "SELECT '[0:2]={10,20,30}'::int[] = '{10,20,30}'::int[]");
        assertValue("t", "SELECT '[0:2]={10,20,30}'::int[] = '[0:2]={10,20,30}'::int[]");
    }

    @Test
    void statedBoundsMustDescribeTheBracesThatFollow() {
        assertMalformedArray("SELECT '[0:2]={10,20}'::int[]");
        assertMalformedArray("SELECT '[0:2]{10,20,30}'::int[]");
        assertMalformedArray("SELECT '[0:2]'::int[]");
        assertMalformedArray("SELECT '[]={1}'::int[]");
        assertFails("2202E", "upper bound cannot be less than lower bound",
                "SELECT '[2:0]={}'::int[]");
        assertFails("2202E", "upper bound cannot be less than lower bound",
                "SELECT '[1:0]={}'::int[]");
        assertFails("2202E", "upper bound cannot be less than lower bound",
                "SELECT '[0:-1]={}'::int[]");
    }

    // ---- array literal: malformed input ----

    @Test
    void aMalformedArrayLiteralIsRefusedRatherThanReadAsSomethingElse() {
        assertMalformedArray("SELECT '{\"a\"b}'::text[]");
        assertMalformedArray("SELECT '{a\"b\"}'::text[]");
        assertMalformedArray("SELECT '{\"a\"\"b\"}'::text[]");
        assertMalformedArray("SELECT '{{1,{2}},{2,3}}'::text[]");
        assertMalformedArray("SELECT '{}}'::text[]");
        assertMalformedArray("SELECT '{ }}'::text[]");
        assertMalformedArray("SELECT '}{'::text[]");
        assertMalformedArray("SELECT '{foo{}}'::text[]");
        assertMalformedArray("SELECT '{foo,,bar}'::text[]");
        assertMalformedArray("SELECT '{1,}'::text[]");
        assertMalformedArray("SELECT '{{1,}}'::text[]");
        assertMalformedArray("SELECT '{{\"1 2\" x},{3}}'::text[]");
        assertMalformedArray("SELECT '{,}'::text[]");
        assertMalformedArray("SELECT '{1,2,3,}'::int[]");
        assertMalformedArray("SELECT '{\"a\" \"b\"}'::text[]");
        assertMalformedArray("SELECT '{a,{1}}'::text[]");
        assertMalformedArray("SELECT '{1,2'::int[]");
        assertMalformedArray("SELECT '1,2}'::int[]");
        assertMalformedArray("SELECT '3'::int[]");
    }

    @Test
    void nestedBracesMustDescribeARectangle() throws Exception {
        assertMalformedArray("SELECT '{{1,2},{3}}'::int[]");
        assertMalformedArray("SELECT '{{1,2},3}'::int[]");
        assertMalformedArray("SELECT '{{1},{2,3}}'::int[]");
        assertMalformedArray("SELECT '{{},{1}}'::int[]");
        // Levels that are all empty describe an array with no dimensions at all
        assertValue("{}", "SELECT '{{},{}}'::int[]");
        assertValue("{}", "SELECT '{{}}'::int[]");
        assertNull(scalar("SELECT array_length('{{},{}}'::int[],1)"));
    }

    @Test
    void theSpellingsPostgresAcceptsStayAccepted() throws Exception {
        assertValue("{}", "SELECT '{}'::text[]");
        assertValue("{}", "SELECT '{ }'::text[]");
        assertValue("{1,2,3}", "SELECT '  {1,2,3}  '::int[]");
        assertValue("{1,2,3}", "SELECT '{ 1 , 2 , 3 }'::int[]");
        assertValue("{1,2}", "SELECT '{  1  ,  2  }'::int[]");
        assertValue("{\"a b\",c}", "SELECT '{\"a b\",c}'::text[]");
        assertValue("{\"a b\",c}", "SELECT '{a b,c}'::text[]");
        assertValue("{\"a,b\"}", "SELECT '{\"a,b\"}'::text[]");
        assertValue("{\"a\\\"b\"}", "SELECT '{\"a\\\"b\"}'::text[]");
        assertValue("{NULL,1}", "SELECT '{NULL,1}'::int[]");
        assertValue("{NULL}", "SELECT '{null}'::text[]");
        assertValue("{NULL}", "SELECT '{Null}'::text[]");
        assertValue("{NULL}", "SELECT '{ NULL }'::text[]");
        assertValue("{\" NULL \"}", "SELECT '{\" NULL \"}'::text[]");
        assertValue("{\"\"}", "SELECT '{\"\"}'::text[]");
        assertValue("{\"{\"}", "SELECT '{\"{\"}'::text[]");
        assertValue("{\"}\"}", "SELECT '{\"}\"}'::text[]");
        assertValue("{\",\"}", "SELECT '{\",\"}'::text[]");
        assertValue("{{1,2},{3,4}}", "SELECT '{{1,2},{3,4}}'::int[]");
        assertValue("{{{1},{2}},{{3},{4}}}", "SELECT '{{{1},{2}},{{3},{4}}}'::int[]");
        assertValue("{a,b}", "SELECT '{\"a\" ,\"b\"}'::text[]");
        assertValue("{01,02}", "SELECT '{01,02}'::text[]");
    }

    @Test
    void aQuotedNullIsTheWordAndSurvivesBeingWrittenBack() throws Exception {
        // Unquoted it would read back as the SQL null, so the output has to quote it again
        assertValue("{\"NULL\"}", "SELECT '{\"NULL\"}'::text[]");
        assertValue("{\"a\\\\b\"}", "SELECT '{\"a\\\\b\"}'::text[]");
        assertValue("{\"a\\\\b\"}", "SELECT '{a\\\\b}'::text[]");
        assertValue("{\"\\\\NULL\"}", "SELECT '{\\\\NULL}'::text[]");
    }

    // ---- ARRAY constructor ----

    @Test
    void anArrayConstructorResolvesOneTypeFromItsElements() throws Exception {
        assertFails("42804", "ARRAY types integer[] and integer cannot be matched",
                "SELECT ARRAY[ARRAY[1,2], 3]");
        assertFails("42804", "ARRAY types integer and integer[] cannot be matched",
                "SELECT ARRAY[3, ARRAY[1,2]]");
        assertFails("2202E", "multidimensional arrays must have array expressions",
                "SELECT ARRAY[ARRAY[1,2], NULL]");
        assertFails("2202E", "multidimensional arrays must have array expressions",
                "SELECT ARRAY[ARRAY[1,2], ARRAY[3]]");
        assertValue("{{1,2},{3,4}}", "SELECT ARRAY[ARRAY[1,2], ARRAY[3,4]]");
        assertValue("{1,2,3}", "SELECT ARRAY[1,2] || 3");
        assertValue("{3,1,2}", "SELECT 3 || ARRAY[1,2]");
        assertValue("{1,2,3,4}", "SELECT ARRAY[1,2] || ARRAY[3,4]");
        assertValue("{1,2,3,4}", "SELECT ARRAY[1,2] || '{3,4}'");
        assertValue("{{1,2,3},{4,5,6},{7,8,9}}",
                "SELECT ARRAY[1,2,3] || ARRAY[[4,5,6],[7,8,9]]");
    }

    // ---- array_agg ----

    @Test
    void arrayAggOverArraysBuildsOneArrayOfTheNextDimensionUp() throws Exception {
        assertValue("{{1,2},{3,4}}",
                "SELECT array_agg(a) FROM (VALUES ('{1,2}'::int[]),('{3,4}'::int[])) v(a)");
        assertValue("{{a,b},{c,d}}",
                "SELECT array_agg(a) FROM (VALUES ('{a,b}'::text[]),('{c,d}'::text[])) v(a)");
        assertFails("2202E", "cannot accumulate arrays of different dimensionality",
                "SELECT array_agg(a) FROM (VALUES ('{1,2}'::int[]),('{3}'::int[])) v(a)");
        assertFails("22004", "cannot accumulate null arrays",
                "SELECT array_agg(a) FROM (VALUES (NULL::int[]),('{3,4}'::int[])) v(a)");
        // A null among scalars is still just an element
        assertValue("{1,NULL}", "SELECT array_agg(x) FROM (VALUES (1),(NULL::int)) v(x)");
        assertValue("{1,2}", "SELECT array_agg(x) FROM (VALUES (1),(2)) v(x)");
        assertValue("{a,\"b c\"}", "SELECT array_agg(x) FROM (VALUES ('a'),('b c')) v(x)");
    }

    // ---- array_fill ----

    @Test
    void arrayFillValidatesItsDimensionsAndLowerBounds() {
        assertFails("22004", "dimension array or low bound array cannot be null",
                "SELECT array_fill(1, NULL::int[])");
        assertFails("22004", "dimension array or low bound array cannot be null",
                "SELECT array_fill(1, ARRAY[2,2], NULL::int[])");
        assertFails("2202E", "wrong number of array subscripts",
                "SELECT array_fill(1, ARRAY[2,2], '{}'::int[])");
        assertFails("2202E", "wrong number of array subscripts",
                "SELECT array_fill(1, ARRAY[3,3], ARRAY[1,1,1])");
        assertFails("2202E", "wrong number of array subscripts",
                "SELECT array_fill(1, ARRAY[2], ARRAY[2,3])");
        assertFails("22004", "dimension values cannot be null",
                "SELECT array_fill(1, ARRAY[2], ARRAY[NULL]::int[])");
        assertFails("54000", "array size exceeds the maximum allowed (134217727)",
                "SELECT array_fill(1, ARRAY[-1])");
        assertFails("54000", "number of array dimensions (7) exceeds the maximum allowed (6)",
                "SELECT array_fill(1, ARRAY[1,1,1,1,1,1,1])");
        assertFails("42804", "could not determine polymorphic type because input has type unknown",
                "SELECT array_fill('x', ARRAY[2])");
        assertFails("42804", "could not determine polymorphic type because input has type unknown",
                "SELECT array_fill(NULL, ARRAY[2])");
    }

    @Test
    void arrayFillWritesItsBoundsOnlyWhenTheyAreNotTheDefault() throws Exception {
        assertValue("{}", "SELECT array_fill(1, ARRAY[]::int[])");
        assertValue("{}", "SELECT array_fill(1, ARRAY[0])");
        assertValue("{{1,1},{1,1}}", "SELECT array_fill(1, ARRAY[2,2])");
        assertValue("{{1,1},{1,1}}", "SELECT array_fill(1, ARRAY[2,2], ARRAY[1,1])");
        assertValue("[2:4]={1,1,1}", "SELECT array_fill(1, ARRAY[3], ARRAY[2])");
        assertValue("[2:4]", "SELECT array_dims(array_fill(1, ARRAY[3], ARRAY[2]))");
        assertValue("[0:1][0:1]", "SELECT array_dims(array_fill(1, ARRAY[2,2], ARRAY[0,0]))");
        assertValue("2", "SELECT array_ndims(array_fill(1, ARRAY[2,2], ARRAY[0,0]))");
        assertValue("0", "SELECT array_lower(array_fill(1, ARRAY[2,2], ARRAY[0,0]), 1)");
        assertValue("1", "SELECT array_upper(array_fill(1, ARRAY[2,2], ARRAY[0,0]), 2)");
        assertValue("{NULL,NULL}", "SELECT array_fill(NULL::int, ARRAY[2])");
        assertValue("{{{1,1},{1,1}},{{1,1},{1,1}}}", "SELECT array_fill(1, ARRAY[2,2,2])");
        assertValue("{{1,1},{1,1}}", "SELECT array_fill(1, '{2,2}')");
        assertValue("[0:1][0:1]={{1,1},{1,1}}", "SELECT array_fill(1, '{2,2}', '{0,0}')");
    }

    // ---- range arithmetic ----

    @Test
    void rangeArithmeticAnswersInTheRangeTypeAndKeepsTheBoundsAsWritten() throws Exception {
        assertValue("[2.0,2.5)", "SELECT '[1.5,2.5)'::numrange * '[2.0,3.0)'::numrange");
        assertValue("(1.5,3.5]", "SELECT '(1.5,2.5]'::numrange + '(2.0,3.5]'::numrange");
        assertValue("[1.5,3.5)", "SELECT '[1.5,2.0)'::numrange + '[2.0,3.5)'::numrange");
        assertValue("[5,10)", "SELECT '[1,10)'::int4range * '[5,20)'::int4range");
        assertValue("[1,20)", "SELECT '[1,10)'::int4range + '[5,20)'::int4range");
        assertValue("[1,5)", "SELECT '[1,10)'::int4range - '[5,20)'::int4range");
        assertValue("[5,10)", "SELECT '[1,10)'::int4range - '[1,5)'::int4range");
        assertValue("[1,10)", "SELECT '[1,10)'::int4range - '[15,20)'::int4range");
        assertValue("empty", "SELECT '[1,10)'::int4range * '[20,30)'::int4range");
        assertValue("[1,4)", "SELECT '[1,2)'::int4range + '[2,4)'::int4range");
        // An untyped literal beside a range is read as that range
        assertValue("[1,10)", "SELECT '[1,10)'::int4range + '[2,3)'");
        assertValue("[1,10)", "SELECT '[2,3)' + '[1,10)'::int4range");
    }

    @Test
    void aRangeHoldsOnePairOfBoundsSoAGapIsAnError() {
        assertFails("22000", "result of range union would not be contiguous",
                "SELECT '[1.5,2.0)'::numrange + '[2.4,3.5)'::numrange");
        assertFails("22000", "result of range union would not be contiguous",
                "SELECT '[1,2)'::int4range + '[3,4)'::int4range");
        assertFails("22000", "result of range difference would not be contiguous",
                "SELECT '[1.5,2.0)'::numrange - '[1.8,1.9)'::numrange");
        assertFails("42883", "operator does not exist: int4range + numrange",
                "SELECT '[1,10)'::int4range + '[1,10)'::numrange");
    }

    @Test
    void theEmptyRangeIsTheIdentityForJoinAndAbsorbsTheRest() throws Exception {
        assertValue("empty", "SELECT 'empty'::numrange * '[1,2)'::numrange");
        assertValue("[1,2)", "SELECT 'empty'::numrange + '[1,2)'::numrange");
        assertValue("empty", "SELECT 'empty'::numrange - '[1,2)'::numrange");
        assertValue("[1,2)", "SELECT '[1,2)'::numrange - 'empty'::numrange");
        assertValue("empty", "SELECT '[1,10)'::int4range * 'empty'::int4range");
        assertValue("[1,10)", "SELECT '[1,10)'::int4range + 'empty'::int4range");
        assertValue("empty", "SELECT 'empty'::int4range + 'empty'::int4range");
        assertValue("empty", "SELECT 'empty'::int4range - 'empty'::int4range");
    }

    @Test
    void multirangeArithmeticAnswersInTheMultirangeType() throws Exception {
        assertValue("{[1,3),[5,7)}",
                "SELECT '{[1,3)}'::int4multirange + '{[5,7)}'::int4multirange");
        assertValue("{[3,5)}", "SELECT '{[1,5)}'::int4multirange * '{[3,7)}'::int4multirange");
        assertValue("{[1,3)}", "SELECT '{[1,5)}'::int4multirange - '{[3,7)}'::int4multirange");
    }

    // ---- range containment element type ----

    @Test
    void containmentTakesExactlyTheTypeTheRangeIsBuiltOver() throws Exception {
        assertFails("42883", "operator does not exist: numrange @> integer",
                "SELECT '[1,5]'::numrange @> 5");
        assertFails("42883", "operator does not exist: numrange @> double precision",
                "SELECT '[1,5]'::numrange @> 5::float8");
        assertFails("42883", "operator does not exist: int4range @> numeric",
                "SELECT '[1,5]'::int4range @> 5.0");
        assertFails("42883", "operator does not exist: int4range @> bigint",
                "SELECT '[1,5]'::int4range @> 5::bigint");
        assertFails("42883", "operator does not exist: int8range @> integer",
                "SELECT '[1,5]'::int8range @> 5");
        assertFails("42883", "operator does not exist: numrange <@ integer",
                "SELECT '[1,5]'::numrange <@ 5");
        assertFails("42883", "operator does not exist: integer <@ numrange",
                "SELECT 5 <@ '[1,5]'::numrange");
        assertFails("42883", "operator does not exist: int4multirange @> numeric",
                "SELECT '{[1,5)}'::int4multirange @> 3.0");
        assertFails("42883", "operator does not exist: nummultirange @> integer",
                "SELECT '{[1,5)}'::nummultirange @> 3");
        assertFails("42883", "operator does not exist: daterange @> integer",
                "SELECT '[2020-01-01,2020-02-01]'::daterange @> 5");
        // The type is decided before the value, so a null operand is the same error
        assertFails("42883", "operator does not exist: numrange @> integer",
                "SELECT '[1,5]'::numrange @> NULL::int");
    }

    @Test
    void containmentOverTheMatchingTypeStillWorks() throws Exception {
        assertValue("t", "SELECT '[1,5]'::numrange @> 5::numeric");
        assertValue("t", "SELECT '[1,5]'::numrange @> 5.0");
        assertValue("t", "SELECT '[1,5]'::int4range @> 5");
        assertValue("t", "SELECT '[1,5]'::int8range @> 5::bigint");
        assertValue("t", "SELECT 5 <@ '[1,5]'::int4range");
        assertValue("t", "SELECT '[1,5]'::int4range @> '[2,3]'::int4range");
        assertValue("t", "SELECT '{[1,5)}'::int4multirange @> 3");
        assertValue("t", "SELECT '[2020-01-01,2020-02-01]'::daterange @> '2020-01-15'::date");
        assertNull(scalar("SELECT '[1,5]'::int4range @> NULL"));
    }

    // ---- multirange literals ----

    @Test
    void aMultirangeLiteralMayNameTheEmptyRangeAndSpaceOutItsMembers() throws Exception {
        assertValue("{[1,3),[5,7)}", "SELECT '{[1,3),empty,[5,7)}'::int4multirange");
        assertValue("{[1,3)}", "SELECT '{[1,3),EMPTY}'::int4multirange");
        assertValue("{}", "SELECT '{empty}'::int4multirange");
        assertValue("{}", "SELECT '{empty,empty}'::int4multirange");
        assertValue("{[1,3),[5,7)}", "SELECT '{[1,3), [5,7)}'::int4multirange");
        assertValue("{[1,3),[5,7)}", "SELECT '{ [1,3) , [5,7) }'::int4multirange");
        assertValue("{[1,3),[5,7)}", "SELECT '{ [1,3),[5,7) }'::int4multirange");
        assertValue("{[1.5,3.5),[4,5)}", "SELECT '{[1.5,3.5), [4,5)}'::nummultirange");
        assertValue("{}", "SELECT '{}'::int4multirange");
        assertValue("{}", "SELECT '{ }'::int4multirange");
        assertValue("{[1,3),[5,7)}", "SELECT '{[5,7),[1,3)}'::int4multirange");
        assertValue("{[1,7)}", "SELECT '{[1,3),[3,7)}'::int4multirange");
        assertValue("{[1,7)}", "SELECT '{[1,3),[2,7)}'::int4multirange");
    }

    @Test
    void anythingElseBetweenTheBracesIsAMalformedMultirange() {
        assertFails("22P02", "malformed multirange literal: \"{,}\"",
                "SELECT '{,}'::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"{[1,3),[5,7),}\"",
                "SELECT '{[1,3),[5,7),}'::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"{[1,3),,[5,7)}\"",
                "SELECT '{[1,3),,[5,7)}'::int4multirange");
        assertFails("22P02", "malformed multirange literal",
                "SELECT '{\"[1,3)\",\"[5,7)\"}'::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"{[1,3)\"",
                "SELECT '{[1,3)'::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"{}}\"",
                "SELECT '{}}'::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"\"",
                "SELECT ''::int4multirange");
        assertFails("22P02", "malformed multirange literal: \"  \"",
                "SELECT '  '::int4multirange");
    }

    // ---- neighbours ----

    @Test
    void theArraySearchOperatorsAreUnchanged() throws Exception {
        assertValue("t", "SELECT ARRAY[1,2] @> ARRAY[2]");
        assertValue("t", "SELECT ARRAY[1,2] <@ ARRAY[1,2,3]");
        assertValue("t", "SELECT ARRAY[1,2] && ARRAY[2,3]");
        assertValue("t", "SELECT ARRAY[1,2] @> '{}'::int[]");
        assertValue("t", "SELECT '{}'::int[] @> '{}'::int[]");
        assertValue("{1,2}", "SELECT ARRAY[1,2] || NULL::int[]");
        // Core PostgreSQL has no "array must not contain nulls" here -- that rule belongs to the
        // intarray extension, so a null element simply matches nothing
        assertValue("t", "SELECT ARRAY[1,NULL]::int[] @> ARRAY[1]::int[]");
        assertValue("f", "SELECT ARRAY[NULL]::int[] @> ARRAY[NULL]::int[]");
    }

    @Test
    void multirangeMembershipAndMergeAreUnchanged() throws Exception {
        assertValue("[1,7)", "SELECT range_merge('{[1,3),[5,7)}'::int4multirange)");
        assertValue("{[1,3),[5,7)}", "SELECT int4multirange(int4range(1,3), int4range(5,7))");
    }
}
