package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A value written out has to read back as itself, and two values that are equal have to be equal
 * everywhere.
 *
 * <p>Neither held. An array was carried either as a list or as the text of its literal, and four
 * writers disagreed about which characters needed quoting, so the same array had four spellings
 * and only one of them could be read again. A composite went into a column through a writer that
 * quoted nothing and came back out through a reader that un-escaped nothing, so a field holding a
 * comma became two fields. And the key that DISTINCT, GROUP BY and a unique index are built from
 * knew Java's equality rather than the type's, so {@code =} and {@code GROUP BY} disagreed about
 * an interval, a timestamptz and a negative zero.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class ValueRepresentationTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        return thrown.getSQLState();
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    // ------------------------------------------------------------ the output function

    /** One writer, and it is each type's own: a boolean is a letter and a bytea is its hex form. */
    @Test
    void everyElementIsWrittenByItsOwnTypesOutputFunction() throws Exception {
        assertEquals("{t,f}", scalar("SELECT ARRAY[true,false]::text"));
        assertEquals("{\"\\\\x0102\"}", scalar("SELECT ARRAY['\\x0102'::bytea]::text"));
        assertEquals("{0.0000000000000001}", scalar("SELECT ARRAY['1e-16'::numeric]::text"));
        assertEquals("{1.50}", scalar("SELECT ARRAY[1.50::numeric]::text"));
        assertEquals("t,f", scalar("SELECT array_to_string(ARRAY[true,false], ',')"));
        assertEquals("(\"4713-01-01 BC\",24:00:00,1e-300)",
                scalar("SELECT ROW('4713-01-01 BC'::date, '24:00:00'::time, 1e-300::float8)::text"));
    }

    /** An element is quoted exactly where its text would otherwise not read back as one element. */
    @Test
    void anElementIsQuotedWhereItsTextWouldNotReadBack() throws Exception {
        assertEquals("{\"a\\\\b\"}", scalar("SELECT ARRAY['a\\b']::text"));
        assertEquals("{\"a\\\"b\"}", scalar("SELECT ARRAY['a\"b']::text"));
        assertEquals("{\"a,b\"}", scalar("SELECT ARRAY['a,b']::text"));
        assertEquals("{\"a b\"}", scalar("SELECT ARRAY['a b']::text"));
        assertEquals("{\"\"}", scalar("SELECT ARRAY['']::text"));
        assertEquals("{\"NULL\"}", scalar("SELECT ARRAY['NULL']::text"));
        assertEquals("{NULL}", scalar("SELECT ARRAY[NULL::text]::text"));
        // A quote inside the value is content; the apostrophe and the colon are not structure.
        assertEquals("{a'b}", scalar("SELECT ARRAY['a''b']::text"));
        assertEquals("{192.168.0.1}", scalar("SELECT ARRAY['192.168.0.1'::inet]::text"));
    }

    /** A bytea array survives being stored and read back, element by element. */
    @Test
    void aByteaArrayRoundTripsThroughAColumn() throws Exception {
        exec("DROP TABLE IF EXISTS vr_ba");
        exec("CREATE TABLE vr_ba (a bytea[])");
        exec("INSERT INTO vr_ba VALUES (ARRAY['\\x0102'::bytea])");
        assertEquals("{\"\\\\x0102\"}", scalar("SELECT a::text FROM vr_ba"));
        assertEquals("t", scalar("SELECT a[1] = '\\x0102'::bytea AS ok FROM vr_ba"));
        assertEquals("\\x0102", scalar("SELECT unnest(a)::text AS u FROM vr_ba"));
    }

    // ------------------------------------------------------------ the bounds an array carries

    /** An array states where its dimensions begin, and every function reads that statement. */
    @Test
    void anArrayCarriesTheBoundsItWasWrittenWith() throws Exception {
        assertEquals("[0:1]={1,2}", scalar("SELECT '[0:1]={1,2}'::int[]::text"));
        assertEquals("0", scalar("SELECT array_lower('[0:1]={1,2}'::int[],1)"));
        assertEquals("1", scalar("SELECT array_upper('[0:1]={1,2}'::int[],1)"));
        assertEquals("[0:1]", scalar("SELECT array_dims('[0:1]={1,2}'::int[])"));
        assertEquals("[0:2]={1,2,3}", scalar("SELECT array_append('[0:1]={1,2}'::int[], 3)::text"));
        assertEquals("[0:1]={1,3}", scalar("SELECT array_remove('[0:2]={1,2,3}'::int[], 2)::text"));
        assertEquals("2", scalar("SELECT array_position('[0:2]={1,2,3}'::int[], 3)"));
        assertEquals("1,2,3", scalar("SELECT array_to_string('[0:2]={1,2,3}'::int[], ',')"));
        assertEquals("t", scalar("SELECT 1 = ANY('[0:1]={1,2}'::int[])"));
        assertEquals("t", scalar("SELECT '[0:1]={1,2}'::int[] @> '{1}'::int[]"));
        // Two arrays holding the same elements at different subscripts are not the same array.
        assertEquals("f", scalar("SELECT '[0:2]={10,20,30}'::int[] = '{10,20,30}'::int[]"));
    }

    /** An array with nothing in it has no dimensions at all, which is not zero of them. */
    @Test
    void anEmptyArrayHasNoDimensions() throws Exception {
        assertNull(scalar("SELECT array_ndims('{}'::int[])"));
        assertNull(scalar("SELECT array_dims('{}'::int[])"));
        assertNull(scalar("SELECT array_length('{}'::int[],1)"));
        assertEquals("[1:1][1:1][1:1]", scalar("SELECT array_dims('{{{1}}}'::int[])"));
        assertEquals("[1:1][1:1][1:2]", scalar("SELECT array_dims(ARRAY[[['a','b']]])"));
    }

    // ------------------------------------------------------------ subscripting

    /** A subscript is one reference into one array, however many brackets it is written with. */
    @Test
    void aSubscriptNamesAsManyDimensionsAsTheArrayHas() throws Exception {
        assertEquals("2", scalar("SELECT (ARRAY[1,2,3])[2]"));
        assertNull(scalar("SELECT (ARRAY[1,2,3])[1][1]"));
        assertNull(scalar("SELECT (ARRAY[[1,2],[3,4]])[1]"));
        assertEquals("2", scalar("SELECT (ARRAY[[1,2],[3,4]])[1][2]"));
        assertEquals("{{1},{3}}", scalar("SELECT (ARRAY[[1,2],[3,4]])[1:2][1:1]::text"));
    }

    /** A fractional subscript is rounded, and one no integer can hold is refused. */
    @Test
    void aSubscriptIsReadAsAnInteger() throws Exception {
        assertEquals("2", scalar("SELECT (ARRAY[1,2,3])[1.5]"));
        assertEquals("1", scalar("SELECT (ARRAY[1,2,3])[1.4]"));
        assertNull(scalar("SELECT (ARRAY[1,2,3])[0]"));
        assertNull(scalar("SELECT (ARRAY[1,2,3])[NULL]"));
        assertEquals("22003", stateOf("SELECT (ARRAY[1,2,3])[4294967297]"));
        assertEquals("42804", stateOf("SELECT (ARRAY[1,2,3])['x'::text]"));
    }

    /** Only a container has a subscript, and the complaint is about the type. */
    @Test
    void aValueWithNoPartsCannotBeSubscripted() throws Exception {
        assertEquals("42804", stateOf("SELECT ('abcdef'::text)[2]"));
        assertEquals("42804", stateOf("SELECT ('{\"a\": 1}'::json)['a']"));
        assertEquals("1", scalar("SELECT ('{\"a\":1}'::jsonb)['a']"));
        assertEquals("3", scalar("SELECT ('[1,2,3]'::jsonb)[-1]"));
        assertEquals("2", scalar("SELECT ('(1,2)'::point)[1]"));
        assertNull(scalar("SELECT ('(1,2)'::point)[2]"));
    }

    /** A subscript is of the element type, and a range of them is of the array's own. */
    @Test
    void aSubscriptCarriesTheElementType() throws Exception {
        assertEquals("integer", scalar("SELECT pg_typeof((ARRAY[1,2,3])[1])::text"));
        assertEquals("integer[]", scalar("SELECT pg_typeof((ARRAY[1,2,3])[1:2])::text"));
        // PostgreSQL names a subscript after what it reaches into.
        assertEquals("array", columnLabel("SELECT (ARRAY[1,2,3])[1]"));
    }

    private static String columnLabel(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnLabel(1);
        }
    }

    // ------------------------------------------------------------ writing through a subscript

    /** Writing before the start of an array moves the start rather than being discarded. */
    @Test
    void writingBelowTheLowerBoundMovesIt() throws Exception {
        exec("DROP TABLE IF EXISTS vr_a1");
        exec("CREATE TABLE vr_a1 (id int, a int[])");
        exec("INSERT INTO vr_a1 VALUES (1,'{1,2,3}'),(2,NULL)");
        exec("UPDATE vr_a1 SET a[0] = 99 WHERE id = 1");
        exec("UPDATE vr_a1 SET a[2] = 99 WHERE id = 2");
        assertEquals("[0:3]={99,1,2,3}", scalar("SELECT a::text FROM vr_a1 WHERE id = 1"));
        assertEquals("[2:2]={99}", scalar("SELECT a::text FROM vr_a1 WHERE id = 2"));
    }

    /** One element of a two-dimensional array is one element, not a whole row of it. */
    @Test
    void writingOneElementOfATwoDimensionalArrayLeavesTheRest() throws Exception {
        exec("DROP TABLE IF EXISTS vr_a3");
        exec("CREATE TABLE vr_a3 (id int, m int[])");
        exec("INSERT INTO vr_a3 VALUES (1,'{{1,2},{3,4}}')");
        exec("UPDATE vr_a3 SET m[1][1] = 99 WHERE id = 1");
        assertEquals("{{99,2},{3,4}}", scalar("SELECT m::text FROM vr_a3"));
        assertEquals("2202E", stateOf("UPDATE vr_a3 SET m[3][1] = 99 WHERE id = 1"));
    }

    /** A slice is filled from an array that has to be long enough to fill it. */
    @Test
    void aSliceIsRefusedWhenTheSourceIsTooShort() throws Exception {
        exec("DROP TABLE IF EXISTS vr_a2");
        exec("CREATE TABLE vr_a2 (id int, a int[])");
        exec("INSERT INTO vr_a2 VALUES (3,'{1,2,3}')");
        assertEquals("2202E", stateOf("UPDATE vr_a2 SET a[2:3] = '{7}' WHERE id = 3"));
        assertEquals("{1,2,3}", scalar("SELECT a::text FROM vr_a2"));
        exec("UPDATE vr_a2 SET a[2:3] = '{7,8}' WHERE id = 3");
        assertEquals("{1,7,8}", scalar("SELECT a::text FROM vr_a2"));
    }

    /** The subscript is a whole expression, and the element still has to fit the element type. */
    @Test
    void aSubscriptedAssignmentTakesAnExpressionAndKeepsTheElementType() throws Exception {
        exec("DROP TABLE IF EXISTS vr_a4");
        exec("CREATE TABLE vr_a4 (i int, a int[])");
        exec("INSERT INTO vr_a4 VALUES (1, '{1,2,3}')");
        exec("UPDATE vr_a4 SET a[i+1] = 50 WHERE i = 1");
        assertEquals("{1,50,3}", scalar("SELECT a::text FROM vr_a4"));
        assertEquals("22P02", stateOf("UPDATE vr_a4 SET a[1] = 'x'"));
    }

    /** A json key written through brackets takes a jsonb value, not the text of one. */
    @Test
    void writingAJsonKeyReadsTheValueAsJson() throws Exception {
        exec("DROP TABLE IF EXISTS vr_js");
        exec("CREATE TABLE vr_js (b jsonb)");
        exec("INSERT INTO vr_js VALUES ('{}')");
        exec("UPDATE vr_js SET b['c'] = '5'");
        assertEquals("{\"c\": 5}", scalar("SELECT b::text FROM vr_js"));
        assertEquals("number", scalar("SELECT jsonb_typeof(b -> 'c') FROM vr_js"));
    }

    // ------------------------------------------------------------ composites

    /** A composite is written with its fields quoted, and read back with the quoting undone. */
    @Test
    void aCompositeRoundTripsThroughAColumn() throws Exception {
        exec("DROP TABLE IF EXISTS vr_ct");
        exec("DROP TYPE IF EXISTS vr_c");
        exec("CREATE TYPE vr_c AS (a text, b text)");
        exec("CREATE TABLE vr_ct (c vr_c)");
        exec("INSERT INTO vr_ct VALUES (ROW('a\"b','c,d'))");
        assertEquals("(\"a\"\"b\",\"c,d\")", scalar("SELECT c::text FROM vr_ct"));
        assertEquals("<a\"b>", scalar("SELECT '<' || (c).a || '>' FROM vr_ct"));
        assertEquals("<c,d>", scalar("SELECT '<' || (c).b || '>' FROM vr_ct"));
        assertEquals("a\"b", scalar("SELECT ('(\"a\"\"b\",z)'::vr_c).a"));
        assertEquals("a\\b", scalar("SELECT ('(\"a\\\\b\",z)'::vr_c).a"));
        assertEquals("t", scalar("SELECT (ROW('a\"b','z')::vr_c::text::vr_c).a = 'a\"b' AS ok"));
        // A quoted empty field is the empty string; an unquoted one is the SQL null.
        assertEquals("t", scalar("SELECT ('(\"\",z)'::vr_c).a = '' AS ok"));
        assertNull(scalar("SELECT ('(,z)'::vr_c).a"));
    }

    /** Two composites are one value only when their fields are, boundaries and all. */
    @Test
    void compositesAreGroupedByTheirFieldsAndNotByOneStringOfThem() throws Exception {
        exec("DROP TABLE IF EXISTS vr_cu");
        exec("DROP TYPE IF EXISTS vr_c2");
        exec("CREATE TYPE vr_c2 AS (a text, b text)");
        exec("CREATE TABLE vr_cu (c vr_c2)");
        exec("INSERT INTO vr_cu VALUES (ROW('a','b,c')), (ROW('a,b','c'))");
        assertEquals("2", scalar("SELECT count(DISTINCT c) FROM vr_cu"));
        assertEquals("2", scalar("SELECT count(*) FROM (SELECT c FROM vr_cu GROUP BY c) g"));
    }

    /** An array inside a composite is an array on the way out and on the way back in. */
    @Test
    void anArrayInsideACompositeIsStillAnArray() throws Exception {
        exec("DROP TYPE IF EXISTS vr_ca");
        exec("CREATE TYPE vr_ca AS (arr text[])");
        assertEquals("(\"{a,b}\")", scalar("SELECT ROW(ARRAY['a','b'])::vr_ca::text"));
        assertEquals("b",
                scalar("SELECT ((ROW(ARRAY['a','b'])::vr_ca::text::vr_ca).arr)[2]"));
    }

    // ------------------------------------------------------------ one value, one key

    /** What {@code =} calls one value, DISTINCT, GROUP BY and a unique index call one value too. */
    @Test
    void groupingAgreesWithEquality() throws Exception {
        assertEquals("1", scalar("SELECT count(DISTINCT x) FROM (SELECT '1 day'::interval AS x"
                + " UNION ALL SELECT '24 hours'::interval) t"));
        assertEquals("1", scalar("SELECT count(DISTINCT x) FROM"
                + " (SELECT '2020-01-01 12:00:00+00'::timestamptz AS x"
                + " UNION ALL SELECT '2020-01-01 13:00:00+01'::timestamptz) t"));
        assertEquals("1", scalar("SELECT count(*) FROM (SELECT x FROM"
                + " (SELECT 0.0::float8 AS x UNION ALL SELECT -0.0::float8) t GROUP BY x) g"));
        exec("DROP TABLE IF EXISTS vr_iv");
        exec("CREATE TABLE vr_iv (v interval UNIQUE)");
        exec("INSERT INTO vr_iv VALUES ('1 day')");
        assertEquals("23505", stateOf("INSERT INTO vr_iv VALUES ('24 hours')"));
    }

    /** IEEE's two zeros are one number, and neither is greater than the other. */
    @Test
    void negativeZeroIsTheSameNumberAsPositiveZero() throws Exception {
        assertEquals("t", scalar("SELECT 0.0::float8 = -0.0::float8"));
        assertEquals("f", scalar("SELECT 0.0::float8 > -0.0::float8"));
    }

    /** A recursive UNION counts a row by its columns, not by one string of them all. */
    @Test
    void aRecursiveUnionCountsRowsByTheirColumns() throws Exception {
        assertEquals("2", scalar("SELECT count(*) FROM ("
                + "WITH RECURSIVE vr_r(a, b) AS ("
                + "  SELECT 'a'::text, 'b, c'::text"
                + " UNION "
                + "  SELECT 'a, b'::text, 'c'::text FROM vr_r WHERE a = 'a'"
                + ") SELECT a, b FROM vr_r) q"));
    }

    /** Arrays are ordered element by element, with a null element after every value. */
    @Test
    void arraysAreOrderedElementByElement() throws Exception {
        exec("DROP TABLE IF EXISTS vr_c1");
        exec("CREATE TABLE vr_c1 (a int[])");
        exec("INSERT INTO vr_c1 VALUES ('{1,2}'),('{1}'),('{}'),('{1,2,3}'),('{2}')");
        assertEquals("{}", scalar("SELECT min(a)::text FROM vr_c1"));
        assertEquals("{2}", scalar("SELECT max(a)::text FROM vr_c1"));
        assertEquals("f", scalar("SELECT ARRAY[1,NULL]::int[] < ARRAY[1,2]::int[]"));
        assertEquals("t", scalar("SELECT ARRAY[1.0]::numeric[] @> ARRAY[1.00]::numeric[]"));
    }

    // ------------------------------------------------------------ ranges

    /** The bounds are checked as they were written, before anything is canonicalised. */
    @Test
    void aRangeIsCheckedBeforeItIsCanonicalised() throws Exception {
        assertEquals("empty", scalar("SELECT '(5,5)'::int4range::text"));
        assertEquals("empty", scalar("SELECT '[5,5)'::int4range::text"));
        assertEquals("22000", stateOf("SELECT '[5,4]'::int4range"));
        assertEquals("22003", stateOf("SELECT '[-2147483648,2147483647]'::int4range"));
        assertEquals("22003", stateOf("SELECT '[1,9223372036854775807]'::int8range"));
    }

    /** A range keeps the bounds it was written with, in the type the query declared. */
    @Test
    void rangeArithmeticAnswersInTheTypeThatWasWritten() throws Exception {
        assertEquals("(1,10]", scalar("SELECT ('(1,5]'::numrange + '(5,10]'::numrange)::text"));
        assertEquals("(3,5]", scalar("SELECT ('(1,5]'::numrange * '(3,10]'::numrange)::text"));
        assertEquals("(1,5]", scalar("SELECT ('(1,10]'::numrange - '(5,10]'::numrange)::text"));
        assertEquals("(1,10]",
                scalar("SELECT range_merge('(1,5]'::numrange,'(7,10]'::numrange)::text"));
        assertEquals("{[1.5,4.5)}",
                scalar("SELECT nummultirange(numrange(1.5,2.5), numrange(2.5,4.5))::text"));
        assertEquals("t", scalar("SELECT '[1.5,2.5)'::numrange = '[1.50,2.50)'::numrange"));
        assertEquals("t", scalar("SELECT numrange(1.5,3.0) < numrange(1.9,3.0)"));
    }

    /** An operator between two ranges is declared over one range type and no other. */
    @Test
    void thereIsNoOperatorBetweenTwoDifferentRangeTypes() {
        assertEquals("42883", stateOf("SELECT '[1,5)'::int4range && '[2,3)'::numrange"));
        assertEquals("42883", stateOf("SELECT '[1,5)'::int4range + '{[6,8)}'::int4multirange"));
    }

    /** {@code &<} asks whether one range reaches past the other's end, not where a box sits. */
    @Test
    void theRangeOverlapOperatorsAreTheRangeOnesAndNotTheGeometricOnes() throws Exception {
        assertEquals("f", scalar("SELECT '[1,5)'::int4range &< '[1,3)'::int4range"));
        assertEquals("f", scalar("SELECT 'empty'::int4range &< '[1,2)'::int4range"));
        assertEquals("t",
                scalar("SELECT '{[1,5)}'::int4multirange &< '{[3,10)}'::int4multirange"));
    }

    /** A range column excludes what overlaps it, comparing bounds as values and not as text. */
    @Test
    void anExclusionConstraintComparesBoundsAsValues() throws Exception {
        exec("DROP TABLE IF EXISTS vr_ex");
        exec("CREATE TABLE vr_ex (r int4range, EXCLUDE USING gist (r WITH &&))");
        exec("INSERT INTO vr_ex VALUES ('[1,10)')");
        assertEquals("23P01", stateOf("INSERT INTO vr_ex VALUES ('[9,20)')"));
        assertEquals("1", scalar("SELECT count(*) FROM vr_ex"));
    }

    /** A range type the reader defines is a type, usable everywhere a type is. */
    @Test
    void aUserDefinedRangeTypeIsAType() throws Exception {
        exec("DROP TYPE IF EXISTS vr_fr CASCADE");
        exec("CREATE TYPE vr_fr AS RANGE (subtype = float8)");
        assertEquals("[1.5,2.5)", scalar("SELECT '[1.5,2.5)'::vr_fr::text"));
        assertEquals("vr_fr", scalar("SELECT pg_typeof(vr_fr(1.5,2.5))::text"));
        exec("DROP TABLE IF EXISTS vr_rt");
        exec("CREATE TABLE vr_rt (r vr_fr)");
        // A bound is quoted when its text would not read back as one bound.
        exec("DROP TYPE IF EXISTS vr_tr CASCADE");
        exec("CREATE TYPE vr_tr AS RANGE (subtype = text)");
        assertEquals("[\"a,b\",z)", scalar("SELECT vr_tr('a,b','z')::text"));
        assertEquals("[\"\",z)", scalar("SELECT vr_tr('','z')::text"));
        assertEquals("[\"a b\",z)", scalar("SELECT vr_tr('a b','z')::text"));
    }

    /** The option list is read rather than skipped. */
    @Test
    void aRangeTypesOptionsAreChecked() {
        assertEquals("42601",
                stateOf("CREATE TYPE vr_bad1 AS RANGE (SUBTYPE = int4, NOSUCHOPT = 1)"));
        assertEquals("42P17",
                stateOf("CREATE TYPE vr_bad2 AS RANGE (SUBTYPE = int4, CANONICAL = missingfn)"));
        assertEquals("42704", stateOf("CREATE TYPE vr_bad3 AS RANGE (SUBTYPE = json)"));
    }

    // ------------------------------------------------------------ quoting

    /** One quoting function, so what quote_literal writes is what %L writes. */
    @Test
    void thereIsOneQuotingFunction() throws Exception {
        assertEquals("E'a\\\\b'", scalar("SELECT quote_literal('a\\b')"));
        assertEquals("E'a\\\\b'", scalar("SELECT format('%L','a\\b')"));
        assertEquals("E'a\\\\b'", scalar("SELECT quote_nullable('a\\b')"));
        assertEquals("NULL", scalar("SELECT quote_nullable(NULL)"));
        // A name the grammar knows has to be quoted, whatever else it also is.
        assertEquals("\"current_date\"", scalar("SELECT quote_ident('current_date')"));
        assertEquals("\"trim\"", scalar("SELECT quote_ident('trim')"));
        assertEquals("\"coalesce\"", scalar("SELECT quote_ident('coalesce')"));
        assertEquals("\"select\"", scalar("SELECT quote_ident('select')"));
        assertEquals("\"a b\"", scalar("SELECT quote_ident('a b')"));
        assertEquals("\"Abc\"", scalar("SELECT quote_ident('Abc')"));
        assertEquals("abc", scalar("SELECT quote_ident('abc')"));
    }

    /** {@code %s} writes the value the way its own type writes it. */
    @Test
    void formatWritesEachValueByItsOwnTypesOutputFunction() throws Exception {
        assertEquals("\\x61", scalar("SELECT format('%s', '\\x61'::bytea)"));
        assertEquals("{1,2}", scalar("SELECT format('%s', ARRAY[1,2])"));
        assertEquals("t", scalar("SELECT format('%s', true)"));
        assertEquals("true", scalar("SELECT ('\\x61'::bytea LIKE 'a')::text"));
    }

    /** hstore writes its own text so that its own reader can read it. */
    @Test
    void hstoreEscapesWhatItsReaderWouldOtherwiseStopAt() throws Exception {
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        assertEquals("\"k\"=>\"a\\\"b\"", scalar("SELECT hstore('k', 'a\"b')::text"));
        assertEquals("t", scalar("SELECT (hstore('k','a\"b')::text::hstore -> 'k') = 'a\"b' AS ok"));
        assertEquals("\"k\"=>\"a\\\\b\"", scalar("SELECT hstore('k','a\\b')::text"));
        assertEquals("t", scalar("SELECT (hstore('k','a\\b')::text::hstore -> 'k') = 'a\\b' AS ok"));
        // The two arrays are paired position by position, so two of different lengths pair with
        // nothing rather than leaving the keys left over holding a value nobody wrote.
        assertEquals("2202E", stateOf("SELECT hstore(ARRAY['a','b'], ARRAY['1'])"));
    }

    /** An enum's labels are an array, and a label that needs quoting gets it. */
    @Test
    void enumRangeQuotesTheLabelsThatNeedIt() throws Exception {
        exec("DROP TYPE IF EXISTS vr_en CASCADE");
        exec("CREATE TYPE vr_en AS ENUM ('e,f', 'z')");
        assertEquals("{\"e,f\",z}", scalar("SELECT enum_range(NULL::vr_en)::text"));
        assertEquals("2", scalar("SELECT count(*) FROM"
                + " (SELECT unnest(enum_range(NULL::vr_en)) AS x) t"));
    }

    // ------------------------------------------------------------ values carrying their own type

    /** A value the query produced is carried across as it is, not written out and read back. */
    @Test
    void insertFromSelectCarriesTheValueAndNotItsSpelling() throws Exception {
        exec("DROP TABLE IF EXISTS vr_sel");
        exec("CREATE TABLE vr_sel (b bytea, a int[])");
        exec("INSERT INTO vr_sel SELECT '\\x00010203de'::bytea, ARRAY[1,2,3]");
        assertEquals("\\x00010203de", scalar("SELECT b::text FROM vr_sel"));
        assertEquals("{1,2,3}", scalar("SELECT a::text FROM vr_sel"));
    }

    /** Text spelled the way an array is spelled is still text. */
    @Test
    void textThatLooksLikeAnArrayIsNotOne() throws Exception {
        assertEquals("{a,b}c", scalar("SELECT '{a,b}' || 'c'"));
        assertEquals("f", scalar("SELECT 'a' IN ('{a,b}')"));
        assertEquals("42883", stateOf("SELECT array_length('{1,2,3}'::text, 1)"));
        assertEquals("42883", stateOf("SELECT unnest('{1,2,3}'::text)"));
        assertEquals("42804", stateOf("SELECT ('{1,2}'::int[] COLLATE \"C\")"));
    }

    /** An array joined to a null element gains the element rather than losing it. */
    @Test
    void concatenatingANullElementKeepsIt() throws Exception {
        assertEquals("{1,2,NULL}", scalar("SELECT (ARRAY[1,2] || NULL::int)::text"));
        assertEquals("3", scalar("SELECT cardinality(ARRAY[1,2] || NULL::int)"));
    }

    /** MERGE writes through the table, so the indexes move with the values. */
    @Test
    void mergeMaintainsTheIndexesItWritesThrough() throws Exception {
        exec("DROP TABLE IF EXISTS vr_mt");
        exec("DROP TABLE IF EXISTS vr_ms");
        exec("CREATE TABLE vr_mt (id int PRIMARY KEY, v int)");
        exec("CREATE TABLE vr_ms (id int, v int)");
        exec("INSERT INTO vr_mt VALUES (1,10),(2,20)");
        exec("INSERT INTO vr_ms VALUES (1,111),(2,222)");
        exec("MERGE INTO vr_mt t USING vr_ms s ON t.id = s.id"
                + " WHEN MATCHED THEN UPDATE SET id = t.id + 100, v = s.v");
        assertEquals("23505", stateOf("INSERT INTO vr_mt VALUES (101, 999)"));
        assertEquals("2", scalar("SELECT count(*) FROM vr_mt"));
        assertEquals("111", scalar("SELECT v FROM vr_mt WHERE id = 101"));
    }

    // ------------------------------------------------------------ what an argument may be

    /** The element functions work on one dimension, and say so rather than answering nothing. */
    @Test
    void theElementFunctionsRefuseADeeperArray() {
        assertEquals("22000", stateOf("SELECT array_append(ARRAY[[1,2],[3,4]], 5)"));
        assertEquals("0A000", stateOf("SELECT array_position(ARRAY[[1,2],[3,4]], 3)"));
        assertEquals("0A000", stateOf("SELECT array_remove(ARRAY[[1,2],[3,4]], 3)"));
        assertEquals("22P02", stateOf("SELECT array_append(ARRAY[1,2], '3.7')"));
        assertEquals("22023", stateOf("SELECT array_sample(ARRAY[1,2,3], 5)"));
        assertEquals("2202E", stateOf("SELECT array_fill(1, ARRAY[[2]])"));
    }

    /** array_replace reaches every element, whatever shape the array has. */
    @Test
    void arrayReplaceReachesEveryElement() throws Exception {
        assertEquals("{{1,2},{9,4}}", scalar("SELECT array_replace(ARRAY[[1,2],[3,4]], 3, 9)::text"));
    }

    /** A histogram over an array finds its bucket by bisection, as PostgreSQL does. */
    @Test
    void widthBucketBisectsItsThresholds() throws Exception {
        assertEquals("3", scalar("SELECT width_bucket(5, ARRAY[8,4,1])"));
        assertEquals("0", scalar("SELECT width_bucket(0, ARRAY[8,4,1])"));
        assertEquals("2", scalar("SELECT width_bucket(5, ARRAY[1,4,8])"));
        assertEquals("3", scalar("SELECT width_bucket(9, ARRAY[1,4,8])"));
    }

    /** An array with nothing in it settles ANY and ALL without comparing anything. */
    @Test
    void anEmptyArraySettlesAnyAndAll() throws Exception {
        assertEquals("f", scalar("SELECT NULL::int = ANY(ARRAY[]::int[])"));
        assertEquals("t", scalar("SELECT NULL::int = ALL(ARRAY[]::int[])"));
    }

    /** An aggregate's ordering reads the clause it was given, nulls and all. */
    @Test
    void arrayAggReadsItsOrdering() throws Exception {
        assertEquals("{NULL,1,2}", scalar("SELECT array_agg(v ORDER BY v NULLS FIRST)::text"
                + " FROM (VALUES (1),(NULL::int),(2)) t(v)"));
        assertEquals("{10,20,30,40,NULL}", scalar("SELECT array_agg(DISTINCT v)::text"
                + " FROM (VALUES (10),(20),(20),(30),(NULL::int),(40)) t(v)"));
        assertEquals("{t}", scalar("SELECT array_agg(x)::text FROM (SELECT true AS x) t"));
    }

    /** to_jsonb of a document is that document, not a second layer around it. */
    @Test
    void toJsonbOfAJsonValueIsThatValue() throws Exception {
        assertEquals("\"x\"", scalar("SELECT to_jsonb('\"x\"'::jsonb)::text"));
        assertEquals("\"x\"", scalar("SELECT to_jsonb(to_jsonb('\"x\"'::jsonb))::text"));
    }

    /** A column may be named with brackets in an INSERT, which writes part of its value. */
    @Test
    void anInsertMayNameOnePartOfAColumn() throws Exception {
        exec("DROP TABLE IF EXISTS vr_ins");
        exec("CREATE TABLE vr_ins (i int, arr int[])");
        exec("INSERT INTO vr_ins (i, arr[1]) VALUES (2, 5)");
        assertEquals("{5}", scalar("SELECT arr::text FROM vr_ins"));
    }

    /**
     * An array type says nothing about how many dimensions it has, so {@code = ANY} over an array
     * of arrays compares against the innermost value.
     */
    @Test
    void anyOverAnArrayOfArraysComparesTheInnermostValue() throws Exception {
        assertEquals("42883", stateOf("SELECT ARRAY[1] = ANY (ARRAY[ARRAY[1]])"));
        assertEquals("42883", stateOf("SELECT ARRAY[1] = ANY (ARRAY[ARRAY[1],ARRAY[2]])"));
        assertEquals("42883", stateOf("SELECT ARRAY[1] = ALL (ARRAY[ARRAY[1],ARRAY[2]])"));
        assertEquals("t", scalar("SELECT 1 = ANY (ARRAY[ARRAY[1],ARRAY[2]])"));
        // The ordinary shapes are unchanged, and IN is a comparison against each written value.
        assertEquals("t", scalar("SELECT 1 = ANY (ARRAY[1,2])"));
        assertEquals("t", scalar("SELECT 'a' = ANY (ARRAY['a','b'])"));
        assertEquals("t", scalar("SELECT ARRAY[1] IN (ARRAY[1], ARRAY[2])"));
    }

    /** A json document is not a range, however alike the two are written. */
    @Test
    void containmentOverAJsonDocumentIsJsonContainment() throws Exception {
        assertEquals("f", scalar("SELECT '[1,5]'::jsonb @> '[2,3]'::jsonb"));
        assertEquals("f", scalar("SELECT '[1,10]'::jsonb @> '[5]'::jsonb"));
        assertEquals("t", scalar("SELECT '[1,2,3]'::jsonb @> '[1,2]'::jsonb"));
        assertEquals("t", scalar("SELECT '{\"a\":1,\"b\":2}'::jsonb @> '{\"a\":1}'::jsonb"));
        // And a range still answers a range question.
        assertEquals("t", scalar("SELECT '[1,5)'::int4range @> 3"));
    }

    /** The json arrow takes a text key or an int4 position, and a bigint is neither. */
    @Test
    void theJsonArrowIsDeclaredOverIntegerAndText() {
        assertEquals("42883", stateOf("SELECT '[1,2]'::jsonb -> 1::bigint"));
    }

    /** A record beside a bare literal is run together with it, as anynonarray || text. */
    @Test
    void aRecordConcatenatedWithALiteralIsWrittenOutAndJoined() throws Exception {
        assertEquals("(1,2)x", scalar("SELECT ROW(1,2) || 'x'"));
    }

    /** A written IN entry is one value, read as the type it is compared against. */
    @Test
    void aWrittenInEntryIsReadAsTheComparisonType() throws Exception {
        assertEquals("f", scalar("SELECT 'a' IN ('{a,b}')"));
        assertEquals("22P02", stateOf("SELECT 1 IN ('{1,2}')"));
    }

    /** PostgreSQL's own single-byte type is spelled with its quotes, and names a column with them. */
    @Test
    void theSingleByteCharTypeCanBeAColumnsType() throws Exception {
        exec("DROP TABLE IF EXISTS vr_ch");
        exec("CREATE TABLE vr_ch (v \"char\")");
        exec("INSERT INTO vr_ch VALUES ('a')");
        assertEquals("a", scalar("SELECT v::text FROM vr_ch"));
    }

    // ------------------------------------------------------------ what a malformed value says

    /** A range ends at the bracket that closes it, and what follows belongs to nothing. */
    @Test
    void aRangeEndsWhereItCloses() throws Exception {
        assertEquals("Junk after right parenthesis or bracket.",
                detailOf("SELECT '[1,2)]'::int4range"));
        assertEquals("Junk after right parenthesis or bracket.",
                detailOf("SELECT '[1,2))'::int4range"));
        assertEquals("Junk after right parenthesis or bracket.",
                detailOf("SELECT '(1,2)x'::int4range"));
        assertEquals("Unexpected end of input.", detailOf("SELECT '[1,2'::int4range"));
        // Trailing space is not junk, and a range that closes at its end is read as it always was.
        assertEquals("[1,2)", scalar("SELECT ('[1,2) '::int4range)::text"));
    }

    /** A range holds two bounds, and text between its brackets that holds none says so. */
    @Test
    void aRangeWithoutACommaNamesTheMissingOne() {
        assertEquals("Missing comma after lower bound.", detailOf("SELECT '[)'::int4range"));
        assertEquals("Missing comma after lower bound.", detailOf("SELECT '[]'::int4range"));
        assertEquals("Missing left parenthesis or bracket.", detailOf("SELECT 'x'::int4range"));
    }

    /** A multirange literal beside a multirange operator is read as one, and faulted as one. */
    @Test
    void aMultirangeLiteralIsFaultedWhereverItIsRead() {
        assertEquals("Missing left brace.", detailOf("SELECT '[1,2)'::int4multirange"));
        assertEquals("Missing left brace.",
                detailOf("SELECT '{[1,3),[5,7)}'::int4multirange @> '[1,2)'"));
        assertEquals("Unexpected end of input.",
                detailOf("SELECT '{[1,3)}'::int4multirange @> '{[1,2)'"));
        assertEquals("Junk after closing right brace.",
                detailOf("SELECT '{[1,3)}'::int4multirange @> '{}x'"));
    }

    /**
     * A virtual generated column has no value in the row, so the row prints the word rather than
     * what the column works out to. A stored one is in the row and prints like any other column.
     */
    @Test
    void aFailingRowPrintsAVirtualColumnAsVirtual() throws Exception {
        exec("DROP TABLE IF EXISTS vr_vg");
        exec("CREATE TABLE vr_vg (id int, a int, b int,"
                + " total int GENERATED ALWAYS AS (a + b) VIRTUAL,"
                + " CONSTRAINT vr_vg_chk CHECK (total > 0))");
        assertEquals("Failing row contains (2, -50, 10, virtual).",
                detailOf("INSERT INTO vr_vg (id, a, b) VALUES (2, -50, 10)"));

        exec("DROP TABLE IF EXISTS vr_vs");
        exec("CREATE TABLE vr_vs (id int, a int, b int,"
                + " total int GENERATED ALWAYS AS (a + b) STORED,"
                + " CONSTRAINT vr_vs_chk CHECK (total > 0))");
        assertEquals("Failing row contains (2, -50, 10, -40).",
                detailOf("INSERT INTO vr_vs (id, a, b) VALUES (2, -50, 10)"));
    }

    /**
     * A name is suggested only while it is spelled nearly the same: within three edits of what was
     * written, and within half of it. Casing counts as spelling, so a column named one way is no
     * suggestion for the same letters cased another.
     */
    @Test
    void aSuggestedColumnIsSpelledNearlyTheSame() throws Exception {
        exec("DROP TABLE IF EXISTS vr_near");
        exec("CREATE TABLE vr_near (abcd int)");
        assertEquals("Perhaps you meant to reference the column \"vr_near.abcd\".",
                hintOf("SELECT abcdefg FROM vr_near"));
        assertNull(hintOf("SELECT abcdefgh FROM vr_near"));

        exec("DROP TABLE IF EXISTS vr_one");
        exec("CREATE TABLE vr_one (p int)");
        assertNull(hintOf("SELECT x FROM vr_one"));

        exec("DROP TABLE IF EXISTS vr_case");
        exec("CREATE TABLE vr_case (\"MiXeD\" int)");
        assertNull(hintOf("SELECT \"mixed\" FROM vr_case"));

        exec("DROP TABLE IF EXISTS vr_tail");
        exec("CREATE TABLE vr_tail (read_bytes int)");
        assertNull(hintOf("SELECT op_bytes FROM vr_tail"));
    }

    /**
     * An operator written in front of its one operand has one argument to cast, and the advice is
     * in the singular.
     */
    @Test
    void aPrefixOperatorIsAdvisedAboutInTheSingular() {
        assertEquals("No operator matches the given name and argument type."
                + " You might need to add an explicit type cast.",
                hintOf("SELECT @ '-10'::text"));
        assertEquals("No operator matches the given name and argument types."
                + " You might need to add explicit type casts.",
                hintOf("SELECT '-10'::text + 1::int"));
    }
}
