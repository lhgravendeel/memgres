package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The type a value is described as travels separately from the value itself, and a client decodes
 * the column by the description. Nine places got the description wrong or let a value through that
 * has no type to belong to: a range's strictly-left test answered in the range type instead of
 * boolean, date_part and the ordered-set aggregates answered in text, the range constructors
 * answered in text and int8range narrowed its own bounds to int, COLLATE was accepted over
 * anything at all and erased the type underneath it, timestamp had no calendar bounds, and every
 * composite inside an array was double-quoted so {@code {(1),(2)}} came back as an array of two
 * different strings.
 *
 * <p>The rules added here are all stated narrowly, so each one is paired with the ordinary shapes
 * around it: refusing SQL PostgreSQL runs would cost more than the permissiveness removed.
 */
class TypeResidualTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE tr_t (id int PRIMARY KEY, s text, v varchar(10), n int, b bigint)");
            st.execute("INSERT INTO tr_t VALUES (1,'a','x',3,4)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected a row from: " + sql);
                return rs.getString(1);
            }
        }
    }

    /** The type name the server put in the RowDescription for the first column. */
    private static String columnType(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                return rs.getMetaData().getColumnTypeName(1);
            }
        }
    }

    private static void assertValue(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertType(String expected, String sql) throws SQLException {
        assertEquals(expected, columnType(sql), "column type of: " + sql);
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(10);
                st.execute(sql);
            }
        }, sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage() + " for " + sql);
    }

    // ---- << and >> over a range or a shape ----

    @Test
    void aRangeStrictlyLeftOfAnotherIsABoolean() throws Exception {
        assertType("bool", "SELECT '[1,3)'::int4range << '[5,8)'::int4range");
        assertType("bool", "SELECT '[5,8)'::int4range >> '[1,3)'::int4range");
        assertType("bool", "SELECT '[1,3)'::numrange << '[5,8)'::numrange");
        assertType("bool", "SELECT '[1,3)'::int8range << '[5,8)'::int8range");
        assertType("bool", "SELECT '{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange");
        assertValue("t", "SELECT '[1,3)'::int4range << '[5,8)'::int4range");
        assertValue("f", "SELECT '[1,3)'::int4range << '[2,8)'::int4range");
        assertValue("boolean", "SELECT pg_typeof('[1,3)'::int4range << '[5,8)'::int4range)");
    }

    @Test
    void theFourShapesThatHaveTheOperatorAnswerItToo() throws Exception {
        assertType("bool", "SELECT point '(1,1)' << point '(3,3)'");
        assertValue("t", "SELECT point '(1,1)' << point '(3,3)'");
        assertValue("t", "SELECT box '((0,0),(1,1))' << box '((3,3),(4,4))'");
        assertValue("t", "SELECT circle '<(0,0),1>' << circle '<(5,5),1>'");
        assertValue("t", "SELECT polygon '((0,0),(1,1))' << polygon '((5,5),(6,6))'");
        assertValue("f", "SELECT point '(5,5)' << point '(3,3)'");
    }

    @Test
    void aRangeBesideSomethingElseHasNoSuchOperator() {
        assertFails("42883", "operator does not exist: int4range << integer",
                "SELECT '[1,3)'::int4range << 3");
        assertFails("42883", "operator does not exist: int4range >> integer",
                "SELECT '[1,3)'::int4range >> 3");
        assertFails("42883", "operator does not exist: integer << int4range",
                "SELECT 3 << '[1,3)'::int4range");
    }

    /**
     * The two range values whose text gives nothing away — "empty" reads as a word and the empty
     * multirange "{}" as an empty array — are still ranges, and the operator still resolves.
     */
    @Test
    void anEmptyRangeIsStillARange() throws Exception {
        assertValue("f", "SELECT 'empty'::int4range << '[5,8)'::int4range");
        assertValue("f", "SELECT '[1,3)'::int4range << 'empty'::int4range");
        assertValue("f", "SELECT '{}'::int4multirange << '{[5,8)}'::int4multirange");
        assertValue("f", "SELECT '{[1,3)}'::int4multirange >> '{}'::int4multirange");
        assertValue("t", "SELECT '{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange");
        assertValue("t", "SELECT daterange('2020-01-01','2020-02-01')"
                + " << daterange('2020-03-01','2020-04-01')");
    }

    /** Everything << and >> meant before is untouched: only a range or a shape changed meaning. */
    @Test
    void shiftingBitsStillShiftsBits() throws Exception {
        assertType("int4", "SELECT 1 << 3");
        assertValue("8", "SELECT 1 << 3");
        assertValue("2", "SELECT 8 >> 2");
        assertType("int8", "SELECT 1::bigint << 2");
        assertValue("4", "SELECT 1::bigint << 2");
        assertValue("0000", "SELECT '10'::bit(4) << 1");
        assertValue("t", "SELECT inet '192.168.1.5' << inet '192.168.1.0/24'");
        assertValue("t", "SELECT inet '192.168.1.0/24' >> inet '192.168.1.5'");
        // the neighbouring range operators were already boolean and stay so
        assertValue("f", "SELECT '[1,3)'::int4range && '[5,8)'::int4range");
        assertValue("t", "SELECT '[1,3)'::int4range &< '[5,8)'::int4range");
        assertValue("t", "SELECT '[1,3)'::int4range -|- '[3,8)'::int4range");
    }

    // ---- the calendar timestamp and date hold ----

    @Test
    void timestampStopsWhereItsCalendarDoes() throws Exception {
        assertValue("294276-12-31 23:59:59", "SELECT '294276-12-31 23:59:59'::timestamp");
        assertFails("22008", "timestamp out of range", "SELECT '294277-01-01 00:00:00'::timestamp");
        assertFails("22008", "timestamp out of range", "SELECT '9999999-01-01 00:00:00'::timestamp");
        assertValue("4714-11-24 00:00:00 BC", "SELECT timestamp '4714-11-24 BC'");
        assertFails("22008", "timestamp out of range", "SELECT timestamp '4714-11-23 BC'");
        assertFails("22008", "timestamp out of range", "SELECT timestamp '4715-01-01 BC'");
    }

    @Test
    void dateStopsAtTheSameFirstDayAndSaysDate() throws Exception {
        assertValue("4714-11-24 BC", "SELECT date '4714-11-24 BC'");
        assertFails("22008", "date out of range", "SELECT date '4714-11-23 BC'");
        assertValue("5874897-01-01", "SELECT '5874897-01-01'::date");
        assertFails("22008", "date out of range", "SELECT '5874898-01-01'::date");
    }

    @Test
    void thereIsNoYearZero() {
        assertFails("22008", "date/time field value out of range", "SELECT timestamp '0000-01-01'");
        assertFails("22008", "date/time field value out of range",
                "SELECT timestamp '0000-01-01 BC'");
        assertFails("22008", "date/time field value out of range", "SELECT date '0000-06-15'");
    }

    @Test
    void aFieldPastItsRangeSaysSoRatherThanBlamingTheSpelling() {
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-01-01 25:00:00'::timestamp");
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-01-01 00:60:00'::timestamp");
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-01-01 00:00:61'::timestamp");
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-13-01 00:00:00'::timestamp");
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-01-32 00:00:00'::timestamp");
        assertFails("22008", "date/time field value out of range", "SELECT '2000-02-30'::timestamp");
        assertFails("22008", "date/time field value out of range",
                "SELECT '2000-01-01 24:00:01'::timestamp");
    }

    /** The two readings PostgreSQL carries over rather than refusing. */
    @Test
    void midnightAtTheEndOfTheDayAndASixtiethSecondRollOver() throws Exception {
        assertValue("2000-01-02 00:00:00", "SELECT '2000-01-01 24:00:00'::timestamp");
        assertValue("2000-01-01 00:01:00", "SELECT '2000-01-01 00:00:60'::timestamp");
        assertValue("2000-01-02 00:00:00", "SELECT '2000-01-01 23:59:60'::timestamp");
    }

    /** Text that is not a calendar reading at all is still a spelling problem, not a range one. */
    @Test
    void ordinaryTimestampsAreReadExactlyAsBefore() throws Exception {
        assertFails("22007", "invalid input syntax for type timestamp", "SELECT 'garbage'::timestamp");
        assertValue("2000-01-01 12:34:56", "SELECT '2000-01-01 12:34:56'::timestamp");
        assertValue("2000-01-01 12:34:56.789", "SELECT '2000-01-01 12:34:56.789'::timestamp");
        assertValue("2000-01-01 00:00:00", "SELECT '2000-01-01'::timestamp");
        assertValue("2000-01-01 00:00:00", "SELECT '2000-01-01T00:00:00'::timestamp");
        assertValue("2000-02-29 00:00:00", "SELECT '2000-02-29'::timestamp");
        assertValue("2000-01-01 12:34:00", "SELECT '2000-01-01 12:34'::timestamp");
        assertValue("2000-01-01 12:34:56", "SELECT '2000-01-01 12:34:56 CET'::timestamp");
        assertValue("2000-01-01 12:34:56", "SELECT '2000-01-01 12:34:56+02'::timestamp");
        assertValue("1970-01-01 00:00:00", "SELECT timestamp 'epoch'");
        assertValue("infinity", "SELECT timestamp 'infinity'");
        assertValue("9999-12-31 23:59:59", "SELECT timestamp '9999-12-31 23:59:59'");
        assertValue("0001-01-01 00:00:00 BC", "SELECT timestamp '0001-01-01 00:00:00 BC'");
        assertValue("2020-05-18 11:22:33",
                "SELECT timestamp '2020-05-17 11:22:33' + interval '1 day'");
        assertValue("2000-01-01", "SELECT '2000-01-01'::date");
        assertValue("2000-01-01 00:00:00+00", "SELECT '2000-01-01 12:34:56'::timestamptz - interval '12 hours 34 minutes 56 seconds'");
    }

    /**
     * After a bare date a trailing {@code -05} is another date field, not an offset. A literal
     * that names a time of day is the only one whose trailing offset is read as one.
     */
    @Test
    void aDateOnlyLiteralStillReadsItsOffsetTheWayItAlwaysDid() throws Exception {
        assertValue("2001-01-01 00:00:00+00", "SELECT timestamptz '2001-01-01+00'");
        assertValue("2000-12-31 22:00:00+00", "SELECT timestamptz '2001-01-01+02'");
        assertValue("2001-01-01 05:00:00+00", "SELECT timestamptz '2001-01-01 -05'");
        assertFails("22007", "invalid input syntax for type timestamp with time zone",
                "SELECT timestamptz '2001-01-01-05'");
        assertValue("2001-01-01 00:00:00", "SELECT timestamp '2001-01-01+02'");
        assertFails("22008", "timestamp out of range",
                "SELECT '294277-01-01 00:00:00+00'::timestamptz");
        assertValue("2000-01-01 10:34:56+00", "SELECT '2000-01-01 12:34:56+02'::timestamptz");
        assertValue("2000-01-01 17:34:56+00", "SELECT '2000-01-01 12:34:56-05'::timestamptz");
    }

    // ---- a composite inside an array ----

    @Test
    void aCompositeIsQuotedByTheArraysOwnRule() throws Exception {
        assertValue("{(1),(2)}", "SELECT ARRAY[ROW(1), ROW(2)]");
        assertValue("{(1),(2)}", "SELECT (ARRAY[ROW(1), ROW(2)])::text");
        assertValue("{(1),(2)}", "SELECT ARRAY[ROW(1),ROW(2)]::text[]");
        // a comma inside the composite is structure, so that one is quoted
        assertValue("{\"(1,a)\",\"(2,b)\"}", "SELECT ARRAY[ROW(1,'a'), ROW(2,'b')]");
        assertValue("{\"(1,\\\"a b\\\")\"}", "SELECT ARRAY[ROW(1,'a b')]");
    }

    @Test
    void aCompositeQuotesTheFieldsThatNeedIt() throws Exception {
        assertValue("(1)", "SELECT ROW(1)");
        assertValue("(1,a)", "SELECT ROW(1,'a')");
        assertValue("(1,\"a b\")", "SELECT ROW(1,'a b')");
        assertValue("(1,\"c,d\")", "SELECT ROW(1,'c,d')");
        assertValue("(1,,x)", "SELECT ROW(1,NULL,'x')");
        assertValue("(1,\"\")", "SELECT ROW(1,'')");
        assertValue("(t,f)", "SELECT ROW(true,false)");
        assertValue("(2020-01-01)", "SELECT ROW(date '2020-01-01')");
    }

    @Test
    void theSearchSetColumnIsAnArrayOfComposites() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3)"
                             + " SEARCH DEPTH FIRST BY n SET ord SELECT n, ord FROM r ORDER BY n")) {
            assertTrue(rs.next());
            assertEquals("{(1)}", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("{(1),(2)}", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("{(1),(2),(3)}", rs.getString(2));
        }
    }

    /** Plain text arrays are quoted exactly as they were. */
    @Test
    void aTextArrayIsUnchanged() throws Exception {
        assertValue("{a,b}", "SELECT ARRAY['a','b']");
        assertValue("{\"a b\",\"c,d\"}", "SELECT ARRAY['a b','c,d']");
        assertValue("{\"a,b\",c}", "SELECT ARRAY['a,b','c']");
        assertValue("{1,2,3}", "SELECT ARRAY[1,2,3]");
        assertValue("{}", "SELECT ARRAY[]::int[]");
        assertValue("{NULL,a}", "SELECT ARRAY[NULL,'a']::text[]");
    }

    /**
     * The 22004 the finding reported for a NULL element comes from the intarray extension, which
     * redeclares these three operators over integer[]. Stock PostgreSQL answers, and so does this.
     */
    @Test
    void containmentAndOverlapStillAnswerOverANullElement() throws Exception {
        assertValue("f", "SELECT ARRAY[1,NULL] @> ARRAY[NULL]::int[]");
        assertValue("t", "SELECT ARRAY[1,NULL] @> ARRAY[1]");
        assertValue("f", "SELECT ARRAY[1] @> ARRAY[NULL]::int[]");
        assertValue("f", "SELECT ARRAY[NULL]::int[] <@ ARRAY[NULL]::int[]");
        assertValue("f", "SELECT ARRAY[1,2] && ARRAY[NULL]::int[]");
        assertValue("t", "SELECT ARRAY['a',NULL]::text[] @> ARRAY['a']::text[]");
        assertValue("f", "SELECT ARRAY[NULL]::text[] && ARRAY[NULL]::text[]");
    }

    // ---- the JSON builders ----

    @Test
    void theJsonBuildersAnswerInTheFlavourTheirNameSays() throws Exception {
        assertType("json", "SELECT json_object('{a,1,b,2}')");
        assertValue("json", "SELECT pg_typeof(json_object('{a,1,b,2}'))");
        assertValue("json", "SELECT pg_typeof(json_object('{a,b}', '{1,2}'))");
        assertValue("json", "SELECT pg_typeof(json_build_object('a',1))");
        assertValue("jsonb", "SELECT pg_typeof(jsonb_build_object('a',1))");
        assertValue("json", "SELECT pg_typeof(json_build_array(1))");
        assertValue("json", "SELECT pg_typeof(to_json(1))");
        assertValue("jsonb", "SELECT pg_typeof(to_jsonb(1))");
        assertValue("json", "SELECT pg_typeof(json_agg(1))");
        assertValue("jsonb", "SELECT pg_typeof(jsonb_agg(1))");
        assertValue("{\"a\" : \"1\", \"b\" : \"2\"}", "SELECT json_object('{a,1,b,2}')");
    }

    @Test
    void aRejectedEscapeSaysWhichEscapeItWas() throws Exception {
        assertFails("22P02", "Escape sequence \"\\q\" is invalid.",
                "SELECT ('\"' || chr(92) || 'q\"')::jsonb");
        assertFails("22P02", "must be followed by four hexadecimal digits.",
                "SELECT ('\"' || chr(92) || 'u12\"')::jsonb");
        assertFails("22P02", "must be followed by four hexadecimal digits.",
                "SELECT ('\"' || chr(92) || 'u\"')::jsonb");
        assertFails("22P02", "Unicode low surrogate must follow a high surrogate.",
                "SELECT ('\"' || chr(92) || 'ud834\"')::jsonb");
        assertFails("22P05", "\\u0000 cannot be converted to text.",
                "SELECT ('\"' || chr(92) || 'u0000\"')::jsonb");
        // the escapes JSON does have are still read
        assertValue("{\"a\": \"b\\\\c\"}",
                "SELECT ('{\"a\":\"b' || chr(92) || chr(92) || 'c\"}')::jsonb::text");
        assertValue("{\"a\": \"x\"}",
                "SELECT ('{\"a\":\"' || chr(92) || 'u0078\"}')::jsonb::text");
    }

    // ---- date_part and extract ----

    @Test
    void datePartAnswersInDoublePrecisionAndExtractInNumeric() throws Exception {
        assertType("float8", "SELECT date_part('year', timestamp '2020-05-17 11:22:33')");
        assertType("float8", "SELECT date_part('y', timestamp '2020-05-17 11:22:33')");
        assertType("float8", "SELECT date_part('epoch', timestamp '2020-05-17 11:22:33')");
        assertType("float8", "SELECT date_part('year', date '2020-05-17')");
        assertType("float8", "SELECT date_part('hour', time '11:22:33')");
        assertType("float8", "SELECT date_part('year', interval '2 years')");
        assertType("numeric", "SELECT EXTRACT(YEAR FROM timestamp '2020-05-17 11:22:33')");
        assertValue("double precision",
                "SELECT pg_typeof(date_part('year', timestamp '2020-05-17 11:22:33'))");
    }

    @Test
    void dividingADatePartIsARealDivision() throws Exception {
        assertType("float8", "SELECT date_part('year', timestamp '2020-05-17 11:22:33') / 7");
        assertValue("288.57142857142856",
                "SELECT date_part('year', timestamp '2020-05-17 11:22:33') / 7");
        assertValue("2.5", "SELECT date_part('month', timestamp '2020-05-17 11:22:33') / 2");
        assertValue("2020", "SELECT date_part('y', timestamp '2020-05-17 11:22:33')");
        assertValue("33000", "SELECT date_part('ms', timestamp '2020-05-17 11:22:33')");
        assertValue("20", "SELECT date_part('weeks', timestamp '2020-05-17 11:22:33')");
    }

    // ---- the range constructors ----

    @Test
    void aRangeConstructorAnswersInItsOwnRangeType() throws Exception {
        assertType("int4range", "SELECT int4range(1,2)");
        assertType("int8range", "SELECT int8range(1,2)");
        assertType("numrange", "SELECT numrange(1,2)");
        assertType("daterange", "SELECT daterange(date '2020-01-01', date '2020-02-01')");
        assertType("tsrange", "SELECT tsrange(timestamp '2020-01-01', timestamp '2020-02-01')");
        assertValue("int4range", "SELECT pg_typeof(int4range(1,2))");
        assertValue("numrange", "SELECT pg_typeof(numrange(1,2))");
    }

    @Test
    void int8rangeHoldsABigintBoundWhole() throws Exception {
        assertValue("[1,99999999999)", "SELECT int8range(1, 99999999999)");
        assertValue("[1,9223372036854775807)", "SELECT int8range(1, 9223372036854775807)");
        assertValue("[1,4)", "SELECT int8range(1,3,'[]')");
        assertValue("(,3)", "SELECT int8range(NULL,3)");
        assertValue("[1,)", "SELECT int8range(1,NULL)");
        assertValue("bigint", "SELECT pg_typeof(lower(int8range(1,9999999999)))");
    }

    @Test
    void int4rangeHasNoOverloadForAWiderBound() {
        assertFails("42883", "function int4range(integer, bigint) does not exist",
                "SELECT int4range(1, 99999999999)");
        assertFails("42883", "function int4range(integer, bigint) does not exist",
                "SELECT int4range(1, 2147483648)");
        assertFails("42883", "function int4range(bigint, bigint) does not exist",
                "SELECT int4range(1::bigint, 2::bigint)");
        assertFails("42883", "function int4range(integer, numeric) does not exist",
                "SELECT int4range(1, 2.0)");
        assertFails("42883", "function int4range(integer, bigint) does not exist",
                "SELECT int4range(1, b) FROM tr_t");
    }

    /** An integer bound, in every spelling that is one, is what the constructor takes. */
    @Test
    void anIntegerBoundIsStillAccepted() throws Exception {
        assertValue("[1,2)", "SELECT int4range(1,2)");
        assertValue("[1,2)", "SELECT int4range(1, 2::smallint)");
        assertValue("[1,3)", "SELECT int4range(1, 1+1+1)");
        assertValue("[1,11)", "SELECT int4range(1,10,'[]')");
        assertValue("(,10)", "SELECT int4range(NULL,10)");
        assertValue("[1,)", "SELECT int4range(1,NULL)");
        assertValue("[1,3)", "SELECT int4range(1, n) FROM tr_t");
        assertValue("t", "SELECT int4range(1,2) @> 1");
        assertValue("t", "SELECT int4range(1,2) = '[1,2)'::int4range");
        assertFails("22000", "range lower bound must be less than or equal to range upper bound",
                "SELECT int4range(5,1)");
    }

    @Test
    void lowerAndUpperOfARangeAnswerInItsSubtype() throws Exception {
        assertType("int4", "SELECT lower('[1,5)'::int4range)");
        assertType("int4", "SELECT upper(int4range(1,5))");
        assertType("numeric", "SELECT lower('[1.5,5)'::numrange)");
        assertType("date", "SELECT lower('[2020-01-01,2020-02-01)'::daterange)");
        assertValue("integer", "SELECT pg_typeof(lower('[1,5)'::int4range))");
        assertValue("1", "SELECT lower(int4range(1,5))");
        assertValue("5", "SELECT upper('[1,5)'::int4range)");
    }

    /** Over anything that is not a range, lower and upper are the string functions they were. */
    @Test
    void lowerAndUpperOverAStringAreStillTheStringFunctions() throws Exception {
        assertType("text", "SELECT lower('abcDEF')");
        assertValue("abcdef", "SELECT lower('abcDEF')");
        assertValue("ABCDEF", "SELECT upper('abcDEF')");
        assertValue("a", "SELECT lower(s) FROM tr_t");
        assertNull(scalar("SELECT lower(NULL)"));
    }

    // ---- COLLATE ----

    @Test
    void collateIsRefusedOverATypeThatCarriesNoCollation() {
        assertFails("42804", "collations are not supported by type integer", "SELECT 1 COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type integer",
                "SELECT (1 + 1) COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type integer",
                "SELECT n COLLATE \"C\" FROM tr_t");
        assertFails("42804", "collations are not supported by type bigint",
                "SELECT 1::bigint COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type numeric",
                "SELECT 1.5 COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type boolean",
                "SELECT true COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type date",
                "SELECT current_date COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type timestamp without time zone",
                "SELECT '2020-01-01'::timestamp COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type interval",
                "SELECT '1 day'::interval COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type uuid",
                "SELECT '00000000-0000-0000-0000-000000000000'::uuid COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type bytea",
                "SELECT 'a'::bytea COLLATE \"C\"");
        assertFails("42804", "collations are not supported by type record",
                "SELECT ROW(1) COLLATE \"C\"");
    }

    /** The string types carry a collation, and COLLATE leaves the type it was written over alone. */
    @Test
    void collateOverAStringKeepsTheStringAndItsType() throws Exception {
        assertValue("a", "SELECT 'a' COLLATE \"C\"");
        assertValue("a", "SELECT 'a'::text COLLATE \"C\"");
        assertValue("ab", "SELECT ('a' || 'b') COLLATE \"C\"");
        assertValue("f", "SELECT 'b' < 'a' COLLATE \"C\"");
        assertValue("a", "SELECT (CASE WHEN true THEN 'a' ELSE 'b' END) COLLATE \"C\"");
        assertValue("A", "SELECT upper(s) COLLATE \"C\" FROM tr_t");
        assertValue("a", "SELECT s COLLATE \"C\" FROM tr_t");
        assertValue("a", "SELECT s FROM tr_t ORDER BY s COLLATE \"C\"");
        assertNull(scalar("SELECT NULL COLLATE \"C\""));
        assertType("text", "SELECT 'a'::text COLLATE \"C\"");
        assertType("varchar", "SELECT 'a'::varchar COLLATE \"C\"");
        assertType("bpchar", "SELECT 'a'::char(3) COLLATE \"C\"");
        assertType("name", "SELECT 'a'::name COLLATE \"C\"");
        assertType("_text", "SELECT '{a}'::text[] COLLATE \"C\"");
        assertType("varchar", "SELECT v COLLATE \"C\" FROM tr_t");
    }

    // ---- the ordered-set aggregates ----

    @Test
    void anOrderedSetAggregateAnswersInItsOwnType() throws Exception {
        String from = " FROM (VALUES (1),(2)) v(x)";
        assertType("float8",
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)" + from);
        assertType("int4", "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x)" + from);
        assertType("int4", "SELECT mode() WITHIN GROUP (ORDER BY x)" + from);
        assertType("int8", "SELECT rank(1) WITHIN GROUP (ORDER BY x)" + from);
        assertType("int8", "SELECT dense_rank(1) WITHIN GROUP (ORDER BY x)" + from);
        assertType("float8", "SELECT percent_rank(1) WITHIN GROUP (ORDER BY x)" + from);
        assertType("float8", "SELECT cume_dist(1) WITHIN GROUP (ORDER BY x)" + from);
        assertType("_float8",
                "SELECT percentile_cont(ARRAY[0.25,0.5]) WITHIN GROUP (ORDER BY x)" + from);
        assertValue("1.5", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)" + from);
        assertValue("1", "SELECT mode() WITHIN GROUP (ORDER BY x)" + from);
        assertValue("{1.25,1.5}",
                "SELECT percentile_cont(ARRAY[0.25,0.5]) WITHIN GROUP (ORDER BY x)" + from);
    }

    /** The two that give back one of the values they sorted follow that value's own type. */
    @Test
    void theSortedValuesTypeCarriesThrough() throws Exception {
        assertType("text", "SELECT mode() WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES ('a'),('b')) v(x)");
        assertType("numeric", "SELECT mode() WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES (1.5),(2.5)) v(x)");
        assertType("date", "SELECT mode() WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES (date '2020-01-01')) v(x)");
        assertType("numeric", "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES (1.5),(2.5)) v(x)");
        // percentile_cont interpolates, so it resolves through float8 whatever it sorted
        assertType("float8", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES (1.5),(2.5)) v(x)");
        assertType("float8", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x)"
                + " FROM (VALUES (1::bigint),(2::bigint)) v(x)");
    }

    /** WITHIN GROUP on an ordinary aggregate is still refused. */
    @Test
    void anOrdinaryAggregateStillTakesNoWithinGroup() {
        assertFails("42883", "function sum(integer, integer) does not exist",
                "SELECT sum(x) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2)) v(x)");
    }
}
