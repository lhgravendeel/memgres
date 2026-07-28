package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL resolves an operator from the declared types of its operands, and the operator it
 * resolves carries a declared result type too. Both halves of that were wrong here.
 *
 * <p>On the way in, reading the values instead of the declarations let {@code '5'::text = 5} and
 * {@code '2020-01-01'::text = date '2020-01-01'} answer true, and let a text column compare
 * against an integer one — a query that fails in production passed here. On the way out, the
 * result type was guessed from the shape of the operator rather than looked up: {@code date + 1}
 * was declared int4, so pgjdbc's getObject threw "Bad value for type int : 2020-01-02" on a value
 * the engine had computed perfectly well, and every geometric, network, array, range, jsonb and
 * tsvector predicate arrived as text rather than as a boolean.
 *
 * <p>Between the two, an untyped literal beside a date, a time or an interval was read as a bare
 * number, so {@code date + '1'} lost the date and {@code interval / '2'} came out NaN, while the
 * array mutators handed an unknown array literal silently dropped it.
 *
 * <p>Every expectation below was measured against PostgreSQL 18 before and after the change.
 */
class CrossTypeOperatorTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
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

    /** The type name the server puts in the row description, which is what a driver decodes by. */
    private static String columnType(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
        }
    }

    private static void assertColumnType(String expected, String sql) throws SQLException {
        assertEquals(expected, columnType(sql), sql);
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class,
                () -> { try (Statement st = conn.createStatement()) { st.execute(sql); } }, sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage() + " for " + sql);
    }

    private static void assertNoOperator(String signature, String sql) {
        assertFails("42883", "operator does not exist: " + signature, sql);
    }

    // ---- (a) an operator is resolved from the declared types ----

    @Test
    void textCannotBeComparedWithADateOrATimestamp() {
        assertNoOperator("text = date", "SELECT '2020-01-01'::text = date '2020-01-01'");
        assertNoOperator("date = text", "SELECT date '2020-01-01' = '2020-01-01'::text");
        assertNoOperator("text < date", "SELECT '2020-01-01'::text < date '2020-01-02'");
        assertNoOperator("character varying = date", "SELECT '2020-01-01'::varchar = date '2020-01-01'");
        assertNoOperator("text = timestamp without time zone",
                "SELECT '2020-01-01'::text = timestamp '2020-01-01'");
    }

    @Test
    void textCannotBeComparedWithATimeOrAnInterval() {
        assertNoOperator("text = time without time zone", "SELECT '10:00'::text = time '10:00'");
        assertNoOperator("text = interval", "SELECT '1 day'::text = interval '1 day'");
    }

    @Test
    void textCannotBeComparedWithAUuidAnInetOrABytea() {
        assertNoOperator("text = uuid",
                "SELECT '00000000-0000-0000-0000-000000000000'::text"
                        + " = '00000000-0000-0000-0000-000000000000'::uuid");
        assertNoOperator("uuid = text",
                "SELECT '00000000-0000-0000-0000-000000000000'::uuid"
                        + " = '00000000-0000-0000-0000-000000000000'::text");
        assertNoOperator("uuid < text",
                "SELECT '00000000-0000-0000-0000-000000000000'::uuid"
                        + " < 'ffffffff-0000-0000-0000-000000000000'::text");
        assertNoOperator("text = inet", "SELECT '10.0.0.1'::text = '10.0.0.1'::inet");
        assertNoOperator("text = bytea", "SELECT 'ab'::text = 'ab'::bytea");
    }

    @Test
    void aDateOrTimestampCannotBeComparedWithAnInteger() {
        assertNoOperator("date = integer", "SELECT date '2020-01-01' = 1");
        assertNoOperator("date < integer", "SELECT date '2020-01-01' < 1");
        assertNoOperator("timestamp without time zone = integer", "SELECT timestamp '2020-01-01' = 1");
    }

    @Test
    void betweenResolvesTheComparisonsItStandsFor() {
        assertNoOperator("text >= integer", "SELECT '5'::text BETWEEN 1 AND 9");
        assertNoOperator("integer >= text", "SELECT 5 BETWEEN '1'::text AND '9'::text");
    }

    @Test
    void inAndAnyResolveTheSameEquality() {
        assertNoOperator("text = integer", "SELECT '5'::text IN (1,2,5)");
        assertNoOperator("integer = text", "SELECT 5 IN ('1'::text,'5'::text)");
        assertNoOperator("text = integer", "SELECT '5'::text = ANY(ARRAY[1,2,5])");
    }

    @Test
    void nullifIsAnEqualityAndResolvesLikeOne() {
        assertNoOperator("integer = text", "SELECT NULLIF(1, '2'::text)");
        assertNoOperator("integer = text", "SELECT NULLIF(1, '1'::text)");
        assertNoOperator("text = integer", "SELECT NULLIF('1'::text, 1)");
        assertNoOperator("date = text", "SELECT NULLIF(date '2020-01-01', '2020-01-01'::text)");
        // the operator is missing whatever the literal spells, so this is not an input error
        assertNoOperator("integer = text", "SELECT NULLIF(1, 'a'::text)");
    }

    @Test
    void textCarriesNoArithmetic() {
        assertNoOperator("text % integer", "SELECT '10'::text % 3");
        assertNoOperator("character varying % integer", "SELECT '10'::varchar % 3");
        assertNoOperator("text % text", "SELECT '10'::text % '3'::text");
        assertNoOperator("integer % text", "SELECT 10 % '3'::text");
        assertNoOperator("text ^ integer", "SELECT '10'::text ^ 2");
        assertNoOperator("text - text", "SELECT '10'::text - '5'::text");
        assertNoOperator("@ text", "SELECT @ '-10'::text");
    }

    @Test
    void theRuleReachesRealTableColumnsAndNotOnlyLiterals() throws SQLException {
        exec("DROP TABLE IF EXISTS xta_ti");
        exec("CREATE TABLE xta_ti(t text, i int)");
        exec("INSERT INTO xta_ti VALUES ('5', 5)");
        assertNoOperator("text = integer", "SELECT count(*) FROM xta_ti WHERE t = i");
        assertNoOperator("text >= integer", "SELECT count(*) FROM xta_ti WHERE t BETWEEN 1 AND 9");

        exec("DROP TABLE IF EXISTS xta_td");
        exec("CREATE TABLE xta_td(t text, d date)");
        exec("INSERT INTO xta_td VALUES ('2020-01-01', date '2020-01-01')");
        assertNoOperator("text = date", "SELECT count(*) FROM xta_td WHERE t = d");

        exec("DROP TABLE IF EXISTS xta_ut");
        exec("CREATE TABLE xta_ut(u uuid, t text)");
        exec("INSERT INTO xta_ut VALUES ('00000000-0000-0000-0000-000000000000',"
                + " '00000000-0000-0000-0000-000000000000')");
        assertNoOperator("uuid = text", "SELECT count(*) FROM xta_ut WHERE u = t");

        exec("DROP TABLE IF EXISTS xta_tm");
        exec("CREATE TABLE xta_tm(t text, i int)");
        exec("INSERT INTO xta_tm VALUES ('10', 3)");
        assertNoOperator("text % integer", "SELECT t % i FROM xta_tm");
    }

    @Test
    void setOperationsRefuseTwoUnmatchedDeclaredTypes() {
        assertFails("42804", "UNION types integer and text cannot be matched",
                "SELECT 1 UNION SELECT 'a'::text");
        assertFails("42804", "UNION types text and integer cannot be matched",
                "SELECT 'a'::text UNION SELECT 1");
        assertFails("42804", "INTERSECT types integer and text cannot be matched",
                "SELECT 1 INTERSECT SELECT 'a'::text");
        assertFails("42804", "EXCEPT types integer and text cannot be matched",
                "SELECT 1 EXCEPT SELECT 'a'::text");
        // the message names the SQL type, not the internal int4
        assertFails("42804", "UNION types date and integer cannot be matched",
                "SELECT date '2020-01-01' UNION SELECT 1");
    }

    // ---- (a) the neighbours that must keep working ----

    @Test
    void anUntypedLiteralStillResolvesAgainstTheOtherSide() throws SQLException {
        assertValue("t", "SELECT '5' = 5");
        assertValue("t", "SELECT 'a'::text = 'a'::text");
        assertValue("t", "SELECT 1 = 1.0");
        assertValue("t", "SELECT '2020-01-01'::date = '2020-01-01'::date");
        assertValue("t", "SELECT 5 BETWEEN 1 AND 9");
        assertValue("t", "SELECT 'abc' LIKE 'a%'");
        assertValue("1", "SELECT 10 % 3");
        assertValue("10", "SELECT @ -10");
        assertValue("1", "SELECT NULLIF(1, 2)");
        assertNull(scalar("SELECT NULLIF('a', 'a')"));
    }

    @Test
    void aDerivedColumnsInferredTypeNeverRefusesAnOperator() throws SQLException {
        // The type of rn out of a subquery is whatever the engine settled on; refusing an
        // operator on the strength of that is exactly the regression this guards against.
        assertValue("1", "SELECT count(*)::int FROM (SELECT row_number() OVER () AS rn"
                + " FROM (SELECT 1) t) sub WHERE sub.rn >= 1");
        assertValue("t", "SELECT (SELECT rn FROM (SELECT 1 AS rn) sub) >= 1");

        exec("DROP TABLE IF EXISTS xta_der");
        exec("CREATE TABLE xta_der(i int, t text)");
        exec("INSERT INTO xta_der VALUES (1,'a')");
        assertValue("1", "SELECT count(*)::int FROM (SELECT i AS x FROM xta_der) s WHERE s.x = 1");
        assertValue("1", "SELECT count(*)::int FROM (SELECT t AS x FROM xta_der) s WHERE s.x = 'a'");

        exec("DROP VIEW IF EXISTS xta_der_v");
        exec("CREATE VIEW xta_der_v AS SELECT i, t FROM xta_der");
        assertValue("1", "SELECT count(*)::int FROM xta_der_v WHERE i = 1 AND t = 'a'");
    }

    @Test
    void matchingTypesKeepComparingInEveryClause() throws SQLException {
        exec("DROP TABLE IF EXISTS xta_ok");
        exec("CREATE TABLE xta_ok(i int, t text, d date)");
        exec("INSERT INTO xta_ok VALUES (1,'a',date '2020-01-01'),(2,'b',date '2020-01-02')");
        assertValue("2", "SELECT count(*)::int FROM xta_ok WHERE i BETWEEN 0 AND 9");
        assertValue("2", "SELECT count(*)::int FROM xta_ok WHERE t IN ('a','b')");
        assertValue("2", "SELECT count(*)::int FROM xta_ok WHERE t = ANY(ARRAY['a','b'])");
        assertValue("2", "SELECT count(*)::int FROM xta_ok WHERE d >= date '2020-01-01'");
        assertValue("1", "SELECT i FROM xta_ok WHERE t = 'a' ORDER BY i");
        assertValue("1", "SELECT i FROM xta_ok GROUP BY i HAVING count(*) >= 1 ORDER BY i");
    }

    @Test
    void aNullOperandStillAnswersNullRatherThanFailing() throws SQLException {
        assertNull(scalar("SELECT NULL::int = 1"));
        assertNull(scalar("SELECT NULL::text = 'a'"));
        assertNull(scalar("SELECT NULL::date = date '2020-01-01'"));
        // but a declared type mismatch is decided before the values, NULL or not
        assertNoOperator("text = date", "SELECT '2020-01-01'::text = NULL::date");
    }

    // ---- (b) date and time arithmetic with an untyped literal ----

    @Test
    void aDatePlusAnUntypedLiteralIsAmbiguous() {
        assertFails("42725", "operator is not unique: date + unknown",
                "SELECT date '2020-01-01' + '1'");
        assertFails("42725", "operator is not unique: date + unknown",
                "SELECT date '2020-01-01' + '1 day'");
        assertFails("42725", "operator is not unique: unknown + date",
                "SELECT '1 day' + date '2020-01-01'");
        assertFails("42725", "operator is not unique: time with time zone + unknown",
                "SELECT time with time zone '10:00+02' + '1 hour'");
    }

    @Test
    void aDateMinusAnUntypedLiteralResolvesToDateMinusDate() throws SQLException {
        assertValue("1", "SELECT date '2020-01-02' - '2020-01-01'");
        assertColumnType("int4", "SELECT date '2020-01-02' - '2020-01-01'");
        assertFails("22007", "invalid input syntax for type date: \"1\"",
                "SELECT date '2020-01-01' - '1'");
    }

    @Test
    void aMomentPlusAnUntypedLiteralReadsItAsAnInterval() throws SQLException {
        assertValue("2020-01-03 00:00:00", "SELECT timestamp '2020-01-02' + '1 day'");
        assertColumnType("timestamp", "SELECT timestamp '2020-01-02' + '1 day'");
        assertValue("11:00:00", "SELECT time '10:00' + '1 hour'");
        assertColumnType("time", "SELECT time '10:00' + '1 hour'");
        assertValue("11:00:00", "SELECT '1 hour' + time '10:00'");
    }

    @Test
    void aMomentMinusAnUntypedLiteralReadsItAsAnotherMoment() {
        assertFails("22007", "invalid input syntax for type timestamp: \"1 day\"",
                "SELECT timestamp '2020-01-02' - '1 day'");
        assertFails("22007", "invalid input syntax for type time: \"1 hour\"",
                "SELECT time '10:00' - '1 hour'");
    }

    @Test
    void anIntervalScaledByAnUntypedLiteralReadsItAsANumber() throws SQLException {
        assertValue("2 days", "SELECT interval '1 day' * '2'");
        assertColumnType("interval", "SELECT interval '1 day' * '2'");
        assertValue("12:00:00", "SELECT interval '1 day' / '2'");
        assertColumnType("interval", "SELECT interval '1 day' / '2'");
    }

    @Test
    void theSameResolutionAppliesToADateColumn() throws SQLException {
        exec("DROP TABLE IF EXISTS xta_dt");
        exec("CREATE TABLE xta_dt(d date)");
        exec("INSERT INTO xta_dt VALUES (date '2020-01-01')");
        assertFails("42725", "operator is not unique: date + unknown", "SELECT d + '1' FROM xta_dt");
        assertFails("22007", "invalid input syntax for type date: \"1\"",
                "SELECT d - '1' FROM xta_dt");
        assertValue("2020-01-02", "SELECT d + 1 FROM xta_dt");
        assertColumnType("date", "SELECT d + 1 FROM xta_dt");
    }

    // ---- (c) the array mutators given an unknown array literal ----

    @Test
    void anUnknownArrayLiteralIsReadAsAnArray() throws SQLException {
        assertValue("{1,2,3}", "SELECT array_append('{1,2}', 3)");
        assertValue("{a,b,c}", "SELECT array_append('{a,b}', 'c')");
        assertValue("{1,2,NULL}", "SELECT array_append('{1,2}', NULL)");
        assertValue("{0,1,2}", "SELECT array_prepend(0, '{1,2}')");
        assertValue("{1,2,3}", "SELECT array_cat('{1,2}', '{3}')");
        assertValue("{1,2,3}", "SELECT array_cat('{1,2}'::int[], '{3}')");
        assertValue("{1,2,3}", "SELECT array_cat('{1,2}', '{3}'::int[])");
        assertValue("{1,3}", "SELECT array_remove('{1,2,3}', 2)");
        assertValue("{a,c}", "SELECT array_remove('{a,b,c}', 'b')");
        assertValue("{1,9,3}", "SELECT array_replace('{1,2,3}', 2, 9)");
        assertValue("{2}", "SELECT array_positions('{1,2,3}', 2)");
        assertValue("2", "SELECT array_position('{1,2,3}', 2)");
    }

    @Test
    void unnestOfAnUnknownLiteralHasNoUniqueCandidate() {
        assertFails("42725", "function unnest(unknown) is not unique",
                "SELECT * FROM unnest('{1,2,3}')");
    }

    @Test
    void arraysThatAlreadyCarryATypeKeepWorking() throws SQLException {
        assertValue("{1,2,3}", "SELECT array_append(ARRAY[1,2], 3)");
        assertValue("{1,2,3}", "SELECT array_cat(ARRAY[1,2], ARRAY[3])");
        assertValue("{1,9,3}", "SELECT array_replace('{1,2,3}'::int[], 2, 9)");
        assertValue("3", "SELECT array_length(ARRAY[1,2,3], 1)");
        assertValue("{1,2,3}", "SELECT '{1,2}'::int[] || 3");
        assertValue("1", "SELECT (SELECT min(u) FROM unnest('{1,2,3}'::int[]) AS u)");
    }

    // ---- (d) the declared result type of an operator ----

    @Test
    void dateArithmeticIsDeclaredAsADateNotAnInteger() throws SQLException {
        assertColumnType("date", "SELECT date '2020-01-01' + 1");
        assertColumnType("date", "SELECT date '2020-01-01' - 1");
        assertColumnType("date", "SELECT 1 + date '2020-01-01'");
        assertColumnType("int4", "SELECT date '2020-01-02' - date '2020-01-01'");
    }

    /** The whole point of the descriptor: pgjdbc decodes by it and threw on a date under int4. */
    @Test
    void aDateComesBackAsADateThroughGetObject() throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT date '2020-01-01' + 1")) {
            assertTrue(rs.next());
            assertEquals(java.sql.Date.valueOf("2020-01-02"), rs.getObject(1));
            assertEquals(java.sql.Date.valueOf("2020-01-02"), rs.getDate(1));
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '255.255.255.255'::inet - '0.0.0.0'::inet")) {
            assertTrue(rs.next());
            assertEquals(4294967295L, rs.getObject(1));
        }
    }

    @Test
    void dateAndTimeArithmeticIsDeclaredInItsOwnFamily() throws SQLException {
        assertColumnType("timestamp", "SELECT date '2020-01-01' + interval '1 day'");
        assertColumnType("timestamp", "SELECT timestamp '2020-01-01' + interval '1 day'");
        assertColumnType("interval", "SELECT timestamp '2020-01-02' - timestamp '2020-01-01'");
        assertColumnType("interval", "SELECT interval '1 day' + interval '1 hour'");
        assertColumnType("time", "SELECT time '10:00' + interval '1 hour'");
        assertColumnType("interval", "SELECT interval '1 day' * 2");
        assertColumnType("interval", "SELECT interval '1 day' / 2");
        assertColumnType("interval", "SELECT interval '1 day' * 2.5");
    }

    @Test
    void networkOperatorsAreDeclaredAsInetAndAsBool() throws SQLException {
        assertColumnType("inet", "SELECT '10.0.0.1'::inet & '255.0.0.0'::inet");
        assertColumnType("inet", "SELECT '192.168.1.1'::inet | '0.0.0.255'");
        assertColumnType("inet", "SELECT ~ '192.168.1.1'::inet");
        assertColumnType("int8", "SELECT '255.255.255.255'::inet - '0.0.0.0'::inet");
        assertColumnType("bool", "SELECT '10.0.0.1'::inet << '10.0.0.0/8'::inet");
        assertColumnType("bool", "SELECT '10.0.0.1'::inet <<= '10.0.0.0/8'::inet");
        assertColumnType("bool", "SELECT '10.0.0.0/8'::inet >> '10.0.0.1'::inet");
        assertColumnType("bool", "SELECT '10.0.0.0/8'::inet >>= '10.0.0.1'::inet");
        assertColumnType("bool", "SELECT '10.0.0.0/8'::inet && '10.0.0.1'::inet");
    }

    @Test
    void geometricOperatorsAreDeclaredAsShapesAndAsBool() throws SQLException {
        assertColumnType("bool", "SELECT '((0,0),(2,2))'::box @> '(1,1)'::point");
        assertColumnType("float8", "SELECT '(0,0)'::point <-> '(3,4)'::point");
        assertColumnType("bool", "SELECT '(1,2)'::point ~= '(1,2)'::point");
        assertColumnType("point", "SELECT '(1,2)'::point + '(3,4)'");
        assertColumnType("bool", "SELECT '<(0,0),5>'::circle && '<(1,1),5>'");
        assertColumnType("bool", "SELECT '[(0,0),(2,2)]'::lseg ?# '[(0,2),(2,0)]'");
    }

    @Test
    void arrayOperatorsAndFunctionsAreDeclaredAsArrays() throws SQLException {
        assertColumnType("_int4", "SELECT ARRAY[1,2] || ARRAY[3]");
        assertColumnType("bool", "SELECT ARRAY[1,2] @> ARRAY[1]");
        assertColumnType("bool", "SELECT ARRAY[1,2] && ARRAY[1]");
        assertColumnType("_int4", "SELECT array_append(ARRAY[1,2],3)");
        assertColumnType("_int4", "SELECT array_cat(ARRAY[1,2],ARRAY[3])");
        assertColumnType("_int4", "SELECT array_replace('{1,2,3}'::int[],2,9)");
        assertColumnType("_int4", "SELECT array_fill(1, ARRAY[3])");
        assertColumnType("_text", "SELECT string_to_array('a,b',',')");
        assertColumnType("int4", "SELECT * FROM unnest('{1,2,3}'::int[])");
    }

    @Test
    void rangeAndJsonbOperatorsCarryTheirOwnTypes() throws SQLException {
        assertColumnType("bool", "SELECT '[1,10)'::int4range @> 5");
        assertColumnType("bool", "SELECT '[1,3)'::int4range && '[2,5)'::int4range");
        assertValue("int4range", "SELECT pg_typeof('[1,3)'::int4range + '[2,5)'::int4range)");
        assertColumnType("bool", "SELECT '{\"a\":1}'::jsonb ?| ARRAY['a']");
        assertColumnType("bool", "SELECT '{\"a\":1}'::jsonb @> '{\"a\":1}'");
        assertColumnType("jsonb", "SELECT '{\"a\":1}'::jsonb -> 'a'");
        assertColumnType("jsonb", "SELECT '{\"a\":1,\"b\":2}'::jsonb - 'a'");
        assertColumnType("text", "SELECT '{\"a\":1}'::jsonb ->> 'a'");
    }

    @Test
    void textSearchRegexAndBitOperatorsCarryTheirOwnTypes() throws SQLException {
        assertColumnType("bool", "SELECT to_tsvector('simple','a b') @@ 'a'::tsquery");
        assertColumnType("tsvector", "SELECT 'a'::tsvector || 'b'");
        assertColumnType("bool", "SELECT 'abc' ~ 'a'");
        assertColumnType("bool", "SELECT 'abc' !~ 'z'");
        assertColumnType("bool", "SELECT 'abc' LIKE 'a%'");
        assertColumnType("bit", "SELECT B'101' & '111'");
        assertColumnType("float8", "SELECT 2 ^ 10");
        assertColumnType("int4", "SELECT @ -10");
    }

    @Test
    void pgTypeofIsDeclaredAsARegtype() throws SQLException {
        assertColumnType("regtype", "SELECT pg_typeof(1)");
        assertColumnType("regtype", "SELECT pg_typeof('a')");
        assertColumnType("regtype", "SELECT pg_typeof(date '2020-01-01' + 1)");
        assertValue("date", "SELECT pg_typeof(date '2020-01-01' + 1)");
        assertValue("unknown", "SELECT pg_typeof('a')");
    }

    @Test
    void aTsqueryBesideAnUntypedLiteralReadsItAsADocument() throws SQLException {
        assertValue("t", "SELECT 'a'::tsquery @@ 'a b'");
        assertValue("t", "SELECT 'cat'::tsquery @@ 'cat dog'");
    }

    // ---- the odds and ends the same resolution decides ----

    @Test
    void greatestAndLeastSettleOnOneTypeBeforeComparing() throws SQLException {
        assertValue("integer", "SELECT pg_typeof(GREATEST('10', 9))");
        assertValue("10", "SELECT GREATEST('10', 9)");
        assertValue("9", "SELECT LEAST('10', 9)");
        assertColumnType("int4", "SELECT GREATEST('10', 9)");
    }

    @Test
    void numericInputReadsTheNonDecimalIntegerForms() throws SQLException {
        assertValue("42", "SELECT '0x2a'::numeric");
        assertValue("42", "SELECT '0o52'::numeric");
        assertValue("42", "SELECT '0b101010'::numeric");
        assertValue("1000", "SELECT '1_000'::numeric");
        assertValue("42", "SELECT '0x2a'::float8");
        // the decimal forms are untouched
        assertValue("1.5", "SELECT '1.5'::numeric");
        assertFails("22P02", "invalid input syntax for type numeric", "SELECT 'zz'::numeric");
    }

    @Test
    void aRangeConcatenatedResolvesThroughText() throws SQLException {
        assertValue("[1.0,3.0)[2.0,5.0)", "SELECT '[1.0,3.0)'::numrange || '[2.0,5.0)'");
        assertValue("[1,3)[5,7)", "SELECT '[1,3)'::int4range || '[5,7)'");
        assertValue("x[1,3)", "SELECT 'x' || '[1,3)'::int4range");
        assertValue("[1,3)x", "SELECT '[1,3)'::int4range || 'x'");
        assertNoOperator("numrange || numrange",
                "SELECT '[1.0,3.0)'::numrange || '[2.0,5.0)'::numrange");
        assertNoOperator("int4range || int4range",
                "SELECT '[1,3)'::int4range || '[5,7)'::int4range");
    }

    @Test
    void twoPathsAreJoinedRatherThanTranslated() throws SQLException {
        assertNull(scalar("SELECT '((0,0),(1,1))'::path + '((2,2))'"));
        assertColumnType("path", "SELECT '((0,0),(1,1))'::path + '((2,2))'");
        // a path really translated by a point still is
        assertValue("[(1,1),(2,2)]", "SELECT '[(0,0),(1,1)]'::path + '(1,1)'::point");
    }

    @Test
    void isUnknownTakesABooleanAndNothingElse() throws SQLException {
        assertFails("42804", "argument of IS UNKNOWN must be type boolean, not type text",
                "SELECT 'a'::text IS UNKNOWN");
        assertFails("42804", "argument of IS NOT UNKNOWN must be type boolean, not type text",
                "SELECT 'a'::text IS NOT UNKNOWN");
        // an untyped literal is coerced to boolean and fails on its own input
        assertFails("22P02", "invalid input syntax for type boolean: \"a\"", "SELECT 'a' IS UNKNOWN");
        // a genuine boolean still answers
        assertValue("f", "SELECT true IS UNKNOWN");
        assertValue("t", "SELECT NULL::boolean IS UNKNOWN");
        assertValue("t", "SELECT (1 = 1) IS NOT UNKNOWN");
    }

    @Test
    void timeWithTimeZoneLiteralsParse() throws SQLException {
        assertValue("10:00:00+02", "SELECT time with time zone '10:00+02'");
        assertValue("10:00:00", "SELECT time without time zone '10:00'");
        assertColumnType("timetz", "SELECT time with time zone '10:00+02'");
    }
}
