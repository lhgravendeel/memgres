package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The jsonpath item methods and array accessors, and the json_each family called where a set of
 * records is expected rather than a table.
 *
 * <p>A path ending in {@code .type()}, {@code .size()} or {@code .abs()} used to select nothing,
 * which reads exactly like a path that matched nothing; {@code $[1 to 2]} and {@code $[last]}
 * used to fall back to the whole document. Strict mode used to let a subscript past the end of an
 * array pass silently, so a document the path had misread looked like a document with no match.
 *
 * <p>{@code jsonb_each} outside a FROM clause reported that no such function existed, because the
 * engine had no way to carry a record with its own column names through a select list.
 *
 * <p>Every expectation here was measured against PostgreSQL 18.
 */
class JsonPathMethodTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE jpm_t (id int, doc jsonb, j json)");
        exec("INSERT INTO jpm_t VALUES (1, '{\"a\":1,\"b\":2}', '{\"a\":1}'), "
                + "(2, '[1,2,3]', '[1]'), (3, NULL, NULL)");
        exec("CREATE VIEW jpm_v AS SELECT id, doc FROM jpm_t");
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

    private static List<String> column(String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) values.add(rs.getString(1));
        }
        return values;
    }

    /** Every row rendered as its columns joined by a pipe, so column counts are asserted too. */
    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i) == null ? "<null>" : rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static void assertFails(String state, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(state, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- item methods ----

    @Test
    void typeNamesTheKindOfItemThePathSelected() throws Exception {
        assertEquals("\"number\"", scalar("SELECT jsonb_path_query('2', '$.type()')"));
        assertEquals("\"string\"", scalar("SELECT jsonb_path_query('\"abc\"', '$.type()')"));
        assertEquals("\"array\"", scalar("SELECT jsonb_path_query('[1,2]', '$.type()')"));
        assertEquals("\"object\"", scalar("SELECT jsonb_path_query('{\"a\":1}', '$.type()')"));
        assertEquals("\"null\"", scalar("SELECT jsonb_path_query('null', '$.type()')"));
        assertEquals("\"boolean\"", scalar("SELECT jsonb_path_query('true', '$.type()')"));
    }

    @Test
    void typeAndSizeDescribeAnArrayRatherThanLookInsideIt() throws Exception {
        assertEquals("\"array\"", scalar("SELECT jsonb_path_query('[1,2,3]', 'lax $.type()')"));
        assertEquals("\"array\"", scalar("SELECT jsonb_path_query('[1,2,3]', 'strict $.type()')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('[1,2,3]', 'lax $.size()')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('[1,2,3]', 'strict $.size()')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('[[1,2],[3]]', 'lax $.size()')"));
    }

    @Test
    void sizeOfAnythingThatIsNotAnArrayIsOne() throws Exception {
        assertEquals("1", scalar("SELECT jsonb_path_query('1', '$.size()')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}', '$.size()')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('\"str\"', '$.size()')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('null', '$.size()')"));
    }

    @Test
    void theArithmeticMethodsRoundTheWayNumericDoes() throws Exception {
        assertEquals("2.5", scalar("SELECT jsonb_path_query('-2.5', '$.abs()')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('-2', '$.abs()')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('2.5', '$.floor()')"));
        assertEquals("-3", scalar("SELECT jsonb_path_query('-2.5', '$.floor()')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('2.5', '$.ceiling()')"));
        assertEquals("-2", scalar("SELECT jsonb_path_query('-2.5', '$.ceiling()')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('2', '$.double()')"));
        assertEquals("2.5", scalar("SELECT jsonb_path_query('2.5', '$.double()')"));
        assertEquals("2.5", scalar("SELECT jsonb_path_query('\"2.5\"', '$.double()')"));
    }

    @Test
    void laxUnwrapsAnArrayForAMethodThatWantsOneValueAndStrictDoesNot() throws Exception {
        assertEquals(List.of("1", "2", "3"),
                column("SELECT jsonb_path_query('[1,2,3]', 'lax $.abs()')"));
        assertEquals(List.of("1", "2", "3"),
                column("SELECT jsonb_path_query('[1,2,3]', 'strict $[*].abs()')"));
        assertEquals(List.of("1", "2"),
                column("SELECT jsonb_path_query('{\"a\":[1,-2]}', '$.a[*].abs()')"));
        assertFails("22036", "jsonpath item method .abs() can only be applied to a numeric value",
                "SELECT jsonb_path_query('[1,2,3]', 'strict $.abs()')");
        assertFails("22036", "jsonpath item method .floor() can only be applied to a numeric value",
                "SELECT jsonb_path_query('[1,2,3]', 'strict $.floor()')");
    }

    @Test
    void anArithmeticMethodOnSomethingThatIsNotANumberIsAnError() throws Exception {
        assertFails("22036", "jsonpath item method .abs() can only be applied to a numeric value",
                "SELECT jsonb_path_query('\"x\"', '$.abs()')");
        assertFails("22036", "jsonpath item method .abs() can only be applied to a numeric value",
                "SELECT jsonb_path_query('true', '$.abs()')");
        assertFails("22036", "jsonpath item method .abs() can only be applied to a numeric value",
                "SELECT jsonb_path_query('{\"a\":1}', 'strict $.abs()')");
        assertFails("22036",
                "argument \"abc\" of jsonpath item method .double() is invalid for type double precision",
                "SELECT jsonb_path_query('\"abc\"', '$.double()')");
    }

    @Test
    void keyvalueTurnsEveryMemberIntoAnObjectOfItsOwn() throws Exception {
        assertEquals("{\"id\": 0, \"key\": \"a\", \"value\": 1}",
                scalar("SELECT jsonb_path_query('{\"a\":1}', '$.keyvalue()')"));
        assertEquals(List.of("{\"id\": 0, \"key\": \"a\", \"value\": 1}",
                        "{\"id\": 0, \"key\": \"b\", \"value\": 2}"),
                column("SELECT jsonb_path_query('{\"a\":1,\"b\":2}', '$.keyvalue()')"));
        assertEquals("[{\"id\": 0, \"key\": \"a\", \"value\": 1}, {\"id\": 0, \"key\": \"b\", \"value\": 2}]",
                scalar("SELECT jsonb_path_query_array('{\"a\":1,\"b\":2}','$.keyvalue()')::text"));
        assertEquals("\"a\"", scalar("SELECT jsonb_path_query('{\"a\":1}','$.keyvalue().key')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}','$.keyvalue().value')"));
    }

    @Test
    void keyvalueOnAnythingThatIsNotAnObjectIsAnError() throws Exception {
        assertFails("2203C", "jsonpath item method .keyvalue() can only be applied to an object",
                "SELECT jsonb_path_query('[1]', 'lax $.keyvalue()')");
        assertFails("2203C", "jsonpath item method .keyvalue() can only be applied to an object",
                "SELECT jsonb_path_query('1', 'strict $.keyvalue()')");
        assertFails("2203C", "jsonpath item method .keyvalue() can only be applied to an object",
                "SELECT id, jsonb_path_query_array(doc,'$.keyvalue()')::text FROM jpm_t ORDER BY id");
    }

    @Test
    void datetimeKeepsTheShapeTheStringCameInWith() throws Exception {
        assertEquals("\"2020-01-01\"",
                scalar("SELECT jsonb_path_query('\"2020-01-01\"', '$.datetime()')"));
        assertEquals("\"2020-01-01T10:00:00\"",
                scalar("SELECT jsonb_path_query('\"2020-01-01T10:00:00\"', '$.datetime()')"));
        assertEquals("\"2020-01-01T10:00:00\"",
                scalar("SELECT jsonb_path_query('\"2020-01-01 10:00:00\"', '$.datetime()')"));
        assertEquals("\"10:00:00\"",
                scalar("SELECT jsonb_path_query('\"10:00:00\"', '$.datetime()')"));
        assertEquals("\"2020-01-01T10:00:00+00:00\"",
                scalar("SELECT jsonb_path_query('\"2020-01-01T10:00:00Z\"', '$.datetime()')"));
        assertEquals("[\"2020-01-01\"]",
                scalar("SELECT jsonb_path_query_array('{\"a\":\"2020-01-01\"}', '$.a.datetime()')::text"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"d\":\"2020-01-05\"}',"
                + "'$.d.datetime() > \"2020-01-01\".datetime()')"));
    }

    // ---- array accessors ----

    @Test
    void aRangeSubscriptSelectsEveryElementItSpans() throws Exception {
        assertEquals(List.of("2", "3"), column("SELECT jsonb_path_query('[1,2,3]', '$[1 to 2]')"));
        assertEquals(List.of("1"), column("SELECT jsonb_path_query('[1,2,3]', '$[0 to 0]')"));
        assertEquals(List.of("1", "2", "3"),
                column("SELECT jsonb_path_query('[1,2,3]', '$[0 to last]')"));
        assertEquals(List.of("1", "3"), column("SELECT jsonb_path_query('[1,2,3]', '$[0,2]')"));
        assertEquals(List.of("1", "2", "3"),
                column("SELECT jsonb_path_query('[1,2,3]', '$[0 to 1, 2]')"));
    }

    @Test
    void lastNamesTheEndOfTheArrayAndCanBeShifted() throws Exception {
        assertEquals("3", scalar("SELECT jsonb_path_query('[1,2,3]', '$[last]')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('[1,2,3]', '$[last-1]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[1,2,3]', '$[last+1]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[1,2,3]', 'lax $[last+5]')"));
    }

    @Test
    void laxClampsASubscriptAndStrictRefusesIt() throws Exception {
        assertEquals(List.of("2", "3"), column("SELECT jsonb_path_query('[1,2,3]', '$[1 to 5]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[1,2,3]', '$[2 to 1]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[1,2,3]', 'lax $[5]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[]','$[0]')"));
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query('[1]', 'strict $[1]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query('[1,2,3]','strict $[3]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query('[1,2,3]', 'strict $[1 to 5]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query('[1,2,3]', 'strict $[2 to 1]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query('[]','strict $[last]')");
    }

    @Test
    void laxIndexesANonArrayAsAnArrayOfOneAndStrictDoesNot() throws Exception {
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_path_query('{\"a\":1}', 'lax $[0]')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('1', '$[last]')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('1', 'lax $[*]')"));
        assertFails("22039", "jsonpath array accessor can only be applied to an array",
                "SELECT jsonb_path_query('{\"a\":1}', 'strict $[0]')");
        assertFails("22039", "jsonpath array accessor can only be applied to an array",
                "SELECT jsonb_path_query('1', 'strict $[last]')");
        assertFails("22039", "jsonpath wildcard array accessor can only be applied to an array",
                "SELECT jsonb_path_query('1', 'strict $[*]')");
    }

    @Test
    void aMemberAccessorNeedsAnObjectUnderStrictAndUnwrapsUnderLax() throws Exception {
        assertEquals("1", scalar("SELECT jsonb_path_query('[{\"a\":1}]','lax $.a')"));
        assertEquals(List.of("1", "2"),
                column("SELECT jsonb_path_query('[{\"a\":1},{\"a\":2}]','lax $.a')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('{\"a\":1}', 'lax $.a.b')"));
        assertFails("2203A", "jsonpath member accessor can only be applied to an object",
                "SELECT jsonb_path_query('{\"a\":1}', 'strict $.a.b')");
        assertFails("2203A", "jsonpath member accessor can only be applied to an object",
                "SELECT jsonb_path_query('[1]', 'strict $.a')");
        assertFails("2203A", "jsonpath member accessor can only be applied to an object",
                "SELECT jsonb_path_query('1', 'strict $.a')");
        assertFails("2203A", "does not contain key \"b\"",
                "SELECT jsonb_path_query('{\"a\":1}', 'strict $.b')");
    }

    // ---- predicates, silence and the operators ----

    @Test
    void aPredicateThatCannotWalkTheDocumentIsUnknownRatherThanAnError() throws Exception {
        assertNull(scalar("SELECT jsonb_path_match('{\"a\":[1,2,3]}', 'strict $.b == 1')"));
        assertNull(scalar("SELECT jsonb_path_match('{\"a\":1}','strict $.b == 1')"));
        assertEquals("f", scalar("SELECT jsonb_path_match('{\"a\":1}','lax $.b == 1')"));
        assertEquals("t", scalar("SELECT jsonb_path_match('{\"a\":1}','$.a == 1')"));
        assertEquals("t", scalar("SELECT jsonb_path_match('{\"a\":1}','strict $.a == 1')"));
        assertEquals("t", scalar("SELECT jsonb_path_match('[1,2]','$[*] > 1')"));
        assertEquals("t", scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, 'exists($.a ? (@ == 2))')"));
    }

    @Test
    void aMatchNeedsExactlyOneBooleanAndSaysSoWhenItHasNone() throws Exception {
        assertEquals("t", scalar("SELECT jsonb_path_match('{\"a\":true}', '$.a')"));
        assertFails("22038", "single boolean result is expected",
                "SELECT jsonb_path_match('{\"a\":1}', '$.a')");
        assertFails("22038", "single boolean result is expected",
                "SELECT jsonb_path_match('{\"a\":1}','lax $.b')");
        assertFails("22038", "single boolean result is expected",
                "SELECT jsonb_path_match('[true,true]','$[*]')");
        assertFails("2203A", "does not contain key \"b\"",
                "SELECT jsonb_path_match('{\"a\":1}','strict $.b')");
    }

    @Test
    void theSilentFlagAndTheOperatorsAnswerNullWhereTheyWouldHaveRaised() throws Exception {
        assertNull(scalar("SELECT '[1]'::jsonb @? 'strict $[1]'"));
        assertNull(scalar("SELECT jsonb_path_exists('[1]', 'strict $[1]', '{}', true)"));
        assertNull(scalar("SELECT '{\"a\":1}'::jsonb @@ '$.a'"));
        assertNull(scalar("SELECT '{\"a\":1}'::jsonb @@ 'strict $.b == 1'"));
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_exists('[1]', 'strict $[1]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query_array('[1]','strict $[1]')");
        assertFails("22033", "jsonpath array subscript is out of bounds",
                "SELECT jsonb_path_query_first('[1]','strict $[1]')");
    }

    @Test
    void aWholePathMadeOfAComparisonAlwaysProducesAnItem() throws Exception {
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":1}','$.a == 2')"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":1}','lax $.b == 1')"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":1}','strict $.b == 1')"));
        assertEquals("t", scalar("SELECT '{\"a\":1}'::jsonb @? '$.a == 2'"));
        // A filter is not a predicate path: it selects, and can select nothing
        assertEquals("f", scalar("SELECT jsonb_path_exists('[1,2]','$[*] ? (@ > 5)')"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 2)')"));
    }

    // ---- json_each in the select list ----

    @Test
    void eachProducesOneRecordPerMember() throws Exception {
        assertEquals(List.of("(a,1)"), column("SELECT jsonb_each('{\"a\":1}'::jsonb)"));
        assertEquals(List.of("(a,1)"), column("SELECT json_each('{\"a\":1}'::json)"));
        assertEquals(List.of("(a,1)"), column("SELECT jsonb_each_text('{\"a\":1}'::jsonb)"));
        assertEquals(List.of("(a,1)"), column("SELECT json_each_text('{\"a\":1}'::json)"));
        assertEquals(List.of("(a,1)", "(b,2)"),
                column("SELECT jsonb_each('{\"a\":1,\"b\":2}'::jsonb)"));
        assertEquals(List.of("(a,1)"), column("SELECT jsonb_each('{\"a\":1}'::jsonb)::text"));
    }

    @Test
    void aRecordFieldQuotesWhateverWouldMoveTheCommas() throws Exception {
        assertEquals(List.of("(a,\"\"\"x y\"\"\")"),
                column("SELECT jsonb_each('{\"a\":\"x y\"}'::jsonb)"));
        assertEquals(List.of("(a,\"x y\")"),
                column("SELECT jsonb_each_text('{\"a\":\"x y\"}'::jsonb)"));
        assertEquals(List.of("(a,null)", "(b,\"[1, 2]\")", "(c,\"{\"\"d\"\": 1}\")"),
                column("SELECT jsonb_each('{\"a\":null,\"b\":[1,2],\"c\":{\"d\":1}}'::jsonb)"));
        // A json null becomes SQL NULL in the _text form, which prints as an empty field
        assertEquals(List.of("(a,)", "(b,\"[1, 2]\")"),
                column("SELECT jsonb_each_text('{\"a\":null,\"b\":[1,2]}'::jsonb)"));
    }

    @Test
    void anEmptyOrNullDocumentProducesNoRows() throws Exception {
        assertEquals(List.of(), column("SELECT jsonb_each('{}'::jsonb)"));
        assertEquals(List.of(), column("SELECT jsonb_each(NULL::jsonb)"));
        assertEquals(List.of(), column("SELECT jsonb_each_text(NULL::jsonb)"));
        assertEquals(List.of(), column("SELECT json_each(NULL::json)"));
        assertEquals(List.of(), column("SELECT jsonb_each(doc) FROM jpm_t WHERE id=3"));
    }

    @Test
    void eachOnAnythingThatIsNotAnObjectIsAContainerError() throws Exception {
        assertFails("22023", "cannot deconstruct an array as an object",
                "SELECT json_each('[1,2]'::json)");
        assertFails("22023", "cannot deconstruct a scalar",
                "SELECT json_each('3'::json)");
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT jsonb_each('3'::jsonb)");
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT jsonb_each('\"s\"'::jsonb)");
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT jsonb_each('[1,2]'::jsonb)");
        assertFails("22023", "cannot call jsonb_each_text on a non-object",
                "SELECT jsonb_each_text('[1,2]'::jsonb)");
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT jsonb_each(doc) FROM jpm_t ORDER BY 1");
    }

    @Test
    void aRecordCanBeExpandedIntoColumnsOrReadFieldByField() throws Exception {
        assertEquals(List.of("a|1"), rows("SELECT (jsonb_each('{\"a\":1}'::jsonb)).*"));
        assertEquals(List.of("a|1", "b|2"), rows("SELECT (jsonb_each(doc)).* FROM jpm_t WHERE id=1"));
        assertEquals(List.of("a", "b"),
                column("SELECT (jsonb_each('{\"a\":1,\"b\":2}'::jsonb)).key"));
        assertEquals(List.of("1", "2"),
                column("SELECT (jsonb_each('{\"a\":1,\"b\":2}'::jsonb)).value"));
        assertEquals(List.of("a", "b"),
                column("SELECT (jsonb_each_text(doc)).key FROM jpm_t WHERE id=1 ORDER BY 1"));
        assertFails("42703", "could not identify column \"nosuch\" in record data type",
                "SELECT (jsonb_each('{\"a\":1}'::jsonb)).nosuch");
    }

    @Test
    void aRecordSetExpandsTheRowItSitsIn() throws Exception {
        assertEquals(List.of("1|(a,1)", "1|(b,2)"),
                rows("SELECT id, jsonb_each(doc) FROM jpm_t WHERE id=1"));
        assertEquals(List.of("(a,1)|(b,2)", "<null>|(c,3)"),
                rows("SELECT jsonb_each('{\"a\":1}'::jsonb), jsonb_each('{\"b\":2,\"c\":3}'::jsonb)"));
    }

    @Test
    void theJsonTypeWalksItsMembersInTheOrderTheyWereWritten() throws Exception {
        assertEquals(List.of("(b,1)", "(a,2)"),
                column("SELECT json_each('{\"b\":1,\"a\":2}'::json)"));
        assertEquals(List.of("b", "a"),
                column("SELECT json_object_keys('{\"b\":1,\"a\":2}'::json)"));
        assertEquals(List.of("b", "aa", "a"),
                column("SELECT * FROM json_object_keys('{\"b\":1,\"aa\":2,\"a\":3}'::json)"));
        // jsonb stores its keys shortest first, so it walks them in that order instead
        assertEquals(List.of("(a,2)", "(b,1)"),
                column("SELECT jsonb_each('{\"b\":1,\"a\":2}'::jsonb)"));
        assertEquals(List.of("a", "b", "aa"),
                column("SELECT * FROM jsonb_object_keys('{\"b\":1,\"aa\":2,\"a\":3}'::jsonb)"));
    }

    // ---- neighbours that must keep working ----

    @Test
    void theFromClauseFormOfEachIsUnchanged() throws Exception {
        assertEquals(List.of("a|1", "b|2"),
                rows("SELECT * FROM jsonb_each('{\"a\":1,\"b\":2}'::jsonb)"));
        assertEquals(List.of("a|1", "b|2"),
                rows("SELECT k, v FROM jsonb_each('{\"a\":1,\"b\":2}'::jsonb) AS t(k,v)"));
        assertEquals(List.of("a|x"), rows("SELECT * FROM json_each_text('{\"a\":\"x\"}'::json)"));
        assertEquals(List.of("a|1", "b|2"), rows(
                "SELECT k, v::text FROM (SELECT doc FROM jpm_t WHERE id=1) s,"
                        + " jsonb_each(s.doc) AS e(k,v) ORDER BY k"));
        assertEquals(List.of("a|1", "b|2"), rows(
                "SELECT k, v::text FROM (SELECT doc FROM jpm_t WHERE id=1) s,"
                        + " LATERAL jsonb_each(s.doc) AS e(k,v) ORDER BY k"));
    }

    @Test
    void theOrdinaryPathShapesStillSelectWhatTheyDid() throws Exception {
        assertEquals(List.of("1", "2", "3"),
                column("SELECT jsonb_path_query('{\"a\":[1,2,3]}', '$.a[*]')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('{\"a\":[1,2,3]}', '$.a[1]')"));
        assertEquals("[1, 2, 3]", scalar("SELECT jsonb_path_query_array('[1,2,3]', '$[*]')"));
        assertEquals("1", scalar("SELECT jsonb_path_query_first('[1,2,3]', '$[*]')"));
        assertEquals(List.of("2", "3"), column("SELECT jsonb_path_query('[1,2,3]','$[*] ? (@ > 1)')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}', 'strict $.a')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('{\"a\":{\"b\":2}}', 'strict $.a.b')"));
        assertEquals(List.of("1", "2", "3"), column("SELECT jsonb_path_query('[1,2,3]', 'strict $[*]')"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('{\"a\":1}', 'lax $.b')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}','$.*')"));
        assertEquals("t", scalar("SELECT '{\"a\":1}'::jsonb @? '$.a'"));
        assertEquals("t", scalar("SELECT '{\"a\":1}'::jsonb @@ '$.a == 1'"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":1}', '$.a')"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('[1]', 'strict $[0]')"));
    }

    @Test
    void theOtherSetReturningJsonFunctionsAreUnchanged() throws Exception {
        assertEquals(List.of("1", "2"), column("SELECT jsonb_array_elements('[1,2]'::jsonb)"));
        assertEquals(List.of("a"), column("SELECT jsonb_object_keys('{\"a\":1}'::jsonb)"));
        assertEquals(List.of("1", "2", "3"), column("SELECT jsonb_path_query('[1,2,3]','$[*]')"));
        assertEquals(List.of("1", "2"), column("SELECT generate_series(1,2)"));
        assertEquals(List.of("1|1", "1|2"), rows("SELECT id, generate_series(1,2) FROM jpm_t WHERE id=1"));
    }

    @Test
    void theMethodsReadAColumnThroughAViewASubqueryAndAGroupedQuery() throws Exception {
        assertEquals(List.of("1|[\"object\"]", "2|[\"array\"]", "3|<null>"),
                rows("SELECT id, jsonb_path_query_array(doc,'$.type()')::text FROM jpm_t ORDER BY id"));
        assertEquals(List.of("1|[1]", "2|[3]", "3|<null>"),
                rows("SELECT id, jsonb_path_query_array(doc,'$.size()')::text FROM jpm_t ORDER BY id"));
        assertEquals(List.of("1|[{\"a\": 1, \"b\": 2}]", "2|[1, 2]", "3|<null>"),
                rows("SELECT id, jsonb_path_query_array(doc,'$[0 to 1]')::text FROM jpm_t ORDER BY id"));
        assertEquals(List.of("1|[{\"a\": 1, \"b\": 2}]", "2|[3]", "3|<null>"),
                rows("SELECT id, jsonb_path_query_array(doc,'$[last]')::text FROM jpm_t ORDER BY id"));
        assertEquals(List.of("1"), column("SELECT id FROM jpm_v WHERE doc @? '$.a' ORDER BY id"));
        assertEquals(List.of("1"), column(
                "SELECT id FROM (SELECT id, doc FROM jpm_t) sub WHERE sub.doc @? '$.a' ORDER BY id"));
        assertEquals(List.of("1"), column("SELECT id FROM jpm_t WHERE jsonb_path_match(doc,'$.a == 1') ORDER BY id"));
        assertEquals(List.of("1|[\"object\"]", "2|[\"array\"]", "3|<null>"), rows(
                "SELECT id, jsonb_path_query_array(doc,'$.type()')::text AS t FROM jpm_t"
                        + " GROUP BY id, doc ORDER BY id"));
        assertEquals(java.util.Arrays.asList("[1]", "[3]", null), column(
                "SELECT jsonb_path_query_array(doc,'$.size()')::text FROM jpm_t"
                        + " ORDER BY jsonb_path_query_array(doc,'$.size()')::text"));
        assertEquals(List.of("1"), column("SELECT count(*) FROM jpm_t WHERE doc @? '$.a'"));
    }

    @Test
    void aNullDocumentStaysNullThroughEveryPathFunction() throws Exception {
        assertNull(scalar("SELECT jsonb_path_query_first(NULL::jsonb,'$.a')"));
        assertNull(scalar("SELECT jsonb_path_query_array(NULL::jsonb,'$.a')"));
        assertNull(scalar("SELECT jsonb_path_exists(NULL::jsonb,'$.a')"));
        assertNull(scalar("SELECT jsonb_path_match(NULL::jsonb,'$.a == 1')"));
        assertNull(scalar("SELECT NULL::jsonb @? '$.a'"));
        assertEquals(List.of(), column("SELECT jsonb_path_query(NULL::jsonb,'$.a')"));
        assertEquals(List.of("1|1", "2|<null>", "3|<null>"),
                rows("SELECT id, jsonb_path_query_first(doc,'$.a')::text FROM jpm_t ORDER BY id"));
        assertEquals(List.of("1|true", "2|false", "3|<null>"),
                rows("SELECT id, (jsonb_path_exists(doc,'$.a'))::text FROM jpm_t ORDER BY id"));
    }
}
