package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A jsonpath is a path, not a text substitution.
 *
 * <p>The engine was a walker over the path's characters. It found its filter with
 * {@code indexOf('?')}, so a path with two filters or a filter with a bracket in it was read
 * wrong; it handled one filter shape, so anything else selected nothing at all rather than
 * failing; and it carried a table of eight method names, leaving the other twelve unknown. What a
 * path actually is is a grammar — roots, wildcards, any-level descent, subscripts written as
 * expressions, filters that nest and combine, arithmetic, and twenty item methods — and each of
 * its accessors has a lax reading and a strict one, differing in whether an array is unwrapped, a
 * scalar wrapped, or a miss refused.
 *
 * <p>Filters answer in three values, because two items of different kinds are not unequal but
 * incomparable, and it is the errors from walking a document that the {@code silent} argument and
 * the {@code @?} and {@code @@} operators turn into unknown — a path that does not parse is a
 * syntax error however loudly the caller asked for silence. {@code .datetime()} and its typed
 * relatives make a value that is a date rather than the string it prints as, so which of the five
 * shapes it has decides what it may be cast to and what it compares with.
 */
class JsonPathEngineTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE jpe_doc (id int, j jsonb)");
            st.execute("INSERT INTO jpe_doc VALUES"
                    + " (1, '{\"a\":[{\"b\":[1,2,3]},{\"b\":[4]}]}'), (2, '{\"a\":[]}')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The one value a path query answers with, as text. */
    private static String query(String json, String path) throws SQLException {
        return one("SELECT jsonb_path_query_array('" + json + "', '" + path + "')");
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The message itself, without the position and the hint the driver appends to it. */
    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0];
        }
    }

    // ----------------------------------------------------------------- parsing

    /** A filter is found by parsing the path, so a second one after it is a second filter. */
    @Test
    void filtersMayFollowEachOtherAndStandPartWayAlongAPath() throws Exception {
        assertEquals("[2, 3]", query("[1,2,3]", "$[*] ? (@ > 1) ? (@ < 4)"));
        assertEquals("[2, 3]",
                query("{\"a\":[{\"b\":[1,2,3]},{\"b\":[4]}]}",
                        "$.a[*] ? (@.b.size() > 1).b[*] ? (@ > 1)"));
    }

    /** A filter's own body may hold the brackets and question marks the walker cut the path at. */
    @Test
    void aFilterMayHoldBracketsAndFiltersOfItsOwn() throws Exception {
        assertEquals("[{\"b\": [1, 2]}]",
                query("[{\"b\":[1,2]},{\"b\":[3]}]", "$[*] ? (@.b[0] < 3)"));
        assertEquals("[{\"a\": [1, 2]}]",
                query("[{\"a\":[1,2]},{\"a\":[9]}]", "$[*] ? (exists (@.a[*] ? (@ < 3)))"));
    }

    /** The logical operators combine predicates rather than being characters in a substitution. */
    @Test
    void predicatesCombine() throws Exception {
        assertEquals("[{\"x\": 1, \"y\": 2}]",
                query("[{\"x\":1,\"y\":2},{\"x\":3,\"y\":1}]", "$[*] ? (@.x > 0 && @.y > 1)"));
        assertEquals("[{\"x\": 1}, {\"x\": 5}]",
                query("[{\"x\":1},{\"x\":5},{\"x\":3}]", "$[*] ? (@.x < 2 || @.x > 4)"));
        assertEquals("[{\"x\": 1}]", query("[{\"x\":1},{\"x\":5}]", "$[*] ? (!(@.x > 2))"));
    }

    /** A key is whatever the path quotes, dots and all, and space around a dot is not a key. */
    @Test
    void aKeyIsWhatThePathNames() throws Exception {
        assertEquals("[1]", query("{\"a.b\":1}", "$.\"a.b\""));
        assertEquals("[1]", query("{\"a\":1}", "$ . a"));
        assertEquals("[1]", query("{\"a\":1}", "$ /* the whole document */ .a"));
    }

    /** Any-level descent, and the levels it may be bounded to. */
    @Test
    void anyLevelDescentReachesEveryDepthOrTheDepthsItIsGiven() throws Exception {
        assertEquals("[1]", query("{\"a\":{\"b\":{\"c\":1}}}", "$.**.c"));
        assertEquals("[{\"b\": {\"c\": 1}}]", query("{\"a\":{\"b\":{\"c\":1}}}", "$.**{1}"));
        assertEquals("[1]", query("{\"a\":{\"b\":{\"c\":1}}}", "$.**{1 to 2}.c"));
    }

    /** A subscript is an expression, and may be several. */
    @Test
    void aSubscriptIsAnExpression() throws Exception {
        assertEquals("[2, 3]", query("[1,2,3,4]", "$[1 to 2]"));
        assertEquals("[4]", query("[1,2,3,4]", "$[last]"));
        assertEquals("[3]", query("[1,2,3,4]", "$[last - 1]"));
        assertEquals("[1, 3, 4]", query("[1,2,3,4]", "$[0, 2 to 3]"));
    }

    /** last names the end of the array being subscripted, so outside one it names nothing. */
    @Test
    void lastBelongsToASubscript() {
        assertEquals("42601", stateOf("SELECT jsonb_path_query_array('1', '$ ? (last > 0)')"));
        assertEquals("ERROR: LAST is allowed only in array subscripts",
                messageOf("SELECT jsonb_path_query_array('1', '$ ? (last > 0)')"));
    }

    /** A name the path does not have is refused rather than quietly selecting nothing. */
    @Test
    void aPathThatDoesNotParseIsRefused() {
        assertEquals("42601", stateOf("SELECT jsonb_path_query_array('1', '$.nosuchmethod()')"));
        assertEquals("42601", stateOf("SELECT jsonb_path_query_array('1', '$ &')"));
        assertEquals("42601", stateOf("SELECT jsonb_path_query_array('1', '(1')"));
        assertEquals("ERROR: syntax error at end of jsonpath input",
                messageOf("SELECT jsonb_path_query_array('1', 'a.b')"));
        assertEquals("ERROR: syntax error at or near \"&\" of jsonpath input",
                messageOf("SELECT jsonb_path_query_array('1', '$ &')"));
    }

    // -------------------------------------------------------------- arithmetic

    /** Arithmetic is an expression with an order of operations and parentheses. */
    @Test
    void arithmeticIsAnExpression() throws Exception {
        assertEquals("[3]", query("[1,2,3]", "$[*] ? ((@ + 1) * 2 > 6)"));
        assertEquals("[2]", query("{\"a\":5}", "$.a % 3"));
        assertEquals("[-5]", query("{\"a\":5}", "-$.a"));
        assertEquals("[-1, -2]", query("{\"a\":[1,2]}", "-$.a[*]"));
    }

    /** Division keeps numeric's scale, and by zero it is refused as any division is. */
    @Test
    void divisionIsNumericDivision() throws Exception {
        assertEquals("[2.5000000000000000]", query("{\"a\":5}", "$.a / 2"));
        assertEquals("22012", stateOf("SELECT jsonb_path_query('{\"a\":1}', '$.a / 0')"));
    }

    /** An operand of arithmetic is one number; a sequence of them is not a number. */
    @Test
    void anOperandIsASingleNumber() {
        assertEquals("22038", stateOf("SELECT jsonb_path_query('{\"a\":[1,2]}', '$.a[*] + 1')"));
        assertEquals("ERROR: left operand of jsonpath operator + is not a single numeric value",
                messageOf("SELECT jsonb_path_query('{\"a\":[1,2]}', '$.a[*] + 1')"));
    }

    // --------------------------------------------------------------- three-valued

    /** Two items of different kinds are incomparable, which is neither true nor false. */
    @Test
    void aComparisonOfUnlikeKindsIsUnknown() throws Exception {
        assertEquals("null", one("SELECT jsonb_path_query('[1]', '$[*] > \"a\"')"));
        assertEquals("true", one("SELECT jsonb_path_query('[1]', '($[*] > \"a\") is unknown')"));
        // An array is never compared with anything, not even an array of the same items --
        // though in lax an operand of one item is unwrapped down to the item first.
        assertEquals("null", one("SELECT jsonb_path_query('[[1],[1]]', 'strict $[0] == $[1]')"));
        assertEquals("true", one("SELECT jsonb_path_query('[[1],[1]]', '$[0] == $[1]')"));
    }

    /** null is the one kind every other value is unequal to and none is ordered against. */
    @Test
    void nullIsUnequalToEverythingAndOrderedAgainstNothing() throws Exception {
        assertEquals("true", one("SELECT jsonb_path_query('[1]', '$[*] != null')"));
        assertEquals("true", one("SELECT jsonb_path_query('[null]', '$[*] == null')"));
        assertEquals("false", one("SELECT jsonb_path_query('[null]', '$[*] > null')"));
    }

    /** The predicates that are not comparisons. */
    @Test
    void theOtherPredicates() throws Exception {
        assertEquals("[\"abc\"]", query("[\"abc\",\"abd\"]", "$[*] ? (@ starts with \"abc\")"));
        assertEquals("[\"abc\"]", query("[\"abc\",\"xbc\"]", "$[*] ? (@ like_regex \"^a\")"));
        assertEquals("[\"ABC\"]",
                query("[\"ABC\",\"xbc\"]", "$[*] ? (@ like_regex \"^a\" flag \"i\")"));
        assertEquals("[{\"a\": 1}]", query("[{\"a\":1},{\"b\":1}]", "$[*] ? (exists (@.a))"));
    }

    /** A path made wholly of a predicate answers once, with the answer. */
    @Test
    void aPredicatePathAnswersOnce() throws Exception {
        assertEquals("t", one("SELECT jsonb_path_match('{\"a\":[1,2]}', '$.a[*] == 1')"));
        assertEquals("t", one("SELECT '{\"a\":1}'::jsonb @@ '$.a == 1'"));
        // Asked whether it selects anything, a predicate always does: unknown is still an answer.
        assertEquals("t", one("SELECT jsonb_path_exists('{\"a\":1}', '$.b == 1')"));
    }

    /** jsonb_path_match wants one boolean and says so rather than guessing at anything else. */
    @Test
    void matchWantsASingleBoolean() {
        assertEquals("22038", stateOf("SELECT jsonb_path_match('{\"a\":1}', '$.a')"));
        assertEquals("ERROR: single boolean result is expected",
                messageOf("SELECT jsonb_path_match('{\"a\":1}', '$.a')"));
    }

    // ------------------------------------------------------------- lax and strict

    /** lax unwraps an array one level on its way into an accessor; strict leaves it alone. */
    @Test
    void laxUnwrapsOneLevel() throws Exception {
        assertEquals("[1]", query("[[{\"a\":1}]]", "lax $[*].a"));
        assertEquals("2203A",
                stateOf("SELECT jsonb_path_query_array('[[{\"a\":1}]]', 'strict $[*].a')"));
        assertEquals("[1, 2]", query("[{\"a\":1},{\"a\":2}]", "lax $.a"));
    }

    /** lax wraps a scalar where an array was wanted; strict refuses. */
    @Test
    void laxWrapsAScalar() throws Exception {
        assertEquals("[1]", query("1", "lax $[*]"));
        assertEquals("[1]", query("1", "lax $[0]"));
        assertEquals("[1]", query("1", "lax $.size()"));
        assertEquals("22039", stateOf("SELECT jsonb_path_query_array('1', 'strict $[*]')"));
        assertEquals("ERROR: jsonpath item method .size() can only be applied to an array",
                messageOf("SELECT jsonb_path_query_array('1', 'strict $.size()')"));
    }

    /** A key or an index the document does not have is nothing in lax and a mistake in strict. */
    @Test
    void aMissIsNothingInLaxAndAMistakeInStrict() throws Exception {
        assertEquals("[]", query("{\"a\":1}", "lax $.b"));
        assertEquals("2203A", stateOf("SELECT jsonb_path_query_array('{\"a\":1}', 'strict $.b')"));
        assertEquals("ERROR: JSON object does not contain key \"b\"",
                messageOf("SELECT jsonb_path_query_array('{\"a\":1}', 'strict $.b')"));
        assertEquals("[]", query("[1,2]", "lax $[5]"));
        assertEquals("22033", stateOf("SELECT jsonb_path_query_array('[1,2]', 'strict $[5]')"));
    }

    /** .type() and .size() describe the item they are handed, so neither unwraps it. */
    @Test
    void typeAndSizeDoNotUnwrap() throws Exception {
        assertEquals("[\"array\"]", query("[[1,2]]", "lax $[*].type()"));
        assertEquals("[\"array\"]", query("{\"a\":[1,2]}", "lax $.a.type()"));
    }

    /** Any-level descent neither unwraps nor wraps, and takes in the item it started from. */
    @Test
    void anyLevelDescentUnwrapsNothing() throws Exception {
        assertEquals("[[[1, 2]], [1, 2], 1, 2]", query("[[1,2]]", "lax $.**"));
    }

    // ---------------------------------------------------------------- silence

    /** The silent argument turns a walk that could not be made into no answer at all. */
    @Test
    void silenceSwallowsTheErrorsOfWalkingTheDocument() throws Exception {
        assertEquals("[]",
                one("SELECT jsonb_path_query_array('{\"a\":1}', 'strict $.b', '{}', true)"));
        assertEquals("NULL",
                one("SELECT jsonb_path_query_first('{\"a\":1}', 'strict $.b', '{}', true)"));
        assertEquals("NULL",
                one("SELECT jsonb_path_exists('{\"a\":1}', 'strict $.b', '{}', true)"));
        // The operators are the silent form of the functions: unknown, not false.
        assertEquals("NULL", one("SELECT '{\"a\":1}'::jsonb @? 'strict $.b'"));
        assertEquals("NULL", one("SELECT '{\"a\":1}'::jsonb @@ 'strict $.b == 1'"));
        // Asked the same thing without silence, the function raises.
        assertEquals("2203A", stateOf("SELECT jsonb_path_exists('{\"a\":1}', 'strict $.b')"));
    }

    /** A path that does not parse is a syntax error however loudly silence was asked for. */
    @Test
    void silenceDoesNotSwallowASyntaxError() {
        assertEquals("42601",
                stateOf("SELECT jsonb_path_query_array('1', 'a', '{}', true)"));
    }

    /** The functions are strict, so a NULL in any argument written makes the call NULL. */
    @Test
    void aNullArgumentNullsTheWholeCall() throws Exception {
        assertEquals("NULL",
                one("SELECT jsonb_path_query_array('{\"a\":1}', 'strict $.b', NULL, true)"));
        assertEquals("NULL", one("SELECT jsonb_path_query_array('{\"a\":1}', '$.a', '{}', NULL)"));
        assertEquals("NULL", one("SELECT jsonb_path_exists('{\"a\":1}', '$.a', NULL)"));
        assertEquals(0,
                rows("SELECT * FROM jsonb_path_query('{\"a\":1}', '$.a', NULL) t").size());
    }

    /** $name is read out of the vars argument, which has to be an object to have names in it. */
    @Test
    void variablesComeFromTheVarsArgument() throws Exception {
        assertEquals("[2]", one("SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $m)',"
                + " '{\"m\":1}')"));
        assertEquals("42704", stateOf("SELECT jsonb_path_query_array('[1,2]',"
                + " '$[*] ? (@ > $x)', '{\"y\":1}')"));
        assertEquals("22023",
                stateOf("SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $x)', '1')"));
        assertEquals("ERROR: \"vars\" argument is not an object",
                messageOf("SELECT jsonb_path_query_array('[1,2]', '$[*] ? (@ > $x)', '1')"));
    }

    // ----------------------------------------------------------- item methods

    /** A number is converted as a cast converts it; a string is read as the target is written. */
    @Test
    void theNumericConversions() throws Exception {
        assertEquals("[2]", query("1.7", "$.integer()"));
        assertEquals("22036", stateOf("SELECT jsonb_path_query('\"1.7\"', '$.integer()')"));
        assertEquals("[123.46]", query("123.456", "$.decimal(5, 2)"));
        assertEquals("22036", stateOf("SELECT jsonb_path_query('123.456', '$.decimal(2, 1)')"));
        assertEquals("[1.5]", query("-1.5", "$.abs()"));
        assertEquals("[-2]", query("-1.5", "$.floor()"));
        assertEquals("[-1]", query("-1.5", "$.ceiling()"));
    }

    /**
     * A double is the nearest double, and the number that comes back is the one it prints as
     * rather than the exact binary value it holds — which is 1 and three hundred zeros rather
     * than 1 and three hundred digits of accident.
     */
    @Test
    void aDoubleIsWhatTheDoublePrintsAs() throws Exception {
        assertEquals("[1" + "0".repeat(300) + "]", query("1e300", "$.double()"));
    }

    /** inf and nan are values float8 can be written as but that a jsonpath has nowhere to put. */
    @Test
    void infinityIsRefusedForWhatItIs() {
        assertEquals("ERROR: NaN or Infinity is not allowed for jsonpath item method .double()",
                messageOf("SELECT jsonb_path_query('\"inf\"', '$.double()')"));
    }

    /** A method applied to the wrong kind of item says which kinds it takes. */
    @Test
    void aMethodNamesTheKindsItTakes() {
        assertEquals("ERROR: jsonpath item method .integer() can only be applied to a string or"
                + " numeric value", messageOf("SELECT jsonb_path_query('true', '$.integer()')"));
        assertEquals("ERROR: jsonpath item method .abs() can only be applied to a numeric value",
                messageOf("SELECT jsonb_path_query('\"x\"', '$.abs()')"));
        assertEquals("ERROR: jsonpath item method .keyvalue() can only be applied to an object",
                messageOf("SELECT jsonb_path_query_array('1', '$.keyvalue()')"));
    }

    /** The conversions hold their target's range. */
    @Test
    void aConvertedNumberHasToFit() {
        assertEquals("22036", stateOf("SELECT jsonb_path_query('2147483648', '$.integer()')"));
        assertEquals("22036",
                stateOf("SELECT jsonb_path_query('9223372036854775808', '$.bigint()')"));
    }

    /** .string() writes the value the way its type writes it. */
    @Test
    void stringWritesTheValueOut() throws Exception {
        assertEquals("[\"true\"]", query("true", "$.string()"));
        assertEquals("[\"1.20\"]", query("1.20", "$.string()"));
    }

    // ------------------------------------------------------------- datetimes

    /** What a string spells is what it is read as, and that is what .type() reports. */
    @Test
    void aDatetimeIsTheShapeItsStringSpells() throws Exception {
        assertEquals("[\"date\"]", query("\"2020-01-02\"", "$.datetime().type()"));
        assertEquals("[\"time without time zone\"]", query("\"12:00:00\"", "$.time().type()"));
        assertEquals("[\"time with time zone\"]", query("\"12:00:00+05\"", "$.time_tz().type()"));
        assertEquals("[\"timestamp without time zone\"]",
                query("\"2020-01-02 03:04:05\"", "$.timestamp().type()"));
        assertEquals("[\"timestamp with time zone\"]",
                query("\"2020-01-02 03:04:05+05\"", "$.timestamp_tz().type()"));
    }

    /** A datetime is not the string it prints as, so the methods that read a string refuse it. */
    @Test
    void aDatetimeIsNotAString() {
        assertEquals("ERROR: jsonpath item method .date() can only be applied to a string",
                messageOf("SELECT jsonb_path_query('\"2020-01-02\"', '$.date().date()')"));
        assertEquals("ERROR: jsonpath item method .double() can only be applied to a string or"
                        + " numeric value",
                messageOf("SELECT jsonb_path_query('\"2020-01-02\"', '$.datetime().double()')"));
    }

    /** A date read as a date has no time of day to give, and midnight is not one it may invent. */
    @Test
    void theCastsBetweenTheShapes() throws Exception {
        assertEquals("[\"2020-01-02T00:00:00\"]", query("\"2020-01-02\"", "$.timestamp()"));
        assertEquals("ERROR: time format is not recognized: \"2020-01-02\"",
                messageOf("SELECT jsonb_path_query('\"2020-01-02\"', '$.time()')"));
    }

    /** Crossing the offset boundary needs a zone, which only the _tz functions may consult. */
    @Test
    void aZoneIsOnlyConsultedWhereItWasAskedFor() throws Exception {
        assertEquals("0A000",
                stateOf("SELECT jsonb_path_query('\"2020-01-02\"', '$.timestamp_tz()')"));
        assertEquals("ERROR: cannot convert value from date to timestamptz without time zone"
                        + " usage",
                messageOf("SELECT jsonb_path_query('\"2020-01-02\"', '$.timestamp_tz()')"));
        assertEquals("\"2020-01-02T00:00:00+00:00\"",
                one("SELECT jsonb_path_query_tz('\"2020-01-02\"', '$.timestamp_tz()')"));
    }

    /** A template says how to read the string, and one naming no time field describes a day. */
    @Test
    void aTemplateSaysHowToReadIt() throws Exception {
        assertEquals("[\"2020-12-01\"]", query("\"12/2020\"", "$.datetime(\"MM/YYYY\")"));
        assertEquals("[\"date\"]", query("\"12/2020\"", "$.datetime(\"MM/YYYY\").type()"));
        assertEquals("22031", stateOf("SELECT jsonb_path_query('\"nope\"', '$.datetime()')"));
    }

    /** Fractional seconds are rounded to the digits asked for. */
    @Test
    void aPrecisionRoundsTheSeconds() throws Exception {
        assertEquals("[\"03:04:05.68\"]", query("\"03:04:05.6789\"", "$.time(2)"));
    }

    /** A day and a moment in a day compare; a day and a time of day are different questions. */
    @Test
    void whatADatetimeComparesWith() throws Exception {
        assertEquals("[1]",
                query("[1]", "$[*] ? (\"2020-01-01\".date() == \"2020-01-01T00:00:00\""
                        + ".timestamp())"));
        assertEquals("[]",
                query("[1]", "$[*] ? (\"2020-01-01\".date() == \"12:00:00\".time())"));
        assertEquals("[1]",
                query("[1]", "$[*] ? ((\"2020-01-01\".date() == \"12:00:00\".time())"
                        + " is unknown)"));
        assertEquals("[]", query("[1]", "$[*] ? (\"2020-01-01\".date() == 1)"));
    }

    /**
     * A time carrying a zone remembers where it was written, so two naming the same moment in
     * different zones are two values and the one further west is the greater.
     */
    @Test
    void aZonedTimeIsItsMomentAndItsZone() throws Exception {
        assertEquals("[]",
                query("[1]", "$[*] ? (\"12:00:00+00\".time_tz() == \"13:00:00+01\".time_tz())"));
        assertEquals("[1]",
                query("[1]", "$[*] ? (\"12:00:00+00\".time_tz() > \"13:00:00+01\".time_tz())"));
        // A timestamp carries a day as well, so it is only the moment that is compared.
        assertEquals("[1]", query("[1]", "$[*] ? (\"2020-01-01T12:00:00+00\".timestamp_tz() =="
                + " \"2020-01-01T13:00:00+01\".timestamp_tz())"));
    }

    // ------------------------------------------------------------- over a column

    /** The path is read once and applied to every row. */
    @Test
    void thePathRunsOverEveryRow() throws Exception {
        assertEquals(List.of("1|[3, 4]", "2|[]"),
                rows("SELECT id, jsonb_path_query_array(j, '$.a[*].b[*] ? (@ > 2)')"
                        + " FROM jpe_doc ORDER BY id"));
        assertEquals(List.of("1|1", "2|NULL"),
                rows("SELECT id, jsonb_path_query_first(j, '$.a[*].b[*]') FROM jpe_doc"
                        + " ORDER BY id"));
    }

    /** A set-returning call gives an item to a row, and a document with none gives no rows. */
    @Test
    void aSetReturningCallGivesAnItemToARow() throws Exception {
        assertEquals(List.of("1|1", "1|2", "1|3", "1|4"),
                rows("SELECT d.id, q.v FROM jpe_doc d CROSS JOIN LATERAL"
                        + " jsonb_path_query(d.j, '$.a[*].b[*]') AS q(v) ORDER BY d.id, q.v::text"));
    }

    /** The items of a set-returning call are jsonb, so a string among them keeps its quotes. */
    @Test
    void theItemsOfASetReturningCallAreDocuments() throws Exception {
        assertEquals(List.of("\"a\"", "\"b\""),
                rows("SELECT * FROM jsonb_path_query('[\"a\",\"b\"]', '$[*]') t"));
    }
}
