package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a set-returning call may stand, and what it produces when it does.
 *
 * <p>A set-returning function is one that answers with rows rather than a value, so it belongs
 * wherever rows are still being produced and nowhere that reads rows already produced. Every case
 * below was measured against PostgreSQL 18 before and after the change.
 *
 * <p><b>Two errors carried a position PostgreSQL does not send, and two did not carry one it
 * does.</b> The protocol layer guesses a Position by finding the message's quoted name in the
 * statement text, which is right often enough to be worth keeping and wrong for the errors raised
 * while a query's range table is built — a FROM name given twice, a USING column named twice or
 * missing — where PostgreSQL sends no Position at all. Meanwhile the placement refusals, which
 * PostgreSQL points at the call, quote no name at all and so got nothing. Both are now decided at
 * the throw site: an error either names the word its position points at or says it has none.
 *
 * <p><b>NULLIF, GREATEST and LEAST are not conditionals.</b> PostgreSQL refuses a set inside CASE
 * and COALESCE because those may skip evaluating an argument, so which rows the query answers with
 * would depend on a value the planner does not have. The other three evaluate every argument;
 * {@code SELECT nullif(generate_series(1,2), 0)} answers two rows. Listing them refused valid SQL.
 *
 * <p><b>A set expands after the window and before the sort.</b> A select-list call beside a window
 * function used to stop expanding and come back as an array literal — a wrong answer rather than
 * an error. PostgreSQL numbers the window over the input rows and then expands, so each output row
 * of the window becomes one row per element carrying the same window value. A call in the window's
 * own PARTITION BY or ORDER BY is a sort key computed below the window, so it expands the input
 * instead; so does one written only in the query's ORDER BY.
 *
 * <p><b>An UPDATE assignment and a RETURNING item may not hold one</b> — each answers one value per
 * row, with nowhere to put a second — while a one-row VALUES list may, and writes one row per
 * element. Two or more VALUES rows are a scan of a constant table with nowhere to expand into, and
 * PostgreSQL refuses that; both halves are measured, not reasoned about.
 *
 * <p><b>A function FROM item is a relation, not always a lateral one.</b> Running one row by row
 * on the nullable side of a RIGHT or FULL join dropped every unmatched row on both sides, because
 * the per-row loop has no way to answer with the rows nothing matched. A lateral reference is
 * illegal there anyway, so the item is an ordinary relation and the ordinary join answers it.
 *
 * <p>The last nested class is the reason to prefer narrow rules to broad ones: every shape in it is
 * SQL PostgreSQL runs, and each new refusal is one more way to refuse it.
 */
class SrfCorrectionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE src_dpt (id int PRIMARY KEY, name text, budget int)");
        exec("INSERT INTO src_dpt VALUES (1,'eng',100),(2,'ops',200),(3,'hr',300)");
        exec("CREATE TABLE src_emp (id int PRIMARY KEY, name text, dept_id int)");
        exec("INSERT INTO src_emp VALUES (1,'amy',1),(2,'bob',2),(3,'cal',3),(4,'dan',null)");
        exec("CREATE TABLE src_r1 (a int PRIMARY KEY)");
        exec("INSERT INTO src_r1 VALUES (1),(2)");
        exec("CREATE TABLE src_r3 (a int PRIMARY KEY)");
        exec("CREATE TABLE src_t2 (j int, k text)");
        exec("INSERT INTO src_t2 VALUES (1,'a'),(2,'b')");
        exec("CREATE FUNCTION src_fint() RETURNS SETOF int"
                + " AS $$ SELECT 1 UNION ALL SELECT 2 $$ LANGUAGE sql");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void emptyScratchTable() throws SQLException {
        exec("DELETE FROM src_r3");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    /** One row per entry, columns joined by '|', in the order the query answered. */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append('|');
                        sb.append(rs.getString(i));
                    }
                    out.add(sb.toString());
                }
                return out;
            }
        }
    }

    /** The same, sorted, for a query with no ORDER BY of its own. */
    private static List<String> sortedRows(String sql) throws SQLException {
        List<String> out = rows(sql);
        Collections.sort(out);
        return out;
    }

    /** The column labels and type names the wire reports, as "label:type". */
    private static List<String> columns(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                List<String> out = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    out.add(md.getColumnLabel(i) + ":" + md.getColumnTypeName(i));
                }
                return out;
            }
        }
    }

    private static SQLException rejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
        return e;
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    /** The server's Position field, or 0 when it sent none. */
    private static int positionOf(SQLException e) {
        String message = e.getMessage();
        int at = message.indexOf("Position: ");
        if (at < 0) return 0;
        int end = at + "Position: ".length();
        int stop = end;
        while (stop < message.length() && Character.isDigit(message.charAt(stop))) stop++;
        return Integer.parseInt(message.substring(end, stop));
    }

    private static void assertNoPosition(SQLException e) {
        assertEquals(0, positionOf(e), "expected no Position: " + e.getMessage());
    }

    // ---- 1: errors PostgreSQL raises with no parse location of their own ----

    @Nested
    class ErrorsWithoutAPosition {

        @Test
        void aFromNameGivenTwiceCarriesNoPosition() {
            assertNoPosition(rejected("SELECT count(*) FROM src_dpt, src_dpt",
                    "42712", "table name \"src_dpt\" specified more than once"));
        }

        @Test
        void anAliasGivenTwiceCarriesNoPosition() {
            assertNoPosition(rejected("SELECT count(*) FROM src_dpt a, src_dpt a",
                    "42712", "table name \"a\" specified more than once"));
        }

        @Test
        void twoFunctionItemsOfOneNameCarryNoPosition() {
            assertNoPosition(rejected("SELECT count(*) FROM generate_series(1,2), generate_series(1,3)",
                    "42712", "table name \"generate_series\" specified more than once"));
        }

        @Test
        void aUsingColumnNamedTwiceCarriesNoPosition() {
            assertNoPosition(rejected("SELECT * FROM src_dpt a JOIN src_dpt b USING (id, id)",
                    "42701", "column name \"id\" appears more than once in USING clause"));
        }

        @Test
        void aMissingUsingColumnCarriesNoPosition() {
            assertNoPosition(rejected("SELECT * FROM src_dpt a JOIN src_emp b USING (nosuch)",
                    "42703", "does not exist in left table"));
        }

        @Test
        void anAliasListTooLongForAFunctionCarriesNoPosition() {
            assertNoPosition(rejected("SELECT * FROM generate_series(1,2) AS t(v, w)",
                    "42P10", "table \"t\" has 1 columns available but 2 columns specified"));
        }

        @Test
        void anAliasListTooLongForASubqueryCarriesNoPosition() {
            assertNoPosition(rejected("SELECT * FROM (SELECT 1, 2) AS t(a, b, c)",
                    "42P10", "table \"t\" has 2 columns available but 3 columns specified"));
        }

        @Test
        void aWithQueryIsNamedAsOneAndDoesCarryAPosition() {
            SQLException e = rejected("WITH c(a, b) AS (SELECT 1) SELECT * FROM c",
                    "42P10", "WITH query \"c\" has 1 columns available but 2 columns specified");
            assertEquals(6, positionOf(e));
        }
    }

    // ---- 2: the placement refusals point at the call ----

    @Nested
    class PlacementMessages {

        @Test
        void limitPointsAtTheCall() {
            SQLException e = rejected("SELECT * FROM src_dpt LIMIT generate_series(1,1)",
                    "0A000", "set-returning functions are not allowed in LIMIT");
            assertEquals("SELECT * FROM src_dpt LIMIT ".length() + 1, positionOf(e));
        }

        @Test
        void offsetPointsAtTheCall() {
            SQLException e = rejected("SELECT * FROM src_dpt OFFSET generate_series(1,1)",
                    "0A000", "set-returning functions are not allowed in OFFSET");
            assertEquals("SELECT * FROM src_dpt OFFSET ".length() + 1, positionOf(e));
        }

        @Test
        void whereHavingAndJoinConditionsAreNamedAndPositioned() {
            assertTrue(positionOf(rejected("SELECT * FROM src_dpt WHERE id = generate_series(1,1)",
                    "0A000", "not allowed in WHERE")) > 0);
            assertTrue(positionOf(rejected(
                    "SELECT count(*) FROM src_dpt HAVING count(*) = generate_series(1,1)",
                    "0A000", "not allowed in HAVING")) > 0);
            assertTrue(positionOf(rejected(
                    "SELECT * FROM src_dpt a JOIN src_emp b ON a.id = generate_series(1,1)",
                    "0A000", "not allowed in JOIN conditions")) > 0);
        }

        @Test
        void aConditionalAlsoOffersTheLateralHint() {
            assertTrue(rejected("SELECT CASE WHEN true THEN generate_series(1,2) ELSE 0 END",
                    "0A000", "not allowed in CASE").getMessage().contains("LATERAL FROM item"));
            assertTrue(rejected("SELECT coalesce(generate_series(1,2), 0)",
                    "0A000", "not allowed in COALESCE").getMessage().contains("LATERAL FROM item"));
        }

        @Test
        void limitDoesNotOfferTheLateralHint() {
            assertFalse(rejected("SELECT * FROM src_dpt LIMIT generate_series(1,1)",
                    "0A000", "not allowed in LIMIT").getMessage().contains("LATERAL FROM item"));
        }

        @Test
        void anAggregateArgumentIsRefusedAndPositioned() {
            SQLException e = rejected("SELECT count(generate_series(1,2))", "0A000",
                    "aggregate function calls cannot contain set-returning function calls");
            assertEquals("SELECT count(".length() + 1, positionOf(e));
        }

        @Test
        void filterBelongsToAnAggregateAndNamesTheFunction() {
            rejected("SELECT generate_series(1,2) FILTER (WHERE true)", "42809",
                    "FILTER specified, but generate_series is not an aggregate function");
        }
    }

    // ---- 3: NULLIF, GREATEST and LEAST evaluate every argument ----

    @Nested
    class NotConditionals {

        @Test
        void nullifExpandsItsSet() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "2"),
                    sortedRows("SELECT nullif(generate_series(1,2), 0)"));
        }

        @Test
        void nullifStillNullsTheMatchingElement() throws SQLException {
            assertEquals(java.util.Arrays.asList("2", "null"),
                    sortedRows("SELECT nullif(generate_series(1,2), 1)"));
        }

        @Test
        void greatestAndLeastExpandTheirSets() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "2"),
                    sortedRows("SELECT greatest(generate_series(1,2), 0)"));
            assertEquals(java.util.Arrays.asList("1", "2"),
                    sortedRows("SELECT least(generate_series(1,2), 5)"));
        }

        @Test
        void theArgumentOrderDoesNotMatter() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "2"),
                    sortedRows("SELECT greatest(0, generate_series(1,2))"));
        }

        @Test
        void caseAndCoalesceAreStillRefused() {
            rejected("SELECT coalesce(generate_series(1,2), 0)", "0A000", "not allowed in COALESCE");
            rejected("SELECT CASE WHEN true THEN generate_series(1,2) ELSE 0 END",
                    "0A000", "not allowed in CASE");
        }
    }

    // ---- 4: where the expansion happens relative to windows and sorting ----

    @Nested
    class ExpansionOrder {

        @Test
        void aSetBesideAWindowCallExpandsIntoRows() throws SQLException {
            assertEquals(java.util.Arrays.asList("1|1", "2|1", "3|1"),
                    rows("SELECT generate_series(1,3) g, count(*) OVER () ORDER BY 1"));
        }

        @Test
        void everyExpandedRowCarriesTheWindowValueOfItsInputRow() throws SQLException {
            assertEquals(java.util.Arrays.asList("1|3", "1|3", "2|3", "2|3", "3|3", "3|3"),
                    rows("SELECT generate_series(1,3) g, sum(a) OVER () FROM src_r1 ORDER BY 1, 2"));
        }

        @Test
        void aSetInAWindowsOrderByExpandsTheInput() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT row_number() OVER (ORDER BY generate_series(1,2))"));
            assertEquals(java.util.Arrays.asList("3", "3", "6", "6"),
                    sortedRows("SELECT sum(a) OVER (ORDER BY generate_series(1,2)) FROM src_r1"));
        }

        @Test
        void aSetInAWindowsPartitionByExpandsTheInput() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "1"),
                    sortedRows("SELECT count(*) OVER (PARTITION BY generate_series(1,2))"));
        }

        @Test
        void aSetWrittenOnlyInOrderByExpandsTheRows() throws SQLException {
            // Four rows, two of each: the sort key is the series, so which of a's two values
            // comes first within a key is not decided by the query. The order below is the one
            // PostgreSQL 18 answers with.
            assertEquals(java.util.Arrays.asList("1", "2", "1", "2"),
                    rows("SELECT a FROM src_r1 ORDER BY generate_series(1,2)"));
        }

        @Test
        void aSortKeyThatIsAlsoATargetIsExpandedOnlyOnce() throws SQLException {
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT generate_series(1,2) g FROM src_r1 WHERE a = 1 ORDER BY g"));
        }
    }

    // ---- 5: the statements that write rows ----

    @Nested
    class WritingStatements {

        @Test
        void anUpdateAssignmentMayNotHoldASet() {
            rejected("UPDATE src_r3 SET a = generate_series(1,2) WHERE a = 5",
                    "0A000", "set-returning functions are not allowed in UPDATE");
        }

        @Test
        void itIsRefusedEvenWhenNoRowWouldMatch() {
            rejected("UPDATE src_dpt SET budget = generate_series(1,1) WHERE id = -1",
                    "0A000", "set-returning functions are not allowed in UPDATE");
        }

        @Test
        void onConflictDoUpdateIsAnUpdateToo() {
            rejected("INSERT INTO src_r1 VALUES (1)"
                            + " ON CONFLICT (a) DO UPDATE SET a = generate_series(1,1)",
                    "0A000", "set-returning functions are not allowed in UPDATE");
        }

        @Test
        void returningMayNotHoldASet() {
            rejected("INSERT INTO src_r3 VALUES (1) RETURNING generate_series(1,2)",
                    "0A000", "set-returning functions are not allowed in RETURNING");
            rejected("UPDATE src_r1 SET a = a RETURNING generate_series(1,2)",
                    "0A000", "set-returning functions are not allowed in RETURNING");
            rejected("DELETE FROM src_r3 RETURNING generate_series(1,2)",
                    "0A000", "set-returning functions are not allowed in RETURNING");
        }

        @Test
        void aOneRowValuesListWritesOneRowPerElement() throws SQLException {
            exec("INSERT INTO src_r3 VALUES (generate_series(5,6))");
            assertEquals(java.util.Arrays.asList("5", "6"), sortedRows("SELECT a FROM src_r3"));
        }

        @Test
        void aValuesListOfSeveralRowsMayNotHoldASet() {
            rejected("INSERT INTO src_r3 VALUES (generate_series(1,2)), (5)",
                    "0A000", "set-returning functions are not allowed in VALUES");
        }
    }

    // ---- 6: a function FROM item is a relation ----

    @Nested
    class FunctionItemsInJoins {

        @Test
        void aFullJoinOfTwoFunctionsKeepsBothUnmatchedSides() throws SQLException {
            assertEquals(java.util.Arrays.asList("1|null", "2|2", "3|3", "null|4"),
                    rows("SELECT a.n, b.n FROM generate_series(1,3) a(n)"
                            + " FULL JOIN generate_series(2,4) b(n) ON a.n = b.n ORDER BY 1, 2"));
        }

        @Test
        void aFullJoinOfATableAndAFunctionKeepsTheUnmatchedRow() throws SQLException {
            assertEquals(java.util.Arrays.asList("amy|1", "bob|2", "cal|3", "dan|null"),
                    rows("SELECT e.name, x FROM src_emp e"
                            + " FULL JOIN generate_series(1,3) x ON e.dept_id = x ORDER BY 1, 2"));
        }

        @Test
        void aRightJoinKeepsTheUnmatchedRightRow() throws SQLException {
            assertEquals(java.util.Arrays.asList("2|2", "3|3", "null|4"),
                    rows("SELECT a.n, b.n FROM generate_series(1,3) a(n)"
                            + " RIGHT JOIN generate_series(2,4) b(n) ON a.n = b.n ORDER BY 1, 2"));
        }

        @Test
        void aFullJoinStillHasToBeOneThePlannerCouldAnswer() {
            rejected("SELECT count(*) FROM generate_series(1,3) a(x)"
                            + " FULL JOIN generate_series(1,3) b(y) ON a.x < b.y",
                    "0A000", "FULL JOIN is only supported with merge-joinable");
        }

        @Test
        void aNullPaddedFunctionItemAnswersNullAndNotAnEmptyRecord() throws SQLException {
            assertEquals("dan|null", rows("SELECT e.name, g FROM src_emp e"
                    + " LEFT JOIN LATERAL generate_series(1, e.dept_id) g ON true"
                    + " ORDER BY 1, 2").get(6));
        }

        @Test
        void aCommaJoinDropsTheRowTheFunctionProducedNothingFor() throws SQLException {
            assertEquals(6, rows("SELECT e.name, g FROM src_emp e,"
                    + " LATERAL generate_series(1, e.dept_id) g ORDER BY 1, 2").size());
        }
    }

    // ---- 7: what a function FROM item and a set-returning call are called and typed ----

    @Nested
    class NamesAndTypes {

        @Test
        void aUserSetofFunctionAnswersInItsDeclaredType() throws SQLException {
            assertEquals(java.util.Arrays.asList("src_fint:int4"),
                    columns("SELECT * FROM src_fint()"));
        }

        @Test
        void withOrdinalityAddsTheColumnToAUserFunctionToo() throws SQLException {
            assertEquals(java.util.Arrays.asList("src_fint:int4", "ordinality:int8"),
                    columns("SELECT * FROM src_fint() WITH ORDINALITY"));
            assertEquals(java.util.Arrays.asList("1|1", "2|2"),
                    rows("SELECT * FROM src_fint() WITH ORDINALITY"));
        }

        @Test
        void anAliasListRenamesBothColumns() throws SQLException {
            assertEquals(java.util.Arrays.asList("v:int4", "n:int8"),
                    columns("SELECT * FROM src_fint() WITH ORDINALITY t(v, n)"));
        }

        @Test
        void everyColumnOfAManyArgumentUnnestIsNamedForTheFunction() throws SQLException {
            assertEquals(java.util.Arrays.asList("unnest:int4", "unnest:text"),
                    columns("SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b'])"));
        }

        @Test
        void theManyArgumentFormExistsOnlyInFrom() {
            rejected("SELECT unnest(ARRAY[1,2], ARRAY['a','b'])", "42883",
                    "function unnest(integer[], text[]) does not exist");
        }

        @Test
        void aPaddedFunctionColumnKeepsTheTypeTheCallWouldHaveAnswered() throws SQLException {
            assertEquals(java.util.Arrays.asList("g:int4", "j:int4"),
                    columns("SELECT a.g, b.j FROM src_t2 b"
                            + " LEFT JOIN generate_series(1,0) AS a(g) ON a.g = b.j ORDER BY b.j"));
        }

        @Test
        void aSetReturningCallInTheSelectListReportsItsOwnType() throws SQLException {
            assertEquals(java.util.Arrays.asList("regexp_matches:_text"),
                    columns("SELECT regexp_matches('abc', 'b')"));
            assertEquals(java.util.Arrays.asList("json_array_elements:json"),
                    columns("SELECT json_array_elements('[1,2]'::json)"));
            assertEquals(java.util.Arrays.asList("json_each:record"),
                    columns("SELECT json_each('{\"a\":1}'::json)"));
        }

        @Test
        void aSubqueryUsedAsAValueReportsItsColumnsType() throws SQLException {
            assertEquals(java.util.Arrays.asList("max:int4"),
                    columns("SELECT (SELECT max(x) FROM (SELECT 1 AS x, 2 AS y) t)"));
            assertEquals(java.util.Arrays.asList("array:_int4"), columns("SELECT ARRAY(SELECT 1)"));
        }

        @Test
        void aScalarSubqueryWrittenWithAStarKeepsTheColumnsName() throws SQLException {
            assertEquals(java.util.Arrays.asList("id:int4"),
                    columns("WITH c AS (SELECT id FROM src_dpt) SELECT (SELECT * FROM c LIMIT 1)"));
            assertEquals(java.util.Arrays.asList("generate_series:int4"),
                    columns("SELECT (SELECT * FROM generate_series(1,1))"));
        }

        @Test
        void aRowsFromItemNamesItselfAfterItsFirstFunction() {
            rejected("SELECT count(*) FROM ROWS FROM (generate_series(1,2)), generate_series(1,2)",
                    "42712", "table name \"generate_series\" specified more than once");
        }
    }

    // ---- 8: the calls that only worked in FROM ----

    @Nested
    class SelectListForms {

        @Test
        void stringToTableProducesRowsInTheSelectList() throws SQLException {
            assertEquals(java.util.Arrays.asList("a", "b", "c"),
                    rows("SELECT string_to_table('a,b,c', ',')"));
        }

        @Test
        void itsThirdArgumentStillNamesTheNullString() throws SQLException {
            assertEquals(java.util.Arrays.asList("a", "null", "c"),
                    rows("SELECT string_to_table('a,b,c', ',', 'b')"));
        }

        @Test
        void regexpSplitToTableProducesRowsInTheSelectList() throws SQLException {
            assertEquals(java.util.Arrays.asList("a", "b", "c"),
                    rows("SELECT regexp_split_to_table('a1b2c', '[0-9]')"));
        }

        @Test
        void bothAnswerTheSameRowsFromWhereTheyAlreadyWorked() throws SQLException {
            assertEquals(rows("SELECT * FROM string_to_table('a,b,c', ',')"),
                    rows("SELECT string_to_table('a,b,c', ',')"));
            assertEquals(rows("SELECT * FROM regexp_split_to_table('a1b2c', '[0-9]')"),
                    rows("SELECT regexp_split_to_table('a1b2c', '[0-9]')"));
        }

        @Test
        void aSetIsNotAValueIn() {
            rejected("SELECT generate_series(1,2) IN (1)", "42804",
                    "argument of IN must not return a set");
            rejected("SELECT 1 IN (generate_series(1,2))", "42804",
                    "argument of IN must not return a set");
        }
    }

    // ---- 9: ordinary SQL, which has to keep working ----

    @Nested
    class OrdinarySql {

        @Test
        void plainSetReturningCalls() {
            assertAccepted("SELECT generate_series(1,3)");
            assertAccepted("SELECT generate_series(1,3) + 1");
            assertAccepted("SELECT * FROM generate_series(1,3)");
            assertAccepted("SELECT unnest(ARRAY[1,2,3])");
            assertAccepted("SELECT a, generate_series(1,2) FROM src_r1");
            assertAccepted("SELECT generate_series(1,2), generate_series(1,3)");
            assertAccepted("SELECT array_agg(x) FROM generate_series(1,3) x");
        }

        @Test
        void conditionalsOverOrdinaryValues() {
            assertAccepted("SELECT coalesce(a, 0) FROM src_r1");
            assertAccepted("SELECT nullif(a, 1) FROM src_r1");
            assertAccepted("SELECT greatest(a, 1), least(a, 1) FROM src_r1");
            assertAccepted("SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END FROM src_r1");
        }

        @Test
        void inOverOrdinaryValues() throws SQLException {
            assertAccepted("SELECT 1 IN (1, 2)");
            assertAccepted("SELECT a IN (1, 2) FROM src_r1");
            assertAccepted("SELECT 1 NOT IN (2, 3)");
            assertAccepted("SELECT 1 IN (NULL)");
            assertAccepted("SELECT 2 BETWEEN 1 AND 3");
            // A set inside a sub-query belongs to that query, not to the IN
            assertEquals(java.util.Arrays.asList("t"),
                    rows("SELECT 1 IN (SELECT generate_series(1,2))"));
        }

        @Test
        void joinsOfEveryKind() {
            assertAccepted("SELECT * FROM src_dpt d JOIN src_emp e ON d.id = e.dept_id");
            assertAccepted("SELECT * FROM src_dpt d, src_emp e WHERE d.id = e.dept_id");
            assertAccepted("SELECT * FROM src_dpt a JOIN src_dpt b USING (id)");
            assertAccepted("SELECT count(*) FROM src_dpt d FULL JOIN src_emp e ON d.id = e.dept_id");
            assertAccepted("SELECT count(*) FROM src_r1 a FULL JOIN src_r1 b ON a.a = b.a");
            assertAccepted("SELECT e.name, g FROM src_emp e,"
                    + " LATERAL generate_series(1, e.dept_id) g");
            assertAccepted("SELECT count(*) FROM src_r1 a LEFT JOIN generate_series(1,0) b(y)"
                    + " ON a.a = b.y");
        }

        @Test
        void aliasListsThatFit() {
            assertAccepted("SELECT * FROM generate_series(1,2) AS t(v)");
            assertAccepted("SELECT * FROM generate_series(1,2) WITH ORDINALITY AS t(v, w)");
            assertAccepted("SELECT * FROM (SELECT 1, 2) AS t(a, b)");
            assertAccepted("SELECT * FROM (SELECT 1, 2) AS t(a)");
            assertAccepted("WITH c(x) AS (SELECT 1) SELECT * FROM c");
        }

        @Test
        void windowsAndSortingWithoutAnySet() {
            assertAccepted("SELECT row_number() OVER () FROM src_r1");
            assertAccepted("SELECT sum(a) OVER (ORDER BY a) FROM src_r1");
            assertAccepted("SELECT count(*) OVER (PARTITION BY a) FROM src_r1");
            assertAccepted("SELECT DISTINCT ON (a) a, row_number() OVER (ORDER BY a)"
                    + " FROM src_r1 ORDER BY a");
            assertAccepted("SELECT count(*) FILTER (WHERE true) FROM src_r1");
        }

        @Test
        void limitOffsetAndOrdinaryWrites() throws SQLException {
            assertAccepted("SELECT * FROM src_dpt LIMIT 1");
            assertAccepted("SELECT * FROM src_dpt OFFSET 1");
            assertAccepted("UPDATE src_dpt SET budget = 1 WHERE id = -1");
            exec("INSERT INTO src_r3 VALUES (1), (2)");
            assertEquals(java.util.Arrays.asList("1", "2"), sortedRows("SELECT a FROM src_r3"));
            exec("INSERT INTO src_r3 SELECT generate_series(3,4)");
            assertEquals(java.util.Arrays.asList("1", "2", "3", "4"),
                    sortedRows("SELECT a FROM src_r3"));
        }

        @Test
        void unnestKeepsItsElementTypeThroughALateral() throws SQLException {
            assertEquals(java.util.Arrays.asList("u:int4"),
                    columns("SELECT u FROM src_r1, LATERAL unnest(ARRAY[a]) u"));
        }
    }
}
