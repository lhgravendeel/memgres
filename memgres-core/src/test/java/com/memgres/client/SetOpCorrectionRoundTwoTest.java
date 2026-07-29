package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a set operation, a row comparison and a multi-column assignment mean, measured against
 * PostgreSQL 18.
 *
 * <p><b>The parentheses of a set operation come off one at a time.</b> What stands inside them is
 * a whole query expression whose own arms may be parenthesised in turn, so each closing
 * parenthesis belongs to whatever its own opening one began. Counting the run of opening
 * parentheses and then expecting that many closing ones made
 * {@code (SELECT 1) UNION ((SELECT 2) UNION (SELECT 3))} a syntax error at the inner UNION --
 * ordinary SQL, refused.
 *
 * <p><b>An INSERT is not an arm of a set operation.</b> {@code INSERT INTO t(id) (SELECT 1) UNION
 * (SELECT 2)} inserts both rows; reading the parentheses off the source and stopping there left
 * the UNION to the statement level, which made the INSERT itself the left arm. The INSERT ran, the
 * arms were then found to be of different widths, and the row it had already written stayed --
 * a statement that reported an error and changed the table anyway.
 *
 * <p><b>Two rows are unequal when some pair of members is non-null and unequal</b>, whatever else
 * is null, and unknown only when no pair settles it. A ROW value is not a java list, so a row
 * compared against a subquery missed the row path entirely and was compared as its printed text:
 * {@code (1, NULL) = (SELECT 1, 2)} came back false because "(1,)" and "(1,2)" read differently,
 * and {@code (1, NULL) = (SELECT 1, NULL)} came back true because they read the same.
 *
 * <p><b>One assignment may name several columns.</b> {@code SET (a, b) = (SELECT x, y ...)} and
 * {@code SET (a, b) = (1, 'z')} were a syntax error at the parenthesis. The columns of one such
 * assignment share the source node, so a sub-SELECT is read once per updated row however many
 * columns read from it.
 *
 * <p><b>A name two output columns answer to names neither.</b> Written on a set operation any
 * repeat is ambiguous; written on a plain SELECT it is ambiguous unless the two are the same
 * expression, which is one thing under two names. And a quoted alias keeps its case, so
 * {@code ORDER BY foo} does not reach a column called {@code "Foo"}.
 *
 * <p><b>A VALUES list is a query</b>, so it takes ORDER BY, LIMIT and OFFSET. A list of two rows
 * or more is rewritten as a set operation, whose parsing read them; a list of one row was left
 * with nothing to read them at all.
 *
 * <p>The last nested class is the point of all of it: every shape there is SQL PostgreSQL runs.
 */
class SetOpCorrectionRoundTwoTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE stn_t (a int, b text)");
        exec("CREATE TABLE stn_u (c int, d text)");
        exec("CREATE TABLE stn_n (i int, j int)");
        exec("INSERT INTO stn_n VALUES (1, NULL), (1, 2), (2, 2)");
        exec("CREATE TABLE stn_tgt (id int PRIMARY KEY, dept_id int, name text)");
        exec("CREATE TABLE stn_dept (id int PRIMARY KEY, name text)");
        exec("INSERT INTO stn_dept VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");
        exec("CREATE TABLE stn_emp (id int PRIMARY KEY, dept_id int, name text)");
        exec("INSERT INTO stn_emp VALUES (1, 1, 'e'), (5, 2, 'f')");
        exec("CREATE TABLE stn_ob (n int, s text)");
        exec("INSERT INTO stn_ob VALUES (1, 'zz'), (2, 'aa')");
        exec("CREATE TABLE stn_p (k int, m int)");
        exec("INSERT INTO stn_p VALUES (1, 2)");
        exec("CREATE FUNCTION stn_arr() RETURNS int[] AS $$ SELECT ARRAY[1,2] $$ LANGUAGE sql");
        exec("CREATE FUNCTION stn_out(OUT x int, OUT y text) RETURNS SETOF record"
                + " AS $$ SELECT 1, 'a' $$ LANGUAGE sql");
        exec("CREATE FUNCTION stn_setofint() RETURNS SETOF int"
                + " AS $$ SELECT 1 UNION ALL SELECT 2 $$ LANGUAGE sql");
    }

    @BeforeEach
    void reload() throws Exception {
        exec("DELETE FROM stn_t");
        exec("INSERT INTO stn_t VALUES (1, 'a'), (2, 'b')");
        exec("DELETE FROM stn_u");
        exec("INSERT INTO stn_u VALUES (3, 'c')");
        exec("DELETE FROM stn_tgt");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---------------------------------------------------------------- helpers

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
            return st.getUpdateCount();
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> r = rows(sql);
        return r.isEmpty() ? null : r.get(0);
    }

    /** Column labels of {@code sql}, joined with "|". */
    private static String labels(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                sb.append(md.getColumnLabel(i)).append(':').append(md.getColumnTypeName(i));
            }
            return sb.toString();
        }
    }

    /** Rows of {@code sql} in the order returned, each row's columns joined with "|". */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            ResultSet rs = st.executeQuery(sql);
            int n = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder r = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) r.append('|');
                    Object o = rs.getObject(i);
                    r.append(o == null ? "<null>" : String.valueOf(o));
                }
                out.add(r.toString());
            }
            return out;
        }
    }

    /** Rows of {@code sql} sorted, for queries whose own order is not fixed. */
    private static List<String> sortedRows(String sql) throws SQLException {
        List<String> out = new ArrayList<>(rows(sql));
        Collections.sort(out);
        return out;
    }

    private static SQLException fails(String sql, String sqlState) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        return e;
    }

    private static void failsWith(String sql, String sqlState, String message) {
        SQLException e = fails(sql, sqlState);
        assertTrue(e.getMessage().contains(message),
                sql + " -> expected \"" + message + "\" in: " + e.getMessage());
    }

    private static String detailOf(SQLException e) {
        org.postgresql.util.ServerErrorMessage m =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        return m == null ? null : m.getDetail();
    }

    private static String hintOf(SQLException e) {
        org.postgresql.util.ServerErrorMessage m =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        return m == null ? null : m.getHint();
    }

    // ================================================================

    @Nested
    class TheParenthesesOfASetOperationComeOffOneAtATime {

        @Test
        void aParenthesisedSetOperationOnTheRight() throws Exception {
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT 1 UNION ((SELECT 2) UNION (SELECT 3)) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2", "3", "4"),
                    rows("((SELECT 1) UNION (SELECT 2)) UNION ((SELECT 3) UNION (SELECT 4))"
                            + " ORDER BY 1"));
            assertEquals(Arrays.asList("2"),
                    rows("((SELECT 1) UNION (SELECT 2)) INTERSECT ((SELECT 2) UNION (SELECT 5))"
                            + " ORDER BY 1"));
            assertEquals(Arrays.asList("1"),
                    rows("SELECT 1 EXCEPT ((SELECT 2) UNION (SELECT 3)) ORDER BY 1"));
        }

        @Test
        void anyDepthOfParentheses() throws Exception {
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT 1 UNION (((SELECT 2) UNION (SELECT 3))) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("((((SELECT 1)) UNION ((SELECT 2)))) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("(SELECT 1) UNION ((SELECT 2) UNION ((SELECT 3))) ORDER BY 1"));
        }

        @Test
        void theSameShapeUnderINTERSECTAndEXCEPT() throws Exception {
            assertEquals(Arrays.asList("2"),
                    rows("(SELECT 1 UNION SELECT 2) INTERSECT ((SELECT 2) UNION (SELECT 9))"
                            + " ORDER BY 1"));
            assertEquals(Arrays.asList("1"),
                    rows("(SELECT 1 UNION SELECT 2) EXCEPT ((SELECT 2) UNION (SELECT 9))"
                            + " ORDER BY 1"));
        }

        @Test
        void aParenthesisedArmStillKeepsItsOwnLimit() throws Exception {
            assertEquals(Arrays.asList("1", "3"),
                    rows("(SELECT a FROM stn_t ORDER BY a LIMIT 1)"
                            + " UNION (SELECT c FROM stn_u) ORDER BY 1"));
        }
    }

    @Nested
    class AnInsertIsNotAnArmOfASetOperation {

        @Test
        void aSetOperationAsAnInsertSource() throws Exception {
            assertEquals(2, update("INSERT INTO stn_tgt(id) (SELECT 1) UNION (SELECT 2)"));
            assertEquals(Arrays.asList("1", "2"), rows("SELECT id FROM stn_tgt ORDER BY id"));
        }

        @Test
        void eachArmWritingEveryColumn() throws Exception {
            assertEquals(2,
                    update("INSERT INTO stn_tgt(id, name) (SELECT 1, 'x') UNION (SELECT 2, 'y')"));
            assertEquals(Arrays.asList("1|x", "2|y"),
                    rows("SELECT id, name FROM stn_tgt ORDER BY id"));
        }

        @Test
        void theWholeSetOperationParenthesised() throws Exception {
            assertEquals(2, update("INSERT INTO stn_tgt(id) ((SELECT 1) UNION (SELECT 2))"));
            assertEquals(Arrays.asList("1", "2"), rows("SELECT id FROM stn_tgt ORDER BY id"));
        }

        @Test
        void theSetOperationsOwnOrderByAndLimit() throws Exception {
            assertEquals(2,
                    update("INSERT INTO stn_tgt(id) (SELECT 1) UNION ALL (SELECT 2) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"), rows("SELECT id FROM stn_tgt ORDER BY id"));
        }

        @Test
        void aRefusedInsertLeavesNothingBehind() throws Exception {
            // The arms differ in width, so there is no statement to run -- and nothing written.
            fails("INSERT INTO stn_tgt(id) SELECT 1, 2 UNION SELECT 3", "42601");
            assertEquals(Collections.emptyList(), rows("SELECT id FROM stn_tgt"));
            fails("INSERT INTO stn_tgt(id) VALUES (1), (2), (1)", "23505");
            assertEquals(Collections.emptyList(), rows("SELECT id FROM stn_tgt"));
        }

        @Test
        void aSetOperatorAfterAWritingStatementIsASyntaxError() throws Exception {
            fails("UPDATE stn_t SET a = 9 UNION SELECT 1", "42601");
            fails("DELETE FROM stn_t UNION SELECT 1", "42601");
            assertEquals(Arrays.asList("1|a", "2|b"), rows("SELECT a, b FROM stn_t ORDER BY a"));
        }
    }

    @Nested
    class TwoRowsAreUnequalWhenSomePairSettlesIt {

        @Test
        void aNullMemberDoesNotSettleTheAnswer() throws Exception {
            assertEquals("<null>", one("SELECT ((1, NULL) = (SELECT 1, 2))::text"));
            assertEquals("<null>", one("SELECT ((1, NULL) = (SELECT 1, NULL))::text"));
            assertEquals("<null>", one("SELECT ((1, NULL) <> (SELECT 1, 2))::text"));
            assertEquals("<null>", one("SELECT ((1, NULL) = (1, 2))::text"));
        }

        @Test
        void aPairThatDiffersSettlesItAnyway() throws Exception {
            assertEquals("false", one("SELECT ((NULL, 1) = (2, 3))::text"));
            assertEquals("false", one("SELECT ((NULL, 1) = (NULL, 3))::text"));
            assertEquals("true", one("SELECT ((NULL, 1) <> (2, 3))::text"));
            assertEquals("false", one("SELECT ((1, NULL) = (SELECT 2, NULL))::text"));
        }

        @Test
        void theSameJudgementInAWhereClause() throws Exception {
            assertEquals(Arrays.asList("2|2"),
                    rows("SELECT i, j FROM stn_n WHERE (i, j) <> (SELECT 1, 2)"
                            + " ORDER BY i, j NULLS LAST"));
        }

        @Test
        void anArrayIsOneValueAndNullsInItAreAPartOfIt() throws Exception {
            assertEquals("true", one("SELECT (ARRAY[NULL, 1] = ARRAY[NULL, 1])::text"));
            assertEquals("false", one("SELECT (ARRAY[NULL, 1] <> ARRAY[NULL, 1])::text"));
            assertEquals("false", one("SELECT (ARRAY[NULL, 1] = ARRAY[2, 3])::text"));
        }

        @Test
        void anEntryWithoutATypeIsReadAsTheOneOpposite() throws Exception {
            failsWith("SELECT ((1, 'a') = (SELECT 1, 2))::text", "22P02",
                    "invalid input syntax for type integer: \"a\"");
            failsWith("SELECT ((1, 'a') = (1, 1))::text", "22P02",
                    "invalid input syntax for type integer: \"a\"");
            // ... and text that is a value of that type is simply read as one
            assertEquals("true", one("SELECT ((1, '1') = (1, 1))::text"));
            assertEquals("false", one("SELECT ((1, 'a') = (1, 'b'))::text"));
        }
    }

    @Nested
    class ARowAgainstSomethingThatIsNotOne {

        @Test
        void rowsOfDifferentWidthsHaveNoComparison() throws Exception {
            failsWith("SELECT (ROW(1,2) IN (ROW(1,2,3)))::text", "42601",
                    "unequal number of entries in row expressions");
            failsWith("SELECT (ROW(1,2) IN (ROW(1,2), ROW(1,2,3)))::text", "42601",
                    "unequal number of entries in row expressions");
        }

        @Test
        void aRowAgainstASingleValueHasNoOperator() throws Exception {
            SQLException e = fails("SELECT ((1,2) IS DISTINCT FROM (SELECT 1))::text", "42883");
            assertTrue(e.getMessage().contains("operator does not exist: record = integer"),
                    e.getMessage());
            assertNotNull(hintOf(e));
            fails("SELECT ((1,2) IS NOT DISTINCT FROM (SELECT 1))::text", "42883");
            fails("SELECT ((1,2) IS DISTINCT FROM 1)::text", "42883");
            fails("SELECT (1 IS DISTINCT FROM (1,2))::text", "42883");
            fails("SELECT count(*) FROM stn_dept d WHERE (d.id, d.name) IN ((1))", "42883");
        }
    }

    @Nested
    class ASubqueryWhereOneValueStands {

        @Test
        void anEntryOfAnArrayOrAnInList() throws Exception {
            failsWith("SELECT 1 WHERE 1 = ANY (ARRAY[(SELECT 1, 2)])", "42601",
                    "subquery must return only one column");
            failsWith("SELECT 1 WHERE 1 IN (1, (SELECT 1, 2))", "42601",
                    "subquery must return only one column");
            failsWith("SELECT 1 WHERE 1 IN (1, 2, (SELECT 3, 4))", "42601",
                    "subquery must return only one column");
            failsWith("SELECT ARRAY[1, (SELECT 1, 2)]", "42601",
                    "subquery must return only one column");
        }

        @Test
        void theSubqueryFormOfInStillReportsItsWidth() throws Exception {
            failsWith("SELECT 1 WHERE 1 IN (SELECT 1, 2)", "42601",
                    "subquery has too many columns");
            failsWith("SELECT 1 WHERE (1,2) IN (SELECT 1)", "42601",
                    "subquery has too few columns");
        }
    }

    @Nested
    class OneAssignmentMayNameSeveralColumns {

        @Test
        void aRowConstructorAsTheSource() throws Exception {
            exec("INSERT INTO stn_tgt(id, dept_id, name) VALUES (610, 1, 'a')");
            assertEquals(1, update("UPDATE stn_tgt SET (dept_id, name) = (2, 'b') WHERE id = 610"));
            assertEquals(Arrays.asList("610|2|b"),
                    rows("SELECT id, dept_id, name FROM stn_tgt ORDER BY id"));
        }

        @Test
        void aSubSelectAsTheSource() throws Exception {
            assertEquals(1, update("UPDATE stn_t SET (a, b) = (SELECT 9, 'z') WHERE a = 1"));
            assertEquals(Arrays.asList("2|b", "9|z"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void aSubSelectThatReadsAnotherTable() throws Exception {
            assertEquals(1, update("UPDATE stn_t SET (a, b) = (SELECT c, d FROM stn_u WHERE c = 3)"
                    + " WHERE a = 1"));
            assertEquals(Arrays.asList("2|b", "3|c"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void theWordRowWrittenOut() throws Exception {
            assertEquals(1, update("UPDATE stn_t SET (a, b) = ROW(7, 'q') WHERE a = 1"));
            assertEquals(Arrays.asList("2|b", "7|q"), sortedRows("SELECT a, b FROM stn_t"));
            // one column named, and ROW() is still a row
            assertEquals(1, update("UPDATE stn_t SET (a) = ROW(5) WHERE a = 7"));
            assertEquals(Arrays.asList("2|b", "5|q"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void aSourceThatIsNeitherASubSelectNorARow() throws Exception {
            failsWith("UPDATE stn_t SET (a) = (5) WHERE a = 1", "0A000",
                    "source for a multiple-column UPDATE item must be a sub-SELECT"
                            + " or ROW() expression");
            failsWith("UPDATE stn_t SET (a, b) = (9) WHERE a = 1", "0A000",
                    "source for a multiple-column UPDATE item must be a sub-SELECT"
                            + " or ROW() expression");
            assertEquals(Arrays.asList("1|a", "2|b"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void aSourceOfTheWrongWidth() throws Exception {
            failsWith("UPDATE stn_t SET (a, b) = (1, 'z', 3) WHERE a = 1", "42601",
                    "number of columns does not match number of values");
            failsWith("UPDATE stn_t SET (a, b) = (SELECT 1) WHERE a = 1", "42601",
                    "number of columns does not match number of values");
            failsWith("UPDATE stn_t SET (a, b) = (SELECT 1, 'z', 3) WHERE a = 1", "42601",
                    "number of columns does not match number of values");
            assertEquals(Arrays.asList("1|a", "2|b"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void aColumnNamedTwiceAmongTheAssignments() throws Exception {
            failsWith("UPDATE stn_t SET (a, b) = (9, 'z'), b = 'k' WHERE a = 1", "42601",
                    "multiple assignments to same column \"b\"");
            failsWith("UPDATE stn_t SET b = 'x', b = 'y' WHERE a = 1", "42601",
                    "multiple assignments to same column \"b\"");
            assertEquals(Arrays.asList("1|a", "2|b"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void aSourceThatFindsNoRowLeavesEveryColumnNull() throws Exception {
            assertEquals(1,
                    update("UPDATE stn_t SET (a, b) = (SELECT 9, 'z' WHERE false) WHERE a = 1"));
            assertEquals(Arrays.asList("2|b", "<null>|<null>"),
                    sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void noRowMatchedAndNoRowChanged() throws Exception {
            assertEquals(0, update("UPDATE stn_t SET (a, b) = (SELECT 9, 'z') WHERE a = 99"));
            assertEquals(Arrays.asList("1|a", "2|b"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void withAnAliasAndWithReturning() throws Exception {
            assertEquals(Arrays.asList("3|c"),
                    rows("UPDATE stn_t t SET (a, b) = (SELECT c, d FROM stn_u WHERE c = 3)"
                            + " WHERE t.a = 1 RETURNING a, b"));
        }
    }

    @Nested
    class ANameTwoOutputColumnsAnswerTo {

        @Test
        void twoColumnsOfOneNameOnASetOperation() throws Exception {
            failsWith("SELECT id, id AS id FROM stn_dept UNION SELECT id, id FROM stn_emp"
                    + " ORDER BY id", "42702", "ORDER BY \"id\" is ambiguous");
            failsWith("SELECT a, a FROM stn_t UNION SELECT c, c FROM stn_u ORDER BY a",
                    "42702", "ORDER BY \"a\" is ambiguous");
            failsWith("SELECT a AS k, b AS k FROM stn_t UNION SELECT c, d FROM stn_u ORDER BY k",
                    "42702", "ORDER BY \"k\" is ambiguous");
        }

        @Test
        void twoColumnsOfOneNameOnAPlainSelect() throws Exception {
            failsWith("SELECT n, s AS n FROM stn_ob ORDER BY n", "42702",
                    "ORDER BY \"n\" is ambiguous");
            failsWith("SELECT a AS k, b AS k FROM stn_t ORDER BY k", "42702",
                    "ORDER BY \"k\" is ambiguous");
            failsWith("SELECT a, a + 1 AS a FROM stn_t ORDER BY a", "42702",
                    "ORDER BY \"a\" is ambiguous");
            failsWith("SELECT a AS b, b FROM stn_t ORDER BY b", "42702",
                    "ORDER BY \"b\" is ambiguous");
        }

        @Test
        void oneExpressionUnderTwoNamesIsStillOneThing() throws Exception {
            assertEquals(Arrays.asList("1|1", "2|2"),
                    rows("SELECT a, a FROM stn_t ORDER BY a"));
            assertEquals(Arrays.asList("1|1", "2|2"),
                    rows("SELECT a AS k, a AS k FROM stn_t ORDER BY k"));
        }

        @Test
        void aQuotedAliasKeepsTheCaseItWasWrittenWith() throws Exception {
            SQLException e = fails("SELECT a AS \"Foo\" FROM stn_t UNION SELECT c FROM stn_u"
                    + " ORDER BY foo", "42703");
            assertTrue(e.getMessage().contains("column \"foo\" does not exist"), e.getMessage());
            assertEquals("Perhaps you meant to reference the column \"*SELECT* 1.Foo\".",
                    hintOf(e));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT a AS \"Foo\" FROM stn_t UNION SELECT c FROM stn_u"
                            + " ORDER BY \"Foo\""));
        }

        @Test
        void aNameOfAnArmTheSetOperationDoesNotCarry() throws Exception {
            SQLException e = fails("SELECT id AS a FROM stn_dept UNION SELECT id AS b"
                    + " FROM stn_emp ORDER BY b", "42703");
            assertEquals("There is a column named \"b\" in table \"*SELECT* 2\", but it cannot be"
                    + " referenced from this part of the query.", detailOf(e));
        }

        @Test
        void aSchemaWrittenInFrontOfAnAliasedEntry() throws Exception {
            SQLException e = fails("SELECT pg_catalog.stn_emp.id FROM stn_emp", "42P01");
            assertEquals("There is an entry for table \"stn_emp\", but it cannot be referenced"
                    + " from this part of the query.", detailOf(e));
            SQLException e2 = fails("SELECT public.a.k FROM stn_p a", "42P01");
            assertEquals("There is an entry for table \"a\", but it cannot be referenced"
                    + " from this part of the query.", detailOf(e2));
        }
    }

    @Nested
    class AValuesListIsAQueryOfItsOwn {

        @Test
        void oneRowTakesTheClausesTwoRowsAlreadyTook() throws Exception {
            assertEquals(Arrays.asList("7"), rows("VALUES (7) ORDER BY 1"));
            assertEquals(Arrays.asList("7"), rows("VALUES (7) ORDER BY column1"));
            assertEquals(Arrays.asList("7"), rows("VALUES (7) LIMIT 1"));
            assertEquals(Collections.emptyList(), rows("VALUES (7) OFFSET 1"));
            assertEquals(Arrays.asList("1|a"), rows("VALUES (1, 'a') ORDER BY column2"));
        }

        @Test
        void theSameRefusalsAsAnyOtherQuery() throws Exception {
            failsWith("VALUES (7) ORDER BY NULL", "42601", "non-integer constant in ORDER BY");
            fails("VALUES (7) ORDER BY 2", "42P10");
        }

        @Test
        void severalRowsKeepWorking() throws Exception {
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("VALUES (3), (1), (2) ORDER BY column1"));
            assertEquals(Arrays.asList("7", "8"), rows("VALUES (7) UNION VALUES (8) ORDER BY 1"));
        }
    }

    @Nested
    class WhatASetOperationCanTellRowsApartBy {

        @Test
        void aTypeWithNoEqualityCannotBeCompared() throws Exception {
            failsWith("SELECT '{}'::json UNION SELECT '[]'::json", "42883",
                    "could not identify an equality operator for type json");
            failsWith("SELECT '{}'::json INTERSECT SELECT '[]'::json", "42883",
                    "could not identify an equality operator for type json");
            failsWith("SELECT '{}'::json EXCEPT SELECT '[]'::json", "42883",
                    "could not identify an equality operator for type json");
            failsWith("SELECT '<a/>'::xml UNION SELECT '<b/>'::xml", "42883",
                    "could not identify an equality operator for type xml");
            failsWith("SELECT point '(1,2)' UNION SELECT point '(3,4)'", "42883",
                    "could not identify an equality operator for type point");
        }

        @Test
        void unionAllComparesNothingAndTakesThem() throws Exception {
            assertEquals(2, rows("SELECT '{}'::json UNION ALL SELECT '[]'::json").size());
            assertEquals(1, rows("SELECT '{\"a\":1}'::jsonb UNION"
                    + " SELECT '{\"a\":1}'::jsonb").size());
        }

        @Test
        void aDateAndATimestampAreOneColumnOfTimestamps() throws Exception {
            assertEquals("date:timestamp",
                    labels("SELECT '2020-01-01'::date UNION SELECT '2020-01-01'::timestamp"));
            assertEquals(Arrays.asList("2020-01-01 00:00:00.0"),
                    rows("SELECT '2020-01-01'::date UNION SELECT '2020-01-01'::timestamp"
                            + " ORDER BY 1"));
            assertEquals(Arrays.asList("2020-01-01 00:00:00.0", "2020-01-02 00:00:00.0"),
                    rows("SELECT '2020-01-01'::timestamp UNION SELECT '2020-01-02'::date"
                            + " ORDER BY 1"));
        }

        @Test
        void aDateAndATimeHaveNoOneTypeBetweenThem() throws Exception {
            failsWith("SELECT '2020-01-01'::date UNION SELECT '10:00'::time", "42846",
                    "UNION could not convert type time without time zone to date");
            failsWith("SELECT '10:00'::time INTERSECT SELECT '2020-01-01'::date", "42846",
                    "INTERSECT could not convert type date to time without time zone");
        }
    }

    @Nested
    class TheTypeAndTheShapeTheResultCarries {

        @Test
        void arrayOverASetOperationCarriesItsArmsType() throws Exception {
            assertEquals("r:_int4", labels("SELECT ARRAY(SELECT 1 UNION ALL SELECT 2) AS r"));
            assertEquals("r:int4", labels("SELECT (SELECT 1 UNION SELECT 1) AS r"));
            assertEquals(Arrays.asList("{1,2}"), rows("SELECT ARRAY(SELECT 1 UNION ALL SELECT 2)"));
        }

        @Test
        void aFunctionThatReturnsAnArrayReturnsOneValue() throws Exception {
            assertEquals("stn_arr:_int4", labels("SELECT * FROM stn_arr()"));
            assertEquals(Arrays.asList("{1,2}"), rows("SELECT * FROM stn_arr()"));
        }

        @Test
        void aBuiltInReturningRecordNeedsItsColumnsNamed() throws Exception {
            failsWith("SELECT * FROM json_to_record('{\"a\":1}'::json)", "42601",
                    "a column definition list is required for functions returning \"record\"");
            failsWith("SELECT * FROM json_to_recordset('[{\"a\":1}]'::json)", "42601",
                    "a column definition list is required for functions returning \"record\"");
            assertEquals(Arrays.asList("1"),
                    rows("SELECT * FROM json_to_record('{\"a\":1}'::json) AS t(a int)"));
        }

        @Test
        void anAggregateMayNotHoldASetReturningCall() throws Exception {
            failsWith("SELECT string_agg((stn_out()).y, ',')", "0A000",
                    "aggregate function calls cannot contain set-returning function calls");
            failsWith("SELECT sum(stn_setofint())", "0A000",
                    "aggregate function calls cannot contain set-returning function calls");
        }

        @Test
        void aCastThatCarriesALengthIsACoercion() throws Exception {
            failsWith("SELECT c1 FROM (SELECT 1.5::numeric AS c1) x UNION SELECT 2.5"
                    + " ORDER BY c1::numeric(10,2)", "0A000",
                    "invalid UNION/INTERSECT/EXCEPT ORDER BY clause");
            failsWith("SELECT c1 FROM (SELECT 'x'::varchar(4) AS c1) x UNION SELECT 'y'"
                    + " ORDER BY c1::varchar(4)", "0A000",
                    "invalid UNION/INTERSECT/EXCEPT ORDER BY clause");
            // ... and one that carries no length over a column of that type is still no cast
            assertEquals(Arrays.asList("1.5", "2.5"),
                    rows("SELECT c1 FROM (SELECT 1.5::numeric AS c1) x UNION SELECT 2.5"
                            + " ORDER BY c1::numeric"));
        }
    }

    // ================================================================

    @Nested
    class OrdinarySqlThatHasToKeepWorking {

        @Test
        void setOperationsOfEveryShape() throws Exception {
            assertEquals(Arrays.asList("1", "2"), rows("SELECT 1 UNION SELECT 2 ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"), rows("(SELECT 1) UNION (SELECT 2) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("(SELECT 1 UNION SELECT 2) UNION SELECT 3 ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT 1 UNION (SELECT 2 UNION SELECT 3) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"), rows("((SELECT 1)) UNION ((SELECT 2)) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"), rows("((SELECT 1) UNION (SELECT 2)) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "1", "2"),
                    rows("((SELECT 1) UNION ALL (SELECT 1)) UNION ALL ((SELECT 2)) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("WITH w AS ((SELECT 1) UNION (SELECT 2)) SELECT * FROM w ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("SELECT * FROM (SELECT 1 UNION SELECT 2) z ORDER BY 1"));
        }

        @Test
        void orderByNamesThatReachExactlyOneColumn() throws Exception {
            assertEquals(Arrays.asList("1|a", "2|b"), rows("SELECT a, b FROM stn_t ORDER BY a"));
            assertEquals(Arrays.asList("1|a", "2|b"),
                    rows("SELECT a AS k, b FROM stn_t ORDER BY k"));
            assertEquals(Arrays.asList("1|2", "2|3"),
                    rows("SELECT a, a + 1 AS a2 FROM stn_t ORDER BY a"));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT a AS k FROM stn_t UNION SELECT c FROM stn_u ORDER BY k"));
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT a AS k FROM stn_t UNION SELECT c FROM stn_u ORDER BY K"));
            assertEquals(Arrays.asList("1|a", "2|b", "3|c"),
                    rows("SELECT a, b FROM stn_t UNION SELECT c, d FROM stn_u ORDER BY a, b"));
            // an ordinal never has a name to be ambiguous about
            assertEquals(Arrays.asList("1|a", "2|b"),
                    rows("SELECT a AS k, b AS k FROM stn_t ORDER BY 1"));
            assertEquals(Arrays.asList("1|a", "2|b"),
                    rows("SELECT a AS k, b AS k FROM stn_t ORDER BY a"));
            assertEquals(Arrays.asList("2|b", "1|a"),
                    rows("SELECT a, b FROM stn_t ORDER BY a DESC, b"));
        }

        @Test
        void assignmentsOfEveryOtherShape() throws Exception {
            assertEquals(1, update("UPDATE stn_t SET a = 9 WHERE a = 1"));
            assertEquals(1, update("UPDATE stn_t SET a = 8, b = 'z' WHERE a = 9"));
            assertEquals(1, update("UPDATE stn_t SET a = (SELECT 7) WHERE a = 8"));
            assertEquals(1, update("UPDATE stn_t SET b = (SELECT d FROM stn_u WHERE c = 3)"
                    + " WHERE a = 7"));
            assertEquals(1, update("UPDATE stn_t SET a = 4 WHERE a IN (SELECT c FROM stn_u)"
                    + " OR a = 7"));
            assertEquals(Arrays.asList("2|b", "4|c"), sortedRows("SELECT a, b FROM stn_t"));
        }

        @Test
        void insertsFromAQueryOfEveryOtherShape() throws Exception {
            assertEquals(1, update("INSERT INTO stn_tgt(id) SELECT 5"));
            assertEquals(1, update("INSERT INTO stn_tgt(id) (SELECT 6)"));
            assertEquals(2, update("INSERT INTO stn_tgt(id) VALUES (7), (8)"));
            assertEquals(Arrays.asList("5", "6", "7", "8"),
                    rows("SELECT id FROM stn_tgt ORDER BY id"));
        }

        @Test
        void comparisonsAgainstSetsAndRows() throws Exception {
            assertEquals(Arrays.asList("1"), rows("SELECT 1 WHERE 1 IN (1, 2, 3)"));
            assertEquals(Arrays.asList("1"), rows("SELECT 1 WHERE 1 IN (SELECT 1)"));
            assertEquals(Arrays.asList("1"), rows("SELECT 1 WHERE 1 = ANY (ARRAY[1, 2])"));
            assertEquals(Arrays.asList("1"),
                    rows("SELECT 1 WHERE 1 = ANY (SELECT 1 UNION SELECT 2)"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("SELECT a FROM stn_t WHERE (a, b) IN ((1, 'a'), (2, 'b')) ORDER BY 1"));
            assertEquals("true", one("SELECT ((1, 2) = (SELECT 1, 2))::text"));
            assertEquals("true", one("SELECT ((1, 2) < (SELECT 1, 3))::text"));
            assertEquals("true", one("SELECT (ROW(1, 'a') = ROW(1, 'a'))::text"));
            assertEquals("true", one("SELECT ((1,2) IS DISTINCT FROM (1,3))::text"));
            assertEquals(Arrays.asList("{1,2}"), rows("SELECT ARRAY[1, (SELECT 2)]"));
        }

        @Test
        void functionsInFromThatStillProduceSets() throws Exception {
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT * FROM generate_series(1, 3) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("SELECT * FROM unnest(ARRAY[1, 2]) ORDER BY 1"));
            assertEquals(Arrays.asList("1", "2"),
                    rows("SELECT * FROM stn_setofint() ORDER BY 1"));
            assertEquals(Arrays.asList("1|a"), rows("SELECT * FROM stn_out()"));
        }

        @Test
        void setOperationsOverOrdinaryTypes() throws Exception {
            assertEquals(Arrays.asList("1", "2", "3"),
                    rows("SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1"));
            assertEquals(Arrays.asList("1", "1.5"), rows("SELECT 1 UNION SELECT 1.5 ORDER BY 1"));
            assertEquals(Arrays.asList("a", "b", "c"),
                    rows("SELECT b FROM stn_t UNION SELECT d FROM stn_u ORDER BY 1"));
            assertEquals(Arrays.asList("2020-01-01", "2020-01-02"),
                    rows("SELECT '2020-01-01'::date UNION SELECT '2020-01-02'::date ORDER BY 1"));
            assertEquals(Arrays.asList("10:00:00", "11:00:00"),
                    rows("SELECT '10:00'::time UNION SELECT '11:00'::time ORDER BY 1"));
            assertEquals(Arrays.asList("1", "<null>"),
                    rows("SELECT NULL UNION ALL SELECT 1 ORDER BY 1"));
        }
    }
}
