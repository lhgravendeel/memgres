package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which FULL JOINs PostgreSQL refuses, decided the way PostgreSQL decides it.
 *
 * <p>PostgreSQL has no nested-loop plan for a full join: it must merge or hash the two sides, and
 * both need an equality between one side's value and the other's, so a condition offering neither
 * is {@code 0A000 FULL JOIN is only supported with merge-joinable or hash-joinable join
 * conditions}. Memgres reproduces a limitation it does not have, so that an application cannot
 * tell the two engines apart — which makes a refusal PostgreSQL would not have raised pure loss,
 * and the rule is written to accept whatever it cannot read.
 *
 * <p><b>PostgreSQL does not read the condition as written.</b> {@code eval_const_expressions} runs
 * first: NOT is eliminated by replacing an operator with its negator and by de Morgan, so
 * {@code NOT (a <> b)} is an equality; {@code x = true} folds to {@code x}; a cast that changes
 * nothing disappears; a CASE whose conditions are constant collapses to the arm that holds. Then
 * {@code prepqual} factors an OR whose arms share a clause, so {@code (A AND B) OR (A AND C)}
 * becomes {@code A AND (B OR C)} and even {@code A OR A} becomes {@code A}. A single-element
 * {@code IN} never reaches the planner as one: the parser writes it as an equality, and a list
 * whose members are not all constants as an OR of them. All of that was measured against
 * PostgreSQL 18 rather than reasoned about, in both directions — {@code NOT (a = b)},
 * {@code (a = b) IS TRUE}, {@code a IN (b, 1)} and {@code a = (SELECT 1)} are still refused.
 *
 * <p><b>A full join above which a strict qual sits is not a full join.</b>
 * {@code reduce_outer_joins} downgrades it to a left, right or inner join when a qual rejects the
 * rows one side was padded with, and the downgraded join is never asked the question. The qual may
 * be in WHERE, in a HAVING clause with no aggregate in it, or in the ON condition of an inner join
 * above. Only a strict qual counts: {@code WHERE a.x IS NOT NULL} downgrades the join and
 * {@code WHERE a.x IS NULL} does not, which is the difference between answering and refusing. It
 * counts relations and not columns, as {@code find_nonnullable_rels} does: an OR proves what every
 * arm of it proves, so {@code a.x = 1 OR a.t = 'b'} proves that {@code a} is not null, and
 * {@code a.x IS NOT NULL OR b.y IS NOT NULL} proves nothing.
 *
 * <p><b>Only the query the client sent is judged.</b> The qual that rescues a join may be written
 * a long way from it: PostgreSQL pulls simple subqueries up, and pushes a qual down into the ones
 * it cannot pull up, which it declines to do only for LIMIT and OFFSET. Deciding which applies is
 * reimplementing the optimiser, and two attempts to do it refused thirty-seven queries PostgreSQL
 * answers. So a full join inside a subquery, a WITH query, a view, an arm of a set operation, the
 * source of a writing statement or a function body is accepted unread. What is bought for that is
 * that no valid statement is refused; what is paid is that a handful PostgreSQL declines to plan
 * are answered.
 *
 * <p><b>Analysis comes before planning.</b> A view's body is analysed when the view is defined, so
 * the complaints that belong to analysis — a column name the view would answer to twice among them
 * — are raised then. A name no relation of the query answers to is reported before anything is
 * planned too, so it comes ahead of the refusal.
 *
 * <p>The last section is ordinary SQL, which has to keep working: the cost of a rule that reaches
 * too far is a refused valid statement.
 */
class FullJoinAdmissibilityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE fja_a (x int, t text)");
        exec("CREATE TABLE fja_b (y int, t2 text)");
        exec("CREATE TABLE fja_c (z int, t3 text)");
        exec("INSERT INTO fja_a VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("INSERT INTO fja_b VALUES (1,'a'),(9,'z')");
        exec("INSERT INTO fja_c VALUES (1,'a'),(2,'b')");
        exec("CREATE TABLE fja_p (id int, av text)");
        exec("CREATE TABLE fja_q (id int, bv text)");
        exec("CREATE VIEW fja_fv AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x < b.y");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    /** One row per entry, columns joined by '|', in the order the query answered. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append('|');
                        sb.append(rs.getString(i) == null ? "-" : rs.getString(i));
                    }
                    out.add(sb.toString());
                }
                return String.join(",", out);
            }
        }
    }

    private static final String UNMERGEABLE =
            "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions";

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    /** The condition on the two-table fixture, and the number of rows it answers with. */
    private static void on(String condition, String expected) throws SQLException {
        assertEquals(expected, rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON " + condition),
                "ON " + condition);
    }

    private static void onRefused(String condition) {
        assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON " + condition,
                "0A000", UNMERGEABLE);
    }

    // ---- 1: the condition is normalised before it is judged ----

    @Nested
    class NormalisedBeforeJudged {

        @Test
        void notOverAnInequalityIsTheEqualityItNegates() throws SQLException {
            on("NOT (a.x <> b.y)", "4");
            on("NOT (a.x <> b.y) AND a.x > 0", "4");
        }

        @Test
        void twoNotsCancel() throws SQLException {
            on("NOT NOT (a.x = b.y)", "4");
            on("NOT (NOT (a.x = b.y))", "4");
            on("NOT (NOT (NOT (a.x <> b.y)))", "4");
        }

        @Test
        void notDistributesOverAndAndOr() throws SQLException {
            on("NOT (a.x <> b.y OR a.t <> b.t2)", "4");
            on("NOT (a.x <> b.y OR a.t = b.t2)", "5");
        }

        @Test
        void threeNotsAreOneNot() {
            onRefused("NOT NOT NOT (a.x = b.y)");
        }

        @Test
        void aNegatedEqualityIsStillAnInequality() {
            onRefused("NOT (a.x = b.y)");
            onRefused("NOT (a.x < b.y)");
            onRefused("NOT (a.x >= b.y)");
            onRefused("NOT (NOT (a.x <> b.y))");
            onRefused("NOT (a.x IS DISTINCT FROM b.y)");
            onRefused("NOT (a.x = b.y OR a.t = b.t2)");
            onRefused("NOT (a.x <> b.y AND a.t <> b.t2)");
            onRefused("NOT (a.x >= b.y AND a.x <= b.y)");
        }

        @Test
        void comparingABooleanWithTrueIsTheBooleanItself() throws SQLException {
            on("(a.x = b.y) = true", "4");
            on("true = (a.x = b.y)", "4");
            on("(a.x = b.y) <> false", "4");
            on("(a.x = b.y) = (1=1)", "4");
        }

        @Test
        void comparingItWithFalseNegatesIt() {
            onRefused("(a.x = b.y) = false");
            onRefused("(a.x = b.y) <> true");
            onRefused("(a.x < b.y) = true");
        }

        @Test
        void aBooleanTestIsNotABooleanEquality() {
            onRefused("(a.x = b.y) IS TRUE");
            onRefused("(a.x = b.y) IS NOT FALSE");
            onRefused("(a.x = b.y) IS NOT NULL");
        }

        @Test
        void aCastToBooleanOfABooleanChangesNothing() throws SQLException {
            on("(a.x = b.y)::boolean", "4");
            on("((a.x = b.y)::boolean)::boolean", "4");
            on("(a.x = b.y)::bool = true", "4");
        }

        @Test
        void aSingleElementInIsAnEquality() throws SQLException {
            on("a.x IN (b.y)", "4");
            on("a.x IN (b.y, b.y)", "4");
        }

        @Test
        void anInWithAnythingElseInItIsNot() {
            onRefused("a.x IN (b.y, 1)");
            onRefused("a.x IN (b.y, b.y+1)");
            onRefused("a.x IN (1,2)");
            onRefused("a.x NOT IN (b.y)");
        }

        @Test
        void aSubqueryOverOneSideKeepsTheClauseAJoinClause() throws SQLException {
            on("a.x = (SELECT b.y)", "4");
            on("a.x = (SELECT b.y WHERE true)", "4");
        }

        @Test
        void aSubqueryOverNeitherSideDoesNot() {
            onRefused("a.x = (SELECT 1)");
            onRefused("a.x = (SELECT max(y) FROM fja_b)");
        }

        @Test
        void anOrWhoseArmsShareAClauseIsFactored() throws SQLException {
            on("a.x = b.y OR a.x = b.y", "4");
            on("a.x = b.y OR a.x = b.y OR a.x = b.y", "4");
            on("a.x = b.y OR (a.x = b.y AND a.t = b.t2)", "4");
            on("a.x = b.y OR (a.t = b.t2 AND a.x = b.y)", "4");
            on("(a.x = b.y AND a.t = b.t2) OR (a.x = b.y AND a.t < b.t2)", "4");
            on("(a.x = b.y AND a.x > 0) OR (a.x = b.y AND b.y > 0)", "4");
            on("(a.x = b.y AND a.t = b.t2) OR (a.t = b.t2 AND a.x = b.y)", "4");
            on("(a.x = b.y AND true) OR (a.x = b.y AND false)", "4");
            on("((a.x = b.y AND a.x > 0) OR (a.x = b.y AND b.y > 0)) AND a.t < b.t2", "5");
        }

        @Test
        void factoringOutAnInequalityLeavesAnInequality() {
            onRefused("(a.x < b.y AND a.t = b.t2) OR (a.x < b.y AND a.t < b.t2)");
            onRefused("(a.x < b.y AND a.x > 0) OR (a.x < b.y AND b.y > 0)");
            onRefused("a.x < b.y OR a.x < b.y");
        }

        @Test
        void anOrWithNothingInCommonStaysAnOr() {
            onRefused("a.x = b.y OR a.t = b.t2");
        }

        @Test
        void theOperatorMayBeWrittenWithItsSchema() throws SQLException {
            on("a.x operator(pg_catalog.=) b.y", "4");
            onRefused("a.x OPERATOR(pg_catalog.<) b.y");
        }

        @Test
        void aCaseWithConstantConditionsCollapses() throws SQLException {
            on("CASE WHEN true THEN a.x = b.y ELSE false END", "4");
            onRefused("CASE WHEN false THEN a.x = b.y ELSE a.x < b.y END");
            onRefused("CASE WHEN a.x > 0 THEN a.x = b.y ELSE false END");
        }

        @Test
        void aConditionThatFoldsToAConstantIsNeverRefused() throws SQLException {
            on("true", "6");
            on("false", "5");
            on("null", "5");
            on("1=2", "5");
            on("false AND a.x < b.y", "5");
            on("NULL AND a.x < b.y", "5");
            on("a.x < b.y OR true", "6");
            on("a.x < b.y AND NULL", "5");
        }

        @Test
        void oneEqualityCarriesAnyNumberOfUnmergeableClausesBesideIt() throws SQLException {
            on("a.x = b.y AND a.x < b.y", "5");
            on("a.x = b.y AND (a.t = b.t2 OR a.t < b.t2)", "4");
            on("(a.x = b.y OR a.t = b.t2) AND a.x = b.y", "4");
        }

        @Test
        void everythingElseIsStillRefused() {
            onRefused("a.x < b.y");
            onRefused("a.x <> b.y");
            onRefused("a.x = 1");
            onRefused("a.x IS NOT DISTINCT FROM b.y");
            onRefused("(a.x + b.y) = 5");
            onRefused("a.x IS NULL");
            onRefused("a.t LIKE b.t2");
            onRefused("a.x BETWEEN b.y AND b.y");
            onRefused("coalesce(a.x = b.y, false)");
            onRefused("EXISTS (SELECT 1 FROM fja_c c WHERE c.z = a.x AND c.z = b.y)");
            onRefused("1=1 AND a.x < b.y");
        }
    }

    // ---- 2: a strict qual above the join makes it an inner one ----

    @Nested
    class QualsAboveTheJoin {

        @Test
        void aStrictWhereOnEitherSideLiftsTheRestriction() throws SQLException {
            assertEquals("3", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x IS NOT NULL"));
            assertEquals("4", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE b.y IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x > 0"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE NOT (a.x IS NULL)"));
            assertEquals("2", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x IN (1,2)"));
            assertEquals("1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE upper(a.t) = 'A'"));
            assertEquals("1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x::text = '1'"));
            assertEquals("0", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x IS NOT NULL AND b.y IS NULL"));
        }

        @Test
        void aWhereThatToleratesNullsDoesNot() {
            String q = "SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE ";
            assertRejected(q + "a.x IS NULL", "0A000", UNMERGEABLE);
            assertRejected(q + "b.y IS NULL", "0A000", UNMERGEABLE);
            assertRejected(q + "coalesce(a.x,0) >= 0", "0A000", UNMERGEABLE);
            assertRejected(q + "greatest(a.x,0) = 1", "0A000", UNMERGEABLE);
            assertRejected(q + "a.x IS NULL OR a.x > 0", "0A000", UNMERGEABLE);
            assertRejected(q + "a.x IS NOT NULL OR b.y IS NOT NULL", "0A000", UNMERGEABLE);
            assertRejected(q + "true", "0A000", UNMERGEABLE);
            assertRejected(q + "1 = 1", "0A000", UNMERGEABLE);
            assertRejected(q + "exists (select 1)", "0A000", UNMERGEABLE);
        }

        @Test
        void aWhereThatCanNeverHoldLeavesNoJoinToPlan() throws SQLException {
            assertEquals("0", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE false"));
            assertEquals("0", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE 1=2"));
            assertEquals("0", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE null"));
        }

        @Test
        void aHavingClauseWithNoAggregateIsAWhereClause() throws SQLException {
            assertEquals("1,1,1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " GROUP BY a.x HAVING a.x IS NOT NULL"));
            assertEquals("1,1,1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " GROUP BY a.x HAVING a.x IS NOT NULL AND count(*) > 0"));
        }

        @Test
        void aHavingClauseWithOneIsNot() {
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " GROUP BY a.x HAVING count(*) > 0", "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " HAVING count(*) > 0", "0A000", UNMERGEABLE);
        }

        @Test
        void anInnerJoinAboveFiltersBothOfItsArms() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " JOIN fja_c c ON c.z = a.x"));
            assertEquals("2", rows("SELECT count(*) FROM fja_c c"
                    + " JOIN (fja_a a FULL JOIN fja_b b ON a.x < b.y) ON c.z = a.x"));
            assertEquals("2", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y,"
                    + " fja_c c WHERE c.z = a.x"));
        }

        @Test
        void anOuterJoinAboveOnlyFiltersTheArmItMayPadAway() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM fja_c c"
                    + " LEFT JOIN (fja_a a FULL JOIN fja_b b ON a.x < b.y) ON c.z = a.x"));
            assertEquals("2", rows("SELECT count(*) FROM (fja_a a FULL JOIN fja_b b ON a.x < b.y)"
                    + " RIGHT JOIN fja_c c ON c.z = a.x"));
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " LEFT JOIN fja_c c ON c.z = a.x", "0A000", UNMERGEABLE);
        }

        @Test
        void aJoinAboveThatFiltersNothingLeavesTheRestrictionInPlace() {
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " JOIN fja_c c ON true", "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " CROSS JOIN fja_c c", "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y,"
                    + " fja_c c", "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y"
                    + " FULL JOIN fja_c c ON b.y < c.z", "0A000", UNMERGEABLE);
        }

        @Test
        void aQualOfTheQueryAboveReachesIntoASubqueryItPullsUp() throws SQLException {
            assertEquals("3", rows("SELECT count(*) FROM"
                    + " (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) s"
                    + " WHERE s.x IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM"
                    + " (SELECT a.x AS xx, b.y FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) s"
                    + " WHERE s.xx IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM"
                    + " (SELECT a.x + 0 AS xx FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) s"
                    + " WHERE s.xx IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM (SELECT * FROM"
                    + " (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y) t) s"
                    + " WHERE s.x IS NOT NULL"));
        }

        @Test
        void andIntoAWithQueryAndAView() throws SQLException {
            assertEquals("3", rows("WITH s AS (SELECT * FROM fja_a a FULL JOIN fja_b b ON a.x < b.y)"
                    + " SELECT count(*) FROM s WHERE s.x IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM fja_fv WHERE x IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM fja_fv v JOIN fja_c c ON c.z = v.x"));
            assertEquals("2", rows("SELECT count(*) FROM fja_fv v, fja_c c WHERE c.z = v.x"));
        }

        @Test
        void aQualNamingAnotherRelationProvesNothingAboutThisJoin() {
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " JOIN fja_c c ON c.z > 0", "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y,"
                    + " fja_c c WHERE c.z > 0", "0A000", UNMERGEABLE);
        }

        @Test
        void anOrProvesOnlyWhatEveryArmOfItProves() throws SQLException {
            String q = "SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y WHERE ";
            // Both arms reject a null row of a, whatever column of it each one reads.
            assertEquals("3", rows(q + "a.x IS NOT NULL OR a.t IS NOT NULL"));
            assertEquals("2", rows(q + "a.x = 1 OR a.t = 'b'"));
            assertEquals("0", rows(q + "NOT (a.x > 0 AND a.t > '')"));
            assertEquals("3", rows(q + "(a.x = 1 AND a.t = 'a') OR a.x > 1"));
            // One arm rejects a null a and the other a null b, so neither is proved.
            assertRejected(q + "a.x IS NOT NULL OR b.y IS NOT NULL", "0A000", UNMERGEABLE);
            assertRejected(q + "a.x IS NOT NULL OR a.t IS NULL", "0A000", UNMERGEABLE);
        }

        @Test
        void aQualInsideAnythingElseIsNotThisQuerysQual() throws SQLException {
            // The join no longer stands in the query being planned, so nothing here judges it:
            // PostgreSQL may still rescue it by pulling the subquery up or pushing a qual down.
            assertEquals("4", rows("SELECT count(*) FROM (SELECT * FROM fja_a a FULL JOIN fja_b b"
                    + " ON a.x < b.y) s"));
            assertEquals("3", rows("SELECT count(*) FROM (SELECT * FROM fja_a a FULL JOIN fja_b b"
                    + " ON a.x < b.y LIMIT 10) s WHERE s.x IS NOT NULL"));
            assertEquals("4", rows("WITH s AS (SELECT * FROM fja_a a FULL JOIN fja_b b"
                    + " ON a.x < b.y) SELECT count(*) FROM s"));
            assertEquals("4", rows("SELECT count(*) FROM fja_fv"));
            assertEquals("4", rows("SELECT (SELECT count(*) FROM fja_a a FULL JOIN fja_b b"
                    + " ON a.x < b.y)"));
            assertEquals("4,1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " UNION ALL SELECT 1"));
        }

        @Test
        void theWhereOfAWritingStatementIsNotAQualAboveItsFromClause() throws SQLException {
            exec("UPDATE fja_c SET t3 = t3 FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE fja_c.z = a.x");
            exec("DELETE FROM fja_c USING fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE fja_c.z = a.x AND fja_c.z = 99");
            exec("UPDATE fja_c SET t3 = t3 FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE fja_c.z = 99");
        }

        @Test
        void neitherLimitNorOrderByGetsPastTheRefusal() {
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y LIMIT 0",
                    "0A000", UNMERGEABLE);
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y ORDER BY 1",
                    "0A000", UNMERGEABLE);
        }
    }

    // ---- 3: what is reported before the plan is made ----

    @Nested
    class AnalysisBeforePlanning {

        @Test
        void aNameThatDoesNotResolveIsReportedFirst() {
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE c.z = 1", "42P01", "missing FROM-clause entry for table \"c\"");
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE nosuchcol = 1", "42703", "column \"nosuchcol\" does not exist");
            assertRejected("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.nosuch = 1", "42703", "column a.nosuch does not exist");
        }

        @Test
        void anAmbiguousNameInTheConditionIsReportedAsAmbiguous() {
            assertRejected("SELECT count(*) FROM fja_a FULL JOIN fja_a a2 ON x = x",
                    "42702", "column reference \"x\" is ambiguous");
        }

        @Test
        void aViewOverARefusedJoinIsStoredAndReadsWithoutComplaint() throws SQLException {
            exec("CREATE VIEW fja_v1 AS SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x > b.y");
            assertEquals("4", rows("SELECT count(*) FROM fja_v1"));
            exec("DROP VIEW fja_v1");
        }

        @Test
        void aViewWhoseOutputRepeatsAColumnNameIsStillRefused() {
            assertRejected("CREATE VIEW fja_v2 AS SELECT * FROM fja_p a FULL JOIN fja_q b"
                    + " ON a.id > b.id", "42701", "column \"id\" specified more than once");
            assertRejected("CREATE VIEW fja_v2 AS SELECT * FROM fja_p a FULL JOIN fja_q b"
                    + " ON a.id = b.id", "42701", "column \"id\" specified more than once");
        }

        @Test
        void aMaterializedViewsBodyIsNotTheQueryBeingPlanned() throws SQLException {
            exec("CREATE MATERIALIZED VIEW fja_mv AS SELECT a.x, b.y FROM fja_a a"
                    + " FULL JOIN fja_b b ON a.x > b.y");
            assertEquals("4", rows("SELECT count(*) FROM fja_mv"));
            exec("DROP MATERIALIZED VIEW fja_mv");
            exec("CREATE MATERIALIZED VIEW fja_mv AS SELECT a.x, b.y FROM fja_a a"
                    + " FULL JOIN fja_b b ON a.x > b.y WITH NO DATA");
            exec("DROP MATERIALIZED VIEW fja_mv");
        }

        @Test
        void theSourceOfAWritingStatementIsNotEither() throws SQLException {
            exec("CREATE TABLE fja_w (z int, t3 text)");
            exec("INSERT INTO fja_w SELECT a.x, b.t2 FROM fja_a a FULL JOIN fja_b b ON a.x < b.y");
            assertEquals("4", rows("SELECT count(*) FROM fja_w"));
            exec("DROP TABLE fja_w");
        }
    }

    // ---- 4: ordinary SQL around the rule ----

    @Nested
    class OrdinarySql {

        @Test
        void aPlainFullJoinPadsBothWays() throws SQLException {
            assertEquals("1|1,2|-,3|-,-|9",
                    rows("SELECT a.x, b.y FROM fja_a a FULL JOIN fja_b b ON a.x = b.y ORDER BY 1,2"));
        }

        @Test
        void anEqualityBetweenExpressionsIsAccepted() throws SQLException {
            on("coalesce(a.x,0) = coalesce(b.y,0)", "4");
            on("abs(a.x) = abs(b.y)", "4");
            on("a.x = length(b.t2)", "4");
            on("a.x * 1 = b.y", "4");
            on("a.x = b.y::int", "4");
            on("(a.x, a.t) = (b.y, b.t2)", "4");
            on("a.x = b.y AND a.x IN (SELECT z FROM fja_c)", "4");
        }

        @Test
        void usingAndNaturalJoinOnEqualityAndAreAccepted() throws SQLException {
            assertEquals("6", rows("SELECT count(*) FROM fja_a a NATURAL FULL JOIN fja_b b"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_a a2 USING (x)"));
        }

        @Test
        void onlyAFullJoinIsRestricted() throws SQLException {
            assertEquals("3", rows("SELECT count(*) FROM fja_a a LEFT JOIN fja_b b ON a.x < b.y"));
            assertEquals("4", rows("SELECT count(*) FROM fja_a a RIGHT JOIN fja_b b ON a.x < b.y"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a JOIN fja_b b ON a.x < b.y"));
            assertEquals("0", rows("SELECT count(*) FROM fja_a a LEFT JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x IS NULL"));
        }

        @Test
        void aJoinTreeKeepsWorkingWhateverSitsAboveIt() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM fja_a a JOIN fja_b b ON a.x < b.y"
                    + " JOIN fja_c c ON c.z = a.x"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a LEFT JOIN fja_b b ON a.x < b.y"
                    + " LEFT JOIN fja_c c ON c.z = b.y"));
            assertEquals("2", rows("SELECT count(*) FROM fja_a a JOIN fja_b b ON a.x = b.y"
                    + " RIGHT JOIN fja_c c ON c.z = a.x"));
            assertEquals("1", rows("SELECT count(*) FROM fja_a a, fja_b b WHERE a.x = b.y"));
            assertEquals("1", rows("SELECT count(*) FROM fja_a a CROSS JOIN fja_b b"
                    + " WHERE a.x = b.y"));
        }

        @Test
        void aLateralJoinIsUnaffected() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM fja_a a JOIN LATERAL"
                    + " (SELECT b.y FROM fja_b b WHERE b.y >= a.x) s ON true"));
            assertEquals("3", rows("SELECT count(*) FROM fja_a a LEFT JOIN LATERAL"
                    + " (SELECT b.y FROM fja_b b WHERE b.y = a.x) s ON true"));
        }

        @Test
        void aMergeableFullJoinAnswersWhateverIsAskedOfIt() throws SQLException {
            assertEquals("1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y"
                    + " WHERE a.x IS NULL"));
            assertEquals("4", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y"
                    + " WHERE coalesce(a.x,0) >= 0"));
            assertEquals("1,1,1,1", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b"
                    + " ON a.x = b.y GROUP BY a.x HAVING count(*) > 0"));
            assertEquals("5", rows("SELECT count(*) FROM fja_a a FULL JOIN fja_b b ON a.x = b.y"
                    + " FULL JOIN fja_c c ON b.y = c.z"));
            assertEquals("1,2,3", rows("SELECT a.x FROM fja_a a FULL JOIN fja_b b ON a.x < b.y"
                    + " WHERE a.x IS NOT NULL ORDER BY 1"));
        }
    }
}
