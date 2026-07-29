package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a set operation may be ordered by, and how wide a subquery standing for one value may be.
 *
 * <p><b>The ORDER BY of a set operation sees the output columns and nothing else.</b> Once the arms
 * have been combined there is no relation left to read a name from, so PostgreSQL takes an output
 * column name or its position and refuses everything else — and the refusal differs by shape: an
 * ordinal outside the select list is out of range (42P10), a bare non-integer constant names no
 * column at all (42601), a name written anywhere in the item that the output does not account for
 * is a missing column (42703, or 42P01 when it is qualified), and anything that survives that and
 * is still not a plain column name is an expression the clause does not take (0A000). None of this
 * was checked: the sort matched what it could and quietly ignored the rest.
 *
 * <p><b>The LIMIT and OFFSET beside it were read with neither a sign check nor a rounding</b>, so
 * {@code OFFSET -1} changed the row order instead of raising — a wrong answer rather than an error
 * — and {@code LIMIT -1} came back as an internal error from a negative sublist index. Both now go
 * through the same reader a plain SELECT uses. Measured, not assumed: PostgreSQL casts a fractional
 * count to bigint rather than truncating it, so {@code LIMIT 1.5} is two rows and {@code OFFSET 0.5}
 * skips one, and it judges the sign after the rounding, so {@code LIMIT -0.4} is a limit of zero.
 *
 * <p><b>A subquery standing where one value is expected may have one column</b>, and that is a
 * property of its select list rather than of the rows it happens to return. The width was read off
 * the first row, so a wide subquery that returned nothing answered NULL, {@code ARRAY(SELECT 1, 2)}
 * came back as {@code {1}} with the second column dropped, and a query whose real fault was its
 * width was reported as returning more than one row.
 *
 * <p><b>The other half of the same rule ran the wrong way.</b> A row constructor compared against a
 * subquery reads the whole subquery row, so {@code (1, 2) = (SELECT 1, 2)} is a row comparison and
 * true; sending it down the scalar path refused it. Six operators take that reading. Measured
 * against PostgreSQL 18 rather than assumed: IS DISTINCT FROM does not, and neither does a subquery
 * written on the left of the comparison — both still refuse a wide subquery.
 *
 * <p><b>A bare constant in ORDER BY is an output-column position and nothing else</b>, in a
 * FROM-less SELECT as much as anywhere. That check existed but the FROM-less path never reached it.
 *
 * <p>The last nested class is the reason to prefer narrow rules to broad ones: every shape in it is
 * SQL PostgreSQL runs, and each new refusal here is one more way to refuse it.
 */
class SetOpSubqueryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE sos_t (a int, b text)");
        exec("INSERT INTO sos_t VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        exec("CREATE TABLE sos_p (x int, y int)");
        exec("INSERT INTO sos_p VALUES (1, 2)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** The single value of a one-row, one-column query, as text. */
    static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for " + sql);
            String value = rs.getString(1);
            assertFalse(rs.next(), "more than one row for " + sql);
            return value;
        }
    }

    /** Every row of a single-column query, joined in the order the query returned them. */
    static String column(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append(',');
                sb.append(rs.getString(1));
            }
        }
        return sb.toString();
    }

    static void assertFails(String state, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(state, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    @Nested
    class SetOpOrderByPosition {

        @Test
        void ordinalOutsideTheSelectListIsOutOfRange() {
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 UNION SELECT 2 ORDER BY 5");
            assertFails("42P10", "ORDER BY position 0 is not in select list",
                    "SELECT 1 UNION SELECT 2 ORDER BY 0");
            assertFails("42P10", "ORDER BY position -1 is not in select list",
                    "SELECT 1 UNION SELECT 2 ORDER BY -1");
        }

        @Test
        void everyOperationIsJudgedTheSameWay() {
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 INTERSECT SELECT 2 ORDER BY 5");
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 EXCEPT SELECT 2 ORDER BY 5");
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 INTERSECT ALL SELECT 2 ORDER BY 5");
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 EXCEPT ALL SELECT 2 ORDER BY 5");
        }

        @Test
        void aChainIsJudgedByTheOutputOfTheWholeChain() {
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 5");
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 UNION (SELECT 2 UNION SELECT 3) ORDER BY 5");
        }

        @Test
        void andItIsJudgedWhereverTheSetOperationStands() {
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "WITH c AS (SELECT 1 UNION SELECT 2 ORDER BY 5) SELECT * FROM c");
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT * FROM (SELECT 1 UNION SELECT 2 ORDER BY 5) s");
        }
    }

    @Nested
    class SetOpOrderByName {

        @Test
        void aNameTheOutputDoesNotAccountForIsAMissingColumn() {
            assertFails("42703", "column \"a\" does not exist",
                    "SELECT 1 UNION SELECT 2 ORDER BY a + 1");
            assertFails("42703", "column \"a\" does not exist",
                    "SELECT 1 EXCEPT SELECT 2 ORDER BY a + 1");
            assertFails("42703", "column \"b\" does not exist",
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY b");
        }

        @Test
        void theOutputNameComesFromTheFirstArm() {
            assertFails("42703", "column \"z\" does not exist",
                    "SELECT 1 UNION ALL SELECT 2 AS z ORDER BY z");
            assertFails("42703", "column \"a\" does not exist",
                    "SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY a");
        }

        @Test
        void aQualifiedNameHasNoFromEntryLeftToQualifyAgainst() {
            assertFails("42P01", "missing FROM-clause entry for table \"t\"",
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY t.a");
            assertFails("42P01", "missing FROM-clause entry for table \"sos_t\"",
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY sos_t.a");
        }

        @Test
        void aSubqueryInTheItemBringsItsOwnScopeAndIsNotSearchedForNames() {
            // The a inside resolves against sos_t; what is wrong is that the item is an
            // expression, not that a is missing.
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT 1 UNION SELECT 2 ORDER BY (SELECT max(a) FROM sos_t)");
        }
    }

    @Nested
    class SetOpOrderByExpression {

        @Test
        void anythingThatResolvesAndIsNotAPlainColumnNameIsAnExpression() {
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT 1 UNION SELECT 2 ORDER BY 5 - 4");
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT 1 UNION SELECT 2 ORDER BY 1 + 0");
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY upper('a')");
        }

        @Test
        void includingAnOutputColumnWithAnythingDoneToIt() {
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY a + 1");
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY -a");
        }

        @Test
        void aCastIsAnExpressionWhereTheBareLiteralWouldHaveBeenAConstant() {
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY 'x'::text");
        }

        @Test
        void collateMakesAnExpressionOfTheColumnItIsWrittenOn() {
            assertFails("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause",
                    "SELECT b FROM sos_t UNION ALL SELECT 'q' ORDER BY b COLLATE \"C\"");
        }

        @Test
        void andIsRefusedEarlierWhenTheThingCollatedHasNoCollationToGive() {
            assertFails("42804", "collations are not supported by type integer",
                    "SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY x COLLATE \"C\"");
            // COLLATE binds to the ordinal itself, so the type asked is integer whatever the
            // column at that position is.
            assertFails("42804", "collations are not supported by type integer",
                    "SELECT b FROM sos_t UNION ALL SELECT 'q' ORDER BY 1 COLLATE \"C\"");
        }
    }

    @Nested
    class BareConstantInOrderBy {

        @Test
        void aNonIntegerConstantAfterASetOperationNamesNoColumn() {
            assertFails("42601", "non-integer constant in ORDER BY",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY NULL");
            assertFails("42601", "non-integer constant in ORDER BY",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY 1.5");
            assertFails("42601", "non-integer constant in ORDER BY",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY true");
            assertFails("42601", "non-integer constant in ORDER BY",
                    "SELECT 1 AS q UNION SELECT 2 ORDER BY 'x'");
        }

        @Test
        void norInAFromLessSelect() {
            assertFails("42601", "non-integer constant in ORDER BY", "SELECT 1 AS a ORDER BY NULL");
            assertFails("42601", "non-integer constant in ORDER BY", "SELECT 1 AS a ORDER BY 'x'");
            assertFails("42601", "non-integer constant in ORDER BY", "SELECT 1 AS a ORDER BY 1.5");
            assertFails("42601", "non-integer constant in ORDER BY", "SELECT 1 AS a ORDER BY true");
        }

        @Test
        void parenthesesDoNotMakeAnExpressionOfAConstant() {
            assertFails("42601", "non-integer constant in ORDER BY", "SELECT 1 AS a ORDER BY (NULL)");
        }

        @Test
        void anIntegerConstantThereIsStillAPosition() {
            assertFails("42P10", "ORDER BY position 5 is not in select list",
                    "SELECT 1 AS a ORDER BY 5");
        }

        @Test
        void aSetReturningTargetExpandsToRowsAndTakesTheSameRule() {
            assertFails("42601", "non-integer constant in ORDER BY",
                    "SELECT generate_series(1, 3) AS g ORDER BY NULL");
        }
    }

    @Nested
    class SetOpLimitAndOffset {

        @Test
        void aNegativeOffsetRaisesInsteadOfReorderingTheRows() {
            assertFails("2201X", "OFFSET must not be negative",
                    "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 OFFSET -1");
        }

        @Test
        void aNegativeLimitRaisesInsteadOfFailingOnItsOwnSublistIndex() {
            assertFails("2201W", "LIMIT must not be negative",
                    "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT -1");
        }

        @Test
        void theTypeItIsReadAsIsBigint() {
            assertFails("22P02", "invalid input syntax for type bigint: \"x\"",
                    "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 OFFSET 'x'");
            assertFails("22P02", "invalid input syntax for type bigint: \"x\"",
                    "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 'x'");
        }

        @Test
        void aFractionalCountIsCastToBigintRatherThanTruncated() throws Exception {
            assertEquals("1,2", column("SELECT a FROM sos_t ORDER BY 1 LIMIT 1.5"));
            assertEquals("1", column("SELECT a FROM sos_t ORDER BY 1 LIMIT 0.5"));
            assertEquals("2,3", column("SELECT a FROM sos_t ORDER BY 1 OFFSET 0.5"));
            assertEquals("3", column("SELECT a FROM sos_t ORDER BY 1 OFFSET 1.5"));
            assertEquals("2,3", column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 OFFSET 1.5"));
        }

        @Test
        void andTheSignIsJudgedAfterTheRounding() throws Exception {
            assertEquals("", column("SELECT a FROM sos_t ORDER BY 1 LIMIT -0.4"));
        }
    }

    @Nested
    class ScalarSubqueryWidth {

        @Test
        void widthIsSettledWithoutLookingAtARow() {
            assertFails("42601", "subquery must return only one column",
                    "SELECT (SELECT 1, 2 WHERE false)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT (SELECT * FROM sos_t WHERE false)");
        }

        @Test
        void andBeforeTheRowCountIs() {
            assertFails("42601", "subquery must return only one column",
                    "SELECT (SELECT a, b FROM sos_t)");
        }

        @Test
        void arrayCollectsOneColumnSoASecondHasNowhereToGo() {
            assertFails("42601", "subquery must return only one column", "SELECT ARRAY(SELECT 1, 2)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT ARRAY(SELECT 1, 2 WHERE false)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT ARRAY(SELECT a, b FROM sos_t)");
        }

        @Test
        void inEveryPositionAScalarSubqueryCanStandIn() {
            assertFails("42601", "subquery must return only one column",
                    "SELECT * FROM sos_t WHERE a = (SELECT 1, 2 WHERE false)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT 1 + (SELECT 1, 2 WHERE false)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT abs((SELECT 1, 2 WHERE false))");
            assertFails("42601", "subquery must return only one column",
                    "SELECT CASE WHEN true THEN (SELECT 1, 2 WHERE false) ELSE 0 END");
            assertFails("42601", "subquery must return only one column",
                    "SELECT 1 IN ((SELECT 1, 2 WHERE false), 3)");
            assertFails("42601", "subquery must return only one column",
                    "SELECT (SELECT 1, 2 WHERE false) IS NULL");
        }

        @Test
        void aDefaultTakesNoSubqueryAtAllOfAnyWidth() {
            assertFails("0A000", "cannot use subquery in DEFAULT expression",
                    "CREATE TABLE sos_d (x int DEFAULT (SELECT 1, 2))");
        }
    }

    @Nested
    class RowComparisonAgainstSubquery {

        @Test
        void allSixOrderedComparisonsReadTheWholeRow() throws Exception {
            assertEquals("true", one("SELECT ((1, 2) = (SELECT 1, 2))::text"));
            assertEquals("true", one("SELECT ((1, 2) <> (SELECT 1, 3))::text"));
            assertEquals("true", one("SELECT ((1, 2) < (SELECT 1, 3))::text"));
            assertEquals("true", one("SELECT ((1, 2) <= (SELECT 1, 2))::text"));
            assertEquals("true", one("SELECT ((1, 2) >= (SELECT 1, 2))::text"));
            assertEquals("true", one("SELECT ((1, 2) > (SELECT 1, 1))::text"));
        }

        @Test
        void rowWrittenOutIsTheSameConstructor() throws Exception {
            assertEquals("true", one("SELECT (ROW(1, 2) = (SELECT 1, 2))::text"));
        }

        @Test
        void aRowOfOneIsARowToo() throws Exception {
            assertEquals("true", one("SELECT (ROW(1) = (SELECT 1))::text"));
            assertEquals("true", one("SELECT (ROW('x') = (SELECT b FROM sos_t WHERE a = 1))::text"));
        }

        @Test
        void theEntriesMayComeFromTheQuerysOwnRowsOnEitherSide() throws Exception {
            assertEquals("true", one("SELECT ((x, y) = (SELECT 1, 2))::text FROM sos_p"));
            assertEquals("true", one("SELECT ((1, 'x') = (SELECT a, b FROM sos_t WHERE a = 1))::text"));
        }

        @Test
        void itIsABooleanLikeAnyOther() throws Exception {
            assertEquals("1", one("SELECT 1 WHERE (1, 2) = (SELECT 1, 2)"));
            assertEquals("true", one("SELECT (NOT ((1, 2) = (SELECT 1, 3)))::text"));
        }

        @Test
        void noRowMakesTheComparisonUnknownRatherThanFalse() throws Exception {
            assertNull(one("SELECT ((1, 2) = (SELECT 1, 2 LIMIT 0))::text"));
        }

        @Test
        void theWidthsHaveToAgreeAndThatIsSettledBeforeAnyRowIsRead() {
            assertFails("42601", "subquery has too few columns", "SELECT ((1, 2) = (SELECT 1))::text");
            assertFails("42601", "subquery has too few columns",
                    "SELECT ((1, 2, 3) = (SELECT 1, 2))::text");
            assertFails("42601", "subquery has too few columns",
                    "SELECT ((1, 2, 3) = (SELECT x, y FROM sos_p WHERE false))::text");
            assertFails("42601", "subquery has too many columns",
                    "SELECT ((1, 2) = (SELECT 1, 2, 3))::text");
        }

        @Test
        void entryByEntryTheComparisonHasToHaveAnOperator() {
            assertFails("42883", "operator does not exist: integer = text",
                    "SELECT ((1, 2) = (SELECT a, b FROM sos_t WHERE a = 1))::text");
            assertFails("42883", "operator does not exist: integer = text",
                    "SELECT ((1, 2) = (SELECT a, b FROM sos_t WHERE false))::text");
        }

        @Test
        void whenTheWidthsAgreeWhatIsLeftIsTheRowCount() {
            assertFails("21000", "more than one row returned by a subquery used as an expression",
                    "SELECT ((1, 2) = (SELECT a, a FROM sos_t))::text");
        }

        @Test
        void isDistinctFromDoesNotTakeTheRowReading() {
            assertFails("42601", "subquery must return only one column",
                    "SELECT ((1, 2) IS DISTINCT FROM (SELECT 1, 2))::text");
            assertFails("42601", "subquery must return only one column",
                    "SELECT ((1, 2) IS NOT DISTINCT FROM (SELECT 1, 2))::text");
        }

        @Test
        void norDoesASubqueryWrittenOnTheLeft() {
            assertFails("42601", "subquery must return only one column",
                    "SELECT ((SELECT 1, 2) = (1, 2))::text");
        }
    }

    @Nested
    class OrdinarySql {

        @Test
        void anOutputColumnNameAndItsPositionOrderedEitherWay() throws Exception {
            assertEquals("1,2,2,3",
                    column("SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY x"));
            assertEquals("1,2,2,3",
                    column("SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY 1"));
            assertEquals("3,2,2,1",
                    column("SELECT a AS x FROM sos_t UNION ALL SELECT 2 ORDER BY 1 DESC"));
            assertEquals("null,1,2,3",
                    column("SELECT a AS x FROM sos_t UNION ALL SELECT NULL ORDER BY 1 NULLS FIRST"));
            assertEquals("3,2,1,null",
                    column("SELECT a AS x FROM sos_t UNION ALL SELECT NULL ORDER BY x DESC NULLS LAST"));
        }

        @Test
        void aQuotedOutputNameAndOneThatCameFromAnAggregate() throws Exception {
            assertEquals("1,2,2,3", column(
                    "SELECT a AS \"Weird Name\" FROM sos_t UNION ALL SELECT 2 ORDER BY \"Weird Name\""));
            assertEquals("2,6",
                    column("SELECT sum(a) AS s FROM sos_t UNION ALL SELECT 2 ORDER BY s"));
        }

        @Test
        void severalItemsByNameAndByPosition() throws Exception {
            assertEquals("2,1,2,3",
                    column("SELECT a, b FROM sos_t UNION ALL SELECT 2, 'q' ORDER BY b, a"));
            assertEquals("2,1,2,3",
                    column("SELECT a, b FROM sos_t UNION ALL SELECT 2, 'q' ORDER BY 2, 1"));
        }

        @Test
        void usingNamesTheOperatorToSortBy() throws Exception {
            assertEquals("3,2,2,1",
                    column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 USING >"));
            assertEquals("1,2,2,3",
                    column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY a USING <"));
        }

        @Test
        void theOtherOperationsAndAValuesArm() throws Exception {
            assertEquals("1,3", column("SELECT a FROM sos_t EXCEPT SELECT 2 ORDER BY a"));
            assertEquals("2", column("SELECT a FROM sos_t INTERSECT SELECT 2 ORDER BY a DESC"));
            assertEquals("1,2,3", column("VALUES (1), (2) UNION SELECT 3 ORDER BY 1"));
        }

        @Test
        void chainsParenthesisedOrNot() throws Exception {
            assertEquals("1,2,2,3,4",
                    column("SELECT a FROM sos_t UNION ALL (SELECT 2 UNION ALL SELECT 4) ORDER BY a"));
            assertEquals("1,2,2,3,9",
                    column("(SELECT a FROM sos_t UNION ALL SELECT 2) UNION ALL SELECT 9 ORDER BY a"));
        }

        @Test
        void withLimitAndOffsetAfterItInEitherSpelling() throws Exception {
            assertEquals("2,2",
                    column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 LIMIT 2 OFFSET 1"));
            assertEquals("1,2", column(
                    "SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 FETCH FIRST 2 ROWS ONLY"));
            assertEquals("1,2,2,3",
                    column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 LIMIT ALL"));
            assertEquals("1,2,2,3",
                    column("SELECT a FROM sos_t UNION ALL SELECT 2 ORDER BY 1 OFFSET NULL"));
        }

        @Test
        void insideACteAndInsideASubSelect() throws Exception {
            assertEquals("2,3", column("WITH c AS (SELECT a FROM sos_t UNION ALL SELECT 2"
                    + " ORDER BY a DESC LIMIT 2) SELECT * FROM c ORDER BY 1"));
            assertEquals("1,2", column("SELECT * FROM (SELECT a FROM sos_t UNION ALL SELECT 2"
                    + " ORDER BY a LIMIT 2) s ORDER BY 1"));
        }

        @Test
        void aFromLessSelectOrderedByAPositionANameAndAnExpression() throws Exception {
            assertEquals("1", one("SELECT 1 AS a ORDER BY 1"));
            assertEquals("1", one("SELECT 1 AS a ORDER BY a DESC"));
            assertEquals("1", one("SELECT 1 AS a ORDER BY 1 + 0"));
            assertEquals("1", one("SELECT 1 AS a ORDER BY 2 - 1"));
            assertEquals("1", one("SELECT 1 AS a ORDER BY random()"));
            assertEquals("1", one("SELECT 1 AS a, 2 AS b ORDER BY 2"));
            assertEquals("3,2,1", column("SELECT generate_series(1, 3) AS g ORDER BY 1 DESC"));
        }

        @Test
        void aConstantThatIsCastIsAnExpressionAndNotAConstant() throws Exception {
            assertEquals("1", one("SELECT 1 AS a ORDER BY NULL::int"));
            assertEquals("1", one("SELECT 1 AS a ORDER BY 'x'::text"));
        }

        @Test
        void oneColumnScalarSubqueriesTheWidthRuleMustNotTouch() throws Exception {
            assertEquals("1", one("SELECT (SELECT 1) AS ok"));
            assertEquals("3", one("SELECT (SELECT max(a) FROM sos_t)"));
            assertEquals("{1,2,3}", one("SELECT ARRAY(SELECT a FROM sos_t ORDER BY a)"));
            assertEquals("{}", one("SELECT ARRAY(SELECT a FROM sos_t WHERE false)"));
        }

        @Test
        void aOneColumnSubqueryOfManyRowsIsStillARowCountError() {
            assertFails("21000", "more than one row returned by a subquery used as an expression",
                    "SELECT (SELECT a FROM sos_t)");
        }

        @Test
        void inOverATwoColumnSubqueryIsARowComparisonOfItsOwn() throws Exception {
            assertEquals("true", one("SELECT ((1, 2) IN (SELECT 1, 2))::text"));
            assertEquals("false", one("SELECT ((1, 2) IN (SELECT a, a FROM sos_t))::text"));
        }

        @Test
        void aSetOperationIsAnInsertSourceLikeAnyOther() throws Exception {
            exec("CREATE TABLE sos_ins (x int, y int)");
            exec("INSERT INTO sos_ins SELECT 5, 6 UNION ALL SELECT 7, 8 ORDER BY 1");
            assertEquals("5,7", column("SELECT x FROM sos_ins ORDER BY 1"));
            exec("DROP TABLE sos_ins");
        }
    }
}
