package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a join's names resolve to, and which joins may be asked at all.
 *
 * <p><b>An outer join names the side that contributed nothing.</b> The whole purpose of
 * {@code t1 RIGHT JOIN t2} is to answer with NULLs where {@code t1} has no match, and those rows
 * still have to answer to {@code t1}'s name. The shape of a side was taken from its first row,
 * which works only while it has one; when the side was empty the padded rows carried no binding
 * for it and every reference to its alias was {@code 42P01 missing FROM-clause entry}. Measured
 * rather than assumed: the two queries the finding gives already worked, because their left side
 * had rows — the failure needs a side that produces none, from an empty relation, a subquery or
 * view that filters everything away, or a WITH query.
 *
 * <p><b>A FULL JOIN may only be asked what PostgreSQL can answer.</b> It has no nested-loop plan
 * for one: it must merge or hash the two sides, and both need an equality between one side's value
 * and the other's, so anything else is {@code 0A000 FULL JOIN is only supported with merge-joinable
 * or hash-joinable join conditions}. What it accepts was measured across forty conditions rather
 * than guessed, and the shape of the rule follows from them: one AND-ed clause being a cross-side
 * equality is enough however unmergeable the rest are, a condition that folds to a constant is
 * always fine, and a WHERE that rejects the rows a side was padded with lifts the restriction
 * because the join is then planned as an inner one. PostgreSQL normalises the condition before it
 * judges it and only a strict qual above the join downgrades it; both are measured in
 * {@link FullJoinAdmissibilityTest}, which this class leaves to it.
 *
 * <p><b>A NATURAL join merges the columns both sides share</b>, exactly as a USING clause naming
 * them would. Only the USING half was known to the ambiguity check, so a reference to a merged
 * column after a NATURAL join was {@code 42702 ambiguous}. The merges are counted rather than
 * remembered: {@code t JOIN u USING (s) JOIN v ON true} is still ambiguous, because the third
 * relation's column was never merged into anything.
 *
 * <p><b>A schema may be written in front of a column's relation.</b> {@code public.t.c} resolves
 * against an unaliased {@code FROM t} — but only when the schema really holds that relation: a WITH
 * query, a subquery alias and a relation of another schema are all
 * {@code invalid reference to FROM-clause entry}, which is what PostgreSQL says of them.
 *
 * <p><b>A duplicate name is a duplicate name wherever it is written</b>, including in a view body,
 * which the view path swallowed. The one exemption SQL grants — two relations of the same name
 * from different schemas, both written without an alias — has to survive it, and is measured here
 * as well as the refusal.
 */
class JoinResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE jr_t1 (i int, s text)");
        exec("CREATE TABLE jr_t2 (j int, s text)");
        exec("CREATE TABLE jr_t3 (k int, s text)");
        exec("CREATE TABLE jr_e1 (i int)");
        exec("CREATE TABLE jr_e2 (j int)");
        exec("CREATE TABLE jr_m1 (i int, s text)");
        exec("CREATE TABLE jr_m2 (j int, s text)");
        exec("INSERT INTO jr_t1 VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("INSERT INTO jr_t2 VALUES (2,'x'),(3,'y'),(4,'z')");
        exec("INSERT INTO jr_t3 VALUES (3,'p'),(5,'q')");
        exec("INSERT INTO jr_m1 VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO jr_m2 VALUES (5,'a'),(6,'c')");
        exec("CREATE VIEW jr_empty_v AS SELECT i, s FROM jr_t1 WHERE false");
        exec("CREATE VIEW jr_v AS SELECT i, s FROM jr_t1");
        exec("CREATE SCHEMA jr_s");
        exec("CREATE TABLE jr_s.jr_t1 (i int, z text)");
        exec("INSERT INTO jr_s.jr_t1 VALUES (7,'s')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per entry, columns joined by '|', in the order the query answered. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
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

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    private static final String UNMERGEABLE =
            "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions";

    // ---- 1: an outer join names the side that contributed nothing ----

    @Nested
    class OuterJoinAliases {

        @Test
        void anEmptyLeftSideStillAnswersToItsAlias() throws SQLException {
            assertEquals("-|2,-|3,-|4",
                    rows("SELECT a.i, b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY b.j"));
        }

        @Test
        void aFullJoinPadsBothWays() throws SQLException {
            assertEquals("2|2,3|3,-|4,1|-",
                    rows("SELECT a.i, b.j FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j"
                            + " ORDER BY b.j, a.i"));
        }

        @Test
        void aConditionThatIsNeverTrueLeavesEverySideNull() throws SQLException {
            assertEquals("true|2,true|3,true|4",
                    rows("SELECT (a.i IS NULL)::text, b.j FROM jr_t1 a RIGHT JOIN jr_t2 b ON false"
                            + " ORDER BY b.j"));
        }

        @Test
        void bothSidesEmptyIsNoRowsRatherThanAnError() throws SQLException {
            assertEquals("", rows("SELECT a.i, b.j FROM jr_e1 a FULL JOIN jr_e2 b ON a.i = b.j"));
        }

        @Test
        void aChainOfOuterJoinsKeepsEveryAlias() throws SQLException {
            assertEquals("-|3|3,-|-|5",
                    rows("SELECT a.i, b.j, c.k FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j"
                            + " RIGHT JOIN jr_t3 c ON b.j = c.k ORDER BY 1,2,3"));
        }

        @Test
        void anEmptySubqueryStillAnswersToItsAlias() throws SQLException {
            assertEquals("-|2,-|3,-|4",
                    rows("SELECT a.i, b.j FROM (SELECT i FROM jr_t1 WHERE false) a"
                            + " FULL JOIN jr_t2 b ON a.i = b.j ORDER BY b.j"));
        }

        @Test
        void anEmptyViewStillAnswersToItsAlias() throws SQLException {
            assertEquals("-|2,-|3,-|4",
                    rows("SELECT a.i, b.j FROM jr_empty_v a FULL JOIN jr_t2 b ON a.i = b.j"
                            + " ORDER BY b.j"));
        }

        @Test
        void anEmptyWithQueryStillAnswersToItsName() throws SQLException {
            assertEquals("-|2,-|3,-|4",
                    rows("WITH c AS (SELECT i FROM jr_t1 WHERE false)"
                            + " SELECT c.i, b.j FROM c FULL JOIN jr_t2 b ON c.i = b.j ORDER BY b.j"));
        }

        @Test
        void aLateralOverAnEmptySideIsDescribableAndEmpty() throws SQLException {
            assertEquals("", rows("SELECT a.i, b.j FROM jr_e1 a"
                    + " LEFT JOIN LATERAL (SELECT j FROM jr_t2 WHERE j = a.i) b ON true ORDER BY 1"));
        }

        @Test
        void aSelfJoinOfAnEmptyRelationIsEmpty() throws SQLException {
            assertEquals("", rows("SELECT a.i, b.i FROM jr_e1 a FULL JOIN jr_e1 b ON a.i = b.i"));
        }

        @Test
        void theAliasResolvesInWhere() throws SQLException {
            assertEquals("2,3,4", rows("SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j"
                    + " WHERE a.i IS NULL ORDER BY b.j"));
        }

        @Test
        void theAliasResolvesInGroupBy() throws SQLException {
            assertEquals("-|3", rows("SELECT a.i, count(*) FROM jr_e1 a"
                    + " RIGHT JOIN jr_t2 b ON a.i = b.j GROUP BY a.i ORDER BY 1"));
        }

        @Test
        void theAliasResolvesInHaving() throws SQLException {
            assertEquals("2,3,4", rows("SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j"
                    + " GROUP BY b.j HAVING count(a.i) = 0 ORDER BY 1"));
        }

        @Test
        void theAliasResolvesInOrderByAlone() throws SQLException {
            assertEquals("2,3,4", rows("SELECT b.j FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j"
                    + " ORDER BY a.i, b.j"));
        }

        @Test
        void theAliasResolvesInsideASubquery() throws SQLException {
            assertEquals("2|0,3|0,4|0",
                    rows("SELECT b.j, (SELECT count(*) FROM jr_t3 c WHERE c.k = a.i)"
                            + " FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY b.j"));
        }

        @Test
        void theAliasResolvesInAWindowPartition() throws SQLException {
            assertEquals("2|3,3|3,4|3",
                    rows("SELECT b.j, count(*) OVER (PARTITION BY a.i)"
                            + " FROM jr_e1 a RIGHT JOIN jr_t2 b ON a.i = b.j ORDER BY b.j"));
        }

        @Test
        void anAliasThatIsNotInTheJoinIsStillMissing() {
            assertRejected("SELECT c.i FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i = b.j",
                    "42P01", "missing FROM-clause entry for table \"c\"");
        }

        @Test
        void aColumnThatTheAliasedRelationHasNotIsStillMissing() {
            assertRejected("SELECT a.nosuch FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i = b.j",
                    "42703", "column a.nosuch does not exist");
        }
    }

    // ---- 2: which conditions a FULL JOIN may carry ----

    @Nested
    class FullJoinConditions {

        @Test
        void anInequalityIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i < b.j",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void anOrOfEqualitiesIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i = b.j OR a.s = b.s", "0A000", UNMERGEABLE);
        }

        @Test
        void aConditionNamingOneSideOnlyIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = 1",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void isNotDistinctFromIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i IS NOT DISTINCT FROM b.j", "0A000", UNMERGEABLE);
        }

        @Test
        void anInequalityOperatorIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i <> b.j",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void aNegatedEqualityIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON NOT (a.i = b.j)",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void anEqualityWrappedInABooleanTestIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON (a.i = b.j) IS TRUE",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void anEqualityReadingBothSidesOnOneOfItsOwnIsRefused() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON (a.i + b.j) = 5",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void aConstantTrueSurvivingAnAndDoesNotSaveTheClauseBesideIt() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON 1=1 AND a.i < b.j",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void aSecondFullJoinIsJudgedOnItsOwn() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j"
                    + " FULL JOIN jr_t3 c ON b.j < c.k", "0A000", UNMERGEABLE);
        }

        @Test
        void aWithQuerySideIsJudgedTheSameWay() {
            assertRejected("WITH q AS (SELECT i FROM jr_t1)"
                    + " SELECT count(*) FROM q FULL JOIN jr_t2 b ON q.i < b.j", "0A000", UNMERGEABLE);
        }

        @Test
        void theRefusalComesBeforeAnyRowIsRead() {
            assertRejected("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i < b.j LIMIT 0",
                    "0A000", UNMERGEABLE);
        }

        @Test
        void aPlainEqualityIsAccepted() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON a.i = b.j"));
        }

        @Test
        void anEqualityBetweenExpressionsIsAccepted() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON coalesce(a.i,0) = coalesce(b.j,0)"));
        }

        @Test
        void oneEqualityCarriesAnyNumberOfUnmergeableClausesBesideIt() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i = b.j AND a.i > 1 AND a.s < b.s"));
        }

        @Test
        void aConditionThatFoldsToTrueIsAccepted() throws SQLException {
            assertEquals("9", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b ON 1=1"));
        }

        @Test
        void aConditionThatFoldsToFalseIsAccepted() throws SQLException {
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON false AND a.i < b.j"));
        }

        @Test
        void aNullConditionIsAccepted() throws SQLException {
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON NULL AND a.i < b.j"));
        }

        @Test
        void anAlternativeThatIsAlwaysTrueIsAccepted() throws SQLException {
            assertEquals("9", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i < b.j OR true"));
        }

        @Test
        void anAlternativeThatNeverHoldsDropsOutOfTheCondition() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i = b.j OR false"));
            assertEquals("4", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i = b.j OR NULL"));
        }

        @Test
        void usingAndNaturalJoinOnEqualityAndAreAccepted() throws SQLException {
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b USING (s)"));
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a NATURAL FULL JOIN jr_t2 b"));
        }

        @Test
        void aWhereThatReadsEitherSideMakesTheJoinAnInnerOne() throws SQLException {
            assertEquals("0", rows("SELECT count(*) FROM jr_t1 a FULL JOIN jr_t2 b"
                    + " ON a.i > b.j WHERE a.i = b.j"));
        }

        @Test
        void onlyAFullJoinIsRestricted() throws SQLException {
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a LEFT JOIN jr_t2 b ON a.i < b.j"));
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a RIGHT JOIN jr_t2 b ON a.i < b.j"));
            assertEquals("6", rows("SELECT count(*) FROM jr_t1 a JOIN jr_t2 b ON a.i < b.j"));
        }

        @Test
        void anAmbiguousNameInTheConditionIsReportedAsAmbiguous() {
            assertRejected("SELECT count(*) FROM jr_t1 FULL JOIN jr_t2 ON s = s",
                    "42702", "column reference \"s\" is ambiguous");
        }
    }

    // ---- 3: a NATURAL join merges the columns both sides share ----

    @Nested
    class NaturalJoinMerging {

        @Test
        void aMergedColumnIsOneColumn() throws SQLException {
            assertEquals("a", rows("SELECT s FROM jr_m1 NATURAL JOIN jr_m2 ORDER BY 1"));
        }

        @Test
        void aMergedColumnResolvesInEveryClause() throws SQLException {
            assertEquals("a", rows("SELECT s FROM jr_m1 NATURAL JOIN jr_m2 WHERE s = 'a'"));
            assertEquals("a", rows("SELECT s FROM jr_m1 NATURAL JOIN jr_m2 GROUP BY s ORDER BY 1"));
            assertEquals("a", rows("SELECT s FROM jr_m1 NATURAL JOIN jr_m2 ORDER BY s"));
        }

        @Test
        void aMergedColumnResolvesWhenTheJoinAnswersWithNoRows() throws SQLException {
            assertEquals("", rows("SELECT s FROM jr_t1 NATURAL JOIN jr_t2"));
            assertEquals("", rows("SELECT s FROM jr_t1 NATURAL JOIN jr_t2 WHERE s = 'x'"));
        }

        @Test
        void everyOuterFlavourMergesTheSameWay() throws SQLException {
            assertEquals("a,b,c", rows("SELECT s FROM jr_m1 NATURAL FULL JOIN jr_m2 ORDER BY 1"));
            assertEquals("a,b", rows("SELECT s FROM jr_m1 NATURAL LEFT JOIN jr_m2 ORDER BY 1"));
            assertEquals("a,c", rows("SELECT s FROM jr_m1 NATURAL RIGHT JOIN jr_m2 ORDER BY 1"));
        }

        @Test
        void chainedNaturalJoinsMergeOnceEach() throws SQLException {
            assertEquals("", rows("SELECT s FROM jr_t1 NATURAL JOIN jr_t2 NATURAL JOIN jr_t3"));
        }

        @Test
        void aMergedColumnIsStillReadableThroughEitherRelation() throws SQLException {
            assertEquals("a", rows("SELECT jr_m2.s FROM jr_m1 NATURAL JOIN jr_m2"));
            assertEquals("a|a", rows("SELECT jr_m1.s, jr_m2.s FROM jr_m1 NATURAL JOIN jr_m2"));
            assertEquals("", rows("SELECT jr_t2.s FROM jr_t1 JOIN jr_t2 USING (s)"));
        }

        @Test
        void aRelationJoinedOnAfterwardsMakesTheNameAmbiguousAgain() {
            assertRejected("SELECT s FROM jr_t1 NATURAL JOIN jr_t2 JOIN jr_t3 ON true",
                    "42702", "column reference \"s\" is ambiguous");
        }

        @Test
        void anUnmergedSharedNameIsStillAmbiguous() {
            assertRejected("SELECT s FROM jr_t1 JOIN jr_t2 ON jr_t1.i = jr_t2.j",
                    "42702", "column reference \"s\" is ambiguous");
            assertRejected("SELECT s FROM jr_t1, jr_t2", "42702", "column reference \"s\" is ambiguous");
            assertRejected("SELECT s FROM jr_t1 CROSS JOIN jr_t2",
                    "42702", "column reference \"s\" is ambiguous");
        }
    }

    // ---- 4: a schema written in front of a column's relation ----

    @Nested
    class SchemaQualifiedColumns {

        @Test
        void aSchemaReachesTheRelationItHolds() throws SQLException {
            assertEquals("1,2,3", rows("SELECT public.jr_t1.i FROM jr_t1 ORDER BY 1"));
            assertEquals("1,2,3", rows("SELECT public.jr_t1.i FROM public.jr_t1 ORDER BY 1"));
            assertEquals("7", rows("SELECT jr_s.jr_t1.i FROM jr_s.jr_t1"));
        }

        @Test
        void itResolvesInEveryClauseAndUnderAnAggregate() throws SQLException {
            assertEquals("2,3", rows("SELECT public.jr_t1.i FROM jr_t1"
                    + " WHERE public.jr_t1.i > 1 ORDER BY public.jr_t1.i"));
            assertEquals("6", rows("SELECT sum(public.jr_t1.i) FROM jr_t1"));
            assertEquals("2,3", rows("SELECT public.jr_t1.i FROM jr_t1"
                    + " GROUP BY public.jr_t1.i HAVING public.jr_t1.i > 1 ORDER BY 1"));
        }

        @Test
        void itResolvesInAJoinCondition() throws SQLException {
            assertEquals("2,3", rows("SELECT public.jr_t1.i FROM jr_t1 JOIN jr_t2"
                    + " ON public.jr_t1.i = jr_t2.j ORDER BY 1"));
        }

        @Test
        void aViewIsAlsoARelationASchemaHolds() throws SQLException {
            assertEquals("1,2,3", rows("SELECT public.jr_v.i FROM jr_v ORDER BY 1"));
        }

        @Test
        void twoSchemasHoldingOneNameAreEachReachable() throws SQLException {
            assertEquals("1|7,2|7,3|7", rows("SELECT public.jr_t1.i, jr_s.jr_t1.i"
                    + " FROM jr_t1, jr_s.jr_t1 ORDER BY 1"));
        }

        @Test
        void aStarMayBeSchemaQualifiedToo() throws SQLException {
            assertEquals("1|a,2|b,3|c", rows("SELECT public.jr_t1.* FROM jr_t1 ORDER BY 1"));
        }

        @Test
        void theWrongSchemaIsAnInvalidReferenceRatherThanAMissingOne() {
            assertRejected("SELECT public.jr_t1.i FROM jr_s.jr_t1",
                    "42P01", "invalid reference to FROM-clause entry for table \"jr_t1\"");
            assertRejected("SELECT jr_s.jr_v.i FROM jr_v",
                    "42P01", "invalid reference to FROM-clause entry for table \"jr_v\"");
            assertRejected("SELECT nosuch.jr_t1.i FROM jr_t1",
                    "42P01", "invalid reference to FROM-clause entry for table \"jr_t1\"");
        }

        @Test
        void aWithQueryIsNotReachableThroughASchema() {
            assertRejected("WITH jr_t1 AS (SELECT 9 AS i) SELECT public.jr_t1.i FROM jr_t1",
                    "42P01", "invalid reference to FROM-clause entry for table \"jr_t1\"");
        }

        @Test
        void aSubqueryAliasIsNotReachableThroughASchema() {
            assertRejected("SELECT public.s.i FROM (SELECT 1 i) s",
                    "42P01", "invalid reference to FROM-clause entry for table \"s\"");
        }

        @Test
        void anAliasStillHidesTheRelationName() {
            assertRejected("SELECT public.jr_t1.i FROM jr_t1 a",
                    "42P01", "invalid reference to FROM-clause entry for table \"jr_t1\"");
        }

        @Test
        void aNameNoFromEntryAnswersToIsStillMissing() {
            assertRejected("SELECT public.jr_nosuch.i FROM jr_t1",
                    "42P01", "missing FROM-clause entry for table \"jr_nosuch\"");
        }
    }

    // ---- 5: a duplicate name is a duplicate name wherever it is written ----

    @Nested
    class DuplicateFromNames {

        @Test
        void aViewBodyIsCheckedForThemToo() {
            assertRejected("CREATE VIEW jr_dupv AS SELECT 1 AS z FROM jr_t1 x JOIN jr_t2 x ON true",
                    "42712", "table name \"x\" specified more than once");
        }

        @Test
        void aPlainSelectIsCheckedForThem() {
            assertRejected("SELECT * FROM jr_t1 x JOIN jr_t2 x ON true",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT 1 AS c FROM jr_t1, jr_t1",
                    "42712", "table name \"jr_t1\" specified more than once");
            assertRejected("SELECT 1 AS c FROM jr_t1, (SELECT 1) jr_t1",
                    "42712", "table name \"jr_t1\" specified more than once");
        }

        @Test
        void twoUnaliasedRelationsOfOneNameFromDifferentSchemasAreAllowed() {
            assertAccepted("SELECT 1 AS c FROM public.jr_t1, jr_s.jr_t1");
            assertAccepted("SELECT 1 AS c FROM jr_t1 JOIN jr_s.jr_t1 ON true");
            assertAccepted("CREATE VIEW jr_okv AS SELECT 1 AS z FROM public.jr_t1, jr_s.jr_t1");
            assertAccepted("DROP VIEW jr_okv");
        }

        @Test
        void anAliasOnEitherOfThemBringsTheClashBack() {
            assertRejected("SELECT 1 AS c FROM public.jr_t1 q, jr_s.jr_t1 q",
                    "42712", "table name \"q\" specified more than once");
        }
    }
}
