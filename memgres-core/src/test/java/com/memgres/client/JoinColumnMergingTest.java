package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How the columns a join merges are matched, ordered and named.
 *
 * <p><b>A chain of USING or NATURAL joins was a cross product.</b> A join's USING column was
 * looked for by scanning the whole row for two relations holding a column of that name, which is
 * right for one join and wrong for every one after it: {@code a JOIN b USING (id) JOIN c USING
 * (id)} found {@code a.id} and {@code b.id}, compared them — the first join had already made them
 * equal — and never looked at {@code c} at all, so the third relation was cross-joined in
 * silently. A join's sides are now described by the columns they <em>expose</em>, so the second
 * clause equates the first join's merged {@code id} with {@code c}'s, and a column an earlier
 * join merged answers with whichever relation behind it is not null.
 *
 * <p><b>SELECT * lists the merged columns first.</b> PostgreSQL's join output is every merged
 * column, in the order USING names them or a NATURAL join finds them, then what is left of the
 * left side and then of the right side. {@code u1(id,s,p) JOIN u2(id,s,q) USING (s)} is therefore
 * {@code s, id, p, id, q}, not the left side with a column crossed out — and the third relation of
 * a chain keeps its own {@code id}, which a merge two joins earlier used to swallow.
 *
 * <p><b>A name a USING clause gives must be one column of each side.</b> Two of them on the left
 * is {@code 42702 common column name "x" appears more than once in left table} — which is what
 * {@code (a JOIN b ON true) JOIN c USING (id)} writes — and none is {@code 42703 column "x"
 * specified in USING clause does not exist in left table}, naming the side that actually lacks it.
 * The name is matched as written, so {@code USING ("ID")} does not find a column named {@code id}.
 *
 * <p><b>A function in FROM is not always the right side of a lateral join.</b> Running it once per
 * left row answers an INNER or LEFT join on a condition and nothing else: it has nowhere to put
 * the rows the right side kept to itself, so a FULL JOIN against one dropped every unmatched row
 * on both sides, and it never saw the columns a USING or NATURAL clause named.
 *
 * <p><b>USING will not equate a number with a string.</b> PostgreSQL has no such {@code =}
 * operator and refuses all twenty-four combinations of its six numeric and four string types;
 * memgres compared them as text and joined whatever happened to spell the same. Only that pair is
 * refused, and only between declared columns — a relation a function produced carries no type
 * worth judging — because refusing valid SQL costs more than the permissiveness left behind.
 *
 * <p>The last section is ordinary SQL that has to keep working, measured against PostgreSQL 18
 * alongside everything above.
 */
class JoinColumnMergingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE jcm_a (id int, av text)");
        exec("CREATE TABLE jcm_b (id int, bv text)");
        exec("CREATE TABLE jcm_c (id int, cv text)");
        exec("CREATE TABLE jcm_d (id int, dv text)");
        exec("CREATE TABLE jcm_e (id int, ev text)");
        exec("CREATE TABLE jcm_f (bv text, id int)");
        exec("CREATE TABLE jcm_u1 (id int, s int, p text)");
        exec("CREATE TABLE jcm_u2 (id int, s int, q text)");
        exec("CREATE TABLE jcm_u3 (id int, s int, r text)");
        exec("CREATE TABLE jcm_u4 (id text, w int)");
        exec("CREATE TABLE jcm_emp (id int, name text, dept_id int, salary int)");
        exec("CREATE TABLE jcm_dpt (id int, name text, budget int)");
        exec("INSERT INTO jcm_a VALUES (1,'a1'),(2,'a2'),(3,'a3')");
        exec("INSERT INTO jcm_b VALUES (1,'b1'),(2,'b2'),(4,'b4')");
        exec("INSERT INTO jcm_c VALUES (1,'c1'),(3,'c3'),(4,'c4')");
        exec("INSERT INTO jcm_d VALUES (1,'d1'),(5,'d5')");
        exec("INSERT INTO jcm_f VALUES ('b1',1),('bz',9)");
        exec("INSERT INTO jcm_u1 VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO jcm_u2 VALUES (1,10,'x'),(2,99,'y'),(4,40,'z')");
        exec("INSERT INTO jcm_u3 VALUES (1,10,'p'),(3,30,'q')");
        exec("INSERT INTO jcm_u4 VALUES ('1',5)");
        exec("INSERT INTO jcm_emp VALUES (1,'ann',10,100),(2,'bob',20,200),(3,'cy',30,300)");
        exec("INSERT INTO jcm_dpt VALUES (1,'ann',999),(2,'zed',888)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- chains of USING and NATURAL joins ----

    @Nested
    class ChainedMerges {

        @Test
        void threeRelationsJoinedOnTheSameNameKeepJoining() throws Exception {
            assertEquals("1", rows("SELECT count(*) FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c USING (id)"));
            assertEquals("1|a1|b1|c1", rows("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c USING (id)"));
        }

        @Test
        void fourRelationsToo() throws Exception {
            assertEquals("1|a1|b1|c1|d1", rows("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c USING (id) JOIN jcm_d USING (id)"));
            assertEquals("1", rows("SELECT count(*) FROM jcm_a a JOIN jcm_b b USING (id)"
                    + " JOIN jcm_c c USING (id) JOIN jcm_d d USING (id)"));
        }

        @Test
        void naturalChainsTheSameWay() throws Exception {
            assertEquals("1", rows("SELECT count(*) FROM jcm_u1 NATURAL JOIN jcm_u2"
                    + " NATURAL JOIN jcm_u3"));
            assertEquals("1|10|a|x|p", rows("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2"
                    + " NATURAL JOIN jcm_u3"));
            assertEquals("1|a1|b1|c1|d1", rows("SELECT * FROM jcm_a NATURAL JOIN jcm_b"
                    + " NATURAL JOIN jcm_c NATURAL JOIN jcm_d"));
        }

        @Test
        void anOuterChainMatchesOnWhicheverSideIsNotNull() throws Exception {
            assertEquals("1|a1|b1|c1,2|a2|b2|-,3|a3|-|c3",
                    rows("SELECT * FROM jcm_a LEFT JOIN jcm_b USING (id)"
                            + " LEFT JOIN jcm_c USING (id) ORDER BY 1"));
            assertEquals("3", rows("SELECT count(*) FROM jcm_u1 LEFT JOIN jcm_u2 USING (id)"
                    + " LEFT JOIN jcm_u3 USING (id)"));
            assertEquals("1|10|a|x|p,2|20|b|-|-,3|30|c|-|q",
                    rows("SELECT * FROM jcm_u1 NATURAL LEFT JOIN jcm_u2"
                            + " NATURAL LEFT JOIN jcm_u3 ORDER BY 1"));
        }

        @Test
        void aFullChainKeepsEverySide() throws Exception {
            assertEquals("1|a1|b1|c1,2|a2|b2|-,3|a3|-|c3,4|-|b4|c4",
                    rows("SELECT * FROM jcm_a FULL JOIN jcm_b USING (id)"
                            + " FULL JOIN jcm_c USING (id) ORDER BY 1"));
            assertEquals("1|10|a|10|x|10|p,2|20|b|99|y|-|-,3|30|c|-|-|30|q,4|-|-|40|z|-|-",
                    rows("SELECT * FROM jcm_u1 FULL JOIN jcm_u2 USING (id)"
                            + " FULL JOIN jcm_u3 USING (id) ORDER BY 1"));
        }

        @Test
        void innerAndOuterMixedInOneChain() throws Exception {
            assertEquals("1|a1|b1|c1,3|a3|-|c3",
                    rows("SELECT * FROM jcm_a LEFT JOIN jcm_b USING (id)"
                            + " JOIN jcm_c USING (id) ORDER BY 1"));
            assertEquals("1|a1|b1|c1,2|a2|b2|-",
                    rows("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                            + " LEFT JOIN jcm_c USING (id) ORDER BY 1"));
            assertEquals("1|a1|b1|c1,3|-|-|c3,4|-|b4|c4",
                    rows("SELECT * FROM jcm_a RIGHT JOIN jcm_b USING (id)"
                            + " RIGHT JOIN jcm_c USING (id) ORDER BY 1"));
        }

        @Test
        void usingAndNaturalMixedInOneChain() throws Exception {
            assertEquals("1|a1|b1|c1", rows("SELECT * FROM jcm_a NATURAL JOIN jcm_b"
                    + " JOIN jcm_c USING (id)"));
            assertEquals("1|a1|b1|c1", rows("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                    + " NATURAL JOIN jcm_c"));
        }

        @Test
        void aMergeMixedWithAnOnConditionKeepsTheThirdRelationsColumn() throws Exception {
            assertEquals("1|a1|b1|1|c1",
                    rows("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                            + " JOIN jcm_c ON jcm_c.id = jcm_a.id"));
        }

        @Test
        void parenthesisedGroupingsJoinTheGroupTheyAreWrittenAs() throws Exception {
            assertEquals("1|a1|b1|c1", rows("SELECT * FROM (jcm_a JOIN jcm_b USING (id))"
                    + " JOIN jcm_c USING (id)"));
            assertEquals("1|a1|b1|c1", rows("SELECT * FROM jcm_a"
                    + " JOIN (jcm_b JOIN jcm_c USING (id)) USING (id)"));
            assertEquals("1|a1|b1|c1|d1", rows("SELECT * FROM (jcm_a NATURAL JOIN jcm_b)"
                    + " NATURAL JOIN (jcm_c NATURAL JOIN jcm_d)"));
            assertEquals("1|a1|1|b1|c1,2|a2|-|-|-,3|a3|-|-|-",
                    rows("SELECT * FROM jcm_a a LEFT JOIN (jcm_b b JOIN jcm_c c USING (id))"
                            + " ON a.id = b.id ORDER BY 1"));
        }

        @Test
        void aRelationJoinedOnAfterwardsIsNotMergedIntoTheChain() throws Exception {
            assertEquals("1|10|a|x|1|10|p,1|10|a|x|3|30|q",
                    rows("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 JOIN jcm_u3 ON true"
                            + " ORDER BY 5"));
            assertEquals("1|10|a|x|1|10|p,1|10|a|x|3|30|q",
                    rows("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2, jcm_u3 ORDER BY 5"));
            assertEquals("1|10|a|x|1|10|p,1|10|a|x|3|30|q",
                    rows("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 CROSS JOIN jcm_u3"
                            + " ORDER BY 5"));
        }

        @Test
        void anEmptySideStillDescribesTheChain() throws Exception {
            assertEquals("", rows("SELECT * FROM jcm_e JOIN jcm_a USING (id) JOIN jcm_b USING (id)"));
            assertEquals("0", rows("SELECT count(*) FROM jcm_e NATURAL JOIN jcm_a NATURAL JOIN jcm_b"));
            assertEquals("id|av|ev", labels("SELECT * FROM jcm_a LEFT JOIN jcm_e USING (id)"));
            assertEquals("1|a1|-,2|a2|-,3|a3|-",
                    rows("SELECT * FROM jcm_a LEFT JOIN jcm_e USING (id) ORDER BY 1"));
        }
    }

    // ---- what SELECT * lists, and in what order ----

    @Nested
    class MergedColumnOrder {

        @Test
        void aMergedColumnComesFirst() throws Exception {
            assertEquals("s|id|p|id|q", labels("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s)"));
            assertEquals("10|1|a|1|x", rows("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s)"));
        }

        @Test
        void severalMergedColumnsComeInTheOrderUsingNamesThem() throws Exception {
            assertEquals("s|id|p|q", labels("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s, id)"));
            assertEquals("id|s|p|q", labels("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (id, s)"));
            assertEquals("10|1|a|x", rows("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s, id)"));
        }

        @Test
        void aNaturalJoinFindsThemInTheLeftSidesOrder() throws Exception {
            assertEquals("id|name|dept_id|salary|budget",
                    labels("SELECT * FROM jcm_emp NATURAL JOIN jcm_dpt"));
            assertEquals("1|ann|10|100|999", rows("SELECT * FROM jcm_emp NATURAL JOIN jcm_dpt"));
            assertEquals("id|bv|av", labels("SELECT * FROM jcm_f NATURAL JOIN jcm_a"));
            assertEquals("id|av|bv", labels("SELECT * FROM jcm_a NATURAL JOIN jcm_f"));
        }

        @Test
        void aChainListsOneMergedColumnAndEveryRelationsRest() throws Exception {
            assertEquals("id|av|bv|cv", labels("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c USING (id)"));
            assertEquals("id|s|p|s|q|s|r", labels("SELECT * FROM jcm_u1 FULL JOIN jcm_u2 USING (id)"
                    + " FULL JOIN jcm_u3 USING (id)"));
            assertEquals("id|av|bv|id|cv", labels("SELECT * FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c ON jcm_c.id = jcm_a.id"));
            assertEquals("id|s|p|q|id|s|r",
                    labels("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u2 JOIN jcm_u3 ON true"));
        }

        @Test
        void anOuterJoinsMergedColumnTakesTheSideThatIsNotNull() throws Exception {
            assertEquals("1|a1|b1,2|a2|b2,3|a3|-,4|-|b4",
                    rows("SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY 1"));
            assertEquals("1,2,3,4", rows("SELECT id FROM jcm_a FULL JOIN jcm_b USING (id)"
                    + " ORDER BY 1"));
            assertEquals("1,2,4,-", rows("SELECT jcm_b.id FROM jcm_a FULL JOIN jcm_b USING (id)"
                    + " ORDER BY 1"));
            assertEquals("1|ann|10|100|999,2|bob|20|200|-,2|zed|-|-|888,3|cy|30|300|-",
                    rows("SELECT * FROM jcm_emp NATURAL FULL JOIN jcm_dpt ORDER BY 1, 2"));
        }

        @Test
        void anOrdinalCountsTheColumnsTheJoinExposes() throws Exception {
            assertEquals("10|1|a|1|x", rows("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (s)"
                    + " ORDER BY 1"));
            assertEquals("4|-|b4,3|a3|-,2|a2|b2,1|a1|b1",
                    rows("SELECT * FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY 1 DESC"));
            // Two output columns named id: ordering by the first is the merged one.
            assertEquals("1|a1|b1|1|c1,1|a1|b1|3|c3,1|a1|b1|4|c4,"
                            + "2|a2|b2|1|c1,2|a2|b2|3|c3,2|a2|b2|4|c4",
                    rows("SELECT * FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c ON true"
                            + " ORDER BY 1, 4"));
        }

        @Test
        void groupingAndWindowingSeeTheSameColumns() throws Exception {
            assertEquals("1|a1|b1,2|a2|b2,3|a3|-,4|-|b4",
                    rows("SELECT * FROM jcm_a FULL JOIN jcm_b USING (id)"
                            + " GROUP BY id, av, bv ORDER BY 1"));
            assertEquals("1|a1|b1,2|a2|b2,3|a3|-,4|-|b4",
                    rows("SELECT * FROM jcm_a FULL JOIN jcm_b USING (id)"
                            + " GROUP BY 1,2,3 ORDER BY 1"));
            assertEquals("1|a1|b1|1,2|a2|b2|2,3|a3|-|3,4|-|b4|4",
                    rows("SELECT *, row_number() OVER (ORDER BY id)"
                            + " FROM jcm_a FULL JOIN jcm_b USING (id) ORDER BY id"));
            assertEquals("10|1|a|1|x|1",
                    rows("SELECT *, count(*) OVER () FROM jcm_u1 JOIN jcm_u2 USING (s)"));
        }

        @Test
        void aQualifiedStarStillListsThatRelationsOwnColumns() throws Exception {
            assertEquals("1|a1|1|b1,2|a2|2|b2",
                    rows("SELECT a.*, b.* FROM jcm_a a JOIN jcm_b b USING (id) ORDER BY 1"));
            assertEquals("1|a1,2|a2,3|a3,-|-",
                    rows("SELECT a.* FROM jcm_a a FULL JOIN jcm_b b USING (id) ORDER BY 1"));
        }
    }

    // ---- names a merge makes, and the ones it refuses ----

    @Nested
    class NameResolution {

        @Test
        void aMergedNameResolvesButAThirdRelationMakesItAmbiguousAgain() throws Exception {
            assertEquals("1,2", rows("SELECT id FROM jcm_a JOIN jcm_b USING (id) ORDER BY 1"));
            assertEquals("1", rows("SELECT id FROM jcm_a JOIN jcm_b USING (id)"
                    + " JOIN jcm_c USING (id)"));
            assertRejected("SELECT id FROM jcm_a JOIN jcm_b USING (id) JOIN jcm_c ON true",
                    "42702", "column reference \"id\" is ambiguous");
            assertRejected("SELECT s FROM jcm_u1 JOIN jcm_u2 USING (id) JOIN jcm_u3 ON true",
                    "42702", "column reference \"s\" is ambiguous");
            assertRejected("SELECT id FROM jcm_u1 JOIN jcm_u2 USING (s)",
                    "42702", "column reference \"id\" is ambiguous");
        }

        @Test
        void aRelationsOwnColumnIsStillReadableThroughItsName() throws Exception {
            assertEquals("1,2", rows("SELECT jcm_a.id FROM jcm_a JOIN jcm_b USING (id) ORDER BY 1"));
            assertEquals("1|1,2|2,3|-,-|4",
                    rows("SELECT a.id, b.id FROM jcm_a a FULL JOIN jcm_b b USING (id)"
                            + " ORDER BY 1, 2"));
            assertEquals("1|a1|b1", rows("SELECT * FROM jcm_a a JOIN jcm_b b USING (id)"
                    + " WHERE b.id = 1"));
        }

        @Test
        void aUsingNameMustBeOneColumnOfEachSide() {
            assertRejected("SELECT * FROM (jcm_u1 JOIN jcm_u2 ON true) JOIN jcm_u3 USING (id)",
                    "42702", "common column name \"id\" appears more than once in left table");
            assertRejected("SELECT * FROM jcm_a JOIN jcm_b ON jcm_a.id = jcm_b.id"
                            + " JOIN jcm_c USING (id)",
                    "42702", "common column name \"id\" appears more than once in left table");
            assertRejected("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (id) NATURAL JOIN jcm_u3",
                    "42702", "common column name \"s\" appears more than once in left table");
        }

        @Test
        void aMissingUsingNameNamesTheSideThatLacksIt() {
            assertRejected("SELECT * FROM jcm_u1 JOIN jcm_u4 USING (w)",
                    "42703", "column \"w\" specified in USING clause does not exist in left table");
            assertRejected("SELECT * FROM jcm_u4 JOIN jcm_u1 USING (w)",
                    "42703", "column \"w\" specified in USING clause does not exist in right table");
            assertRejected("SELECT * FROM jcm_a JOIN jcm_b USING (nope)",
                    "42703", "column \"nope\" specified in USING clause does not exist in left table");
            assertRejected("SELECT * FROM jcm_a JOIN jcm_b USING (av)",
                    "42703", "column \"av\" specified in USING clause does not exist in right table");
        }

        @Test
        void aQuotedUsingNameMatchesTheCaseItIsWrittenIn() throws Exception {
            assertRejected("SELECT * FROM jcm_u1 JOIN jcm_u2 USING (\"ID\")",
                    "42703", "column \"ID\" specified in USING clause does not exist in left table");
            // An unquoted name is folded down, so the ordinary spellings all still find it.
            assertEquals("2", rows("SELECT count(*) FROM jcm_a JOIN jcm_b USING (ID)"));
            assertEquals("2", rows("SELECT count(*) FROM jcm_a JOIN jcm_b USING (\"id\")"));
        }

        @Test
        void aStarQualifiedByAnUnknownNameIsRefused() {
            assertRejected("SELECT j.* FROM jcm_a JOIN jcm_b USING (id)",
                    "42P01", "missing FROM-clause entry for table \"j\"");
            assertRejected("SELECT j.* FROM jcm_a", "42P01",
                    "missing FROM-clause entry for table \"j\"");
        }

        @Test
        void aNumberIsNotEquatableWithAString() {
            assertRejected("SELECT count(*) FROM jcm_u1 JOIN jcm_u4 USING (id)",
                    "42883", "operator does not exist: integer = text");
            assertRejected("SELECT * FROM jcm_u1 NATURAL JOIN jcm_u4",
                    "42883", "operator does not exist: integer = text");
            assertRejected("SELECT count(*) FROM jcm_u4 JOIN jcm_u1 USING (id)",
                    "42883", "operator does not exist: text = integer");
        }
    }

    // ---- a function in FROM as one side of a join ----

    @Nested
    class SetReturningSide {

        @Test
        void aFullJoinAgainstOneKeepsBothSidesUnmatchedRows() throws Exception {
            assertEquals("1|a1|-,2|a2|2,3|a3|3,-|-|4,-|-|5",
                    rows("SELECT * FROM jcm_a FULL JOIN generate_series(2,5) g(id)"
                            + " ON jcm_a.id = g.id ORDER BY 1 NULLS LAST, 3"));
            assertEquals("1|a1,2|a2,3|a3,4|-,5|-",
                    rows("SELECT * FROM jcm_a FULL JOIN generate_series(2,5) g(id)"
                            + " USING (id) ORDER BY 1"));
        }

        @Test
        void aRightJoinAgainstOneKeepsItsRows() throws Exception {
            assertEquals("2|a2|2,3|a3|3,-|-|4",
                    rows("SELECT * FROM jcm_a RIGHT JOIN generate_series(2,4) g(id)"
                            + " ON g.id = jcm_a.id ORDER BY 3"));
        }

        @Test
        void aUsingOrNaturalClauseAgainstOneIsHonoured() throws Exception {
            assertEquals("1|a1,2|a2", rows("SELECT * FROM jcm_a JOIN generate_series(1,2) g(id)"
                    + " USING (id) ORDER BY 1"));
            assertEquals("1|a1,2|a2", rows("SELECT * FROM jcm_a NATURAL JOIN"
                    + " generate_series(1,2) g(id) ORDER BY 1"));
        }

        @Test
        void aLateralFunctionStillReadsTheRowsToItsLeft() throws Exception {
            assertEquals("1|a1|2,2|a2|3,3|a3|4",
                    rows("SELECT * FROM jcm_a CROSS JOIN LATERAL"
                            + " (SELECT jcm_a.id + 1 AS n) l ORDER BY 1"));
            assertEquals("1|a1|1,2|a2|2,3|a3|3",
                    rows("SELECT * FROM jcm_a JOIN generate_series(1,3) g(n)"
                            + " ON g.n = jcm_a.id ORDER BY 1"));
        }
    }

    // ---- ordinary SQL, which has to keep working ----

    @Nested
    class OrdinarySql {

        @Test
        void plainJoinsAreUnchanged() throws Exception {
            assertEquals("1|a1|1|b1,2|a2|2|b2",
                    rows("SELECT * FROM jcm_a a JOIN jcm_b b ON a.id = b.id ORDER BY 1"));
            assertEquals("1|a1|1|b1|1|c1", rows("SELECT * FROM jcm_a JOIN jcm_b"
                    + " ON jcm_a.id = jcm_b.id JOIN jcm_c ON jcm_b.id = jcm_c.id"));
            assertEquals("9", rows("SELECT count(*) FROM jcm_a CROSS JOIN jcm_b"));
            assertEquals("27", rows("SELECT count(*) FROM jcm_a, jcm_b, jcm_c"));
            assertEquals("2", rows("SELECT count(*) FROM jcm_a JOIN jcm_b USING (id)"));
        }

        @Test
        void aMergedColumnReadsFromEveryClause() throws Exception {
            assertEquals("3,4", rows("SELECT id FROM jcm_a FULL JOIN jcm_b USING (id)"
                    + " WHERE id > 2 ORDER BY 1"));
            assertEquals("1|1,2|1,3|1,4|1", rows("SELECT id, count(*) FROM jcm_a"
                    + " FULL JOIN jcm_b USING (id) GROUP BY id HAVING count(*) > 0 ORDER BY 1"));
            assertEquals("4", rows("SELECT max(id) FROM jcm_a FULL JOIN jcm_b USING (id)"));
            assertEquals("1,2,3,4", rows("SELECT DISTINCT id FROM jcm_a NATURAL FULL JOIN jcm_b"
                    + " ORDER BY 1"));
        }

        @Test
        void derivedRelationsMergeLikeStoredOnes() throws Exception {
            assertEquals("1|a1|b1,2|a2|b2",
                    rows("SELECT * FROM (SELECT id, av FROM jcm_a) s"
                            + " JOIN (SELECT id, bv FROM jcm_b) t USING (id) ORDER BY 1"));
            assertEquals("1|a1|b1,2|a2|b2",
                    rows("SELECT * FROM (SELECT id, av FROM jcm_a) s"
                            + " NATURAL JOIN (SELECT id, bv FROM jcm_b) t ORDER BY 1"));
            assertEquals("1|a1|b1|c1",
                    rows("WITH w AS (SELECT * FROM jcm_a) SELECT * FROM w"
                            + " NATURAL JOIN jcm_b NATURAL JOIN jcm_c"));
            assertEquals("1|a1|b1,2|a2|b2",
                    rows("SELECT * FROM (jcm_a JOIN jcm_b USING (id)) x ORDER BY 1"));
            assertEquals("1,2", rows("SELECT x.id FROM (jcm_a JOIN jcm_b USING (id)) x ORDER BY 1"));
        }

        @Test
        void widerNumericAndStringPairsAreStillEquatable() throws Exception {
            assertEquals("2", rows("SELECT count(*) FROM jcm_a"
                    + " JOIN (SELECT id::bigint AS id FROM jcm_b) t USING (id)"));
            assertEquals("2", rows("SELECT count(*) FROM jcm_a"
                    + " JOIN (SELECT id::numeric AS id FROM jcm_b) t USING (id)"));
            assertEquals("3", rows("SELECT count(*) FROM jcm_a a"
                    + " JOIN (SELECT av::varchar AS av FROM jcm_a) t USING (av)"));
            assertEquals("3", rows("SELECT count(*) FROM jcm_a a NATURAL JOIN jcm_a b"));
        }

        @Test
        void aViewOverAMergeIsReadableAndJoinableAgain() throws Exception {
            exec("CREATE VIEW jcm_v1 AS SELECT * FROM jcm_a JOIN jcm_b USING (id)");
            try {
                assertEquals("id|av|bv", labels("SELECT * FROM jcm_v1"));
                assertEquals("1|a1|b1,2|a2|b2", rows("SELECT * FROM jcm_v1 ORDER BY 1"));
                assertEquals("1|a1|b1|c1", rows("SELECT * FROM jcm_v1 NATURAL JOIN jcm_c"));
            } finally {
                exec("DROP VIEW jcm_v1");
            }
        }

        @Test
        void aMergeReadFromASubqueryAndFromDml() throws Exception {
            assertEquals("2", rows("SELECT (SELECT count(*) FROM jcm_a JOIN jcm_b USING (id))"));
            assertEquals("t", rows("SELECT EXISTS (SELECT * FROM jcm_a NATURAL JOIN jcm_b)"));
            assertEquals("1", rows("SELECT id FROM jcm_a WHERE id IN"
                    + " (SELECT id FROM jcm_b NATURAL JOIN jcm_c) ORDER BY 1"));
            exec("INSERT INTO jcm_e SELECT id, av FROM jcm_a NATURAL JOIN jcm_b");
            try {
                assertEquals("1|a1,2|a2", rows("SELECT * FROM jcm_e ORDER BY 1"));
            } finally {
                exec("DELETE FROM jcm_e");
            }
        }
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    /** One row per entry, columns joined by '|', NULL written as '-'. */
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
                        String v = rs.getString(i);
                        sb.append(v == null ? "-" : v);
                    }
                    out.add(sb.toString());
                }
                return String.join(",", out);
            }
        }
    }

    /** The column names the query answers with, joined by '|'. */
    private static String labels(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append('|');
                    sb.append(md.getColumnLabel(i));
                }
                return sb.toString();
            }
        }
    }

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }
}
