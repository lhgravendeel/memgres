package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How a grouped query is formed, measured against PostgreSQL 18.
 *
 * <p>Three rules, all about the shape of the grouping rather than about what it licenses the
 * select list to read:
 *
 * <ul>
 *   <li><b>HAVING with no GROUP BY forms exactly one group over the whole table.</b>
 *       {@code SELECT 1 FROM t HAVING true} answers a single row and {@code HAVING false} none;
 *       memgres filtered row by row and answered one row per table row. WHERE does not take the
 *       group away — it exists even when no row reaches it — so {@code WHERE false HAVING true}
 *       still answers one row, where memgres answered none.</li>
 *   <li><b>Several grouping elements are cross-multiplied.</b>
 *       {@code GROUP BY ROLLUP(a), ROLLUP(b)} groups by the Cartesian product of the two lists
 *       of grouping sets; memgres used the first element's sets and dropped the rest, which both
 *       lost rows and left the dropped element's columns ungrouped, so the query was rejected.
 *       GROUP BY DISTINCT drops the sets the product repeats and GROUP BY ALL keeps them.</li>
 *   <li><b>GROUPING() answers for a plain GROUP BY too.</b> memgres rejected every GROUPING call
 *       in a query without GROUPING SETS / ROLLUP / CUBE, so {@code SELECT a, grouping(a) ...
 *       GROUP BY a} — where the answer is 0 — was an error; and it accepted an argument the
 *       query does not group by, where PostgreSQL rejects it. Its result is int4, not text.</li>
 * </ul>
 *
 * <p>Ordering: PostgreSQL's hash aggregate emits groups in an arbitrary order, so every result
 * here is either ordered by the query or sorted before comparison.
 */
class GroupingSemanticsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE gsm_nokey (a int, b text)");
        exec("INSERT INTO gsm_nokey VALUES (1,'p'),(1,'q'),(2,'r')");
        exec("CREATE TABLE gsm_other (x int)");
        exec("INSERT INTO gsm_other VALUES (1)");
        exec("CREATE TABLE gsm_t (id int PRIMARY KEY, a int, b text, n int)");
        exec("INSERT INTO gsm_t VALUES (1,10,'x',5),(2,20,'y',6),(3,10,'z',7)");
        exec("CREATE TABLE gsm_child (cid int PRIMARY KEY, tid int, amt int)");
        exec("INSERT INTO gsm_child VALUES (1,1,5),(2,1,6),(3,2,7)");
        exec("CREATE TABLE gsm_empty (a int, b text)");
        exec("CREATE VIEW gsm_v AS SELECT a, count(*) AS c FROM gsm_nokey GROUP BY a");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** All rows of a query, each row rendered as pipe-joined column values, in query order. */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
            return out;
        }
    }

    /** The rows of a query the server may order as it likes, sorted so they compare. */
    private static List<String> sorted(String sql) throws SQLException {
        List<String> out = rows(sql);
        Collections.sort(out);
        return out;
    }

    /** The SQLSTATE the statement fails with, or null when it succeeds. */
    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    private static String typeOf(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(rs.getMetaData().getColumnCount());
        }
    }

    // ---- 1. HAVING with no GROUP BY ----

    @Test
    void havingWithoutGroupByFormsOneGroup() throws Exception {
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 FROM gsm_nokey HAVING true"));
        assertEquals(java.util.Collections.emptyList(), rows("SELECT 1 FROM gsm_nokey HAVING false"));
    }

    @Test
    void theGroupExistsEvenWhenWhereEmptiesIt() throws Exception {
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT 1 FROM gsm_nokey WHERE false HAVING true"));
        assertEquals(java.util.Arrays.asList("0"),
                rows("SELECT count(*) FROM gsm_nokey WHERE false HAVING count(*) = 0"));
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 FROM gsm_empty HAVING true"));
        assertEquals(java.util.Collections.emptyList(), rows("SELECT 1 FROM gsm_empty HAVING false"));
    }

    @Test
    void theOneGroupIsFilteredByAnyHavingCondition() throws Exception {
        assertEquals(java.util.Collections.emptyList(),
                rows("SELECT 1 FROM gsm_nokey HAVING 1 > (SELECT count(*) FROM gsm_other)"));
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT 1 FROM gsm_nokey HAVING count(*) = 3"));
        assertEquals(java.util.Collections.emptyList(),
                rows("SELECT 1 FROM gsm_nokey HAVING NULL"));
    }

    @Test
    void oneGroupSurvivesJoinsLimitOrderByAndSubqueries() throws Exception {
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT 1 FROM gsm_nokey, gsm_other HAVING true"));
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT 1 AS one FROM gsm_nokey HAVING true LIMIT 5"));
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT 1 FROM gsm_nokey HAVING true ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT count(*) FROM (SELECT 1 FROM gsm_nokey HAVING true) s"));
        assertEquals(java.util.Arrays.asList("1"),
                rows("WITH w AS (SELECT 1 AS one FROM gsm_nokey HAVING true) SELECT * FROM w"));
    }

    @Test
    void aSelectListWithoutFromAlsoGroupsOnce() throws Exception {
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 WHERE true HAVING true"));
        assertEquals(java.util.Collections.emptyList(), rows("SELECT 1 HAVING false"));
    }

    @Test
    void theOneGroupIsJudgedLikeAnyOtherGroup() {
        assertEquals("42803", stateOf("SELECT a FROM gsm_nokey HAVING true"));
        assertEquals("42803", stateOf("SELECT 1 FROM gsm_nokey HAVING a > 0"));
        assertEquals("42803", stateOf("SELECT * FROM gsm_nokey HAVING true"));
        assertEquals("42803", stateOf("SELECT count(*) FROM gsm_nokey HAVING sum(a) > 0 AND b > 'a'"));
    }

    // ---- 2. several grouping elements are cross-multiplied ----

    @Test
    void twoRollupsAreCrossMultiplied() throws Exception {
        // (a,b), (a), (b), () over three rows: 3 + 2 + 3 + 1 = 9 groups
        assertEquals(java.util.Arrays.asList("1", "1", "1", "1", "1", "1", "1", "2", "3"),
                rows("SELECT count(*) FROM gsm_nokey GROUP BY ROLLUP(a), ROLLUP(b) ORDER BY 1"));
    }

    @Test
    void aGroupingSetMultipliesWithAPlainColumn() throws Exception {
        // ROLLUP(a) x (b) = (a,b), (b)
        assertEquals(java.util.Arrays.asList("1", "1", "1", "1", "1", "1"),
                rows("SELECT count(*) FROM gsm_nokey GROUP BY ROLLUP(a), b ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|p|1", "1|q|1", "1|null|2", "2|r|1", "2|null|1"),
                rows("SELECT a, b, count(*) FROM gsm_nokey"
                        + " GROUP BY a, GROUPING SETS ((b), ()) ORDER BY 1, 2"));
        assertEquals(java.util.Arrays.asList("1|p|1", "1|q|1", "2|r|1", "null|p|1", "null|q|1", "null|r|1"),
                rows("SELECT a, b, count(*) FROM gsm_nokey"
                        + " GROUP BY GROUPING SETS ((a), ()), b ORDER BY 1, 2"));
    }

    @Test
    void twoGroupingSetsClausesAreCrossMultiplied() throws Exception {
        assertEquals(java.util.Arrays.asList("1", "1", "1"),
                rows("SELECT count(*) FROM gsm_nokey"
                        + " GROUP BY GROUPING SETS ((a)), GROUPING SETS ((b)) ORDER BY 1"));
    }

    @Test
    void twoCubesAreCrossMultiplied() throws Exception {
        assertEquals(java.util.Arrays.asList(
                        "1|p|1", "1|q|1", "1|null|2", "2|r|1", "2|null|1",
                        "null|p|1", "null|q|1", "null|r|1", "null|null|3"),
                rows("SELECT a, b, count(*) FROM gsm_nokey GROUP BY CUBE(a), CUBE(b) ORDER BY 1, 2"));
        // The same nine sets as the one written CUBE(a, b)
        assertEquals(sorted("SELECT a, b, count(*) FROM gsm_nokey GROUP BY CUBE(a, b) ORDER BY 1, 2"),
                sorted("SELECT a, b, count(*) FROM gsm_nokey GROUP BY CUBE(a), CUBE(b) ORDER BY 1, 2"));
    }

    @Test
    void groupByDistinctDropsRepeatedSetsAndAllKeepsThem() throws Exception {
        // ROLLUP(a) x ROLLUP(a,b) is (a,b),(a),(a),(a,b),(a),() -- three distinct sets
        assertEquals(java.util.Arrays.asList(
                        "1|p|1", "1|q|1", "1|null|2", "2|r|1", "2|null|1", "null|null|3"),
                rows("SELECT a, b, count(*) FROM gsm_nokey"
                        + " GROUP BY DISTINCT ROLLUP(a), ROLLUP(a, b) ORDER BY 1, 2"));
        assertEquals(13, rows("SELECT a, b, count(*) FROM gsm_nokey"
                + " GROUP BY ALL ROLLUP(a), ROLLUP(a, b) ORDER BY 1, 2").size());
        assertEquals(13, rows("SELECT a, b, count(*) FROM gsm_nokey"
                + " GROUP BY ROLLUP(a), ROLLUP(a, b) ORDER BY 1, 2").size());
    }

    @Test
    void theSetQuantifierIsAlsoAcceptedOnAPlainGroupBy() throws Exception {
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY ALL a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY DISTINCT a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY DISTINCT a, a ORDER BY 1"));
    }

    @Test
    void everyElementsColumnsCountAsGrouped() {
        // Dropping the trailing element left its columns ungrouped, and the query was rejected.
        assertNull(stateOf("SELECT a, b, count(*) FROM gsm_nokey GROUP BY CUBE(a), CUBE(b)"));
        assertNull(stateOf("SELECT a, b, count(*) FROM gsm_nokey GROUP BY ROLLUP(a), ROLLUP(a,b)"));
        assertNull(stateOf("SELECT a, b, count(*) FROM gsm_nokey GROUP BY GROUPING SETS ((a), ()), b"));
    }

    // ---- 3. GROUPING() ----

    @Test
    void groupingAnswersForAPlainGroupBy() throws Exception {
        assertEquals(java.util.Arrays.asList("1|0", "2|0"),
                rows("SELECT a, grouping(a) FROM gsm_nokey GROUP BY a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|0", "2|0", "3|0"),
                rows("SELECT id, grouping(id) FROM gsm_t GROUP BY id ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("2|0", "3|0"),
                rows("SELECT a+1, grouping(a+1) FROM gsm_nokey GROUP BY a+1 ORDER BY 1"));
    }

    @Test
    void groupingMatchesTheGroupingExpressionHoweverItIsWritten() throws Exception {
        assertEquals(java.util.Arrays.asList("1|0", "2|0"),
                rows("SELECT a, grouping(gsm_nokey.a) FROM gsm_nokey GROUP BY a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|0", "2|0"),
                rows("SELECT t.a, grouping(a) FROM gsm_nokey t GROUP BY t.a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|0", "2|0"),
                rows("SELECT a, grouping(a) FROM gsm_nokey GROUP BY 1 ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|0", "2|0"),
                rows("SELECT a, grouping(a) FROM gsm_nokey GROUP BY (a) ORDER BY 1"));
    }

    @Test
    void groupingStandsInHavingAndOrderBy() throws Exception {
        assertEquals(java.util.Arrays.asList("1", "2"),
                rows("SELECT a FROM gsm_nokey GROUP BY a HAVING grouping(a) = 0 ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1", "2"),
                rows("SELECT a FROM gsm_nokey GROUP BY a ORDER BY grouping(a), a"));
    }

    @Test
    void groupingStillReportsTheSetsOfAGroupingSetsQuery() throws Exception {
        assertEquals(java.util.Arrays.asList("1|0", "2|0", "null|1"),
                rows("SELECT a, grouping(a) FROM gsm_nokey GROUP BY ROLLUP (a) ORDER BY 1, 2"));
        assertEquals(java.util.Arrays.asList(
                        "1|p|0", "1|q|0", "1|null|1", "2|r|0", "2|null|1",
                        "null|p|2", "null|q|2", "null|r|2", "null|null|3"),
                rows("SELECT a, b, grouping(a,b) FROM gsm_nokey GROUP BY CUBE(a,b) ORDER BY 1, 2"));
    }

    @Test
    void groupingsResultIsInt4() throws Exception {
        assertEquals("int4", typeOf("SELECT grouping(a) FROM gsm_nokey GROUP BY a"));
        assertEquals(java.util.Arrays.asList("integer"),
                rows("SELECT pg_typeof(grouping(a)) FROM gsm_nokey GROUP BY a LIMIT 1"));
        assertEquals(java.util.Arrays.asList("1|1", "2|1"),
                rows("SELECT a, grouping(a) + 1 FROM gsm_nokey GROUP BY a ORDER BY 1"));
    }

    @Test
    void groupingRejectsAnArgumentTheQueryDoesNotGroupBy() {
        String expected = "arguments to GROUPING must be grouping expressions "
                + "of the associated query level";
        assertEquals("42803", stateOf("SELECT a, grouping(b) FROM gsm_nokey GROUP BY ROLLUP (a)"));
        assertTrue(messageOf("SELECT a, grouping(b) FROM gsm_nokey GROUP BY ROLLUP (a)")
                .contains(expected));
        assertEquals("42803", stateOf("SELECT grouping(a) FROM gsm_nokey"));
        assertEquals("42803", stateOf("SELECT grouping(a) FROM gsm_nokey GROUP BY b"));
        assertEquals("42803", stateOf("SELECT count(*) FROM gsm_nokey GROUP BY a HAVING grouping(b) = 0"));
        // Grouping over an empty table is rejected before any row could carry the error
        assertEquals("42803", stateOf("SELECT a, grouping(b) FROM gsm_empty GROUP BY a"));
    }

    @Test
    void groupingMayNotStandInWhere() {
        assertEquals("42803", stateOf("SELECT count(*) FROM gsm_nokey WHERE grouping(a) = 0 GROUP BY a"));
        assertTrue(messageOf("SELECT count(*) FROM gsm_nokey WHERE grouping(a) = 0 GROUP BY a")
                .contains("grouping operations are not allowed in WHERE"));
    }

    // ---- Regression guard: the ordinary shapes a grouped query takes ----

    @Test
    void plainGroupByHavingAndOrderByStillWork() throws Exception {
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY a ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY a HAVING count(*) > 1 ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a AS k, count(*) AS c FROM gsm_nokey GROUP BY a ORDER BY k"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY a ORDER BY count(*) DESC, a"));
        assertEquals(java.util.Arrays.asList("2|1"),
                rows("SELECT a, count(*) FROM gsm_nokey GROUP BY a HAVING a > 1 ORDER BY 1"));
    }

    @Test
    void distinctWindowsJoinsViewsAndCtesStillWork() throws Exception {
        assertEquals(java.util.Arrays.asList("1", "2"), rows("SELECT DISTINCT a FROM gsm_nokey ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|1", "1|2", "2|3"),
                rows("SELECT a, row_number() OVER (ORDER BY a) FROM gsm_nokey ORDER BY 1, 2"));
        assertEquals(java.util.Arrays.asList("18", "18", "18"),
                rows("SELECT sum(n) OVER () FROM gsm_t GROUP BY n ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|11", "2|7"),
                rows("SELECT t.id, sum(c.amt) FROM gsm_t t JOIN gsm_child c ON c.tid = t.id"
                        + " GROUP BY t.id ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"), rows("SELECT * FROM gsm_v ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("WITH w AS (SELECT a, count(*) c FROM gsm_nokey GROUP BY a) SELECT * FROM w ORDER BY 1"));
    }

    @Test
    void derivedColumnsSubqueriesAndLateralStillWork() throws Exception {
        assertEquals(java.util.Arrays.asList("1|2", "2|1"),
                rows("SELECT * FROM (SELECT a, count(*) c FROM gsm_nokey GROUP BY a) s"
                        + " WHERE s.c >= 1 ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1", "2", "3"),
                rows("SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a) rn FROM gsm_nokey) sub"
                        + " WHERE sub.rn >= 1 ORDER BY 1"));
        assertEquals(java.util.Arrays.asList("1|2", "2|1", "3|0"),
                rows("SELECT t.id, x.c FROM gsm_t t,"
                        + " LATERAL (SELECT count(*) c FROM gsm_child ch WHERE ch.tid = t.id) x ORDER BY 1"));
        // A primary key still determines the rest of its row
        assertEquals(java.util.Arrays.asList("1|10", "2|20", "3|10"),
                rows("SELECT id, a FROM gsm_t GROUP BY id ORDER BY 1"));
    }

    @Test
    void ungroupedAggregatesStillAnswerOneRow() throws Exception {
        assertEquals(java.util.Arrays.asList("3"), rows("SELECT count(*) FROM gsm_nokey"));
        assertEquals(java.util.Arrays.asList("0"), rows("SELECT count(*) FROM gsm_nokey WHERE false"));
        assertEquals(java.util.Arrays.asList("2"), rows("SELECT max(a) FROM gsm_nokey HAVING true"));
    }
}
