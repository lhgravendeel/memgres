package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where an aggregate or a window function may not appear, and where it still may.
 *
 * <p>An aggregate has a value only once a group of rows has been collected; a window call only
 * once the result rows exist to be numbered against one another. A clause read before either has
 * happened — WHERE, a JOIN condition, LIMIT, OFFSET, a VALUES row, the SET list of an UPDATE, a
 * CHECK constraint, an index expression, a DEFAULT, a generation expression — cannot hold one, and
 * PostgreSQL names the clause instead of evaluating something arbitrary. Every one of those was
 * accepted here, and the INSERT case wrote a row.
 *
 * <p>The scans that did exist stopped one level in: the aggregate-in-WHERE check descended into
 * CASE, NOT and a function's arguments but not into an IN list, a BETWEEN bound or ANY(ARRAY[...]),
 * so those three were places an aggregate could be written and quietly evaluated. The walk is now
 * reflective and complete rather than a list of containers to remember, and it stops at a nested
 * query — which is the other half of the rule, since {@code WHERE a > (SELECT count(*) FROM u)} is
 * ordinary SQL. The one exception is PostgreSQL's rule about which query level an aggregate
 * belongs to: one whose arguments name only columns of the enclosing relation belongs to the
 * enclosing query, so {@code UPDATE t SET c = (SELECT max(c) FROM other)} is an aggregate in the
 * UPDATE.
 *
 * <p>Also covered: a window function written without OVER, which was reported as a function that
 * does not exist rather than one missing a clause; and a row lock combined with something that
 * collapses rows, which has no row left to point at.
 */
class AggregatePlacementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE apl_nokey (a int, b text)");
        exec("INSERT INTO apl_nokey VALUES (1,'p'),(1,'q'),(2,'r')");
        exec("CREATE TABLE apl_other (k int, s text)");
        exec("INSERT INTO apl_other VALUES (1,'x'),(2,'y')");
        exec("CREATE TABLE apl_key (id int PRIMARY KEY, o text)");
        exec("INSERT INTO apl_key VALUES (1,'m'),(2,'n')");
        exec("CREATE VIEW apl_view AS SELECT a, count(*) n FROM apl_nokey GROUP BY a");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per line, columns joined by '|', sorted so an unordered result compares stably. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
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
            Collections.sort(out);
            return String.join(",", out);
        }
    }

    /** The SQLSTATE and message of the error {@code sql} raises; fails when it raises none. */
    private static SQLException error(String sql) {
        SQLException caught = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        return caught;
    }

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = error(sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    // ---- an aggregate in WHERE, wherever the clause buries it ----

    @Test
    void aggregateInWhereIsRejectedAtEveryDepth() {
        String[] clauses = {
            "count(*) > 1",
            "a IN (1, count(*)::int)",
            "a BETWEEN 0 AND count(*)::int",
            "a = ANY (ARRAY[count(*)::int])",
            "NOT (a = count(*)::int)",
            "CASE WHEN a = count(*)::int THEN true ELSE false END",
            "abs(count(*)::int) > 0",
            "count(*)::int = ANY (SELECT k FROM apl_other)",
            "(a, 1) = (count(*)::int, 1)",
        };
        for (String clause : clauses) {
            assertRejected("SELECT * FROM apl_nokey WHERE " + clause,
                    "42803", "aggregate functions are not allowed in WHERE");
        }
    }

    @Test
    void aggregateInJoinConditionIsRejected() {
        assertRejected("SELECT a FROM apl_nokey JOIN apl_other ON apl_nokey.a IN (apl_other.k, count(*)::int)",
                "42803", "aggregate functions are not allowed in JOIN conditions");
    }

    /** A sub-select brings its own rows, so the aggregate in it is the sub-select's. */
    @Test
    void aggregateInsideASubqueryOfWhereBelongsToTheSubquery() throws Exception {
        assertEquals("2|r", rows(
                "SELECT * FROM apl_nokey WHERE a IN (SELECT max(k) FROM apl_other)"));
        assertEquals("1|p,1|q,2|r", rows(
                "SELECT * FROM apl_nokey WHERE EXISTS (SELECT count(*) FROM apl_other)"));
        assertEquals("1|p,1|q,2|r", rows(
                "SELECT * FROM apl_nokey WHERE a > (SELECT min(k) FROM apl_other) - 1"));
        assertEquals("1|p,1|q", rows(
                "SELECT * FROM apl_nokey WHERE a IN (1, (SELECT count(*)::int FROM apl_other) - 1)"));
    }

    // ---- LIMIT and OFFSET ----

    @Test
    void aggregateInLimitOrOffsetIsRejected() {
        assertRejected("SELECT count(*) FROM apl_nokey LIMIT count(*)",
                "42803", "aggregate functions are not allowed in LIMIT");
        assertRejected("SELECT count(*) FROM apl_nokey OFFSET count(*)",
                "42803", "aggregate functions are not allowed in OFFSET");
        assertRejected("SELECT count(*) FROM apl_nokey LIMIT rank() OVER ()",
                "42P20", "window functions are not allowed in LIMIT");
    }

    @Test
    void aSubqueryInLimitStillCounts() throws Exception {
        assertEquals("1,1", rows(
                "SELECT a FROM apl_nokey ORDER BY a LIMIT (SELECT max(k) FROM apl_other)"));
    }

    // ---- UPDATE and DELETE ----

    @Test
    void aggregateInUpdateOrDeleteIsRejected() {
        assertRejected("DELETE FROM apl_other WHERE count(*) > 100",
                "42803", "aggregate functions are not allowed in WHERE");
        assertRejected("UPDATE apl_other SET s = 'z' WHERE sum(k) > 1000",
                "42803", "aggregate functions are not allowed in WHERE");
        assertRejected("DELETE FROM apl_other WHERE k = ANY (ARRAY[count(*)::int])",
                "42803", "aggregate functions are not allowed in WHERE");
        assertRejected("UPDATE apl_other SET k = count(*) WHERE false",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("UPDATE apl_other SET k = 1 + max(k) WHERE false",
                "42803", "aggregate functions are not allowed in UPDATE");
    }

    /**
     * An aggregate over a column the sub-select does not supply is one query level up, and the
     * level it belongs to is the level that has to refuse it.
     */
    @Test
    void anAggregateOverOnlyOuterColumnsBelongsToTheOuterStatement() {
        assertRejected("UPDATE apl_other SET s = (SELECT max(s) FROM apl_nokey) WHERE false",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("UPDATE apl_other o SET s = (SELECT max(o.s) FROM apl_nokey) WHERE false",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("DELETE FROM apl_other WHERE k > (SELECT max(k) FROM apl_nokey)",
                "42803", "aggregate functions are not allowed in WHERE");
    }

    /** …and one over a column the sub-select does supply belongs to the sub-select. */
    @Test
    void anAggregateOverTheSubqueryOwnColumnsIsFine() throws Exception {
        exec("CREATE TABLE apl_upd (k int, s text)");
        exec("INSERT INTO apl_upd VALUES (1,'x'),(2,'y')");
        exec("UPDATE apl_upd SET s = (SELECT max(b) FROM apl_nokey) WHERE k = 1");
        assertEquals("1|r,2|y", rows("SELECT k, s FROM apl_upd"));
        exec("UPDATE apl_upd SET s = (SELECT max(b) FROM apl_nokey WHERE a = apl_upd.k) WHERE k = 2");
        assertEquals("1|r,2|r", rows("SELECT k, s FROM apl_upd"));
        exec("UPDATE apl_upd SET k = (SELECT count(*) FROM apl_nokey) WHERE k = 2");
        assertEquals("1|r,3|r", rows("SELECT k, s FROM apl_upd"));
        exec("DELETE FROM apl_upd WHERE k > (SELECT max(a) FROM apl_nokey)");
        assertEquals("1|r", rows("SELECT k, s FROM apl_upd"));
        exec("DROP TABLE apl_upd");
    }

    // ---- VALUES ----

    @Test
    void aggregateInValuesIsRejectedAndWritesNothing() throws Exception {
        exec("CREATE TABLE apl_ins (k int, s text)");
        assertRejected("INSERT INTO apl_ins VALUES (count(*), 'x')",
                "42803", "aggregate functions are not allowed in VALUES");
        assertRejected("INSERT INTO apl_ins VALUES (1, 'x'), (count(*)::int, 'y')",
                "42803", "aggregate functions are not allowed in VALUES");
        assertRejected("INSERT INTO apl_ins VALUES (row_number() OVER ()::int, 'x')",
                "42P20", "window functions are not allowed in VALUES");
        assertEquals("", rows("SELECT k, s FROM apl_ins"));
        exec("DROP TABLE apl_ins");
    }

    @Test
    void aggregateInAStandaloneOrDerivedValuesListIsRejected() {
        assertRejected("VALUES (count(*))", "42803",
                "aggregate functions are not allowed in VALUES");
        assertRejected("SELECT * FROM (VALUES (count(*))) v", "42803",
                "aggregate functions are not allowed in VALUES");
        assertRejected("SELECT * FROM (VALUES (row_number() OVER ())) v", "42P20",
                "window functions are not allowed in VALUES");
    }

    /**
     * A FROM-less SELECT is not a VALUES row. It reads the one-row relation PostgreSQL supplies
     * when there is no FROM, so {@code SELECT count(*)} is a legal aggregate over that one row and
     * answers 1 — measured against PostgreSQL 18, not assumed.
     */
    @Test
    void aFromlessSelectMayStillAggregate() throws Exception {
        assertEquals("1", rows("SELECT count(*)"));
        assertEquals("3", rows("SELECT (SELECT count(*) FROM apl_nokey)"));
        assertEquals("3", rows("SELECT * FROM (VALUES ((SELECT count(*) FROM apl_nokey))) v(x)"));
        assertEquals("3", rows("SELECT sum(x) FROM (VALUES (1), (2)) v(x)"));
        assertEquals("1|a,2|b", rows("SELECT * FROM (VALUES (1,'a'),(2,'b')) v(x,y)"));
    }

    // ---- definitions evaluated one row at a time ----

    @Test
    void aggregateInADefinitionIsRejected() {
        assertRejected("CREATE TABLE apl_chk (x int CHECK (count(x) > 0))",
                "42803", "aggregate functions are not allowed in check constraints");
        assertRejected("CREATE TABLE apl_chk (x int, CHECK (sum(x) > 0))",
                "42803", "aggregate functions are not allowed in check constraints");
        assertRejected("ALTER TABLE apl_other ADD CONSTRAINT apl_c1 CHECK (count(k) > 0)",
                "42803", "aggregate functions are not allowed in check constraints");
        assertRejected("CREATE INDEX apl_i1 ON apl_nokey ((count(a)))",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("CREATE TABLE apl_chk (x int DEFAULT count(*))",
                "42803", "aggregate functions are not allowed in DEFAULT expressions");
        assertRejected("CREATE TABLE apl_chk (x int GENERATED ALWAYS AS (count(x)) STORED)",
                "42803", "aggregate functions are not allowed in column generation expressions");
        assertRejected("CREATE TABLE apl_chk (x int CHECK (row_number() OVER () > 0))",
                "42P20", "window functions are not allowed in check constraints");
        assertRejected("CREATE INDEX apl_i2 ON apl_nokey ((rank() OVER ()))",
                "42P20", "window functions are not allowed in index expressions");
    }

    @Test
    void ordinaryDefinitionsAreStillAccepted() throws Exception {
        exec("CREATE TABLE apl_ok (x int CHECK (x > 0 AND x < 100), y text DEFAULT upper('a'),"
                + " z int GENERATED ALWAYS AS (x * 2) STORED)");
        exec("CREATE INDEX apl_ok_i ON apl_ok ((x + 1)) WHERE x > 0");
        exec("INSERT INTO apl_ok (x) VALUES (5)");
        assertEquals("5|A|10", rows("SELECT x, y, z FROM apl_ok"));
        exec("DROP TABLE apl_ok");
    }

    // ---- window functions in DML ----

    @Test
    void windowCallInDmlIsRejected() {
        assertRejected("UPDATE apl_nokey SET a = row_number() OVER () WHERE false",
                "42P20", "window functions are not allowed in UPDATE");
        assertRejected("DELETE FROM apl_other WHERE row_number() OVER () > 1",
                "42P20", "window functions are not allowed in WHERE");
        assertRejected("UPDATE apl_other SET s = 'z' WHERE rank() OVER () > 1",
                "42P20", "window functions are not allowed in WHERE");
        assertRejected("SELECT * FROM apl_nokey WHERE a IN (1, row_number() OVER ()::int)",
                "42P20", "window functions are not allowed in WHERE");
        assertRejected("SELECT * FROM apl_nokey WHERE a BETWEEN 0 AND rank() OVER ()::int",
                "42P20", "window functions are not allowed in WHERE");
    }

    // ---- a window function with no OVER clause ----

    @Test
    void aWindowFunctionWithoutOverNamesTheMissingClause() {
        String[] calls = {"row_number()", "rank()", "dense_rank()", "percent_rank()",
                "cume_dist()", "ntile(2)", "lag(a)", "lead(a)", "first_value(a)",
                "last_value(a)", "nth_value(a, 1)", "lag(a, 1)"};
        for (String call : calls) {
            String name = call.substring(0, call.indexOf('('));
            assertRejected("SELECT " + call + " FROM apl_nokey",
                    "42809", "window function " + name + " requires an OVER clause");
        }
    }

    @Test
    void theMissingClauseIsNamedWhereverTheCallStands() {
        assertRejected("SELECT * FROM apl_nokey WHERE a = rank()",
                "42809", "window function rank requires an OVER clause");
        assertRejected("SELECT count(*) FROM apl_nokey GROUP BY rank()",
                "42809", "window function rank requires an OVER clause");
        assertRejected("SELECT a FROM apl_nokey ORDER BY row_number()",
                "42809", "window function row_number requires an OVER clause");
    }

    /**
     * rank, dense_rank, percent_rank and cume_dist are ordered-set aggregates as well as window
     * functions: given arguments, what they are missing is WITHIN GROUP, not OVER.
     */
    @Test
    void aHypotheticalSetCallWithArgumentsIsMissingWithinGroupInstead() {
        assertRejected("SELECT rank(a) FROM apl_nokey",
                "42809", "WITHIN GROUP is required for ordered-set aggregate rank");
        assertRejected("SELECT dense_rank(a) FROM apl_nokey",
                "42809", "WITHIN GROUP is required for ordered-set aggregate dense_rank");
    }

    @Test
    void bothWrittenFormsStillWork() throws Exception {
        assertEquals("1", rows("SELECT rank(1) WITHIN GROUP (ORDER BY a) FROM apl_nokey"));
        assertEquals("1,1,3", rows("SELECT rank() OVER (ORDER BY a) FROM apl_nokey"));
        assertEquals("1,2,3", rows("SELECT row_number() OVER (ORDER BY a, b) FROM apl_nokey"));
        assertEquals("1,1,2", rows("SELECT ntile(2) OVER (ORDER BY a, b) FROM apl_nokey"));
    }

    // ---- row locks over rows that were collapsed ----

    @Test
    void aRowLockNeedsARowToPointAt() {
        assertRejected("SELECT a FROM apl_nokey GROUP BY a FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with GROUP BY clause");
        assertRejected("SELECT a FROM apl_nokey GROUP BY a FOR SHARE",
                "0A000", "FOR SHARE is not allowed with GROUP BY clause");
        assertRejected("SELECT a FROM apl_nokey GROUP BY a FOR NO KEY UPDATE",
                "0A000", "FOR NO KEY UPDATE is not allowed with GROUP BY clause");
        assertRejected("SELECT a FROM apl_nokey GROUP BY GROUPING SETS ((a)) FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with GROUP BY clause");
        assertRejected("SELECT DISTINCT a FROM apl_nokey FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with DISTINCT clause");
        assertRejected("SELECT a FROM apl_nokey HAVING count(*) > 0 FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with HAVING clause");
        assertRejected("SELECT count(*) FROM apl_nokey FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with aggregate functions");
        assertRejected("SELECT a, rank() OVER (ORDER BY a) FROM apl_nokey FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with window functions");
        assertRejected("SELECT a FROM apl_nokey UNION SELECT k FROM apl_other FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT");
        assertRejected("SELECT * FROM (SELECT a FROM apl_nokey GROUP BY a) q FOR UPDATE",
                "0A000", "FOR UPDATE is not allowed with GROUP BY clause");
    }

    @Test
    void aLockOverUncollapsedRowsIsStillAllowed() throws Exception {
        assertEquals("1,1,2", rows("SELECT a FROM apl_nokey FOR UPDATE"));
        assertEquals("1,1", rows("SELECT a FROM apl_nokey WHERE a = 1 FOR UPDATE OF apl_nokey"));
        assertEquals("1,1,2", rows("SELECT a FROM apl_nokey ORDER BY a FOR UPDATE NOWAIT"));
        assertEquals("1,2", rows("SELECT id FROM apl_key FOR NO KEY UPDATE"));
        assertEquals("1,1,2", rows(
                "SELECT a FROM apl_nokey n WHERE n.a IN (SELECT max(k) FROM apl_other) OR true FOR UPDATE"));
    }

    // ---- a view body is judged when the view is written ----

    @Test
    void aViewOverAnUngroupedColumnIsRefusedAtDefinitionTime() throws Exception {
        assertRejected("CREATE VIEW apl_badview AS SELECT a, b FROM apl_nokey GROUP BY a",
                "42803", "must appear in the GROUP BY clause");
        // and nothing was recorded under that name
        assertRejected("SELECT * FROM apl_badview", "42P01", "apl_badview");
    }

    @Test
    void aWellFormedViewIsStillDefinedAndRead() throws Exception {
        assertEquals("1|2,2|1", rows("SELECT a, n FROM apl_view ORDER BY a"));
        exec("CREATE VIEW apl_view2 AS SELECT id, o FROM apl_key GROUP BY id");
        assertEquals("1|m,2|n", rows("SELECT id, o FROM apl_view2"));
        exec("DROP VIEW apl_view2");
    }

    // ---- the shapes every real query is made of ----

    @Test
    void ordinaryGroupedAndWindowedQueriesAreUntouched() throws Exception {
        assertEquals("1|2,2|1", rows("SELECT a, count(*) FROM apl_nokey GROUP BY a"));
        assertEquals("1|2", rows(
                "SELECT a, count(*) c FROM apl_nokey GROUP BY a HAVING count(*) > 1 ORDER BY c"));
        assertEquals("1|2,2|1", rows(
                "SELECT a, count(*) FROM apl_nokey GROUP BY a ORDER BY count(*) DESC, a"));
        assertEquals("1,2", rows("SELECT DISTINCT a FROM apl_nokey"));
        assertEquals("1|m|1,2|n|1", rows(
                "SELECT id, o, count(*) FROM apl_key GROUP BY id ORDER BY id"));
        assertEquals("1|2,2|1", rows(
                "SELECT n.a, count(o.k) FROM apl_nokey n JOIN apl_other o ON n.a = o.k GROUP BY n.a"));
        assertEquals("1|3,2|3", rows(
                "SELECT a, sum(count(*)) OVER () FROM apl_nokey GROUP BY a"));
        assertEquals("1|1,2|2", rows(
                "SELECT a, rank() OVER (ORDER BY count(*) DESC) FROM apl_nokey GROUP BY a"));
        assertEquals("1|1,1|1,2|3", rows(
                "SELECT a, rank() OVER (ORDER BY a) FROM apl_nokey"));
    }

    @Test
    void derivedRelationsCtesAndLateralAreUntouched() throws Exception {
        assertEquals("1,2,3", rows(
                "SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a, b) rn FROM apl_nokey) sub"
                        + " WHERE sub.rn >= 1"));
        assertEquals("1|2,2|1", rows(
                "WITH c AS (SELECT a, count(*) n FROM apl_nokey GROUP BY a) SELECT * FROM c"));
        assertEquals("1|1,2|2", rows(
                "SELECT k.id, l.m FROM apl_key k,"
                        + " LATERAL (SELECT max(a) m FROM apl_nokey WHERE a = k.id) l"));
        assertEquals("1|p,1|q,2|r", rows(
                "SELECT * FROM apl_nokey WHERE a IN"
                        + " (SELECT k FROM apl_other GROUP BY k HAVING count(*) >= 1)"));
        assertEquals("1|2,2|1", rows("SELECT a, n FROM apl_view"));
    }

    @Test
    void dmlWithAggregatingSubqueriesIsUntouched() throws Exception {
        exec("CREATE TABLE apl_dml (k int, s text)");
        exec("INSERT INTO apl_dml SELECT a, b FROM apl_nokey");
        exec("INSERT INTO apl_dml SELECT count(*)::int, 'agg' FROM apl_nokey GROUP BY a");
        assertEquals("1|agg,1|p,1|q,2|agg,2|r", rows("SELECT k, s FROM apl_dml"));
        exec("UPDATE apl_dml SET s = 'z' WHERE k IN (SELECT a FROM apl_nokey GROUP BY a HAVING count(*) > 1)");
        assertEquals("1|z,1|z,1|z,2|agg,2|r", rows("SELECT k, s FROM apl_dml"));
        exec("DELETE FROM apl_dml WHERE k > (SELECT min(a) FROM apl_nokey)");
        assertEquals("1|z,1|z,1|z", rows("SELECT k, s FROM apl_dml"));
        exec("DROP TABLE apl_dml");
    }
}
