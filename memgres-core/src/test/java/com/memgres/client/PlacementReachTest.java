package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The clauses the aggregate/window placement walk did not reach, and the names it puts in its
 * messages.
 *
 * <p>The rule was already right — a clause read before there is a group to aggregate or a result
 * row to be numbered against cannot hold an aggregate or a window call, and PostgreSQL names the
 * clause. What was missing was reach. Nine kinds of clause never asked: ON CONFLICT's DO UPDATE
 * list and its WHERE, its index expression and index predicate, MERGE's ON condition and every one
 * of its actions, a function's arguments in FROM (a TABLESAMPLE percentage is one of those), a
 * WINDOW clause entry, a partition bound, a CREATE VIEW whose body is a VALUES list, and a SQL
 * function's body. Each of them either ran the statement — the ON CONFLICT and MERGE cases wrote
 * rows — or reported something else entirely.
 *
 * <p>Four names were also wrong. GROUPING is not an aggregate: PostgreSQL keeps a parallel set of
 * messages for it, and FILTER, GROUP BY, RETURNING and a window frame offset all said "aggregate
 * functions" where it says "grouping operations". A window function written without OVER is 42809
 * naming the function — in DML and in a definition as much as in a SELECT, where it was reported
 * as a function that does not exist.
 *
 * <p>The reach comes from one classifying walk asked once per clause rather than a call per site:
 * the walk names whichever kind of call it reaches first, so the same code that catches an
 * aggregate in a MERGE action catches a bare window call in an index predicate. The last two tests
 * are the reason to prefer that over a broader rule: every ordinary shape has to keep working.
 */
class PlacementReachTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE plr_tgt (id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO plr_tgt VALUES (1,10,'x'),(2,20,'y'),(3,10,'z')");
        exec("CREATE TABLE plr_src (id int PRIMARY KEY, a int, c text)");
        exec("INSERT INTO plr_src VALUES (1,10,'p'),(2,30,'q')");
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

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    // ---- ON CONFLICT ----

    @Test
    void onConflictDoUpdateIsAnUpdateAndItsWhereIsAWhere() throws Exception {
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id) DO UPDATE SET a = count(*)",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id)"
                        + " DO UPDATE SET a = row_number() OVER ()",
                "42P20", "window functions are not allowed in UPDATE");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id)"
                        + " DO UPDATE SET a = grouping(plr_tgt.a)",
                "42803", "grouping operations are not allowed in UPDATE");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id)"
                        + " DO UPDATE SET a = 5 WHERE count(*) > 1",
                "42803", "aggregate functions are not allowed in WHERE");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id)"
                        + " DO UPDATE SET a = 5 WHERE row_number() OVER () = 1",
                "42P20", "window functions are not allowed in WHERE");
        // and nothing was written
        assertEquals("1|10|x", rows("SELECT id, a, b FROM plr_tgt WHERE id = 1"));
    }

    /** The conflict target names an index, so it is an index expression and an index predicate. */
    @Test
    void anOnConflictTargetIsAnIndexDefinition() {
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'m') ON CONFLICT ((count(id))) DO NOTHING",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'m') ON CONFLICT ((a + count(id))) DO NOTHING",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'m') ON CONFLICT (id) WHERE count(*) > 0 DO NOTHING",
                "42803", "aggregate functions are not allowed in index predicates");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'m') ON CONFLICT (id)"
                        + " WHERE row_number() OVER () = 1 DO NOTHING",
                "42P20", "window functions are not allowed in index predicates");
    }

    // ---- MERGE ----

    @Test
    void mergeNamesItsOnClauseItsWhenConditionAndEachAction() throws Exception {
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON count(t.id) = 1"
                        + " WHEN MATCHED THEN UPDATE SET a = 1",
                "42803", "aggregate functions are not allowed in JOIN conditions");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON row_number() OVER () = 1"
                        + " WHEN MATCHED THEN UPDATE SET a = 1",
                "42P20", "window functions are not allowed in JOIN conditions");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON grouping(t.id) = 0"
                        + " WHEN MATCHED THEN DELETE",
                "42803", "grouping operations are not allowed in JOIN conditions");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN MATCHED AND count(*) > 1 THEN UPDATE SET a = 1",
                "42803", "aggregate functions are not allowed in MERGE WHEN conditions");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN MATCHED AND row_number() OVER () = 1 THEN DELETE",
                "42P20", "window functions are not allowed in MERGE WHEN conditions");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN MATCHED THEN UPDATE SET a = count(*)",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN NOT MATCHED THEN INSERT VALUES (count(*), 1, 'x')",
                "42803", "aggregate functions are not allowed in VALUES");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN NOT MATCHED THEN INSERT VALUES (row_number() OVER (), 1, 'x')",
                "42P20", "window functions are not allowed in VALUES");
        // no row of either table was touched
        assertEquals("1|10|x,2|20|y,3|10|z", rows("SELECT id, a, b FROM plr_tgt"));
    }

    /**
     * The action is judged before its column list: PostgreSQL reports the aggregate in a MERGE's
     * UPDATE even when the column it assigns to does not exist.
     */
    @Test
    void aMergeActionIsJudgedBeforeItsColumnsAreResolved() {
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN MATCHED THEN UPDATE SET nosuchcol = count(*)",
                "42803", "aggregate functions are not allowed in UPDATE");
    }

    // ---- a function in FROM ----

    @Test
    void aFunctionInFromIsSettledBeforeThereIsARowToRead() {
        assertRejected("SELECT * FROM plr_tgt, generate_series(1, count(plr_tgt.a))",
                "42803", "aggregate functions are not allowed in functions in FROM");
        assertRejected("SELECT count(*) FROM generate_series(1, count(*)) g",
                "42803", "aggregate functions are not allowed in functions in FROM");
        assertRejected("SELECT * FROM generate_series(1, row_number() OVER ())",
                "42P20", "window functions are not allowed in functions in FROM");
        assertRejected("SELECT * FROM generate_series(1, grouping(1))",
                "42803", "grouping operations are not allowed in functions in FROM");
        // a TABLESAMPLE percentage is carried as a function item and is the same clause
        assertRejected("SELECT * FROM plr_tgt TABLESAMPLE BERNOULLI (count(*))",
                "42803", "aggregate functions are not allowed in functions in FROM");
    }

    /** A relation that does not exist is still reported first, as PostgreSQL reports it. */
    @Test
    void aMissingRelationStillOutranksTheClause() {
        assertRejected("SELECT * FROM plr_nosuch JOIN plr_src ON count(*) = 1",
                "42P01", "relation \"plr_nosuch\" does not exist");
    }

    // ---- a WINDOW clause entry ----

    @Test
    void aWindowDefinitionMayNotHoldAWindowCall() {
        assertRejected("SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (PARTITION BY row_number() OVER ())",
                "42P20", "window functions are not allowed in window definitions");
        assertRejected("SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (ORDER BY row_number() OVER ())",
                "42P20", "window functions are not allowed in window definitions");
        // an entry nothing references is judged all the same
        assertRejected("SELECT 1 FROM plr_tgt WINDOW w AS (PARTITION BY row_number() OVER ())",
                "42P20", "window functions are not allowed in window definitions");
        assertRejected("SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (PARTITION BY row_number())",
                "42809", "window function row_number requires an OVER clause");
    }

    /** …but an aggregate in one is ordinary: it is read once per result row. */
    @Test
    void aWindowDefinitionMayHoldAnAggregate() throws Exception {
        assertEquals("1", rows("SELECT count(*) OVER w FROM plr_tgt WINDOW w AS (PARTITION BY count(*))"));
        assertEquals("10|1,20|1", rows(
                "SELECT a, count(*) OVER w FROM plr_tgt GROUP BY a WINDOW w AS (PARTITION BY count(*))"));
        assertEquals("10|2,20|2", rows(
                "SELECT a, count(*) OVER (PARTITION BY grouping(a)) FROM plr_tgt GROUP BY a"));
    }

    // ---- a partition bound ----

    @Test
    void aPartitionBoundIsSettledWhenThePartitionIsCreated() throws Exception {
        exec("CREATE TABLE plr_part (a int) PARTITION BY RANGE (a)");
        assertRejected("CREATE TABLE plr_pa PARTITION OF plr_part FOR VALUES FROM (count(1)) TO (10)",
                "42803", "aggregate functions are not allowed in partition bound");
        assertRejected("CREATE TABLE plr_pb PARTITION OF plr_part"
                        + " FOR VALUES FROM (row_number() OVER ()) TO (10)",
                "42P20", "window functions are not allowed in partition bound");
        exec("CREATE TABLE plr_lst (a int) PARTITION BY LIST (a)");
        assertRejected("CREATE TABLE plr_lsta PARTITION OF plr_lst FOR VALUES IN (count(1))",
                "42803", "aggregate functions are not allowed in partition bound");
        // an ordinary constant expression is a bound like any other
        exec("CREATE TABLE plr_pc PARTITION OF plr_part FOR VALUES FROM (abs(-1)) TO (1 + 9)");
        exec("INSERT INTO plr_part VALUES (5)");
        assertEquals("5", rows("SELECT a FROM plr_pc"));
        exec("DROP TABLE plr_part CASCADE");
        exec("DROP TABLE plr_lst CASCADE");
    }

    // ---- a view whose body is a VALUES list ----

    @Test
    void aViewBodyMayBeAValuesList() throws Exception {
        assertRejected("CREATE VIEW plr_vbad AS VALUES (count(*))",
                "42803", "aggregate functions are not allowed in VALUES");
        assertRejected("CREATE VIEW plr_vbad AS VALUES (row_number() OVER ())",
                "42P20", "window functions are not allowed in VALUES");
        exec("CREATE VIEW plr_vvalues AS VALUES (1), (2)");
        assertEquals("1,2", rows("SELECT * FROM plr_vvalues"));
        exec("DROP VIEW plr_vvalues");
    }

    // ---- a SQL function body ----

    @Test
    void aSqlFunctionBodyIsAnalysedWhenTheFunctionIsWritten() throws Exception {
        assertRejected("CREATE FUNCTION plr_fbad() RETURNS int AS $$"
                        + " SELECT a FROM plr_tgt WHERE count(a) > 1 $$ LANGUAGE sql",
                "42803", "aggregate functions are not allowed in WHERE");
        assertRejected("CREATE FUNCTION plr_fbad() RETURNS bigint AS $$"
                        + " SELECT row_number() $$ LANGUAGE sql",
                "42809", "window function row_number requires an OVER clause");
        // a body that says something is still stored and still runs
        exec("CREATE FUNCTION plr_fok() RETURNS bigint AS $$ SELECT count(*) FROM plr_tgt $$ LANGUAGE sql");
        assertEquals("3", rows("SELECT plr_fok()"));
        exec("DROP FUNCTION plr_fok()");
    }

    // ---- a sub-query in a definition replayed one row at a time ----

    @Test
    void aDefinitionMayNotReadAnotherRelation() {
        assertRejected("CREATE TABLE plr_chk (id int PRIMARY KEY, v int"
                        + " CHECK (v < (SELECT count(*) FROM plr_src)))",
                "0A000", "cannot use subquery in check constraint");
        assertRejected("CREATE TABLE plr_chk2 (id int PRIMARY KEY, v int CHECK (v < (SELECT 1)))",
                "0A000", "cannot use subquery in check constraint");
        assertRejected("ALTER TABLE plr_tgt ADD CONSTRAINT plr_c1 CHECK (a < (SELECT count(*) FROM plr_src))",
                "0A000", "cannot use subquery in check constraint");
        assertRejected("CREATE INDEX plr_ix1 ON plr_tgt ((a + (SELECT count(*) FROM plr_src)))",
                "0A000", "cannot use subquery in index expression");
        assertRejected("CREATE INDEX plr_ix2 ON plr_tgt (a) WHERE a < (SELECT count(*) FROM plr_src)",
                "0A000", "cannot use subquery in index predicate");
    }

    // ---- the clause names ----

    @Test
    void groupingIsNamedAsItsOwnKindOfOperation() {
        assertRejected("SELECT a FROM plr_tgt GROUP BY grouping(a)",
                "42803", "grouping operations are not allowed in GROUP BY");
        assertRejected("SELECT count(*) FILTER (WHERE grouping(a) = 0) FROM plr_tgt GROUP BY a",
                "42803", "grouping operations are not allowed in FILTER");
        assertRejected("UPDATE plr_tgt SET a = 1 RETURNING grouping(a)",
                "42803", "grouping operations are not allowed in RETURNING");
        assertRejected("SELECT sum(a) OVER (ROWS grouping(a) PRECEDING) FROM plr_tgt GROUP BY a",
                "42803", "grouping operations are not allowed in window ROWS");
        assertRejected("SELECT * FROM plr_tgt JOIN plr_src ON grouping(plr_tgt.a) = 0",
                "42803", "grouping operations are not allowed in JOIN conditions");
    }

    @Test
    void aWindowCallWithoutOverNamesTheMissingClauseInDmlToo() {
        assertRejected("UPDATE plr_tgt SET a = row_number() WHERE id = 1",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("DELETE FROM plr_tgt WHERE row_number() = 1",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("INSERT INTO plr_tgt VALUES (99, row_number(), 'w')",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("INSERT INTO plr_tgt VALUES (1,1,'w') ON CONFLICT (id) DO UPDATE SET a = row_number()",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("MERGE INTO plr_tgt t USING plr_src u ON t.id = u.id"
                        + " WHEN MATCHED THEN UPDATE SET a = lag(u.a)",
                "42809", "window function lag requires an OVER clause");
        assertRejected("UPDATE plr_tgt SET a = 1 RETURNING ntile(2)",
                "42809", "window function ntile requires an OVER clause");
        assertRejected("SELECT * FROM plr_tgt JOIN plr_src ON row_number() = 1",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("SELECT * FROM plr_tgt TABLESAMPLE BERNOULLI (row_number())",
                "42809", "window function row_number requires an OVER clause");
    }

    @Test
    void aWindowCallWithoutOverNamesTheMissingClauseInADefinitionToo() {
        assertRejected("CREATE INDEX plr_wi ON plr_tgt ((lag(a)))",
                "42809", "window function lag requires an OVER clause");
        assertRejected("CREATE INDEX plr_wp ON plr_tgt (a) WHERE row_number() = 1",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("CREATE TABLE plr_wt (x int CHECK (row_number() = 1))",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("CREATE TABLE plr_wd (x int DEFAULT rank())",
                "42809", "window function rank requires an OVER clause");
    }

    /**
     * Which call is named is decided by which one the walk reaches first, because PostgreSQL
     * analyses an expression as it reads it.
     */
    @Test
    void theFirstMisplacedCallReachedIsTheOneNamed() {
        assertRejected("UPDATE plr_tgt SET a = count(*) + row_number()",
                "42803", "aggregate functions are not allowed in UPDATE");
        assertRejected("UPDATE plr_tgt SET a = row_number() + count(*)",
                "42809", "window function row_number requires an OVER clause");
        assertRejected("SELECT * FROM plr_tgt WHERE grouping(a) = 0 AND count(*) > 1",
                "42803", "grouping operations are not allowed in WHERE");
        // an aggregate under a window call is reached second, so the window call is still named
        assertRejected("SELECT * FROM plr_tgt WHERE count(*) OVER () > 1",
                "42P20", "window functions are not allowed in WHERE");
    }

    // ---- the shapes every real query is made of ----

    @Test
    void ordinaryGroupedAndWindowedQueriesAreUntouched() throws Exception {
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM plr_tgt GROUP BY a"));
        assertEquals("10|2", rows(
                "SELECT a, count(*) c FROM plr_tgt GROUP BY a HAVING count(*) > 1 ORDER BY c"));
        assertEquals("10,20", rows("SELECT DISTINCT a FROM plr_tgt"));
        assertEquals("10|x,20|y", rows("SELECT DISTINCT ON (a) a, b FROM plr_tgt ORDER BY a, b"));
        assertEquals("10|1,10|1,20|3", rows("SELECT a, rank() OVER (ORDER BY a) FROM plr_tgt"));
        assertEquals("10|1,20|2", rows(
                "SELECT a, rank() OVER (ORDER BY count(*) DESC) FROM plr_tgt GROUP BY a"));
        assertEquals("10|0,20|0,null|1", rows(
                "SELECT a, grouping(a) FROM plr_tgt GROUP BY ROLLUP(a) ORDER BY 1, 2").toLowerCase());
        assertEquals("1,2,3", rows(
                "SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY id) rn FROM plr_tgt) sub"
                        + " WHERE sub.rn >= 1"));
        assertEquals("10|2,20|1", rows(
                "WITH c AS (SELECT a, count(*) n FROM plr_tgt GROUP BY a) SELECT * FROM c"));
        assertEquals("10|1,20|1", rows(
                "SELECT t.a, count(u.id) FROM plr_tgt t JOIN plr_src u ON t.id = u.id GROUP BY t.a"));
        assertEquals("1|10,2|30", rows(
                "SELECT t.id, l.m FROM plr_tgt t,"
                        + " LATERAL (SELECT max(u.a) m FROM plr_src u WHERE u.id = t.id) l"
                        + " WHERE l.m IS NOT NULL"));
        assertEquals("10,20,30", rows("SELECT a FROM plr_tgt UNION SELECT a FROM plr_src"));
        assertEquals("1,2", rows(
                "SELECT * FROM generate_series(1, (SELECT count(*)::int FROM plr_src)) g"));
        assertEquals("1|10|x,2|20|y,3|10|z", rows("SELECT * FROM plr_tgt TABLESAMPLE SYSTEM (100)"));
        assertEquals("2", rows("SELECT count(*) FILTER (WHERE a > 0) FROM plr_tgt GROUP BY a HAVING a = 10"));
    }

    @Test
    void writingStatementsThatMeanSomethingStillRun() throws Exception {
        exec("CREATE TABLE plr_run (id int PRIMARY KEY, n int)");
        exec("INSERT INTO plr_run VALUES (1, 1)");
        exec("INSERT INTO plr_run VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = excluded.n"
                + " WHERE plr_run.n < excluded.n");
        assertEquals("1|5", rows("SELECT id, n FROM plr_run"));
        exec("MERGE INTO plr_run t USING plr_src u ON t.id = u.id"
                + " WHEN MATCHED THEN UPDATE SET n = t.n + 1"
                + " WHEN NOT MATCHED THEN INSERT VALUES (u.id, u.a)");
        assertEquals("1|6,2|30", rows("SELECT id, n FROM plr_run"));
        exec("MERGE INTO plr_run t USING (SELECT id, count(*)::int c FROM plr_src GROUP BY id) u"
                + " ON t.id = u.id WHEN MATCHED THEN UPDATE SET n = u.c");
        assertEquals("1|1,2|1", rows("SELECT id, n FROM plr_run"));
        exec("INSERT INTO plr_run SELECT 9, count(*)::int FROM plr_tgt");
        assertEquals("1|1,2|1,9|3", rows("SELECT id, n FROM plr_run"));
        exec("CREATE INDEX plr_okix ON plr_run ((n + 1)) WHERE n > 0");
        exec("CREATE VIEW plr_okv AS SELECT n, count(*) c FROM plr_run GROUP BY n");
        assertEquals("1|2,3|1", rows("SELECT n, c FROM plr_okv"));
        exec("DROP VIEW plr_okv");
        exec("DROP TABLE plr_run");
    }
}
