package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Which error a statement with several faults reports, outside the SELECT path.
 *
 * <p>PostgreSQL builds the range table before it reads any clause, and for a statement that writes
 * it puts the written relation into that table first: {@code INSERT INTO nosuch VALUES (abs(1)
 * FILTER (WHERE true))} is 42P01, not 42809. It validates the written column list against that
 * relation next, before the VALUES or the SELECT behind them, so a column the target does not have
 * is 42703. {@code ErrorPrecedenceTest} pins the same idea for SELECT; the four statement kinds
 * here resolve their target inside their own executor rather than through the FROM resolver, which
 * is where the ordering used to stop.
 *
 * <p>The rest is about resolving a call. A function is resolved by name and argument list together,
 * so a call with more arguments than any signature of that name resolves to nothing at all rather
 * than to the function with the extra ones dropped — {@code upper('a','b')} answered {@code A} and
 * {@code sum(v,v)} answered the sum of the first. And a qualifier is resolved to a schema before
 * anything is looked for inside it, so a qualifier naming no schema is 3F000.
 *
 * <p>Every assertion here was measured against PostgreSQL 18. The ordinary shapes each rule stands
 * next to are asserted with it, because the mistake that matters is refusing SQL PostgreSQL runs.
 */
class DmlErrorPrecedenceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS dep_t CASCADE");
        exec("DROP TABLE IF EXISTS dep_u CASCADE");
        exec("CREATE TABLE dep_t (v int PRIMARY KEY, s text, b boolean)");
        exec("CREATE TABLE dep_u (v int PRIMARY KEY, s text)");
        exec("INSERT INTO dep_t VALUES (1,'a',true),(2,'b',false)");
        exec("INSERT INTO dep_u VALUES (1,'a'),(2,'b')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The first line of the message a statement raises, or "OK". */
    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "");
        }
    }

    /** The first column of the first row, as text. */
    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    // =========================================================================
    // The relation a statement writes is resolved before its clauses are judged
    // =========================================================================

    @Test
    void aMissingTargetOutranksAClauseLevelRefusal() {
        assertEquals("relation \"dep_nosuch\" does not exist",
                messageOf("INSERT INTO dep_nosuch VALUES (9, abs(1) FILTER (WHERE true))"));
        assertEquals("42P01",
                stateOf("INSERT INTO dep_nosuch VALUES (9, abs(DISTINCT 1))"));
        assertEquals("42P01",
                stateOf("INSERT INTO dep_nosuch VALUES (9) RETURNING abs(1) FILTER (WHERE true)"));
        assertEquals("42P01", stateOf("INSERT INTO dep_nosuch VALUES (9)"
                + " ON CONFLICT (v) DO UPDATE SET a = abs(1) FILTER (WHERE true)"));
        assertEquals("42P01",
                stateOf("UPDATE dep_nosuch SET a = abs(1) FILTER (WHERE true)"));
        assertEquals("42P01",
                stateOf("UPDATE dep_nosuch SET a = 1 WHERE abs(1) FILTER (WHERE true) = 1"));
        assertEquals("42P01",
                stateOf("DELETE FROM dep_nosuch WHERE abs(1) FILTER (WHERE true) = 1"));
        assertEquals("42P01",
                stateOf("DELETE FROM dep_nosuch RETURNING abs(1) FILTER (WHERE true)"));
        assertEquals("42P01", stateOf("MERGE INTO dep_nosuch t USING dep_u s ON t.v = s.v"
                + " WHEN MATCHED THEN UPDATE SET a = abs(1) FILTER (WHERE true)"));
    }

    @Test
    void aRelationOnlyReadIsResolvedTheSameWay() {
        assertEquals("relation \"dep_nosuch\" does not exist",
                messageOf("MERGE INTO dep_t t USING dep_nosuch s ON t.v = s.v"
                        + " WHEN MATCHED THEN UPDATE SET s = (abs(1) FILTER (WHERE true))::text"));
        assertEquals("42P01", stateOf(
                "INSERT INTO dep_t SELECT abs(1) FILTER (WHERE true), 'z', true FROM dep_nosuch"));
        assertEquals("42P01", stateOf(
                "UPDATE dep_t SET v = 1 FROM dep_nosuch WHERE abs(1) FILTER (WHERE true) = 1"));
        assertEquals("42P01", stateOf(
                "DELETE FROM dep_t USING dep_nosuch WHERE abs(1) FILTER (WHERE true) = 1"));
    }

    // =========================================================================
    // The written column list is validated next
    // =========================================================================

    @Test
    void aMissingTargetColumnOutranksAClauseLevelRefusal() {
        assertEquals("column \"nosuchcol\" of relation \"dep_t\" does not exist",
                messageOf("INSERT INTO dep_t (nosuchcol) VALUES (abs(1) FILTER (WHERE true))"));
        assertEquals("42703",
                stateOf("INSERT INTO dep_t (nosuchcol) VALUES (abs(DISTINCT 1))"));
        assertEquals("42703", stateOf("INSERT INTO dep_t (v, nosuchcol) VALUES (9, 1)"));
        assertEquals("42703", stateOf(
                "INSERT INTO dep_t (nosuchcol) SELECT abs(1) FILTER (WHERE true) FROM dep_t"));
    }

    @Test
    void theWhereOfAnUpdateOrDeleteIsResolvedBeforeItsAssignments() {
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("UPDATE dep_t SET v = 1 WHERE nosuchcol = abs(1) FILTER (WHERE true)"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("DELETE FROM dep_t WHERE nosuchcol = abs(1) FILTER (WHERE true)"));
    }

    @Test
    void aRefusedStatementWritesNothing() throws Exception {
        exec("DROP TABLE IF EXISTS dep_sink CASCADE");
        exec("CREATE TABLE dep_sink (id int PRIMARY KEY)");
        assertEquals("42P01", stateOf("WITH ins AS (INSERT INTO dep_sink VALUES (1) RETURNING id)"
                + " INSERT INTO dep_nosuch SELECT id FROM ins"));
        assertEquals("0", scalar("SELECT count(*)::text FROM dep_sink"));
        assertEquals("42703", stateOf("WITH ins AS (INSERT INTO dep_sink VALUES (2) RETURNING id)"
                + " INSERT INTO dep_sink (nosuchcol) SELECT id FROM ins"));
        assertEquals("0", scalar("SELECT count(*)::text FROM dep_sink"));
        exec("DROP TABLE IF EXISTS dep_sink CASCADE");
    }

    // =========================================================================
    // A call is resolved by name and argument list together
    // =========================================================================

    @Test
    void aCallWithMoreArgumentsThanAnySignatureResolvesToNothing() {
        assertEquals("function abs(integer, integer) does not exist", messageOf("SELECT abs(1, 2)"));
        assertEquals("function upper(unknown, unknown) does not exist",
                messageOf("SELECT upper('a', 'b')"));
        assertEquals("function now(integer) does not exist", messageOf("SELECT now(1)"));
        assertEquals("function btrim(unknown, unknown, unknown) does not exist",
                messageOf("SELECT btrim('a', 'b', 'c')"));
        assertEquals("function substring(unknown, integer, integer, integer) does not exist",
                messageOf("SELECT substring('abc', 1, 2, 3)"));
        assertEquals("42883", stateOf("SELECT md5('a', 'b')"));
        assertEquals("42883", stateOf("SELECT chr(65, 66)"));
        assertEquals("42883", stateOf("SELECT round(1, 2, 3)"));
        assertEquals("42883", stateOf("SELECT sqrt(4, 5)"));
        assertEquals("42883", stateOf("SELECT initcap('ab', 'c')"));
        assertEquals("42883", stateOf("SELECT reverse('ab', 'c')"));
    }

    @Test
    void anAggregateIsResolvedByItsArgumentListToo() {
        assertEquals("function sum(integer, integer) does not exist",
                messageOf("SELECT sum(v, v) FROM dep_t"));
        assertEquals("function count(integer, integer) does not exist",
                messageOf("SELECT count(v, v) FROM dep_t"));
        assertEquals("function sum() does not exist", messageOf("SELECT sum() FROM dep_t"));
        assertEquals("42883", stateOf("SELECT max(v, v) FROM dep_t"));
        assertEquals("42883", stateOf("SELECT min(v, v) FROM dep_t"));
        assertEquals("42883", stateOf("SELECT avg(v, v) FROM dep_t"));
        assertEquals("42883", stateOf("SELECT string_agg(s, ',', s) FROM dep_t"));
    }

    @Test
    void aQualifierIsResolvedToASchemaFirst() {
        assertEquals("schema \"dep_nosuchschema\" does not exist",
                messageOf("SELECT dep_nosuchschema.f(1)"));
        assertEquals("3F000", stateOf("SELECT dep_nosuchschema.abs(1)"));
        assertEquals("3F000", stateOf("SELECT dep_nosuchschema.f(1) FILTER (WHERE true)"));
    }

    // =========================================================================
    // Within one query level, the leftmost fault of the earliest clause
    // =========================================================================

    @Test
    void theEarlierClauseAndTheLeftmostFaultInItAreReported() {
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2), abs(1) FILTER (WHERE true) FROM dep_t"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM dep_t WHERE abs(1) FILTER (WHERE true) = 1"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT nosuchcol FROM dep_t WHERE abs(1) FILTER (WHERE true) = 1"));
        // A join condition belongs to the FROM clause, which is built before any clause is read.
        assertEquals("column dep_t.nosuch2 does not exist", messageOf(
                "SELECT abs(1) FILTER (WHERE true) FROM dep_t JOIN dep_u ON dep_t.nosuch2 = dep_u.v"));
    }

    @Test
    void theCallIsResolvedBeforeTheClauseItCarriesIsJudged() {
        assertEquals("function abs(integer, integer) does not exist",
                messageOf("SELECT abs(v, v) FILTER (WHERE true) FROM dep_t"));
        assertEquals("function abs(integer, integer) does not exist",
                messageOf("SELECT abs(v, v) OVER () FROM dep_t"));
        // A cast states a type as certainly as a column does.
        assertEquals("function abs(text) does not exist",
                messageOf("SELECT abs('x'::text) FILTER (WHERE true) FROM dep_t"));
    }

    // =========================================================================
    // The ordinary shapes none of this may touch
    // =========================================================================

    @Test
    void theOrdinaryWritesAreUntouched() throws Exception {
        assertEquals("OK", stateOf("INSERT INTO dep_t (v, s) VALUES (30, 'x')"));
        assertEquals("OK", stateOf("UPDATE dep_t SET s = 'y' WHERE v = 30"));
        assertEquals("OK", stateOf("DELETE FROM dep_t WHERE v = 30"));
        assertEquals("OK", stateOf("INSERT INTO dep_t (v, s) SELECT 31, 'z'"));
        assertEquals("OK", stateOf("DELETE FROM dep_t WHERE v = 31"));
        assertEquals("OK",
                stateOf("WITH c AS (SELECT 40 AS k) INSERT INTO dep_t (v) SELECT k FROM c"));
        assertEquals("OK", stateOf("DELETE FROM dep_t WHERE v = 40"));
        assertEquals("OK", stateOf("INSERT INTO dep_t (v) VALUES (50) ON CONFLICT (v) DO NOTHING"));
        assertEquals("OK", stateOf("INSERT INTO dep_t (v, s) VALUES (50, 'a')"
                + " ON CONFLICT (v) DO UPDATE SET s = excluded.s"));
        assertEquals("OK", stateOf("DELETE FROM dep_t WHERE v = 50"));
        assertEquals("OK", stateOf("MERGE INTO dep_t t USING dep_u s ON t.v = s.v"
                + " WHEN MATCHED THEN UPDATE SET s = s.s"));
        assertEquals("OK", stateOf("UPDATE dep_t SET s = 'q' FROM dep_u u WHERE dep_t.v = u.v"));
        assertEquals("OK", stateOf("DELETE FROM dep_t USING dep_u u WHERE dep_t.v = u.v AND false"));
        assertEquals("2", scalar("SELECT count(*)::text FROM dep_t"));
    }

    @Test
    void aWriteThroughAViewStillResolves() throws Exception {
        exec("DROP VIEW IF EXISTS dep_v CASCADE");
        exec("CREATE VIEW dep_v AS SELECT v, s FROM dep_t");
        assertEquals("OK", stateOf("INSERT INTO dep_v (v, s) VALUES (60, 'v')"));
        assertEquals("OK", stateOf("UPDATE dep_v SET s = 'w' WHERE v = 60"));
        assertEquals("OK", stateOf("DELETE FROM dep_v WHERE v = 60"));
        // A view names its own columns, and the complaint names the view.
        assertEquals("42703", stateOf("INSERT INTO dep_v (nosuchcol) VALUES (1)"));
        exec("DROP VIEW IF EXISTS dep_v CASCADE");
    }

    @Test
    void aWriteToATemporaryTableStillResolves() throws Exception {
        exec("DROP TABLE IF EXISTS dep_tmp");
        exec("CREATE TEMP TABLE dep_tmp (v int PRIMARY KEY)");
        assertEquals("OK", stateOf("INSERT INTO dep_tmp VALUES (1)"));
        assertEquals("OK", stateOf("UPDATE dep_tmp SET v = 2 WHERE v = 1"));
        assertEquals("OK", stateOf("DELETE FROM dep_tmp WHERE v = 2"));
        exec("DROP TABLE IF EXISTS dep_tmp");
    }

    @Test
    void aWriteNamingACommonTableExpressionIsNotLookedForAsARelation() throws Exception {
        assertEquals("OK", stateOf("WITH dep_cte AS (SELECT 70 AS k)"
                + " INSERT INTO dep_t (v) SELECT k FROM dep_cte"));
        assertEquals("OK", stateOf("DELETE FROM dep_t WHERE v = 70"));
    }

    @Test
    void everyArityTheseFunctionsReallyHaveStillResolves() throws Exception {
        assertEquals("1", scalar("SELECT abs(-1)::text"));
        assertEquals("2", scalar("SELECT round(1.5)::text"));
        assertEquals("1.55", scalar("SELECT round(1.554, 2)::text"));
        assertEquals("ab", scalar("SELECT substring('abcdef', 1, 2)"));
        assertEquals("abc", scalar("SELECT substring('abcdef' FROM 1 FOR 3)"));
        assertEquals("__a", scalar("SELECT lpad('a', 3, '_')"));
        assertEquals("a", scalar("SELECT btrim('xax', 'x')"));
        assertEquals("A", scalar("SELECT upper('a')"));
        assertEquals("2", scalar("SELECT log(100)::text"));
        assertEquals("3.0000000000000000", scalar("SELECT log(2, 8)::text"));
        assertEquals("b", scalar("SELECT split_part('a,b,c', ',', 2)"));
    }

    @Test
    void aVariadicFunctionTakesAsManyArgumentsAsItIsGiven() throws Exception {
        assertEquals("abcd", scalar("SELECT concat('a','b','c','d')"));
        assertEquals("a-b-c", scalar("SELECT concat_ws('-','a','b','c')"));
        assertEquals("3", scalar("SELECT greatest(1,2,3)::text"));
        assertEquals("1", scalar("SELECT least(1,2,3)::text"));
        assertEquals("1", scalar("SELECT coalesce(NULL, NULL, 1)::text"));
        assertEquals("a-b", scalar("SELECT format('%s-%s','a','b')"));
    }

    @Test
    void theAggregatesThemselvesStillRun() throws Exception {
        assertEquals("3", scalar("SELECT sum(v)::text FROM dep_t"));
        assertEquals("2", scalar("SELECT count(*)::text FROM dep_t"));
        assertEquals("1", scalar("SELECT (count(*) FILTER (WHERE b))::text FROM dep_t"));
        assertEquals("2", scalar("SELECT count(DISTINCT v)::text FROM dep_t"));
        assertEquals("1,2", scalar("SELECT string_agg(v::text, ',' ORDER BY v) FROM dep_t"));
        assertEquals("2", scalar("SELECT max(v)::text FROM dep_t"));
    }

    @Test
    void aQualifierThatDoesNameASchemaResolvesThroughIt() throws Exception {
        assertEquals("1", scalar("SELECT pg_catalog.abs(-1)::text"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'dep_t'"));
        exec("DROP SCHEMA IF EXISTS dep_s CASCADE");
        exec("CREATE SCHEMA dep_s");
        exec("CREATE TABLE dep_s.dep_q (v int PRIMARY KEY)");
        assertEquals("OK", stateOf("INSERT INTO dep_s.dep_q VALUES (1)"));
        assertEquals("OK", stateOf("DELETE FROM dep_s.dep_q"));
        exec("DROP SCHEMA IF EXISTS dep_s CASCADE");
    }

    /**
     * The clause a fault is reported from, where both faults are plain column references.
     *
     * <p>PostgreSQL transforms one query level in a fixed order — WITH items, the FROM clause and
     * its join conditions, the select list, WHERE, HAVING, ORDER BY, GROUP BY — and a call's
     * arguments before the call itself. Each of these is wrong in two places, and the one reported
     * is the one the earlier clause is wrong about, whatever memgres would have run into first.
     */
    @Test
    void theEarliestClauseAndTheLeftmostFaultAreTheOnesReported() {
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM dep_t ORDER BY nosuch3"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM dep_t WHERE sum(v) > 1"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT dep_nosuchfn(nosuch2) FROM dep_t"),
                "a call's arguments are transformed before the function is resolved");
        assertEquals("column \"nosuch_c\" does not exist",
                messageOf("SELECT v FROM dep_t GROUP BY nosuch_b HAVING nosuch_c > 1"),
                "GROUP BY is transformed last, so HAVING's fault is the one reported");
    }

    // =========================================================================
    // Where memgres still reports the later fault — measured, not yet fixed
    // =========================================================================

    /**
     * A name that is no function outranks the clause it carries, and a call with a number of
     * arguments no signature of that name takes is no function either. Both are PostgreSQL's own
     * answers, measured on the reference server.
     *
     * <p>Neither could be decided outside the evaluator until the two tables the rules read were
     * completed: the register of names memgres can dispatch, which the catalog's list is not, and
     * the signature table, which recorded several names only in the long form PostgreSQL keeps
     * internally. The last three used to reach the implementation and come back as XX000 — an
     * index off the end of the argument list, reported to the client as an internal error.
     */
    @Test
    void aCallIsResolvedBeforeTheClauseCarryingItIsJudged() {
        assertEquals("42883",
                stateOf("SELECT dep_nosuchfn(1), abs(1) FILTER (WHERE true) FROM dep_t"));
        assertEquals("42883", stateOf("SELECT dep_nosuchfn(1), nosuchcol FROM dep_t"));
        assertEquals("42883", stateOf("SELECT dep_nosuchfn(v) OVER () FROM dep_t"));
        assertEquals("42883", stateOf("SELECT lpad('a')"));
        assertEquals("42883", stateOf("SELECT split_part('a,b', ',')"));
        assertEquals("42883", stateOf("SELECT age()"));
        // random takes none or two, and one of them is neither.
        assertEquals("42883", stateOf("SELECT random(1)"));

        // The ordinary shapes each of those stands next to, which PostgreSQL runs.
        assertEquals("OK", stateOf("SELECT abs(1) FROM dep_t"));
        assertEquals("OK", stateOf("SELECT sum(v) FILTER (WHERE true) FROM dep_t"));
        assertEquals("OK", stateOf("SELECT row_number() OVER () FROM dep_t"));
        assertEquals("OK", stateOf("SELECT lpad('a', 2)"));
        assertEquals("OK", stateOf("SELECT lpad('a', 2, 'x')"));
        assertEquals("OK", stateOf("SELECT split_part('a,b', ',', 1)"));
        assertEquals("OK", stateOf("SELECT age(now())"));
        assertEquals("OK", stateOf("SELECT age(now(), now())"));
        assertEquals("OK", stateOf("SELECT random()"));
        assertEquals("OK", stateOf("SELECT random(1, 5)"));
        assertEquals("OK", stateOf("SELECT sin(1), cos(1), coalesce(1, 2), greatest(1, 2)"));
        assertEquals("OK", stateOf("SELECT concat('a', 'b'), format('%s', 'a'), num_nonnulls(1)"));
        assertEquals("OK", stateOf("SELECT json_build_object(), jsonb_build_array()"));
    }

    /**
     * Nothing is left to record here. Each branch of this work closed the cases the other had
     * written down: a name that is no function and a wrong arity are now resolved before the
     * clause on the call is judged, and a query level's clauses are read in PostgreSQL's own
     * order. The orderings they used to record are asserted above and in KnownGapsTest.
     */
}
