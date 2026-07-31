package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which error a statement with more than one fault reports.
 *
 * <p>PostgreSQL analyses a query in a fixed order and that order decides the answer. Raw parse runs
 * first, so a syntax error beats every lookup. Then the range table is built, so a relation that
 * does not exist beats every complaint about a clause. Only then is the rest of the query
 * transformed against it, and within a single function call {@code transformFuncCall} transforms
 * the arguments, then the FILTER expression (coercing it to boolean), and only then resolves the
 * function — which is why {@code abs(nosuchcol) FILTER (WHERE true)} is 42703, {@code abs(id)
 * FILTER (WHERE 1)} is 42804, {@code "ABS"(1) FILTER (…)} is 42883, and only a call that resolves
 * to a real non-aggregate earns 42809.
 *
 * <p>memgres judges a query level's clauses after its FROM clause has been resolved, and resolves
 * what a call names before refusing the call, so those orderings now agree. The cases where the two
 * engines still differ are asserted too — against what memgres does today, each one carrying the
 * answer PostgreSQL gives — so that the gap is measured rather than forgotten, and so that closing
 * one makes this test fail and say so.
 */
class ErrorPrecedenceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS ept_t CASCADE");
        exec("CREATE TABLE ept_t (id int PRIMARY KEY, v int, txt text, b boolean)");
        exec("INSERT INTO ept_t VALUES (1,10,'a',true),(2,20,'b',false)");
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

    /** The first column of the first row, as text. */
    private static String rowsOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "");
        }
    }

    // =========================================================================
    // Where the order already matches PostgreSQL
    // =========================================================================

    @Test
    void aSyntaxErrorOutranksEveryLookup() {
        assertEquals("42601", stateOf("SELECT abs(v) FILTER (WHERE b) FROM ept_t WHERE"));
    }

    @Test
    void aMissingRelationOutranksTheClauseChecksThatAlreadyWait() {
        assertEquals("42P01", stateOf("SELECT abs(id) OVER () FROM ept_nosuch"));
        assertEquals("42P01", stateOf("SELECT abs(1) WITHIN GROUP (ORDER BY v) FROM ept_nosuch"));
        assertEquals("42P01", stateOf("SELECT id FROM ept_nosuch WHERE count(*) > 0"));
        assertEquals("42P01", stateOf("SELECT v, count(*) FROM ept_nosuch GROUP BY id"));
        assertEquals("42P01", stateOf("SELECT nosuchcol FROM ept_nosuch"));
        assertEquals("42P01", stateOf("SELECT 1 FROM ept_nosuch_a, ept_nosuch_b"));
    }

    /**
     * Only the NAMES of the relations are resolved before the clauses are judged, never their
     * rows: reading a FROM item is observable, and this statement may yet be refused. So a missing
     * relation is reported first without a WITH item that writes ever being applied.
     */
    @Test
    void aMissingRelationOutranksTheClauseChecksToo() {
        assertEquals("42P01", stateOf("SELECT abs(id) FILTER (WHERE true) FROM ept_nosuch"));
        assertEquals("42P01", stateOf("SELECT abs(DISTINCT id) FROM ept_nosuch"));
        assertEquals("42P01",
                stateOf("SELECT id FROM ept_nosuch WHERE generate_series(1,2) > 0"));
        assertEquals("42P01", stateOf(
                "SELECT * FROM ept_nosuch x WHERE EXISTS (SELECT abs(1) FILTER (WHERE true))"),
                "the range table covers the whole statement, sub-queries included");
    }

    /** A statement PostgreSQL refuses performs none of the writes its WITH items describe. */
    @Test
    void aRefusedStatementAppliesNoneOfItsDataModifyingWithItems() throws Exception {
        exec("DROP TABLE IF EXISTS ept_sink CASCADE");
        exec("CREATE TABLE ept_sink (id int PRIMARY KEY)");

        assertEquals("42809", stateOf(
                "WITH ins AS (INSERT INTO ept_sink VALUES (1) RETURNING id) "
                        + "SELECT abs(1) FILTER (WHERE true) FROM ins"));
        assertEquals("0", rowsOf("SELECT count(*) FROM ept_sink"),
                "the INSERT must not have run");

        assertEquals("42P01", stateOf(
                "WITH ins AS (INSERT INTO ept_sink VALUES (2) RETURNING id) "
                        + "SELECT id FROM ept_nosuch"));
        assertEquals("0", rowsOf("SELECT count(*) FROM ept_sink"));
        exec("DROP TABLE IF EXISTS ept_sink CASCADE");
    }

    /**
     * Within one call PostgreSQL transforms the arguments, then the FILTER expression — coercing it
     * to boolean — and only then resolves the function. Each of those faults therefore outranks the
     * complaint that the call is not an aggregate.
     */
    @Test
    void withinOneCallTheArgumentsAndTheFilterAreReadFirst() {
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT abs(nosuchcol) FILTER (WHERE true) FROM ept_t"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT abs(id) FILTER (WHERE nosuchcol) FROM ept_t"));
        assertEquals("42703", stateOf("SELECT abs(nosuchcol) OVER () FROM ept_t"));
        assertEquals("42703", stateOf("SELECT abs(DISTINCT nosuchcol) FROM ept_t"));
        assertEquals("column x.nosuchcol does not exist",
                messageOf("SELECT abs(x.nosuchcol) FILTER (WHERE true) FROM ept_t x"),
                "a qualified reference is named in full");
        assertEquals("42703", stateOf("SELECT id FROM ept_t WHERE count(nosuchcol) > 0"),
                "an aggregate's arguments are read before the clause it may not stand in");
    }

    @Test
    void aFilterPredicateIsCoercedToBooleanBeforeTheCallIsResolved() {
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT abs(id) FILTER (WHERE 1) FROM ept_t"));
        assertEquals("argument of FILTER must be type boolean, not type text",
                messageOf("SELECT abs(v) FILTER (WHERE txt) FROM ept_t"));
        // A FILTER predicate is a condition whatever it hangs off, so an aggregate's is coerced
        // too even though nothing else about the call is wrong.
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT count(v) FILTER (WHERE 1) FROM ept_t"));
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT max(v) FILTER (WHERE v) FROM ept_t"));
    }

    @Test
    void aCallThatResolvesToNothingIsReportedAsThat() {
        assertEquals("function ABS(integer) does not exist",
                messageOf("SELECT \"ABS\"(1) FILTER (WHERE true)"),
                "a quoted name keeps its case, and \"ABS\" is not abs");
        assertEquals("function ABS(integer) does not exist", messageOf("SELECT \"ABS\"(1)"));
        assertEquals("function abs(text) does not exist",
                messageOf("SELECT abs(txt) FILTER (WHERE b) FROM ept_t"),
                "a function is resolved by argument type as well as by name");
        assertEquals("function information_schema.abs(integer) does not exist",
                messageOf("SELECT information_schema.abs(v) FILTER (WHERE true) FROM ept_t"),
                "a qualifier has to name the schema the function is really in");
        assertEquals("function information_schema.abs(integer) does not exist",
                messageOf("SELECT information_schema.abs(-1)"));
    }

    @Test
    void amongFaultsOfOneStageTheEarlierClauseWins() {
        assertEquals("column \"nosuch_a\" does not exist",
                messageOf("SELECT nosuch_a FROM ept_t WHERE nosuch_b > 0"),
                "the select list is transformed before WHERE");
        assertEquals("column \"nosuch_b\" does not exist",
                messageOf("SELECT id FROM ept_t WHERE nosuch_b > 0 ORDER BY nosuch_c"),
                "and WHERE before ORDER BY");
    }

    @Test
    void eachClauseLevelRefusalStillFiresOnItsOwn() {
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("SELECT abs(v) FILTER (WHERE b) FROM ept_t"));
        assertEquals("DISTINCT specified, but abs is not an aggregate function",
                messageOf("SELECT abs(DISTINCT v) FROM ept_t"),
                "DISTINCT inside a call is refused in the same words as FILTER");
        assertEquals("42809", stateOf("SELECT abs(v) OVER () FROM ept_t"));
        assertEquals("42803", stateOf("SELECT id FROM ept_t WHERE count(*) > 0"));
        assertEquals("42803", stateOf("SELECT 1 WHERE count(*) > 0"),
                "a FROM-less query has no range table, but its WHERE is still a clause an "
                        + "aggregate may not stand in");
        assertEquals("0A000", stateOf("SELECT id FROM ept_t WHERE generate_series(1,2) > 0"));
        assertEquals("42883", stateOf("SELECT ept_nosuchfn(id) FILTER (WHERE true) FROM ept_t"));
    }

    @Test
    void theRefusalDoesNotDependOnThereBeingRows() {
        assertEquals("42809", stateOf("SELECT abs(v) FILTER (WHERE b) FROM ept_t LIMIT 0"));
        assertEquals("42809", stateOf("SELECT abs(v) FILTER (WHERE b) FROM ept_t WHERE false"));
        assertEquals("42809", stateOf("SELECT abs(v) FILTER (WHERE b) FROM ept_t WHERE id = -1"));
        assertEquals("42809",
                stateOf("WITH c AS (SELECT abs(1) FILTER (WHERE true)) SELECT * FROM c"));
        assertEquals("42809",
                stateOf("WITH c AS (SELECT abs(1) FILTER (WHERE true)) SELECT 1"),
                "a WITH item nothing reads is analysed all the same");
        assertEquals("42809",
                stateOf("SELECT 'a' AS c WHERE false AND (abs(1) FILTER (WHERE true)) > 0"),
                "and so is a sub-query the WHERE never reaches");
    }

    @Test
    void aScopeThisCannotReadIsAScopeItDoesNotJudge() {
        assertEquals("42809",
                stateOf("SELECT abs(s.v) FILTER (WHERE s.b) FROM (SELECT * FROM ept_t) s"),
                "a derived table supplies columns like any other relation");
        assertEquals("42809",
                stateOf("SELECT abs(g) FILTER (WHERE true) FROM generate_series(1,3) g"),
                "a FROM-function's column is not knowable from the catalog");
        assertEquals("OK", stateOf("SELECT pg_catalog.abs(-1)"));
        assertEquals("OK", stateOf("SELECT \"abs\"(-1)"));
        assertEquals("OK", stateOf("SELECT count(*) FROM pg_class WHERE relname = 'ept_t'"));
    }

    @Test
    void theOrdinaryShapesAreUntouched() throws Exception {
        assertEquals("OK", stateOf("SELECT count(*) FILTER (WHERE b) FROM ept_t"));
        assertEquals("OK", stateOf("SELECT count(DISTINCT v) FROM ept_t"));
        assertEquals("OK", stateOf("SELECT count(*) FILTER (WHERE b) OVER () FROM ept_t"));
        assertEquals("OK", stateOf("WITH ept_cte AS (SELECT 1 AS x) SELECT x FROM ept_cte"));
        assertEquals("OK",
                stateOf("SELECT s.x FROM (SELECT v AS x FROM ept_t WHERE id = 1) s"));
    }

    // =========================================================================
    // Where memgres still reports the later fault — measured, not yet fixed
    // =========================================================================

    /**
     * Each of these is a statement with two faults where PostgreSQL reports the earlier one and
     * memgres reports the later. They are asserted against memgres's present answer so the branch
     * is honest about its own scope: closing one of them fails this test, which is the intent.
     *
     * <p>What they have in common is a fault memgres finds only by running something. A column
     * reference is still resolved a row at a time everywhere except a query level's own clauses,
     * so a query that reads no rows never reaches it.
     *
     * <p>Two groups that stood here are now the other way round. The relation a data-modifying
     * statement writes is resolved before its clauses are judged, so those report 42P01 and are
     * asserted in {@code DmlErrorPrecedenceTest}. And a FILTER predicate is coerced to boolean
     * wherever it hangs, an aggregate's included, so those are asserted above.
     */
    @Test
    void theCasesStillOutOfOrderAreRecordedRatherThanAsserted() {
        // PostgreSQL: 42703 — a column reference nested in an expression is resolved when the
        // clause is analysed, not when a row reaches it.
        assertEquals("OK", stateOf("SELECT abs(nosuchcol) FROM ept_t WHERE false"));
        assertEquals("42809", stateOf(
                "SELECT * FROM ept_t t WHERE EXISTS (SELECT abs(t.nosuchcol) FILTER (WHERE true))"),
                "a sub-query's own scope is not this query level's, so it is not judged here");
    }
}
