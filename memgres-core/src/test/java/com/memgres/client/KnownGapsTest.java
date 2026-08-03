package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What memgres does not yet answer the way PostgreSQL 18 does.
 *
 * <p><b>This test is expected to fail.</b> Every assertion is PostgreSQL's own answer, measured on
 * the reference server, for a case that branch 3 and its predecessors deliberately left open. It is
 * a to-do list that reports itself: a gap that closes turns an assertion green, and a gap that
 * reopens turns one red again.
 *
 * <p>Each group's comment says why it was left, and in every case the reason was the same — the
 * obvious fix refused SQL PostgreSQL runs, and that is the worse mistake. The companion file
 * known-gaps.sql carries the same cases through the differential harness.
 */
class KnownGapsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS kgt_u CASCADE");
        exec("DROP TABLE IF EXISTS kgt_t CASCADE");
        exec("DROP DOMAIN IF EXISTS kgt_yn CASCADE");
        exec("CREATE DOMAIN kgt_yn AS boolean");
        exec("CREATE TABLE kgt_t (id int PRIMARY KEY, v int, n int, txt text, y kgt_yn)");
        exec("INSERT INTO kgt_t VALUES (1,1,1,'aa',true),(2,2,0,'ab',false)");
        exec("CREATE TABLE kgt_u (id int PRIMARY KEY, v int)");
        exec("INSERT INTO kgt_u VALUES (1,1)");
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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /**
     * PostgreSQL types a derived table's, a CTE's and a FROM-function's columns and refuses a
     * non-boolean condition over one. memgres cannot: the binding it builds for a derived relation
     * carries an inferred type, often the wrong one, and refusing on the strength of that rejected
     * ordinary joins and sub-queries. BooleanContext therefore answers "unknown".
     */
    @Test
    void aConditionOverADerivedColumnIsTyped() {
        assertEquals("42804", stateOf("SELECT id FROM (SELECT id, n FROM kgt_t) q WHERE n"));
        assertEquals("42804",
                stateOf("WITH c AS (SELECT id, n FROM kgt_t) SELECT id FROM c WHERE n"));
        assertEquals("42804", stateOf("SELECT g FROM generate_series(1,2) g WHERE g"));
    }

    /**
     * PostgreSQL resolves a call before judging the clause on it. Deciding "this name is no
     * function" without evaluating needs a complete register of the names memgres can call, and
     * BuiltinFunctionNames is not one — some five hundred engine case labels are absent from it.
     */
    @Test
    void aNameThatIsNoFunctionOutranksTheClauseItCarries() {
        assertEquals("function kgt_nosuchfn(integer) does not exist",
                messageOf("SELECT kgt_nosuchfn(1), abs(1) FILTER (WHERE true) FROM kgt_t"));
        assertEquals("function kgt_nosuchfn(integer) does not exist",
                messageOf("SELECT kgt_nosuchfn(1), nosuchcol FROM kgt_t"));
        assertEquals("function kgt_nosuchfn(integer) does not exist",
                messageOf("SELECT kgt_nosuchfn(v) OVER () FROM kgt_t"));
    }

    /**
     * The ordered walk that gets this right only runs once a refusal at that query level has
     * already been found, so it can never refuse a statement that was going to succeed. Running it
     * unconditionally cost 340 test failures: the scope predicates are one-sided and unsound for
     * FROM-function bindings.
     */
    @Test
    void theEarliestClauseAndTheLeftmostFaultWin() {
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM kgt_t ORDER BY nosuch3"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM kgt_t WHERE sum(v) > 1"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT v FROM kgt_t WHERE nosuch2 = 1 AND sum(v) > 1"));
        assertEquals("column \"nosuch_c\" does not exist",
                messageOf("SELECT v FROM kgt_t GROUP BY nosuch_b HAVING nosuch_c > 1"),
                "GROUP BY is transformed last, so HAVING's fault is reported");
    }

    /**
     * The arity rule refuses only more arguments than the longest recorded signature, because
     * BuiltinFunctionSignatures records several names only in their long form and reading "too
     * few" out of it refused working SQL. So a short call reaches the implementation and crashes.
     */
    @Test
    void tooFewArgumentsIsAMissingFunction() {
        assertEquals("42883", stateOf("SELECT lpad('a')"));
        assertEquals("42883", stateOf("SELECT split_part('a,b', ',')"));
        assertEquals("42883", stateOf("SELECT age()"));
        assertEquals("42883", stateOf("SELECT random(1)"));
    }

    /** Finding #14. A qualified function name already reports 3F000; these two do not. */
    @Test
    void aSchemaThatDoesNotExistIsReportedAsThat() {
        assertEquals("3F000", stateOf("SELECT kgt_noschema.abs(1)"), "this one already agrees");
        assertEquals("3F000", stateOf("SELECT 1::kgt_noschema.int4"));
        assertEquals("3F000", stateOf("CREATE INDEX kgt_ix ON kgt_noschema.kgt_t (id)"));
    }

    /**
     * abs('x'::text) has a certain argument type and no matching signature, but the resolution
     * check runs only where something else has already gone wrong.
     */
    @Test
    void aCallWithNoMatchingSignatureIsResolvedEvenWhenNothingElseIsWrong() {
        assertEquals("function abs(text) does not exist", messageOf("SELECT abs('x'::text)"));
    }

    /**
     * DdlTableExecutor and DdlObjectExecutor call the placement check and never FilterCheck, so a
     * stored definition accepts clauses only an aggregate may carry.
     */
    @Test
    void aStoredDefinitionIsJudgedLikeAnyOtherExpression() {
        assertEquals("42809",
                stateOf("CREATE TABLE kgt_g1 (i int CHECK (abs(i) FILTER (WHERE true) > 0))"));
        assertEquals("42809",
                stateOf("CREATE TABLE kgt_g2 (i int CHECK (abs(DISTINCT i) > 0))"));
        assertEquals("42809",
                stateOf("CREATE TABLE kgt_g3 (i int CHECK (abs(i ORDER BY i) > 0))"));
        assertEquals("42809",
                stateOf("CREATE INDEX kgt_g4 ON kgt_t ((abs(id ORDER BY id)))"));
        assertEquals("42809",
                stateOf("CREATE TABLE kgt_g5 (i int DEFAULT (abs(1 ORDER BY 1)))"));
    }

    /** The parser refuses the combination before the missing signature is ever reached. */
    @Test
    void anAggregateOrderByOnAWindowCallIsAMissingFunction() {
        assertEquals("function row_number(integer) does not exist",
                messageOf("SELECT row_number(v ORDER BY v) OVER () FROM kgt_t"));
    }

    /**
     * The relation is in the statement but not in this part of it, which PostgreSQL words
     * differently from a name that is nowhere. memgres makes the distinction in one path only.
     */
    @Test
    void anOutOfScopeFromEntryIsWordedAsThat() {
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM kgt_t a JOIN (SELECT a.v) b ON true"));
    }

    /**
     * upper is overloaded, so the "every signature returns one type" rule stays silent and the
     * runtime coercion reports the input rather than the type.
     */
    @Test
    void aJoinConditionNamesTheTypeRatherThanTheValue() {
        assertEquals("argument of JOIN/ON must be type boolean, not type text",
                messageOf("SELECT count(*) FROM kgt_t a JOIN kgt_t b ON upper('a')"));
    }

    /** pg_typeof reads enum, composite and array element names, but not a domain's. */
    @Test
    void aDomainAnswersToItsOwnName() throws Exception {
        assertEquals("kgt_yn", scalar("SELECT pg_typeof(y)::text FROM kgt_t LIMIT 1"));
        assertEquals("kgt_yn",
                scalar("SELECT pg_typeof(b)::text FROM (SELECT y AS b FROM kgt_t) q LIMIT 1"));
    }

    /** Two that already agree, kept so that they stay agreed. */
    @Test
    void theOnesThatAlreadyAgree() throws Exception {
        assertEquals("42702",
                stateOf("UPDATE kgt_t SET v = v FROM kgt_u WHERE kgt_t.id = kgt_u.id"));
        assertEquals("boolean",
                scalar("SELECT pg_typeof(f)::text FROM (SELECT starts_with('a','a') AS f) q"));
    }
}
