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
 * Where a construct may stand, and what a join or a LATERAL exposes.
 *
 * <p>Four subjects, all measured against PostgreSQL 18:
 *
 * <ul>
 *   <li><b>A window call in a clause that cannot hold one.</b> A window function is numbered
 *       against the result rows, so every clause read before those rows exist refuses one and
 *       PostgreSQL names the clause. Two orderings decide what a query wrong in more than one way
 *       reports: an expression is analysed from the leaves up, so a window call inside an aggregate
 *       is what the clause is refused for; and a call's OVER specification is transformed with the
 *       query's other window definitions once every clause has been read, so a column that is not
 *       there in one is never reached.</li>
 *   <li><b>What a LATERAL exposes.</b> A column projected through one is the column it came from,
 *       down to the parts a bare type name does not carry. Typed as text, {@code abs()} over one
 *       answered "function abs(text) does not exist".</li>
 *   <li><b>An ORDER BY among a call's arguments</b> says which order the call accumulates its input
 *       in, which only an aggregate does. PostgreSQL reads DISTINCT, then ORDER BY, then FILTER,
 *       and refuses the call for the first of them it finds.</li>
 *   <li><b>A join condition has to be a condition.</b> {@code ON a.id} joins on nothing;
 *       PostgreSQL coerces the qualification to boolean as it builds the range table.</li>
 * </ul>
 *
 * <p>Every refusal here is paired with the ordinary shapes around it, because the cost of a rule
 * that fires too widely is refusing SQL PostgreSQL runs.
 */
class ClausePlacementAndJoinTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS cpj_t CASCADE");
        exec("DROP TABLE IF EXISTS cpj_u CASCADE");
        exec("DROP TABLE IF EXISTS cpj_typ CASCADE");
        exec("DROP DOMAIN IF EXISTS cpj_yn CASCADE");
        exec("CREATE TABLE cpj_t (v int PRIMARY KEY, s text, flag boolean)");
        exec("INSERT INTO cpj_t VALUES (1,'a',true),(2,'b',false)");
        exec("CREATE TABLE cpj_u (v int PRIMARY KEY, s text)");
        exec("INSERT INTO cpj_u VALUES (1,'x'),(3,'y')");
        exec("CREATE DOMAIN cpj_yn AS boolean");
        exec("CREATE TABLE cpj_typ (id int PRIMARY KEY, i8 bigint, n numeric(10,2), t text,"
                + " vc varchar(10), c char(3), b boolean, d date, u uuid, jb jsonb,"
                + " ar int[], y cpj_yn)");
        exec("INSERT INTO cpj_typ VALUES (1, 4, 5.50, 'txt', 'vc', 'ccc', true,"
                + " date '2020-01-01', '11111111-1111-1111-1111-111111111111'::uuid,"
                + " '{\"a\":1}'::jsonb, ARRAY[1,2], true)");
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

    /** The SQLSTATE and the first line of the message, which is what PostgreSQL is compared on. */
    private static String errorOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState() + " " + e.getMessage().split("\n")[0].replace("ERROR: ", "");
        }
    }

    /** The first row, columns joined with "|", or "(no rows)". */
    private static String rowOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) return "(no rows)";
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) sb.append("|");
                sb.append(rs.getString(i));
            }
            return sb.toString();
        }
    }

    // =========================================================================
    // A window call in a clause that cannot hold one
    // =========================================================================

    @Test
    void everyClauseReadBeforeTheResultRowsExistRefusesAWindowCall() {
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cpj_t WHERE row_number() OVER (ORDER BY v) = 1"));
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cpj_t WHERE v = 1 AND rank() OVER () = 1"));
        assertEquals("42P20 window functions are not allowed in GROUP BY",
                errorOf("SELECT count(*) FROM cpj_t GROUP BY row_number() OVER ()"));
        assertEquals("42P20 window functions are not allowed in HAVING",
                errorOf("SELECT count(*) FROM cpj_t HAVING row_number() OVER () = 1"));
        assertEquals("42P20 window functions are not allowed in JOIN conditions",
                errorOf("SELECT cpj_t.v FROM cpj_t JOIN cpj_u ON row_number() OVER () = cpj_u.v"));
        assertEquals("42P20 window functions are not allowed in LIMIT",
                errorOf("SELECT v FROM cpj_t LIMIT row_number() OVER ()"));
        assertEquals("42P20 window functions are not allowed in OFFSET",
                errorOf("SELECT v FROM cpj_t OFFSET row_number() OVER ()"));
        assertEquals("42P20 window functions are not allowed in check constraints",
                errorOf("CREATE TABLE cpj_chk (i int CHECK (row_number() OVER () = 1))"));
        assertEquals("42P20 window functions are not allowed in index predicates",
                errorOf("CREATE INDEX cpj_ix ON cpj_t (v) WHERE row_number() OVER () = 1"));
        assertEquals("42P20 window functions are not allowed in index expressions",
                errorOf("CREATE INDEX cpj_ix ON cpj_t ((row_number() OVER ()))"));
        assertEquals("42P20 window functions are not allowed in DEFAULT expressions",
                errorOf("CREATE TABLE cpj_chk (i int DEFAULT (row_number() OVER ()))"));
        assertEquals("42P20 window functions are not allowed in VALUES",
                errorOf("INSERT INTO cpj_t VALUES (row_number() OVER (), 'z', true)"));
        assertEquals("42P20 window functions are not allowed in UPDATE",
                errorOf("UPDATE cpj_t SET s = row_number() OVER ()::text"));
        assertEquals("42P20 window functions are not allowed in RETURNING",
                errorOf("DELETE FROM cpj_t WHERE v = 99 RETURNING row_number() OVER ()"));
    }

    /**
     * The OVER specification is transformed with the query's other window definitions, once every
     * clause has been read — so nothing in one is reached while the clause holding the call is
     * being judged. The call's own arguments are transformed first and a fault in one is reported.
     */
    @Test
    void theOverSpecificationIsNotReadWhileTheClauseIsBeingJudged() {
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cpj_t WHERE row_number() OVER (ORDER BY nosuchcol) = 1"));
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cpj_t WHERE row_number() OVER (PARTITION BY nosuchcol) = 1"));
        assertEquals("42703 column \"nosuchcol\" does not exist",
                errorOf("SELECT v FROM cpj_t WHERE lag(nosuchcol) OVER () = 1"));
        assertEquals("42703 column \"nosuchcol\" does not exist",
                errorOf("SELECT v FROM cpj_t WHERE sum(nosuchcol) OVER () = 1"));
    }

    /**
     * An expression is analysed from the leaves up, so the window call inside the aggregate is what
     * the clause is refused for — not the aggregate the clause equally cannot hold.
     */
    @Test
    void aWindowCallInsideAnAggregateIsReachedFirst() {
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT count(*) FROM cpj_t WHERE sum(row_number() OVER ()) > 1"));
        assertEquals("42P20 window functions are not allowed in HAVING",
                errorOf("SELECT count(*) FROM cpj_t HAVING sum(row_number() OVER ()) > 1"));
        // In the select list a window call is allowed, so what is left is the aggregate reading it.
        assertEquals("42803 aggregate function calls cannot contain window function calls",
                errorOf("SELECT sum(row_number() OVER ()) FROM cpj_t"));
    }

    /** The select list and ORDER BY are where a window call may stand. */
    @Test
    void theSelectListAndOrderByHoldWindowCalls() throws Exception {
        assertEquals("1|1", rowOf("SELECT v, row_number() OVER (ORDER BY v) FROM cpj_t ORDER BY v"));
        assertEquals("2", rowOf("SELECT v FROM cpj_t ORDER BY row_number() OVER (ORDER BY v DESC)"));
        assertEquals("1|1", rowOf(
                "SELECT * FROM (SELECT v, row_number() OVER (ORDER BY v) r FROM cpj_t) x WHERE r = 1"));
        assertEquals("1|1", rowOf("WITH w AS (SELECT v, row_number() OVER (ORDER BY v) r FROM cpj_t)"
                + " SELECT * FROM w WHERE r = 1"));
        assertEquals("1|1", rowOf("SELECT v, ntile(2) OVER (ORDER BY v) FROM cpj_t ORDER BY v"));
        assertEquals("2", rowOf("SELECT count(*) OVER () FROM cpj_t"));
        // An aggregate in a window's specification is ordinary: by the time it is read the groups
        // have been collected.
        assertEquals("2", rowOf(
                "SELECT sum(count(*)) OVER (PARTITION BY count(*)) FROM cpj_t GROUP BY v"));
    }

    // =========================================================================
    // What a LATERAL exposes
    // =========================================================================

    @Test
    void aLateralExposesTheTypeTheColumnActuallyHas() throws Exception {
        assertEquals("integer|bigint|numeric|text", rowOf(
                "SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3), pg_typeof(z4) FROM cpj_typ t"
                        + " CROSS JOIN LATERAL (SELECT t.id AS z1, t.i8 AS z2, t.n AS z3, t.t AS z4) l"));
        assertEquals("character varying|character|boolean|date", rowOf(
                "SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3), pg_typeof(z4) FROM cpj_typ t"
                        + " CROSS JOIN LATERAL (SELECT t.vc AS z1, t.c AS z2, t.b AS z3, t.d AS z4) l"));
        assertEquals("uuid|jsonb|integer[]", rowOf(
                "SELECT pg_typeof(z1), pg_typeof(z2), pg_typeof(z3) FROM cpj_typ t"
                        + " CROSS JOIN LATERAL (SELECT t.u AS z1, t.jb AS z2, t.ar AS z3) l"));
    }

    /** An alias list renames what the item exposes, and renaming is all it does. */
    @Test
    void anAliasListRenamesWithoutRetypingWhateverFormTheJoinTakes() throws Exception {
        assertEquals("integer|character varying|integer[]", rowOf(
                "SELECT pg_typeof(p), pg_typeof(q), pg_typeof(r) FROM cpj_typ t"
                        + " CROSS JOIN LATERAL (SELECT t.id, t.vc, t.ar) l(p, q, r)"));
        assertEquals("integer|integer[]", rowOf(
                "SELECT pg_typeof(p), pg_typeof(q) FROM cpj_typ t,"
                        + " LATERAL (SELECT t.id, t.ar) l(p, q)"));
        assertEquals("3", rowOf("SELECT l.p FROM cpj_typ t, LATERAL (SELECT t.id + 2) l(p)"));
    }

    /** The comma form is the same join, so it exposes the same names. */
    @Test
    void theCommaFormOfALateralIsDescribedTheSameWay() throws Exception {
        assertEquals("integer|integer[]", rowOf(
                "SELECT pg_typeof(p), pg_typeof(q) FROM cpj_typ t,"
                        + " LATERAL (SELECT t.id AS p, t.ar AS q) l"));
        assertEquals("integer", rowOf("SELECT pg_typeof(z) FROM cpj_typ t CROSS JOIN LATERAL"
                + " (WITH w AS (SELECT t.id AS y) SELECT y AS z FROM w) l"));
        assertEquals("integer", rowOf(
                "SELECT DISTINCT pg_typeof(g) FROM cpj_typ t CROSS JOIN LATERAL generate_series(1, t.id) AS g"));
        assertEquals("integer", rowOf(
                "SELECT DISTINCT pg_typeof(g) FROM cpj_typ t CROSS JOIN LATERAL unnest(t.ar) AS g"));
    }

    /**
     * The type a LATERAL exposes is what resolves a call written over it, so a call carrying a
     * FILTER it may not have is refused for the FILTER rather than for an argument type it never
     * had.
     */
    @Test
    void aCallOverALateralColumnResolvesAgainstTheRealType() throws Exception {
        assertEquals("42809 FILTER specified, but abs is not an aggregate function", errorOf(
                "SELECT abs(z) FILTER (WHERE true) FROM cpj_typ t"
                        + " CROSS JOIN LATERAL (SELECT t.id AS z) l"));
        assertEquals("42809 FILTER specified, but abs is not an aggregate function", errorOf(
                "SELECT abs(z) FILTER (WHERE true) FROM cpj_typ t, LATERAL (SELECT t.id AS z) l"));
        assertEquals("1", rowOf(
                "SELECT abs(z) FROM cpj_typ t CROSS JOIN LATERAL (SELECT t.id AS z) l"));
    }

    // =========================================================================
    // An ORDER BY among a plain call's arguments
    // =========================================================================

    @Test
    void anOrderByAmongTheArgumentsBelongsToAnAggregate() {
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("SELECT abs(v ORDER BY v) FROM cpj_t"));
        assertEquals("42809 ORDER BY specified, but length is not an aggregate function",
                errorOf("SELECT length(s ORDER BY v) FROM cpj_t"));
        assertEquals("42809 ORDER BY specified, but upper is not an aggregate function",
                errorOf("SELECT upper('a' ORDER BY v) FROM cpj_t"));
        assertEquals("42809 ORDER BY specified, but pg_catalog.abs is not an aggregate function",
                errorOf("SELECT pg_catalog.abs(v ORDER BY v) FROM cpj_t"));
        // The refusal is a property of the call, so it holds wherever the call stands.
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("SELECT v FROM cpj_t WHERE abs(v ORDER BY v) = 1"));
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("SELECT v FROM cpj_t GROUP BY v HAVING abs(v ORDER BY v) = 1"));
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("UPDATE cpj_t SET s = abs(v ORDER BY v)::text"));
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("INSERT INTO cpj_t VALUES (abs(3 ORDER BY 1), 'z', true)"));
    }

    /**
     * PostgreSQL reads a call's clauses in a fixed order — DISTINCT, then ORDER BY, then FILTER —
     * and stops at the first, while a name that is no function at all is a missing function before
     * any of them, and a fault in an argument is reported before the call is judged.
     */
    @Test
    void theClauseNamedIsTheFirstOnePostgresReads() {
        assertEquals("42809 DISTINCT specified, but abs is not an aggregate function",
                errorOf("SELECT abs(DISTINCT v ORDER BY v) FROM cpj_t"));
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("SELECT abs(v ORDER BY v) FILTER (WHERE true) FROM cpj_t"));
        assertEquals("42809 FILTER specified, but abs is not an aggregate function",
                errorOf("SELECT abs(v) FILTER (WHERE true) FROM cpj_t"));
        assertEquals("42883", stateOf("SELECT cpj_nofunc(v ORDER BY v) FROM cpj_t"));
        assertEquals("42703 column \"nosuchcol\" does not exist",
                errorOf("SELECT abs(nosuchcol ORDER BY v) FROM cpj_t"));
        // The ORDER BY expressions themselves are never resolved: the call is refused first.
        assertEquals("42809 ORDER BY specified, but abs is not an aggregate function",
                errorOf("SELECT abs(v ORDER BY nosuchcol) FROM cpj_t"));
    }

    /** An ORDER BY inside an aggregate is what the clause is for. */
    @Test
    void anAggregateKeepsItsOwnOrderBy() throws Exception {
        assertEquals("b,a", rowOf("SELECT string_agg(s, ',' ORDER BY v DESC) FROM cpj_t"));
        assertEquals("{2,1}", rowOf("SELECT array_agg(v ORDER BY v DESC) FROM cpj_t"));
        assertEquals("2", rowOf("SELECT count(v ORDER BY v) FROM cpj_t"));
        assertEquals("3", rowOf("SELECT sum(v ORDER BY v) FROM cpj_t"));
        assertEquals("1.5", rowOf("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM cpj_t"));
        // An aggregate's own ORDER BY and a window frame both fix the order it accumulates in;
        // PostgreSQL has never implemented the combination and neither has this.
        assertEquals("0A000", stateOf("SELECT sum(v ORDER BY v) OVER (ORDER BY v) FROM cpj_t"));
    }

    // =========================================================================
    // A join condition has to be a condition
    // =========================================================================

    @Test
    void aJoinConditionThatIsNotABooleanIsRefused() {
        assertEquals("22P02 invalid input syntax for type boolean: \"x\"",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 'x'"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type integer",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 1"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type integer",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type text",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.s"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type text",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v::text"));
        // A call whose every signature returns the same type is as certain as a column is.
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type timestamp with time zone",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON now()"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type integer",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON length(a.s)"));
        // An outer join is refused for the same reason, before anything else about it comes up.
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type integer",
                errorOf("SELECT count(*) FROM cpj_t a FULL JOIN cpj_u b ON a.v"));
        assertEquals("42804 argument of JOIN/ON must be type boolean, not type text",
                errorOf("SELECT count(*) FROM cpj_t a LEFT JOIN cpj_u b ON a.s"));
    }

    /** AND, OR and NOT each want a condition of their own, and PostgreSQL names the operator. */
    @Test
    void aLogicalOperatorInAJoinConditionIsNamedInItsOwnRight() {
        assertEquals("22P02 invalid input syntax for type boolean: \"x\"",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND 'x'"));
        assertEquals("42804 argument of AND must be type boolean, not type integer",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND b.v"));
        assertEquals("42804 argument of OR must be type boolean, not type text",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v OR b.s"));
        assertEquals("42804 argument of NOT must be type boolean, not type text",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON NOT a.s"));
    }

    /**
     * A call that cannot stand in a join condition at all is refused for that, whatever type it
     * would have had.
     */
    @Test
    void aMisplacedCallInAJoinConditionKeepsItsOwnRefusal() {
        assertEquals("42803 aggregate functions are not allowed in JOIN conditions",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON count(*)"));
        assertEquals("42P20 window functions are not allowed in JOIN conditions",
                errorOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON row_number() OVER () = 1"));
    }

    /** The join conditions PostgreSQL runs, which the rule above must leave alone. */
    @Test
    void theOrdinaryJoinConditionsStillRun() throws Exception {
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v"));
        assertEquals("2", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag"));
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag AND b.flag"));
        assertEquals("2", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_t b ON NOT a.flag"));
        assertEquals("3", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_t b ON a.flag OR b.flag"));
        // A domain over boolean is a boolean.
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_typ a JOIN cpj_typ b ON a.y"));
        // An unadorned string is boolean input, so a boolean word is a condition and 'off' is false.
        assertEquals("4", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 't'"));
        assertEquals("0", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON 'off'"));
        assertEquals("4", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON true"));
        assertEquals("0", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON NULL"));
        assertEquals("1", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v AND a.s <> b.s"));
        assertEquals("2", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN cpj_u b ON CASE WHEN a.v = 1 THEN true ELSE false END"));
        assertEquals("1", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN cpj_u b ON coalesce(a.v = b.v, true)"));
        assertEquals("4", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON isfinite(now())"));
        assertEquals("4", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN cpj_u b ON starts_with(a.s || 'z', a.s)"));
        assertEquals("2", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v IN (SELECT v FROM cpj_u)"));
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b ON a.v = b.v"
                + " JOIN cpj_t c ON c.v = a.v"));
        assertEquals("2", rowOf(
                "SELECT count(*) FROM cpj_t a JOIN LATERAL (SELECT a.v AS w) l ON l.w = a.v"));
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_t a JOIN cpj_u b USING (v)"));
        assertEquals("0", rowOf("SELECT count(*) FROM cpj_t NATURAL JOIN cpj_u"));
        assertEquals("2", rowOf("SELECT count(*) FROM cpj_t a FULL JOIN cpj_t b ON a.v = b.v"));
        assertEquals("2", rowOf("SELECT count(*) FROM cpj_t a JOIN generate_series(1,2) g ON g = a.v"));
        assertEquals("2", rowOf(
                "WITH w AS (SELECT * FROM cpj_t) SELECT count(*) FROM w a JOIN w b ON a.flag"));
    }

    /** Nothing this branch adds may change what a well-formed query answers. */
    @Test
    void theOrdinaryQueriesAroundAllOfThisAreUntouched() throws Exception {
        assertEquals("1|1|1", rowOf("SELECT v, row_number() OVER (ORDER BY v) rn,"
                + " rank() OVER (ORDER BY s) rk FROM cpj_t ORDER BY v"));
        assertEquals("1", rowOf("SELECT sum(v) OVER (PARTITION BY s ORDER BY v"
                + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM cpj_t"));
        assertEquals("2", rowOf(
                "SELECT v FROM cpj_t WHERE v > 1 GROUP BY v HAVING count(*) > 0 ORDER BY v LIMIT 2"));
        assertEquals("1|x", rowOf(
                "SELECT a.v, b.s FROM cpj_t a LEFT JOIN cpj_u b ON a.v = b.v ORDER BY a.v"));
        assertEquals("1|a|t|2", rowOf(
                "SELECT * FROM cpj_t t CROSS JOIN LATERAL (SELECT t.v * 2 AS d) l ORDER BY t.v"));
        assertEquals("2", rowOf("SELECT count(DISTINCT v) FROM cpj_t"));
        assertEquals("2", rowOf("SELECT max(v) FILTER (WHERE s <> 'a') FROM cpj_t"));
        assertEquals("1", rowOf("SELECT count(*) FROM cpj_t"
                + " WHERE EXISTS (SELECT 1 FROM cpj_u WHERE cpj_u.v = cpj_t.v)"));
        assertEquals("1|a|t|1", rowOf("SELECT * FROM cpj_t t JOIN LATERAL"
                + " (SELECT count(*) c FROM cpj_u u WHERE u.v = t.v) l ON true ORDER BY t.v"));
    }
}
