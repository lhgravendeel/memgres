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
 * Which of a statement's faults is reported, and how an entry out of reach is worded.
 *
 * <p>PostgreSQL analyses one query level in a fixed order — WITH items, the FROM clause and its
 * join conditions, the select list, WHERE, HAVING, ORDER BY, and GROUP BY last of all — and that
 * order decides which fault a statement wrong in several places names. memgres reads the same
 * clauses in the same order against what the relations supply, so it no longer reports whichever
 * fault it happened to run into first.
 *
 * <p>The second half of this class is the guard. Reading a clause without a row to read it against
 * is what makes these refusals possible, and it is also what could refuse SQL PostgreSQL runs: a
 * derived relation whose columns only running it settles, a name a sub-select binds for itself, a
 * relation renamed by an alias list. Every one of them is asserted to run.
 *
 * <p>Companion corpus file: clause-order.sql, which carries the same cases through the differential
 * harness against a live PostgreSQL 18.
 */
class ClauseOrderTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS cot_u CASCADE");
        exec("DROP TABLE IF EXISTS cot_t CASCADE");
        exec("CREATE TABLE cot_t (id int PRIMARY KEY, v int, n int, txt text, b boolean)");
        exec("INSERT INTO cot_t VALUES (1,1,1,'aa',true),(2,2,0,'ab',false)");
        exec("CREATE TABLE cot_u (id int PRIMARY KEY, v int)");
        exec("INSERT INTO cot_u VALUES (1,1),(2,2)");
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

    /** The first line of the refusal, or "OK" when the statement runs. */
    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    /** The SQLSTATE and the first line of the refusal, or "OK". */
    private static String errorOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState() + " "
                    + e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    /** The first column of the first row, as text. */
    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    // =========================================================================
    // The order one query level's clauses are read in
    // =========================================================================

    /** The select list stands before WHERE, HAVING, ORDER BY and GROUP BY. */
    @Test
    void theSelectListIsReadBeforeTheClausesBehindIt() {
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM cot_t ORDER BY nosuch3"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM cot_t WHERE sum(v) > 1"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM cot_t GROUP BY nosuch3"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT abs(nosuch2) FROM cot_t HAVING count(nosuch3) > 0"));
    }

    /** Within one clause the expressions are read left to right, arguments included. */
    @Test
    void theLeftmostFaultOfAClauseIsTheOneNamed() {
        assertEquals("column \"nosuch1\" does not exist",
                messageOf("SELECT nosuch1, nosuch2 FROM cot_t"));
        assertEquals("column \"nosuch1\" does not exist",
                messageOf("SELECT v FROM cot_t WHERE nosuch1 = 1 OR nosuch2 = 2"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT v FROM cot_t WHERE nosuch2 = 1 AND sum(v) > 1"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT cot_nosuchfn(nosuch2) FROM cot_t"),
                "a call's arguments are transformed before the function is resolved");
    }

    /** HAVING stands before ORDER BY, and GROUP BY is transformed last of all. */
    @Test
    void groupByIsTransformedLast() {
        assertEquals("column \"nosuch_c\" does not exist",
                messageOf("SELECT v FROM cot_t GROUP BY nosuch_b HAVING nosuch_c > 1"));
        assertEquals("column \"nosuch_c\" does not exist",
                messageOf("SELECT v FROM cot_t GROUP BY nosuch_b ORDER BY nosuch_c"));
        assertEquals("42P10 ORDER BY position 9 is not in select list",
                errorOf("SELECT id, v FROM cot_t GROUP BY nosuch_b ORDER BY 9"));
        assertEquals("42601 non-integer constant in ORDER BY",
                errorOf("SELECT id, v FROM cot_t GROUP BY nosuch_b ORDER BY 2.5"));
    }

    /** WHERE stands before everything WHERE stands in front of. */
    @Test
    void whereIsReadBeforeWhatFollowsIt() {
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT v FROM cot_t WHERE nosuch2 = 1 ORDER BY nosuch3"));
        assertEquals("column \"nosuch2\" does not exist",
                messageOf("SELECT v FROM cot_t WHERE nosuch2 = 1 GROUP BY nosuch3"));
    }

    /**
     * A clause is transformed against what the relations supply, not against a row, so a query
     * that reads nothing is judged all the same.
     */
    @Test
    void aClauseIsJudgedWithoutARowToJudgeItOn() {
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT abs(nosuchcol) FROM cot_t WHERE false"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT v FROM cot_t WHERE id > 1000 ORDER BY abs(nosuchcol)"));
        assertEquals("column \"nosuchcol\" does not exist",
                messageOf("SELECT abs(nosuchcol) FROM cot_t LIMIT 0"));
    }

    /**
     * The window definitions are transformed once every clause has been read, so nothing in an
     * OVER specification is reached while the clause holding the call is being judged.
     */
    @Test
    void theOverSpecificationIsNotReadWithTheClause() {
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cot_t WHERE row_number() OVER (ORDER BY nosuchcol) = 1"));
        assertEquals("42P20 window functions are not allowed in WHERE",
                errorOf("SELECT v FROM cot_t WHERE row_number() OVER (PARTITION BY nosuchcol) = 1"));
        assertEquals("42703 column \"nosuchcol\" does not exist",
                errorOf("SELECT v FROM cot_t WHERE lag(nosuchcol) OVER () = 1"),
                "the call's own arguments are transformed with the clause");
    }

    /** The range table is finished before a clause is read. */
    @Test
    void theRangeTableOutranksEveryClause() {
        assertEquals("42712 table name \"x\" specified more than once",
                errorOf("SELECT * FROM cot_t x JOIN cot_u x ON nosuchcol = 1"));
        assertEquals("42P01 relation \"cot_nosuchtable\" does not exist",
                errorOf("SELECT nosuchcol FROM cot_nosuchtable"));
    }

    // =========================================================================
    // An entry out of reach, and one that is nowhere
    // =========================================================================

    /**
     * The range table is built left to right — a join's left side before its right, a nested join
     * in the place it stands — and a sub-select is transformed against the entries made before it.
     * An entry already made and out of reach is worded differently from one not yet made.
     */
    @Test
    void anEntryAlreadyMadeIsOutOfReachRatherThanMissing() {
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a JOIN (SELECT a.v) b ON true"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a, (SELECT a.v) b"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a CROSS JOIN (SELECT a.v) b"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a LEFT JOIN (SELECT a.v) b ON true"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a RIGHT JOIN (SELECT a.v) b ON true"));
        assertEquals("invalid reference to FROM-clause entry for table \"b\"",
                messageOf("SELECT count(*) FROM (cot_t a JOIN cot_u b ON true)"
                        + " JOIN (SELECT b.v) c ON true"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a"
                        + " JOIN ((SELECT a.v) x JOIN cot_u b ON true) ON true"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM cot_t a, (SELECT (SELECT a.v)) b"));
        assertEquals("invalid reference to FROM-clause entry for table \"cot_t\"",
                messageOf("SELECT count(*) FROM cot_t a JOIN (SELECT cot_t.v) b ON true"),
                "an alias hides the relation's name, and the entry answers to either");
    }

    /** An entry not yet made, or written nowhere at all, is missing. */
    @Test
    void anEntryNotYetMadeIsMissing() {
        assertEquals("missing FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM (SELECT a.v) b JOIN cot_t a ON true"));
        assertEquals("missing FROM-clause entry for table \"a\"",
                messageOf("SELECT count(*) FROM (SELECT a.v) b, cot_t a"));
        assertEquals("missing FROM-clause entry for table \"z\"",
                messageOf("SELECT count(*) FROM cot_t a JOIN (SELECT z.v) b ON true"));
        assertEquals("missing FROM-clause entry for table \"w\"",
                messageOf("WITH w AS (SELECT 1 AS n)"
                        + " SELECT count(*) FROM cot_t a JOIN (SELECT w.n) b ON true"),
                "a WITH item is not a FROM entry of the level that defines it");
        assertEquals("missing FROM-clause entry for table \"a\"",
                messageOf("SELECT a.v FROM cot_t b"));
    }

    // =========================================================================
    // THE GUARD: the ordinary shapes none of the above may touch
    // =========================================================================

    /** LATERAL is the word that brings the entry into reach, and a call in FROM is lateral. */
    @Test
    void lateralStillReadsTheItemsToItsLeft() throws Exception {
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a JOIN LATERAL (SELECT a.v) b ON true"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a, LATERAL (SELECT a.v) b"));
        assertEquals("3", scalar("SELECT count(*) FROM cot_t a, generate_series(1, a.v) g"));
        assertEquals("1", scalar("SELECT count(*) FROM cot_t a JOIN cot_u b ON b.v = (SELECT a.v)"
                + " WHERE a.id = 1"));
    }

    /** A name the sub-select binds for itself is its own, whatever a sibling is called. */
    @Test
    void aNameTheSubSelectBindsIsNotTheSiblings() throws Exception {
        assertEquals("4",
                scalar("SELECT count(*) FROM cot_t a JOIN (SELECT a.v FROM cot_u a) b ON true"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a"
                + " JOIN (WITH a AS (SELECT 1 AS v) SELECT a.v FROM a) b ON true"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a"
                + " JOIN (SELECT (SELECT a.v FROM cot_u a LIMIT 1)) b ON true"));
    }

    /** An enclosing query level's entry is in scope, sub-select in FROM or not. */
    @Test
    void anEnclosingLevelIsStillInScope() throws Exception {
        assertEquals("1",
                scalar("SELECT (SELECT count(*) FROM (SELECT a.v) c) FROM cot_t a LIMIT 1"));
        assertEquals("1", scalar("SELECT count(*) FROM cot_t a"
                + " WHERE EXISTS (SELECT 1 FROM cot_u b WHERE b.v = a.v AND a.id = 1)"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a"
                + " WHERE a.v IN (SELECT v FROM cot_u b WHERE b.id = a.id)"));
    }

    /** The ordinary joins and derived tables, which reference nothing odd. */
    @Test
    void theOrdinaryJoinsAndDerivedTablesAreUntouched() throws Exception {
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a JOIN cot_u b ON a.id = b.id"));
        assertEquals("2",
                scalar("SELECT count(*) FROM (SELECT v FROM cot_u) b JOIN cot_t a ON a.v = b.v"));
        assertEquals("2",
                scalar("SELECT count(*) FROM cot_t a JOIN generate_series(1,2) g ON g = a.v"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a"
                + " FULL JOIN cot_u b ON a.id = b.id"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t a JOIN cot_u b USING (id)"));
        assertEquals("2", scalar("SELECT count(*) FROM cot_t NATURAL JOIN cot_u"));
    }

    /** ORDER BY by position, by name, by alias and by expression. */
    @Test
    void everyWayOfWritingAnOrderByStillSorts() throws Exception {
        assertEquals("1", scalar("SELECT v FROM cot_t ORDER BY 1"));
        assertEquals("1", scalar("SELECT * FROM cot_u ORDER BY 2"));
        assertEquals("2", scalar("SELECT v AS k FROM cot_t ORDER BY k DESC"));
        assertEquals("2", scalar("SELECT v FROM cot_t ORDER BY abs(v) DESC"));
        assertEquals("2", scalar("SELECT * FROM cot_t ORDER BY 2 DESC"));
        assertEquals("1", scalar("SELECT t.* FROM cot_t t ORDER BY 2"));
        assertEquals("0", scalar("SELECT n FROM cot_t GROUP BY n HAVING count(*) > 0 ORDER BY 1"));
    }

    /** A window call where one may stand, with an OVER that names columns. */
    @Test
    void aWindowCallWhereOneMayStandStillRuns() throws Exception {
        assertEquals("1,2", scalar("SELECT string_agg(r::text, ',' ORDER BY r) FROM"
                + " (SELECT row_number() OVER (ORDER BY v) AS r FROM cot_t) q"));
        assertEquals("1,1", scalar("SELECT string_agg(r::text, ',' ORDER BY r) FROM"
                + " (SELECT row_number() OVER (PARTITION BY n ORDER BY v) AS r FROM cot_t) q"));
        assertEquals("1,2", scalar("SELECT string_agg(c::text, ',' ORDER BY c) FROM"
                + " (SELECT count(*) OVER (ORDER BY v) AS c FROM cot_t) q"));
        assertEquals("1,3", scalar("SELECT string_agg(s::text, ',' ORDER BY s) FROM"
                + " (SELECT sum(v) OVER (ORDER BY t.v) AS s FROM cot_t t) q"));
        assertEquals("2", scalar("SELECT string_agg(l::text, ',' ORDER BY l) FROM"
                + " (SELECT lag(v) OVER (ORDER BY v DESC) AS l FROM cot_t) q"));
    }

    /** A relation renamed by an alias list answers to the names the query wrote. */
    @Test
    void anAliasListRenamesWhatTheQueryCanWrite() throws Exception {
        assertEquals("2", scalar("SELECT count(*) FROM cot_t x(c1,c2,c3,c4,c5) WHERE c2 > 0"));
        assertEquals("2", scalar("SELECT count(*) FROM (SELECT v FROM cot_t) q(z) WHERE z > 0"));
        assertEquals("3", scalar("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL"
                + " SELECT n + 1 FROM r WHERE n < 3) SELECT count(*) FROM r AS z(m) WHERE m > 0"));
    }

    /** A relation whose columns only running it settles is not judged from a description. */
    @Test
    void aRelationOnlyRunningItDescribesIsLeftAlone() throws Exception {
        assertEquals("a", scalar("SELECT key FROM jsonb_each('{\"a\":1}')"));
        assertEquals("1",
                scalar("SELECT count(*) FROM jsonb_each('{\"a\":1}') e WHERE e.key = 'a'"));
        assertEquals("1", scalar("SELECT g FROM generate_series(1,2) g"));
        assertEquals("2", scalar("SELECT count(*) FROM generate_series(1,2) g WHERE g > 0"));
    }

    /** A catalog function reached over an oid column, and over a plain integer. */
    @Test
    void aCatalogFunctionOverAnIntegerStillResolves() throws Exception {
        assertEquals("t", scalar("SELECT count(*) > 0 FROM pg_class"
                + " WHERE pg_table_is_visible(oid) AND relname = 'cot_t'"));
        assertEquals("t", scalar("SELECT pg_get_userbyid(relowner) IS NOT NULL"
                + " FROM pg_class WHERE relname = 'cot_t'"));
        assertEquals("t", scalar("SELECT pg_type_is_visible(23) IS NOT NULL"));
    }
}
