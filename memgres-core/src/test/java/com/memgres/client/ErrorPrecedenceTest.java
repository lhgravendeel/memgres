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
 * <p>memgres runs its placement checks over the raw syntax tree, before anything is resolved, so a
 * later complaint can win. The cases where the two engines already agree are asserted here and in
 * error-precedence.sql. The cases where they do not are asserted too — against what memgres does
 * today, each one carrying the answer PostgreSQL gives — so that the gap is measured rather than
 * forgotten, and so that closing one makes this test fail and say so.
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
     * <p>Every one has the same cause — the placement checks run over the raw syntax tree, before
     * relations, columns and functions are resolved. Closing them means running those checks after
     * resolution, which is a larger change than this branch makes.
     */
    @Test
    void theCasesStillOutOfOrderAreRecordedRatherThanAsserted() {
        // PostgreSQL: 42P01, because the range table is built first.
        assertEquals("42809", stateOf("SELECT abs(id) FILTER (WHERE true) FROM ept_nosuch"));
        assertEquals("0A000",
                stateOf("SELECT id FROM ept_nosuch WHERE generate_series(1,2) > 0"));
        assertEquals("42809", stateOf(
                "SELECT * FROM ept_nosuch x WHERE EXISTS (SELECT abs(1) FILTER (WHERE true))"));
        assertEquals("42809", stateOf("SELECT abs(DISTINCT id) FROM ept_nosuch"));

        // PostgreSQL: 42703 — the arguments and the FILTER expression are transformed before the
        // function is resolved, so an unknown column in either is reported first.
        assertEquals("42809", stateOf("SELECT abs(nosuchcol) FILTER (WHERE true) FROM ept_t"));
        assertEquals("42809", stateOf("SELECT abs(id) FILTER (WHERE nosuchcol) FROM ept_t"));
        assertEquals("42809", stateOf("SELECT abs(nosuchcol) OVER () FROM ept_t"));
        assertEquals("42809", stateOf("SELECT abs(DISTINCT nosuchcol) FROM ept_t"));
        assertEquals("42803", stateOf("SELECT id FROM ept_t WHERE count(nosuchcol) > 0"));

        // PostgreSQL: 42804 "argument of FILTER must be type boolean, not type integer".
        assertEquals("42809", stateOf("SELECT abs(id) FILTER (WHERE 1) FROM ept_t"));

        // PostgreSQL: 42883 — the call resolves to nothing, so it never reaches the FILTER rule.
        assertEquals("42809", stateOf("SELECT \"ABS\"(1) FILTER (WHERE true)"));
        assertEquals("42809", stateOf("SELECT abs(txt) FILTER (WHERE b) FROM ept_t"));

        // PostgreSQL: 42883 — memgres ignores an unknown schema qualifier and runs the call.
        assertEquals("OK",
                stateOf("SELECT information_schema.abs(v) FILTER (WHERE true) FROM ept_t"));
    }
}
