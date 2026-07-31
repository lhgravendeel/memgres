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
 * Where a boolean is required, and what is said when something else is written there.
 *
 * <p>PostgreSQL coerces the expression to boolean while it transforms the clause the expression
 * stands in. A type with no coercion to boolean is refused there and then, naming the clause —
 * {@code argument of WHERE must be type boolean, not type integer} (42804). The clauses that do
 * this are WHERE, HAVING, a JOIN's ON, a searched CASE's WHEN, FILTER, AND, OR, NOT, a CHECK
 * constraint, a policy expression, a partial index's predicate, a rule's qualification, a
 * trigger's WHEN and a MERGE action's AND.
 *
 * <p>Two rules decide which error, and neither of them looks at a value. A bare string literal is
 * still of type {@code unknown}, so boolean's own input function reads it: {@code WHERE 't'} is
 * accepted and {@code WHERE 'zzz'} is 22P02, while {@code WHERE 'zzz'::text} — the same text, now
 * carrying a type — is 42804. And an expression whose type cannot be settled without evaluating it
 * is left alone, so a column of a derived table or a CTE is accepted where PostgreSQL refuses it.
 *
 * <p>PL/pgSQL is deliberately different: it has the value in hand, so it does not raise the type
 * system's error at all but puts the value through boolean's input function. {@code IF i} where i
 * is 1 runs, because "1" is boolean input, and {@code IF i + 1} fails on the text "2".
 *
 * <p>Every message and every accepted shape below was measured against PostgreSQL 18.
 */
class BooleanContextTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS bct_t CASCADE");
        exec("DROP TABLE IF EXISTS bct_u CASCADE");
        exec("CREATE TABLE bct_t (id int PRIMARY KEY, i int, n numeric, s text, b boolean,"
                + " arr int[], j jsonb)");
        exec("INSERT INTO bct_t VALUES (1, 1, 1.5, 'x', true, ARRAY[1,2], '{\"a\":1}')");
        exec("INSERT INTO bct_t VALUES (2, 0, 0.0, 'y', false, ARRAY[3], '{\"a\":2}')");
        exec("CREATE TABLE bct_u (id int PRIMARY KEY, k int)");
        exec("INSERT INTO bct_u VALUES (1, 1), (2, 2)");
        exec("CREATE FUNCTION bct_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$"
                + " LANGUAGE plpgsql");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP FUNCTION IF EXISTS bct_f() CASCADE");
            exec("DROP TABLE IF EXISTS bct_t CASCADE");
            exec("DROP TABLE IF EXISTS bct_u CASCADE");
            conn.close();
        }
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
    private static String rowsOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    // =========================================================================
    // The clause is named, and so is the type
    // =========================================================================

    @Test
    void whereNamesItselfAndEveryTypeThatIsNotABoolean() {
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE 1"));
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE i"));
        assertEquals("argument of WHERE must be type boolean, not type numeric",
                messageOf("SELECT id FROM bct_t WHERE 1.5"));
        assertEquals("argument of WHERE must be type boolean, not type numeric",
                messageOf("SELECT id FROM bct_t WHERE n"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE s"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE 'x'::text"));
        assertEquals("argument of WHERE must be type boolean, not type integer[]",
                messageOf("SELECT id FROM bct_t WHERE arr"));
        assertEquals("argument of WHERE must be type boolean, not type jsonb",
                messageOf("SELECT id FROM bct_t WHERE j"));
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE (SELECT 1)"));
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT 1 WHERE (SELECT 1)"));
    }

    /** The type of an expression, not only of a bare column. */
    @Test
    void anOperatorAndACallCarryAResultTypeOfTheirOwn() {
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE i + 1"));
        assertEquals("argument of WHERE must be type boolean, not type numeric",
                messageOf("SELECT id FROM bct_t WHERE i * n"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE s || 'a'"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE upper(s)"),
                "a call is resolved by its argument types, and upper(text) is text");
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE length(s)"));
        assertEquals("argument of HAVING must be type boolean, not type bigint",
                messageOf("SELECT count(*) FROM bct_t HAVING count(*)"),
                "an aggregate's result type is its own");
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE NULL::int"),
                "an explicit NULL of a type is still that type");
    }

    @Test
    void everyClauseThatWantsAConditionNamesItself() {
        assertEquals("argument of HAVING must be type boolean, not type integer",
                messageOf("SELECT count(*) FROM bct_t HAVING 1"));
        assertEquals("argument of HAVING must be type boolean, not type integer",
                messageOf("SELECT count(*) FROM bct_t GROUP BY i HAVING i"));
        assertEquals("argument of JOIN/ON must be type boolean, not type integer",
                messageOf("SELECT count(*) FROM bct_t JOIN bct_u ON 1"));
        assertEquals("argument of JOIN/ON must be type boolean, not type text",
                messageOf("SELECT count(*) FROM bct_t LEFT JOIN bct_u ON s"));
        assertEquals("argument of CASE/WHEN must be type boolean, not type integer",
                messageOf("SELECT CASE WHEN 1 THEN 1 ELSE 2 END"));
        assertEquals("argument of CASE/WHEN must be type boolean, not type integer",
                messageOf("SELECT CASE WHEN i THEN 1 END FROM bct_t"));
        assertEquals("argument of CASE/WHEN must be type boolean, not type bigint",
                messageOf("SELECT CASE WHEN count(*) THEN 1 END FROM bct_t"));
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT count(*) FILTER (WHERE 1) FROM bct_t"));
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT count(*) FILTER (WHERE i) FROM bct_t"));
        assertEquals("argument of FILTER must be type boolean, not type integer",
                messageOf("SELECT abs(i) FILTER (WHERE 1) FROM bct_t"),
                "a FILTER is coerced before the call carrying it is resolved");
        assertEquals("argument of AND must be type boolean, not type integer",
                messageOf("SELECT id FROM bct_t WHERE b AND i"));
        assertEquals("argument of OR must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE b OR s"));
        assertEquals("argument of NOT must be type boolean, not type numeric",
                messageOf("SELECT id FROM bct_t WHERE NOT n"));
    }

    /** AND, OR and NOT are operators, so they hold wherever they are written. */
    @Test
    void theLogicalOperatorsAreCheckedOutsideAConditionToo() {
        assertEquals("argument of AND must be type boolean, not type integer",
                messageOf("SELECT i AND true FROM bct_t"));
        assertEquals("argument of OR must be type boolean, not type bigint",
                messageOf("SELECT count(*) OR false FROM bct_t"));
        assertEquals("argument of NOT must be type boolean, not type jsonb",
                messageOf("SELECT NOT j FROM bct_t"));
        assertEquals("argument of NOT must be type boolean, not type integer",
                messageOf("SELECT NOT 1"));
    }

    /**
     * A CASE is a boolean context only when it is searched: {@code CASE i WHEN 1 THEN} compares
     * the WHEN with the operand, so an integer there is exactly what belongs.
     */
    @Test
    void aSimpleCaseIsNotABooleanContext() throws SQLException {
        assertEquals("1", rowsOf("SELECT CASE i WHEN 1 THEN 1 ELSE 2 END FROM bct_t"
                + " WHERE id = 1"));
        assertEquals("2", rowsOf("SELECT CASE s WHEN 'y' THEN 2 ELSE 1 END FROM bct_t"
                + " WHERE id = 2"));
    }

    @Test
    void theRefusalDoesNotDependOnThereBeingARowToTryItOn() {
        assertEquals("42804", stateOf("SELECT id FROM bct_t WHERE id < 0 AND i > 0 AND i"));
        assertEquals("42804", stateOf("SELECT id FROM bct_t WHERE false AND i"));
        assertEquals("42804", stateOf("SELECT id FROM bct_t WHERE i LIMIT 0"));
        assertEquals("42804", stateOf("DELETE FROM bct_t WHERE id < 0 AND j"));
    }

    // =========================================================================
    // A bare string literal is of type unknown
    // =========================================================================

    @Test
    void aBareStringLiteralIsReadByBooleansInputFunction() throws SQLException {
        assertEquals("invalid input syntax for type boolean: \"zzz\"",
                messageOf("SELECT id FROM bct_t WHERE 'zzz'"));
        assertEquals("22P02", stateOf("SELECT id FROM bct_t WHERE 'zzz'"));
        assertEquals("22P02", stateOf("SELECT count(*) FROM bct_t HAVING 'zzz'"));
        assertEquals("22P02", stateOf("SELECT count(*) FROM bct_t JOIN bct_u ON 'zzz'"));
        assertEquals("22P02", stateOf("SELECT CASE WHEN 'abc' THEN 1 END"));
        assertEquals("22P02", stateOf("SELECT count(*) FILTER (WHERE 'zzz') FROM bct_t"));
        assertEquals("22P02", stateOf("SELECT id FROM bct_t WHERE b AND 'zzz'"));
        assertEquals("22P02", stateOf("UPDATE bct_t SET i = i WHERE 'zzz'"));

        // The same text carrying a type is the type system's complaint instead.
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM bct_t WHERE 'zzz'::text"));
    }

    /** Every word boolean input knows, written bare, is a condition PostgreSQL accepts. */
    @Test
    void theBooleanWordsWrittenBareAreAccepted() throws SQLException {
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE 't'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE 'true'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE '1'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE 'y'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE 'on'"));
        // These four were all being read as true, because they are neither empty nor "false".
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE 'f'"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE 'false'"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE 'no'"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE 'off'"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE '0'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE NOT 'off'"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE NULL"));
    }

    // =========================================================================
    // Definitions that store a condition
    // =========================================================================

    @Test
    void aStoredConditionIsCheckedWhenItIsWritten() {
        assertEquals("argument of CHECK must be type boolean, not type integer",
                messageOf("CREATE TABLE bct_bad (i int, CHECK (i))"));
        assertEquals("argument of CHECK must be type boolean, not type integer",
                messageOf("CREATE TABLE bct_bad (i int, CHECK (i + 1))"));
        assertEquals("22P02", stateOf("CREATE TABLE bct_bad (i int, CHECK ('zzz'))"));

        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("CREATE INDEX bct_ix ON bct_t (id) WHERE i"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("CREATE INDEX bct_ix ON bct_t (id) WHERE s"));
        assertEquals("22P02", stateOf("CREATE INDEX bct_ix ON bct_t (id) WHERE 'zzz'"));

        assertEquals("argument of POLICY must be type boolean, not type integer",
                messageOf("CREATE POLICY bct_p ON bct_t USING (i)"));
        assertEquals("argument of POLICY must be type boolean, not type jsonb",
                messageOf("CREATE POLICY bct_p ON bct_t WITH CHECK (j)"));
        assertEquals("22P02", stateOf("CREATE POLICY bct_p ON bct_t USING ('zzz')"));
        assertEquals("aggregate functions are not allowed in policy expressions",
                messageOf("CREATE POLICY bct_p ON bct_t USING (count(*))"),
                "an aggregate is refused before the expression's type is judged");

        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("CREATE RULE bct_r AS ON INSERT TO bct_t WHERE new.i DO INSTEAD NOTHING"));
        assertEquals("22P02",
                stateOf("CREATE RULE bct_r AS ON INSERT TO bct_t WHERE 'zzz' DO INSTEAD NOTHING"));

        assertEquals("argument of WHEN must be type boolean, not type integer",
                messageOf("CREATE TRIGGER bct_tg BEFORE UPDATE ON bct_t FOR EACH ROW"
                        + " WHEN (new.i) EXECUTE FUNCTION bct_f()"));
        assertEquals("22P02",
                stateOf("CREATE TRIGGER bct_tg BEFORE UPDATE ON bct_t FOR EACH ROW"
                        + " WHEN ('zzz') EXECUTE FUNCTION bct_f()"));
    }

    @Test
    void aDataModifyingStatementsConditionIsCheckedToo() throws SQLException {
        assertEquals("argument of WHERE must be type boolean, not type integer",
                messageOf("UPDATE bct_t SET i = i WHERE i"));
        assertEquals("argument of WHERE must be type boolean, not type jsonb",
                messageOf("DELETE FROM bct_t WHERE j"));
        assertEquals("argument of CASE/WHEN must be type boolean, not type integer",
                messageOf("UPDATE bct_t SET i = CASE WHEN i THEN 1 ELSE 2 END"));
        assertEquals("argument of WHEN must be type boolean, not type numeric",
                messageOf("MERGE INTO bct_t t USING bct_u u ON t.id = u.id"
                        + " WHEN MATCHED AND (n) THEN DO NOTHING"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t"), "none of those wrote anything");
        assertEquals("1", rowsOf("SELECT i FROM bct_t WHERE id = 1"));
    }

    /**
     * A MERGE's ON is deliberately left alone: PostgreSQL transforms a MERGE's join condition
     * without coercing it to boolean, so {@code MERGE ... ON (1)} is accepted where the same
     * condition written in a JOIN is not.
     */
    @Test
    void aMergeOnConditionIsNotCoerced() {
        assertEquals("OK", stateOf("MERGE INTO bct_t t USING bct_u u ON (1)"
                + " WHEN MATCHED THEN DO NOTHING"));
        assertEquals("OK", stateOf("MERGE INTO bct_t t USING bct_u u ON ('zzz')"
                + " WHEN MATCHED THEN DO NOTHING"));
    }

    // =========================================================================
    // PL/pgSQL reads the value, not the type
    // =========================================================================

    @Test
    void plpgsqlPutsTheValueThroughBooleansInputFunction() {
        assertEquals("OK", stateOf("DO $$ DECLARE i int := 1;"
                + " BEGIN IF i THEN NULL; END IF; END $$"), "\"1\" is boolean input");
        assertEquals("invalid input syntax for type boolean: \"2\"",
                messageOf("DO $$ DECLARE i int := 1; BEGIN IF i + 1 THEN NULL; END IF; END $$"));
        assertEquals("invalid input syntax for type boolean: \"1.5\"",
                messageOf("DO $$ DECLARE n numeric := 1.5;"
                        + " BEGIN WHILE n LOOP EXIT; END LOOP; END $$"));
        assertEquals("invalid input syntax for type boolean: \"x\"",
                messageOf("DO $$ DECLARE s text := 'x'; BEGIN IF s THEN NULL; END IF; END $$"));
        assertEquals("invalid input syntax for type boolean: \"{1}\"",
                messageOf("DO $$ DECLARE arr int[] := ARRAY[1];"
                        + " BEGIN IF arr THEN NULL; END IF; END $$"));
        assertEquals("invalid input syntax for type boolean: \"3\"",
                messageOf("DO $$ BEGIN LOOP EXIT WHEN 3; END LOOP; END $$"));
        assertEquals("invalid input syntax for type boolean: \"7\"",
                messageOf("DO $$ BEGIN CASE WHEN 7 THEN NULL; ELSE NULL; END CASE; END $$"));
    }

    @Test
    void anOrdinaryPlpgsqlConditionStillRuns() throws SQLException {
        exec("DROP TABLE IF EXISTS bct_loop CASCADE");
        exec("DO $$ DECLARE c int := 0; BEGIN WHILE c < 3 LOOP c := c + 1; END LOOP;"
                + " CREATE TABLE bct_loop AS SELECT c AS r; END $$");
        assertEquals("3", rowsOf("SELECT r FROM bct_loop"));
        exec("DROP TABLE IF EXISTS bct_loop CASCADE");
        assertEquals("OK", stateOf("DO $$ DECLARE b boolean := true;"
                + " BEGIN IF b AND NOT false THEN NULL; END IF; END $$"));
        assertEquals("OK", stateOf("DO $$ DECLARE s text := 'off';"
                + " BEGIN IF s THEN RAISE EXCEPTION 'off is not true'; END IF; END $$"));
    }

    // =========================================================================
    // The ordinary shapes, in every clause that wants a boolean
    // =========================================================================

    @Test
    void theOrdinaryConditionsAreUntouchedInEveryClause() throws SQLException {
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE b"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE i = 1"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE i <> 1"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE b AND true"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE b OR false"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE NOT b"));
        assertEquals("0", rowsOf("SELECT count(*) FROM bct_t WHERE i IS NULL"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE i IS NOT NULL"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE i IN (0, 1)"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE id IN (SELECT id FROM bct_u)"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE EXISTS (SELECT 1 FROM bct_u)"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE s LIKE 'x'"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE s ~ 'x'"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE i BETWEEN 1 AND 2"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE (SELECT true)"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE starts_with(s, 'x')"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t"
                + " WHERE CASE WHEN i = 1 THEN true ELSE false END"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE b IS TRUE"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE b IS NOT TRUE"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE length(s) = 1"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE coalesce(b, false)"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE i = ANY (ARRAY[1, 5])"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE i IS DISTINCT FROM 1"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE 't'::boolean"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t WHERE j ->> 'a' = '1'"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t WHERE arr[1] IS NOT NULL"));
    }

    @Test
    void theOrdinaryConditionsHoldInTheOtherClausesToo() throws SQLException {
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t JOIN bct_u ON bct_t.id = bct_u.id"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t LEFT JOIN bct_u"
                + " ON bct_t.id = bct_u.id AND bct_u.k > 0"));
        assertEquals("2", rowsOf("SELECT count(*) FROM bct_t HAVING count(*) > 1"));
        assertEquals("1", rowsOf("SELECT count(*) FILTER (WHERE b) FROM bct_t"));
        assertEquals("1", rowsOf("SELECT count(*) FILTER (WHERE i = 1) FROM bct_t"));
        assertEquals("1", rowsOf("SELECT count(*) FROM bct_t x"
                + " WHERE EXISTS (SELECT 1 FROM bct_u WHERE x.b)"),
                "a correlated reference is resolved against the enclosing relation");
        assertEquals("2", rowsOf("SELECT count(*) FROM (SELECT * FROM bct_t) d WHERE d.b OR true"));
        assertEquals("2", rowsOf("WITH w AS (SELECT * FROM bct_t) SELECT count(*) FROM w"
                + " WHERE w.i >= 0"));
        assertEquals("OK", stateOf("SELECT CASE WHEN true THEN 1 ELSE 2 END"));
        assertEquals("OK", stateOf("SELECT id FROM bct_t ORDER BY CASE WHEN b THEN 1 ELSE 2 END"));
    }

    /**
     * A type this cannot settle is a type it says nothing about. A derived table's and a CTE's
     * columns carry whatever the engine inferred while building the result, so a condition over
     * one is accepted here where PostgreSQL refuses it — a gap, and deliberately on that side.
     */
    @Test
    void aTypeThatCannotBeSettledIsLeftAlone() {
        assertEquals("OK", stateOf("SELECT id FROM (SELECT * FROM bct_t) d WHERE i"));
        assertEquals("OK", stateOf("WITH w AS (SELECT * FROM bct_t) SELECT id FROM w WHERE i"));
        assertEquals("OK", stateOf("SELECT g FROM generate_series(1, 2) g WHERE g"));
    }

    @Test
    void aDefinitionThatIsAConditionIsStoredWithoutComplaint() throws SQLException {
        exec("DROP TABLE IF EXISTS bct_ok CASCADE");
        assertEquals("OK", stateOf("CREATE TABLE bct_ok (i int, b boolean,"
                + " CHECK (i > 0), CHECK (b), CHECK (i IS NOT NULL), CHECK (b OR i > 1))"));
        assertEquals("OK", stateOf("CREATE INDEX bct_ix_ok ON bct_t (id) WHERE b"));
        assertEquals("OK", stateOf("CREATE INDEX bct_ix_ok2 ON bct_t (id)"
                + " WHERE i > 0 AND s IS NOT NULL"));
        assertEquals("OK", stateOf("CREATE POLICY bct_p_ok ON bct_t USING (b)"));
        assertEquals("OK", stateOf("CREATE POLICY bct_p_ok2 ON bct_t"
                + " WITH CHECK (i > 0 OR s LIKE 'x')"));
        assertEquals("OK", stateOf("CREATE RULE bct_r_ok AS ON INSERT TO bct_t"
                + " WHERE new.b DO INSTEAD NOTHING"));
        assertEquals("OK", stateOf("CREATE TRIGGER bct_tg_ok BEFORE UPDATE ON bct_t"
                + " FOR EACH ROW WHEN (new.i > old.i) EXECUTE FUNCTION bct_f()"));
        exec("DROP TRIGGER IF EXISTS bct_tg_ok ON bct_t");
        exec("DROP RULE IF EXISTS bct_r_ok ON bct_t");
        exec("DROP TABLE IF EXISTS bct_ok CASCADE");
    }

    /** A condition supplied as a parameter is bound, not written, so it types as a boolean. */
    @Test
    void aBoundParameterIsAConditionLikeAnyOther() throws Exception {
        try (Connection extended = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword())) {
            try (java.sql.PreparedStatement ps = extended.prepareStatement(
                    "SELECT count(*) FROM bct_t WHERE b = ?")) {
                ps.setBoolean(1, true);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(1, rs.getInt(1));
                }
            }
            try (java.sql.PreparedStatement ps = extended.prepareStatement(
                    "SELECT count(*) FROM bct_t WHERE CASE WHEN i = ? THEN true ELSE false END")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }
}
