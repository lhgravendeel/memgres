package com.memgres.wire;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the wire says the server did.
 *
 * <p>A client reads the command tag to learn what ran and how many rows it touched. Chosen from a
 * small enum of result kinds rather than from the statement, several commands shared one tag:
 * every DROP said DROP TABLE, an ALTER said CREATE TABLE, a TRUNCATE said DELETE 0, and SHOW and
 * EXPLAIN both said SELECT. A client that logs what it ran, or decides what to do next from the
 * tag, was told about a statement nobody had written.
 *
 * <p>And a query holding no statement is still a query: PostgreSQL answers it with an
 * EmptyQueryResponse where a CommandComplete would stand. The extended protocol already sent one
 * and the simple protocol did not, so the two disagreed about the same empty string.
 *
 * <p>These are read off the socket rather than through a driver, because a driver turns the tag
 * into an update count and the error fields into a message — which is to say it hides exactly
 * what is being checked.
 */
class TheWireSaysWhatTheServerDidTest {

    static Memgres memgres;
    static WireTalk wire;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        wire = new WireTalk("127.0.0.1", memgres.getPort(), memgres.getUser(), "memgres",
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (wire != null) wire.close();
        if (memgres != null) memgres.close();
    }

    private static String tag(String sql) throws Exception {
        List<String> tags = wire.tags(sql);
        assertEquals(1, tags.size(), sql + " -> " + tags);
        return tags.get(0);
    }

    /** The tag names the command that ran, and each kind of DROP names its own kind. */
    @Test
    void everyDropSaysWhatItDropped() throws Exception {
        assertEquals("CREATE TABLE", tag("CREATE TABLE zwt_t (i int)"));
        assertEquals("CREATE VIEW", tag("CREATE VIEW zwt_v AS SELECT i FROM zwt_t"));
        assertEquals("CREATE SEQUENCE", tag("CREATE SEQUENCE zwt_s"));
        assertEquals("CREATE INDEX", tag("CREATE INDEX zwt_ix ON zwt_t (i)"));
        assertEquals("CREATE SCHEMA", tag("CREATE SCHEMA zwt_sc"));
        assertEquals("CREATE TYPE", tag("CREATE TYPE zwt_ty AS (x int)"));
        assertEquals("CREATE FUNCTION",
                tag("CREATE FUNCTION zwt_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"));
        try {
            assertEquals("DROP INDEX", tag("DROP INDEX zwt_ix"));
            assertEquals("DROP VIEW", tag("DROP VIEW zwt_v"));
            assertEquals("DROP SEQUENCE", tag("DROP SEQUENCE zwt_s"));
            assertEquals("DROP SCHEMA", tag("DROP SCHEMA zwt_sc"));
            assertEquals("DROP TYPE", tag("DROP TYPE zwt_ty"));
            assertEquals("DROP FUNCTION", tag("DROP FUNCTION zwt_f()"));
            assertEquals("DROP TABLE", tag("DROP TABLE zwt_t"));
        } finally {
            wire.query("DROP TABLE IF EXISTS zwt_t CASCADE");
            wire.query("DROP VIEW IF EXISTS zwt_v");
            wire.query("DROP SEQUENCE IF EXISTS zwt_s");
            wire.query("DROP SCHEMA IF EXISTS zwt_sc");
            wire.query("DROP TYPE IF EXISTS zwt_ty");
            wire.query("DROP FUNCTION IF EXISTS zwt_f()");
        }
    }

    /** An ALTER is not a CREATE, a TRUNCATE is not a DELETE, and neither reports a row count. */
    @Test
    void aStatementIsNotTheStatementThatSharesItsResultKind() throws Exception {
        wire.query("CREATE TABLE zwt_a (i int)");
        try {
            assertEquals("ALTER TABLE", tag("ALTER TABLE zwt_a ADD COLUMN j int"));
            assertEquals("TRUNCATE TABLE", tag("TRUNCATE zwt_a"));
            assertEquals("SHOW", tag("SHOW work_mem"));
            assertEquals("EXPLAIN", tag("EXPLAIN SELECT 1"));
            assertEquals("COMMENT", tag("COMMENT ON TABLE zwt_a IS 'x'"));
        } finally {
            wire.query("DROP TABLE zwt_a");
        }
    }

    /** The count in the tag is the rows the statement touched, and a SELECT counts its own. */
    @Test
    void theCountInTheTagIsTheRowsTouched() throws Exception {
        wire.query("CREATE TABLE zwt_n (i int)");
        try {
            assertEquals("INSERT 0 3", tag("INSERT INTO zwt_n VALUES (1),(2),(3)"));
            assertEquals("UPDATE 3", tag("UPDATE zwt_n SET i = i + 1"));
            assertEquals("SELECT 3", tag("SELECT i FROM zwt_n"));
            assertEquals("DELETE 3", tag("DELETE FROM zwt_n"));
            assertEquals("SELECT 0", tag("SELECT i FROM zwt_n"));
        } finally {
            wire.query("DROP TABLE zwt_n");
        }
    }

    /** A transaction statement names itself, and the ones inside a block name themselves too. */
    @Test
    void theTransactionStatementsNameThemselves() throws Exception {
        assertEquals("BEGIN", tag("BEGIN"));
        try {
            assertEquals("SAVEPOINT", tag("SAVEPOINT zwt_sp"));
            assertEquals("RELEASE", tag("RELEASE zwt_sp"));
        } finally {
            assertEquals("ROLLBACK", tag("ROLLBACK"));
        }
    }

    /** A query holding no statement is answered with an EmptyQueryResponse. */
    @Test
    void anEmptyQueryIsAnswered() throws Exception {
        assertEquals("IZ", wire.shape(""));
        assertEquals("IZ", wire.shape("   "));
        assertEquals("IZ", wire.shape("-- nothing but a comment"));
        assertEquals("IZ", wire.shape(";"));
        // One that does hold a statement is answered the ordinary way.
        assertEquals("TDCZ", wire.shape("SELECT 1"));
    }

    /** A statement list answers each statement in turn, and only one ReadyForQuery closes it. */
    @Test
    void aStatementListIsAnsweredStatementByStatement() throws Exception {
        assertEquals(List.of("SELECT 1", "SELECT 1"), wire.tags("SELECT 1; SELECT 2"));
        assertEquals("TDCTDCZ", wire.shape("SELECT 1; SELECT 2"));
    }

    /** What the server knows about an error is what the client is told. */
    @Test
    void theErrorFieldsTheServerHasReachTheClient() throws Exception {
        wire.query("CREATE TABLE zwt_e (i int PRIMARY KEY, j int NOT NULL CHECK (j > 0))");
        wire.query("INSERT INTO zwt_e VALUES (1,1)");
        try {
            Map<Character, String> duplicate = wire.error("INSERT INTO zwt_e VALUES (1,1)");
            assertEquals("23505", duplicate.get('C'));
            assertEquals("Key (i)=(1) already exists.", duplicate.get('D'));
            assertEquals("public", duplicate.get('s'));
            assertEquals("zwt_e", duplicate.get('t'));
            assertEquals("zwt_e_pkey", duplicate.get('n'));

            Map<Character, String> notNull = wire.error("INSERT INTO zwt_e VALUES (2,NULL)");
            assertEquals("23502", notNull.get('C'));
            assertEquals("Failing row contains (2, null).", notNull.get('D'));
            assertEquals("j", notNull.get('c'));

            Map<Character, String> check = wire.error("INSERT INTO zwt_e VALUES (3,-1)");
            assertEquals("23514", check.get('C'));
            assertEquals("zwt_e_j_check", check.get('n'));

            Map<Character, String> noFunction = wire.error("SELECT nosuchfunction(1)");
            assertEquals("42883", noFunction.get('C'));
            assertEquals("No function matches the given name and argument types."
                    + " You might need to add explicit type casts.", noFunction.get('H'));

            // Severity is written twice: once localised and once not, which is what a client
            // reading the newer field expects to find beside the older one.
            assertEquals("ERROR", noFunction.get('S'));
            assertEquals("ERROR", noFunction.get('V'));
        } finally {
            wire.query("DROP TABLE zwt_e");
        }
    }

    /**
     * The Position points at the word the statement went wrong on, or nowhere.
     *
     * <p>Guessed by looking for the first double-quoted name from the message anywhere in the
     * statement text, it landed on a string literal that happened to hold the same word, invented
     * a location for the utility statements PostgreSQL deliberately reports without one, and in a
     * query holding two statements measured the offset against the wrong one. A missing routine
     * got no Position at all, because its message names it without quotes.
     */
    @Test
    void aPositionPointsAtTheWordThatWentWrong() throws Exception {
        assertEquals("8", wire.error("SELECT nosuchcol FROM (SELECT 1 AS a) s").get('P'));
        assertEquals("15", wire.error("SELECT * FROM zwt_absent").get('P'));
        // A literal holding the same word is a value, not the name that could not be resolved.
        assertEquals("22",
                wire.error("SELECT 'nosuchcol' , nosuchcol FROM (SELECT 1 AS a) s").get('P'));
        // A bad value is reported against the whole literal, from where it opens.
        assertEquals("8", wire.error("SELECT 'abc'::integer").get('P'));
        assertEquals("12", wire.error("SELECT 1 + 'abc'::integer").get('P'));
        // A routine is named without quotes, and the name still points somewhere.
        assertEquals("8", wire.error("SELECT zwt_nosuchfunc(1)").get('P'));
        // A statement that names the object it acts on is reported without a location.
        assertNull(wire.error("DROP TABLE zwt_absent").get('P'));
        assertNull(wire.error("ALTER TABLE zwt_absent RENAME TO zwt_other").get('P'));
        assertNull(wire.error("TRUNCATE zwt_absent").get('P'));
        assertNull(wire.error("COMMENT ON TABLE zwt_absent IS 'x'").get('P'));
        // In a query holding two statements the offset is into the string the client sent.
        assertEquals("18", wire.error("SELECT 1; SELECT nosuchcol2").get('P'));
    }

    /**
     * A parameter the client keeps its own copy of is reported when, and only when, it changes.
     *
     * <p>Decided by looking for the parameter's name inside a statement beginning "SET", nothing
     * was reported for a RESET, for set_config, for DISCARD, or for the COMMIT that ends the
     * transaction a SET LOCAL was written in — so a client went on using a value the server had
     * already let go of. A SET that changed nothing reported a change all the same.
     */
    @Test
    void aChangedParameterIsReportedAndAnUnchangedOneIsNot() throws Exception {
        try {
            assertEquals(List.of("application_name=zwt_one"),
                    wire.reportedParameters("SET application_name = 'zwt_one'"));
            // Setting it to what it already holds changes nothing, so there is nothing to say.
            assertEquals(List.of(),
                    wire.reportedParameters("SET application_name = 'zwt_one'"));
            assertEquals(List.of("application_name=zwt_two"),
                    wire.reportedParameters(
                            "SELECT set_config('application_name', 'zwt_two', false)"));
            assertEquals(List.of("application_name="),
                    wire.reportedParameters("RESET application_name"));
            assertEquals(List.of("search_path=zwt_nowhere, public"),
                    wire.reportedParameters("SET search_path = zwt_nowhere, public"));
            assertEquals(List.of("search_path=\"$user\", public"),
                    wire.reportedParameters("RESET search_path"));
            // A parameter no client keeps a copy of is not reported at all.
            assertEquals(List.of(), wire.reportedParameters("SET work_mem = '5MB'"));
            // A setting that lasts as long as the transaction is let go of when it ends, and
            // the client is told both times.
            wire.query("BEGIN");
            assertEquals(List.of("application_name=zwt_local"),
                    wire.reportedParameters("SET LOCAL application_name = 'zwt_local'"));
            assertEquals(List.of("application_name="), wire.reportedParameters("COMMIT"));
        } finally {
            wire.query("RESET ALL");
        }
    }
}
