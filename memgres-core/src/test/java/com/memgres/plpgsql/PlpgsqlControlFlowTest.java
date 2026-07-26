package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A loop label is an ordinary name, so it may be spelled with a word that is also SQL keyword —
 * {@code outer} and {@code end} are the ones people reach for. When EXIT refuses such a label it
 * does not fail loudly: the signal is read as an unlabelled EXIT, leaves the wrong loop, and the
 * intended one spins forever. A label on a block must carry the same weight as a label on a loop.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>A1 nested labelled EXIT, A2 labelled block EXIT, A8 ON COMMIT DROP inside a function.
 */
class PlpgsqlControlFlowTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    /**
     * Each check gets its own connection. A loop that fails to terminate wedges the connection it
     * runs on, and a shared one would carry that stall into every later check.
     */
    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    /** Define a no-argument function whose body is {@code body} and return what it yields. */
    private static String call(String body) throws SQLException {
        try (Connection c = open()) {
            exec(c, "CREATE OR REPLACE FUNCTION cf() RETURNS text AS $$ DECLARE t text := ''; i int; "
                    + "BEGIN " + body + " RETURN t; END $$ LANGUAGE plpgsql");
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT cf()")) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    /** The label sits on the outer loop and EXIT is issued from the inner one. */
    @Test
    void aLabelledExitLeavesTheLabelledLoopNotTheInnermost() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("done", call(
                        "<<lp>> LOOP i := 0;"
                      + "  LOOP i := i + 1; EXIT lp WHEN i > 1; END LOOP;"
                      + "END LOOP; t := 'done';")));
    }

    /** {@code outer} is also a SQL keyword; as a label it is just a name. */
    @Test
    void aLabelSpelledWithAKeywordStillWorks() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("done", call(
                        "<<outer>> LOOP i := 0;"
                      + "  LOOP i := i + 1; EXIT outer WHEN i > 1; END LOOP;"
                      + "END LOOP; t := 'done';")));
    }

    @Test
    void aKeywordLabelWorksForContinueToo() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("13", call(
                        "<<outer>> FOR i IN 1..3 LOOP"
                      + "  CONTINUE outer WHEN i = 2; t := t || i::text;"
                      + "END LOOP;")));
    }

    /** EXIT with no label still leaves only the innermost loop. */
    @Test
    void anUnlabelledExitLeavesTheInnermostLoop() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("ab", call(
                        "FOR i IN 1..2 LOOP"
                      + "  LOOP t := t || 'a'; EXIT; END LOOP;"
                      + "  EXIT WHEN true;"
                      + "END LOOP; t := t || 'b';")));
    }

    /** A label may name a block, and EXIT then leaves that block. */
    @Test
    void aLabelledBlockCanBeExited() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("after", call(
                        "<<blk>> BEGIN EXIT blk; t := 'inside'; END; t := 'after';")));
    }

    @Test
    void anExitFromAnInnerBlockLeavesTheNamedOuterBlock() {
        assertTimeoutPreemptively(Duration.ofSeconds(15), () ->
                assertEquals("after", call(
                        "<<blk>> BEGIN BEGIN EXIT blk; END; t := 'inside'; END; t := 'after';")));
    }

    /**
     * A function body runs inside one statement, so a table it creates ON COMMIT DROP must live
     * until that statement finishes rather than vanishing at the CREATE.
     */
    @Test
    void aTempTableCreatedOnCommitDropSurvivesTheFunctionThatMadeIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE OR REPLACE FUNCTION cf_tmp() RETURNS int AS $$ "
                    + "BEGIN CREATE TEMP TABLE cf_t (i int) ON COMMIT DROP; "
                    + "INSERT INTO cf_t VALUES (1),(2); "
                    + "RETURN (SELECT count(*) FROM cf_t); END $$ LANGUAGE plpgsql");
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT cf_tmp()")) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    /** ON COMMIT DROP outside any transaction still drops as soon as the statement is over. */
    @Test
    void aTempTableCreatedOnCommitDropIsGoneAfterTheStatement() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TEMP TABLE cf_t2 (i int) ON COMMIT DROP");
            try (Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM information_schema.tables WHERE table_name='cf_t2'")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        }
    }
}
