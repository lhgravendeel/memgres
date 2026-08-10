package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The grammar and the context a statement runs in.
 *
 * <p>The cursor statements were matched keyword by keyword in a fixed order, so {@code SCROLL
 * BINARY} was a syntax error while {@code SCROLL NO SCROLL} was a cursor; a fetch count went
 * through a bare {@code Integer.parseInt}, so one too large reached the client as an internal
 * error; and nothing required a statement to end, so anything written after one was ignored.
 * LISTEN, NOTIFY, UNLISTEN and DISCARD read their argument with the general identifier reader,
 * which accepts a string literal and a reserved word alike. A DO block's language was read and
 * discarded, so every block ran as PL/pgSQL whatever it said it was, and its body was never
 * compiled.
 */
class StatementLifecycleTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    private static void syntaxError(String sql) {
        assertEquals("42601", refused(sql).getSQLState(), sql);
    }

    /** A cursor's options are a list, in any order and repeatable. */
    @Test
    void cursorOptionsAreAList() throws Exception {
        exec("BEGIN");
        try {
            exec("DECLARE zz_slt1 SCROLL BINARY CURSOR FOR SELECT 1");
            exec("DECLARE zz_slt2 INSENSITIVE BINARY CURSOR FOR SELECT 1");
            exec("DECLARE zz_slt3 BINARY BINARY CURSOR FOR SELECT 1");
            exec("DECLARE zz_slt4 SCROLL INSENSITIVE CURSOR FOR SELECT 1");
            exec("DECLARE zz_slt5 NO SCROLL BINARY CURSOR FOR SELECT 1");
            exec("DECLARE zz_slt6 SCROLL SCROLL CURSOR FOR SELECT 1");
        } finally {
            exec("ROLLBACK");
        }
    }

    /** But a cursor cannot be told to scroll and not to scroll. */
    @Test
    void scrollAndNoScrollCannotBothBeAsked() {
        assertEquals("42P11", refused("DECLARE zz_slt7 SCROLL NO SCROLL CURSOR FOR SELECT 1").getSQLState());
        assertEquals("42P11", refused("DECLARE zz_slt8 NO SCROLL SCROLL CURSOR FOR SELECT 1").getSQLState());
    }

    /** HOLD is not optional after WITH, SCROLL is not optional after NO, and the name is a ColId. */
    @Test
    void theCursorGrammarIsFinished() {
        syntaxError("DECLARE zz_slt9 CURSOR WITH FOR SELECT 1");
        syntaxError("DECLARE zz_slta CURSOR WITHOUT FOR SELECT 1");
        syntaxError("DECLARE zz_sltb NO CURSOR FOR SELECT 1");
        syntaxError("DECLARE select CURSOR FOR SELECT 1");
        syntaxError("DECLARE all CURSOR FOR SELECT 1");
    }

    /** A fetch count is a signed integer, and one too large is a syntax error at the number. */
    @Test
    void fetchCountsAreSignedIntegers() throws Exception {
        for (String sql : new String[]{
                "FETCH 3000000000 FROM zz_sltc",
                "FETCH FORWARD 3000000000 FROM zz_sltc",
                "FETCH BACKWARD 3000000000 FROM zz_sltc",
                "FETCH ABSOLUTE 4000000000 FROM zz_sltc",
                "FETCH RELATIVE 9999999999 FROM zz_sltc",
                "MOVE ABSOLUTE 4000000000 IN zz_sltc"}) {
            SQLException e = refused(sql);
            assertEquals("42601", e.getSQLState(), sql);
            assertFalse(e.getMessage().contains("Internal error"), sql);
        }
        // A leading plus belongs to the count, so the cursor is what is missing, not the syntax.
        for (String sql : new String[]{
                "FETCH FORWARD +1 FROM zz_sltc", "FETCH FORWARD -1 FROM zz_sltc",
                "FETCH +2 FROM zz_sltc", "MOVE FORWARD +1 IN zz_sltc"}) {
            assertEquals("34000", refused(sql).getSQLState(), sql);
        }
    }

    /** Nothing may be written after a cursor statement. */
    @Test
    void aCursorStatementHasToEnd() {
        syntaxError("FETCH NEXT FROM zz_sltc junk");
        syntaxError("MOVE NEXT FROM zz_sltc 1 2 3");
        syntaxError("CLOSE zz_sltc junk");
        syntaxError("CLOSE ALL junk");
    }

    /** A channel is one plain identifier. */
    @Test
    void channelsAreNamedLikeColumns() {
        syntaxError("LISTEN zz_l1, zz_l2");
        syntaxError("LISTEN 'zz_lit'");
        syntaxError("NOTIFY 'zz_lit'");
        syntaxError("UNLISTEN 'zz_lit'");
        syntaxError("LISTEN select");
        syntaxError("NOTIFY all");
        syntaxError("UNLISTEN table");
        syntaxError("NOTIFY zz_ex, 'a' || 'b'");
        syntaxError("NOTIFY zz_ex, 'a', 'b'");
    }

    /** A payload is one string constant, however it is quoted. */
    @Test
    void aPayloadMayBeDollarQuoted() throws Exception {
        exec("NOTIFY zz_dq, $$hi$$");
    }

    /** And a channel keeps the case it was quoted with. */
    @Test
    void channelNamesAreCaseSensitive() throws Exception {
        exec("LISTEN \"Zz_Mixed\"");
        try {
            assertEquals("Zz_Mixed", scalar("SELECT c FROM pg_listening_channels() c"));
            exec("UNLISTEN \"zz_mixed\"");
            assertEquals("1", scalar("SELECT count(*) FROM pg_listening_channels()"));
        } finally {
            exec("UNLISTEN \"Zz_Mixed\"");
        }
    }

    /** DISCARD names one of the things it knows how to throw away. */
    @Test
    void discardNamesOneOfItsTargets() throws Exception {
        syntaxError("DISCARD BOGUS");
        syntaxError("DISCARD SEQUENCE");
        syntaxError("DISCARD ALL EXTRA");
        exec("DISCARD PLANS");
    }

    /** A custom parameter set once exists for the session, RESET or no RESET. */
    @Test
    void resetKeepsACustomParameter() throws Exception {
        exec("SET zz_x.k = 'v'");
        exec("RESET ALL");
        assertEquals("", scalar("SELECT current_setting('zz_x.k', true)"));
        assertEquals("f", scalar("SELECT current_setting('zz_x.k', true) IS NULL"));
    }

    /** Only a language with an inline handler can carry a DO block. */
    @Test
    void aDoBlockNamesItsLanguage() {
        assertEquals("42704", refused("DO LANGUAGE nosuchlang_zz $$ BEGIN NULL; END $$").getSQLState());
        assertEquals("42704", refused("DO LANGUAGE \"PLPGSQL\" $$ BEGIN NULL; END $$").getSQLState());
        assertEquals("0A000", refused("DO LANGUAGE sql $$ SELECT 1 $$").getSQLState());
        assertEquals("0A000", refused("DO LANGUAGE c $$ BEGIN NULL; END $$").getSQLState());
        assertEquals("0A000", refused("DO $$ BEGIN NULL; END $$ LANGUAGE sql").getSQLState());
        SQLException twice = refused("DO $$ BEGIN NULL; END $$ LANGUAGE plpgsql LANGUAGE plpgsql");
        assertEquals("42601", twice.getSQLState());
        assertTrue(twice.getMessage().contains("conflicting or redundant options"));
    }

    /** And it needs a body, with nothing written after it. */
    @Test
    void aDoBlockNeedsABody() {
        syntaxError("DO");
        syntaxError("DO $$ $$");
        syntaxError("DO ''");
        syntaxError("DO 42");
        syntaxError("DO $$ BEGIN NULL; END $$ EXTRA");
    }

    /** The body is compiled before it runs. */
    @Test
    void aDoBlockIsCompiled() throws Exception {
        syntaxError("DO $$ BEGIN NULL END $$");
        syntaxError("DO $$ DECLARE x int; BEGIN x := 1 END $$");
        SQLException noDest = refused("DO $$ BEGIN SELECT 1; END $$");
        assertEquals("42601", noDest.getSQLState());
        assertTrue(noDest.getMessage().contains("no destination for result data"));
        assertEquals("42804", refused("DO $$ BEGIN RETURN 1; END $$").getSQLState());
        assertEquals("42804",
                refused("DO $$ DECLARE x int; BEGIN x := 1; RETURN NEXT x; END $$").getSQLState());
        assertEquals("42804", refused("DO $$ BEGIN RETURN QUERY SELECT 1; END $$").getSQLState());
        // The two forms that do say where the rows go still run.
        exec("DO $$ BEGIN PERFORM 1; END $$");
        exec("DO $$ DECLARE x int; BEGIN SELECT 1 INTO x; END $$");
    }

    /** Statements that cannot run from a routine see the routine, not the transaction. */
    @Test
    void aRoutineIsAContextOfItsOwn() throws Exception {
        assertEquals("25001", refused("DO $$ BEGIN EXECUTE 'DISCARD ALL'; END $$").getSQLState());
        exec("CREATE TABLE IF NOT EXISTS zz_slt_vt (a int)");
        try {
            SQLException e = refused("DO $$ BEGIN VACUUM zz_slt_vt; END $$");
            assertEquals("25001", e.getSQLState());
            assertTrue(e.getMessage().contains("cannot be executed from a function"));
        } finally {
            exec("DROP TABLE IF EXISTS zz_slt_vt");
        }
    }

    /** A read-only transaction that has run a query cannot be made read-write again. */
    @Test
    void readWriteIsChosenBeforeTheSnapshot() throws Exception {
        exec("BEGIN");
        try {
            exec("SELECT 1");
            exec("SET TRANSACTION READ WRITE");
            exec("SET TRANSACTION READ ONLY");
            SQLException e = refused("SET TRANSACTION READ WRITE");
            assertEquals("25001", e.getSQLState());
            assertTrue(e.getMessage().contains("must be set before any query"));
        } finally {
            exec("ROLLBACK");
        }
    }
}
