package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a routine definition must settle before it is one.
 *
 * <p>A CREATE FUNCTION carries a list of options, and PostgreSQL reads them left to right in
 * whatever order they were written. memgres read them as an AS clause and a LANGUAGE clause in one
 * of two fixed arrangements, so anything written a third way — WINDOW ahead of LANGUAGE, say — was
 * left sitting unread after the statement, and the routine was created out of the part that had
 * been reached. Nothing checked that a language had been named at all, that there was a body to
 * run, or that an option was given once, so a definition PostgreSQL refuses outright was accepted
 * and what it created was not what had been written.
 */
class RoutineDefinitionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void clean() throws Exception {
        exec("DROP FUNCTION IF EXISTS rd_f(int) CASCADE");
        exec("DROP FUNCTION IF EXISTS rd_f() CASCADE");
        exec("DROP PROCEDURE IF EXISTS rd_p() CASCADE");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** Runs {@code sql} and returns the refusal, failing when it is accepted. */
    private static SQLException refused(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), "should have been refused: " + sql);
        return e;
    }

    private static void refusedWith(String state, String message, String sql) {
        SQLException e = refused(sql);
        assertEquals(state, e.getSQLState(), sql);
        assertTrue(e.getMessage() != null && e.getMessage().contains(message),
                "expected \"" + message + "\" in: " + e.getMessage());
    }

    // ------------------------------------------------------- a language, and something to run

    /** A definition that never says what language its body is written in is not a definition. */
    @Test
    void aDefinitionMustNameItsLanguage() {
        refusedWith("42P13", "no language specified",
                "CREATE FUNCTION rd_f(p int) RETURNS int");
        refusedWith("42P13", "no language specified",
                "CREATE FUNCTION rd_f(p int) RETURNS int AS $$ SELECT p $$");
        refusedWith("42P13", "no language specified",
                "CREATE FUNCTION rd_f(p int) RETURNS int IMMUTABLE");
        refusedWith("42P13", "no language specified", "CREATE PROCEDURE rd_p()");
        refusedWith("42P13", "no language specified",
                "CREATE PROCEDURE rd_p() AS $$ SELECT 1 $$");
    }

    /** And having named one, it must say what that language is to run. */
    @Test
    void aDefinitionMustCarryABody() {
        refusedWith("42P13", "no function body specified",
                "CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE sql");
        refusedWith("42P13", "no function body specified",
                "CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE plpgsql");
        refusedWith("42P13", "no function body specified",
                "CREATE PROCEDURE rd_p() LANGUAGE sql");
    }

    /**
     * The language is looked up first: there is no point asking what a language is to run before
     * knowing whether it is a language.
     */
    @Test
    void anUnknownLanguageIsReportedAheadOfTheMissingBody() {
        refusedWith("42704", "language \"nosuchlang\" does not exist",
                "CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE nosuchlang");
        refusedWith("42704", "language \"nosuchlang\" does not exist",
                "CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE nosuchlang AS $$ SELECT p $$");
    }

    // ------------------------------------------------------- an option given twice

    /** An option may be given once. It is the group that repeats, not only the word. */
    @Test
    void anOptionGivenTwiceIsRefused() {
        String[] twice = {
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ LANGUAGE sql",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ AS $$ SELECT 2 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql STRICT STRICT AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql STRICT CALLED ON NULL INPUT AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql IMMUTABLE IMMUTABLE AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql IMMUTABLE STABLE AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql VOLATILE IMMUTABLE AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql COST 1 COST 2 AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql ROWS 5 ROWS 6 AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql SECURITY DEFINER SECURITY INVOKER AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql LEAKPROOF NOT LEAKPROOF AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql PARALLEL SAFE PARALLEL UNSAFE AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int WINDOW WINDOW LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int SUPPORT a SUPPORT b LANGUAGE sql AS $$ SELECT 1 $$",
        };
        for (String sql : twice) {
            refusedWith("42601", "conflicting or redundant options", sql);
        }
    }

    /** SET is the exception: it names a parameter apiece, so it accumulates rather than repeats. */
    @Test
    void setNamesAParameterApieceAndAccumulates() throws Exception {
        exec("CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql"
                + " SET work_mem = '1MB' SET statement_timeout = '5s' AS $$ SELECT 1 $$");
        assertEquals("1", scalar("SELECT rd_f()"));
    }

    /** ALTER reads the same option list, so the same repeat is the same refusal. */
    @Test
    void alterReadsTheSameOptionList() throws Exception {
        exec("CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        refusedWith("42601", "conflicting or redundant options", "ALTER FUNCTION rd_f() IMMUTABLE STABLE");
        refusedWith("42601", "conflicting or redundant options", "ALTER FUNCTION rd_f() STRICT STRICT");
        refusedWith("42601", "conflicting or redundant options", "ALTER FUNCTION rd_f() COST 5 COST 6");
        // One of each is still one of each.
        exec("ALTER FUNCTION rd_f() IMMUTABLE STRICT COST 5 PARALLEL SAFE");
        assertEquals("i", scalar("SELECT provolatile FROM pg_proc WHERE proname = 'rd_f'"));
    }

    // ------------------------------------------------------- what only a function may be

    /**
     * A procedure is called for its effect rather than its value, so nothing describing how a
     * value is computed applies to it. That check comes before the repeat check, which is why a
     * procedure written {@code STRICT STRICT} is refused for being a procedure.
     */
    @Test
    void aProcedureRefusesTheAttributesOnlyAFunctionHas() {
        String[] functionOnly = {
            "CREATE PROCEDURE rd_p() STRICT LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() STRICT STRICT LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() IMMUTABLE LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() LEAKPROOF LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() COST 5 LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() ROWS 5 LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() PARALLEL SAFE LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() WINDOW LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE PROCEDURE rd_p() CALLED ON NULL INPUT LANGUAGE sql AS $$ SELECT 1 $$",
        };
        for (String sql : functionOnly) {
            refusedWith("42P13", "invalid attribute in procedure definition", sql);
        }
    }

    /** The ones a procedure does take. */
    @Test
    void aProcedureTakesLanguageSecurityAndSet() throws Exception {
        exec("CREATE PROCEDURE rd_p() SECURITY DEFINER LANGUAGE sql AS $$ SELECT 1 $$");
        assertEquals("true", scalar("SELECT prosecdef::text FROM pg_proc WHERE proname = 'rd_p'"));
        exec("DROP PROCEDURE rd_p()");
        exec("CREATE PROCEDURE rd_p() SET work_mem = '1MB' LANGUAGE sql AS $$ SELECT 1 $$");
        assertEquals("{work_mem=1MB}", scalar("SELECT proconfig::text FROM pg_proc WHERE proname = 'rd_p'"));
    }

    // ------------------------------------------------------- how many bodies, and of which kind

    /** Only C reads a second AS item, as the symbol inside the object file the first one names. */
    @Test
    void onlyCReadsASecondAsItem() {
        refusedWith("42P13", "only one AS item needed for language \"sql\"",
                "CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE sql AS 'SELECT p', 'extra'");
    }

    /** Two bodies is not more definition than one, it is a definition that says two things. */
    @Test
    void twoBodiesAreRefused() {
        refusedWith("42P13", "duplicate function body specified",
                "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$ RETURN 1");
    }

    /**
     * A SQL-standard body says by itself that the language is SQL, so it needs no LANGUAGE clause
     * — and cannot be given a different one.
     */
    @Test
    void aSqlStandardBodySpeaksForTheLanguage() throws Exception {
        exec("CREATE FUNCTION rd_f() RETURNS int RETURN 1");
        assertEquals("1", scalar("SELECT rd_f()"));
        exec("DROP FUNCTION rd_f()");
        exec("CREATE FUNCTION rd_f() RETURNS int BEGIN ATOMIC SELECT 1; END");
        assertEquals("1", scalar("SELECT rd_f()"));
        exec("DROP FUNCTION rd_f()");
        // An option in front of it does not change that.
        exec("CREATE FUNCTION rd_f() RETURNS int STRICT RETURN 1");
        assertEquals("true", scalar("SELECT proisstrict::text FROM pg_proc WHERE proname = 'rd_f'"));
        exec("DROP FUNCTION rd_f()");
        refusedWith("42P13", "inline SQL function body only valid for language SQL",
                "CREATE FUNCTION rd_f() RETURNS int LANGUAGE plpgsql RETURN 1");
    }

    // ------------------------------------------------------- the options are read, whatever the order

    /**
     * An option written ahead of the language used to leave the rest of the statement unread: the
     * body went unnoticed and the routine was created empty, having reported no error at all.
     */
    @Test
    void anOptionAheadOfTheLanguageDoesNotSwallowTheRest() throws Exception {
        exec("CREATE FUNCTION rd_f(p int) RETURNS int WINDOW LANGUAGE sql AS $$ SELECT p $$");
        assertEquals("SELECT p", scalar("SELECT trim(prosrc) FROM pg_proc WHERE proname = 'rd_f'"));
        assertEquals("w", scalar("SELECT prokind FROM pg_proc WHERE proname = 'rd_f'"));
    }

    /** And the orders that already worked still do. */
    @Test
    void everyOrderOfTheSameOptionsMeansTheSameThing() throws Exception {
        String[] orders = {
            "CREATE FUNCTION rd_f() RETURNS int AS $$ SELECT 1 $$ IMMUTABLE LANGUAGE sql STRICT",
            "CREATE FUNCTION rd_f() RETURNS int IMMUTABLE LANGUAGE sql STRICT AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT 1 $$",
            "CREATE FUNCTION rd_f() RETURNS int STRICT AS $$ SELECT 1 $$ LANGUAGE sql IMMUTABLE",
        };
        for (String sql : orders) {
            exec("DROP FUNCTION IF EXISTS rd_f()");
            exec(sql);
            assertEquals("itrue", scalar("SELECT provolatile::text || proisstrict::text"
                    + " FROM pg_proc WHERE proname = 'rd_f'"), sql);
            assertEquals("1", scalar("SELECT rd_f()"), sql);
        }
    }

    // ------------------------------------------------------- and are read back out again

    /**
     * The attribute line is part of the definition rather than decoration, so a definition
     * deparsed without it does not create what was asked about. Every attribute came back missing.
     */
    @Test
    void theDefinitionReadsBackWithItsAttributes() throws Exception {
        exec("CREATE FUNCTION rd_f(p int) RETURNS int LANGUAGE sql"
                + " IMMUTABLE STRICT PARALLEL SAFE LEAKPROOF SECURITY DEFINER COST 42 AS $$ SELECT p $$");
        assertEquals("CREATE OR REPLACE FUNCTION public.rd_f(p integer)\n"
                        + " RETURNS integer\n"
                        + " LANGUAGE sql\n"
                        + " IMMUTABLE PARALLEL SAFE STRICT SECURITY DEFINER LEAKPROOF COST 42\n"
                        + "AS $function$ SELECT p $function$\n",
                scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rd_f'"));
    }

    /** A window function says so, and a procedure's body is quoted as a procedure's. */
    @Test
    void theDeparsedFormNamesWhatWasDefined() throws Exception {
        exec("CREATE FUNCTION rd_f(p int) RETURNS int WINDOW LANGUAGE sql AS $$ SELECT p $$");
        assertTrue(scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rd_f'")
                .contains("\n WINDOW\n"));
        exec("CREATE PROCEDURE rd_p() LANGUAGE sql AS $$ SELECT 1 $$");
        assertEquals("CREATE OR REPLACE PROCEDURE public.rd_p()\n"
                        + " LANGUAGE sql\n"
                        + "AS $procedure$ SELECT 1 $procedure$\n",
                scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rd_p'"));
    }

    /**
     * A cost nobody wrote is the language's own. PostgreSQL charges 100 for a routine it has to
     * interpret, SQL and PL/pgSQL alike, and 1 only where the call is compiled in — and it leaves
     * the clause off the deparsed definition when the cost is that default.
     */
    @Test
    void anUnwrittenCostIsTheLanguagesOwn() throws Exception {
        for (String lang : new String[]{"sql", "plpgsql"}) {
            exec("DROP FUNCTION IF EXISTS rd_f()");
            exec("CREATE FUNCTION rd_f() RETURNS int LANGUAGE " + lang
                    + (lang.equals("sql") ? " AS $$ SELECT 1 $$" : " AS $$ BEGIN RETURN 1; END $$"));
            assertEquals("100", scalar("SELECT procost::text FROM pg_proc WHERE proname = 'rd_f'"), lang);
            assertFalse(scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rd_f'")
                    .contains("COST"), lang);
        }
        // One that was written is kept, and is printed because it is not the default.
        exec("DROP FUNCTION rd_f()");
        exec("CREATE FUNCTION rd_f() RETURNS int LANGUAGE sql COST 7 AS $$ SELECT 1 $$");
        assertEquals("7", scalar("SELECT procost::text FROM pg_proc WHERE proname = 'rd_f'"));
        assertTrue(scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rd_f'")
                .contains("COST 7"));
    }
}
