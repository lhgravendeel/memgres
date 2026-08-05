package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a PL/pgSQL variable holds, and what a function is allowed to hand back.
 *
 * <p><b>A variable holds values of its declared type.</b> PostgreSQL coerces on every assignment,
 * not only on the initialiser: an {@code int} takes 2 from 1.7, a {@code boolean} takes true from
 * {@code 'yes'}, and neither takes {@code 'abc'} at all. Storing whatever the expression produced
 * let a variable hold something its own type could never represent, and the wrong value then
 * travelled everywhere the variable did — the {@code boolean} case answered <em>false</em> to
 * {@code 'yes'}, which is a wrong answer rather than a missing error.
 *
 * <p><b>A row constructor being returned is not coerced either.</b> {@code RETURN ROW(x, x)} whose
 * {@code x} is an {@code integer} does not fit a {@code (bigint, bigint)}, and a bare {@code 'a'}
 * is {@code unknown} rather than {@code text}, so it fits no attribute at all. The record would
 * otherwise be handed back under a name it does not fit and read at the wrong field offsets.
 *
 * <p><b>A cursor opened without a name gets one.</b> It is not the variable's name: two functions
 * each declaring a cursor called {@code c} open two different portals, and the generated name is
 * what a caller handed the refcursor back has to FETCH from.
 */
class PlpgsqlValueSemanticsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- SECTION A
    // A variable holds values of its declared type.

    @Test
    void anAssignmentIsCoercedToTheDeclaredType() throws Exception {
        exec("CREATE FUNCTION pv_round() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x int; BEGIN x := 1.7; RETURN x::text; END $$");
        assertEquals("2", scalar("SELECT pv_round()"));
        exec("CREATE FUNCTION pv_parse() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x int; BEGIN x := '42'; RETURN x::text; END $$");
        assertEquals("42", scalar("SELECT pv_parse()"));
        exec("CREATE FUNCTION pv_totext() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x text; BEGIN x := 42; RETURN x; END $$");
        assertEquals("42", scalar("SELECT pv_totext()"));
        exec("CREATE FUNCTION pv_date() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x date; BEGIN x := '2020-01-01'; RETURN x::text; END $$");
        assertEquals("2020-01-01", scalar("SELECT pv_date()"));
    }

    /**
     * The boolean case is the one that mattered: {@code 'yes'} is true to PostgreSQL's input
     * function and false to a naive parse, so the variable held the opposite of what was written.
     */
    @Test
    void aBooleanTakesPostgresBooleanInput() throws Exception {
        exec("CREATE FUNCTION pv_yes() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x boolean; BEGIN x := 'yes'; RETURN x::text; END $$");
        assertEquals("true", scalar("SELECT pv_yes()"));
        exec("CREATE FUNCTION pv_on() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x boolean; BEGIN x := 'on'; RETURN x::text; END $$");
        assertEquals("true", scalar("SELECT pv_on()"));
        exec("CREATE FUNCTION pv_off() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x boolean; BEGIN x := 'off'; RETURN x::text; END $$");
        assertEquals("false", scalar("SELECT pv_off()"));
    }

    @Test
    void aValueTheTypeCannotHoldIsRefused() throws Exception {
        exec("CREATE FUNCTION pv_bad() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x int; BEGIN x := 'abc'; RETURN x::text; END $$");
        assertRejected("22P02", "invalid input syntax for type integer", "SELECT pv_bad()");
        exec("CREATE FUNCTION pv_badbool() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE x boolean; BEGIN x := 'abc'; RETURN x::text; END $$");
        assertRejected("22P02", "invalid input syntax for type boolean", "SELECT pv_badbool()");
        // The same holds for a value arriving through SELECT ... INTO.
        assertRejected("22P02", "invalid input syntax for type integer",
                "DO $$ DECLARE x int; BEGIN SELECT 'abc' INTO x; END $$");
    }

    /** The declared width still bites, and it is checked rather than quietly truncating. */
    @Test
    void aDeclaredWidthIsStillEnforced() {
        assertRejected("22001", "value too long for type character varying(3)",
                "DO $$ DECLARE v varchar(3); BEGIN v := 'abcdef'; END $$");
        assertRejected("22001", "value too long for type character(3)",
                "DO $$ DECLARE v char(3); BEGIN v := 'abcdef'; END $$");
        assertRejected("22003", "numeric field overflow",
                "DO $$ DECLARE n numeric(3,1); BEGIN n := 12345.6; END $$");
        assertAccepted("DO $$ DECLARE v varchar(3); BEGIN v := 'abc'; END $$");
    }

    // ---------------------------------------------------------------- SECTION B
    // A returned row is built from the return type's own attribute types.

    @Test
    void aReturnedRowMustBeBuiltOfTheRightTypes() throws Exception {
        exec("CREATE TYPE pv_two AS (q1 bigint, q2 bigint)");
        // An integer expression does not fit a bigint attribute: RETURN coerces nothing.
        exec("CREATE FUNCTION pv_narrow(x int) RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW(x, x); END $$");
        assertRejected("42804", "returned record type does not match expected record type",
                "SELECT (pv_narrow(42)).q1");
        // Nor do integer literals.
        exec("CREATE FUNCTION pv_lits() RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW(1, 2); END $$");
        assertRejected("42804", "returned record type does not match expected record type",
                "SELECT (pv_lits()).q1");
        // A bigint parameter, an explicit cast, or a variable of the type all do.
        exec("CREATE FUNCTION pv_exact(x bigint) RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW(x, x); END $$");
        assertEquals("42", scalar("SELECT (pv_exact(42)).q1"));
        exec("CREATE FUNCTION pv_cast(x int) RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW(x::bigint, x::bigint); END $$");
        assertEquals("42", scalar("SELECT (pv_cast(42)).q1"));
        exec("CREATE FUNCTION pv_rowvar() RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ DECLARE r pv_two; BEGIN r.q1 := 1; r.q2 := 2; RETURN r; END $$");
        assertEquals("1", scalar("SELECT (pv_rowvar()).q1"));
        // A row of the wrong width is refused as it always was.
        exec("CREATE FUNCTION pv_wide(x bigint) RETURNS pv_two LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW(x, x, x); END $$");
        assertRejected("42804", "returned record type does not match expected record type",
                "SELECT (pv_wide(42)).q1");
    }

    /** A bare string literal is unknown rather than text, so it fits no attribute at all. */
    @Test
    void anUnknownLiteralFitsNoAttribute() throws Exception {
        exec("CREATE TYPE pv_t AS (a text)");
        exec("CREATE FUNCTION pv_unknown() RETURNS pv_t LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW('a'); END $$");
        assertRejected("42804", "returned record type does not match expected record type",
                "SELECT (pv_unknown()).a");
        // Said to be text, it fits.
        exec("CREATE FUNCTION pv_known() RETURNS pv_t LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN ROW('a'::text); END $$");
        assertEquals("a", scalar("SELECT (pv_known()).a"));
        // ...and varchar is a different type from text, so it does not.
        exec("CREATE FUNCTION pv_vc() RETURNS pv_t LANGUAGE plpgsql AS"
                + " $$ DECLARE v varchar(5) := 'a'; BEGIN RETURN ROW(v); END $$");
        assertRejected("42804", "returned record type does not match expected record type",
                "SELECT (pv_vc()).a");
    }

    /** A trigger returns the row it wants written; a scalar is not one. */
    @Test
    void aTriggerCannotReturnAScalar() throws Exception {
        exec("CREATE TABLE pv_tr (a int, b text)");
        exec("INSERT INTO pv_tr VALUES (1, 'x')");
        exec("CREATE FUNCTION pv_badtrig() RETURNS trigger LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN 1; END $$");
        exec("CREATE TRIGGER pv_t1 BEFORE UPDATE ON pv_tr FOR EACH ROW"
                + " EXECUTE FUNCTION pv_badtrig()");
        assertRejected("42804", "cannot return non-composite value from function returning"
                + " composite type", "UPDATE pv_tr SET a = a");
        // The row is untouched by the refused write.
        assertEquals("1", scalar("SELECT a::text FROM pv_tr"));
    }

    // ---------------------------------------------------------------- SECTION C
    // The residuals around them.

    /**
     * A cursor with no name of its own gets a generated portal name, and that is what the
     * variable holds — so two functions each declaring a {@code c} do not collide.
     */
    @Test
    void anUnnamedCursorIsGivenAPortalName() throws Exception {
        exec("CREATE TABLE pv_c (a int)");
        exec("INSERT INTO pv_c VALUES (1),(2)");
        exec("CREATE FUNCTION pv_open() RETURNS refcursor LANGUAGE plpgsql AS"
                + " $$ DECLARE c refcursor; BEGIN OPEN c FOR SELECT a FROM pv_c ORDER BY a;"
                + " RETURN c; END $$");
        assertTrue(scalar("SELECT pv_open()::text").startsWith("<unnamed portal "),
                "an unnamed cursor should be given a generated portal name");
        // A cursor that was given a name keeps it, and a caller can FETCH from that name.
        exec("CREATE FUNCTION pv_named() RETURNS refcursor LANGUAGE plpgsql AS"
                + " $$ DECLARE c refcursor := 'pv_cur'; BEGIN OPEN c FOR SELECT a FROM pv_c"
                + " ORDER BY a; RETURN c; END $$");
        assertEquals("pv_cur", scalar("SELECT pv_named()::text"));
        // Reopening one that is still open is refused under the name it actually has.
        assertRejected("42P03", "already in use",
                "DO $$ DECLARE c refcursor := 'pv_cur2';"
                        + " BEGIN OPEN c FOR SELECT a FROM pv_c; OPEN c FOR SELECT a FROM pv_c; END $$");
    }

    /** ERRCODE takes a SQLSTATE or a condition name, and names no condition otherwise. */
    @Test
    void raiseErrcodeNamesAConditionOrACode() {
        assertRejected("42704", "unrecognized exception condition \"notvalid\"",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'notvalid'; END $$");
        assertRejected("22012", "boom",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'division_by_zero'; END $$");
        assertRejected("12345", "boom",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = '12345'; END $$");
        assertRejected("ABCDE", "boom",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING ERRCODE = 'ABCDE'; END $$");
    }

    /** A word PostgreSQL did not recognise is quoted back the way it was written. */
    @Test
    void anUnrecognisedWordIsEchoedAsWritten() {
        assertRejected("42601", "unrecognized RAISE statement option at or near \"NoSuchOpt\"",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING NoSuchOpt = 'x'; END $$");
        assertRejected("42601", "unrecognized GET DIAGNOSTICS item at or near \"NoSuchItem\"",
                "DO $$ DECLARE x text; BEGIN GET DIAGNOSTICS x = NoSuchItem; END $$");
        // The lower-case spelling comes back lower-case, so nothing is being upper-cased either.
        assertRejected("42601", "unrecognized RAISE statement option at or near \"nosuchopt\"",
                "DO $$ BEGIN RAISE EXCEPTION 'boom' USING nosuchopt = 'x'; END $$");
    }

    /** EXECUTE runs whatever the string holds, and an INTO takes its row from the last one. */
    @Test
    void executeRunsEveryStatementInTheString() throws Exception {
        exec("CREATE TABLE pv_e (a int)");
        exec("DO $$ BEGIN EXECUTE 'INSERT INTO pv_e VALUES (1); INSERT INTO pv_e VALUES (2)'; END $$");
        assertEquals("2", scalar("SELECT count(*)::text FROM pv_e"));
        exec("CREATE FUNCTION pv_last() RETURNS int LANGUAGE plpgsql AS"
                + " $$ DECLARE v int; BEGIN EXECUTE 'SELECT 7; SELECT 8; SELECT 9' INTO v;"
                + " RETURN v; END $$");
        assertEquals("9", scalar("SELECT pv_last()::text"));
        // A semicolon inside a literal is text, not a separator.
        exec("CREATE FUNCTION pv_semi() RETURNS text LANGUAGE plpgsql AS"
                + " $$ DECLARE v text; BEGIN EXECUTE 'SELECT ''a;b''' INTO v; RETURN v; END $$");
        assertEquals("a;b", scalar("SELECT pv_semi()"));
        // A statement that is not one is still a syntax error.
        assertRejected("42601", "syntax error", "DO $$ BEGIN EXECUTE 'SELECT 1; garbage'; END $$");
    }
}
