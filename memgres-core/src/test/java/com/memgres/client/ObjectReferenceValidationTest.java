package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Naming an object that is not there, and naming one of the wrong kind.
 *
 * <p>COMMENT ON filed a comment against whatever it was given: a sequence, a role, a language or a
 * collation that did not exist all recorded one, and COMMENT ON TABLE on a sequence filed it as
 * though the sequence were a table. A routine's argument list was dropped before the name was
 * looked up, so a comment on one overload was a comment on whichever overload came first. GRANT
 * and REVOKE of a role membership recorded names that were not roles. Advisory locks coerced a key
 * too wide for the form that was called, so two callers naming different keys could take the same
 * one. And a transaction command written inside a PL/pgSQL body was passed to the engine, which
 * opened a transaction nothing then closed.
 */
class ObjectReferenceValidationTest {

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

    private static void state(String expected, String sql) {
        assertEquals(expected, refused(sql).getSQLState(), sql);
    }

    // ---- COMMENT ON names something that is there ----

    /** Each kind of object a comment can be on has to exist to carry one. */
    @Test
    void commentNamesAnObjectThatExists() {
        assertEquals("42P01", refused("COMMENT ON SEQUENCE zz_or_nosuch IS 'x'").getSQLState());
        assertEquals("42P01", refused("COMMENT ON MATERIALIZED VIEW zz_or_nosuch IS 'x'").getSQLState());
        assertMissing("role", "COMMENT ON ROLE zz_or_nosuch IS 'x'");
        assertMissing("extension", "COMMENT ON EXTENSION zz_or_nosuch IS 'x'");
        assertMissing("language", "COMMENT ON LANGUAGE zz_or_nosuch IS 'x'");
        assertMissing("event trigger", "COMMENT ON EVENT TRIGGER zz_or_nosuch IS 'x'");
        SQLException collation = refused("COMMENT ON COLLATION zz_or_nosuch IS 'x'");
        assertEquals("42704", collation.getSQLState());
        assertTrue(collation.getMessage()
                .contains("collation \"zz_or_nosuch\" for encoding \"UTF8\" does not exist"));
        SQLException large = refused("COMMENT ON LARGE OBJECT 987654321 IS 'x'");
        assertEquals("42704", large.getSQLState());
        assertTrue(large.getMessage().contains("large object 987654321 does not exist"));
        SQLException agg = refused("COMMENT ON AGGREGATE zz_or_nosuch(int) IS 'x'");
        assertEquals("42883", agg.getSQLState());
        assertTrue(agg.getMessage().contains("aggregate zz_or_nosuch(integer) does not exist"),
                agg.getMessage());
    }

    private static void assertMissing(String kind, String sql) {
        SQLException e = refused(sql);
        assertEquals("42704", e.getSQLState(), sql);
        assertTrue(e.getMessage().contains(kind + " \"zz_or_nosuch\" does not exist"),
                sql + " -> " + e.getMessage());
    }

    /** And it has to be of the kind the statement says it is. */
    @Test
    void commentNamesTheRightKindOfObject() throws Exception {
        exec("CREATE SEQUENCE zz_or_s");
        exec("CREATE FUNCTION zz_or_f(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$");
        exec("CREATE PROCEDURE zz_or_p() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        try {
            SQLException notTable = refused("COMMENT ON TABLE zz_or_s IS 'x'");
            assertEquals("42809", notTable.getSQLState());
            assertTrue(notTable.getMessage().contains("\"zz_or_s\" is not a table"));

            SQLException notAgg = refused("COMMENT ON AGGREGATE zz_or_f(int) IS 'x'");
            assertEquals("42809", notAgg.getSQLState());
            assertTrue(notAgg.getMessage().contains("function zz_or_f(integer) is not an aggregate"),
                    notAgg.getMessage());

            SQLException notFunction = refused("COMMENT ON FUNCTION zz_or_p() IS 'x'");
            assertEquals("42809", notFunction.getSQLState());
            assertTrue(notFunction.getMessage().contains("zz_or_p() is not a function"),
                    notFunction.getMessage());

            SQLException notProcedure = refused("COMMENT ON PROCEDURE zz_or_f(int) IS 'x'");
            assertEquals("42809", notProcedure.getSQLState());
            assertTrue(notProcedure.getMessage().contains("zz_or_f(integer) is not a procedure"),
                    notProcedure.getMessage());

            // A signature that names no overload is a routine that is not there.
            SQLException wrongArgs = refused("COMMENT ON FUNCTION zz_or_f(text) IS 'x'");
            assertEquals("42883", wrongArgs.getSQLState());
            assertTrue(wrongArgs.getMessage().contains("function zz_or_f(text) does not exist"),
                    wrongArgs.getMessage());
        } finally {
            exec("DROP PROCEDURE zz_or_p()");
            exec("DROP FUNCTION zz_or_f(int)");
            exec("DROP SEQUENCE zz_or_s");
        }
    }

    /** A composite type has attributes, and one of them can carry a comment. */
    @Test
    void aCompositeTypeAttributeCarriesAComment() throws Exception {
        exec("CREATE TYPE zz_or_ct AS (a int, b text)");
        try {
            exec("COMMENT ON COLUMN zz_or_ct.a IS 'ctcol'");
            assertEquals("ctcol", scalar("SELECT col_description('zz_or_ct'::regclass, 1)"));
            assertEquals("42703",
                    refused("COMMENT ON COLUMN zz_or_ct.nosuch IS 'x'").getSQLState());
        } finally {
            exec("DROP TYPE zz_or_ct");
        }
    }

    // ---- role memberships are between roles ----

    /** Both sides of a membership grant have to be roles. */
    @Test
    void aMembershipIsBetweenRoles() throws Exception {
        exec("CREATE ROLE zz_or_a");
        try {
            assertMissingRole("zz_or_nosuch", "GRANT zz_or_a TO zz_or_nosuch");
            assertMissingRole("zz_or_nosuch", "GRANT zz_or_nosuch TO zz_or_a");
            assertMissingRole("zz_or_nosuch", "REVOKE zz_or_a FROM zz_or_nosuch");
            assertMissingRole("public", "GRANT zz_or_a TO PUBLIC");
            // The session's own role may be named rather than spelled out.
            exec("GRANT zz_or_a TO CURRENT_USER");
            assertEquals("true", scalar(
                    "SELECT pg_has_role(current_user, 'zz_or_a', 'MEMBER')::text"));
        } finally {
            exec("DROP ROLE zz_or_a");
        }
    }

    private static void assertMissingRole(String role, String sql) {
        SQLException e = refused(sql);
        assertEquals("42704", e.getSQLState(), sql);
        assertTrue(e.getMessage().contains("role \"" + role + "\" does not exist"),
                sql + " -> " + e.getMessage());
    }

    // ---- advisory locks take the keys their form declares ----

    /** The two-key form takes two integers and the one-key form a bigint; wider is no function. */
    @Test
    void advisoryLockKeysAreNotWidened() {
        SQLException wide = refused("SELECT pg_try_advisory_lock(4294967296, 5)");
        assertEquals("42883", wide.getSQLState());
        assertTrue(wide.getMessage()
                .contains("function pg_try_advisory_lock(bigint, integer) does not exist"),
                wide.getMessage());
        SQLException huge = refused("SELECT pg_try_advisory_lock(9223372036854775808)");
        assertEquals("42883", huge.getSQLState());
        assertTrue(huge.getMessage()
                .contains("function pg_try_advisory_lock(numeric) does not exist"), huge.getMessage());
    }

    // ---- a body cannot open a transaction ----

    /** COMMIT and ROLLBACK are the transaction commands PL/pgSQL has; the rest it refuses. */
    @Test
    void aBodyCannotOpenATransaction() throws Exception {
        // BEGIN is not in this list: inside a body it opens a block, not a transaction.
        for (String command : new String[]{"START TRANSACTION", "SAVEPOINT sp", "ABORT"}) {
            SQLException e = refused("DO $$ BEGIN " + command + "; END $$");
            assertEquals("0A000", e.getSQLState(), command);
            assertTrue(e.getMessage().contains("unsupported transaction command in PL/pgSQL"),
                    command + " -> " + e.getMessage());
        }
        // PL/pgSQL's own ROLLBACK takes no savepoint, so that spelling is a syntax error.
        assertEquals("42601",
                refused("DO $$ BEGIN ROLLBACK TO SAVEPOINT sp; END $$").getSQLState());
        // And the session is still usable, because nothing was left open.
        assertEquals("after", scalar("SELECT 'after'"));
    }

    // ---- names and words the grammar bounds ----

    /** An identifier written with nothing between its quotes names nothing. */
    @Test
    void aDelimitedIdentifierIsNotEmpty() {
        for (String sql : new String[]{"LISTEN \"\"", "NOTIFY \"\"", "SELECT \"\""}) {
            SQLException e = refused(sql);
            assertEquals("42601", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("zero-length delimited identifier"), sql);
        }
    }

    /** A savepoint is named the way a column is. */
    @Test
    void savepointsAreNamedLikeColumns() throws Exception {
        // Each refusal aborts the transaction it is in, so each one gets a transaction of its own.
        for (String sql : new String[]{"SAVEPOINT ALL", "SAVEPOINT select",
                "RELEASE SAVEPOINT ALL"}) {
            exec("BEGIN");
            try {
                state("42601", sql);
            } finally {
                exec("ROLLBACK");
            }
        }
    }

    /** An unrecognised VACUUM option is named, folded the way any unquoted word is. */
    @Test
    void vacuumNamesTheOptionItDidNotKnow() {
        SQLException e = refused("VACUUM (BOGUS_OPTION)");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("unrecognized VACUUM option \"bogus_option\""),
                e.getMessage());
    }

    /** An array constructor with nothing in it has no type to be. */
    @Test
    void anEmptyArrayHasNoType() {
        SQLException e = refused("SELECT ARRAY[]");
        assertEquals("42P18", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot determine type of empty array"), e.getMessage());
    }

    /** A cursor option list takes the words PostgreSQL's does, keyword or not. */
    @Test
    void cursorOptionsIncludeTheUnreservedOnes() throws Exception {
        exec("CREATE TABLE zz_or_c (i int)");
        try {
            exec("BEGIN");
            try {
                exec("DECLARE zz_or_x1 ASENSITIVE CURSOR FOR SELECT i FROM zz_or_c ORDER BY i");
                exec("DECLARE zz_or_x2 INSENSITIVE SCROLL CURSOR FOR SELECT i FROM zz_or_c");
            } finally {
                exec("ROLLBACK");
            }
        } finally {
            exec("DROP TABLE zz_or_c");
        }
    }

    /** A catalogue is a relation, and one can be locked like any other. */
    @Test
    void catalogsCanBeLocked() throws Exception {
        exec("BEGIN");
        try {
            exec("LOCK TABLE pg_class IN ACCESS SHARE MODE");
        } finally {
            exec("ROLLBACK");
        }
    }

    // ---- CALL takes its arguments by name too ----

    /** A CALL may name its arguments with => or :=, and may pass a tail with VARIADIC. */
    @Test
    void callTakesNamedAndVariadicArguments() throws Exception {
        exec("CREATE PROCEDURE zz_or_np(a int, b int, OUT c text) LANGUAGE plpgsql"
                + " AS $$ BEGIN c := a::text || b::text; END $$");
        try {
            assertEquals("12", scalar("CALL zz_or_np(a => 1, b => 2, c => NULL)"));
            assertEquals("12", scalar("CALL zz_or_np(a := 1, b := 2, c := NULL)"));
            assertEquals("12", scalar("CALL zz_or_np(1, b => 2, c => NULL)"));
        } finally {
            exec("DROP PROCEDURE zz_or_np(int, int)");
        }
    }

    /** A procedure answers with nothing, so it has no RETURNS and cannot stand in a FROM list. */
    @Test
    void aProcedureIsNotAFunction() throws Exception {
        state("42601", "CREATE PROCEDURE zz_or_cb() RETURNS void LANGUAGE plpgsql"
                + " AS $$ BEGIN NULL; END $$");
        state("42601", "CREATE PROCEDURE zz_or_cc() RETURNS SETOF int LANGUAGE plpgsql"
                + " AS $$ BEGIN NULL; END $$");
        exec("CREATE PROCEDURE zz_or_rp(a int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        try {
            SQLException e = refused("SELECT * FROM zz_or_rp(1)");
            assertEquals("42809", e.getSQLState());
            assertTrue(e.getMessage().contains("is a procedure"), e.getMessage());
        } finally {
            exec("DROP PROCEDURE zz_or_rp(int)");
        }
    }

    // ---- settings hold the values their definitions describe ----

    /** A parameter counted in whole units holds a whole number of them. */
    @Test
    void integerSettingsRoundToTheirUnit() throws Exception {
        exec("SET default_statistics_target = 100.7");
        try {
            assertEquals("101", scalar("SELECT current_setting('default_statistics_target')"));
        } finally {
            exec("RESET default_statistics_target");
        }
        exec("SET lock_timeout = '2500us'");
        try {
            assertEquals("2ms", scalar("SHOW lock_timeout"));
        } finally {
            exec("RESET lock_timeout");
        }
    }

    /** DateStyle names a style, an order, or both; the one it leaves out keeps what it had. */
    @Test
    void dateStyleKeepsTheFieldItDoesNotName() throws Exception {
        exec("SET datestyle = 'ISO, YMD'");
        try {
            exec("SET datestyle = 'ISO'");
            assertEquals("ISO, YMD", scalar("SELECT current_setting('datestyle')"));
        } finally {
            exec("RESET datestyle");
        }
    }

    /** set_config is SET written as a call, and is judged by the same rules. */
    @Test
    void setConfigChecksItsValue() {
        for (String[] each : new String[][]{
                {"TimeZone", "bogus/zone"}, {"client_encoding", "BOGUS"}, {"role", "zz_or_nosuch"}}) {
            SQLException e = assertThrows(SQLException.class,
                    () -> exec("SELECT set_config('" + each[0] + "', '" + each[1] + "', false)"),
                    each[0]);
            assertEquals("22023", e.getSQLState(), each[0]);
        }
    }

    /** And a parameter fixed at server start refuses SET ... TO DEFAULT as it refuses RESET. */
    @Test
    void aFixedParameterRefusesDefaultToo() {
        assertEquals("55P02", refused("SET block_size TO DEFAULT").getSQLState());
    }

    /** A notify payload is bounded in bytes, not in the characters it was written with. */
    @Test
    void aPayloadIsBoundedInBytes() {
        SQLException e = refused(
                "DO $$ BEGIN EXECUTE 'NOTIFY zz_or_b, ' || quote_literal(repeat(U&'\\00E9', 4000)); END $$");
        assertEquals("22023", e.getSQLState());
        assertTrue(e.getMessage().contains("payload string too long"), e.getMessage());
    }
}
