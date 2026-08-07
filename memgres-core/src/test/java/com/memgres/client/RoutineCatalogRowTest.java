package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a user-created routine's pg_proc row says about it.
 *
 * <p>The row is how a client finds out what a routine takes and what it gives back, and most of it
 * was wrong for anything beyond a function of plain scalars returning one value. SETOF was recorded
 * as part of the return type rather than as the flag beside it, so {@code prorettype} was 0 — no
 * type at all — for every set-returning function. An array-typed parameter or result was 0 for the
 * same reason, and so was one carrying a precision. {@code pronargs} counted the OUT parameters a
 * call does not pass. A function deriving its result from its OUT parameters recorded a record
 * whatever it really returned, and a procedure with an INOUT parameter could not be created at all.
 * {@code proretset}, {@code prorows}, {@code provariadic} and {@code pronargdefaults} were left at
 * zero for every routine.
 */
class RoutineCatalogRowTest {

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

    /** Defines {@code sql} and answers {@code column} from the pg_proc row it left behind. */
    private static String define(String name, String sql, String column) throws SQLException {
        exec("DROP FUNCTION IF EXISTS " + name + " CASCADE");
        exec("DROP PROCEDURE IF EXISTS " + name + " CASCADE");
        exec(sql);
        return scalar("SELECT " + column + " FROM pg_proc WHERE proname = '" + name + "'");
    }

    // ------------------------------------------------------- what comes back

    /** SETOF is not part of the type — it is the flag beside it, and the row records both. */
    @Test
    void aSetReturningFunctionRecordsItsTypeAndItsFlag() throws Exception {
        assertEquals("integer|true|1000", define("rc1",
                "CREATE FUNCTION rc1(p int) RETURNS SETOF int LANGUAGE sql AS $$ SELECT p $$",
                "prorettype::regtype::text || '|' || proretset::text || '|' || prorows::text"));
        // A row estimate that was written is the one kept.
        assertEquals("integer|true|7", define("rc2",
                "CREATE FUNCTION rc2(p int) RETURNS SETOF int LANGUAGE sql ROWS 7 AS $$ SELECT p $$",
                "prorettype::regtype::text || '|' || proretset::text || '|' || prorows::text"));
        // One that answers a single value has no row estimate at all.
        assertEquals("integer|false|0", define("rc3",
                "CREATE FUNCTION rc3(p int) RETURNS int LANGUAGE sql AS $$ SELECT p $$",
                "prorettype::regtype::text || '|' || proretset::text || '|' || prorows::text"));
    }

    /** A result derived from OUT parameters is one of them, or a record when there are several. */
    @Test
    void aDerivedResultIsWhatTheOutParametersMake() throws Exception {
        assertEquals("integer", define("rc4",
                "CREATE FUNCTION rc4(a int, OUT b int) LANGUAGE sql AS $$ SELECT a $$",
                "prorettype::regtype::text"));
        assertEquals("record", define("rc5",
                "CREATE FUNCTION rc5(a int, OUT b int, OUT c text) LANGUAGE sql AS $$ SELECT a, 'x' $$",
                "prorettype::regtype::text"));
        assertEquals("integer", define("rc6",
                "CREATE FUNCTION rc6(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$",
                "prorettype::regtype::text"));
    }

    /** RETURNS TABLE is a set of records whose columns are recorded as parameters of mode 't'. */
    @Test
    void aTableResultIsASetOfRecords() throws Exception {
        assertEquals("record|true|0|{t,t}", define("rc7",
                "CREATE FUNCTION rc7() RETURNS TABLE(a int, b text) LANGUAGE sql AS $$ SELECT 1, 'x' $$",
                "prorettype::regtype::text || '|' || proretset::text || '|' || pronargs::text"
                        + " || '|' || proargmodes::text"));
        assertEquals("TABLE(a integer, b text)",
                scalar("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'rc7'"));
        // A table column is a column of the result, so it is not among the arguments.
        assertEquals("", scalar("SELECT pg_get_function_arguments(oid)"
                + " FROM pg_proc WHERE proname = 'rc7'"));
    }

    /** A procedure gives nothing back, unless it has a parameter to give it back through. */
    @Test
    void aProcedureResultIsNothingOrARecord() throws Exception {
        assertEquals("void", define("rc8",
                "CREATE PROCEDURE rc8(a int) LANGUAGE sql AS $$ SELECT a $$", "prorettype::regtype::text"));
        assertNull(scalar("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'rc8'"));
        assertEquals("record", define("rc9",
                "CREATE PROCEDURE rc9(INOUT a int) LANGUAGE sql AS $$ SELECT a $$",
                "prorettype::regtype::text"));
        assertEquals("record", define("rc10",
                "CREATE PROCEDURE rc10(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$",
                "prorettype::regtype::text"));
    }

    // ------------------------------------------------------- what goes in

    /** pronargs and proargtypes name what a call passes: the OUT parameters are not that. */
    @Test
    void onlyTheArgumentsACallPassesAreCounted() throws Exception {
        assertEquals("1|23|{23,23}|{i,o}", define("rc11",
                "CREATE FUNCTION rc11(a int, OUT b int) LANGUAGE sql AS $$ SELECT a $$",
                "pronargs::text || '|' || proargtypes::text || '|' || proallargtypes::text"
                        + " || '|' || proargmodes::text"));
        // INOUT is passed as well as returned, so it counts.
        assertEquals("2|23 23|{i,b}", define("rc12",
                "CREATE FUNCTION rc12(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$",
                "pronargs::text || '|' || proargtypes::text || '|' || proargmodes::text"));
    }

    /** The three array columns say nothing, and are NULL, when every parameter is a plain input. */
    @Test
    void theArrayColumnsAreNullWhenTheyWouldSayNothing() throws Exception {
        assertEquals("true", define("rc13",
                "CREATE FUNCTION rc13(a int, b text) RETURNS int LANGUAGE sql AS $$ SELECT a $$",
                "(proargmodes IS NULL AND proallargtypes IS NULL)::text"));
        assertEquals("{a,b}", scalar("SELECT proargnames::text FROM pg_proc WHERE proname = 'rc13'"));
    }

    /** An array of a type is a type of its own, and the row records that type rather than none. */
    @Test
    void anArrayTypedParameterHasAnArrayTypesOid() throws Exception {
        assertEquals("1007|1007", define("rc14",
                "CREATE FUNCTION rc14(p int[]) RETURNS int[] LANGUAGE sql AS $$ SELECT p $$",
                "proargtypes::text || '|' || prorettype::text"));
        assertEquals("p integer[]", scalar("SELECT pg_get_function_arguments(oid)"
                + " FROM pg_proc WHERE proname = 'rc14'"));
        assertEquals("integer[]", scalar("SELECT pg_get_function_result(oid)"
                + " FROM pg_proc WHERE proname = 'rc14'"));
    }

    /** pg_proc has nowhere to put a modifier, so the type it records is the type without one. */
    @Test
    void aParameterCarryingAModifierIsStillItsType() throws Exception {
        assertEquals("1700 1043 1186", define("rc15",
                "CREATE FUNCTION rc15(p numeric(10,2), q varchar(5), r interval day to second(2))"
                        + " RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
                "proargtypes::text"));
        assertEquals("p numeric, q character varying, r interval",
                scalar("SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'rc15'"));
    }

    /**
     * provariadic names the type the tail collects into, which is the array's ELEMENT type — a
     * client working out how many arguments the tail may take reads one level of array too many
     * otherwise. A routine with no VARIADIC parameter has none, which reads as a dash.
     */
    @Test
    void variadicNamesTheElementType() throws Exception {
        assertEquals("integer", define("rc16",
                "CREATE FUNCTION rc16(VARIADIC c int[]) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
                "provariadic::regtype::text"));
        assertEquals("text", define("rc17",
                "CREATE FUNCTION rc17(a int, VARIADIC c text[]) RETURNS int LANGUAGE sql AS $$ SELECT a $$",
                "provariadic::regtype::text"));
        assertEquals("-", define("rc18",
                "CREATE FUNCTION rc18(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$",
                "provariadic::regtype::text"));
        assertEquals("-", scalar("SELECT 0::oid::regtype::text"));
    }

    /** A default is counted, and printed where the arguments are printed with them. */
    @Test
    void defaultsAreCountedAndPrinted() throws Exception {
        assertEquals("2", define("rc19",
                "CREATE FUNCTION rc19(a int, b int DEFAULT 3, c int DEFAULT 4) RETURNS int"
                        + " LANGUAGE sql AS $$ SELECT a $$",
                "pronargdefaults::text"));
        assertEquals("a integer, b integer DEFAULT 3, c integer DEFAULT 4",
                scalar("SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'rc19'"));
        // The identity form is the one that names a routine, and a default is no part of that.
        assertEquals("a integer, b integer, c integer",
                scalar("SELECT pg_get_function_identity_arguments(oid)"
                        + " FROM pg_proc WHERE proname = 'rc19'"));
    }

    // ------------------------------------------------------- reading the row back out

    /** The identity form names the OUT parameters too, and a procedure's inputs are written IN. */
    @Test
    void theArgumentListIsWrittenAsPostgresWritesIt() throws Exception {
        exec("DROP FUNCTION IF EXISTS rc20 CASCADE");
        exec("CREATE FUNCTION rc20(a int, OUT b int, OUT c text) LANGUAGE sql AS $$ SELECT a, 'x' $$");
        assertEquals("a integer, OUT b integer, OUT c text",
                scalar("SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'rc20'"));
        assertEquals("a integer, OUT b integer, OUT c text",
                scalar("SELECT pg_get_function_identity_arguments(oid)"
                        + " FROM pg_proc WHERE proname = 'rc20'"));
        exec("DROP PROCEDURE IF EXISTS rc21 CASCADE");
        exec("CREATE PROCEDURE rc21(a int, INOUT b int) LANGUAGE sql AS $$ SELECT b $$");
        assertEquals("IN a integer, INOUT b integer",
                scalar("SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'rc21'"));
    }

    /**
     * A routine is identified by what a call passes it, so a signature may name the OUT parameters
     * and is not matched on them — which is what makes a definition memgres deparses one that
     * memgres can find again.
     */
    @Test
    void aSignatureNamingOutParametersStillFindsTheRoutine() throws Exception {
        exec("DROP FUNCTION IF EXISTS rc22 CASCADE");
        exec("CREATE FUNCTION rc22(a int, OUT b int) LANGUAGE sql AS $$ SELECT a $$");
        exec("ALTER FUNCTION rc22(a int, OUT b int) IMMUTABLE");
        assertEquals("i", scalar("SELECT provolatile FROM pg_proc WHERE proname = 'rc22'"));
        // Naming only what a call passes finds the same one.
        exec("ALTER FUNCTION rc22(int) STABLE");
        assertEquals("s", scalar("SELECT provolatile FROM pg_proc WHERE proname = 'rc22'"));
        exec("DROP FUNCTION rc22(a int, OUT b int)");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'rc22'"));
    }

    /**
     * A SQL-standard body is written out as the block it was declared as. Deparsed inside dollar
     * quotes it would not re-create the routine, because that is not what the language accepts.
     */
    @Test
    void aStandardBodyIsDeparsedAsItsBlock() throws Exception {
        exec("DROP FUNCTION IF EXISTS rc23 CASCADE");
        exec("CREATE FUNCTION rc23(p int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT p; END");
        String def = scalar("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'rc23'");
        assertTrue(def.contains("\nBEGIN ATOMIC\n"), def);
        assertTrue(def.trim().endsWith("END"), def);
        assertFalse(def.contains("$function$"), def);
    }
}
