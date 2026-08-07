package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whether a call's arguments can reach the parameters of anything declared under that name.
 *
 * <p>PostgreSQL resolves a call on the types its arguments were written with: an argument reaches
 * a parameter of its own type, or of one it casts to on its own. memgres judged the argument's
 * <em>category</em> instead, which is right about kind and silent about type — so a numeric
 * reached a bigint parameter and {@code pg_advisory_lock(1.5)} ran a function PostgreSQL says does
 * not exist. There is a cast from numeric to bigint, but only for an assignment, and a call is not
 * one.
 *
 * <p>What makes the stricter rule safe is that the conversions are PostgreSQL's own list rather
 * than an inference from the categories: nothing about a category says a date reaches a timestamp,
 * a cidr an inet, or a bit a bit varying, and those calls have to keep working.
 */
class CallResolutionReachTest {

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

    @AfterEach
    void releaseLocks() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("SELECT pg_advisory_unlock_all()");
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void noSuchFunction(String signature, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), "should be refused: " + sql);
        assertEquals("42883", e.getSQLState(), sql);
        assertEquals("ERROR: function " + signature + " does not exist",
                e.getMessage() == null ? null : e.getMessage().split("\n")[0], sql);
    }

    private static void runs(String sql) {
        assertDoesNotThrow(() -> scalar(sql), sql);
    }

    // ------------------------------------------------------- a numeric is not a bigint

    /** The advisory locks take a bigint, or two integers, and nothing wider reaches either. */
    @Test
    void aNumericDoesNotReachABigintParameter() {
        noSuchFunction("pg_advisory_lock(numeric)", "SELECT pg_advisory_lock(1.5)");
        noSuchFunction("pg_advisory_unlock(numeric)", "SELECT pg_advisory_unlock(1.5)");
        noSuchFunction("pg_try_advisory_lock(numeric)", "SELECT pg_try_advisory_lock(1.5)");
        noSuchFunction("pg_advisory_xact_lock(numeric)", "SELECT pg_advisory_xact_lock(1.5)");
        noSuchFunction("pg_advisory_lock(numeric, numeric)", "SELECT pg_advisory_lock(1.5, 2.5)");
        // The value is beside the point: it is the type that cannot get there.
        noSuchFunction("pg_advisory_lock(numeric)", "SELECT pg_advisory_lock(1::numeric)");
        noSuchFunction("pg_advisory_lock(double precision)", "SELECT pg_advisory_lock(1.0::float8)");
        noSuchFunction("pg_advisory_lock(real)", "SELECT pg_advisory_lock(1::real)");
    }

    /** The same rule wherever an integer parameter is given something wider. */
    @Test
    void theSameRuleHoldsForEveryIntegerParameter() {
        noSuchFunction("to_hex(numeric)", "SELECT to_hex(10.5)");
        noSuchFunction("round(numeric, numeric)", "SELECT round(1.5, 2.5)");
        noSuchFunction("trunc(numeric, numeric)", "SELECT trunc(1.5, 2.5)");
        noSuchFunction("width_bucket(numeric, numeric, numeric, numeric)",
                "SELECT width_bucket(1.0, 0.0, 10.0, 5.5)");
    }

    /** And an integer still reaches every parameter an integer casts to on its own. */
    @Test
    void anIntegerStillReachesWhatItCastsTo() throws Exception {
        assertEquals("", scalar("SELECT pg_advisory_lock(1)"));
        assertEquals("", scalar("SELECT pg_advisory_lock(1::smallint)"));
        assertEquals("", scalar("SELECT pg_advisory_lock(1::bigint)"));
        assertEquals("", scalar("SELECT pg_advisory_lock(1, 2)"));
        for (String sql : new String[]{
                "SELECT setseed(0)", "SELECT sqrt(4)", "SELECT ln(1)", "SELECT pg_sleep(0)",
                "SELECT power(2, 3)", "SELECT log(100)", "SELECT round(1, 2)",
                "SELECT trunc(1, 2)", "SELECT to_char(1, '9')", "SELECT to_hex(10::bigint)",
                "SELECT width_bucket(1.0, 0.0, 10.0, 5)", "SELECT array_fill(1, ARRAY[2])"}) {
            runs(sql);
        }
        // A numeric reaches the floating-point parameters, which is a cast PostgreSQL does make.
        for (String sql : new String[]{"SELECT setseed(0.5)", "SELECT sqrt(4.0)", "SELECT pg_sleep(0.0)"}) {
            runs(sql);
        }
    }

    /**
     * The conversions outside the numeric types have to keep working: nothing about a category
     * says a date reaches a timestamp or a cidr an inet, and PostgreSQL makes both casts unasked.
     */
    @Test
    void theOtherConversionsPostgresMakesStillReach() {
        for (String sql : new String[]{
                "SELECT age('2020-01-01'::date, '2019-01-01'::date)",
                "SELECT age('2020-01-01'::date)",
                "SELECT host('10.0.0.0/8'::cidr)",
                "SELECT abbrev('10.0.0.0/8'::cidr)",
                "SELECT masklen('10.0.0.0/8'::cidr)",
                "SELECT length('101'::bit(3))",
                "SELECT length('101'::varbit)",
                "SELECT trunc('08:00:2b:01:02:03'::macaddr)",
                "SELECT upper('abc'::name)",
                "SELECT length('abc'::varchar)",
                "SELECT btrim('abc'::char(5))",
                "SELECT ascii('abc'::name)",
                "SELECT justify_days('1 mon'::interval)",
                "SELECT date_part('hour', '01:02:03'::time)"}) {
            runs(sql);
        }
    }

    /** An argument the statement says nothing about takes the parameter's own type. */
    @Test
    void anArgumentWithNoTypeOfItsOwnIsLeftAlone() {
        runs("SELECT repeat('a', NULL)");
        runs("SELECT chr(NULL)");
        runs("SELECT repeat('a', '2')");
        runs("SELECT pg_advisory_lock('42')");
    }

    // ------------------------------------------------------- a sign is no part of a type

    /**
     * {@code -4} is the integer {@code 4} was. Reading the sign as saying nothing left the call it
     * stood in to be resolved on its category's preferred type instead, so {@code abs(-4)} came
     * out a double precision where {@code abs(4)} was an integer — and the difference showed only
     * once something refused to take a double precision.
     */
    @Test
    void aSignSaysNothingAboutTheType() throws Exception {
        assertEquals("abcd", scalar("SELECT left('abcde', abs(-4))"));
        assertEquals("integer", scalar("SELECT pg_typeof(abs(-4))::text"));
        assertEquals("integer", scalar("SELECT pg_typeof(-4)::text"));
        assertEquals("numeric", scalar("SELECT pg_typeof(-4.5)::text"));
        assertEquals("", scalar("SELECT pg_advisory_lock(-1)"));
        noSuchFunction("pg_advisory_lock(numeric)", "SELECT pg_advisory_lock(-1.5)");
    }

    // ------------------------------------------------------- how a refusal names the routine

    /**
     * PostgreSQL reports a refused call the way the statement wrote it, and it writes the
     * grammar-spelled forms schema-qualified. The same missing routine is
     * {@code pg_catalog.substring} written one way and {@code substring} written the other.
     */
    @Test
    void aGrammarSpelledCallIsNamedSchemaQualified() {
        noSuchFunction("pg_catalog.substring(unknown, bigint, integer)",
                "SELECT substring('abcdef' from 2::bigint for 3)");
        noSuchFunction("pg_catalog.substring(unknown, bigint)",
                "SELECT substring('abcdef' from 2::bigint)");
        noSuchFunction("pg_catalog.overlay(unknown, unknown, bigint)",
                "SELECT overlay('abcdef' placing 'XY' from 2::bigint)");
        noSuchFunction("pg_catalog.ltrim(unknown, bigint)",
                "SELECT trim(leading 2::bigint from 'abc')");
        noSuchFunction("pg_catalog.rtrim(unknown, bigint)",
                "SELECT trim(trailing 2::bigint from 'abc')");
        noSuchFunction("pg_catalog.btrim(unknown, bigint)",
                "SELECT trim(both 2::bigint from 'abc')");
        noSuchFunction("pg_catalog.extract(unknown, bigint)",
                "SELECT extract(year from 2::bigint)");
    }

    /** The same routines written as ordinary calls are named without the qualifier. */
    @Test
    void anOrdinaryCallIsNamedWithoutTheQualifier() {
        noSuchFunction("substring(unknown, bigint, integer)",
                "SELECT substring('abcdef', 2::bigint, 3)");
        noSuchFunction("substring(unknown, bigint)", "SELECT substring('abcdef', 2::bigint)");
        noSuchFunction("ltrim(unknown, bigint)", "SELECT ltrim('abc', 2::bigint)");
        noSuchFunction("rtrim(unknown, bigint)", "SELECT rtrim('abc', 2::bigint)");
        noSuchFunction("btrim(unknown, bigint)", "SELECT btrim('abc', 2::bigint)");
    }

    /** And the grammar-spelled forms still run when their arguments do reach. */
    @Test
    void theGrammarSpelledFormsStillRun() throws Exception {
        assertEquals("aXYef", scalar("SELECT overlay('abcdef' placing 'XY' from 2 for 3)"));
        assertEquals("bcd", scalar("SELECT substring('abcdef' from 2 for 3)"));
        assertEquals("2", scalar("SELECT position('b' in 'abc')::text"));
        assertEquals("bc", scalar("SELECT trim(leading 'a' from 'abc')"));
        assertEquals("2020", scalar("SELECT extract(year from '2020-01-01'::date)::text"));
    }
}
