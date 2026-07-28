package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Polymorphic pseudo-types and the per-schema function namespace.
 *
 * anyelement, anyarray, anynonarray and anycompatible are core PostgreSQL: without them no
 * polymorphic routine can be declared at all, and a signature PostgreSQL rejects (a polymorphic
 * result with nothing to determine it from) was being accepted. Separately, functions used to
 * share one flat namespace, so two schemas could not each define a function of the same name and
 * search_path-based resolution between schemas could not be exercised.
 */
class PolymorphicTypeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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

    private static String columnLabel(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnName(1);
        }
    }

    private static SQLException failure(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = failure(sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    // ---- the pseudo-types themselves ----

    @Test
    void polymorphicPseudoTypesAreRegistered() throws Exception {
        assertEquals("2283", scalar("SELECT oid FROM pg_type WHERE typname = 'anyelement'"));
        assertEquals("2277", scalar("SELECT oid FROM pg_type WHERE typname = 'anyarray'"));
        assertEquals("2776", scalar("SELECT oid FROM pg_type WHERE typname = 'anynonarray'"));
        assertEquals("3500", scalar("SELECT oid FROM pg_type WHERE typname = 'anyenum'"));
        assertEquals("3831", scalar("SELECT oid FROM pg_type WHERE typname = 'anyrange'"));
        assertEquals("5077", scalar("SELECT oid FROM pg_type WHERE typname = 'anycompatible'"));
        assertEquals("5078", scalar("SELECT oid FROM pg_type WHERE typname = 'anycompatiblearray'"));
        assertEquals("p", scalar("SELECT typtype FROM pg_type WHERE typname = 'anyelement'"));
        assertEquals("P", scalar("SELECT typcategory FROM pg_type WHERE typname = 'anyelement'"));
        // The container-shaped ones are varlena, like the concrete types they stand in for.
        assertEquals("-1", scalar("SELECT typlen FROM pg_type WHERE typname = 'anyarray'"));
        assertEquals("4", scalar("SELECT typlen FROM pg_type WHERE typname = 'anyelement'"));
    }

    @Test
    void polymorphicTypeNamesCastThroughRegtype() throws Exception {
        assertEquals("anyelement", scalar("SELECT 'anyelement'::regtype AS t"));
        assertEquals("anyarray", scalar("SELECT 'anyarray'::regtype AS t"));
        assertEquals("anynonarray", scalar("SELECT 'anynonarray'::regtype AS t"));
        assertEquals("anycompatible", scalar("SELECT 'anycompatible'::regtype AS t"));
        assertEquals("anyelement", scalar("SELECT 2283::regtype AS t"));
        assertEquals("anycompatible", scalar("SELECT 5077::regtype AS t"));
    }

    // ---- declaring and calling polymorphic routines ----

    @Test
    void anyelementResolvesFromTheArgument() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_ident(anyelement)");
        exec("CREATE FUNCTION pt_ident(x anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT x $$");

        assertEquals("5", scalar("SELECT pt_ident(5)"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_ident(5))::text AS t"));
        assertEquals("abc", scalar("SELECT pt_ident('abc'::text)"));
        assertEquals("text", scalar("SELECT pg_typeof(pt_ident('abc'::text))::text AS t"));
        assertEquals("numeric", scalar("SELECT pg_typeof(pt_ident(1.5))::text AS t"));
        assertEquals("boolean", scalar("SELECT pg_typeof(pt_ident(true))::text AS t"));
        assertEquals(Types.INTEGER, columnType("SELECT pt_ident(5)"));
    }

    private static int columnType(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnType(1);
        }
    }

    @Test
    void aTypedNullDeterminesThePolymorphButABareNullDoesNot() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_null_id(anyelement)");
        exec("CREATE FUNCTION pt_null_id(x anyelement) RETURNS anyelement LANGUAGE sql AS $$ SELECT x $$");

        assertNull(scalar("SELECT pt_null_id(NULL::int)"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_null_id(NULL::int))::text AS t"));
        assertState("42804", "SELECT pt_null_id(NULL)");
    }

    @Test
    void anyarrayAndAnynonarrayConstrainTheArgumentShape() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_first(anyarray)");
        exec("DROP FUNCTION IF EXISTS pt_wrap(anyelement)");
        exec("DROP FUNCTION IF EXISTS pt_nonarr(anynonarray)");
        exec("CREATE FUNCTION pt_first(a anyarray) RETURNS anyelement LANGUAGE sql AS $$ SELECT a[1] $$");
        exec("CREATE FUNCTION pt_wrap(x anyelement) RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[x] $$");
        exec("CREATE FUNCTION pt_nonarr(x anynonarray) RETURNS text LANGUAGE sql AS $$ SELECT x::text $$");

        assertEquals("10", scalar("SELECT pt_first(ARRAY[10,20,30])"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_first(ARRAY[10,20,30]))::text AS t"));
        assertEquals("a", scalar("SELECT pt_first(ARRAY['a','b']::text[])"));
        assertEquals("text", scalar("SELECT pg_typeof(pt_first(ARRAY['a','b']::text[]))::text AS t"));
        assertEquals("integer[]", scalar("SELECT pg_typeof(pt_wrap(7))::text AS t"));
        assertEquals("text[]", scalar("SELECT pg_typeof(pt_wrap('z'::text))::text AS t"));
        assertEquals("9", scalar("SELECT pt_nonarr(9)"));

        // A scalar cannot bind anyarray, nor an array anynonarray.
        SQLException noArray = failure("SELECT pt_first(5)");
        assertEquals("42883", noArray.getSQLState());
        assertTrue(noArray.getMessage().contains("pt_first(integer)"), noArray.getMessage());
        SQLException noScalar = failure("SELECT pt_nonarr(ARRAY[1,2])");
        assertEquals("42883", noScalar.getSQLState());
        assertTrue(noScalar.getMessage().contains("pt_nonarr(integer[])"), noScalar.getMessage());
    }

    @Test
    void everyAnyelementSlotMustLandOnTheSameType() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_pair(anyelement, anyelement)");
        exec("CREATE FUNCTION pt_pair(a anyelement, b anyelement) RETURNS anyelement"
                + " LANGUAGE sql AS $$ SELECT a $$");

        assertEquals("1", scalar("SELECT pt_pair(1, 2)"));
        SQLException e = failure("SELECT pt_pair(1, 'x'::text)");
        assertEquals("42883", e.getSQLState());
        assertTrue(e.getMessage().contains("pt_pair(integer, text)"), e.getMessage());
    }

    @Test
    void anycompatibleOnlyNeedsACommonType() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_cmp(anycompatible, anycompatible)");
        exec("CREATE FUNCTION pt_cmp(a anycompatible, b anycompatible) RETURNS anycompatible"
                + " LANGUAGE sql AS $$ SELECT CASE WHEN a > b THEN a ELSE b END $$");

        assertEquals("5", scalar("SELECT pt_cmp(2, 5)"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_cmp(2, 5))::text AS t"));
        assertEquals("5.5", scalar("SELECT pt_cmp(2, 5.5)"));
        assertEquals("numeric", scalar("SELECT pg_typeof(pt_cmp(2, 5.5))::text AS t"));
    }

    @Test
    void plpgsqlAndColumnArgumentsResolveTheSameWay() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_plp(anyelement)");
        exec("CREATE FUNCTION pt_plp(x anyelement) RETURNS anyelement LANGUAGE plpgsql"
                + " AS $$ BEGIN RETURN x; END $$");
        assertEquals("3", scalar("SELECT pt_plp(3)"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_plp(3))::text AS t"));
        assertEquals("text", scalar("SELECT pg_typeof(pt_plp('q'::text))::text AS t"));

        exec("DROP TABLE IF EXISTS pt_t");
        exec("CREATE TABLE pt_t (a int, b text)");
        exec("INSERT INTO pt_t VALUES (1, 'x')");
        assertEquals("1", scalar("SELECT pt_plp(a) FROM pt_t"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_plp(a))::text AS t FROM pt_t"));
        exec("DROP TABLE pt_t");
    }

    // ---- signatures PostgreSQL rejects ----

    @Test
    void aPolymorphicResultWithNothingToResolveItIsRejected() {
        assertState("42P13", "CREATE FUNCTION pt_bad1() RETURNS anyelement LANGUAGE sql AS $$ SELECT 1 $$");
        assertState("42P13", "CREATE FUNCTION pt_bad2() RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[1] $$");
        assertState("42P13", "CREATE FUNCTION pt_bad3() RETURNS anynonarray LANGUAGE sql AS $$ SELECT 1 $$");
        assertState("42P13", "CREATE FUNCTION pt_bad4() RETURNS anycompatible LANGUAGE sql AS $$ SELECT 1 $$");
        assertState("42P13",
                "CREATE FUNCTION pt_bad5(x int) RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[x] $$");
        // Cross-family: an anyelement argument cannot determine an anycompatible result.
        assertState("42P13",
                "CREATE FUNCTION pt_bad6(x anyelement) RETURNS anycompatible LANGUAGE sql AS $$ SELECT x $$");
    }

    @Test
    void aPolymorphicArgumentOfTheSameFamilyIsEnough() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_ok1(anyelement)");
        exec("DROP FUNCTION IF EXISTS pt_ok2(anycompatible)");
        exec("CREATE FUNCTION pt_ok1(x anyelement) RETURNS anyarray LANGUAGE sql AS $$ SELECT ARRAY[x] $$");
        exec("CREATE FUNCTION pt_ok2(x anycompatible) RETURNS anycompatiblearray"
                + " LANGUAGE sql AS $$ SELECT ARRAY[x] $$");
        assertEquals("{7}", scalar("SELECT pt_ok1(7)::text AS t"));
        assertEquals("{7}", scalar("SELECT pt_ok2(7)::text AS t"));
    }

    // ---- the per-schema function namespace ----

    @Test
    void twoSchemasMayEachDefineTheSameFunctionName() throws Exception {
        exec("DROP SCHEMA IF EXISTS pt_s1 CASCADE");
        exec("DROP SCHEMA IF EXISTS pt_s2 CASCADE");
        exec("CREATE SCHEMA pt_s1");
        exec("CREATE SCHEMA pt_s2");
        exec("CREATE FUNCTION pt_s1.pt_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        exec("CREATE FUNCTION pt_s2.pt_f() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$");

        assertEquals("1", scalar("SELECT pt_s1.pt_f()"));
        assertEquals("2", scalar("SELECT pt_s2.pt_f()"));
        // The column is labelled with the bare routine name, not the qualifier.
        assertEquals("pt_f", columnLabel("SELECT pt_s1.pt_f()"));
        assertEquals("2", scalar(
                "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
                        + " WHERE p.proname = 'pt_f'"));

        // Same name, same schema, same argument types is still a duplicate.
        SQLException dup = failure("CREATE FUNCTION pt_s2.pt_f() RETURNS int LANGUAGE sql AS $$ SELECT 3 $$");
        assertEquals("42723", dup.getSQLState());
        // A different argument list in the same schema is an overload.
        exec("CREATE FUNCTION pt_s2.pt_f(x int) RETURNS int LANGUAGE sql AS $$ SELECT x $$");
        assertEquals("41", scalar("SELECT pt_s2.pt_f(41)"));

        exec("DROP SCHEMA pt_s1 CASCADE");
        exec("DROP SCHEMA pt_s2 CASCADE");
    }

    @Test
    void searchPathPicksTheRightSameNamedFunction() throws Exception {
        exec("DROP SCHEMA IF EXISTS pt_p1 CASCADE");
        exec("DROP SCHEMA IF EXISTS pt_p2 CASCADE");
        exec("CREATE SCHEMA pt_p1");
        exec("CREATE SCHEMA pt_p2");
        exec("CREATE FUNCTION pt_p1.pt_g() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        exec("CREATE FUNCTION pt_p2.pt_g() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$");
        try {
            exec("SET search_path = pt_p1, pt_p2");
            assertEquals("1", scalar("SELECT pt_g()"));
            exec("SET search_path = pt_p2, pt_p1");
            assertEquals("2", scalar("SELECT pt_g()"));
            // Neither schema is on the path, so the unqualified name resolves to nothing.
            exec("SET search_path = public");
            assertState("42883", "SELECT pt_g()");
            // Nor does an unqualified DROP reach it.
            exec("DROP FUNCTION IF EXISTS pt_g()");
            assertEquals("2", scalar("SELECT pt_p2.pt_g()"));
        } finally {
            exec("SET search_path = public");
        }

        // DROP removes only the copy it names.
        exec("DROP FUNCTION pt_p1.pt_g()");
        assertEquals("2", scalar("SELECT pt_p2.pt_g()"));
        assertState("42883", "SELECT pt_p1.pt_g()");

        exec("DROP SCHEMA pt_p1 CASCADE");
        exec("DROP SCHEMA pt_p2 CASCADE");
    }

    @Test
    void droppingASchemaLeavesTheSameNamedFunctionElsewhere() throws Exception {
        exec("DROP SCHEMA IF EXISTS pt_d1 CASCADE");
        exec("DROP FUNCTION IF EXISTS pt_h()");
        exec("CREATE SCHEMA pt_d1");
        exec("CREATE FUNCTION pt_d1.pt_h() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        exec("CREATE FUNCTION public.pt_h() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$");

        exec("DROP SCHEMA pt_d1 CASCADE");
        assertEquals("2", scalar("SELECT public.pt_h()"));
        exec("DROP FUNCTION public.pt_h()");
    }

    // ---- neighbouring behaviour that must not change ----

    @Test
    void nonPolymorphicOverloadsStillResolveByArgumentType() throws Exception {
        exec("DROP FUNCTION IF EXISTS pt_plain(int)");
        exec("DROP FUNCTION IF EXISTS pt_plain(text)");
        exec("CREATE FUNCTION pt_plain(x int) RETURNS int LANGUAGE sql AS $$ SELECT x + 1 $$");
        assertEquals("5", scalar("SELECT pt_plain(4)"));
        assertEquals("integer", scalar("SELECT pg_typeof(pt_plain(4))::text AS t"));

        exec("CREATE FUNCTION pt_plain(x text) RETURNS text LANGUAGE sql AS $$ SELECT upper(x) $$");
        assertEquals("AB", scalar("SELECT pt_plain('ab'::text)"));
        assertEquals(Types.VARCHAR, columnType("SELECT pt_plain('ab'::text)"));
        assertEquals("5", scalar("SELECT pt_plain(4)"));
        assertEquals(Types.INTEGER, columnType("SELECT pt_plain(4)"));

        exec("DROP FUNCTION pt_plain(int)");
        exec("DROP FUNCTION pt_plain(text)");
        assertState("42883", "SELECT pt_plain(4)");
    }

    @Test
    void builtinsKeepTheirBareColumnLabel() throws Exception {
        assertEquals("AB", scalar("SELECT pg_catalog.upper('ab')"));
        assertEquals("upper", columnLabel("SELECT pg_catalog.upper('ab')"));
        assertEquals("upper", columnLabel("SELECT upper('ab')"));
        assertEquals("3", scalar("SELECT length('abc')"));
    }

    @Test
    void droppingAFunctionThatDoesNotExistStillReports42883() {
        assertState("42883", "DROP FUNCTION pt_never_created()");
    }
}
