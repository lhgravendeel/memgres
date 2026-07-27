package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An identifier that matches no column, variable or system value must be rejected rather than
 * evaluated as its own name in text form. Returning the name lets a typo become a plausible value
 * that defeats the declared type of whatever it is assigned to, which is hardest to spot inside
 * PL/pgSQL where composite variables were reaching the same fallback.
 */
class IdentifierResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE irt_t (a int, b text)");
        exec("INSERT INTO irt_t VALUES (1, 'x'), (2, 'y')");
        exec("CREATE TYPE irt_inner AS (a int, b int)");
        exec("CREATE TYPE irt_outer AS (x int, y irt_inner)");
        exec("CREATE TYPE irt_txt AS (a text, b text)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    // ----------------------------------------------------------------- query

    @Test
    void unresolvableIdentifiersInQueriesAreRejected() {
        assertState("42703", "SELECT nosuchthing");
        assertState("42703", "SELECT nosuchcol FROM irt_t");
        assertState("42703", "SELECT 1 FROM irt_t WHERE nosuchcol = 1");
        assertState("42703", "SELECT a FROM irt_t ORDER BY nosuchcol");
        assertState("42703", "SELECT nosuchthing + 1");
        assertState("42703", "SELECT upper(nosuchthing)");
    }

    @Test
    void resolvableReferencesStillWork() throws Exception {
        assertEquals("x,y", scalar("SELECT string_agg(b, ',' ORDER BY a) FROM irt_t"));
        assertEquals("1,2", scalar(
                "SELECT string_agg(x::text, ',' ORDER BY x) FROM (SELECT a AS x FROM irt_t) t"));
        // an output alias is still reachable from ORDER BY
        assertEquals("1,2", scalar("SELECT string_agg(xx::text, ',' ORDER BY xx)"
                + " FROM (SELECT a AS xx FROM irt_t ORDER BY xx) t"));
        assertEquals("literal", scalar("SELECT 'literal'"));
        assertEquals("2", scalar("SELECT count(*)::text FROM irt_t t1"
                + " JOIN irt_t t2 ON t1.a = t2.a"));
        assertEquals("true", scalar("SELECT (current_user IS NOT NULL)::text"));
    }

    // --------------------------------------------------------------- plpgsql

    @Test
    void unresolvableIdentifiersInProceduralCodeAreRejected() {
        // a variable referenced before its own declaration is not in scope yet
        assertState("42703", "DO $$ declare a int := b; b int := 5;"
                + " begin raise notice '%', a; end $$");
        assertState("42703", "DO $$ declare a int; begin a := nosuchthing; end $$");
        assertState("42703", "DO $$ declare a text; begin a := nosuchthing; end $$");
        assertState("42703", "DO $$ begin raise notice '%', nosuchthing; end $$");
    }

    @Test
    void declaredVariablesStillResolve() throws Exception {
        exec("DO $$ declare a int := 5; b int;"
                + " begin b := a; if b <> 5 then raise 'wrong'; end if; end $$");
        exec("DO $$ declare a int := 5;"
                + " begin if a::text <> '5' then raise 'wrong'; end if; end $$");
        exec("DO $$ declare r record; begin select * into r from irt_t where a = 1;"
                + " if r.b <> 'x' then raise 'wrong'; end if; end $$");
    }

    // ------------------------------------------------------------- composite

    @Test
    void compositeVariableUsedAsWholeValue() throws Exception {
        exec("DROP FUNCTION IF EXISTS irt_one()");
        exec("CREATE FUNCTION irt_one() RETURNS text LANGUAGE plpgsql AS $$"
                + " declare v irt_inner; begin v.a := 1; v.b := 2; return v::text; end $$");
        assertEquals("(1,2)", scalar("SELECT irt_one()"));
    }

    @Test
    void nestedCompositeFieldAssignment() throws Exception {
        exec("DROP FUNCTION IF EXISTS irt_nested()");
        exec("CREATE FUNCTION irt_nested() RETURNS text LANGUAGE plpgsql AS $$"
                + " declare v irt_outer; begin v.x := 1; v.y.a := 2; v.y.b := 3;"
                + " return v::text; end $$");
        // the inner composite keeps its structure and is quoted inside the outer one
        assertEquals("(1,\"(2,3)\")", scalar("SELECT irt_nested()"));
    }

    @Test
    void compositeReturnedAsItsDeclaredType() throws Exception {
        exec("DROP FUNCTION IF EXISTS irt_typed()");
        exec("CREATE FUNCTION irt_typed() RETURNS irt_inner LANGUAGE plpgsql AS $$"
                + " declare v irt_inner; begin v.a := 1; v.b := 2; return v; end $$");
        assertEquals("(1,2)", scalar("SELECT irt_typed()::text"));
        assertEquals("1", scalar("SELECT (irt_typed()).a::text"));
    }

    @Test
    void compositeFieldsAreQuotedWhenTheyNeedIt() throws Exception {
        exec("DROP FUNCTION IF EXISTS irt_quoted()");
        exec("CREATE FUNCTION irt_quoted() RETURNS text LANGUAGE plpgsql AS $$"
                + " declare v irt_txt; begin v.a := 'has,comma'; v.b := 'has \"quote\"';"
                + " return v::text; end $$");
        assertEquals("(\"has,comma\",\"has \"\"quote\"\"\")", scalar("SELECT irt_quoted()"));
        // an empty field is quoted; a NULL field is written as nothing at all
        exec("DROP FUNCTION IF EXISTS irt_empty()");
        exec("CREATE FUNCTION irt_empty() RETURNS text LANGUAGE plpgsql AS $$"
                + " declare v irt_txt; begin v.a := ''; v.b := NULL; return v::text; end $$");
        assertEquals("(\"\",)", scalar("SELECT irt_empty()"));
    }

    @Test
    void fieldAccessStillWorksAlongsideWholeValueUse() throws Exception {
        exec("DROP FUNCTION IF EXISTS irt_field()");
        exec("CREATE FUNCTION irt_field() RETURNS text LANGUAGE plpgsql AS $$"
                + " declare v irt_outer; begin v.x := 7; v.y.a := 8;"
                + " return v.x::text || '/' || (v.y).a::text; end $$");
        assertEquals("7/8", scalar("SELECT irt_field()"));
    }

    // ----------------------------------------------------- row into a scalar

    @Test
    void wholeRowAssignedToScalarVariableIsRejected() {
        assertState("22P02", "DO $$ declare r irt_t%rowtype; v int;"
                + " begin select * into r from irt_t where a = 1; v := r; end $$");
        assertState("22P02", "DO $$ declare r irt_t%rowtype; v bigint;"
                + " begin select * into r from irt_t where a = 1; v := r; end $$");
        assertState("22P02", "DO $$ declare r irt_t%rowtype; v boolean;"
                + " begin select * into r from irt_t where a = 1; v := r; end $$");
    }

    @Test
    void wholeRowAssignedToTextVariableIsAllowed() throws Exception {
        // a row has a text form, so this one is a legal conversion
        exec("DO $$ declare r irt_t%rowtype; v text;"
                + " begin select * into r from irt_t where a = 1; v := r; end $$");
    }

    // ---------------------------------------------------------------- FILTER

    @Test
    void filterMayNotSwallowThePrecedingExpression() {
        assertState("42601", "SELECT b FILTER (WHERE a = 1) FROM irt_t");
        assertState("42601", "SELECT a FILTER (WHERE a = 1) FROM irt_t");
        assertState("42601", "SELECT 1 filter");
    }

    @Test
    void filterOnRealAggregatesIsUnaffected() throws Exception {
        assertEquals("1", scalar("SELECT count(*) FILTER (WHERE a = 1)::text FROM irt_t"));
        assertEquals("1", scalar("SELECT sum(a) FILTER (WHERE b = 'x')::text FROM irt_t"));
        assertEquals("x,y", scalar("SELECT string_agg(b, ',') FILTER (WHERE a > 0) FROM irt_t"));
    }

    @Test
    void normalizationFormKeywordsStillResolve() throws Exception {
        // the form is a bare keyword in the grammar, not a column reference
        assertEquals("fi", scalar("SELECT normalize(U&'\\FB01', NFKC)"));
        assertEquals("fi", scalar("SELECT normalize(U&'\\FB01', NFKD)"));
        assertNotNull(scalar("SELECT normalize(U&'\\FB01', NFC)"));
        assertNotNull(scalar("SELECT normalize(U&'\\FB01', NFD)"));
        assertNotNull(scalar("SELECT normalize(U&'\\FB01')"));
        assertEquals("false", scalar("SELECT (U&'\\FB01' IS NFKC NORMALIZED)::text"));
    }

    @Test
    void filterRemainsUsableAsAnIntroducedAlias() throws Exception {
        assertEquals("1", scalar("SELECT 1 AS filter"));
        assertEquals("1", scalar("SELECT 1 AS \"filter\""));
    }
}
