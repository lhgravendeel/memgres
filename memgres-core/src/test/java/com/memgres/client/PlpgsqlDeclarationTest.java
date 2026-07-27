package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every modifier a PL/pgSQL declaration offers used to be parsed and then ignored: CONSTANT,
 * NOT NULL, the declared type, a domain's constraints, {@code %TYPE} and {@code %ROWTYPE}. A
 * function therefore carried none of the safety its declarations promised — a constant could be
 * written, a domain value that no column would accept moved freely through a variable of it, and
 * a {@code %TYPE} naming a column that a schema change had renamed still compiled.
 */
class PlpgsqlDeclarationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE pdt_misc (f1 int, f2 text)");
        exec("CREATE TYPE pdt_record AS (f1 int, f2 int)");
        exec("CREATE DOMAIN pdt_int_nn AS int NOT NULL");
        exec("CREATE DOMAIN pdt_pos AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE pdt_dom (c pdt_pos)");
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

    /** @return the SQLSTATE the statement failed with */
    private static String state(String sql) {
        try {
            exec(sql);
        } catch (SQLException e) {
            return e.getSQLState();
        }
        return fail("expected an error from: " + sql);
    }

    private static SQLException error(String sql) {
        try {
            exec(sql);
        } catch (SQLException e) {
            return e;
        }
        return fail("expected an error from: " + sql);
    }

    // ---- CONSTANT ----

    @Test
    void constantRejectsEveryWritePath() {
        assertEquals("22005", state("DO $$ declare x constant int := 1; begin x := 2; end$$"));
        assertEquals("22005", state("DO $$ declare x constant int := 1; begin select 2 into x; end$$"));
        assertEquals("22005", state("DO $$ declare x constant pdt_record; begin x.f1 := 42; end$$"));
        assertEquals("22005", state("DO $$ declare x constant int[] := array[1,2]; begin x[1] := 9; end$$"));
        assertEquals("22005",
                state("DO $$ declare x constant int; y int; begin for x, y in select 1,2 loop end loop; end$$"));
        assertEquals("22005", state("DO $$ declare x constant int := 1; begin for x in select 1 loop null; end loop; end$$"));
        assertEquals("22005", state("DO $$ declare x constant int := 1; begin execute 'select 2' into x; end$$"));
        assertEquals("22005", state("DO $$ declare x constant int := 1; begin get diagnostics x = row_count; end$$"));
        assertEquals("22005",
                state("DO $$ declare x constant int := 1; begin foreach x in array array[1] loop null; end loop; end$$"));
        assertEquals("22005",
                state("DO $$ declare c cursor for select 1; x constant int := 1; begin open c; fetch c into x; end$$"));
    }

    @Test
    void constantIsCheckedWhenTheFunctionIsCreated() {
        SQLException e = error("CREATE FUNCTION pdt_const() RETURNS int AS $$"
                + " declare x constant int := 1; begin x := 2; return x; end $$ LANGUAGE plpgsql");
        assertEquals("22005", e.getSQLState());
        assertTrue(e.getMessage().contains("variable \"x\" is declared CONSTANT"), e.getMessage());
        assertEquals("42883", state("SELECT pdt_const()"));
    }

    @Test
    void constantStillReadsAndAnInnerShadowStillWrites() throws Exception {
        exec("DO $$ declare x constant int := 1; begin raise notice '%', x; end$$");
        exec("DO $$ declare x constant int := 1; begin declare x int := 3; begin x := 2; end; end$$");
        // An integer FOR loop declares its own variable, so the outer constant is untouched
        exec("DO $$ declare x constant int := 1; begin for x in 1..2 loop null; end loop; end$$");
    }

    // ---- NOT NULL ----

    @Test
    void notNullNeedsADefault() {
        SQLException e = error("DO $$ declare x int not null; begin null; end$$");
        assertEquals("22004", e.getSQLState());
        assertTrue(e.getMessage().contains("must have a default value"), e.getMessage());
        assertEquals("22004", state("DO $$ declare x record not null; begin x := row(1); end$$"));
        assertEquals("22004", state("DO $$ declare x pdt_misc.f1%type not null; begin null; end$$"));
    }

    @Test
    void notNullRejectsANullAssignment() {
        assertEquals("22004", state("DO $$ declare x int not null := null; begin null; end$$"));
        assertEquals("22004", state("DO $$ declare x int not null := 3; begin x := null; end$$"));
        assertEquals("22004", state("DO $$ declare x int not null := 3; begin select null::int into x; end$$"));
        assertEquals("22004", state("DO $$ declare x record not null := row(42); begin x := null; end$$"));
        SQLException e = error("DO $$ declare x int not null := 3; begin x := null; end$$");
        assertTrue(e.getMessage().contains("null value cannot be assigned to variable \"x\""), e.getMessage());
    }

    @Test
    void notNullAcceptsANonNullAssignment() throws Exception {
        exec("DO $$ declare x int not null := 3; begin x := 4; end$$");
        exec("DO $$ declare x record not null := row(1); begin x := row(2); end$$");
    }

    // ---- Domain constraints ----

    @Test
    void domainNotNullReachesVariables() {
        assertEquals("23502", state("DO $$ declare x pdt_int_nn; begin null; end$$"));
        assertEquals("23502", state("DO $$ declare x pdt_int_nn := 42; begin x := null; end$$"));
    }

    @Test
    void domainCheckReachesVariables() {
        assertEquals("23514", state("DO $$ declare x pdt_pos := -1; begin null; end$$"));
        assertEquals("23514", state("DO $$ declare x pdt_pos := 5; begin x := -3; end$$"));
        assertEquals("23514", state("DO $$ declare x pdt_pos := 5; begin select -7 into x; end$$"));
        assertEquals("23514", state("DO $$ declare x pdt_pos := 5; begin x := x - 9; end$$"));
        assertEquals("23514", state("DO $$ declare x pdt_dom.c%type := -5; begin null; end$$"));
    }

    @Test
    void domainViolationSurfacesWhenTheFunctionRuns() throws Exception {
        exec("CREATE FUNCTION pdt_dfun() RETURNS int AS $$"
                + " declare x pdt_pos := 5; begin x := -1; return x; end $$ LANGUAGE plpgsql");
        assertEquals("23514", state("SELECT pdt_dfun()"));
    }

    @Test
    void valuesTheDomainAllowsStillPass() throws Exception {
        exec("DO $$ declare x pdt_pos := 5; begin x := 7; end$$");
        exec("DO $$ declare x pdt_pos := null; begin null; end$$");
        exec("DO $$ declare x pdt_pos; begin null; end$$");
        exec("DO $$ declare x pdt_int_nn := 42; begin x := 7; end$$");
    }

    // ---- Initialiser type checking ----

    @Test
    void anInitialiserGoesThroughTheDeclaredTypesInput() {
        assertEquals("22P02", state("DO $$ declare x int := 'abc'; begin null; end$$"));
        assertEquals("22P02", state("DO $$ declare x boolean := 'notabool'; begin null; end$$"));
        assertEquals("22P02", state("DO $$ declare x numeric := 'zz'; begin null; end$$"));
        assertEquals("22P02", state("DO $$ declare x pdt_misc.f1%type := 'abc'; begin null; end$$"));
    }

    @Test
    void aBadInitialiserFailsWhenTheFunctionRuns() throws Exception {
        exec("CREATE FUNCTION pdt_bad() RETURNS int AS $$"
                + " declare x int := 'abc'; begin return x; end $$ LANGUAGE plpgsql");
        assertEquals("22P02", state("SELECT pdt_bad()"));
    }

    @Test
    void initialisersTheTypeAcceptsAreUnchanged() throws Exception {
        exec("DO $$ declare x int := '42'; begin assert x = 42; end$$");
        exec("DO $$ declare x int := 3.7; begin assert x = 4; end$$");
        exec("DO $$ declare x text := 42; begin assert x = '42'; end$$");
        exec("DO $$ declare x int[] := '{1,2}'; begin assert x[1] = 1; end$$");
        exec("DO $$ declare x date := '2020-01-01'; begin null; end$$");
        exec("DO $$ declare x pdt_record := row(1,2); begin null; end$$");
        exec("DO $$ declare x pdt_misc%rowtype := null; begin null; end$$");
        exec("DO $$ declare x refcursor := 'cx'; begin null; end$$");
    }

    // ---- %TYPE and %ROWTYPE ----

    @Test
    void typeAndRowtypeMustResolve() {
        assertEquals("42704", state("DO $$ declare x pdt_nosuch%type; begin null; end $$"));
        assertEquals("42P01", state("DO $$ declare x pdt_nosuch.bar%type; begin null; end $$"));
        assertEquals("42703", state("DO $$ declare x public.pdt_misc.zed%type; begin null; end $$"));
        assertEquals("42P01", state("DO $$ declare x pdt_nosuch%rowtype; begin null; end $$"));
        assertEquals("3F000", state("DO $$ declare x pdt_nosuch.bar%rowtype; begin null; end $$"));
        assertEquals("42P01", state("DO $$ declare x public.pdt_nosuch.col%type; begin null; end $$"));
        assertEquals("3F000", state("DO $$ declare x nosuchschema.tbl.col%type; begin null; end $$"));
        assertEquals("42704", state("DO $$ declare x pdt_notatype; begin null; end $$"));
    }

    @Test
    void aMisspelledTypeReferenceStopsTheFunctionBeingCreated() {
        SQLException e = error("CREATE FUNCTION pdt_bad2() RETURNS int AS $$"
                + " declare x pdt_nosuch%type; begin return 1; end $$ LANGUAGE plpgsql");
        assertEquals("42704", e.getSQLState());
        assertTrue(e.getMessage().contains("variable \"pdt_nosuch\" does not exist"), e.getMessage());
    }

    @Test
    void referencesThatDoResolveStillWork() throws Exception {
        exec("DO $$ declare x pdt_misc%rowtype; begin null; end $$");
        exec("DO $$ declare x public.pdt_misc%rowtype; begin null; end $$");
        exec("DO $$ declare x public.pdt_misc.f1%type := 4; begin assert x = 4; end $$");
        exec("DO $$ declare x int; y x%type; begin null; end $$");
        exec("CREATE FUNCTION pdt_ok(a int) RETURNS int AS $$"
                + " declare x pdt_misc.f1%type := a; begin return x; end $$ LANGUAGE plpgsql");
        assertEquals("7", scalar("SELECT pdt_ok(7)"));
    }

    // ---- Duplicate declarations ----

    @Test
    void aNameMayBeDeclaredOncePerBlock() {
        SQLException e = error("DO $$ declare x int := 1; x int := 2; begin null; end $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("duplicate declaration"), e.getMessage());
        assertEquals("42601", state("DO $$ declare c cursor for select 1; c int; begin null; end $$"));
        assertEquals("42601", state("CREATE FUNCTION pdt_dup() RETURNS int AS $$"
                + " declare x int := 1; x int := 2; begin return x; end $$ LANGUAGE plpgsql"));
    }

    @Test
    void anInnerBlockMayReuseTheName() throws Exception {
        exec("DO $$ declare x int := 1; begin declare x int := 2; begin assert x = 2; end; end $$");
    }
}
