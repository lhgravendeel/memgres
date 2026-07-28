package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PL/pgSQL statements were accepted without the checks that give them meaning: EXIT and CONTINUE
 * named labels that enclosed nothing, FOREACH iterated whatever it was handed, RETURN ignored the
 * routine it sat in — a function that forgot to RETURN yielded NULL rather than raising —
 * exception handlers named conditions that do not exist and so never fired, ASSERT took anything
 * truthy, GET DIAGNOSTICS answered for items it has no business with, VARIADIC and parameter
 * defaults went unchecked, EXECUTE ran with placeholders nothing supplied and STRICT was inert
 * under it, and a cursor read after CLOSE ended quietly instead of erroring.
 */
class PlpgsqlStatementValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE psv_t (a int, b int)");
        exec("INSERT INTO psv_t VALUES (1,2),(3,4)");
        exec("CREATE TYPE psv_two AS (q1 bigint, q2 bigint)");
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

    // ---- EXIT and CONTINUE labels ----

    @Test
    void exitAndContinueNeedAnEnclosingLoop() {
        SQLException e = error("DO $$ begin begin continue; end; end; $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("CONTINUE cannot be used outside a loop"), e.getMessage());
        assertEquals("42601", state("DO $$ begin begin exit; end; end; $$"));
        assertEquals("42601", state("DO $$ begin exit; end; $$"));
    }

    @Test
    void aNamedLabelMustEncloseTheStatement() {
        SQLException e = error("DO $$ begin loop continue psv_nolabel; end loop; end; $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("there is no label \"psv_nolabel\""), e.getMessage());
        assertEquals("42601", state("DO $$ begin loop exit psv_nolabel; end loop; end; $$"));
        assertEquals("42601", state("CREATE FUNCTION psv_lbl() RETURNS int AS $$"
                + " begin loop continue psv_nolabel; end loop; return 1; end $$ LANGUAGE plpgsql"));
    }

    @Test
    void continueCannotNameABlock() {
        SQLException e = error("DO $$ <<psv_blk>> begin loop continue psv_blk; end loop; end; $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("block label \"psv_blk\" cannot be used in CONTINUE"),
                e.getMessage());
    }

    @Test
    void labelsThatDoEncloseStillWork() throws Exception {
        exec("DO $$ <<psv_blk>> begin exit psv_blk; end; $$");
        exec("DO $$ begin <<lp>> loop exit lp; end loop; end; $$");
        exec("DO $$ declare i int := 0; begin <<lp>> loop i := i + 1;"
                + " continue lp when i < 2; exit lp; end loop; assert i = 2; end; $$");
    }

    // ---- FOREACH ----

    @Test
    void foreachChecksItsOperand() {
        SQLException e = error("DO $$ declare x int; begin foreach x in array null::int[] loop null; end loop; end $$");
        assertEquals("22004", e.getSQLState());
        assertTrue(e.getMessage().contains("FOREACH expression must not be null"), e.getMessage());
        e = error("DO $$ declare x int; begin foreach x in array 42 loop null; end loop; end $$");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("must yield an array, not type integer"), e.getMessage());
        assertEquals("2202E", state("DO $$ declare x int[]; begin"
                + " foreach x slice 2 in array array[1,2,3] loop null; end loop; end $$"));
        assertEquals("42804", state("DO $$ declare x int; begin"
                + " foreach x slice 1 in array array[1,2] loop null; end loop; end $$"));
    }

    @Test
    void aScalarForeachVariableWalksTheFlattenedArray() throws Exception {
        exec("DO $$ declare x int; s text := ''; begin"
                + " foreach x in array array[[1,2],[3,4]] loop s := s || x || ','; end loop;"
                + " assert s = '1,2,3,4,', s; end $$");
        exec("DO $$ declare x int; s text := ''; begin"
                + " foreach x in array array[1,2,3] loop s := s || x || ','; end loop;"
                + " assert s = '1,2,3,', s; end $$");
        exec("DO $$ declare x int[]; s text := ''; begin"
                + " foreach x slice 1 in array array[[1,2],[3,4]] loop s := s || x[1] || ','; end loop;"
                + " assert s = '1,3,', s; end $$");
    }

    // ---- SETOF and RETURN ----

    @Test
    void returnMustSuitTheRoutine() {
        SQLException e = error("CREATE FUNCTION psv_r1() RETURNS SETOF int AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("RETURN cannot have a parameter in function returning set"),
                e.getMessage());
        e = error("CREATE FUNCTION psv_r2() RETURNS int AS $$"
                + " begin return next 1; end $$ LANGUAGE plpgsql");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot use RETURN NEXT in a non-SETOF function"),
                e.getMessage());
        assertEquals("42601", state("CREATE PROCEDURE psv_p1() AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql"));
        assertEquals("42804", state("CREATE FUNCTION psv_r3(IN a int, OUT b int) AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql"));
    }

    @Test
    void aFunctionThatFallsOffTheEndRaises() throws Exception {
        exec("CREATE FUNCTION psv_noret() RETURNS int AS $$ begin null; end $$ LANGUAGE plpgsql");
        SQLException e = error("SELECT psv_noret()");
        assertEquals("2F005", e.getSQLState());
        assertTrue(e.getMessage().contains("control reached end of function without RETURN"),
                e.getMessage());
        exec("CREATE FUNCTION psv_maybe(a int) RETURNS int AS $$"
                + " begin if a > 0 then return 1; end if; end $$ LANGUAGE plpgsql");
        assertEquals("1", scalar("SELECT psv_maybe(1)"));
        assertEquals("2F005", state("SELECT psv_maybe(-1)"));
    }

    @Test
    void aSetReturningBodyIsCheckedAgainstItsResultType() throws Exception {
        exec("CREATE FUNCTION psv_shape() RETURNS SETOF psv_t AS $$"
                + " begin return query select a from psv_t; end $$ LANGUAGE plpgsql");
        SQLException e = error("SELECT * FROM psv_shape()");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("structure of query does not match function result type"),
                e.getMessage());
        exec("CREATE FUNCTION psv_badnext() RETURNS SETOF int AS $$"
                + " begin return next 'notanint'; end $$ LANGUAGE plpgsql");
        assertEquals("22P02", state("SELECT * FROM psv_badnext()"));
    }

    @Test
    void wellFormedSetReturningFunctionsStillWork() throws Exception {
        exec("CREATE FUNCTION psv_ok() RETURNS SETOF int AS $$"
                + " begin return next 1; return next 2; end $$ LANGUAGE plpgsql");
        assertEquals("1", scalar("SELECT * FROM psv_ok() LIMIT 1"));
        exec("CREATE FUNCTION psv_void() RETURNS void AS $$ begin null; end $$ LANGUAGE plpgsql");
        exec("SELECT psv_void()");
        exec("CREATE PROCEDURE psv_p2() AS $$ begin return; end $$ LANGUAGE plpgsql");
        exec("CALL psv_p2()");
    }

    // ---- Exception conditions ----

    @Test
    void aHandlerMustNameAConditionThatExists() {
        SQLException e = error("DO $$ begin null; exception when psv_no_such_condition then null; end $$");
        assertEquals("42704", e.getSQLState());
        assertTrue(e.getMessage().contains("unrecognized exception condition"), e.getMessage());
        e = error("DO $$ begin null; exception when SQLSTATE 'notavalidstate' then null; end $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("invalid SQLSTATE code"), e.getMessage());
    }

    @Test
    void conditionsThatDoExistStillCompile() throws Exception {
        exec("DO $$ begin null; exception when SQLSTATE '22012' then null; end $$");
        exec("DO $$ begin null; exception when division_by_zero then null; end $$");
        exec("DO $$ begin null; exception when others then null; end $$");
        exec("DO $$ begin null; exception when no_data_found or too_many_rows then null; end $$");
    }

    // ---- ASSERT ----

    @Test
    void assertTakesOnlyABoolean() {
        SQLException e = error("DO $$ begin assert 42; end $$");
        assertEquals("22P02", e.getSQLState());
        assertTrue(e.getMessage().contains("invalid input syntax for type boolean: \"42\""),
                e.getMessage());
        assertEquals("22P02", state("DO $$ begin assert 'x'; end $$"));
    }

    @Test
    void assertsThatWorkedStillWork() throws Exception {
        exec("DO $$ begin assert true; end $$");
        assertEquals("P0004", state("DO $$ begin assert false, 'boom'; end $$"));
        assertEquals("P0004", state("DO $$ begin assert null; end $$"));
    }

    // ---- GET DIAGNOSTICS ----

    @Test
    void getStackedDiagnosticsBelongsInAHandler() {
        SQLException e = error("DO $$ declare st text; begin"
                + " get stacked diagnostics st = returned_sqlstate; end $$");
        assertEquals("0Z002", e.getSQLState());
        assertTrue(e.getMessage().contains("outside an exception handler"), e.getMessage());
    }

    @Test
    void eachFormOffersItsOwnItems() {
        assertEquals("42601", state("DO $$ declare n int; begin begin null; exception when others"
                + " then get stacked diagnostics n = row_count; end; end $$"));
        SQLException e = error("DO $$ declare v text; begin get diagnostics v = message_text; end $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains(
                "diagnostics item MESSAGE_TEXT is not allowed in GET CURRENT DIAGNOSTICS"),
                e.getMessage());
        e = error("DO $$ declare v text; begin get diagnostics v = psv_no_such_item; end $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("unrecognized GET DIAGNOSTICS item"), e.getMessage());
    }

    @Test
    void theDiagnosticsThatWorkedStillWork() throws Exception {
        exec("DO $$ declare n int; begin insert into psv_t values (9,9);"
                + " get diagnostics n = row_count; assert n = 1; end $$");
        exec("DO $$ declare st text; begin begin raise division_by_zero;"
                + " exception when others then get stacked diagnostics st = returned_sqlstate;"
                + " assert st = '22012', st; end; end $$");
    }

    @Test
    void sqlstateAndSqlerrmStayInsideAHandler() throws Exception {
        exec("CREATE FUNCTION psv_sq1() RETURNS text AS $$"
                + " begin return sqlstate; end $$ LANGUAGE plpgsql");
        assertEquals("42703", state("SELECT psv_sq1()"));
        exec("CREATE FUNCTION psv_sq2() RETURNS text AS $$"
                + " begin raise division_by_zero; exception when others then return sqlstate; end $$"
                + " LANGUAGE plpgsql");
        assertEquals("22012", scalar("SELECT psv_sq2()"));
    }

    // ---- Signatures ----

    @Test
    void variadicMustBeAnArrayAndComeLast() {
        SQLException e = error("CREATE FUNCTION psv_v1(VARIADIC a int) RETURNS int AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql");
        assertEquals("42P13", e.getSQLState());
        assertTrue(e.getMessage().contains("VARIADIC parameter must be an array"), e.getMessage());
        assertEquals("42883", state("SELECT psv_v1(1)"));
        e = error("CREATE FUNCTION psv_v2(VARIADIC a int[], b int) RETURNS int AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql");
        assertEquals("42P13", e.getSQLState());
        assertTrue(e.getMessage().contains("must be the last input parameter"), e.getMessage());
    }

    @Test
    void parametersAfterADefaultNeedDefaultsToo() {
        SQLException e = error("CREATE FUNCTION psv_d1(a int DEFAULT 1, b int) RETURNS int AS $$"
                + " begin return 1; end $$ LANGUAGE plpgsql");
        assertEquals("42P13", e.getSQLState());
        assertTrue(e.getMessage().contains("must also have defaults"), e.getMessage());
    }

    @Test
    void signaturesThatAreLegalStillWork() throws Exception {
        exec("CREATE FUNCTION psv_v3(VARIADIC a int[]) RETURNS int AS $$"
                + " begin return array_length(a,1); end $$ LANGUAGE plpgsql");
        assertEquals("3", scalar("SELECT psv_v3(1,2,3)"));
        exec("CREATE FUNCTION psv_d2(a int, b int DEFAULT 2) RETURNS int AS $$"
                + " begin return a+b; end $$ LANGUAGE plpgsql");
        assertEquals("3", scalar("SELECT psv_d2(1)"));
    }

    // ---- EXECUTE ----

    @Test
    void executeChecksItsPlaceholders() {
        SQLException e = error("DO $$ declare v int; begin"
                + " execute 'select a from psv_t where a = $1 and b = $2' into v using 1; end $$");
        assertEquals("42P02", e.getSQLState());
        assertTrue(e.getMessage().contains("there is no parameter $2"), e.getMessage());
    }

    @Test
    void strictIsEnforcedUnderExecute() {
        assertEquals("P0002", state("DO $$ declare v int; begin"
                + " execute 'select a from psv_t where a = 99' into strict v; end $$"));
        assertEquals("P0003", state("DO $$ declare v int; begin"
                + " execute 'select a from psv_t' into strict v; end $$"));
    }

    @Test
    void executeThatWasCorrectStaysCorrect() throws Exception {
        exec("DO $$ declare v int; begin execute 'select a from psv_t where a = 1' into strict v;"
                + " assert v = 1; end $$");
        exec("DO $$ declare v int; begin execute 'select a from psv_t where a = $1' into v using 1, 2;"
                + " assert v = 1; end $$");
        exec("DO $$ declare v int; begin select a into strict v from psv_t where a = 1;"
                + " assert v = 1; end $$");
    }

    // ---- Returned record type ----

    @Test
    void aReturnedRecordMustHaveTheDeclaredFields() throws Exception {
        exec("CREATE FUNCTION psv_rec(i int) RETURNS psv_two AS $$ declare r record;"
                + " begin r := row(i, i, i); return r; end $$ LANGUAGE plpgsql");
        SQLException e = error("SELECT (psv_rec(42)).q1");
        assertEquals("42804", e.getSQLState());
        assertTrue(e.getMessage().contains("returned record type does not match expected record type"),
                e.getMessage());
    }

    // ---- Cursors ----

    @Test
    void aCursorForLoopNeedsABoundCursor() {
        SQLException e = error("DO $$ declare c refcursor; r record;"
                + " begin for r in c loop null; end loop; end $$");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("cursor FOR loop must use a bound cursor variable"),
                e.getMessage());
    }

    @Test
    void aCursorCannotBeOpenedTwice() {
        assertEquals("42P03", state("DO $$ declare c cursor for select 1;"
                + " begin open c; open c; end $$"));
        assertEquals("42P03", state("DO $$ declare c1 refcursor := 'psv_dup'; c2 refcursor := 'psv_dup';"
                + " begin open c1 for select 1; open c2 for select 2; end $$"));
    }

    @Test
    void readingPastACloseRaises() {
        assertEquals("34000", state("DO $$ declare c cursor for select 1; v int;"
                + " begin open c; close c; fetch c into v; end $$"));
        assertEquals("34000", state("DO $$ declare c cursor for select 1;"
                + " begin open c; close c; close c; end $$"));
        assertEquals("22004", state("DO $$ declare c refcursor; v int;"
                + " begin fetch c into v; end $$"));
    }

    @Test
    void cursorsUsedProperlyStillWork() throws Exception {
        exec("DO $$ declare c cursor for select 1; v int;"
                + " begin open c; fetch c into v; close c; assert v = 1; end $$");
        exec("DO $$ declare c refcursor; v int;"
                + " begin open c for select 7; fetch c into v; close c; assert v = 7; end $$");
        exec("DO $$ declare c cursor for select 5; v int;"
                + " begin open c; fetch c into v; close c; open c; fetch c into v; close c;"
                + " assert v = 5; end $$");
    }
}
