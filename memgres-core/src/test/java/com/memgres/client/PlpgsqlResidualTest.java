package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a PL/pgSQL body still let through, measured against PostgreSQL 18.
 *
 * <p>Two of these lost data without saying so. A {@code FETCH} that landed on no row left the INTO
 * target holding whatever the previous fetch put there, so the loop idiom that reads "fetch until
 * the variable stops changing" saw the last row a second time; PostgreSQL sets the target to NULL.
 * And a write to a field a composite variable has not got was dropped into a field map nobody reads
 * back, so a misspelt field name lost the value with no sign of it.
 *
 * <p>The rest are checks that were simply absent: a declaration's length and precision were
 * advisory, {@code ALIAS FOR} did not parse, {@code RAISE} accepted a format string with more
 * placeholders than arguments and an option name that is not one of its own, {@code GET STACKED
 * DIAGNOSTICS} judged its arguments in the wrong order, and {@code SAVEPOINT} worked outside a
 * transaction block.
 *
 * <p>Which of them belong to the compile and which to the run was measured against the server
 * rather than assumed, because getting that backwards refuses bodies PostgreSQL accepts. Each rule
 * below is therefore paired with the same statement made unreachable: a compile-time rule still
 * fires there and a run-time rule does not. Several of the finding's own claims did not survive
 * being run — SCROLL cursors, MOVE, DISTINCT ON against a mismatched ORDER BY, FOR UPDATE with
 * GROUP BY and duplicate RAISE options already matched PostgreSQL — and the FETCH claim was
 * backwards: it is PostgreSQL that nulls the target, and memgres that kept the old value.
 */
class PlpgsqlResidualTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TYPE plrt_two AS (q1 bigint, q2 bigint)");
        exec("CREATE TYPE plrt_nested AS (c1 bigint, c2 plrt_two)");
        exec("CREATE TABLE plrt_dt (a varchar(3), b int)");
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

    /** The one value a single-column, single-row query returns, as text. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "no row from: " + sql);
                return rs.getString(1);
            }
        }
    }

    /** The SQLSTATE the statement fails with, or null when it succeeds. */
    private static String state(String sql) {
        try {
            exec(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The message the statement fails with, or null when it succeeds. */
    private static String message(String sql) {
        try {
            exec(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        String actual = message(sql);
        assertNotNull(actual, "expected failure from: " + sql);
        assertEquals(sqlState, state(sql), "wrong SQLSTATE for: " + sql + " -> " + actual);
        assertTrue(actual.toLowerCase().contains(messagePart.toLowerCase()),
                "wrong message for: " + sql + " -> " + actual);
    }

    private static void assertOk(String sql) {
        assertNull(state(sql), "expected success from: " + sql);
    }

    // ---- A FETCH that finds no row nulls its target ----

    @Test
    void aFetchPastTheLastRowNullsItsTarget() throws Exception {
        exec("CREATE FUNCTION plrt_keep() RETURNS text AS $$"
                + " declare cur cursor for select 42; n int;"
                + " begin open cur; fetch cur into n; fetch cur into n;"
                + " return coalesce(n::text,'NULL'); end $$ LANGUAGE plpgsql");
        assertEquals("NULL", scalar("SELECT plrt_keep()"));
    }

    @Test
    void theFetchLoopIdiomSeesNoRowRatherThanTheLastRowTwice() throws Exception {
        exec("CREATE FUNCTION plrt_loop() RETURNS text AS $$"
                + " declare cur refcursor; n int; acc text := '';"
                + " begin open cur for select * from (values (1),(2),(3)) v(x);"
                + " loop fetch cur into n; exit when not found; acc := acc || n::text; end loop;"
                + " fetch cur into n;"
                + " return acc || '/' || found::text || '/' || coalesce(n::text,'NULL'); end $$ LANGUAGE plpgsql");
        assertEquals("123/false/NULL", scalar("SELECT plrt_loop()"));
    }

    @Test
    void aRecordTargetIsEmptiedTheSameWay() throws Exception {
        exec("CREATE FUNCTION plrt_rec() RETURNS text AS $$"
                + " declare cur cursor for select 7 as a; r record;"
                + " begin open cur; fetch cur into r; fetch cur into r;"
                + " return coalesce(r.a::text,'NULL'); end $$ LANGUAGE plpgsql");
        assertEquals("NULL", scalar("SELECT plrt_rec()"));
    }

    @Test
    void aFetchThatFindsARowStillStoresIt() throws Exception {
        exec("CREATE FUNCTION plrt_hit() RETURNS text AS $$"
                + " declare cur cursor for select 42; n int;"
                + " begin open cur; fetch cur into n; return found::text || '/' || n::text; end $$ LANGUAGE plpgsql");
        assertEquals("true/42", scalar("SELECT plrt_hit()"));
    }

    // ---- A field the variable has not got ----

    @Test
    void writingAFieldACompositeHasNotGotIsRefused() {
        assertFails("42703", "record \"c\" has no field \"x\"",
                "DO $$ declare c plrt_two; begin c.x := 1; end $$");
    }

    @Test
    void readingAFieldACompositeHasNotGotIsRefused() {
        assertFails("42703", "record \"c\" has no field \"x\"",
                "DO $$ declare c plrt_two; v bigint; begin v := c.x; end $$");
    }

    @Test
    void aNestedFieldIsCheckedAgainstItsOwnType() {
        assertFails("42703", "no such column in data type plrt_two",
                "DO $$ declare c plrt_nested; begin c.c2.x := 1; end $$");
        assertFails("42703", "record \"c\" has no field \"zz\"",
                "DO $$ declare c plrt_nested; begin c.zz.q1 := 1; end $$");
    }

    @Test
    void aRowtypeVariableIsCheckedAgainstItsRelation() {
        assertFails("42703", "record \"r\" has no field \"nosuch\"",
                "DO $$ declare r plrt_dt%rowtype; begin r.nosuch := 1; end $$");
    }

    @Test
    void aRecordIsCheckedAgainstTheRowAssignedToIt() {
        assertFails("42703", "record \"r\" has no field \"zzz\"",
                "DO $$ declare r record; begin select 1 as a into r; r.zzz := 2; end $$");
        assertFails("42703", "record \"r\" has no field \"zzz\"",
                "DO $$ declare r record; v int; begin select 1 as a into r; v := r.zzz; end $$");
    }

    /** The field check belongs to the run: an unreachable bad write is still a valid body. */
    @Test
    void anUnreachableWriteToAMissingFieldIsAccepted() {
        assertOk("DO $$ declare c plrt_two; begin if false then c.x := 1; end if; end $$");
        assertOk("DO $$ declare r plrt_dt%rowtype; begin if false then r.nosuch := 1; end if; end $$");
        assertOk("DO $$ declare r record; begin select 1 as a into r;"
                + " if false then r.zzz := 2; end if; end $$");
    }

    @Test
    void theFieldsAVariableDoesHaveStillWorkBothWays() {
        assertOk("DO $$ declare c plrt_two; begin c.q1 := 1;"
                + " if c.q1 <> 1 then raise exception 'bad'; end if; end $$");
        assertOk("DO $$ declare c plrt_nested; begin c.c2.q1 := 5; end $$");
        assertOk("DO $$ declare c plrt_two; v bigint; begin v := c.q1;"
                + " if v is not null then raise exception 'bad'; end if; end $$");
        assertOk("DO $$ declare r record; begin select 1 as a into r; r.a := 2;"
                + " if r.a <> 2 then raise exception 'bad'; end if; end $$");
        assertOk("DO $$ declare r record; v text; begin select 1 as a, 2 as b into r;"
                + " v := r.a::text || '/' || r.b::text;"
                + " if v <> '1/2' then raise exception 'bad %', v; end if; end $$");
        assertOk("DO $$ declare r plrt_dt%rowtype; begin select 'ab' into r.a;"
                + " if r.a <> 'ab' then raise exception 'bad %', r.a; end if; end $$");
    }

    // ---- Declared length and precision ----

    @Test
    void aDeclaredLengthHoldsOnEveryWrite() {
        assertFails("22001", "value too long for type character varying(3)",
                "DO $$ declare v varchar(3); begin v := 'abcdef'; end $$");
        assertFails("22001", "value too long for type character varying(3)",
                "DO $$ declare v varchar(3) := 'abcdef'; begin null; end $$");
    }

    @Test
    void percentTypeCopiesTheColumnsLength() {
        assertFails("22001", "value too long for type character varying(3)",
                "DO $$ declare v plrt_dt.a%type; begin v := 'abcdef'; end $$");
    }

    @Test
    void aDeclaredPrecisionRoundsAndOverflows() throws Exception {
        assertFails("22003", "numeric field overflow",
                "DO $$ declare v numeric(3,1); begin v := 12345.6; end $$");
        assertOk("DO $$ declare v numeric(4,2); begin v := 1.239;"
                + " if v <> 1.24 then raise exception 'bad %', v; end if; end $$");
    }

    /** The length check belongs to the run, so an unreachable write is a valid body. */
    @Test
    void anUnreachableOverlongWriteIsAccepted() {
        assertOk("DO $$ declare v varchar(3); begin if false then v := 'abcdef'; end if; end $$");
    }

    @Test
    void whatFitsIsStoredAndAnUnconstrainedTypeTakesAnything() {
        assertOk("DO $$ declare v varchar(3); begin v := 'abc';"
                + " if v <> 'abc' then raise exception 'bad'; end if; end $$");
        assertOk("DO $$ declare v char(5); begin v := 'ab';"
                + " if v <> 'ab   ' then raise exception 'nopad [%]', v; end if; end $$");
        assertOk("DO $$ declare v varchar; begin v := 'abcdefghij';"
                + " if length(v) <> 10 then raise exception 'bad'; end if; end $$");
        assertOk("DO $$ declare v text; begin v := 'abcdefghij'; end $$");
    }

    // ---- CONSTANT and NOT NULL, and which check the compile makes ----

    @Test
    void aWriteToAConstantIsRefusedWhileTheBodyIsCompiled() {
        // Unreachable, and still refused: the compile never gets as far as asking whether it runs
        assertFails("22005", "is declared CONSTANT",
                "CREATE FUNCTION plrt_c1() RETURNS void AS $x$ declare c constant int := 1;"
                + " begin if false then c := 2; end if; end $x$ LANGUAGE plpgsql");
    }

    @Test
    void everyFormOfWriteToAConstantCounts() {
        assertFails("22005", "is declared CONSTANT",
                "DO $$ declare c constant int := 1; cur cursor for select 1;"
                + " begin open cur; fetch cur into c; end $$");
        assertFails("22005", "is declared CONSTANT",
                "DO $$ declare c constant int := 1; begin get diagnostics c = row_count; end $$");
        assertFails("22005", "is declared CONSTANT",
                "DO $$ declare c constant int := 1;"
                + " begin foreach c in array array[1,2] loop null; end loop; end $$");
        assertOk("DO $$ declare c constant int := 1;"
                + " begin if c <> 1 then raise exception 'bad'; end if; end $$");
    }

    @Test
    void notNullSplitsAcrossTheTwoPhases() {
        // No default is a fact about the declaration, so the compile settles it
        assertFails("22004", "must have a default value",
                "CREATE FUNCTION plrt_n1() RETURNS void AS $x$ declare y int not null;"
                + " begin null; end $x$ LANGUAGE plpgsql");
        // Assigning NULL is a fact about the run, so only a reached assignment fails
        assertFails("22004", "declared NOT NULL",
                "DO $$ declare x int not null := 1; begin x := null; end $$");
        assertOk("DO $$ declare x int not null := 1; begin if false then x := null; end if; end $$");
        assertOk("DO $$ declare x int not null default 3;"
                + " begin if x <> 3 then raise exception 'bad'; end if; end $$");
    }

    // ---- ALIAS FOR ----

    @Test
    void aliasForNamesAPositionalParameter() throws Exception {
        exec("CREATE FUNCTION plrt_alias(int) RETURNS int AS $$ declare n alias for $1;"
                + " begin return n + 1; end $$ LANGUAGE plpgsql");
        assertEquals("6", scalar("SELECT plrt_alias(5)"));
    }

    @Test
    void writingThroughAnAliasWritesTheVariableItNames() throws Exception {
        exec("CREATE FUNCTION plrt_aliasvar(a int) RETURNS int AS $$ declare n alias for a;"
                + " begin n := n + 1; return a; end $$ LANGUAGE plpgsql");
        assertEquals("6", scalar("SELECT plrt_aliasvar(5)"));
    }

    @Test
    void anAliasHasToNameSomethingThatExists() {
        assertFails("42704", "variable \"plrt_nosuchvar\" does not exist",
                "DO $$ declare n alias for plrt_nosuchvar; begin null; end $$");
    }

    // ---- RAISE ----

    @Test
    void theFormatStringAndItsArgumentsHaveToAgree() {
        assertFails("42601", "too few parameters specified for RAISE",
                "DO $$ begin raise notice 'too few: %, %, %', 1, 1; end $$");
        assertFails("42601", "too few parameters specified for RAISE",
                "DO $$ begin raise notice 'trailing percent %'; end $$");
        assertFails("42601", "too many parameters specified for RAISE",
                "DO $$ begin raise notice 'one %', 1, 2; end $$");
    }

    /** Counting is a fact about the text, so the compile settles it even for a dead statement. */
    @Test
    void theArgumentCountIsCheckedWhileTheBodyIsCompiled() {
        assertFails("42601", "too few parameters specified for RAISE",
                "CREATE FUNCTION plrt_r1() RETURNS void AS $x$"
                + " begin if false then raise notice '% %', 1; end if; end $x$ LANGUAGE plpgsql");
    }

    @Test
    void anOptionNameThatIsNotOneOfRaisesIsRefused() {
        assertFails("42601", "unrecognized RAISE statement option",
                "DO $$ begin raise notice 'x' using nosuchopt = 'd'; end $$");
    }

    @Test
    void aConditionNameHasToBeOneThatExists() {
        assertFails("42704", "unrecognized exception condition",
                "DO $$ begin raise notice plrt_no_such_condition; end $$");
    }

    /** Giving an option twice is judged when the RAISE runs, not while it is compiled. */
    @Test
    void aDuplicateOptionIsJudgedWhenTheRaiseRuns() {
        assertFails("42601", "RAISE option already specified: DETAIL",
                "DO $$ begin raise notice 'x' using detail = 'd', detail = 'e'; end $$");
        assertFails("42601", "RAISE option already specified: MESSAGE",
                "DO $$ begin raise exception 'lit' using message = 'other'; end $$");
        assertOk("DO $$ begin if false then"
                + " raise notice 'x' using hint = 'a', hint = 'b'; end if; end $$");
    }

    @Test
    void aBareRaiseNeedsAHandlerToReRaiseFrom() {
        assertFails("0Z002", "RAISE without parameters cannot be used outside an exception handler",
                "DO $$ begin raise; end $$");
        assertOk("DO $$ begin if false then raise; end if; end $$");
    }

    @Test
    void aConditionNameMayFollowALevel() {
        assertOk("DO $$ begin raise notice division_by_zero; end $$");
        assertOk("DO $$ begin raise notice unique_violation; end $$");
        assertFails("22012", "custom message",
                "DO $$ begin raise division_by_zero using message = 'custom' || ' message'; end $$");
    }

    @Test
    void theOrdinaryRaiseShapesStillWork() {
        assertOk("DO $$ begin raise notice 'plain %', 42; end $$");
        assertOk("DO $$ begin raise notice '100%% done'; end $$");
        assertOk("DO $$ begin raise debug 'd'; raise log 'l'; raise info 'i'; raise warning 'w'; end $$");
        assertFails("22012", "boom",
                "DO $$ begin raise exception 'boom' using hint = 'a' || 'b', errcode = '22012'; end $$");
    }

    // ---- GET DIAGNOSTICS ----

    /** Target first, then item name, then the form's own list — and only then, where it is. */
    @Test
    void anUnknownTargetOutranksEverythingElse() {
        assertFails("42601", "\"plrt_nv\" is not a known variable",
                "DO $$ begin get stacked diagnostics plrt_nv = returned_sqlstate; end $$");
        assertFails("42601", "\"plrt_nv\" is not a known variable",
                "DO $$ begin get stacked diagnostics plrt_nv = plrt_no_such_item; end $$");
    }

    @Test
    void anUnknownItemNameOutranksTheFormsOwnList() {
        assertFails("42601", "unrecognized GET DIAGNOSTICS item",
                "DO $$ declare v text; begin get stacked diagnostics v = plrt_no_such_item; end $$");
        assertFails("42601", "unrecognized GET DIAGNOSTICS item",
                "DO $$ declare v int; w text;"
                + " begin get stacked diagnostics v = row_count, w = plrt_no_such_item; end $$");
    }

    @Test
    void anItemHasToBelongToTheFormItIsWrittenIn() {
        assertFails("42601", "ROW_COUNT is not allowed in GET STACKED DIAGNOSTICS",
                "DO $$ declare n int; begin get stacked diagnostics n = row_count; end $$");
        assertFails("42601", "RETURNED_SQLSTATE is not allowed in GET CURRENT DIAGNOSTICS",
                "DO $$ declare v text; begin get diagnostics v = returned_sqlstate; end $$");
    }

    /** Whether a handler is running is the one question left to the run. */
    @Test
    void beingOutsideAHandlerIsTheOnlyRunTimeComplaint() {
        assertFails("0Z002", "GET STACKED DIAGNOSTICS cannot be used outside an exception handler",
                "DO $$ declare v text; begin get stacked diagnostics v = returned_sqlstate; end $$");
        assertOk("DO $$ declare v text;"
                + " begin if false then get stacked diagnostics v = returned_sqlstate; end if; end $$");
    }

    @Test
    void getCurrentDiagnosticsSpellsOutWhatPlainGetDiagnosticsMeans() {
        assertOk("DO $$ declare n int; s text;"
                + " begin get current diagnostics n = row_count, s = pg_context; end $$");
    }

    @Test
    void insideAHandlerTheRaisedConditionIsReportedByItsOwnName() throws Exception {
        exec("CREATE FUNCTION plrt_diag() RETURNS text AS $$ declare s text; m text;"
                + " begin begin raise division_by_zero; exception when others then"
                + " get stacked diagnostics s = returned_sqlstate, m = message_text; end;"
                + " return s || '/' || m; end $$ LANGUAGE plpgsql");
        assertEquals("22012/division_by_zero", scalar("SELECT plrt_diag()"));
    }

    @Test
    void getDiagnosticsRowCountAfterDmlStillWorks() throws Exception {
        exec("CREATE TABLE plrt_rc (a int)");
        exec("CREATE FUNCTION plrt_count() RETURNS int AS $$ declare n int;"
                + " begin insert into plrt_rc values (1),(2),(3); get diagnostics n = row_count;"
                + " return n; end $$ LANGUAGE plpgsql");
        assertEquals("3", scalar("SELECT plrt_count()"));
    }

    // ---- Guards a statement gets before it runs ----

    @Test
    void savepointCommandsNeedATransactionBlock() {
        assertFails("25P01", "SAVEPOINT can only be used in transaction blocks",
                "SAVEPOINT plrt_sp");
        assertFails("25P01", "RELEASE SAVEPOINT can only be used in transaction blocks",
                "RELEASE SAVEPOINT plrt_nosuch");
        assertFails("25P01", "ROLLBACK TO SAVEPOINT can only be used in transaction blocks",
                "ROLLBACK TO SAVEPOINT plrt_nosuch");
    }

    @Test
    void insideATransactionBlockTheSavepointCommandsWork() throws Exception {
        try (Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(10);
                st.execute("SAVEPOINT plrt_sp");
                st.execute("RELEASE SAVEPOINT plrt_sp");
                // A savepoint that was never taken is a different complaint from having no block
                SQLException e = assertThrows(SQLException.class,
                        () -> st.execute("RELEASE SAVEPOINT plrt_nosuch"));
                assertEquals("3B001", e.getSQLState());
            }
            c.rollback();
        }
    }

    /** A PL/pgSQL exception block takes a savepoint of its own and must not be caught by this. */
    @Test
    void anExceptionBlockStillWorksOutsideATransactionBlock() throws Exception {
        exec("CREATE FUNCTION plrt_exc() RETURNS text AS $$ begin"
                + " begin perform 1/0; exception when division_by_zero then return 'caught'; end;"
                + " return 'no'; end $$ LANGUAGE plpgsql");
        assertEquals("caught", scalar("SELECT plrt_exc()"));
    }

    @Test
    void encodeNamesAnEncodingThatExists() throws Exception {
        assertFails("22023", "unrecognized encoding: \"nosuch\"",
                "SELECT encode('a'::bytea, 'nosuch')");
        assertEquals("YQ==", scalar("SELECT encode('a'::bytea,'base64')"));
        assertEquals("61", scalar("SELECT encode('a'::bytea,'hex')"));
        assertEquals("a", scalar("SELECT encode('a'::bytea,'escape')"));
    }

    @Test
    void toNumberNeedsSomethingToRead() throws Exception {
        assertFails("22P02", "invalid input syntax for type numeric",
                "SELECT to_number('abc', '9999')");
        assertEquals("12", scalar("SELECT to_number('12abc','9999')::text"));
        assertEquals("-12", scalar("SELECT to_number('-12','S999')::text"));
    }

    /** A query names a relation, so the whole qualified name is what is reported missing. */
    @Test
    void aQualifiedRelationInAMissingSchemaIsAMissingRelation() {
        assertFails("42P01", "relation \"plrt_nosuchschema.t\" does not exist",
                "SELECT * FROM plrt_nosuchschema.t");
        assertFails("42P01", "relation \"plrt_nosuchschema.t\" does not exist",
                "INSERT INTO plrt_nosuchschema.t VALUES (1)");
        assertFails("42P01", "relation \"plrt_nosuchschema.t\" does not exist",
                "DELETE FROM plrt_nosuchschema.t");
        // Creating in a schema does name the schema, and a missing one there is still 3F000
        assertFails("3F000", "schema \"plrt_nosuchschema\" does not exist",
                "CREATE TABLE plrt_nosuchschema.t (a int)");
    }

    @Test
    void aYearPastWhatTheTypeHoldsIsARangeProblem() throws Exception {
        assertFails("22008", "timestamp out of range", "SELECT '294277-01-01'::timestamp");
        assertFails("22008", "date out of range", "SELECT '5874898-01-01'::date");
        // A year the type does hold is read, however wide it is written
        assertEquals("294277-01-01", scalar("SELECT '294277-01-01'::date::text"));
        // and text that is not a date at all never had a field to overflow
        assertFails("22007", "invalid input syntax for type date", "SELECT 'nonsense'::date");
    }
}
