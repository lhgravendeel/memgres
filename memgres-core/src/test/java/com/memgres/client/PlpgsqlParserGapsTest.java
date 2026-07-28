package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PL/pgSQL a test suite can legitimately write and memgres refused: declaration initialisers
 * spelled with {@code =}, array variables in every form, scrollable cursors, loop end labels,
 * the {@code #variable_conflict} pragma, block-label qualification, named cursor arguments and
 * expression-valued RAISE options. A rejected function cannot be worked around, so each of these
 * cost more than a missing check would.
 */
class PlpgsqlParserGapsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE ppg_rr (a int, b int)");
        exec("CREATE TYPE ppg_pc AS (x int, y text)");
        exec("CREATE TABLE ppg_nums (n int)");
        exec("INSERT INTO ppg_nums VALUES (1),(2),(3),(4),(5)");
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

    /** Create a function returning text and call it, so the body is exercised end to end. */
    private static String bodyResult(String name, String body) throws SQLException {
        exec("CREATE OR REPLACE FUNCTION " + name + "() RETURNS text LANGUAGE plpgsql AS $$"
                + body + "$$");
        return scalar("SELECT " + name + "()");
    }

    private static void assertFails(String state, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(state, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(message),
                "expected \"" + message + "\" in: " + e.getMessage());
    }

    // ---- = as a declaration initialiser ----

    @Test
    void equalsInitialisesADeclaredVariable() throws Exception {
        assertEquals("11", bodyResult("ppg_eq1",
                " declare a int = 10; b int = 1; begin return (a + b)::text; end "));
        // the other two spellings still work, and mix freely with it
        assertEquals("6", bodyResult("ppg_eq2",
                " declare a int = 1; b int := 2; c int default 3; begin return (a+b+c)::text; end "));
    }

    @Test
    void aDeclarationWithNoInitialiserIsStillASyntaxError() {
        assertFails("42601", "syntax error at or near \"10\"",
                "CREATE FUNCTION ppg_eq3() RETURNS int LANGUAGE plpgsql AS $$"
                        + " declare a int 10; begin return a; end $$");
    }

    /** The body is compiled by CREATE FUNCTION, so a bad one never becomes a callable function. */
    @Test
    void aBodyThatDoesNotParseIsRejectedAtCreateTime() {
        assertFails("42601", "syntax error",
                "CREATE FUNCTION ppg_eq4() RETURNS int LANGUAGE plpgsql AS $$"
                        + " declare a int 10; begin return a; end $$");
        assertFails("42883", "ppg_eq4", "SELECT ppg_eq4()");
    }

    // ---- array variables ----

    @Test
    void arrayVariablesTakeBothInitialiserSpellings() throws Exception {
        assertEquals("{1,2,3}", bodyResult("ppg_ar1",
                " declare a int[] := array[1,2,3]; begin return a::text; end "));
        assertEquals("{1,2,3}", bodyResult("ppg_ar2",
                " declare a int[] := '{1,2,3}'; begin return a::text; end "));
    }

    @Test
    void subscriptedAssignmentReplacesOneElement() throws Exception {
        assertEquals("{1,99,3}", bodyResult("ppg_ar3",
                " declare a int[] := '{1,2,3}'; begin a[2] := 99; return a::text; end "));
        // past the end the array grows, with NULLs in the gap
        assertEquals("{1,2,3,NULL,7}", bodyResult("ppg_ar4",
                " declare a int[] := array[1,2,3]; begin a[5] := 7; return a::text; end "));
        // and an array that started out NULL becomes a one-element array
        assertEquals("{5}", bodyResult("ppg_ar5",
                " declare a int[]; begin a[1] := 5; return a::text; end "));
        assertEquals("{42,2,3}", bodyResult("ppg_ar6",
                " declare a int[]; begin a := array[1,2,3]; a[1] := 42; return a::text; end "));
    }

    @Test
    void sliceAssignmentReplacesARange() throws Exception {
        assertEquals("{1,8,9,4}", bodyResult("ppg_ar7",
                " declare a int[] := '{1,2,3,4}'; begin a[2:3] := '{8,9}'; return a::text; end "));
    }

    @Test
    void aSecondSubscriptReachesIntoTheSecondDimension() throws Exception {
        assertEquals("{{1,20},{3,4}}", bodyResult("ppg_ar8",
                " declare a int[] := '{{1,2},{3,4}}'; begin a[1][2] := 20; return a::text; end "));
    }

    @Test
    void anArrayOfCompositesTakesAFieldAssignment() throws Exception {
        assertEquals("{\"(9,a)\"}", bodyResult("ppg_ar9",
                " declare a ppg_pc[] := '{\"(1,a)\"}'; begin a[1].x := 9; return a::text; end "));
    }

    @Test
    void wholeArrayAssignmentIsUnchanged() throws Exception {
        assertEquals("{1,2,3}", bodyResult("ppg_ar10",
                " declare a int[] := '{1,2}'; begin a := a || 3; return a::text; end "));
        assertEquals("{1,2,3}", bodyResult("ppg_ar11",
                " declare a int[] := array[1,2]; begin return array_append(a, 3)::text; end "));
    }

    @Test
    void textArraysBehaveTheSameWay() throws Exception {
        assertEquals("{a,b,c}", bodyResult("ppg_ar12",
                " declare a text[] := '{a,b}'; begin a[3] := 'c'; return a::text; end "));
    }

    // ---- scrollable cursors ----

    @Test
    void scrollCursorsDeclareAndNavigateInBothDirections() throws Exception {
        assertEquals("1,2,1,5,1", bodyResult("ppg_sc1",
                " declare c scroll cursor for select n from ppg_nums order by n;"
                        + " r int; out text := '';"
                        + " begin open c; fetch c into r; out := out || r;"
                        + " fetch next from c into r; out := out || ',' || r;"
                        + " fetch prior from c into r; out := out || ',' || r;"
                        + " fetch last from c into r; out := out || ',' || r;"
                        + " fetch absolute 1 from c into r; out := out || ',' || r;"
                        + " close c; return out; end "));
    }

    @Test
    void noScrollIsAcceptedToo() throws Exception {
        assertEquals("1", bodyResult("ppg_sc2",
                " declare c no scroll cursor for select n from ppg_nums order by n; r int;"
                        + " begin open c; fetch c into r; close c; return r::text; end "));
    }

    @Test
    void moveRepositionsWithoutReturningARow() throws Exception {
        assertEquals("3,2,5", bodyResult("ppg_sc3",
                " declare c scroll cursor for select n from ppg_nums order by n;"
                        + " r int; out text := '';"
                        + " begin open c; move 2 from c; fetch c into r; out := out || r;"
                        + " move absolute 1 from c; fetch c into r; out := out || ',' || r;"
                        + " move relative 2 from c; fetch c into r; out := out || ',' || r;"
                        + " close c; return out; end "));
    }

    @Test
    void relativeFetchesCountFromTheCurrentRow() throws Exception {
        assertEquals("3,2", bodyResult("ppg_sc4",
                " declare c scroll cursor for select n from ppg_nums order by n;"
                        + " r int; out text := '';"
                        + " begin open c; fetch relative 3 from c into r; out := out || r;"
                        + " fetch relative -1 from c into r; out := out || ',' || r;"
                        + " close c; return out; end "));
    }

    @Test
    void aPlainBoundCursorStillWorks() throws Exception {
        assertEquals("1", bodyResult("ppg_sc5",
                " declare c cursor for select n from ppg_nums order by n; r int;"
                        + " begin open c; fetch c into r; close c; return r::text; end "));
    }

    @Test
    void fetchingFromAnUnopenedCursorReportsTheNullVariable() {
        assertFails("22004", "cursor variable \"c\" is null",
                "DO $$ declare c refcursor; r int; begin fetch c into r; end $$");
    }

    // ---- loop end labels ----

    @Test
    void aLoopMayRepeatItsLabelAfterEndLoop() throws Exception {
        exec("DO $$ begin <<ppg_flbl>> for i in 1 .. 10 loop exit ppg_flbl;"
                + " end loop ppg_flbl; end $$");
        exec("DO $$ begin <<ppg_l2>> loop exit ppg_l2; end loop ppg_l2; end $$");
        exec("DO $$ begin <<ppg_l3>> while true loop exit ppg_l3; end loop ppg_l3; end $$");
        exec("DO $$ declare x int; begin <<ppg_l4>> foreach x in array array[1,2] loop"
                + " exit ppg_l4; end loop ppg_l4; end $$");
    }

    @Test
    void anEndLabelIsCheckedRatherThanSkipped() {
        assertFails("42601", "end label \"ppg_flbl\" specified for unlabeled block",
                "DO $$ begin for i in 1 .. 10 loop exit; end loop ppg_flbl; end $$");
        assertFails("42601", "end label \"ppg_outer\" differs from block's label \"ppg_inner\"",
                "DO $$ <<ppg_outer>> begin <<ppg_inner>> for i in 1..3 loop exit;"
                        + " end loop ppg_outer; end $$");
        assertFails("42601", "end label \"ppg_c\" differs from block's label \"ppg_b\"",
                "DO $$ <<ppg_b>> begin null; end ppg_c $$");
        assertFails("42601", "end label \"ppg_c\" specified for unlabeled block",
                "DO $$ begin null; end ppg_c $$");
    }

    @Test
    void aMatchingBlockEndLabelIsAccepted() throws Exception {
        exec("DO $$ <<ppg_b2>> begin null; end ppg_b2 $$");
    }

    // ---- #variable_conflict ----

    @Test
    void variableConflictNamesTheWinner() throws Exception {
        exec("CREATE OR REPLACE FUNCTION ppg_vc1() RETURNS int LANGUAGE plpgsql AS $$"
                + " #variable_conflict use_column\n"
                + " declare n int := 99; begin return (select max(n) from ppg_nums); end $$");
        assertEquals("5", scalar("SELECT ppg_vc1()"));

        exec("CREATE OR REPLACE FUNCTION ppg_vc2() RETURNS int LANGUAGE plpgsql AS $$"
                + " #variable_conflict use_variable\n"
                + " declare n int := 99; begin return (select max(n) from ppg_nums); end $$");
        assertEquals("99", scalar("SELECT ppg_vc2()"));
    }

    @Test
    void errorIsStillTheDefaultAndStillTheDefaultBehaviour() throws Exception {
        exec("CREATE OR REPLACE FUNCTION ppg_vc3() RETURNS int LANGUAGE plpgsql AS $$"
                + " #variable_conflict error\n"
                + " declare n int := 99; begin return n; end $$");
        assertEquals("99", scalar("SELECT ppg_vc3()"));
        // without the pragma an ambiguous reference is still an error
        exec("CREATE OR REPLACE FUNCTION ppg_vc5() RETURNS int LANGUAGE plpgsql AS $$"
                + " declare n int := 99; begin return (select max(n) from ppg_nums); end $$");
        SQLException e = assertThrows(SQLException.class, () -> scalar("SELECT ppg_vc5()"));
        assertEquals("42702", e.getSQLState(), e.getMessage());
    }

    @Test
    void anUnknownVariableConflictValueIsRejected() {
        assertFails("42601", "syntax error at or near \"nonsense_value\"",
                "CREATE FUNCTION ppg_vc4() RETURNS int LANGUAGE plpgsql AS $$"
                        + " #variable_conflict nonsense_value\n"
                        + " declare n int := 99; begin return n; end $$");
    }

    // ---- block-label qualification ----

    @Test
    void aBlockLabelReachesAVariableAnInnerBlockShadows() throws Exception {
        assertEquals("1/2", bodyResult("ppg_bl1",
                " <<ppg_ob>> declare x int := 1; begin declare x int := 2;"
                        + " begin return ppg_ob.x::text || '/' || x::text; end; end "));
    }

    // ---- cursor arguments ----

    @Test
    void aBoundCursorTakesNamedAndPositionalArguments() throws Exception {
        assertEquals("41", bodyResult("ppg_cu1",
                " declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;"
                        + " begin open c1(param1 := 20, param2 := 21); fetch c1 into r;"
                        + " close c1; return r::text; end "));
        assertEquals("41", bodyResult("ppg_cu2",
                " declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;"
                        + " begin open c1(20, 21); fetch c1 into r; close c1; return r::text; end "));
    }

    @Test
    void abadCursorArgumentListKeepsTheFunctionFromBeingCreated() {
        String head = "CREATE FUNCTION ppg_cu3() RETURNS int LANGUAGE plpgsql AS $$"
                + " declare c1 cursor (param1 int, param2 int) for select param1 + param2; r int;"
                + " begin ";
        String tail = " fetch c1 into r; close c1; return r; end $$";
        assertFails("42601", "value for parameter \"param2\" of cursor \"c1\" specified more than once",
                head + "open c1(param2 := 20, 21);" + tail);
        assertFails("42601", "value for parameter \"param2\" of cursor \"c1\" specified more than once",
                head + "open c1(param2 := 20, param2 := 21);" + tail);
        assertFails("42601", "not enough arguments for cursor \"c1\"",
                head + "open c1(param2 := 20);" + tail);
        assertFails("42883", "ppg_cu3", "SELECT ppg_cu3()");
    }

    // ---- RAISE option values ----

    @Test
    void raiseUsingTakesAnExpression() {
        assertFails("22012", "custom message",
                "DO $$ begin raise division_by_zero using message = 'custom' || ' message'; end $$");
        assertFails("22012", "got: abc",
                "DO $$ declare v text := 'abc'; begin"
                        + " raise exception using message = 'got: ' || v, errcode = '22012'; end $$");
    }

    @Test
    void raiseOptionsStillAcceptPlainLiterals() throws Exception {
        assertFails("P0001", "plain", "DO $$ begin raise exception using message = 'plain'; end $$");
        assertFails("P0001", "boom 42",
                "DO $$ begin raise exception 'boom %', 42 using hint = 'try', detail = 'dd'; end $$");
        exec("DO $$ begin raise notice 'x' using detail = 'a' || 'b'; end $$");
    }

    @Test
    void anOptionGivenTwiceIsRejected() {
        assertFails("42601", "RAISE option already specified: DETAIL",
                "DO $$ begin raise notice 'x' using detail = 'd', detail = 'e'; end $$");
    }

    // ---- a table's composite type ----

    @Test
    void aTableAlsoNamesTheCompositeTypeOfItsRows() throws Exception {
        assertEquals("(1,)", scalar("SELECT row(1,null)::ppg_rr"));
        assertEquals("1", scalar("SELECT (row(1,2)::ppg_rr).a"));
        assertEquals("1", bodyResult("ppg_rc1",
                " declare r ppg_rr; begin r := row(1,2)::ppg_rr; return r.a::text; end "));
    }

    @Test
    void anUnknownTypeIsStillUnknown() {
        assertFails("42704", "type \"ppg_nope\" does not exist", "SELECT row(1,2)::ppg_nope");
    }

    // ---- neighbouring PL/pgSQL that must keep working ----

    @Test
    void recordFieldsReadAndWriteAsBefore() throws Exception {
        assertEquals("(3,q)", bodyResult("ppg_nb1",
                " declare v ppg_pc; begin v.x := 3; v.y := 'q'; return v::text; end "));
        assertEquals("42", bodyResult("ppg_nb2",
                " declare v ppg_pc; t text; begin v.x := 42; t := v.x::text; return t; end "));
    }

    @Test
    void caseStatementsWorkOverVariablesDeclaredWithEquals() throws Exception {
        assertEquals("gt", bodyResult("ppg_nb3",
                " declare a int = 10; b int = 1; begin"
                        + " case when a > b then return 'gt'; else return 'le'; end case; end "));
        assertEquals("in", bodyResult("ppg_nb4",
                " declare a int = 3; begin"
                        + " case a when 3,4,3+5 then return 'in'; else return 'out'; end case; end "));
    }

    @Test
    void anUnmatchedCaseIsStillCaseNotFound() {
        assertFails("20000", "case not found",
                "DO $$ declare a int = 9; begin case a when 1 then raise notice 'one';"
                        + " end case; end $$");
    }

    @Test
    void foreachOverAnArrayIsUnchanged() throws Exception {
        assertEquals("1,2,3", bodyResult("ppg_nb5",
                " declare x int; out text := ''; begin foreach x in array array[1,2,3] loop"
                        + " out := out || case when out = '' then '' else ',' end || x;"
                        + " end loop; return out; end "));
    }
}
