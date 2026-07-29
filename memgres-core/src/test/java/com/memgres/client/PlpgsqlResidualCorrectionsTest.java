package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where the PL/pgSQL checks reached past what PostgreSQL 18 refuses, and where they still fell
 * short of it. Every expectation below was measured against a live PostgreSQL 18 before it was
 * written down.
 *
 * <p>Four of these refused bodies the server runs, which is the worse direction of the two:
 *
 * <ul>
 *   <li>A RAISE argument list was split on every comma at the top level, so the commas inside
 *       {@code ARRAY[1,2]} counted as argument separators and the body was refused with "too many
 *       parameters specified for RAISE". Brackets nest an expression exactly as parentheses do.
 *   <li>A format string written with dollar quotes was not recognised as a format string at all,
 *       so the parse fell through to the condition-name branch and failed.
 *   <li>{@code SELECT}/{@code FETCH}/{@code EXECUTE}/{@code FOR} into a %ROWTYPE or composite
 *       variable keyed the row by the query's own column names, so anything the query computed
 *       landed under a name the variable has not got — and the field read that followed was then
 *       refused as a missing field. PostgreSQL assigns the columns to the fields in order.
 *   <li>{@code ALIAS FOR $1} resolved only when the parameter had no name of its own, so the
 *       ordinary spelling over a named parameter failed the moment it ran.
 * </ul>
 *
 * <p>The rest are things the checks let through or reported differently: what a {@code %} writes
 * for an array, a boolean and a NULL; the message {@code RAISE SQLSTATE} carries; the length a
 * number stored in a {@code varchar(n)} is held to; the case a trigger names its row in; and
 * {@code RAISE INFO}, which PostgreSQL sends to the client whatever client_min_messages says.
 */
class PlpgsqlResidualCorrectionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TYPE plrx_two AS (q1 bigint, q2 bigint)");
        exec("CREATE TABLE plrx_t (id int primary key, n int, s varchar(10))");
        exec("INSERT INTO plrx_t VALUES (1,10,'aa'),(2,20,'bb')");
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
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "no row from: " + sql);
                return rs.getString(1);
            }
        }
    }

    /** Every notice the statement sent, in order. */
    private static List<String> notices(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
            for (SQLWarning w = st.getWarnings(); w != null; w = w.getNextWarning()) {
                out.add(w.getMessage() == null ? "" : w.getMessage().trim());
            }
        }
        return out;
    }

    private static String state(String sql) {
        try {
            exec(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        String message;
        try {
            exec(sql);
            message = null;
        } catch (SQLException e) {
            message = e.getMessage();
        }
        assertNotNull(message, "expected failure from: " + sql);
        assertEquals(sqlState, state(sql), "wrong SQLSTATE for: " + sql + " -> " + message);
        assertTrue(message.toLowerCase().contains(messagePart.toLowerCase()),
                "wrong message for: " + sql + " -> " + message);
    }

    private static void assertOk(String sql) {
        assertNull(state(sql), "expected success from: " + sql);
    }

    // ---- An array literal is one RAISE argument ----

    @Test
    void anArrayLiteralIsOneRaiseArgumentNotOnePerElement() throws Exception {
        assertEquals("{1,2}", notices("DO $$ begin raise notice '%', ARRAY[1,2]; end $$").get(0));
        assertEquals("{1,2,3}",
                notices("DO $$ begin raise notice '%', array[ 1 , 2 , 3 ]; end $$").get(0));
        assertEquals("{1,2} {a,b}",
                notices("DO $$ begin raise notice '% %', ARRAY[1,2], ARRAY['a','b']; end $$").get(0));
        assertEquals("{1,2,3}",
                notices("DO $$ begin raise notice '%', ARRAY[1] || ARRAY[2,3]; end $$").get(0));
        // and the same inside a body the server compiles rather than runs
        assertOk("CREATE FUNCTION plrx_arr() RETURNS void AS $x$"
                + " begin raise notice '%', ARRAY[1,2]; end $x$ LANGUAGE plpgsql");
        exec("DROP FUNCTION plrx_arr()");
        assertFails("22012", "{1,2}",
                "DO $$ begin raise exception '%', ARRAY[1,2] using errcode = '22012'; end $$");
    }

    @Test
    void argumentsEitherSideOfAnArrayAreStillCountedTheSameWay() throws Exception {
        // the shapes that already worked
        assertEquals("1 4",
                notices("DO $$ begin raise notice '% %', coalesce(1,2), greatest(3,4); end $$").get(0));
        assertEquals("(1,2)", notices("DO $$ begin raise notice '%', row(1,2); end $$").get(0));
        assertEquals("100% done", notices("DO $$ begin raise notice '100%% done'; end $$").get(0));
        // and a comma that really does separate arguments still counts as one
        assertFails("42601", "too many parameters specified for RAISE",
                "DO $$ begin raise notice '%', 1, 2; end $$");
        assertFails("42601", "too few parameters specified for RAISE",
                "DO $$ begin raise notice '% %', ARRAY[1,2]; end $$");
    }

    // ---- A dollar-quoted format string is a format string ----

    @Test
    void aDollarQuotedFormatStringIsReadAsOne() throws Exception {
        assertEquals("dollar 7 quoted",
                notices("DO $$ begin raise notice $q$dollar % quoted$q$, 7; end $$").get(0));
        // a bare name after a level is still a condition name
        assertFails("22012", "division_by_zero", "DO $$ begin raise division_by_zero; end $$");
    }

    // ---- What a % writes ----

    @Test
    void aPlaceholderWritesTheValueTheWayItsTypeWritesIt() throws Exception {
        assertEquals("<NULL>", notices("DO $$ begin raise notice '%', NULL; end $$").get(0));
        assertEquals("t", notices("DO $$ begin raise notice '%', true; end $$").get(0));
        assertEquals("f", notices("DO $$ begin raise notice '%', false; end $$").get(0));
        assertEquals("{}", notices("DO $$ begin raise notice '%', ARRAY[]::int[]; end $$").get(0));
        assertEquals("{a,\"b c\"}",
                notices("DO $$ begin raise notice '%', ARRAY['a','b c']; end $$").get(0));
        assertEquals("{{1,2},{3,4}}",
                notices("DO $$ begin raise notice '%', ARRAY[ARRAY[1,2],ARRAY[3,4]]; end $$").get(0));
        assertEquals("\\x616263",
                notices("DO $$ begin raise notice '%', 'abc'::bytea; end $$").get(0));
        // an unset variable is nothing, and says so
        assertEquals("<NULL>", notices("DO $$ declare v int; begin raise notice '%', v; end $$").get(0));
        // and the ordinary values are untouched
        assertEquals("plain", notices("DO $$ begin raise notice '%', 'plain'; end $$").get(0));
        assertEquals("1/two", notices("DO $$ begin raise notice '%/%', 1, 'two'; end $$").get(0));
        assertEquals("1.50", notices("DO $$ begin raise notice '%', 1.50::numeric; end $$").get(0));
    }

    // ---- A row variable takes the query's columns in order ----

    @Test
    void selectIntoARowVariableAssignsItsFieldsInOrder() throws Exception {
        exec("CREATE FUNCTION plrx_rows() RETURNS text AS $x$"
                + " declare r plrx_t%rowtype; c plrx_two; out_ text := '';"
                + " begin"
                + "   select id, n*2, upper(s) into r from plrx_t where id = 1;"
                + "   out_ := r.id || '/' || r.n || '/' || r.s;"
                + "   select 5, 6 into c;"
                + "   out_ := out_ || ' ' || c.q1 || '/' || c.q2;"
                + "   execute 'select id, n*3, s from plrx_t where id = 2' into r;"
                + "   return out_ || ' ' || r.id || '/' || r.n || '/' || r.s;"
                + " end $x$ LANGUAGE plpgsql");
        assertEquals("1/20/AA 5/6 2/60/bb", scalar("SELECT plrx_rows()"));
        exec("DROP FUNCTION plrx_rows()");
    }

    @Test
    void fetchIntoARowVariableAssignsItsFieldsInOrder() {
        assertOk("DO $$ declare c cursor for select id, n*2, s from plrx_t order by id;"
                + " r plrx_t%rowtype; begin open c; fetch c into r;"
                + " if r.id || '/' || r.n || '/' || r.s <> '1/20/aa'"
                + " then raise exception 'bad %', r.n; end if; end $$");
    }

    @Test
    void aForLoopBindsEachRowIntoARowVariableTheSameWay() throws Exception {
        exec("CREATE FUNCTION plrx_loop() RETURNS text AS $x$"
                + " declare r plrx_t%rowtype; c plrx_two; out_ text := '';"
                + " begin"
                + "   for r in select id, n*2, s from plrx_t order by id"
                + "     loop out_ := out_ || r.id || ':' || r.n || ' '; end loop;"
                + "   for c in select 8, 9 loop out_ := out_ || c.q1 || '/' || c.q2; end loop;"
                + "   return out_;"
                + " end $x$ LANGUAGE plpgsql");
        assertEquals("1:20 2:40 8/9", scalar("SELECT plrx_loop()"));
        exec("DROP FUNCTION plrx_loop()");
    }

    @Test
    void aRecordStillTakesTheQuerysOwnColumnNames() {
        assertOk("DO $$ declare r record; t int := 0; begin"
                + " for r in select id, n*2 as doubled from plrx_t order by id"
                + "   loop t := t + r.doubled; end loop;"
                + " if t <> 60 then raise exception 'bad %', t; end if; end $$");
        // and the whole row still arrives when the names do match
        assertOk("DO $$ declare r plrx_t%rowtype; begin"
                + " select * into r from plrx_t where id = 2;"
                + " if r.id || '/' || r.n || '/' || r.s <> '2/20/bb'"
                + " then raise exception 'bad %', r.n; end if; end $$");
    }

    @Test
    void aFieldTheLoopVariableHasNotGotIsRefused() {
        assertFails("42703", "record \"r\" has no field \"nosuch\"",
                "DO $$ declare r plrx_t%rowtype; begin for r in select * from plrx_t"
                        + " loop raise notice '%', r.nosuch; end loop; end $$");
        assertFails("42703", "record \"r\" has no field \"nosuch\"",
                "DO $$ declare r record; begin for r in select * from plrx_t"
                        + " loop raise notice '%', r.nosuch; end loop; end $$");
        assertFails("42703", "record \"r\" has no field \"nosuch\"",
                "DO $$ declare r record; begin for r in execute 'select * from plrx_t'"
                        + " loop raise notice '%', r.nosuch; end loop; end $$");
    }

    // ---- ALIAS FOR reaches a named parameter through its position ----

    @Test
    void aliasForAPositionReachesTheParameterEvenWhenItHasAName() throws Exception {
        exec("CREATE FUNCTION plrx_alias(p int) RETURNS int AS $x$"
                + " declare a alias for $1; begin return a + 1; end $x$ LANGUAGE plpgsql");
        assertEquals("6", scalar("SELECT plrx_alias(5)"));
        exec("DROP FUNCTION plrx_alias(int)");

        // writing through the alias writes the parameter, which its own name still reads
        exec("CREATE FUNCTION plrx_aliasw(p int) RETURNS int AS $x$"
                + " declare a alias for $1; begin a := a * 2; return p; end $x$ LANGUAGE plpgsql");
        assertEquals("10", scalar("SELECT plrx_aliasw(5)"));
        exec("DROP FUNCTION plrx_aliasw(int)");

        // an unnamed parameter and an alias of another variable go on working
        exec("CREATE FUNCTION plrx_alias2(int, int) RETURNS text AS $x$"
                + " declare a alias for $1; b alias for $2;"
                + " begin return a::text || '/' || b::text; end $x$ LANGUAGE plpgsql");
        assertEquals("4/5", scalar("SELECT plrx_alias2(4,5)"));
        exec("DROP FUNCTION plrx_alias2(int,int)");
        assertFails("42704", "variable \"plrx_nosuchvar\" does not exist",
                "DO $$ declare n alias for plrx_nosuchvar; begin null; end $$");
    }

    // ---- RAISE SQLSTATE reports the SQLSTATE as its message ----

    @Test
    void raiseSqlstateWithNothingElseToSayReportsTheSqlstate() {
        assertFails("22012", "22012", "DO $$ begin raise sqlstate '22012'; end $$");
        assertFails("ZZ999", "ZZ999", "DO $$ begin raise exception sqlstate 'ZZ999'; end $$");
        // but a message it was given is the message
        assertFails("22012", "said so",
                "DO $$ begin raise exception sqlstate '22012' using message = 'said so'; end $$");
    }

    // ---- A varchar(n) local holds a number to its written length ----

    @Test
    void aNumberStoredInAVarcharIsHeldToItsWrittenLength() {
        assertFails("22001", "value too long for type character varying(5)",
                "DO $$ declare v varchar(5); begin v := 123456; end $$");
        assertOk("DO $$ declare v varchar(5); begin v := 12345;"
                + " if v::text <> '12345' then raise exception 'bad %', v; end if; end $$");
        assertOk("DO $$ declare v varchar; begin v := 1234567890; end $$");
    }

    // ---- A trigger names its row the way the body wrote it ----

    @Test
    void aTriggerRowIsNamedInTheCaseTheBodyWroteIt() throws Exception {
        exec("CREATE FUNCTION plrx_tf() RETURNS trigger AS $x$"
                + " begin new.nosuchcol := 1; return new; end $x$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER plrx_tr BEFORE INSERT ON plrx_t"
                + " FOR EACH ROW EXECUTE FUNCTION plrx_tf()");
        assertFails("42703", "record \"new\" has no field \"nosuchcol\"",
                "INSERT INTO plrx_t VALUES (9,9,'z')");
        exec("DROP TRIGGER plrx_tr ON plrx_t");
        exec("DROP FUNCTION plrx_tf()");
    }

    // ---- RAISE INFO is not governed by client_min_messages ----

    @Test
    void raiseInfoReachesTheClientWhateverClientMinMessagesSays() throws Exception {
        exec("SET client_min_messages = warning");
        try {
            assertEquals("shown", notices("DO $$ begin raise info 'shown'; end $$").get(0));
            // NOTICE, which the setting does govern, stays behind
            assertTrue(notices("DO $$ begin raise notice 'hidden'; end $$").isEmpty());
        } finally {
            exec("SET client_min_messages = notice");
        }
        assertEquals("seen", notices("DO $$ begin raise notice 'seen'; end $$").get(0));
    }
}
