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
 * The rest of what a PL/pgSQL body still got wrong, measured against a live PostgreSQL 18 before
 * and after each change. Three of these were bodies the server runs and memgres refused, which is
 * the direction that matters most:
 *
 * <ul>
 *   <li>A FOR over rows bound its variable in a scope of its own, so reading the variable after
 *       the loop did not resolve at all. In PostgreSQL the target is the variable the block
 *       declared and it still holds the last row the loop saw.
 *   <li>A whole row assigned to a %ROWTYPE variable — {@code r := (SELECT t FROM t …)} or
 *       {@code r := ROW(…)} — was stored as one opaque value, so every field of it read NULL.
 *   <li>{@code SELECT … INTO} a record that matched no row left the record with no shape, so the
 *       field read that followed did not resolve. PostgreSQL leaves the query's shape with every
 *       field NULL.
 * </ul>
 *
 * <p>The rest are values reported differently or checks that were missing: what a {@code %}
 * writes for a whole row, a float and a date; the blanks a char(n) loses when it is read as text;
 * the length and precision an array declaration holds each element to; the fractional-seconds
 * precision a declared timestamp rounds to; the 25P02 an aborted transaction owes SAVEPOINT and
 * RELEASE SAVEPOINT; the 0A000 a FETCH with a multi-row direction and an INTO owes at compile
 * time; the 3F000 a DROP naming an absent schema owes; and the value of type void, which a
 * PL/pgSQL function really does return and which is not NULL.
 */
class PlpgsqlResidualClosuresTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TYPE plrz_ct AS (a int, b text)");
        exec("CREATE TYPE plrz_two AS (q1 bigint, q2 bigint)");
        exec("CREATE TYPE plrz_nested AS (c1 bigint, c2 plrz_two)");
        exec("CREATE TABLE plrz_t (id int primary key, n int, s varchar(10))");
        exec("INSERT INTO plrz_t VALUES (1,10,'aa'),(2,20,'bb')");
        exec("CREATE TABLE plrz_char (charcol char(4))");
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

    /** The first notice a DO block sent, which is what most of these measure. */
    private static String notice(String sql) throws SQLException {
        List<String> all = notices(sql);
        assertTrue(!all.isEmpty(), "no notice from: " + sql);
        return all.get(0);
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

    // ---- The FOR loop target is the variable the block declared ----

    @Test
    void aQueryLoopVariableIsTheDeclaredOneAndKeepsItsLastRow() throws Exception {
        assertEquals("2 20", notice("DO $$ declare r record; begin"
                + " for r in select * from plrz_t order by id loop null; end loop;"
                + " raise notice '% %', r.id, r.n; end $$"));
        assertEquals("2 20", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " for r in select * from plrz_t order by id loop null; end loop;"
                + " raise notice '% %', r.id, r.n; end $$"));
        // and after an EXIT, which is where the value the loop stopped on is wanted
        assertEquals("1", notice("DO $$ declare r record; begin"
                + " for r in select * from plrz_t order by id loop exit; end loop;"
                + " raise notice '%', r.id; end $$"));
        // a FOR over EXECUTE binds the same variable
        assertEquals("1 10", notice("DO $$ declare r record; begin"
                + " for r in execute 'select * from plrz_t where id = 1' loop null; end loop;"
                + " raise notice '% %', r.id, r.n; end $$"));
    }

    @Test
    void aQueryLoopThatSawNoRowLeavesTheQuerysShapeWithNullFields() throws Exception {
        assertEquals("<NULL>", notice("DO $$ declare r record; begin"
                + " for r in select * from plrz_t where false loop null; end loop;"
                + " raise notice '%', r.id; end $$"));
        // the shape is the query's, so a field it never had is still refused
        assertFails("42703", "record \"r\" has no field \"nosuch\"",
                "DO $$ declare r record; begin"
                + " for r in select * from plrz_t where false loop null; end loop;"
                + " raise notice '%', r.nosuch; end $$");
    }

    @Test
    void aScalarLoopTargetTakesTheColumnAndNotARow() throws Exception {
        // A single declared scalar is a list of one scalar, not a record target
        assertEquals("1 2", String.join(" ", notices(
                "DO $$ declare i int; begin for i in select id from plrz_t order by id"
                + " loop raise notice '%', i; end loop; end $$")));
        assertEquals("2", notice("DO $$ declare i int; begin"
                + " for i in select id from plrz_t order by id loop null; end loop;"
                + " raise notice '%', i; end $$"));
        // including a parameter, which is declared by the signature
        assertOk("CREATE FUNCTION plrz_ploop(p int) RETURNS int AS $x$ begin"
                + " for p in select id from plrz_t order by id loop null; end loop;"
                + " return p; end $x$ LANGUAGE plpgsql");
        assertEquals("2", scalar("SELECT plrz_ploop(9)"));
        exec("DROP FUNCTION plrz_ploop(int)");
    }

    @Test
    void anInnerBlocksOwnDeclarationStillShadowsTheOuterVariable() throws Exception {
        assertEquals("outer", notice("DO $$ declare r text := 'outer'; begin"
                + " declare r record; begin"
                + "   for r in select * from plrz_t loop null; end loop;"
                + " end;"
                + " raise notice '%', r; end $$"));
        // and a comma-separated list of scalars keeps working the way it did
        assertEquals("2 20", notice("DO $$ declare a int; b int; begin"
                + " for a, b in select id, n from plrz_t order by id loop null; end loop;"
                + " raise notice '% %', a, b; end $$"));
    }

    @Test
    void aLoopTargetNobodyDeclaredIsRefusedWhereTheBodyIsCompiled() throws Exception {
        assertFails("42601", "loop variable of loop over rows must be a record variable",
                "DO $$ begin for r in select id from plrz_t loop null; end loop; end $$");
        assertFails("42601", "\"a\" is not a known variable",
                "DO $$ begin for a, b in select id, n from plrz_t loop null; end loop; end $$");
        // FOR ... IN EXECUTE reports the same complaint under 42804, as PostgreSQL does
        assertFails("42804", "loop variable of loop over rows must be a record variable",
                "DO $$ begin for r in execute 'select 1' loop null; end loop; end $$");
        // and it is a compile-time check: a branch that never runs still refuses to be created
        assertFails("42601", "loop variable of loop over rows must be a record variable",
                "CREATE FUNCTION plrz_badloop() RETURNS void AS $x$ begin"
                + " if false then for r in select id from plrz_t loop null; end loop; end if;"
                + " end $x$ LANGUAGE plpgsql");
        // every declared shape is still accepted
        assertOk("DO $$ declare r record; begin"
                + " for r in select id from plrz_t loop null; end loop; end $$");
        assertOk("DO $$ declare r plrz_t%rowtype; begin"
                + " for r in select * from plrz_t loop null; end loop; end $$");
        assertOk("DO $$ declare i int; begin"
                + " for i in select id from plrz_t loop null; end loop; end $$");
        assertOk("DO $$ declare a int; b int; begin"
                + " for a, b in select id, n from plrz_t loop null; end loop; end $$");
        // an integer FOR defines its own variable and never needed a declaration
        assertOk("DO $$ begin for i in 1..3 loop null; end loop; end $$");
    }

    // ---- A whole row assigned to a row variable ----

    @Test
    void aWholeRowAssignedToARowVariableReachesItsFields() throws Exception {
        assertEquals("1 10 aa", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " r := (select x from plrz_t x where id = 1);"
                + " raise notice '% % %', r.id, r.n, r.s; end $$"));
        assertEquals("1 2 zz", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " r := row(1,2,'zz'); raise notice '% % %', r.id, r.n, r.s; end $$"));
        assertEquals("7 q", notice("DO $$ declare r plrz_ct; begin"
                + " r := row(7,'q'); raise notice '% %', r.a, r.b; end $$"));
        // one row variable copied into another keeps its fields
        assertEquals("aa", notice("DO $$ declare r plrz_t%rowtype; s plrz_t%rowtype; begin"
                + " select * into r from plrz_t where id = 1; s := r;"
                + " raise notice '%', s.s; end $$"));
        // and a row variable set to NULL reads NULL out of every field
        assertEquals("<NULL>", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " r := null; raise notice '%', r.s; end $$"));
    }

    @Test
    void selectIntoWithNoRowLeavesTheQuerysShape() throws Exception {
        assertEquals("<NULL>", notice("DO $$ declare r record; begin"
                + " select * into r from plrz_t where false; raise notice '%', r.id; end $$"));
        assertEquals("<NULL>", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " select * into r from plrz_t where false; raise notice '%', r.s; end $$"));
        // a scalar target is simply NULL, and FOUND is false either way
        assertEquals("<NULL> f", notice("DO $$ declare a int; begin"
                + " select id into a from plrz_t where false;"
                + " raise notice '% %', a, found; end $$"));
    }

    // ---- What a % writes ----

    @Test
    void aWholeRowWritesItselfInParenthesesLikeItsOutputFunction() throws Exception {
        assertEquals("(1,2)",
                notice("DO $$ declare r record; begin select 1,2 into r; raise notice '%', r; end $$"));
        assertEquals("(1,x)", notice("DO $$ declare r record; begin"
                + " select 1 as a, 'x' as b into r; raise notice '%', r; end $$"));
        assertEquals("(1,5,\"x y\")", notice("DO $$ declare r record; begin"
                + " select 1 as a, 5 as b, 'x y' as c into r; raise notice '%', r; end $$"));
        // a field that would otherwise be read as structure is quoted, and a NULL is empty
        assertEquals("(1,\"a,b\",\"q\"\"z\")", notice("DO $$ declare r record; begin"
                + " select 1 as a, 'a,b' as b, 'q\"z' as c into r; raise notice '%', r; end $$"));
        assertEquals("(1,\"x y\",)", notice("DO $$ declare r record; begin"
                + " select 1 as a, 'x y' as b, null::text as c into r; raise notice '%', r; end $$"));
    }

    @Test
    void twoColumnsOfOneNameAreTwoFieldsAndTheFirstIsTheOneReadBack() throws Exception {
        assertEquals("1", notice("DO $$ declare r record; begin"
                + " select 1 as a, 2 as a into r; raise notice '%', r.a; end $$"));
        assertEquals("(1,2)", notice("DO $$ declare r record; begin"
                + " select 1 as a, 2 as a into r; raise notice '%', r; end $$"));
    }

    @Test
    void aFloatWritesItselfTheWayFloat8outDoes() throws Exception {
        assertEquals("3", notice("DO $$ begin raise notice '%', 3.0::float8; end $$"));
        assertEquals("3.5", notice("DO $$ begin raise notice '%', 3.5::float8; end $$"));
        assertEquals("1e+20", notice("DO $$ begin raise notice '%', 1e20::float8; end $$"));
        assertEquals("3", notice("DO $$ begin raise notice '%', 3.0::float4; end $$"));
        // numeric keeps its own scale, which is not the same thing
        assertEquals("3.0", notice("DO $$ begin raise notice '%', 3.0::numeric; end $$"));
    }

    @Test
    void aDateOrTimeWritesItselfTheWayItsOutputFunctionDoes() throws Exception {
        assertEquals("2020-01-01 01:02:03",
                notice("DO $$ begin raise notice '%', '2020-01-01 01:02:03'::timestamp; end $$"));
        assertEquals("2020-01-01",
                notice("DO $$ begin raise notice '%', '2020-01-01'::date; end $$"));
        assertEquals("01:02:03",
                notice("DO $$ begin raise notice '%', '01:02:03'::time; end $$"));
        assertEquals("1 day", notice("DO $$ begin raise notice '%', '1 day'::interval; end $$"));
    }

    // ---- A declared length or precision reaches every element of an array ----

    @Test
    void anArrayDeclarationHoldsEveryElementToItsElementType() throws Exception {
        assertFails("22001", "value too long for type character varying(5)",
                "DO $$ declare v varchar(5)[]; begin v := ARRAY['abcdefgh']; end $$");
        assertFails("22003", "numeric field overflow",
                "DO $$ declare v numeric(4,2)[]; begin v := ARRAY[12345.1]; end $$");
        assertEquals("{1.24}", notice("DO $$ declare v numeric(4,2)[]; begin"
                + " v := ARRAY[1.239]; raise notice '%', v; end $$"));
        assertEquals("{\"ab \",\"c  \"}", notice("DO $$ declare v char(3)[]; begin"
                + " v := ARRAY['ab','c']; raise notice '%', v; end $$"));
        // and everything that fits, or carries no length at all, is left alone
        assertEquals("{abcde,x}", notice("DO $$ declare v varchar(5)[]; begin"
                + " v := ARRAY['abcde','x']; raise notice '%', v; end $$"));
        assertEquals("{NULL,ab}", notice("DO $$ declare v varchar(5)[]; begin"
                + " v := ARRAY[NULL, 'ab']; raise notice '%', v; end $$"));
        assertEquals("<NULL>", notice("DO $$ declare v varchar(5)[]; begin"
                + " v := NULL; raise notice '%', v; end $$"));
        assertEquals("{abcdefgh}", notice("DO $$ declare v text[]; begin"
                + " v := ARRAY['abcdefgh']; raise notice '%', v; end $$"));
        assertEquals("{{1,2},{3,4}}", notice("DO $$ declare v int[][]; begin"
                + " v := ARRAY[ARRAY[1,2],ARRAY[3,4]]; raise notice '%', v; end $$"));
    }

    // ---- A declared fractional-seconds precision rounds ----

    @Test
    void aDeclaredTemporalPrecisionRoundsRatherThanTruncates() throws Exception {
        assertEquals("2020-01-01 01:02:04", notice("DO $$ declare v timestamp(0); begin"
                + " v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$"));
        assertEquals("2020-01-01 01:02:03.99", notice("DO $$ declare v timestamp(2); begin"
                + " v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$"));
        assertEquals("01:02:04", notice("DO $$ declare v time(0); begin"
                + " v := '01:02:03.987'; raise notice '%', v; end $$"));
        assertEquals("1 day 00:00:02", notice("DO $$ declare v interval(0); begin"
                + " v := '1 day 00:00:01.987'; raise notice '%', v; end $$"));
        // the cast that spells the same precision rounds the same way
        assertEquals("2020-01-01 01:02:04",
                scalar("SELECT '2020-01-01 01:02:03.987'::timestamp(0)::text"));
        assertEquals("01:02:03.99", scalar("SELECT '01:02:03.987'::time(2)::text"));
        // a declaration with no precision keeps every digit
        assertEquals("2020-01-01 01:02:03.987", notice("DO $$ declare v timestamp; begin"
                + " v := '2020-01-01 01:02:03.987'; raise notice '%', v; end $$"));
        assertEquals("2020-01-01 01:02:03.987654", notice("DO $$ declare v timestamp(6); begin"
                + " v := '2020-01-01 01:02:03.987654'; raise notice '%', v; end $$"));
    }

    // ---- char(n) padding is dropped when the value is read as text ----

    @Test
    void aCharNLosesItsPaddingWhenItIsReadAsText() throws Exception {
        assertOk("CREATE FUNCTION plrz_ch() RETURNS text AS $x$"
                + " declare v plrz_char.charcol%TYPE := 'ab'; begin return v; end"
                + " $x$ LANGUAGE plpgsql");
        assertEquals("[ab]", scalar("SELECT '[' || plrz_ch() || ']'"));
        exec("DROP FUNCTION plrz_ch()");

        assertEquals("[ab]", notice("DO $$ declare v char(4) := 'ab'; w text; begin"
                + " w := v; raise notice '[%]', w; end $$"));
        assertEquals("[ab]", notice("DO $$ declare v char(4) := 'ab'; w varchar; begin"
                + " w := v; raise notice '[%]', w; end $$"));
        // the variable itself is still padded, which is what RAISE shows
        assertEquals("[ab  ]", notice("DO $$ declare v char(4) := 'ab'; begin"
                + " raise notice '[%]', v; end $$"));
        // and a value with blanks of its own keeps them
        assertEquals("[ab  ]", notice("DO $$ declare w text; begin"
                + " w := 'ab  '; raise notice '[%]', w; end $$"));
    }

    // ---- An aborted transaction ----

    @Test
    void anAbortedTransactionRefusesSavepointAndRelease() throws Exception {
        exec("BEGIN");
        assertEquals("22012", state("SELECT 1/0"));
        assertFails("25P02", "current transaction is aborted", "SAVEPOINT plrz_sp");
        assertFails("25P02", "current transaction is aborted", "RELEASE SAVEPOINT plrz_sp");
        // ROLLBACK TO is the one that is still allowed, because it is the way out
        assertEquals("3B001", state("ROLLBACK TO SAVEPOINT plrz_nosuch"));
        exec("ROLLBACK");
        assertEquals("1", scalar("SELECT 1"));
    }

    @Test
    void theOrdinarySavepointSequenceIsUnaffected() throws Exception {
        exec("CREATE TABLE plrz_sp_t (id int primary key)");
        exec("BEGIN");
        exec("SAVEPOINT a");
        exec("INSERT INTO plrz_sp_t VALUES (1)");
        exec("SAVEPOINT b");
        exec("INSERT INTO plrz_sp_t VALUES (2)");
        exec("ROLLBACK TO a");
        exec("INSERT INTO plrz_sp_t VALUES (3)");
        exec("RELEASE SAVEPOINT a");
        exec("COMMIT");
        assertEquals("3", scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM plrz_sp_t"));
        // and a savepoint taken before the error still recovers the transaction
        exec("BEGIN");
        exec("SAVEPOINT c");
        assertEquals("22012", state("SELECT 1/0"));
        exec("ROLLBACK TO SAVEPOINT c");
        exec("INSERT INTO plrz_sp_t VALUES (4)");
        exec("COMMIT");
        assertEquals("3,4", scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM plrz_sp_t"));
        exec("DROP TABLE plrz_sp_t");
    }

    // ---- FETCH … INTO takes one row ----

    @Test
    void aFetchIntoWithAMultiRowDirectionIsRefusedWhereTheBodyIsCompiled() throws Exception {
        assertFails("0A000", "FETCH statement cannot return multiple rows",
                "DO $$ declare c refcursor; a int; b int; begin"
                + " open c for select id, n from plrz_t; fetch forward all from c into a, b; end $$");
        // it is the direction and not the count's value that decides
        assertFails("0A000", "FETCH statement cannot return multiple rows",
                "DO $$ declare c refcursor; a int; b int; begin"
                + " open c for select id, n from plrz_t; fetch forward 1 from c into a, b; end $$");
        assertFails("0A000", "FETCH statement cannot return multiple rows",
                "DO $$ declare c refcursor; a int; b int; begin"
                + " open c for select id, n from plrz_t; fetch backward 1 from c into a, b; end $$");
        assertFails("0A000", "FETCH statement cannot return multiple rows",
                "DO $$ declare c refcursor; a int; b int; begin"
                + " open c for select id, n from plrz_t; fetch all from c into a, b; end $$");
        // and it is a compile-time check, so an unreachable branch refuses to be created
        assertFails("0A000", "FETCH statement cannot return multiple rows",
                "CREATE FUNCTION plrz_badfetch() RETURNS void AS $x$"
                + " declare c refcursor; a int; b int; begin"
                + " if false then fetch forward all from c into a, b; end if; end"
                + " $x$ LANGUAGE plpgsql");
    }

    @Test
    void theSingleRowFetchDirectionsAndMoveAreUnaffected() throws Exception {
        assertEquals("1 aa", notice("DO $$ declare c refcursor; a int; b text; begin"
                + " open c for select id, s from plrz_t order by id;"
                + " fetch from c into a, b; raise notice '% %', a, b; end $$"));
        assertEquals("2 bb", notice("DO $$ declare c refcursor; a int; b text; begin"
                + " open c for select id, s from plrz_t order by id;"
                + " fetch last from c into a, b; raise notice '% %', a, b; end $$"));
        assertEquals("2 bb", notice("DO $$ declare c refcursor; a int; b text; begin"
                + " open c for select id, s from plrz_t order by id;"
                + " fetch absolute 2 from c into a, b; raise notice '% %', a, b; end $$"));
        assertEquals("1 aa", notice("DO $$ declare c refcursor; a int; b text; begin"
                + " open c for select id, s from plrz_t order by id;"
                + " fetch relative 1 from c into a, b; raise notice '% %', a, b; end $$"));
        assertEquals("1 aa", notice("DO $$ declare c refcursor; a int; b text; begin"
                + " open c for select id, s from plrz_t order by id;"
                + " fetch forward from c into a, b; raise notice '% %', a, b; end $$"));
        // MOVE takes no INTO, so no count of its own is ever too many
        assertEquals("ok", notice("DO $$ declare c refcursor; begin"
                + " open c for select id from plrz_t; move forward all in c;"
                + " raise notice 'ok'; end $$"));
    }

    // ---- A DROP naming a schema that is not there ----

    @Test
    void aDropNamingAnAbsentSchemaReportsTheSchema() throws Exception {
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "DROP TABLE plrz_nosuch.t");
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "DROP VIEW plrz_nosuch.v");
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "DROP SEQUENCE plrz_nosuch.s");
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "DROP INDEX plrz_nosuch.i");
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "DROP TYPE plrz_nosuch.ty");
        assertFails("3F000", "schema \"plrz_nosuch\" does not exist",
                "TRUNCATE plrz_nosuch.t");
        // IF EXISTS skips the schema by name and says so
        assertEquals("schema \"plrz_nosuch\" does not exist, skipping",
                notices("DROP TABLE IF EXISTS plrz_nosuch.t").get(0));
        assertEquals("schema \"plrz_nosuch\" does not exist, skipping",
                notices("DROP VIEW IF EXISTS plrz_nosuch.v").get(0));
        assertEquals("schema \"plrz_nosuch\" does not exist, skipping",
                notices("DROP SCHEMA IF EXISTS plrz_nosuch").get(0));
    }

    @Test
    void aSchemaThatIsThereStillReportsTheObject() throws Exception {
        assertFails("42P01", "table \"plrz_nosuchtable\" does not exist",
                "DROP TABLE public.plrz_nosuchtable");
        assertEquals("table \"plrz_nosuchtable\" does not exist, skipping",
                notices("DROP TABLE IF EXISTS public.plrz_nosuchtable").get(0));
        // a query names a relation rather than a schema, and that reading is unchanged
        assertEquals("42P01", state("SELECT * FROM plrz_nosuch.t"));
        // and an ordinary schema-qualified DROP still drops
        exec("CREATE SCHEMA plrz_s1");
        exec("CREATE TABLE plrz_s1.t (id int primary key)");
        exec("CREATE VIEW plrz_s1.v AS SELECT 1 AS a");
        exec("TRUNCATE plrz_s1.t");
        exec("DROP VIEW plrz_s1.v");
        exec("DROP TABLE plrz_s1.t");
        exec("DROP SCHEMA plrz_s1");
    }

    // ---- A field path of three parts ----

    @Test
    void aThreePartFieldReadIsNotAFieldPathAtAll() throws Exception {
        // PL/pgSQL substitutes names of one and two parts; the third part leaves the expression
        // to the SQL parser, which reads c.c2 as a relation it cannot find
        assertFails("42P01", "missing FROM-clause entry for table \"c2\"",
                "DO $$ declare c plrz_nested; begin raise notice '%', c.c2.q1; end $$");
        // the write is a target rather than an expression, and PostgreSQL does resolve it
        assertOk("DO $$ declare c plrz_nested; begin c.c2.q1 := 5; raise notice 'ok'; end $$");
        assertEquals("5", notice("DO $$ declare c plrz_nested; begin"
                + " c.c2.q1 := 5; raise notice '%', (c.c2).q1; end $$"));
        // and every ordinary two-part read is untouched
        assertEquals("<NULL>",
                notice("DO $$ declare c plrz_nested; begin raise notice '%', c.c1; end $$"));
        assertEquals("15.0", notice("DO $$ declare r plrz_t%rowtype; begin"
                + " select * into r from plrz_t where id = 1; raise notice '%', r.n * 1.5; end $$"));
    }

    // ---- The value of type void ----

    @Test
    void aPlpgsqlVoidFunctionReturnsTheVoidValueWhichIsNotNull() throws Exception {
        exec("CREATE FUNCTION plrz_vf() RETURNS void LANGUAGE plpgsql AS $x$ begin null; end $x$");
        assertEquals("f", scalar("SELECT plrz_vf() IS NULL"));
        assertEquals("[]", scalar("SELECT '[' || plrz_vf()::text || ']'"));
        exec("CREATE FUNCTION plrz_vf2() RETURNS void LANGUAGE plpgsql AS $x$ begin return; end $x$");
        assertEquals("f", scalar("SELECT plrz_vf2() IS NULL"));
        // a LANGUAGE SQL function of the same signature really does yield NULL
        exec("CREATE FUNCTION plrz_vf3() RETURNS void LANGUAGE sql AS $x$ SELECT 1 $x$");
        assertEquals("t", scalar("SELECT plrz_vf3() IS NULL"));
        exec("DROP FUNCTION plrz_vf()");
        exec("DROP FUNCTION plrz_vf2()");
        exec("DROP FUNCTION plrz_vf3()");
    }
}
