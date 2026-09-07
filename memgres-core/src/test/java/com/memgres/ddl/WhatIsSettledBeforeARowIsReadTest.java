package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a statement settles while it is read, and what the catalogue says afterwards.
 *
 * <p>An assignment is held to the column's type while the statement is read, not as each row is
 * written: judged the other way round, the same UPDATE was refused over a relation holding rows and
 * accepted over an empty one. A modifier is an integer, so a word standing where one belongs is
 * input the integer reader cannot read. And a column with no type is where PostgreSQL's grammar
 * runs out of input.
 *
 * <p>An operator is defined by the routine that performs it, so the routine cannot be dropped while
 * the operator is there to call it.
 *
 * <p>A policy's roles are held as their numbers, with PUBLIC as zero; a routine is one catalogue row
 * per signature; and a column points at its collation's own pinned number.
 */
class WhatIsSettledBeforeARowIsReadTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** An assignment is judged while the statement is read, not once a row is found. */
    @Test
    void whenAnAssignmentIsJudged() throws SQLException {
        exec("CREATE TABLE zwi_as (i int)");
        assertEquals("42804", stateOf("UPDATE zwi_as SET i = true"));
        assertTrue(messageOf("UPDATE zwi_as SET i = true")
                .contains("column \"i\" is of type integer but expression is of type boolean"));
        exec("INSERT INTO zwi_as VALUES (1)");
        assertEquals("42804", stateOf("UPDATE zwi_as SET i = true"));
        // What the column can take is still taken.
        assertNull(stateOf("UPDATE zwi_as SET i = 2"));
        assertEquals("2", one("SELECT i::text FROM zwi_as"));
        exec("DROP TABLE zwi_as");
    }

    /** A type modifier is an integer, and a column has to have a type at all. */
    @Test
    void whatADeclarationMustSay() {
        assertEquals("22P02", stateOf("CREATE FUNCTION zwi_numbad() RETURNS numeric AS"
                + " $$ DECLARE v numeric(5,abc) := 1; BEGIN RETURN v; END $$ LANGUAGE plpgsql"));
        assertTrue(messageOf("CREATE TABLE zwi_bad (a numeric(5,abc))")
                .contains("invalid input syntax for type integer: \"abc\""));
        assertEquals("42601", stateOf("CREATE TABLE zwi_t (a)"));
        assertTrue(messageOf("CREATE TABLE zwi_t (a)").contains("syntax error at end of input"));
        // A later column with no type is the token that could not follow.
        assertTrue(messageOf("CREATE TABLE zwi_u (a int, b)")
                .contains("syntax error at or near \")\""));
    }

    /** A routine an operator calls is not the definer's alone to drop. */
    @Test
    void whatAnOperatorKeepsAlive() throws SQLException {
        exec("CREATE FUNCTION zwi_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1+$2 $$");
        exec("CREATE OPERATOR ###! (LEFTARG=int, RIGHTARG=int, FUNCTION = zwi_add)");
        assertEquals("2BP01", stateOf("DROP FUNCTION zwi_add(int, int)"));
        assertTrue(messageOf("DROP FUNCTION zwi_add(int, int)")
                .contains("cannot drop function zwi_add(integer,integer)"
                        + " because other objects depend on it"));
        // CASCADE takes the operator along.
        assertNull(stateOf("DROP FUNCTION zwi_add(int, int) CASCADE"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_operator WHERE oprname='###!'"));
    }

    /** A raise carries the code it was given, unless that code says nothing went wrong. */
    @Test
    void whichCodeARaiseCarries() {
        assertEquals("P0001",
                stateOf("DO $$ BEGIN RAISE EXCEPTION 'x' USING ERRCODE = '00000'; END $$"));
        assertEquals("22012",
                stateOf("DO $$ BEGIN RAISE EXCEPTION 'x' USING ERRCODE = '22012'; END $$"));
        assertEquals("22012", stateOf("DO $$ BEGIN RAISE EXCEPTION 'x'"
                + " USING ERRCODE = 'division_by_zero'; END $$"));
        assertEquals("ZZ999",
                stateOf("DO $$ BEGIN RAISE EXCEPTION 'x' USING ERRCODE = 'ZZ999'; END $$"));
    }

    /** The catalogue holds a policy's roles as numbers and a routine per signature. */
    @Test
    void whatTheCatalogueHolds() throws SQLException {
        exec("CREATE TABLE zwi_pt (i int)");
        exec("CREATE ROLE zwi_r");
        exec("CREATE POLICY zwi_pp ON zwi_pt USING (true)");
        assertEquals("{0}|oid[]", one("SELECT polroles::text || '|' || pg_typeof(polroles)::text"
                + " FROM pg_policy WHERE polname='zwi_pp'"));
        assertEquals("{public}",
                one("SELECT roles::text FROM pg_policies WHERE policyname='zwi_pp'"));
        exec("DROP TABLE zwi_pt");
        exec("DROP ROLE zwi_r");
        // Two overloads are two routines.
        exec("CREATE FUNCTION zwi_f(int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        exec("CREATE FUNCTION zwi_f(text) RETURNS int LANGUAGE sql AS $$ SELECT 2 $$");
        assertEquals("2", one("SELECT count(*)::text FROM information_schema.routines"
                + " WHERE routine_name='zwi_f'"));
        exec("DROP FUNCTION zwi_f(int)");
        assertEquals("1", one("SELECT count(*)::text FROM information_schema.routines"
                + " WHERE routine_name='zwi_f'"));
        exec("DROP FUNCTION zwi_f(text)");
    }

    /** A column points at its collation's own number, and a collation may name its schema. */
    @Test
    void whichCollationAColumnPointsAt() throws SQLException {
        exec("CREATE TABLE zwi_c (i int, s text, n text COLLATE \"C\")");
        assertEquals(java.util.Arrays.asList("i|0", "s|100", "n|950"),
                rows("SELECT attname, attcollation::text FROM pg_attribute"
                        + " WHERE attrelid='zwi_c'::regclass AND attnum>0 ORDER BY attnum"));
        assertEquals("C", one("SELECT (SELECT collname FROM pg_collation WHERE oid=a.attcollation)"
                + " FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid"
                + " WHERE c.relname='zwi_c' AND a.attname='n'"));
        exec("DROP TABLE zwi_c");
        // A collation created in a schema of its own may be written with it.
        exec("CREATE SCHEMA zwi_ts");
        exec("CREATE COLLATION zwi_ts.zwi_co (LOCALE = 'C')");
        assertEquals("a", one("SELECT 'a' COLLATE zwi_ts.zwi_co"));
        exec("DROP SCHEMA zwi_ts CASCADE");
    }
}
