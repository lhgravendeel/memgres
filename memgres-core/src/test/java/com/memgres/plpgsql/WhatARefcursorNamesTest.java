package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a refcursor names, how far ALL moves, and what a view's check option is set to.
 *
 * <p>A refcursor variable may hold nothing but a portal name -- that is what one routine hands
 * another, and what makes returning a refcursor worth doing. Read only as a cursor the body had
 * opened itself, a portal opened elsewhere looked like an unopened variable, so the routine that
 * was handed one could not read from it at all. The position belongs to the portal, not to the
 * variable naming it, so two routines fetching from one refcursor take successive rows.
 *
 * <p>ALL is a count, not a number: it means every remaining row, in whichever direction was
 * written. Read as an expression it evaluated to nothing and the count fell back to one, so
 * {@code MOVE ALL} went a single row forward and the fetch after it read the second row where
 * PostgreSQL reads the last.
 *
 * <p>And {@code check_option} is not an ordinary storage option: it is what the view promises
 * about the rows written through it. Merged into the storage options alone, ALTER VIEW SET
 * (check_option='local') changed nothing a reader could see, and RESET was not read at all.
 */
class WhatARefcursorNamesTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** ALL moves over every remaining row, in the direction that was written. */
    @Test
    void howFarAllMoves() throws SQLException {
        exec("CREATE TABLE zwf_z (id int)");
        exec("INSERT INTO zwf_z SELECT g FROM generate_series(1,6) g");
        exec("CREATE FUNCTION zwf_last() RETURNS text AS $$"
                + " declare c scroll cursor for select id from zwf_z order by id; v int;"
                + " begin open c; move all from c; fetch backward from c into v;"
                + " close c; return v::text; end $$ LANGUAGE plpgsql");
        assertEquals("6", one("SELECT zwf_last()"));
        exec("DROP FUNCTION zwf_last()");
        // And backward all lands before the first row, so the next fetch reads the first.
        exec("CREATE FUNCTION zwf_first() RETURNS text AS $$"
                + " declare c scroll cursor for select id from zwf_z order by id; v int;"
                + " begin open c; move all from c; move backward all from c;"
                + " fetch from c into v; close c; return v::text; end $$ LANGUAGE plpgsql");
        assertEquals("1", one("SELECT zwf_first()"));
        exec("DROP FUNCTION zwf_first()");
        exec("DROP TABLE zwf_z");
    }

    /** A portal one routine opened is one another can read, and they share its place. */
    @Test
    void whatARoutineCanBeHanded() throws SQLException {
        exec("CREATE TABLE zwf_t (id int)");
        exec("INSERT INTO zwf_t SELECT g FROM generate_series(1,4) g");
        exec("CREATE FUNCTION zwf_open() RETURNS refcursor AS $$"
                + " declare c refcursor := 'zwf_shared';"
                + " begin open c for select id from zwf_t order by id; return c; end $$"
                + " LANGUAGE plpgsql");
        exec("CREATE FUNCTION zwf_read(c refcursor) RETURNS text AS $$"
                + " declare v int; s text := '';"
                + " begin fetch c into v; s := v::text; fetch c into v;"
                + " return s || ',' || v; end $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION zwf_both() RETURNS text AS $$"
                + " declare c refcursor; begin c := zwf_open(); return zwf_read(c); end $$"
                + " LANGUAGE plpgsql");
        // Two fetches through the handed-on name take the first two rows in order.
        assertEquals("1,2", one("SELECT zwf_both()"));
        exec("DROP FUNCTION zwf_both()");
        exec("DROP FUNCTION zwf_read(refcursor)");
        exec("DROP FUNCTION zwf_open()");
        exec("DROP TABLE zwf_t");
    }

    /** A view's check option is what SET and RESET change. */
    @Test
    void whatSetsAViewsCheckOption() throws SQLException {
        exec("CREATE TABLE zwf_v (i int, v text)");
        exec("CREATE VIEW zwf_view AS SELECT i, v FROM zwf_v WHERE i < 10 WITH CHECK OPTION");
        assertEquals("CASCADED", checkOption());
        exec("ALTER VIEW zwf_view SET (check_option='local')");
        assertEquals("LOCAL", checkOption());
        exec("ALTER VIEW zwf_view SET (check_option='cascaded')");
        assertEquals("CASCADED", checkOption());
        exec("ALTER VIEW zwf_view RESET (check_option)");
        assertEquals("NONE", checkOption());
        exec("DROP VIEW zwf_view");
        exec("DROP TABLE zwf_v");
    }

    private static String checkOption() throws SQLException {
        return one("SELECT check_option FROM information_schema.views"
                + " WHERE table_name='zwf_view'");
    }
}
