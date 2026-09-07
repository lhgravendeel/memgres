package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a row variable holds, and how a row reaches a set.
 *
 * <p>A row is spread over the variable's own field names, whether it was assigned later or given as
 * the declaration's initialiser: stored whole, every field of a variable declared
 * {@code := ROW(...)} read back as nothing. A variable declared {@code record} takes its shape from
 * whatever is assigned to it, and a row written without a type of its own names its fields f1, f2
 * and so on.
 *
 * <p>{@code r.*} is every field of the record, written out in order, which is how a whole row
 * variable is handed to an INSERT; read as a field called "*", it was reported as a field the record
 * has not got.
 *
 * <p>And a set of a composite type is a set of rows of that type, so a row handed to RETURN NEXT
 * joins it as its fields rather than going whole into the first column.
 */
class WhatARowVariableHoldsTest {

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

    /** A bare row assigned to a record names its fields f1, f2 and so on. */
    @Test
    void whatABareRowNamesItsFields() throws SQLException {
        exec("CREATE FUNCTION zwr_k12() RETURNS text AS"
                + " $$ declare r record; begin r := row(1,2); return r.f1::text; end $$"
                + " LANGUAGE plpgsql");
        assertEquals("1", one("SELECT zwr_k12()"));
        exec("DROP FUNCTION zwr_k12()");
    }

    /** A declaration's own initialiser is spread over the fields like any other assignment. */
    @Test
    void whatADeclarationsInitialiserBecomes() throws SQLException {
        exec("CREATE TYPE zwr_nc AS (a int, b text)");
        exec("CREATE TABLE zwr_nt (id int, nm text)");
        exec("CREATE FUNCTION zwr_k2() RETURNS text AS"
                + " $$ declare v zwr_nc := row(7,'q'); begin return v.a::text; end $$"
                + " LANGUAGE plpgsql");
        assertEquals("7", one("SELECT zwr_k2()"));
        exec("CREATE FUNCTION zwr_rowty() RETURNS text AS"
                + " $$ declare v zwr_nt%rowtype := row(3,'x'); begin return v.nm; end $$"
                + " LANGUAGE plpgsql");
        assertEquals("x", one("SELECT zwr_rowty()"));
        exec("DROP FUNCTION zwr_rowty()");
        exec("DROP FUNCTION zwr_k2()");
        exec("DROP TABLE zwr_nt");
        exec("DROP TYPE zwr_nc");
    }

    /** A star after a record is every field of it, in order. */
    @Test
    void whatAStarAfterARecordIs() throws SQLException {
        exec("CREATE TABLE zwr_t (a int, b text)");
        exec("INSERT INTO zwr_t VALUES (1,'x')");
        exec("CREATE FUNCTION zwr_k22() RETURNS text AS"
                + " $$ declare r zwr_t%rowtype;"
                + " begin select * into r from zwr_t limit 1;"
                + " insert into zwr_t values (r.*);"
                + " return (select count(*)::text from zwr_t); end $$ LANGUAGE plpgsql");
        assertEquals("2", one("SELECT zwr_k22()"));
        exec("DROP FUNCTION zwr_k22()");
        exec("DROP TABLE zwr_t");
    }

    /** A row handed to RETURN NEXT joins the set as the composite's own columns. */
    @Test
    void howARowJoinsASetOfComposites() throws SQLException {
        exec("CREATE TYPE zwr_sc AS (a int, b text)");
        exec("CREATE FUNCTION zwr_k14() RETURNS SETOF zwr_sc AS"
                + " $$ begin return next row(1,'a')::zwr_sc; end $$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM zwr_k14()")) {
            assertEquals(2, rs.getMetaData().getColumnCount());
            assertEquals("a", rs.getMetaData().getColumnName(1));
            assertEquals("b", rs.getMetaData().getColumnName(2));
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals("a", rs.getString(2));
        }
        // The other two ways of answering with a composite still answer the same way.
        exec("CREATE FUNCTION zwr_one() RETURNS zwr_sc AS"
                + " $$ begin return row(2,'b')::zwr_sc; end $$ LANGUAGE plpgsql");
        assertEquals("2", one("SELECT a::text FROM zwr_one()"));
        exec("CREATE FUNCTION zwr_q() RETURNS SETOF zwr_sc AS"
                + " $$ begin return query select 3, 'c'; end $$ LANGUAGE plpgsql");
        assertEquals("3", one("SELECT a::text FROM zwr_q()"));
        exec("DROP FUNCTION zwr_q()");
        exec("DROP FUNCTION zwr_one()");
        exec("DROP FUNCTION zwr_k14()");
        exec("DROP TYPE zwr_sc");
    }
}
