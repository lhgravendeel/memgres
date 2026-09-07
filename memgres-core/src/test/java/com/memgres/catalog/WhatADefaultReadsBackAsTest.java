package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a column default reads back as, and when a parameter's default is evaluated.
 *
 * <p>PostgreSQL never echoes the text a default was written as: it prints the tree parse analysis
 * left behind. A CASE is printed over several lines, an element taken out of something is written
 * with the something in parentheses, and a value written out inside either carries the type the
 * column settled on. Handled by neither the writer nor the reader, a CASE default and a subscript
 * default were recorded as the word "null", so the catalogue said the column had no default at
 * all -- for a column that plainly had one.
 *
 * <p>A parameter's default is an expression the call evaluates, and what goes wrong while
 * evaluating it is what the call raises. Swallowed and answered with null, a default of 1/0 handed
 * the body a null where PostgreSQL says division by zero -- but only on a call written with named
 * arguments, so the same function behaved differently depending on how it was called.
 */
class WhatADefaultReadsBackAsTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String defaultOf(String column) throws SQLException {
        return one("SELECT coalesce(column_default,'<null>') FROM information_schema.columns"
                + " WHERE table_name='zwf_t' AND column_name='" + column + "'");
    }

    /** A default reads back as the tree, not as the text it was written as. */
    @Test
    void whatTheCatalogueSaysADefaultIs() throws SQLException {
        exec("CREATE TABLE zwf_t (a text DEFAULT CASE WHEN true THEN 'x' ELSE 'y' END,"
                + " b text DEFAULT COALESCE(NULL,'c'),"
                + " c int DEFAULT (ARRAY[7,8,9])[2],"
                + " d int DEFAULT GREATEST(1,2),"
                + " e text DEFAULT NULLIF('a','b'))");
        assertEquals("\nCASE\n    WHEN true THEN 'x'::text\n    ELSE 'y'::text\nEND",
                defaultOf("a"));
        assertEquals("COALESCE(NULL::text, 'c'::text)", defaultOf("b"));
        assertEquals("(ARRAY[7, 8, 9])[2]", defaultOf("c"));
        assertEquals("GREATEST(1, 2)", defaultOf("d"));
        assertEquals("NULLIF('a'::text, 'b'::text)", defaultOf("e"));
        // pg_attrdef says the same thing, because it is what information_schema reads.
        assertEquals("(ARRAY[7, 8, 9])[2]",
                one("SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef d"
                        + " JOIN pg_class c ON c.oid=d.adrelid"
                        + " WHERE c.relname='zwf_t' AND d.adnum=3"));
        exec("DROP TABLE zwf_t");
    }

    /** A default that will not evaluate is what the call raises, however it was written. */
    @Test
    void whenAParametersDefaultIsEvaluated() throws SQLException {
        exec("CREATE FUNCTION zwf_d(a int, b int DEFAULT 1/0) RETURNS int LANGUAGE sql"
                + " AS $$ SELECT $1 $$");
        assertEquals("22012", stateOf("SELECT zwf_d(a => 5)"));
        assertEquals("22012", stateOf("SELECT zwf_d(5)"));
        // A call that supplies the argument never evaluates the default at all.
        assertEquals("5", one("SELECT zwf_d(5, 2)::text"));
        exec("DROP FUNCTION zwf_d(int,int)");
        // A default that does evaluate is used, whichever way the call was written.
        exec("CREATE FUNCTION zwf_e(a int, b int DEFAULT 7) RETURNS int LANGUAGE sql"
                + " AS $$ SELECT $1 + $2 $$");
        assertEquals("12", one("SELECT zwf_e(5)::text"));
        assertEquals("12", one("SELECT zwf_e(a => 5)::text"));
        exec("DROP FUNCTION zwf_e(int,int)");
    }

    /** The value the default produces is still the value it always produced. */
    @Test
    void whatADefaultStillProduces() throws SQLException {
        exec("CREATE TABLE zwf_v (a text DEFAULT CASE WHEN true THEN 'x' ELSE 'y' END,"
                + " c int DEFAULT (ARRAY[7,8,9])[2])");
        exec("INSERT INTO zwf_v DEFAULT VALUES");
        assertEquals("x|8", one("SELECT a || '|' || c::text FROM zwf_v"));
        exec("DROP TABLE zwf_v");
    }
}
