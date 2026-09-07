package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which operands an operator somebody declared reaches.
 *
 * <p>An operator a user creates is an operator. Checked against the list PostgreSQL ships and
 * nothing else, a {@code #} declared for two texts was reported as not existing before anything
 * ever looked for it -- and where the check did look, an operator that answered NULL was taken for
 * one that was not there, so a function returning nothing became "operator does not exist".
 */
class WhatAnOperatorSomebodyDeclaredReachesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE FUNCTION zwo_nothing(text,text) RETURNS text"
                + " LANGUAGE sql AS $$ SELECT NULL::text $$");
        exec("CREATE FUNCTION zwo_join(text,text) RETURNS text"
                + " LANGUAGE sql AS $$ SELECT $1 || $2 $$");
        exec("CREATE OPERATOR # (leftarg = text, rightarg = text, function = zwo_nothing)");
        exec("CREATE OPERATOR ## (leftarg = text, rightarg = text, function = zwo_join)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP OPERATOR # (text, text)");
            exec("DROP OPERATOR ## (text, text)");
            exec("DROP FUNCTION zwo_nothing(text,text)");
            exec("DROP FUNCTION zwo_join(text,text)");
            conn.close();
        }
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

    /** A spelling PostgreSQL ships for other types still reaches the one declared here. */
    @Test
    void whichOperatorTheOperandsReach() throws SQLException {
        assertEquals("true", one("SELECT (('a'::text # 'b'::text) IS NULL)::text"));
        assertEquals("ab", one("SELECT 'a'::text ## 'b'::text"));
        // The shipped meanings of the same spellings are untouched.
        assertEquals("3", one("SELECT (1 # 2)::text"));
        // And types nobody declared it for still have no such operator.
        assertEquals("42883", stateOf("SELECT 'a'::text # 1"));
    }
}
