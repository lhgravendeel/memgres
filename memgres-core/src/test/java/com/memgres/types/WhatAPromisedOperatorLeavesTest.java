package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What naming an operator that is not there yet leaves behind.
 *
 * <p>A NEGATOR or COMMUTATOR naming an operator nobody has created is a promise that somebody
 * will, and PostgreSQL files a shell for it: a row with a name, the operand types the reference
 * implies and no function behind it, with the two operators pointing at each other. A commutator
 * takes the same operands the other way round, so its shell is declared with the types reversed.
 *
 * <p>Left unfiled, the name reached nothing at all -- the operator that named it pointed at
 * nothing, and a reader following oprnegate found no row to follow it to.
 */
class WhatAPromisedOperatorLeavesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE FUNCTION zwp_eq(int,int) RETURNS bool LANGUAGE sql AS $$ SELECT $1 = $2 $$");
        exec("CREATE FUNCTION zwp_lt(int,text) RETURNS bool LANGUAGE sql AS $$ SELECT true $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP FUNCTION zwp_eq(int,int)");
            exec("DROP FUNCTION zwp_lt(int,text)");
            conn.close();
        }
        if (memgres != null) memgres.close();
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

    /** A negator's shell takes the same operands, and the two point at each other. */
    @Test
    void whatANegatorLeaves() throws SQLException {
        exec("CREATE OPERATOR ###= (LEFTARG=int, RIGHTARG=int, FUNCTION=zwp_eq, NEGATOR = ###!)");
        assertEquals(java.util.Arrays.asList(
                        "###!|integer|integer|-|###=|-",
                        "###=|integer|integer|zwp_eq|###!|boolean"),
                operators("'###=','###!'"));
        // A shell has nothing to run, which is a different complaint from having no operator.
        assertEquals("42883", stateOf("SELECT 1 ###! 2"));
        // Dropping the shell leaves the operator that named it pointing at nothing.
        exec("DROP OPERATOR ###! (int, int)");
        assertEquals(java.util.Arrays.asList("###=|integer|integer|zwp_eq|0|boolean"),
                operators("'###=','###!'"));
        exec("DROP OPERATOR ###= (int, int)");
    }

    /** A commutator's shell takes the operands the other way round. */
    @Test
    void whatACommutatorLeaves() throws SQLException {
        exec("CREATE OPERATOR ###< (LEFTARG=int, RIGHTARG=text, FUNCTION=zwp_lt, COMMUTATOR = ###>)");
        assertEquals(java.util.Arrays.asList(
                        "###<|integer|text|zwp_lt|###>|boolean",
                        "###>|text|integer|-|###<|-"),
                commutators("'###<','###>'"));
        exec("DROP OPERATOR ###< (int, text)");
        exec("DROP OPERATOR ###> (text, int)");
    }

    private static List<String> operators(String names) throws SQLException {
        return rows("SELECT oprname, oprleft::regtype::text, oprright::regtype::text,"
                + " oprcode::text, oprnegate::regoper::text, oprresult::regtype::text"
                + " FROM pg_operator WHERE oprname IN (" + names + ") ORDER BY oprname");
    }

    private static List<String> commutators(String names) throws SQLException {
        return rows("SELECT oprname, oprleft::regtype::text, oprright::regtype::text,"
                + " oprcode::text, oprcom::regoper::text, oprresult::regtype::text"
                + " FROM pg_operator WHERE oprname IN (" + names + ") ORDER BY oprname");
    }
}
