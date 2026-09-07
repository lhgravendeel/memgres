package com.memgres.aggregate;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an aggregate over no rows is still held to.
 *
 * <p>A group with no rows in it has no row to read a type off, but the relation it came from still
 * has columns. An aggregate whose argument does not fit the one it declares is a call that names
 * no aggregate, and PostgreSQL says so whether or not the qualification kept anything: the same
 * call answered where a row survived and came back empty-handed where none did, so the same
 * statement was an error or an answer depending on the data.
 */
class WhatAnAggregateOverNoRowsChecksTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE FUNCTION zwa_cat(text,text) RETURNS text LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT COALESCE($1,'') || COALESCE($2,'') $$");
        exec("CREATE AGGREGATE zwa_agg (text) (SFUNC = zwa_cat, STYPE = text, INITCOND = '')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP AGGREGATE zwa_agg(text)");
            exec("DROP FUNCTION zwa_cat(text,text)");
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

    /** An argument that does not fit is refused with rows and without them alike. */
    @Test
    void whatAnAggregateIsHeldTo() throws SQLException {
        // With a row, the mismatch was already reported.
        assertEquals("42883", stateOf("SELECT zwa_agg(v) FROM (VALUES (1),(2)) t(v)"));
        // And with none, because the relation still says what its column is.
        assertEquals("42883", stateOf("SELECT zwa_agg(v) FROM (VALUES (1)) t(v) WHERE v > 99"));
        // The same over a table whose rows a qualification removed.
        exec("CREATE TABLE zwa_t (n int, s text)");
        exec("INSERT INTO zwa_t VALUES (1,'a')");
        assertEquals("42883", stateOf("SELECT zwa_agg(n) FROM zwa_t WHERE n > 99"));
        // An argument that does fit is aggregated, over rows and over none.
        assertEquals("a", one("SELECT zwa_agg(s) FROM zwa_t"));
        assertEquals("", one("SELECT zwa_agg(s) FROM zwa_t WHERE n > 99"));
        exec("DROP TABLE zwa_t");
    }
}
