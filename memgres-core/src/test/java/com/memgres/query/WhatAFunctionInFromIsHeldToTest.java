package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a function written in FROM is held to when there is nothing to read.
 *
 * <p>A function in FROM runs once per row of what stands to its left, so with no rows it never
 * runs -- and a column name in its arguments was never looked for. A query reading a column no
 * relation has answered with an empty result instead of saying the column does not exist, which
 * made the same statement an error or an answer depending on whether the table happened to be
 * empty. What names are in scope is known from the FROM items alone.
 *
 * <p>And a near miss in an enclosing scope is still worth suggesting: the suggestion was thrown
 * away with the refusal it arrived on, so the complaint came out bare.
 */
class WhatAFunctionInFromIsHeldToTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TYPE zwf_c AS (a int, b text)");
        exec("CREATE TABLE zwf_t (id int, c zwf_c)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwf_t");
            exec("DROP TYPE zwf_c");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static PSQLException refusalOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return (PSQLException) e;
        }
    }

    /** Empty or not, the argument's names have to be names something answers to. */
    @Test
    void whichNamesAFunctionInFromMayRead() throws SQLException {
        assertUnknownColumn("SELECT id, u.a FROM zwf_t, unnest(cs) AS u", "zwf_t.c");
        assertUnknownColumn("SELECT * FROM zwf_t, unnest(nope) AS u", null);
        // The same once there is a row to read, which is where it was already refused.
        exec("INSERT INTO zwf_t VALUES (1, ROW(1,'x')::zwf_c)");
        assertUnknownColumn("SELECT id, u.a FROM zwf_t, unnest(cs) AS u", "zwf_t.c");
        // A name that is there is read as before.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM zwf_t,"
                     + " generate_series(1, id) AS g")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        exec("DELETE FROM zwf_t");
    }

    private static void assertUnknownColumn(String sql, String suggested) {
        PSQLException refusal = refusalOf(sql);
        assertNotNull(refusal, sql);
        assertEquals("42703", refusal.getSQLState(), sql);
        String hint = refusal.getServerErrorMessage().getHint();
        if (suggested == null) {
            assertNull(hint, sql);
        } else {
            assertEquals("Perhaps you meant to reference the column \"" + suggested + "\".",
                    hint, sql);
        }
    }
}
