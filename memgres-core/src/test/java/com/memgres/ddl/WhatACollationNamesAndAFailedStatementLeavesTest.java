package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a collation a reader made sorts by, and what a statement that failed leaves behind.
 *
 * <p>A collation a reader created is a name for a locale, and it sorts the way that locale does:
 * {@code CREATE COLLATION mine (LOCALE = 'C')} orders as C, which puts every capital before every
 * small letter. Read as a name and nothing else, only the two collations PostgreSQL ships under
 * those words were recognised, so every collation a reader made fell back to the database's own
 * ordering however it had been declared -- and a query that asked for C got something else.
 *
 * <p>A statement that fails never happened, and the sequence a serial or identity column would
 * have brought with it is part of the statement. Left behind, a CREATE TABLE refused for something
 * further down its definition still took the sequence's name, so a sequence outlived a table that
 * was never made.
 */
class WhatACollationNamesAndAFailedStatementLeavesTest {

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
            while (rs.next()) out.add(rs.getString(1));
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

    /** A collation named for a locale sorts the way that locale does. */
    @Test
    void whatACollationAReaderMadeSortsBy() throws SQLException {
        String values = "(VALUES ('B'),('a'),('A'),('b')) t(s)";
        exec("CREATE COLLATION zwc_c (LOCALE = 'C')");
        assertEquals(java.util.Arrays.asList("A", "B", "a", "b"),
                rows("SELECT s FROM " + values + " ORDER BY s COLLATE zwc_c"));
        // Which is what the collation PostgreSQL ships under that name does.
        assertEquals(rows("SELECT s FROM " + values + " ORDER BY s COLLATE \"C\""),
                rows("SELECT s FROM " + values + " ORDER BY s COLLATE zwc_c"));
        exec("CREATE COLLATION zwc_d (LC_COLLATE = 'C', LC_CTYPE = 'C')");
        assertEquals(java.util.Arrays.asList("A", "B", "a", "b"),
                rows("SELECT s FROM " + values + " ORDER BY s COLLATE zwc_d"));
        exec("DROP COLLATION zwc_c");
        exec("DROP COLLATION zwc_d");
    }

    /** A refused CREATE TABLE leaves no sequence behind. */
    @Test
    void whatARefusedCreateLeaves() throws SQLException {
        assertEquals("42P16", stateOf("CREATE TABLE zwc_s (id serial PRIMARY KEY, b int PRIMARY KEY)"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE relname='zwc_s_id_seq'"));
        assertEquals("42P16", stateOf("CREATE TABLE zwc_i (id int GENERATED ALWAYS AS IDENTITY,"
                + " b int PRIMARY KEY, c int PRIMARY KEY)"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE relname='zwc_i_id_seq'"));
        // A statement that succeeds keeps its sequence, and the sequence works.
        exec("CREATE TABLE zwc_ok (id serial PRIMARY KEY, b int)");
        assertEquals("1", one("SELECT count(*)::text FROM pg_class WHERE relname='zwc_ok_id_seq'"));
        assertEquals("1", one("SELECT nextval('zwc_ok_id_seq')::text"));
        exec("DROP TABLE zwc_ok");
    }
}
