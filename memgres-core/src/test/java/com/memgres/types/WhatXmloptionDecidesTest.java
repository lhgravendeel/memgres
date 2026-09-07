package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What xmloption decides.
 *
 * <p>Reading text as xml without saying how is an implicit parse, and {@code xmloption} is what
 * says how: a session that has asked for documents means it. Taking anything that parses either
 * way let {@code 'text'::xml} through under {@code xmloption = document}, where a bare word is not
 * a document at all -- and two sibling elements went in as one value where PostgreSQL has no
 * value.
 */
class WhatXmloptionDecidesTest {

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

    /** Under document, only a whole document is read; under content, a fragment will do. */
    @Test
    void whatEachSettingReads() throws SQLException {
        // Content is the default, and it takes a bare word and a pair of siblings alike.
        assertEquals("text", one("SELECT ('text'::xml)::text"));
        assertEquals("<a/><b/>", one("SELECT ('<a/><b/>'::xml)::text"));
        exec("SET xmloption = 'document'");
        assertEquals("document", one("SHOW xmloption"));
        assertEquals("2200M", stateOf("SELECT 'text'::xml"));
        assertEquals("2200M", stateOf("SELECT '<a/><b/>'::xml"));
        // A document still goes through.
        assertEquals("<a/>", one("SELECT ('<a/>'::xml)::text"));
        // The literal form is the same implicit parse.
        assertEquals("2200M", stateOf("SELECT xml 'text'"));
        exec("RESET xmloption");
        assertEquals("text", one("SELECT ('text'::xml)::text"));
    }

    /** A parse that says which it wants is not the one xmloption decides. */
    @Test
    void whatAnExplicitParseReads() throws SQLException {
        exec("SET xmloption = 'document'");
        assertEquals("text", one("SELECT XMLPARSE(CONTENT 'text')::text"));
        assertEquals("2200M", stateOf("SELECT XMLPARSE(DOCUMENT 'text')"));
        exec("RESET xmloption");
        assertEquals("2200M", stateOf("SELECT XMLPARSE(DOCUMENT 'text')"));
    }
}
