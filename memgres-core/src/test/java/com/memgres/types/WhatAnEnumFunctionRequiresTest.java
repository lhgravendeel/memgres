package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the enum functions require of the type they are asked about.
 *
 * <p>{@code enum_range(NULL::e)} names its type in a cast, and a cast still has to name a type
 * that exists. Read for its written name alone and never looked up, a cast to a type nobody had
 * defined made these functions answer NULL -- so a misspelled type name came back as no labels
 * rather than as no such type.
 */
class WhatAnEnumFunctionRequiresTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TYPE zwe_e AS ENUM ('a','b','c')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TYPE zwe_e");
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

    /** A type nobody defined is no such type, whichever enum function was asked. */
    @Test
    void whatANameThatReachesNoTypeGets() {
        assertEquals("42704", stateOf("SELECT enum_range(NULL::zwe_nope)"));
        assertEquals("42704", stateOf("SELECT enum_range('c'::zwe_nope, 'a'::zwe_nope)"));
        assertEquals("42704", stateOf("SELECT enum_first(NULL::zwe_nope)"));
        assertEquals("42704", stateOf("SELECT enum_last(NULL::zwe_nope)"));
    }

    /** A type that is there answers as before. */
    @Test
    void whatATypeThatIsThereAnswers() throws SQLException {
        assertEquals("{a,b,c}", one("SELECT enum_range(NULL::zwe_e)::text"));
        assertEquals("{a,b,c}", one("SELECT enum_range('a'::zwe_e, 'c'::zwe_e)::text"));
        assertEquals("{}", one("SELECT enum_range('c'::zwe_e, 'a'::zwe_e)::text"));
        assertEquals("a", one("SELECT enum_first(NULL::zwe_e)"));
        assertEquals("c", one("SELECT enum_last(NULL::zwe_e)"));
    }
}
