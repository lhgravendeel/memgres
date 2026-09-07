package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What hstore's slice needs.
 *
 * <p>slice is strict: no list of keys is no answer, not the empty slice. Read as "no keys were
 * named", a null key array handed back an empty hstore -- a value where PostgreSQL has none, so a
 * caller testing the result for null went the wrong way.
 */
class WhatSliceNeedsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
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

    /** Nothing in, nothing out. */
    @Test
    void whatSliceAnswers() throws SQLException {
        assertEquals("true",
                one("SELECT (slice('a=>1, b=>2'::hstore, NULL) IS NULL)::text"));
        assertEquals("true",
                one("SELECT (slice(NULL::hstore, ARRAY['a']) IS NULL)::text"));
        // A list that names keys answers with those it found.
        assertEquals("\"a\"=>\"1\"",
                one("SELECT slice('a=>1, b=>2'::hstore, ARRAY['a'])::text"));
        // An empty list is a list, and names nothing.
        assertEquals("", one("SELECT slice('a=>1'::hstore, ARRAY[]::text[])::text"));
    }
}
