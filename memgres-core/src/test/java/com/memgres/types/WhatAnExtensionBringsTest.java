package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an extension brings, and what an xml value is written as.
 *
 * <p>An extension that brings a type owns that type, and pg_depend is where a reader follows that
 * ownership to find what the extension contributed. Recorded only for the routines an extension
 * brings, citext -- which brings a type and no routine of its own -- looked like it had brought
 * nothing at all.
 *
 * <p>And an xml value is the text it was written as: a content fragment may be surrounded by
 * whitespace, and that whitespace belongs to the fragment. Trimmed on the way in, {@code ' <a/>
 * '::xml} came back as {@code <a/>}; indenting one drops what is in front of the first thing to
 * indent and leaves what follows the last.
 */
class WhatAnExtensionBringsTest {

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

    /** The type an extension brings is recorded as belonging to it. */
    @Test
    void whatPgDependSaysAnExtensionBrought() throws SQLException {
        assertEquals("false", owns("citext"));
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        assertEquals("true", owns("citext"));
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        assertEquals("true", owns("hstore"));
        // One nobody installed owns nothing.
        assertEquals("false", owns("cube"));
    }

    private static String owns(String extension) throws SQLException {
        return one("SELECT (count(*) > 0)::text FROM pg_depend d"
                + " JOIN pg_extension e ON e.oid = d.refobjid"
                + " WHERE e.extname = '" + extension + "' AND d.deptype = 'e'");
    }

    /** The whitespace around a fragment is the fragment's. */
    @Test
    void whatAnXmlFragmentKeeps() throws SQLException {
        assertEquals("[ <a/> ]", one("SELECT '[' || (' <a/> '::xml)::text || ']'"));
        assertEquals("[<a/> ]",
                one("SELECT '[' || XMLSERIALIZE(CONTENT ' <a/> '::xml AS text INDENT) || ']'"));
        assertEquals("[<a/>  ]",
                one("SELECT '[' || XMLSERIALIZE(CONTENT '<a/>  '::xml AS text INDENT) || ']'"));
        assertEquals("[<a/>]",
                one("SELECT '[' || XMLSERIALIZE(CONTENT '  <a/>'::xml AS text INDENT) || ']'"));
    }
}
