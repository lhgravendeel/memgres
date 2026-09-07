package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What may stand in an index key, and what a path in an XMLTABLE may name.
 *
 * <p>A cast is a function too, and some of them are no more than stable: reading a
 * {@code timestamptz} as a date depends on the session's TimeZone, and writing a date out depends
 * on its DateStyle. Left unchecked, an index was built over a key whose value changes with a
 * setting, so the same row belonged in two places depending on who looked. The casts that do not
 * depend on a setting -- a timestamp read as a date, a number written out -- are keys as they
 * always were.
 *
 * <p>XMLNAMESPACES stands in front of the path and says what the prefixes in it mean. Read as an
 * ordinary expression, the word AS inside it ended the argument and the whole form was a syntax
 * error, so a path written with a prefix could not be written at all -- and a document read
 * without its namespaces in mind matched {@code x:root} against an element literally called that.
 */
class WhatAnIndexKeyAndAnXmlPathMayBeTest {

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

    /** A key whose value depends on a session setting is not a key. */
    @Test
    void whichCastsMayStandInAnIndexKey() throws SQLException {
        exec("CREATE TABLE zwx_t (tz timestamptz, ts timestamp, d date, t time, tt timetz,"
                + " iv interval, s text, n int)");
        for (String key : new String[]{"tz::date", "tz::timestamp", "tz::time", "tz::text",
                "ts::timestamptz", "ts::text", "d::timestamptz", "d::text", "iv::text",
                "s::date", "s::timestamp", "s::timestamptz", "t::timetz"}) {
            assertEquals("42P17",
                    stateOf("CREATE INDEX zwx_i ON zwx_t ((" + key + "))"), key);
        }
        // What does not depend on a setting is a key as it always was.
        for (String key : new String[]{"ts::date", "d::timestamp", "n::text", "s::int",
                "t::text", "tt::text", "tt::time"}) {
            assertNull(stateOf("CREATE INDEX zwx_ok ON zwx_t ((" + key + "))"), key);
            exec("DROP INDEX zwx_ok");
        }
        exec("DROP TABLE zwx_t");
    }

    /** A path may name a prefix the query declared. */
    @Test
    void whatAPathMayName() throws SQLException {
        exec("CREATE TABLE zwx_xd (id int, x xml)");
        exec("INSERT INTO zwx_xd VALUES (1,"
                + "'<root xmlns=\"http://ex.com\"><row><a>1</a></row></root>')");
        assertEquals("1", one("SELECT t.a::text FROM zwx_xd d,"
                + " xmltable(XMLNAMESPACES('http://ex.com' AS x), '/x:root/x:row'"
                + " PASSING d.x COLUMNS a int PATH 'x:a') t"));
        // A default namespace would have to reach every unprefixed name, which PostgreSQL says
        // it does not do rather than reading the clause and ignoring what it says.
        assertEquals("0A000", stateOf("SELECT t.* FROM zwx_xd d,"
                + " xmltable(XMLNAMESPACES(DEFAULT 'http://ex.com'), '/root/row'"
                + " PASSING d.x COLUMNS a int PATH 'a') t"));
        // A query that declares no prefixes reads the document as it always did.
        exec("INSERT INTO zwx_xd VALUES (2,'<root><row><a>7</a></row></root>')");
        assertEquals("7", one("SELECT t.a::text FROM zwx_xd d,"
                + " xmltable('/root/row' PASSING d.x COLUMNS a int PATH 'a') t"
                + " WHERE d.id=2"));
        exec("DROP TABLE zwx_xd");
    }
}
