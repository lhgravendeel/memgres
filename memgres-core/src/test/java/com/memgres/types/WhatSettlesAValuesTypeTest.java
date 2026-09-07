package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What settles the type a written-out value is read as, and what a minus sign in front of a year
 * means.
 *
 * <p>A branch written as a string is read as the type the other branches settle on, and the
 * complaint names that type. Named integer whatever the numeric branch was, a CASE beside 1.5
 * described a reading PostgreSQL never attempted -- it settles on numeric there, and on bigint or
 * double precision where those were written. An array constructor settles one element type the
 * same way.
 *
 * <p>{@code IS JSON} may be written {@code WITH} or {@code WITHOUT UNIQUE}, and {@code KEYS} may be
 * left off either way; so may the same clause inside {@code json_object} and {@code json_array}.
 * Only one of the four spellings was read, and the others were syntax errors.
 *
 * <p>And a minus sign in front of a year is a time zone displacement as far as PostgreSQL's date
 * reader is concerned, not a negative year: {@code '-2020-01-01'} is a displacement of two
 * thousand and twenty hours, which is out of range. Read as a year, it became 2021 BC -- a date
 * that text never names.
 */
class WhatSettlesAValuesTypeTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** The complaint names the type the branches settled on. */
    @Test
    void whichTypeAWrittenOutValueIsReadAs() {
        assertTrue(messageOf("SELECT CASE WHEN true THEN 1.5 ELSE 'abc' END")
                .contains("invalid input syntax for type numeric: \"abc\""));
        assertTrue(messageOf("SELECT CASE WHEN true THEN 1 ELSE 'abc' END")
                .contains("invalid input syntax for type integer: \"abc\""));
        assertTrue(messageOf("SELECT CASE WHEN true THEN 1.5::float8 ELSE 'abc' END")
                .contains("invalid input syntax for type double precision: \"abc\""));
        // The branch that will be taken makes no difference: the reading is settled first.
        assertTrue(messageOf("SELECT CASE WHEN false THEN 1.5 ELSE 'abc' END")
                .contains("invalid input syntax for type numeric: \"abc\""));
        assertEquals("22P02", stateOf("SELECT CASE WHEN true THEN 1.5 ELSE 'abc' END"));
        // An array constructor settles one element type over the whole of it.
        assertTrue(messageOf("SELECT ARRAY[1.5,'abc']")
                .contains("invalid input syntax for type numeric: \"abc\""));
        assertTrue(messageOf("SELECT ARRAY[1,'abc']")
                .contains("invalid input syntax for type integer: \"abc\""));
    }

    /** A value that does read as the settled type is read as it, and kept. */
    @Test
    void whatStillReads() throws SQLException {
        assertEquals("{1.5,2}", one("SELECT (ARRAY[1.5,'2'])::text"));
        assertEquals("numeric[]", one("SELECT pg_typeof(ARRAY[1.5,'2'])::text"));
        assertEquals("2", one("SELECT (CASE WHEN false THEN 1.5 ELSE '2' END)::text"));
    }

    /** Both halves of the uniqueness clause may be written, and KEYS may be left off. */
    @Test
    void howTheUniquenessClauseMayBeWritten() throws SQLException {
        assertEquals("true", one("SELECT ('{\"a\":1}' IS JSON WITH UNIQUE KEYS)::text"));
        assertEquals("true", one("SELECT ('{\"a\":1}' IS JSON WITH UNIQUE)::text"));
        assertEquals("true", one("SELECT ('{\"a\":1}' IS JSON WITHOUT UNIQUE KEYS)::text"));
        assertEquals("true", one("SELECT ('{\"a\":1}' IS JSON WITHOUT UNIQUE)::text"));
        // WITHOUT is what the test does anyway, so a repeated key is still JSON.
        assertEquals("true", one("SELECT ('{\"a\":1,\"a\":2}' IS JSON WITHOUT UNIQUE KEYS)::text"));
        assertEquals("false", one("SELECT ('{\"a\":1,\"a\":2}' IS JSON WITH UNIQUE KEYS)::text"));
        assertEquals("true", one("SELECT ('{\"a\":1}' IS JSON OBJECT WITHOUT UNIQUE KEYS)::text"));
        assertEquals("false", one("SELECT ('{\"a\":1}' IS NOT JSON WITHOUT UNIQUE KEYS)::text"));
        // The constructors read the same clause.
        assertEquals("{\"a\" : 1}", one("SELECT json_object('a' VALUE 1 WITH UNIQUE)::text"));
        assertEquals("{\"a\" : 1, \"a\" : 2}",
                one("SELECT json_object('a' VALUE 1, 'a' VALUE 2 WITHOUT UNIQUE)::text"));
    }

    /** A minus sign in front of the year is a displacement, and one that is out of range. */
    @Test
    void whatAMinusInFrontOfAYearIs() {
        for (String written : new String[]{"-2020-01-01", "-0100-01-01", "-123-01-01",
                "-12345-01-01"}) {
            assertEquals("22009", stateOf("SELECT '" + written + "'::timestamp"), written);
            assertEquals("22009", stateOf("SELECT '" + written + "'::date"), written);
        }
        assertTrue(messageOf("SELECT '-2020-01-01'::timestamp")
                .contains("time zone displacement out of range: \"-2020-01-01\""));
        assertEquals("22009", stateOf("SELECT '-100-01-01 10:00:00'::timestamptz"));
        // One or two digits are too few to be read as a displacement at all.
        assertEquals("22007", stateOf("SELECT '-1-01-01'::timestamp"));
        assertEquals("22007", stateOf("SELECT '-12-01-01'::timestamp"));
        // The complaint names the whole of what was written, era marker and all.
        assertTrue(messageOf("SELECT '-1-01-01 BC'::timestamp")
                .contains("invalid input syntax for type timestamp: \"-1-01-01 BC\""));
        assertTrue(messageOf("SELECT 'garbage BC'::timestamp")
                .contains("invalid input syntax for type timestamp: \"garbage BC\""));
        assertTrue(messageOf("SELECT '-1-01-01 BC'::date")
                .contains("invalid input syntax for type date: \"-1-01-01 BC\""));
    }

    /** A year written the ordinary way, with or without an era, still reads. */
    @Test
    void whatStillReadsAsADate() throws SQLException {
        assertEquals("4713-01-01 00:00:00 BC", one("SELECT '4713-01-01 BC'::timestamp::text"));
        assertEquals("2020-01-01", one("SELECT '2020-01-01'::date::text"));
        assertEquals("22008", stateOf("SELECT '4714-01-01 BC'::timestamp"));
    }
}
