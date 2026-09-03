package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a backslash names in a pattern.
 *
 * <p>PostgreSQL's regular expressions give a backslash before a letter a meaning of their own, and
 * it is not Java's: {@code \b} is a backspace wherever it stands rather than a word boundary,
 * {@code \B} is a lone backslash, {@code \v} is a vertical tab rather than a class of every
 * vertical space, {@code \x} takes as many hexadecimal digits as follow it rather than two, and
 * {@code \U} takes eight. Read by Java's rules these matched other characters than the pattern
 * named, and {@code \0} named a character Java refused to read at all.
 *
 * <p>A SIMILAR TO escape hands what follows it to the same reader: the escape takes a pattern
 * character's meaning away, and where the character had none to take away the regular
 * expression's reading of it stands.
 */
class WhatABackslashNamesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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

    /** The escapes that name one character by what it is. */
    @Test
    void theEscapesThatNameOneCharacter() throws SQLException {
        assertEquals("false", one("SELECT ('abc' ~ 'a\\bc')::text"));
        assertEquals("true", one("SELECT ('a' || chr(8) || 'c' ~ 'a\\bc')::text"));
        assertEquals("true", one("SELECT ('a\\c' ~ 'a\\Bc')::text"));
        assertEquals("true", one("SELECT ('a' || chr(11) || 'c' ~ 'a\\vc')::text"));
        assertEquals("true", one("SELECT ('a' || chr(27) || 'c' ~ 'a\\ec')::text"));
        assertEquals("true", one("SELECT ('a' || chr(1) || 'c' ~ 'a\\cAc')::text"));
        // A null character is written \0, which is a character and never a reference to a group.
        assertEquals("false", one("SELECT ('abc' ~ 'a\\0c')::text"));
        assertEquals("true", one("SELECT (chr(1) ~ '\\01')::text"));
    }

    /** The escapes that name one character by its number. */
    @Test
    void theEscapesThatNameACharacterByItsNumber() throws SQLException {
        // \\x takes every digit that follows it, so \\x41c is one Cyrillic letter and not an A
        // before a c.
        assertEquals("false", one("SELECT ('aAc' ~ 'a\\x41c')::text"));
        assertEquals("true", one("SELECT ('a' || chr(1052) ~ 'a\\x41c')::text"));
        assertEquals("true", one("SELECT ('aAc' ~ 'a\\U00000041c')::text"));
        // A hexadecimal escape with no digits after it names nothing.
        assertEquals("2201B", stateOf("SELECT 'abc' ~ 'a\\x{41}c'"));
    }

    /** Inside a bracket expression the same escapes name the same characters. */
    @Test
    void theSameEscapesInsideBrackets() throws SQLException {
        assertEquals("false", one("SELECT ('abc' ~ 'a[\\b]c')::text"));
        assertEquals("true", one("SELECT ('a' || chr(8) || 'c' ~ 'a[\\b]c')::text"));
        assertEquals("true", one("SELECT ('a\\c' ~ 'a[\\B]c')::text"));
        assertEquals("true", one("SELECT ('aAc' ~ 'a[\\x41]c')::text"));
    }

    /** A SIMILAR TO escape hands what follows it to the regular expression. */
    @Test
    void whatASimilarToEscapeHandsOn() throws SQLException {
        // The pattern characters lose their meaning, which is what the escape is for.
        assertEquals("true", one("SELECT ('50%' SIMILAR TO '50\\%')::text"));
        assertEquals("true", one("SELECT ('a_b' SIMILAR TO 'a\\_b')::text"));
        assertEquals("false", one("SELECT ('axb' SIMILAR TO 'a\\_b')::text"));
        assertEquals("true", one("SELECT ('a(b' SIMILAR TO 'a\\(b')::text"));
        // A letter had no meaning to lose, so the regular expression's reading of it stands.
        assertEquals("false", one("SELECT ('abc' SIMILAR TO 'a\\bc')::text"));
        assertEquals("true", one("SELECT ('a' || chr(8) || 'c' SIMILAR TO 'a\\bc')::text"));
        assertEquals("true", one("SELECT ('a1b' SIMILAR TO 'a\\db')::text"));
        // LIKE has no such reading: there a backslash makes the next character itself.
        assertEquals("true", one("SELECT ('abc' LIKE 'a\\bc')::text"));
    }
}
