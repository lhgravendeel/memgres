package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a name reaches when a privilege is asked about, and what an assertion reads.
 *
 * <p>A sequence is looked for where the name says to look: in the schema written, or along the
 * search path when none was. Found by its bare name whatever schema held it, a sequence in another
 * schema answered as though the name had reached it -- so a question about a name that reaches
 * nothing came back "no" instead of saying there is no such relation, which is a different answer
 * to a different question.
 *
 * <p>PostgreSQL reads an ASSERT's condition with boolean's input function, which takes the words
 * and the two digits and nothing else: {@code ASSERT 1} passes, {@code ASSERT 0} fails, and 42 is
 * not a boolean at all. Read that way only when it was already a string, a number reached the
 * complaint however it was written -- so {@code ASSERT 1} was refused.
 */
class WhatANameReachesForAPrivilegeTest {

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

    /** A privilege question about a name that reaches nothing is refused, not answered. */
    @Test
    void whichSequenceANameReaches() throws SQLException {
        exec("CREATE SCHEMA zwp_sc");
        exec("CREATE SEQUENCE zwp_sc.zwp_sq");
        exec("CREATE ROLE zwp_sr");
        exec("GRANT USAGE ON SEQUENCE zwp_sc.zwp_sq TO zwp_sr");
        assertEquals("true",
                one("SELECT has_sequence_privilege('zwp_sr','zwp_sc.zwp_sq','USAGE')::text"));
        // The bare name reaches nothing: the sequence is not in a schema on the search path.
        assertEquals("42P01", stateOf("SELECT has_sequence_privilege('zwp_sr','zwp_sq','USAGE')"));
        // With the schema on the path, the bare name reaches it.
        exec("SET search_path = zwp_sc, public");
        assertEquals("true", one("SELECT has_sequence_privilege('zwp_sr','zwp_sq','USAGE')::text"));
        exec("SET search_path = public");
        // A sequence in the default schema is reached by its bare name as it always was.
        exec("CREATE SEQUENCE zwp_here");
        assertEquals("false", one("SELECT has_sequence_privilege('zwp_sr','zwp_here','USAGE')::text"));
        exec("DROP SEQUENCE zwp_here");
        exec("DROP SEQUENCE zwp_sc.zwp_sq");
        exec("DROP SCHEMA zwp_sc");
        exec("DROP ROLE zwp_sr");
    }

    /** An assertion's condition is read with boolean's input function. */
    @Test
    void whatAnAssertionReads() {
        assertNull(stateOf("DO $$ BEGIN ASSERT 1; END $$"));
        assertNull(stateOf("DO $$ BEGIN ASSERT 'yes'; END $$"));
        // Zero is false, so the assertion fails rather than being unreadable.
        assertEquals("P0004", stateOf("DO $$ BEGIN ASSERT 0; END $$"));
        // And anything boolean's input function cannot read is an input error.
        assertEquals("22P02", stateOf("DO $$ BEGIN ASSERT 42; END $$"));
        assertEquals("22P02", stateOf("DO $$ BEGIN ASSERT 1.5; END $$"));
        assertEquals("22P02", stateOf("DO $$ BEGIN ASSERT 'nope'; END $$"));
    }
}
