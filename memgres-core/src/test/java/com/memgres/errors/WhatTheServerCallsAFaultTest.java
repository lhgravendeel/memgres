package com.memgres.errors;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The words the server puts a fault in, and the order it finds faults in.
 *
 * <p>A schema written in front of a name is opened before anything inside it is looked for, so a
 * name qualified with a schema nobody has is reported as the missing schema and not as the object
 * that schema does not hold.
 *
 * <p>Two regular-expression engines find the same faults and word them differently, and it is the
 * message that reaches the reader: PostgreSQL says the parentheses are not balanced where the Java
 * engine says a group is unclosed. And a value put into a message is the value read as text, which
 * for an array is the braces PostgreSQL writes it in.
 */
class WhatTheServerCallsAFaultTest {

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

    /** The schema is opened before what it would hold is looked for. */
    @Test
    void aSchemaIsOpenedBeforeWhatIsInIt() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE ROLE zwf_r");
        }
        assertEquals("3F000", stateOf("GRANT USAGE ON DOMAIN zwf_nosch.dm TO zwf_r"));
        assertEquals("3F000", stateOf("GRANT EXECUTE ON FUNCTION zwf_nosch.fn(int) TO zwf_r"));
        assertTrue(messageOf("GRANT USAGE ON DOMAIN zwf_nosch.dm TO zwf_r")
                .contains("schema \"zwf_nosch\" does not exist"));
    }

    /** A malformed pattern is described in PostgreSQL's words, not the Java engine's. */
    @Test
    void howAMalformedPatternIsDescribed() {
        assertTrue(messageOf("SELECT 'abc' ~ '('")
                .contains("invalid regular expression: parentheses () not balanced"));
        assertTrue(messageOf("SELECT 'abc' ~ ')'")
                .contains("invalid regular expression: parentheses () not balanced"));
        assertTrue(messageOf("SELECT 'abc' ~ '['")
                .contains("invalid regular expression: brackets [] not balanced"));
        assertTrue(messageOf("SELECT 'abc' ~ '*'")
                .contains("invalid regular expression: quantifier operand invalid"));
        assertTrue(messageOf("SELECT 'abc' ~ 'a**'")
                .contains("invalid regular expression: quantifier operand invalid"));
    }

    /** What an assertion says is the value read as text. */
    @Test
    void whatAFailedAssertionSays() {
        assertTrue(messageOf("DO $$ BEGIN ASSERT false, ARRAY[1,2]; END $$").contains("{1,2}"));
        assertTrue(messageOf("DO $$ BEGIN ASSERT false, 'plain'; END $$").contains("plain"));
        assertEquals("P0004", stateOf("DO $$ BEGIN ASSERT false, 42; END $$"));
    }

    /** A comment is one plain string constant, and a national one is not that. */
    @Test
    void whichStringAComentMayBe() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zwf_t (a int)");
        }
        assertTrue(messageOf("COMMENT ON TABLE zwf_t IS N'x'")
                .contains("syntax error at or near \"N\""));
        assertNull(stateOf("COMMENT ON TABLE zwf_t IS 'ok'"));
        // The same literal is a perfectly ordinary value where a value belongs.
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT N'abc'")) {
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zwf_t");
        }
    }
}
