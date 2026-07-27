package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * currval reports what <em>this</em> connection last drew from a sequence. Answering it from the
 * sequence's own counter — which every connection shares — hands one caller another caller's
 * generated key, and the insert-then-fetch-key idiom is exactly how a parallel test suite uses a
 * sequence. These use two connections, because a single one cannot tell the two apart.
 */
class SequenceSessionScopeTest {

    static Memgres memgres;
    static Connection first;
    static Connection second;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        first = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        second = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        first.setAutoCommit(true);
        second.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (first != null) first.close();
        if (second != null) second.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) { st.execute(sql); }
    }

    private static void assertState(Connection c, String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(c, sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    private static void freshSequence(String name) throws SQLException {
        exec(first, "DROP SEQUENCE IF EXISTS " + name + " CASCADE");
        exec(first, "CREATE SEQUENCE " + name);
    }

    @Test
    void currvalIsNotVisibleToAnotherSession() throws Exception {
        freshSequence("sst_a");
        assertEquals("1", scalar(first, "SELECT nextval('sst_a')::text"));
        // the other connection drew nothing, so it has no currval of its own
        assertState(second, "55000", "SELECT currval('sst_a')");
        // and the drawing session still sees its own value
        assertEquals("1", scalar(first, "SELECT currval('sst_a')::text"));
    }

    @Test
    void eachSessionSeesItsOwnDraw() throws Exception {
        freshSequence("sst_b");
        assertEquals("1", scalar(first, "SELECT nextval('sst_b')::text"));
        assertEquals("2", scalar(second, "SELECT nextval('sst_b')::text"));
        // the counter is shared, but currval is not
        assertEquals("1", scalar(first, "SELECT currval('sst_b')::text"));
        assertEquals("2", scalar(second, "SELECT currval('sst_b')::text"));
    }

    @Test
    void lastvalIsAlsoPerSession() throws Exception {
        freshSequence("sst_c");
        // a connection of its own, since lastval reflects everything the session has drawn
        try (Connection fresh = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword())) {
            assertEquals("1", scalar(first, "SELECT nextval('sst_c')::text"));
            assertState(fresh, "55000", "SELECT lastval()");
            assertEquals("1", scalar(first, "SELECT lastval()::text"));
        }
    }

    @Test
    void currvalBeforeAnyDrawIsUndefined() throws Exception {
        freshSequence("sst_d");
        assertState(first, "55000", "SELECT currval('sst_d')");
        assertState(first, "42P01", "SELECT currval('sst_no_such')");
    }

    @Test
    void setvalDefinesCurrvalForTheCallingSessionOnly() throws Exception {
        freshSequence("sst_e");
        assertEquals("42", scalar(first, "SELECT setval('sst_e', 42)::text"));
        assertEquals("42", scalar(first, "SELECT currval('sst_e')::text"));
        // the other session still has none of its own
        assertState(second, "55000", "SELECT currval('sst_e')");
        assertEquals("43", scalar(second, "SELECT nextval('sst_e')::text"));
        assertEquals("43", scalar(second, "SELECT currval('sst_e')::text"));
    }

    @Test
    void setvalWithIsCalledFalseLeavesCurrvalUndefined() throws Exception {
        freshSequence("sst_f");
        assertEquals("42", scalar(first, "SELECT setval('sst_f', 42, false)::text"));
        assertState(first, "55000", "SELECT currval('sst_f')");
        assertEquals("42", scalar(first, "SELECT nextval('sst_f')::text"));
        assertEquals("42", scalar(first, "SELECT currval('sst_f')::text"));
    }

    @Test
    void repeatedCurrvalDoesNotAdvanceTheSequence() throws Exception {
        freshSequence("sst_g");
        assertEquals("1", scalar(first, "SELECT nextval('sst_g')::text"));
        assertEquals("1", scalar(first, "SELECT currval('sst_g')::text"));
        assertEquals("1", scalar(first, "SELECT currval('sst_g')::text"));
        assertEquals("2", scalar(first, "SELECT nextval('sst_g')::text"));
        assertEquals("2", scalar(first, "SELECT currval('sst_g')::text"));
    }

    @Test
    void generatedKeyIdiomReturnsThisSessionsKey() throws Exception {
        exec(first, "DROP TABLE IF EXISTS sst_t CASCADE");
        exec(first, "CREATE TABLE sst_t (i serial, v text)");
        exec(first, "INSERT INTO sst_t (v) VALUES ('a')");
        exec(second, "INSERT INTO sst_t (v) VALUES ('b')");
        // each connection reads back the key its own insert generated
        assertEquals("1", scalar(first, "SELECT currval('sst_t_i_seq')::text"));
        assertEquals("2", scalar(second, "SELECT currval('sst_t_i_seq')::text"));
        exec(first, "DROP TABLE sst_t CASCADE");
    }

    @Test
    void theCounterItselfRemainsShared() throws Exception {
        freshSequence("sst_h");
        assertEquals("1", scalar(first, "SELECT nextval('sst_h')::text"));
        assertEquals("2", scalar(second, "SELECT nextval('sst_h')::text"));
        assertEquals("3", scalar(first, "SELECT nextval('sst_h')::text"));
        // and a rolled back draw is still consumed
        first.setAutoCommit(false);
        assertEquals("4", scalar(first, "SELECT nextval('sst_h')::text"));
        first.rollback();
        first.setAutoCommit(true);
        assertEquals("5", scalar(second, "SELECT nextval('sst_h')::text"));
    }
}
