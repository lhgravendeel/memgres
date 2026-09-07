package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a handler naming several conditions catches, and how long a cursor a routine opened lives.
 *
 * <p>OTHERS is every error but the two a block is never meant to swallow: a cancelled statement,
 * which the client asked to stop, and a failed assertion, which is the author's own claim about
 * the program. Either is caught only by its own name -- but a handler may name several conditions,
 * and OTHERS not matching says nothing about the ones written beside it. Answered on OTHERS alone,
 * {@code WHEN OTHERS OR assert_failure} caught no assertion at all.
 *
 * <p>A cursor without WITH HOLD lives no longer than the transaction that opened it, and outside a
 * transaction block that is one statement. Only the explicit COMMIT swept them, so a cursor a
 * routine opened and did not close stayed open for the rest of the session, and every call of that
 * routine left another behind for pg_cursors to list.
 */
class WhatAHandlerCatchesAndACursorOutlivesTest {

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

    /** A condition written beside OTHERS is still a condition the handler names. */
    @Test
    void whatAHandlerNamingSeveralConditionsCatches() throws SQLException {
        exec("CREATE FUNCTION zwh_a() RETURNS text AS $$ BEGIN BEGIN ASSERT false, 'boom';"
                + " EXCEPTION WHEN OTHERS OR assert_failure THEN RETURN 'caught'; END;"
                + " RETURN 'no'; END $$ LANGUAGE plpgsql");
        assertEquals("caught", one("SELECT zwh_a()"));
        exec("DROP FUNCTION zwh_a()");
        // Its own name alone catches it too.
        exec("CREATE FUNCTION zwh_b() RETURNS text AS $$ BEGIN BEGIN ASSERT false, 'boom';"
                + " EXCEPTION WHEN assert_failure THEN RETURN 'caught'; END; END $$ LANGUAGE plpgsql");
        assertEquals("caught", one("SELECT zwh_b()"));
        exec("DROP FUNCTION zwh_b()");
        // And OTHERS alone still does not.
        exec("CREATE FUNCTION zwh_c() RETURNS text AS $$ BEGIN BEGIN ASSERT false, 'boom';"
                + " EXCEPTION WHEN OTHERS THEN RETURN 'caught'; END; END $$ LANGUAGE plpgsql");
        assertEquals("P0004", stateOf("SELECT zwh_c()"));
        exec("DROP FUNCTION zwh_c()");
        // OTHERS beside another name still catches what OTHERS catches.
        exec("CREATE FUNCTION zwh_d() RETURNS text AS $$ BEGIN BEGIN"
                + " PERFORM 1/0; EXCEPTION WHEN OTHERS OR assert_failure THEN RETURN 'caught';"
                + " END; END $$ LANGUAGE plpgsql");
        assertEquals("caught", one("SELECT zwh_d()"));
        exec("DROP FUNCTION zwh_d()");
    }

    /** A cursor a routine leaves open is gone when the statement is. */
    @Test
    void howLongARoutinesCursorLives() throws SQLException {
        exec("CREATE TABLE zwh_c1 (id int)");
        exec("INSERT INTO zwh_c1 VALUES (1),(2)");
        exec("CREATE FUNCTION zwh_leak() RETURNS int AS $$"
                + " DECLARE c CURSOR FOR SELECT id FROM zwh_c1 ORDER BY id; n int;"
                + " BEGIN OPEN c; FETCH c INTO n; RETURN n; END $$ LANGUAGE plpgsql");
        assertEquals("1", one("SELECT zwh_leak()::text"));
        assertEquals("1", one("SELECT zwh_leak()::text"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_cursors"));
        // Inside a transaction block it lives until the block ends.
        exec("BEGIN");
        assertEquals("1", one("SELECT zwh_leak()::text"));
        assertEquals("1", one("SELECT count(*)::text FROM pg_cursors"));
        exec("COMMIT");
        assertEquals("0", one("SELECT count(*)::text FROM pg_cursors"));
        exec("DROP FUNCTION zwh_leak()");
        exec("DROP TABLE zwh_c1");
    }
}
