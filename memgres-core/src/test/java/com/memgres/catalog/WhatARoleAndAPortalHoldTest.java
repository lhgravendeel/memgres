package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a role's password is stored as, what a statement naming several roles does when one of them
 * is not there, where a temporary view lives, and what a cursor settles first.
 *
 * <p>A password is never held as it was written: PostgreSQL runs it through SCRAM-SHA-256, and
 * {@code pg_authid.rolpassword} holds the verifier -- which is what tells a reader the method. It
 * is pg_roles, the view, that masks it to eight stars for every role alike, whether or not one has
 * a password at all. Copied from the view, pg_authid showed the mask, and a role with no password
 * was indistinguishable from one with.
 *
 * <p>One DROP ROLE is one statement, so either every name goes or none does. Dropped as the list
 * was walked, a DROP ROLE whose second name reached nothing had already taken the first, and the
 * role that was there was gone even though the statement was refused.
 *
 * <p>A temporary view lives in the session's own schema, as a temporary table does. The word TEMP
 * was read as nothing but a promise to drop it later, so the catalogue said it stood in public.
 *
 * <p>A DECLARE reads its query through before it decides anything else, so a query naming a
 * relation that is not there says so whether or not a transaction block is open.
 */
class WhatARoleAndAPortalHoldTest {

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

    /** pg_authid holds the verifier; pg_roles holds the mask. */
    @Test
    void whatIsStoredForAPassword() throws SQLException {
        exec("CREATE ROLE zwp_np");
        exec("CREATE ROLE zwp_pw LOGIN PASSWORD 'secret'");
        assertEquals("********",
                one("SELECT rolpassword FROM pg_roles WHERE rolname='zwp_np'"));
        assertEquals("********",
                one("SELECT rolpassword FROM pg_roles WHERE rolname='zwp_pw'"));
        assertEquals("<null>", one("SELECT coalesce(rolpassword,'<null>') FROM pg_authid"
                + " WHERE rolname='zwp_np'"));
        assertEquals("SCRAM-SHA-256", one("SELECT substring(rolpassword from 1 for 13)"
                + " FROM pg_authid WHERE rolname='zwp_pw'"));
        // A verifier carries a salt and two keys, so it is far longer than the method's name.
        assertEquals("true", one("SELECT (length(rolpassword) > 60)::text FROM pg_authid"
                + " WHERE rolname='zwp_pw'"));
        // The password itself is nowhere in it.
        assertEquals("false", one("SELECT (rolpassword LIKE '%secret%')::text FROM pg_authid"
                + " WHERE rolname='zwp_pw'"));
        // A password given later is encrypted the same way, and PASSWORD NULL takes it away.
        exec("ALTER ROLE zwp_np PASSWORD 'other'");
        assertEquals("SCRAM-SHA-256", one("SELECT substring(rolpassword from 1 for 13)"
                + " FROM pg_authid WHERE rolname='zwp_np'"));
        exec("ALTER ROLE zwp_np PASSWORD NULL");
        assertEquals("<null>", one("SELECT coalesce(rolpassword,'<null>') FROM pg_authid"
                + " WHERE rolname='zwp_np'"));
        exec("DROP ROLE zwp_np");
        exec("DROP ROLE zwp_pw");
    }

    /** A DROP ROLE that names something missing takes nothing. */
    @Test
    void whatARefusedDropTakes() throws SQLException {
        exec("CREATE ROLE zwp_l1");
        assertEquals("42704", stateOf("DROP ROLE zwp_l1, zwp_no_such_role_x"));
        assertEquals("1", one("SELECT count(*)::text FROM pg_roles WHERE rolname='zwp_l1'"));
        // With every name there, every one of them goes.
        exec("CREATE ROLE zwp_l2");
        assertNull(stateOf("DROP ROLE zwp_l1, zwp_l2"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_roles WHERE rolname LIKE 'zwp_l%'"));
    }

    /** A temporary view stands in the session's own schema. */
    @Test
    void whereATemporaryViewStands() throws SQLException {
        exec("CREATE TEMP VIEW zwp_tv AS SELECT 1 AS x");
        assertEquals("true", one("SELECT (schemaname LIKE 'pg_temp%')::text FROM pg_views"
                + " WHERE viewname='zwp_tv'"));
        assertEquals("true", one("SELECT (n.nspname LIKE 'pg_temp%')::text FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='zwp_tv'"));
        assertEquals("true", one("SELECT (table_schema LIKE 'pg_temp%')::text"
                + " FROM information_schema.views WHERE table_name='zwp_tv'"));
        exec("DROP VIEW zwp_tv");
        // An ordinary view still stands where it was created.
        exec("CREATE VIEW zwp_v AS SELECT 1 AS x");
        assertEquals("public", one("SELECT schemaname FROM pg_views WHERE viewname='zwp_v'"));
        exec("DROP VIEW zwp_v");
    }

    /** A cursor's query is read before anything else is asked about the statement. */
    @Test
    void whatADeclareSettlesFirst() throws SQLException {
        assertEquals("42P01", stateOf("DECLARE zwp_k CURSOR FOR SELECT i FROM zwp_nosuch"));
        // A query that names something real still meets the rule about transaction blocks.
        exec("CREATE TABLE zwp_ct (i int)");
        assertEquals("25P01", stateOf("DECLARE zwp_k CURSOR FOR SELECT i FROM zwp_ct"));
        exec("BEGIN");
        assertNull(stateOf("DECLARE zwp_k CURSOR FOR SELECT i FROM zwp_ct"));
        exec("COMMIT");
        exec("DROP TABLE zwp_ct");
    }

    /** A large object is made under the number it was asked for. */
    @Test
    void underWhichNumberALargeObjectIsMade() throws SQLException {
        assertEquals("3000000000", one("SELECT lo_create(3000000000)::text"));
        assertEquals("3000000000", one("SELECT oid::text FROM pg_largeobject_metadata"
                + " WHERE oid = 3000000000"));
        assertEquals("1", one("SELECT lo_unlink(3000000000)::text"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_largeobject_metadata"));
    }

    /** A parameter number is an int, and one too large to be is said to be. */
    @Test
    void whatTooLargeAParameterNumberIs() throws SQLException {
        exec("CREATE FUNCTION zwp_big() RETURNS int AS $$ DECLARE n int;"
                + " BEGIN EXECUTE 'SELECT $99999999999' INTO n USING 1; RETURN n; END $$"
                + " LANGUAGE plpgsql");
        assertEquals("42601", stateOf("SELECT zwp_big()"));
        exec("DROP FUNCTION zwp_big()");
        assertEquals("42601", stateOf("SELECT $99999999999"));
    }
}
