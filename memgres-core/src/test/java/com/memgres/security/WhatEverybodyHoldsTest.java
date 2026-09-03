package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The privileges PUBLIC holds without anybody granting them, and what taking one away does.
 *
 * <p>Some kinds give PUBLIC a privilege by default -- EXECUTE on a routine, USAGE on a type or a
 * language, CONNECT and TEMPORARY on a database -- and a REVOKE takes that away. There is no grant
 * to remove, so the revocation has to be recorded as one: read only as an absence of grants, a type
 * whose USAGE had been revoked from everyone still answered that everyone had it, and a routine
 * nobody had granted anything on could be executed by nobody.
 *
 * <p>A template database is the exception the server ships: initdb writes it a list giving everyone
 * the right to connect and nothing else.
 *
 * <p>And a grant belongs to the object it was written on, so it goes when the object goes.
 */
class WhatEverybodyHoldsTest {

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

    /** USAGE on a type is everybody's until it is revoked. */
    @Test
    void whatEverybodyHoldsOnAType() throws SQLException {
        exec("CREATE ROLE zwe_r");
        exec("CREATE TYPE zwe_ty AS ENUM ('a')");
        assertEquals("true", one("SELECT has_type_privilege('zwe_r','zwe_ty','USAGE')::text"));
        assertNull(one("SELECT typacl::text FROM pg_type WHERE typname='zwe_ty'"));
        exec("REVOKE USAGE ON TYPE zwe_ty FROM PUBLIC");
        assertEquals("false", one("SELECT has_type_privilege('zwe_r','zwe_ty','USAGE')::text"));
        assertEquals("{memgres=U/memgres}",
                one("SELECT typacl::text FROM pg_type WHERE typname='zwe_ty'"));
        // Granted back, it is held again.
        exec("GRANT USAGE ON TYPE zwe_ty TO PUBLIC");
        assertEquals("true", one("SELECT has_type_privilege('zwe_r','zwe_ty','USAGE')::text"));
        exec("DROP TYPE zwe_ty");
        exec("DROP ROLE zwe_r");
    }

    /** EXECUTE on a routine is everybody's until it is revoked, and goes with the routine. */
    @Test
    void whatEverybodyHoldsOnARoutine() throws SQLException {
        exec("CREATE ROLE zwe_fr");
        exec("CREATE FUNCTION zwe_f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        assertEquals("true", one("SELECT has_function_privilege('zwe_fr','zwe_f()','EXECUTE')::text"));
        exec("GRANT EXECUTE ON FUNCTION zwe_f() TO zwe_fr");
        // Taking the role's own grant away leaves the one everybody has.
        exec("REVOKE EXECUTE ON FUNCTION zwe_f() FROM zwe_fr");
        assertEquals("true", one("SELECT has_function_privilege('zwe_fr','zwe_f()','EXECUTE')::text"));
        exec("REVOKE EXECUTE ON FUNCTION zwe_f() FROM PUBLIC");
        assertEquals("false", one("SELECT has_function_privilege('zwe_fr','zwe_f()','EXECUTE')::text"));
        assertEquals("{memgres=X/memgres}",
                one("SELECT proacl::text FROM pg_proc WHERE proname='zwe_f'"));
        exec("GRANT EXECUTE ON FUNCTION zwe_f() TO zwe_fr");
        // The grant goes with the routine, so the role has nothing depending on it afterwards.
        exec("DROP FUNCTION zwe_f()");
        assertNull(stateOf("DROP ROLE zwe_fr"));
    }

    /** USAGE on a language is everybody's until it is revoked. */
    @Test
    void whatEverybodyHoldsOnALanguage() throws SQLException {
        exec("CREATE ROLE zwe_lr");
        assertEquals("true", one("SELECT has_language_privilege('zwe_lr','sql','USAGE')::text"));
        exec("REVOKE USAGE ON LANGUAGE sql FROM PUBLIC");
        assertEquals("false", one("SELECT has_language_privilege('zwe_lr','sql','USAGE')::text"));
        exec("GRANT USAGE ON LANGUAGE sql TO PUBLIC");
        assertEquals("true", one("SELECT has_language_privilege('zwe_lr','sql','USAGE')::text"));
        exec("DROP ROLE zwe_lr");
    }

    /** A template database lets everyone connect and nothing else. */
    @Test
    void whatEverybodyHoldsOnATemplate() throws SQLException {
        exec("CREATE ROLE zwe_dr");
        assertEquals("true",
                one("SELECT has_database_privilege('zwe_dr','template0','CONNECT')::text"));
        assertEquals("false",
                one("SELECT has_database_privilege('zwe_dr','template0','TEMPORARY')::text"));
        assertEquals("false",
                one("SELECT has_database_privilege('zwe_dr','template1','TEMPORARY')::text"));
        assertEquals("false",
                one("SELECT has_database_privilege('zwe_dr','template1','CREATE')::text"));
        assertEquals("{=c/memgres,memgres=CTc/memgres}",
                one("SELECT datacl::text FROM pg_database WHERE datname='template0'"));
        exec("DROP ROLE zwe_dr");
    }
}
