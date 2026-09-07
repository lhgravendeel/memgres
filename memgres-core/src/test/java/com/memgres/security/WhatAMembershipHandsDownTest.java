package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a membership hands down.
 *
 * <p>PostgreSQL records INHERIT per membership as well as per role: {@code GRANT a TO b WITH
 * INHERIT FALSE} makes b a member of a that holds none of a's privileges until it does SET ROLE.
 * Read from the member role's own attribute alone, that grant handed b everything a had -- and
 * pg_auth_members said the membership inherited when the grant had said the opposite.
 */
class WhatAMembershipHandsDownTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE ROLE zwh_holder");
        exec("CREATE ROLE zwh_member LOGIN");
        exec("CREATE TABLE zwh_t (i int)");
        exec("GRANT SELECT ON zwh_t TO zwh_holder");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("RESET ROLE");
            exec("DROP TABLE zwh_t");
            exec("DROP ROLE zwh_member");
            exec("DROP ROLE zwh_holder");
            conn.close();
        }
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

    /** A membership that does not inherit hands nothing down until the member becomes the role. */
    @Test
    void whatANonInheritingMembershipHandsDown() throws SQLException {
        exec("GRANT zwh_holder TO zwh_member WITH INHERIT FALSE");
        assertEquals("false", one("SELECT inherit_option::text FROM pg_auth_members"
                + " WHERE roleid = 'zwh_holder'::regrole AND member = 'zwh_member'::regrole"));
        exec("SET ROLE zwh_member");
        assertEquals("42501", stateOf("SELECT count(*) FROM zwh_t"));
        // Becoming the role that holds the privilege is what reaches it.
        exec("SET ROLE zwh_holder");
        assertEquals("0", one("SELECT count(*)::text FROM zwh_t"));
        exec("RESET ROLE");
        // Regranting with INHERIT hands it down again.
        exec("GRANT zwh_holder TO zwh_member WITH INHERIT TRUE");
        assertEquals("true", one("SELECT inherit_option::text FROM pg_auth_members"
                + " WHERE roleid = 'zwh_holder'::regrole AND member = 'zwh_member'::regrole"));
        exec("SET ROLE zwh_member");
        assertEquals("0", one("SELECT count(*)::text FROM zwh_t"));
        exec("RESET ROLE");
        exec("REVOKE zwh_holder FROM zwh_member");
    }
}
