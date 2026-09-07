package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a privilege and an owned object depend on, and what a drop that says nothing does.
 *
 * <p>A grant made by somebody holding the privilege WITH GRANT OPTION depends on that holding:
 * PostgreSQL will not take the option away and leave the grant behind. A plain REVOKE -- which is
 * RESTRICT -- is refused while one stands, and CASCADE takes both. Recorded nowhere, the two
 * grants looked alike, so the first went and the second was left granted by nobody. A grant the
 * owner made is not one of these, and revoking from a different role is not refused because of it.
 *
 * <p>DROP OWNED BY says nothing about cascading either, so it refuses to drop what another object
 * still depends on, exactly as DROP TABLE does. Cascading regardless, it took a table down and
 * left the foreign key that referenced it naming nothing.
 */
class WhatAPrivilegeDependsOnTest {

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

    /** A grant passed on depends on the option it was passed with. */
    @Test
    void whatAPassedOnGrantDependsOn() throws SQLException {
        exec("CREATE TABLE zwv_d (i int)");
        exec("CREATE ROLE zwv_r1");
        exec("CREATE ROLE zwv_r2");
        exec("GRANT SELECT ON zwv_d TO zwv_r1 WITH GRANT OPTION");
        exec("SET ROLE zwv_r1");
        exec("GRANT SELECT ON zwv_d TO zwv_r2");
        exec("RESET ROLE");
        assertEquals("2BP01", stateOf("REVOKE SELECT ON zwv_d FROM zwv_r1 RESTRICT"));
        assertEquals("true", one("SELECT has_table_privilege('zwv_r2','zwv_d','SELECT')::text"));
        // CASCADE takes the grant that was passed on as well.
        assertNull(stateOf("REVOKE SELECT ON zwv_d FROM zwv_r1 CASCADE"));
        assertEquals("false", one("SELECT has_table_privilege('zwv_r2','zwv_d','SELECT')::text"));
        exec("DROP TABLE zwv_d");
        exec("DROP ROLE zwv_r1");
        exec("DROP ROLE zwv_r2");
    }

    /** A grant the owner made is nobody else's to depend on. */
    @Test
    void whatAnOwnersOwnGrantDependsOn() throws SQLException {
        exec("CREATE TABLE zwv_e (i int)");
        exec("CREATE ROLE zwv_s1");
        exec("CREATE ROLE zwv_s2");
        exec("GRANT SELECT ON zwv_e TO zwv_s1 WITH GRANT OPTION");
        exec("GRANT SELECT ON zwv_e TO zwv_s2");
        assertNull(stateOf("REVOKE SELECT ON zwv_e FROM zwv_s1 RESTRICT"));
        assertEquals("true", one("SELECT has_table_privilege('zwv_s2','zwv_e','SELECT')::text"));
        exec("DROP TABLE zwv_e");
        exec("DROP ROLE zwv_s1");
        exec("DROP ROLE zwv_s2");
    }

    /** DROP OWNED BY refuses what another object depends on, unless told to cascade. */
    @Test
    void whatADropOwnedByTakes() throws SQLException {
        exec("CREATE ROLE zwv_a");
        exec("GRANT CREATE ON SCHEMA public TO zwv_a");
        exec("SET ROLE zwv_a");
        exec("CREATE TABLE zwv_base (i int PRIMARY KEY)");
        exec("RESET ROLE");
        exec("CREATE TABLE zwv_dep (i int REFERENCES zwv_base(i))");
        assertEquals("2BP01", stateOf("DROP OWNED BY zwv_a RESTRICT"));
        assertEquals("2BP01", stateOf("DROP OWNED BY zwv_a"));
        assertEquals("1", one("SELECT count(*)::text FROM pg_class WHERE relname='zwv_base'"));
        // With the dependent gone, the plain form takes it.
        exec("DROP TABLE zwv_dep");
        assertNull(stateOf("DROP OWNED BY zwv_a"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE relname='zwv_base'"));
        exec("REVOKE CREATE ON SCHEMA public FROM zwv_a");
        exec("DROP ROLE zwv_a");
    }

    /** CASCADE takes the dependent along. */
    @Test
    void whatADropOwnedByCascadeTakes() throws SQLException {
        exec("CREATE ROLE zwv_b");
        exec("GRANT CREATE ON SCHEMA public TO zwv_b");
        exec("SET ROLE zwv_b");
        exec("CREATE TABLE zwv_b2 (i int PRIMARY KEY)");
        exec("RESET ROLE");
        exec("CREATE TABLE zwv_b3 (i int REFERENCES zwv_b2(i))");
        assertNull(stateOf("DROP OWNED BY zwv_b CASCADE"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE relname='zwv_b2'"));
        exec("DROP TABLE IF EXISTS zwv_b3");
        exec("REVOKE CREATE ON SCHEMA public FROM zwv_b");
        exec("DROP ROLE zwv_b");
    }
}
