package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for commands where identifiers are wrapped in single quotes.
 *
 * PostgreSQL allows single-quoted strings in many places where an identifier
 * is expected, particularly in SET, CREATE ROLE, ALTER ROLE, GRANT, and
 * ownership commands. Some tools (pg_dump, ORMs, migration frameworks)
 * generate these quoted forms. The parser must accept them.
 */
class QuotedIdentifierInCommandTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // SET ROLE with quoted name
    // =========================================================================

    @Test
    void testSetRoleQuoted() throws SQLException {
        exec("SET ROLE 'memgres'");
    }

    @Test
    void testSetLocalRoleQuoted() throws SQLException {
        exec("SET LOCAL ROLE 'memgres'");
    }

    @Test
    void testSetSessionRoleQuoted() throws SQLException {
        exec("SET SESSION ROLE 'memgres'");
    }

    // =========================================================================
    // SET SESSION AUTHORIZATION with quoted name
    // =========================================================================

    @Test
    void testSetSessionAuthorizationQuoted() throws SQLException {
        exec("SET SESSION AUTHORIZATION 'memgres'");
    }

    @Test
    void testSetSessionAuthorizationDefault() throws SQLException {
        exec("SET SESSION AUTHORIZATION DEFAULT");
    }

    // =========================================================================
    // SET parameter with quoted value (search_path, timezone)
    // =========================================================================

    @Test
    void testSetSearchPathQuoted() throws SQLException {
        exec("SET search_path = 'public'");
    }

    @Test
    void testSetSearchPathMultipleQuoted() throws SQLException {
        exec("SET search_path TO 'public', 'pg_catalog'");
    }

    @Test
    void testSetTimezoneQuoted() throws SQLException {
        exec("SET timezone = 'UTC'");
    }

    @Test
    void testSetStatementTimeoutQuoted() throws SQLException {
        exec("SET statement_timeout = '30s'");
    }

    // =========================================================================
    // CREATE ROLE / CREATE USER with quoted name
    // =========================================================================

    @Test
    void testCreateRoleQuoted() throws SQLException {
        exec("CREATE ROLE 'app_reader'");
    }

    @Test
    void testCreateUserQuoted() throws SQLException {
        exec("CREATE USER 'app_writer'");
    }

    @Test
    void testCreateRoleQuotedWithOptions() throws SQLException {
        exec("CREATE ROLE 'app_admin' LOGIN NOSUPERUSER");
    }

    // =========================================================================
    // ALTER ROLE with quoted name
    // =========================================================================

    @Test
    void testAlterRoleQuoted() throws SQLException {
        exec("CREATE ROLE alter_target_role");
        exec("ALTER ROLE 'alter_target_role' NOSUPERUSER");
    }

    @Test
    void testAlterRoleSetQuoted() throws SQLException {
        exec("CREATE ROLE alter_set_role");
        exec("ALTER ROLE 'alter_set_role' SET statement_timeout = '30s'");
    }

    // =========================================================================
    // DROP ROLE with quoted name
    // =========================================================================

    @Test
    void testDropRoleQuoted() throws SQLException {
        exec("CREATE ROLE drop_target_role");
        exec("DROP ROLE 'drop_target_role'");
    }

    @Test
    void testDropRoleIfExistsQuoted() throws SQLException {
        exec("DROP ROLE IF EXISTS 'nonexistent_quoted_role'");
    }

    // =========================================================================
    // GRANT / REVOKE with quoted role name
    // =========================================================================

    @Test
    void testGrantToQuotedRole() throws SQLException {
        exec("CREATE TABLE grant_quoted_tbl (id serial PRIMARY KEY)");
        exec("CREATE ROLE 'grant_target'");
        exec("GRANT SELECT ON grant_quoted_tbl TO 'grant_target'");
    }

    @Test
    void testRevokeFromQuotedRole() throws SQLException {
        exec("CREATE TABLE revoke_quoted_tbl (id serial PRIMARY KEY)");
        exec("CREATE ROLE 'revoke_target'");
        exec("GRANT SELECT ON revoke_quoted_tbl TO 'revoke_target'");
        exec("REVOKE SELECT ON revoke_quoted_tbl FROM 'revoke_target'");
    }

    // =========================================================================
    // ALTER TABLE OWNER TO with quoted role
    // =========================================================================

    @Test
    void testAlterTableOwnerToQuoted() throws SQLException {
        exec("CREATE TABLE owner_quoted_tbl (id serial PRIMARY KEY)");
        exec("ALTER TABLE owner_quoted_tbl OWNER TO 'memgres'");
    }

    // =========================================================================
    // REASSIGN OWNED BY with quoted roles
    // =========================================================================

    @Test
    void testReassignOwnedByQuoted() throws SQLException {
        exec("CREATE ROLE 'reassign_from'");
        exec("CREATE ROLE 'reassign_to'");
        exec("REASSIGN OWNED BY 'reassign_from' TO 'reassign_to'");
    }

    // =========================================================================
    // Connection remains usable after quoted-identifier commands
    // =========================================================================

    @Test
    void testConnectionUsableAfterQuotedCommands() throws SQLException {
        exec("GRANT memgres TO test");
        exec("SET ROLE 'memgres'");
        exec("CREATE TABLE after_quoted_role (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO after_quoted_role (val) VALUES ('works')");
        assertEquals("works", query1("SELECT val FROM after_quoted_role"));
    }
}
