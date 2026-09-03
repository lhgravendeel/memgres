package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an access list holds, and how it reads apart.
 *
 * <p>{@code aclexplode} answers with four columns -- who granted, who holds it, which privilege,
 * and whether it may be handed on. Answered as one value, a caller naming grantee found no such
 * column; and read as one string whatever it arrived as, a list of items kept the brackets Java
 * prints around a list, so the first item began with one.
 *
 * <p>{@code pg_default_acl} is keyed by the role that wrote the statement, the schema and the kind
 * of object, and the list holds every grantee. A row per statement listed the same role and schema
 * twice and wrote the privileges out by name where the column holds an access list.
 */
class WhatAnAccessListHoldsTest {

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

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** An access list reads apart into the four columns it is declared with. */
    @Test
    void howAnAccessListReadsApart() throws SQLException {
        assertEquals(java.util.Arrays.asList(
                        "DELETE|false", "INSERT|false", "MAINTAIN|false", "REFERENCES|false",
                        "SELECT|false", "TRIGGER|false", "TRUNCATE|false", "UPDATE|false"),
                rows("SELECT privilege_type, is_grantable::text FROM"
                        + " aclexplode('{memgres=arwdDxtm/memgres}'::aclitem[])"
                        + " ORDER BY privilege_type"));
        // The grantor and the grantee are the roles the item names, not one fixed role.
        assertEquals(java.util.Arrays.asList("memgres|memgres"),
                rows("SELECT (SELECT rolname FROM pg_roles WHERE oid=grantor),"
                        + " (SELECT rolname FROM pg_roles WHERE oid=grantee)"
                        + " FROM aclexplode('{memgres=r/memgres}'::aclitem[])"));
        // An empty grantee is PUBLIC, which the catalogue numbers zero.
        assertEquals(java.util.Arrays.asList("0|SELECT"),
                rows("SELECT grantee::text, privilege_type"
                        + " FROM aclexplode('{=r/memgres}'::aclitem[])"));
        // A list with nothing in it has no dimension at all.
        assertEquals("22023", stateOf("SELECT * FROM aclexplode('{}'::aclitem[])"));
        assertTrue(messageOf("SELECT * FROM aclexplode('{}'::aclitem[])")
                .contains("ACL arrays must be one-dimensional"));
        assertEquals(java.util.Arrays.asList(),
                rows("SELECT * FROM aclexplode(NULL::aclitem[])"));
    }

    /** A default-privileges list is one row per role, schema and kind. */
    @Test
    void whatADefaultPrivilegesListHolds() throws SQLException {
        exec("CREATE ROLE zwa_a");
        exec("CREATE ROLE zwa_b");
        exec("CREATE SCHEMA zwa_s");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s GRANT SELECT, INSERT ON TABLES TO zwa_a");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s GRANT EXECUTE ON FUNCTIONS TO zwa_b");
        assertEquals(java.util.Arrays.asList(
                        "f|{zwa_b=X/memgres}", "r|{zwa_a=ar/memgres}"),
                rows("SELECT defaclobjtype::text, defaclacl::text FROM pg_default_acl"
                        + " WHERE defaclnamespace='zwa_s'::regnamespace ORDER BY 1"));
        // A second statement about the same kind adds a grantee to the same list.
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s GRANT SELECT ON TABLES TO zwa_b");
        assertEquals(java.util.Arrays.asList(
                        "f|{zwa_b=X/memgres}", "r|{zwa_a=ar/memgres,zwa_b=r/memgres}"),
                rows("SELECT defaclobjtype::text, defaclacl::text FROM pg_default_acl"
                        + " WHERE defaclnamespace='zwa_s'::regnamespace ORDER BY 1"));
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s REVOKE SELECT, INSERT ON TABLES FROM zwa_a");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s REVOKE SELECT ON TABLES FROM zwa_b");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwa_s REVOKE EXECUTE ON FUNCTIONS FROM zwa_b");
        exec("DROP SCHEMA zwa_s");
        exec("DROP ROLE zwa_a");
        exec("DROP ROLE zwa_b");
    }

    /** A policy cannot be renamed onto a name a policy on the relation already answers to. */
    @Test
    void whichNameAPolicyMayTake() throws SQLException {
        exec("CREATE TABLE zwa_pt (i int)");
        exec("CREATE POLICY zwa_pp ON zwa_pt USING (true)");
        assertEquals("42710", stateOf("ALTER POLICY zwa_pp ON zwa_pt RENAME TO zwa_pp"));
        assertTrue(messageOf("ALTER POLICY zwa_pp ON zwa_pt RENAME TO zwa_pp")
                .contains("policy \"zwa_pp\" for table \"zwa_pt\" already exists"));
        // Onto a name nobody has, it is renamed.
        assertNull(stateOf("ALTER POLICY zwa_pp ON zwa_pt RENAME TO zwa_pq"));
        exec("DROP TABLE zwa_pt");
    }
}
