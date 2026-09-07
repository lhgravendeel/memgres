package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Who owns a type a session created, who a default grant is recorded for, and what is released
 * when the type goes.
 *
 * <p>Whoever creates a type owns it, and that is the role the session is acting as -- not the one
 * it logged in as. Read from the login role instead, an enum, a composite and a domain created
 * under SET ROLE all named the wrong owner, so a reader joining pg_type back to pg_roles was told
 * something the server never said.
 *
 * <p>Ownership is also what DROP ROLE follows: a role owning something cannot be dropped. So the
 * record has to go when the type does. DROP DOMAIN left its ownership standing, and the role that
 * had created the domain could not be dropped afterwards -- refused on account of a type that was
 * no longer there. The same held when the domain was carried off by a CASCADE from the type it was
 * written over.
 *
 * <p>And ALTER DEFAULT PRIVILEGES records the grant against whoever is asking, which is again the
 * role in effect rather than the login role.
 */
class WhoOwnsATypeAndADefaultTest {

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

    /** Whoever the session is acting as owns what it creates. */
    @Test
    void whoOwnsATypeCreatedUnderARole() throws SQLException {
        exec("CREATE ROLE zwo_a");
        exec("GRANT CREATE ON SCHEMA public TO zwo_a");
        exec("SET ROLE zwo_a");
        exec("CREATE TYPE zwo_en AS ENUM ('x')");
        exec("CREATE TYPE zwo_c AS (a int)");
        exec("CREATE DOMAIN zwo_d AS int");
        exec("RESET ROLE");
        assertEquals("3", one("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zwo_en','zwo_c','zwo_d')"
                + " AND typowner = (SELECT oid FROM pg_roles WHERE rolname='zwo_a')"));
        // Dropping the types releases the ownership, so nothing depends on the role any more.
        exec("DROP DOMAIN zwo_d");
        exec("DROP TYPE zwo_c");
        exec("DROP TYPE zwo_en");
        exec("REVOKE CREATE ON SCHEMA public FROM zwo_a");
        assertNull(stateOf("DROP ROLE zwo_a"));
    }

    /** A domain carried off by a cascade releases its ownership too. */
    @Test
    void whatACascadeReleases() throws SQLException {
        exec("CREATE ROLE zwo_b");
        exec("GRANT CREATE ON SCHEMA public TO zwo_b");
        exec("SET ROLE zwo_b");
        exec("CREATE TYPE zwo_base AS ENUM ('p','q')");
        exec("CREATE DOMAIN zwo_over AS zwo_base");
        exec("RESET ROLE");
        exec("DROP TYPE zwo_base CASCADE");
        assertEquals("0", one("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zwo_base','zwo_over')"));
        exec("REVOKE CREATE ON SCHEMA public FROM zwo_b");
        assertNull(stateOf("DROP ROLE zwo_b"));
    }

    /** A default grant is recorded against the role in effect. */
    @Test
    void whoADefaultGrantIsRecordedFor() throws SQLException {
        exec("CREATE ROLE zwo_own");
        exec("CREATE SCHEMA zwo_ds AUTHORIZATION zwo_own");
        exec("SET ROLE zwo_own");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwo_ds GRANT SELECT ON TABLES TO zwo_own");
        exec("RESET ROLE");
        assertEquals("zwo_own", one("SELECT r.rolname FROM pg_default_acl d"
                + " JOIN pg_roles r ON r.oid=d.defaclrole"
                + " JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='zwo_ds'"));
        exec("SET ROLE zwo_own");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwo_ds REVOKE SELECT ON TABLES FROM zwo_own");
        exec("RESET ROLE");
        exec("DROP SCHEMA zwo_ds");
        exec("DROP ROLE zwo_own");
    }
}
