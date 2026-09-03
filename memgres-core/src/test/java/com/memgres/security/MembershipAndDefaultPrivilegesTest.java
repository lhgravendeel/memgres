package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a membership carries, and what an object is created holding.
 *
 * <p>The three questions asked of a membership are different questions. MEMBER asks whether it is
 * there at all; USAGE asks whether the role's privileges come with it, which they do only when
 * every step of the chain inherits; and the WITH ADMIN OPTION form asks whether the membership may
 * be granted on. A role may be made a member of another as it is created, and the ROLE clause runs
 * the opposite way from IN ROLE.
 *
 * <p>Privileges set aside with ALTER DEFAULT PRIVILEGES reach every object of the kind they were
 * set aside for: a view and a materialised view are tables here, a table built from a query is a
 * table, and the sequence a serial column brings with it is a sequence.
 */
class MembershipAndDefaultPrivilegesTest {

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

    /** A membership reaches through a chain, and a step that does not inherit stops the privileges. */
    @Test
    void whatAMembershipCarries() throws SQLException {
        exec("CREATE ROLE zmd_a");
        exec("CREATE ROLE zmd_b");
        exec("CREATE ROLE zmd_c");
        exec("CREATE ROLE zmd_n NOINHERIT");
        exec("GRANT zmd_b TO zmd_a");
        exec("GRANT zmd_c TO zmd_b");
        exec("GRANT zmd_b TO zmd_n");
        assertEquals("true", one("SELECT pg_has_role('zmd_a','zmd_c','MEMBER')::text"));
        assertEquals("true", one("SELECT pg_has_role('zmd_a','zmd_b','USAGE')::text"));
        // The membership is there, but a role that does not inherit takes nothing from it.
        assertEquals("true", one("SELECT pg_has_role('zmd_n','zmd_b','MEMBER')::text"));
        assertEquals("false", one("SELECT pg_has_role('zmd_n','zmd_b','USAGE')::text"));
    }

    /** Whether the membership may be granted on is recorded on the grant itself. */
    @Test
    void whetherAMembershipMayBeGrantedOn() throws SQLException {
        exec("CREATE ROLE zmd_ad");
        exec("CREATE ROLE zmd_ad2");
        exec("CREATE ROLE zmd_plain");
        exec("GRANT zmd_ad2 TO zmd_ad WITH ADMIN OPTION");
        exec("GRANT zmd_ad2 TO zmd_plain");
        assertEquals("true",
                one("SELECT pg_has_role('zmd_ad','zmd_ad2','MEMBER WITH ADMIN OPTION')::text"));
        assertEquals("true",
                one("SELECT pg_has_role('zmd_ad','zmd_ad2','USAGE WITH GRANT OPTION')::text"));
        assertEquals("false",
                one("SELECT pg_has_role('zmd_plain','zmd_ad2','MEMBER WITH ADMIN OPTION')::text"));
    }

    /** The ROLE clause makes those roles members of the new one; IN ROLE runs the other way. */
    @Test
    void theMembershipsACreateRoleWritesDown() throws SQLException {
        exec("CREATE ROLE zmd_g1");
        exec("CREATE ROLE zmd_a1");
        exec("CREATE ROLE zmd_c1 IN ROLE zmd_g1 ROLE zmd_a1 ADMIN zmd_a1");
        assertEquals("true", one("SELECT pg_has_role('zmd_a1','zmd_c1','MEMBER')::text"));
        assertEquals("true", one("SELECT pg_has_role('zmd_c1','zmd_g1','MEMBER')::text"));
        assertEquals("true",
                one("SELECT pg_has_role('zmd_a1','zmd_c1','MEMBER WITH ADMIN OPTION')::text"));
    }

    /** What is set aside for a kind of object reaches every object of that kind. */
    @Test
    void whatAnObjectIsCreatedHolding() throws SQLException {
        exec("CREATE ROLE zmd_r");
        exec("CREATE SCHEMA zmd_s");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zmd_s GRANT SELECT ON TABLES TO zmd_r");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zmd_s GRANT USAGE ON SEQUENCES TO zmd_r");
        exec("CREATE TABLE zmd_s.t (i int)");
        exec("CREATE VIEW zmd_s.v AS SELECT 1 AS i");
        exec("CREATE MATERIALIZED VIEW zmd_s.mv AS SELECT 1 AS i");
        exec("CREATE TABLE zmd_s.cta AS SELECT 1 AS i");
        exec("CREATE SEQUENCE zmd_s.q");
        exec("CREATE TABLE zmd_s.ser (id serial)");
        for (String rel : new String[]{"t", "v", "mv", "cta"}) {
            assertEquals("true",
                    one("SELECT has_table_privilege('zmd_r','zmd_s." + rel + "','SELECT')::text"),
                    rel);
        }
        for (String seq : new String[]{"q", "ser_id_seq"}) {
            assertEquals("true",
                    one("SELECT has_sequence_privilege('zmd_r','zmd_s." + seq + "','USAGE')::text"),
                    seq);
        }
    }

    /** A number nothing answers to names no object, and there is nothing to say about it. */
    @Test
    void askingAboutSomethingThatIsNotThere() throws SQLException {
        exec("CREATE TABLE zmd_t (a int)");
        assertNull(one("SELECT has_table_privilege(999999999::oid, 'SELECT')::text"));
        assertEquals("false",
                one("SELECT has_table_privilege(999999999::oid, 'zmd_t', 'SELECT')::text"));
        exec("DROP TABLE zmd_t");
    }
}
