package com.memgres.rls;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whose rights a read runs with, and which of them a role actually holds.
 *
 * <p>A view reads its relations as its owner, unless it was made with {@code security_invoker},
 * which says to read them as whoever is reading the view.
 *
 * <p>A role holds what the roles it belongs to hold only if it inherits: one created NOINHERIT is
 * a member of them and holds none of their privileges until it does SET ROLE. Walking every
 * membership regardless gave such a role everything the roles above it had.
 */
class WhoseRightsAReadRunsWithTest {

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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** A view made with security_invoker reads as whoever is reading it. */
    @Test
    void whoAViewReadsAs() throws SQLException {
        exec("CREATE TABLE zwr_st (id int)");
        exec("INSERT INTO zwr_st VALUES (1),(2)");
        exec("CREATE VIEW zwr_inv WITH (security_invoker=true) AS SELECT * FROM zwr_st");
        exec("CREATE VIEW zwr_own AS SELECT * FROM zwr_st");
        exec("CREATE ROLE zwr_r LOGIN");
        exec("GRANT SELECT ON zwr_inv TO zwr_r");
        exec("GRANT SELECT ON zwr_own TO zwr_r");
        exec("SET ROLE zwr_r");
        try {
            assertEquals("42501", stateOf("SELECT id FROM zwr_inv ORDER BY id"));
            assertTrue(messageOf("SELECT id FROM zwr_inv ORDER BY id")
                    .contains("permission denied for table zwr_st"));
            // The ordinary view reads as its owner, who may read the table.
            assertEquals("1", one("SELECT id FROM zwr_own ORDER BY id"));
        } finally {
            exec("RESET ROLE");
        }
        exec("DROP VIEW zwr_inv");
        exec("DROP VIEW zwr_own");
        exec("DROP TABLE zwr_st");
        exec("DROP ROLE zwr_r");
    }

    /** An ON CONFLICT that updates asks for what an update asks for. */
    @Test
    void whatAConflictingUpdateAsksFor() throws SQLException {
        exec("CREATE TABLE zwr_s (id int primary key, n int)");
        exec("INSERT INTO zwr_s VALUES (1,10)");
        exec("CREATE ROLE zwr_ur LOGIN");
        exec("GRANT SELECT, INSERT ON zwr_s TO zwr_ur");
        exec("SET ROLE zwr_ur");
        try {
            assertEquals("42501", stateOf("INSERT INTO zwr_s VALUES (1,1)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 5"));
            // DO NOTHING writes no row that was already there, and asks for nothing more.
            assertNull(stateOf("INSERT INTO zwr_s VALUES (2,1) ON CONFLICT (id) DO NOTHING"));
        } finally {
            exec("RESET ROLE");
        }
        exec("GRANT UPDATE ON zwr_s TO zwr_ur");
        exec("SET ROLE zwr_ur");
        try {
            assertNull(stateOf("INSERT INTO zwr_s VALUES (1,1)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 5"));
        } finally {
            exec("RESET ROLE");
        }
        exec("DROP TABLE zwr_s");
        exec("DROP ROLE zwr_ur");
    }

    /** The row a conflicting update rewrites is a row the update policies must admit. */
    @Test
    void whichRowAConflictingUpdateMayRewrite() throws SQLException {
        exec("CREATE TABLE zwr_rls (id int primary key, owner text, n int)");
        exec("INSERT INTO zwr_rls VALUES (1,'zwr_pr',10),(2,'other',20)");
        exec("CREATE ROLE zwr_pr LOGIN");
        exec("GRANT SELECT, INSERT, UPDATE ON zwr_rls TO zwr_pr");
        exec("ALTER TABLE zwr_rls ENABLE ROW LEVEL SECURITY");
        exec("CREATE POLICY zwr_p1 ON zwr_rls FOR SELECT USING (true)");
        exec("CREATE POLICY zwr_p2 ON zwr_rls FOR INSERT WITH CHECK (true)");
        exec("CREATE POLICY zwr_p3 ON zwr_rls FOR UPDATE USING (owner = current_user)"
                + " WITH CHECK (n < 100)");
        exec("SET ROLE zwr_pr");
        try {
            // The row it collided with is one this role may not rewrite.
            assertEquals("42501", stateOf("INSERT INTO zwr_rls VALUES (2,'other',7)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 7"));
            assertTrue(messageOf("INSERT INTO zwr_rls VALUES (2,'other',7)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 7")
                    .contains("new row violates row-level security policy (USING expression)"
                            + " for table \"zwr_rls\""));
            // Its own row it may rewrite, so long as what it writes passes the check.
            assertNull(stateOf("INSERT INTO zwr_rls VALUES (1,'zwr_pr',7)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 7"));
            assertEquals("42501", stateOf("INSERT INTO zwr_rls VALUES (1,'zwr_pr',7)"
                    + " ON CONFLICT (id) DO UPDATE SET n = 700"));
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("7", one("SELECT n::text FROM zwr_rls WHERE id = 1"));
        assertEquals("20", one("SELECT n::text FROM zwr_rls WHERE id = 2"));
        exec("DROP TABLE zwr_rls");
        exec("DROP ROLE zwr_pr");
    }

    /** What is set aside in advance is given to every kind of object that has such a list. */
    @Test
    void whatIsSetAsideForTheObjectsToCome() throws SQLException {
        exec("CREATE ROLE zwr_da");
        exec("CREATE SCHEMA zwr_ds");
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwr_ds GRANT EXECUTE ON FUNCTIONS TO zwr_da");
        exec("CREATE FUNCTION zwr_ds.f() RETURNS int LANGUAGE sql AS 'SELECT 1'");
        assertEquals("true",
                one("SELECT has_function_privilege('zwr_da','zwr_ds.f()','EXECUTE')::text"));
        exec("ALTER DEFAULT PRIVILEGES IN SCHEMA zwr_ds GRANT SELECT ON TABLES TO zwr_da");
        exec("CREATE TABLE zwr_ds.t (i int)");
        assertEquals("true", one("SELECT has_table_privilege('zwr_da','zwr_ds.t','SELECT')::text"));
        // A list written with no schema is about the schemas to come.
        exec("ALTER DEFAULT PRIVILEGES GRANT USAGE ON SCHEMAS TO zwr_da");
        exec("CREATE SCHEMA zwr_ds2");
        assertEquals("true", one("SELECT has_schema_privilege('zwr_da','zwr_ds2','USAGE')::text"));
        // A schema made before the list was written has nothing from it.
        assertEquals("false", one("SELECT has_schema_privilege('zwr_da','zwr_ds','USAGE')::text"));
        exec("DROP SCHEMA zwr_ds CASCADE");
        exec("DROP SCHEMA zwr_ds2");
        exec("ALTER DEFAULT PRIVILEGES REVOKE USAGE ON SCHEMAS FROM zwr_da");
        exec("DROP ROLE zwr_da");
    }

    /** A role that does not inherit holds nothing of the roles it belongs to. */
    @Test
    void whatARoleThatDoesNotInheritHolds() throws SQLException {
        exec("CREATE ROLE zwr_a");
        exec("CREATE ROLE zwr_b NOINHERIT");
        exec("CREATE ROLE zwr_c");
        exec("CREATE TABLE zwr_t (i int)");
        exec("INSERT INTO zwr_t VALUES (1)");
        exec("GRANT SELECT ON zwr_t TO zwr_a");
        exec("GRANT zwr_a TO zwr_b");
        exec("GRANT zwr_a TO zwr_c");
        assertEquals("false", one("SELECT has_table_privilege('zwr_b','zwr_t','SELECT')::text"));
        assertEquals("true", one("SELECT has_table_privilege('zwr_c','zwr_t','SELECT')::text"));
        exec("SET ROLE zwr_b");
        try {
            assertEquals("42501", stateOf("SELECT count(*) FROM zwr_t"));
        } finally {
            exec("RESET ROLE");
        }
        exec("SET ROLE zwr_c");
        try {
            assertEquals("1", one("SELECT count(*)::text FROM zwr_t"));
        } finally {
            exec("RESET ROLE");
        }
        exec("DROP TABLE zwr_t");
        exec("DROP ROLE zwr_b");
        exec("DROP ROLE zwr_c");
        exec("DROP ROLE zwr_a");
    }
}
