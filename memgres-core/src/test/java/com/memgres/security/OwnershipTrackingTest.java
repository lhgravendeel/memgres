package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 compatibility tests for ownership tracking.
 *
 * In PostgreSQL every object (table, view, sequence, function, schema, type,
 * domain, index) has an owner role.  These tests exercise the core rules:
 *
 *  1. DROP ROLE fails (2BP01) when the role owns tables
 *  2. DROP ROLE fails (2BP01) when the role owns sequences
 *  3. DROP ROLE fails (2BP01) when the role owns functions
 *  4. DROP ROLE fails (2BP01) when the role owns schemas
 *  5. DROP ROLE IF EXISTS still fails if the role has dependents
 *  6. DROP ROLE succeeds after transferring ownership away (REASSIGN OWNED / ALTER … OWNER TO)
 *  7. ALTER TABLE / VIEW / SEQUENCE OWNER TO syntax is accepted
 *  8. Tables created in a session are owned by the session user by default
 *  9. pg_class.relowner reflects the correct owner OID after ownership changes
 */
class OwnershipTrackingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // 1. DROP ROLE fails when role owns a table
    // ========================================================================

    @Test
    @DisplayName("own01: DROP ROLE fails with 2BP01 when role owns a table")
    void dropRole_ownsTable_raises2BP01() throws SQLException {
        exec("CREATE ROLE own_tbl_owner1");
        exec("CREATE TABLE own_tbl_t1 (id int)");
        exec("ALTER TABLE own_tbl_t1 OWNER TO own_tbl_owner1");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP ROLE own_tbl_owner1"),
                    "DROP ROLE should fail with 2BP01 when the role owns a table");
            assertEquals("2BP01", ex.getSQLState(),
                    "Expected sqlstate 2BP01 (dependent_objects_still_exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP TABLE IF EXISTS own_tbl_t1");
            exec("DROP ROLE IF EXISTS own_tbl_owner1");
        }
    }

    // ========================================================================
    // 2. DROP ROLE fails when role owns a sequence
    // ========================================================================

    @Test
    @DisplayName("own02: DROP ROLE fails with 2BP01 when role owns a sequence")
    void dropRole_ownsSequence_raises2BP01() throws SQLException {
        exec("CREATE ROLE own_seq_owner1");
        exec("CREATE SEQUENCE own_seq_s1");
        exec("ALTER SEQUENCE own_seq_s1 OWNER TO own_seq_owner1");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP ROLE own_seq_owner1"),
                    "DROP ROLE should fail with 2BP01 when the role owns a sequence");
            assertEquals("2BP01", ex.getSQLState(),
                    "Expected sqlstate 2BP01 (dependent_objects_still_exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP SEQUENCE IF EXISTS own_seq_s1");
            exec("DROP ROLE IF EXISTS own_seq_owner1");
        }
    }

    // ========================================================================
    // 3. DROP ROLE fails when role owns a function
    // ========================================================================

    @Test
    @DisplayName("own03: DROP ROLE fails with 2BP01 when role owns a function")
    void dropRole_ownsFunction_raises2BP01() throws SQLException {
        exec("CREATE ROLE own_func_owner1");
        exec("CREATE FUNCTION own_func_f1() RETURNS int LANGUAGE sql AS 'SELECT 1'");
        exec("ALTER FUNCTION own_func_f1() OWNER TO own_func_owner1");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP ROLE own_func_owner1"),
                    "DROP ROLE should fail with 2BP01 when the role owns a function");
            assertEquals("2BP01", ex.getSQLState(),
                    "Expected sqlstate 2BP01 (dependent_objects_still_exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP FUNCTION IF EXISTS own_func_f1()");
            exec("DROP ROLE IF EXISTS own_func_owner1");
        }
    }

    // ========================================================================
    // 4. DROP ROLE fails when role owns a schema
    // ========================================================================

    @Test
    @DisplayName("own04: DROP ROLE fails with 2BP01 when role owns a schema")
    void dropRole_ownsSchema_raises2BP01() throws SQLException {
        exec("CREATE ROLE own_sch_owner1");
        exec("CREATE SCHEMA own_sch_s1");
        exec("ALTER SCHEMA own_sch_s1 OWNER TO own_sch_owner1");
        try {
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP ROLE own_sch_owner1"),
                    "DROP ROLE should fail with 2BP01 when the role owns a schema");
            assertEquals("2BP01", ex.getSQLState(),
                    "Expected sqlstate 2BP01 (dependent_objects_still_exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP SCHEMA IF EXISTS own_sch_s1");
            exec("DROP ROLE IF EXISTS own_sch_owner1");
        }
    }

    // ========================================================================
    // 5. DROP ROLE IF EXISTS still fails when the role has dependents
    // ========================================================================

    @Test
    @DisplayName("own05: DROP ROLE IF EXISTS fails with 2BP01 even when role exists but owns objects")
    void dropRoleIfExists_ownsObjects_stillRaises2BP01() throws SQLException {
        exec("CREATE ROLE own_ifex_owner1");
        exec("CREATE TABLE own_ifex_t1 (id int)");
        exec("ALTER TABLE own_ifex_t1 OWNER TO own_ifex_owner1");
        try {
            // IF EXISTS only suppresses the "role does not exist" notice; it does NOT
            // bypass the dependency check when the role does exist and owns objects.
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP ROLE IF EXISTS own_ifex_owner1"),
                    "DROP ROLE IF EXISTS should still fail with 2BP01 when the role owns objects");
            assertEquals("2BP01", ex.getSQLState(),
                    "Expected sqlstate 2BP01 (dependent_objects_still_exist), got: "
                            + ex.getSQLState() + ": " + ex.getMessage());
        } finally {
            exec("DROP TABLE IF EXISTS own_ifex_t1");
            exec("DROP ROLE IF EXISTS own_ifex_owner1");
        }
    }

    // ========================================================================
    // 6. DROP ROLE succeeds after transferring ownership
    // ========================================================================

    @Test
    @DisplayName("own06: DROP ROLE succeeds after REASSIGN OWNED BY transfers all objects")
    void dropRole_afterReassignOwned_succeeds() throws SQLException {
        exec("CREATE ROLE own_xfer_owner1");
        exec("CREATE TABLE own_xfer_t1 (id int)");
        exec("ALTER TABLE own_xfer_t1 OWNER TO own_xfer_owner1");
        try {
            // Transfer all objects owned by own_xfer_owner1 to the current session user
            exec("REASSIGN OWNED BY own_xfer_owner1 TO CURRENT_USER");
            // After reassignment the role owns nothing, so DROP should succeed
            assertDoesNotThrow(() -> exec("DROP ROLE own_xfer_owner1"),
                    "DROP ROLE should succeed once ownership has been transferred away");
        } finally {
            exec("DROP TABLE IF EXISTS own_xfer_t1");
            exec("DROP ROLE IF EXISTS own_xfer_owner1");
        }
    }

    @Test
    @DisplayName("own06b: DROP ROLE succeeds after ALTER TABLE OWNER TO transfers the table")
    void dropRole_afterAlterOwner_succeeds() throws SQLException {
        exec("CREATE ROLE own_xfer_owner2");
        exec("CREATE TABLE own_xfer_t2 (id int)");
        exec("ALTER TABLE own_xfer_t2 OWNER TO own_xfer_owner2");
        try {
            // Transfer ownership of the individual table back to current user
            exec("ALTER TABLE own_xfer_t2 OWNER TO CURRENT_USER");
            assertDoesNotThrow(() -> exec("DROP ROLE own_xfer_owner2"),
                    "DROP ROLE should succeed once the table's owner has been transferred back");
        } finally {
            exec("DROP TABLE IF EXISTS own_xfer_t2");
            exec("DROP ROLE IF EXISTS own_xfer_owner2");
        }
    }

    // ========================================================================
    // 7. ALTER TABLE / VIEW / SEQUENCE OWNER TO syntax
    // ========================================================================

    @Test
    @DisplayName("own07a: ALTER TABLE OWNER TO syntax is accepted")
    void alterTableOwnerTo_syntaxAccepted() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE own_ownsyn_r1");
            exec("CREATE TABLE own_ownsyn_t1 (id int)");
            exec("ALTER TABLE own_ownsyn_t1 OWNER TO own_ownsyn_r1");
        }, "ALTER TABLE … OWNER TO should be accepted syntax");
        assertDoesNotThrow(() -> {
            exec("DROP TABLE IF EXISTS own_ownsyn_t1");
            exec("DROP ROLE IF EXISTS own_ownsyn_r1");
        });
    }

    @Test
    @DisplayName("own07b: ALTER VIEW OWNER TO syntax is accepted")
    void alterViewOwnerTo_syntaxAccepted() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE own_ownsyn_r2");
            exec("CREATE TABLE own_ownsyn_base2 (id int, v text)");
            exec("CREATE VIEW own_ownsyn_v2 AS SELECT id, v FROM own_ownsyn_base2");
            exec("ALTER VIEW own_ownsyn_v2 OWNER TO own_ownsyn_r2");
        }, "ALTER VIEW … OWNER TO should be accepted syntax");
        assertDoesNotThrow(() -> {
            exec("DROP VIEW IF EXISTS own_ownsyn_v2");
            exec("DROP TABLE IF EXISTS own_ownsyn_base2");
            exec("DROP ROLE IF EXISTS own_ownsyn_r2");
        });
    }

    @Test
    @DisplayName("own07c: ALTER SEQUENCE OWNER TO syntax is accepted")
    void alterSequenceOwnerTo_syntaxAccepted() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE own_ownsyn_r3");
            exec("CREATE SEQUENCE own_ownsyn_seq3");
            exec("ALTER SEQUENCE own_ownsyn_seq3 OWNER TO own_ownsyn_r3");
        }, "ALTER SEQUENCE … OWNER TO should be accepted syntax");
        assertDoesNotThrow(() -> {
            exec("DROP SEQUENCE IF EXISTS own_ownsyn_seq3");
            exec("DROP ROLE IF EXISTS own_ownsyn_r3");
        });
    }

    @Test
    @DisplayName("own07d: ALTER FUNCTION OWNER TO syntax is accepted")
    void alterFunctionOwnerTo_syntaxAccepted() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE own_ownsyn_r4");
            exec("CREATE FUNCTION own_ownsyn_f4() RETURNS int LANGUAGE sql AS 'SELECT 42'");
            exec("ALTER FUNCTION own_ownsyn_f4() OWNER TO own_ownsyn_r4");
        }, "ALTER FUNCTION … OWNER TO should be accepted syntax");
        assertDoesNotThrow(() -> {
            exec("DROP FUNCTION IF EXISTS own_ownsyn_f4()");
            exec("DROP ROLE IF EXISTS own_ownsyn_r4");
        });
    }

    @Test
    @DisplayName("own07e: ALTER SCHEMA OWNER TO syntax is accepted")
    void alterSchemaOwnerTo_syntaxAccepted() {
        assertDoesNotThrow(() -> {
            exec("CREATE ROLE own_ownsyn_r5");
            exec("CREATE SCHEMA own_ownsyn_sch5");
            exec("ALTER SCHEMA own_ownsyn_sch5 OWNER TO own_ownsyn_r5");
        }, "ALTER SCHEMA … OWNER TO should be accepted syntax");
        assertDoesNotThrow(() -> {
            exec("DROP SCHEMA IF EXISTS own_ownsyn_sch5 CASCADE");
            exec("DROP ROLE IF EXISTS own_ownsyn_r5");
        });
    }

    // ========================================================================
    // 8. Default ownership is the session user
    // ========================================================================

    @Test
    @DisplayName("own08: table created in session is owned by the session user")
    void newTable_ownedBySessionUser() throws SQLException {
        exec("CREATE TABLE own_default_t1 (id int)");
        try {
            String sessionUser = scalar("SELECT SESSION_USER");
            assertNotNull(sessionUser, "SESSION_USER should not be null");

            // pg_class.relowner should equal the OID of the session user
            String ownerName = scalar(
                    "SELECT r.rolname " +
                    "FROM pg_class c " +
                    "JOIN pg_roles r ON r.oid = c.relowner " +
                    "WHERE c.relname = 'own_default_t1'");

            assertNotNull(ownerName,
                    "pg_class should contain an entry for own_default_t1 with a valid owner");
            assertEquals(sessionUser, ownerName,
                    "Table owner should equal SESSION_USER at creation time, got: " + ownerName);
        } finally {
            exec("DROP TABLE IF EXISTS own_default_t1");
        }
    }

    @Test
    @DisplayName("own08b: sequence created in session is owned by the session user")
    void newSequence_ownedBySessionUser() throws SQLException {
        exec("CREATE SEQUENCE own_default_seq1");
        try {
            String sessionUser = scalar("SELECT SESSION_USER");
            assertNotNull(sessionUser);

            String ownerName = scalar(
                    "SELECT r.rolname " +
                    "FROM pg_class c " +
                    "JOIN pg_roles r ON r.oid = c.relowner " +
                    "WHERE c.relname = 'own_default_seq1'");

            assertNotNull(ownerName,
                    "pg_class should contain an entry for own_default_seq1 with a valid owner");
            assertEquals(sessionUser, ownerName,
                    "Sequence owner should equal SESSION_USER at creation time, got: " + ownerName);
        } finally {
            exec("DROP SEQUENCE IF EXISTS own_default_seq1");
        }
    }

    // ========================================================================
    // 9. pg_class.relowner reflects correct owner OID after ownership changes
    // ========================================================================

    @Test
    @DisplayName("own09a: pg_class.relowner OID matches the creating role's pg_roles.oid")
    void pgClassRelowner_matchesPgRolesOid() throws SQLException {
        exec("CREATE TABLE own_pgcls_t1 (id int)");
        try {
            // relowner in pg_class must equal the oid in pg_roles for SESSION_USER
            String matches = scalar(
                    "SELECT (c.relowner = r.oid)::text " +
                    "FROM pg_class c, pg_roles r " +
                    "WHERE c.relname = 'own_pgcls_t1' " +
                    "  AND r.rolname = SESSION_USER");

            assertEquals("true", matches,
                    "pg_class.relowner should equal pg_roles.oid for SESSION_USER");
        } finally {
            exec("DROP TABLE IF EXISTS own_pgcls_t1");
        }
    }

    @Test
    @DisplayName("own09b: pg_class.relowner updates after ALTER TABLE OWNER TO")
    void pgClassRelowner_updatesAfterAlterOwner() throws SQLException {
        exec("CREATE ROLE own_pgcls_r2");
        exec("CREATE TABLE own_pgcls_t2 (id int)");
        try {
            // Record original owner OID
            String originalOwnerOid = scalar(
                    "SELECT relowner::text FROM pg_class WHERE relname = 'own_pgcls_t2'");
            assertNotNull(originalOwnerOid);

            // Change ownership to own_pgcls_r2
            exec("ALTER TABLE own_pgcls_t2 OWNER TO own_pgcls_r2");

            String newOwnerOid = scalar(
                    "SELECT relowner::text FROM pg_class WHERE relname = 'own_pgcls_t2'");
            assertNotNull(newOwnerOid);

            assertNotEquals(originalOwnerOid, newOwnerOid,
                    "pg_class.relowner should change after ALTER TABLE OWNER TO");

            // The new OID must correspond to own_pgcls_r2 in pg_roles
            String newOwnerName = scalar(
                    "SELECT rolname FROM pg_roles WHERE oid = " + newOwnerOid);
            assertEquals("own_pgcls_r2", newOwnerName,
                    "pg_class.relowner OID should now point to own_pgcls_r2");
        } finally {
            // Transfer ownership back so we can drop the role cleanly
            exec("ALTER TABLE IF EXISTS own_pgcls_t2 OWNER TO CURRENT_USER");
            exec("DROP TABLE IF EXISTS own_pgcls_t2");
            exec("DROP ROLE IF EXISTS own_pgcls_r2");
        }
    }

    @Test
    @DisplayName("own09c: pg_class.relowner for a view matches the creating session user")
    void pgClassRelowner_view_matchesCreatingUser() throws SQLException {
        exec("CREATE TABLE own_pgcls_base3 (id int, v text)");
        exec("CREATE VIEW own_pgcls_v3 AS SELECT id FROM own_pgcls_base3");
        try {
            String matches = scalar(
                    "SELECT (c.relowner = r.oid)::text " +
                    "FROM pg_class c, pg_roles r " +
                    "WHERE c.relname = 'own_pgcls_v3' " +
                    "  AND r.rolname = SESSION_USER");

            assertEquals("true", matches,
                    "pg_class.relowner for a new view should equal pg_roles.oid for SESSION_USER");
        } finally {
            exec("DROP VIEW IF EXISTS own_pgcls_v3");
            exec("DROP TABLE IF EXISTS own_pgcls_base3");
        }
    }

    // ========================================================================
    // Extra: ALTER TABLE OWNER TO non-existent role should fail
    // ========================================================================

    @Test
    @DisplayName("own10: ALTER TABLE OWNER TO non-existent role raises error")
    void alterTableOwnerTo_nonExistentRole_raisesError() throws SQLException {
        exec("CREATE TABLE own_badowner_t1 (id int)");
        try {
            assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE own_badowner_t1 OWNER TO own_no_such_role_xyz"),
                    "ALTER TABLE OWNER TO a non-existent role should raise an error");
        } finally {
            exec("DROP TABLE IF EXISTS own_badowner_t1");
        }
    }

    // ========================================================================
    // Extra: REASSIGN OWNED BY non-existent role should fail
    // ========================================================================

    @Test
    @DisplayName("own11: REASSIGN OWNED BY non-existent role raises error")
    void reassignOwnedBy_nonExistentRole_raisesError() {
        assertThrows(SQLException.class,
                () -> exec("REASSIGN OWNED BY own_no_such_role_xyz TO CURRENT_USER"),
                "REASSIGN OWNED BY a non-existent role should raise an error");
    }

    // ========================================================================
    // Extra: DROP OWNED BY removes all objects owned by a role
    // ========================================================================

    @Test
    @DisplayName("own12: DROP OWNED BY removes objects so that DROP ROLE can then succeed")
    void dropOwnedBy_thenDropRole_succeeds() throws SQLException {
        exec("CREATE ROLE own_dropowned_r1");
        exec("CREATE TABLE own_dropowned_t1 (id int)");
        exec("ALTER TABLE own_dropowned_t1 OWNER TO own_dropowned_r1");
        try {
            // DROP OWNED BY removes all objects owned by the role inside this database
            exec("DROP OWNED BY own_dropowned_r1");
            // Now the role owns nothing, so DROP ROLE should succeed
            assertDoesNotThrow(() -> exec("DROP ROLE own_dropowned_r1"),
                    "DROP ROLE should succeed after DROP OWNED BY has removed all owned objects");
        } finally {
            // own_dropowned_t1 was removed by DROP OWNED BY; guard with IF EXISTS anyway
            exec("DROP TABLE IF EXISTS own_dropowned_t1");
            exec("DROP ROLE IF EXISTS own_dropowned_r1");
        }
    }
}
