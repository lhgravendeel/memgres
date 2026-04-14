package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PG 18 NOT ENFORCED behavior on CHECK constraints.
 *
 * PG 18 does NOT allow ALTER CONSTRAINT ... [NOT] ENFORCED on CHECK constraints —
 * only FOREIGN KEY constraints support this. CHECK constraints must have their
 * enforceability set at creation time and it cannot be changed afterward.
 */
class RemainingNotEnforcedCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ne_compat CASCADE");
            s.execute("CREATE SCHEMA ne_compat");
            s.execute("SET search_path = ne_compat, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ne_compat CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * PG 18: ALTER CONSTRAINT ... NOT ENFORCED on a CHECK constraint is rejected
     * with SQLSTATE 42809 "cannot alter enforceability of constraint".
     */
    @Test
    void stmt33_alterCheckNotEnforcedRejected() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0))");

            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED");
                fail("ALTER CONSTRAINT NOT ENFORCED on CHECK should fail");
            } catch (SQLException e) {
                assertEquals("42809", e.getSQLState());
            }
        }
    }

    /**
     * PG 18: ALTER CONSTRAINT ... ENFORCED on a CHECK (NOT ENFORCED) constraint
     * is also rejected — cannot toggle enforceability of CHECK at all.
     */
    @Test
    void stmt35_alterCheckEnforcedAlsoRejected() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_toggle CASCADE");
            s.execute("CREATE TABLE ne_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_toggle CHECK (val > 0) NOT ENFORCED)");

            try {
                s.execute("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle ENFORCED");
                fail("ALTER CONSTRAINT ENFORCED on CHECK should fail");
            } catch (SQLException e) {
                assertEquals("42809", e.getSQLState());
            }
        }
    }

    /**
     * CHECK NOT ENFORCED still shows enforced='NO' in information_schema and
     * cannot be toggled. The enforced state is immutable after creation.
     */
    @Test
    void stmt135_informationSchemaEnforcedImmutable() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ne_info_toggle CASCADE");
            s.execute("CREATE TABLE ne_info_toggle (id integer PRIMARY KEY, val integer, "
                    + "CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED)");

            // NOT ENFORCED shows as 'NO'
            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next(), "Expected one row from information_schema");
                assertEquals("NO", rs.getString("enforced"),
                        "NOT ENFORCED constraint should show enforced='NO'");
            }

            // Cannot toggle to ENFORCED — rejected for CHECK constraints
            try {
                s.execute("ALTER TABLE ne_info_toggle ALTER CONSTRAINT chk_info ENFORCED");
                fail("ALTER CONSTRAINT ENFORCED on CHECK should fail");
            } catch (SQLException e) {
                assertEquals("42809", e.getSQLState());
            }

            // Still shows 'NO' (unchanged)
            try (ResultSet rs = s.executeQuery(
                    "SELECT enforced FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_compat'")) {
                assertTrue(rs.next());
                assertEquals("NO", rs.getString("enforced"),
                        "After failed ALTER, constraint should still show enforced='NO'");
            }
        }
    }

    /**
     * FK constraints DO support ALTER CONSTRAINT ... [NOT] ENFORCED (the only type that does).
     */
    @Test
    void fk_alter_constraint_enforced_toggle_works() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS child CASCADE");
            s.execute("DROP TABLE IF EXISTS parent CASCADE");
            s.execute("CREATE TABLE parent (id integer PRIMARY KEY)");
            s.execute("CREATE TABLE child (id integer PRIMARY KEY, pid integer, "
                    + "CONSTRAINT fk_pid FOREIGN KEY (pid) REFERENCES parent(id))");

            // Toggle to NOT ENFORCED — should succeed for FK
            s.execute("ALTER TABLE child ALTER CONSTRAINT fk_pid NOT ENFORCED");
            // Toggle back to ENFORCED — should succeed
            s.execute("ALTER TABLE child ALTER CONSTRAINT fk_pid ENFORCED");
        }
    }
}
