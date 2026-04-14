package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NOT ENFORCED constraint PG 18 compatibility.
 *
 * PG 18 rules:
 * - CHECK and FK constraints can be created with NOT ENFORCED
 * - Only FK constraints support ALTER CONSTRAINT ... [NOT] ENFORCED
 * - UNIQUE, PRIMARY KEY, and EXCLUDE constraints cannot be NOT ENFORCED
 */
class NotEnforcedConstraintsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ne_test CASCADE");
            s.execute("CREATE SCHEMA ne_test");
            s.execute("SET search_path = ne_test, public");

            // Setup for info_schema test
            s.execute("CREATE TABLE ne_info_toggle ("
                    + "  id integer PRIMARY KEY,"
                    + "  val integer,"
                    + "  CONSTRAINT chk_info CHECK (val > 0) NOT ENFORCED"
                    + ")");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ne_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    /**
     * ALTER CONSTRAINT ... NOT ENFORCED on CHECK constraint should be rejected.
     * PG 18 only allows this for FK constraints.
     */
    @Test
    void testAlterCheckConstraintNotEnforcedRejected() {
        assertDoesNotThrow(() -> exec("CREATE TABLE ne_toggle ("
                + "  id integer PRIMARY KEY,"
                + "  val integer,"
                + "  CONSTRAINT chk_toggle CHECK (val > 0)"
                + ")"));
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE ne_toggle ALTER CONSTRAINT chk_toggle NOT ENFORCED"));
        assertEquals("42809", ex.getSQLState());
    }

    /**
     * UNIQUE NOT ENFORCED should error with SQLSTATE 0A000.
     */
    @Test
    void testUniqueNotEnforcedErrorsSqlstate0A000() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE ne_unique ("
                        + "  id integer PRIMARY KEY,"
                        + "  code text,"
                        + "  CONSTRAINT uq_code UNIQUE (code) NOT ENFORCED"
                        + ")")
        );
        assertEquals("0A000", ex.getSQLState());
    }

    /**
     * PRIMARY KEY NOT ENFORCED should error with SQLSTATE 0A000.
     */
    @Test
    void testPrimaryKeyNotEnforcedErrorsSqlstate0A000() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE ne_pk_fail ("
                        + "  id integer,"
                        + "  CONSTRAINT pk_fail PRIMARY KEY (id) NOT ENFORCED"
                        + ")")
        );
        assertEquals("0A000", ex.getSQLState());
    }

    /**
     * ALTER CONSTRAINT ... ENFORCED on CHECK constraint should also be rejected
     * (not just NOT ENFORCED — all enforceability toggling is blocked for CHECK).
     */
    @Test
    void testAlterCheckConstraintEnforcedRejected() {
        assertDoesNotThrow(() -> exec("CREATE TABLE ne_toggle_validate ("
                + "  id integer PRIMARY KEY,"
                + "  val integer,"
                + "  CONSTRAINT chk_toggle CHECK (val > 0) NOT ENFORCED"
                + ")"));
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE ne_toggle_validate ALTER CONSTRAINT chk_toggle ENFORCED")
        );
        assertEquals("42809", ex.getSQLState());
    }

    /**
     * information_schema.table_constraints.enforced should report 'NO'
     * for a NOT ENFORCED constraint.
     */
    @Test
    void testInformationSchemaEnforcedReportsNoForNotEnforced() throws SQLException {
        String enforced = query1(
                "SELECT enforced FROM information_schema.table_constraints "
                        + "WHERE constraint_name = 'chk_info' AND table_schema = 'ne_test'");
        assertEquals("NO", enforced);
    }
}
