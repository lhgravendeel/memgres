package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Privileges are per-table, not per-name: a grant on one schema's table must not carry
 * to a same-named table in another schema. And a view is read with its owner's rights,
 * so granting on the view alone is enough. Verified against PostgreSQL 18.0.
 *
 * <p>N14 schema-blind privilege checks, N15 view owner's rights.
 */
class PrivilegeSchemaScopingTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // N14 — a grant names one schema's table
    // ------------------------------------------------------------------

    @Test
    void grantOnOneSchemaDoesNotReachTheSameNameElsewhere() throws Exception {
        exec("CREATE ROLE psalice LOGIN");
        exec("CREATE SCHEMA ps1");
        exec("CREATE SCHEMA ps2");
        exec("CREATE TABLE ps1.shared (id int)");
        exec("CREATE TABLE ps2.shared (id int)");
        exec("INSERT INTO ps1.shared VALUES (1)");
        exec("INSERT INTO ps2.shared VALUES (2)");
        exec("GRANT USAGE ON SCHEMA ps1 TO psalice");
        exec("GRANT USAGE ON SCHEMA ps2 TO psalice");
        exec("GRANT SELECT ON ps1.shared TO psalice");

        assertEquals(Arrays.asList("t|f"),
                rows("SELECT has_table_privilege('psalice','ps1.shared','SELECT'), "
                        + "has_table_privilege('psalice','ps2.shared','SELECT')"));

        exec("SET ROLE psalice");
        try {
            assertEquals(Arrays.asList("1"), rows("SELECT id FROM ps1.shared"));
            SQLException e = assertThrows(SQLException.class,
                    () -> rows("SELECT id FROM ps2.shared"));
            assertEquals("42501", e.getSQLState());
        } finally {
            exec("RESET ROLE");
        }
    }

    /** The grant follows the table, so the other schema stays unreadable after a revoke. */
    @Test
    void revokeOnOneSchemaLeavesTheOtherUnchanged() throws Exception {
        exec("CREATE ROLE psbob LOGIN");
        exec("CREATE SCHEMA pb1");
        exec("CREATE SCHEMA pb2");
        exec("CREATE TABLE pb1.t (id int)");
        exec("CREATE TABLE pb2.t (id int)");
        exec("GRANT SELECT ON pb1.t TO psbob");
        exec("GRANT SELECT ON pb2.t TO psbob");

        exec("REVOKE SELECT ON pb1.t FROM psbob");

        assertEquals(Arrays.asList("f|t"),
                rows("SELECT has_table_privilege('psbob','pb1.t','SELECT'), "
                        + "has_table_privilege('psbob','pb2.t','SELECT')"));
    }

    // ------------------------------------------------------------------
    // N15 — a view is read with its owner's rights
    // ------------------------------------------------------------------

    @Test
    void viewGrantIsEnoughWithoutBaseTableGrant() throws Exception {
        exec("CREATE ROLE pscarol LOGIN");
        exec("CREATE TABLE pvbase (id int)");
        exec("INSERT INTO pvbase VALUES (7)");
        exec("CREATE VIEW pvview AS SELECT * FROM pvbase");
        exec("GRANT SELECT ON pvview TO pscarol");

        assertEquals(Arrays.asList("t|f"),
                rows("SELECT has_table_privilege('pscarol','pvview','SELECT'), "
                        + "has_table_privilege('pscarol','pvbase','SELECT')"));

        exec("SET ROLE pscarol");
        try {
            assertEquals(Arrays.asList("7"), rows("SELECT id FROM pvview"));
        } finally {
            exec("RESET ROLE");
        }
    }

    /** Without a grant on the view either, the read is still refused. */
    @Test
    void viewWithoutGrantIsStillRefused() throws Exception {
        exec("CREATE ROLE psdave LOGIN");
        exec("CREATE TABLE pvbase2 (id int)");
        exec("INSERT INTO pvbase2 VALUES (7)");
        exec("CREATE VIEW pvview2 AS SELECT * FROM pvbase2");

        exec("SET ROLE psdave");
        try {
            SQLException e = assertThrows(SQLException.class, () -> rows("SELECT id FROM pvview2"));
            assertEquals("42501", e.getSQLState());
        } finally {
            exec("RESET ROLE");
        }
    }
}
