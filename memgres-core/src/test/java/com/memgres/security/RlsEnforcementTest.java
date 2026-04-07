package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that Row-Level Security policies actually filter data.
 *
 * Key PG behaviors:
 * - ENABLE ROW LEVEL SECURITY activates RLS for non-superuser roles
 * - FORCE ROW LEVEL SECURITY makes RLS apply even to table owners
 * - Policies with USING expressions filter SELECT/UPDATE/DELETE
 * - Policies with WITH CHECK enforce INSERT/UPDATE values
 * - SET ROLE / RESET ROLE switches the active role for RLS evaluation
 * - current_setting('app.var') in policies for multi-tenant patterns
 * - ALTER POLICY modifies the USING expression
 */
class RlsEnforcementTest {

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

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Basic RLS filtering with current_setting
    // ========================================================================

    @Test
    void rls_with_current_setting_filters_rows() throws SQLException {
        exec("CREATE TABLE td(id int PRIMARY KEY, tenant_id int NOT NULL, note text)");
        exec("INSERT INTO td VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c')");
        exec("ALTER TABLE td ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY tenant_pol ON td FOR SELECT
            USING (tenant_id = current_setting('app.tenant_id')::int)
            """);
        try {
            exec("ALTER TABLE td FORCE ROW LEVEL SECURITY");
            exec("SET app.tenant_id = '10'");

            int count = countRows("SELECT * FROM td");
            // Only rows with tenant_id = 10 should be visible → 2 rows
            assertEquals(2, count, "RLS should filter to only tenant_id=10 rows");
        } finally {
            exec("DROP TABLE IF EXISTS td CASCADE");
        }
    }

    @Test
    void rls_force_applies_to_table_owner() throws SQLException {
        exec("CREATE TABLE td2(id int PRIMARY KEY, tenant_id int NOT NULL, note text)");
        exec("INSERT INTO td2 VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c')");
        exec("ALTER TABLE td2 ENABLE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY sel_pol ON td2 FOR SELECT
            USING (tenant_id = current_setting('app.tid')::int)
            """);
        try {
            // Without FORCE, owner sees all rows
            exec("SET app.tid = '10'");
            // Note: In memgres, we're always the table owner, so ENABLE alone might not filter
            // FORCE ROW LEVEL SECURITY makes it apply to the owner too
            exec("ALTER TABLE td2 FORCE ROW LEVEL SECURITY");
            int count = countRows("SELECT * FROM td2");
            assertEquals(2, count, "FORCE RLS should filter even for table owner");
        } finally {
            exec("DROP TABLE IF EXISTS td2 CASCADE");
        }
    }

    // ========================================================================
    // ALTER POLICY changes filtering
    // ========================================================================

    @Test
    void alter_policy_changes_filter() throws SQLException {
        exec("CREATE TABLE td3(id int PRIMARY KEY, tenant_id int NOT NULL, deleted_at timestamptz)");
        exec("INSERT INTO td3 VALUES (1, 10, NULL), (2, 10, NULL), (3, 10, CURRENT_TIMESTAMP)");
        exec("ALTER TABLE td3 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE td3 FORCE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY sel_pol ON td3 FOR SELECT
            USING (tenant_id = current_setting('app.t')::int)
            """);
        exec("SET app.t = '10'");
        try {
            // Initially all 3 rows with tenant_id=10 are visible
            assertEquals(3, countRows("SELECT * FROM td3"));

            // Alter policy to also exclude soft-deleted
            exec("""
                ALTER POLICY sel_pol ON td3
                USING (tenant_id = current_setting('app.t')::int AND deleted_at IS NULL)
                """);

            assertEquals(2, countRows("SELECT * FROM td3"),
                    "After ALTER POLICY, soft-deleted row should be filtered out");
        } finally {
            exec("DROP TABLE IF EXISTS td3 CASCADE");
        }
    }

    // ========================================================================
    // RLS with INSERT check
    // ========================================================================

    @Test
    void rls_insert_policy_enforces_with_check() throws SQLException {
        exec("CREATE TABLE td4(id int PRIMARY KEY, tenant_id int NOT NULL, note text)");
        exec("ALTER TABLE td4 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE td4 FORCE ROW LEVEL SECURITY");
        exec("SET app.ins_tid = '10'");
        exec("""
            CREATE POLICY ins_pol ON td4 FOR INSERT
            WITH CHECK (tenant_id = current_setting('app.ins_tid')::int)
            """);
        exec("""
            CREATE POLICY sel_pol ON td4 FOR SELECT USING (true)
            """);
        try {
            // Should succeed: tenant_id matches
            exec("INSERT INTO td4 VALUES (1, 10, 'ok')");
            assertEquals(1, countRows("SELECT * FROM td4"));

            // Should fail: tenant_id doesn't match the policy
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO td4 VALUES (2, 20, 'bad')"),
                    "INSERT violating WITH CHECK should fail");
        } finally {
            exec("DROP TABLE IF EXISTS td4 CASCADE");
        }
    }

    // ========================================================================
    // DISABLE / NO FORCE ROW LEVEL SECURITY
    // ========================================================================

    @Test
    void disable_rls_removes_filtering() throws SQLException {
        exec("CREATE TABLE td5(id int PRIMARY KEY, tenant_id int NOT NULL)");
        exec("INSERT INTO td5 VALUES (1, 10), (2, 20)");
        exec("ALTER TABLE td5 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE td5 FORCE ROW LEVEL SECURITY");
        exec("""
            CREATE POLICY p ON td5 FOR SELECT
            USING (tenant_id = 10)
            """);
        try {
            assertEquals(1, countRows("SELECT * FROM td5"));

            exec("ALTER TABLE td5 DISABLE ROW LEVEL SECURITY");
            assertEquals(2, countRows("SELECT * FROM td5"),
                    "DISABLE RLS should remove all filtering");
        } finally {
            exec("DROP TABLE IF EXISTS td5 CASCADE");
        }
    }

    @Test
    void no_force_rls_stops_applying_to_owner() throws SQLException {
        exec("CREATE TABLE td6(id int PRIMARY KEY, tenant_id int NOT NULL)");
        exec("INSERT INTO td6 VALUES (1, 10), (2, 20)");
        exec("ALTER TABLE td6 ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE td6 FORCE ROW LEVEL SECURITY");
        exec("CREATE POLICY p ON td6 FOR SELECT USING (tenant_id = 10)");
        try {
            assertEquals(1, countRows("SELECT * FROM td6"));

            exec("ALTER TABLE td6 NO FORCE ROW LEVEL SECURITY");
            // Owner should now see all rows again (ENABLE is still on, but not forced)
            assertEquals(2, countRows("SELECT * FROM td6"),
                    "NO FORCE should let owner bypass RLS");
        } finally {
            exec("DROP TABLE IF EXISTS td6 CASCADE");
        }
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    @Test
    void alter_nonexistent_policy_fails() {
        try {
            exec("CREATE TABLE td_err(id int)");
            assertThrows(SQLException.class,
                    () -> exec("ALTER POLICY no_such ON td_err USING (true)"));
        } catch (SQLException e) {
            // table creation might also fail in some edge case
        } finally {
            try { exec("DROP TABLE IF EXISTS td_err"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void drop_nonexistent_policy_fails() {
        try {
            exec("CREATE TABLE td_err2(id int)");
            assertThrows(SQLException.class,
                    () -> exec("DROP POLICY no_such ON td_err2"));
        } catch (SQLException e) {
            // ignore
        } finally {
            try { exec("DROP TABLE IF EXISTS td_err2"); } catch (SQLException ignored) {}
        }
    }
}
