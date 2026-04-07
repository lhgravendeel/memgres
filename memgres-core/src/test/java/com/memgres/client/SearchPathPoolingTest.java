package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 6, 10, 28 (Java/JDBC): search_path and schema selection
 * in pooled connections.
 * Tests search_path resolution, session state isolation across pooled
 * connections, RESET/DISCARD ALL cleanup, temp table leakage, role and
 * timezone reset, savepoint cleanup, and error recovery.
 */
class SearchPathPoolingTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    static String scalar(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    // --- 1. Changing search_path and verifying table resolution changes ---

    @Test void search_path_changes_table_resolution() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "CREATE SCHEMA sp_resolve_a");
            exec(c, "CREATE TABLE sp_resolve_a.sp_items(val text)");
            exec(c, "INSERT INTO sp_resolve_a.sp_items VALUES ('found_in_a')");
            exec(c, "SET search_path = sp_resolve_a");
            assertEquals("found_in_a", scalar(c, "SELECT val FROM sp_items"));
            exec(c, "SET search_path = public");
            assertThrows(SQLException.class, () -> scalar(c, "SELECT val FROM sp_items"));
            exec(c, "DROP SCHEMA sp_resolve_a CASCADE");
        }
    }

    // --- 2. search_path with multiple schemas ---

    @Test void search_path_multiple_schemas() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "CREATE SCHEMA sp_multi_x");
            exec(c, "CREATE SCHEMA sp_multi_y");
            exec(c, "CREATE TABLE sp_multi_x.sp_xdata(v text)");
            exec(c, "INSERT INTO sp_multi_x.sp_xdata VALUES ('from_x')");
            exec(c, "CREATE TABLE sp_multi_y.sp_ydata(v text)");
            exec(c, "INSERT INTO sp_multi_y.sp_ydata VALUES ('from_y')");
            exec(c, "SET search_path = sp_multi_x, sp_multi_y");
            assertEquals("from_x", scalar(c, "SELECT v FROM sp_xdata"));
            assertEquals("from_y", scalar(c, "SELECT v FROM sp_ydata"));
            // First schema in path wins for identically named tables
            exec(c, "CREATE TABLE sp_multi_y.sp_xdata(v text)");
            exec(c, "INSERT INTO sp_multi_y.sp_xdata VALUES ('from_y_shadow')");
            assertEquals("from_x", scalar(c, "SELECT v FROM sp_xdata"),
                    "First schema in search_path should take precedence");
            exec(c, "SET search_path = public");
            exec(c, "DROP SCHEMA sp_multi_x CASCADE");
            exec(c, "DROP SCHEMA sp_multi_y CASCADE");
        }
    }

    // --- 3. Temp tables not leaking across connections (simulate pool reuse) ---

    @Test void temp_tables_do_not_leak_across_connections() throws Exception {
        // Simulate connection pool: first "user" creates a temp table
        try (Connection c1 = newConn()) {
            exec(c1, "CREATE TEMP TABLE sp_temp_leak(secret text)");
            exec(c1, "INSERT INTO sp_temp_leak VALUES ('sensitive_data')");
            assertEquals("sensitive_data", scalar(c1, "SELECT secret FROM sp_temp_leak"));
        }
        // Second "user" gets a new connection; temp table must not be visible
        try (Connection c2 = newConn()) {
            assertThrows(SQLException.class, () -> scalar(c2, "SELECT secret FROM sp_temp_leak"));
        }
    }

    // --- 4. Role settings persisting across statements in same connection ---

    @Test void role_settings_persist_within_connection() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "SET search_path = pg_catalog, public");
            assertEquals("pg_catalog, public", scalar(c, "SHOW search_path").replace("\"", ""));
            // Run several statements; the setting should persist
            scalar(c, "SELECT 1");
            scalar(c, "SELECT current_database()");
            String sp = scalar(c, "SHOW search_path").replace("\"", "");
            assertEquals("pg_catalog, public", sp,
                    "search_path should persist across statements");
        }
    }

    // --- 5. RESET ALL clearing session state ---

    @Test void reset_all_clears_session_state() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "SET search_path = pg_catalog");
            exec(c, "SET statement_timeout = '5s'");
            exec(c, "RESET ALL");
            String sp = scalar(c, "SHOW search_path");
            // After RESET ALL, search_path returns to the default
            assertNotEquals("pg_catalog", sp, "search_path should be reset");
            String timeout = scalar(c, "SHOW statement_timeout");
            assertEquals("0", timeout, "statement_timeout should be reset to default");
        }
    }

    // --- 6. DISCARD ALL clearing everything ---

    @Test void discard_all_clears_everything() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "CREATE TEMP TABLE sp_discard_tmp(id int)");
            exec(c, "SET search_path = pg_catalog");
            exec(c, "PREPARE sp_discard_plan AS SELECT 1");
            exec(c, "DISCARD ALL");
            // Temp table should be gone
            assertThrows(SQLException.class,
                    () -> scalar(c, "SELECT * FROM sp_discard_tmp"));
            // search_path should be reset
            String sp = scalar(c, "SHOW search_path");
            assertNotEquals("pg_catalog", sp, "search_path should be reset after DISCARD ALL");
            // Prepared statement should be gone
            assertThrows(SQLException.class,
                    () -> exec(c, "EXECUTE sp_discard_plan"));
        }
    }

    // --- 7. SET search_path then RESET search_path ---

    @Test void set_then_reset_search_path() throws Exception {
        try (Connection c = newConn()) {
            String defaultSp = scalar(c, "SHOW search_path");
            exec(c, "SET search_path = pg_catalog");
            assertEquals("pg_catalog", scalar(c, "SHOW search_path"));
            exec(c, "RESET search_path");
            assertEquals(defaultSp, scalar(c, "SHOW search_path"),
                    "RESET search_path should restore the default");
        }
    }

    // --- 8. Connection with custom schema, verify table creation location ---

    @Test void table_created_in_first_schema_of_search_path() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "CREATE SCHEMA sp_target");
            exec(c, "SET search_path = sp_target, public");
            exec(c, "CREATE TABLE sp_loc_test(id int)");
            // Table should have been created in sp_target (first schema)
            String schema = scalar(c,
                    "SELECT schemaname FROM pg_tables WHERE tablename = 'sp_loc_test'");
            assertEquals("sp_target", schema,
                    "Table should be created in the first schema of search_path");
            exec(c, "DROP TABLE sp_loc_test");
            exec(c, "SET search_path = public");
            exec(c, "DROP SCHEMA sp_target CASCADE");
        }
    }

    // --- 9. search_path with pg_catalog explicit ordering ---

    @Test void search_path_pg_catalog_explicit_ordering() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "CREATE SCHEMA sp_cat_order");
            // When pg_catalog is listed explicitly, it takes only that position
            exec(c, "SET search_path = sp_cat_order, pg_catalog");
            // pg_catalog functions should still work
            assertNotNull(scalar(c, "SELECT pg_typeof(1)"));
            // current_schema should be sp_cat_order (first usable schema)
            assertEquals("sp_cat_order", scalar(c, "SELECT current_schema()"));
            exec(c, "SET search_path = public");
            exec(c, "DROP SCHEMA sp_cat_order CASCADE");
        }
    }

    // --- 10. Session variable persistence after error ---

    @Test void session_variable_persists_after_error() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "SET search_path = public");
            exec(c, "SET statement_timeout = '30s'");
            // Cause an error
            try {
                exec(c, "SELECT * FROM sp_nonexistent_table_xyz");
            } catch (SQLException ignored) {}
            // Session variables should still hold after the error (autocommit mode)
            assertEquals("30s", scalar(c, "SHOW statement_timeout"));
            String sp = scalar(c, "SHOW search_path");
            assertEquals("public", sp, "search_path should survive an error in autocommit mode");
        }
    }

    // --- 11. Connection state after failed DDL migration ---

    @Test void connection_usable_after_failed_ddl() throws Exception {
        try (Connection c = newConn()) {
            exec(c, "SET search_path = public");
            // Attempt DDL that will fail (add column to nonexistent table)
            try {
                exec(c, "ALTER TABLE sp_no_such_migration ADD COLUMN v text");
            } catch (SQLException ignored) {}
            // In autocommit mode, the connection should remain usable
            assertEquals("public", scalar(c, "SHOW search_path"));
            // Can still execute normal queries
            assertNotNull(scalar(c, "SELECT 1"));
        }
    }

    // --- 12. Savepoint cleanup after error leaves connection usable ---

    @Test void savepoint_cleanup_after_error() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            exec(c, "CREATE TABLE sp_svp_cleanup(id int PRIMARY KEY)");
            c.commit();
            exec(c, "INSERT INTO sp_svp_cleanup VALUES (1)");
            Savepoint sp = c.setSavepoint("sp_err_svp");
            try {
                exec(c, "INSERT INTO sp_svp_cleanup VALUES (1)"); // duplicate key
            } catch (SQLException ignored) {}
            // Rollback to savepoint should recover the transaction
            c.rollback(sp);
            exec(c, "INSERT INTO sp_svp_cleanup VALUES (2)");
            c.commit();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "SELECT id FROM sp_svp_cleanup ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next(), "Only rows 1 and 2 should exist");
            }
            exec(c, "DROP TABLE sp_svp_cleanup");
            c.commit();
        }
    }

    // --- 13. Timezone reset between pooled uses ---

    @Test void timezone_reset_between_pooled_uses() throws Exception {
        // First pool user sets timezone
        try (Connection c1 = newConn()) {
            exec(c1, "SET TimeZone = 'Asia/Tokyo'");
            assertEquals("Asia/Tokyo", scalar(c1, "SHOW timezone"));
        }
        // Simulate pool returning a fresh connection; timezone should be default
        try (Connection c2 = newConn()) {
            String tz = scalar(c2, "SHOW timezone");
            assertNotEquals("Asia/Tokyo", tz,
                    "New connection should not inherit previous connection's timezone");
        }
        // Simulate pool with DISCARD ALL cleanup
        try (Connection c3 = newConn()) {
            exec(c3, "SET TimeZone = 'US/Eastern'");
            exec(c3, "DISCARD ALL");
            String tz = scalar(c3, "SHOW timezone");
            assertNotEquals("US/Eastern", tz,
                    "DISCARD ALL should reset timezone to default");
        }
    }

    // --- 14. Role reset between pooled uses ---

    @Test void role_reset_between_pooled_uses() throws Exception {
        try (Connection c = newConn()) {
            // Create a test role
            exec(c, "CREATE ROLE sp_pool_role NOLOGIN");
            exec(c, "GRANT sp_pool_role TO memgres");
        }
        try (Connection c1 = newConn()) {
            String originalRole = scalar(c1, "SELECT current_user");
            exec(c1, "SET ROLE sp_pool_role");
            assertEquals("sp_pool_role", scalar(c1, "SELECT current_user"));
            // RESET ROLE should restore original
            exec(c1, "RESET ROLE");
            assertEquals(originalRole, scalar(c1, "SELECT current_user"));
        }
        // Simulate pool: DISCARD ALL should also reset role
        try (Connection c2 = newConn()) {
            String originalRole = scalar(c2, "SELECT current_user");
            exec(c2, "SET ROLE sp_pool_role");
            assertEquals("sp_pool_role", scalar(c2, "SELECT current_user"));
            exec(c2, "DISCARD ALL");
            assertEquals(originalRole, scalar(c2, "SELECT current_user"),
                    "DISCARD ALL should reset role to original");
        }
        // Cleanup
        try (Connection c = newConn()) {
            exec(c, "DROP ROLE sp_pool_role");
        }
    }
}
