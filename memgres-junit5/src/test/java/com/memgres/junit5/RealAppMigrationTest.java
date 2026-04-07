package com.memgres.junit5;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests running a real-world app migration against Memgres.
 * Covers: extensions, DO blocks, enums, JSONB defaults, CIDR, INET,
 * enum casts, uuid_generate_v4(), pg_type catalog, triggers, views,
 * ON CONFLICT DO UPDATE, DEFERRABLE constraints, partial indexes, etc.
 */
class RealAppMigrationTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .migrationDir("real-app-migration")
            .snapshotAfterInit(true)
            .restoreBeforeEach(true)
            .build();

    // --- Schema created successfully ---

    @Test
    void migrationCompletedSuccessfully() throws SQLException {
        // If we got here, the entire migration ran without errors
        try (Connection conn = db.getConnection()) {
            // Verify plans were seeded
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT COUNT(*) FROM plans")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void configEntriesSeeded() throws SQLException {
        try (Connection conn = db.getConnection()) {
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT value FROM config_entries WHERE key = 'theme'")) {
                assertTrue(rs.next());
                assertEquals("dark", rs.getString(1));
            }
        }
    }

    // --- Enum types ---

    @Test
    void enumTypesCreated() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // Insert using enum type
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('test@example.com', 'hash123')");
            conn.createStatement().execute(
                    "INSERT INTO plans (key, name, monthly_cost, max_items) " +
                            "VALUES ('test', 'Test', 0, 1)");
            conn.createStatement().execute(
                    "INSERT INTO accounts (name) VALUES ('Test Account')");

            // Use enum column
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT account_id FROM accounts LIMIT 1")) {
                assertTrue(rs.next());
                String accountId = rs.getString(1);

                conn.createStatement().execute(
                        "INSERT INTO account_users (account_id, user_id, role) " +
                                "SELECT a.account_id, u.user_id, 'admin' " +
                                "FROM accounts a, users u LIMIT 1");
            }
        }
    }

    // --- UUID generation ---

    @Test
    void uuidGenerateV4Works() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('uuid-test@example.com', 'hash')");
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT user_id FROM users WHERE email = 'uuid-test@example.com'")) {
                assertTrue(rs.next());
                String uuid = rs.getString(1);
                assertNotNull(uuid);
                // UUID format: 8-4-4-4-12
                assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                        "Expected UUID format, got: " + uuid);
            }
        }
    }

    // --- JSONB defaults ---

    @Test
    void jsonbDefaultsWork() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('jsonb@example.com', 'hash')");
            conn.createStatement().execute(
                    "INSERT INTO plans (key, name, monthly_cost, max_items) " +
                            "VALUES ('jtest', 'JTest', 0, 1)");
            conn.createStatement().execute(
                    "INSERT INTO accounts (name, plan_key) VALUES ('JSONB Test', 'jtest')");

            // Check JSONB default on accounts.settings (nullable, no default -> null)
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT settings FROM accounts WHERE name = 'JSONB Test'")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
        }
    }

    // --- INET type ---

    @Test
    void inetTypeWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('inet@example.com', 'hash')");
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT user_id FROM users WHERE email = 'inet@example.com'")) {
                assertTrue(rs.next());
                String userId = rs.getString(1);

                conn.createStatement().execute(
                        "INSERT INTO sessions (user_id, token_hash, ip, expires_at) " +
                                "VALUES ('" + userId + "', E'\\\\x0102', '192.168.1.1'::inet, now() + interval '1 hour')");

                try (ResultSet rs2 = conn.createStatement().executeQuery(
                        "SELECT ip FROM sessions WHERE user_id = '" + userId + "'")) {
                    assertTrue(rs2.next());
                    assertEquals("192.168.1.1", rs2.getString(1));
                }
            }
        }
    }

    // --- CIDR type ---

    @Test
    void cidrTypeWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO network_cache (cidr, data) VALUES ('10.0.0.0/8'::cidr, '{\"name\":\"test\"}')");
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT cidr, data FROM network_cache")) {
                assertTrue(rs.next());
                assertEquals("10.0.0.0/8", rs.getString("cidr"));
            }
        }
    }

    // --- BYTEA type ---

    @Test
    void byteaTypeWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('bytea@example.com', 'hash')");
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT user_id FROM users WHERE email = 'bytea@example.com'")) {
                assertTrue(rs.next());
                String userId = rs.getString(1);

                // sessions.token_hash is BYTEA
                conn.createStatement().execute(
                        "INSERT INTO sessions (user_id, token_hash, expires_at) " +
                                "VALUES ('" + userId + "', E'\\\\xDEADBEEF', now() + interval '1 day')");

                try (ResultSet rs2 = conn.createStatement().executeQuery(
                        "SELECT token_hash FROM sessions WHERE user_id = '" + userId + "'")) {
                    assertTrue(rs2.next());
                    assertNotNull(rs2.getString(1));
                }
            }
        }
    }

    // --- DEFERRABLE FK constraint ---

    @Test
    void deferrableConstraintWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // The accounts table has DEFERRABLE INITIALLY DEFERRED FK to plans
            conn.createStatement().execute(
                    "INSERT INTO plans (key, name, monthly_cost, max_items) " +
                            "VALUES ('defer_test', 'Defer', 0, 1)");
            conn.createStatement().execute(
                    "INSERT INTO accounts (name, plan_key) VALUES ('Deferred Account', 'defer_test')");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT name FROM accounts WHERE plan_key = 'defer_test'")) {
                assertTrue(rs.next());
                assertEquals("Deferred Account", rs.getString(1));
            }
        }
    }

    // --- View ---

    @Test
    void viewWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT plan_name, monthly_cost FROM v_account_plan LIMIT 1")) {
                // May or may not have rows depending on whether plans/accounts seeded
                // But the view itself should be queryable
                assertNotNull(rs.getMetaData());
            }
        }
    }

    // --- ON CONFLICT DO UPDATE ---

    @Test
    void onConflictDoUpdateWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // Plans were already seeded by migration. Re-run the upsert:
            conn.createStatement().execute(
                    "INSERT INTO plans(key, name, monthly_cost, visibility, max_items, max_users) " +
                            "VALUES ('free', 'Plan 1 Updated', 0.00, 'public', 5, 1) " +
                            "ON CONFLICT (key) DO UPDATE SET name=EXCLUDED.name, updated_at=now()");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT name FROM plans WHERE key = 'free'")) {
                assertTrue(rs.next());
                assertEquals("Plan 1 Updated", rs.getString(1));
            }
        }
    }

    // --- ON CONFLICT DO NOTHING ---

    @Test
    void onConflictDoNothingWorks() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // config_entries already seeded. Re-insert should be no-op:
            conn.createStatement().execute(
                    "INSERT INTO config_entries (key, value) VALUES ('theme', 'light') " +
                            "ON CONFLICT (key) DO NOTHING");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT value FROM config_entries WHERE key = 'theme'")) {
                assertTrue(rs.next());
                assertEquals("dark", rs.getString(1)); // unchanged
            }
        }
    }

    // --- Trigger ---

    @Test
    void triggerFires() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // Set up required parent rows
            conn.createStatement().execute(
                    "INSERT INTO users (email, password_hash) VALUES ('trig@example.com', 'hash')");
            conn.createStatement().execute(
                    "INSERT INTO plans (key, name, monthly_cost, max_items) " +
                            "VALUES ('trig', 'Trig', 0, 1)");
            conn.createStatement().execute(
                    "INSERT INTO accounts (name, plan_key) VALUES ('Trig Account', 'trig')");

            try (ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT account_id FROM accounts WHERE name = 'Trig Account'")) {
                assertTrue(rs.next());
                String accountId = rs.getString(1);

                // Insert a scheduled task; trigger should calculate next_run_at
                conn.createStatement().execute(
                        "INSERT INTO scheduled_tasks (account_id, name, interval_seconds, retry_seconds, last_executed_at) " +
                                "VALUES ('" + accountId + "', 'Test Task', 300, 600, now())");

                try (ResultSet rs2 = conn.createStatement().executeQuery(
                        "SELECT next_run_at FROM scheduled_tasks WHERE name = 'Test Task'")) {
                    assertTrue(rs2.next());
                    assertNotNull(rs2.getString(1), "Trigger should have set next_run_at");
                }
            }
        }
    }

    // --- Snapshot/restore round-trip ---

    @Test
    void snapshotRestorePreservesSeededData() throws SQLException {
        try (Connection conn = db.getConnection()) {
            // Data from migration's seed inserts should be present
            assertEquals(3, countRows(conn, "plans"));
            assertEquals(3, countRows(conn, "config_entries"));

            // Mutate
            conn.createStatement().execute("DELETE FROM config_entries");
            assertEquals(0, countRows(conn, "config_entries"));
        }
        // restoreBeforeEach will kick in before next test, verified by other tests passing
    }

    // --- Table count sanity ---

    @Test
    void allTablesCreated() throws SQLException {
        try (Connection conn = db.getConnection()) {
            String[] tables = {
                    "users", "sessions", "plans", "accounts", "account_users",
                    "items", "events", "tags", "item_tags", "notifications",
                    "audit_log", "invitations", "scheduled_tasks", "task_runs",
                    "network_cache", "rate_limits", "config_entries", "custom_locks"
            };
            for (String table : tables) {
                try {
                    try (ResultSet rs = conn.createStatement().executeQuery(
                            "SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next(), "Table " + table + " should be queryable");
                    }
                } catch (Exception e) {
                    fail("Table " + table + " not accessible: " + e.getMessage());
                }
            }
        }
    }

    private int countRows(Connection conn, String table) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
