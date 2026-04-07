package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Mixed-application-version database compatibility scenarios from:
 * 1320_mixed_app_version_database_compatibility_scenarios.md
 *
 * Covers: additive column migration, new-column defaults, view-based
 * compatibility layers, backward-compatible renames, new table additions,
 * enum value additions, index additions, constraint relaxation, function
 * signature changes, and trigger additions, all without breaking existing
 * queries that represent the "old app".
 *
 * Table prefix: mav_
 * All tests share a single autocommit=true connection.
 */
class MixedAppVersionCompatTest {

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
        if (conn != null && !conn.isClosed()) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. Additive column migration: old SELECT specific columns still works
    // =========================================================================

    @Test
    void testAdditiveColumnMigration() throws Exception {
        try (Statement s = conn.createStatement()) {
            // "Old schema": table with id and name
            s.execute("CREATE TABLE mav_products (id SERIAL PRIMARY KEY, name text NOT NULL)");
            s.execute("INSERT INTO mav_products (name) VALUES ('widget'), ('gadget')");
        }

        // "New migration": add a description column
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE mav_products ADD COLUMN IF NOT EXISTS description text");
        }

        // "Old app" query, which only selects columns it knows about
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM mav_products ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("widget", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals("gadget", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // 2. New column with default: old INSERT without new column gets default
    // =========================================================================

    @Test
    void testNewColumnWithDefault() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mav_orders (
                        id SERIAL PRIMARY KEY,
                        item text NOT NULL
                    )
                    """);
        }

        // Migration: add status column with a default value
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE mav_orders ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending'");
        }

        // Old app INSERT with no mention of status
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO mav_orders (item) VALUES ('book')");
        }

        // Verify the default was applied
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT status FROM mav_orders WHERE item = 'book'")) {
            assertTrue(rs.next());
            assertEquals("pending", rs.getString(1),
                    "New column should receive its default value when old app omits it");
        }
    }

    // =========================================================================
    // 3. View-based compatibility layer: view hides schema changes from old app
    // =========================================================================

    @Test
    void testViewBasedCompatibilityLayer() throws Exception {
        try (Statement s = conn.createStatement()) {
            // New schema: name split into first_name / last_name
            s.execute("""
                    CREATE TABLE mav_users_v2 (
                        id SERIAL PRIMARY KEY,
                        first_name text,
                        last_name text,
                        email text
                    )
                    """);
            s.execute("INSERT INTO mav_users_v2 (first_name, last_name, email) VALUES ('Ada', 'Lovelace', 'ada@example.com')");

            // Compatibility view that presents the old "name" column
            s.execute("""
                    CREATE VIEW mav_users_compat AS
                        SELECT id,
                               first_name || ' ' || last_name AS name,
                               email
                        FROM mav_users_v2
                    """);
        }

        // Old app queries the compatibility view using the old column name
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, email FROM mav_users_compat WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("Ada Lovelace", rs.getString("name"),
                    "Compatibility view must expose composed 'name' column");
            assertEquals("ada@example.com", rs.getString("email"));
        }
    }

    // =========================================================================
    // 4. Backward-compatible column rename: old column still readable
    // =========================================================================

    @Test
    void testBackwardCompatibleColumnRename() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mav_invoices (id SERIAL PRIMARY KEY, amount_cents int)");
            s.execute("INSERT INTO mav_invoices (amount_cents) VALUES (1999)");
        }

        // Migration: add new column name, backfill, keep old column
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE mav_invoices ADD COLUMN IF NOT EXISTS total_cents int");
            s.execute("UPDATE mav_invoices SET total_cents = amount_cents");
        }

        // Old app still reads amount_cents
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT amount_cents FROM mav_invoices WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(1999, rs.getInt(1),
                    "Old column name must still be readable after backward-compatible rename");
        }

        // New app reads total_cents
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT total_cents FROM mav_invoices WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(1999, rs.getInt(1), "New column must contain the backfilled value");
        }
    }

    // =========================================================================
    // 5. New table addition: old app unaffected by new tables
    // =========================================================================

    @Test
    void testNewTableAddition() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Old table the old app uses
            s.execute("CREATE TABLE mav_catalog (id SERIAL PRIMARY KEY, sku text)");
            s.execute("INSERT INTO mav_catalog (sku) VALUES ('SKU-001')");

            // New table added by new deployment
            s.execute("CREATE TABLE mav_catalog_metadata (sku text PRIMARY KEY, tags text[])");
            s.execute("INSERT INTO mav_catalog_metadata VALUES ('SKU-001', ARRAY['featured','sale'])");
        }

        // Old app queries only the old table and must work as before
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT sku FROM mav_catalog WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("SKU-001", rs.getString(1), "Old app query must be unaffected by new table");
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // 6. Enum value addition: old values still queryable
    // =========================================================================

    @Test
    void testEnumValueAddition() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mav_order_status AS ENUM ('pending', 'shipped')");
            s.execute("""
                    CREATE TABLE mav_enum_orders (
                        id SERIAL PRIMARY KEY,
                        status mav_order_status NOT NULL DEFAULT 'pending'
                    )
                    """);
            s.execute("INSERT INTO mav_enum_orders DEFAULT VALUES");
        }

        // Migration: add new enum value
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TYPE mav_order_status ADD VALUE IF NOT EXISTS 'delivered'");
        }

        // Old app queries using old enum values must still work
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id FROM mav_enum_orders WHERE status = 'pending'")) {
            assertTrue(rs.next(), "Old app must still be able to query with old enum value 'pending'");
        }

        // New app can insert and query with the new enum value
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO mav_enum_orders (status) VALUES ('delivered')");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM mav_enum_orders WHERE status = 'delivered'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
        }
    }

    // =========================================================================
    // 7. Index addition: doesn't break existing queries
    // =========================================================================

    @Test
    void testIndexAddition() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mav_events (id SERIAL PRIMARY KEY, event_type text, occurred_at timestamptz DEFAULT now())");
            s.execute("INSERT INTO mav_events (event_type) VALUES ('login'), ('logout'), ('login')");
        }

        // Migration: add index on event_type
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE INDEX IF NOT EXISTS mav_events_type_idx ON mav_events (event_type)");
        }

        // Old app query must return the same results after index creation
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM mav_events WHERE event_type = 'login'")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1),
                    "Query results must be identical after adding an index");
        }
    }

    // =========================================================================
    // 8. Constraint relaxation: removing constraint doesn't break old app
    // =========================================================================

    @Test
    void testConstraintRelaxation() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mav_profiles (
                        id SERIAL PRIMARY KEY,
                        username text NOT NULL,
                        bio text NOT NULL
                    )
                    """);
            s.execute("INSERT INTO mav_profiles (username, bio) VALUES ('alice', 'developer')");
        }

        // Migration: relax bio from NOT NULL to nullable
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE mav_profiles ALTER COLUMN bio DROP NOT NULL");
        }

        // Old app INSERT (with bio) still works
        try (Statement s = conn.createStatement()) {
            assertDoesNotThrow(
                    () -> s.execute("INSERT INTO mav_profiles (username, bio) VALUES ('bob', 'designer')"),
                    "Old app INSERT with bio must still work after constraint relaxation");
        }

        // New app INSERT (without bio) now also works
        try (Statement s = conn.createStatement()) {
            assertDoesNotThrow(
                    () -> s.execute("INSERT INTO mav_profiles (username) VALUES ('charlie')"),
                    "New app INSERT without bio must work after constraint is relaxed");
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM mav_profiles")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
        }
    }

    // =========================================================================
    // 9. Function signature change: CREATE OR REPLACE with compatible signature
    // =========================================================================

    @Test
    void testFunctionSignatureChange() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Original function: single argument
            s.execute("""
                    CREATE OR REPLACE FUNCTION mav_greet(name text)
                    RETURNS text
                    LANGUAGE sql
                    AS $$ SELECT 'Hello, ' || name || '!' $$
                    """);
        }

        // Old app call
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT mav_greet('World')")) {
            assertTrue(rs.next());
            assertEquals("Hello, World!", rs.getString(1));
        }

        // Migration: replace function with optional second argument (default)
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE OR REPLACE FUNCTION mav_greet(name text, greeting text DEFAULT 'Hello')
                    RETURNS text
                    LANGUAGE sql
                    AS $$ SELECT greeting || ', ' || name || '!' $$
                    """);
        }

        // Old app single-arg call must still work
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT mav_greet('World')")) {
            assertTrue(rs.next());
            assertEquals("Hello, World!", rs.getString(1),
                    "Old single-arg call must still work after backward-compatible signature change");
        }

        // New app two-arg call also works
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT mav_greet('World', 'Hi')")) {
            assertTrue(rs.next());
            assertEquals("Hi, World!", rs.getString(1));
        }
    }

    // =========================================================================
    // 10. Trigger addition: doesn't break old INSERT patterns
    // =========================================================================

    @Test
    void testTriggerAddition() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mav_audit_target (
                        id SERIAL PRIMARY KEY,
                        val text
                    )
                    """);
            s.execute("""
                    CREATE TABLE mav_audit_log (
                        id SERIAL PRIMARY KEY,
                        target_id int,
                        action text,
                        logged_at timestamptz DEFAULT now()
                    )
                    """);

            // Migration: add an audit trigger
            s.execute("""
                    CREATE OR REPLACE FUNCTION mav_audit_trigger_fn()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        INSERT INTO mav_audit_log (target_id, action) VALUES (NEW.id, TG_OP);
                        RETURN NEW;
                    END;
                    $$
                    """);
            s.execute("""
                    CREATE TRIGGER mav_audit_trigger
                    AFTER INSERT ON mav_audit_target
                    FOR EACH ROW EXECUTE FUNCTION mav_audit_trigger_fn()
                    """);
        }

        // Old app INSERT must still work transparently
        try (Statement s = conn.createStatement()) {
            assertDoesNotThrow(
                    () -> s.execute("INSERT INTO mav_audit_target (val) VALUES ('record-one')"),
                    "Old app INSERT pattern must work even with trigger present");
        }

        // Trigger must have fired and created an audit log entry
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT action FROM mav_audit_log LIMIT 1")) {
            assertTrue(rs.next(), "Trigger must have inserted an audit log row");
            assertEquals("INSERT", rs.getString(1));
        }
    }
}
