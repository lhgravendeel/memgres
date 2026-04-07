package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for INSERT into tables with UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
 * where the PK value is auto-generated (not provided by the caller), and then
 * used as a foreign key target by a subsequent INSERT.
 *
 * This is the standard pattern for multi-table inserts in modern apps:
 *   1. INSERT INTO users (email, ...) VALUES (?, ...)  (PK auto-generated)
 *   2. Retrieve the generated user_id (via RETURNING or getGeneratedKeys)
 *   3. INSERT INTO accounts (...) VALUES (...)  (explicit or auto PK)
 *   4. INSERT INTO account_users (account_id, user_id, ...) (FK to both)
 *
 * The bug scenario: step 4 fails with FK violation because the user row
 * from step 1 isn't visible, even though the generated key was returned.
 * This could be caused by:
 *   - uuid_generate_v4() DEFAULT not being evaluated during INSERT
 *   - The generated UUID not matching what's stored in the row
 *   - The row not being persisted before the FK check in step 4
 */
class UuidDefaultForeignKeyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Core scenario: user + account + junction table with auto-generated UUIDs
    // =========================================================================

    @Test
    void testAutoUuidInsertThenFkJunction() throws SQLException {
        exec("""
            CREATE TABLE test_users (
                user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE,
                pass_hash TEXT NOT NULL,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);
        exec("""
            CREATE TABLE test_accounts (
                account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);
        exec("""
            CREATE TABLE test_account_users (
                account_id UUID NOT NULL REFERENCES test_accounts (account_id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES test_users (user_id) ON DELETE CASCADE,
                role TEXT NOT NULL DEFAULT 'member',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (account_id, user_id)
            )
        """);

        // Step 1: Insert user WITHOUT specifying user_id; relies on DEFAULT uuid_generate_v4()
        String userId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test_users (email, pass_hash, verified) VALUES (?, ?, ?) RETURNING user_id")) {
            ps.setString(1, "alice@example.com");
            ps.setString(2, "$2a$10$fakehashvalue");
            ps.setBoolean(3, false);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                userId = rs.getString(1);
                assertNotNull(userId, "Must return generated UUID");
                // Verify it's a valid UUID
                assertDoesNotThrow(() -> UUID.fromString(userId));
            }
        }

        // Step 2: Insert account with explicit account_id
        String accountId = UUID.randomUUID().toString();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test_accounts (account_id, name) VALUES (?::uuid, ?)")) {
            ps.setString(1, accountId);
            ps.setString(2, "Test Corp");
            assertEquals(1, ps.executeUpdate());
        }

        // Step 3: Insert junction row; this MUST find both the user and the account
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test_account_users (account_id, user_id, role) VALUES (?::uuid, ?::uuid, ?)")) {
            ps.setString(1, accountId);
            ps.setString(2, userId);
            ps.setString(3, "owner");
            assertEquals(1, ps.executeUpdate());
        }

        // Verify the full chain
        assertEquals("alice@example.com", query1(
                "SELECT u.email FROM test_users u " +
                "JOIN test_account_users au ON au.user_id = u.user_id " +
                "JOIN test_accounts a ON a.account_id = au.account_id " +
                "WHERE a.name = 'Test Corp'"));
    }

    // =========================================================================
    // Same pattern with gen_random_uuid() instead of uuid_generate_v4()
    // =========================================================================

    @Test
    void testGenRandomUuidInsertThenFk() throws SQLException {
        exec("""
            CREATE TABLE gr_parents (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL
            )
        """);
        exec("""
            CREATE TABLE gr_children (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                parent_id UUID NOT NULL REFERENCES gr_parents (id),
                label TEXT
            )
        """);

        String parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gr_parents (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent_row");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                parentId = rs.getString(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gr_children (parent_id, label) VALUES (?::uuid, ?)")) {
            ps.setString(1, parentId);
            ps.setString(2, "child_row");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // getGeneratedKeys path (no explicit RETURNING)
    // =========================================================================

    @Test
    void testAutoUuidViaGetGeneratedKeys() throws SQLException {
        exec("""
            CREATE TABLE ggk_main (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                val TEXT NOT NULL
            )
        """);
        exec("""
            CREATE TABLE ggk_ref (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                main_id UUID NOT NULL REFERENCES ggk_main (id),
                info TEXT
            )
        """);

        String mainId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ggk_main (val) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "generated_key_row");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                mainId = keys.getString(1);
                assertNotNull(mainId);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO ggk_ref (main_id, info) VALUES (?::uuid, ?)")) {
            ps.setString(1, mainId);
            ps.setString(2, "ref_row");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // Verify the DEFAULT UUID is actually stored (SELECT by generated key)
    // =========================================================================

    @Test
    void testAutoUuidSelectByGeneratedKey() throws SQLException {
        exec("CREATE TABLE uuid_verify (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), data TEXT)");

        String generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO uuid_verify (data) VALUES (?) RETURNING id")) {
            ps.setString(1, "verify_me");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = rs.getString(1);
            }
        }

        // The stored row MUST be findable by the returned UUID
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT data FROM uuid_verify WHERE id = ?::uuid")) {
            ps.setString(1, generatedId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Row must be findable by its generated UUID");
                assertEquals("verify_me", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Multiple users, one account, junction table
    // =========================================================================

    @Test
    void testMultipleUsersOneAccount() throws SQLException {
        exec("""
            CREATE TABLE mu_users (
                user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE
            )
        """);
        exec("""
            CREATE TABLE mu_accounts (
                account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT NOT NULL
            )
        """);
        exec("""
            CREATE TABLE mu_memberships (
                account_id UUID NOT NULL REFERENCES mu_accounts (account_id),
                user_id UUID NOT NULL REFERENCES mu_users (user_id),
                role TEXT NOT NULL,
                PRIMARY KEY (account_id, user_id)
            )
        """);

        // Insert 3 users
        String[] userIds = new String[3];
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO mu_users (email) VALUES (?) RETURNING user_id")) {
            for (int i = 0; i < 3; i++) {
                ps.setString(1, "user" + i + "@test.com");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    userIds[i] = rs.getString(1);
                }
            }
        }

        // Insert account
        String accountId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO mu_accounts (name) VALUES (?) RETURNING account_id")) {
            ps.setString(1, "Shared Account");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                accountId = rs.getString(1);
            }
        }

        // Add all 3 users to the account
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO mu_memberships (account_id, user_id, role) VALUES (?::uuid, ?::uuid, ?)")) {
            String[] roles = {"owner", "admin", "member"};
            for (int i = 0; i < 3; i++) {
                ps.setString(1, accountId);
                ps.setString(2, userIds[i]);
                ps.setString(3, roles[i]);
                assertEquals(1, ps.executeUpdate());
            }
        }

        assertEquals("3", query1("SELECT COUNT(*) FROM mu_memberships WHERE account_id = '" + accountId + "'::uuid"));
    }

    // =========================================================================
    // UUID default with additional columns having defaults
    // =========================================================================

    @Test
    void testManyDefaultColumns() throws SQLException {
        exec("""
            CREATE TABLE many_defaults (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE,
                pass_hash TEXT NOT NULL,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                verify_code TEXT,
                verify_count INT NOT NULL DEFAULT 0,
                login_required BOOLEAN NOT NULL DEFAULT FALSE,
                login_code TEXT,
                login_expires TIMESTAMPTZ,
                login_failures INT NOT NULL DEFAULT 0,
                max_rate INT NOT NULL DEFAULT 10,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);

        // Insert with only required columns; all defaults must fire
        String id;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO many_defaults (email, pass_hash, verified, verify_code, login_required) " +
                "VALUES (?, ?, ?, ?, ?) RETURNING id")) {
            ps.setString(1, "test@example.com");
            ps.setString(2, "$hash$");
            ps.setBoolean(3, false);
            ps.setString(4, "ABC123");
            ps.setBoolean(5, false);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                id = rs.getString(1);
                assertNotNull(id);
                assertDoesNotThrow(() -> UUID.fromString(id));
            }
        }

        // Verify defaults were applied
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT verified, verify_count, login_required, login_failures, max_rate " +
                "FROM many_defaults WHERE id = ?::uuid")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));  // verified
                assertEquals(0, rs.getInt(2));   // verify_count
                assertFalse(rs.getBoolean(3));  // login_required
                assertEquals(0, rs.getInt(4));   // login_failures
                assertEquals(10, rs.getInt(5));  // max_rate
            }
        }
    }

    // =========================================================================
    // Deferred FK constraint with UUID
    // =========================================================================

    @Test
    void testDeferredFkWithUuid() throws SQLException {
        exec("CREATE TABLE def_plans (key TEXT PRIMARY KEY)");
        exec("INSERT INTO def_plans (key) VALUES ('free'), ('pro')");
        exec("""
            CREATE TABLE def_accounts (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT NOT NULL,
                plan_key TEXT NOT NULL DEFAULT 'free',
                CONSTRAINT fk_plan FOREIGN KEY (plan_key) REFERENCES def_plans (key) DEFERRABLE INITIALLY DEFERRED
            )
        """);

        // Deferred FK: can insert account first, plan check happens at commit
        conn.setAutoCommit(false);
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO def_accounts (name, plan_key) VALUES (?, ?) RETURNING id")) {
                ps.setString(1, "Deferred Corp");
                ps.setString(2, "free");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertNotNull(rs.getString(1));
                }
            }
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }

        assertEquals("1", query1("SELECT COUNT(*) FROM def_accounts"));
    }
}
