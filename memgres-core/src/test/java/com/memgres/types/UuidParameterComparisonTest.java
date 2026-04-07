package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for UUID parameter handling in the extended query protocol.
 *
 * When a Java UUID is sent via PreparedStatement, the PG JDBC driver can
 * send it in several ways depending on how the parameter is set:
 *   - setObject(idx, uuid)              → OID 2950 (uuid), text format
 *   - setObject(idx, uuid, Types.OTHER) → OID 2950 (uuid), text format
 *   - setString(idx, uuid.toString())   → OID 1043 (varchar), text format
 *
 * The server must correctly compare the incoming parameter against stored
 * UUID values regardless of how the parameter was sent. Specifically,
 * WHERE uuid_col = $1 must work whether $1 is sent as UUID type or as
 * a text string.
 *
 * The critical scenario: INSERT with DEFAULT uuid_generate_v4() generates
 * a UUID, RETURNING sends it back. The same UUID sent back as a parameter
 * in WHERE uuid_col = ? MUST find the row.
 */
class UuidParameterComparisonTest {

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
    // Round-trip: INSERT (auto UUID) → RETURNING → SELECT WHERE pk = ?
    // Using java.util.UUID (setObject)
    // =========================================================================

    @Test
    void testRoundTripUuidViaSetObject() throws SQLException {
        exec("CREATE TABLE rt_obj (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), val TEXT)");

        // INSERT without explicit id, get generated UUID as java.util.UUID
        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rt_obj (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "round_trip_setObject");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = (UUID) rs.getObject(1);
                assertNotNull(generatedId);
            }
        }

        // SELECT by that UUID using setObject(idx, uuid), no explicit cast
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM rt_obj WHERE id = ?")) {
            ps.setObject(1, generatedId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by UUID via setObject(idx, uuid)");
                assertEquals("round_trip_setObject", rs.getString(1));
            }
        }
    }

    @Test
    void testRoundTripUuidViaSetObjectTypesOther() throws SQLException {
        exec("CREATE TABLE rt_other (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), val TEXT)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rt_other (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "round_trip_OTHER");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = (UUID) rs.getObject(1);
            }
        }

        // SELECT using setObject with explicit Types.OTHER
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM rt_other WHERE id = ?")) {
            ps.setObject(1, generatedId, Types.OTHER);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by UUID via setObject(idx, uuid, Types.OTHER)");
                assertEquals("round_trip_OTHER", rs.getString(1));
            }
        }
    }

    @Test
    void testRoundTripUuidViaSetString() throws SQLException {
        exec("CREATE TABLE rt_str (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), val TEXT)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rt_str (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "round_trip_setString");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = (UUID) rs.getObject(1);
            }
        }

        // SELECT using setString with UUID.toString(), no cast in SQL
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM rt_str WHERE id = ?")) {
            ps.setString(1, generatedId.toString());
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by UUID string via setString without ::uuid cast");
                assertEquals("round_trip_setString", rs.getString(1));
            }
        }
    }

    @Test
    void testRoundTripUuidViaSetStringWithCast() throws SQLException {
        exec("CREATE TABLE rt_cast (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), val TEXT)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rt_cast (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "round_trip_cast");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = (UUID) rs.getObject(1);
            }
        }

        // SELECT using setString WITH explicit ::uuid cast
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM rt_cast WHERE id = ?::uuid")) {
            ps.setString(1, generatedId.toString());
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by UUID string with ::uuid cast");
                assertEquals("round_trip_cast", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Same with gen_random_uuid()
    // =========================================================================

    @Test
    void testRoundTripGenRandomUuid() throws SQLException {
        exec("CREATE TABLE rt_gen (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), val TEXT)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO rt_gen (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "gen_random");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                generatedId = (UUID) rs.getObject(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM rt_gen WHERE id = ?")) {
            ps.setObject(1, generatedId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by gen_random_uuid() via setObject");
                assertEquals("gen_random", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // getGeneratedKeys path: retrieving auto-generated UUID PK
    // =========================================================================

    @Test
    void testGetGeneratedKeysUuidThenSelectById() throws SQLException {
        exec("CREATE TABLE gku_lookup (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), email TEXT NOT NULL)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gku_lookup (email) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "test@test.com");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                generatedId = (UUID) keys.getObject(1);
                assertNotNull(generatedId);
            }
        }

        // Find by UUID
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT email FROM gku_lookup WHERE id = ?")) {
            ps.setObject(1, generatedId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by UUID from getGeneratedKeys via setObject");
                assertEquals("test@test.com", rs.getString(1));
            }
        }
    }

    @Test
    void testGetGeneratedKeysNamedColumnThenSelectById() throws SQLException {
        exec("CREATE TABLE gkn_lookup (user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), email TEXT NOT NULL)");

        UUID generatedId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gkn_lookup (email) VALUES (?)",
                new String[]{"user_id"})) {
            ps.setString(1, "named@test.com");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                generatedId = (UUID) keys.getObject(1);
                assertNotNull(generatedId);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT email FROM gkn_lookup WHERE user_id = ?")) {
            ps.setObject(1, generatedId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by named generated UUID column");
                assertEquals("named@test.com", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // UUID comparison on non-PK column
    // =========================================================================

    @Test
    void testUuidComparisonNonPkColumn() throws SQLException {
        exec("CREATE TABLE uuid_nonpk (id serial PRIMARY KEY, ref_id UUID NOT NULL, label TEXT)");
        UUID refId = UUID.randomUUID();
        exec("INSERT INTO uuid_nonpk (ref_id, label) VALUES ('" + refId + "', 'find_me')");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT label FROM uuid_nonpk WHERE ref_id = ?")) {
            ps.setObject(1, refId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by non-PK UUID column via setObject");
                assertEquals("find_me", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // UUID in JOIN ON clause
    // =========================================================================

    @Test
    void testUuidParameterInJoinCondition() throws SQLException {
        exec("CREATE TABLE uuid_join_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE uuid_join_child (id serial PRIMARY KEY, parent_id UUID REFERENCES uuid_join_parent(id), info TEXT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO uuid_join_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "joinable_parent");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                parentId = (UUID) rs.getObject(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO uuid_join_child (parent_id, info) VALUES (?, ?)")) {
            ps.setObject(1, parentId);
            ps.setString(2, "child_info");
            ps.executeUpdate();
        }

        // Join query with UUID parameter
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT c.info FROM uuid_join_child c JOIN uuid_join_parent p ON p.id = c.parent_id WHERE p.id = ?")) {
            ps.setObject(1, parentId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find joined row via UUID parameter in JOIN");
                assertEquals("child_info", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // UUID case sensitivity (lowercase vs uppercase)
    // =========================================================================

    @Test
    void testUuidCaseInsensitive() throws SQLException {
        exec("CREATE TABLE uuid_case (id UUID PRIMARY KEY, val TEXT)");
        UUID testId = UUID.randomUUID();
        exec("INSERT INTO uuid_case (id, val) VALUES ('" + testId + "', 'case_test')");

        // Send uppercase UUID string
        try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM uuid_case WHERE id = ?")) {
            ps.setString(1, testId.toString().toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "UUID comparison must be case-insensitive for text input");
            }
        }

        // Send lowercase UUID string
        try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM uuid_case WHERE id = ?")) {
            ps.setString(1, testId.toString().toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "UUID comparison must work with lowercase too");
            }
        }
    }

    // =========================================================================
    // INSERT subset of columns → SELECT * WHERE pk = ?
    // =========================================================================

    @Test
    void testDaoPatternInsertSubsetSelectByPk() throws SQLException {
        exec("""
            CREATE TABLE dao_users (
                user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE,
                pass_hash TEXT NOT NULL,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                verify_code TEXT,
                login_required BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);

        // INSERT only some columns (auto-PK, other columns have defaults)
        UUID userId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO dao_users (email, pass_hash, verified, verify_code, login_required) " +
                "VALUES (?, ?, ?, ?, ?) RETURNING user_id")) {
            ps.setString(1, "dao@example.com");
            ps.setString(2, "$2a$10$somehashvalue");
            ps.setBoolean(3, false);
            ps.setString(4, "VERIFY123");
            ps.setBoolean(5, false);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                userId = (UUID) rs.getObject(1);
                assertNotNull(userId, "RETURNING must give a valid UUID");
            }
        }

        // SELECT * WHERE user_id = ? using the returned UUID
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM dao_users WHERE user_id = ?")) {
            ps.setObject(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SELECT by PK with setObject(UUID) must find the inserted row");
                assertEquals("dao@example.com", rs.getString("email"));
                assertFalse(rs.getBoolean("verified"));
                assertEquals("VERIFY123", rs.getString("verify_code"));
            }
        }

        // Also verify SELECT by email works (the workaround path)
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT user_id FROM dao_users WHERE LOWER(email) = ?")) {
            ps.setString(1, "dao@example.com");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                UUID foundId = (UUID) rs.getObject(1);
                assertEquals(userId, foundId, "UUID from email lookup must match UUID from RETURNING");
            }
        }
    }

    // =========================================================================
    // Full flow: insert user → insert account → insert junction
    // =========================================================================

    @Test
    void testFullDaoAccountFlow() throws SQLException {
        exec("""
            CREATE TABLE jf_users (
                user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE,
                pass_hash TEXT NOT NULL,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                verify_code TEXT,
                login_required BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);
        exec("""
            CREATE TABLE jf_accounts (
                account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """);
        exec("""
            CREATE TABLE jf_account_users (
                account_id UUID NOT NULL REFERENCES jf_accounts (account_id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES jf_users (user_id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (account_id, user_id)
            )
        """);

        // Step 1: Insert user (subset of cols, auto PK)
        UUID userId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jf_users (email, pass_hash, verified, verify_code, login_required) " +
                "VALUES (?, ?, ?, ?, ?) RETURNING user_id")) {
            ps.setString(1, "flow@example.com");
            ps.setString(2, "$hash$");
            ps.setBoolean(3, false);
            ps.setString(4, "CODE");
            ps.setBoolean(5, false);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                userId = (UUID) rs.getObject(1);
            }
        }

        // Verify findById works right after insert
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM jf_users WHERE user_id = ?")) {
            ps.setObject(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "findById must work immediately after insert");
            }
        }

        // Step 2: Insert account
        UUID accountId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jf_accounts (name) VALUES (?) RETURNING account_id")) {
            ps.setString(1, "Flow Corp");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                accountId = (UUID) rs.getObject(1);
            }
        }

        // Step 3: Insert junction; this requires both FK targets to be visible
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jf_account_users (account_id, user_id, role) VALUES (?, ?, ?)")) {
            ps.setObject(1, accountId);
            ps.setObject(2, userId);
            ps.setString(3, "owner");
            assertEquals(1, ps.executeUpdate());
        }

        // Verify full join
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT u.email, a.name, au.role
            FROM jf_account_users au
            JOIN jf_users u ON u.user_id = au.user_id
            JOIN jf_accounts a ON a.account_id = au.account_id
            WHERE au.user_id = ?
        """)) {
            ps.setObject(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("flow@example.com", rs.getString(1));
                assertEquals("Flow Corp", rs.getString(2));
                assertEquals("owner", rs.getString(3));
            }
        }
    }

    // =========================================================================
    // UUID in UPDATE WHERE clause
    // =========================================================================

    @Test
    void testUpdateByUuidParameter() throws SQLException {
        exec("CREATE TABLE uuid_upd (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), status TEXT DEFAULT 'new')");

        UUID id;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO uuid_upd DEFAULT VALUES RETURNING id")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                id = (UUID) rs.getObject(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE uuid_upd SET status = ? WHERE id = ?")) {
            ps.setString(1, "active");
            ps.setObject(2, id);
            assertEquals(1, ps.executeUpdate(), "UPDATE by UUID must affect 1 row");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT status FROM uuid_upd WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("active", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // UUID in DELETE WHERE clause
    // =========================================================================

    @Test
    void testDeleteByUuidParameter() throws SQLException {
        exec("CREATE TABLE uuid_del (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), val TEXT)");

        UUID id;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO uuid_del (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "delete_me");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                id = (UUID) rs.getObject(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM uuid_del WHERE id = ?")) {
            ps.setObject(1, id);
            assertEquals(1, ps.executeUpdate(), "DELETE by UUID must affect 1 row");
        }
    }

    // =========================================================================
    // Multiple UUID columns in WHERE
    // =========================================================================

    @Test
    void testMultipleUuidColumnsInWhere() throws SQLException {
        exec("""
            CREATE TABLE multi_uuid (
                a UUID NOT NULL,
                b UUID NOT NULL,
                val TEXT,
                PRIMARY KEY (a, b)
            )
        """);

        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO multi_uuid (a, b, val) VALUES (?, ?, ?)")) {
            ps.setObject(1, a);
            ps.setObject(2, b);
            ps.setString(3, "found");
            ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM multi_uuid WHERE a = ? AND b = ?")) {
            ps.setObject(1, a);
            ps.setObject(2, b);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find row by composite UUID PK");
                assertEquals("found", rs.getString(1));
            }
        }
    }
}
