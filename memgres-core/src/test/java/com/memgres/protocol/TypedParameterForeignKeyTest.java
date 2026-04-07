package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for FK constraint checks when parameters are sent via PreparedStatement
 * with various Java data types.
 *
 * The FK enforcement path inside the database engine must correctly compare
 * the incoming parameter value against the referenced table's PK. If the
 * comparison uses a different type path than the WHERE clause, FKs fail
 * even though the referenced row exists.
 *
 * Also covers ON CONFLICT (upsert) with typed parameters, custom ENUM casts,
 * mixed-type INSERTs, and cascade behavior with typed parameters.
 */
class TypedParameterForeignKeyTest {

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
            s.execute("CREATE TYPE member_role AS ENUM ('owner', 'admin', 'editor', 'viewer')");
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
    // 1. FK constraint check with UUID setObject
    // =========================================================================

    @Test
    void testFkUuidViaSetObject() throws SQLException {
        exec("CREATE TABLE fk_uuid_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE fk_uuid_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES fk_uuid_parent(id), info TEXT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO fk_uuid_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO fk_uuid_child (parent_id, info) VALUES (?, ?)")) {
            ps.setObject(1, parentId);
            ps.setString(2, "child_via_setObject");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 2. FK constraint check with UUID setString (no cast)
    // =========================================================================

    @Test
    void testFkUuidViaSetStringNoCast() throws SQLException {
        exec("CREATE TABLE fk_ustr_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE fk_ustr_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES fk_ustr_parent(id), info TEXT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO fk_ustr_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO fk_ustr_child (parent_id, info) VALUES (?, ?)")) {
            ps.setString(1, parentId.toString());
            ps.setString(2, "child_via_setString");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 3. Composite UUID FK: junction table with two UUID FKs via setObject
    // =========================================================================

    @Test
    void testCompositeUuidFkViaSetObject() throws SQLException {
        exec("CREATE TABLE comp_users (user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), email TEXT UNIQUE)");
        exec("CREATE TABLE comp_groups (group_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("""
            CREATE TABLE comp_membership (
                user_id UUID NOT NULL REFERENCES comp_users(user_id) ON DELETE CASCADE,
                group_id UUID NOT NULL REFERENCES comp_groups(group_id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                PRIMARY KEY (user_id, group_id)
            )
        """);

        UUID userId, groupId;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO comp_users (email) VALUES (?) RETURNING user_id")) {
            ps.setString(1, "user@test.com");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); userId = (UUID) rs.getObject(1); }
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO comp_groups (name) VALUES (?) RETURNING group_id")) {
            ps.setString(1, "admins");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); groupId = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO comp_membership (user_id, group_id, role) VALUES (?, ?, ?)")) {
            ps.setObject(1, userId);
            ps.setObject(2, groupId);
            ps.setString(3, "admin");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 4. ON CONFLICT DO UPDATE with UUID PK
    // =========================================================================

    @Test
    void testOnConflictDoUpdateUuidPk() throws SQLException {
        exec("CREATE TABLE upsert_uuid (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), val TEXT, version INT DEFAULT 1)");

        UUID id;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO upsert_uuid (val) VALUES (?) RETURNING id")) {
            ps.setString(1, "original");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); id = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO upsert_uuid (id, val, version) VALUES (?, ?, 1) " +
                "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, version = upsert_uuid.version + 1")) {
            ps.setObject(1, id);
            ps.setString(2, "updated");
            ps.executeUpdate();
        }

        assertEquals("updated", query1("SELECT val FROM upsert_uuid WHERE id = '" + id + "'"));
        assertEquals("2", query1("SELECT version FROM upsert_uuid WHERE id = '" + id + "'"));
    }

    // =========================================================================
    // 5. ON CONFLICT DO UPDATE with composite UUID PK (junction table upsert)
    // =========================================================================

    @Test
    void testOnConflictDoUpdateCompositeUuidPk() throws SQLException {
        exec("CREATE TABLE oc_p1 (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE oc_p2 (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), label TEXT)");
        exec("""
            CREATE TABLE oc_junction (
                p1_id UUID NOT NULL REFERENCES oc_p1(id) ON DELETE CASCADE,
                p2_id UUID NOT NULL REFERENCES oc_p2(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                PRIMARY KEY (p1_id, p2_id)
            )
        """);

        UUID p1Id, p2Id;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO oc_p1 (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "entity1");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); p1Id = (UUID) rs.getObject(1); }
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO oc_p2 (label) VALUES (?) RETURNING id")) {
            ps.setString(1, "entity2");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); p2Id = (UUID) rs.getObject(1); }
        }

        // First insert
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO oc_junction (p1_id, p2_id, role) VALUES (?, ?, ?) " +
                "ON CONFLICT (p1_id, p2_id) DO UPDATE SET role = EXCLUDED.role")) {
            ps.setObject(1, p1Id);
            ps.setObject(2, p2Id);
            ps.setString(3, "viewer");
            ps.executeUpdate();
        }

        // Upsert with same PK, different role
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO oc_junction (p1_id, p2_id, role) VALUES (?, ?, ?) " +
                "ON CONFLICT (p1_id, p2_id) DO UPDATE SET role = EXCLUDED.role")) {
            ps.setObject(1, p1Id);
            ps.setObject(2, p2Id);
            ps.setString(3, "owner");
            ps.executeUpdate();
        }

        assertEquals("owner", query1("SELECT role FROM oc_junction WHERE p1_id = '" + p1Id + "'"));
        assertEquals("1", query1("SELECT COUNT(*) FROM oc_junction"));
    }

    // =========================================================================
    // 6. Custom ENUM type via cast in PreparedStatement
    // =========================================================================

    @Test
    void testEnumCastInInsert() throws SQLException {
        exec("CREATE TABLE enum_ins (id serial PRIMARY KEY, role member_role NOT NULL)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO enum_ins (role) VALUES (?::member_role)")) {
            ps.setString(1, "admin");
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals("admin", query1("SELECT role FROM enum_ins WHERE id = 1"));
    }

    @Test
    void testEnumCastInSelect() throws SQLException {
        exec("CREATE TABLE enum_sel (id serial PRIMARY KEY, role member_role NOT NULL)");
        exec("INSERT INTO enum_sel (role) VALUES ('editor')");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM enum_sel WHERE role = ?::member_role")) {
            ps.setString(1, "editor");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void testEnumCastInOnConflictDoUpdate() throws SQLException {
        exec("""
            CREATE TABLE enum_upsert (
                account_id UUID NOT NULL,
                user_id UUID NOT NULL,
                role member_role NOT NULL,
                PRIMARY KEY (account_id, user_id)
            )
        """);

        UUID acct = UUID.randomUUID();
        UUID user = UUID.randomUUID();

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO enum_upsert (account_id, user_id, role) VALUES (?, ?, ?::member_role) " +
                "ON CONFLICT (account_id, user_id) DO UPDATE SET role = ?::member_role")) {
            ps.setObject(1, acct);
            ps.setObject(2, user);
            ps.setString(3, "viewer");
            ps.setString(4, "viewer");
            ps.executeUpdate();
        }

        // Upsert with different role
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO enum_upsert (account_id, user_id, role) VALUES (?, ?, ?::member_role) " +
                "ON CONFLICT (account_id, user_id) DO UPDATE SET role = ?::member_role")) {
            ps.setObject(1, acct);
            ps.setObject(2, user);
            ps.setString(3, "owner");
            ps.setString(4, "owner");
            ps.executeUpdate();
        }

        assertEquals("owner", query1("SELECT role FROM enum_upsert WHERE account_id = '" + acct + "'"));
    }

    // =========================================================================
    // 7. ENUM FK column
    // =========================================================================

    @Test
    void testEnumFkColumn() throws SQLException {
        exec("CREATE TABLE enum_ref_roles (role member_role PRIMARY KEY)");
        exec("INSERT INTO enum_ref_roles VALUES ('owner'), ('admin'), ('editor'), ('viewer')");
        exec("CREATE TABLE enum_ref_assignments (id serial PRIMARY KEY, role member_role REFERENCES enum_ref_roles(role), label TEXT)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO enum_ref_assignments (role, label) VALUES (?::member_role, ?)")) {
            ps.setString(1, "admin");
            ps.setString(2, "test_assignment");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 8. Mixed types in one INSERT
    // =========================================================================

    @Test
    void testMixedTypesInsert() throws SQLException {
        exec("""
            CREATE TABLE mixed_all (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE,
                score NUMERIC(10,2),
                role member_role NOT NULL DEFAULT 'viewer',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                counter INT NOT NULL DEFAULT 0,
                metadata JSONB
            )
        """);

        UUID id;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO mixed_all (name, active, score, role, counter, metadata) " +
                "VALUES (?, ?, ?, ?::member_role, ?, ?::jsonb) RETURNING id")) {
            ps.setString(1, "mixed_row");
            ps.setBoolean(2, true);
            ps.setBigDecimal(3, new BigDecimal("95.50"));
            ps.setString(4, "editor");
            ps.setInt(5, 42);
            ps.setObject(6, "{\"key\": \"value\"}", Types.OTHER);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                id = (UUID) rs.getObject(1);
            }
        }

        // Verify all types survived the round trip
        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM mixed_all WHERE id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("mixed_row", rs.getString("name"));
                assertTrue(rs.getBoolean("active"));
                assertEquals(new BigDecimal("95.50"), rs.getBigDecimal("score"));
                assertEquals("editor", rs.getString("role"));
                assertEquals(42, rs.getInt("counter"));
                assertNotNull(rs.getString("metadata"));
            }
        }
    }

    // =========================================================================
    // 9. FK check with INTEGER setInt
    // =========================================================================

    @Test
    void testFkIntegerViaSetInt() throws SQLException {
        exec("CREATE TABLE fk_int_parent (id serial PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE fk_int_child (id serial PRIMARY KEY, parent_id INT NOT NULL REFERENCES fk_int_parent(id), info TEXT)");

        int parentId;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO fk_int_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "int_parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = rs.getInt(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO fk_int_child (parent_id, info) VALUES (?, ?)")) {
            ps.setInt(1, parentId);
            ps.setString(2, "int_child");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 10. FK check with TEXT setString
    // =========================================================================

    @Test
    void testFkTextViaSetString() throws SQLException {
        exec("CREATE TABLE fk_txt_parent (code TEXT PRIMARY KEY, label TEXT)");
        exec("INSERT INTO fk_txt_parent (code, label) VALUES ('ABC', 'parent_label')");
        exec("CREATE TABLE fk_txt_child (id serial PRIMARY KEY, parent_code TEXT NOT NULL REFERENCES fk_txt_parent(code), info TEXT)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO fk_txt_child (parent_code, info) VALUES (?, ?)")) {
            ps.setString(1, "ABC");
            ps.setString(2, "text_child");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 11. FK check with TIMESTAMPTZ setTimestamp
    // =========================================================================

    @Test
    void testFkTimestampViaSetTimestamp() throws SQLException {
        exec("CREATE TABLE fk_ts_parent (event_time TIMESTAMPTZ PRIMARY KEY, name TEXT)");
        Timestamp ts = Timestamp.valueOf(LocalDateTime.of(2024, 6, 15, 10, 30, 0));
        exec("INSERT INTO fk_ts_parent (event_time, name) VALUES ('" + ts + "', 'ts_parent')");
        exec("CREATE TABLE fk_ts_child (id serial PRIMARY KEY, event_time TIMESTAMPTZ REFERENCES fk_ts_parent(event_time), info TEXT)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO fk_ts_child (event_time, info) VALUES (?, ?)")) {
            ps.setTimestamp(1, ts);
            ps.setString(2, "ts_child");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // 12. ON CONFLICT with ENUM columns
    // =========================================================================

    @Test
    void testOnConflictWithEnumInCompositeKey() throws SQLException {
        exec("""
            CREATE TABLE enum_oc_comp (
                entity_id UUID NOT NULL,
                role member_role NOT NULL,
                permissions TEXT,
                PRIMARY KEY (entity_id, role)
            )
        """);

        UUID entityId = UUID.randomUUID();

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO enum_oc_comp (entity_id, role, permissions) VALUES (?, ?::member_role, ?) " +
                "ON CONFLICT (entity_id, role) DO UPDATE SET permissions = EXCLUDED.permissions")) {
            ps.setObject(1, entityId);
            ps.setString(2, "admin");
            ps.setString(3, "read");
            ps.executeUpdate();

            // Upsert same key, different permissions
            ps.setObject(1, entityId);
            ps.setString(2, "admin");
            ps.setString(3, "read,write");
            ps.executeUpdate();
        }

        assertEquals("read,write", query1("SELECT permissions FROM enum_oc_comp WHERE entity_id = '" + entityId + "'"));
        assertEquals("1", query1("SELECT COUNT(*) FROM enum_oc_comp"));
    }

    // =========================================================================
    // 13. UPDATE FK column via UUID setObject
    // =========================================================================

    @Test
    void testUpdateFkUuidColumn() throws SQLException {
        exec("CREATE TABLE upd_fk_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE upd_fk_child (id serial PRIMARY KEY, parent_id UUID REFERENCES upd_fk_parent(id), data TEXT)");

        UUID parent1, parent2;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO upd_fk_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent_A"); try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parent1 = (UUID) rs.getObject(1); }
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO upd_fk_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent_B"); try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parent2 = (UUID) rs.getObject(1); }
        }

        // Insert child pointing to parent1
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO upd_fk_child (parent_id, data) VALUES (?, ?)")) {
            ps.setObject(1, parent1); ps.setString(2, "movable"); ps.executeUpdate();
        }

        // Update FK to point to parent2
        try (PreparedStatement ps = conn.prepareStatement("UPDATE upd_fk_child SET parent_id = ? WHERE data = ?")) {
            ps.setObject(1, parent2);
            ps.setString(2, "movable");
            assertEquals(1, ps.executeUpdate());
        }

        // Verify FK points to parent2 now
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT p.name FROM upd_fk_parent p JOIN upd_fk_child c ON c.parent_id = p.id WHERE c.data = ?")) {
            ps.setString(1, "movable");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("parent_B", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // 14. Nullable UUID FK with setNull
    // =========================================================================

    @Test
    void testNullableUuidFk() throws SQLException {
        exec("CREATE TABLE null_fk_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE null_fk_child (id serial PRIMARY KEY, parent_id UUID REFERENCES null_fk_parent(id), data TEXT)");

        // Insert child with NULL FK, which should bypass FK check
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO null_fk_child (parent_id, data) VALUES (?, ?)")) {
            ps.setNull(1, Types.OTHER);
            ps.setString(2, "orphan");
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("1", query1("SELECT COUNT(*) FROM null_fk_child WHERE parent_id IS NULL"));
    }

    // =========================================================================
    // 15. DELETE with UUID FK cascade
    // =========================================================================

    @Test
    void testDeleteUuidFkCascade() throws SQLException {
        exec("CREATE TABLE cascade_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE cascade_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES cascade_parent(id) ON DELETE CASCADE, info TEXT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO cascade_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "doomed_parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO cascade_child (parent_id, info) VALUES (?, ?)")) {
            ps.setObject(1, parentId); ps.setString(2, "child1"); ps.executeUpdate();
            ps.setObject(1, parentId); ps.setString(2, "child2"); ps.executeUpdate();
        }

        assertEquals("2", query1("SELECT COUNT(*) FROM cascade_child"));

        // Delete parent by UUID; children must cascade
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM cascade_parent WHERE id = ?")) {
            ps.setObject(1, parentId);
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("0", query1("SELECT COUNT(*) FROM cascade_child"));
    }

    // =========================================================================
    // 16. Batch INSERT into FK table with UUID parameters
    // =========================================================================

    @Test
    void testBatchInsertWithUuidFk() throws SQLException {
        exec("CREATE TABLE batch_fk_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE batch_fk_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES batch_fk_parent(id), seq INT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO batch_fk_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "batch_parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = (UUID) rs.getObject(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO batch_fk_child (parent_id, seq) VALUES (?, ?)")) {
            for (int i = 1; i <= 20; i++) {
                ps.setObject(1, parentId);
                ps.setInt(2, i);
                ps.addBatch();
            }
            int[] results = ps.executeBatch();
            assertEquals(20, results.length);
            for (int r : results) assertEquals(1, r);
        }

        assertEquals("20", query1("SELECT COUNT(*) FROM batch_fk_child"));
    }

    // =========================================================================
    // Full DAO-style flow: user → account → junction with ENUM + UUID + ON CONFLICT
    // =========================================================================

    @Test
    void testFullDaoFlowWithEnumAndOnConflict() throws SQLException {
        exec("""
            CREATE TABLE flow_users (
                user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email TEXT NOT NULL UNIQUE,
                pass_hash TEXT NOT NULL,
                verified BOOLEAN NOT NULL DEFAULT FALSE,
                verify_code TEXT,
                login_required BOOLEAN NOT NULL DEFAULT FALSE
            )
        """);
        exec("""
            CREATE TABLE flow_accounts (
                account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                name TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT TRUE
            )
        """);
        exec("""
            CREATE TABLE flow_account_users (
                account_id UUID NOT NULL REFERENCES flow_accounts(account_id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES flow_users(user_id) ON DELETE CASCADE,
                role member_role NOT NULL,
                PRIMARY KEY (account_id, user_id)
            )
        """);

        // Step 1: Insert user (auto-generated PK via RETURNING)
        UUID userId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO flow_users (email, pass_hash, verified, verify_code, login_required) " +
                "VALUES (?, ?, ?, ?, ?) RETURNING user_id")) {
            ps.setString(1, "flow@example.com");
            ps.setString(2, "$2a$hash");
            ps.setBoolean(3, false);
            ps.setString(4, "VERIFY");
            ps.setBoolean(5, false);
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); userId = (UUID) rs.getObject(1); }
        }

        // Step 2: Insert account
        UUID accountId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO flow_accounts (name) VALUES (?) RETURNING account_id")) {
            ps.setString(1, "Flow Corp");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); accountId = (UUID) rs.getObject(1); }
        }

        // Step 3: Add user to account with ON CONFLICT and ENUM cast
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO flow_account_users (account_id, user_id, role) " +
                "VALUES (?, ?, ?::member_role) " +
                "ON CONFLICT (account_id, user_id) DO UPDATE SET role = ?::member_role")) {
            ps.setObject(1, accountId);
            ps.setObject(2, userId);
            ps.setString(3, "owner");
            ps.setString(4, "owner");
            assertEquals(1, ps.executeUpdate());
        }

        // Step 4: Upsert again to change role
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO flow_account_users (account_id, user_id, role) " +
                "VALUES (?, ?, ?::member_role) " +
                "ON CONFLICT (account_id, user_id) DO UPDATE SET role = ?::member_role")) {
            ps.setObject(1, accountId);
            ps.setObject(2, userId);
            ps.setString(3, "admin");
            ps.setString(4, "admin");
            ps.executeUpdate();
        }

        // Verify
        assertEquals("admin", query1(
                "SELECT role FROM flow_account_users WHERE account_id = '" + accountId + "' AND user_id = '" + userId + "'"));
        assertEquals("1", query1("SELECT COUNT(*) FROM flow_account_users"));
    }
}
