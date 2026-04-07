package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for INSERT row visibility across sequential statements on the same connection.
 *
 * When a row is inserted (with or without RETURNING), it must be immediately
 * visible to subsequent statements on the same connection in autocommit mode.
 * This is fundamental to how every application works:
 *   1. INSERT INTO parent ... RETURNING id  → get generated key
 *   2. INSERT INTO child (parent_id) VALUES (?)  → use the key as FK
 *
 * If step 2 fails with "FK violation" or the parent row isn't found,
 * the INSERT from step 1 was not properly persisted/committed.
 *
 * This tests multiple possible root causes:
 *   - Row not persisted after RETURNING
 *   - Autocommit not actually committing between statements
 *   - Read snapshot not updated between statements
 *   - Extended protocol (PreparedStatement) vs simple protocol divergence
 *   - FK constraint checking against stale data
 *   - getGeneratedKeys() path vs explicit RETURNING path
 */
class InsertVisibilityTest {

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
    // Core scenario: INSERT RETURNING id, then FK insert using that id
    // =========================================================================

    @Test
    void testInsertReturningThenFkInsert() throws SQLException {
        exec("CREATE TABLE vis_parent (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL)");
        exec("CREATE TABLE vis_child (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), parent_id uuid NOT NULL REFERENCES vis_parent(id), data text)");

        // Step 1: Insert parent via PreparedStatement with RETURNING
        String parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "test_parent");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                parentId = rs.getString(1);
                assertNotNull(parentId);
            }
        }

        // Step 2: Insert child referencing the parent; this MUST work
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_child (parent_id, data) VALUES (?::uuid, ?) RETURNING id")) {
            ps.setString(1, parentId);
            ps.setString(2, "child_data");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }

        // Verify both rows exist
        assertEquals("1", query1("SELECT COUNT(*) FROM vis_parent"));
        assertEquals("1", query1("SELECT COUNT(*) FROM vis_child"));
    }

    @Test
    void testInsertReturningSerialThenFkInsert() throws SQLException {
        exec("CREATE TABLE vis_orders (id serial PRIMARY KEY, customer text NOT NULL)");
        exec("CREATE TABLE vis_items (id serial PRIMARY KEY, order_id int NOT NULL REFERENCES vis_orders(id), product text)");

        // Insert order, get serial ID
        int orderId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_orders (customer) VALUES (?) RETURNING id")) {
            ps.setString(1, "Alice");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                orderId = rs.getInt(1);
                assertTrue(orderId > 0);
            }
        }

        // Insert items referencing that order
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_items (order_id, product) VALUES (?, ?)")) {
            ps.setInt(1, orderId);
            ps.setString(2, "Widget");
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, orderId);
            ps.setString(2, "Gadget");
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("2", query1("SELECT COUNT(*) FROM vis_items WHERE order_id = " + orderId));
    }

    // =========================================================================
    // getGeneratedKeys() path (RETURN_GENERATED_KEYS flag)
    // =========================================================================

    @Test
    void testGetGeneratedKeysThenFkInsert() throws SQLException {
        exec("CREATE TABLE gk_accounts (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL)");
        exec("CREATE TABLE gk_users (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), account_id uuid NOT NULL REFERENCES gk_accounts(id), email text)");

        // Insert account with getGeneratedKeys
        String accountId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gk_accounts (name) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "Acme Corp");
            assertEquals(1, ps.executeUpdate());
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                accountId = keys.getString(1);
                assertNotNull(accountId);
            }
        }

        // Insert user referencing that account, which MUST see the account row
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gk_users (account_id, email) VALUES (?::uuid, ?)")) {
            ps.setString(1, accountId);
            ps.setString(2, "user@acme.com");
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("Acme Corp", query1("SELECT a.name FROM gk_accounts a JOIN gk_users u ON u.account_id = a.id WHERE u.email = 'user@acme.com'"));
    }

    @Test
    void testGetGeneratedKeysSerialThenFkInsert() throws SQLException {
        exec("CREATE TABLE gk_parent (id serial PRIMARY KEY, label text)");
        exec("CREATE TABLE gk_child (id serial PRIMARY KEY, parent_id int REFERENCES gk_parent(id), info text)");

        int parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gk_parent (label) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "parent_label");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                parentId = keys.getInt(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gk_child (parent_id, info) VALUES (?, ?)")) {
            ps.setInt(1, parentId);
            ps.setString(2, "child_info");
            assertEquals(1, ps.executeUpdate());
        }
    }

    // =========================================================================
    // Row visible to SELECT immediately after INSERT
    // =========================================================================

    @Test
    void testInsertThenImmediateSelectViaPrepared() throws SQLException {
        exec("CREATE TABLE vis_imm (id serial PRIMARY KEY, val text)");

        try (PreparedStatement insert = conn.prepareStatement("INSERT INTO vis_imm (val) VALUES (?)");
             PreparedStatement select = conn.prepareStatement("SELECT val FROM vis_imm WHERE id = ?")) {

            insert.setString(1, "row_one");
            insert.executeUpdate();

            select.setInt(1, 1);
            try (ResultSet rs = select.executeQuery()) {
                assertTrue(rs.next(), "Inserted row must be immediately visible to SELECT on same connection");
                assertEquals("row_one", rs.getString(1));
            }
        }
    }

    @Test
    void testInsertThenCountViaPrepared() throws SQLException {
        exec("CREATE TABLE vis_count (id serial PRIMARY KEY, val text)");

        try (PreparedStatement insert = conn.prepareStatement("INSERT INTO vis_count (val) VALUES (?)");
             PreparedStatement count = conn.prepareStatement("SELECT COUNT(*) FROM vis_count")) {

            for (int i = 1; i <= 5; i++) {
                insert.setString(1, "item_" + i);
                insert.executeUpdate();

                try (ResultSet rs = count.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1), "Count should be " + i + " after " + i + " inserts");
                }
            }
        }
    }

    // =========================================================================
    // INSERT visibility across simple and extended protocol
    // =========================================================================

    @Test
    void testInsertViaSimpleThenSelectViaPrepared() throws SQLException {
        exec("CREATE TABLE vis_cross1 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO vis_cross1 (val) VALUES ('from_simple')");

        try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM vis_cross1 WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Row inserted via simple protocol must be visible to PreparedStatement");
                assertEquals("from_simple", rs.getString(1));
            }
        }
    }

    @Test
    void testInsertViaPreparedThenSelectViaSimple() throws SQLException {
        exec("CREATE TABLE vis_cross2 (id serial PRIMARY KEY, val text)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_cross2 (val) VALUES (?)")) {
            ps.setString(1, "from_prepared");
            ps.executeUpdate();
        }

        assertEquals("from_prepared", query1("SELECT val FROM vis_cross2 WHERE id = 1"));
    }

    // =========================================================================
    // UPDATE visibility after INSERT
    // =========================================================================

    @Test
    void testInsertThenUpdateThenSelect() throws SQLException {
        exec("CREATE TABLE vis_upd (id serial PRIMARY KEY, status text DEFAULT 'new')");

        int id;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_upd DEFAULT VALUES RETURNING id")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                id = rs.getInt(1);
            }
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE vis_upd SET status = ? WHERE id = ?")) {
            ps.setString(1, "active");
            ps.setInt(2, id);
            assertEquals(1, ps.executeUpdate());
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT status FROM vis_upd WHERE id = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("active", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // Multiple parent-child inserts in sequence
    // =========================================================================

    @Test
    void testMultipleParentChildSequence() throws SQLException {
        exec("CREATE TABLE vis_teams (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE vis_members (id serial PRIMARY KEY, team_id int REFERENCES vis_teams(id), member_name text)");

        for (int t = 1; t <= 3; t++) {
            int teamId;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO vis_teams (name) VALUES (?) RETURNING id")) {
                ps.setString(1, "team_" + t);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    teamId = rs.getInt(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO vis_members (team_id, member_name) VALUES (?, ?)")) {
                for (int m = 1; m <= 3; m++) {
                    ps.setInt(1, teamId);
                    ps.setString(2, "member_" + t + "_" + m);
                    assertEquals(1, ps.executeUpdate());
                }
            }
        }

        assertEquals("3", query1("SELECT COUNT(*) FROM vis_teams"));
        assertEquals("9", query1("SELECT COUNT(*) FROM vis_members"));
    }

    // =========================================================================
    // DELETE then verify gone, INSERT then verify present
    // =========================================================================

    @Test
    void testDeleteThenInsertVisibility() throws SQLException {
        exec("CREATE TABLE vis_del (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO vis_del (val) VALUES ('original')");

        try (PreparedStatement del = conn.prepareStatement("DELETE FROM vis_del WHERE val = ?")) {
            del.setString(1, "original");
            assertEquals(1, del.executeUpdate());
        }

        // Must see 0 rows now
        try (PreparedStatement count = conn.prepareStatement("SELECT COUNT(*) FROM vis_del")) {
            try (ResultSet rs = count.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }

        // Insert new row
        try (PreparedStatement ins = conn.prepareStatement("INSERT INTO vis_del (val) VALUES (?) RETURNING id")) {
            ins.setString(1, "replacement");
            try (ResultSet rs = ins.executeQuery()) {
                assertTrue(rs.next());
            }
        }

        // Must see 1 row
        assertEquals("replacement", query1("SELECT val FROM vis_del"));
    }

    // =========================================================================
    // Unique constraint: INSERT, then attempt duplicate
    // =========================================================================

    @Test
    void testInsertThenDuplicateDetection() throws SQLException {
        exec("CREATE TABLE vis_uniq (id serial PRIMARY KEY, code text UNIQUE)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_uniq (code) VALUES (?)")) {
            ps.setString(1, "ABC");
            assertEquals(1, ps.executeUpdate());
        }

        // Duplicate must be detected; the first INSERT must have been persisted
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_uniq (code) VALUES (?)")) {
            ps.setString(1, "ABC");
            assertThrows(SQLException.class, ps::executeUpdate);
        }
    }

    // =========================================================================
    // Three-level FK chain via extended protocol
    // =========================================================================

    @Test
    void testThreeLevelFkChain() throws SQLException {
        exec("CREATE TABLE vis_l1 (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text)");
        exec("CREATE TABLE vis_l2 (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), l1_id uuid REFERENCES vis_l1(id), label text)");
        exec("CREATE TABLE vis_l3 (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), l2_id uuid REFERENCES vis_l2(id), detail text)");

        String l1Id, l2Id;

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_l1 (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "level1");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); l1Id = rs.getString(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_l2 (l1_id, label) VALUES (?::uuid, ?) RETURNING id")) {
            ps.setString(1, l1Id);
            ps.setString(2, "level2");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); l2Id = rs.getString(1); }
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_l3 (l2_id, detail) VALUES (?::uuid, ?)")) {
            ps.setString(1, l2Id);
            ps.setString(2, "level3");
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("1", query1("SELECT COUNT(*) FROM vis_l3"));
    }

    // =========================================================================
    // Batch insert then verify all visible
    // =========================================================================

    @Test
    void testBatchInsertVisibility() throws SQLException {
        exec("CREATE TABLE vis_batch (id serial PRIMARY KEY, val text)");

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO vis_batch (val) VALUES (?)")) {
            for (int i = 1; i <= 100; i++) {
                ps.setString(1, "batch_" + i);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        // All 100 rows must be visible immediately
        assertEquals("100", query1("SELECT COUNT(*) FROM vis_batch"));
    }

    // =========================================================================
    // RETURNING with complex expressions
    // =========================================================================

    @Test
    void testReturningMultipleColumns() throws SQLException {
        exec("CREATE TABLE vis_ret_multi (id serial PRIMARY KEY, name text, created_at timestamp DEFAULT now())");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_ret_multi (name) VALUES (?) RETURNING id, name, created_at")) {
            ps.setString(1, "returned_row");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                int id = rs.getInt(1);
                String name = rs.getString(2);
                Timestamp ts = rs.getTimestamp(3);
                assertTrue(id > 0);
                assertEquals("returned_row", name);
                assertNotNull(ts);
            }
        }

        // Must also be queryable
        assertEquals("returned_row", query1("SELECT name FROM vis_ret_multi"));
    }

    @Test
    void testReturningComputedValue() throws SQLException {
        exec("CREATE TABLE vis_ret_computed (id serial PRIMARY KEY, a int, b int)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO vis_ret_computed (a, b) VALUES (?, ?) RETURNING id, a + b AS total")) {
            ps.setInt(1, 30);
            ps.setInt(2, 12);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt("total"));
            }
        }

        assertEquals("42", query1("SELECT a + b FROM vis_ret_computed WHERE id = 1"));
    }
}
