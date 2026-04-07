package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for column metadata correctness when tables are empty.
 *
 * Root cause: when a table has 0 rows, the FROM resolver returns 0 RowContexts.
 * The SELECT executor extracts baseBindings from the first RowContext, but there
 * is none. So baseBindings is empty, and SELECT * resolves to 0 columns.
 * The RowDescription sent to the JDBC driver says "0 columns", so the driver
 * maps every value to null.
 */
class EmptyTableMetadataTest {

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

    // =========================================================================
    // 1. SELECT * on empty table must return correct column count
    // =========================================================================

    @Test
    void testSelectStarEmptyTableColumnCount() throws SQLException {
        exec("CREATE TABLE empty_meta1 (id serial PRIMARY KEY, name text, value int)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM empty_meta1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(),
                    "SELECT * on empty table must report 3 columns (id, name, value)");
            assertEquals("id", md.getColumnName(1));
            assertEquals("name", md.getColumnName(2));
            assertEquals("value", md.getColumnName(3));
        }
    }

    // =========================================================================
    // 2. PreparedStatement on empty table then insert: metadata must be correct
    // =========================================================================

    @Test
    void testPreparedSelectOnEmptyTableThenInsert() throws SQLException {
        exec("""
            CREATE TABLE empty_meta2 (
                id serial PRIMARY KEY,
                email text NOT NULL,
                password_hash text NOT NULL,
                active boolean DEFAULT true
            )
        """);

        // First: PreparedStatement SELECT on empty table
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, email, password_hash, active FROM empty_meta2 WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                // Should return 0 rows but correct column metadata
                assertFalse(rs.next(), "Empty table should return 0 rows");
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(4, md.getColumnCount(),
                        "Must have 4 columns even on empty table");
            }
        }

        // Insert a row
        exec("INSERT INTO empty_meta2 (email, password_hash) VALUES ('user@test.com', 'hash123')");

        // Now the SAME PreparedStatement SQL should return correct data
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, email, password_hash, active FROM empty_meta2 WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find the inserted row");
                assertEquals("user@test.com", rs.getString("email"));
                assertEquals("hash123", rs.getString("password_hash"),
                        "password_hash must NOT be null");
                assertTrue(rs.getBoolean("active"));
            }
        }
    }

    // =========================================================================
    // 3. Insert → findById (works) → findByOtherKey (fails)
    // =========================================================================

    @Test
    void testDaoPatternInsertThenFindByDifferentKey() throws SQLException {
        exec("""
            CREATE TABLE webhooks (
                id serial PRIMARY KEY,
                account_id int NOT NULL,
                url text NOT NULL,
                secret text,
                active boolean DEFAULT true,
                created_at timestamp DEFAULT now()
            )
        """);

        // Insert a webhook
        int webhookId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO webhooks (account_id, url, secret) VALUES (?, ?, ?) RETURNING id")) {
            ps.setInt(1, 42);
            ps.setString(2, "https://example.com/hook");
            ps.setString(3, "secret123");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                webhookId = rs.getInt(1);
            }
        }

        // findById typically works because this PS was used during insert's RETURNING
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, account_id, url, secret, active, created_at FROM webhooks WHERE id = ?")) {
            ps.setInt(1, webhookId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("https://example.com/hook", rs.getString("url"));
                assertEquals("secret123", rs.getString("secret"),
                        "secret must not be null via findById");
            }
        }

        // findByAccountId is a DIFFERENT PreparedStatement, first time used
        // If the table was empty when this PS was first described, column metadata is wrong
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, account_id, url, secret, active, created_at FROM webhooks WHERE account_id = ?")) {
            ps.setInt(1, 42);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find webhook by account_id");
                assertEquals(webhookId, rs.getInt("id"));
                assertEquals("https://example.com/hook", rs.getString("url"),
                        "url must not be null via findByAccountId");
                assertEquals("secret123", rs.getString("secret"),
                        "secret must not be null via findByAccountId");
                assertTrue(rs.getBoolean("active"),
                        "active must not be null via findByAccountId");
            }
        }
    }

    // =========================================================================
    // 4. TRUNCATE then SELECT: metadata must still be correct
    // =========================================================================

    @Test
    void testTruncateThenSelectMetadata() throws SQLException {
        exec("CREATE TABLE trunc_meta (id serial PRIMARY KEY, name text, score int)");
        exec("INSERT INTO trunc_meta (name, score) VALUES ('a', 10)");
        exec("TRUNCATE trunc_meta");

        // Table is now empty; SELECT must still report correct columns
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM trunc_meta")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(),
                    "After TRUNCATE, SELECT * must still report 3 columns");
        }
    }

    // =========================================================================
    // 5. PreparedStatement reuse: first on empty, then with data
    // =========================================================================

    @Test
    void testPreparedReuseEmptyThenWithData() throws SQLException {
        exec("""
            CREATE TABLE reuse_meta (
                id serial PRIMARY KEY,
                payload text NOT NULL,
                score numeric(10,2),
                tags text[]
            )
        """);

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, payload, score, tags FROM reuse_meta WHERE id = ?")) {
            // First execution on empty table
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next());
                assertEquals(4, rs.getMetaData().getColumnCount(),
                        "Must have 4 columns even on empty table");
            }

            // Insert data
            exec("INSERT INTO reuse_meta (payload, score) VALUES ('test', 42.50)");

            // Reuse same PS, must return correct data
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must find the inserted row");
                assertEquals("test", rs.getString("payload"),
                        "payload must not be null");
                assertNotNull(rs.getBigDecimal("score"),
                        "score must not be null");
            }
        }
    }

    // =========================================================================
    // 6. WHERE with no matches: still must have correct column metadata
    // =========================================================================

    @Test
    void testWhereNoMatchesStillHasColumnMetadata() throws SQLException {
        exec("CREATE TABLE nomatch_meta (id serial PRIMARY KEY, name text, data jsonb)");
        exec("INSERT INTO nomatch_meta (name, data) VALUES ('exists', '{}'::jsonb)");

        // WHERE matches nothing
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name, data FROM nomatch_meta WHERE name = ?")) {
            ps.setString(1, "nonexistent");
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next(), "No rows should match");
                assertEquals(3, rs.getMetaData().getColumnCount(),
                        "Must still report 3 columns when WHERE matches nothing");
            }
        }
    }

    // =========================================================================
    // 7. Multiple tables: cross join with one empty table
    // =========================================================================

    @Test
    void testJoinWithEmptyTable() throws SQLException {
        exec("CREATE TABLE join_a (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE join_b (id serial PRIMARY KEY, ref_id int, data text)");
        exec("INSERT INTO join_a (val) VALUES ('exists')");
        // join_b is empty

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a.id, a.val, b.data FROM join_a a LEFT JOIN join_b b ON b.ref_id = a.id")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(),
                    "LEFT JOIN with empty right table must still report 3 columns");
            assertTrue(rs.next());
            assertEquals("exists", rs.getString("val"));
            // b.data is null because join_b is empty, and that's correct
        }
    }
}
