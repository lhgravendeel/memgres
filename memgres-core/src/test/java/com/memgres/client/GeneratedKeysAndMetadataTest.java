package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for getGeneratedKeys(), DatabaseMetaData queries, and connection
 * pool validation patterns.
 *
 * getGeneratedKeys() is used by virtually every ORM and application that
 * inserts rows and needs the auto-generated ID back. The JDBC driver
 * appends RETURNING to the INSERT and sends it via extended protocol.
 *
 * DatabaseMetaData methods (getTables, getColumns, getPrimaryKeys, etc.)
 * use PreparedStatements internally to query pg_catalog. If the extended
 * protocol doesn't work, ALL metadata queries fail.
 *
 * Connection pools validate connections with
 * simple queries like "SELECT 1" via PreparedStatement.
 */
class GeneratedKeysAndMetadataTest {

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
            s.execute("CREATE TABLE gen_key_test (id serial PRIMARY KEY, name text NOT NULL, created_at timestamp DEFAULT now())");
            s.execute("CREATE TABLE gen_key_bigint (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label text)");
            s.execute("CREATE TABLE meta_test (id serial PRIMARY KEY, name text NOT NULL, category text, price numeric(10,2), active boolean DEFAULT true)");
            s.execute("CREATE INDEX idx_meta_category ON meta_test (category)");
            s.execute("CREATE UNIQUE INDEX idx_meta_name ON meta_test (name)");
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

    // =========================================================================
    // getGeneratedKeys() with RETURN_GENERATED_KEYS flag
    // =========================================================================

    @Test
    void testGetGeneratedKeysSerial() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gen_key_test (name) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "auto_key_item");
            assertEquals(1, ps.executeUpdate());

            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next(), "Must return generated key");
                int generatedId = keys.getInt(1);
                assertTrue(generatedId > 0, "Generated ID must be positive");
            }
        }
    }

    @Test
    void testGetGeneratedKeysMultipleInserts() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gen_key_test (name) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            int previousId = 0;
            for (int i = 0; i < 5; i++) {
                ps.setString(1, "batch_key_" + i);
                ps.executeUpdate();
                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue(keys.next());
                    int id = keys.getInt(1);
                    assertTrue(id > previousId, "IDs must be increasing");
                    previousId = id;
                }
            }
        }
    }

    @Test
    void testGetGeneratedKeysBigintIdentity() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gen_key_bigint (label) VALUES (?)",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "identity_item");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                long id = keys.getLong(1);
                assertTrue(id > 0);
            }
        }
    }

    @Test
    void testGetGeneratedKeysWithColumnNames() throws SQLException {
        // Specify which columns to return
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO gen_key_test (name) VALUES (?)",
                new String[]{"id", "created_at"})) {
            ps.setString(1, "specific_cols");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                assertTrue(keys.next());
                assertNotNull(keys.getObject(1)); // id
            }
        }
    }

    // =========================================================================
    // DatabaseMetaData.getTables()
    // =========================================================================

    @Test
    void testGetTables() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, "public", "meta_test", new String[]{"TABLE"})) {
            assertTrue(rs.next(), "meta_test table should be found");
            assertEquals("meta_test", rs.getString("TABLE_NAME"));
            assertEquals("public", rs.getString("TABLE_SCHEM"));
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        }
    }

    @Test
    void testGetTablesWithPattern() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, "public", "meta%", new String[]{"TABLE"})) {
            assertTrue(rs.next(), "Should find tables matching meta%");
        }
    }

    @Test
    void testGetTablesReturnsEmptyForNonexistent() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, "public", "nonexistent_xyz", new String[]{"TABLE"})) {
            assertFalse(rs.next(), "Should not find nonexistent table");
        }
    }

    // =========================================================================
    // DatabaseMetaData.getColumns()
    // =========================================================================

    @Test
    void testGetColumns() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, "public", "meta_test", null)) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertNotNull(rs.getString("COLUMN_NAME"));
                assertNotNull(rs.getString("TYPE_NAME"));
            }
            assertTrue(count >= 5, "meta_test has 5 columns (id, name, category, price, active)");
        }
    }

    @Test
    void testGetColumnsSpecific() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, "public", "meta_test", "name")) {
            assertTrue(rs.next());
            assertEquals("name", rs.getString("COLUMN_NAME"));
            assertFalse(rs.next(), "Should return exactly one column");
        }
    }

    // =========================================================================
    // DatabaseMetaData.getPrimaryKeys()
    // =========================================================================

    @Test
    void testGetPrimaryKeys() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getPrimaryKeys(null, "public", "meta_test")) {
            assertTrue(rs.next(), "meta_test has a primary key");
            assertEquals("id", rs.getString("COLUMN_NAME"));
        }
    }

    // =========================================================================
    // DatabaseMetaData.getIndexInfo()
    // =========================================================================

    @Test
    void testGetIndexInfo() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getIndexInfo(null, "public", "meta_test", false, false)) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertNotNull(rs.getString("INDEX_NAME"));
            }
            assertTrue(count >= 2, "meta_test has at least 2 indexes (pk + idx_meta_category)");
        }
    }

    // =========================================================================
    // DatabaseMetaData.getSchemas()
    // =========================================================================

    @Test
    void testGetSchemas() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getSchemas()) {
            boolean foundPublic = false;
            while (rs.next()) {
                if ("public".equals(rs.getString("TABLE_SCHEM"))) {
                    foundPublic = true;
                }
            }
            assertTrue(foundPublic, "Should find 'public' schema");
        }
    }

    // =========================================================================
    // DatabaseMetaData.getTypeInfo()
    // =========================================================================

    @Test
    void testGetTypeInfo() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTypeInfo()) {
            boolean foundInt4 = false;
            boolean foundText = false;
            while (rs.next()) {
                String typeName = rs.getString("TYPE_NAME");
                if ("int4".equals(typeName)) foundInt4 = true;
                if ("text".equals(typeName)) foundText = true;
            }
            assertTrue(foundInt4, "Should list int4 type");
            assertTrue(foundText, "Should list text type");
        }
    }

    // =========================================================================
    // Connection pool validation patterns
    // =========================================================================

    @Test
    void testConnectionValidationSelect1() throws SQLException {
        // Connection pool default validation query
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void testConnectionValidationViaIsValid() throws SQLException {
        // JDBC 4.0 isValid() sends a lightweight protocol-level check
        assertTrue(conn.isValid(5));
    }

    @Test
    void testConnectionValidationAfterQuery() throws SQLException {
        // Pool validates after returning from app code
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM gen_key_test WHERE id = ?")) {
            ps.setInt(1, 999);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next()); // no rows, but query succeeds
            }
        }
        // Validation
        assertTrue(conn.isValid(5));
    }

    // =========================================================================
    // Connection metadata
    // =========================================================================

    @Test
    void testDatabaseProductName() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        assertNotNull(md.getDatabaseProductName());
    }

    @Test
    void testDatabaseProductVersion() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        assertNotNull(md.getDatabaseProductVersion());
    }

    // =========================================================================
    // Multiple metadata calls on same connection
    // =========================================================================

    @Test
    void testSequentialMetadataCalls() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();

        // Call getTables, then getColumns, then getPrimaryKeys
        try (ResultSet rs = md.getTables(null, "public", "meta_test", new String[]{"TABLE"})) {
            assertTrue(rs.next());
        }
        try (ResultSet rs = md.getColumns(null, "public", "meta_test", null)) {
            assertTrue(rs.next());
        }
        try (ResultSet rs = md.getPrimaryKeys(null, "public", "meta_test")) {
            assertTrue(rs.next());
        }

        // Connection still works for normal queries
        try (PreparedStatement ps = conn.prepareStatement("SELECT 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
            }
        }
    }
}
