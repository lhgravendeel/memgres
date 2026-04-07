package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PreparedStatement queries against pg_catalog system tables.
 *
 * Migration tools, ORM frameworks, SQL builder frameworks, and JDBC
 * DatabaseMetaData all issue parameterized queries against pg_catalog to
 * check schema state. These go through the extended query protocol
 * (Parse/Bind/Describe/Execute/Sync) and the server must respond with
 * the correct message sequence:
 *   ParseComplete → BindComplete → RowDescription → DataRow* → CommandComplete → ReadyForQuery
 *
 * If any message is missing or out of order, the JDBC driver throws
 * NoSuchElementException (protocol response queue underflow) or
 * "Received resultset tuples, but no field structure for them".
 *
 * These tests cover:
 *   - Table existence checks (pg_class + pg_namespace)
 *   - Column existence checks (pg_attribute)
 *   - Constraint checks (pg_constraint)
 *   - Sequence checks
 *   - Schema checks
 *   - Index checks (pg_index)
 *   - Type/enum checks (pg_type)
 *   - Combined checks with UNION ALL
 *   - EXISTS returning true and false
 *   - Multiple sequential PreparedStatement executions on same connection
 */
class PreparedCatalogQueryTest {

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
            s.execute("CREATE TABLE catalog_test_items (id serial PRIMARY KEY, name text NOT NULL UNIQUE, status text DEFAULT 'active')");
            s.execute("CREATE INDEX idx_ct_status ON catalog_test_items (status)");
            s.execute("CREATE TYPE catalog_test_kind AS ENUM ('typeA', 'typeB')");
            s.execute("CREATE SEQUENCE catalog_test_seq START WITH 100");
            s.execute("INSERT INTO catalog_test_items (name) VALUES ('first'), ('second')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // Table existence: SELECT EXISTS(... pg_class ... WHERE relname = ?)
    // =========================================================================

    @Test
    void testTableExistsTrue() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'r'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must return a row");
                assertTrue(rs.getBoolean(1), "Table should exist");
            }
        }
    }

    @Test
    void testTableExistsFalse() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'r'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "nonexistent_table_xyz");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Must return a row even when table doesn't exist");
                assertFalse(rs.getBoolean(1), "Table should not exist");
            }
        }
    }

    @Test
    void testTableExistsSimplified() throws SQLException {
        // Simpler form using pg_tables
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = ? AND tablename = ?)")) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // View / sequence / type existence using relkind
    // =========================================================================

    @Test
    void testSequenceExistsViaRelkind() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'S'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_seq");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testRelkindInList() throws SQLException {
        // Check for any object matching multiple relkinds (table, view, sequence)
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relkind IN ('r', 'v', 'S', 't')
            )
        """)) {
            ps.setString(1, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Column existence: pg_attribute
    // =========================================================================

    @Test
    void testColumnExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND a.attname = ? AND a.attnum > 0
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            ps.setString(3, "name");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testColumnNotExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND a.attname = ? AND a.attnum > 0
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            ps.setString(3, "nonexistent_column");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Schema existence
    // =========================================================================

    @Test
    void testSchemaExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?)")) {
            ps.setString(1, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testSchemaNotExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?)")) {
            ps.setString(1, "nonexistent_schema_abc");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Constraint existence: pg_constraint
    // =========================================================================

    @Test
    void testConstraintExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.connamespace
                WHERE n.nspname = ? AND c.conname = ?
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items_pkey");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Index existence: pg_index
    // =========================================================================

    @Test
    void testIndexExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'i'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "idx_ct_status");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Type / enum existence: pg_type
    // =========================================================================

    @Test
    void testEnumTypeExists() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = ? AND t.typname = ? AND t.typtype = 'e'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_kind");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // UNION ALL across catalog tables
    // =========================================================================

    @Test
    void testPreparedUnionAllCatalogCheck() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relkind IN ('r', 'v', 'S', 't')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = ? AND t.typcategory NOT IN ('A', 'C')
              UNION ALL
                SELECT p.oid FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = ?
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "public");
            ps.setString(3, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedUnionAllCatalogCheckEmptySchema() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relkind IN ('r', 'v', 'S', 't')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = ?
            )
        """)) {
            ps.setString(1, "nonexistent_schema_xyz");
            ps.setString(2, "nonexistent_schema_xyz");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Sequential PreparedStatement executions on same connection
    // =========================================================================

    @Test
    void testSequentialPreparedStatements() throws SQLException {
        // First query
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = ?)")) {
            ps.setString(1, "catalog_test_items");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
        // Second query, different PreparedStatement on same connection
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM catalog_test_items WHERE name = ?")) {
            ps.setString(1, "first");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
        // Third query, back to catalog
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = ?)")) {
            ps.setString(1, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testReusePreparedStatementWithDifferentParams() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'r'
            )
        """)) {
            // First execution: table exists
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }

            // Second execution with same PreparedStatement: table doesn't exist
            ps.setString(1, "public");
            ps.setString(2, "nonexistent_table");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }

            // Third execution: different schema
            ps.setString(1, "pg_catalog");
            ps.setString(2, "pg_class");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // =========================================================================
    // Mixed Statement + PreparedStatement on same connection
    // =========================================================================

    @Test
    void testMixedStatementAndPrepared() throws SQLException {
        // Regular Statement
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mixed_test (id serial PRIMARY KEY, val text)");
        }

        // PreparedStatement catalog check
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ? AND c.relname = ? AND c.relkind = 'r'
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "mixed_test");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }

        // Back to regular Statement
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO mixed_test (val) VALUES ('after_prepared')");
            try (ResultSet rs = s.executeQuery("SELECT val FROM mixed_test")) {
                assertTrue(rs.next());
                assertEquals("after_prepared", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // information_schema queries via PreparedStatement
    // =========================================================================

    @Test
    void testPreparedInformationSchemaTableCheck() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedInformationSchemaColumnCheck() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = ? AND table_name = ? AND column_name = ?
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "catalog_test_items");
            ps.setString(3, "status");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }
}
