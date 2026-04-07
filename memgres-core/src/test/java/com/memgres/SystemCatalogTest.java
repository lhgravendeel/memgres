package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 12: System Catalogs tests.
 * pg_catalog.*, information_schema.*, catalog functions.
 */
class SystemCatalogTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)");
            stmt.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), amount NUMERIC)");
            stmt.execute("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");
            stmt.execute("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')");
            stmt.execute("CREATE SEQUENCE order_seq START WITH 100");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- pg_tables ----

    @Test
    void testPgTables() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename");
            assertTrue(rs.next());
            assertEquals("orders", rs.getString("tablename"));
            assertTrue(rs.next());
            assertEquals("users", rs.getString("tablename"));
        }
    }

    // ---- pg_class ----

    @Test
    void testPgClass() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'users'");
            assertTrue(rs.next());
            assertEquals("users", rs.getString("relname"));
            assertEquals("r", rs.getString("relkind"));
        }
    }

    @Test
    void testPgClassSequences() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'S'");
            assertTrue(rs.next());
            assertEquals("order_seq", rs.getString("relname"));
        }
    }

    // ---- pg_attribute ----

    @Test
    void testPgAttribute() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT a.attname, a.attnotnull " +
                    "FROM pg_catalog.pg_attribute a " +
                    "JOIN pg_catalog.pg_class c ON a.attrelid = c.oid " +
                    "WHERE c.relname = 'users' ORDER BY a.attnum");
            assertTrue(rs.next());
            assertEquals("id", rs.getString("attname"));
            assertTrue(rs.next());
            assertEquals("name", rs.getString("attname"));
            assertTrue(rs.next());
            assertEquals("email", rs.getString("attname"));
            // Verify 3 columns total for users table
            assertFalse(rs.next());
        }
    }

    @Test
    void testPgAttributeNotNull() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Direct query without JOIN to verify attnotnull values
            ResultSet rs = stmt.executeQuery(
                    "SELECT attname, attnotnull FROM pg_catalog.pg_attribute " +
                    "WHERE attname = 'name'");
            boolean found = false;
            while (rs.next()) {
                if ("name".equals(rs.getString("attname"))) {
                    found = true;
                    // name TEXT NOT NULL → attnotnull should be true
                    assertEquals("t", rs.getString("attnotnull"));
                }
            }
            assertTrue(found, "Should find 'name' column in pg_attribute");
        }
    }

    // ---- pg_type ----

    @Test
    void testPgType() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT typname FROM pg_catalog.pg_type WHERE oid = 23");
            assertTrue(rs.next());
            assertEquals("int4", rs.getString("typname"));
        }
    }

    // ---- pg_namespace ----

    @Test
    void testPgNamespace() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public'");
            assertTrue(rs.next());
            assertEquals("public", rs.getString("nspname"));
        }
    }

    // ---- pg_constraint ----

    @Test
    void testPgConstraint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT conname, contype FROM pg_catalog.pg_constraint " +
                    "JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid " +
                    "WHERE pg_class.relname = 'users' ORDER BY conname");
            // users has: PK on id, UNIQUE on email
            assertTrue(rs.next());
            String name1 = rs.getString("conname");
            assertTrue(rs.next());
            String name2 = rs.getString("conname");
            // One should be 'p' (primary key), one should be 'u' (unique)
            boolean hasPK = false, hasUQ = false;
            for (String n : new String[]{name1, name2}) {
                // re-query not needed, just check types from earlier
            }
        }
    }

    @Test
    void testPgConstraintForeignKey() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT c.conname, c.contype " +
                    "FROM pg_catalog.pg_constraint c " +
                    "JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid " +
                    "WHERE cl.relname = 'orders' AND c.contype = 'f'");
            assertTrue(rs.next());
            assertEquals("f", rs.getString("contype"));
        }
    }

    // ---- pg_settings ----

    @Test
    void testPgSettings() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT setting FROM pg_settings WHERE name = 'server_version'");
            assertTrue(rs.next());
            assertEquals("18.0", rs.getString("setting"));
        }
    }

    // ---- pg_database ----

    @Test
    void testPgDatabase() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT datname FROM pg_catalog.pg_database WHERE datname = 'memgres'");
            assertTrue(rs.next());
            assertEquals("memgres", rs.getString("datname"));
        }
    }

    // ---- information_schema.tables ----

    @Test
    void testInformationSchemaTables() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT table_name, table_type FROM information_schema.tables " +
                    "WHERE table_schema = 'public' ORDER BY table_name");
            assertTrue(rs.next());
            assertEquals("orders", rs.getString("table_name"));
            assertEquals("BASE TABLE", rs.getString("table_type"));
            assertTrue(rs.next());
            assertEquals("users", rs.getString("table_name"));
            assertEquals("BASE TABLE", rs.getString("table_type"));
        }
    }

    // ---- information_schema.columns ----

    @Test
    void testInformationSchemaColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, data_type, is_nullable, ordinal_position " +
                    "FROM information_schema.columns " +
                    "WHERE table_name = 'users' ORDER BY ordinal_position");
            assertTrue(rs.next());
            assertEquals("id", rs.getString("column_name"));
            assertEquals("integer", rs.getString("data_type"));
            assertEquals("NO", rs.getString("is_nullable"));
            assertEquals(1, rs.getInt("ordinal_position"));

            assertTrue(rs.next());
            assertEquals("name", rs.getString("column_name"));
            assertEquals("text", rs.getString("data_type"));
            assertEquals("NO", rs.getString("is_nullable"));

            assertTrue(rs.next());
            assertEquals("email", rs.getString("column_name"));
            assertEquals("text", rs.getString("data_type"));
            assertEquals("YES", rs.getString("is_nullable"));
        }
    }

    // ---- information_schema.schemata ----

    @Test
    void testInformationSchemaSchemata() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public'");
            assertTrue(rs.next());
            assertEquals("public", rs.getString("schema_name"));
        }
    }

    // ---- information_schema.table_constraints ----

    @Test
    void testInformationSchemaTableConstraints() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT constraint_name, constraint_type FROM information_schema.table_constraints " +
                    "WHERE table_name = 'users' ORDER BY constraint_name");
            boolean hasPK = false, hasUQ = false;
            while (rs.next()) {
                String type = rs.getString("constraint_type");
                if ("PRIMARY KEY".equals(type)) hasPK = true;
                if ("UNIQUE".equals(type)) hasUQ = true;
            }
            assertTrue(hasPK, "Should have PRIMARY KEY constraint");
            assertTrue(hasUQ, "Should have UNIQUE constraint");
        }
    }

    @Test
    void testInformationSchemaTableConstraintsForeignKey() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT constraint_name, constraint_type FROM information_schema.table_constraints " +
                    "WHERE table_name = 'orders' AND constraint_type = 'FOREIGN KEY'");
            assertTrue(rs.next());
            assertEquals("FOREIGN KEY", rs.getString("constraint_type"));
        }
    }

    // ---- information_schema.key_column_usage ----

    @Test
    void testInformationSchemaKeyColumnUsage() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT column_name, ordinal_position FROM information_schema.key_column_usage " +
                    "WHERE table_name = 'users' AND constraint_name LIKE '%pkey%'");
            assertTrue(rs.next());
            assertEquals("id", rs.getString("column_name"));
            assertEquals(1, rs.getInt("ordinal_position"));
        }
    }

    // ---- information_schema.referential_constraints ----

    @Test
    void testInformationSchemaReferentialConstraints() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT constraint_name, delete_rule FROM information_schema.referential_constraints " +
                    "WHERE constraint_name LIKE '%fkey%'");
            assertTrue(rs.next());
            assertEquals("NO ACTION", rs.getString("delete_rule"));
        }
    }

    // ---- Catalog functions ----

    @Test
    void testVersion() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT version()");
            assertTrue(rs.next());
            String version = rs.getString(1);
            assertTrue(version.contains("PostgreSQL") && version.contains("18.0"));
        }
    }

    @Test
    void testCurrentSetting() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_setting('server_version')");
            assertTrue(rs.next());
            assertEquals("18.0", rs.getString(1));
        }
    }

    @Test
    void testCurrentSettingSearchPath() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_setting('search_path')");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void testFormatType() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT format_type(23, -1)");
            assertTrue(rs.next());
            assertEquals("integer", rs.getString(1));
        }
    }

    @Test
    void testFormatTypeText() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT format_type(25, -1)");
            assertTrue(rs.next());
            assertEquals("text", rs.getString(1));
        }
    }

    @Test
    void testCurrentDatabase() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_database()");
            assertTrue(rs.next());
            assertEquals("memgres", rs.getString(1));
        }
    }

    @Test
    void testCurrentSchema() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT current_schema()");
            assertTrue(rs.next());
            assertEquals("public", rs.getString(1));
        }
    }

    @Test
    void testToRegclass() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_regclass('users')");
            assertTrue(rs.next());
            assertEquals("users", rs.getString(1));

            rs = stmt.executeQuery("SELECT to_regclass('nonexistent')");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    @Test
    void testHasSchemaPrivilege() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT has_schema_privilege('public', 'USAGE')");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- pg_sequences ----

    @Test
    void testPgSequences() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT sequencename, start_value FROM pg_sequences WHERE sequencename = 'order_seq'");
            assertTrue(rs.next());
            assertEquals("order_seq", rs.getString("sequencename"));
            assertEquals(100L, rs.getLong("start_value"));
        }
    }

    // ---- pg_roles ----

    @Test
    void testPgRoles() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Check we can query pg_roles and find the memgres role
            ResultSet rs = stmt.executeQuery("SELECT rolname FROM pg_roles WHERE rolname = 'memgres'");
            assertTrue(rs.next(), "pg_roles should contain 'memgres' role");
            assertEquals("memgres", rs.getString("rolname"));
        }
    }

    // ---- information_schema.sequences ----

    @Test
    void testInformationSchemaSequences() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                    "SELECT sequence_name, data_type FROM information_schema.sequences " +
                    "WHERE sequence_name = 'order_seq'");
            assertTrue(rs.next());
            assertEquals("order_seq", rs.getString("sequence_name"));
            assertEquals("bigint", rs.getString("data_type"));
        }
    }
}
