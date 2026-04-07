package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 12, 27 (Java/JDBC): DatabaseMetaData and introspection.
 * Tests getTables, getColumns, getPrimaryKeys, getImportedKeys, getExportedKeys,
 * getIndexInfo, getSchemas, getCatalogs, getTypeInfo, supportsTransactions,
 * view discovery, identity column detection, product name/version.
 */
class DatabaseMetadataTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        exec("CREATE TABLE meta_parent(id int PRIMARY KEY, name text NOT NULL DEFAULT 'unknown')");
        exec("CREATE TABLE meta_child(child_id serial PRIMARY KEY, parent_id int NOT NULL REFERENCES meta_parent(id), description text, active boolean DEFAULT true)");
        exec("CREATE TABLE meta_types(id int PRIMARY KEY, col_int int, col_text text NOT NULL, col_bool boolean, col_numeric numeric(10,2) DEFAULT 0.00, col_ts timestamp)");
        exec("CREATE INDEX meta_types_col_int_idx ON meta_types(col_int)");
        exec("CREATE TABLE meta_identity(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, label text)");
        exec("CREATE VIEW meta_view AS SELECT id, name FROM meta_parent");
    }

    @AfterAll static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP VIEW IF EXISTS meta_view");
            exec("DROP TABLE IF EXISTS meta_identity");
            exec("DROP TABLE IF EXISTS meta_child");
            exec("DROP TABLE IF EXISTS meta_parent");
            exec("DROP TABLE IF EXISTS meta_types");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // --- 1. getTables finds created table ---

    @Test void getTables_finds_created_table() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, null, "meta_parent", null)) {
            assertTrue(rs.next(), "meta_parent should be found");
            assertEquals("meta_parent", rs.getString("TABLE_NAME"));
        }
    }

    // --- 2. getTables with schema filter ---

    @Test void getTables_with_schema_filter() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, "public", "meta_parent", null)) {
            assertTrue(rs.next(), "meta_parent should be found in public schema");
            assertEquals("public", rs.getString("TABLE_SCHEM"));
            assertEquals("meta_parent", rs.getString("TABLE_NAME"));
        }
    }

    @Test void getTables_with_wrong_schema_filter() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, "nonexistent_schema", "meta_parent", null)) {
            assertFalse(rs.next(), "meta_parent should not be found in nonexistent schema");
        }
    }

    // --- 3. getTables with table type filter ---

    @Test void getTables_table_type_filter_table() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, null, "meta_parent", new String[]{"TABLE"})) {
            assertTrue(rs.next(), "meta_parent should be found with TABLE type filter");
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
        }
    }

    @Test void getTables_table_type_filter_view_excludes_table() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, null, "meta_parent", new String[]{"VIEW"})) {
            assertFalse(rs.next(), "meta_parent should not appear with VIEW type filter");
        }
    }

    // --- 4. getColumns returns correct column names ---

    @Test void getColumns_returns_correct_column_names() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", null)) {
            assertTrue(rs.next());
            assertEquals("id", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("col_int", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("col_text", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("col_bool", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("col_numeric", rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals("col_ts", rs.getString("COLUMN_NAME"));
            assertFalse(rs.next(), "No more columns expected");
        }
    }

    // --- 5. getColumns returns correct data types ---

    @Test void getColumns_returns_correct_data_type_int() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_int")) {
            assertTrue(rs.next());
            assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
        }
    }

    @Test void getColumns_returns_correct_data_type_text() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_text")) {
            assertTrue(rs.next());
            assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
        }
    }

    @Test void getColumns_returns_correct_data_type_boolean() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_bool")) {
            assertTrue(rs.next());
            int type = rs.getInt("DATA_TYPE");
            assertTrue(type == Types.BOOLEAN || type == Types.BIT, "Boolean data type: " + type);
        }
    }

    @Test void getColumns_returns_correct_data_type_numeric() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_numeric")) {
            assertTrue(rs.next());
            assertEquals(Types.NUMERIC, rs.getInt("DATA_TYPE"));
        }
    }

    @Test void getColumns_returns_correct_data_type_timestamp() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_ts")) {
            assertTrue(rs.next());
            assertEquals(Types.TIMESTAMP, rs.getInt("DATA_TYPE"));
        }
    }

    // --- 6. getColumns returns correct nullability ---

    @Test void getColumns_nullable_column() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_int")) {
            assertTrue(rs.next());
            assertEquals(DatabaseMetaData.columnNullable, rs.getInt("NULLABLE"));
        }
    }

    @Test void getColumns_not_null_column() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_text")) {
            assertTrue(rs.next());
            assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
        }
    }

    // --- 7. getColumns returns correct default values ---

    @Test void getColumns_default_value_boolean() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_child", "active")) {
            assertTrue(rs.next());
            String def = rs.getString("COLUMN_DEF");
            assertNotNull(def, "Default value should be present");
            assertTrue(def.contains("true"), "Default should contain true, got: " + def);
        }
    }

    @Test void getColumns_default_value_text() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_parent", "name")) {
            assertTrue(rs.next());
            String def = rs.getString("COLUMN_DEF");
            assertNotNull(def, "Default value should be present");
            assertTrue(def.contains("unknown"), "Default should contain 'unknown', got: " + def);
        }
    }

    @Test void getColumns_default_value_numeric() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_numeric")) {
            assertTrue(rs.next());
            String def = rs.getString("COLUMN_DEF");
            assertNotNull(def, "Default value should be present");
            assertTrue(def.contains("0.00") || def.contains("0"), "Default should contain 0.00, got: " + def);
        }
    }

    @Test void getColumns_no_default_value() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_types", "col_int")) {
            assertTrue(rs.next());
            String def = rs.getString("COLUMN_DEF");
            assertNull(def, "Column without default should have null COLUMN_DEF");
        }
    }

    // --- 8. getPrimaryKeys returns PK columns ---

    @Test void getPrimaryKeys_single_column() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getPrimaryKeys(null, null, "meta_parent")) {
            assertTrue(rs.next(), "Primary key should be found");
            assertEquals("id", rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getShort("KEY_SEQ"));
            assertFalse(rs.next(), "Only one PK column expected");
        }
    }

    @Test void getPrimaryKeys_serial_column() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getPrimaryKeys(null, null, "meta_child")) {
            assertTrue(rs.next(), "Primary key should be found");
            assertEquals("child_id", rs.getString("COLUMN_NAME"));
        }
    }

    // --- 9. getImportedKeys returns FK relationships ---

    @Test void getImportedKeys_returns_fk() throws Exception {
        // Use a separate connection to avoid cascade failure if the FK metadata query crashes
        try (Connection fkConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            DatabaseMetaData md = fkConn.getMetaData();
            try (ResultSet rs = md.getImportedKeys(null, null, "meta_child")) {
                assertTrue(rs.next(), "Foreign key should be found");
                assertEquals("meta_parent", rs.getString("PKTABLE_NAME"));
                assertEquals("id", rs.getString("PKCOLUMN_NAME"));
                assertEquals("meta_child", rs.getString("FKTABLE_NAME"));
                assertEquals("parent_id", rs.getString("FKCOLUMN_NAME"));
                assertFalse(rs.next(), "Only one FK expected");
            }
        }
    }

    // --- 10. getExportedKeys returns reverse FK relationships ---

    @Test void getExportedKeys_returns_reverse_fk() throws Exception {
        // Use a separate connection to avoid cascade failure if the FK metadata query crashes
        try (Connection fkConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            DatabaseMetaData md = fkConn.getMetaData();
            try (ResultSet rs = md.getExportedKeys(null, null, "meta_parent")) {
                assertTrue(rs.next(), "Exported key should be found");
                assertEquals("meta_parent", rs.getString("PKTABLE_NAME"));
                assertEquals("id", rs.getString("PKCOLUMN_NAME"));
                assertEquals("meta_child", rs.getString("FKTABLE_NAME"));
                assertEquals("parent_id", rs.getString("FKCOLUMN_NAME"));
                assertFalse(rs.next(), "Only one exported key expected");
            }
        }
    }

    // --- 11. getIndexInfo returns index columns ---

    @Test void getIndexInfo_returns_index() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        boolean foundIndex = false;
        try (ResultSet rs = md.getIndexInfo(null, null, "meta_types", false, true)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (indexName != null && indexName.equals("meta_types_col_int_idx")) {
                    foundIndex = true;
                    assertEquals("col_int", rs.getString("COLUMN_NAME"));
                    assertFalse(rs.getBoolean("NON_UNIQUE") == false && rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic,
                            "Should be a regular index, not a statistic entry");
                }
            }
        }
        assertTrue(foundIndex, "Index meta_types_col_int_idx should be found");
    }

    @Test void getIndexInfo_includes_pk_index() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        boolean foundPkIndex = false;
        try (ResultSet rs = md.getIndexInfo(null, null, "meta_types", true, true)) {
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                if ("id".equals(colName)) {
                    foundPkIndex = true;
                    assertFalse(rs.getBoolean("NON_UNIQUE"), "PK index should be unique");
                }
            }
        }
        assertTrue(foundPkIndex, "Primary key index should be found");
    }

    // --- 12. getSchemas includes public schema ---

    @Test void getSchemas_includes_public() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        boolean foundPublic = false;
        try (ResultSet rs = md.getSchemas()) {
            while (rs.next()) {
                if ("public".equals(rs.getString("TABLE_SCHEM"))) {
                    foundPublic = true;
                }
            }
        }
        assertTrue(foundPublic, "public schema should be present");
    }

    // --- 13. getCatalogs returns catalog ---

    @Test void getCatalogs_returns_catalog() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getCatalogs()) {
            assertTrue(rs.next(), "At least one catalog should exist");
            String catalog = rs.getString("TABLE_CAT");
            assertNotNull(catalog, "Catalog name should not be null");
            assertFalse(catalog.isEmpty(), "Catalog name should not be empty");
        }
    }

    // --- 14. getTypeInfo returns supported types ---

    @Test void getTypeInfo_returns_types() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        boolean foundInt = false;
        boolean foundVarchar = false;
        boolean foundBool = false;
        try (ResultSet rs = md.getTypeInfo()) {
            while (rs.next()) {
                String typeName = rs.getString("TYPE_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                if (dataType == Types.INTEGER) foundInt = true;
                if (dataType == Types.VARCHAR) foundVarchar = true;
                if (dataType == Types.BOOLEAN || dataType == Types.BIT) foundBool = true;
            }
        }
        assertTrue(foundInt, "INTEGER type should be supported");
        assertTrue(foundVarchar, "VARCHAR type should be supported");
        assertTrue(foundBool, "BOOLEAN/BIT type should be supported");
    }

    // --- 15. supportsTransactions ---

    @Test void supportsTransactions_returns_true() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        assertTrue(md.supportsTransactions(), "Database should support transactions");
    }

    // --- 16. View discovery via getTables with VIEW type ---

    @Test void getTables_discovers_view() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, null, "meta_view", new String[]{"VIEW"})) {
            assertTrue(rs.next(), "meta_view should be found as VIEW");
            assertEquals("VIEW", rs.getString("TABLE_TYPE"));
            assertEquals("meta_view", rs.getString("TABLE_NAME"));
        }
    }

    @Test void getTables_view_not_found_as_table() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getTables(null, null, "meta_view", new String[]{"TABLE"})) {
            assertFalse(rs.next(), "meta_view should not appear with TABLE type filter");
        }
    }

    // --- 17. Identity column detection via getColumns ---

    @Test void getColumns_identity_column_has_autoincrement() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_identity", "id")) {
            assertTrue(rs.next());
            String isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
            assertEquals("YES", isAutoIncrement, "Identity column should report IS_AUTOINCREMENT=YES");
        }
    }

    @Test void getColumns_non_identity_column() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, "meta_identity", "label")) {
            assertTrue(rs.next());
            String isAutoIncrement = rs.getString("IS_AUTOINCREMENT");
            assertEquals("NO", isAutoIncrement, "Non-identity column should report IS_AUTOINCREMENT=NO");
        }
    }

    // --- 18. getDatabaseProductName and getDatabaseProductVersion ---

    @Test void getDatabaseProductName() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        String productName = md.getDatabaseProductName();
        assertNotNull(productName, "Product name should not be null");
        assertFalse(productName.isEmpty(), "Product name should not be empty");
    }

    @Test void getDatabaseProductVersion() throws Exception {
        DatabaseMetaData md = conn.getMetaData();
        String version = md.getDatabaseProductVersion();
        assertNotNull(version, "Product version should not be null");
        assertFalse(version.isEmpty(), "Product version should not be empty");
    }
}
