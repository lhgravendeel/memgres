package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class SystemInfoCoverageTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private long queryLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // Item 58: Session & System Information Functions
    // ========================================================================

    // --- current_database() ---

    @Test
    void testCurrentDatabase() throws SQLException {
        String result = query1("SELECT current_database()");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testCurrentDatabaseReturnsMemgres() throws SQLException {
        String result = query1("SELECT current_database()");
        assertEquals("memgres", result);
    }

    // --- current_schema() ---

    @Test
    void testCurrentSchema() throws SQLException {
        String result = query1("SELECT current_schema()");
        assertEquals("public", result);
    }

    @Test
    void testCurrentSchemaNotNull() throws SQLException {
        String result = query1("SELECT current_schema()");
        assertNotNull(result);
    }

    // --- current_schemas(boolean) ---

    @Test
    void testCurrentSchemasTrue() throws SQLException {
        String result = query1("SELECT current_schemas(true)");
        assertNotNull(result);
        assertTrue(result.contains("public"));
    }

    @Test
    void testCurrentSchemasFalse() throws SQLException {
        String result = query1("SELECT current_schemas(false)");
        assertNotNull(result);
        assertTrue(result.contains("public"));
    }

    // --- current_user ---

    @Test
    void testCurrentUser() throws SQLException {
        String result = query1("SELECT current_user");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testCurrentUserValue() throws SQLException {
        String result = query1("SELECT current_user");
        assertEquals("memgres", result);
    }

    // --- session_user ---

    @Test
    void testSessionUser() throws SQLException {
        String result = query1("SELECT session_user");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testSessionUserValue() throws SQLException {
        String result = query1("SELECT session_user");
        assertEquals("memgres", result);
    }

    // --- current_role ---

    @Test
    void testCurrentRole() throws SQLException {
        String result = query1("SELECT current_role");
        assertNotNull(result);
    }

    @Test
    void testCurrentRoleEqualsCurrentUser() throws SQLException {
        String role = query1("SELECT current_role");
        String user = query1("SELECT current_user");
        assertEquals(user, role);
    }

    // --- current_catalog ---

    @Test
    void testCurrentCatalog() throws SQLException {
        String result = query1("SELECT current_catalog");
        assertNotNull(result);
    }

    @Test
    void testCurrentCatalogEqualsCurrentDatabase() throws SQLException {
        String catalog = query1("SELECT current_catalog");
        String db = query1("SELECT current_database()");
        assertEquals(db, catalog);
    }

    // --- pg_backend_pid() ---

    @Test
    void testPgBackendPid() throws SQLException {
        int pid = queryInt("SELECT pg_backend_pid()");
        assertTrue(pid > 0, "pg_backend_pid should return a positive integer");
    }

    @Test
    void testPgBackendPidIsInteger() throws SQLException {
        String result = query1("SELECT pg_backend_pid()");
        assertNotNull(result);
        assertDoesNotThrow(() -> Integer.parseInt(result));
    }

    // --- version() ---

    @Test
    void testVersion() throws SQLException {
        String result = query1("SELECT version()");
        assertNotNull(result);
        assertTrue(result.contains("PostgreSQL"));
    }

    @Test
    void testVersionContainsPostgreSQL() throws SQLException {
        String result = query1("SELECT version()");
        assertTrue(result.contains("PostgreSQL"));
    }

    // --- inet_server_addr() ---

    @Test
    void testInetServerAddr() throws SQLException {
        String result = query1("SELECT inet_server_addr()");
        // Can be null for local connections or return an IP
        if (result != null) {
            assertFalse(result.isEmpty());
        }
    }

    // --- inet_server_port() ---

    @Test
    void testInetServerPort() throws SQLException {
        int port = queryInt("SELECT inet_server_port()");
        assertTrue(port >= 0, "port should be non-negative");
    }

    // --- inet_client_addr() ---

    @Test
    void testInetClientAddr() throws SQLException {
        String result = query1("SELECT inet_client_addr()");
        // Can return null or an IP address
        if (result != null) {
            assertFalse(result.isEmpty());
        }
    }

    // --- inet_client_port() ---

    @Test
    void testInetClientPort() throws SQLException {
        int port = queryInt("SELECT inet_client_port()");
        assertTrue(port >= 0, "client port should be non-negative");
    }

    // --- pg_conf_load_time() ---

    @Test
    void testPgConfLoadTime() throws SQLException {
        String result = query1("SELECT pg_conf_load_time()");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // --- pg_postmaster_start_time() ---

    @Test
    void testPgPostmasterStartTime() throws SQLException {
        String result = query1("SELECT pg_postmaster_start_time()");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // --- pg_is_in_recovery() ---

    @Test
    void testPgIsInRecovery() throws SQLException {
        boolean result = queryBool("SELECT pg_is_in_recovery()");
        assertFalse(result, "Primary server should not be in recovery");
    }

    // --- current_setting('setting_name') ---

    @Test
    void testCurrentSettingServerVersion() throws SQLException {
        String result = query1("SELECT current_setting('server_version')");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testCurrentSettingServerEncoding() throws SQLException {
        String result = query1("SELECT current_setting('server_encoding')");
        assertEquals("UTF8", result);
    }

    @Test
    void testCurrentSettingClientEncoding() throws SQLException {
        String result = query1("SELECT current_setting('client_encoding')");
        assertEquals("UTF8", result);
    }

    @Test
    void testCurrentSettingSearchPath() throws SQLException {
        String result = query1("SELECT current_setting('search_path')");
        assertNotNull(result);
        assertTrue(result.contains("public"));
    }

    @Test
    void testCurrentSettingTimezone() throws SQLException {
        String result = query1("SELECT current_setting('timezone')");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testCurrentSettingStandardConformingStrings() throws SQLException {
        String result = query1("SELECT current_setting('standard_conforming_strings')");
        assertEquals("on", result);
    }

    @Test
    void testCurrentSettingMaxConnections() throws SQLException {
        String result = query1("SELECT current_setting('max_connections')");
        assertNotNull(result);
        int maxConn = Integer.parseInt(result);
        assertTrue(maxConn > 0);
    }

    // --- set_config('setting', 'value', is_local) ---

    @Test
    void testSetConfigBasic() throws SQLException {
        String result = query1("SELECT set_config('search_path', 'public', false)");
        assertEquals("public", result);
    }

    @Test
    void testSetConfigLocal() throws SQLException {
        String result = query1("SELECT set_config('timezone', 'UTC', true)");
        assertEquals("UTC", result);
    }

    @Test
    void testSetConfigReturnsNewValue() throws SQLException {
        String result = query1("SELECT set_config('application_name', 'test_app', false)");
        assertEquals("test_app", result);
    }

    // ========================================================================
    // Item 59: Object Information Functions
    // ========================================================================

    // --- pg_typeof(expr) ---

    @Test
    void testPgTypeofInteger() throws SQLException {
        String result = query1("SELECT pg_typeof(1)");
        assertNotNull(result);
        assertTrue(result.contains("int"), "Expected integer type, got: " + result);
    }

    @Test
    void testPgTypeofText() throws SQLException {
        String result = query1("SELECT pg_typeof('hello')");
        assertNotNull(result);
        assertTrue(result.contains("text") || result.contains("varchar") || result.contains("char") || result.equals("unknown"),
                "Expected text/unknown type, got: " + result);
    }

    @Test
    void testPgTypeofBoolean() throws SQLException {
        String result = query1("SELECT pg_typeof(true)");
        assertNotNull(result);
        assertTrue(result.contains("bool"), "Expected boolean type, got: " + result);
    }

    @Test
    void testPgTypeofNumeric() throws SQLException {
        String result = query1("SELECT pg_typeof(1.5)");
        assertNotNull(result);
        // Could be numeric, decimal, double, etc.
        assertFalse(result.isEmpty());
    }

    @Test
    void testPgTypeofNull() throws SQLException {
        String result = query1("SELECT pg_typeof(NULL)");
        assertNotNull(result);
        // PG returns "unknown" for NULL
    }

    @Test
    void testPgTypeofExpressionAddition() throws SQLException {
        String result = query1("SELECT pg_typeof(1 + 1)");
        assertNotNull(result);
        assertTrue(result.contains("int"), "Expected integer type for 1+1, got: " + result);
    }

    @Test
    void testPgTypeofExpressionConcat() throws SQLException {
        String result = query1("SELECT pg_typeof('hello' || 'world')");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testPgTypeofTimestamp() throws SQLException {
        String result = query1("SELECT pg_typeof(now())");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    // --- col_description(table_oid, column_number) ---

    @Test
    void testColDescriptionReturnsNull() throws SQLException {
        // No comments stored, should return null
        String result = query1("SELECT col_description(0, 1)");
        assertNull(result);
    }

    @Test
    void testColDescriptionDifferentColumn() throws SQLException {
        String result = query1("SELECT col_description(0, 2)");
        assertNull(result);
    }

    // --- obj_description(object_oid, catalog_name) ---

    @Test
    void testObjDescriptionReturnsNull() throws SQLException {
        String result = query1("SELECT obj_description(0, 'pg_class')");
        assertNull(result);
    }

    @Test
    void testObjDescriptionDifferentCatalog() throws SQLException {
        String result = query1("SELECT obj_description(0, 'pg_type')");
        assertNull(result);
    }

    // --- shobj_description(object_oid, catalog_name) ---

    @Test
    void testShobjDescriptionReturnsNull() throws SQLException {
        String result = query1("SELECT shobj_description(0, 'pg_database')");
        assertNull(result);
    }

    @Test
    void testShobjDescriptionDifferentCatalog() throws SQLException {
        String result = query1("SELECT shobj_description(0, 'pg_tablespace')");
        assertNull(result);
    }

    // --- format_type(type_oid, typemod) ---

    @Test
    void testFormatTypeInteger() throws SQLException {
        // OID 23 = integer in PG
        String result = query1("SELECT format_type(23, -1)");
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    void testFormatTypeText() throws SQLException {
        // OID 25 = text in PG
        String result = query1("SELECT format_type(25, -1)");
        assertNotNull(result);
    }

    @Test
    void testFormatTypeBoolean() throws SQLException {
        // OID 16 = boolean in PG
        String result = query1("SELECT format_type(16, -1)");
        assertNotNull(result);
    }

    @Test
    void testFormatTypeNull() throws SQLException {
        String result = query1("SELECT format_type(NULL, NULL)");
        assertNotNull(result);
    }

    // --- pg_get_viewdef(view_oid) ---

    @Test
    void testPgGetViewdef() throws SQLException {
        String result = query1("SELECT pg_get_viewdef(0)");
        // Stub returns empty string
        assertNotNull(result);
    }

    // --- pg_get_constraintdef(constraint_oid) ---

    @Test
    void testPgGetConstraintdef() throws SQLException {
        String result = query1("SELECT pg_get_constraintdef(0)");
        assertNotNull(result);
    }

    @Test
    void testPgGetConstraintdefNonExistent() throws SQLException {
        String result = query1("SELECT pg_get_constraintdef(99999)");
        assertNotNull(result);
    }

    // --- pg_get_indexdef(index_oid) ---

    @Test
    void testPgGetIndexdef() throws SQLException {
        String result = query1("SELECT pg_get_indexdef(0)");
        assertNotNull(result);
    }

    @Test
    void testPgGetIndexdefNonExistent() throws SQLException {
        String result = query1("SELECT pg_get_indexdef(99999)");
        assertNotNull(result);
    }

    // --- pg_get_serial_sequence('table', 'column') ---

    @Test
    void testPgGetSerialSequenceNonExistent() throws SQLException {
        String result = query1("SELECT pg_get_serial_sequence('nonexistent', 'id')");
        // Returns null when no sequence found
        assertNull(result);
    }

    @Test
    void testPgGetSerialSequenceNoTable() throws SQLException {
        String result = query1("SELECT pg_get_serial_sequence('foo', 'bar')");
        assertNull(result);
    }

    // --- pg_table_is_visible(table_oid) ---

    @Test
    void testPgTableIsVisible() throws SQLException {
        boolean result = queryBool("SELECT pg_table_is_visible(0)");
        assertTrue(result);
    }

    @Test
    void testPgTableIsVisibleAnyOid() throws SQLException {
        boolean result = queryBool("SELECT pg_table_is_visible(12345)");
        assertTrue(result);
    }

    // --- pg_function_is_visible(function_oid) ---

    @Test
    void testPgFunctionIsVisible() throws SQLException {
        boolean result = queryBool("SELECT pg_function_is_visible(0)");
        assertTrue(result);
    }

    @Test
    void testPgFunctionIsVisibleAnyOid() throws SQLException {
        boolean result = queryBool("SELECT pg_function_is_visible(999)");
        assertTrue(result);
    }

    // --- pg_type_is_visible(type_oid) ---

    @Test
    void testPgTypeIsVisible() throws SQLException {
        boolean result = queryBool("SELECT pg_type_is_visible(0)");
        assertTrue(result);
    }

    @Test
    void testPgTypeIsVisibleAnyOid() throws SQLException {
        boolean result = queryBool("SELECT pg_type_is_visible(23)");
        assertTrue(result);
    }

    // --- pg_table_size(regclass) ---

    @Test
    void testPgTableSize() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS sysinfo_test1 (id INT)");
        long size = queryLong("SELECT pg_table_size('sysinfo_test1')");
        assertTrue(size >= 0, "Table size should be non-negative");
    }

    // --- pg_relation_size(regclass) ---

    @Test
    void testPgRelationSize() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS sysinfo_test2 (id INT)");
        long size = queryLong("SELECT pg_relation_size('sysinfo_test2')");
        assertTrue(size >= 0, "Relation size should be non-negative");
    }

    // --- pg_total_relation_size(regclass) ---

    @Test
    void testPgTotalRelationSize() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS sysinfo_test3 (id INT)");
        long size = queryLong("SELECT pg_total_relation_size('sysinfo_test3')");
        assertTrue(size >= 0, "Total relation size should be non-negative");
    }

    // --- pg_database_size(name) ---

    @Test
    void testPgDatabaseSize() throws SQLException {
        long size = queryLong("SELECT pg_database_size('memgres')");
        assertTrue(size >= 0, "Database size should be non-negative");
    }

    @Test
    void testPgDatabaseSizeAnyName() throws SQLException {
        long size = queryLong("SELECT pg_database_size('test')");
        assertTrue(size >= 0);
    }

    // --- pg_indexes_size(regclass) ---

    @Test
    void testPgIndexesSize() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS sysinfo_test4 (id INT)");
        long size = queryLong("SELECT pg_indexes_size('sysinfo_test4')");
        assertTrue(size >= 0, "Indexes size should be non-negative");
    }

    // --- has_table_privilege ---

    @Test
    void testHasTablePrivilegeWithUser() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS priv_test1 (id INT)");
        boolean result = queryBool("SELECT has_table_privilege('memgres', 'priv_test1', 'SELECT')");
        assertTrue(result);
    }

    @Test
    void testHasTablePrivilegeCurrentUser() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS priv_test2 (id INT)");
        boolean result = queryBool("SELECT has_table_privilege('priv_test2', 'SELECT')");
        assertTrue(result);
    }

    @Test
    void testHasTablePrivilegeInsert() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS priv_test3 (id INT)");
        boolean result = queryBool("SELECT has_table_privilege('priv_test3', 'INSERT')");
        assertTrue(result);
    }

    @Test
    void testHasTablePrivilegeUpdate() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS priv_test4 (id INT)");
        boolean result = queryBool("SELECT has_table_privilege('priv_test4', 'UPDATE')");
        assertTrue(result);
    }

    @Test
    void testHasTablePrivilegeDelete() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS priv_test5 (id INT)");
        boolean result = queryBool("SELECT has_table_privilege('priv_test5', 'DELETE')");
        assertTrue(result);
    }

    // --- has_column_privilege ---

    @Test
    void testHasColumnPrivilegeSelect() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS col_priv_test (id INT, name TEXT)");
        boolean result = queryBool("SELECT has_column_privilege('col_priv_test', 'id', 'SELECT')");
        assertTrue(result);
    }

    @Test
    void testHasColumnPrivilegeInsert() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS col_priv_test2 (id INT)");
        boolean result = queryBool("SELECT has_column_privilege('col_priv_test2', 'id', 'INSERT')");
        assertTrue(result);
    }

    // --- has_schema_privilege ---

    @Test
    void testHasSchemaPrivilegeUsage() throws SQLException {
        boolean result = queryBool("SELECT has_schema_privilege('public', 'USAGE')");
        assertTrue(result);
    }

    @Test
    void testHasSchemaPrivilegeCreate() throws SQLException {
        boolean result = queryBool("SELECT has_schema_privilege('public', 'CREATE')");
        assertTrue(result);
    }

    // --- has_database_privilege ---

    @Test
    void testHasDatabasePrivilegeConnect() throws SQLException {
        boolean result = queryBool("SELECT has_database_privilege('memgres', 'CONNECT')");
        assertTrue(result);
    }

    @Test
    void testHasDatabasePrivilegeCreate() throws SQLException {
        boolean result = queryBool("SELECT has_database_privilege('memgres', 'CREATE')");
        assertTrue(result);
    }

    // --- has_function_privilege ---

    @Test
    void testHasFunctionPrivilege() throws SQLException {
        boolean result = queryBool("SELECT has_function_privilege('now()', 'EXECUTE')");
        assertTrue(result);
    }

    @Test
    void testHasFunctionPrivilegeVersion() throws SQLException {
        boolean result = queryBool("SELECT has_function_privilege('version()', 'EXECUTE')");
        assertTrue(result);
    }

    // --- has_sequence_privilege ---

    @Test
    void testHasSequencePrivilege() throws SQLException {
        exec("CREATE SEQUENCE IF NOT EXISTS priv_seq");
        boolean result = queryBool("SELECT has_sequence_privilege('priv_seq', 'USAGE')");
        assertTrue(result);
    }

    @Test
    void testHasSequencePrivilegeSelect() throws SQLException {
        exec("CREATE SEQUENCE IF NOT EXISTS priv_seq2");
        boolean result = queryBool("SELECT has_sequence_privilege('priv_seq2', 'SELECT')");
        assertTrue(result);
    }

    // --- pg_get_function_arguments(func_oid) ---

    @Test
    void testPgGetFunctionArguments() throws SQLException {
        String result = query1("SELECT pg_get_function_arguments(0)");
        assertNotNull(result);
    }

    @Test
    void testPgGetFunctionArgumentsNonExistent() throws SQLException {
        String result = query1("SELECT pg_get_function_arguments(99999)");
        assertNotNull(result);
    }

    // --- pg_get_function_result(func_oid) ---

    @Test
    void testPgGetFunctionResult() throws SQLException {
        String result = query1("SELECT pg_get_function_result(0)");
        assertNotNull(result);
    }

    @Test
    void testPgGetFunctionResultNonExistent() throws SQLException {
        String result = query1("SELECT pg_get_function_result(99999)");
        assertNotNull(result);
    }

    // ========================================================================
    // Combined / cross-function tests
    // ========================================================================

    @Test
    void testCurrentUserAndSessionUserMatch() throws SQLException {
        String cu = query1("SELECT current_user");
        String su = query1("SELECT session_user");
        assertEquals(cu, su);
    }

    @Test
    void testCurrentDatabaseAndCatalogMatch() throws SQLException {
        String db = query1("SELECT current_database()");
        String cat = query1("SELECT current_catalog");
        assertEquals(db, cat);
    }

    @Test
    void testVersionIsNotEmpty() throws SQLException {
        String v = query1("SELECT version()");
        assertNotNull(v);
        assertTrue(v.length() > 10);
    }

    @Test
    void testPgTypeofIntegerLiteral() throws SQLException {
        String result = query1("SELECT pg_typeof(42)");
        assertNotNull(result);
    }

    @Test
    void testPgTypeofBigNumber() throws SQLException {
        String result = query1("SELECT pg_typeof(9999999999)");
        assertNotNull(result);
    }

    @Test
    void testPgTypeofDate() throws SQLException {
        String result = query1("SELECT pg_typeof(current_date)");
        assertNotNull(result);
    }

    @Test
    void testSizesFunctionsReturnZeroOrMore() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS size_test (id INT, val TEXT)");
        long tableSize = queryLong("SELECT pg_table_size('size_test')");
        long relSize = queryLong("SELECT pg_relation_size('size_test')");
        long totalSize = queryLong("SELECT pg_total_relation_size('size_test')");
        long idxSize = queryLong("SELECT pg_indexes_size('size_test')");
        assertTrue(tableSize >= 0);
        assertTrue(relSize >= 0);
        assertTrue(totalSize >= 0);
        assertTrue(idxSize >= 0);
    }

    @Test
    void testAllPrivilegeChecksReturnTrue() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS all_priv_t (id INT)");
        assertTrue(queryBool("SELECT has_table_privilege('all_priv_t', 'SELECT')"));
        assertTrue(queryBool("SELECT has_schema_privilege('public', 'USAGE')"));
        assertTrue(queryBool("SELECT has_database_privilege('memgres', 'CONNECT')"));
    }

    @Test
    void testMultipleSystemFunctionsInOneQuery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT current_database(), current_schema(), version()")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
        }
    }

    @Test
    void testMultipleKeywordFunctionsInQuery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT current_user, session_user")) {
            assertTrue(rs.next());
            assertEquals("memgres", rs.getString(1));
            assertEquals("memgres", rs.getString(2));
        }
    }

    @Test
    void testPgTypeofWithCast() throws SQLException {
        String result = query1("SELECT pg_typeof(1::text)");
        assertNotNull(result);
    }

    @Test
    void testCurrentSettingWithMissingOkTrue() throws SQLException {
        String result = query1("SELECT current_setting('nonexistent_setting', true)");
        assertNull(result);
    }

    @Test
    void testFormatTypeWithZeroOid() throws SQLException {
        String result = query1("SELECT format_type(0, -1)");
        assertNotNull(result);
    }

    @Test
    void testPgBackendPidConsistent() throws SQLException {
        int pid1 = queryInt("SELECT pg_backend_pid()");
        int pid2 = queryInt("SELECT pg_backend_pid()");
        // Both calls should return a valid pid (may or may not be equal)
        assertTrue(pid1 > 0);
        assertTrue(pid2 > 0);
    }
}
