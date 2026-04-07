package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that Memgres returns PG 18-compatible results for column properties queries.
 * Each test verifies a specific difference found between Memgres and PG 18.
 * See issues-11.md for full analysis.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class Pg18CompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text NOT NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ==================== Issue 1: COALESCE with GROUP BY ====================

    @Test
    @Order(1)
    void coalesceOnGroupedColumnIsAllowed() throws Exception {
        // PG 18 allows COALESCE(x, constant) in SELECT when x is in GROUP BY
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT COALESCE(username, 'unknown') AS uname, count(*) AS cnt " +
                     "FROM users GROUP BY username ORDER BY uname")) {
            // Should not throw; COALESCE wrapping a grouped column is valid
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    @Order(2)
    void coalesceOnGroupedColumnWithJoin() throws Exception {
        // Mirrors the ACL query pattern:
        // GROUP BY g.rolname, gt.rolname ... SELECT COALESCE(gt.rolname, 'PUBLIC')
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_roles (oid int, rolname text)");
            s.execute("INSERT INTO test_roles VALUES (1, 'alice'), (2, NULL)");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT COALESCE(r2.rolname, 'PUBLIC') AS grantee, " +
                     "       r1.rolname AS grantor, " +
                     "       count(*) AS cnt " +
                     "FROM test_roles r1 " +
                     "LEFT JOIN test_roles r2 ON r1.oid = r2.oid " +
                     "GROUP BY r1.rolname, r2.rolname " +
                     "ORDER BY grantee")) {
            assertTrue(rs.next());
        }
    }

    @Test
    @Order(3)
    void caseExprOnGroupedColumnIsAllowed() throws Exception {
        // CASE WHEN on grouped column should also be allowed
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT CASE WHEN username IS NULL THEN 'anon' ELSE username END AS uname, " +
                     "       count(*) AS cnt " +
                     "FROM users GROUP BY username")) {
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    @Order(4)
    void castOnGroupedColumnIsAllowed() throws Exception {
        // Cast on grouped column should be allowed
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT username::varchar AS uname, count(*) AS cnt " +
                     "FROM users GROUP BY username")) {
            assertNotNull(rs.getMetaData());
        }
    }

    @Test
    @Order(5)
    void nullifOnGroupedColumnIsAllowed() throws Exception {
        // NULLIF on grouped column should be allowed
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT NULLIF(username, '') AS uname, count(*) AS cnt " +
                     "FROM users GROUP BY username")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // ==================== Issue 2: has_*_privilege return type ====================

    @Test
    @Order(10)
    void hasDatabasePrivilegeReturnsBoolean() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT has_database_privilege(db.oid, 'CREATE') as cancreate " +
                     "FROM pg_catalog.pg_database db " +
                     "WHERE db.datname = current_database()")) {
            assertTrue(rs.next());
            // PG 18 returns Boolean, not String
            Object val = rs.getObject("cancreate");
            assertInstanceOf(Boolean.class, val,
                    "has_database_privilege should return Boolean, got " +
                    val.getClass().getSimpleName() + " = " + val);
        }
    }

    @Test
    @Order(11)
    void hasSchemaPrivilegeReturnsBoolean() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT has_schema_privilege(nsp.oid, 'CREATE') as can_create, " +
                     "       has_schema_privilege(nsp.oid, 'USAGE') as has_usage " +
                     "FROM pg_catalog.pg_namespace nsp " +
                     "WHERE nsp.nspname = 'public'")) {
            assertTrue(rs.next());
            Object canCreate = rs.getObject("can_create");
            assertInstanceOf(Boolean.class, canCreate,
                    "has_schema_privilege('CREATE') should return Boolean, got " +
                    canCreate.getClass().getSimpleName() + " = " + canCreate);
            Object hasUsage = rs.getObject("has_usage");
            assertInstanceOf(Boolean.class, hasUsage,
                    "has_schema_privilege('USAGE') should return Boolean, got " +
                    hasUsage.getClass().getSimpleName() + " = " + hasUsage);
        }
    }

    @Test
    @Order(12)
    void hasTablePrivilegeReturnsBoolean() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT has_table_privilege('users', 'SELECT') as can_select")) {
            assertTrue(rs.next());
            Object val = rs.getObject("can_select");
            assertInstanceOf(Boolean.class, val,
                    "has_table_privilege should return Boolean, got " +
                    val.getClass().getSimpleName() + " = " + val);
        }
    }

    // ==================== Issue 3: pg_is_wal_replay_paused return type ====================

    @Test
    @Order(20)
    void walReplayPausedInCaseReturnsBoolean() throws Exception {
        // The recovery check query pattern
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT CASE WHEN usesuper " +
                     "       THEN pg_catalog.pg_is_in_recovery() " +
                     "       ELSE FALSE END as inrecovery, " +
                     "       CASE WHEN usesuper AND pg_catalog.pg_is_in_recovery() " +
                     "       THEN pg_is_wal_replay_paused() " +
                     "       ELSE FALSE END as isreplaypaused " +
                     "FROM pg_catalog.pg_user WHERE usename=current_user")) {
            assertTrue(rs.next());
            Object inrecovery = rs.getObject("inrecovery");
            assertInstanceOf(Boolean.class, inrecovery,
                    "inrecovery should be Boolean, got " +
                    inrecovery.getClass().getSimpleName() + " = " + inrecovery);
            Object isreplaypaused = rs.getObject("isreplaypaused");
            assertInstanceOf(Boolean.class, isreplaypaused,
                    "isreplaypaused should be Boolean, got " +
                    isreplaypaused.getClass().getSimpleName() + " = " + isreplaypaused);
        }
    }

    // ==================== Issue 4: Scalar subquery count() type ====================

    @Test
    @Order(30)
    void scalarSubqueryCountReturnsLong() throws Exception {
        // PG 18 returns count(*) as int8 (Long), not text
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT rel.relname AS name, " +
                     "  (SELECT count(*) FROM pg_catalog.pg_trigger " +
                     "   WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, " +
                     "  (SELECT count(1) FROM pg_catalog.pg_inherits " +
                     "   WHERE inhrelid=rel.oid LIMIT 1) as is_inherits " +
                     "FROM pg_catalog.pg_class rel " +
                     "WHERE rel.relname = 'users'")) {
            assertTrue(rs.next());
            Object triggercount = rs.getObject("triggercount");
            assertTrue(triggercount instanceof Long || triggercount instanceof Integer,
                    "triggercount should be numeric (Long/Integer), got " +
                    (triggercount == null ? "null" : triggercount.getClass().getSimpleName() + " = " + triggercount));
            Object isInherits = rs.getObject("is_inherits");
            assertTrue(isInherits instanceof Long || isInherits instanceof Integer,
                    "is_inherits should be numeric (Long/Integer), got " +
                    (isInherits == null ? "null" : isInherits.getClass().getSimpleName() + " = " + isInherits));
        }
    }

    @Test
    @Order(31)
    void scalarSubqueryCountMetadataType() throws Exception {
        // Verify the JDBC column type metadata is numeric, not text
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (SELECT count(*) FROM pg_catalog.pg_trigger " +
                     "        WHERE tgrelid=rel.oid) AS cnt " +
                     "FROM pg_catalog.pg_class rel WHERE rel.relname = 'users'")) {
            ResultSetMetaData md = rs.getMetaData();
            int colType = md.getColumnType(1);
            // Should be BIGINT (java.sql.Types.BIGINT = -5) or INTEGER (4)
            assertTrue(colType == java.sql.Types.BIGINT || colType == java.sql.Types.INTEGER,
                    "Scalar subquery count(*) column type should be BIGINT or INTEGER, got SQL type " + colType +
                    " (" + md.getColumnTypeName(1) + ")");
        }
    }

    // ==================== Issue 5: format_type for aclitem ====================

    @Test
    @Order(40)
    void formatTypeAclitemReturnsAclitem() throws Exception {
        // format_type(1033, NULL) should return 'aclitem', not 'unknown'
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_catalog.format_type(1033, NULL) AS typname")) {
            assertTrue(rs.next());
            assertEquals("aclitem", rs.getString("typname"),
                    "format_type(1033, NULL) should return 'aclitem'");
        }
    }

    @Test
    @Order(41)
    void formatTypeAclitemArrayReturnsAclitemArray() throws Exception {
        // format_type(1034, NULL) should return 'aclitem[]', not 'unknown'
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_catalog.format_type(1034, NULL) AS typname")) {
            assertTrue(rs.next());
            String result = rs.getString("typname");
            // PG 18 returns "aclitem[]"
            assertEquals("aclitem[]", result,
                    "format_type(1034, NULL) should return 'aclitem[]'");
        }
    }
}
