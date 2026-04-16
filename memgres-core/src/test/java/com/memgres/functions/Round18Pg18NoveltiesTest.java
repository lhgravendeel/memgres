package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category R: PG 18 novelty features.
 *
 * Covers:
 *  - Bare uuidv4() / uuidv7() / uuidv1() constructors
 *  - uuid_extract_timestamp / uuid_extract_version
 *  - ANY_VALUE aggregate
 *  - has_largeobject_privilege
 *  - log_lock_failures GUC
 *  - U&"..." / UESCAPE identifiers and literals
 *  - pg_publication / pg_subscription catalog existence
 */
class Round18Pg18NoveltiesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // R1. Bare uuidv4() / uuidv7()
    // =========================================================================

    @Test
    void uuidv4_bare_constructor_returns_uuid() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT uuidv4()")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v, "uuidv4() must return a non-null uuid");
            assertTrue(v.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
                    "uuidv4() must return a v4 UUID; got: " + v);
        }
    }

    @Test
    void uuidv7_bare_constructor_returns_uuid() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT uuidv7()")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v, "uuidv7() must return a non-null uuid");
            // v7: version nibble must be '7'
            assertTrue(v.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
                    "uuidv7() must return a v7 UUID; got: " + v);
        }
    }

    // =========================================================================
    // R2. uuid_extract_timestamp / uuid_extract_version
    // =========================================================================

    @Test
    void uuid_extract_version_in_pg_proc() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='uuid_extract_version'") >= 1,
                "uuid_extract_version must be registered in pg_proc");
    }

    @Test
    void uuid_extract_timestamp_in_pg_proc() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='uuid_extract_timestamp'") >= 1,
                "uuid_extract_timestamp must be registered in pg_proc");
    }

    // =========================================================================
    // R3. ANY_VALUE aggregate
    // =========================================================================

    @Test
    void any_value_aggregate_returns_some_value() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_anyv");
        exec("CREATE TABLE r18_anyv(g int, v int)");
        exec("INSERT INTO r18_anyv VALUES (1,10),(1,20),(2,30)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT g, any_value(v) FROM r18_anyv GROUP BY g ORDER BY g")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            int v1 = rs.getInt(2);
            assertTrue(v1 == 10 || v1 == 20, "any_value(v) for group 1 must be 10 or 20; got " + v1);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(30, rs.getInt(2));
        }
    }

    // =========================================================================
    // R4. has_largeobject_privilege
    // =========================================================================

    @Test
    void has_largeobject_privilege_in_pg_proc() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_proc WHERE proname='has_largeobject_privilege'") >= 1,
                "has_largeobject_privilege must be registered in pg_proc");
    }

    // =========================================================================
    // R5. log_lock_failures GUC
    // =========================================================================

    @Test
    void log_lock_failures_guc_exists() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_settings WHERE name='log_lock_failures'") >= 1,
                "log_lock_failures GUC must appear in pg_settings");
    }

    // =========================================================================
    // R6. U&"..." unicode identifier escapes
    // =========================================================================

    @Test
    void unicode_identifier_escape_resolves() throws SQLException {
        // U&"d\0061t\+000061" should be identifier "data"
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 AS U&\"d\\0061t\\0061\"")) {
            assertTrue(rs.next());
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("data", md.getColumnLabel(1),
                    "U&\"d\\0061t\\0061\" must resolve to identifier 'data'; got " + md.getColumnLabel(1));
        }
    }

    @Test
    void unicode_string_literal_escape_resolves() throws SQLException {
        // U&'d\0061t\0061' should equal 'data'
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT U&'d\\0061t\\0061'")) {
            assertTrue(rs.next());
            assertEquals("data", rs.getString(1),
                    "U&'d\\0061t\\0061' must decode to 'data'");
        }
    }

    // =========================================================================
    // R7. pg_publication / pg_subscription catalogs
    // =========================================================================

    @Test
    void pg_publication_is_queryable() throws SQLException {
        // Must not 42P01 (undefined relation); row count may be 0.
        int1("SELECT count(*)::int FROM pg_publication");
    }

    @Test
    void pg_subscription_is_queryable() throws SQLException {
        int1("SELECT count(*)::int FROM pg_subscription");
    }
}
