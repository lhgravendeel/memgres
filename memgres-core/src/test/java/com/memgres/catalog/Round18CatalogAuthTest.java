package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category S: Role / authentication catalog stubbing.
 *
 * Covers:
 *  - pg_authid present with superuser-only columns
 *  - rolvaliduntil populated from CREATE ROLE VALID UNTIL
 *  - rolconnlimit populated from CONNECTION LIMIT
 *  - rolconfig populated from ALTER ROLE SET
 *  - rolbypassrls honored
 */
class Round18CatalogAuthTest {

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

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static Integer intN(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            int v = rs.getInt(1);
            return rs.wasNull() ? null : v;
        }
    }

    private static boolean bool1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // S1. pg_authid exposes rolvaliduntil / rolconnlimit / rolpassword
    // =========================================================================

    @Test
    void pg_authid_exposes_superuser_columns() throws SQLException {
        // pg_authid is superuser-readable and must expose password-like cols.
        try (ResultSet rs = conn.getMetaData().getColumns(null, "pg_catalog", "pg_authid", "%")) {
            boolean hasPwd = false, hasValid = false, hasConn = false;
            while (rs.next()) {
                String c = rs.getString("COLUMN_NAME");
                if ("rolpassword".equals(c)) hasPwd = true;
                if ("rolvaliduntil".equals(c)) hasValid = true;
                if ("rolconnlimit".equals(c)) hasConn = true;
            }
            assertTrue(hasPwd, "pg_authid must expose rolpassword column");
            assertTrue(hasValid, "pg_authid must expose rolvaliduntil column");
            assertTrue(hasConn, "pg_authid must expose rolconnlimit column");
        }
    }

    // =========================================================================
    // S2. CREATE ROLE ... VALID UNTIL populates rolvaliduntil
    // =========================================================================

    @Test
    void create_role_valid_until_sets_rolvaliduntil() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rv");
        exec("CREATE ROLE r18_rv VALID UNTIL '2099-01-01 00:00:00+00'");
        String v = str(
                "SELECT to_char(rolvaliduntil, 'YYYY-MM-DD') FROM pg_roles WHERE rolname='r18_rv'");
        assertEquals("2099-01-01", v,
                "pg_roles.rolvaliduntil must reflect VALID UNTIL; got '" + v + "'");
    }

    // =========================================================================
    // S3. CREATE ROLE ... CONNECTION LIMIT populates rolconnlimit
    // =========================================================================

    @Test
    void create_role_connection_limit_populates() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rc");
        exec("CREATE ROLE r18_rc CONNECTION LIMIT 5");
        Integer v = intN("SELECT rolconnlimit FROM pg_roles WHERE rolname='r18_rc'");
        assertEquals(Integer.valueOf(5), v,
                "pg_roles.rolconnlimit must reflect CONNECTION LIMIT 5; got " + v);
    }

    // =========================================================================
    // S4. ALTER ROLE ... SET populates rolconfig
    // =========================================================================

    @Test
    void alter_role_set_populates_rolconfig() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rf");
        exec("CREATE ROLE r18_rf");
        exec("ALTER ROLE r18_rf SET work_mem = '42MB'");
        String v = str("SELECT array_to_string(rolconfig, ',') FROM pg_roles WHERE rolname='r18_rf'");
        assertNotNull(v, "rolconfig must be populated; got null");
        assertTrue(v.contains("work_mem=42MB"),
                "rolconfig must record 'work_mem=42MB'; got '" + v + "'");
    }

    // =========================================================================
    // S5. CREATE ROLE BYPASSRLS sets rolbypassrls
    // =========================================================================

    @Test
    void create_role_bypassrls_flag() throws SQLException {
        exec("DROP ROLE IF EXISTS r18_rb");
        exec("CREATE ROLE r18_rb BYPASSRLS");
        boolean b = bool1("SELECT rolbypassrls FROM pg_roles WHERE rolname='r18_rb'");
        assertTrue(b, "pg_roles.rolbypassrls must reflect BYPASSRLS flag");
    }

    // =========================================================================
    // S6. pg_db_role_setting exists and is queryable (pg_dump depends on it)
    // =========================================================================

    @Test
    void pg_db_role_setting_exists_and_queryable() throws SQLException {
        // pg_dump issues: SELECT unnest(setconfig) FROM pg_db_role_setting WHERE ...
        // The table must exist even if empty.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT setdatabase, setrole, setconfig FROM pg_catalog.pg_db_role_setting LIMIT 0")) {
            // Table exists and has the expected columns — no rows needed
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(), "pg_db_role_setting must have 3 columns");
            assertEquals("setdatabase", md.getColumnName(1));
            assertEquals("setrole", md.getColumnName(2));
            assertEquals("setconfig", md.getColumnName(3));
        }
    }

    @Test
    void pg_db_role_setting_unnest_query() throws SQLException {
        // Exact query pg_dump uses — must not throw "relation does not exist"
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT unnest(setconfig) FROM pg_db_role_setting WHERE setrole = 0 AND setdatabase = 0::oid")) {
            assertFalse(rs.next(), "empty pg_db_role_setting should return no rows");
        }
    }
}
