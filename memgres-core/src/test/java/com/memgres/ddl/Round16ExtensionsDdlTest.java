package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category P: CREATE / ALTER EXTENSION.
 *
 * Covers:
 *  - CREATE EXTENSION VERSION / SCHEMA / CASCADE
 *  - ALTER EXTENSION UPDATE / SET SCHEMA / ADD / DROP
 */
class Round16ExtensionsDdlTest {

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

    // =========================================================================
    // P1. CREATE EXTENSION ... SCHEMA
    // =========================================================================

    @Test
    void create_extension_with_schema_installs_in_that_schema() throws SQLException {
        exec("DROP EXTENSION IF EXISTS pgcrypto CASCADE");
        exec("CREATE SCHEMA IF NOT EXISTS r16_ext_s");
        exec("CREATE EXTENSION pgcrypto WITH SCHEMA r16_ext_s");
        String n = str(
                "SELECT n.nspname FROM pg_extension e " +
                        "JOIN pg_namespace n ON e.extnamespace=n.oid " +
                        "WHERE e.extname='pgcrypto'");
        assertEquals("r16_ext_s", n,
                "CREATE EXTENSION pgcrypto WITH SCHEMA r16_ext_s must install in r16_ext_s; got '" + n + "'");
    }

    // =========================================================================
    // P2. CREATE EXTENSION ... VERSION
    // =========================================================================

    @Test
    void create_extension_with_version_recorded() throws SQLException {
        exec("DROP EXTENSION IF EXISTS pgcrypto CASCADE");
        // Request a version that exists — pgcrypto has a '1.3' variant. Fall back to 1.3.
        try {
            exec("CREATE EXTENSION pgcrypto VERSION '1.3'");
        } catch (SQLException e) {
            // If 1.3 is unknown, at least \"VERSION\" must parse and raise a version-related error
            // (not a syntax error).
            assertNotEquals("42601", e.getSQLState(),
                    "CREATE EXTENSION ... VERSION must parse; got SQLSTATE " + e.getSQLState());
            return;
        }
        String v = str("SELECT extversion FROM pg_extension WHERE extname='pgcrypto'");
        assertEquals("1.3", v,
                "pg_extension.extversion must reflect the VERSION clause; got '" + v + "'");
    }

    // =========================================================================
    // P3. CREATE EXTENSION ... CASCADE
    // =========================================================================

    @Test
    void create_extension_cascade_parses() throws SQLException {
        exec("DROP EXTENSION IF EXISTS pgcrypto CASCADE");
        exec("CREATE EXTENSION pgcrypto CASCADE");
        int n = Integer.parseInt(str(
                "SELECT count(*)::int FROM pg_extension WHERE extname='pgcrypto'"));
        assertEquals(1, n, "CREATE EXTENSION ... CASCADE must install pgcrypto");
    }

    // =========================================================================
    // P4. ALTER EXTENSION UPDATE
    // =========================================================================

    @Test
    void alter_extension_update_parses() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        // May be a no-op if already at latest; must not raise a parse/syntax error.
        try {
            exec("ALTER EXTENSION pgcrypto UPDATE");
        } catch (SQLException e) {
            assertNotEquals("42601", e.getSQLState(),
                    "ALTER EXTENSION ... UPDATE must parse; got SQLSTATE " + e.getSQLState());
        }
    }

    // =========================================================================
    // P5. ALTER EXTENSION SET SCHEMA
    // =========================================================================

    @Test
    void alter_extension_set_schema_moves_namespace() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS r16_ext_mv1");
        exec("CREATE SCHEMA IF NOT EXISTS r16_ext_mv2");
        exec("DROP EXTENSION IF EXISTS pgcrypto CASCADE");
        exec("CREATE EXTENSION pgcrypto WITH SCHEMA r16_ext_mv1");
        exec("ALTER EXTENSION pgcrypto SET SCHEMA r16_ext_mv2");
        String n = str(
                "SELECT n.nspname FROM pg_extension e " +
                        "JOIN pg_namespace n ON e.extnamespace=n.oid " +
                        "WHERE e.extname='pgcrypto'");
        assertEquals("r16_ext_mv2", n,
                "ALTER EXTENSION SET SCHEMA must update pg_extension.extnamespace; got '" + n + "'");
    }
}
