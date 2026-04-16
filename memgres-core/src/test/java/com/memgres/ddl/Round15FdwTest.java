package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category D: Foreign Data Wrapper DDL + catalogs.
 *
 * In-memory Memgres can't actually reach a remote server, but it should
 * still support the FDW DDL surface and populate catalogs so ORMs and
 * pg_dump don't choke.
 *
 * Covers:
 *  - CREATE FOREIGN DATA WRAPPER
 *  - CREATE SERVER
 *  - CREATE USER MAPPING
 *  - CREATE FOREIGN TABLE
 *  - IMPORT FOREIGN SCHEMA
 *  - pg_foreign_data_wrapper / pg_foreign_server / pg_user_mapping /
 *    pg_foreign_table populated
 */
class Round15FdwTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. CREATE FOREIGN DATA WRAPPER + pg_foreign_data_wrapper
    // =========================================================================

    @Test
    void create_foreign_data_wrapper_visible() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_basic");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_data_wrapper WHERE fdwname='r15_fdw_basic'");
        assertEquals(1, n, "CREATE FDW must populate pg_foreign_data_wrapper");
    }

    @Test
    void create_foreign_data_wrapper_with_options() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_opts "
                + "OPTIONS (debug 'true', retries '3')");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_data_wrapper "
                        + "WHERE fdwname='r15_fdw_opts' AND fdwoptions IS NOT NULL");
        assertEquals(1, n);
    }

    @Test
    void drop_foreign_data_wrapper() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_drop");
        exec("DROP FOREIGN DATA WRAPPER r15_fdw_drop");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_data_wrapper WHERE fdwname='r15_fdw_drop'");
        assertEquals(0, n);
    }

    // =========================================================================
    // B. CREATE SERVER + pg_foreign_server
    // =========================================================================

    @Test
    void create_server_visible() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_s");
        exec("CREATE SERVER r15_srv FOREIGN DATA WRAPPER r15_fdw_s");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_server WHERE srvname='r15_srv'");
        assertEquals(1, n, "CREATE SERVER must populate pg_foreign_server");
    }

    @Test
    void create_server_with_options() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_so");
        exec("CREATE SERVER r15_srv_opts FOREIGN DATA WRAPPER r15_fdw_so "
                + "OPTIONS (host 'localhost', port '5432', dbname 'foo')");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_server "
                        + "WHERE srvname='r15_srv_opts' AND srvoptions IS NOT NULL");
        assertEquals(1, n);
    }

    @Test
    void alter_server_options_appends_and_replaces() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_a");
        exec("CREATE SERVER r15_srv_a FOREIGN DATA WRAPPER r15_fdw_a "
                + "OPTIONS (host 'h1')");
        exec("ALTER SERVER r15_srv_a OPTIONS (SET host 'h2', ADD port '5432')");
        // Just confirm no error and server still present
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_server WHERE srvname='r15_srv_a'");
        assertEquals(1, n);
    }

    // =========================================================================
    // C. CREATE USER MAPPING + pg_user_mapping
    // =========================================================================

    @Test
    void create_user_mapping_visible() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_um");
        exec("CREATE SERVER r15_srv_um FOREIGN DATA WRAPPER r15_fdw_um");
        exec("CREATE USER MAPPING FOR CURRENT_USER SERVER r15_srv_um "
                + "OPTIONS (user 'u', password 'p')");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_user_mapping m "
                        + "JOIN pg_foreign_server s ON m.umserver = s.oid "
                        + "WHERE s.srvname='r15_srv_um'");
        assertEquals(1, n, "CREATE USER MAPPING must populate pg_user_mapping");
    }

    @Test
    void create_user_mapping_for_public() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_pu");
        exec("CREATE SERVER r15_srv_pu FOREIGN DATA WRAPPER r15_fdw_pu");
        exec("CREATE USER MAPPING FOR PUBLIC SERVER r15_srv_pu");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_user_mappings m "
                        + "WHERE m.srvname='r15_srv_pu'");
        assertTrue(n >= 1, "pg_user_mappings view should show the mapping");
    }

    // =========================================================================
    // D. CREATE FOREIGN TABLE + pg_foreign_table
    // =========================================================================

    @Test
    void create_foreign_table_visible() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_ft");
        exec("CREATE SERVER r15_srv_ft FOREIGN DATA WRAPPER r15_fdw_ft");
        exec("CREATE FOREIGN TABLE r15_ft_t (id int, name text) "
                + "SERVER r15_srv_ft OPTIONS (schema_name 'public', table_name 't')");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_foreign_table ft "
                        + "JOIN pg_class c ON ft.ftrelid = c.oid "
                        + "WHERE c.relname='r15_ft_t'");
        assertEquals(1, n, "CREATE FOREIGN TABLE must populate pg_foreign_table");
    }

    @Test
    void foreign_table_relkind_is_f() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_rk");
        exec("CREATE SERVER r15_srv_rk FOREIGN DATA WRAPPER r15_fdw_rk");
        exec("CREATE FOREIGN TABLE r15_ft_rk (id int) SERVER r15_srv_rk");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_class WHERE relname='r15_ft_rk' AND relkind='f'");
        assertEquals(1, n, "Foreign table should have pg_class.relkind='f'");
    }

    @Test
    void foreign_table_has_columns_in_pg_attribute() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_cols");
        exec("CREATE SERVER r15_srv_cols FOREIGN DATA WRAPPER r15_fdw_cols");
        exec("CREATE FOREIGN TABLE r15_ft_cols (id int, nm text) SERVER r15_srv_cols");

        int n = scalarInt(
                "SELECT count(*)::int FROM pg_attribute a "
                        + "JOIN pg_class c ON a.attrelid = c.oid "
                        + "WHERE c.relname='r15_ft_cols' AND NOT a.attisdropped AND a.attnum > 0");
        assertEquals(2, n, "Foreign table columns must appear in pg_attribute");
    }

    // =========================================================================
    // E. IMPORT FOREIGN SCHEMA — at minimum, accept the syntax
    // =========================================================================

    @Test
    void import_foreign_schema_syntax_accepted() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_imp");
        exec("CREATE SERVER r15_srv_imp FOREIGN DATA WRAPPER r15_fdw_imp");
        exec("CREATE SCHEMA r15_imp_target");
        // PG requires the foreign server to actually provide the schema; for
        // a stub engine we only require the DDL statement to parse and not error.
        try {
            exec("IMPORT FOREIGN SCHEMA public FROM SERVER r15_srv_imp "
                    + "INTO r15_imp_target");
        } catch (SQLException e) {
            // Acceptable for a stubbed FDW: an error mentioning FDW/server is OK.
            // But a syntax error is NOT OK — surface it.
            String msg = (e.getMessage() == null ? "" : e.getMessage().toLowerCase());
            assertTrue(msg.contains("foreign") || msg.contains("server")
                            || msg.contains("wrapper") || msg.contains("handler")
                            || msg.contains("not supported"),
                    "IMPORT FOREIGN SCHEMA error must reference FDW; got: " + msg);
        }
    }

    @Test
    void import_foreign_schema_with_limit_to_syntax() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r15_fdw_l");
        exec("CREATE SERVER r15_srv_l FOREIGN DATA WRAPPER r15_fdw_l");
        exec("CREATE SCHEMA r15_imp_l");
        try {
            exec("IMPORT FOREIGN SCHEMA public LIMIT TO (t1, t2) "
                    + "FROM SERVER r15_srv_l INTO r15_imp_l");
        } catch (SQLException e) {
            String msg = (e.getMessage() == null ? "" : e.getMessage().toLowerCase());
            assertTrue(msg.contains("foreign") || msg.contains("server")
                            || msg.contains("wrapper") || msg.contains("handler")
                            || msg.contains("not supported"),
                    "LIMIT TO clause parse must reference FDW in error; got: " + msg);
        }
    }
}
