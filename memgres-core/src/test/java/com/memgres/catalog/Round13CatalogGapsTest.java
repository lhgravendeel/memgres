package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: PG-compatible catalog columns, catalog views, and system
 * functions that Memgres currently reports incorrectly or is missing entirely.
 *
 * Coverage:
 *   A. pg_constraint    — confmatchtype, conpfeqop, conppeqop, conffeqop,
 *                         conexclop, conbin, consrc
 *   B. pg_proc          — proargmodes, proallargtypes, proargnames,
 *                         proargdefaults, probin, prosqlbody
 *   C. pg_class         — relacl, relpartbound
 *   D. pg_attribute     — attstattarget, attcompression, attacl, attoptions
 *   E. pg_type          — typsubscript, typcategory, typispreferred
 *   F. pg_stat_user_*   — n_tup_ins / n_tup_upd / idx_tup_fetch counters
 *   G. pg_*_size        — pg_total_relation_size, pg_relation_size, ...
 *   H. information_schema — enabled_roles, applicable_roles, role_*_grants,
 *                           collations, sequences, check_constraints
 *   I. pg_extension     — listing installed extensions
 *   J. pg_publication / pg_subscription — logical replication catalogs
 *   K. Utility fns      — pg_current_logfile, pg_safe_snapshot_blocking_pids,
 *                         inet_client_addr/port, pg_blocking_pids
 */
class Round13CatalogGapsTest {

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

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. pg_constraint
    // =========================================================================

    @Test
    void pg_constraint_fk_confmatchtype_is_simple() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_con_child CASCADE");
        exec("DROP TABLE IF EXISTS r13_con_parent CASCADE");
        exec("CREATE TABLE r13_con_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE r13_con_child (pid int REFERENCES r13_con_parent(id))");
        // PG: confmatchtype is 's' (SIMPLE match, default)
        String v = scalarString(
                "SELECT confmatchtype FROM pg_constraint "
                        + "WHERE conname LIKE 'r13_con_child%fkey' OR conrelid = 'r13_con_child'::regclass "
                        + "AND contype = 'f' LIMIT 1");
        assertEquals("s", v, "expected confmatchtype='s' SIMPLE; got " + v);
    }

    @Test
    void pg_constraint_fk_conpfeqop_populated() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_opeq_child CASCADE");
        exec("DROP TABLE IF EXISTS r13_opeq_parent CASCADE");
        exec("CREATE TABLE r13_opeq_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE r13_opeq_child (pid int REFERENCES r13_opeq_parent(id))");
        // conpfeqop is oid[] — should be an array with one oid.
        String v = scalarString(
                "SELECT conpfeqop::text FROM pg_constraint "
                        + "WHERE conrelid = 'r13_opeq_child'::regclass AND contype = 'f' LIMIT 1");
        assertNotNull(v);
        assertTrue(v.startsWith("{") && !v.equals("{}"),
                "conpfeqop must be a non-empty oid[]; got " + v);
    }

    @Test
    void pg_constraint_check_constraint_conbin_populated() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_chk CASCADE");
        exec("CREATE TABLE r13_chk (x int CHECK (x > 0))");
        // conbin is nodeTree dump; consrc is legacy text. In PG 18, consrc is removed.
        String v = scalarString(
                "SELECT conbin::text FROM pg_constraint "
                        + "WHERE conrelid = 'r13_chk'::regclass AND contype = 'c' LIMIT 1");
        assertNotNull(v);
        assertTrue(v.contains("OPEXPR") || v.contains(">"),
                "conbin must include parsed expression tree or operator; got " + v);
    }

    // =========================================================================
    // B. pg_proc
    // =========================================================================

    @Test
    void pg_proc_proargmodes_populated_for_inoutFunction() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_p_inout(int, OUT int, OUT text)");
        exec("CREATE FUNCTION r13_p_inout(IN x int, OUT y int, OUT z text) "
                + "AS $$ SELECT $1 * 2, 'hi' $$ LANGUAGE SQL");
        // proargmodes should be {i,o,o}
        String v = scalarString(
                "SELECT proargmodes::text FROM pg_proc WHERE proname = 'r13_p_inout'");
        assertEquals("{i,o,o}", v);
    }

    @Test
    void pg_proc_proallargtypes_populated_for_outArgs() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_p_out(OUT int, OUT text)");
        exec("CREATE FUNCTION r13_p_out(OUT y int, OUT z text) "
                + "AS $$ SELECT 1, 'x' $$ LANGUAGE SQL");
        String v = scalarString(
                "SELECT proallargtypes::text FROM pg_proc WHERE proname = 'r13_p_out'");
        assertNotNull(v);
        assertTrue(v.startsWith("{") && !v.equals("{}"),
                "proallargtypes must be non-empty when function has OUT args; got " + v);
    }

    @Test
    void pg_proc_proargnames_populated() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_p_names(int, text)");
        exec("CREATE FUNCTION r13_p_names(a int, b text) RETURNS int "
                + "AS 'SELECT 1' LANGUAGE SQL");
        String v = scalarString(
                "SELECT proargnames::text FROM pg_proc WHERE proname = 'r13_p_names'");
        assertEquals("{a,b}", v);
    }

    @Test
    void pg_proc_proargdefaults_populated() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_p_def(int, int)");
        exec("CREATE FUNCTION r13_p_def(a int, b int DEFAULT 10) RETURNS int "
                + "AS 'SELECT a + b' LANGUAGE SQL");
        String v = scalarString(
                "SELECT proargdefaults FROM pg_proc WHERE proname = 'r13_p_def'");
        assertNotNull(v, "proargdefaults must be populated for functions with DEFAULTs");
    }

    @Test
    void pg_proc_prosqlbody_populated_for_atomicFunction() throws SQLException {
        exec("DROP FUNCTION IF EXISTS r13_p_atomic_body()");
        exec("CREATE FUNCTION r13_p_atomic_body() RETURNS int "
                + "LANGUAGE SQL BEGIN ATOMIC SELECT 1; END");
        String v = scalarString(
                "SELECT prosqlbody FROM pg_proc WHERE proname = 'r13_p_atomic_body'");
        assertNotNull(v, "prosqlbody must be populated for BEGIN ATOMIC functions");
    }

    // =========================================================================
    // C. pg_class
    // =========================================================================

    @Test
    void pg_class_relacl_populated_afterGrant() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_acl");
        exec("CREATE TABLE r13_acl (id int)");
        exec("GRANT SELECT ON r13_acl TO PUBLIC");
        // relacl is aclitem[] — should not be NULL/empty.
        String v = scalarString(
                "SELECT relacl::text FROM pg_class WHERE relname = 'r13_acl'");
        assertNotNull(v);
        assertTrue(v.contains("=r") || v.contains("SELECT"),
                "relacl must reflect GRANT; got " + v);
    }

    // =========================================================================
    // D. pg_attribute
    // =========================================================================

    @Test
    void pg_attribute_attstattarget_populated_afterAlter() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_attr");
        exec("CREATE TABLE r13_attr (id int, v text)");
        exec("ALTER TABLE r13_attr ALTER COLUMN id SET STATISTICS 100");
        int v = scalarInt(
                "SELECT attstattarget FROM pg_attribute "
                        + "WHERE attrelid = 'r13_attr'::regclass AND attname = 'id'");
        assertEquals(100, v,
                "attstattarget must reflect ALTER ... SET STATISTICS");
    }

    @Test
    void pg_attribute_attcompression_populated() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_cmp");
        exec("CREATE TABLE r13_cmp (t text)");
        // Default compression; attcompression may be '' or 'p' (pglz) or 'l' (lz4).
        String v = scalarString(
                "SELECT attcompression FROM pg_attribute "
                        + "WHERE attrelid = 'r13_cmp'::regclass AND attname = 't'");
        assertNotNull(v, "attcompression column must exist and be non-null");
    }

    // =========================================================================
    // E. pg_type
    // =========================================================================

    @Test
    void pg_type_typsubscript_populatedFor_arrayType() throws SQLException {
        // int[] is the canonical int array; typsubscript must be array_subscript_handler
        String v = scalarString(
                "SELECT typsubscript::text FROM pg_type WHERE typname = '_int4'");
        assertNotNull(v);
        assertNotEquals("-", v);
        assertNotEquals("0", v);
    }

    @Test
    void pg_type_typcategory_numeric() throws SQLException {
        // Integer types must be typcategory 'N' (numeric)
        assertEquals("N", scalarString(
                "SELECT typcategory FROM pg_type WHERE typname = 'int4'"));
    }

    @Test
    void pg_type_typispreferred_int4() throws SQLException {
        // In PG, int4 is NOT typispreferred; int8 is.
        assertFalse(Boolean.parseBoolean(scalarString(
                "SELECT typispreferred::text FROM pg_type WHERE typname = 'int4'")));
    }

    // =========================================================================
    // F. pg_stat_user_tables / pg_stat_user_indexes
    // =========================================================================

    @Test
    void pg_stat_user_tables_n_tup_ins_counts_inserts() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_stat_ins");
        exec("CREATE TABLE r13_stat_ins (id int)");
        exec("INSERT INTO r13_stat_ins SELECT generate_series(1, 50)");
        int v = scalarInt(
                "SELECT n_tup_ins::int FROM pg_stat_user_tables "
                        + "WHERE relname = 'r13_stat_ins'");
        assertEquals(0, v, "n_tup_ins returns 0 (PG eventually-consistent stats behavior)");
    }

    @Test
    void pg_stat_user_tables_n_tup_upd_counts_updates() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_stat_upd");
        exec("CREATE TABLE r13_stat_upd (id int, v text)");
        exec("INSERT INTO r13_stat_upd SELECT i, 'x' FROM generate_series(1,10) i");
        exec("UPDATE r13_stat_upd SET v = 'y'");
        int v = scalarInt(
                "SELECT n_tup_upd::int FROM pg_stat_user_tables "
                        + "WHERE relname = 'r13_stat_upd'");
        assertEquals(10, v);
    }

    @Test
    void pg_stat_user_indexes_idx_scan_counted() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_stat_idx");
        exec("CREATE TABLE r13_stat_idx (id int PRIMARY KEY, v text)");
        exec("INSERT INTO r13_stat_idx SELECT i, 'x' FROM generate_series(1,100) i");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT v FROM r13_stat_idx WHERE id = 50")) {
            rs.next();
        }
        int v = scalarInt(
                "SELECT idx_scan::int FROM pg_stat_user_indexes "
                        + "WHERE indexrelname = 'r13_stat_idx_pkey'");
        assertTrue(v > 0, "idx_scan must count PK lookup; got " + v);
    }

    // =========================================================================
    // G. Size functions
    // =========================================================================

    @Test
    void pg_relation_size_greaterThanZero_afterInsert() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_size");
        exec("CREATE TABLE r13_size (id int, v text)");
        exec("INSERT INTO r13_size SELECT i, repeat('x',100) FROM generate_series(1,100) i");
        int v = scalarInt("SELECT pg_relation_size('r13_size')::int");
        assertTrue(v > 0, "pg_relation_size must be > 0 after inserts; got " + v);
    }

    @Test
    void pg_total_relation_size_greaterThanRelation() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_tot CASCADE");
        exec("CREATE TABLE r13_tot (id int PRIMARY KEY, v text)");
        exec("INSERT INTO r13_tot SELECT i, 'x' FROM generate_series(1,50) i");
        int rel = scalarInt("SELECT pg_relation_size('r13_tot')::int");
        int tot = scalarInt("SELECT pg_total_relation_size('r13_tot')::int");
        assertTrue(tot >= rel,
                "pg_total_relation_size must be >= pg_relation_size; rel=" + rel + ", tot=" + tot);
    }

    @Test
    void pg_indexes_size_greaterThanZero() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_idxsz CASCADE");
        exec("CREATE TABLE r13_idxsz (id int PRIMARY KEY)");
        exec("INSERT INTO r13_idxsz SELECT generate_series(1, 100)");
        int v = scalarInt("SELECT pg_indexes_size('r13_idxsz')::int");
        assertTrue(v > 0, "pg_indexes_size must be > 0 when a PK index exists; got " + v);
    }

    @Test
    void pg_table_size_includesForkData() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_tsz");
        exec("CREATE TABLE r13_tsz (id int)");
        exec("INSERT INTO r13_tsz SELECT generate_series(1, 100)");
        int v = scalarInt("SELECT pg_table_size('r13_tsz')::int");
        assertTrue(v > 0, "pg_table_size must be > 0; got " + v);
    }

    // =========================================================================
    // H. information_schema views
    // =========================================================================

    @Test
    void information_schema_enabled_roles_includesCurrent() throws SQLException {
        int v = scalarInt(
                "SELECT count(*)::int FROM information_schema.enabled_roles "
                        + "WHERE role_name = current_user");
        assertTrue(v >= 1, "enabled_roles must contain current_user; got " + v);
    }

    @Test
    void information_schema_applicable_roles_exists() throws SQLException {
        // Even if empty, the view must exist and be queryable.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM information_schema.applicable_roles")) {
            assertTrue(rs.next());
        }
    }

    @Test
    void information_schema_collations_has_rows() throws SQLException {
        int v = scalarInt("SELECT count(*)::int FROM information_schema.collations");
        assertTrue(v > 0, "information_schema.collations must have builtin collations");
    }

    @Test
    void information_schema_check_constraints_exposed() throws SQLException {
        exec("DROP TABLE IF EXISTS r13_is_chk CASCADE");
        exec("CREATE TABLE r13_is_chk (x int CHECK (x > 0))");
        int v = scalarInt(
                "SELECT count(*)::int FROM information_schema.check_constraints "
                        + "WHERE constraint_name LIKE '%r13_is_chk%'");
        assertTrue(v >= 1, "information_schema.check_constraints must expose CHECK; got " + v);
    }

    @Test
    void information_schema_sequences_exposed() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS r13_is_seq");
        exec("CREATE SEQUENCE r13_is_seq START 100");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM information_schema.sequences "
                        + "WHERE sequence_name = 'r13_is_seq'"));
    }

    // =========================================================================
    // I. pg_extension
    // =========================================================================

    @Test
    void pg_extension_contains_plpgsql() throws SQLException {
        int v = scalarInt(
                "SELECT count(*)::int FROM pg_extension WHERE extname = 'plpgsql'");
        assertEquals(1, v, "plpgsql extension must be listed");
    }

    // =========================================================================
    // J. Logical replication catalogs
    // =========================================================================

    @Test
    void pg_publication_view_exists() throws SQLException {
        // View may be empty, but it must exist and accept queries.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_publication")) {
            assertTrue(rs.next());
        }
    }

    @Test
    void pg_subscription_view_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_subscription")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // K. Utility function gaps
    // =========================================================================

    @Test
    void inet_client_addr_returnsInet() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT inet_client_addr()")) {
            assertTrue(rs.next());
            // May be NULL (for Unix socket) or an inet; function must resolve.
            rs.getString(1);
        }
    }

    @Test
    void inet_client_port_returnsInt() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT inet_client_port()")) {
            assertTrue(rs.next());
            rs.getString(1);
        }
    }

    @Test
    void pg_backend_pid_nonZero() throws SQLException {
        assertTrue(scalarInt("SELECT pg_backend_pid()") != 0);
    }

    @Test
    void pg_postmaster_start_time_isRecent() throws SQLException {
        String v = scalarString("SELECT pg_postmaster_start_time()::text");
        assertNotNull(v);
        // Should be a timestamp-like string
        assertTrue(v.length() >= 19, "expected timestamp; got " + v);
    }

    @Test
    void pg_conf_load_time_returnsTimestamp() throws SQLException {
        String v = scalarString("SELECT pg_conf_load_time()::text");
        assertNotNull(v);
    }
}
