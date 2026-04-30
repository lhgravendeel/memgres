package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 36: Fix remaining PG 18 vs Memgres behavioral gaps found by
 * FeatureComparisonReport (clean run with DB reset).
 */
class Round36PgGapTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void assertSqlError(String sqlState, String sql) {
        try {
            exec(sql);
            fail("Expected SQL error " + sqlState + " but statement succeeded: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + "\nMessage: " + e.getMessage());
        }
    }

    // ========================================================================
    // 1. SECURITY LABEL — must error when no providers loaded (7 PG-vs-Memgres)
    // ========================================================================

    @Test
    void securityLabel_onTable_errorsNoProvider() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_sl_t (id int)");
        assertSqlError("22023", "SECURITY LABEL ON TABLE r36_sl_t IS 'secret'");
    }

    @Test
    void securityLabel_onColumn_errorsNoProvider() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_sl_c (v text)");
        assertSqlError("22023", "SECURITY LABEL ON COLUMN r36_sl_c.v IS 'confidential'");
    }

    @Test
    void securityLabel_onSchema_errorsNoProvider() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS r36_seclab");
        assertSqlError("22023", "SECURITY LABEL ON SCHEMA r36_seclab IS 'restricted'");
    }

    @Test
    void securityLabel_onFunction_errorsNoProvider() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r36_sl_fn() RETURNS int LANGUAGE SQL AS 'SELECT 1'");
        assertSqlError("22023", "SECURITY LABEL ON FUNCTION r36_sl_fn() IS 'sensitive'");
    }

    @Test
    void securityLabel_onRole_errorsNoProvider() throws SQLException {
        assertSqlError("22023", "SECURITY LABEL ON ROLE test IS 'admin-role'");
    }

    @Test
    void securityLabel_forProvider_errorsNotLoaded() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_sl_prov (id int)");
        assertSqlError("22023", "SECURITY LABEL FOR selinux ON TABLE r36_sl_prov IS 'system_u:object_r:sepgsql_table_t:s0'");
    }

    @Test
    void securityLabel_null_errorsNoProvider() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_sl_nil (id int)");
        assertSqlError("22023", "SECURITY LABEL ON TABLE r36_sl_nil IS NULL");
    }

    // ========================================================================
    // 2. Domain binary-compatible casts — must reject (1 diff + 3 setup)
    // ========================================================================

    @Test
    void domainBinaryCast_rejected() throws SQLException {
        exec("CREATE DOMAIN r36_dom_text AS text");
        assertSqlError("42P17", "CREATE CAST (text AS r36_dom_text) WITHOUT FUNCTION AS IMPLICIT");
    }

    // ========================================================================
    // 3. Partitioned PK must include partition columns (1 diff + 4 setup)
    // ========================================================================

    @Test
    void partitionedPk_mustIncludePartitionColumns() throws SQLException {
        // PG: ERROR 0A000: unique constraint on partitioned table must include all partitioning columns
        assertSqlError("0A000",
                "CREATE TABLE r36_fkp_p (id int PRIMARY KEY, region text) PARTITION BY LIST (region)");
    }

    // ========================================================================
    // 4. lo_put auto-creates large object (1 diff + 2 setup)
    // ========================================================================

    @Test
    void loPut_autoCreatesObject() throws SQLException {
        // PG: lo_put on a non-existent LO succeeds (auto-creates)
        exec("SELECT lo_create(998877)");
        // Verify lo_put works on existing object
        exec("SELECT lo_put(998877, 0, 'hello'::bytea)");
        // Now test on a non-existent LO — PG auto-creates, Memgres should too
        exec("SELECT lo_put(998878, 0, 'world'::bytea)");
        String ok = q("SELECT (count(*) >= 1)::text FROM pg_largeobject WHERE loid = 998878");
        assertEquals("true", ok);
        // Cleanup
        exec("SELECT lo_unlink(998877)");
        exec("SELECT lo_unlink(998878)");
    }

    // ========================================================================
    // 5. pg_opfamily — no phantom GIN/GiST integer_ops (2 diffs)
    // ========================================================================

    @Test
    void pgOpfamily_noGinIntegerOps() throws SQLException {
        String n = q("SELECT count(*)::int FROM pg_opfamily f JOIN pg_am a ON a.oid=f.opfmethod WHERE a.amname='gin' AND f.opfname='integer_ops'");
        assertEquals("0", n);
    }

    @Test
    void pgOpfamily_noGistIntegerOps() throws SQLException {
        String n = q("SELECT count(*)::int FROM pg_opfamily f JOIN pg_am a ON a.oid=f.opfmethod WHERE a.amname='gist' AND f.opfname='integer_ops'");
        assertEquals("0", n);
    }

    // ========================================================================
    // 6. string_agg(DISTINCT ... ORDER BY) — should work, not return NULL
    // ========================================================================

    @Test
    void stringAgg_distinctOrderBy() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_sagg (v int)");
        exec("INSERT INTO r36_sagg VALUES (3),(1),(2),(1),(3)");
        String result = q("SELECT string_agg(DISTINCT v::text, ',' ORDER BY v::text) FROM r36_sagg");
        assertNotNull(result, "string_agg(DISTINCT ... ORDER BY) returned NULL");
        assertTrue(result.contains("2"), "Result should contain '2': " + result);
    }

    // ========================================================================
    // 7. range_agg with FILTER — should work, not return NULL
    // ========================================================================

    @Test
    void rangeAgg_filter() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_ra (r int4range, incl boolean)");
        exec("INSERT INTO r36_ra VALUES ('[1,5)', true), ('[3,8)', false), ('[10,15)', true)");
        String result = q("SELECT range_agg(r) FILTER (WHERE incl)::text FROM r36_ra");
        assertNotNull(result, "range_agg with FILTER returned NULL");
        assertTrue(result.contains("[1,5)"), "Result should contain [1,5): " + result);
    }

    // ========================================================================
    // 8. ctid — unique per row
    // ========================================================================

    @Test
    void ctid_uniquePerRow() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_ctid (id int)");
        exec("INSERT INTO r36_ctid VALUES (1),(2),(3)");
        String d = q("SELECT count(DISTINCT ctid)::int FROM r36_ctid");
        assertEquals("3", d, "count(DISTINCT ctid) should equal number of rows");
    }

    // ========================================================================
    // 9. pg_promote() — must error when not in recovery
    // ========================================================================

    @Test
    void pgPromote_errorsNotInRecovery() throws SQLException {
        assertSqlError("55000", "SELECT pg_promote(false, 0)");
    }

    // ========================================================================
    // 10. extract(julian) — PG uses midnight-based Julian days (no -0.5)
    // ========================================================================

    @Test
    void extractJulian_midnightBased() throws SQLException {
        // PG: extract(julian from '2000-01-01'::date) = 2451545 (not 2451544.5)
        String val = q("SELECT extract(julian from '2000-01-01'::date)::text");
        assertTrue(val.startsWith("2451545"), "Julian for 2000-01-01 should start with 2451545, got: " + val);
    }

    // ========================================================================
    // 11. RESULT_OID removed from PL/pgSQL GET DIAGNOSTICS
    // ========================================================================

    @Test
    void resultOid_removedFromPlpgsql() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_roid (id int)");
        // PG 18 errors: unrecognized GET DIAGNOSTICS item "RESULT_OID"
        assertSqlError("42601",
                "CREATE FUNCTION r36_fn_roid() RETURNS oid AS $$ " +
                "DECLARE v_oid oid; BEGIN INSERT INTO r36_roid VALUES (1); " +
                "GET DIAGNOSTICS v_oid = RESULT_OID; RETURN v_oid; END; $$ LANGUAGE plpgsql");
    }

    // ========================================================================
    // 12. GRANT BY validates grantor = current user
    // ========================================================================

    @Test
    void grantBy_validatesCurrentUser() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_gt (id int)");
        exec("DROP ROLE IF EXISTS r36_grantor");
        exec("DROP ROLE IF EXISTS r36_grantee");
        exec("CREATE ROLE r36_grantor");
        exec("CREATE ROLE r36_grantee");
        // PG: ERROR 0A000: grantor must be current user
        assertSqlError("0A000", "GRANT SELECT ON r36_gt TO r36_grantee GRANTED BY r36_grantor");
    }

    // ========================================================================
    // 13. IMPORT FOREIGN SCHEMA — requires FDW handler
    // ========================================================================

    @Test
    void importForeignSchema_requiresHandler() throws SQLException {
        exec("CREATE FOREIGN DATA WRAPPER r36_fdw_imp");
        exec("CREATE SERVER r36_srv_imp FOREIGN DATA WRAPPER r36_fdw_imp");
        exec("CREATE SCHEMA IF NOT EXISTS r36_imp_target");
        // PG: ERROR 55000: foreign-data wrapper "r36_fdw_imp" has no handler
        assertSqlError("55000",
                "IMPORT FOREIGN SCHEMA public FROM SERVER r36_srv_imp INTO r36_imp_target");
    }

    // ========================================================================
    // 14. CLUSTER in autocommit — should succeed
    // ========================================================================

    @Test
    void cluster_inAutocommit() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r36_cl (id int PRIMARY KEY)");
        exec("INSERT INTO r36_cl VALUES (1)");
        // Should NOT error with "CLUSTER cannot run inside a transaction block"
        exec("CLUSTER r36_cl USING r36_cl_pkey");
    }

    // ========================================================================
    // 15. pg_stat_io — no op_bytes column in PG 18
    // ========================================================================

    @Test
    void pgStatIo_noOpBytesColumn() throws SQLException {
        assertSqlError("42703",
                "SELECT op_bytes FROM pg_stat_io LIMIT 1");
    }

    // ========================================================================
    // 16. pg_stat_user_indexes — no idx_blks_read column in PG 18
    // ========================================================================

    @Test
    void pgStatUserIndexes_noIdxBlksRead() throws SQLException {
        assertSqlError("42703",
                "SELECT idx_blks_read FROM pg_stat_user_indexes LIMIT 1");
    }

    // ========================================================================
    // 17. pg_stat_statements — requires shared_preload_libraries
    // ========================================================================

    @Test
    void pgStatStatements_requiresPreload() throws SQLException {
        assertSqlError("55000",
                "SELECT count(*) FROM pg_stat_statements");
    }

    @Test
    void pgStatStatementsInfo_requiresPreload() throws SQLException {
        assertSqlError("55000",
                "SELECT count(*) FROM pg_stat_statements_info");
    }

    @Test
    void pgStatStatementsReset_requiresPreload() throws SQLException {
        assertSqlError("55000",
                "SELECT pg_stat_statements_reset()");
    }

    @Test
    void showPgStatStatementsMax_unrecognized() throws SQLException {
        assertSqlError("42704",
                "SHOW pg_stat_statements.max");
    }
}
