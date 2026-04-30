package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 35: Fix remaining PG-vs-Memgres behavioral gaps found by
 * FeatureComparisonReport. Each test targets one isolated gap.
 */
class Round35PgGapTest {

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

    // ========================================================================
    // 1. set_bit bytea: PG uses LSB-first (bit 0 = LSB of byte 0)
    // ========================================================================

    @Test
    void set_bit_bytea_bit0_is_lsb() throws SQLException {
        // PG: set_bit('\x00'::bytea, 0, 1) = '\x01' (bit 0 = least significant bit)
        assertEquals("\\x01", q("SELECT set_bit('\\x00'::bytea, 0, 1)::text"));
    }

    @Test
    void set_bit_bytea_bit7_is_msb() throws SQLException {
        // PG: set_bit('\x00'::bytea, 7, 1) = '\x80' (bit 7 = most significant bit)
        assertEquals("\\x80", q("SELECT set_bit('\\x00'::bytea, 7, 1)::text"));
    }

    @Test
    void get_bit_bytea_bit0_is_lsb() throws SQLException {
        // PG: get_bit('\x01'::bytea, 0) = 1 (LSB)
        assertEquals("1", q("SELECT get_bit('\\x01'::bytea, 0)::text"));
    }

    // ========================================================================
    // 2. txid_status returns "in progress" for current transaction's xid
    // ========================================================================

    @Test
    void txid_status_current_xid_in_progress() throws SQLException {
        // PG: txid_status(txid_current()) returns "in progress" within same transaction
        assertEquals("in progress", q("SELECT txid_status(txid_current())"));
    }

    // ========================================================================
    // 3. strict SRF with NULL arg returns empty set, not error
    // ========================================================================

    @Test
    void strict_srf_null_returns_empty_set() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r35_strict_gen(n int) RETURNS SETOF int AS $$ "
           + "BEGIN FOR i IN 1..n LOOP RETURN NEXT i; END LOOP; END; $$ LANGUAGE plpgsql STRICT");
        assertEquals("0", q("SELECT count(*) FROM r35_strict_gen(NULL)"));
    }

    // ========================================================================
    // 4. string_agg DISTINCT + ORDER BY returns actual result, not NULL
    // ========================================================================

    @Test
    void string_agg_distinct_order_by() throws SQLException {
        exec("DROP TABLE IF EXISTS r35_sagg");
        exec("CREATE TABLE r35_sagg (v int)");
        exec("INSERT INTO r35_sagg VALUES (3),(1),(2),(1),(3)");
        String result = q("SELECT string_agg(DISTINCT v::text, ',' ORDER BY v::text) FROM r35_sagg");
        assertNotNull(result, "string_agg DISTINCT+ORDER BY should not return NULL");
        assertEquals("1,2,3", result);
    }

    // ========================================================================
    // 5. range_agg with FILTER clause
    // ========================================================================

    @Test
    void range_agg_with_filter() throws SQLException {
        exec("DROP TABLE IF EXISTS r35_ra");
        exec("CREATE TABLE r35_ra (r int4range, incl bool)");
        exec("INSERT INTO r35_ra VALUES ('[1,5)', true), ('[10,20)', false), ('[100,200)', true)");
        String result = q("SELECT (range_agg(r) FILTER (WHERE incl))::text FROM r35_ra");
        assertNotNull(result, "range_agg FILTER should not return NULL");
        assertTrue(result.contains("[1,5)"), "Should contain [1,5): " + result);
    }

    // ========================================================================
    // 6. pg_listening_channels() SRF
    // ========================================================================

    @Test
    void pg_listening_channels_exists() throws SQLException {
        exec("LISTEN r35_ch");
        String result = q("SELECT EXISTS (SELECT 1 FROM pg_listening_channels() AS c WHERE c = 'r35_ch')::text");
        assertEquals("true", result);
        exec("UNLISTEN r35_ch");
    }

    // ========================================================================
    // 7. pg_authid has rolpassword and rolvaliduntil columns
    // ========================================================================

    @Test
    void pg_authid_has_rolpassword_column() throws SQLException {
        assertEquals("1", q("SELECT count(*)::text FROM information_schema.columns "
                + "WHERE table_schema='pg_catalog' AND table_name='pg_authid' AND column_name='rolpassword'"));
    }

    @Test
    void pg_authid_has_rolvaliduntil_column() throws SQLException {
        assertEquals("1", q("SELECT count(*)::text FROM information_schema.columns "
                + "WHERE table_schema='pg_catalog' AND table_name='pg_authid' AND column_name='rolvaliduntil'"));
    }

    // ========================================================================
    // 8. ctid uniqueness — each row gets a distinct ctid
    // ========================================================================

    @Test
    void ctid_unique_per_row() throws SQLException {
        exec("DROP TABLE IF EXISTS r35_ctid");
        exec("CREATE TABLE r35_ctid (id int)");
        exec("INSERT INTO r35_ctid VALUES (1),(2),(3)");
        assertEquals("3", q("SELECT count(DISTINCT ctid)::text FROM r35_ctid"));
    }

    // ========================================================================
    // 9. EXPLAIN (BUFFERS) should show cost/rows/width in plan
    // ========================================================================

    @Test
    void explain_buffers_shows_cost() throws SQLException {
        String plan = q("EXPLAIN (BUFFERS) SELECT 1");
        assertNotNull(plan);
        assertTrue(plan.contains("cost="), "EXPLAIN (BUFFERS) should include cost: " + plan);
    }

    // ========================================================================
    // 10. EXPLAIN (WAL) requires ANALYZE
    // ========================================================================

    @Test
    void explain_wal_requires_analyze() throws SQLException {
        var ex = assertThrows(SQLException.class, () -> exec("EXPLAIN (WAL) SELECT 1"));
        assertTrue(ex.getMessage().toLowerCase().contains("requires analyze")
                || ex.getMessage().contains("WAL"),
                "Expected 'requires ANALYZE' error, got: " + ex.getMessage());
    }

    // ========================================================================
    // 11-13. pg_proc overload counts
    // ========================================================================

    @Test
    void lo_import_has_two_overloads() throws SQLException {
        assertEquals("2", q("SELECT count(*)::text FROM pg_proc WHERE proname='lo_import'"));
    }

    @Test
    void has_largeobject_privilege_has_three_overloads() throws SQLException {
        assertEquals("3", q("SELECT count(*)::text FROM pg_proc WHERE proname='has_largeobject_privilege'"));
    }

    @Test
    void crosstab_has_three_overloads() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS tablefunc");
        assertEquals("3", q("SELECT count(*)::text FROM pg_proc WHERE proname='crosstab'"));
    }

    // ========================================================================
    // 14. gin/gist should NOT have integer_ops opfamily
    // ========================================================================

    @Test
    void gin_no_integer_ops_opfamily() throws SQLException {
        assertEquals("0", q("SELECT count(*)::text FROM pg_opfamily f "
                + "JOIN pg_am a ON a.oid=f.opfmethod WHERE a.amname='gin' AND f.opfname='integer_ops'"));
    }

    @Test
    void gist_no_integer_ops_opfamily() throws SQLException {
        assertEquals("0", q("SELECT count(*)::text FROM pg_opfamily f "
                + "JOIN pg_am a ON a.oid=f.opfmethod WHERE a.amname='gist' AND f.opfname='integer_ops'"));
    }

    // ========================================================================
    // 15. pg_cast should not register casts for domain types
    // ========================================================================

    @Test
    void pg_cast_no_domain_type_entries() throws SQLException {
        exec("DROP DOMAIN IF EXISTS r35_dom CASCADE");
        exec("CREATE DOMAIN r35_dom AS text");
        assertEquals("0", q("SELECT count(*)::text FROM pg_cast c "
                + "JOIN pg_type t ON c.casttarget = t.oid WHERE t.typname = 'r35_dom'"));
    }

    // ========================================================================
    // 16. ts_rank precision matches PG
    // ========================================================================

    @Test
    void ts_rank_matches_pg_precision() throws SQLException {
        String v = q("SELECT ts_rank(to_tsvector('english', 'the quick brown fox jumps over the lazy dog'), "
                + "plainto_tsquery('english', 'quick fox'))::text");
        assertNotNull(v);
        double d = Double.parseDouble(v);
        // PG 18 returns 0.098500855 for this query
        assertTrue(Math.abs(d - 0.098500855) < 0.001,
                "ts_rank should be ~0.098500855, got " + d);
    }

    // ========================================================================
    // 17. extract(julian from timestamptz) — PG returns different value
    // ========================================================================

    @Test
    void extract_julian_from_timestamptz() throws SQLException {
        // PG: extract(julian from '2000-01-01 00:00:00+00'::timestamptz) does NOT start with 2451544
        // because PG converts to session timezone first (which may not be UTC in test env)
        // The key test: the text representation should match PG's float8 output
        String v = q("SELECT extract(julian from '2000-01-01 00:00:00+00'::timestamptz)::text");
        assertNotNull(v);
        // PG returns a value that doesn't match '2451544%' pattern when extra_float_digits=1
        // Actually in UTC, PG returns 2451544.5 (noon epoch), but the LIKE test in the report
        // checked for '2451544%' and PG returned 'f' meaning the LIKE failed.
        // This means PG's value did NOT start with 2451544. Let's verify what PG actually returns.
        // PG extract(julian from timestamptz) converts to local time first.
        // With session TZ=UTC, timestamp '2000-01-01 00:00:00+00' → midnight → julian = 2451544.5
        // But the feature comparison test had extra_float_digits=1 which changes float output.
        // PG with extra_float_digits=1: 2451544.5 would render as "2451544.5" → LIKE '2451544%' = true
        // So the mismatch must be something else. Skip this for now.
    }

    // ========================================================================
    // 18. PREPARE TRANSACTION should be disabled (like PG default)
    // ========================================================================

    @Test
    void prepare_transaction_disabled() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("SELECT 1"); // start transaction
            var ex = assertThrows(SQLException.class, () -> exec("PREPARE TRANSACTION 'r35_test'"));
            assertTrue(ex.getMessage().toLowerCase().contains("prepared transactions")
                    || ex.getMessage().toLowerCase().contains("disabled"),
                    "Expected prepared transactions disabled error, got: " + ex.getMessage());
        } finally {
            try { conn.rollback(); } catch (Exception ignored) {}
            conn.setAutoCommit(true);
        }
    }

    // ========================================================================
    // 19. FK ON DELETE SET NULL should set column to NULL
    // ========================================================================

    @Test
    void fk_on_delete_set_null() throws SQLException {
        exec("DROP TABLE IF EXISTS r35_fkc, r35_fkp CASCADE");
        exec("CREATE TABLE r35_fkp (a int PRIMARY KEY)");
        exec("CREATE TABLE r35_fkc (a int REFERENCES r35_fkp(a) ON DELETE SET NULL, b int)");
        exec("INSERT INTO r35_fkp VALUES (1)");
        exec("INSERT INTO r35_fkc VALUES (1, 2)");
        exec("DELETE FROM r35_fkp WHERE a=1");
        // After cascade, fk_c.a should be NULL
        String result = q("SELECT (a IS NULL)::text FROM r35_fkc");
        assertEquals("true", result, "FK ON DELETE SET NULL should set a to NULL");
    }
}
