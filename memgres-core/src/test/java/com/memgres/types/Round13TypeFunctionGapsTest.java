package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: built-in type/utility functions that PG 18 ships with but
 * that Memgres has not yet implemented (or has implemented only partially).
 *
 * These tests pin down PG 18's observable behavior. They are EXPECTED to fail
 * on Memgres today. No Memgres source code is modified in this turn.
 *
 * Coverage:
 *   A. Numeric utilities        — trim_scale, random(min,max), scale(), min_scale()
 *   B. Bit string functions     — bit_count, getbit, setbit, get_bit, set_bit
 *   C. JSONB helpers            — jsonb_path_query_array, json_populate_record,
 *                                 json_populate_recordset, json_to_record, jsonb_to_record
 *   D. Full-text search         — ts_rank / ts_rank_cd precision, tsquery_phrase
 *   E. Array slicing            — arr[lo:hi], arr[:hi], arr[lo:], arr[lo:hi, lo:hi]
 *   F. reg* types               — regconfig, regdictionary, regnamespace, regrole
 *   G. Network helpers          — inet_merge, hostmask
 *   H. System info              — pg_current_logfile, txid_current_if_assigned,
 *                                 pg_safe_snapshot_blocking_pids
 */
class Round13TypeFunctionGapsTest {

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
    // A. Numeric utilities
    // =========================================================================

    /** trim_scale strips trailing zeros after the decimal point. PG 13+. */
    @Test
    void trim_scale_stripsTrailingZeros() throws SQLException {
        assertEquals("123.45", scalarString("SELECT trim_scale(NUMERIC '123.4500')"));
    }

    @Test
    void trim_scale_noTrailingZerosLeavesUnchanged() throws SQLException {
        assertEquals("123.45", scalarString("SELECT trim_scale(NUMERIC '123.45')"));
    }

    @Test
    void trim_scale_allZeroFraction() throws SQLException {
        assertEquals("123", scalarString("SELECT trim_scale(NUMERIC '123.0000')"));
    }

    /** random(lo, hi) returns a bigint/numeric in [lo,hi]. PG 17+ — bigint overload. */
    @Test
    void random_bigintMinMax_withinRange() throws SQLException {
        for (int i = 0; i < 20; i++) {
            int v = scalarInt("SELECT random(1, 10)::int");
            assertTrue(v >= 1 && v <= 10,
                    "random(1,10) must be in [1,10]; got " + v);
        }
    }

    @Test
    void random_numericMinMax_withinRange() throws SQLException {
        for (int i = 0; i < 20; i++) {
            String v = scalarString("SELECT random(0.0::numeric, 1.0::numeric)");
            assertNotNull(v);
        }
    }

    /** scale() returns the scale (digits after decimal point) of a numeric. */
    @Test
    void scale_ofNumericLiteral() throws SQLException {
        assertEquals(2, scalarInt("SELECT scale(NUMERIC '10.25')"));
    }

    @Test
    void scale_ofIntegerNumeric() throws SQLException {
        assertEquals(0, scalarInt("SELECT scale(NUMERIC '100')"));
    }

    /** min_scale() returns smallest scale that preserves the value. PG 13+. */
    @Test
    void min_scale_stripsTrailingZeros() throws SQLException {
        assertEquals(2, scalarInt("SELECT min_scale(NUMERIC '10.2500')"));
    }

    // =========================================================================
    // B. Bit string functions
    // =========================================================================

    /** bit_count counts set bits in a bytea/bit value. PG 14+. */
    @Test
    void bit_count_bytea() throws SQLException {
        // 0xFF has 8 one-bits
        assertEquals(8, scalarInt("SELECT bit_count('\\xff'::bytea)::int"));
    }

    @Test
    void bit_count_bitString() throws SQLException {
        assertEquals(3, scalarInt("SELECT bit_count(B'1010100')::int"));
    }

    /** get_bit(bytea, n) reads a single bit. PG standard. */
    @Test
    void get_bit_byteaLSB() throws SQLException {
        // byte 0xFF: bit 0 = 1
        assertEquals(1, scalarInt("SELECT get_bit('\\xff'::bytea, 0)"));
    }

    @Test
    void get_bit_byteaZero() throws SQLException {
        assertEquals(0, scalarInt("SELECT get_bit('\\x00'::bytea, 0)"));
    }

    /** set_bit(bytea, n, val) returns bytea with bit set. */
    @Test
    void set_bit_changesSingleBit() throws SQLException {
        // PG: bit 0 is the MSB of byte 0. Setting bit 0 of 0x00 → 0x80.
        assertEquals("\\x80",
                scalarString("SELECT set_bit('\\x00'::bytea, 0, 1)"));
    }

    /** get_bit / set_bit on bit-string values (PG: getbit/setbit names). */
    @Test
    void getbit_bitStringSupported() throws SQLException {
        assertEquals(1, scalarInt("SELECT get_bit(B'10000000', 0)"));
    }

    // =========================================================================
    // C. JSONB helpers
    // =========================================================================

    /** jsonb_path_query_array aggregates all path matches into one jsonb array. PG 12+. */
    @Test
    void jsonb_path_query_array_simple() throws SQLException {
        assertEquals("[1, 2, 3]",
                scalarString("SELECT jsonb_path_query_array('[1,2,3]'::jsonb, '$[*]')"));
    }

    @Test
    void jsonb_path_query_array_noMatches_returnsEmpty() throws SQLException {
        assertEquals("[]",
                scalarString("SELECT jsonb_path_query_array('{}'::jsonb, '$.nothing')"));
    }

    /** jsonb_path_query_first returns first match or NULL. */
    @Test
    void jsonb_path_query_first_returnsFirstMatch() throws SQLException {
        assertEquals("1",
                scalarString("SELECT jsonb_path_query_first('[1,2,3]'::jsonb, '$[*]')::text"));
    }

    /** json_populate_record materializes a JSON object into a row of a composite type. */
    @Test
    void json_populate_record_mapsFields() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TYPE IF EXISTS r13_prec CASCADE");
            s.execute("CREATE TYPE r13_prec AS (a int, b text)");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (json_populate_record(NULL::r13_prec, '{\"a\":1,\"b\":\"x\"}'::json)).a")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void json_populate_recordset_expandsArray() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TYPE IF EXISTS r13_prs CASCADE");
            s.execute("CREATE TYPE r13_prs AS (a int)");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a FROM json_populate_recordset(NULL::r13_prs, "
                             + "'[{\"a\":1},{\"a\":2}]'::json) ORDER BY a")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    /** json_to_record converts JSON object to on-the-fly anonymous row type. */
    @Test
    void json_to_record_withColumnList() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM json_to_record('{\"a\":1,\"b\":\"x\"}'::json) "
                             + "AS x(a int, b text)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals("x", rs.getString("b"));
        }
    }

    /** jsonb_path_match returns a boolean result for path with boolean expression. */
    @Test
    void jsonb_path_match_trueCondition() throws SQLException {
        assertEquals("true",
                scalarString("SELECT jsonb_path_match('{\"a\":1}'::jsonb, '$.a == 1')::text"));
    }

    // =========================================================================
    // D. Full-text search
    // =========================================================================

    /** ts_rank should match PG 18 precision (0.0607927 for the standard corpus). */
    @Test
    void ts_rank_precision_matchesPg() throws SQLException {
        // PG 18 value for this exact query is 0.0607927 (sometimes formatted 0.06079271).
        String v = scalarString(
                "SELECT ts_rank(to_tsvector('english', 'the quick brown fox jumps over the lazy dog'), "
                        + "plainto_tsquery('english', 'quick fox'))::text");
        assertNotNull(v);
        double d = Double.parseDouble(v);
        // PG's float4 output for this corpus is ~0.0607927.
        // Memgres often returns 0.0624 (different scoring algorithm).
        assertTrue(Math.abs(d - 0.0607927) < 1e-4,
                "ts_rank must match PG 18 precision; expected ~0.0607927, got " + d);
    }

    /** ts_rank_cd (cover density ranking). */
    @Test
    void ts_rank_cd_returnsNonZeroForMatch() throws SQLException {
        String v = scalarString(
                "SELECT ts_rank_cd(to_tsvector('english', 'the quick brown fox'), "
                        + "plainto_tsquery('english', 'fox'))::text");
        assertNotNull(v);
        assertTrue(Double.parseDouble(v) > 0.0);
    }

    // =========================================================================
    // E. Array slicing
    // =========================================================================

    /** arr[lo:hi] returns a sub-array. PG standard. */
    @Test
    void array_slice_loToHi() throws SQLException {
        assertEquals("{2,3,4}",
                scalarString("SELECT (ARRAY[1,2,3,4,5])[2:4]"));
    }

    @Test
    void array_slice_openLower() throws SQLException {
        assertEquals("{1,2,3}",
                scalarString("SELECT (ARRAY[1,2,3,4,5])[:3]"));
    }

    @Test
    void array_slice_openUpper() throws SQLException {
        assertEquals("{3,4,5}",
                scalarString("SELECT (ARRAY[1,2,3,4,5])[3:]"));
    }

    @Test
    void array_slice_multiDim() throws SQLException {
        // 2D slice: full first-dim (1:2), second-dim 1:1 → gives {{1},{3}}
        assertEquals("{{1},{3}}",
                scalarString("SELECT (ARRAY[[1,2],[3,4]])[1:2][1:1]"));
    }

    @Test
    void array_slice_outOfRange_returnsEmpty() throws SQLException {
        assertEquals("{}",
                scalarString("SELECT (ARRAY[1,2,3])[10:20]"));
    }

    // =========================================================================
    // F. reg* types (besides regclass/regtype/regproc which exist)
    // =========================================================================

    /** regconfig: text-search configuration OID type. PG standard. */
    @Test
    void regconfig_castFromString() throws SQLException {
        assertEquals("english",
                scalarString("SELECT 'english'::regconfig::text"));
    }

    /** regnamespace: schema OID type. */
    @Test
    void regnamespace_castFromString() throws SQLException {
        assertEquals("public",
                scalarString("SELECT 'public'::regnamespace::text"));
    }

    /** regrole: role OID type. */
    @Test
    void regrole_castFromString() throws SQLException {
        // PG always has a 'postgres' role in the default setup
        String v = scalarString("SELECT 'memgres'::regrole::text");
        assertNotNull(v);
    }

    /** regdictionary: text-search dictionary OID type. */
    @Test
    void regdictionary_castFromString() throws SQLException {
        String v = scalarString("SELECT 'simple'::regdictionary::text");
        assertEquals("simple", v);
    }

    // =========================================================================
    // G. Network helpers
    // =========================================================================

    /** inet_merge returns the smallest CIDR containing both inets. PG standard. */
    @Test
    void inet_merge_twoSubnets() throws SQLException {
        assertEquals("192.168.0.0/22",
                scalarString("SELECT inet_merge('192.168.1.5/24', '192.168.2.5/24')::text"));
    }

    /** hostmask returns the hostmask for a CIDR. */
    @Test
    void hostmask_cidr24() throws SQLException {
        assertEquals("0.0.0.255",
                scalarString("SELECT hostmask('192.168.1.0/24'::cidr)::text"));
    }

    /** inet_same_family returns true when both addresses are same IP family. */
    @Test
    void inet_same_family_bothIpv4() throws SQLException {
        assertEquals("true",
                scalarString("SELECT inet_same_family('1.2.3.4'::inet, '5.6.7.8'::inet)::text"));
    }

    // =========================================================================
    // H. System info helpers
    // =========================================================================

    /** pg_current_logfile() returns current log file path or NULL. */
    @Test
    void pg_current_logfile_returnsText() throws SQLException {
        // Must not error; may return NULL in an in-memory DB.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_current_logfile()")) {
            assertTrue(rs.next());
            rs.getString(1); // just verify that the function resolves
        }
    }

    /** txid_current_if_assigned returns NULL when no xid has been assigned. */
    @Test
    void txid_current_if_assigned_unassigned() throws SQLException {
        conn.setAutoCommit(false);
        try {
            // Fresh transaction; no write → xid may be unassigned → NULL
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT txid_current_if_assigned()")) {
                assertTrue(rs.next());
                // Implementation may return NULL or a value, but function must exist.
                rs.getString(1);
            }
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /** pg_safe_snapshot_blocking_pids returns empty array with no blockers. */
    @Test
    void pg_safe_snapshot_blocking_pids_empty() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_safe_snapshot_blocking_pids(pg_backend_pid())")) {
            assertTrue(rs.next());
            // Must return an array-typed value (possibly empty), not error out.
            rs.getString(1);
        }
    }

    /** pg_blocking_pids returns empty array when no one is blocking. */
    @Test
    void pg_blocking_pids_noBlockers() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_blocking_pids(pg_backend_pid())")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            // Expected: "{}" (empty int[]) when there are no blockers.
            assertEquals("{}", v);
        }
    }

    /** txid_status returns 'committed'/'aborted'/'in progress' for an xid. */
    @Test
    void txid_status_commited() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT txid_status(txid_current())")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            // For the current xid, status is "in progress".
            assertNotNull(v);
            assertTrue(v.equals("in progress") || v.equals("committed") || v.equals("aborted"),
                    "txid_status must return well-formed label; got " + v);
        }
    }
}
