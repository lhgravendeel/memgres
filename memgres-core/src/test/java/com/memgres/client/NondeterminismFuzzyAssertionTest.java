package com.memgres.client;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Nondeterminism and fuzzy-assertion scenarios from:
 * 1190_nondeterminism_and_fuzzy_assertion_scenarios.md
 *
 * Covers: float precision tolerance, NUMERIC precision, random() range,
 * clock_timestamp() behaviour, now() stability within a transaction, UUID
 * uniqueness, txid_current() monotonicity, float aggregate precision,
 * ORDER BY with ties, hash-based set operations, pg_size_pretty formatting,
 * sequence monotonicity, EXPLAIN output validity, and ts_rank epsilon checks.
 *
 * Table prefix: ndf_
 * All tests share a single autocommit=true connection.
 * Fuzzy assertions use assertTrue(Math.abs(actual - expected) < epsilon).
 */
class NondeterminismFuzzyAssertionTest {

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
        if (conn != null && !conn.isClosed()) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. Float precision tolerance, 1.0/3.0 ≈ 0.333...
    // =========================================================================

    @Test
    void testFloatPrecisionTolerance() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CAST(1.0/3.0 AS float8) AS result")) {
            assertTrue(rs.next());
            double actual = rs.getDouble("result");
            double expected = 1.0 / 3.0;
            double epsilon = 1e-10;
            assertTrue(Math.abs(actual - expected) < epsilon,
                    "CAST(1.0/3.0 AS float8) should be approximately 0.333…, got " + actual);
        }
    }

    // =========================================================================
    // 2. NUMERIC precision: 1.0/3.0 has many decimal places
    // =========================================================================

    @Test
    void testNumericPrecision() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (1.0/3.0)::numeric(38,20) AS result")) {
            assertTrue(rs.next());
            String raw = rs.getString("result");
            assertNotNull(raw);
            // The decimal representation should contain many digits after the decimal point
            int dotIndex = raw.indexOf('.');
            assertTrue(dotIndex >= 0, "NUMERIC result should have a decimal point");
            String decimals = raw.substring(dotIndex + 1);
            assertTrue(decimals.length() >= 10,
                    "NUMERIC 1/3 should have at least 10 decimal digits, got: " + raw);
        }
    }

    // =========================================================================
    // 3. random() range: must be in [0, 1)
    // =========================================================================

    @Test
    void testRandomRange() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT r FROM generate_series(1,100) AS g(i), LATERAL (SELECT random() AS r) sub")) {
            int count = 0;
            while (rs.next()) {
                double r = rs.getDouble(1);
                assertTrue(r >= 0.0, "random() must be >= 0.0, got " + r);
                assertTrue(r < 1.0,  "random() must be < 1.0,  got " + r);
                count++;
            }
            assertEquals(100, count, "Should have generated 100 random values");
        }
    }

    // =========================================================================
    // 4. clock_timestamp() changes: two calls may differ (not necessarily equal)
    // =========================================================================

    @Test
    void testClockTimestampChanges() throws Exception {
        Timestamp t1, t2;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT clock_timestamp()")) {
            assertTrue(rs.next());
            t1 = rs.getTimestamp(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT clock_timestamp()")) {
            assertTrue(rs.next());
            t2 = rs.getTimestamp(1);
        }
        assertNotNull(t1);
        assertNotNull(t2);
        // t2 must not precede t1 (wall-clock must be non-decreasing)
        assertFalse(t2.before(t1),
                "Second clock_timestamp() must not be earlier than the first");
    }

    // =========================================================================
    // 5. now() within transaction: stable across multiple calls
    // =========================================================================

    @Test
    void testNowStableWithinTransaction() throws Exception {
        conn.setAutoCommit(false);
        try {
            Timestamp t1, t2, t3;
            try (Statement s = conn.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT now()")) {
                    assertTrue(rs.next());
                    t1 = rs.getTimestamp(1);
                }
                try (ResultSet rs = s.executeQuery("SELECT now()")) {
                    assertTrue(rs.next());
                    t2 = rs.getTimestamp(1);
                }
                try (ResultSet rs = s.executeQuery("SELECT now()")) {
                    assertTrue(rs.next());
                    t3 = rs.getTimestamp(1);
                }
            }
            assertNotNull(t1);
            assertEquals(t1, t2, "now() must return the same value within a single transaction");
            assertEquals(t1, t3, "now() must return the same value within a single transaction");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // 6. UUID uniqueness: multiple gen_random_uuid() calls produce unique values
    // =========================================================================

    @Test
    void testUuidUniqueness() throws Exception {
        Set<String> uuids = new HashSet<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT gen_random_uuid()::text FROM generate_series(1, 1000)")) {
            while (rs.next()) {
                String uuid = rs.getString(1);
                assertTrue(uuids.add(uuid),
                        "gen_random_uuid() produced a duplicate value: " + uuid);
            }
        }
        assertEquals(1000, uuids.size(), "All 1000 generated UUIDs must be unique");
    }

    // =========================================================================
    // 7. txid_current() monotonicity: successive transactions have increasing txids
    // =========================================================================

    @Test
    void testTxidCurrentMonotonicity() throws Exception {
        // Each call to txid_current() in a new transaction should return a larger value.
        // We use autocommit=true so each statement is its own transaction.
        List<Long> txids = new ArrayList<>();
        try (Statement s = conn.createStatement()) {
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = s.executeQuery("SELECT txid_current()")) {
                    assertTrue(rs.next());
                    txids.add(rs.getLong(1));
                }
            }
        }
        assertEquals(5, txids.size());
        for (int i = 1; i < txids.size(); i++) {
            assertTrue(txids.get(i) > txids.get(i - 1),
                    "txid_current() must be strictly increasing: "
                            + txids.get(i - 1) + " vs " + txids.get(i));
        }
    }

    // =========================================================================
    // 8. Aggregate on floating point: SUM has acceptable precision
    // =========================================================================

    @Test
    void testFloatAggregateHasAcceptablePrecision() throws Exception {
        // SUM of 1000 × 0.1 should be close to 100.0 (within float8 precision)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT SUM(0.1::float8) FROM generate_series(1, 1000)")) {
            assertTrue(rs.next());
            double actual = rs.getDouble(1);
            double expected = 100.0;
            double epsilon = 1e-6; // allow small accumulated floating-point error
            assertTrue(Math.abs(actual - expected) < epsilon,
                    "SUM of 1000 × 0.1 should be approximately 100.0, got " + actual);
        }
    }

    // =========================================================================
    // 9. ORDER BY with ties: tied rows may appear in any order, set must match
    // =========================================================================

    @Test
    void testOrderByWithTies() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ndf_ties (id int, grp text)");
            s.execute("INSERT INTO ndf_ties VALUES (1,'a'),(2,'a'),(3,'b')");
        }

        // Order is deterministic within a stable sort but ties may vary.
        // Assert only that the set of returned rows is complete and correct.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM ndf_ties ORDER BY grp")) {
            Set<Integer> ids = new HashSet<>();
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
            assertEquals(Cols.setOf(1, 2, 3), ids,
                    "ORDER BY with ties must return all rows regardless of tie-breaking order");
        }
    }

    // =========================================================================
    // 10. Hash-based set operations: EXCEPT/INTERSECT may return rows in any order
    // =========================================================================

    @Test
    void testHashBasedSetOperations() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ndf_set_a (val int)");
            s.execute("CREATE TABLE ndf_set_b (val int)");
            s.execute("INSERT INTO ndf_set_a VALUES (1),(2),(3),(4)");
            s.execute("INSERT INTO ndf_set_b VALUES (3),(4),(5),(6)");
        }

        // EXCEPT: values in A but not B
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ndf_set_a EXCEPT SELECT val FROM ndf_set_b")) {
            Set<Integer> result = new HashSet<>();
            while (rs.next()) {
                result.add(rs.getInt(1));
            }
            assertEquals(Cols.setOf(1, 2), result,
                    "EXCEPT must return {1,2} regardless of output row order");
        }

        // INTERSECT: values in both A and B
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ndf_set_a INTERSECT SELECT val FROM ndf_set_b")) {
            Set<Integer> result = new HashSet<>();
            while (rs.next()) {
                result.add(rs.getInt(1));
            }
            assertEquals(Cols.setOf(3, 4), result,
                    "INTERSECT must return {3,4} regardless of output row order");
        }
    }

    // =========================================================================
    // 11. pg_size_pretty formatting: output is non-negative and non-absurd
    // =========================================================================

    @Test
    void testPgSizePrettyFormatting() throws Exception {
        long[] sampleBytes = {0L, 1024L, 1048576L, 1073741824L};
        try (PreparedStatement ps = conn.prepareStatement("SELECT pg_size_pretty(?::bigint)")) {
            for (long bytes : sampleBytes) {
                ps.setLong(1, bytes);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    String pretty = rs.getString(1);
                    assertNotNull(pretty, "pg_size_pretty must not return NULL");
                    assertFalse(Strs.isBlank(pretty), "pg_size_pretty must not return blank string");
                    // Must not start with '-' (no negative sizes for non-negative input)
                    assertFalse(pretty.trim().startsWith("-"),
                            "pg_size_pretty of " + bytes + " bytes must not be negative: " + pretty);
                }
            }
        }
    }

    // =========================================================================
    // 12. Sequence values: nextval is monotonically increasing
    // =========================================================================

    @Test
    void testSequenceMonotonicity() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE IF NOT EXISTS ndf_mono_seq");
        }

        List<Long> vals = new ArrayList<>();
        try (Statement s = conn.createStatement()) {
            for (int i = 0; i < 10; i++) {
                try (ResultSet rs = s.executeQuery("SELECT nextval('ndf_mono_seq')")) {
                    assertTrue(rs.next());
                    vals.add(rs.getLong(1));
                }
            }
        }

        assertEquals(10, vals.size());
        for (int i = 1; i < vals.size(); i++) {
            assertTrue(vals.get(i) > vals.get(i - 1),
                    "nextval must be strictly increasing: "
                            + vals.get(i - 1) + " >= " + vals.get(i));
        }
    }

    // =========================================================================
    // 13. EXPLAIN output: produces non-empty output (content may vary)
    // =========================================================================

    @Test
    void testExplainOutputNonEmpty() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS ndf_explain_target (id int, val text)");
            s.execute("INSERT INTO ndf_explain_target VALUES (1, 'a'), (2, 'b')");
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "EXPLAIN SELECT * FROM ndf_explain_target WHERE id = 1")) {
            List<String> lines = new ArrayList<>();
            while (rs.next()) {
                lines.add(rs.getString(1));
            }
            assertFalse(lines.isEmpty(), "EXPLAIN must produce at least one output line");
            // Each line must be a non-null, non-blank string
            for (String line : lines) {
                assertNotNull(line);
                assertFalse(Strs.isBlank(line), "EXPLAIN output line must not be blank");
            }
        }
    }

    // =========================================================================
    // 14. ts_rank tolerance: full-text ranking approximately correct (within epsilon)
    // =========================================================================

    @Test
    void testTsRankTolerance() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE ndf_fts (
                        id SERIAL PRIMARY KEY,
                        doc text,
                        tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', doc)) STORED
                    )
                    """);
            // High-relevance doc: keyword appears multiple times
            s.execute("INSERT INTO ndf_fts (doc) VALUES " +
                    "('postgres postgres postgres full text search')," +   // high rank
                    "('unrelated content about cooking recipes')");        // low rank
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                     SELECT id,
                            ts_rank(tsv, to_tsquery('english', 'postgres')) AS rank
                     FROM ndf_fts
                     ORDER BY rank DESC
                     """)) {
            // First row should have a positive rank (the high-relevance doc)
            assertTrue(rs.next(), "Should return at least one ranked row");
            double highRank = rs.getDouble("rank");
            assertTrue(highRank > 0.0,
                    "High-relevance document should have ts_rank > 0, got " + highRank);

            // Second row should have a lower (or zero) rank
            assertTrue(rs.next(), "Should return a second ranked row");
            double lowRank = rs.getDouble("rank");
            assertTrue(highRank >= lowRank,
                    "High-relevance doc (" + highRank + ") must rank >= low-relevance doc (" + lowRank + ")");

            // Fuzzy: the high rank should be within a reasonable range [0, 1] (ts_rank default)
            double epsilon = 1.0; // ts_rank normalisation=0 can exceed 1, so we allow generous bound
            assertTrue(Math.abs(highRank) < 1.0 + epsilon,
                    "ts_rank should be within a reasonable magnitude, got " + highRank);
        }
    }
}
