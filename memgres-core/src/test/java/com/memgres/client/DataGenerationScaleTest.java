package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Parameterized data generation and scale profile tests, covering
 * 1590_test_data_generation_and_scale_profiles.md.
 *
 * Exercises generate_series, random data, bulk inserts, aggregations,
 * joins, hierarchical data, categorical distributions, and index
 * effectiveness across small and medium scale datasets.
 *
 * Table prefix: dgs_
 */
class DataGenerationScaleTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getString(1);
        }
    }

    static long queryLong(String sql) throws SQLException {
        return Long.parseLong(query1(sql).trim());
    }

    static double queryDouble(String sql) throws SQLException {
        return Double.parseDouble(query1(sql).trim());
    }

    // =========================================================================
    // 1. generate_series integer
    // =========================================================================

    /**
     * generate_series(1, 1000) must produce exactly 1000 rows.
     */
    @Test
    void generateSeriesIntegerProduces1000Rows() throws SQLException {
        long count = queryLong("SELECT COUNT(*) FROM generate_series(1, 1000)");
        assertEquals(1000L, count, "generate_series(1,1000) must produce 1000 rows");
    }

    // =========================================================================
    // 2. generate_series timestamp
    // =========================================================================

    /**
     * generate_series over a timestamp range with a 1-day interval must
     * produce one row per day for a 30-day span (31 rows inclusive).
     */
    @Test
    void generateSeriesTimestampProducesExpectedRows() throws SQLException {
        long count = queryLong("""
                SELECT COUNT(*) FROM generate_series(
                    '2024-01-01'::timestamp,
                    '2024-01-31'::timestamp,
                    '1 day'::interval
                )
                """);
        assertEquals(31L, count,
                "generate_series over 31 days must produce 31 rows, got: " + count);
    }

    // =========================================================================
    // 3. Random data generation
    // =========================================================================

    /**
     * random() * 1000 must produce values in [0, 1000).
     * Verifying that min >= 0 and max < 1000 over 500 generated rows.
     */
    @Test
    void randomDataInExpectedRange() throws SQLException {
        double min = queryDouble(
                "SELECT MIN(random() * 1000) FROM generate_series(1, 500)");
        double max = queryDouble(
                "SELECT MAX(random() * 1000) FROM generate_series(1, 500)");
        assertTrue(min >= 0.0, "random() * 1000 min must be >= 0, got: " + min);
        assertTrue(max < 1000.0, "random() * 1000 max must be < 1000, got: " + max);
    }

    // =========================================================================
    // 4. Bulk insert via generate_series
    // =========================================================================

    /**
     * INSERT INTO ... SELECT FROM generate_series must insert exactly N rows.
     */
    @Test
    void bulkInsertViaGenerateSeries() throws SQLException {
        exec("""
                CREATE TABLE dgs_bulk (
                    id int PRIMARY KEY,
                    val text
                )
                """);
        try {
            exec("""
                    INSERT INTO dgs_bulk (id, val)
                    SELECT gs, 'row_' || gs
                    FROM generate_series(1, 500) AS gs
                    """);
            long count = queryLong("SELECT COUNT(*) FROM dgs_bulk");
            assertEquals(500L, count, "Bulk insert via generate_series must insert 500 rows");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_bulk");
        }
    }

    // =========================================================================
    // 5. Small scale (100 rows): aggregation
    // =========================================================================

    /**
     * Aggregation queries (SUM, AVG, COUNT) must return mathematically correct
     * results on a 100-row table of integers 1..100.
     */
    @Test
    void smallScaleAggregationsCorrect() throws SQLException {
        exec("CREATE TABLE dgs_small (id int PRIMARY KEY, val int)");
        try {
            exec("""
                    INSERT INTO dgs_small (id, val)
                    SELECT gs, gs FROM generate_series(1, 100) AS gs
                    """);
            long count = queryLong("SELECT COUNT(*) FROM dgs_small");
            assertEquals(100L, count);

            long sum = queryLong("SELECT SUM(val) FROM dgs_small");
            assertEquals(5050L, sum, "SUM(1..100) must be 5050");

            double avg = queryDouble("SELECT AVG(val) FROM dgs_small");
            assertEquals(50.5, avg, 0.001, "AVG(1..100) must be 50.5");

            long minVal = queryLong("SELECT MIN(val) FROM dgs_small");
            assertEquals(1L, minVal, "MIN(1..100) must be 1");

            long maxVal = queryLong("SELECT MAX(val) FROM dgs_small");
            assertEquals(100L, maxVal, "MAX(1..100) must be 100");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_small");
        }
    }

    // =========================================================================
    // 6. Medium scale (1000 rows): join queries
    // =========================================================================

    /**
     * An INNER JOIN between two 1000-row tables keyed on integer id must
     * return the correct number of matching rows.
     */
    @Test
    void mediumScaleJoinCorrect() throws SQLException {
        exec("CREATE TABLE dgs_left  (id int PRIMARY KEY, label text)");
        exec("CREATE TABLE dgs_right (id int PRIMARY KEY, score int)");
        try {
            exec("INSERT INTO dgs_left  (id, label) SELECT gs, 'L'||gs FROM generate_series(1,1000) gs");
            exec("INSERT INTO dgs_right (id, score) SELECT gs, gs*2     FROM generate_series(1,1000) gs");

            long joined = queryLong("""
                    SELECT COUNT(*) FROM dgs_left l
                    INNER JOIN dgs_right r ON l.id = r.id
                    """);
            assertEquals(1000L, joined,
                    "INNER JOIN of two 1000-row tables on PK must yield 1000 rows");

            // Spot-check: score for id=500 must be 1000
            long score = queryLong("""
                    SELECT r.score FROM dgs_left l
                    JOIN dgs_right r ON l.id = r.id
                    WHERE l.id = 500
                    """);
            assertEquals(1000L, score, "score for id=500 must be 500*2=1000");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_left");
            exec("DROP TABLE IF EXISTS dgs_right");
        }
    }

    // =========================================================================
    // 7. Cross join for data multiplication
    // =========================================================================

    /**
     * A CROSS JOIN of a 10-row table with a 20-row table must produce 200 rows.
     */
    @Test
    void crossJoinMultipliesRowCounts() throws SQLException {
        long count = queryLong("""
                SELECT COUNT(*) FROM
                    generate_series(1, 10) AS a(x)
                CROSS JOIN
                    generate_series(1, 20) AS b(y)
                """);
        assertEquals(200L, count, "CROSS JOIN of 10 x 20 must produce 200 rows");
    }

    // =========================================================================
    // 8. Sequence-based ID generation at scale
    // =========================================================================

    /**
     * INSERT with a DEFAULT serial column must assign sequential IDs and
     * the max ID after N inserts must equal N.
     */
    @Test
    void sequenceBasedIdGenerationAtScale() throws SQLException {
        exec("CREATE TABLE dgs_serial (id serial PRIMARY KEY, data text)");
        try {
            exec("INSERT INTO dgs_serial (data) SELECT 'x'||gs FROM generate_series(1,200) gs");
            long count = queryLong("SELECT COUNT(*) FROM dgs_serial");
            assertEquals(200L, count, "Serial insert must produce 200 rows");
            long maxId = queryLong("SELECT MAX(id) FROM dgs_serial");
            assertEquals(200L, maxId, "MAX(id) after 200 serial inserts must be 200");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_serial");
        }
    }

    // =========================================================================
    // 9. Random string generation via md5
    // =========================================================================

    /**
     * md5(random()::text) must produce 32-character hexadecimal strings.
     * Validate that all generated strings have length 32 and are distinct
     * (collision probability is negligible for 100 rows).
     */
    @Test
    void randomStringGenerationViaMd5() throws SQLException {
        exec("CREATE TABLE dgs_md5 (id serial PRIMARY KEY, hash text)");
        try {
            exec("INSERT INTO dgs_md5 (hash) SELECT md5(random()::text) FROM generate_series(1,100)");
            long count = queryLong("SELECT COUNT(*) FROM dgs_md5");
            assertEquals(100L, count);

            long badLen = queryLong("SELECT COUNT(*) FROM dgs_md5 WHERE length(hash) != 32");
            assertEquals(0L, badLen, "All md5 hashes must be exactly 32 characters");

            long distinct = queryLong("SELECT COUNT(DISTINCT hash) FROM dgs_md5");
            assertEquals(100L, distinct, "All 100 md5 hashes must be distinct");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_md5");
        }
    }

    // =========================================================================
    // 10. Date range generation
    // =========================================================================

    /**
     * Generating one row per day for a full year (2024, a leap year) must
     * produce exactly 366 rows.
     */
    @Test
    void dateRangeGenerationForOneYear() throws SQLException {
        long count = queryLong("""
                SELECT COUNT(*) FROM generate_series(
                    '2024-01-01'::date,
                    '2024-12-31'::date,
                    '1 day'::interval
                ) AS d(day)
                """);
        assertEquals(366L, count, "2024 is a leap year and must produce 366 daily rows");
    }

    // =========================================================================
    // 11. Hierarchical data generation
    // =========================================================================

    /**
     * A self-referencing table (parent_id FK) must accept a tree of nodes
     * and a recursive CTE must be able to traverse the hierarchy.
     */
    @Test
    void hierarchicalDataGenerationWithRecursiveCte() throws SQLException {
        exec("""
                CREATE TABLE dgs_tree (
                    id int PRIMARY KEY,
                    parent_id int REFERENCES dgs_tree(id),
                    name text NOT NULL
                )
                """);
        try {
            // Root node
            exec("INSERT INTO dgs_tree VALUES (1, NULL, 'root')");
            // Level 1 children
            exec("INSERT INTO dgs_tree VALUES (2, 1, 'child_a'), (3, 1, 'child_b')");
            // Level 2 children
            exec("INSERT INTO dgs_tree VALUES (4, 2, 'grandchild_1'), (5, 2, 'grandchild_2'), (6, 3, 'grandchild_3')");

            long total = queryLong("SELECT COUNT(*) FROM dgs_tree");
            assertEquals(6L, total, "Tree must have 6 nodes total");

            // Recursive CTE: traverse all descendants of root (id=1)
            long descendants = queryLong("""
                    WITH RECURSIVE tree_cte AS (
                        SELECT id FROM dgs_tree WHERE id = 1
                        UNION ALL
                        SELECT t.id FROM dgs_tree t
                        JOIN tree_cte c ON t.parent_id = c.id
                    )
                    SELECT COUNT(*) FROM tree_cte
                    """);
            assertEquals(6L, descendants,
                    "Recursive CTE from root must reach all 6 nodes");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_tree");
        }
    }

    // =========================================================================
    // 12. Categorical distribution
    // =========================================================================

    /**
     * Generate 300 rows split evenly across 3 categories (100 each).
     * Verify each category has exactly the expected count.
     */
    @Test
    void categoricalDistributionIsCorrect() throws SQLException {
        exec("""
                CREATE TABLE dgs_categories (
                    id int PRIMARY KEY,
                    category text NOT NULL
                )
                """);
        try {
            exec("""
                    INSERT INTO dgs_categories (id, category)
                    SELECT gs,
                           CASE
                               WHEN gs % 3 = 1 THEN 'alpha'
                               WHEN gs % 3 = 2 THEN 'beta'
                               ELSE 'gamma'
                           END
                    FROM generate_series(1, 300) AS gs
                    """);
            long alphaCount = queryLong("SELECT COUNT(*) FROM dgs_categories WHERE category = 'alpha'");
            long betaCount  = queryLong("SELECT COUNT(*) FROM dgs_categories WHERE category = 'beta'");
            long gammaCount = queryLong("SELECT COUNT(*) FROM dgs_categories WHERE category = 'gamma'");
            assertEquals(100L, alphaCount, "alpha must have 100 rows");
            assertEquals(100L, betaCount,  "beta must have 100 rows");
            assertEquals(100L, gammaCount, "gamma must have 100 rows");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_categories");
        }
    }

    // =========================================================================
    // 13. Aggregate correctness at scale
    // =========================================================================

    /**
     * SUM, AVG, MIN, MAX, COUNT on 1000 rows of integer 1..1000 must all
     * match their analytically-derived expected values.
     */
    @Test
    void aggregateCorrectnessAtScale() throws SQLException {
        exec("CREATE TABLE dgs_agg (id int PRIMARY KEY, n int)");
        try {
            exec("INSERT INTO dgs_agg (id, n) SELECT gs, gs FROM generate_series(1, 1000) gs");

            long count = queryLong("SELECT COUNT(*) FROM dgs_agg");
            assertEquals(1000L, count);

            long sum = queryLong("SELECT SUM(n) FROM dgs_agg");
            // SUM(1..1000) = 1000*1001/2 = 500500
            assertEquals(500500L, sum, "SUM(1..1000) must be 500500");

            double avg = queryDouble("SELECT AVG(n) FROM dgs_agg");
            assertEquals(500.5, avg, 0.001, "AVG(1..1000) must be 500.5");

            long minN = queryLong("SELECT MIN(n) FROM dgs_agg");
            assertEquals(1L, minN, "MIN(1..1000) must be 1");

            long maxN = queryLong("SELECT MAX(n) FROM dgs_agg");
            assertEquals(1000L, maxN, "MAX(1..1000) must be 1000");
        } finally {
            exec("DROP TABLE IF EXISTS dgs_agg");
        }
    }

    // =========================================================================
    // 14. Index effectiveness at scale
    // =========================================================================

    /**
     * A B-tree index on a 1000-row table must allow point lookups and range
     * queries to return the correct subset of rows.
     */
    @Test
    void indexEffectivenessAtScale() throws SQLException {
        exec("CREATE TABLE dgs_indexed (id int PRIMARY KEY, score int)");
        exec("CREATE INDEX dgs_indexed_score_idx ON dgs_indexed (score)");
        try {
            exec("INSERT INTO dgs_indexed (id, score) SELECT gs, gs FROM generate_series(1, 1000) gs");

            // Point lookup via indexed column
            long point = queryLong("SELECT COUNT(*) FROM dgs_indexed WHERE score = 42");
            assertEquals(1L, point, "Point lookup on indexed column must return exactly 1 row");

            // Range query: scores 1..100
            long range = queryLong("SELECT COUNT(*) FROM dgs_indexed WHERE score BETWEEN 1 AND 100");
            assertEquals(100L, range,
                    "Range query score BETWEEN 1 AND 100 must return 100 rows");

            // Verify the actual value returned by index lookup
            long val = queryLong("SELECT score FROM dgs_indexed WHERE score = 777");
            assertEquals(777L, val, "Index lookup for score=777 must return 777");
        } finally {
            exec("DROP INDEX IF EXISTS dgs_indexed_score_idx");
            exec("DROP TABLE IF EXISTS dgs_indexed");
        }
    }
}
