package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parser syntax gap tests.
 * Tests SQL syntax that PostgreSQL 18 supports but Memgres parser may not yet handle.
 * Each test verifies that valid PG18 syntax is accepted and produces correct results.
 */
class ParserSyntaxGapTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Table for GROUPING SETS / ROLLUP / CUBE tests
        exec("CREATE TABLE gs_data (a int, b text, c int)");
        exec("INSERT INTO gs_data VALUES (1, 'x', 10)");
        exec("INSERT INTO gs_data VALUES (1, 'y', 20)");
        exec("INSERT INTO gs_data VALUES (2, 'x', 30)");
        exec("INSERT INTO gs_data VALUES (2, 'y', 40)");
        exec("INSERT INTO gs_data VALUES (3, 'x', 50)");

        // Table for WITHIN GROUP / ordered-set aggregate tests
        exec("CREATE TABLE agg_data (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO agg_data (val) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
        exec("INSERT INTO agg_data (val) VALUES (5),(5),(5)"); // extra 5s for mode

        // Table for array tests
        exec("CREATE TABLE arr_data (id int, arr int[])");
        exec("INSERT INTO arr_data VALUES (1, ARRAY[10,20,30,40,50])");
        exec("INSERT INTO arr_data VALUES (2, ARRAY[100,200,300])");

        // Table for TABLESAMPLE tests (100+ rows)
        exec("CREATE TABLE sample_data (id serial PRIMARY KEY, val int)");
        StringBuilder sb = new StringBuilder("INSERT INTO sample_data (val) VALUES ");
        for (int i = 1; i <= 200; i++) {
            if (i > 1) sb.append(",");
            sb.append("(").append(i).append(")");
        }
        exec(sb.toString());

        // Edges table for recursive CTE SEARCH/CYCLE tests
        exec("CREATE TABLE edges (src int, dst int)");
        exec("INSERT INTO edges VALUES (1, 2)");
        exec("INSERT INTO edges VALUES (2, 3)");
        exec("INSERT INTO edges VALUES (3, 4)");
        exec("INSERT INTO edges VALUES (4, 5)");
        exec("INSERT INTO edges VALUES (3, 1)"); // cycle: 1->2->3->1

        // Table for FETCH/OFFSET tests
        exec("CREATE TABLE fetch_data (id serial PRIMARY KEY, label text)");
        exec("INSERT INTO fetch_data (label) VALUES ('a'),('b'),('c'),('d'),('e'),('f'),('g'),('h'),('i'),('j')");

        // Table for COLLATE tests
        exec("CREATE TABLE collate_data (id int, col text)");
        exec("INSERT INTO collate_data VALUES (1, 'banana')");
        exec("INSERT INTO collate_data VALUES (2, 'Apple')");
        exec("INSERT INTO collate_data VALUES (3, 'cherry')");
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

    static int queryRowCount(String sql) throws SQLException {
        int count = 0;
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) count++;
        }
        return count;
    }

    static List<String> queryColumn(String sql, int colIndex) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) results.add(rs.getString(colIndex));
        }
        return results;
    }

    /** Assert that SQL produces an error with the expected SQLSTATE. */
    static void assertSqlError(String sql, String expectedState) {
        try {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
            fail("Expected error for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got message: " + e.getMessage());
        }
    }

    /** Assert that SQL produces any error (don't care about SQLSTATE). */
    static void assertSqlFails(String sql) {
        assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        }, "Expected error for: " + sql);
    }

    // ========================================================================
    // Section 1: OPERATOR(schema.op) Syntax
    // ========================================================================

    @Test void operator_qualified_plus() throws SQLException {
        assertEquals("3", q("SELECT 1 OPERATOR(pg_catalog.+) 2"));
    }

    @Test void operator_qualified_multiply() throws SQLException {
        assertEquals("4", q("SELECT 1 OPERATOR(pg_catalog.*) 4"));
    }

    @Test void operator_qualified_equals() throws SQLException {
        assertEquals("t", q("SELECT 1 OPERATOR(pg_catalog.=) 1"));
    }

    @Test void operator_qualified_not_equals() throws SQLException {
        assertEquals("t", q("SELECT 1 OPERATOR(pg_catalog.<>) 2"));
    }

    @Test void operator_qualified_less_than() throws SQLException {
        assertEquals("t", q("SELECT 1 OPERATOR(pg_catalog.<) 2"));
    }

    @Test void operator_qualified_greater_than() throws SQLException {
        assertEquals("f", q("SELECT 1 OPERATOR(pg_catalog.>) 2"));
    }

    @Test void operator_qualified_concat() throws SQLException {
        assertEquals("ab", q("SELECT 'a' OPERATOR(pg_catalog.||) 'b'"));
    }

    @Test void operator_qualified_minus() throws SQLException {
        assertEquals("3", q("SELECT 5 OPERATOR(pg_catalog.-) 2"));
    }

    @Test void operator_qualified_divide() throws SQLException {
        assertEquals("5", q("SELECT 10 OPERATOR(pg_catalog./) 2"));
    }

    @Test void operator_qualified_less_equal() throws SQLException {
        assertEquals("t", q("SELECT 2 OPERATOR(pg_catalog.<=) 2"));
    }

    @Test void operator_qualified_greater_equal() throws SQLException {
        assertEquals("t", q("SELECT 3 OPERATOR(pg_catalog.>=) 2"));
    }

    @Test void operator_bad_schema() {
        assertSqlFails("SELECT 1 OPERATOR(no_such_schema.+) 2");
    }

    @Test void operator_bad_operator() {
        assertSqlFails("SELECT 1 OPERATOR(pg_catalog.???) 2");
    }

    @Test void operator_qualified_in_where_clause() throws SQLException {
        assertEquals("1", q("SELECT count(*) FROM gs_data WHERE a OPERATOR(pg_catalog.=) 1 AND b OPERATOR(pg_catalog.=) 'x'"));
    }

    // ========================================================================
    // Section 2: GROUPING SETS / ROLLUP / CUBE
    // ========================================================================

    @Test void grouping_sets_basic() throws SQLException {
        // GROUP BY GROUPING SETS ((a), ()) should produce one row per 'a' value plus grand total
        int rows = queryRowCount("SELECT a, sum(c) FROM gs_data GROUP BY GROUPING SETS ((a), ())");
        // 3 distinct a values + 1 grand total = 4 rows
        assertEquals(4, rows);
    }

    @Test void grouping_sets_multi_column() throws SQLException {
        int rows = queryRowCount("SELECT a, b, sum(c) FROM gs_data GROUP BY GROUPING SETS ((a), (b), ())");
        // 3 a values + 2 b values + 1 grand total = 6
        assertEquals(6, rows);
    }

    @Test void grouping_sets_grand_total_only() throws SQLException {
        String result = q("SELECT sum(c) FROM gs_data GROUP BY GROUPING SETS (())");
        assertEquals("150", result);
    }

    @Test void grouping_sets_with_order_by() throws SQLException {
        // grand total row should have NULL for a, order by a nulls last
        List<String> vals = queryColumn(
                "SELECT a FROM gs_data GROUP BY GROUPING SETS ((a), ()) ORDER BY a NULLS LAST", 1);
        assertEquals(4, vals.size());
        assertNull(vals.get(3)); // grand total row last
    }

    @Test void rollup_basic() throws SQLException {
        int rows = queryRowCount("SELECT a, sum(c) FROM gs_data GROUP BY ROLLUP (a)");
        // 3 a values + 1 grand total = 4
        assertEquals(4, rows);
    }

    @Test void rollup_multi_column() throws SQLException {
        int rows = queryRowCount("SELECT a, b, sum(c) FROM gs_data GROUP BY ROLLUP (a, b)");
        // groups: (a,b), (a), (), so 5 (a,b) combos + 3 (a) subtotals + 1 grand total = 9
        assertEquals(9, rows);
    }

    @Test void rollup_with_having() throws SQLException {
        // Only groups with sum > 10
        int rows = queryRowCount("SELECT a, sum(c) FROM gs_data GROUP BY ROLLUP (a) HAVING sum(c) > 10");
        assertTrue(rows >= 1, "Expected at least one row after HAVING filter");
    }

    @Test void rollup_grand_total_value() throws SQLException {
        // The grand total (a=NULL) should be 150
        String sql = "SELECT sum(c) FROM gs_data GROUP BY ROLLUP (a) HAVING a IS NULL";
        // In ROLLUP, the row where all grouping columns are NULL is the grand total
        // but HAVING a IS NULL filters specifically, so we need a different approach
        String result = q("SELECT sum(c) FROM gs_data GROUP BY ROLLUP (a) ORDER BY a NULLS LAST LIMIT 1 OFFSET 3");
        // The last row (grand total) should show 150
        assertEquals("150", result);
    }

    @Test void rollup_empty() {
        // PG18 rejects empty ROLLUP() with 42601 syntax error
        assertSqlError("SELECT sum(c) FROM gs_data GROUP BY ROLLUP ()", "42601");
    }

    @Test void cube_basic() throws SQLException {
        int rows = queryRowCount("SELECT a, sum(c) FROM gs_data GROUP BY CUBE (a)");
        // same as ROLLUP for single column: 3 a values + 1 grand total = 4
        assertEquals(4, rows);
    }

    @Test void cube_multi_column() throws SQLException {
        int rows = queryRowCount("SELECT a, b, sum(c) FROM gs_data GROUP BY CUBE (a, b)");
        // CUBE(a,b) = GROUPING SETS ((a,b),(a),(b),())
        // (a,b): 5 combos, (a): 3, (b): 2, (): 1 = 11
        assertEquals(11, rows);
    }

    @Test void grouping_function_basic() throws SQLException {
        // grouping(a) returns 1 when a is aggregated (NULL from grouping set), 0 otherwise
        List<String> vals = queryColumn(
                "SELECT grouping(a) FROM gs_data GROUP BY GROUPING SETS ((a), ()) ORDER BY a NULLS LAST", 1);
        assertEquals(4, vals.size());
        assertEquals("0", vals.get(0)); // a=1
        assertEquals("0", vals.get(1)); // a=2
        assertEquals("0", vals.get(2)); // a=3
        assertEquals("1", vals.get(3)); // grand total
    }

    @Test void grouping_function_multi_column() throws SQLException {
        int rows = queryRowCount(
                "SELECT a, b, sum(c), grouping(a), grouping(b) FROM gs_data " +
                "GROUP BY GROUPING SETS ((a, b), (a), (b), ()) ORDER BY a NULLS LAST, b NULLS LAST");
        // (a,b): 5, (a): 3, (b): 2, (): 1 = 11
        assertEquals(11, rows);
    }

    @Test void grouping_sets_with_count() throws SQLException {
        String result = q("SELECT count(*) FROM gs_data GROUP BY GROUPING SETS (())");
        assertEquals("5", result);
    }

    @Test void grouping_sets_with_avg() throws SQLException {
        String result = q("SELECT avg(c) FROM gs_data GROUP BY GROUPING SETS (())");
        // avg of 10,20,30,40,50 = 30
        // PG returns numeric with full precision for avg() of integers
        assertEquals("30.0000000000000000", result);
    }

    @Test void grouping_sets_combined_with_regular_group_by() throws SQLException {
        // PG allows mixing: GROUP BY a, GROUPING SETS ((b), ())
        int rows = queryRowCount(
                "SELECT a, b, sum(c) FROM gs_data GROUP BY a, GROUPING SETS ((b), ())");
        // For each a value, we get groups (b) and (), so 3 * (2+1) = 9
        assertEquals(9, rows);
    }

    // ========================================================================
    // Section 3: WITHIN GROUP / Ordered-Set Aggregates
    // ========================================================================

    @Test void percentile_disc_median() throws SQLException {
        String result = q("SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertEquals("5", result);
    }

    @Test void percentile_cont_median() throws SQLException {
        String result = q("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
        // continuous interpolation, should be around 5
        double v = Double.parseDouble(result);
        assertTrue(v >= 4.0 && v <= 6.0, "Expected median around 5, got " + v);
    }

    @Test void percentile_disc_25th() throws SQLException {
        String result = q("SELECT percentile_disc(0.25) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
    }

    @Test void percentile_disc_75th() throws SQLException {
        String result = q("SELECT percentile_disc(0.75) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
    }

    @Test void percentile_disc_zero() throws SQLException {
        String result = q("SELECT percentile_disc(0) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertEquals("1", result);
    }

    @Test void percentile_disc_one() throws SQLException {
        String result = q("SELECT percentile_disc(1) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertEquals("10", result);
    }

    @Test void percentile_cont_zero() throws SQLException {
        String result = q("SELECT percentile_cont(0) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
        assertEquals("1", result);
    }

    @Test void percentile_cont_one() throws SQLException {
        String result = q("SELECT percentile_cont(1) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
        assertEquals("10", result);
    }

    @Test void mode_aggregate() throws SQLException {
        String result = q("SELECT mode() WITHIN GROUP (ORDER BY val) FROM agg_data");
        // 5 appears most often (original + 3 extra = 4 times)
        assertEquals("5", result);
    }

    @Test void hypothetical_rank() throws SQLException {
        // rank(10) among values 1..10 + extras
        String result = q("SELECT rank(10) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
    }

    @Test void hypothetical_dense_rank() throws SQLException {
        String result = q("SELECT dense_rank(10) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
    }

    @Test void hypothetical_percent_rank() throws SQLException {
        String result = q("SELECT percent_rank(10) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
        double v = Double.parseDouble(result);
        assertTrue(v >= 0.0 && v <= 1.0, "percent_rank should be between 0 and 1");
    }

    @Test void hypothetical_cume_dist() throws SQLException {
        String result = q("SELECT cume_dist(10) WITHIN GROUP (ORDER BY val) FROM agg_data");
        assertNotNull(result);
        double v = Double.parseDouble(result);
        assertTrue(v > 0.0 && v <= 1.0, "cume_dist should be between 0 and 1");
    }

    @Test void percentile_disc_bad_fraction() {
        // fraction must be between 0 and 1
        assertSqlFails("SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY val) FROM agg_data");
    }

    @Test void percentile_disc_type_mismatch() {
        assertSqlFails("SELECT percentile_disc('x') WITHIN GROUP (ORDER BY val) FROM agg_data");
    }

    @Test void percentile_cont_desc_order() throws SQLException {
        String result = q("SELECT percentile_cont(0) WITHIN GROUP (ORDER BY val DESC) FROM agg_data");
        assertNotNull(result);
        // 0th percentile in DESC order = max value = 10
        assertEquals("10", result);
    }

    // ========================================================================
    // Section 4: SEARCH / CYCLE in Recursive CTEs
    // ========================================================================

    @Test void recursive_cte_search_breadth_first() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
                WHERE g.dst <> 1
            )
            SEARCH BREADTH FIRST BY dst SET ordcol
            SELECT src, dst FROM graph ORDER BY ordcol
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 1, "Expected at least one row from breadth-first search");
    }

    @Test void recursive_cte_search_depth_first() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
                WHERE g.dst <> 1
            )
            SEARCH DEPTH FIRST BY dst SET ordcol
            SELECT src, dst FROM graph ORDER BY ordcol
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 1, "Expected at least one row from depth-first search");
    }

    @Test void recursive_cte_cycle_detection() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
            )
            CYCLE dst SET is_cycle USING path
            SELECT src, dst, is_cycle FROM graph
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 4, "Expected traversal with cycle detection");
    }

    @Test void recursive_cte_cycle_filter_non_cyclic() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
            )
            CYCLE dst SET is_cycle USING path
            SELECT src, dst FROM graph WHERE NOT is_cycle
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 1, "Expected non-cyclic rows");
    }

    @Test void recursive_cte_search_and_cycle_combined() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
            )
            SEARCH BREADTH FIRST BY dst SET ordcol
            CYCLE dst SET is_cycle USING path
            SELECT src, dst, is_cycle FROM graph ORDER BY ordcol
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 1, "Expected rows from combined SEARCH + CYCLE");
    }

    @Test void recursive_cte_cycle_custom_column_names() throws SQLException {
        String sql = """
            WITH RECURSIVE graph(src, dst) AS (
              SELECT src, dst FROM edges WHERE src = 1
              UNION ALL
              SELECT e.src, e.dst FROM graph g JOIN edges e ON g.dst = e.src
            )
            CYCLE dst SET cycled TO true DEFAULT false USING traversal_path
            SELECT src, dst, cycled FROM graph
            """;
        int rows = queryRowCount(sql);
        assertTrue(rows >= 1, "Expected rows with custom cycle column names");
    }

    // ========================================================================
    // Section 5: ROWS FROM(...) Syntax
    // ========================================================================

    @Test void rows_from_two_generate_series() throws SQLException {
        String sql = "SELECT * FROM ROWS FROM(generate_series(1,3), generate_series(10,12)) AS x(a,b)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    @Test void rows_from_mismatched_lengths() throws SQLException {
        String sql = "SELECT * FROM ROWS FROM(generate_series(1,3), generate_series(10,14)) AS x(a,b)";
        int rows = queryRowCount(sql);
        // longer series defines row count = 5
        assertEquals(5, rows);
    }

    @Test void rows_from_single_srf() throws SQLException {
        String sql = "SELECT * FROM ROWS FROM(unnest(ARRAY[1,2,3])) AS x(v)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    @Test void rows_from_unnest_and_generate_series() throws SQLException {
        String sql = "SELECT * FROM ROWS FROM(unnest(ARRAY['a','b','c']), generate_series(1,3)) AS x(letter,num)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    @Test void rows_from_with_ordinality() throws SQLException {
        String sql = "SELECT * FROM ROWS FROM(generate_series(1,3)) WITH ORDINALITY AS x(v,ord)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    // ========================================================================
    // Section 6: TABLESAMPLE Syntax
    // ========================================================================

    @Test void tablesample_system_50() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE SYSTEM (50)");
        assertNotNull(result);
        int count = Integer.parseInt(result);
        // With 200 rows and 50%, we expect roughly 100 but sampling is random
        assertTrue(count >= 0 && count <= 200, "Count should be between 0 and 200, got " + count);
    }

    @Test void tablesample_bernoulli_50() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE BERNOULLI (50)");
        assertNotNull(result);
        int count = Integer.parseInt(result);
        assertTrue(count >= 0 && count <= 200, "Count should be between 0 and 200, got " + count);
    }

    @Test void tablesample_bernoulli_repeatable() throws SQLException {
        String result1 = q("SELECT count(*) FROM sample_data TABLESAMPLE BERNOULLI (50) REPEATABLE (42)");
        String result2 = q("SELECT count(*) FROM sample_data TABLESAMPLE BERNOULLI (50) REPEATABLE (42)");
        // Same seed should produce same results
        assertEquals(result1, result2, "REPEATABLE with same seed should produce same count");
    }

    @Test void tablesample_system_zero() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE SYSTEM (0)");
        assertNotNull(result);
        int count = Integer.parseInt(result);
        // 0% should return 0 rows (or at most very few due to block-level sampling)
        assertTrue(count <= 10, "0% sample should return very few rows, got " + count);
    }

    @Test void tablesample_system_100() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE SYSTEM (100)");
        assertEquals("200", result);
    }

    @Test void tablesample_bernoulli_100() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE BERNOULLI (100)");
        assertEquals("200", result);
    }

    @Test void tablesample_bernoulli_0() throws SQLException {
        String result = q("SELECT count(*) FROM sample_data TABLESAMPLE BERNOULLI (0)");
        assertEquals("0", result);
    }

    @Test void tablesample_negative_percentage() {
        assertSqlError("SELECT * FROM sample_data TABLESAMPLE SYSTEM (-1)", "2202H");
    }

    @Test void tablesample_invalid_method() {
        assertSqlError("SELECT * FROM sample_data TABLESAMPLE no_such_method (10)", "42704");
    }

    @Test void tablesample_over_100() {
        assertSqlFails("SELECT * FROM sample_data TABLESAMPLE SYSTEM (101)");
    }

    // ========================================================================
    // Section 7: Array Slice Syntax
    // ========================================================================

    @Test void array_slice_basic() throws SQLException {
        String result = q("SELECT arr[2:3] FROM arr_data WHERE id = 1");
        // Should return {20,30}
        assertEquals("{20,30}", result);
    }

    @Test void array_slice_first_two() throws SQLException {
        String result = q("SELECT arr[1:2] FROM arr_data WHERE id = 1");
        assertEquals("{10,20}", result);
    }

    @Test void array_slice_to_end() throws SQLException {
        // arr[3:], PG syntax for "from index 3 to end"
        String result = q("SELECT arr[3:] FROM arr_data WHERE id = 1");
        assertEquals("{30,40,50}", result);
    }

    @Test void array_slice_from_start() throws SQLException {
        // arr[:2], up to index 2
        String result = q("SELECT arr[:2] FROM arr_data WHERE id = 1");
        assertEquals("{10,20}", result);
    }

    @Test void array_slice_reversed_bounds() throws SQLException {
        // arr[3:1], reversed bounds should return empty/NULL
        String result = q("SELECT arr[3:1] FROM arr_data WHERE id = 1");
        // PG returns empty array {} or NULL depending on version
        assertTrue(result == null || result.equals("{}"),
                "Reversed bounds should return empty/NULL, got: " + result);
    }

    @Test void array_subscript_single_element() throws SQLException {
        String result = q("SELECT arr[1] FROM arr_data WHERE id = 1");
        assertEquals("10", result);
    }

    @Test void array_subscript_last_element() throws SQLException {
        String result = q("SELECT arr[5] FROM arr_data WHERE id = 1");
        assertEquals("50", result);
    }

    @Test void array_subscript_out_of_bounds() throws SQLException {
        String result = q("SELECT arr[99] FROM arr_data WHERE id = 1");
        assertNull(result); // out of bounds returns NULL in PG
    }

    @Test void array_slice_full_range() throws SQLException {
        String result = q("SELECT arr[1:5] FROM arr_data WHERE id = 1");
        assertEquals("{10,20,30,40,50}", result);
    }

    @Test void array_slice_single_element_range() throws SQLException {
        String result = q("SELECT arr[3:3] FROM arr_data WHERE id = 1");
        assertEquals("{30}", result);
    }

    @Test void array_literal_slice() throws SQLException {
        String result = q("SELECT (ARRAY[10,20,30,40,50])[2:4]");
        assertEquals("{20,30,40}", result);
    }

    // ========================================================================
    // Section 8: Custom Lower-Bound Array Literals
    // ========================================================================

    @Test void array_custom_lower_bound_zero() throws SQLException {
        String result = q("SELECT '[0:2]={10,20,30}'::int[]");
        assertNotNull(result);
    }

    @Test void array_lower_custom_bound() throws SQLException {
        String result = q("SELECT array_lower('[0:2]={10,20,30}'::int[], 1)");
        assertEquals("0", result);
    }

    @Test void array_upper_custom_bound() throws SQLException {
        String result = q("SELECT array_upper('[0:2]={10,20,30}'::int[], 1)");
        assertEquals("2", result);
    }

    @Test void array_custom_lower_bound_5() throws SQLException {
        String result = q("SELECT '[5:7]={a,b,c}'::text[]");
        assertNotNull(result);
    }

    @Test void array_custom_lower_bound_access() throws SQLException {
        // With 0-based array, index 0 should return first element
        String result = q("SELECT ('[0:2]={10,20,30}'::int[])[0]");
        assertEquals("10", result);
    }

    @Test void array_custom_lower_bound_access_5() throws SQLException {
        String result = q("SELECT ('[5:7]={a,b,c}'::text[])[5]");
        assertEquals("a", result);
    }

    @Test void array_custom_lower_bound_empty() {
        // empty array with reversed bounds should error
        assertSqlFails("SELECT '[2:1]={}'::int[]");
    }

    // ========================================================================
    // Section 9: VARIADIC Function Arguments
    // ========================================================================

    @Test void variadic_concat_ws() throws SQLException {
        String result = q("SELECT concat_ws(',', VARIADIC ARRAY['a','b','c'])");
        assertEquals("a,b,c", result);
    }

    @Test void variadic_empty_array() throws SQLException {
        String result = q("SELECT concat_ws(',', VARIADIC ARRAY[]::text[])");
        // Empty variadic = no extra args, just separator present but nothing to join
        assertEquals("", result);
    }

    @Test void variadic_mixed_positional() throws SQLException {
        String result = q("SELECT concat_ws(',', 'x', VARIADIC ARRAY['a','b'])");
        assertEquals("x,a,b", result);
    }

    @Test void variadic_concat_ws_single_element() throws SQLException {
        String result = q("SELECT concat_ws(',', VARIADIC ARRAY['only'])");
        assertEquals("only", result);
    }

    @Test void variadic_concat_ws_null_elements() throws SQLException {
        String result = q("SELECT concat_ws(',', VARIADIC ARRAY['a', NULL, 'c']::text[])");
        // concat_ws skips NULLs
        assertEquals("a,c", result);
    }

    // ========================================================================
    // Section 10: FETCH FIRST / OFFSET Syntax Variations
    // ========================================================================

    @Test void fetch_first_5_rows() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST 5 ROWS ONLY");
        assertEquals(5, rows);
    }

    @Test void fetch_first_row() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST ROW ONLY");
        assertEquals(1, rows);
    }

    @Test void fetch_first_with_offset_before() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data OFFSET 2 FETCH FIRST 3 ROWS ONLY");
        assertEquals(3, rows);
    }

    @Test void fetch_first_with_offset_after() throws SQLException {
        // PG allows FETCH before OFFSET (reversed order)
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST 3 ROWS ONLY OFFSET 2");
        assertEquals(3, rows);
    }

    @Test void fetch_next_instead_of_first() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH NEXT 5 ROWS ONLY");
        assertEquals(5, rows);
    }

    @Test void offset_with_rows_keyword() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY");
        assertEquals(3, rows);
    }

    @Test void fetch_first_1_row_only() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST 1 ROW ONLY");
        assertEquals(1, rows);
    }

    @Test void fetch_first_no_number() throws SQLException {
        // FETCH FIRST ROW ONLY = FETCH FIRST 1 ROW ONLY
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST ROW ONLY");
        assertEquals(1, rows);
    }

    @Test void fetch_next_row_only() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH NEXT ROW ONLY");
        assertEquals(1, rows);
    }

    @Test void offset_beyond_table() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data OFFSET 100 FETCH FIRST 5 ROWS ONLY");
        assertEquals(0, rows);
    }

    @Test void fetch_all_rows() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data FETCH FIRST 100 ROWS ONLY");
        assertEquals(10, rows); // only 10 rows in table
    }

    @Test void fetch_first_with_order_by() throws SQLException {
        String result = q("SELECT label FROM fetch_data ORDER BY id FETCH FIRST 1 ROW ONLY");
        assertEquals("a", result);
    }

    @Test void fetch_first_with_order_by_desc() throws SQLException {
        String result = q("SELECT label FROM fetch_data ORDER BY id DESC FETCH FIRST 1 ROW ONLY");
        assertEquals("j", result);
    }

    @Test void offset_rows_fetch_next() throws SQLException {
        int rows = queryRowCount("SELECT * FROM fetch_data OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY");
        assertEquals(3, rows);
    }

    // ========================================================================
    // Section 11: Substring Syntax Variants
    // ========================================================================

    @Test void substring_from_for_standard() throws SQLException {
        assertEquals("ell", q("SELECT substring('hello' FROM 2 FOR 3)"));
    }

    @Test void substring_for_from_reversed() throws SQLException {
        // PG allows FOR before FROM
        assertEquals("ell", q("SELECT substring('hello' FOR 3 FROM 2)"));
    }

    @Test void substring_from_only() throws SQLException {
        assertEquals("ello", q("SELECT substring('hello' FROM 2)"));
    }

    @Test void substring_for_only() throws SQLException {
        assertEquals("hel", q("SELECT substring('hello' FOR 3)"));
    }

    @Test void substring_regex_form() throws SQLException {
        // substring('hello' FROM '...$'), regex match, should return last 3 chars
        assertEquals("llo", q("SELECT substring('hello' FROM '...$')"));
    }

    @Test void substring_regex_form_pattern() throws SQLException {
        assertEquals("ell", q("SELECT substring('hello' FROM 'e..')"));
    }

    @Test void substring_similar_to_form() throws SQLException {
        // SQL standard SIMILAR TO form with escape
        assertEquals("ell", q("SELECT substring('hello' SIMILAR '%#\"ell#\"%' ESCAPE '#')"));
    }

    @Test void substring_from_for_edge_zero_length() throws SQLException {
        assertEquals("", q("SELECT substring('hello' FROM 2 FOR 0)"));
    }

    @Test void substring_from_beyond_length() throws SQLException {
        assertEquals("", q("SELECT substring('hello' FROM 10)"));
    }

    @Test void substring_from_1_for_full_length() throws SQLException {
        assertEquals("hello", q("SELECT substring('hello' FROM 1 FOR 5)"));
    }

    // ========================================================================
    // Section 12: COLLATE Syntax
    // ========================================================================

    @Test void collate_on_literal() throws SQLException {
        String result = q("SELECT 'abc' COLLATE \"C\"");
        assertEquals("abc", result);
    }

    @Test void collate_on_column() throws SQLException {
        List<String> vals = queryColumn(
                "SELECT col COLLATE \"C\" FROM collate_data ORDER BY id", 1);
        assertEquals(3, vals.size());
        assertEquals("banana", vals.get(0));
    }

    @Test void collate_with_function() throws SQLException {
        String result = q("SELECT upper(col COLLATE \"C\") FROM collate_data WHERE id = 1");
        assertEquals("BANANA", result);
    }

    @Test void collate_in_order_by() throws SQLException {
        List<String> vals = queryColumn(
                "SELECT col FROM collate_data ORDER BY col COLLATE \"C\"", 1);
        assertEquals(3, vals.size());
        // C collation: uppercase before lowercase, so 'Apple' < 'banana' < 'cherry'
        assertEquals("Apple", vals.get(0));
    }

    @Test void collate_default() throws SQLException {
        String result = q("SELECT 'abc' COLLATE \"default\"");
        assertEquals("abc", result);
    }

    @Test void collate_in_where() throws SQLException {
        int rows = queryRowCount(
                "SELECT * FROM collate_data WHERE col COLLATE \"C\" < 'b'");
        // In C collation, 'Apple' < 'b' is true (uppercase letters have lower codes)
        assertTrue(rows >= 1);
    }

    // ========================================================================
    // Section 13: Multirange Constructor
    // ========================================================================

    @Test void int4multirange_basic() throws SQLException {
        String result = q("SELECT int4multirange(int4range(1,5), int4range(7,9))");
        assertNotNull(result);
        // Expected: {[1,5),[7,9)}
        assertTrue(result.contains("[1,5)") && result.contains("[7,9)"),
                "Expected multirange with [1,5) and [7,9), got: " + result);
    }

    @Test void int4multirange_empty() throws SQLException {
        String result = q("SELECT int4multirange()");
        assertNotNull(result);
        assertEquals("{}", result);
    }

    @Test void int4multirange_single() throws SQLException {
        String result = q("SELECT int4multirange(int4range(1,5))");
        assertNotNull(result);
        assertTrue(result.contains("[1,5)"), "Expected [1,5), got: " + result);
    }

    @Test void nummultirange_basic() throws SQLException {
        String result = q("SELECT nummultirange(numrange(1.0, 2.0))");
        assertNotNull(result);
    }

    @Test void int4multirange_literal() throws SQLException {
        String result = q("SELECT '{[1,5),[7,9)}'::int4multirange");
        assertNotNull(result);
        assertTrue(result.contains("[1,5)") && result.contains("[7,9)"),
                "Expected multirange literal parse, got: " + result);
    }

    @Test void int4multirange_overlapping_ranges_merge() throws SQLException {
        // Overlapping ranges should be merged
        String result = q("SELECT int4multirange(int4range(1,5), int4range(3,8))");
        assertNotNull(result);
        // Should merge to {[1,8)}
        assertTrue(result.contains("[1,8)"), "Expected merged range [1,8), got: " + result);
    }

    @Test void int4multirange_adjacent_merge() throws SQLException {
        String result = q("SELECT int4multirange(int4range(1,3), int4range(3,5))");
        assertNotNull(result);
        // Adjacent ranges should merge: {[1,5)}
        assertTrue(result.contains("[1,5)"), "Expected merged [1,5), got: " + result);
    }

    // ========================================================================
    // Section 14: Range Adjacency Operator
    // ========================================================================

    @Test void range_adjacency_true() throws SQLException {
        assertEquals("t", q("SELECT int4range(1,5) -|- int4range(5,10)"));
    }

    @Test void range_adjacency_false() throws SQLException {
        assertEquals("f", q("SELECT int4range(1,5) -|- int4range(6,10)"));
    }

    @Test void range_intersection() throws SQLException {
        String result = q("SELECT int4range(1,5) * int4range(3,8)");
        assertNotNull(result);
        assertEquals("[3,5)", result);
    }

    @Test void range_union() throws SQLException {
        String result = q("SELECT int4range(1,5) + int4range(3,8)");
        assertNotNull(result);
        assertEquals("[1,8)", result);
    }

    @Test void range_difference() throws SQLException {
        // Use a contiguous result: int4range(1,10) - int4range(5,10) = [1,5)
        String result = q("SELECT int4range(1,10) - int4range(5,10)");
        assertNotNull(result);
        assertEquals("[1,5)", result);
    }

    @Test void range_containment_element() throws SQLException {
        assertEquals("t", q("SELECT int4range(1,10) @> 5"));
    }

    @Test void range_containment_range() throws SQLException {
        assertEquals("t", q("SELECT int4range(1,10) @> int4range(3,5)"));
    }

    @Test void range_overlap() throws SQLException {
        assertEquals("t", q("SELECT int4range(1,5) && int4range(3,8)"));
    }

    @Test void range_no_overlap() throws SQLException {
        assertEquals("f", q("SELECT int4range(1,3) && int4range(5,8)"));
    }

    // ========================================================================
    // Section 15: generate_subscripts Function
    // ========================================================================

    @Test void generate_subscripts_basic() throws SQLException {
        List<String> vals = queryColumn("SELECT generate_subscripts(ARRAY[10,20,30], 1)", 1);
        assertEquals(3, vals.size());
        assertEquals("1", vals.get(0));
        assertEquals("2", vals.get(1));
        assertEquals("3", vals.get(2));
    }

    @Test void generate_subscripts_2d_first_dim() throws SQLException {
        List<String> vals = queryColumn("SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 1)", 1);
        assertEquals(2, vals.size()); // 2 rows in first dimension
    }

    @Test void generate_subscripts_2d_second_dim() throws SQLException {
        List<String> vals = queryColumn("SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 2)", 1);
        assertEquals(2, vals.size()); // 2 columns in second dimension
    }

    @Test void generate_subscripts_reverse() throws SQLException {
        List<String> vals = queryColumn("SELECT generate_subscripts(ARRAY[10,20,30], 1, true)", 1);
        assertEquals(3, vals.size());
        assertEquals("3", vals.get(0));
        assertEquals("2", vals.get(1));
        assertEquals("1", vals.get(2));
    }

    @Test void generate_subscripts_empty_array() throws SQLException {
        int rows = queryRowCount("SELECT generate_subscripts(ARRAY[]::int[], 1)");
        assertEquals(0, rows);
    }

    @Test void generate_subscripts_single_element() throws SQLException {
        List<String> vals = queryColumn("SELECT generate_subscripts(ARRAY[42], 1)", 1);
        assertEquals(1, vals.size());
        assertEquals("1", vals.get(0));
    }

    // ========================================================================
    // Section 16: unnest with Multiple Arrays
    // ========================================================================

    @Test void unnest_parallel_two_arrays() throws SQLException {
        String sql = "SELECT * FROM unnest(ARRAY[1,2,3], ARRAY['a','b','c']) AS u(i,t)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    @Test void unnest_parallel_mismatched_lengths() throws SQLException {
        String sql = "SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b','c']) AS u(i,t)";
        int rows = queryRowCount(sql);
        // Shorter array padded with NULLs
        assertEquals(3, rows);
    }

    @Test void unnest_parallel_mismatched_check_nulls() throws SQLException {
        String sql = "SELECT i, t FROM unnest(ARRAY[1,2], ARRAY['a','b','c']) AS u(i,t) ORDER BY t";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next(); // a
            assertEquals("1", rs.getString(1));
            assertEquals("a", rs.getString(2));
            rs.next(); // b
            assertEquals("2", rs.getString(1));
            assertEquals("b", rs.getString(2));
            rs.next(); // c
            assertNull(rs.getString(1)); // NULL for missing element
            assertEquals("c", rs.getString(2));
        }
    }

    @Test void unnest_parallel_three_arrays() throws SQLException {
        String sql = "SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b'], ARRAY[true,false]) AS u(i,t,b)";
        int rows = queryRowCount(sql);
        assertEquals(2, rows);
    }

    @Test void unnest_parallel_with_ordinality() throws SQLException {
        String sql = "SELECT * FROM unnest(ARRAY[10,20,30]) WITH ORDINALITY AS u(val, ord)";
        int rows = queryRowCount(sql);
        assertEquals(3, rows);
    }

    @Test void unnest_parallel_empty_first() throws SQLException {
        String sql = "SELECT * FROM unnest(ARRAY[]::int[], ARRAY['a','b']) AS u(i,t)";
        int rows = queryRowCount(sql);
        assertEquals(2, rows); // padded with NULLs for first array
    }
}
