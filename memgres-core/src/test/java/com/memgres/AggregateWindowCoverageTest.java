package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 23-24 (Aggregate & Window Functions).
 *
 * 23. Aggregate functions
 * 24. Window functions
 */
class AggregateWindowCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());

        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE emp (id SERIAL, name TEXT, dept TEXT, salary INTEGER, active BOOLEAN)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Alice', 'Eng', 90000, TRUE)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Bob', 'Eng', 85000, TRUE)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Charlie', 'Sales', 70000, FALSE)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Diana', 'Sales', 75000, TRUE)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Eve', 'HR', 65000, TRUE)");
            s.execute("INSERT INTO emp (name, dept, salary, active) VALUES ('Frank', 'HR', 60000, FALSE)");

            s.execute("CREATE TABLE scores (student TEXT, subject TEXT, score INTEGER)");
            s.execute("INSERT INTO scores VALUES ('Alice', 'Math', 90), ('Alice', 'Science', 85), ('Alice', 'English', 92)");
            s.execute("INSERT INTO scores VALUES ('Bob', 'Math', 78), ('Bob', 'Science', 88), ('Bob', 'English', 72)");
            s.execute("INSERT INTO scores VALUES ('Charlie', 'Math', 95), ('Charlie', 'Science', 80), ('Charlie', 'English', 85)");

            s.execute("CREATE TABLE bits (id INTEGER, val INTEGER)");
            s.execute("INSERT INTO bits VALUES (1, 12), (2, 10), (3, 14)"); // 1100, 1010, 1110
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 23. Aggregate Functions
    // =========================================================================

    // --- COUNT ---

    @Test
    void count_star() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1));
        }
    }

    @Test
    void count_column() throws SQLException {
        // COUNT(expr) counts non-null values
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(name) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1));
        }
    }

    @Test
    void count_distinct() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(DISTINCT dept) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void count_with_where() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM emp WHERE active = TRUE")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    // --- SUM ---

    @Test
    void sum_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT SUM(salary) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(445000, rs.getInt(1));
        }
    }

    @Test
    void sum_grouped() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT dept, SUM(salary) FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertEquals(175000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(125000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(145000, rs.getInt(2));
        }
    }

    @Test
    void sum_distinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sum_dist (val INTEGER)");
            s.execute("INSERT INTO sum_dist VALUES (1), (2), (2), (3), (3), (3)");
            ResultSet rs = s.executeQuery("SELECT SUM(DISTINCT val) FROM sum_dist");
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1)); // 1 + 2 + 3
            s.execute("DROP TABLE sum_dist");
        }
    }

    // --- AVG ---

    @Test
    void avg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT AVG(salary) FROM emp")) {
            assertTrue(rs.next());
            double avg = rs.getDouble(1);
            assertTrue(avg > 74000 && avg < 75000); // 445000/6 ≈ 74166.67
        }
    }

    @Test
    void avg_distinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE avg_dist (val INTEGER)");
            s.execute("INSERT INTO avg_dist VALUES (10), (20), (20), (30), (30), (30)");
            ResultSet rs = s.executeQuery("SELECT AVG(DISTINCT val) FROM avg_dist");
            assertTrue(rs.next());
            assertEquals(20.0, rs.getDouble(1), 0.01); // (10+20+30)/3
            s.execute("DROP TABLE avg_dist");
        }
    }

    // --- MIN / MAX ---

    @Test
    void min_max() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT MIN(salary), MAX(salary) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(60000, rs.getInt(1));
            assertEquals(90000, rs.getInt(2));
        }
    }

    @Test
    void min_max_strings() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT MIN(name), MAX(name) FROM emp")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals("Frank", rs.getString(2));
        }
    }

    @Test
    void min_max_grouped() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT dept, MIN(salary), MAX(salary) FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertEquals(85000, rs.getInt(2)); assertEquals(90000, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(60000, rs.getInt(2)); assertEquals(65000, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(70000, rs.getInt(2)); assertEquals(75000, rs.getInt(3));
        }
    }

    // --- STRING_AGG ---

    @Test
    void string_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT STRING_AGG(name, ', ' ORDER BY name) FROM emp")) {
            assertTrue(rs.next());
            assertEquals("Alice, Bob, Charlie, Diana, Eve, Frank", rs.getString(1));
        }
    }

    @Test
    void string_agg_grouped() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT dept, STRING_AGG(name, '/' ORDER BY name) FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertEquals("Alice/Bob", rs.getString(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals("Eve/Frank", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals("Charlie/Diana", rs.getString(2));
        }
    }

    // --- ARRAY_AGG ---

    @Test
    void array_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY_AGG(name ORDER BY name) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain Alice before Bob
            assertTrue(val.indexOf("Alice") < val.indexOf("Bob"));
        }
    }

    @Test
    void array_agg_grouped() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT dept, ARRAY_AGG(name ORDER BY name) FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
        }
    }

    // --- BOOL_AND / BOOL_OR / EVERY ---

    @Test
    void bool_and_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT BOOL_AND(active) FROM emp")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // Not all active
        }
    }

    @Test
    void bool_and_grouped() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT dept, BOOL_AND(active) FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertTrue(rs.getBoolean(2));   // Both active
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertFalse(rs.getBoolean(2));  // Frank is inactive
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertFalse(rs.getBoolean(2)); // Charlie is inactive
        }
    }

    @Test
    void bool_or_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT BOOL_OR(active) FROM emp")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1)); // At least one active
        }
    }

    @Test
    void every_is_bool_and() throws SQLException {
        // EVERY is an alias for BOOL_AND
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT EVERY(active) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1)); // Both Eng employees are active
        }
    }

    // --- BIT_AND / BIT_OR ---

    @Test
    void bit_and_aggregate() throws SQLException {
        // 12 & 10 & 14 = 1100 & 1010 & 1110 = 1000 = 8
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT BIT_AND(val) FROM bits")) {
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1));
        }
    }

    @Test
    void bit_or_aggregate() throws SQLException {
        // 12 | 10 | 14 = 1100 | 1010 | 1110 = 1110 = 14
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT BIT_OR(val) FROM bits")) {
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1));
        }
    }

    @Test
    void bit_and_grouped() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bit_grp (grp TEXT, val INTEGER)");
            s.execute("INSERT INTO bit_grp VALUES ('A', 15), ('A', 12), ('B', 7), ('B', 3)");
            ResultSet rs = s.executeQuery("SELECT grp, BIT_AND(val) FROM bit_grp GROUP BY grp ORDER BY grp");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals(12, rs.getInt(2)); // 15&12=12
            assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals(3, rs.getInt(2));  // 7&3=3
            s.execute("DROP TABLE bit_grp");
        }
    }

    // --- JSON_AGG / JSONB_AGG ---

    @Test
    void json_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_AGG(name) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertNotNull(json);
            assertTrue(json.startsWith("["));
            assertTrue(json.endsWith("]"));
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("Bob"));
        }
    }

    @Test
    void jsonb_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSONB_AGG(salary) FROM emp WHERE dept = 'HR'")) {
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertNotNull(json);
            assertTrue(json.contains("65000"));
            assertTrue(json.contains("60000"));
        }
    }

    @Test
    void json_agg_with_nulls() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE jn_test (val TEXT)");
            s.execute("INSERT INTO jn_test VALUES ('a'), (NULL), ('c')");
            ResultSet rs = s.executeQuery("SELECT JSON_AGG(val) FROM jn_test");
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertTrue(json.contains("null"));
            assertTrue(json.contains("a"));
            assertTrue(json.contains("c"));
            s.execute("DROP TABLE jn_test");
        }
    }

    // --- JSON_OBJECT_AGG / JSONB_OBJECT_AGG ---

    @Test
    void json_object_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_OBJECT_AGG(name, salary) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertNotNull(json);
            assertTrue(json.startsWith("{"));
            assertTrue(json.endsWith("}"));
            assertTrue(json.contains("Alice"));
            assertTrue(json.contains("90000"));
        }
    }

    @Test
    void jsonb_object_agg_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSONB_OBJECT_AGG(name, dept) FROM emp WHERE dept = 'Sales'")) {
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertTrue(json.contains("Charlie"));
            assertTrue(json.contains("Sales"));
        }
    }

    // --- FILTER (WHERE) clause ---

    @Test
    void count_filter() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FILTER (WHERE active = TRUE) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void sum_filter() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT SUM(salary) FILTER (WHERE active = TRUE) FROM emp")) {
            assertTrue(rs.next());
            assertEquals(315000, rs.getInt(1)); // 90+85+75+65=315k
        }
    }

    @Test
    void multiple_filters_same_query() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT COUNT(*) FILTER (WHERE dept = 'Eng'), " +
                "COUNT(*) FILTER (WHERE dept = 'Sales'), " +
                "COUNT(*) FILTER (WHERE dept = 'HR') FROM emp")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(2, rs.getInt(3));
        }
    }

    @Test
    void filter_with_group_by() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT dept, COUNT(*) FILTER (WHERE active = TRUE) AS active_count FROM emp GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Eng", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(1, rs.getInt(2));
        }
    }

    // --- Aggregate ORDER BY (within agg) ---

    @Test
    void string_agg_with_order_by_desc() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT STRING_AGG(name, ', ' ORDER BY name DESC) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            assertEquals("Bob, Alice", rs.getString(1));
        }
    }

    @Test
    void array_agg_with_order_by() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY_AGG(salary ORDER BY salary DESC) FROM emp WHERE dept = 'Eng'")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            // 90000 should come before 85000
            assertTrue(val.indexOf("90000") < val.indexOf("85000"));
        }
    }

    // --- Empty group aggregates ---

    @Test
    void aggregates_on_empty_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE empty_agg (val INTEGER)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM empty_agg");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1)); // COUNT returns 0
            assertNull(rs.getObject(2));   // SUM returns NULL
            assertNull(rs.getObject(3));   // AVG returns NULL
            assertNull(rs.getObject(4));   // MIN returns NULL
            assertNull(rs.getObject(5));   // MAX returns NULL
            s.execute("DROP TABLE empty_agg");
        }
    }

    // =========================================================================
    // 24. Window Functions
    // =========================================================================

    // --- ROW_NUMBER ---

    @Test
    void row_number_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(2));
        }
    }

    @Test
    void row_number_partitioned() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn " +
                "FROM emp ORDER BY dept, rn")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(3));
        }
    }

    // --- RANK ---

    @Test
    void rank_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rank_test (val INTEGER)");
            s.execute("INSERT INTO rank_test VALUES (10), (20), (20), (30)");
            ResultSet rs = s.executeQuery("SELECT val, RANK() OVER (ORDER BY val) AS rnk FROM rank_test ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(4, rs.getInt(2)); // Skips 3
            s.execute("DROP TABLE rank_test");
        }
    }

    // --- DENSE_RANK ---

    @Test
    void dense_rank_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE drank_test (val INTEGER)");
            s.execute("INSERT INTO drank_test VALUES (10), (20), (20), (30)");
            ResultSet rs = s.executeQuery("SELECT val, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM drank_test ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(3, rs.getInt(2)); // No skip
            s.execute("DROP TABLE drank_test");
        }
    }

    // --- NTILE ---

    @Test
    void ntile_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, NTILE(3) OVER (ORDER BY salary DESC) AS tile FROM emp ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(2)); // Diana
            assertTrue(rs.next()); assertEquals(2, rs.getInt(2)); // Charlie
            assertTrue(rs.next()); assertEquals(3, rs.getInt(2)); // Eve
            assertTrue(rs.next()); assertEquals(3, rs.getInt(2)); // Frank
        }
    }

    // --- LAG ---

    @Test
    void lag_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, LAG(salary) OVER (ORDER BY salary) AS prev_salary FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertNull(rs.getObject(3)); // No previous
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1)); assertEquals(60000, rs.getInt(3));
        }
    }

    @Test
    void lag_with_offset_and_default() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, LAG(salary, 2, 0) OVER (ORDER BY salary) AS prev2 FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertEquals(0, rs.getInt(3)); // default
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1)); assertEquals(0, rs.getInt(3));   // default
            assertTrue(rs.next()); assertEquals(60000, rs.getInt(3)); // Frank's salary
        }
    }

    // --- LEAD ---

    @Test
    void lead_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, LEAD(salary) OVER (ORDER BY salary DESC) AS next_salary FROM emp ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(85000, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(75000, rs.getInt(3));
        }
    }

    @Test
    void lead_with_offset_and_default() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, LEAD(salary, 2, -1) OVER (ORDER BY salary DESC) AS next2 FROM emp ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(75000, rs.getInt(3));
        }
    }

    // --- FIRST_VALUE / LAST_VALUE / NTH_VALUE ---

    @Test
    void first_value() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS top_earner " +
                "FROM emp ORDER BY dept, salary DESC")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Alice", rs.getString(3));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Alice", rs.getString(3));
        }
    }

    @Test
    void last_value_with_frame() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, LAST_VALUE(name) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last " +
                "FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(3)); // Highest salary name
        }
    }

    @Test
    void nth_value() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, NTH_VALUE(name, 2) OVER (ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second " +
                "FROM emp ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(3)); // Second highest
        }
    }

    // --- PERCENT_RANK ---

    @Test
    void percent_rank_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prank_test (val INTEGER)");
            s.execute("INSERT INTO prank_test VALUES (10), (20), (30), (40)");
            ResultSet rs = s.executeQuery("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM prank_test ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(0.0, rs.getDouble(2), 0.001);   // (1-1)/(4-1) = 0
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.333, rs.getDouble(2), 0.01);  // (2-1)/(4-1) = 1/3
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(0.667, rs.getDouble(2), 0.01);  // (3-1)/(4-1) = 2/3
            assertTrue(rs.next()); assertEquals(40, rs.getInt(1)); assertEquals(1.0, rs.getDouble(2), 0.001);   // (4-1)/(4-1) = 1
            s.execute("DROP TABLE prank_test");
        }
    }

    @Test
    void percent_rank_with_ties() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prank_ties (val INTEGER)");
            s.execute("INSERT INTO prank_ties VALUES (10), (20), (20), (30)");
            ResultSet rs = s.executeQuery("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM prank_ties ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(0.0, rs.getDouble(2), 0.001);
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.333, rs.getDouble(2), 0.01);
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.333, rs.getDouble(2), 0.01); // Same rank as above
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(1.0, rs.getDouble(2), 0.001);
            s.execute("DROP TABLE prank_ties");
        }
    }

    @Test
    void percent_rank_single_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prank_one (val INTEGER)");
            s.execute("INSERT INTO prank_one VALUES (42)");
            ResultSet rs = s.executeQuery("SELECT val, PERCENT_RANK() OVER (ORDER BY val) AS pr FROM prank_one");
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(2), 0.001); // Single row → 0
            s.execute("DROP TABLE prank_one");
        }
    }

    // --- CUME_DIST ---

    @Test
    void cume_dist_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cdist_test (val INTEGER)");
            s.execute("INSERT INTO cdist_test VALUES (10), (20), (30), (40)");
            ResultSet rs = s.executeQuery("SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM cdist_test ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(0.25, rs.getDouble(2), 0.001);  // 1/4
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.50, rs.getDouble(2), 0.001);  // 2/4
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(0.75, rs.getDouble(2), 0.001);  // 3/4
            assertTrue(rs.next()); assertEquals(40, rs.getInt(1)); assertEquals(1.0, rs.getDouble(2), 0.001);   // 4/4
            s.execute("DROP TABLE cdist_test");
        }
    }

    @Test
    void cume_dist_with_ties() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cdist_ties (val INTEGER)");
            s.execute("INSERT INTO cdist_ties VALUES (10), (20), (20), (30)");
            ResultSet rs = s.executeQuery("SELECT val, CUME_DIST() OVER (ORDER BY val) AS cd FROM cdist_ties ORDER BY val");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertEquals(0.25, rs.getDouble(2), 0.001);
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.75, rs.getDouble(2), 0.001); // 3/4 (both 20s)
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(0.75, rs.getDouble(2), 0.001);
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(1.0, rs.getDouble(2), 0.001);
            s.execute("DROP TABLE cdist_ties");
        }
    }

    // --- Aggregate OVER (window) ---

    @Test
    void sum_over() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, SUM(salary) OVER () AS total FROM emp ORDER BY salary DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals(445000, rs.getInt(3));
        }
    }

    @Test
    void sum_over_partition() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) AS dept_total " +
                "FROM emp ORDER BY dept, name LIMIT 2")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(175000, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(175000, rs.getInt(4));
        }
    }

    @Test
    void count_over() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM emp ORDER BY dept, name LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals(2, rs.getInt(2));
        }
    }

    @Test
    void avg_over() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, AVG(salary) OVER (PARTITION BY dept) AS dept_avg " +
                "FROM emp WHERE dept = 'Eng' ORDER BY name LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals(87500.0, rs.getDouble(3), 0.1);
        }
    }

    @Test
    void running_sum() throws SQLException {
        // Running (cumulative) sum with ORDER BY
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, SUM(salary) OVER (ORDER BY salary) AS running_total FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertEquals(60000, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1)); assertEquals(125000, rs.getInt(3));
        }
    }

    // --- Frame clauses ---

    @Test
    void rows_between_preceding_and_following() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, AVG(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg " +
                "FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); // Frank (60k), avg of [60k, 65k] = 62500
            assertEquals("Frank", rs.getString(1));
            double avg1 = rs.getDouble(3);
            assertTrue(avg1 > 62000 && avg1 < 63000);
        }
    }

    @Test
    void rows_unbounded_preceding() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, COUNT(*) OVER (ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_count " +
                "FROM emp ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(3));
        }
    }

    // --- Named WINDOW clause ---

    @Test
    void named_window_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, ROW_NUMBER() OVER w AS rn " +
                "FROM emp WINDOW w AS (ORDER BY salary DESC) ORDER BY salary DESC LIMIT 3")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(3));
        }
    }

    @Test
    void named_window_with_partition() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, dept, salary, RANK() OVER w AS dept_rank " +
                "FROM emp WINDOW w AS (PARTITION BY dept ORDER BY salary DESC) ORDER BY dept, dept_rank")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(4));
        }
    }

    @Test
    void named_window_multiple_functions() throws SQLException {
        // Multiple window functions sharing the same named window
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, " +
                "ROW_NUMBER() OVER w AS rn, " +
                "RANK() OVER w AS rnk, " +
                "SUM(salary) OVER w AS running " +
                "FROM emp WINDOW w AS (ORDER BY salary) ORDER BY salary LIMIT 2")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1));
            assertEquals(1, rs.getInt(3)); // rn
            assertEquals(1, rs.getInt(4)); // rank
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1));
            assertEquals(2, rs.getInt(3)); // rn
            assertEquals(2, rs.getInt(4)); // rank
        }
    }

    @Test
    void named_window_two_definitions() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, " +
                "ROW_NUMBER() OVER w_all AS global_rn, " +
                "ROW_NUMBER() OVER w_dept AS dept_rn " +
                "FROM emp " +
                "WINDOW w_all AS (ORDER BY salary DESC), w_dept AS (PARTITION BY dept ORDER BY salary DESC) " +
                "ORDER BY salary DESC LIMIT 2")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(3)); assertEquals(1, rs.getInt(4));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(3)); assertEquals(2, rs.getInt(4));
        }
    }

    // --- Multiple window functions in one query ---

    @Test
    void multiple_window_functions() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, salary, " +
                "ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn, " +
                "SUM(salary) OVER (PARTITION BY dept) AS dept_total, " +
                "MIN(salary) OVER () AS global_min, " +
                "MAX(salary) OVER () AS global_max " +
                "FROM emp ORDER BY salary DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals(1, rs.getInt(3));       // rn
            assertEquals(175000, rs.getInt(4));  // Eng dept total
            assertEquals(60000, rs.getInt(5));   // global min
            assertEquals(90000, rs.getInt(6));   // global max
        }
    }

    // --- Window with WHERE ---

    @Test
    void window_with_where() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn " +
                "FROM emp WHERE active = TRUE ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1)); assertEquals(3, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1)); assertEquals(4, rs.getInt(2));
            assertFalse(rs.next()); // Only 4 active employees
        }
    }
}
