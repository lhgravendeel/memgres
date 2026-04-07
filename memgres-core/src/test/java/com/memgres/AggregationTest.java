package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 2 integration tests: Aggregation & Grouping.
 * Tests GROUP BY, HAVING, and aggregate functions (COUNT, SUM, AVG, MIN, MAX,
 * STRING_AGG, ARRAY_AGG, BOOL_AND, BOOL_OR, COUNT(DISTINCT)).
 */
class AggregationTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() throws SQLException {
        memgres = Memgres.builder().port(0).build().start();
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE sales (id integer, product text, category text, amount integer, qty integer)");
            stmt.execute("INSERT INTO sales (id, product, category, amount, qty) VALUES (1, 'Apple', 'fruit', 100, 10)");
            stmt.execute("INSERT INTO sales (id, product, category, amount, qty) VALUES (2, 'Banana', 'fruit', 50, 20)");
            stmt.execute("INSERT INTO sales (id, product, category, amount, qty) VALUES (3, 'Carrot', 'vegetable', 30, 15)");
            stmt.execute("INSERT INTO sales (id, product, category, amount, qty) VALUES (4, 'Dates', 'fruit', 200, 5)");
            stmt.execute("INSERT INTO sales (id, product, category, amount, qty) VALUES (5, 'Eggplant', 'vegetable', 80, 8)");
        }
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ---- COUNT ----

    @Test
    void shouldCountAllRows() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM sales")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldCountNonNullValues() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE t (id integer, name text)");
            stmt.execute("INSERT INTO t (id, name) VALUES (1, 'a')");
            stmt.execute("INSERT INTO t (id, name) VALUES (2, null)");
            stmt.execute("INSERT INTO t (id, name) VALUES (3, 'c')");

            try (ResultSet rs = stmt.executeQuery("SELECT count(name) FROM t")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getLong(1));
            }
        }
    }

    @Test
    void shouldCountDistinct() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(DISTINCT category) FROM sales")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
        }
    }

    @Test
    void shouldReturnZeroCountForEmptyTable() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_t (id integer)");

            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM empty_t")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getLong(1));
            }
        }
    }

    // ---- SUM ----

    @Test
    void shouldSumColumn() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT sum(amount) FROM sales")) {
            assertTrue(rs.next());
            assertEquals(460, rs.getLong(1));
        }
    }

    @Test
    void shouldReturnNullSumForEmptyTable() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE empty_t (id integer)");

            try (ResultSet rs = stmt.executeQuery("SELECT sum(id) FROM empty_t")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    // ---- AVG ----

    @Test
    void shouldAvgColumn() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT avg(amount) FROM sales")) {
            assertTrue(rs.next());
            assertEquals(92.0, rs.getDouble(1), 0.001);
        }
    }

    // ---- MIN / MAX ----

    @Test
    void shouldFindMinAndMax() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT min(amount), max(amount) FROM sales")) {
            assertTrue(rs.next());
            assertEquals("30", rs.getString(1));
            assertEquals("200", rs.getString(2));
        }
    }

    // ---- GROUP BY ----

    @Test
    void shouldGroupBySingleColumn() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) AS cnt FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(3, rs.getLong("cnt"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals(2, rs.getLong("cnt"));

            assertFalse(rs.next());
        }
    }

    @Test
    void shouldGroupByWithSum() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, sum(amount) AS total FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(350, rs.getLong("total"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals(110, rs.getLong("total"));

            assertFalse(rs.next());
        }
    }

    @Test
    void shouldGroupByWithAvg() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, avg(amount) AS avg_amount FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            double fruitAvg = rs.getDouble("avg_amount");
            assertEquals(116.67, fruitAvg, 0.01);

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals(55.0, rs.getDouble("avg_amount"), 0.01);

            assertFalse(rs.next());
        }
    }

    @Test
    void shouldGroupByWithMinMax() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, min(amount) AS mn, max(amount) AS mx FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals("50", rs.getString("mn"));
            assertEquals("200", rs.getString("mx"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals("30", rs.getString("mn"));
            assertEquals("80", rs.getString("mx"));

            assertFalse(rs.next());
        }
    }

    @Test
    void shouldGroupByWithMultipleAggregates() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) AS cnt, sum(amount) AS total, avg(qty) AS avg_qty " +
                             "FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(3, rs.getLong("cnt"));
            assertEquals(350, rs.getLong("total"));
            // avg qty for fruit: (10+20+5)/3 = 11.67
            assertEquals(11.67, rs.getDouble("avg_qty"), 0.01);

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals(2, rs.getLong("cnt"));
            assertEquals(110, rs.getLong("total"));
            // avg qty for vegetable: (15+8)/2 = 11.5
            assertEquals(11.5, rs.getDouble("avg_qty"), 0.01);

            assertFalse(rs.next());
        }
    }

    // ---- HAVING ----

    @Test
    void shouldFilterGroupsWithHaving() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) AS cnt FROM sales GROUP BY category HAVING count(*) > 2")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(3, rs.getLong("cnt"));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldFilterWithHavingSum() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, sum(amount) AS total FROM sales GROUP BY category HAVING sum(amount) > 200")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(350, rs.getLong("total"));
            assertFalse(rs.next());
        }
    }

    // ---- WHERE + GROUP BY + HAVING combined ----

    @Test
    void shouldCombineWhereGroupByHaving() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) AS cnt FROM sales WHERE amount > 40 GROUP BY category HAVING count(*) >= 2 ORDER BY category")) {
            // After WHERE amount > 40: Apple(100), Banana(50), Dates(200), Eggplant(80)
            // GROUP BY: fruit=3, vegetable=1
            // HAVING count(*) >= 2: only fruit
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(3, rs.getLong("cnt"));
            assertFalse(rs.next());
        }
    }

    // ---- Aggregate without GROUP BY ----

    @Test
    void shouldAggregateWithoutGroupBy() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*), sum(amount), min(amount), max(amount), avg(amount) FROM sales")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));
            assertEquals(460, rs.getLong(2));
            assertEquals("30", rs.getString(3));
            assertEquals("200", rs.getString(4));
            assertEquals(92.0, rs.getDouble(5), 0.001);
            assertFalse(rs.next());
        }
    }

    // ---- STRING_AGG ----

    @Test
    void shouldStringAgg() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, string_agg(product, ', ') AS products FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            String products = rs.getString("products");
            // Order within group is insertion order
            assertTrue(products.contains("Apple"));
            assertTrue(products.contains("Banana"));
            assertTrue(products.contains("Dates"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));

            assertFalse(rs.next());
        }
    }

    // ---- BOOL_AND / BOOL_OR ----

    @Test
    void shouldBoolAnd() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE flags (category text, active boolean)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('a', true)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('a', true)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('b', true)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('b', false)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT category, bool_and(active) AS all_active FROM flags GROUP BY category ORDER BY category")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString("category"));
                assertEquals("t", rs.getString("all_active"));

                assertTrue(rs.next());
                assertEquals("b", rs.getString("category"));
                assertEquals("f", rs.getString("all_active"));

                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldBoolOr() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE flags (category text, active boolean)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('a', false)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('a', false)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('b', false)");
            stmt.execute("INSERT INTO flags (category, active) VALUES ('b', true)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT category, bool_or(active) AS any_active FROM flags GROUP BY category ORDER BY category")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString("category"));
                assertEquals("f", rs.getString("any_active"));

                assertTrue(rs.next());
                assertEquals("b", rs.getString("category"));
                assertEquals("t", rs.getString("any_active"));

                assertFalse(rs.next());
            }
        }
    }

    // ---- ORDER BY with GROUP BY ----

    @Test
    void shouldOrderGroupedResults() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, sum(amount) AS total FROM sales GROUP BY category ORDER BY total DESC")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertEquals(350, rs.getLong("total"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals(110, rs.getLong("total"));

            assertFalse(rs.next());
        }
    }

    // ---- LIMIT with GROUP BY ----

    @Test
    void shouldLimitGroupedResults() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) AS cnt FROM sales GROUP BY category ORDER BY cnt DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            assertFalse(rs.next());
        }
    }

    // ---- Expressions with aggregates ----

    @Test
    void shouldSupportExpressionsWithAggregates() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, sum(amount) * 2 AS doubled FROM sales GROUP BY category ORDER BY category")) {
            assertTrue(rs.next());
            assertEquals("fruit", rs.getString("category"));
            // sum(amount) for fruit = 350, * 2 = 700
            assertEquals("700", rs.getString("doubled"));

            assertTrue(rs.next());
            assertEquals("vegetable", rs.getString("category"));
            assertEquals("220", rs.getString("doubled"));

            assertFalse(rs.next());
        }
    }

    // ---- COUNT with GROUP BY on empty groups ----

    @Test
    void shouldHandleGroupByOnFilteredEmptyResult() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT category, count(*) FROM sales WHERE amount > 10000 GROUP BY category")) {
            // No rows match, so no groups
            assertFalse(rs.next());
        }
    }

    // ---- Multiple columns in GROUP BY ----

    @Test
    void shouldGroupByMultipleColumns() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE orders (region text, product text, amount integer)");
            stmt.execute("INSERT INTO orders (region, product, amount) VALUES ('east', 'A', 10)");
            stmt.execute("INSERT INTO orders (region, product, amount) VALUES ('east', 'A', 20)");
            stmt.execute("INSERT INTO orders (region, product, amount) VALUES ('east', 'B', 30)");
            stmt.execute("INSERT INTO orders (region, product, amount) VALUES ('west', 'A', 40)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT region, product, sum(amount) AS total FROM orders GROUP BY region, product ORDER BY region, product")) {
                assertTrue(rs.next());
                assertEquals("east", rs.getString("region"));
                assertEquals("A", rs.getString("product"));
                assertEquals(30, rs.getLong("total"));

                assertTrue(rs.next());
                assertEquals("east", rs.getString("region"));
                assertEquals("B", rs.getString("product"));
                assertEquals(30, rs.getLong("total"));

                assertTrue(rs.next());
                assertEquals("west", rs.getString("region"));
                assertEquals("A", rs.getString("product"));
                assertEquals(40, rs.getLong("total"));

                assertFalse(rs.next());
            }
        }
    }
}
