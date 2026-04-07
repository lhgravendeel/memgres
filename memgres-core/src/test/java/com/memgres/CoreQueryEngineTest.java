package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1 integration tests: Core Query Engine.
 * Tests expression evaluation, WHERE, ORDER BY, LIMIT/OFFSET, DISTINCT,
 * NULL semantics, multi-table FROM (cross join), column aliases, etc.
 */
class CoreQueryEngineTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ---- SELECT expressions (no FROM) ----

    @Test
    void shouldEvaluateArithmetic() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 2 + 3 * 4")) {
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1));
        }
    }

    @Test
    void shouldEvaluateStringConcatenation() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'hello' || ' ' || 'world'")) {
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
    }

    @Test
    void shouldEvaluateCaseExpression() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'negative' END")) {
            assertTrue(rs.next());
            assertEquals("positive", rs.getString(1));
        }
    }

    @Test
    void shouldEvaluateCoalesce() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT coalesce(null, null, 'found')")) {
            assertTrue(rs.next());
            assertEquals("found", rs.getString(1));
        }
    }

    @Test
    void shouldEvaluateNullif() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT nullif(1, 1)")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT nullif(1, 2)")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
            }
        }
    }

    // ---- Type casting ----

    @Test
    void shouldCastToInteger() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '42'::integer")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void shouldCastToText() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 42::text")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
        }
    }

    @Test
    void shouldCastToBoolean() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 'true'::boolean")) {
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    // ---- WHERE clause ----

    @Test
    void shouldFilterWithCompoundWhere() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text, price integer)");
            stmt.execute("INSERT INTO items (id, name, price) VALUES (1, 'apple', 100)");
            stmt.execute("INSERT INTO items (id, name, price) VALUES (2, 'banana', 50)");
            stmt.execute("INSERT INTO items (id, name, price) VALUES (3, 'cherry', 200)");
            stmt.execute("INSERT INTO items (id, name, price) VALUES (4, 'date', 150)");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE price > 50 AND price < 200")) {
                assertTrue(rs.next());
                assertEquals("apple", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("date", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithOrCondition() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE id = 1 OR id = 3")) {
                assertTrue(rs.next());
                assertEquals("apple", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("cherry", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithInClause() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE id IN (1, 3)")) {
                assertTrue(rs.next());
                assertEquals("apple", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("cherry", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithNotIn() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE id NOT IN (1, 3)")) {
                assertTrue(rs.next());
                assertEquals("banana", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithBetween() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'a')");
            stmt.execute("INSERT INTO items (id, name) VALUES (5, 'b')");
            stmt.execute("INSERT INTO items (id, name) VALUES (10, 'c')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE id BETWEEN 2 AND 8")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithLike() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'apricot')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'banana')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items WHERE name LIKE 'ap%'")) {
                assertTrue(rs.next());
                assertEquals("apple", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("apricot", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithIsNull() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, null)");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items WHERE name IS NULL")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldFilterWithIsNotNull() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, null)");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items WHERE name IS NOT NULL")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- NULL semantics (three-valued logic) ----

    @Test
    void shouldReturnNoRowsForEqualsNull() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, null)");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'test')");

            // NULL = NULL should be NULL (falsy), so no rows match
            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items WHERE name = null")) {
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldHandleNullInArithmetic() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT null + 1")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    // ---- ORDER BY ----

    @Test
    void shouldOrderByAscending() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items ORDER BY id ASC")) {
                assertTrue(rs.next()); assertEquals("apple", rs.getString(1));
                assertTrue(rs.next()); assertEquals("banana", rs.getString(1));
                assertTrue(rs.next()); assertEquals("cherry", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByDescending() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items ORDER BY id DESC")) {
                assertTrue(rs.next()); assertEquals("cherry", rs.getString(1));
                assertTrue(rs.next()); assertEquals("banana", rs.getString(1));
                assertTrue(rs.next()); assertEquals("apple", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByMultipleColumns() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (category text, name text, price integer)");
            stmt.execute("INSERT INTO items (category, name, price) VALUES ('fruit', 'banana', 50)");
            stmt.execute("INSERT INTO items (category, name, price) VALUES ('fruit', 'apple', 100)");
            stmt.execute("INSERT INTO items (category, name, price) VALUES ('veg', 'carrot', 30)");

            try (ResultSet rs = stmt.executeQuery("SELECT name FROM items ORDER BY category ASC, price DESC")) {
                assertTrue(rs.next()); assertEquals("apple", rs.getString(1));
                assertTrue(rs.next()); assertEquals("banana", rs.getString(1));
                assertTrue(rs.next()); assertEquals("carrot", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByOrdinalPosition() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");

            // ORDER BY 2 means order by the second column in the SELECT list (name)
            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM items ORDER BY 2")) {
                assertTrue(rs.next()); assertEquals("apple", rs.getString("name"));
                assertTrue(rs.next()); assertEquals("banana", rs.getString("name"));
                assertTrue(rs.next()); assertEquals("cherry", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByColumnAlias() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'cherry')");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'apple')");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, name AS item_name FROM items ORDER BY item_name")) {
                assertTrue(rs.next()); assertEquals("apple", rs.getString("item_name"));
                assertTrue(rs.next()); assertEquals("banana", rs.getString("item_name"));
                assertTrue(rs.next()); assertEquals("cherry", rs.getString("item_name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByWithNullsLast() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, null)");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'apple')");

            // NULLS LAST means nulls come after non-null values
            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM items ORDER BY name ASC NULLS LAST")) {
                assertTrue(rs.next()); assertEquals("apple", rs.getString("name"));
                assertTrue(rs.next()); assertEquals("banana", rs.getString("name"));
                assertTrue(rs.next()); assertNull(rs.getObject("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldOrderByWithNullsFirst() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, null)");
            stmt.execute("INSERT INTO items (id, name) VALUES (2, 'banana')");
            stmt.execute("INSERT INTO items (id, name) VALUES (3, 'apple')");

            try (ResultSet rs = stmt.executeQuery("SELECT id, name FROM items ORDER BY name ASC NULLS FIRST")) {
                assertTrue(rs.next()); assertNull(rs.getObject("name"));
                assertTrue(rs.next()); assertEquals("apple", rs.getString("name"));
                assertTrue(rs.next()); assertEquals("banana", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    // ---- LIMIT / OFFSET ----

    @Test
    void shouldApplyLimit() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer)");
            stmt.execute("INSERT INTO items (id) VALUES (1)");
            stmt.execute("INSERT INTO items (id) VALUES (2)");
            stmt.execute("INSERT INTO items (id) VALUES (3)");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items ORDER BY id LIMIT 2")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldApplyOffset() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer)");
            stmt.execute("INSERT INTO items (id) VALUES (1)");
            stmt.execute("INSERT INTO items (id) VALUES (2)");
            stmt.execute("INSERT INTO items (id) VALUES (3)");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items ORDER BY id OFFSET 1")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldApplyLimitAndOffset() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer)");
            for (int i = 1; i <= 10; i++) {
                stmt.execute("INSERT INTO items (id) VALUES (" + i + ")");
            }

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items ORDER BY id LIMIT 3 OFFSET 2")) {
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- DISTINCT ----

    @Test
    void shouldApplyDistinct() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (category text, name text)");
            stmt.execute("INSERT INTO items (category, name) VALUES ('fruit', 'apple')");
            stmt.execute("INSERT INTO items (category, name) VALUES ('fruit', 'banana')");
            stmt.execute("INSERT INTO items (category, name) VALUES ('veg', 'carrot')");

            try (ResultSet rs = stmt.executeQuery("SELECT DISTINCT category FROM items ORDER BY category")) {
                assertTrue(rs.next()); assertEquals("fruit", rs.getString(1));
                assertTrue(rs.next()); assertEquals("veg", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Multi-table FROM (cross join) ----

    @Test
    void shouldSupportCrossJoinWithComma() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE colors (color text)");
            stmt.execute("CREATE TABLE sizes (size text)");
            stmt.execute("INSERT INTO colors (color) VALUES ('red')");
            stmt.execute("INSERT INTO colors (color) VALUES ('blue')");
            stmt.execute("INSERT INTO sizes (size) VALUES ('S')");
            stmt.execute("INSERT INTO sizes (size) VALUES ('L')");

            try (ResultSet rs = stmt.executeQuery("SELECT color, size FROM colors, sizes ORDER BY color, size")) {
                assertTrue(rs.next()); assertEquals("blue", rs.getString(1)); assertEquals("L", rs.getString(2));
                assertTrue(rs.next()); assertEquals("blue", rs.getString(1)); assertEquals("S", rs.getString(2));
                assertTrue(rs.next()); assertEquals("red", rs.getString(1)); assertEquals("L", rs.getString(2));
                assertTrue(rs.next()); assertEquals("red", rs.getString(1)); assertEquals("S", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldSupportCrossJoinWithWhereFilter() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE employees (id integer, name text, dept_id integer)");
            stmt.execute("CREATE TABLE departments (id integer, dept_name text)");
            stmt.execute("INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 10)");
            stmt.execute("INSERT INTO employees (id, name, dept_id) VALUES (2, 'Bob', 20)");
            stmt.execute("INSERT INTO departments (id, dept_name) VALUES (10, 'Engineering')");
            stmt.execute("INSERT INTO departments (id, dept_name) VALUES (20, 'Sales')");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT e.name, d.dept_name FROM employees e, departments d WHERE e.dept_id = d.id ORDER BY e.name")) {
                assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
                assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Sales", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void shouldSupportQualifiedColumnRefsWithAlias() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE users (id integer, name text)");
            stmt.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

            try (ResultSet rs = stmt.executeQuery("SELECT u.id, u.name FROM users u")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("Alice", rs.getString(2));
            }
        }
    }

    // ---- CASE in queries ----

    @Test
    void shouldEvaluateCaseInSelect() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, price integer)");
            stmt.execute("INSERT INTO items (id, price) VALUES (1, 50)");
            stmt.execute("INSERT INTO items (id, price) VALUES (2, 150)");
            stmt.execute("INSERT INTO items (id, price) VALUES (3, 300)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, CASE WHEN price < 100 THEN 'cheap' WHEN price < 200 THEN 'mid' ELSE 'expensive' END AS tier FROM items ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("cheap", rs.getString("tier"));
                assertTrue(rs.next()); assertEquals("mid", rs.getString("tier"));
                assertTrue(rs.next()); assertEquals("expensive", rs.getString("tier"));
                assertFalse(rs.next());
            }
        }
    }

    // ---- String functions ----

    @Test
    void shouldSupportStringFunctions() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT upper('hello')")) {
                assertTrue(rs.next());
                assertEquals("HELLO", rs.getString(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT lower('HELLO')")) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT length('hello')")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT trim('  hello  ')")) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }
        }
    }

    // ---- Math functions ----

    @Test
    void shouldSupportMathFunctions() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT abs(-42)")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT ceil(4.2)")) {
                assertTrue(rs.next());
                assertEquals(5.0, rs.getDouble(1), 0.001);
            }

            try (ResultSet rs = stmt.executeQuery("SELECT floor(4.8)")) {
                assertTrue(rs.next());
                assertEquals(4.0, rs.getDouble(1), 0.001);
            }
        }
    }

    // ---- Boolean expressions ----

    @Test
    void shouldSupportBooleanLiterals() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT true, false")) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString(1));
                assertEquals("f", rs.getString(2));
            }
        }
    }

    @Test
    void shouldSupportNotOperator() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, active boolean)");
            stmt.execute("INSERT INTO items (id, active) VALUES (1, true)");
            stmt.execute("INSERT INTO items (id, active) VALUES (2, false)");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM items WHERE NOT active")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Computed expressions in SELECT ----

    @Test
    void shouldSupportComputedExpressions() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, price integer, qty integer)");
            stmt.execute("INSERT INTO items (id, price, qty) VALUES (1, 100, 3)");
            stmt.execute("INSERT INTO items (id, price, qty) VALUES (2, 50, 10)");

            try (ResultSet rs = stmt.executeQuery("SELECT id, price * qty AS total FROM items ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(300, rs.getInt("total"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(500, rs.getInt("total"));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Greatest / Least ----

    @Test
    void shouldSupportGreatestAndLeast() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("SELECT greatest(1, 5, 3)")) {
                assertTrue(rs.next());
                assertEquals("5", rs.getString(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT least(1, 5, 3)")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
            }
        }
    }

    // ---- Schema-qualified table ----

    @Test
    void shouldSupportSchemaQualifiedTableRef() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (id integer, name text)");
            stmt.execute("INSERT INTO items (id, name) VALUES (1, 'test')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM public.items")) {
                assertTrue(rs.next());
                assertEquals("test", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Negative numbers ----

    @Test
    void shouldHandleNegativeNumbers() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT -5, -3 + 1")) {
            assertTrue(rs.next());
            assertEquals(-5, rs.getInt(1));
            assertEquals(-2, rs.getInt(2));
        }
    }
}
