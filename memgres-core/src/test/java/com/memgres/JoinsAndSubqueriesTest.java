package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3 integration tests: JOINs, Subqueries, and Set Operations.
 */
class JoinsAndSubqueriesTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() throws SQLException {
        memgres = Memgres.builder().port(0).build().start();
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE departments (id integer, name text)");
            stmt.execute("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");
            stmt.execute("INSERT INTO departments (id, name) VALUES (2, 'Sales')");
            stmt.execute("INSERT INTO departments (id, name) VALUES (3, 'Marketing')");

            stmt.execute("CREATE TABLE employees (id integer, name text, dept_id integer, salary integer)");
            stmt.execute("INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 90000)");
            stmt.execute("INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 80000)");
            stmt.execute("INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 70000)");
            stmt.execute("INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Diana', null, 60000)");
        }
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ---- INNER JOIN ----

    @Test
    void shouldInnerJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name, d.name AS dept FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Sales", rs.getString(2));
            assertFalse(rs.next()); // Diana has no dept_id, excluded by INNER JOIN
        }
    }

    @Test
    void shouldInnerJoinWithBareJoinKeyword() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- LEFT JOIN ----

    @Test
    void shouldLeftJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name, d.name AS dept FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Sales", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1)); assertNull(rs.getObject(2)); // LEFT JOIN keeps Diana
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldLeftOuterJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name FROM employees e LEFT OUTER JOIN departments d ON e.dept_id = d.id ORDER BY e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- RIGHT JOIN ----

    @Test
    void shouldRightJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name, d.name AS dept FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ORDER BY d.name, e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertNull(rs.getObject(1)); assertEquals("Marketing", rs.getString(2)); // No employees in Marketing
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Sales", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // ---- FULL JOIN ----

    @Test
    void shouldFullJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name, d.name AS dept FROM employees e FULL JOIN departments d ON e.dept_id = d.id ORDER BY e.name NULLS LAST")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Sales", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1)); assertNull(rs.getObject(2));
            assertTrue(rs.next()); assertNull(rs.getObject(1)); assertEquals("Marketing", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // ---- CROSS JOIN ----

    @Test
    void shouldCrossJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE colors (c text)");
            stmt.execute("INSERT INTO colors (c) VALUES ('R')");
            stmt.execute("INSERT INTO colors (c) VALUES ('G')");

            stmt.execute("CREATE TABLE sizes (s text)");
            stmt.execute("INSERT INTO sizes (s) VALUES ('S')");
            stmt.execute("INSERT INTO sizes (s) VALUES ('L')");

            try (ResultSet rs = stmt.executeQuery("SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s")) {
                assertTrue(rs.next()); assertEquals("G", rs.getString(1)); assertEquals("L", rs.getString(2));
                assertTrue(rs.next()); assertEquals("G", rs.getString(1)); assertEquals("S", rs.getString(2));
                assertTrue(rs.next()); assertEquals("R", rs.getString(1)); assertEquals("L", rs.getString(2));
                assertTrue(rs.next()); assertEquals("R", rs.getString(1)); assertEquals("S", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- JOIN with aggregation ----

    @Test
    void shouldJoinWithGroupBy() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT d.name, count(*) AS cnt, sum(e.salary) AS total " +
                             "FROM employees e INNER JOIN departments d ON e.dept_id = d.id " +
                             "GROUP BY d.name ORDER BY d.name")) {
            assertTrue(rs.next());
            assertEquals("Engineering", rs.getString(1));
            assertEquals(2, rs.getLong(2));
            assertEquals(170000, rs.getLong(3));

            assertTrue(rs.next());
            assertEquals("Sales", rs.getString(1));
            assertEquals(1, rs.getLong(2));
            assertEquals(70000, rs.getLong(3));

            assertFalse(rs.next());
        }
    }

    // ---- Multi-table JOIN ----

    @Test
    void shouldChainMultipleJoins() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE projects (id integer, name text, dept_id integer)");
            stmt.execute("INSERT INTO projects (id, name, dept_id) VALUES (1, 'Project X', 1)");
            stmt.execute("INSERT INTO projects (id, name, dept_id) VALUES (2, 'Project Y', 2)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT e.name, d.name AS dept, p.name AS project " +
                            "FROM employees e " +
                            "JOIN departments d ON e.dept_id = d.id " +
                            "JOIN projects p ON d.id = p.dept_id " +
                            "ORDER BY e.name")) {
                assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Project X", rs.getString(3));
                assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Project X", rs.getString(3));
                assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Project Y", rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    // ---- Subquery in FROM ----

    @Test
    void shouldSupportSubqueryInFrom() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT sub.name FROM (SELECT name FROM employees WHERE salary > 75000) sub ORDER BY sub.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldSupportSubqueryInFromWithAlias() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT s.total FROM (SELECT count(*) AS total FROM employees) s")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getLong(1));
        }
    }

    // ---- Scalar subquery ----

    @Test
    void shouldSupportScalarSubquery() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, (SELECT name FROM departments WHERE id = e.dept_id) AS dept " +
                             "FROM employees e WHERE dept_id IS NOT NULL ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Engineering", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals("Sales", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // ---- EXISTS ----

    @Test
    void shouldSupportExists() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) ORDER BY d.name")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next()); // Marketing has no employees
        }
    }

    @Test
    void shouldSupportNotExists() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)")) {
            assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- IN (subquery) ----

    @Test
    void shouldSupportInSubquery() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldSupportNotInSubquery() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertFalse(rs.next()); // Diana has NULL dept_id, NOT IN with NULL = NULL (excluded)
        }
    }

    // ---- UNION ----

    @Test
    void shouldSupportUnion() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM employees WHERE dept_id = 1 UNION SELECT name FROM employees WHERE dept_id = 2 ORDER BY 1")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldSupportUnionAllWithDuplicates() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT dept_id FROM employees WHERE dept_id = 1 UNION ALL SELECT dept_id FROM employees WHERE dept_id = 1 ORDER BY 1")) {
            // Should have 4 rows (2 from left + 2 from right, all with dept_id=1)
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void shouldSupportUnionRemovesDuplicates() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT dept_id FROM employees WHERE dept_id = 1 UNION SELECT dept_id FROM employees WHERE dept_id = 1")) {
            // UNION (without ALL) removes duplicates
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    // ---- INTERSECT ----

    @Test
    void shouldSupportIntersect() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM employees WHERE salary > 60000 INTERSECT SELECT name FROM employees WHERE dept_id = 1 ORDER BY 1")) {
            // salary > 60000: Alice, Bob, Charlie
            // dept_id = 1: Alice, Bob
            // Intersection: Alice, Bob
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- EXCEPT ----

    @Test
    void shouldSupportExcept() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM employees WHERE salary > 60000 EXCEPT SELECT name FROM employees WHERE dept_id = 1 ORDER BY 1")) {
            // salary > 60000: Alice, Bob, Charlie
            // dept_id = 1: Alice, Bob
            // Except: Charlie
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- Self JOIN ----

    @Test
    void shouldSupportSelfJoin() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tree (id integer, parent_id integer, name text)");
            stmt.execute("INSERT INTO tree (id, parent_id, name) VALUES (1, null, 'root')");
            stmt.execute("INSERT INTO tree (id, parent_id, name) VALUES (2, 1, 'child1')");
            stmt.execute("INSERT INTO tree (id, parent_id, name) VALUES (3, 1, 'child2')");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT c.name AS child, p.name AS parent FROM tree c JOIN tree p ON c.parent_id = p.id ORDER BY c.name")) {
                assertTrue(rs.next()); assertEquals("child1", rs.getString(1)); assertEquals("root", rs.getString(2));
                assertTrue(rs.next()); assertEquals("child2", rs.getString(1)); assertEquals("root", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    // ---- JOIN with WHERE ----

    @Test
    void shouldJoinWithWhereFilter() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT e.name FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.salary > 75000 ORDER BY e.name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ---- USING clause ----

    @Test
    void shouldSupportJoinUsing() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE a (id integer, val text)");
            stmt.execute("CREATE TABLE b (id integer, info text)");
            stmt.execute("INSERT INTO a (id, val) VALUES (1, 'x')");
            stmt.execute("INSERT INTO a (id, val) VALUES (2, 'y')");
            stmt.execute("INSERT INTO b (id, info) VALUES (1, 'p')");
            stmt.execute("INSERT INTO b (id, info) VALUES (3, 'q')");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT a.val, b.info FROM a JOIN b USING (id)")) {
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("p", rs.getString(2));
                assertFalse(rs.next()); // Only id=1 matches
            }
        }
    }

    // === NATURAL JOIN column deduplication ===
    // PG deduplicates common columns in SELECT * for NATURAL JOINs:
    // the join columns appear once (first), then remaining left cols, then remaining right cols.

    @Test
    void naturalJoin_selectStarDeduplicatesJoinColumns() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nat1 (id int, a text)");
            stmt.execute("CREATE TABLE nat2 (id int, b text)");
            stmt.execute("INSERT INTO nat1 VALUES (1, 'x')");
            stmt.execute("INSERT INTO nat2 VALUES (1, 'y')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM nat1 NATURAL JOIN nat2")) {
                ResultSetMetaData md = rs.getMetaData();
                // PG returns 3 columns: id, a, b (NOT id, a, id, b)
                assertEquals(3, md.getColumnCount(),
                        "NATURAL JOIN SELECT * should deduplicate the join column");
                assertEquals("id", md.getColumnName(1));
                assertEquals("a", md.getColumnName(2));
                assertEquals("b", md.getColumnName(3));
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("x", rs.getString(2));
                assertEquals("y", rs.getString(3));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void naturalLeftJoin_selectStarDeduplicatesJoinColumns() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nat3 (id int, a text)");
            stmt.execute("CREATE TABLE nat4 (id int, b text)");
            stmt.execute("INSERT INTO nat3 VALUES (1, 'x'), (2, 'z')");
            stmt.execute("INSERT INTO nat4 VALUES (1, 'y')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM nat3 NATURAL LEFT JOIN nat4 ORDER BY id")) {
                ResultSetMetaData md = rs.getMetaData();
                assertEquals(3, md.getColumnCount(),
                        "NATURAL LEFT JOIN SELECT * should deduplicate the join column");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("x", rs.getString("a"));
                assertEquals("y", rs.getString("b"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("z", rs.getString("a"));
                assertNull(rs.getString("b"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void naturalJoin_multipleCommonColumns() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE nat5 (id int, code text, a text)");
            stmt.execute("CREATE TABLE nat6 (id int, code text, b text)");
            stmt.execute("INSERT INTO nat5 VALUES (1, 'X', 'left')");
            stmt.execute("INSERT INTO nat6 VALUES (1, 'X', 'right')");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM nat5 NATURAL JOIN nat6")) {
                ResultSetMetaData md = rs.getMetaData();
                // PG returns 4 columns: id, code, a, b (NOT id, code, a, id, code, b)
                assertEquals(4, md.getColumnCount(),
                        "NATURAL JOIN with 2 common columns should deduplicate both");
                assertEquals("id", md.getColumnName(1));
                assertEquals("code", md.getColumnName(2));
                assertEquals("a", md.getColumnName(3));
                assertEquals("b", md.getColumnName(4));
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("X", rs.getString(2));
                assertEquals("left", rs.getString(3));
                assertEquals("right", rs.getString(4));
            }
        }
    }
}
