package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Obscure PostgreSQL compatibility tests covering patterns that typically trip up
 * PG-compatible implementations. Covers cross-schema queries, edge-case SQL
 * syntax, operator precedence, empty strings vs NULL, DISTINCT ON,
 * GROUPING SETS, FILTER on aggregates, VALUES as query, correlated subqueries,
 * self-joins, temp tables, and more.
 */
class ObscurePostgresTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ================================================================
    //  1. CROSS-SCHEMA QUERIES
    // ================================================================

    @Test
    void testCreateSchemaAndTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA test_schema");
            s.execute("CREATE TABLE test_schema.items (id INTEGER, name TEXT)");
            s.execute("INSERT INTO test_schema.items VALUES (1, 'widget'), (2, 'gadget')");
            ResultSet rs = s.executeQuery("SELECT name FROM test_schema.items ORDER BY id");
            assertTrue(rs.next()); assertEquals("widget", rs.getString(1));
            assertTrue(rs.next()); assertEquals("gadget", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE test_schema.items");
        }
    }

    @Test
    void testCrossSchemaJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA schema_a");
            s.execute("CREATE SCHEMA schema_b");
            s.execute("CREATE TABLE schema_a.users (id INTEGER, name TEXT)");
            s.execute("CREATE TABLE schema_b.orders (id INTEGER, user_id INTEGER, amount INTEGER)");
            s.execute("INSERT INTO schema_a.users VALUES (1, 'Alice'), (2, 'Bob')");
            s.execute("INSERT INTO schema_b.orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)");
            ResultSet rs = s.executeQuery(
                    "SELECT u.name, SUM(o.amount) AS total " +
                    "FROM schema_a.users u JOIN schema_b.orders o ON u.id = o.user_id " +
                    "GROUP BY u.name ORDER BY u.name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(300, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(50, rs.getInt(2));
            s.execute("DROP TABLE schema_a.users");
            s.execute("DROP TABLE schema_b.orders");
        }
    }

    @Test
    void testSameTableNameDifferentSchemas() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA ns1");
            s.execute("CREATE SCHEMA ns2");
            s.execute("CREATE TABLE ns1.data (val TEXT)");
            s.execute("CREATE TABLE ns2.data (val TEXT)");
            s.execute("INSERT INTO ns1.data VALUES ('from_ns1')");
            s.execute("INSERT INTO ns2.data VALUES ('from_ns2')");
            ResultSet rs1 = s.executeQuery("SELECT val FROM ns1.data");
            assertTrue(rs1.next()); assertEquals("from_ns1", rs1.getString(1));
            ResultSet rs2 = s.executeQuery("SELECT val FROM ns2.data");
            assertTrue(rs2.next()); assertEquals("from_ns2", rs2.getString(1));
            s.execute("DROP TABLE ns1.data");
            s.execute("DROP TABLE ns2.data");
        }
    }

    // ================================================================
    //  2. DISTINCT ON (PostgreSQL extension)
    // ================================================================

    @Test
    void testDistinctOn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dist_on (dept TEXT, emp TEXT, salary INTEGER)");
            s.execute("INSERT INTO dist_on VALUES ('A','x',100),('A','y',200),('A','z',150),('B','p',300),('B','q',250)");
            ResultSet rs = s.executeQuery(
                    "SELECT DISTINCT ON (dept) dept, emp, salary FROM dist_on ORDER BY dept, salary DESC");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals("y", rs.getString(2)); assertEquals(200, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals("p", rs.getString(2)); assertEquals(300, rs.getInt(3));
            assertFalse(rs.next());
            s.execute("DROP TABLE dist_on");
        }
    }

    // ================================================================
    //  3. VALUES as standalone query
    // ================================================================

    @Test
    void testValuesAsQuery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("b", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("c", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void testValuesInSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(num, word) WHERE num > 1 ORDER BY num");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("two", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("three", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // ================================================================
    //  4. HAVING without GROUP BY
    // ================================================================

    @Test
    void testHavingWithoutGroupBy() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE having_test (val INTEGER)");
            s.execute("INSERT INTO having_test VALUES (10),(20),(30),(40),(50)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM having_test HAVING COUNT(*) > 3");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            s.execute("DROP TABLE having_test");
        }
    }

    @Test
    void testHavingWithoutGroupByNoMatch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE having_test2 (val INTEGER)");
            s.execute("INSERT INTO having_test2 VALUES (10),(20)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM having_test2 HAVING COUNT(*) > 10");
            assertFalse(rs.next(), "HAVING filter should eliminate the single group");
            s.execute("DROP TABLE having_test2");
        }
    }

    // ================================================================
    //  5. ORDER BY expressions not in SELECT list
    // ================================================================

    @Test
    void testOrderByExpressionNotInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE oby_expr (name TEXT, salary INTEGER)");
            s.execute("INSERT INTO oby_expr VALUES ('Alice',300),('Bob',100),('Carol',200)");
            ResultSet rs = s.executeQuery("SELECT name FROM oby_expr ORDER BY salary");
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Carol", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            s.execute("DROP TABLE oby_expr");
        }
    }

    @Test
    void testOrderByFunctionNotInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE oby_fn (name TEXT)");
            s.execute("INSERT INTO oby_fn VALUES ('zz'),('aaaa'),('bbb')");
            ResultSet rs = s.executeQuery("SELECT name FROM oby_fn ORDER BY length(name)");
            assertTrue(rs.next()); assertEquals("zz", rs.getString(1));
            assertTrue(rs.next()); assertEquals("bbb", rs.getString(1));
            assertTrue(rs.next()); assertEquals("aaaa", rs.getString(1));
            s.execute("DROP TABLE oby_fn");
        }
    }

    // ================================================================
    //  6. LIMIT/OFFSET edge cases
    // ================================================================

    @Test
    void testLimitZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lim0 (id INTEGER)");
            s.execute("INSERT INTO lim0 VALUES (1),(2),(3)");
            ResultSet rs = s.executeQuery("SELECT * FROM lim0 LIMIT 0");
            assertFalse(rs.next(), "LIMIT 0 should return no rows");
            s.execute("DROP TABLE lim0");
        }
    }

    @Test
    void testOffsetBeyondRows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE off_test (id INTEGER)");
            s.execute("INSERT INTO off_test VALUES (1),(2),(3)");
            ResultSet rs = s.executeQuery("SELECT * FROM off_test OFFSET 100");
            assertFalse(rs.next(), "OFFSET beyond rows should return empty");
            s.execute("DROP TABLE off_test");
        }
    }

    @Test
    void testLimitAllEquivalent() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lim_all (id INTEGER)");
            s.execute("INSERT INTO lim_all VALUES (1),(2),(3)");
            // LIMIT ALL is equivalent to no limit
            ResultSet rs = s.executeQuery("SELECT * FROM lim_all ORDER BY id LIMIT ALL");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(3, count, "LIMIT ALL should return all rows");
            s.execute("DROP TABLE lim_all");
        }
    }

    // ================================================================
    //  7. GROUP BY expressions and ordinals
    // ================================================================

    @Test
    void testGroupByOrdinal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE gb_ord (dept TEXT, val INTEGER)");
            s.execute("INSERT INTO gb_ord VALUES ('A',10),('A',20),('B',30)");
            ResultSet rs = s.executeQuery("SELECT dept, SUM(val) FROM gb_ord GROUP BY 1 ORDER BY 1");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals(30, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals(30, rs.getInt(2));
            s.execute("DROP TABLE gb_ord");
        }
    }

    @Test
    void testGroupByExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE gb_expr (name TEXT, val INTEGER)");
            s.execute("INSERT INTO gb_expr VALUES ('Alice',10),('alice',20),('BOB',30)");
            ResultSet rs = s.executeQuery("SELECT UPPER(name), SUM(val) FROM gb_expr GROUP BY UPPER(name) ORDER BY 1");
            assertTrue(rs.next()); assertEquals("ALICE", rs.getString(1)); assertEquals(30, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("BOB", rs.getString(1)); assertEquals(30, rs.getInt(2));
            s.execute("DROP TABLE gb_expr");
        }
    }

    // ================================================================
    //  8. Empty string vs NULL semantics
    // ================================================================

    @Test
    void testEmptyStringIsNotNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '' IS NULL");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1), "Empty string is NOT NULL in PostgreSQL");
        }
    }

    @Test
    void testEmptyStringLength() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT length('')");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void testEmptyStringConcat() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '' || 'hello'");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
        }
    }

    @Test
    void testEmptyStringCoalesce() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT COALESCE('', 'fallback')");
            assertTrue(rs.next());
            assertEquals("", rs.getString(1), "COALESCE should return '' not 'fallback'");
        }
    }

    @Test
    void testNullConcatReturnsNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'hello' || NULL");
            assertTrue(rs.next());
            assertNull(rs.getString(1), "Concatenation with NULL should return NULL");
        }
    }

    // ================================================================
    //  9. Dollar-quoted strings
    // ================================================================

    @Test
    void testDollarQuotedString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT $$hello world$$");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
        }
    }

    @Test
    void testDollarQuotedWithTag() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT $tag$it's \"easy\"$tag$");
            assertTrue(rs.next());
            assertEquals("it's \"easy\"", rs.getString(1));
        }
    }

    @Test
    void testDollarQuotedMultiline() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT $fn$\nmulti\nline\n$fn$");
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains("multi"));
            assertTrue(rs.getString(1).contains("line"));
        }
    }

    // ================================================================
    //  10. Self-joins and expression joins
    // ================================================================

    @Test
    void testSelfJoinManager() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE emps (id INTEGER, name TEXT, manager_id INTEGER)");
            s.execute("INSERT INTO emps VALUES (1,'Alice',NULL),(2,'Bob',1),(3,'Carol',1),(4,'Dave',2)");
            ResultSet rs = s.executeQuery(
                    "SELECT e.name AS employee, m.name AS manager " +
                    "FROM emps e LEFT JOIN emps m ON e.manager_id = m.id ORDER BY e.name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertNull(rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Carol", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Dave", rs.getString(1)); assertEquals("Bob", rs.getString(2));
            s.execute("DROP TABLE emps");
        }
    }

    @Test
    void testJoinOnExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_join1 (name TEXT)");
            s.execute("CREATE TABLE t_join2 (name TEXT)");
            s.execute("INSERT INTO t_join1 VALUES ('Alice'),('Bob')");
            s.execute("INSERT INTO t_join2 VALUES ('alice'),('CAROL')");
            ResultSet rs = s.executeQuery(
                    "SELECT a.name, b.name FROM t_join1 a JOIN t_join2 b ON LOWER(a.name) = LOWER(b.name)");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals("alice", rs.getString(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE t_join1");
            s.execute("DROP TABLE t_join2");
        }
    }

    // ================================================================
    //  11. Correlated subqueries in SELECT list
    // ================================================================

    @Test
    void testCorrelatedSubqueryInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE depts (id INTEGER, name TEXT)");
            s.execute("CREATE TABLE emp_corr (id INTEGER, dept_id INTEGER, salary INTEGER)");
            s.execute("INSERT INTO depts VALUES (1,'Engineering'),(2,'Sales')");
            s.execute("INSERT INTO emp_corr VALUES (1,1,100),(2,1,200),(3,2,50)");
            ResultSet rs = s.executeQuery(
                    "SELECT d.name, " +
                    "(SELECT AVG(e.salary) FROM emp_corr e WHERE e.dept_id = d.id) AS avg_sal " +
                    "FROM depts d ORDER BY d.name");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(150, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(50, rs.getInt(2));
            s.execute("DROP TABLE emp_corr");
            s.execute("DROP TABLE depts");
        }
    }

    // ================================================================
    //  12. FILTER clause on aggregates
    // ================================================================

    @Test
    void testFilterOnCount() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE filt (status TEXT, amount INTEGER)");
            s.execute("INSERT INTO filt VALUES ('active',100),('active',200),('inactive',50),('active',75)");
            ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) AS total, " +
                    "COUNT(*) FILTER (WHERE status = 'active') AS active_count " +
                    "FROM filt");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            s.execute("DROP TABLE filt");
        }
    }

    @Test
    void testFilterOnSum() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE filt2 (cat TEXT, val INTEGER)");
            s.execute("INSERT INTO filt2 VALUES ('a',10),('b',20),('a',30),('b',40)");
            ResultSet rs = s.executeQuery(
                    "SELECT SUM(val) AS total, " +
                    "SUM(val) FILTER (WHERE cat = 'a') AS sum_a " +
                    "FROM filt2");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
            assertEquals(40, rs.getInt(2));
            s.execute("DROP TABLE filt2");
        }
    }

    // ================================================================
    //  13. INSERT ... SELECT
    // ================================================================

    @Test
    void testInsertSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE src (id INTEGER, val TEXT)");
            s.execute("CREATE TABLE dst (id INTEGER, val TEXT)");
            s.execute("INSERT INTO src VALUES (1,'a'),(2,'b'),(3,'c')");
            s.execute("INSERT INTO dst SELECT * FROM src WHERE id > 1");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM dst");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE src");
            s.execute("DROP TABLE dst");
        }
    }

    @Test
    void testInsertSelectWithTransform() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE src2 (name TEXT)");
            s.execute("CREATE TABLE dst2 (name TEXT)");
            s.execute("INSERT INTO src2 VALUES ('Alice'),('Bob')");
            s.execute("INSERT INTO dst2 SELECT UPPER(name) FROM src2");
            ResultSet rs = s.executeQuery("SELECT name FROM dst2 ORDER BY name");
            assertTrue(rs.next()); assertEquals("ALICE", rs.getString(1));
            assertTrue(rs.next()); assertEquals("BOB", rs.getString(1));
            s.execute("DROP TABLE src2");
            s.execute("DROP TABLE dst2");
        }
    }

    // ================================================================
    //  14. DELETE ... USING
    // ================================================================

    @Test
    void testDeleteUsing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_main (id INTEGER, val TEXT)");
            s.execute("CREATE TABLE del_filter (id INTEGER)");
            s.execute("INSERT INTO del_main VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
            s.execute("INSERT INTO del_filter VALUES (2),(4)");
            s.execute("DELETE FROM del_main USING del_filter WHERE del_main.id = del_filter.id");
            ResultSet rs = s.executeQuery("SELECT id FROM del_main ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE del_main");
            s.execute("DROP TABLE del_filter");
        }
    }

    // ================================================================
    //  15. Nested subqueries
    // ================================================================

    @Test
    void testDeeplyNestedSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 42 AS val) sub1) sub2) sub3");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void testSubqueryInWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE outer_t (id INTEGER, val TEXT)");
            s.execute("CREATE TABLE inner_t (outer_id INTEGER, score INTEGER)");
            s.execute("INSERT INTO outer_t VALUES (1,'a'),(2,'b'),(3,'c')");
            s.execute("INSERT INTO inner_t VALUES (1,100),(1,200),(2,50)");
            ResultSet rs = s.executeQuery(
                    "SELECT val FROM outer_t WHERE id IN (SELECT outer_id FROM inner_t WHERE score > 80) ORDER BY val");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE outer_t");
            s.execute("DROP TABLE inner_t");
        }
    }

    // ================================================================
    //  16. Temp tables
    // ================================================================

    @Test
    void testTempTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TEMP TABLE scratch (id INTEGER, val TEXT)");
            s.execute("INSERT INTO scratch VALUES (1,'temp_data')");
            ResultSet rs = s.executeQuery("SELECT val FROM scratch");
            assertTrue(rs.next());
            assertEquals("temp_data", rs.getString(1));
            s.execute("DROP TABLE scratch");
        }
    }

    @Test
    void testTemporaryTableKeyword() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TEMPORARY TABLE scratch2 (id INTEGER)");
            s.execute("INSERT INTO scratch2 VALUES (42)");
            ResultSet rs = s.executeQuery("SELECT id FROM scratch2");
            assertTrue(rs.next()); assertEquals(42, rs.getInt(1));
            s.execute("DROP TABLE scratch2");
        }
    }

    // ================================================================
    //  17. Row constructor comparisons
    // ================================================================

    @Test
    void testRowComparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ROW(1, 2) = ROW(1, 2)");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    @Test
    void testRowComparisonNotEqual() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ROW(1, 2) = ROW(1, 3)");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1));
        }
    }

    // ================================================================
    //  18. Set operations combined
    // ================================================================

    @Test
    void testUnionExcept() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "(SELECT 1 AS n UNION SELECT 2 UNION SELECT 3) EXCEPT SELECT 2");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(2, count, "Should have 1 and 3");
        }
    }

    @Test
    void testUnionAllVsUnion() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs1 = s.executeQuery("SELECT 1 UNION ALL SELECT 1");
            int count1 = 0;
            while (rs1.next()) count1++;
            assertEquals(2, count1, "UNION ALL keeps duplicates");

            ResultSet rs2 = s.executeQuery("SELECT 1 UNION SELECT 1");
            int count2 = 0;
            while (rs2.next()) count2++;
            assertEquals(1, count2, "UNION removes duplicates");
        }
    }

    @Test
    void testIntersect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 INTERSECT SELECT 1");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            ResultSet rs2 = s.executeQuery("SELECT 1 INTERSECT SELECT 2");
            assertFalse(rs2.next(), "No common rows");
        }
    }

    // ================================================================
    //  19. Multiple CTEs referencing each other
    // ================================================================

    @Test
    void testMultipleCtesCascading() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "WITH " +
                    "  base AS (SELECT 10 AS val), " +
                    "  doubled AS (SELECT val * 2 AS val FROM base), " +
                    "  tripled AS (SELECT val * 3 AS val FROM base) " +
                    "SELECT d.val AS doubled, t.val AS tripled FROM doubled d, tripled t");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            assertEquals(30, rs.getInt(2));
        }
    }

    // ================================================================
    //  20. CASE expression edge cases
    // ================================================================

    @Test
    void testCaseWithNullComparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT CASE NULL WHEN NULL THEN 'match' ELSE 'no match' END");
            assertTrue(rs.next());
            // NULL = NULL is NULL (falsy), so it should NOT match
            assertEquals("no match", rs.getString(1));
        }
    }

    @Test
    void testSearchedCaseWithNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT CASE WHEN NULL THEN 'true' WHEN TRUE THEN 'yes' ELSE 'no' END");
            assertTrue(rs.next());
            assertEquals("yes", rs.getString(1));
        }
    }

    @Test
    void testCaseNoElse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT CASE WHEN FALSE THEN 'x' END");
            assertTrue(rs.next());
            assertNull(rs.getString(1), "CASE without ELSE should return NULL when no match");
        }
    }

    // ================================================================
    //  21. Casting edge cases
    // ================================================================

    @Test
    void testCastBooleanToInteger() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE::integer, FALSE::integer");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        }
    }

    @Test
    void testCastIntegerToBoolean() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1::boolean, 0::boolean");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("f", rs.getString(2));
        }
    }

    @Test
    void testCastNullPreservesNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULL::integer, NULL::text, NULL::boolean");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(3));
        }
    }

    // ================================================================
    //  22. Aggregate with no rows
    // ================================================================

    @Test
    void testAggregateOnEmptyTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE empty_agg (val INTEGER)");
            ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM empty_agg");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1)); // COUNT returns 0
            assertNull(rs.getObject(2));    // SUM returns NULL
            assertNull(rs.getObject(3));    // AVG returns NULL
            assertNull(rs.getObject(4));    // MIN returns NULL
            assertNull(rs.getObject(5));    // MAX returns NULL
            s.execute("DROP TABLE empty_agg");
        }
    }

    // ================================================================
    //  23. Type coercion edge cases
    // ================================================================

    @Test
    void testIntegerDivisionTruncates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7 / 2");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1), "Integer division should truncate");
        }
    }

    @Test
    void testDecimalDivision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 7.0 / 2");
            assertTrue(rs.next());
            assertEquals(3.5, rs.getDouble(1), 0.001, "Decimal division should preserve fraction");
        }
    }

    @Test
    void testImplicitStringToNumberInArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // PG allows: '5' + 3 (if one side is numeric, implicit coercion)
            // But memgres may handle this differently
            ResultSet rs = s.executeQuery("SELECT '5'::integer + 3");
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1));
        }
    }

    // ================================================================
    //  24. NULL comparison semantics
    // ================================================================

    @Test
    void testNullEquality() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULL = NULL");
            assertTrue(rs.next());
            assertNull(rs.getObject(1), "NULL = NULL should be NULL, not TRUE");
        }
    }

    @Test
    void testNullInArithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 + NULL, NULL * 5, NULL - NULL");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(3));
        }
    }

    @Test
    void testNullInComparisons() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULL > 5, NULL < 5, NULL = 5, NULL <> 5");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
            assertNull(rs.getObject(3));
            assertNull(rs.getObject(4));
        }
    }

    @Test
    void testNullNotIn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // NOT IN with NULL is tricky: x NOT IN (1, NULL) is NULL if x != 1
            s.execute("CREATE TABLE null_in (val INTEGER)");
            s.execute("INSERT INTO null_in VALUES (1),(2),(3)");
            ResultSet rs = s.executeQuery("SELECT val FROM null_in WHERE val NOT IN (1, NULL)");
            // Since NOT IN with NULL can never be TRUE for non-matching values,
            // this should return 0 rows (PG behavior)
            assertFalse(rs.next(), "NOT IN with NULL should return no rows (NULL makes everything unknown)");
            s.execute("DROP TABLE null_in");
        }
    }

    // ================================================================
    //  25. Multi-column IN
    // ================================================================

    @Test
    void testMultiColumnUpdate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE multi_upd (id INTEGER, a TEXT, b TEXT)");
            s.execute("INSERT INTO multi_upd VALUES (1,'x','y'),(2,'m','n')");
            s.execute("UPDATE multi_upd SET a = 'updated_a', b = 'updated_b' WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT a, b FROM multi_upd WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("updated_a", rs.getString(1));
            assertEquals("updated_b", rs.getString(2));
            s.execute("DROP TABLE multi_upd");
        }
    }

    // ================================================================
    //  26. Complex CASE in WHERE clause
    // ================================================================

    @Test
    void testCaseInWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE case_where (id INTEGER, status TEXT, priority INTEGER)");
            s.execute("INSERT INTO case_where VALUES (1,'active',1),(2,'inactive',2),(3,'active',3)");
            ResultSet rs = s.executeQuery(
                    "SELECT id FROM case_where WHERE " +
                    "CASE WHEN status = 'active' THEN priority > 1 ELSE FALSE END ORDER BY id");
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE case_where");
        }
    }

    // ================================================================
    //  27. Aliased expressions in ORDER BY
    // ================================================================

    @Test
    void testOrderByAlias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE oby_alias (first_name TEXT, last_name TEXT)");
            s.execute("INSERT INTO oby_alias VALUES ('John','Doe'),('Jane','Smith'),('John','Adams')");
            ResultSet rs = s.executeQuery(
                    "SELECT first_name || ' ' || last_name AS full_name FROM oby_alias ORDER BY full_name");
            assertTrue(rs.next()); assertEquals("Jane Smith", rs.getString(1));
            assertTrue(rs.next()); assertEquals("John Adams", rs.getString(1));
            assertTrue(rs.next()); assertEquals("John Doe", rs.getString(1));
            s.execute("DROP TABLE oby_alias");
        }
    }

    // ================================================================
    //  28. COALESCE with multiple arguments
    // ================================================================

    @Test
    void testCoalesceChain() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT COALESCE(NULL, NULL, NULL, 'found', 'ignored')");
            assertTrue(rs.next());
            assertEquals("found", rs.getString(1));
        }
    }

    @Test
    void testCoalesceAllNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT COALESCE(NULL, NULL, NULL)");
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    // ================================================================
    //  29. NULLIF
    // ================================================================

    @Test
    void testNullIfMatch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULLIF(5, 5)");
            assertTrue(rs.next());
            assertNull(rs.getObject(1), "NULLIF should return NULL when args are equal");
        }
    }

    @Test
    void testNullIfNoMatch() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULLIF(5, 3)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    // ================================================================
    //  30. GREATEST and LEAST
    // ================================================================

    @Test
    void testGreatestWithNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT GREATEST(1, NULL, 3, 2)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testLeastWithNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT LEAST(5, NULL, 2, 8)");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // ================================================================
    //  31. Boolean expressions as values
    // ================================================================

    @Test
    void testBooleanExpressionsInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 > 0, 1 < 0, 1 = 1, 1 <> 1");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("f", rs.getString(2));
            assertEquals("t", rs.getString(3));
            assertEquals("f", rs.getString(4));
        }
    }

    // ================================================================
    //  32. Complex WHERE with mixed AND/OR/NOT
    // ================================================================

    @Test
    void testComplexBooleanWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bool_where (a INTEGER, b INTEGER, c INTEGER)");
            s.execute("INSERT INTO bool_where VALUES (1,2,3),(4,5,6),(7,8,9),(10,11,12)");
            // (a > 3 AND b < 10) OR (c = 9)
            ResultSet rs = s.executeQuery(
                    "SELECT a FROM bool_where WHERE (a > 3 AND b < 10) OR c = 9 ORDER BY a");
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(7, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE bool_where");
        }
    }

    // ================================================================
    //  33. Multiple aggregates in same query
    // ================================================================

    @Test
    void testMultipleAggregates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE multi_agg (cat TEXT, val INTEGER)");
            s.execute("INSERT INTO multi_agg VALUES ('a',10),('a',20),('b',5),('b',15),('b',25)");
            ResultSet rs = s.executeQuery(
                    "SELECT cat, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) " +
                    "FROM multi_agg GROUP BY cat ORDER BY cat");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(30, rs.getInt(3));
            assertEquals(15.0, rs.getDouble(4), 0.01);
            assertEquals(10, rs.getInt(5));
            assertEquals(20, rs.getInt(6));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            s.execute("DROP TABLE multi_agg");
        }
    }

    // ================================================================
    //  34. Subquery in FROM with aggregation
    // ================================================================

    @Test
    void testSubqueryAggregation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE sq_agg (grp TEXT, val INTEGER)");
            s.execute("INSERT INTO sq_agg VALUES ('a',10),('a',20),('b',30)");
            ResultSet rs = s.executeQuery(
                    "SELECT MAX(total) FROM (SELECT SUM(val) AS total FROM sq_agg GROUP BY grp) sub");
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            s.execute("DROP TABLE sq_agg");
        }
    }

    // ================================================================
    //  35. UPDATE with FROM clause (multi-table update)
    // ================================================================

    @Test
    void testUpdateFrom() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_main (id INTEGER, val TEXT)");
            s.execute("CREATE TABLE upd_lookup (id INTEGER, new_val TEXT)");
            s.execute("INSERT INTO upd_main VALUES (1,'old1'),(2,'old2'),(3,'old3')");
            s.execute("INSERT INTO upd_lookup VALUES (1,'new1'),(3,'new3')");
            s.execute("UPDATE upd_main SET val = upd_lookup.new_val FROM upd_lookup WHERE upd_main.id = upd_lookup.id");
            ResultSet rs = s.executeQuery("SELECT id, val FROM upd_main ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("new1", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("old2", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("new3", rs.getString(2));
            s.execute("DROP TABLE upd_main");
            s.execute("DROP TABLE upd_lookup");
        }
    }

    // ================================================================
    //  36. EXISTS with empty and non-empty results
    // ================================================================

    @Test
    void testExistsTrue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXISTS(SELECT 1)");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    @Test
    void testExistsFalse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT EXISTS(SELECT 1 WHERE FALSE)");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1));
        }
    }

    // ================================================================
    //  37. IS DISTINCT FROM
    // ================================================================

    @Test
    void testIsDistinctFrom() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // IS DISTINCT FROM treats NULL as a comparable value
            ResultSet rs = s.executeQuery(
                    "SELECT NULL IS DISTINCT FROM NULL, " +
                    "NULL IS DISTINCT FROM 1, " +
                    "1 IS DISTINCT FROM 1");
            assertTrue(rs.next());
            assertEquals("f", rs.getString(1)); // NULL IS DISTINCT FROM NULL = false
            assertEquals("t", rs.getString(2)); // NULL IS DISTINCT FROM 1 = true
            assertEquals("f", rs.getString(3)); // 1 IS DISTINCT FROM 1 = false
        }
    }

    // ================================================================
    //  38. String functions on edge cases
    // ================================================================

    @Test
    void testRepeatZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT repeat('x', 0)");
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
        }
    }

    @Test
    void testLeftRight() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT left('hello', 3), right('hello', 3)");
            assertTrue(rs.next());
            assertEquals("hel", rs.getString(1));
            assertEquals("llo", rs.getString(2));
        }
    }

    @Test
    void testReverseString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT reverse('abcde')");
            assertTrue(rs.next());
            assertEquals("edcba", rs.getString(1));
        }
    }

    @Test
    void testInitcap() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT initcap('hello world foo')");
            assertTrue(rs.next());
            assertEquals("Hello World Foo", rs.getString(1));
        }
    }

    // ================================================================
    //  39. generate_series with aliases used in expressions
    // ================================================================

    @Test
    void testGenerateSeriesAlias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT n, n * n AS square, n * n * n AS cube FROM generate_series(1, 4) AS n ORDER BY n");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(1, rs.getInt(2)); assertEquals(1, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(4, rs.getInt(2)); assertEquals(8, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(9, rs.getInt(2)); assertEquals(27, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1)); assertEquals(16, rs.getInt(2)); assertEquals(64, rs.getInt(3));
        }
    }

    // ================================================================
    //  40. BETWEEN with various types
    // ================================================================

    @Test
    void testBetweenWithText() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE btw_txt (name TEXT)");
            s.execute("INSERT INTO btw_txt VALUES ('alice'),('bob'),('carol'),('dave')");
            ResultSet rs = s.executeQuery("SELECT name FROM btw_txt WHERE name BETWEEN 'b' AND 'd' ORDER BY name");
            assertTrue(rs.next()); assertEquals("bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("carol", rs.getString(1));
            assertFalse(rs.next()); // 'dave' = 'd' but 'dave' > 'd'
            s.execute("DROP TABLE btw_txt");
        }
    }

    @Test
    void testNotBetween() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 5 NOT BETWEEN 1 AND 3");
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
        }
    }

    // ================================================================
    //  41. LIKE pattern edge cases
    // ================================================================

    @Test
    void testLikeEscapePercent() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE like_esc (val TEXT)");
            s.execute("INSERT INTO like_esc VALUES ('100%'),('100'),('100% done')");
            // Match literal % using escape
            ResultSet rs = s.executeQuery("SELECT val FROM like_esc WHERE val LIKE '%!%%' ESCAPE '!'");
            int count = 0;
            while (rs.next()) {
                assertTrue(rs.getString(1).contains("%"));
                count++;
            }
            assertEquals(2, count, "Should match '100%' and '100% done'");
            s.execute("DROP TABLE like_esc");
        }
    }

    @Test
    void testLikeUnderscore() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE like_us (val TEXT)");
            s.execute("INSERT INTO like_us VALUES ('abc'),('axc'),('ac'),('abbc')");
            ResultSet rs = s.executeQuery("SELECT val FROM like_us WHERE val LIKE 'a_c' ORDER BY val");
            assertTrue(rs.next()); assertEquals("abc", rs.getString(1));
            assertTrue(rs.next()); assertEquals("axc", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE like_us");
        }
    }

    // ================================================================
    //  42. UNION with different column names (takes first query's names)
    // ================================================================

    @Test
    void testUnionColumnNames() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 AS alpha UNION ALL SELECT 2 AS beta");
            assertEquals("alpha", rs.getMetaData().getColumnLabel(1),
                    "UNION should use first query's column names");
        }
    }

    // ================================================================
    //  43. Multiple columns with same alias
    // ================================================================

    @Test
    void testDuplicateAliases() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // PG allows duplicate aliases; it just makes them ambiguous to reference
            ResultSet rs = s.executeQuery("SELECT 1 AS x, 2 AS x");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
        }
    }

    // ================================================================
    //  44. Complex recursive CTE
    // ================================================================

    @Test
    void testRecursiveCteFactorial() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE fact(n, f) AS (" +
                    "  SELECT 1, 1 " +
                    "  UNION ALL " +
                    "  SELECT n + 1, f * (n + 1) FROM fact WHERE n < 7" +
                    ") SELECT n, f FROM fact ORDER BY n");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(6, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1)); assertEquals(24, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(5, rs.getInt(1)); assertEquals(120, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(6, rs.getInt(1)); assertEquals(720, rs.getInt(2));
            assertTrue(rs.next()); assertEquals(7, rs.getInt(1)); assertEquals(5040, rs.getInt(2));
        }
    }

    @Test
    void testRecursiveCteTreeTraversal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tree (id INTEGER, parent_id INTEGER, name TEXT)");
            s.execute("INSERT INTO tree VALUES (1,NULL,'root'),(2,1,'child1'),(3,1,'child2'),(4,2,'grandchild1'),(5,3,'grandchild2')");
            ResultSet rs = s.executeQuery(
                    "WITH RECURSIVE tree_path AS (" +
                    "  SELECT id, name, 0 AS depth FROM tree WHERE parent_id IS NULL " +
                    "  UNION ALL " +
                    "  SELECT t.id, t.name, tp.depth + 1 FROM tree t JOIN tree_path tp ON t.parent_id = tp.id" +
                    ") SELECT name, depth FROM tree_path ORDER BY depth, name");
            assertTrue(rs.next()); assertEquals("root", rs.getString(1)); assertEquals(0, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("child1", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("child2", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("grandchild1", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("grandchild2", rs.getString(1)); assertEquals(2, rs.getInt(2));
            s.execute("DROP TABLE tree");
        }
    }

    // ================================================================
    //  45. Expressions in RETURNING
    // ================================================================

    @Test
    void testReturningWithExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ret_expr (id INTEGER, price INTEGER, qty INTEGER)");
            ResultSet rs = s.executeQuery(
                    "INSERT INTO ret_expr VALUES (1, 10, 5) RETURNING id, price * qty AS total");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(50, rs.getInt(2));
            s.execute("DROP TABLE ret_expr");
        }
    }

    // ================================================================
    //  46. Multiple DEFAULT values and serial behavior
    // ================================================================

    @Test
    void testDefaultValues() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE def_vals (id SERIAL PRIMARY KEY, status TEXT DEFAULT 'pending', count INTEGER DEFAULT 0)");
            s.execute("INSERT INTO def_vals (status) VALUES ('active')");
            s.execute("INSERT INTO def_vals DEFAULT VALUES");
            ResultSet rs = s.executeQuery("SELECT id, status, count FROM def_vals ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("active", rs.getString(2)); assertEquals(0, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("pending", rs.getString(2)); assertEquals(0, rs.getInt(3));
            s.execute("DROP TABLE def_vals");
        }
    }

    // ================================================================
    //  47. NATURAL JOIN
    // ================================================================

    @Test
    void testNaturalJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE nj1 (id INTEGER, name TEXT)");
            s.execute("CREATE TABLE nj2 (id INTEGER, score INTEGER)");
            s.execute("INSERT INTO nj1 VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')");
            s.execute("INSERT INTO nj2 VALUES (1,100),(2,200)");
            ResultSet rs = s.executeQuery("SELECT name, score FROM nj1 NATURAL JOIN nj2 ORDER BY name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(100, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(200, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE nj1");
            s.execute("DROP TABLE nj2");
        }
    }

    // ================================================================
    //  48. Three-way join
    // ================================================================

    @Test
    void testThreeWayJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE j3_a (id INTEGER, name TEXT)");
            s.execute("CREATE TABLE j3_b (a_id INTEGER, val TEXT)");
            s.execute("CREATE TABLE j3_c (a_id INTEGER, score INTEGER)");
            s.execute("INSERT INTO j3_a VALUES (1,'X'),(2,'Y')");
            s.execute("INSERT INTO j3_b VALUES (1,'b1'),(2,'b2')");
            s.execute("INSERT INTO j3_c VALUES (1,100),(2,200)");
            ResultSet rs = s.executeQuery(
                    "SELECT a.name, b.val, c.score " +
                    "FROM j3_a a JOIN j3_b b ON a.id = b.a_id JOIN j3_c c ON a.id = c.a_id ORDER BY a.name");
            assertTrue(rs.next()); assertEquals("X", rs.getString(1)); assertEquals("b1", rs.getString(2)); assertEquals(100, rs.getInt(3));
            assertTrue(rs.next()); assertEquals("Y", rs.getString(1)); assertEquals("b2", rs.getString(2)); assertEquals(200, rs.getInt(3));
            s.execute("DROP TABLE j3_a"); s.execute("DROP TABLE j3_b"); s.execute("DROP TABLE j3_c");
        }
    }

    // ================================================================
    //  49. Chained string functions
    // ================================================================

    @Test
    void testChainedStringFunctions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT UPPER(TRIM(REVERSE('  dcba  ')))");
            assertTrue(rs.next());
            assertEquals("ABCD", rs.getString(1));
        }
    }

    // ================================================================
    //  50. COUNT DISTINCT with GROUP BY
    // ================================================================

    @Test
    void testCountDistinctGroupBy() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cd (grp TEXT, val TEXT)");
            s.execute("INSERT INTO cd VALUES ('a','x'),('a','y'),('a','x'),('b','z'),('b','z')");
            ResultSet rs = s.executeQuery("SELECT grp, COUNT(DISTINCT val) FROM cd GROUP BY grp ORDER BY grp");
            assertTrue(rs.next()); assertEquals("a", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("b", rs.getString(1)); assertEquals(1, rs.getInt(2));
            s.execute("DROP TABLE cd");
        }
    }
}
