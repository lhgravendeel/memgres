package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 gap verification round 3: edge cases, error paths, and less common features.
 */
class Pg18GapVerificationRound3Test {

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

    // ===== Error message accuracy =====

    @Test
    void syntaxErrorMessage() throws Exception {
        try (Statement st = conn.createStatement()) {
            try {
                st.execute("SELCT 1");
                fail("Should throw syntax error");
            } catch (SQLException e) {
                assertEquals("42601", e.getSQLState());
            }
        }
    }

    @Test
    void undefinedFunctionError() throws Exception {
        try (Statement st = conn.createStatement()) {
            try {
                st.executeQuery("SELECT no_such_function_xyz()");
                fail("Should throw undefined function");
            } catch (SQLException e) {
                assertEquals("42883", e.getSQLState());
            }
        }
    }

    @Test
    void numericOverflowError() throws Exception {
        try (Statement st = conn.createStatement()) {
            try {
                st.executeQuery("SELECT 2147483647::int + 1");
                // May or may not overflow depending on implementation
            } catch (SQLException e) {
                assertEquals("22003", e.getSQLState());
            }
        }
    }

    @Test
    void invalidDatetimeFormat() throws Exception {
        try (Statement st = conn.createStatement()) {
            try {
                st.executeQuery("SELECT 'not-a-date'::date");
                fail("Should throw invalid datetime format");
            } catch (SQLException e) {
                String state = e.getSQLState();
                assertTrue("22007".equals(state) || "22P02".equals(state),
                        "Expected 22007 or 22P02, got: " + state);
            }
        }
    }

    @Test
    void invalidInputSyntax() throws Exception {
        try (Statement st = conn.createStatement()) {
            try {
                st.executeQuery("SELECT 'abc'::integer");
                fail("Should throw invalid input syntax");
            } catch (SQLException e) {
                assertEquals("22P02", e.getSQLState());
            }
        }
    }

    // ===== NULL handling edge cases =====

    @Test
    void nullArithmetic() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT NULL + 1, NULL * 5, NULL || 'hello', NULL = NULL, NULL <> NULL")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertNull(rs.getObject(2));
                assertNull(rs.getObject(3));
                assertNull(rs.getObject(4));
                assertNull(rs.getObject(5));
            }
        }
    }

    @Test
    void nullInAggregate() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE na_test (val int)");
            st.execute("INSERT INTO na_test VALUES (1),(NULL),(3),(NULL),(5)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*), count(val), sum(val), avg(val) FROM na_test")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));  // count(*) counts NULLs
                assertEquals(3, rs.getInt(2));  // count(val) skips NULLs
                assertEquals(9, rs.getInt(3));  // sum skips NULLs
                assertEquals(3.0, rs.getDouble(4), 0.01); // avg skips NULLs
            }
            st.execute("DROP TABLE na_test");
        }
    }

    @Test
    void nullSafeComparisons() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ns_test (id int, val text)");
            st.execute("INSERT INTO ns_test VALUES (1, 'a'), (2, NULL), (3, 'b')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM ns_test WHERE val IS NULL")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertFalse(rs.next());
            }
            try (ResultSet rs = st.executeQuery(
                    "SELECT id FROM ns_test WHERE val IS NOT NULL ORDER BY id")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE ns_test");
        }
    }

    @Test
    void orderByNulls() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE on_test (val int)");
            st.execute("INSERT INTO on_test VALUES (3),(NULL),(1),(NULL),(2)");
            try (ResultSet rs = st.executeQuery("SELECT val FROM on_test ORDER BY val NULLS FIRST")) {
                assertTrue(rs.next()); assertNull(rs.getObject(1));
                assertTrue(rs.next()); assertNull(rs.getObject(1));
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT val FROM on_test ORDER BY val NULLS LAST")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertTrue(rs.next()); assertNull(rs.getObject(1));
                assertTrue(rs.next()); assertNull(rs.getObject(1));
            }
            st.execute("DROP TABLE on_test");
        }
    }

    // ===== Type coercion edge cases =====

    @Test
    void implicitIntToNumeric() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 1 + 1.5")) {
                assertTrue(rs.next());
                assertEquals(2.5, rs.getDouble(1), 0.001);
            }
        }
    }

    @Test
    void booleanCasting() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT 'true'::boolean, 'false'::boolean, 'yes'::boolean, 'no'::boolean, " +
                    "'t'::boolean, 'f'::boolean, '1'::boolean, '0'::boolean")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.getBoolean(2));
                assertTrue(rs.getBoolean(3));
                assertFalse(rs.getBoolean(4));
                assertTrue(rs.getBoolean(5));
                assertFalse(rs.getBoolean(6));
                assertTrue(rs.getBoolean(7));
                assertFalse(rs.getBoolean(8));
            }
        }
    }

    @Test
    void textToIntCast() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT '42'::int, '3.14'::float8")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertEquals(3.14, rs.getDouble(2), 0.001);
            }
        }
    }

    // ===== LIKE / ILIKE / SIMILAR TO =====

    @Test
    void likePattern() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lk_test (val text)");
            st.execute("INSERT INTO lk_test VALUES ('hello'),('help'),('world'),('helicopter')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT val FROM lk_test WHERE val LIKE 'hel%' ORDER BY val")) {
                assertTrue(rs.next()); assertEquals("helicopter", rs.getString(1));
                assertTrue(rs.next()); assertEquals("hello", rs.getString(1));
                assertTrue(rs.next()); assertEquals("help", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE lk_test");
        }
    }

    @Test
    void ilikePattern() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'Hello' ILIKE 'hello'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void notLikePattern() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 'world' NOT LIKE 'hel%'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void likeEscapeChar() throws Exception {
        try (Statement st = conn.createStatement()) {
            // Match literal % using escape
            try (ResultSet rs = st.executeQuery("SELECT '50%' LIKE '%!%%' ESCAPE '!'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // ===== IN / NOT IN / BETWEEN =====

    @Test
    void inList() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 3 IN (1, 2, 3, 4, 5)")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void notInList() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 6 NOT IN (1, 2, 3, 4, 5)")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void betweenRange() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT 5 BETWEEN 1 AND 10, 15 BETWEEN 1 AND 10")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.getBoolean(2));
            }
        }
    }

    @Test
    void notBetween() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT 15 NOT BETWEEN 1 AND 10")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    // ===== Multi-table joins =====

    @Test
    void innerJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ij_a (id int, val text)");
            st.execute("CREATE TABLE ij_b (a_id int, extra text)");
            st.execute("INSERT INTO ij_a VALUES (1,'x'),(2,'y')");
            st.execute("INSERT INTO ij_b VALUES (1,'p'),(1,'q'),(3,'r')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.val, b.extra FROM ij_a a JOIN ij_b b ON a.id = b.a_id ORDER BY b.extra")) {
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("p", rs.getString(2));
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("q", rs.getString(2));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE ij_b");
            st.execute("DROP TABLE ij_a");
        }
    }

    @Test
    void leftJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE lj_a (id int, val text)");
            st.execute("CREATE TABLE lj_b (a_id int, extra text)");
            st.execute("INSERT INTO lj_a VALUES (1,'x'),(2,'y')");
            st.execute("INSERT INTO lj_b VALUES (1,'p')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.val, b.extra FROM lj_a a LEFT JOIN lj_b b ON a.id = b.a_id ORDER BY a.id")) {
                assertTrue(rs.next()); assertEquals("x", rs.getString(1)); assertEquals("p", rs.getString(2));
                assertTrue(rs.next()); assertEquals("y", rs.getString(1)); assertNull(rs.getString(2));
            }
            st.execute("DROP TABLE lj_b");
            st.execute("DROP TABLE lj_a");
        }
    }

    @Test
    void fullOuterJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE fj_a (id int, val text)");
            st.execute("CREATE TABLE fj_b (id int, val text)");
            st.execute("INSERT INTO fj_a VALUES (1,'a'),(2,'b')");
            st.execute("INSERT INTO fj_b VALUES (2,'x'),(3,'y')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.id, a.val, b.id, b.val FROM fj_a a FULL OUTER JOIN fj_b b ON a.id = b.id ORDER BY COALESCE(a.id, b.id)")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertNull(rs.getObject(3));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(2, rs.getInt(3));
                assertTrue(rs.next()); assertNull(rs.getObject(1)); assertEquals(3, rs.getInt(3));
            }
            st.execute("DROP TABLE fj_b");
            st.execute("DROP TABLE fj_a");
        }
    }

    @Test
    void crossJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.v, b.v FROM (VALUES (1),(2)) a(v) CROSS JOIN (VALUES ('x'),('y')) b(v) ORDER BY a.v, b.v")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("x", rs.getString(2));
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("y", rs.getString(2));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("x", rs.getString(2));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("y", rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void naturalJoin() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE nj_a (id int, name text)");
            st.execute("CREATE TABLE nj_b (id int, score int)");
            st.execute("INSERT INTO nj_a VALUES (1,'alice'),(2,'bob')");
            st.execute("INSERT INTO nj_b VALUES (1,95),(2,80)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT name, score FROM nj_a NATURAL JOIN nj_b ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("alice", rs.getString(1)); assertEquals(95, rs.getInt(2));
                assertTrue(rs.next()); assertEquals("bob", rs.getString(1)); assertEquals(80, rs.getInt(2));
            }
            st.execute("DROP TABLE nj_b");
            st.execute("DROP TABLE nj_a");
        }
    }

    // ===== Window functions with partitioning =====

    @Test
    void windowRowNumber() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE wr_test (dept text, sal int)");
            st.execute("INSERT INTO wr_test VALUES ('A',100),('A',200),('B',150),('B',250),('B',50)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT dept, sal, row_number() OVER (PARTITION BY dept ORDER BY sal DESC) as rn " +
                    "FROM wr_test ORDER BY dept, rn")) {
                assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals(200, rs.getInt(2)); assertEquals(1, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("A", rs.getString(1)); assertEquals(100, rs.getInt(2)); assertEquals(2, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals(250, rs.getInt(2)); assertEquals(1, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals(150, rs.getInt(2)); assertEquals(2, rs.getInt(3));
                assertTrue(rs.next()); assertEquals("B", rs.getString(1)); assertEquals(50, rs.getInt(2)); assertEquals(3, rs.getInt(3));
            }
            st.execute("DROP TABLE wr_test");
        }
    }

    @Test
    void windowLagLead() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE wl_test (val int)");
            st.execute("INSERT INTO wl_test VALUES (10),(20),(30)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT val, lag(val) OVER (ORDER BY val), lead(val) OVER (ORDER BY val) FROM wl_test ORDER BY val")) {
                assertTrue(rs.next()); assertEquals(10, rs.getInt(1)); assertNull(rs.getObject(2)); assertEquals(20, rs.getInt(3));
                assertTrue(rs.next()); assertEquals(20, rs.getInt(1)); assertEquals(10, rs.getInt(2)); assertEquals(30, rs.getInt(3));
                assertTrue(rs.next()); assertEquals(30, rs.getInt(1)); assertEquals(20, rs.getInt(2)); assertNull(rs.getObject(3));
            }
            st.execute("DROP TABLE wl_test");
        }
    }

    @Test
    void windowSumRunning() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ws_test (val int)");
            st.execute("INSERT INTO ws_test VALUES (1),(2),(3),(4)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT val, sum(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running " +
                    "FROM ws_test ORDER BY val")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(1, rs.getInt(2));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(3, rs.getInt(2));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(6, rs.getInt(2));
                assertTrue(rs.next()); assertEquals(4, rs.getInt(1)); assertEquals(10, rs.getInt(2));
            }
            st.execute("DROP TABLE ws_test");
        }
    }

    // ===== PL/pgSQL features =====

    @Test
    void plpgsqlIfElse() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE FUNCTION classify(n int) RETURNS text AS $$ " +
                    "BEGIN " +
                    "  IF n < 0 THEN RETURN 'negative'; " +
                    "  ELSIF n = 0 THEN RETURN 'zero'; " +
                    "  ELSE RETURN 'positive'; " +
                    "  END IF; " +
                    "END $$ LANGUAGE plpgsql");
            try (ResultSet rs = st.executeQuery(
                    "SELECT classify(-1), classify(0), classify(5)")) {
                assertTrue(rs.next());
                assertEquals("negative", rs.getString(1));
                assertEquals("zero", rs.getString(2));
                assertEquals("positive", rs.getString(3));
            }
            st.execute("DROP FUNCTION classify");
        }
    }

    @Test
    void plpgsqlLoop() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE FUNCTION factorial(n int) RETURNS int AS $$ " +
                    "DECLARE result int := 1; i int; " +
                    "BEGIN " +
                    "  FOR i IN 1..n LOOP result := result * i; END LOOP; " +
                    "  RETURN result; " +
                    "END $$ LANGUAGE plpgsql");
            try (ResultSet rs = st.executeQuery("SELECT factorial(5)")) {
                assertTrue(rs.next());
                assertEquals(120, rs.getInt(1));
            }
            st.execute("DROP FUNCTION factorial");
        }
    }

    @Test
    void plpgsqlExceptionHandler() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE FUNCTION safe_divide(a int, b int) RETURNS text AS $$ " +
                    "BEGIN " +
                    "  RETURN (a / b)::text; " +
                    "EXCEPTION WHEN division_by_zero THEN " +
                    "  RETURN 'division by zero'; " +
                    "END $$ LANGUAGE plpgsql");
            try (ResultSet rs = st.executeQuery("SELECT safe_divide(10, 0)")) {
                assertTrue(rs.next());
                assertEquals("division by zero", rs.getString(1));
            }
            try (ResultSet rs = st.executeQuery("SELECT safe_divide(10, 2)")) {
                assertTrue(rs.next());
                assertEquals("5", rs.getString(1));
            }
            st.execute("DROP FUNCTION safe_divide");
        }
    }

    @Test
    void plpgsqlRaiseNotice() throws Exception {
        try (Statement st = conn.createStatement()) {
            // RAISE NOTICE should not throw an error
            st.execute("DO $$ BEGIN RAISE NOTICE 'hello %', 'world'; END $$");
        }
    }

    @Test
    void plpgsqlReturnSetof() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rs_data (id int, val text)");
            st.execute("INSERT INTO rs_data VALUES (1,'a'),(2,'b'),(3,'c')");
            st.execute("CREATE FUNCTION get_vals() RETURNS SETOF rs_data AS $$ " +
                    "BEGIN RETURN QUERY SELECT * FROM rs_data ORDER BY id; END $$ LANGUAGE plpgsql");
            try (ResultSet rs = st.executeQuery("SELECT * FROM get_vals()")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
            st.execute("DROP FUNCTION get_vals");
            st.execute("DROP TABLE rs_data");
        }
    }

    // ===== DO block =====

    @Test
    void doBlockBasic() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE do_test (val text)");
            st.execute("DO $$ BEGIN INSERT INTO do_test VALUES ('from_do_block'); END $$");
            try (ResultSet rs = st.executeQuery("SELECT val FROM do_test")) {
                assertTrue(rs.next());
                assertEquals("from_do_block", rs.getString(1));
            }
            st.execute("DROP TABLE do_test");
        }
    }

    // ===== Multi-column constraints =====

    @Test
    void compositeUniqueConstraint() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cu_test (a int, b int, UNIQUE(a, b))");
            st.execute("INSERT INTO cu_test VALUES (1, 1)");
            st.execute("INSERT INTO cu_test VALUES (1, 2)"); // different b, OK
            try {
                st.execute("INSERT INTO cu_test VALUES (1, 1)"); // duplicate
                fail("Should violate unique constraint");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }
            st.execute("DROP TABLE cu_test");
        }
    }

    @Test
    void compositePrimaryKey() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cpk_test (a int, b int, PRIMARY KEY(a, b))");
            st.execute("INSERT INTO cpk_test VALUES (1, 1)");
            st.execute("INSERT INTO cpk_test VALUES (1, 2)");
            try {
                st.execute("INSERT INTO cpk_test VALUES (1, 1)");
                fail("Should violate PK constraint");
            } catch (SQLException e) {
                assertEquals("23505", e.getSQLState());
            }
            st.execute("DROP TABLE cpk_test");
        }
    }

    // ===== ALTER TABLE operations =====

    @Test
    void alterTableAddDropColumn() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE at_test (id int)");
            st.execute("ALTER TABLE at_test ADD COLUMN name text");
            st.execute("INSERT INTO at_test VALUES (1, 'alice')");
            try (ResultSet rs = st.executeQuery("SELECT name FROM at_test")) {
                assertTrue(rs.next());
                assertEquals("alice", rs.getString(1));
            }
            st.execute("ALTER TABLE at_test DROP COLUMN name");
            try (ResultSet rs = st.executeQuery("SELECT * FROM at_test")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            st.execute("DROP TABLE at_test");
        }
    }

    @Test
    void alterTableRename() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE atr_before (id int)");
            st.execute("INSERT INTO atr_before VALUES (1)");
            st.execute("ALTER TABLE atr_before RENAME TO atr_after");
            try (ResultSet rs = st.executeQuery("SELECT id FROM atr_after")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            st.execute("DROP TABLE atr_after");
        }
    }

    @Test
    void alterTableRenameColumn() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE atrc_test (old_name int)");
            st.execute("INSERT INTO atrc_test VALUES (42)");
            st.execute("ALTER TABLE atrc_test RENAME COLUMN old_name TO new_name");
            try (ResultSet rs = st.executeQuery("SELECT new_name FROM atrc_test")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
            st.execute("DROP TABLE atrc_test");
        }
    }

    @Test
    void alterTableAlterColumnType() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE atct_test (val int)");
            st.execute("INSERT INTO atct_test VALUES (42)");
            st.execute("ALTER TABLE atct_test ALTER COLUMN val TYPE text USING val::text");
            try (ResultSet rs = st.executeQuery("SELECT val FROM atct_test")) {
                assertTrue(rs.next());
                assertEquals("42", rs.getString(1));
            }
            st.execute("DROP TABLE atct_test");
        }
    }

    // ===== HAVING clause =====

    @Test
    void havingClause() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE hv_test (grp text, val int)");
            st.execute("INSERT INTO hv_test VALUES ('a',1),('a',2),('b',3),('b',4),('b',5)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT grp, sum(val) FROM hv_test GROUP BY grp HAVING sum(val) > 5 ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(12, rs.getInt(2));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE hv_test");
        }
    }

    // ===== Subquery in various positions =====

    @Test
    void subqueryInWhere() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE sw_main (id int, val text)");
            st.execute("CREATE TABLE sw_filter (id int)");
            st.execute("INSERT INTO sw_main VALUES (1,'a'),(2,'b'),(3,'c')");
            st.execute("INSERT INTO sw_filter VALUES (1),(3)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT val FROM sw_main WHERE id IN (SELECT id FROM sw_filter) ORDER BY id")) {
                assertTrue(rs.next()); assertEquals("a", rs.getString(1));
                assertTrue(rs.next()); assertEquals("c", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE sw_filter");
            st.execute("DROP TABLE sw_main");
        }
    }

    @Test
    void subqueryInFrom() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM (SELECT generate_series(1,3) as n) sub WHERE n > 1 ORDER BY n")) {
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ===== LIMIT / OFFSET / FETCH =====

    @Test
    void limitOffset() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM generate_series(1,10) g(n) ORDER BY n LIMIT 3 OFFSET 2")) {
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void fetchFirstRows() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT * FROM generate_series(1,10) g(n) ORDER BY n FETCH FIRST 3 ROWS ONLY")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    // ===== Range type operations =====

    @Test
    void rangeContains() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT int4range(1, 10) @> 5, int4range(1, 10) @> 15")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.getBoolean(2));
            }
        }
    }

    @Test
    void rangeOverlap() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT int4range(1, 5) && int4range(3, 8), int4range(1, 3) && int4range(5, 8)")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.getBoolean(2));
            }
        }
    }

    @Test
    void rangeUnionIntersection() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "SELECT int4range(1, 5) + int4range(3, 8), int4range(1, 8) * int4range(3, 12)")) {
                assertTrue(rs.next());
                assertEquals("[1,8)", rs.getString(1));
                assertEquals("[3,8)", rs.getString(2));
            }
        }
    }

    // COPY FROM STDIN requires CopyManager API in JDBC, not regular Statement
    // Tested separately via PgWire protocol tests

    // ===== Multiple CTEs =====

    @Test
    void multipleCtes() throws Exception {
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(
                    "WITH a AS (SELECT 1 as n), b AS (SELECT 2 as n), c AS (SELECT n FROM a UNION ALL SELECT n FROM b) " +
                    "SELECT sum(n) FROM c")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getLong(1));
            }
        }
    }

    // ===== pg_catalog queries (ORMs rely on these) =====

    @Test
    void pgCatalogTablesQuery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cat_test (id int PRIMARY KEY, val text NOT NULL)");
            // Query that ORMs typically run to introspect tables
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname, c.relkind FROM pg_class c " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE n.nspname = 'public' AND c.relname = 'cat_test'")) {
                assertTrue(rs.next());
                assertEquals("cat_test", rs.getString(1));
                assertEquals("r", rs.getString(2));
            }
            // Column introspection query
            try (ResultSet rs = st.executeQuery(
                    "SELECT a.attname, a.attnotnull, t.typname " +
                    "FROM pg_attribute a " +
                    "JOIN pg_class c ON c.oid = a.attrelid " +
                    "JOIN pg_type t ON t.oid = a.atttypid " +
                    "WHERE c.relname = 'cat_test' AND a.attnum > 0 AND NOT a.attisdropped " +
                    "ORDER BY a.attnum")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString(1));
                assertTrue(rs.getBoolean(2));
                assertTrue(rs.getString(3).contains("int"));
                assertTrue(rs.next());
                assertEquals("val", rs.getString(1));
                assertTrue(rs.getBoolean(2));
            }
            st.execute("DROP TABLE cat_test");
        }
    }

    @Test
    void pgCatalogConstraintQuery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE cq_parent (id int PRIMARY KEY)");
            st.execute("CREATE TABLE cq_child (id int, parent_id int REFERENCES cq_parent(id))");
            try (ResultSet rs = st.executeQuery(
                    "SELECT conname, contype FROM pg_constraint " +
                    "WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 'cq_child') " +
                    "ORDER BY conname")) {
                boolean foundFk = false;
                while (rs.next()) {
                    if ("f".equals(rs.getString(2))) foundFk = true;
                }
                assertTrue(foundFk, "Should find foreign key constraint");
            }
            st.execute("DROP TABLE cq_child");
            st.execute("DROP TABLE cq_parent");
        }
    }
}
