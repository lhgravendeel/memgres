package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ON CONFLICT ... DO UPDATE with the EXCLUDED pseudo-table.
 * Covers simple column refs, casts, functions, coalesce, case, arithmetic,
 * WHERE clause with EXCLUDED, and table-qualified refs.
 */
class OnConflictExcludedTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void simple_excluded_ref() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t1 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t1 VALUES (1, 'a')");
            s.execute("INSERT INTO t1 VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t1 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            s.execute("DROP TABLE t1");
        }
    }

    @Test
    void excluded_case_insensitive() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t2 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t2 VALUES (1, 'a')");
            s.execute("INSERT INTO t2 VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = excluded.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t2 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            s.execute("DROP TABLE t2");
        }
    }

    @Test
    void excluded_with_cast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t3 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t3 VALUES (1, 10)");
            s.execute("INSERT INTO t3 VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v::int");
            ResultSet rs = s.executeQuery("SELECT v FROM t3 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            s.execute("DROP TABLE t3");
        }
    }

    @Test
    void excluded_with_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t4 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t4 VALUES (1, 'hello')");
            s.execute("INSERT INTO t4 VALUES (1, 'world') ON CONFLICT (id) DO UPDATE SET v = upper(EXCLUDED.v)");
            ResultSet rs = s.executeQuery("SELECT v FROM t4 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("WORLD", rs.getString(1));
            s.execute("DROP TABLE t4");
        }
    }

    @Test
    void excluded_with_coalesce() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t5 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t5 VALUES (1, 'existing')");
            s.execute("INSERT INTO t5 VALUES (1, NULL) ON CONFLICT (id) DO UPDATE SET v = COALESCE(EXCLUDED.v, t5.v)");
            ResultSet rs = s.executeQuery("SELECT v FROM t5 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("existing", rs.getString(1));
            s.execute("DROP TABLE t5");
        }
    }

    @Test
    void excluded_with_case_expr() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t6 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t6 VALUES (1, 10)");
            s.execute("INSERT INTO t6 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = CASE WHEN EXCLUDED.v > t6.v THEN EXCLUDED.v ELSE t6.v END");
            ResultSet rs = s.executeQuery("SELECT v FROM t6 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            s.execute("DROP TABLE t6");
        }
    }

    @Test
    void excluded_with_arithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t7 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t7 VALUES (1, 10)");
            s.execute("INSERT INTO t7 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = t7.v + EXCLUDED.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t7 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(15, rs.getInt(1));
            s.execute("DROP TABLE t7");
        }
    }

    @Test
    void excluded_in_where_clause_false() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t8 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t8 VALUES (1, 10)");
            s.execute("INSERT INTO t8 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > t8.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t8 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1)); // no update
            s.execute("DROP TABLE t8");
        }
    }

    @Test
    void excluded_in_where_clause_true() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t9 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t9 VALUES (1, 10)");
            s.execute("INSERT INTO t9 VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > t9.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t9 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            s.execute("DROP TABLE t9");
        }
    }

    @Test
    void excluded_with_concat() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t10 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t10 VALUES (1, 'hello')");
            s.execute("INSERT INTO t10 VALUES (1, 'world') ON CONFLICT (id) DO UPDATE SET v = t10.v || ' ' || EXCLUDED.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t10 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("hello world", rs.getString(1));
            s.execute("DROP TABLE t10");
        }
    }

    @Test
    void excluded_multiple_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t11 (id int PRIMARY KEY, a text, b int)");
            s.execute("INSERT INTO t11 VALUES (1, 'old', 10)");
            s.execute("INSERT INTO t11 VALUES (1, 'new', 20) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b");
            ResultSet rs = s.executeQuery("SELECT a, b FROM t11 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new", rs.getString("a"));
            assertEquals(20, rs.getInt("b"));
            s.execute("DROP TABLE t11");
        }
    }

    @Test
    void excluded_returning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t12 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t12 VALUES (1, 'a')");
            ResultSet rs = s.executeQuery("INSERT INTO t12 VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v RETURNING v");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            s.execute("DROP TABLE t12");
        }
    }

    @Test
    void excluded_on_constraint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t13 (id int, v text, CONSTRAINT t13_pk PRIMARY KEY (id))");
            s.execute("INSERT INTO t13 VALUES (1, 'a')");
            s.execute("INSERT INTO t13 VALUES (1, 'b') ON CONFLICT ON CONSTRAINT t13_pk DO UPDATE SET v = EXCLUDED.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t13 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            s.execute("DROP TABLE t13");
        }
    }

    @Test
    void excluded_with_is_null_in_case() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t14 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t14 VALUES (1, 'existing')");
            s.execute("INSERT INTO t14 (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET v = CASE WHEN EXCLUDED.v IS NULL THEN 'default' ELSE EXCLUDED.v END");
            ResultSet rs = s.executeQuery("SELECT v FROM t14 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("default", rs.getString(1));
            s.execute("DROP TABLE t14");
        }
    }

    @Test
    void excluded_nested_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t15 (id int PRIMARY KEY, v text)");
            s.execute("INSERT INTO t15 VALUES (1, 'hello')");
            s.execute("INSERT INTO t15 VALUES (1, '  WORLD  ') ON CONFLICT (id) DO UPDATE SET v = lower(trim(EXCLUDED.v))");
            ResultSet rs = s.executeQuery("SELECT v FROM t15 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("world", rs.getString(1));
            s.execute("DROP TABLE t15");
        }
    }

    @Test
    void excluded_with_insert_alias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t16 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO t16 VALUES (1, 10)");
            s.execute("INSERT INTO t16 AS t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET v = t.v + EXCLUDED.v");
            ResultSet rs = s.executeQuery("SELECT v FROM t16 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            s.execute("DROP TABLE t16");
        }
    }
}
