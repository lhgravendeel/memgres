package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for JOIN USING column elimination and LATERAL subqueries.
 *
 * In PG, JOIN ... USING (col) produces the join column only once in SELECT *.
 * LATERAL subqueries can reference columns from preceding FROM items.
 * Array || operator must work in recursive CTEs for path accumulation.
 */
class JoinUsingAndLateralTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE j1(id int PRIMARY KEY, v text)");
        exec("CREATE TABLE j2(id int PRIMARY KEY, j1_id int, qty int)");
        exec("INSERT INTO j1 VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("INSERT INTO j2 VALUES (10,1,5),(11,1,6),(12,2,7)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<String> columnNames(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
            return cols;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++;
            return n;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // JOIN USING column elimination
    // ========================================================================

    @Test
    void join_using_eliminates_duplicate_column() throws SQLException {
        // PG: SELECT * FROM j1 JOIN j2 USING (id) → columns: id, v, j1_id, qty (4 cols, not 5)
        List<String> cols = columnNames("SELECT * FROM j1 JOIN j2 USING (id)");
        assertEquals(4, cols.size(),
                "JOIN USING should eliminate duplicate 'id' column; expected 4 columns, not 5");
        // The USING column should appear exactly once
        assertEquals(1, cols.stream().filter(c -> c.equals("id")).count(),
                "'id' column should appear exactly once in JOIN USING result");
    }

    @Test
    void join_using_column_comes_first() throws SQLException {
        // In PG, the USING column(s) appear first in SELECT *
        List<String> cols = columnNames("SELECT * FROM j1 JOIN j2 USING (id)");
        assertEquals("id", cols.get(0), "USING column should be first in SELECT *");
    }

    // ========================================================================
    // LATERAL subqueries
    // ========================================================================

    @Test
    void lateral_subquery_basic() throws SQLException {
        int count = countRows("""
            SELECT *
            FROM j1, LATERAL (SELECT qty FROM j2 WHERE j2.j1_id = j1.id ORDER BY qty LIMIT 1) q
            ORDER BY j1.id
            """);
        // j1 has 3 rows. j2 has j1_ids 1,1,2. So j1 id=1 and id=2 match, id=3 doesn't.
        // LATERAL with implicit INNER JOIN → 2 rows
        assertEquals(2, count, "LATERAL subquery should produce 2 rows (inner join semantics)");
    }

    @Test
    void left_join_lateral() throws SQLException {
        int count = countRows("""
            SELECT *
            FROM j1
            LEFT JOIN LATERAL (
              SELECT qty FROM j2 WHERE j2.j1_id = j1.id ORDER BY qty DESC LIMIT 1
            ) AS q ON true
            ORDER BY j1.id
            """);
        // LEFT JOIN LATERAL → all 3 j1 rows, with NULL for j1.id=3
        assertEquals(3, count, "LEFT JOIN LATERAL should preserve all left rows");
    }

    // ========================================================================
    // Array || operator in recursive CTEs
    // ========================================================================

    @Test
    void array_concat_operator_in_recursive_cte() throws SQLException {
        // This is the pattern: path || (n+1) where path is an int[] and n+1 is int
        // PG supports int[] || int as array append
        List<String> result = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 WITH RECURSIVE tree(n, path) AS (
                   VALUES (1, ARRAY[1])
                   UNION ALL
                   SELECT n + 1, path || (n + 1)
                   FROM tree
                   WHERE n < 4
                 )
                 SELECT n, path FROM tree ORDER BY n
                 """)) {
            while (rs.next()) {
                result.add(rs.getInt(1) + ":" + rs.getString(2));
            }
        }
        assertEquals(4, result.size());
        assertEquals("1:{1}", result.get(0));
        assertEquals("4:{1,2,3,4}", result.get(3),
                "Array || should accumulate path elements in recursive CTE");
    }

    @Test
    void array_append_via_concat_operator() throws SQLException {
        // int[] || int should append element
        String val = scalar("SELECT ARRAY[1,2,3] || 4");
        assertEquals("{1,2,3,4}", val);
    }

    @Test
    void array_prepend_via_concat_operator() throws SQLException {
        // int || int[] should prepend element
        String val = scalar("SELECT 0 || ARRAY[1,2,3]");
        assertEquals("{0,1,2,3}", val);
    }

    @Test
    void array_concat_two_arrays() throws SQLException {
        String val = scalar("SELECT ARRAY[1,2] || ARRAY[3,4]");
        assertEquals("{1,2,3,4}", val);
    }

    // ========================================================================
    // Correlated subquery in SELECT list with alias
    // ========================================================================

    @Test
    void correlated_subquery_in_select_with_alias() throws SQLException {
        int count = countRows("""
            SELECT t.id,
                   (SELECT max(qty) FROM j2 WHERE j2.j1_id = t.id) AS max_qty
            FROM j1 t
            ORDER BY t.id
            """);
        assertEquals(3, count);
    }
}
