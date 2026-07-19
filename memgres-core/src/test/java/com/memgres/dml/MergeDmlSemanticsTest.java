package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MERGE guards, DML CTEs, RETURNING sources: C3, C5, H1, M6.
 */
class MergeDmlSemanticsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private List<String> query(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    // ---- C3: MERGE 21000 on duplicate source matches ----

    @Test
    void merge_duplicate_source_raises_21000() throws SQLException {
        exec("CREATE TABLE t_c3 (id int PRIMARY KEY, val text)");
        exec("INSERT INTO t_c3 VALUES (1, 'a'), (2, 'b')");
        exec("CREATE TABLE s_c3 (id int, val text)");
        exec("INSERT INTO s_c3 VALUES (2, 'x'), (2, 'y')");

        SQLException ex = assertThrows(SQLException.class, () ->
            exec("MERGE INTO t_c3 t USING s_c3 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = s.val"));
        assertEquals("21000", ex.getSQLState());

        exec("DROP TABLE t_c3, s_c3");
    }

    @Test
    void merge_duplicate_source_delete_raises_21000() throws SQLException {
        exec("CREATE TABLE t_c3d (id int PRIMARY KEY, val text)");
        exec("INSERT INTO t_c3d VALUES (1, 'a'), (2, 'b')");
        exec("CREATE TABLE s_c3d (id int, val text)");
        exec("INSERT INTO s_c3d VALUES (2, 'x'), (2, 'y')");

        SQLException ex = assertThrows(SQLException.class, () ->
            exec("MERGE INTO t_c3d t USING s_c3d s ON t.id = s.id WHEN MATCHED THEN DELETE"));
        assertEquals("21000", ex.getSQLState());

        exec("DROP TABLE t_c3d, s_c3d");
    }

    // ---- C5: Unreferenced DML CTEs ----

    @Test
    void unreferenced_insert_cte_executes() throws SQLException {
        exec("CREATE TABLE t_c5 (id int, val text)");
        // CTE is not referenced in the main SELECT
        List<String> result = query("WITH ins AS (INSERT INTO t_c5 VALUES (1, 'inserted') RETURNING id) SELECT 42");
        assertEquals("42", result.get(0));

        // The INSERT should still have been executed
        List<String> rows = query("SELECT val FROM t_c5");
        assertEquals(1, rows.size());
        assertEquals("inserted", rows.get(0));

        exec("DROP TABLE t_c5");
    }

    @Test
    void unreferenced_delete_cte_executes() throws SQLException {
        exec("CREATE TABLE t_c5d (id int, val text)");
        exec("INSERT INTO t_c5d VALUES (1, 'a'), (2, 'b')");

        query("WITH del AS (DELETE FROM t_c5d WHERE id = 1 RETURNING id) SELECT 'done'");

        List<String> rows = query("SELECT id FROM t_c5d");
        assertEquals(1, rows.size());
        assertEquals("2", rows.get(0));

        exec("DROP TABLE t_c5d");
    }

    // ---- H1: RETURNING references FROM/USING tables ----

    @Test
    void update_from_returning_source_column() throws SQLException {
        exec("CREATE TABLE t_h1u (id int PRIMARY KEY, val text)");
        exec("CREATE TABLE s_h1u (id int PRIMARY KEY, sval text)");
        exec("INSERT INTO t_h1u VALUES (1, 'a'), (2, 'b')");
        exec("INSERT INTO s_h1u VALUES (1, 'x'), (2, 'y')");

        List<String> result = query("UPDATE t_h1u t SET val = s.sval FROM s_h1u s WHERE t.id = s.id RETURNING t.id, s.sval");
        assertEquals(2, result.size());
        assertTrue(result.contains("1,x"));
        assertTrue(result.contains("2,y"));

        exec("DROP TABLE t_h1u, s_h1u");
    }

    @Test
    void delete_using_returning_source_column() throws SQLException {
        exec("CREATE TABLE t_h1d (id int PRIMARY KEY, val text)");
        exec("CREATE TABLE s_h1d (id int PRIMARY KEY, sval text)");
        exec("INSERT INTO t_h1d VALUES (1, 'a'), (2, 'b')");
        exec("INSERT INTO s_h1d VALUES (1, 'x'), (2, 'y')");

        List<String> result = query("DELETE FROM t_h1d t USING s_h1d s WHERE t.id = s.id RETURNING t.id, s.sval");
        assertEquals(2, result.size());
        assertTrue(result.contains("1,x"));
        assertTrue(result.contains("2,y"));

        // Verify rows were actually deleted
        List<String> remaining = query("SELECT count(*) FROM t_h1d");
        assertEquals("0", remaining.get(0));

        exec("DROP TABLE t_h1d, s_h1d");
    }

    // ---- M6: MERGE INSERT DEFAULT VALUES ----

    @Test
    void merge_insert_default_values() throws SQLException {
        exec("CREATE TABLE t_m6 (id serial PRIMARY KEY, val text DEFAULT 'default_val')");
        exec("CREATE TABLE s_m6 (id int)");
        exec("INSERT INTO s_m6 VALUES (999)");

        exec("MERGE INTO t_m6 t USING s_m6 s ON t.id = s.id WHEN NOT MATCHED THEN INSERT DEFAULT VALUES");

        List<String> rows = query("SELECT val FROM t_m6");
        assertEquals(1, rows.size());
        assertEquals("default_val", rows.get(0));

        exec("DROP TABLE t_m6, s_m6");
    }
}
