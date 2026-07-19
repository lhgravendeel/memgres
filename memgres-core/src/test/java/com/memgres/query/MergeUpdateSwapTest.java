package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that MERGE ... WHEN MATCHED THEN UPDATE SET a = t.b, b = t.a
 * swaps correctly — SET expressions must see the original row, not the
 * partially-updated row.
 */
class MergeUpdateSwapTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void merge_swap_columns() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ms_target (id int PRIMARY KEY, a int, b int)");
            s.execute("INSERT INTO ms_target VALUES (1, 10, 20)");
            s.execute("CREATE TABLE ms_source (id int PRIMARY KEY)");
            s.execute("INSERT INTO ms_source VALUES (1)");
            s.execute("MERGE INTO ms_target t USING ms_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET a = t.b, b = t.a");
            ResultSet rs = s.executeQuery("SELECT a, b FROM ms_target WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt("a"), "a should get old b value (20)");
            assertEquals(10, rs.getInt("b"), "b should get old a value (10)");
        }
    }

    @Test void merge_self_reference() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ms_self (id int PRIMARY KEY, x int, y int)");
            s.execute("INSERT INTO ms_self VALUES (1, 5, 3)");
            s.execute("CREATE TABLE ms_src2 (id int)");
            s.execute("INSERT INTO ms_src2 VALUES (1)");
            s.execute("MERGE INTO ms_self t USING ms_src2 s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET x = t.x + t.y, y = t.x - t.y");
            ResultSet rs = s.executeQuery("SELECT x, y FROM ms_self WHERE id = 1");
            assertTrue(rs.next());
            // x = 5+3 = 8, y = 5-3 = 2 (using original values)
            assertEquals(8, rs.getInt("x"));
            assertEquals(2, rs.getInt("y"));
        }
    }
}
