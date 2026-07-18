package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that the default window frame with ORDER BY uses RANGE BETWEEN UNBOUNDED
 * PRECEDING AND CURRENT ROW (including peers), not ROWS semantics.
 * PG's default frame includes all rows with the same ORDER BY value as the current row.
 */
class WindowFramePeersTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void sum_with_order_by_includes_peers() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE wfp_t1 (g int, x int)");
            s.execute("INSERT INTO wfp_t1 VALUES (1, 10), (1, 20), (2, 30), (2, 40)");

            // PG: sum over default frame with ORDER BY g includes all peers with same g
            // g=1 rows: sum = 10+20 = 30 (both peers included)
            // g=2 rows: sum = 10+20+30+40 = 100 (all rows up to and including peers)
            try (ResultSet rs = s.executeQuery(
                    "SELECT g, x, sum(x) OVER (ORDER BY g) AS running_sum FROM wfp_t1 ORDER BY g, x")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt("g")); assertEquals(10, rs.getInt("x"));
                assertEquals(30, rs.getInt("running_sum"), "g=1 first row: sum should include both peers (10+20=30)");

                assertTrue(rs.next()); assertEquals(1, rs.getInt("g")); assertEquals(20, rs.getInt("x"));
                assertEquals(30, rs.getInt("running_sum"), "g=1 second row: same peer group, same sum");

                assertTrue(rs.next()); assertEquals(2, rs.getInt("g")); assertEquals(30, rs.getInt("x"));
                assertEquals(100, rs.getInt("running_sum"), "g=2 first row: sum includes all peers (10+20+30+40)");

                assertTrue(rs.next()); assertEquals(2, rs.getInt("g")); assertEquals(40, rs.getInt("x"));
                assertEquals(100, rs.getInt("running_sum"), "g=2 second row: same peer group, same sum");
            }

            s.execute("DROP TABLE wfp_t1");
        }
    }

    @Test void count_with_order_by_includes_peers() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE wfp_t2 (g text, v int)");
            s.execute("INSERT INTO wfp_t2 VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 4)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT g, count(*) OVER (ORDER BY g) AS cnt FROM wfp_t2 ORDER BY g, v")) {
                // All 'a' rows are peers → count should be 3 for all of them
                assertTrue(rs.next()); assertEquals("a", rs.getString("g"));
                assertEquals(3, rs.getInt("cnt"), "All 'a' peers should have count=3");
                assertTrue(rs.next()); assertEquals(3, rs.getInt("cnt"));
                assertTrue(rs.next()); assertEquals(3, rs.getInt("cnt"));
                // 'b' row → count=4
                assertTrue(rs.next()); assertEquals("b", rs.getString("g"));
                assertEquals(4, rs.getInt("cnt"));
            }

            s.execute("DROP TABLE wfp_t2");
        }
    }

    @Test void last_value_default_frame_with_order_by() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE wfp_t3 (g int, v int)");
            s.execute("INSERT INTO wfp_t3 VALUES (1, 10), (1, 20), (2, 30)");

            // last_value with default frame + ORDER BY: frame ends at current row (ROWS)
            // or at last peer (RANGE). PG uses RANGE, so last_value for g=1 should be 20 (last peer).
            try (ResultSet rs = s.executeQuery(
                    "SELECT g, v, last_value(v) OVER (ORDER BY g) AS lv FROM wfp_t3 ORDER BY g, v")) {
                assertTrue(rs.next()); assertEquals(1, rs.getInt("g")); assertEquals(10, rs.getInt("v"));
                assertEquals(20, rs.getInt("lv"), "last_value should be last peer in g=1 group (20)");
                assertTrue(rs.next()); assertEquals(20, rs.getInt("lv"));
                assertTrue(rs.next()); assertEquals(2, rs.getInt("g"));
                assertEquals(30, rs.getInt("lv"));
            }

            s.execute("DROP TABLE wfp_t3");
        }
    }

    @Test void avg_with_order_by_includes_peers() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE wfp_t4 (g int, x numeric)");
            s.execute("INSERT INTO wfp_t4 VALUES (1, 10), (1, 30), (2, 50)");

            try (ResultSet rs = s.executeQuery(
                    "SELECT g, avg(x) OVER (ORDER BY g) AS a FROM wfp_t4 ORDER BY g, x")) {
                // g=1 peers: avg(10,30) = 20
                assertTrue(rs.next()); assertEquals(1, rs.getInt("g"));
                assertEquals(20.0, rs.getDouble("a"), 0.001, "avg should include both g=1 peers");
                assertTrue(rs.next());
                assertEquals(20.0, rs.getDouble("a"), 0.001);
                // g=2: avg(10,30,50) = 30
                assertTrue(rs.next()); assertEquals(2, rs.getInt("g"));
                assertEquals(30.0, rs.getDouble("a"), 0.001);
            }

            s.execute("DROP TABLE wfp_t4");
        }
    }
}
