package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that count(*) over an empty window frame returns 0 (not NULL),
 * and that json_agg/json_object_agg over zero rows returns NULL.
 */
class WindowCountEmptyFrameTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void count_empty_frame_is_zero() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE wcf (id int, grp int)");
            s.execute("INSERT INTO wcf VALUES (1, 1)");
            // ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING for first row = empty frame
            ResultSet rs = s.executeQuery(
                    "SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS cnt FROM wcf");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("cnt"), "count(*) over empty frame should be 0, not NULL");
            assertFalse(rs.wasNull());
        }
    }

    @Test void json_agg_zero_rows_is_null() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE jaz (id int)");
            ResultSet rs = s.executeQuery("SELECT json_agg(id) AS result FROM jaz");
            assertTrue(rs.next());
            assertNull(rs.getString("result"), "json_agg over zero rows should be NULL");
        }
    }

    @Test void json_object_agg_zero_rows_is_null() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE joaz (k text, v text)");
            ResultSet rs = s.executeQuery("SELECT json_object_agg(k, v) AS result FROM joaz");
            assertTrue(rs.next());
            assertNull(rs.getString("result"), "json_object_agg over zero rows should be NULL");
        }
    }
}
