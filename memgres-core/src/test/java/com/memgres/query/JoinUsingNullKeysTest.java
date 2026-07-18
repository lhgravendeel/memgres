package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that FULL/RIGHT JOIN USING produces COALESCE(left.col, right.col) for
 * the merged join column, not just the left side. When the left side is NULL
 * (unmatched right row), the merged column should show the right side's value.
 */
class JoinUsingNullKeysTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void full_join_using_unmatched_right_shows_right_key() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE junk_l (id int, lv text)");
            s.execute("CREATE TABLE junk_r (id int, rv text)");
            s.execute("INSERT INTO junk_l VALUES (1, 'left1')");
            s.execute("INSERT INTO junk_r VALUES (1, 'right1'), (2, 'right2')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT id, lv, rv FROM junk_l FULL JOIN junk_r USING (id) ORDER BY id")) {
                // Matched row: id=1
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("left1", rs.getString("lv"));
                assertEquals("right1", rs.getString("rv"));

                // Unmatched right row: id should be 2 (from right side), not NULL
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"),
                        "FULL JOIN USING: unmatched right row should show right.id, not NULL");
                assertNull(rs.getString("lv"));
                assertEquals("right2", rs.getString("rv"));
            }

            s.execute("DROP TABLE junk_l"); s.execute("DROP TABLE junk_r");
        }
    }

    @Test void right_join_using_unmatched_right_shows_right_key() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE junk_l2 (id int, lv text)");
            s.execute("CREATE TABLE junk_r2 (id int, rv text)");
            s.execute("INSERT INTO junk_l2 VALUES (1, 'left1')");
            s.execute("INSERT INTO junk_r2 VALUES (1, 'right1'), (3, 'right3')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT id, lv, rv FROM junk_l2 RIGHT JOIN junk_r2 USING (id) ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));

                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"),
                        "RIGHT JOIN USING: unmatched right row should show right.id, not NULL");
                assertNull(rs.getString("lv"));
                assertEquals("right3", rs.getString("rv"));
            }

            s.execute("DROP TABLE junk_l2"); s.execute("DROP TABLE junk_r2");
        }
    }

    @Test void full_join_using_unmatched_left_shows_left_key() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE junk_l3 (id int, lv text)");
            s.execute("CREATE TABLE junk_r3 (id int, rv text)");
            s.execute("INSERT INTO junk_l3 VALUES (1, 'left1'), (5, 'left5')");
            s.execute("INSERT INTO junk_r3 VALUES (1, 'right1')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT id, lv, rv FROM junk_l3 FULL JOIN junk_r3 USING (id) ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));

                // Unmatched left: id should be 5 (from left)
                assertTrue(rs.next());
                assertEquals(5, rs.getInt("id"));
                assertEquals("left5", rs.getString("lv"));
                assertNull(rs.getString("rv"));
            }

            s.execute("DROP TABLE junk_l3"); s.execute("DROP TABLE junk_r3");
        }
    }

    @Test void full_join_using_multiple_columns() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE junk_l4 (a int, b int, lv text)");
            s.execute("CREATE TABLE junk_r4 (a int, b int, rv text)");
            s.execute("INSERT INTO junk_l4 VALUES (1, 10, 'L')");
            s.execute("INSERT INTO junk_r4 VALUES (2, 20, 'R')");

            try (ResultSet rs = s.executeQuery(
                    "SELECT a, b, lv, rv FROM junk_l4 FULL JOIN junk_r4 USING (a, b) ORDER BY COALESCE(a, 999)")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("a"));
                assertEquals(10, rs.getInt("b"));

                assertTrue(rs.next());
                assertEquals(2, rs.getInt("a"),
                        "Unmatched right: merged 'a' should be 2 from right side");
                assertEquals(20, rs.getInt("b"),
                        "Unmatched right: merged 'b' should be 20 from right side");
            }

            s.execute("DROP TABLE junk_l4"); s.execute("DROP TABLE junk_r4");
        }
    }
}
