package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class GeometricOpsCompatTest {
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

    @Test
    @DisplayName("Box intersection operator # should return overlapping region")
    void testBoxIntersectionOperator() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT box '(0,0),(2,2)' # box '(1,1),(3,3)' AS inter")) {
            assertTrue(rs.next());
            assertEquals("(2,2),(1,1)", rs.getString("inter"));
        }
    }

    @Test
    @DisplayName("Box distance operator <-> should return 1 for adjacent boxes")
    void testBoxDistance() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT (box '(0,0),(2,2)' <-> box '(1,1),(3,3)')::integer AS dist")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("dist"));
        }
    }

    @Test
    @DisplayName("center(lseg) should not exist as a function (matching PG)")
    void testCenterLsegDoesNotExist() {
        assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT center(lseg '((0,0),(3,4))') AS c")) {
                rs.next();
            }
        });
    }

    @Test
    @DisplayName("area(polygon) should not exist as a function (matching PG)")
    void testAreaPolygonDoesNotExist() {
        assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))')::integer AS a")) {
                rs.next();
            }
        });
    }

    @Test
    @DisplayName("center(polygon) should not exist as a function (matching PG)")
    void testCenterPolygonDoesNotExist() {
        assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT center(polygon '((0,0),(4,0),(4,3),(0,3))') AS c")) {
                rs.next();
            }
        });
    }

    @Test
    @DisplayName("box(circle) should return inscribed square, not bounding box")
    void testBoxOfCircleReturnsInscribedSquare() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT box(circle '((0,0),5)') AS bb")) {
            assertTrue(rs.next());
            String result = rs.getString("bb");
            assertTrue(result.contains("3.535"),
                    "Expected inscribed square with ~3.535 coordinates but got: " + result);
        }
    }

    @Test
    @DisplayName("npoints of polygon created from circle should match vertex count")
    void testNpointsPolygonFromCircle() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT npoints(polygon(8, circle '((0,0),1)')) AS n")) {
            assertTrue(rs.next());
            assertEquals(8, rs.getInt("n"));
        }
    }

    @Test
    @DisplayName("Distance operator <-> should be usable in WHERE clause as boolean comparison")
    void testDistanceOperatorInWhereClause() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE geo_shapes (id integer, shape_type text, loc point)");
            stmt.execute("INSERT INTO geo_shapes VALUES "
                    + "(1, 'building', point(10, 20)), "
                    + "(2, 'park', point(50, 60)), "
                    + "(3, 'school', point(12, 21))");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM geo_shapes WHERE loc <-> point(10, 20) < 5 ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id"));
                assertFalse(rs.next());
            }
        }
    }
}
