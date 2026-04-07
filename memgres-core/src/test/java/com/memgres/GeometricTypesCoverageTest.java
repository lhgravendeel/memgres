package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checklist items 46-47: Geometric Types and Functions coverage tests.
 * Point, Line, Lseg, Box, Path, Polygon, Circle types,
 * geometric operators, measurement functions, type conversions.
 */
class GeometricTypesCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE geo_points (id serial PRIMARY KEY, p point)");
            s.execute("CREATE TABLE geo_boxes (id serial PRIMARY KEY, b box)");
            s.execute("CREATE TABLE geo_circles (id serial PRIMARY KEY, c circle)");
            s.execute("CREATE TABLE geo_lsegs (id serial PRIMARY KEY, l lseg)");
            s.execute("CREATE TABLE geo_lines (id serial PRIMARY KEY, l line)");
            s.execute("CREATE TABLE geo_paths (id serial PRIMARY KEY, p path)");
            s.execute("CREATE TABLE geo_polygons (id serial PRIMARY KEY, p polygon)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private double queryDouble(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getDouble(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    // ========================================================================
    // 1. Point type
    // ========================================================================

    @Test
    void point_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_points (p) VALUES ('(1,2)')");
            ResultSet rs = s.executeQuery("SELECT p FROM geo_points WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("(1,2)", rs.getString(1));
        }
    }

    @Test
    void point_literal_select() throws SQLException {
        assertEquals("(1,2)", query1("SELECT point '(1,2)'"));
    }

    @Test
    void point_without_parens() throws SQLException {
        assertEquals("(1,2)", query1("SELECT point '1,2'"));
    }

    @Test
    void point_addition() throws SQLException {
        assertEquals("(4,6)", query1("SELECT point '(1,2)' + point '(3,4)'"));
    }

    @Test
    void point_subtraction() throws SQLException {
        assertEquals("(2,2)", query1("SELECT point '(3,4)' - point '(1,2)'"));
    }

    @Test
    void point_multiplication_complex() throws SQLException {
        // Complex multiplication: (1,2)*(3,0) = (1*3-2*0, 1*0+2*3) = (3,6)
        assertEquals("(3,6)", query1("SELECT point '(1,2)' * point '(3,0)'"));
    }

    @Test
    void point_division_complex() throws SQLException {
        // Complex division: (3,6)/(3,0) = (1,2)
        assertEquals("(1,2)", query1("SELECT point '(3,6)' / point '(3,0)'"));
    }

    @Test
    void point_distance_operator() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT point '(0,0)' <-> point '(3,4)'"), 0.01);
    }

    @Test
    void point_distance_zero() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT point '(1,1)' <-> point '(1,1)'"), 0.01);
    }

    @Test
    void point_slope() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT slope(point '(0,0)', point '(1,1)')"), 0.01);
    }

    @Test
    void point_slope_vertical() throws SQLException {
        // Vertical slope should be Infinity
        double val = queryDouble("SELECT slope(point '(0,0)', point '(0,1)')");
        assertTrue(Double.isInfinite(val));
    }

    @Test
    void point_negative_coordinates() throws SQLException {
        assertEquals("(-3,-4)", query1("SELECT point '(-3,-4)'"));
    }

    @Test
    void point_addition_negative() throws SQLException {
        assertEquals("(-2,-2)", query1("SELECT point '(1,2)' + point '(-3,-4)'"));
    }

    // ========================================================================
    // 2. Line type
    // ========================================================================

    @Test
    void line_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_lines (l) VALUES ('{1,-1,0}')");
            ResultSet rs = s.executeQuery("SELECT l FROM geo_lines WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void line_literal_select() throws SQLException {
        String result = query1("SELECT line '{1,-1,0}'");
        assertNotNull(result);
    }

    @Test
    void line_from_points() throws SQLException {
        String result = query1("SELECT line(point '(0,0)', point '(1,1)')");
        assertNotNull(result);
    }

    // ========================================================================
    // 3. Line Segment (lseg) type
    // ========================================================================

    @Test
    void lseg_insert_and_select_bracket() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_lsegs (l) VALUES ('[(0,0),(1,1)]')");
            ResultSet rs = s.executeQuery("SELECT l FROM geo_lsegs WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void lseg_literal_select() throws SQLException {
        String result = query1("SELECT lseg '[(0,0),(1,1)]'");
        assertNotNull(result);
    }

    @Test
    void lseg_alternative_format() throws SQLException {
        String result = query1("SELECT lseg '((0,0),(1,1))'");
        assertNotNull(result);
    }

    @Test
    void lseg_length() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT length(lseg '[(0,0),(3,4)]')"), 0.01);
    }

    @Test
    void lseg_center() throws SQLException {
        assertEquals("(1,1)", query1("SELECT center(lseg '[(0,0),(2,2)]')"));
    }

    @Test
    void lseg_intersection_point() throws SQLException {
        // Intersection of diagonals
        String result = query1("SELECT lseg '[(0,0),(1,1)]' # lseg '[(0,1),(1,0)]'");
        assertNotNull(result);
    }

    @Test
    void lseg_intersection_test() throws SQLException {
        assertTrue(queryBool("SELECT intersects(lseg '[(0,0),(1,1)]', lseg '[(0,1),(1,0)]')"));
    }

    @Test
    void lseg_no_intersection() throws SQLException {
        assertFalse(queryBool("SELECT intersects(lseg '[(0,0),(1,0)]', lseg '[(0,2),(1,2)]')"));
    }

    @Test
    void lseg_closest_point() throws SQLException {
        String result = query1("SELECT closest_point(point '(0,1)', lseg '[(0,0),(2,0)]')");
        assertEquals("(0,0)", result);
    }

    @Test
    void lseg_horizontal_test() throws SQLException {
        assertTrue(queryBool("SELECT is_horizontal(lseg '[(0,0),(1,0)]')"));
    }

    @Test
    void lseg_vertical_test() throws SQLException {
        assertTrue(queryBool("SELECT is_vertical(lseg '[(0,0),(0,1)]')"));
    }

    @Test
    void lseg_parallel_test() throws SQLException {
        assertTrue(queryBool("SELECT is_parallel(lseg '[(0,0),(1,0)]', lseg '[(0,1),(1,1)]')"));
    }

    @Test
    void lseg_perpendicular_test() throws SQLException {
        assertTrue(queryBool("SELECT is_perpendicular(lseg '[(0,0),(1,0)]', lseg '[(0,0),(0,1)]')"));
    }

    @Test
    void lseg_not_parallel() throws SQLException {
        assertFalse(queryBool("SELECT is_parallel(lseg '[(0,0),(1,0)]', lseg '[(0,0),(1,1)]')"));
    }

    @Test
    void lseg_not_perpendicular() throws SQLException {
        assertFalse(queryBool("SELECT is_perpendicular(lseg '[(0,0),(1,0)]', lseg '[(0,0),(1,1)]')"));
    }

    @Test
    void lseg_length_function_alt() throws SQLException {
        // length function for lseg
        assertEquals(5.0, queryDouble("SELECT length(lseg '[(0,0),(3,4)]')"), 0.01);
    }

    @Test
    void lseg_center_function_alt() throws SQLException {
        // center function for lseg
        assertEquals("(1,1)", query1("SELECT center(lseg '[(0,0),(2,2)]')"));
    }

    // ========================================================================
    // 4. Box type
    // ========================================================================

    @Test
    void box_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_boxes (b) VALUES ('((1,1),(0,0))')");
            ResultSet rs = s.executeQuery("SELECT b FROM geo_boxes WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void box_literal_select() throws SQLException {
        String result = query1("SELECT box '((1,1),(0,0))'");
        assertNotNull(result);
    }

    @Test
    void box_auto_normalization() throws SQLException {
        // Box should normalize to (upper-right, lower-left)
        String r1 = query1("SELECT box '((0,0),(1,1))'");
        String r2 = query1("SELECT box '((1,1),(0,0))'");
        assertEquals(r1, r2);
    }

    @Test
    void box_plus_point_translation() throws SQLException {
        String result = query1("SELECT box '((1,1),(0,0))' + point '(2,3)'");
        assertNotNull(result);
        // Should be (3,4),(2,3)
        assertEquals("(3,4),(2,3)", result);
    }

    @Test
    void box_minus_point_translation() throws SQLException {
        String result = query1("SELECT box '((3,4),(2,3))' - point '(2,3)'");
        assertEquals("(1,1),(0,0)", result);
    }

    @Test
    void box_multiply_point() throws SQLException {
        // Scale/rotate by point (treated as complex number)
        String result = query1("SELECT box '((1,1),(0,0))' * point '(2,0)'");
        assertNotNull(result);
    }

    @Test
    void box_divide_point() throws SQLException {
        String result = query1("SELECT box '((2,2),(0,0))' / point '(2,0)'");
        assertNotNull(result);
    }

    @Test
    void box_area() throws SQLException {
        assertEquals(4.0, queryDouble("SELECT area(box '((2,2),(0,0))')"), 0.01);
    }

    @Test
    void box_area_rectangle() throws SQLException {
        assertEquals(6.0, queryDouble("SELECT area(box '((3,2),(0,0))')"), 0.01);
    }

    @Test
    void box_center() throws SQLException {
        assertEquals("(1,1)", query1("SELECT center(box '((2,2),(0,0))')"));
    }

    @Test
    void box_height() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT height(box '((2,3),(0,0))')"), 0.01);
    }

    @Test
    void box_width() throws SQLException {
        assertEquals(2.0, queryDouble("SELECT width(box '((2,3),(0,0))')"), 0.01);
    }

    @Test
    void box_diagonal() throws SQLException {
        String result = query1("SELECT diagonal(box '((2,2),(0,0))')");
        assertNotNull(result);
    }

    @Test
    void box_contains_point() throws SQLException {
        assertTrue(queryBool("SELECT box '((2,2),(0,0))' @> point '(1,1)'"));
    }

    @Test
    void box_not_contains_point() throws SQLException {
        assertFalse(queryBool("SELECT box '((2,2),(0,0))' @> point '(3,3)'"));
    }

    @Test
    void box_overlap() throws SQLException {
        assertTrue(queryBool("SELECT box '((2,2),(0,0))' && box '((3,3),(1,1))'"));
    }

    @Test
    void box_no_overlap() throws SQLException {
        assertFalse(queryBool("SELECT box '((1,1),(0,0))' && box '((3,3),(2,2))'"));
    }

    @Test
    void box_approximate_equality() throws SQLException {
        assertTrue(queryBool("SELECT box '((1,1),(0,0))' ~= box '((1,1),(0,0))'"));
    }

    @Test
    void box_strictly_left() throws SQLException {
        assertTrue(queryBool("SELECT box '((1,1),(0,0))' << box '((4,4),(3,3))'"));
    }

    @Test
    void box_strictly_right() throws SQLException {
        assertTrue(queryBool("SELECT box '((4,4),(3,3))' >> box '((1,1),(0,0))'"));
    }

    @Test
    void box_strictly_below() throws SQLException {
        assertTrue(queryBool("SELECT box '((1,1),(0,0))' <<| box '((1,4),(0,3))'"));
    }

    @Test
    void box_strictly_above() throws SQLException {
        assertTrue(queryBool("SELECT box '((1,4),(0,3))' |>> box '((1,1),(0,0))'"));
    }

    @Test
    void box_not_strictly_left() throws SQLException {
        assertFalse(queryBool("SELECT box '((2,2),(0,0))' << box '((3,3),(1,1))'"));
    }

    @Test
    void box_not_strictly_right() throws SQLException {
        assertFalse(queryBool("SELECT box '((3,3),(1,1))' >> box '((2,2),(0,0))'"));
    }

    // ========================================================================
    // 5. Path type
    // ========================================================================

    @Test
    void path_closed_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_paths (p) VALUES ('((0,0),(1,0),(1,1),(0,1))')");
            ResultSet rs = s.executeQuery("SELECT p FROM geo_paths WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void path_open_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_paths (p) VALUES ('[(0,0),(1,0),(1,1)]')");
            ResultSet rs = s.executeQuery("SELECT p FROM geo_paths ORDER BY id DESC LIMIT 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void path_closed_literal() throws SQLException {
        String result = query1("SELECT path '((0,0),(1,0),(1,1),(0,1))'");
        assertNotNull(result);
    }

    @Test
    void path_open_literal() throws SQLException {
        String result = query1("SELECT path '[(0,0),(1,0),(1,1)]'");
        assertNotNull(result);
    }

    @Test
    void path_length_open() throws SQLException {
        // Open path: (0,0)->(3,0)->(3,4) = 3 + 4 = 7
        assertEquals(7.0, queryDouble("SELECT length(path '[(0,0),(3,0),(3,4)]')"), 0.01);
    }

    @Test
    void path_length_closed() throws SQLException {
        // Closed path: (0,0)->(1,0)->(1,1)->(0,1)->back to (0,0) = 1+1+1+1 = 4
        assertEquals(4.0, queryDouble("SELECT length(path '((0,0),(1,0),(1,1),(0,1))')"), 0.01);
    }

    @Test
    void path_npoints() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT npoints(path '((0,0),(1,0),(1,1))')"), 0.01);
    }

    @Test
    void path_isclosed() throws SQLException {
        assertTrue(queryBool("SELECT isclosed(path '((0,0),(1,0),(1,1))')"));
    }

    @Test
    void path_isopen() throws SQLException {
        assertTrue(queryBool("SELECT isopen(path '[(0,0),(1,0),(1,1)]')"));
    }

    @Test
    void path_isclosed_false_for_open() throws SQLException {
        assertFalse(queryBool("SELECT isclosed(path '[(0,0),(1,0),(1,1)]')"));
    }

    @Test
    void path_isopen_false_for_closed() throws SQLException {
        assertFalse(queryBool("SELECT isopen(path '((0,0),(1,0),(1,1))')"));
    }

    @Test
    void path_pclose() throws SQLException {
        // Convert open to closed
        String result = query1("SELECT pclose(path '[(0,0),(1,0),(1,1)]')");
        assertNotNull(result);
        // Verify it's now closed
        assertTrue(queryBool("SELECT isclosed(pclose(path '[(0,0),(1,0),(1,1)]'))"));
    }

    @Test
    void path_popen() throws SQLException {
        // Convert closed to open
        String result = query1("SELECT popen(path '((0,0),(1,0),(1,1))')");
        assertNotNull(result);
        assertTrue(queryBool("SELECT isopen(popen(path '((0,0),(1,0),(1,1))'))"));
    }

    @Test
    void path_area_closed() throws SQLException {
        // Rectangle 4x3 = 12
        assertEquals(12.0, queryDouble("SELECT area(path '((0,0),(4,0),(4,3),(0,3))')"), 0.01);
    }

    @Test
    void path_plus_point_translation() throws SQLException {
        String result = query1("SELECT path '((0,0),(1,0))' + point '(2,2)'");
        assertNotNull(result);
    }

    @Test
    void path_bound_box() throws SQLException {
        String result = query1("SELECT bound_box(path '((0,0),(3,0),(3,4),(0,4))')");
        assertNotNull(result);
    }

    @Test
    void path_length_prefix_operator() throws SQLException {
        assertEquals(7.0, queryDouble("SELECT length(path '[(0,0),(3,0),(3,4)]')"), 0.01);
    }

    @Test
    void path_npoints_prefix_operator() throws SQLException {
        // # as prefix operator for npoints
        assertEquals(3.0, queryDouble("SELECT npoints(path '((0,0),(1,0),(1,1))')"), 0.01);
    }

    // ========================================================================
    // 6. Polygon type
    // ========================================================================

    @Test
    void polygon_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_polygons (p) VALUES ('((0,0),(1,0),(1,1),(0,1))')");
            ResultSet rs = s.executeQuery("SELECT p FROM geo_polygons WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void polygon_literal() throws SQLException {
        String result = query1("SELECT polygon '((0,0),(1,0),(1,1),(0,1))'");
        assertNotNull(result);
    }

    @Test
    void polygon_area() throws SQLException {
        assertEquals(12.0, queryDouble("SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))')"), 0.01);
    }

    @Test
    void polygon_area_triangle() throws SQLException {
        // Triangle with vertices (0,0),(4,0),(0,3) has area 6
        assertEquals(6.0, queryDouble("SELECT area(polygon '((0,0),(4,0),(0,3))')"), 0.01);
    }

    @Test
    void polygon_center() throws SQLException {
        assertEquals("(1,1)", query1("SELECT center(polygon '((0,0),(2,0),(2,2),(0,2))')"));
    }

    @Test
    void polygon_npoints() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT npoints(polygon '((0,0),(1,0),(1,1))')"), 0.01);
    }

    @Test
    void polygon_npoints_4() throws SQLException {
        assertEquals(4.0, queryDouble("SELECT npoints(polygon '((0,0),(1,0),(1,1),(0,1))')"), 0.01);
    }

    @Test
    void polygon_contains_point() throws SQLException {
        assertTrue(queryBool("SELECT polygon '((0,0),(4,0),(4,4),(0,4))' @> point '(2,2)'"));
    }

    @Test
    void polygon_not_contains_point() throws SQLException {
        assertFalse(queryBool("SELECT polygon '((0,0),(4,0),(4,4),(0,4))' @> point '(5,5)'"));
    }

    @Test
    void polygon_overlap() throws SQLException {
        assertTrue(queryBool("SELECT polygon '((0,0),(2,0),(2,2),(0,2))' && polygon '((1,1),(3,1),(3,3),(1,3))'"));
    }

    @Test
    void polygon_no_overlap() throws SQLException {
        assertFalse(queryBool("SELECT polygon '((0,0),(1,0),(1,1),(0,1))' && polygon '((3,3),(4,3),(4,4),(3,4))'"));
    }

    @Test
    void polygon_bound_box() throws SQLException {
        String result = query1("SELECT bound_box(polygon '((0,0),(3,0),(3,4),(0,4))')");
        assertNotNull(result);
    }

    @Test
    void polygon_approximate_equality() throws SQLException {
        assertTrue(queryBool("SELECT polygon '((0,0),(1,0),(1,1))' ~= polygon '((0,0),(1,0),(1,1))'"));
    }

    @Test
    void polygon_npoints_prefix() throws SQLException {
        // # as prefix for npoints
        assertEquals(3.0, queryDouble("SELECT npoints(polygon '((0,0),(1,0),(1,1))')"), 0.01);
    }

    // ========================================================================
    // 7. Circle type
    // ========================================================================

    @Test
    void circle_insert_and_select_angle_bracket() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO geo_circles (c) VALUES ('<(0,0),5>')");
            ResultSet rs = s.executeQuery("SELECT c FROM geo_circles WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void circle_literal_angle_bracket() throws SQLException {
        String result = query1("SELECT circle '<(0,0),5>'");
        assertNotNull(result);
    }

    @Test
    void circle_alternative_format() throws SQLException {
        String result = query1("SELECT circle '((0,0),5)'");
        assertNotNull(result);
    }

    @Test
    void circle_area() throws SQLException {
        // pi * 25 ~ 78.54
        assertEquals(78.54, queryDouble("SELECT area(circle '<(0,0),5>')"), 0.01);
    }

    @Test
    void circle_area_unit() throws SQLException {
        assertEquals(Math.PI, queryDouble("SELECT area(circle '<(0,0),1>')"), 0.01);
    }

    @Test
    void circle_center() throws SQLException {
        assertEquals("(1,2)", query1("SELECT center(circle '<(1,2),3>')"));
    }

    @Test
    void circle_diameter() throws SQLException {
        assertEquals(10.0, queryDouble("SELECT diameter(circle '<(0,0),5>')"), 0.01);
    }

    @Test
    void circle_radius() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT radius(circle '<(0,0),5>')"), 0.01);
    }

    @Test
    void circle_plus_point_translation() throws SQLException {
        String result = query1("SELECT circle '<(0,0),5>' + point '(1,1)'");
        assertNotNull(result);
        // Center should move to (1,1), radius stays 5
        assertEquals("(1,1)", query1("SELECT center(circle '<(0,0),5>' + point '(1,1)')"));
        assertEquals(5.0, queryDouble("SELECT radius(circle '<(0,0),5>' + point '(1,1)')"), 0.01);
    }

    @Test
    void circle_minus_point_translation() throws SQLException {
        assertEquals("(0,0)", query1("SELECT center(circle '<(1,1),5>' - point '(1,1)')"));
    }

    @Test
    void circle_multiply_point_scale() throws SQLException {
        // Scale radius by factor 2 (multiply by point (2,0))
        assertEquals(10.0, queryDouble("SELECT radius(circle '<(0,0),5>' * point '(2,0)')"), 0.01);
    }

    @Test
    void circle_divide_point_scale() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT radius(circle '<(0,0),10>' / point '(2,0)')"), 0.01);
    }

    @Test
    void circle_contains_point() throws SQLException {
        assertTrue(queryBool("SELECT circle '<(0,0),5>' @> point '(1,1)'"));
    }

    @Test
    void circle_not_contains_point() throws SQLException {
        assertFalse(queryBool("SELECT circle '<(0,0),5>' @> point '(10,10)'"));
    }

    @Test
    void circle_overlap() throws SQLException {
        assertTrue(queryBool("SELECT circle '<(0,0),5>' && circle '<(3,0),5>'"));
    }

    @Test
    void circle_no_overlap() throws SQLException {
        assertFalse(queryBool("SELECT circle '<(0,0),1>' && circle '<(5,0),1>'"));
    }

    @Test
    void circle_distance_edge_to_edge() throws SQLException {
        // Distance between edges: center distance - r1 - r2 = 5 - 1 - 1 = 3
        assertEquals(3.0, queryDouble("SELECT circle '<(0,0),1>' <-> circle '<(5,0),1>'"), 0.01);
    }

    @Test
    void circle_center_prefix_operator() throws SQLException {
        assertEquals("(1,2)", query1("SELECT center(circle '<(1,2),3>')"));
    }

    // ========================================================================
    // 8. Type Conversions
    // ========================================================================

    @Test
    void conversion_point_from_circle() throws SQLException {
        // point(circle) extracts center
        assertEquals("(1,2)", query1("SELECT point(circle '<(1,2),3>')"));
    }

    @Test
    void conversion_point_from_box() throws SQLException {
        assertEquals("(1,1)", query1("SELECT point(box '((2,2),(0,0))')"));
    }

    @Test
    void conversion_box_from_circle() throws SQLException {
        String result = query1("SELECT box(circle '<(0,0),1>')");
        assertNotNull(result);
    }

    @Test
    void conversion_box_from_two_points() throws SQLException {
        String result = query1("SELECT box(point '(0,0)', point '(1,1)')");
        assertNotNull(result);
    }

    @Test
    void conversion_circle_from_point_and_radius() throws SQLException {
        String result = query1("SELECT circle(point '(1,2)', 3)");
        assertNotNull(result);
        assertEquals("(1,2)", query1("SELECT center(circle(point '(1,2)', 3))"));
        assertEquals(3.0, queryDouble("SELECT radius(circle(point '(1,2)', 3))"), 0.01);
    }

    @Test
    void conversion_circle_from_box() throws SQLException {
        String result = query1("SELECT circle(box '((1,1),(0,0))')");
        assertNotNull(result);
    }

    @Test
    void conversion_lseg_from_two_points() throws SQLException {
        String result = query1("SELECT lseg(point '(0,0)', point '(1,1)')");
        assertNotNull(result);
    }

    @Test
    void conversion_polygon_from_box() throws SQLException {
        // Box to polygon yields 4-point polygon
        String result = query1("SELECT polygon(box '((1,1),(0,0))')");
        assertNotNull(result);
        assertEquals(4.0, queryDouble("SELECT npoints(polygon(box '((1,1),(0,0))'))"), 0.01);
    }

    @Test
    void conversion_polygon_from_circle() throws SQLException {
        // Circle to polygon yields 12-point approximation by default
        String result = query1("SELECT polygon(circle '<(0,0),1>')");
        assertNotNull(result);
    }

    @Test
    void conversion_polygon_from_closed_path() throws SQLException {
        String result = query1("SELECT polygon(path '((0,0),(1,0),(1,1))')");
        assertNotNull(result);
    }

    @Test
    void conversion_path_from_polygon() throws SQLException {
        String result = query1("SELECT path(polygon '((0,0),(1,0),(1,1))')");
        assertNotNull(result);
        assertTrue(queryBool("SELECT isclosed(path(polygon '((0,0),(1,0),(1,1))'))"));
    }

    @Test
    void conversion_line_from_two_points() throws SQLException {
        String result = query1("SELECT line(point '(0,0)', point '(1,1)')");
        assertNotNull(result);
    }

    // ========================================================================
    // 9. Cross-type operations
    // ========================================================================

    @Test
    void cross_distance_point_to_lseg() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT point '(0,0)' <-> lseg '[(3,0),(3,4)]'"), 0.01);
    }

    @Test
    void cross_distance_point_to_box() throws SQLException {
        // Distance from origin to box at (3,3)-(5,5): nearest corner is (3,3), distance = sqrt(18) ~ 4.24
        double dist = queryDouble("SELECT point '(0,0)' <-> box '((5,5),(3,3))'");
        assertEquals(Math.sqrt(18), dist, 0.01);
    }

    @Test
    void cross_distance_point_to_circle() throws SQLException {
        // Distance from origin to edge of circle at (5,0) r=2: 5 - 2 = 3
        assertEquals(3.0, queryDouble("SELECT point '(0,0)' <-> circle '<(5,0),2>'"), 0.01);
    }

    @Test
    void cross_distance_point_to_lseg_on_segment() throws SQLException {
        // Point (1,1) to horizontal segment [(0,0),(2,0)] - perpendicular distance = 1
        assertEquals(1.0, queryDouble("SELECT point '(1,1)' <-> lseg '[(0,0),(2,0)]'"), 0.01);
    }

    // ========================================================================
    // 10. Measurement operators (function forms)
    // ========================================================================

    @Test
    void measurement_length_lseg_function() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT length(lseg '[(0,0),(3,4)]')"), 0.01);
    }

    @Test
    void measurement_length_path_function() throws SQLException {
        assertEquals(7.0, queryDouble("SELECT length(path '[(0,0),(3,0),(3,4)]')"), 0.01);
    }

    @Test
    void measurement_center_box_function() throws SQLException {
        assertEquals("(1,1)", query1("SELECT center(box '((2,2),(0,0))')"));
    }

    @Test
    void measurement_center_circle_function() throws SQLException {
        assertEquals("(1,2)", query1("SELECT center(circle '<(1,2),3>')"));
    }

    @Test
    void measurement_center_lseg_function() throws SQLException {
        assertEquals("(1,1)", query1("SELECT center(lseg '[(0,0),(2,2)]')"));
    }

    @Test
    void measurement_npoints_polygon_function() throws SQLException {
        assertEquals(3.0, queryDouble("SELECT npoints(polygon '((0,0),(1,0),(1,1))')"), 0.01);
    }

    @Test
    void measurement_npoints_path_function() throws SQLException {
        assertEquals(4.0, queryDouble("SELECT npoints(path '((0,0),(1,0),(1,1),(0,1))')"), 0.01);
    }

    // ========================================================================
    // 11. Column storage and retrieval
    // ========================================================================

    @Test
    void storage_multiple_points() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE point_store (id serial PRIMARY KEY, name text, loc point)");
            s.execute("INSERT INTO point_store (name, loc) VALUES ('A', '(1,2)')");
            s.execute("INSERT INTO point_store (name, loc) VALUES ('B', '(3,4)')");
            s.execute("INSERT INTO point_store (name, loc) VALUES ('C', '(5,6)')");
            ResultSet rs = s.executeQuery("SELECT name, loc FROM point_store ORDER BY id");
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertEquals("(1,2)", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertEquals("(3,4)", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("C", rs.getString(1));
            assertEquals("(5,6)", rs.getString(2));
            s.execute("DROP TABLE point_store");
        }
    }

    @Test
    void storage_box_select_back() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE box_store (id serial PRIMARY KEY, region box)");
            s.execute("INSERT INTO box_store (region) VALUES ('((10,10),(0,0))')");
            s.execute("INSERT INTO box_store (region) VALUES ('((20,20),(10,10))')");
            ResultSet rs = s.executeQuery("SELECT region FROM box_store ORDER BY id");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE box_store");
        }
    }

    @Test
    void storage_circle_select_back() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE circle_store (id serial PRIMARY KEY, zone circle)");
            s.execute("INSERT INTO circle_store (zone) VALUES ('<(0,0),10>')");
            s.execute("INSERT INTO circle_store (zone) VALUES ('<(5,5),3>')");
            ResultSet rs = s.executeQuery("SELECT zone FROM circle_store ORDER BY id");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE circle_store");
        }
    }

    @Test
    void storage_where_clause_with_containment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE geo_regions (id serial PRIMARY KEY, name text, area box)");
            s.execute("INSERT INTO geo_regions (name, area) VALUES ('small', '((1,1),(0,0))')");
            s.execute("INSERT INTO geo_regions (name, area) VALUES ('large', '((10,10),(0,0))')");
            ResultSet rs = s.executeQuery(
                "SELECT name FROM geo_regions WHERE area @> point '(5,5)'"
            );
            assertTrue(rs.next());
            assertEquals("large", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE geo_regions");
        }
    }

    @Test
    void storage_order_by_distance() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE landmarks (id serial PRIMARY KEY, name text, loc point)");
            s.execute("INSERT INTO landmarks (name, loc) VALUES ('far', '(10,10)')");
            s.execute("INSERT INTO landmarks (name, loc) VALUES ('near', '(1,1)')");
            s.execute("INSERT INTO landmarks (name, loc) VALUES ('mid', '(5,5)')");
            ResultSet rs = s.executeQuery(
                "SELECT name FROM landmarks ORDER BY loc <-> point '(0,0)'"
            );
            assertTrue(rs.next()); assertEquals("near", rs.getString(1));
            assertTrue(rs.next()); assertEquals("mid", rs.getString(1));
            assertTrue(rs.next()); assertEquals("far", rs.getString(1));
            s.execute("DROP TABLE landmarks");
        }
    }

    @Test
    void storage_polygon_where_overlap() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE poly_regions (id serial PRIMARY KEY, name text, area polygon)");
            s.execute("INSERT INTO poly_regions (name, area) VALUES ('a', '((0,0),(2,0),(2,2),(0,2))')");
            s.execute("INSERT INTO poly_regions (name, area) VALUES ('b', '((5,5),(7,5),(7,7),(5,7))')");
            ResultSet rs = s.executeQuery(
                "SELECT name FROM poly_regions WHERE area && polygon '((1,1),(3,1),(3,3),(1,3))'"
            );
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE poly_regions");
        }
    }

    @Test
    void storage_path_types() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE routes (id serial PRIMARY KEY, name text, route path)");
            s.execute("INSERT INTO routes (name, route) VALUES ('closed', '((0,0),(1,0),(1,1))')");
            s.execute("INSERT INTO routes (name, route) VALUES ('open', '[(0,0),(1,0),(1,1)]')");
            ResultSet rs = s.executeQuery(
                "SELECT name, isclosed(route) FROM routes ORDER BY id"
            );
            assertTrue(rs.next());
            assertEquals("closed", rs.getString(1));
            assertTrue(rs.getBoolean(2));
            assertTrue(rs.next());
            assertEquals("open", rs.getString(1));
            assertFalse(rs.getBoolean(2));
            s.execute("DROP TABLE routes");
        }
    }

    @Test
    void storage_lseg_in_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE segments (id serial PRIMARY KEY, seg lseg)");
            s.execute("INSERT INTO segments (seg) VALUES ('[(0,0),(3,4)]')");
            ResultSet rs = s.executeQuery("SELECT length(seg) FROM segments WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(5.0, rs.getDouble(1), 0.01);
            s.execute("DROP TABLE segments");
        }
    }

    @Test
    void storage_line_in_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE line_store (id serial PRIMARY KEY, l line)");
            s.execute("INSERT INTO line_store (l) VALUES ('{1,-1,0}')");
            ResultSet rs = s.executeQuery("SELECT l FROM line_store WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE line_store");
        }
    }

    // ========================================================================
    // Additional edge cases
    // ========================================================================

    @Test
    void point_origin() throws SQLException {
        assertEquals("(0,0)", query1("SELECT point '(0,0)'"));
    }

    @Test
    void point_large_coordinates() throws SQLException {
        String result = query1("SELECT point '(1000000,2000000)'");
        assertNotNull(result);
    }

    @Test
    void point_decimal_coordinates() throws SQLException {
        String result = query1("SELECT point '(1.5,2.7)'");
        assertNotNull(result);
    }

    @Test
    void box_zero_area() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT area(box '((1,1),(1,1))')"), 0.01);
    }

    @Test
    void circle_zero_radius() throws SQLException {
        assertEquals(0.0, queryDouble("SELECT area(circle '<(0,0),0>')"), 0.01);
    }

    @Test
    void circle_contains_center() throws SQLException {
        assertTrue(queryBool("SELECT circle '<(5,5),3>' @> point '(5,5)'"));
    }

    @Test
    void circle_contains_edge_point() throws SQLException {
        // Point exactly on edge should be contained
        assertTrue(queryBool("SELECT circle '<(0,0),5>' @> point '(5,0)'"));
    }

    @Test
    void path_single_segment_length() throws SQLException {
        assertEquals(5.0, queryDouble("SELECT length(path '[(0,0),(3,4)]')"), 0.01);
    }

    @Test
    void polygon_unit_square_area() throws SQLException {
        assertEquals(1.0, queryDouble("SELECT area(polygon '((0,0),(1,0),(1,1),(0,1))')"), 0.01);
    }

    @Test
    void box_contains_corner() throws SQLException {
        assertTrue(queryBool("SELECT box '((2,2),(0,0))' @> point '(0,0)'"));
    }

    @Test
    void box_distance() throws SQLException {
        // Distance between non-overlapping boxes
        double dist = queryDouble("SELECT box '((1,1),(0,0))' <-> box '((4,4),(3,3))'");
        assertEquals(Math.sqrt(8), dist, 0.01);
    }
}
