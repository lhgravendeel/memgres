package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Geometry correctness tests for H31, H32, H33, L14, L15.
 */
class GeometryCorrectnessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private boolean qBool(String sql) throws SQLException {
        return "t".equals(q(sql));
    }

    // ========================================================================
    // H32: Output formatting
    // ========================================================================

    @Test
    void h32_fmtD_largeCoordinates() throws SQLException {
        // 1e300 should not overflow to Long.MAX_VALUE
        String result = q("SELECT point '(1e300,2e-300)'");
        assertNotNull(result);
        assertFalse(result.contains("9223372036854775807"), "Should not overflow to Long.MAX_VALUE");
        // Should not contain Java E-notation like 2.0E-300
        assertFalse(result.contains("E-"), "Should not use Java E-notation: " + result);
    }

    @Test
    void h32_negativeZero() throws SQLException {
        // PG normalizes -0 to 0 in geometry output
        String result = q("SELECT point '(-0,3)'");
        assertNotNull(result);
        assertEquals("(0,3)", result);
    }

    @Test
    void h32_nanCoordinates() throws SQLException {
        String result = q("SELECT point '(NaN,NaN)'");
        assertNotNull(result);
        assertTrue(result.contains("NaN"), "Should accept and display NaN: " + result);
    }

    @Test
    void h32_infinityCoordinates() throws SQLException {
        String result = q("SELECT point '(Infinity,-Infinity)'");
        assertNotNull(result);
        assertTrue(result.contains("Infinity"), "Should accept and display Infinity: " + result);
    }

    @Test
    void h32_normalFormatting() throws SQLException {
        assertEquals("(1,2)", q("SELECT point '(1,2)'"));
        assertEquals("(1.5,2.5)", q("SELECT point '(1.5,2.5)'"));
    }

    // ========================================================================
    // H33: Containment boundary
    // ========================================================================

    @Test
    void h33_polygonBoundaryContained() throws SQLException {
        // Point on polygon edge should be contained (PG behavior)
        assertTrue(qBool("SELECT polygon '((0,0),(4,0),(4,4),(0,4))' @> point '(2,0)'"),
            "Point on boundary should be contained");
    }

    @Test
    void h33_polygonContainsItself() throws SQLException {
        assertTrue(qBool("SELECT polygon '((0,0),(1,0),(1,1),(0,1))' @> polygon '((0,0),(1,0),(1,1),(0,1))'"),
            "Polygon should contain itself");
    }

    @Test
    void h33_polygonContainsVertex() throws SQLException {
        assertTrue(qBool("SELECT polygon '((0,0),(4,0),(4,4),(0,4))' @> point '(0,0)'"),
            "Vertex should be contained");
    }

    @Test
    void h33_lsegContainedInLine() throws SQLException {
        // lseg [(0,0),(2,2)] is on the line y=x
        assertTrue(qBool("SELECT lseg '[(0,0),(2,2)]' <@ line '{1,-1,0}'"),
            "Collinear lseg should be contained in line");
    }

    @Test
    void h33_lsegContainedInBox() throws SQLException {
        assertTrue(qBool("SELECT lseg '[(1,1),(2,2)]' <@ box '((0,0),(3,3))'"),
            "lseg inside box should be contained");
    }

    @Test
    void h33_circleBoxCircumscribed() throws SQLException {
        // circle(box '((0,0),(2,2))') should have radius = half-diagonal = sqrt(8)/2 ≈ 1.414
        String result = q("SELECT circle(box '((0,0),(2,2))')");
        assertNotNull(result);
        // PG: <(1,1),1.4142135623731>
        assertTrue(result.contains("1.414"), "circle(box) should be circumscribed, got: " + result);
    }

    @Test
    void h33_areaOpenPathNull() throws SQLException {
        // PG: area(open path) = NULL
        assertNull(q("SELECT area(path '[(0,0),(1,0),(1,1)]')"));
    }

    @Test
    void h33_areaClosedPath() throws SQLException {
        // Closed path area should work
        String result = q("SELECT area(path '((0,0),(4,0),(4,3),(0,3))')");
        assertNotNull(result);
        assertEquals("12", result.trim());
    }

    @Test
    void h33_areaPolygon() throws SQLException {
        // area(polygon) should work
        String result = q("SELECT area(polygon '((0,0),(4,0),(4,3),(0,3))')");
        assertNotNull(result);
        assertEquals("12", result.trim());
    }

    @Test
    void h33_boxFromPoint() throws SQLException {
        // box(point) should create zero-area box
        String result = q("SELECT box(point '(1,2)')");
        assertNotNull(result);
        assertEquals("(1,2),(1,2)", result.trim());
    }

    // ========================================================================
    // H31: Operator families
    // ========================================================================

    @Test
    void h31_pointDistancePath() throws SQLException {
        // point <-> path
        String result = q("SELECT point '(0,0)' <-> path '[(3,0),(3,4)]'");
        assertNotNull(result);
        assertEquals("3", result.trim());
    }

    @Test
    void h31_pointDistancePolygon() throws SQLException {
        // point <-> polygon (point inside)
        String result = q("SELECT point '(1,1)' <-> polygon '((0,0),(4,0),(4,4),(0,4))'");
        assertNotNull(result);
        assertEquals("0", result.trim());
    }

    @Test
    void h31_lsegDistanceBox() throws SQLException {
        // lseg <-> box
        String result = q("SELECT lseg '[(5,0),(5,2)]' <-> box '((0,0),(3,3))'");
        assertNotNull(result);
        assertEquals("2", result.trim());
    }

    @Test
    void h31_lsegIntersectsBox() throws SQLException {
        assertTrue(qBool("SELECT lseg '[(0,0),(5,5)]' ?# box '((1,1),(3,3))'"),
            "lseg should intersect box");
    }

    @Test
    void h31_lineParallelLine() throws SQLException {
        // Two parallel lines: y=0 and y=1
        assertTrue(qBool("SELECT line '{0,1,0}' ?|| line '{0,1,-1}'"),
            "Parallel lines should be detected");
    }

    @Test
    void h31_lineNotParallel() throws SQLException {
        assertFalse(qBool("SELECT line '{1,0,0}' ?|| line '{0,1,0}'"),
            "Perpendicular lines should not be parallel");
    }

    @Test
    void h31_containmentNotMisrouted() throws SQLException {
        // box @> point should work (not misrouted to range)
        assertTrue(qBool("SELECT box '((0,0),(2,2))' @> point '(1,1)'"));
    }

    @Test
    void h31_containedByNotMisrouted() throws SQLException {
        // point <@ box should work
        assertTrue(qBool("SELECT point '(1,1)' <@ box '((0,0),(2,2))'"));
    }

    @Test
    void h31_pointContainedInLseg() throws SQLException {
        assertTrue(qBool("SELECT point '(1,1)' <@ lseg '[(0,0),(2,2)]'"),
            "Point on lseg should be contained");
    }

    @Test
    void h31_pointContainedInLine() throws SQLException {
        assertTrue(qBool("SELECT point '(1,1)' <@ line '{1,-1,0}'"),
            "Point on line should be contained");
    }

    // ========================================================================
    // Misc/regression
    // ========================================================================

    @Test
    void centerLseg() throws SQLException {
        assertEquals("(1,1)", q("SELECT center(lseg '[(0,0),(2,2)]')"));
    }

    @Test
    void centerPolygon() throws SQLException {
        assertEquals("(1,1)", q("SELECT center(polygon '((0,0),(2,0),(2,2),(0,2))')"));
    }

    @Test
    void boxIntersection() throws SQLException {
        // # operator for box intersection
        String result = q("SELECT box '((0,0),(2,2))' # box '((1,1),(3,3))'");
        assertNotNull(result);
        assertEquals("(2,2),(1,1)", result.trim());
    }

    @Test
    void pathDistance() throws SQLException {
        // path <-> path
        String result = q("SELECT path '[(0,0),(1,0)]' <-> path '[(0,3),(1,3)]'");
        assertNotNull(result);
        assertEquals("3", result.trim());
    }

    @Test
    void circleDistancePolygon() throws SQLException {
        // circle <-> polygon (outside)
        String result = q("SELECT circle '<(10,0),1>' <-> polygon '((0,0),(2,0),(2,2),(0,2))'");
        assertNotNull(result);
        // Distance from center(10,0) to nearest edge at x=2 is 8, minus radius 1 = 7
        assertEquals("7", result.trim());
    }
}
