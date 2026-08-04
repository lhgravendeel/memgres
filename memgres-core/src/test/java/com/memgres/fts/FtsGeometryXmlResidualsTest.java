package com.memgres.fts;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual FTS / geometry / XML bug fixes verified against real PostgreSQL 18.
 * Covers: H30 (tsquery parse errors + double negation), H31 (phantom/boolean/unary
 * geometry operators), H32 (float output formatting), H33 (polygon(circle) winding),
 * M18 (ts_stat SRF, ts_headline tag spacing, position cap), H34 (xpath namespaces +
 * array quoting), L15 (no Java class-name leaks) and N46 (EXECUTE USING/INTO order).
 */
class FtsGeometryXmlResidualsTest {

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

    private String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next());
            return r.getString(1);
        }
    }

    private String expectError(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState() + ":" + e.getMessage();
        }
    }

    // ---- H30: tsquery construction ----

    @Test
    void tsquery_two_lexemes_no_operator_is_syntax_error() {
        String err = expectError("SELECT 'fat rat'::tsquery");
        assertNotNull(err);
        assertTrue(err.startsWith("42601"), err);
        assertTrue(err.contains("syntax error in tsquery"), err);
    }

    @Test
    void tsquery_dangling_operator_is_no_operand_error() {
        String err = expectError("SELECT 'cat &'::tsquery");
        assertNotNull(err);
        assertTrue(err.startsWith("42601"), err);
        assertTrue(err.contains("no operand in tsquery"), err);
    }

    @Test
    void to_tsquery_invalid_input_errors() {
        assertTrue(expectError("SELECT to_tsquery('fat rat')").startsWith("42601"));
        assertTrue(expectError("SELECT to_tsquery('cat &')").startsWith("42601"));
    }

    @Test
    void tsquery_double_negation_preserved() throws SQLException {
        assertEquals("!!'cat'", scalar("SELECT '!!cat'::tsquery::text"));
        assertEquals("!'cat'", scalar("SELECT '!cat'::tsquery::text"));
    }

    @Test
    void tsquery_valid_input_still_parses() throws SQLException {
        assertEquals("'cat' & 'dog'", scalar("SELECT to_tsquery('cat & dog')::text"));
    }

    // ---- H31: geometry operator dispatch ----

    @Test
    void phantom_geometry_operators_error_42883() {
        for (String sql : new String[]{
                "SELECT line '{1,2,3}' ## point '(1,2)'",
                "SELECT box '((0,0),(1,1))' ## point '(1,2)'",
                "SELECT line '{1,2,3}' ?# lseg '((0,0),(1,1))'",
                "SELECT line '{1,-1,0}' @> point '(1,2)'"}) {
            String err = expectError(sql);
            assertNotNull(err, sql);
            assertTrue(err.startsWith("42883"), sql + " => " + err);
            assertTrue(err.contains("operator does not exist"), sql + " => " + err);
        }
    }

    @Test
    void point_alignment_operators_return_boolean() throws SQLException {
        assertEquals("true", scalar("SELECT (point '(1,2)' ?- point '(3,2)')::text"));
        assertEquals("false", scalar("SELECT (point '(1,2)' ?- point '(3,9)')::text"));
        assertEquals("true", scalar("SELECT (point '(1,2)' ?| point '(1,5)')::text"));
        assertEquals("false", scalar("SELECT (point '(1,2)' ?| point '(4,5)')::text"));
    }

    @Test
    void unary_geometry_operators_work() throws SQLException {
        assertEquals("(1,1)", scalar("SELECT (@@ box '((0,0),(2,2))')::text"));
        assertEquals("5", scalar("SELECT @-@ lseg '((0,0),(3,4))'"));
        assertEquals("3", scalar("SELECT # path '((0,0),(1,1),(2,2))'"));
    }

    // ---- H32: float output formatting ----

    @Test
    void geometry_float_formatting_matches_pg() throws SQLException {
        assertEquals("(1e+300,2e-300)", scalar("SELECT (point '(1e300,2e-300)')::text"));
        assertEquals("(12345678.5,0)", scalar("SELECT (point '(12345678.5,0)')::text"));
        assertEquals("(-0,3)", scalar("SELECT (point '(-0,3)')::text"));
        assertEquals("(1e-06,1)", scalar("SELECT (point '(1e-6,1)')::text"));
        assertEquals("(0.0001,1)", scalar("SELECT (point '(0.0001,1)')::text"));
        assertEquals("(1e-05,1)", scalar("SELECT (point '(0.00001,1)')::text"));
        assertEquals("(100000000,1)", scalar("SELECT (point '(100000000,1)')::text"));
        assertEquals("(1e+15,1)", scalar("SELECT (point '(1e15,1)')::text"));
    }

    // ---- H33: polygon(circle) winding ----

    @Test
    void polygon_from_circle_winds_like_pg() throws SQLException {
        // PG starts the first vertex at (cx-r, cy) and winds counterclockwise.
        assertEquals("((-2,0),(-1.2246467991473532e-16,2),(2,2.4492935982947064e-16),(3.6739403974420594e-16,-2))",
                scalar("SELECT polygon(4, circle '<(0,0),2>')::text"));
    }

    // ---- M18: FTS misc ----

    @Test
    void ts_stat_is_a_set_returning_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zz_tsstat(v tsvector)");
            s.execute("INSERT INTO zz_tsstat VALUES (to_tsvector('simple','cat cat dog')), (to_tsvector('simple','cat bird'))");
            try (ResultSet r = s.executeQuery("SELECT word, ndoc, nentry FROM ts_stat('SELECT v FROM zz_tsstat') ORDER BY word")) {
                assertTrue(r.next());
                assertEquals("bird", r.getString(1));
                assertEquals(1, r.getInt(2));
                assertEquals(1, r.getInt(3));
                assertTrue(r.next());
                assertEquals("cat", r.getString(1));
                assertEquals(2, r.getInt(2));
                assertEquals(3, r.getInt(3));
                assertTrue(r.next());
                assertEquals("dog", r.getString(1));
            }
            s.execute("DROP TABLE zz_tsstat");
        }
    }

    @Test
    void ts_headline_replaces_tags_with_spaces() throws SQLException {
        String h = scalar("SELECT ts_headline('english','the big<br>cat sat', to_tsquery('cat'))");
        assertTrue(h.contains("big <b>cat</b>"), h);
        assertFalse(h.contains("bigcat"), h);
    }

    @Test
    void position_cap_is_255() throws SQLException {
        // Repeating a lexeme 300 times must cap the position list at 255.
        String t = scalar("SELECT to_tsvector('simple', repeat('cat ', 300))::text");
        int commas = t.length() - t.replace(",", "").length();
        assertEquals(254, commas, t); // 255 positions => 254 separators
    }

    // ---- H34: xpath ----

    @Test
    void xpath_element_results_unquoted_unless_special() throws SQLException {
        assertEquals("{<x>1</x>,<x>2</x>}",
                scalar("SELECT xpath('//x','<root><x>1</x><x>2</x></root>')::text"));
        // Text node containing a space is quoted.
        assertEquals("{\"a b\"}",
                scalar("SELECT xpath('/root/x/text()','<root><x>a b</x></root>')::text"));
    }

    @Test
    void xpath_namespace_mapping_resolves() throws SQLException {
        assertEquals("{hi}",
                scalar("SELECT xpath('//c:item/text()','<root xmlns:c=\"http://ex.com\">"
                        + "<c:item>hi</c:item></root>', ARRAY[ARRAY['c','http://ex.com']])::text"));
    }

    // ---- L15: no Java class-name leaks ----

    @Test
    void geometry_error_messages_use_pg_type_names() {
        String err = expectError("SELECT line '{1,-1,0}' @> point '(1,2)'");
        assertNotNull(err);
        assertTrue(err.contains("line @> point"), err);
        assertFalse(err.contains("PgLine") || err.contains("PgPoint") || err.contains("PgPolygon"), err);
    }

    // ---- N46: EXECUTE USING ... INTO (reversed order) ----

    @Test
    void execute_using_then_into_reversed_order() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION zz_exec_rev() RETURNS int AS $$\n"
                    + "DECLARE r int;\n"
                    + "BEGIN\n"
                    + "  EXECUTE 'SELECT $1 + $2' USING 2, 3 INTO r;\n"
                    + "  RETURN r;\n"
                    + "END; $$ LANGUAGE plpgsql");
            assertEquals("5", scalar("SELECT zz_exec_rev()"));
            s.execute("DROP FUNCTION zz_exec_rev()");
        }
    }

    @Test
    void execute_into_then_using_normal_order_still_works() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE FUNCTION zz_exec_norm() RETURNS int AS $$\n"
                    + "DECLARE r int;\n"
                    + "BEGIN\n"
                    + "  EXECUTE 'SELECT $1 + $2' INTO r USING 4, 6;\n"
                    + "  RETURN r;\n"
                    + "END; $$ LANGUAGE plpgsql");
            assertEquals("10", scalar("SELECT zz_exec_norm()"));
            s.execute("DROP FUNCTION zz_exec_norm()");
        }
    }

    // ---- The closest-point operator over a segment and a box, and over two segments ----
    //
    // Every expectation below was read from the reference server. A segment that meets a box is
    // answered with a point inside the box -- its own point nearest the box's centre -- and one
    // that misses with a point of the nearest side, the sides being walked left, top, right,
    // bottom so that the ties fall the way PostgreSQL's do.

    @Test
    void closest_point_on_a_box_is_the_point_of_the_nearest_side() throws SQLException {
        assertEquals("(2,4)", scalar("SELECT lseg '[(4,0),(4,4)]' ## box '(-2,1),(2,4)'"));
        assertEquals("(3,3)", scalar("SELECT lseg '[(4,0),(4,4)]' ## box '(0,0),(3,3)'"));
        assertEquals("(0,0)", scalar("SELECT lseg '[(-2,0),(-2,3)]' ## box '(0,0),(3,3)'"));
        assertEquals("(0,3)", scalar("SELECT lseg '[(-10,10),(10,10)]' ## box '(0,0),(3,3)'"));
        assertEquals("(-5,-1)", scalar("SELECT lseg '[(-10,10),(10,10)]' ## box '(-5,-5),(-1,-1)'"));
    }

    @Test
    void closest_point_on_a_box_the_segment_meets_is_inside_it() throws SQLException {
        assertEquals("(0.24999999999999997,1.75)",
                scalar("SELECT lseg '[(1,2),(-2,1)]' ## box '(-2,1),(2,4)'"));
        assertEquals("(1.25,1.25)", scalar("SELECT lseg '[(0,0),(6,6)]' ## box '(-2,1),(2,4)'"));
        assertEquals("(1,2)", scalar("SELECT lseg '[(1,2),(-2,1)]' ## box '(0,0),(3,3)'"));
        assertEquals("(-3,-3)", scalar("SELECT lseg '[(-3,-3),(-3,-3)]' ## box '(-5,-5),(-1,-1)'"));
    }

    @Test
    void closest_point_on_the_second_segment_of_two() throws SQLException {
        assertEquals("(-2,1)", scalar("SELECT lseg '[(-3,-3),(-3,-3)]' ## lseg '[(1,2),(-2,1)]'"));
        assertEquals("(1,2)", scalar("SELECT lseg '[(5,0),(0,5)]' ## lseg '[(1,2),(-2,1)]'"));
        assertEquals("(1.4285714285714286,3.571428571428571)",
                scalar("SELECT lseg '[(-2,1),(2,4)]' ## lseg '[(5,0),(0,5)]'"));
        assertEquals("(4,4)", scalar("SELECT lseg '[(2,4),(6,8)]' ## lseg '[(4,0),(4,4)]'"));
    }

    @Test
    void two_segments_of_the_same_slope_have_no_closest_point() throws SQLException {
        assertNull(scalar("SELECT lseg '[(2,4),(6,8)]' ## lseg '[(0,0),(6,6)]'"));
        assertNull(scalar("SELECT lseg '[(-10,10),(10,10)]' ## lseg '[(-10,10),(10,10)]'"));
        // A segment whose endpoints coincide counts as vertical, and so does a vertical one.
        assertNull(scalar("SELECT lseg '[(1,1),(1,1)]' ## lseg '[(4,0),(4,4)]'"));
        assertNull(scalar("SELECT lseg '[(1,1),(1,1)]' ## lseg '[(3,3),(3,3)]'"));
    }

    @Test
    void closest_point_of_a_line_and_a_segment_runs_that_way_round_only() throws SQLException {
        assertEquals("(0,1.6666666666666667)", scalar("SELECT line '{1,0,0}' ## lseg '[(1,2),(-2,1)]'"));
        assertEquals("(0,2)", scalar("SELECT line '{1,0,0}' ## lseg '[(-5,2),(5,2)]'"));
        assertNull(scalar("SELECT line '{1,-1,3}' ## lseg '[(0,0),(4,4)]'"));

        for (String sql : new String[]{
                "SELECT lseg '[(0,0),(4,4)]' ## line '{1,-1,3}'",
                "SELECT box '(-2,1),(2,4)' ## lseg '[(4,0),(4,4)]'"}) {
            String err = expectError(sql);
            assertNotNull(err, sql);
            assertTrue(err.startsWith("42883"), sql + " => " + err);
            assertTrue(err.contains("operator does not exist"), sql + " => " + err);
        }
    }

    @Test
    void the_functions_the_closest_point_operator_is_recorded_as_are_callable() throws SQLException {
        assertEquals("(1,2)",
                scalar("SELECT close_lseg(lseg '[(4,0),(4,4)]', lseg '[(1,2),(-2,1)]')::text"));
        assertEquals("(2,4)",
                scalar("SELECT close_sb(lseg '[(4,0),(4,4)]', box '(-2,1),(2,4)')::text"));
        assertEquals("(0,0)",
                scalar("SELECT close_ls(line '{1,0,0}', lseg '[(0,0),(4,4)]')::text"));
        assertEquals("(1,0)", scalar("SELECT close_ps(point '(1,1)', lseg '[(0,0),(4,0)]')::text"));
    }
}
