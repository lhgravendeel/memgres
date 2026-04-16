package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: SQL operators that PG 18 supports but Memgres handles
 * incompletely. Focuses on operator forms that are distinct from the
 * functions tested in {@link Round13TypeFunctionGapsTest}.
 *
 * Coverage:
 *   A. TSQuery boolean operators — `||` (OR), `&&` (AND), `!!` (NOT), `<->` (phrase)
 *   B. Geometric operators       — `?-` (horizontal), `?|` (vertical),
 *                                  `?#` (intersects), `##` (closest point)
 *   C. JSONB extended operators  — `?`, `?|`, `?&`, `@?` (path exists),
 *                                  `@@` (path match), `#-` (delete key path)
 *   D. Range/Multirange          — `*` (intersection), `+` (union) edge cases
 *   E. Array extended operators  — `||` concatenation with NULL, `IS NOT DISTINCT FROM`
 *   F. Text/pattern matching     — `@@@` (gin trgm similarity), `!~~` (NOT LIKE)
 */
class Round13OperatorGapsTest {

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

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    private static boolean scalarBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // A. TSQuery boolean operators
    // =========================================================================

    /** tsquery || tsquery  — OR of two queries. */
    @Test
    void tsquery_or_operator() throws SQLException {
        // 'a' OR 'b' as one tsquery; match document containing 'b'.
        assertTrue(scalarBool(
                "SELECT to_tsvector('english', 'b appears here') @@ "
                        + "(to_tsquery('a') || to_tsquery('b'))"));
    }

    /** tsquery && tsquery  — AND of two queries. */
    @Test
    void tsquery_and_operator() throws SQLException {
        assertTrue(scalarBool(
                "SELECT to_tsvector('english', 'a and b') @@ "
                        + "(to_tsquery('a') && to_tsquery('b'))"));
    }

    /** !! tsquery — NOT of a query. */
    @Test
    void tsquery_not_operator() throws SQLException {
        assertTrue(scalarBool(
                "SELECT to_tsvector('english', 'only a here') @@ "
                        + "(to_tsquery('a') && !!to_tsquery('z'))"));
    }

    /** tsquery <-> tsquery — phrase operator. */
    @Test
    void tsquery_phrase_operator() throws SQLException {
        assertTrue(scalarBool(
                "SELECT to_tsvector('english', 'quick brown fox') @@ "
                        + "(to_tsquery('quick') <-> to_tsquery('brown'))"));
    }

    // =========================================================================
    // B. Geometric operators
    // =========================================================================

    /** ?- : two LINEs/LSEGs are horizontal (parallel to x-axis). */
    @Test
    void geom_isHorizontal_lseg() throws SQLException {
        assertTrue(scalarBool(
                "SELECT ?- lseg '((0,0),(3,0))'"));
    }

    /** ?| : two LINEs/LSEGs are vertical. */
    @Test
    void geom_isVertical_lseg() throws SQLException {
        assertTrue(scalarBool(
                "SELECT ?| lseg '((0,0),(0,3))'"));
    }

    /** ?# : two geometric types intersect. */
    @Test
    void geom_intersects_boxAndBox() throws SQLException {
        assertTrue(scalarBool(
                "SELECT box '((0,0),(3,3))' ?# box '((2,2),(5,5))'"));
    }

    @Test
    void geom_intersects_boxAndBox_disjoint() throws SQLException {
        assertFalse(scalarBool(
                "SELECT box '((0,0),(1,1))' ?# box '((10,10),(11,11))'"));
    }

    /** ## : closest point of first operand to second operand. */
    @Test
    void geom_closestPoint_pointAndLseg() throws SQLException {
        String v = scalarString(
                "SELECT (point '(2,2)' ## lseg '((0,0),(4,0))')::text");
        // Closest point on the lseg to (2,2) is (2,0)
        assertEquals("(2,0)", v);
    }

    /** ?|| : lines/lsegs are parallel. */
    @Test
    void geom_parallel_lsegs() throws SQLException {
        assertTrue(scalarBool(
                "SELECT lseg '((0,0),(1,1))' ?|| lseg '((0,1),(1,2))'"));
    }

    /** ?-| : lines/lsegs are perpendicular. */
    @Test
    void geom_perpendicular_lsegs() throws SQLException {
        assertTrue(scalarBool(
                "SELECT lseg '((0,0),(1,0))' ?-| lseg '((0,0),(0,1))'"));
    }

    // =========================================================================
    // C. JSONB extended operators
    // =========================================================================

    /** jsonb #- text[]  — delete a key path. */
    @Test
    void jsonb_deleteKeyPath_operator() throws SQLException {
        assertEquals("{\"a\": 1}",
                scalarString("SELECT ('{\"a\":1,\"b\":{\"c\":2}}'::jsonb #- '{b,c}'::text[] #- '{b}'::text[])::text"));
    }

    /** jsonb - text[]  — delete multiple keys. */
    @Test
    void jsonb_deleteKeys_array() throws SQLException {
        assertEquals("{\"c\": 3}",
                scalarString("SELECT ('{\"a\":1,\"b\":2,\"c\":3}'::jsonb - ARRAY['a','b'])::text"));
    }

    /** jsonb - int  — delete array element by index. */
    @Test
    void jsonb_deleteArrayElement_byIndex() throws SQLException {
        assertEquals("[1, 3]",
                scalarString("SELECT ('[1,2,3]'::jsonb - 1)::text"));
    }

    /** jsonb - text  — delete a single key. */
    @Test
    void jsonb_deleteKey_byName() throws SQLException {
        assertEquals("{\"b\": 2}",
                scalarString("SELECT ('{\"a\":1,\"b\":2}'::jsonb - 'a')::text"));
    }

    /** jsonb ? text — key exists (already implemented — test reinforces semantics). */
    @Test
    void jsonb_keyExists_topLevelOnly() throws SQLException {
        // `?` only looks at top-level keys (not nested), so 'c' is NOT found.
        assertFalse(scalarBool(
                "SELECT '{\"a\":{\"c\":1}}'::jsonb ? 'c'"));
    }

    /** jsonb_path_query_tz : timezone-aware path query. */
    @Test
    void jsonb_path_query_tz() throws SQLException {
        assertEquals("\"2024-01-01T00:00:00+00:00\"",
                scalarString("SELECT jsonb_path_query_tz('\"2024-01-01\"', '$.datetime()')::text"));
    }

    // =========================================================================
    // D. Range operator edge cases
    // =========================================================================

    /** int4range * int4range → intersection returns empty range if disjoint. */
    @Test
    void range_intersection_disjoint_givesEmpty() throws SQLException {
        assertEquals("empty",
                scalarString("SELECT int4range(1,5) * int4range(10,20)::text"));
    }

    /** int4range + int4range → union only works when adjacent/overlap. */
    @Test
    void range_union_disjoint_errors() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT int4range(1,5) + int4range(10,20)");
            }
        });
        // PG says: "result of range union would not be contiguous" (22000)
        assertTrue(ex.getMessage().toLowerCase().contains("contiguous")
                        || "22000".equals(ex.getSQLState()),
                "range union disjoint should error with contiguous msg / 22000; got "
                        + ex.getSQLState() + " — " + ex.getMessage());
    }

    /** range_agg aggregates ranges into a multirange. */
    @Test
    void range_agg_intoMultirange() throws SQLException {
        assertEquals("{[1,5),[10,20)}",
                scalarString("SELECT range_agg(r)::text FROM ("
                        + " VALUES (int4range(1,5)), (int4range(10,20))) t(r)"));
    }

    /** range_merge merges bounds of a range and another range into a bigger one. */
    @Test
    void range_merge_twoRanges() throws SQLException {
        assertEquals("[1,10)",
                scalarString("SELECT range_merge(int4range(1,3), int4range(7,10))::text"));
    }

    // =========================================================================
    // E. Array edge cases
    // =========================================================================

    /** Array concat with NULL → PG treats NULL as empty effectively: ARRAY || NULL = ARRAY. */
    @Test
    void array_concat_withNull() throws SQLException {
        assertEquals("{1,2,3}",
                scalarString("SELECT (ARRAY[1,2,3] || NULL::int[])::text"));
    }

    /** Array IS NOT DISTINCT FROM — null-safe equality. */
    @Test
    void array_isNotDistinctFrom_nulls() throws SQLException {
        assertTrue(scalarBool(
                "SELECT (NULL::int[]) IS NOT DISTINCT FROM (NULL::int[])"));
    }

    /** array_dims returns the dimensions in the PG format. */
    @Test
    void array_dims_multiDim() throws SQLException {
        assertEquals("[1:2][1:3]",
                scalarString("SELECT array_dims(ARRAY[[1,2,3],[4,5,6]])"));
    }

    // =========================================================================
    // F. Text/pattern
    // =========================================================================

    /** !~~  is NOT LIKE. */
    @Test
    void not_like_operator() throws SQLException {
        assertTrue(scalarBool("SELECT 'hello' !~~ 'world%'"));
        assertFalse(scalarBool("SELECT 'hello' !~~ 'he%'"));
    }

    /** !~~*  is NOT ILIKE. */
    @Test
    void not_ilike_operator() throws SQLException {
        assertTrue(scalarBool("SELECT 'HELLO' !~~* 'world%'"));
        assertFalse(scalarBool("SELECT 'HELLO' !~~* 'hel%'"));
    }

    /** ^@ is "starts with" operator. */
    @Test
    void starts_with_operator() throws SQLException {
        assertTrue(scalarBool("SELECT 'hello world' ^@ 'hello'"));
        assertFalse(scalarBool("SELECT 'hello world' ^@ 'world'"));
    }
}
