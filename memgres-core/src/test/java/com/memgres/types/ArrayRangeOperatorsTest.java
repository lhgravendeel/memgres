package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for array/range operator bugs:
 * - two-element arrays misidentified as ranges by @> / <@
 * - numrange bounds rounded to integers
 * - empty-range containment/overlap/adjacency semantics
 * - NULL array elements matching in @> and &&
 * - unnest not flattening multidimensional arrays
 * - array literal parsing of quoted elements with commas/escapes
 * - int4range canonicalization with a missing bound
 * - array_upper/array_lower/array_length/cardinality on empty arrays
 */
class ArrayRangeOperatorsTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ---- Bug 1: two-element arrays must use array semantics, not range semantics ----

    @Test void two_element_array_contains_uses_array_semantics() throws SQLException {
        // Range semantics would say true ([1,5] contains [2,3]); array semantics: 2 and 3 not elements
        assertEquals("f", q("SELECT ARRAY[1,5] @> ARRAY[2,3]"));
    }

    @Test void two_element_array_contains_single_element() throws SQLException {
        assertEquals("t", q("SELECT ARRAY[1,2] @> ARRAY[2]"));
    }

    @Test void two_element_array_contained_by() throws SQLException {
        assertEquals("t", q("SELECT ARRAY[1,5] <@ ARRAY[1,2,3,4,5]"));
    }

    @Test void two_element_array_not_contained_by() throws SQLException {
        assertEquals("f", q("SELECT ARRAY[1,6] <@ ARRAY[1,2,3,4,5]"));
    }

    @Test void containment_with_duplicates() throws SQLException {
        assertEquals("t", q("SELECT ARRAY[1] @> ARRAY[1,1]"));
    }

    @Test void empty_array_contained_in_everything() throws SQLException {
        assertEquals("t", q("SELECT ARRAY[1,2] @> ARRAY[]::int[]"));
        assertEquals("t", q("SELECT ARRAY[]::int[] <@ ARRAY[1,2]"));
        assertEquals("t", q("SELECT ARRAY[]::int[] @> ARRAY[]::int[]"));
    }

    @Test void two_element_array_overlap() throws SQLException {
        assertEquals("t", q("SELECT ARRAY[1,5] && ARRAY[5,9]"));
        assertEquals("f", q("SELECT ARRAY[1,5] && ARRAY[2,4]"));
    }

    // ---- Genuine range operators still work ----

    @Test void int4range_contains_value() throws SQLException {
        assertEquals("t", q("SELECT '[1,5)'::int4range @> 3"));
        assertEquals("f", q("SELECT '[1,5)'::int4range @> 5"));
    }

    @Test void int4range_contains_range() throws SQLException {
        assertEquals("t", q("SELECT '[1,10)'::int4range @> '[2,3)'::int4range"));
        assertEquals("f", q("SELECT '[2,3)'::int4range @> '[1,10)'::int4range"));
    }

    @Test void int4range_overlap() throws SQLException {
        assertEquals("t", q("SELECT '[1,5)'::int4range && '[4,8)'::int4range"));
        assertEquals("f", q("SELECT '[1,5)'::int4range && '[5,8)'::int4range"));
    }

    @Test void value_contained_in_range() throws SQLException {
        assertEquals("t", q("SELECT 3 <@ '[1,5)'::int4range"));
    }

    // ---- Bug 2: numrange bounds must be exact decimals ----

    @Test void numrange_contains_decimal_value() throws SQLException {
        assertEquals("t", q("SELECT '[1.5,2.5)'::numrange @> 1.6"));
        assertEquals("f", q("SELECT '[1.5,2.5)'::numrange @> 1.4"));
        assertEquals("t", q("SELECT '[1.5,2.5)'::numrange @> 1.5"));
        assertEquals("f", q("SELECT '[1.5,2.5)'::numrange @> 2.5"));
    }

    @Test void numrange_lower_upper_exact() throws SQLException {
        assertEquals("1.5", q("SELECT lower('[1.5,2.5)'::numrange)"));
        assertEquals("2.5", q("SELECT upper('[1.5,2.5)'::numrange)"));
    }

    @Test void numrange_cast_preserves_decimals() throws SQLException {
        assertEquals("[1.5,2.5)", q("SELECT '[1.5,2.5)'::numrange"));
    }

    @Test void numrange_inclusivity_functions() throws SQLException {
        assertEquals("t", q("SELECT lower_inc('[1.5,2.5]'::numrange)"));
        assertEquals("t", q("SELECT upper_inc('[1.5,2.5]'::numrange)"));
        assertEquals("f", q("SELECT upper_inc('[1.5,2.5)'::numrange)"));
    }

    @Test void numrange_contains_range_decimal() throws SQLException {
        assertEquals("t", q("SELECT '[1.5,2.5)'::numrange @> '[1.6,2.0)'::numrange"));
        assertEquals("f", q("SELECT '[1.5,2.5)'::numrange @> '[1.4,2.0)'::numrange"));
    }

    @Test void numrange_contained_by_decimal() throws SQLException {
        assertEquals("t", q("SELECT '[1.6,2.0)'::numrange <@ '[1.5,2.5)'::numrange"));
        assertEquals("t", q("SELECT 1.6 <@ '[1.5,2.5)'::numrange"));
    }

    @Test void numrange_overlap_decimal() throws SQLException {
        assertEquals("t", q("SELECT '[1.1,1.9)'::numrange && '[1.8,3.0)'::numrange"));
        assertEquals("f", q("SELECT '[1.1,1.5)'::numrange && '[1.5,3.0)'::numrange"));
        // Touching bounds, both inclusive: single common point -> overlap
        assertEquals("t", q("SELECT '[1.1,1.5]'::numrange && '[1.5,3.0)'::numrange"));
    }

    // ---- Bug 3: empty range semantics ----

    @Test void nonempty_range_contains_empty() throws SQLException {
        assertEquals("t", q("SELECT '[1,5)'::int4range @> 'empty'::int4range"));
        assertEquals("t", q("SELECT 'empty'::int4range <@ '[1,5)'::int4range"));
    }

    @Test void empty_contains_only_empty() throws SQLException {
        assertEquals("t", q("SELECT 'empty'::int4range @> 'empty'::int4range"));
        assertEquals("f", q("SELECT 'empty'::int4range @> '[1,2)'::int4range"));
    }

    @Test void empty_never_overlaps() throws SQLException {
        assertEquals("f", q("SELECT 'empty'::int4range && '[1,5)'::int4range"));
        assertEquals("f", q("SELECT 'empty'::int4range && 'empty'::int4range"));
    }

    @Test void empty_never_adjacent() throws SQLException {
        assertEquals("f", q("SELECT 'empty'::int4range -|- '[1,5)'::int4range"));
    }

    @Test void genuine_adjacency_still_works() throws SQLException {
        assertEquals("t", q("SELECT '[1,5)'::int4range -|- '[5,10)'::int4range"));
        assertEquals("f", q("SELECT '[1,5)'::int4range -|- '[6,10)'::int4range"));
    }

    // ---- Bug 4: NULL array elements never match ----

    @Test void null_element_containment() throws SQLException {
        assertEquals("f", q("SELECT ARRAY[1,NULL] @> ARRAY[NULL]::int[]"));
        assertEquals("f", q("SELECT ARRAY[NULL]::int[] <@ ARRAY[1,NULL]"));
        assertEquals("t", q("SELECT ARRAY[1,NULL] @> ARRAY[1]"));
    }

    @Test void null_element_overlap() throws SQLException {
        assertEquals("f", q("SELECT ARRAY[NULL]::int[] && ARRAY[NULL]::int[]"));
        assertEquals("t", q("SELECT ARRAY[1,NULL] && ARRAY[1]"));
        assertEquals("f", q("SELECT ARRAY[NULL,2] && ARRAY[1,NULL]"));
    }

    @Test void literal_null_string_does_not_collide_with_sql_null() throws SQLException {
        // The string 'NULL' is a real value; SQL NULL is not
        assertEquals("f", q("SELECT ARRAY['NULL'] @> ARRAY[NULL]::text[]"));
    }

    // ---- Bug 5: unnest flattens multidimensional arrays ----

    @Test void unnest_flattens_2d() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT unnest(ARRAY[[1,2],[3,4]])")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(rs.getString(1));
            }
            assertEquals("1,2,3,4", sb.toString());
        }
    }

    @Test void unnest_flattens_3d() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT unnest(ARRAY[[[1,2],[3,4]],[[5,6],[7,8]]])")) {
            int count = 0;
            int sum = 0;
            while (rs.next()) {
                count++;
                sum += rs.getInt(1);
            }
            assertEquals(8, count);
            assertEquals(36, sum);
        }
    }

    @Test void unnest_count_2d() throws SQLException {
        assertEquals("4", q("SELECT count(*) FROM unnest(ARRAY[[1,2],[3,4]])"));
    }

    // ---- Bug 6: quoted array elements with commas/escapes ----

    @Test void quoted_elements_with_commas_overlap() throws SQLException {
        assertEquals("f", q("SELECT ARRAY['a,b'] && ARRAY['c,b']"));
        assertEquals("t", q("SELECT ARRAY['a,b'] && ARRAY['a,b','x']"));
    }

    @Test void quoted_elements_with_commas_containment() throws SQLException {
        assertEquals("t", q("SELECT ARRAY['a,b','c'] @> ARRAY['a,b']"));
        assertEquals("f", q("SELECT ARRAY['a,b','c'] @> ARRAY['a']"));
    }

    @Test void quoted_elements_round_trip() throws SQLException {
        assertEquals("{\"a,b\",c}", q("SELECT ARRAY['a,b','c']"));
    }

    @Test void quoted_elements_with_escapes() throws SQLException {
        // Element containing a double quote round-trips with backslash escaping
        assertEquals("{\"a\\\"b\"}", q("SELECT ARRAY['a\"b']"));
        assertEquals("t", q("SELECT ARRAY['a\"b'] @> ARRAY['a\"b']"));
    }

    @Test void quoted_null_string_vs_null_keyword() throws SQLException {
        assertEquals("t", q("SELECT ARRAY['NULL'] @> ARRAY['NULL']"));
        assertEquals("1", q("SELECT cardinality('{\"a,b\"}'::text[])"));
    }

    // ---- Bug 7: int4range canonicalization with a missing bound ----

    @Test void half_unbounded_int4range_canonicalized() throws SQLException {
        assertEquals("(,6)", q("SELECT '(,5]'::int4range"));
        assertEquals("[3,)", q("SELECT '(2,)'::int4range"));
    }

    @Test void bounded_int4range_canonicalization_unchanged() throws SQLException {
        assertEquals("[1,4)", q("SELECT '[1,3]'::int4range"));
        assertEquals("[2,4)", q("SELECT '(1,3]'::int4range"));
    }

    @Test void fully_unbounded_int4range() throws SQLException {
        assertEquals("(,)", q("SELECT '(,)'::int4range"));
    }

    // ---- Bug 8: empty-array dimension functions ----

    @Test void array_upper_empty_is_null() throws SQLException {
        assertNull(q("SELECT array_upper('{}'::int[], 1)"));
        assertNull(q("SELECT array_upper(ARRAY[]::int[], 1)"));
    }

    @Test void array_lower_empty_is_null() throws SQLException {
        assertNull(q("SELECT array_lower('{}'::int[], 1)"));
        assertNull(q("SELECT array_lower(ARRAY[]::int[], 1)"));
    }

    @Test void array_length_empty_is_null() throws SQLException {
        assertNull(q("SELECT array_length('{}'::int[], 1)"));
        assertNull(q("SELECT array_length(ARRAY[]::int[], 1)"));
    }

    @Test void cardinality_empty_is_zero() throws SQLException {
        assertEquals("0", q("SELECT cardinality('{}'::int[])"));
        assertEquals("0", q("SELECT cardinality(ARRAY[]::int[])"));
    }

    @Test void nonempty_dimension_functions_unchanged() throws SQLException {
        assertEquals("3", q("SELECT array_upper(ARRAY[10,20,30], 1)"));
        assertEquals("1", q("SELECT array_lower(ARRAY[10,20,30], 1)"));
        assertEquals("3", q("SELECT array_length(ARRAY[10,20,30], 1)"));
        assertEquals("3", q("SELECT cardinality(ARRAY[10,20,30])"));
    }
}
