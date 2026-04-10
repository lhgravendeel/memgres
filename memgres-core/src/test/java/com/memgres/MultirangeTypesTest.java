package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B4: Comprehensive tests for multirange types — constructors, functions,
 * operators, casting, and all 6 built-in multirange types.
 */
class MultirangeTypesTest {

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

    // ---- Constructors ----

    @Test
    void int4multirangeConstructorEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange()")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString(1));
        }
    }

    @Test
    void int4multirangeConstructorSingleRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange(int4range(1, 5))")) {
            assertTrue(rs.next());
            assertEquals("{[1,5)}", rs.getString(1));
        }
    }

    @Test
    void int4multirangeConstructorMultipleRanges() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange(int4range(1, 5), int4range(10, 20))")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void int4multirangeConstructorMergesOverlapping() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange(int4range(1, 10), int4range(5, 15))")) {
            assertTrue(rs.next());
            assertEquals("{[1,15)}", rs.getString(1));
        }
    }

    @Test
    void int8multirangeConstructor() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int8multirange(int8range(1, 5), int8range(10, 20))")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void nummultirangeConstructor() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT nummultirange(numrange(1.5, 3.5))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("{") && result.endsWith("}"));
        }
    }

    // ---- Casting ----

    @Test
    void castMultirangeLiteral() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void castRangeToMultirange() throws Exception {
        // range → multirange implicit cast
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[1,5)'::int4range::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,5)}", rs.getString(1));
        }
    }

    @Test
    void castEmptyRangeToMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 'empty'::int4range::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString(1));
        }
    }

    @Test
    void castEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString(1));
        }
    }

    // ---- Functions: upper/lower ----

    @Test
    void upperOfMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        }
    }

    @Test
    void lowerOfMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // ---- Functions: isempty ----

    @Test
    void isemptyMultirangeEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT isempty('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void isemptyMultirangeNonEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT isempty('{[1,5)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ---- Functions: lower_inc, upper_inc, lower_inf, upper_inf ----

    @Test
    void lowerIncMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower_inc('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void upperIncMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper_inc('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void lowerInfMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower_inf('{[1,5)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void upperInfMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper_inf('{[1,5)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ---- Functions: range_merge(multirange) ----

    @Test
    void rangeMergeMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT range_merge('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals("[1,20)", rs.getString(1));
        }
    }

    @Test
    void rangeMergeMultirangeEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT range_merge('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals("empty", rs.getString(1));
        }
    }

    // ---- Containment operators ----

    @Test
    void multirangeContainsValue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange @> 3")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeNotContainsValue() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange @> 7")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeContainsRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange @> '[11,15)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeContainsMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,20)}'::int4multirange @> '{[2,5),[10,15)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- Overlap operator ----

    @Test
    void multirangeOverlapsMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange && '{[3,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeNoOverlap() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange && '{[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeOverlapsRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange && '[3,7)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ---- Arithmetic operators: +, -, * ----

    @Test
    void multirangeUnion() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange + '{[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void multirangeUnionMergesOverlapping() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,10)}'::int4multirange + '{[5,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,20)}", rs.getString(1));
        }
    }

    @Test
    void multirangeIntersection() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,10),[20,30)}'::int4multirange * '{[5,25)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[5,10),[20,25)}", rs.getString(1));
        }
    }

    @Test
    void multirangeSubtraction() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,20)}'::int4multirange - '{[5,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void multirangeMinusRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,20)}'::int4multirange - '[5,10)'::int4range")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    @Test
    void multirangePlusRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange + '[10,20)'::int4range")) {
            assertTrue(rs.next());
            assertEquals("{[1,5),[10,20)}", rs.getString(1));
        }
    }

    // ---- pg_range catalog ----

    @Test
    void pgRangeCatalogHasMultirangeEntries() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT count(*) FROM pg_range WHERE rngmultitypid != 0")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 6, "Expected at least 6 pg_range entries with multirange type OIDs");
        }
    }

    // ---- All 6 multirange type constructors ----

    @Test
    void datemultirangeConstructor() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT datemultirange(daterange('2024-01-01', '2024-06-01'))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("{") && result.endsWith("}"));
        }
    }

    @Test
    void tsmultirangeConstructor() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT tsmultirange(tsrange('2024-01-01 00:00:00', '2024-06-01 00:00:00'))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("{") && result.endsWith("}"));
        }
    }

    @Test
    void tstzmultirangeConstructor() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT tstzmultirange(tstzrange('2024-01-01 00:00:00+00', '2024-06-01 00:00:00+00'))")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.startsWith("{") && result.endsWith("}"));
        }
    }

    // ---- Column with multirange type ----

    @Test
    void tableWithMultirangeColumn() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE mr_test (id serial, schedule int4multirange)");
            st.execute("INSERT INTO mr_test (schedule) VALUES ('{[1,5),[10,20)}')");
            try (ResultSet rs = st.executeQuery("SELECT schedule FROM mr_test WHERE schedule @> 3")) {
                assertTrue(rs.next());
                assertEquals("{[1,5),[10,20)}", rs.getString(1));
            }
            st.execute("DROP TABLE mr_test");
        }
    }

    // ---- Malformed literal detection ----

    @Test
    void malformedMultirangeLiteral() {
        assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.executeQuery("SELECT 'not-a-multirange'::int4multirange");
            }
        });
    }

    // ============================================================
    // Edge cases: empty multirange operations
    // ============================================================

    @Test
    void upperOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void lowerOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void lowerIncOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower_inc('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void upperIncOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper_inc('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void lowerInfOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower_inf('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void upperInfOfEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper_inf('{}'::int4multirange)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void emptyMultirangeContainsNothing() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{}'::int4multirange @> 5")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void emptyMultirangeOverlapFalse() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{}'::int4multirange && '{[1,5)}'::int4multirange")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ============================================================
    // CONTAINED_BY (<@) operator
    // ============================================================

    @Test
    void rangeContainedByMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[2,4)'::int4range <@ '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void rangeNotContainedByMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[1,15)'::int4range <@ '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeContainedByMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[2,4)}'::int4multirange <@ '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void valueContainedByMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 3 <@ '{[1,5),[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void valueNotContainedByMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 7 <@ '{[1,5),[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    // ============================================================
    // RANGE_ADJACENT (-|-) operator
    // ============================================================

    @Test
    void multirangeAdjacentRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange -|- '[5,10)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeNotAdjacentRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange -|- '[7,10)'::int4range")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeAdjacentMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange -|- '{[5,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ============================================================
    // Equality / inequality
    // ============================================================

    @Test
    void multirangeEquality() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5),[10,20)}'::int4multirange = '{[1,5),[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeInequality() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange <> '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeEqualityAfterCanonicalization() throws Exception {
        // Overlapping sub-ranges should be merged during cast, so these are equal
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,10),[5,15)}'::int4multirange = '{[1,15)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // ============================================================
    // Integer overflow protection (int8multirange with large values)
    // ============================================================

    @Test
    void int8multirangeWithLargeValues() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT upper('{[1,2147483648)}'::int8multirange)")) {
            assertTrue(rs.next());
            // 2147483648 > Integer.MAX_VALUE — must not be truncated
            assertEquals(2147483648L, rs.getLong(1));
        }
    }

    @Test
    void int8multirangeSubtractionLargeValues() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,3000000000)}'::int8multirange - '{[1000000000,2000000000)}'::int8multirange")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("3000000000"), "Large values should not be truncated: " + result);
        }
    }

    // ============================================================
    // Multirange + Range arithmetic
    // ============================================================

    @Test
    void multirangeIntersectionWithRange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,10),[20,30)}'::int4multirange * '[5,25)'::int4range")) {
            assertTrue(rs.next());
            assertEquals("{[5,10),[20,25)}", rs.getString(1));
        }
    }

    @Test
    void multirangeSubtractionResultsInMultiplePieces() throws Exception {
        // Subtracting from middle of range creates two pieces
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,30)}'::int4multirange - '{[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,10),[20,30)}", rs.getString(1));
        }
    }

    @Test
    void multirangeUnionEmptyPlusNonEmpty() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{}'::int4multirange + '{[1,5)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{[1,5)}", rs.getString(1));
        }
    }

    @Test
    void multirangeIntersectionNoOverlap() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange * '{[10,20)}'::int4multirange")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString(1));
        }
    }

    // ============================================================
    // Constructor edge cases
    // ============================================================

    @Test
    void constructorWithEmptyRangesSkipped() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange(int4range(1, 1))")) {
            // [1,1) is empty, should produce empty multirange
            assertTrue(rs.next());
            assertEquals("{}", rs.getString(1));
        }
    }

    @Test
    void constructorWithAdjacentRangesMerged() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT int4multirange(int4range(1, 5), int4range(5, 10))")) {
            assertTrue(rs.next());
            assertEquals("{[1,10)}", rs.getString(1));
        }
    }

    // ============================================================
    // range_merge on single-element multirange
    // ============================================================

    @Test
    void rangeMergeSingleSubrange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT range_merge('{[1,5)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals("[1,5)", rs.getString(1));
        }
    }

    // ============================================================
    // unnest(multirange) — SELECT list SRF
    // ============================================================

    @Test
    void unnestMultirangeInSelect() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT unnest('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals("[1,5)", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("[10,20)", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void unnestEmptyMultirange() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT unnest('{}'::int4multirange)")) {
            // Empty multirange produces no rows
            assertFalse(rs.next());
        }
    }

    // ============================================================
    // unnest(multirange) — FROM clause SRF
    // ============================================================

    @Test
    void unnestMultirangeInFrom() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM unnest('{[1,5),[10,20)}'::int4multirange)")) {
            assertTrue(rs.next());
            assertEquals("[1,5)", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("[10,20)", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void unnestMultirangeFromWithOrdinality() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT r, ordinality FROM unnest('{[1,5),[10,20),[30,40)}'::int4multirange) WITH ORDINALITY AS t(r, ordinality)")) {
            assertTrue(rs.next());
            assertEquals("[1,5)", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("[10,20)", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("[30,40)", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    // ============================================================
    // range_agg() aggregate function
    // ============================================================

    @Test
    void rangeAggBasic() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ra_test (r int4range)");
            st.execute("INSERT INTO ra_test VALUES ('[1,5)'), ('[10,20)'), ('[3,8)')");
            try (ResultSet rs = st.executeQuery("SELECT range_agg(r) FROM ra_test")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                // [1,5) and [3,8) should merge to [1,8); [10,20) stays separate
                assertEquals("{[1,8),[10,20)}", result);
            }
            st.execute("DROP TABLE ra_test");
        }
    }

    @Test
    void rangeAggAllNulls() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ra_null (r int4range)");
            st.execute("INSERT INTO ra_null VALUES (NULL), (NULL)");
            try (ResultSet rs = st.executeQuery("SELECT range_agg(r) FROM ra_null")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
            st.execute("DROP TABLE ra_null");
        }
    }

    @Test
    void rangeAggEmptyTable() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ra_empty (r int4range)");
            try (ResultSet rs = st.executeQuery("SELECT range_agg(r) FROM ra_empty")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
            st.execute("DROP TABLE ra_empty");
        }
    }

    @Test
    void rangeAggWithGroupBy() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ra_grp (category text, r int4range)");
            st.execute("INSERT INTO ra_grp VALUES ('a', '[1,5)'), ('a', '[3,10)'), ('b', '[20,30)')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT category, range_agg(r) FROM ra_grp GROUP BY category ORDER BY category")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("{[1,10)}", rs.getString(2));
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals("{[20,30)}", rs.getString(2));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE ra_grp");
        }
    }

    // ============================================================
    // Range/multirange ordering operators (<, >, <=, >=)
    // ============================================================

    @Test
    void rangeLessThan() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[1,5)'::int4range < '[1,10)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void rangeGreaterThan() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[5,10)'::int4range > '[1,10)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void rangeLessEqual() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[1,5)'::int4range <= '[1,5)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void rangeGreaterEqual() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '[5,10)'::int4range >= '[1,10)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void emptyRangeSortsFirst() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 'empty'::int4range < '[1,5)'::int4range")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeLessThan() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[1,5)}'::int4multirange < '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void multirangeGreaterThan() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT '{[5,10)}'::int4multirange > '{[1,10)}'::int4multirange")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void rangeOrderByWorks() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ro_test (r int4range)");
            st.execute("INSERT INTO ro_test VALUES ('[5,10)'), ('[1,3)'), ('[1,8)')");
            try (ResultSet rs = st.executeQuery("SELECT r FROM ro_test ORDER BY r")) {
                assertTrue(rs.next());
                assertEquals("[1,3)", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("[1,8)", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("[5,10)", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE ro_test");
        }
    }
}
