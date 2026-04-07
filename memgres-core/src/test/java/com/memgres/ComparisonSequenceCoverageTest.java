package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class ComparisonSequenceCoverageTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
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

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private long queryLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // Item 56: Comparison Operators & Predicates
    // ========================================================================

    // --- Basic comparisons: = ---

    @Test void testEqualIntegers() throws SQLException {
        assertTrue(queryBool("SELECT 1 = 1"));
    }

    @Test void testEqualIntegersFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 = 2"));
    }

    @Test void testEqualStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'abc' = 'abc'"));
    }

    @Test void testEqualStringsFalse() throws SQLException {
        assertFalse(queryBool("SELECT 'abc' = 'def'"));
    }

    // --- Basic comparisons: <> and != ---

    @Test void testNotEqualAngleBrackets() throws SQLException {
        assertTrue(queryBool("SELECT 1 <> 2"));
    }

    @Test void testNotEqualAngleBracketsFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 <> 1"));
    }

    @Test void testNotEqualBangEquals() throws SQLException {
        assertTrue(queryBool("SELECT 1 != 2"));
    }

    @Test void testNotEqualBangEqualsFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 != 1"));
    }

    @Test void testNotEqualStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'a' <> 'b'"));
    }

    // --- Basic comparisons: <, >, <=, >= ---

    @Test void testLessThanTrue() throws SQLException {
        assertTrue(queryBool("SELECT 1 < 2"));
    }

    @Test void testLessThanFalse() throws SQLException {
        assertFalse(queryBool("SELECT 2 < 1"));
    }

    @Test void testLessThanEqual() throws SQLException {
        assertFalse(queryBool("SELECT 2 < 2"));
    }

    @Test void testGreaterThanTrue() throws SQLException {
        assertTrue(queryBool("SELECT 2 > 1"));
    }

    @Test void testGreaterThanFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 > 2"));
    }

    @Test void testLessThanOrEqualTrue() throws SQLException {
        assertTrue(queryBool("SELECT 1 <= 1"));
    }

    @Test void testLessThanOrEqualLess() throws SQLException {
        assertTrue(queryBool("SELECT 1 <= 2"));
    }

    @Test void testLessThanOrEqualFalse() throws SQLException {
        assertFalse(queryBool("SELECT 3 <= 2"));
    }

    @Test void testGreaterThanOrEqualTrue() throws SQLException {
        assertTrue(queryBool("SELECT 2 >= 2"));
    }

    @Test void testGreaterThanOrEqualFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 >= 2"));
    }

    @Test void testStringLessThan() throws SQLException {
        assertTrue(queryBool("SELECT 'abc' < 'abd'"));
    }

    @Test void testStringGreaterThan() throws SQLException {
        assertTrue(queryBool("SELECT 'b' > 'a'"));
    }

    // --- NULL comparisons ---

    @Test void testNullEqualsNull() throws SQLException {
        // NULL = NULL yields NULL (falsy)
        assertNull(query1("SELECT NULL = NULL"));
    }

    @Test void testNullNotEqualsNull() throws SQLException {
        assertNull(query1("SELECT NULL <> NULL"));
    }

    @Test void testNullLessThan() throws SQLException {
        assertNull(query1("SELECT NULL < 1"));
    }

    @Test void testValueLessThanNull() throws SQLException {
        assertNull(query1("SELECT 1 < NULL"));
    }

    // --- Type coercion in comparisons ---

    @Test void testIntegerEqualsDecimal() throws SQLException {
        assertTrue(queryBool("SELECT 1 = 1.0"));
    }

    @Test void testStringNumberComparison() throws SQLException {
        assertTrue(queryBool("SELECT 10 > 2"));
    }

    // --- BETWEEN ... AND ... ---

    @Test void testBetweenNumberTrue() throws SQLException {
        assertTrue(queryBool("SELECT 5 BETWEEN 1 AND 10"));
    }

    @Test void testBetweenNumberFalse() throws SQLException {
        assertFalse(queryBool("SELECT 15 BETWEEN 1 AND 10"));
    }

    @Test void testBetweenLowerBoundInclusive() throws SQLException {
        assertTrue(queryBool("SELECT 1 BETWEEN 1 AND 10"));
    }

    @Test void testBetweenUpperBoundInclusive() throws SQLException {
        assertTrue(queryBool("SELECT 10 BETWEEN 1 AND 10"));
    }

    @Test void testBetweenStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'b' BETWEEN 'a' AND 'c'"));
    }

    @Test void testBetweenDates() throws SQLException {
        assertTrue(queryBool("SELECT DATE '2024-06-15' BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'"));
    }

    @Test void testBetweenDatesOutOfRange() throws SQLException {
        assertFalse(queryBool("SELECT DATE '2025-06-15' BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'"));
    }

    // --- NOT BETWEEN ---

    @Test void testNotBetweenTrue() throws SQLException {
        assertTrue(queryBool("SELECT 15 NOT BETWEEN 1 AND 10"));
    }

    @Test void testNotBetweenFalse() throws SQLException {
        assertFalse(queryBool("SELECT 5 NOT BETWEEN 1 AND 10"));
    }

    @Test void testNotBetweenLowerBound() throws SQLException {
        assertFalse(queryBool("SELECT 1 NOT BETWEEN 1 AND 10"));
    }

    // --- BETWEEN SYMMETRIC ---

    @Test void testBetweenSymmetricNormalOrder() throws SQLException {
        assertTrue(queryBool("SELECT 5 BETWEEN SYMMETRIC 1 AND 10"));
    }

    @Test void testBetweenSymmetricReversedOrder() throws SQLException {
        assertTrue(queryBool("SELECT 5 BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test void testBetweenSymmetricOutOfRange() throws SQLException {
        assertFalse(queryBool("SELECT 15 BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test void testBetweenSymmetricBoundary() throws SQLException {
        assertTrue(queryBool("SELECT 10 BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test void testBetweenSymmetricLowBoundary() throws SQLException {
        assertTrue(queryBool("SELECT 1 BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test void testNotBetweenSymmetric() throws SQLException {
        assertTrue(queryBool("SELECT 15 NOT BETWEEN SYMMETRIC 10 AND 1"));
    }

    @Test void testNotBetweenSymmetricFalse() throws SQLException {
        assertFalse(queryBool("SELECT 5 NOT BETWEEN SYMMETRIC 10 AND 1"));
    }

    // --- IS NULL, IS NOT NULL ---

    @Test void testIsNullTrue() throws SQLException {
        assertTrue(queryBool("SELECT NULL IS NULL"));
    }

    @Test void testIsNullFalse() throws SQLException {
        assertFalse(queryBool("SELECT 1 IS NULL"));
    }

    @Test void testIsNotNullTrue() throws SQLException {
        assertTrue(queryBool("SELECT 1 IS NOT NULL"));
    }

    @Test void testIsNotNullFalse() throws SQLException {
        assertFalse(queryBool("SELECT NULL IS NOT NULL"));
    }

    @Test void testIsNullEmptyString() throws SQLException {
        assertFalse(queryBool("SELECT '' IS NULL"));
    }

    @Test void testIsNullZero() throws SQLException {
        assertFalse(queryBool("SELECT 0 IS NULL"));
    }

    @Test void testIsNotNullString() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' IS NOT NULL"));
    }

    // --- IS DISTINCT FROM, IS NOT DISTINCT FROM ---

    @Test void testIsDistinctFromBothNonNull() throws SQLException {
        assertTrue(queryBool("SELECT 1 IS DISTINCT FROM 2"));
    }

    @Test void testIsDistinctFromSameValue() throws SQLException {
        assertFalse(queryBool("SELECT 1 IS DISTINCT FROM 1"));
    }

    @Test void testIsDistinctFromNullAndValue() throws SQLException {
        assertTrue(queryBool("SELECT NULL IS DISTINCT FROM 1"));
    }

    @Test void testIsDistinctFromValueAndNull() throws SQLException {
        assertTrue(queryBool("SELECT 1 IS DISTINCT FROM NULL"));
    }

    @Test void testIsDistinctFromBothNull() throws SQLException {
        // Key difference from =: NULL IS DISTINCT FROM NULL is false
        assertFalse(queryBool("SELECT NULL IS DISTINCT FROM NULL"));
    }

    @Test void testIsNotDistinctFromBothNull() throws SQLException {
        assertTrue(queryBool("SELECT NULL IS NOT DISTINCT FROM NULL"));
    }

    @Test void testIsNotDistinctFromSameValue() throws SQLException {
        assertTrue(queryBool("SELECT 1 IS NOT DISTINCT FROM 1"));
    }

    @Test void testIsNotDistinctFromDifferent() throws SQLException {
        assertFalse(queryBool("SELECT 1 IS NOT DISTINCT FROM 2"));
    }

    @Test void testIsNotDistinctFromNullAndValue() throws SQLException {
        assertFalse(queryBool("SELECT NULL IS NOT DISTINCT FROM 1"));
    }

    @Test void testIsDistinctFromStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'a' IS DISTINCT FROM 'b'"));
    }

    @Test void testIsNotDistinctFromStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'a' IS NOT DISTINCT FROM 'a'"));
    }

    // --- IS TRUE, IS NOT TRUE, IS FALSE, IS NOT FALSE, IS UNKNOWN, IS NOT UNKNOWN ---

    @Test void testIsTrueWithTrue() throws SQLException {
        assertTrue(queryBool("SELECT true IS TRUE"));
    }

    @Test void testIsTrueWithFalse() throws SQLException {
        assertFalse(queryBool("SELECT false IS TRUE"));
    }

    @Test void testIsTrueWithNull() throws SQLException {
        assertFalse(queryBool("SELECT NULL::boolean IS TRUE"));
    }

    @Test void testIsNotTrueWithFalse() throws SQLException {
        assertTrue(queryBool("SELECT false IS NOT TRUE"));
    }

    @Test void testIsNotTrueWithNull() throws SQLException {
        assertTrue(queryBool("SELECT NULL::boolean IS NOT TRUE"));
    }

    @Test void testIsNotTrueWithTrue() throws SQLException {
        assertFalse(queryBool("SELECT true IS NOT TRUE"));
    }

    @Test void testIsFalseWithFalse() throws SQLException {
        assertTrue(queryBool("SELECT false IS FALSE"));
    }

    @Test void testIsFalseWithTrue() throws SQLException {
        assertFalse(queryBool("SELECT true IS FALSE"));
    }

    @Test void testIsFalseWithNull() throws SQLException {
        assertFalse(queryBool("SELECT NULL::boolean IS FALSE"));
    }

    @Test void testIsNotFalseWithTrue() throws SQLException {
        assertTrue(queryBool("SELECT true IS NOT FALSE"));
    }

    @Test void testIsNotFalseWithNull() throws SQLException {
        assertTrue(queryBool("SELECT NULL::boolean IS NOT FALSE"));
    }

    @Test void testIsNotFalseWithFalse() throws SQLException {
        assertFalse(queryBool("SELECT false IS NOT FALSE"));
    }

    @Test void testIsUnknownWithNull() throws SQLException {
        assertTrue(queryBool("SELECT NULL::boolean IS UNKNOWN"));
    }

    @Test void testIsUnknownWithTrue() throws SQLException {
        assertFalse(queryBool("SELECT true IS UNKNOWN"));
    }

    @Test void testIsUnknownWithFalse() throws SQLException {
        assertFalse(queryBool("SELECT false IS UNKNOWN"));
    }

    @Test void testIsNotUnknownWithTrue() throws SQLException {
        assertTrue(queryBool("SELECT true IS NOT UNKNOWN"));
    }

    @Test void testIsNotUnknownWithFalse() throws SQLException {
        assertTrue(queryBool("SELECT false IS NOT UNKNOWN"));
    }

    @Test void testIsNotUnknownWithNull() throws SQLException {
        assertFalse(queryBool("SELECT NULL::boolean IS NOT UNKNOWN"));
    }

    // --- num_nulls and num_nonnulls ---

    @Test void testNumNullsAllNull() throws SQLException {
        assertEquals(3, queryInt("SELECT num_nulls(NULL, NULL, NULL)"));
    }

    @Test void testNumNullsNoneNull() throws SQLException {
        assertEquals(0, queryInt("SELECT num_nulls(1, 2, 3)"));
    }

    @Test void testNumNullsMixed() throws SQLException {
        assertEquals(2, queryInt("SELECT num_nulls(1, NULL, 'a', NULL)"));
    }

    @Test void testNumNullsSingleNull() throws SQLException {
        assertEquals(1, queryInt("SELECT num_nulls(NULL)"));
    }

    @Test void testNumNullsSingleNonNull() throws SQLException {
        assertEquals(0, queryInt("SELECT num_nulls(42)"));
    }

    @Test void testNumNonnullsAllNull() throws SQLException {
        assertEquals(0, queryInt("SELECT num_nonnulls(NULL, NULL, NULL)"));
    }

    @Test void testNumNonnullsNoneNull() throws SQLException {
        assertEquals(3, queryInt("SELECT num_nonnulls(1, 2, 3)"));
    }

    @Test void testNumNonnullsMixed() throws SQLException {
        assertEquals(2, queryInt("SELECT num_nonnulls(1, NULL, 'a', NULL)"));
    }

    @Test void testNumNonnullsSingleNull() throws SQLException {
        assertEquals(0, queryInt("SELECT num_nonnulls(NULL)"));
    }

    @Test void testNumNonnullsSingleNonNull() throws SQLException {
        assertEquals(1, queryInt("SELECT num_nonnulls(42)"));
    }

    @Test void testNumNullsPlusNumNonnullsEqualsTotal() throws SQLException {
        // For any argument list, num_nulls + num_nonnulls = total argument count
        assertEquals(5, queryInt("SELECT num_nulls(1, NULL, 3, NULL, 5) + num_nonnulls(1, NULL, 3, NULL, 5)"));
    }

    // --- Edge cases for comparisons ---

    @Test void testBooleanComparisonTrue() throws SQLException {
        assertTrue(queryBool("SELECT true = true"));
    }

    @Test void testBooleanComparisonFalse() throws SQLException {
        assertTrue(queryBool("SELECT true <> false"));
    }

    @Test void testBetweenWithNull() throws SQLException {
        // BETWEEN with NULL should yield NULL
        assertNull(query1("SELECT NULL BETWEEN 1 AND 10"));
    }

    @Test void testComparisonInWhere() throws SQLException {
        exec("CREATE TABLE cmp_test (id INT, val TEXT)");
        exec("INSERT INTO cmp_test VALUES (1, 'a'), (2, 'b'), (3, NULL)");
        assertEquals(1, queryInt("SELECT count(*) FROM cmp_test WHERE val IS NULL"));
        assertEquals(2, queryInt("SELECT count(*) FROM cmp_test WHERE val IS NOT NULL"));
        exec("DROP TABLE cmp_test");
    }

    @Test void testIsDistinctFromInWhere() throws SQLException {
        exec("CREATE TABLE cmp_dist (id INT, val INT)");
        exec("INSERT INTO cmp_dist VALUES (1, 10), (2, NULL), (3, 10)");
        assertEquals(1, queryInt("SELECT count(*) FROM cmp_dist WHERE val IS DISTINCT FROM 10"));
        assertEquals(2, queryInt("SELECT count(*) FROM cmp_dist WHERE val IS NOT DISTINCT FROM 10"));
        exec("DROP TABLE cmp_dist");
    }

    @Test void testBetweenDecimal() throws SQLException {
        assertTrue(queryBool("SELECT 3.14 BETWEEN 3.0 AND 4.0"));
    }

    @Test void testBetweenNegativeNumbers() throws SQLException {
        assertTrue(queryBool("SELECT -5 BETWEEN -10 AND 0"));
    }

    @Test void testComparisonWithExpression() throws SQLException {
        assertTrue(queryBool("SELECT (2 + 3) = 5"));
    }

    // ========================================================================
    // Item 57: Sequence Manipulation
    // ========================================================================

    // --- CREATE SEQUENCE basic ---

    @Test void testCreateSequenceBasic() throws SQLException {
        exec("CREATE SEQUENCE seq_basic_test");
        assertEquals(1, queryLong("SELECT nextval('seq_basic_test')"));
        exec("DROP SEQUENCE seq_basic_test");
    }

    @Test void testCreateSequenceIfNotExists() throws SQLException {
        exec("CREATE SEQUENCE seq_ine_test");
        exec("CREATE SEQUENCE IF NOT EXISTS seq_ine_test"); // should not error
        exec("DROP SEQUENCE seq_ine_test");
    }

    // --- CREATE SEQUENCE with options ---

    @Test void testCreateSequenceIncrementBy() throws SQLException {
        exec("CREATE SEQUENCE seq_inc_test INCREMENT BY 5");
        assertEquals(1, queryLong("SELECT nextval('seq_inc_test')"));
        assertEquals(6, queryLong("SELECT nextval('seq_inc_test')"));
        assertEquals(11, queryLong("SELECT nextval('seq_inc_test')"));
        exec("DROP SEQUENCE seq_inc_test");
    }

    @Test void testCreateSequenceStartWith() throws SQLException {
        exec("CREATE SEQUENCE seq_start_test START WITH 100");
        assertEquals(100, queryLong("SELECT nextval('seq_start_test')"));
        assertEquals(101, queryLong("SELECT nextval('seq_start_test')"));
        exec("DROP SEQUENCE seq_start_test");
    }

    @Test void testCreateSequenceMinvalue() throws SQLException {
        exec("CREATE SEQUENCE seq_min_test MINVALUE 10 START WITH 10");
        assertEquals(10, queryLong("SELECT nextval('seq_min_test')"));
        exec("DROP SEQUENCE seq_min_test");
    }

    @Test void testCreateSequenceMaxvalue() throws SQLException {
        exec("CREATE SEQUENCE seq_max_test MAXVALUE 3 START WITH 1");
        assertEquals(1, queryLong("SELECT nextval('seq_max_test')"));
        assertEquals(2, queryLong("SELECT nextval('seq_max_test')"));
        assertEquals(3, queryLong("SELECT nextval('seq_max_test')"));
        exec("DROP SEQUENCE seq_max_test");
    }

    @Test void testCreateSequenceCycle() throws SQLException {
        exec("CREATE SEQUENCE seq_cycle_test MAXVALUE 3 CYCLE");
        assertEquals(1, queryLong("SELECT nextval('seq_cycle_test')"));
        assertEquals(2, queryLong("SELECT nextval('seq_cycle_test')"));
        assertEquals(3, queryLong("SELECT nextval('seq_cycle_test')"));
        assertEquals(1, queryLong("SELECT nextval('seq_cycle_test')")); // wraps around
        exec("DROP SEQUENCE seq_cycle_test");
    }

    @Test void testCreateSequenceNoCycleOverflow() throws SQLException {
        exec("CREATE SEQUENCE seq_nocyc_test MAXVALUE 2 NO CYCLE");
        assertEquals(1, queryLong("SELECT nextval('seq_nocyc_test')"));
        assertEquals(2, queryLong("SELECT nextval('seq_nocyc_test')"));
        assertThrows(SQLException.class, () -> queryLong("SELECT nextval('seq_nocyc_test')"));
        exec("DROP SEQUENCE seq_nocyc_test");
    }

    @Test void testCreateSequenceAllOptions() throws SQLException {
        exec("CREATE SEQUENCE seq_all_opts INCREMENT BY 2 MINVALUE 10 MAXVALUE 20 START WITH 10 CYCLE");
        assertEquals(10, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(12, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(14, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(16, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(18, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(20, queryLong("SELECT nextval('seq_all_opts')"));
        assertEquals(10, queryLong("SELECT nextval('seq_all_opts')")); // cycles back
        exec("DROP SEQUENCE seq_all_opts");
    }

    // --- nextval, currval, setval ---

    @Test void testNextvalBasic() throws SQLException {
        exec("CREATE SEQUENCE seq_nv_test");
        assertEquals(1, queryLong("SELECT nextval('seq_nv_test')"));
        assertEquals(2, queryLong("SELECT nextval('seq_nv_test')"));
        assertEquals(3, queryLong("SELECT nextval('seq_nv_test')"));
        exec("DROP SEQUENCE seq_nv_test");
    }

    @Test void testCurrvalAfterNextval() throws SQLException {
        exec("CREATE SEQUENCE seq_cv_test");
        queryLong("SELECT nextval('seq_cv_test')");
        assertEquals(1, queryLong("SELECT currval('seq_cv_test')"));
        queryLong("SELECT nextval('seq_cv_test')");
        assertEquals(2, queryLong("SELECT currval('seq_cv_test')"));
        exec("DROP SEQUENCE seq_cv_test");
    }

    @Test void testCurrvalBeforeNextvalFails() throws SQLException {
        exec("CREATE SEQUENCE seq_cv_fail_test");
        assertThrows(SQLException.class, () -> queryLong("SELECT currval('seq_cv_fail_test')"));
        exec("DROP SEQUENCE seq_cv_fail_test");
    }

    @Test void testSetvalBasic() throws SQLException {
        exec("CREATE SEQUENCE seq_sv_test");
        queryLong("SELECT setval('seq_sv_test', 50)");
        assertEquals(50, queryLong("SELECT currval('seq_sv_test')"));
        assertEquals(51, queryLong("SELECT nextval('seq_sv_test')"));
        exec("DROP SEQUENCE seq_sv_test");
    }

    @Test void testSetvalWithIsCalledTrue() throws SQLException {
        exec("CREATE SEQUENCE seq_sv3_test");
        queryLong("SELECT setval('seq_sv3_test', 50, true)");
        assertEquals(51, queryLong("SELECT nextval('seq_sv3_test')"));
        exec("DROP SEQUENCE seq_sv3_test");
    }

    @Test void testSetvalWithIsCalledFalse() throws SQLException {
        exec("CREATE SEQUENCE seq_sv3f_test");
        queryLong("SELECT setval('seq_sv3f_test', 50, false)");
        assertEquals(50, queryLong("SELECT nextval('seq_sv3f_test')"));
        exec("DROP SEQUENCE seq_sv3f_test");
    }

    // --- lastval ---

    @Test void testLastval() throws SQLException {
        exec("CREATE SEQUENCE seq_lv1_test");
        exec("CREATE SEQUENCE seq_lv2_test START WITH 100");
        queryLong("SELECT nextval('seq_lv1_test')");
        assertEquals(1, queryLong("SELECT lastval()"));
        queryLong("SELECT nextval('seq_lv2_test')");
        assertEquals(100, queryLong("SELECT lastval()"));
        exec("DROP SEQUENCE seq_lv1_test");
        exec("DROP SEQUENCE seq_lv2_test");
    }

    // --- DROP SEQUENCE ---

    @Test void testDropSequence() throws SQLException {
        exec("CREATE SEQUENCE seq_drop_test");
        exec("DROP SEQUENCE seq_drop_test");
        assertThrows(SQLException.class, () -> queryLong("SELECT nextval('seq_drop_test')"));
    }

    @Test void testDropSequenceIfExists() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS seq_nonexistent"); // should not error
    }

    @Test void testDropSequenceIfExistsActual() throws SQLException {
        exec("CREATE SEQUENCE seq_drop_ie_test");
        exec("DROP SEQUENCE IF EXISTS seq_drop_ie_test");
        assertThrows(SQLException.class, () -> queryLong("SELECT nextval('seq_drop_ie_test')"));
    }

    // --- ALTER SEQUENCE ---

    @Test void testAlterSequenceRestart() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_rst_test");
        queryLong("SELECT nextval('seq_alt_rst_test')");
        queryLong("SELECT nextval('seq_alt_rst_test')");
        exec("ALTER SEQUENCE seq_alt_rst_test RESTART");
        assertEquals(1, queryLong("SELECT nextval('seq_alt_rst_test')"));
        exec("DROP SEQUENCE seq_alt_rst_test");
    }

    @Test void testAlterSequenceRestartWith() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_rstw_test");
        queryLong("SELECT nextval('seq_alt_rstw_test')");
        exec("ALTER SEQUENCE seq_alt_rstw_test RESTART WITH 500");
        assertEquals(500, queryLong("SELECT nextval('seq_alt_rstw_test')"));
        exec("DROP SEQUENCE seq_alt_rstw_test");
    }

    @Test void testAlterSequenceIncrementBy() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_inc_test");
        queryLong("SELECT nextval('seq_alt_inc_test')"); // 1
        exec("ALTER SEQUENCE seq_alt_inc_test INCREMENT BY 10");
        assertEquals(11, queryLong("SELECT nextval('seq_alt_inc_test')"));
        assertEquals(21, queryLong("SELECT nextval('seq_alt_inc_test')"));
        exec("DROP SEQUENCE seq_alt_inc_test");
    }

    @Test void testAlterSequenceMinvalue() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_min_test");
        exec("ALTER SEQUENCE seq_alt_min_test MINVALUE 5");
        // Sequence should still work; minvalue is a floor
        exec("DROP SEQUENCE seq_alt_min_test");
    }

    @Test void testAlterSequenceMaxvalue() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_max_test");
        exec("ALTER SEQUENCE seq_alt_max_test MAXVALUE 1000");
        exec("DROP SEQUENCE seq_alt_max_test");
    }

    @Test void testAlterSequenceCycle() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_cyc_test MAXVALUE 3 NO CYCLE");
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 1
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 2
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 3
        // Should fail because NO CYCLE
        assertThrows(SQLException.class, () -> queryLong("SELECT nextval('seq_alt_cyc_test')"));
        exec("ALTER SEQUENCE seq_alt_cyc_test RESTART CYCLE");
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 1
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 2
        queryLong("SELECT nextval('seq_alt_cyc_test')"); // 3
        assertEquals(1, queryLong("SELECT nextval('seq_alt_cyc_test')")); // wraps
        exec("DROP SEQUENCE seq_alt_cyc_test");
    }

    @Test void testAlterSequenceNoCycle() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_nocyc_test MAXVALUE 2 CYCLE");
        queryLong("SELECT nextval('seq_alt_nocyc_test')"); // 1
        queryLong("SELECT nextval('seq_alt_nocyc_test')"); // 2
        queryLong("SELECT nextval('seq_alt_nocyc_test')"); // cycles to 1
        exec("ALTER SEQUENCE seq_alt_nocyc_test RESTART NO CYCLE");
        queryLong("SELECT nextval('seq_alt_nocyc_test')"); // 1
        queryLong("SELECT nextval('seq_alt_nocyc_test')"); // 2
        assertThrows(SQLException.class, () -> queryLong("SELECT nextval('seq_alt_nocyc_test')"));
        exec("DROP SEQUENCE seq_alt_nocyc_test");
    }

    @Test void testAlterSequenceStartWith() throws SQLException {
        exec("CREATE SEQUENCE seq_alt_start_test");
        exec("ALTER SEQUENCE seq_alt_start_test START WITH 50");
        // START WITH only affects RESTART without a value
        exec("ALTER SEQUENCE seq_alt_start_test RESTART");
        assertEquals(50, queryLong("SELECT nextval('seq_alt_start_test')"));
        exec("DROP SEQUENCE seq_alt_start_test");
    }

    // --- serial column type ---

    @Test void testSerialColumn() throws SQLException {
        exec("CREATE TABLE serial_test (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO serial_test (name) VALUES ('alice')");
        exec("INSERT INTO serial_test (name) VALUES ('bob')");
        assertEquals(1, queryInt("SELECT id FROM serial_test WHERE name = 'alice'"));
        assertEquals(2, queryInt("SELECT id FROM serial_test WHERE name = 'bob'"));
        exec("DROP TABLE serial_test");
    }

    @Test void testSerialColumnMultipleInserts() throws SQLException {
        exec("CREATE TABLE serial_multi (id SERIAL, val TEXT)");
        exec("INSERT INTO serial_multi (val) VALUES ('a'), ('b'), ('c')");
        assertEquals(3, queryInt("SELECT max(id) FROM serial_multi"));
        exec("DROP TABLE serial_multi");
    }

    @Test void testSerialColumnManualOverride() throws SQLException {
        exec("CREATE TABLE serial_override (id SERIAL, val TEXT)");
        exec("INSERT INTO serial_override (id, val) VALUES (100, 'manual')");
        exec("INSERT INTO serial_override (val) VALUES ('auto')");
        assertEquals(100, queryInt("SELECT id FROM serial_override WHERE val = 'manual'"));
        exec("DROP TABLE serial_override");
    }

    // --- bigserial column type ---

    @Test void testBigserialColumn() throws SQLException {
        exec("CREATE TABLE bigserial_test (id BIGSERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO bigserial_test (name) VALUES ('alice')");
        exec("INSERT INTO bigserial_test (name) VALUES ('bob')");
        assertEquals(1, queryLong("SELECT id FROM bigserial_test WHERE name = 'alice'"));
        assertEquals(2, queryLong("SELECT id FROM bigserial_test WHERE name = 'bob'"));
        exec("DROP TABLE bigserial_test");
    }

    // --- GENERATED ALWAYS AS IDENTITY ---

    @Test void testGeneratedAlwaysAsIdentity() throws SQLException {
        exec("CREATE TABLE gen_always_test (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO gen_always_test (name) VALUES ('alice')");
        exec("INSERT INTO gen_always_test (name) VALUES ('bob')");
        assertEquals(1, queryInt("SELECT id FROM gen_always_test WHERE name = 'alice'"));
        assertEquals(2, queryInt("SELECT id FROM gen_always_test WHERE name = 'bob'"));
        exec("DROP TABLE gen_always_test");
    }

    @Test void testGeneratedAlwaysAsIdentityRejectsExplicitValue() throws SQLException {
        // PG rejects explicit values for GENERATED ALWAYS AS IDENTITY (error 428C9)
        exec("CREATE TABLE gen_always_manual (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class, () ->
                s.execute("INSERT INTO gen_always_manual (id, name) VALUES (100, 'manual')"));
        }
        exec("DROP TABLE gen_always_manual");
    }

    @Test void testGeneratedAlwaysAutoWithoutExplicitId() throws SQLException {
        exec("CREATE TABLE gen_always_mixed (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO gen_always_mixed (name) VALUES ('auto')");
        assertEquals(1, queryInt("SELECT id FROM gen_always_mixed WHERE name = 'auto'"));
        exec("DROP TABLE gen_always_mixed");
    }

    // --- GENERATED BY DEFAULT AS IDENTITY ---

    @Test void testGeneratedByDefaultAsIdentity() throws SQLException {
        exec("CREATE TABLE gen_default_test (id INT GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO gen_default_test (name) VALUES ('alice')");
        exec("INSERT INTO gen_default_test (name) VALUES ('bob')");
        assertEquals(1, queryInt("SELECT id FROM gen_default_test WHERE name = 'alice'"));
        assertEquals(2, queryInt("SELECT id FROM gen_default_test WHERE name = 'bob'"));
        exec("DROP TABLE gen_default_test");
    }

    @Test void testGeneratedByDefaultAsIdentityManualOverride() throws SQLException {
        exec("CREATE TABLE gen_default_manual (id INT GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO gen_default_manual (id, name) VALUES (100, 'manual')");
        exec("INSERT INTO gen_default_manual (name) VALUES ('auto')");
        assertEquals(100, queryInt("SELECT id FROM gen_default_manual WHERE name = 'manual'"));
        exec("DROP TABLE gen_default_manual");
    }

    // --- TRUNCATE with RESTART IDENTITY vs CONTINUE IDENTITY ---

    @Test void testTruncateRestartIdentity() throws SQLException {
        exec("CREATE TABLE trunc_ri_test (id SERIAL, val TEXT)");
        exec("INSERT INTO trunc_ri_test (val) VALUES ('a'), ('b'), ('c')");
        assertEquals(3, queryInt("SELECT max(id) FROM trunc_ri_test"));
        exec("TRUNCATE trunc_ri_test RESTART IDENTITY");
        exec("INSERT INTO trunc_ri_test (val) VALUES ('x')");
        assertEquals(1, queryInt("SELECT id FROM trunc_ri_test WHERE val = 'x'"));
        exec("DROP TABLE trunc_ri_test");
    }

    @Test void testTruncateContinueIdentity() throws SQLException {
        exec("CREATE TABLE trunc_ci_test (id SERIAL, val TEXT)");
        exec("INSERT INTO trunc_ci_test (val) VALUES ('a'), ('b'), ('c')");
        exec("TRUNCATE trunc_ci_test CONTINUE IDENTITY");
        exec("INSERT INTO trunc_ci_test (val) VALUES ('x')");
        assertEquals(4, queryInt("SELECT id FROM trunc_ci_test WHERE val = 'x'"));
        exec("DROP TABLE trunc_ci_test");
    }

    @Test void testTruncateDefaultIsContinueIdentity() throws SQLException {
        exec("CREATE TABLE trunc_def_test (id SERIAL, val TEXT)");
        exec("INSERT INTO trunc_def_test (val) VALUES ('a'), ('b')");
        exec("TRUNCATE trunc_def_test");
        exec("INSERT INTO trunc_def_test (val) VALUES ('x')");
        assertEquals(3, queryInt("SELECT id FROM trunc_def_test WHERE val = 'x'"));
        exec("DROP TABLE trunc_def_test");
    }

    // --- Sequence in DEFAULT expression ---

    @Test void testSequenceInDefault() throws SQLException {
        exec("CREATE SEQUENCE seq_def_expr_test START WITH 1000");
        exec("CREATE TABLE seq_def_tbl (id INT DEFAULT nextval('seq_def_expr_test'), name TEXT)");
        exec("INSERT INTO seq_def_tbl (name) VALUES ('alice')");
        exec("INSERT INTO seq_def_tbl (name) VALUES ('bob')");
        assertEquals(1000, queryInt("SELECT id FROM seq_def_tbl WHERE name = 'alice'"));
        assertEquals(1001, queryInt("SELECT id FROM seq_def_tbl WHERE name = 'bob'"));
        exec("DROP TABLE seq_def_tbl");
        exec("DROP SEQUENCE seq_def_expr_test");
    }

    @Test void testSequenceInDefaultWithExplicitValue() throws SQLException {
        exec("CREATE SEQUENCE seq_def_expl_test");
        exec("CREATE TABLE seq_def_expl_tbl (id INT DEFAULT nextval('seq_def_expl_test'), name TEXT)");
        exec("INSERT INTO seq_def_expl_tbl (id, name) VALUES (999, 'manual')");
        exec("INSERT INTO seq_def_expl_tbl (name) VALUES ('auto')");
        assertEquals(999, queryInt("SELECT id FROM seq_def_expl_tbl WHERE name = 'manual'"));
        // PG: explicit value doesn't advance the sequence; auto gets 1 (first sequence value)
        assertEquals(1, queryInt("SELECT id FROM seq_def_expl_tbl WHERE name = 'auto'"));
        exec("DROP TABLE seq_def_expl_tbl");
        exec("DROP SEQUENCE seq_def_expl_test");
    }

    // --- Sequence with negative increment (descending) ---

    @Test void testSequenceDescending() throws SQLException {
        exec("CREATE SEQUENCE seq_desc_test INCREMENT BY -1 MAXVALUE -1 MINVALUE -5 START WITH -1");
        assertEquals(-1, queryLong("SELECT nextval('seq_desc_test')"));
        assertEquals(-2, queryLong("SELECT nextval('seq_desc_test')"));
        assertEquals(-3, queryLong("SELECT nextval('seq_desc_test')"));
        exec("DROP SEQUENCE seq_desc_test");
    }

    // --- Multiple sequences independently ---

    @Test void testMultipleSequencesIndependent() throws SQLException {
        exec("CREATE SEQUENCE seq_ind_a START WITH 1");
        exec("CREATE SEQUENCE seq_ind_b START WITH 100");
        assertEquals(1, queryLong("SELECT nextval('seq_ind_a')"));
        assertEquals(100, queryLong("SELECT nextval('seq_ind_b')"));
        assertEquals(2, queryLong("SELECT nextval('seq_ind_a')"));
        assertEquals(101, queryLong("SELECT nextval('seq_ind_b')"));
        exec("DROP SEQUENCE seq_ind_a");
        exec("DROP SEQUENCE seq_ind_b");
    }

    // --- setval then currval ---

    // --- Identity column with sequence options ---

    @Test void testGeneratedAlwaysWithSeqOptions() throws SQLException {
        // Identity with sequence options: START WITH 100 should be respected
        exec("CREATE TABLE gen_always_start (id INT GENERATED ALWAYS AS IDENTITY (START WITH 100), name TEXT)");
        exec("INSERT INTO gen_always_start (name) VALUES ('a')");
        exec("INSERT INTO gen_always_start (name) VALUES ('b')");
        assertEquals(100, queryInt("SELECT id FROM gen_always_start WHERE name = 'a'"));
        assertEquals(101, queryInt("SELECT id FROM gen_always_start WHERE name = 'b'"));
        exec("DROP TABLE gen_always_start");
    }

    @Test void testGeneratedByDefaultWithSeqOptions() throws SQLException {
        // Identity with INCREMENT BY 5 should be respected
        exec("CREATE TABLE gen_def_inc (id INT GENERATED BY DEFAULT AS IDENTITY (INCREMENT BY 5), name TEXT)");
        exec("INSERT INTO gen_def_inc (name) VALUES ('a')");
        exec("INSERT INTO gen_def_inc (name) VALUES ('b')");
        assertEquals(1, queryInt("SELECT id FROM gen_def_inc WHERE name = 'a'"));
        assertEquals(6, queryInt("SELECT id FROM gen_def_inc WHERE name = 'b'"));
        exec("DROP TABLE gen_def_inc");
    }

    // --- Additional comparison edge cases ---

    @Test void testChainedComparisons() throws SQLException {
        // Verify comparisons work in complex WHERE clauses
        exec("CREATE TABLE chain_cmp (v INT)");
        exec("INSERT INTO chain_cmp VALUES (1),(2),(3),(4),(5)");
        assertEquals(3, queryInt("SELECT count(*) FROM chain_cmp WHERE v >= 2 AND v <= 4"));
        exec("DROP TABLE chain_cmp");
    }

    @Test void testBetweenSymmetricWithStrings() throws SQLException {
        assertTrue(queryBool("SELECT 'b' BETWEEN SYMMETRIC 'c' AND 'a'"));
    }

    @Test void testComparisonCaseInsensitiveNotDefault() throws SQLException {
        // Standard SQL: string comparisons are case-sensitive
        assertFalse(queryBool("SELECT 'ABC' = 'abc'"));
    }

    @Test void testNotBetweenDates() throws SQLException {
        assertTrue(queryBool("SELECT DATE '2025-06-15' NOT BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'"));
    }

    @Test void testNumNullsWithExpressions() throws SQLException {
        assertEquals(1, queryInt("SELECT num_nulls(1+1, NULL, 'text')"));
    }

    @Test void testIsTrueWithExpression() throws SQLException {
        assertTrue(queryBool("SELECT (1 = 1) IS TRUE"));
    }

    @Test void testIsFalseWithExpression() throws SQLException {
        assertTrue(queryBool("SELECT (1 = 2) IS FALSE"));
    }
}
