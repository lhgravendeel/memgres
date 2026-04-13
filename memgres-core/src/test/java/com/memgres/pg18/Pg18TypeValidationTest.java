package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Type validation tests based on PG18 behavior differences.
 * Verifies that memgres produces correct SQLSTATE error codes and type
 * coercion semantics matching real PostgreSQL 18.
 */
class Pg18TypeValidationTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        // Create test types
        exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE DOMAIN posint AS integer CHECK (VALUE > 0)");
        exec("CREATE TYPE pair AS (x int, y int)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static ResultSet query(String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    static void assertSqlError(String sql, String expectedSqlState) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected SQLException with SQLSTATE " + expectedSqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", message: " + e.getMessage());
        }
    }

    static void assertSqlErrorLike(String sql, String expectedSqlState, String messagePart) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected SQLException with SQLSTATE " + expectedSqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", message: " + e.getMessage());
            assertTrue(e.getMessage().toLowerCase().contains(messagePart.toLowerCase()),
                    "Error message should contain '" + messagePart + "' but was: " + e.getMessage());
        }
    }

    // =========================================================================
    // 1. ENUM casting: invalid value
    // =========================================================================

    @Test
    void enumCast_invalidValue_shouldFail22P02() {
        assertSqlError("SELECT 'angry'::mood", "22P02");
    }

    @Test
    void enumCast_validValue_sad() throws SQLException {
        ResultSet rs = query("SELECT 'sad'::mood");
        assertTrue(rs.next());
        assertEquals("sad", rs.getString(1));
    }

    @Test
    void enumCast_validValue_ok() throws SQLException {
        ResultSet rs = query("SELECT 'ok'::mood");
        assertTrue(rs.next());
        assertEquals("ok", rs.getString(1));
    }

    @Test
    void enumCast_validValue_happy() throws SQLException {
        ResultSet rs = query("SELECT 'happy'::mood");
        assertTrue(rs.next());
        assertEquals("happy", rs.getString(1));
    }

    // =========================================================================
    // 2. ENUM comparison order (creation order: sad=0, ok=1, happy=2)
    // =========================================================================

    @Test
    void enumOrder_sadLessThanHappy() throws SQLException {
        ResultSet rs = query("SELECT 'sad'::mood < 'happy'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1), "sad should be < happy in enum creation order");
    }

    @Test
    void enumOrder_happyGreaterThanSad() throws SQLException {
        ResultSet rs = query("SELECT 'happy'::mood > 'sad'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumOrder_sadLessOrEqualOk() throws SQLException {
        ResultSet rs = query("SELECT 'sad'::mood <= 'ok'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumOrder_happyGreaterOrEqualHappy() throws SQLException {
        ResultSet rs = query("SELECT 'happy'::mood >= 'happy'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumOrder_equalsSameValue() throws SQLException {
        ResultSet rs = query("SELECT 'ok'::mood = 'ok'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumOrder_notEqualsDifferentValues() throws SQLException {
        ResultSet rs = query("SELECT 'sad'::mood != 'happy'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumOrder_okBetweenSadAndHappy() throws SQLException {
        ResultSet rs = query("SELECT 'ok'::mood > 'sad'::mood AND 'ok'::mood < 'happy'::mood");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    @Test
    void enumCastToText_usesAlphabeticalOrder() throws SQLException {
        // When cast to text, enum values compare alphabetically, not by creation order.
        // Creation order: sad=0, ok=1, happy=2
        // Alphabetical: happy < ok < sad
        ResultSet rs = query("SELECT 'sad'::mood::text < 'happy'::mood::text");
        assertTrue(rs.next());
        // Alphabetically: 'sad' > 'happy', so sad::text < happy::text is FALSE
        assertFalse(rs.getBoolean(1), "text-cast enum should compare alphabetically: 'sad' > 'happy'");
    }

    @Test
    void enumCastToText_orderBy() throws SQLException {
        exec("CREATE TABLE enum_txt_ord (id serial, m mood)");
        exec("INSERT INTO enum_txt_ord(m) VALUES ('sad'), ('ok'), ('happy')");
        // ORDER BY m::text should be alphabetical: happy, ok, sad
        ResultSet rs = query("SELECT m FROM enum_txt_ord ORDER BY m::text");
        assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
        assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
        assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
        assertFalse(rs.next());
        exec("DROP TABLE enum_txt_ord");
    }

    @Test
    void enumNativeOrder_orderBy() throws SQLException {
        exec("CREATE TABLE enum_nat_ord (id serial, m mood)");
        exec("INSERT INTO enum_nat_ord(m) VALUES ('sad'), ('ok'), ('happy')");
        // ORDER BY m (no cast) should be creation order: sad, ok, happy
        ResultSet rs = query("SELECT m FROM enum_nat_ord ORDER BY m");
        assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
        assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
        assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
        assertFalse(rs.next());
        exec("DROP TABLE enum_nat_ord");
    }

    // =========================================================================
    // 3. Domain constraint enforcement on cast
    // =========================================================================

    
    @Test
    void domainCast_negativeValue_shouldFail23514() {
        assertSqlError("SELECT (-1)::posint", "23514");
    }

    
    @Test
    void domainCast_zeroValue_shouldFail23514() {
        assertSqlError("SELECT 0::posint", "23514");
    }

    @Test
    void domainCast_positiveValue_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 1::posint");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    void domainCast_largePositiveValue_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 999::posint");
        assertTrue(rs.next());
        assertEquals(999, rs.getInt(1));
    }

    // =========================================================================
    // 4. ROW cast to composite: arity mismatch
    // =========================================================================

    @Test
    void rowCastComposite_wrongArity_shouldFail42846() {
        assertSqlError("SELECT ROW(1)::pair", "42846");
    }

    @Test
    void rowCastComposite_correctArity_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT ROW(1, 2)::pair");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    // =========================================================================
    // 5. Array element type checking
    // =========================================================================

    
    @Test
    void arrayMixedTypes_intAndString_shouldFail22P02() {
        assertSqlError("SELECT ARRAY[1,'x']", "22P02");
    }

    @Test
    void arrayHomogeneous_ints_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT ARRAY[1, 2, 3]");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    void arrayHomogeneous_strings_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT ARRAY['a', 'b', 'c']");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    void arrayNumericPromotion_intAndDecimal_shouldSucceed() throws SQLException {
        // numeric promotion: int + decimal = decimal array
        ResultSet rs = query("SELECT ARRAY[1, 2.5]");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    // =========================================================================
    // 6. Bigint overflow detection
    // =========================================================================

    
    @Test
    void bigintOverflow_additionMax_shouldFail22003() {
        assertSqlError("SELECT 9223372036854775807::bigint + 1", "22003");
    }

    
    @Test
    void bigintOverflow_subtractionMin_shouldFail22003() {
        assertSqlError("SELECT (-9223372036854775808)::bigint - 1", "22003");
    }

    
    @Test
    void bigintOverflow_multiplication_shouldFail22003() {
        assertSqlError("SELECT 9223372036854775807::bigint * 2", "22003");
    }

    
    @Test
    void integerOverflow_additionMax_shouldFail22003() {
        assertSqlError("SELECT 2147483647::integer + 1", "22003");
    }

    
    @Test
    void bigintOverflow_literalOutOfRange_shouldFail22003() {
        // PG18: SQLSTATE 22003 not 22P02
        assertSqlError("SELECT '999999999999999999999999999'::bigint", "22003");
    }

    // =========================================================================
    // 7. LIKE on non-text types
    // =========================================================================

    
    @Test
    void likeOnInteger_shouldFail42883() {
        assertSqlError("SELECT 1 LIKE '1'", "42883");
    }

    
    @Test
    void likeOnBoolean_shouldFail42883() {
        assertSqlError("SELECT true LIKE 't'", "42883");
    }

    @Test
    void likeOnText_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 'hello' LIKE 'hel%'");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    // =========================================================================
    // 8. CASE type coercion
    // =========================================================================

    
    @Test
    void caseTypeMismatch_intAndString_shouldFail22P02() {
        assertSqlError("SELECT CASE WHEN true THEN 1 ELSE 'x' END", "22P02");
    }

    @Test
    void caseSameType_ints_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT CASE WHEN true THEN 1 ELSE 2 END");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    void caseSameType_strings_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT CASE WHEN true THEN 'a' ELSE 'b' END");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
    }

    @Test
    void caseWithNull_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT CASE WHEN false THEN 1 ELSE NULL END");
        assertTrue(rs.next());
        rs.getInt(1);
        assertTrue(rs.wasNull());
    }

    // =========================================================================
    // 9. GREATEST/LEAST type coercion
    // =========================================================================

    
    @Test
    void greatestTypeMismatch_intAndString_shouldFail22P02() {
        assertSqlError("SELECT GREATEST(1, 'x')", "22P02");
    }

    
    @Test
    void leastTypeMismatch_intAndString_shouldFail22P02() {
        assertSqlError("SELECT LEAST(1, 'x')", "22P02");
    }

    @Test
    void greatestSameType_ints_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT GREATEST(1, 2, 3)");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
    }

    @Test
    void leastSameType_ints_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT LEAST(1, 2, 3)");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    void greatestSameType_strings_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT GREATEST('a', 'b', 'c')");
        assertTrue(rs.next());
        assertEquals("c", rs.getString(1));
    }

    // =========================================================================
    // 10. COALESCE type coercion
    // =========================================================================

    @Test
    void coalesceTypeMismatch_intAndString_shouldFail22P02() {
        assertSqlError("SELECT COALESCE(1, 'x')", "22P02");
    }

    @Test
    void coalesceSameType_nullAndInt_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT COALESCE(NULL, 1)");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    void coalesceSameType_intAndInt_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT COALESCE(5, 1)");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
    }

    // =========================================================================
    // 11. NULLIF type coercion
    // =========================================================================

    @Test
    void nullifTypeMismatch_intAndString_shouldFail22P02() {
        assertSqlError("SELECT NULLIF(1, 'x')", "22P02");
    }

    @Test
    void nullifSameType_equal_shouldReturnNull() throws SQLException {
        ResultSet rs = query("SELECT NULLIF(1, 1)");
        assertTrue(rs.next());
        rs.getInt(1);
        assertTrue(rs.wasNull());
    }

    @Test
    void nullifSameType_notEqual_shouldReturnFirst() throws SQLException {
        ResultSet rs = query("SELECT NULLIF(1, 2)");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    // =========================================================================
    // 12. Scalar = ROW comparison
    // =========================================================================

    @Test
    void scalarEqualsRow_shouldFail42883() {
        assertSqlError("SELECT 1 = ROW(1,2)", "42883");
    }

    // =========================================================================
    // 13. Integer + interval
    // =========================================================================

    @Test
    void integerPlusInterval_shouldFail42883() {
        assertSqlError("SELECT 1 + INTERVAL '1 day'", "42883");
    }

    @Test
    void datePlusInterval_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT DATE '2024-01-01' + INTERVAL '1 day'");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    @Test
    void timestampPlusInterval_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '2 hours'");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    // =========================================================================
    // 14. Invalid date_part unit
    // =========================================================================

    @Test
    void datePartInvalidUnit_shouldFail22023() {
        assertSqlError("SELECT date_part('bogus', TIMESTAMP '2024-01-01 00:00:00')", "22023");
    }

    @Test
    void datePartValidUnit_year_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT date_part('year', TIMESTAMP '2024-06-15 12:30:00')");
        assertTrue(rs.next());
        assertEquals(2024.0, rs.getDouble(1), 0.001);
    }

    @Test
    void datePartValidUnit_month_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT date_part('month', TIMESTAMP '2024-06-15 12:30:00')");
        assertTrue(rs.next());
        assertEquals(6.0, rs.getDouble(1), 0.001);
    }

    // =========================================================================
    // 15. pg_typeof display names
    // =========================================================================

    @Test
    void pgTypeof_time_shouldReturnFullName() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof(TIME '23:59:59')");
        assertTrue(rs.next());
        assertEquals("time without time zone", rs.getString(1));
    }

    @Test
    void pgTypeof_timestamp_shouldReturnFullName() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof(TIMESTAMP '2024-01-01 00:00:00')");
        assertTrue(rs.next());
        assertEquals("timestamp without time zone", rs.getString(1));
    }

    @Test
    void pgTypeof_timestamptz_shouldReturnFullName() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof(TIMESTAMPTZ '2024-01-01 00:00:00+00')");
        assertTrue(rs.next());
        assertEquals("timestamp with time zone", rs.getString(1));
    }

    @Test
    void pgTypeof_integer_shouldReturnInteger() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof(42)");
        assertTrue(rs.next());
        assertEquals("integer", rs.getString(1));
    }

    @Test
    void pgTypeof_boolean_shouldReturnBoolean() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof(true)");
        assertTrue(rs.next());
        assertEquals("boolean", rs.getString(1));
    }

    @Test
    void pgTypeof_text_shouldReturnText() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof('hello'::text)");
        assertTrue(rs.next());
        assertEquals("text", rs.getString(1));
    }

    @Test
    void pgTypeof_varchar_shouldReturnCharacterVarying() throws SQLException {
        ResultSet rs = query("SELECT pg_typeof('abc'::varchar(5))");
        assertTrue(rs.next());
        assertEquals("character varying", rs.getString(1));
    }

    // =========================================================================
    // 16. UNION type compatibility
    // =========================================================================

    @Test
    void unionTypeMismatch_intAndText_shouldFail22P02() {
        // PG 18: treats 'a' as unknown, attempts coercion to int, fails with 22P02
        assertSqlError("SELECT 1 UNION SELECT 'a'", "22P02");
    }

    @Test
    void unionSameType_ints_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 1 UNION SELECT 2");
        int count = 0;
        while (rs.next()) count++;
        assertEquals(2, count);
    }

    @Test
    void unionSameType_strings_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 'a' UNION SELECT 'b'");
        int count = 0;
        while (rs.next()) count++;
        assertEquals(2, count);
    }

    // =========================================================================
    // 17. substring on non-text
    // =========================================================================

    @Test
    void substringOnInteger_shouldFail42883() {
        assertSqlError("SELECT substring(1 FROM 1 FOR 1)", "42883");
    }

    @Test
    void substringOnText_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT substring('hello' FROM 2 FOR 3)");
        assertTrue(rs.next());
        assertEquals("ell", rs.getString(1));
    }

    // =========================================================================
    // 18. Invalid regex error code
    // =========================================================================

    @Test
    void invalidRegex_shouldFail2201B() {
        assertSqlError("SELECT 'abc' ~ '('", "2201B");
    }

    @Test
    void validRegex_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT 'abc' ~ '^a'");
        assertTrue(rs.next());
        assertTrue(rs.getBoolean(1));
    }

    // =========================================================================
    // 19. make_date out of range
    // =========================================================================

    @Test
    void makeDateOutOfRange_shouldFail22008() {
        assertSqlError("SELECT make_date(2024, 2, 30)", "22008");
    }

    @Test
    void makeDateValid_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT make_date(2024, 2, 29)");
        assertTrue(rs.next());
        assertEquals("2024-02-29", rs.getString(1));
    }

    // =========================================================================
    // 20. overlay negative / zero position
    // =========================================================================

    @Test
    void overlayZeroFrom_shouldFail22011() {
        assertSqlError("SELECT overlay('abc' placing 'xyz' from 0)", "22011");
    }

    @Test
    void overlayValidFrom_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT overlay('abc' placing 'xyz' from 1)");
        assertTrue(rs.next());
        assertNotNull(rs.getString(1));
    }

    // =========================================================================
    // 21. abs of non-numeric
    // =========================================================================

    @Test
    void absNonNumeric_shouldFail22P02() {
        assertSqlError("SELECT abs('x')", "22P02");
    }

    @Test
    void absNumeric_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT abs(-42)");
        assertTrue(rs.next());
        assertEquals(42, rs.getInt(1));
    }

    // =========================================================================
    // 22. LIMIT non-numeric
    // =========================================================================

    @Test
    void limitNonNumeric_shouldFail22P02() {
        assertSqlError("SELECT * FROM (SELECT 1) t LIMIT 'x'", "22P02");
    }

    @Test
    void limitNumeric_shouldSucceed() throws SQLException {
        ResultSet rs = query("SELECT * FROM (SELECT 1) t LIMIT 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    // =========================================================================
    // 23. Bigint literal overflow error code (22003 not 22P02)
    //     Already covered above but with different syntax
    // =========================================================================

    @Test
    void bigintLiteralOverflow_castSyntax_shouldFail22003() {
        assertSqlError("SELECT CAST('999999999999999999999999999' AS bigint)", "22003");
    }

    // =========================================================================
    // Additional type validation tests beyond the 23 specified
    // =========================================================================

    @Test
    void booleanCast_invalidValue_shouldFail22P02() {
        assertSqlError("SELECT 'x'::boolean", "22P02");
    }

    @Test
    void integerCast_invalidString_shouldFail22P02() {
        assertSqlError("SELECT 'abc'::integer", "22P02");
    }

    @Test
    void uuidCast_invalidFormat_shouldFail22P02() {
        assertSqlError("SELECT 'not a uuid'::uuid", "22P02");
    }

    @Test
    void dateCast_invalidDate_shouldFail22008() {
        assertSqlError("SELECT '2024-02-30'::date", "22008");
    }

    @Test
    void integerCast_infinity_shouldFail22P02() {
        assertSqlError("SELECT CAST('infinity' AS integer)", "22P02");
    }

    @Test
    void castIntegerToUuid_shouldFail42846() {
        assertSqlError("SELECT CAST(42 AS uuid)", "42846");
    }

    @Test
    void castRecordToInteger_shouldFail42846() {
        assertSqlError("SELECT CAST(ROW(1,'a') AS integer)", "42846");
    }
}
