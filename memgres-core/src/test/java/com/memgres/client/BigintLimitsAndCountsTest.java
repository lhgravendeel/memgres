package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL resolves an overload from the argument's declared type and never looks at the value
 * in it. It declares {@code left}, {@code right}, {@code repeat}, {@code lpad}, {@code rpad},
 * {@code substr}, {@code substring}, {@code split_part}, {@code overlay}, {@code chr} and the
 * regexp positional routines with an {@code integer} count and no int8, numeric, real or float8
 * form, and none of those types has an implicit cast down to int4 — so a bigint column holding 4
 * finds no function at all, exactly as a literal four billion does. int2 does cast up to int4 and
 * keeps working.
 *
 * <p>Every expectation here was measured on PostgreSQL 18 (localhost:5432/memgrestest).
 */
class BigintLimitsAndCountsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE fes_t (n bigint, m int, sm smallint, nu numeric, d float8, s text)");
        exec("INSERT INTO fes_t VALUES (4, 4, 4, 4, 4, 'abcde')");
        exec("CREATE SEQUENCE fes_seq");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void noSuchFunction(String signature, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals("42883", e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains("function " + signature + " does not exist"),
                "expected function " + signature + " does not exist, got: " + e.getMessage());
    }

    private static void failsWith(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected " + messagePart + " in: " + e.getMessage());
    }

    @Test
    void aBigintTypedCountFindsNoOverloadHoweverSmallItsValue() {
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 4::bigint)");
        noSuchFunction("right(unknown, bigint)", "SELECT right('abcde', 4::bigint)");
        noSuchFunction("repeat(unknown, bigint)", "SELECT repeat('ab', 3::bigint)");
        noSuchFunction("lpad(unknown, bigint, unknown)", "SELECT lpad('abc', 6::bigint, 'x')");
        noSuchFunction("lpad(unknown, bigint)", "SELECT lpad('abc', 6::bigint)");
        noSuchFunction("rpad(unknown, bigint, unknown)", "SELECT rpad('abc', 6::bigint, 'x')");
        noSuchFunction("rpad(unknown, bigint)", "SELECT rpad('abc', 6::bigint)");
        noSuchFunction("substr(unknown, bigint, bigint)",
                "SELECT substr('abcdef', 2::bigint, 3::bigint)");
        noSuchFunction("substr(unknown, bigint)", "SELECT substr('abcdef', 2::bigint)");
        noSuchFunction("substring(unknown, bigint, integer)",
                "SELECT substring('abcdef', 2::bigint, 3)");
        noSuchFunction("split_part(unknown, unknown, bigint)",
                "SELECT split_part('a,b,c', ',', 2::bigint)");
        noSuchFunction("chr(bigint)", "SELECT chr(65::bigint)");
        // the syntax-form routines are reported schema-qualified
        noSuchFunction("pg_catalog.overlay(unknown, unknown, bigint, bigint)",
                "SELECT overlay('abcdef' placing 'XY' from 2::bigint for 3::bigint)");
        noSuchFunction("pg_catalog.overlay(unknown, unknown, integer, bigint)",
                "SELECT overlay('abcdef' placing 'XY' from 2 for 3::bigint)");
    }

    @Test
    void aBigintColumnIsTheCaseAnApplicationActuallyHits() {
        noSuchFunction("left(text, bigint)", "SELECT left(s, n) FROM fes_t");
        noSuchFunction("split_part(unknown, unknown, bigint)",
                "SELECT split_part('a,b,c', ',', n) FROM fes_t");
        noSuchFunction("left(unknown, bigint)",
                "WITH c AS (SELECT n FROM fes_t) SELECT left('abcde', n) FROM c");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', (SELECT n FROM fes_t))");
        noSuchFunction("repeat(unknown, bigint)",
                "SELECT repeat('ab', (SELECT count(*) FROM fes_t))");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', coalesce(n, 4)) FROM fes_t");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', greatest(1::bigint, 4))");
        noSuchFunction("left(unknown, bigint)",
                "SELECT left('abcde', CASE WHEN true THEN 4::bigint ELSE 2 END)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', abs(-4::bigint))");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', nextval('fes_seq'))");
    }

    @Test
    void numericRealAndFloat8CountsAreRefusedByTheirOwnNames() {
        noSuchFunction("left(unknown, numeric)", "SELECT left('abcde', 4.0)");
        noSuchFunction("repeat(unknown, numeric)", "SELECT repeat('ab', 3.7)");
        noSuchFunction("lpad(unknown, numeric, unknown)", "SELECT lpad('abc', 6.2, 'x')");
        noSuchFunction("split_part(unknown, unknown, numeric)",
                "SELECT split_part('a,b,c', ',', 2::numeric)");
        noSuchFunction("left(unknown, double precision)", "SELECT left('abcde', 4::float8)");
        noSuchFunction("left(unknown, real)", "SELECT left('abcde', 4::real)");
        noSuchFunction("left(unknown, numeric)", "SELECT left('abcde', nu) FROM fes_t");
        noSuchFunction("left(unknown, double precision)", "SELECT left('abcde', d) FROM fes_t");
        // a written constant past bigint is numeric, not bigint
        noSuchFunction("left(unknown, numeric)", "SELECT left('abcde', 9223372036854775808)");
    }

    @Test
    void theWidthIsReadOffTheWrittenConstantAndTheArithmeticOverIt() {
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 4294967296)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 2147483648)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', (4294967296 - 4294967292))");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', 2::bigint + 2::bigint)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', -2147483649)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', length('abcd')::bigint)");
    }

    @Test
    void resolutionHappensBeforeExecutionSoNullDoesNotAnswerFirst() {
        noSuchFunction("left(unknown, bigint)", "SELECT left(NULL, 4::bigint)");
        noSuchFunction("left(unknown, bigint)", "SELECT left('abcde', NULL::bigint)");
        noSuchFunction("split_part(unknown, unknown, bigint)",
                "SELECT split_part(NULL, ',', 2::bigint)");
        noSuchFunction("repeat(unknown, bigint)", "SELECT repeat(NULL, 3::bigint)");
        noSuchFunction("substr(unknown, bigint)", "SELECT substr(NULL, 2::bigint)");
    }

    @Test
    void theRefusalSurvivesEveryPlaceTheCallCanSit() {
        noSuchFunction("left(unknown, bigint)",
                "SELECT x FROM (SELECT left('abcde', 4::bigint) AS x) q");
        noSuchFunction("left(unknown, bigint)",
                "SELECT left('abcde', 4::bigint) UNION ALL SELECT 'z'");
        noSuchFunction("left(unknown, bigint)",
                "SELECT left('abcde', 4::bigint) INTERSECT SELECT 'abcd'");
        noSuchFunction("left(unknown, bigint)", "SELECT (SELECT left('abcde', 4::bigint))");
        noSuchFunction("left(text, bigint)", "SELECT * FROM fes_t WHERE left(s, n) = 'abcd'");
    }

    @Test
    void theRegexpRoutinesTakeTheirPositionsAsIntegerToo() {
        noSuchFunction("regexp_count(unknown, unknown, bigint)",
                "SELECT regexp_count('abcabc', 'b', 1::bigint)");
        noSuchFunction("regexp_substr(unknown, unknown, bigint)",
                "SELECT regexp_substr('abcabc', 'b', 1::bigint)");
        noSuchFunction("regexp_instr(unknown, unknown, bigint)",
                "SELECT regexp_instr('abcabc', 'b', 1::bigint)");
        noSuchFunction("regexp_replace(unknown, unknown, unknown, bigint, integer)",
                "SELECT regexp_replace('abcabc', 'b', 'X', 1::bigint, 0)");
        noSuchFunction("regexp_substr(unknown, unknown, integer, bigint)",
                "SELECT regexp_substr('abcabc', 'b', 1, 1::bigint)");
    }

    @Test
    void everythingAnIntegerParameterAcceptsKeepsWorking() throws Exception {
        assertEquals("abcd", scalar("SELECT left('abcde', 4)"));
        assertEquals("abcd", scalar("SELECT left('abcde', 4::int)"));
        assertEquals("abcd", scalar("SELECT left('abcde', 4::integer)"));
        // int2 has an implicit cast up to int4
        assertEquals("abcd", scalar("SELECT left('abcde', 4::smallint)"));
        assertEquals("abcd", scalar("SELECT left(s, sm) FROM fes_t"));
        assertEquals("abcd", scalar("SELECT left(s, m) FROM fes_t"));
        // a bare literal is unknown and takes the parameter's own type
        assertEquals("abcd", scalar("SELECT left('abcde', '4')"));
        assertEquals("abcd", scalar("SELECT left('abcde', char_length('abcd'))"));
        assertEquals("abcd", scalar("SELECT left('abcde', length('abcd'))"));
        assertEquals("abcd", scalar("SELECT left('abcde', strpos('abcd', 'd'))"));
        assertEquals("abcd", scalar("SELECT left('abcde', position('d' in 'abcd'))"));
        assertEquals("abcd", scalar("SELECT left('abcde', abs(-4))"));
        assertEquals("abcd", scalar("SELECT left('abcde', 2 + 2)"));
        assertEquals("abcd", scalar("SELECT left('abcde', (SELECT 4))"));
        assertEquals("abcd", scalar("SELECT left('abcde', (SELECT m FROM fes_t))"));
        assertEquals("abcd", scalar("SELECT left('abcde', greatest(1, 4))"));
        assertEquals("abcd", scalar("SELECT left('abcde', CASE WHEN true THEN 4 ELSE 2 END)"));
        assertEquals("abcd", scalar("SELECT left('abcde', coalesce(m, 4)) FROM fes_t"));
        assertEquals("abcd", scalar("SELECT left('abcde', max(m)) FROM fes_t"));
        assertEquals("abcd", scalar("SELECT left('abcde', nullif(4, 9))"));
        assertEquals("ab", scalar("SELECT left('abcde', (2::bigint)::int)"));
        assertEquals("a", scalar("SELECT left('abcde', (row_number() over ())::int) FROM fes_t"));
    }

    @Test
    void theRefusalIsAboutTypeAndNeverAboutSign() throws Exception {
        assertEquals("abc", scalar("SELECT left('abcde', -2)"));
        assertEquals("cde", scalar("SELECT right('abcde', -2)"));
        assertEquals("", scalar("SELECT repeat('ab', -2)"));
        assertEquals("", scalar("SELECT lpad('abc', -2, 'x')"));
        assertEquals("", scalar("SELECT rpad('abc', -2, 'x')"));
        assertEquals("c", scalar("SELECT split_part('a,b,c', ',', -1)"));
        // the bottom of the integer range: PostgreSQL folds the sign into the constant
        assertEquals("", scalar("SELECT left('abcde', -2147483648)"));
        assertEquals("abcde", scalar("SELECT left('abcde', 2147483647)"));
    }

    @Test
    void everyOtherRoutineKeepsItsIntegerForm() throws Exception {
        assertEquals("ababab", scalar("SELECT repeat('ab', 3)"));
        assertEquals("  ", scalar("SELECT repeat(' ', 2::smallint)"));
        assertEquals("   abc", scalar("SELECT lpad('abc', 6)"));
        assertEquals("xxxabc", scalar("SELECT lpad('abc', 6, 'x')"));
        assertEquals("abcxxx", scalar("SELECT rpad('abc', 6, 'x')"));
        assertEquals("abc", scalar("SELECT lpad('abc', 6, '')"));
        assertEquals("bcd", scalar("SELECT substr('abcdef', 2, 3)"));
        assertEquals("bcdef", scalar("SELECT substr('abcdef', 2)"));
        assertEquals("bcd", scalar("SELECT substring('abcdef', 2, 3)"));
        assertEquals("bcd", scalar("SELECT substring('abcdef' from 2 for 3)"));
        assertEquals("bcdef", scalar("SELECT substring('abcdef' from 2)"));
        assertEquals("b", scalar("SELECT split_part('a,b,c', ',', 2)"));
        assertEquals("aXYef", scalar("SELECT overlay('abcdef' placing 'XY' from 2 for 3)"));
        assertEquals("aXYdef", scalar("SELECT overlay('abcdef' placing 'XY' from 2)"));
        assertEquals("A", scalar("SELECT chr(65)"));
        assertEquals("A", scalar("SELECT chr(65::smallint)"));
        // the pattern forms of substring take strings, not counts, and are untouched
        assertEquals("cd", scalar("SELECT substring('abcdef' from '%#\"cd#\"%' for '#')"));
        assertEquals("cd", scalar("SELECT substring('abcdef' from 'c.')"));
        // the regexp routines with integer positions
        assertEquals("2", scalar("SELECT regexp_count('abcabc', 'b', 1)"));
        assertEquals("2", scalar("SELECT regexp_count('abcabc', 'b', 1, 'i')"));
        assertEquals("b", scalar("SELECT regexp_substr('abcabc', 'b', 1, 1)"));
        assertEquals("2", scalar("SELECT regexp_instr('abcabc', 'b', 1, 1, 0, 'i')"));
        assertEquals("aXcaXc", scalar("SELECT regexp_replace('abcabc', 'b', 'X', 1, 0)"));
        // a text flags argument sitting in the same position is not a count
        assertEquals("aXcaXc", scalar("SELECT regexp_replace('abcabc', 'b', 'X', 'g')"));
    }

    @Test
    void aNullCountWithNoTypeOfItsOwnIsStillNull() throws Exception {
        assertNull(scalar("SELECT left('abcde', NULL)"));
        assertNull(scalar("SELECT right('abcde', NULL)"));
        assertNull(scalar("SELECT repeat('ab', NULL)"));
        assertNull(scalar("SELECT lpad('abc', NULL, 'x')"));
        assertNull(scalar("SELECT split_part('a,b', ',', NULL)"));
        assertNull(scalar("SELECT substr(NULL, 1, 2)"));
        assertNull(scalar("SELECT chr(NULL)"));
    }

    @Test
    void theErrorsThatAreAboutTheValueStayAboutTheValue() {
        failsWith("22011", "negative substring length not allowed",
                "SELECT substr('abcdef', 2, -1)");
        failsWith("22023", "field position must not be zero",
                "SELECT split_part('a,b,c', ',', 0)");
        failsWith("54000", "null character not permitted", "SELECT chr(0)");
        failsWith("54000", "requested character too large for encoding: 2147483647",
                "SELECT chr(2147483647)");
        failsWith("22023", "character number must be positive", "SELECT chr(-1)");
        failsWith("22003", "integer out of range",
                "SELECT left('abcde', 4294967296::bigint::int)");
    }

    /** LIMIT and OFFSET are the other half of the question and are bigint, so these are legal. */
    @Test
    void limitAndOffsetAreBigintAndTakeTheSameNumbersHappily() throws Exception {
        assertEquals("4", scalar("SELECT m FROM fes_t LIMIT 4294967296"));
        assertEquals("4", scalar("SELECT m FROM fes_t LIMIT (2147483647::bigint + 1)"));
        assertEquals("4", scalar("SELECT m FROM fes_t LIMIT 4294967296::bigint"));
        assertEquals("4", scalar("SELECT m FROM fes_t FETCH FIRST 4294967296 ROWS ONLY"));
        assertEquals("4", scalar("SELECT m FROM fes_t LIMIT ALL"));
        assertEquals("4", scalar("SELECT m FROM fes_t LIMIT NULL"));
        assertEquals("4", scalar("SELECT * FROM (SELECT m FROM fes_t LIMIT 4294967296) q"));
        assertEquals("4", scalar("SELECT (SELECT m FROM fes_t LIMIT 4294967296)"));
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT m FROM fes_t OFFSET 4294967296")) {
            assertFalse(rs.next(), "an offset past the end skips every row");
        }
        failsWith("22003", "bigint out of range",
                "SELECT m FROM fes_t LIMIT 9223372036854775808");
        failsWith("2201W", "LIMIT must not be negative", "SELECT m FROM fes_t LIMIT -1");
        failsWith("2201X", "OFFSET must not be negative", "SELECT m FROM fes_t OFFSET -1");
    }
}
