package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the grammar accepts, and where it stops.
 *
 * <p>Each of these was read as something PostgreSQL does not read it as. A comparison could be
 * chained, so {@code 1 < 0 < 5} answered f instead of being the syntax error it is. The IS forms
 * were part of an operand rather than looser than one, so {@code 1 = 1 IS NULL} could not be
 * written at all. A prefix operator bound tighter than arithmetic, so {@code ~ 2 + 1} complemented
 * the 2 and not the sum. And a keyword was allowed to name a column whether or not PostgreSQL
 * reserves it — while three that it does not reserve could not name one.
 */
class ExpressionGrammarTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    private static void syntaxError(String sql) {
        assertEquals("42601", refused(sql).getSQLState(), sql);
    }

    /** Comparison operators do not chain. */
    @Test
    void comparisonsAreNonAssociative() throws Exception {
        syntaxError("SELECT 1 < 0 < 5");
        syntaxError("SELECT 1 = 1 = 1");
        assertEquals("t", scalar("SELECT (1 < 0) < true"));
        assertEquals("t", scalar("SELECT 1 < 2 AND 2 < 3"));
    }

    /** The IS forms bind looser than a comparison, so they test its result. */
    @Test
    void theIsFormsComeAfterAComparison() throws Exception {
        assertEquals("f", scalar("SELECT 1 = 1 IS NULL"));
        assertEquals("t", scalar("SELECT 1 < 2 IS TRUE"));
        assertEquals("t", scalar("SELECT 1 = 1 IS DISTINCT FROM false"));
        assertEquals("t", scalar("SELECT 1 BETWEEN 0 AND 2 IS TRUE"));
        assertEquals("t", scalar("SELECT NULL IS NULL"));
    }

    /** A symbolic prefix operator binds looser than arithmetic. */
    @Test
    void prefixOperatorsBindLooseley() throws Exception {
        assertEquals("-4", scalar("SELECT ~ 2 + 1"));
        assertEquals("-11", scalar("SELECT ~ 5 * 2"));
        assertEquals("2", scalar("SELECT @ -3 + 1"));
        assertEquals("-3", scalar("SELECT ~ 2"));
    }

    /** ANY and ALL stand after a pattern operator as well as after a comparison. */
    @Test
    void patternOperatorsTakeAnyAndAll() throws Exception {
        assertEquals("t", scalar("SELECT 'abc' LIKE ANY(ARRAY['a%','z%'])"));
        assertEquals("f", scalar("SELECT 'abc' LIKE ALL(ARRAY['a%','z%'])"));
        assertEquals("t", scalar("SELECT 'abc' ~ ANY(ARRAY['^a','^z'])"));
        assertEquals("t", scalar("SELECT 'abc' ILIKE ANY(ARRAY['A%','z%'])"));
    }

    /** PostgreSQL 16's non-decimal integer literals. */
    @Test
    void integersCanBeWrittenInOtherBases() throws Exception {
        assertEquals("16", scalar("SELECT 0x10"));
        assertEquals("15", scalar("SELECT 0o17"));
        assertEquals("10", scalar("SELECT 0b1010"));
        assertEquals("255", scalar("SELECT 0xFF"));
    }

    /** A doubled escape in a U& string is one of itself. */
    @Test
    void unicodeStringsTakeADoubledEscape() throws Exception {
        assertEquals("a\\b", scalar("SELECT U&'a\\\\b'"));
        assertEquals("1", scalar("SELECT length(U&'\\\\')"));
    }

    /** A dollar quote's tag is an identifier, so there is no bare "$ ... $" form. */
    @Test
    void aDollarQuoteNeedsAnIdentifierTag() throws Exception {
        syntaxError("SELECT $ 'hello' $");
        assertEquals("hi", scalar("SELECT $$hi$$"));
        assertEquals("hi", scalar("SELECT $tag$hi$tag$"));
    }

    /** NULLIF is grammar, so three arguments is a syntax error at the second comma. */
    @Test
    void nullifTakesExactlyTwoArguments() throws Exception {
        SQLException e = refused("SELECT NULLIF(1, 2, 3)");
        assertEquals("42601", e.getSQLState());
        assertTrue(e.getMessage().contains("at or near \",\""), e.getMessage());
        assertEquals("1", scalar("SELECT NULLIF(1, 2)"));
    }

    /** A type name spelt in two words introduces a constant. */
    @Test
    void twoWordTypeNamesIntroduceConstants() throws Exception {
        assertEquals("1.5", scalar("SELECT double precision '1.5'"));
        assertEquals("1.5", scalar("SELECT character varying '1.5'"));
        assertEquals("101", scalar("SELECT bit varying '101'"));
    }

    /** COLLATION FOR reports what an expression carries. */
    @Test
    void collationForReportsTheCollation() throws Exception {
        assertEquals("\"C\"", scalar("SELECT collation for ('a' COLLATE \"C\")"));
        assertNull(scalar("SELECT collation for ('a')"));
    }

    /** LIMIT and OFFSET take an expression, not only a constant. */
    @Test
    void limitAndOffsetTakeExpressions() throws Exception {
        assertEquals("1", scalar("VALUES (1),(2) LIMIT 1+1"));
        assertEquals("3", scalar("VALUES (1),(2),(3) OFFSET 1+1"));
    }

    /** A reserved keyword cannot name a column or alias a relation. */
    @Test
    void reservedKeywordsAreNotNames() {
        syntaxError("CREATE TABLE zz_eg_kw (order int)");
        syntaxError("CREATE TABLE zz_eg_kw (select int)");
        syntaxError("CREATE TABLE zz_eg_kw (user int)");
        syntaxError("CREATE TABLE zz_eg_kw (from int)");
        syntaxError("CREATE TABLE zz_eg_kw (left int)");
    }

    /** A relation takes one alias. */
    @Test
    void aRelationTakesOneAlias() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_t (a int)");
        exec("INSERT INTO zz_eg_t VALUES (1)");
        syntaxError("SELECT * FROM zz_eg_t AS left");
        syntaxError("SELECT * FROM zz_eg_t AS inner");
        syntaxError("SELECT a FROM zz_eg_t x AS y");
        assertEquals("1", scalar("SELECT x.a FROM zz_eg_t AS x"));
    }

    /** A keyword no construct can follow is an ordinary column name. */
    @Test
    void columnNameKeywordsReadAsColumns() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_cn (exists int, trim int, greatest int)");
        exec("INSERT INTO zz_eg_cn VALUES (1, 7, 5)");
        assertEquals("1", scalar("SELECT exists FROM zz_eg_cn"));
        assertEquals("7", scalar("SELECT trim FROM zz_eg_cn"));
        assertEquals("5", scalar("SELECT greatest FROM zz_eg_cn"));
        assertEquals("t", scalar("SELECT user IS NOT NULL"));
        assertEquals("abc", scalar("SELECT trim(' abc ')"));
    }

    /** A quoted name means that exact name. */
    @Test
    void quotedNamesAreCaseSensitive() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_q (\"MiXeD\" int)");
        exec("INSERT INTO zz_eg_q VALUES (1)");
        assertEquals("42703", refused("SELECT \"mixed\" FROM zz_eg_q").getSQLState());
        assertEquals("42703", refused("UPDATE zz_eg_q SET \"mixed\" = 9").getSQLState());
        assertEquals("1", scalar("SELECT \"MiXeD\" FROM zz_eg_q"));

        exec("CREATE TEMP TABLE zz_eg_dc (keep int, gone int)");
        assertEquals("42703", refused("ALTER TABLE zz_eg_dc DROP COLUMN \"GONE\"").getSQLState());
        assertEquals("2", scalar("SELECT count(*) FROM information_schema.columns"
                + " WHERE table_name = 'zz_eg_dc'"));
    }

    /** A relation's own name standing where a value is wanted is the whole row. */
    @Test
    void aRelationNameIsItsWholeRow() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_m (a int, b int)");
        exec("INSERT INTO zz_eg_m VALUES (1,10)");
        assertEquals("1", scalar("SELECT (zz_eg_m).a FROM zz_eg_m"));
        assertEquals("1", scalar("SELECT (zz_eg_m.*).a FROM zz_eg_m"));
    }

    /** A SQL function's body may write its parameters by position. */
    @Test
    void aSqlBodyCanWriteItsParametersByPosition() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_q11 (a int)");
        exec("INSERT INTO zz_eg_q11 VALUES (1),(2)");
        exec("CREATE FUNCTION zz_eg_f(a int) RETURNS bigint LANGUAGE sql"
                + " AS $$ SELECT count(*) FROM zz_eg_q11 WHERE zz_eg_q11.a = $1 $$");
        try {
            assertEquals("1", scalar("SELECT zz_eg_f(1)"));
            assertEquals("0", scalar("SELECT zz_eg_f(9)"));
        } finally {
            exec("DROP FUNCTION IF EXISTS zz_eg_f(int)");
        }
    }

    /** A regclass prints the relation's own name, whatever case the text was written in. */
    @Test
    void aRegclassPrintsTheRelationsName() throws Exception {
        exec("CREATE TEMP TABLE zz_eg_r1 (a int)");
        assertEquals("zz_eg_r1", scalar("SELECT 'ZZ_EG_R1'::regclass::text"));
    }

    /** The schema-name rules a setting and a CREATE have to keep. */
    @Test
    void schemaNamesAreChecked() {
        syntaxError("SET search_path = $user, public");
        assertEquals("42939", refused("CREATE SCHEMA pg_zz_eg").getSQLState());
        syntaxError("ALTER SCHEMA IF EXISTS zz_eg_nosuch RENAME TO zz_eg_x");
    }
}
