package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a GROUP BY item licenses is the expression it is.
 *
 * <p>Grouping by an expression makes that expression available to the select list and nothing
 * else, so deciding what the query may say means comparing whole expressions. Compared as
 * canonicalised text, the comparison answered for the wrong things: every literal leaf was
 * lowercased, so {@code 'a'} and {@code 'A'} were the same value; a node holding its parts
 * privately had no parts left to compare, so one subscript was every subscript; and a
 * {@code ROW(...)} was flattened into its members as though it had been written as a bare
 * parenthesised list. Each of those let a query through that reads a column the grouping never
 * determined. A cast written {@code float} went the other way: unrecognised as the type the
 * column already has, it left a valid query refused.
 */
class GroupByItemLicensingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE gbl_t (a int, b int, d double precision, r real, s text,"
                    + " arr int[], j jsonb)");
            st.execute("INSERT INTO gbl_t VALUES"
                    + " (1, 2, 1.5, 1.5, 'x', '{10,20}', '{\"k\":1,\"m\":2}'),"
                    + " (3, 4, 2.5, 2.5, 'y', '{30,40}', '{\"k\":3,\"m\":4}')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** Two expressions differing only in a literal's case are two expressions. */
    @Test
    void aLiteralSaysWhatItSpells() {
        assertEquals("42803", stateOf("SELECT s || 'A' FROM gbl_t GROUP BY s || 'a'"));
        assertEquals("42803", stateOf("SELECT s = 'A' FROM gbl_t GROUP BY s = 'a'"));
        assertEquals("42803", stateOf("SELECT s LIKE 'A%' FROM gbl_t GROUP BY s LIKE 'a%'"));
    }

    @Test
    void theSameLiteralStillLicensesTheExpressionAroundIt() throws Exception {
        assertEquals(List.of("xA", "yA"),
                rows("SELECT s || 'A' FROM gbl_t GROUP BY s || 'A' ORDER BY 1"));
        assertEquals(List.of("f", "t"),
                rows("SELECT s LIKE 'x%' FROM gbl_t GROUP BY s LIKE 'x%' ORDER BY 1"));
    }

    /** A name is read without regard to case, which is what makes the literal rule its own. */
    @Test
    void aNameIsStillReadWithoutRegardToCase() throws Exception {
        assertEquals(List.of("X", "Y"), rows("SELECT upper(s) FROM gbl_t GROUP BY UPPER(s) ORDER BY 1"));
        assertEquals(List.of("1", "3"), rows("SELECT a::text FROM gbl_t GROUP BY a::TEXT ORDER BY 1"));
    }

    /** A node keeping its parts privately is still compared by them. */
    @Test
    void oneSubscriptIsNotEverySubscript() {
        assertEquals("42803", stateOf("SELECT arr[1] FROM gbl_t GROUP BY arr[2]"));
        assertEquals("42803", stateOf("SELECT arr[1:2] FROM gbl_t GROUP BY arr[1:1]"));
    }

    @Test
    void theSameSubscriptLicensesItself() throws Exception {
        assertEquals(List.of("10", "30"), rows("SELECT arr[1] FROM gbl_t GROUP BY arr[1] ORDER BY 1"));
        assertEquals(List.of("{10,20}", "{30,40}"),
                rows("SELECT arr[1:2] FROM gbl_t GROUP BY arr[1:2] ORDER BY 1"));
    }

    /** An operator reading a different argument is a different expression. */
    @Test
    void anOperatorIsComparedByWhatItReads() throws Exception {
        assertEquals("42803", stateOf("SELECT j -> 'k' FROM gbl_t GROUP BY j -> 'm'"));
        assertEquals(List.of("1", "3"),
                rows("SELECT j ->> 'k' FROM gbl_t GROUP BY j ->> 'k' ORDER BY 1"));
    }

    /** Written as a bare list, each member is an item of its own. */
    @Test
    void aParenthesisedListGroupsByEachMember() throws Exception {
        assertEquals(List.of("1|2", "3|4"), rows("SELECT a, b FROM gbl_t GROUP BY (a, b) ORDER BY 1"));
        assertEquals(List.of("1|2", "3|4"), rows("SELECT a, b FROM gbl_t GROUP BY ((a, b)) ORDER BY 1"));
        assertEquals(List.of("1|2", "3|4"),
                rows("SELECT a, b FROM gbl_t GROUP BY GROUPING SETS ((a, b)) ORDER BY 1"));
    }

    /** Written with the keyword, it is one expression of row type and licenses that row alone. */
    @Test
    void theRowKeywordGroupsByTheRowAndNothingInIt() {
        assertEquals("42803", stateOf("SELECT a, b FROM gbl_t GROUP BY ROW(a, b)"));
        assertEquals("42803", stateOf("SELECT a FROM gbl_t GROUP BY ROW(a)"));
        assertEquals("42803", stateOf("SELECT a, b FROM gbl_t GROUP BY ROLLUP (ROW(a, b))"));
    }

    /** How the row was written decides how the item is read, not what the expression is. */
    @Test
    void theRowItselfMayBeSelectedUnderEitherSpelling() throws Exception {
        assertEquals(List.of("(1,2)", "(3,4)"),
                rows("SELECT ROW(a, b) FROM gbl_t GROUP BY ROW(a, b) ORDER BY 1"));
        assertEquals(List.of("(1,2)", "(3,4)"),
                rows("SELECT (a, b) FROM gbl_t GROUP BY ROW(a, b) ORDER BY 1"));
        assertEquals(List.of("(1,2)", "(3,4)"),
                rows("SELECT ROW(a, b) FROM gbl_t GROUP BY (a, b) ORDER BY 1"));
    }

    @Test
    void namingTheMembersAsWellLicensesThem() throws Exception {
        assertEquals(List.of("1|2", "3|4"),
                rows("SELECT a, b FROM gbl_t GROUP BY ROW(a, b), a, b ORDER BY 1"));
    }

    /** float names double precision, and float(p) the type that many bits of precision asks for. */
    @Test
    void aCastToTheTypeTheColumnHasIsNotPartOfTheExpression() throws Exception {
        assertEquals(List.of("1.5", "2.5"), rows("SELECT d FROM gbl_t GROUP BY d::float ORDER BY 1"));
        assertEquals(List.of("1.5", "2.5"),
                rows("SELECT d FROM gbl_t GROUP BY d::float(53) ORDER BY 1"));
        assertEquals(List.of("1.5", "2.5"), rows("SELECT r FROM gbl_t GROUP BY r::float(24) ORDER BY 1"));
    }

    /** A cast naming any other type is a conversion, and leaves the column ungrouped. */
    @Test
    void aCastToAnotherTypeIsStillAConversion() {
        assertEquals("42803", stateOf("SELECT d FROM gbl_t GROUP BY d::float(24)"));
        assertEquals("42803", stateOf("SELECT r FROM gbl_t GROUP BY r::float"));
        assertEquals("42803", stateOf("SELECT d FROM gbl_t GROUP BY d::numeric"));
    }

    /** The rules that were already right stay right. */
    @Test
    void whatWasAlreadyLicensedStillIs() throws Exception {
        assertEquals(List.of("3", "7"), rows("SELECT a + b FROM gbl_t GROUP BY a + b ORDER BY 1"));
        assertEquals(List.of("{1}", "{3}"), rows("SELECT ARRAY[a] FROM gbl_t GROUP BY ARRAY[a] ORDER BY 1"));
        assertEquals(List.of("1", "3"), rows("SELECT a FROM gbl_t GROUP BY 1 ORDER BY 1"));
        assertEquals(List.of("1", "3"), rows("SELECT a AS q FROM gbl_t GROUP BY q ORDER BY 1"));
        assertEquals("42803", stateOf("SELECT a + 1 FROM gbl_t GROUP BY 1 + a"));
        assertEquals("42803", stateOf("SELECT ARRAY[a] FROM gbl_t GROUP BY ARRAY[b]"));
        assertEquals("42803", stateOf("SELECT b FROM gbl_t GROUP BY a"));
    }
}
