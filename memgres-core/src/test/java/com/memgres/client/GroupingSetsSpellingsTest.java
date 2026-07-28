package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL accepts more grouping-set spellings than the fully parenthesised
 * {@code GROUPING SETS ((a), ())}: the empty grouping set {@code GROUP BY ()}, a bare
 * unparenthesised column inside GROUPING SETS, a nested ROLLUP/CUBE inside GROUPING SETS,
 * and an output alias or ordinal as a grouping expression. Memgres rejected all of those
 * with 42601, so the ordinary way of writing a partial cube did not parse at all.
 */
class GroupingSetsSpellingsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE gss_t (a text, b text, c int)");
        exec("INSERT INTO gss_t VALUES ('x','p',1),('x','q',2),('y','p',3),('y','q',4),('y','q',5)");
        exec("CREATE TABLE gss_n (a text, b text, c int)");
        exec("INSERT INTO gss_n VALUES ('x',NULL,1),(NULL,'q',2)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** All rows of a query, each row rendered as pipe-joined column values. */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
            return out;
        }
    }

    private static String scalar(String sql) throws SQLException {
        List<String> r = rows(sql);
        assertEquals(1, r.size(), "expected exactly one row from: " + sql);
        return r.get(0);
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    @Test
    void theEmptyGroupingSetProducesOneTotalRow() throws Exception {
        assertEquals("5", scalar("SELECT count(*) FROM gss_t GROUP BY ()"));
        assertEquals("5", scalar("SELECT count(*) FROM gss_t GROUP BY (), ()"));
        assertEquals("5", scalar("SELECT count(*) FROM gss_t GROUP BY GROUPING SETS (())"));
        // GROUP BY (), () cross-products the two into one set, but GROUPING SETS lists two
        assertEquals(List.of("5", "5"), rows("SELECT count(*) FROM gss_t GROUP BY GROUPING SETS ((), ())"));
        // HAVING still filters that single row
        assertEquals("5", scalar("SELECT count(*) FROM gss_t GROUP BY () HAVING count(*) > 2"));
        assertEquals(List.of(), rows("SELECT count(*) FROM gss_t GROUP BY () HAVING count(*) > 9"));
    }

    @Test
    void anEmptySetCrossProductsWithAPlainGroupingColumn() throws Exception {
        // () adds no column, so a, () is just a — not an extra grand-total row
        assertEquals(List.of("x|2", "y|3"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY a, () ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY (), a ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY ROLLUP(a), () ORDER BY 1"));
        assertEquals(List.of("x|null|2", "y|null|3", "null|p|2", "null|q|3"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS (a, b), () ORDER BY 1,2"));
    }

    @Test
    void aBareColumnInsideGroupingSetsIsAOneColumnSet() throws Exception {
        assertEquals(List.of("x|2", "y|3"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY GROUPING SETS (a) ORDER BY 1"));
        // (a) and (a, b): the detail rows plus the per-a subtotals, six rows
        assertEquals(List.of("x|p|1", "x|q|1", "x|null|2", "y|p|1", "y|q|2", "y|null|3"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS (a, (a,b)) ORDER BY 1,2"));
        assertEquals(List.of("x|null|2", "y|null|3", "null|p|2", "null|q|3", "null|null|5"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS (a, (), b) ORDER BY 1,2"));
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY GROUPING SETS ((), a) ORDER BY 1"));
        // duplicate sets are kept unless GROUP BY DISTINCT removes them
        assertEquals(5, rows("SELECT a, count(*) FROM gss_t GROUP BY GROUPING SETS (a, a, ()) ORDER BY 1").size());
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY DISTINCT GROUPING SETS (a, a, ()) ORDER BY 1"));
    }

    @Test
    void groupingBitmasksFollowTheBareColumnSets() throws Exception {
        // set (a): b is not grouped -> grouping(b) = 1 and grouping(a,b) = 0b01
        // set (b): a is not grouped -> grouping(a) = 1 and grouping(a,b) = 0b10
        assertEquals(List.of("x|null|2|0|1|1", "y|null|3|0|1|1", "null|p|2|1|0|2", "null|q|3|1|0|2"),
                rows("SELECT a, b, count(*), grouping(a), grouping(b), grouping(a,b) "
                        + "FROM gss_t GROUP BY GROUPING SETS (a, b) ORDER BY 1,2"));
    }

    @Test
    void aNestedRollupOrCubeContributesItsOwnSets() throws Exception {
        // ROLLUP(a,b) inside GROUPING SETS is the ordinary way to write a partial cube
        assertEquals(List.of("x|p|1|0", "x|q|1|0", "x|null|2|1", "y|p|1|0", "y|q|2|0",
                        "y|null|3|1", "null|null|5|3"),
                rows("SELECT a, b, count(*), grouping(a,b) "
                        + "FROM gss_t GROUP BY GROUPING SETS (ROLLUP (a, b)) ORDER BY 1,2"));
        assertEquals(List.of("x|p|1|0", "x|q|1|0", "x|null|2|1", "y|p|1|0", "y|q|2|0",
                        "y|null|3|1", "null|p|2|2", "null|q|3|2", "null|null|5|3"),
                rows("SELECT a, b, count(*), grouping(a,b) "
                        + "FROM gss_t GROUP BY GROUPING SETS (CUBE (a, b)) ORDER BY 1,2"));
        // ROLLUP(a) expands to (a) and (), which sit alongside the sibling set (b)
        assertEquals(List.of("x|null|2", "y|null|3", "null|p|2", "null|q|3", "null|null|5"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS (ROLLUP(a), b) ORDER BY 1,2"));
        assertEquals(List.of("x|null|2", "y|null|3", "null|p|2", "null|q|3", "null|null|5"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS ((a), ROLLUP(b)) ORDER BY 1,2"));
        // a nested GROUPING SETS flattens into the enclosing list
        assertEquals(List.of("x|null|2", "y|null|3", "null|p|2", "null|q|3"),
                rows("SELECT a, b, count(*) FROM gss_t "
                        + "GROUP BY GROUPING SETS (GROUPING SETS (a, b)) ORDER BY 1,2"));
        assertEquals(List.of("y|q|9", "y|null|12", "null|null|15"),
                rows("SELECT a, b, sum(c) FROM gss_t GROUP BY GROUPING SETS (ROLLUP(a,b)) "
                        + "HAVING sum(c) > 3 ORDER BY 1,2"));
    }

    @Test
    void anOutputAliasNamesTheExpressionToGroupOn() throws Exception {
        assertEquals(List.of("x|2", "y|3"),
                rows("SELECT a AS gss_alias_a, count(*) FROM gss_t "
                        + "GROUP BY GROUPING SETS (gss_alias_a) ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a AS gss_alias_a, count(*) FROM gss_t "
                        + "GROUP BY GROUPING SETS ((gss_alias_a), ()) ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a AS gss_alias_a, count(*) FROM gss_t GROUP BY ROLLUP(gss_alias_a) ORDER BY 1"));
        assertEquals(List.of("x|p|1", "x|q|1", "x|null|2", "y|p|1", "y|q|2", "y|null|3",
                        "null|p|2", "null|q|3", "null|null|5"),
                rows("SELECT a AS gss_alias_a, b AS gss_alias_b, count(*) FROM gss_t "
                        + "GROUP BY CUBE(gss_alias_a, gss_alias_b) ORDER BY 1,2"));
        // an alias for a computed column, and the same expression spelled out
        assertEquals(List.of("X|2", "Y|3", "null|5"),
                rows("SELECT upper(a) AS gss_ua, count(*) FROM gss_t "
                        + "GROUP BY GROUPING SETS (gss_ua, ()) ORDER BY 1"));
        assertEquals(List.of("X|2", "Y|3", "null|5"),
                rows("SELECT upper(a) AS gss_ua, count(*) FROM gss_t "
                        + "GROUP BY GROUPING SETS (upper(a), ()) ORDER BY 1"));
        // an output ordinal resolves the same way
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY GROUPING SETS (1, ()) ORDER BY 1"));
    }

    @Test
    void theSpellingsThatAlreadyWorkedStillWork() throws Exception {
        assertEquals(List.of("x|p|1|0|0", "x|q|1|0|0", "x|null|2|0|1", "y|p|1|0|0",
                        "y|q|2|0|0", "y|null|3|0|1", "null|null|5|1|1"),
                rows("SELECT a, b, count(*), grouping(a), grouping(b) FROM gss_t "
                        + "GROUP BY GROUPING SETS ((a,b),(a),()) ORDER BY 1,2"));
        assertEquals(List.of("x|p|1|0", "x|q|1|0", "x|null|2|1", "y|p|1|0", "y|q|2|0",
                        "y|null|3|1", "null|null|5|3"),
                rows("SELECT a, b, count(*), grouping(a,b) FROM gss_t GROUP BY ROLLUP(a,b) ORDER BY 1,2"));
        assertEquals(9, rows("SELECT a, b, count(*) FROM gss_t GROUP BY CUBE(a,b) ORDER BY 1,2").size());
        assertEquals(List.of("x|p|1", "x|q|1", "x|null|2", "y|p|1", "y|q|2", "y|null|3"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY a, GROUPING SETS ((b),()) ORDER BY 1,2"));
        // a parenthesised tuple is one composite grouping element, not two
        assertEquals(List.of("x|p|1", "x|q|1", "y|p|1", "y|q|2", "null|null|5"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY ROLLUP((a,b)) ORDER BY 1,2"));
        assertEquals(List.of("x|p|1", "x|q|1", "y|p|1", "y|q|2"),
                rows("SELECT a, b, count(*) FROM gss_t GROUP BY GROUPING SETS ((a,b)) ORDER BY 1,2"));
        assertEquals(List.of("x|2", "y|3"), rows("SELECT a, count(*) FROM gss_t GROUP BY a ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3"), rows("SELECT a, count(*) FROM gss_t GROUP BY 1 ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3"), rows("SELECT (a), count(*) FROM gss_t GROUP BY (a) ORDER BY 1"));
        assertEquals(List.of("x|2", "y|3", "null|5"),
                rows("SELECT a, count(*) FROM gss_t GROUP BY DISTINCT ROLLUP(a), CUBE(a) ORDER BY 1"));
    }

    @Test
    void aNullInTheDataKeepsItsOwnGroupApartFromARolledUpNull() throws Exception {
        // grouping() is what tells the data NULL from the subtotal NULL
        assertEquals(List.of("x|null|1|0|1", "null|q|1|1|0", "null|null|1|0|1", "null|null|1|1|0"),
                rows("SELECT a, b, count(*), grouping(a), grouping(b) FROM gss_n "
                        + "GROUP BY GROUPING SETS (a, b) ORDER BY 1,2,4,5"));
        assertEquals(List.of("x|null|1|0", "x|null|1|1", "null|q|1|0", "null|null|1|1", "null|null|2|3"),
                rows("SELECT a, b, count(*), grouping(a,b) FROM gss_n GROUP BY ROLLUP(a,b) ORDER BY 1,2,4"));
    }

    @Test
    void theFormsPostgresRejectsAreStillRejected() {
        assertError("42601", "syntax error at or near \")\"",
                "SELECT count(*) FROM gss_t GROUP BY GROUPING SETS ()");
        assertError("42601", "syntax error at or near \")\"",
                "SELECT count(*) FROM gss_t GROUP BY ROLLUP()");
        assertError("42601", "syntax error at or near \")\"",
                "SELECT count(*) FROM gss_t GROUP BY CUBE()");
        // grouping() over something that is not a grouping expression of this query
        assertError("42803", "arguments to GROUPING must be grouping expressions of the associated query level",
                "SELECT a, grouping(b) FROM gss_t GROUP BY a");
        assertError("42803", "arguments to GROUPING must be grouping expressions of the associated query level",
                "SELECT grouping(a) FROM gss_t");
        // GROUP BY () groups everything, so a bare column in the select list is still ungrouped
        assertError("42803", "must appear in the GROUP BY clause or be used in an aggregate function",
                "SELECT a FROM gss_t GROUP BY ()");
        assertError("42803", "must appear in the GROUP BY clause or be used in an aggregate function",
                "SELECT a, count(*) FROM gss_t GROUP BY ()");
        assertError("42803", "must appear in the GROUP BY clause or be used in an aggregate function",
                "SELECT b, count(*) FROM gss_t GROUP BY GROUPING SETS (a, ())");
    }
}
