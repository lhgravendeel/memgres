package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a grouped query may select, measured against PostgreSQL 18.
 *
 * <p>Two halves. The first is the one that rejected working SQL: PostgreSQL lets a query select
 * a column it did not group by when the GROUP BY covers that column's table primary key, because
 * the key determines the rest of the row. {@code SELECT id, other FROM t GROUP BY id} is ordinary
 * application and ORM SQL and memgres answered it with 42803.
 *
 * <p>The second half is the opposite failure and the larger one: memgres validated grouping only
 * when every GROUP BY item was a bare column or an ordinal, so every other spelling licensed
 * everything. {@code SELECT b FROM t GROUP BY a + 0} returned some row's {@code b} instead of
 * erroring, as did grouping by a cast, a function call, a CASE, an out-of-range ordinal, a
 * non-integer constant, and an output alias that shadows a real column.
 *
 * <p>Every expectation here was run against PostgreSQL 18 first. Two of them contradict what the
 * finding assumed: a UNIQUE NOT NULL constraint does <em>not</em> create a functional dependency
 * (only a PRIMARY KEY does), and {@code GROUP BY ROLLUP (id)} does not either, because its
 * grouping sets include the empty one in which the key is not grouped at all.
 */
class GroupByValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE gbv_t (id int PRIMARY KEY, other text, n int NOT NULL)");
        exec("INSERT INTO gbv_t VALUES (1,'a',10),(2,'b',20)");
        exec("CREATE TABLE gbv_child (cid int PRIMARY KEY, tid int, amt int)");
        exec("INSERT INTO gbv_child VALUES (1,1,5),(2,1,6),(3,2,7)");
        exec("CREATE TABLE gbv_u (uid int UNIQUE NOT NULL, ucol text)");
        exec("INSERT INTO gbv_u VALUES (1,'x'),(2,'y')");
        exec("CREATE TABLE gbv_m (a int, b int, c text, PRIMARY KEY (a, b))");
        exec("INSERT INTO gbv_m VALUES (1,1,'p'),(1,2,'q')");
        exec("CREATE TABLE gbv_p (a int, b int, tx text)");
        exec("INSERT INTO gbv_p VALUES (1,10,'p'),(2,20,'q'),(1,30,'r')");
        exec("CREATE VIEW gbv_v AS SELECT id, other, n FROM gbv_t");
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

    private static void assertRows(String sql, String... expected) throws SQLException {
        assertEquals(List.of(expected), rows(sql), sql);
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> expected message containing <" + messagePart + "> but was <" + e.getMessage() + ">");
    }

    private static void assertUngrouped(String column, String sql) {
        assertError("42803",
                "column \"" + column + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                sql);
    }

    // ---- Functional dependency on a primary key ----

    @Test
    void groupingByThePrimaryKeySelectsTheRestOfTheRow() throws Exception {
        assertRows("SELECT id, other FROM gbv_t GROUP BY id ORDER BY id", "1|a", "2|b");
        assertRows("SELECT id, n FROM gbv_t GROUP BY id ORDER BY id", "1|10", "2|20");
        assertRows("SELECT id, other, n FROM gbv_t GROUP BY id ORDER BY id", "1|a|10", "2|b|20");
        assertRows("SELECT other FROM gbv_t GROUP BY id ORDER BY other", "a", "b");
    }

    @Test
    void theKeyMayBeNamedThroughAnAliasOrTheTableName() throws Exception {
        assertRows("SELECT t.id, t.other FROM gbv_t t GROUP BY t.id ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT t.id, t.other FROM gbv_t AS t GROUP BY id ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT id, other FROM gbv_t GROUP BY gbv_t.id ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT gbv_t.id, other FROM gbv_t GROUP BY id ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT id, other FROM gbv_t t GROUP BY t.id ORDER BY 1", "1|a", "2|b");
    }

    @Test
    void theKeyMayBeNamedByOrdinalOrOutputAlias() throws Exception {
        assertRows("SELECT id, other FROM gbv_t GROUP BY 1 ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT t.id AS k, t.other FROM gbv_t t GROUP BY k ORDER BY 1", "1|a", "2|b");
    }

    @Test
    void theDependencyCarriesAcrossAJoin() throws Exception {
        assertRows("SELECT t.id, t.other, count(c.cid) FROM gbv_t t"
                        + " LEFT JOIN gbv_child c ON c.tid = t.id GROUP BY t.id ORDER BY 1",
                "1|a|2", "2|b|1");
        // Each side keeps its own key: grouping one relation's key says nothing about the other.
        assertRows("SELECT t.other, c.amt FROM gbv_t t JOIN gbv_child c ON c.tid = t.id"
                        + " GROUP BY t.id, c.cid ORDER BY 1, 2",
                "a|5", "a|6", "b|7");
        assertUngrouped("c.amt", "SELECT t.other, c.amt FROM gbv_t t JOIN gbv_child c ON c.tid = t.id"
                + " GROUP BY t.id ORDER BY 1");
    }

    @Test
    void theDependencyReachesHavingAndOrderBy() throws Exception {
        assertRows("SELECT id, count(*) FROM gbv_t GROUP BY id HAVING other = 'a'", "1|1");
        assertRows("SELECT id, other FROM gbv_t GROUP BY id ORDER BY other DESC", "2|b", "1|a");
        assertRows("SELECT id FROM gbv_t GROUP BY id ORDER BY n DESC", "2", "1");
    }

    @Test
    void theDependencyReachesExpressionsOverTheDependentColumn() throws Exception {
        assertRows("SELECT id, upper(other) FROM gbv_t GROUP BY id ORDER BY 1", "1|A", "2|B");
        assertRows("SELECT id, other || n::text FROM gbv_t GROUP BY id ORDER BY 1", "1|a10", "2|b20");
        assertRows("SELECT id, other FROM gbv_t GROUP BY id ORDER BY 1 LIMIT 1", "1|a");
        assertRows("SELECT DISTINCT other FROM gbv_t GROUP BY id ORDER BY 1", "a", "b");
    }

    @Test
    void aStarExpandsToTheColumnsTheKeyDetermines() throws Exception {
        assertRows("SELECT * FROM gbv_t GROUP BY id ORDER BY 1", "1|a|10", "2|b|20");
        assertRows("SELECT t.* FROM gbv_t t GROUP BY t.id ORDER BY 1", "1|a|10", "2|b|20");
        assertRows("SELECT t.*, count(*) FROM gbv_t t GROUP BY t.id ORDER BY 1",
                "1|a|10|1", "2|b|20|1");
    }

    @Test
    void aMultiColumnKeyHasToBeGroupedWhole() throws Exception {
        assertRows("SELECT a, b, c FROM gbv_m GROUP BY a, b ORDER BY 1, 2", "1|1|p", "1|2|q");
        assertRows("SELECT c FROM gbv_m GROUP BY b, a ORDER BY 1", "p", "q");
        assertUngrouped("gbv_m.c", "SELECT a, c FROM gbv_m GROUP BY a ORDER BY 1");
        assertUngrouped("gbv_m.a", "SELECT a, c FROM gbv_m GROUP BY b ORDER BY 1");
    }

    /** PostgreSQL derives the dependency from a PRIMARY KEY only, never from UNIQUE NOT NULL. */
    @Test
    void aUniqueNotNullConstraintGrantsNoDependency() {
        assertUngrouped("gbv_u.ucol", "SELECT uid, ucol FROM gbv_u GROUP BY uid ORDER BY 1");
    }

    /** A view, a sub-select and a CTE expose columns but carry no key of their own. */
    @Test
    void aDerivedRelationGrantsNoDependency() {
        assertUngrouped("gbv_v.other", "SELECT id, other FROM gbv_v GROUP BY id ORDER BY 1");
        assertUngrouped("x.other",
                "SELECT x.id, x.other FROM (SELECT * FROM gbv_t) x GROUP BY x.id ORDER BY 1");
        assertUngrouped("x.other",
                "WITH x AS (SELECT * FROM gbv_t) SELECT x.id, x.other FROM x GROUP BY x.id ORDER BY 1");
    }

    /**
     * A grouping-set specification only determines a row when every one of its sets groups the
     * key: a single set does, and ROLLUP/CUBE, whose sets include the empty one, do not.
     */
    @Test
    void groupingSetsDetermineARowOnlyWhenEverySetGroupsTheKey() throws Exception {
        assertRows("SELECT id, other FROM gbv_t GROUP BY GROUPING SETS ((id)) ORDER BY 1",
                "1|a", "2|b");
        assertUngrouped("gbv_t.other",
                "SELECT id, other FROM gbv_t GROUP BY ROLLUP (id) ORDER BY 1");
        assertUngrouped("gbv_t.other",
                "SELECT id, other FROM gbv_t GROUP BY CUBE (id) ORDER BY 1");
        assertUngrouped("gbv_t.other",
                "SELECT id, other FROM gbv_t GROUP BY GROUPING SETS ((id), ()) ORDER BY 1");
    }

    // ---- Grouping by an expression licenses that expression, not its columns ----

    @Test
    void groupingByAnExpressionDoesNotLicenseItsColumns() {
        assertUngrouped("gbv_p.b", "SELECT b FROM gbv_p GROUP BY a + 0");
        assertUngrouped("gbv_p.b", "SELECT b FROM gbv_p GROUP BY a::text");
        assertUngrouped("gbv_p.b", "SELECT b FROM gbv_p GROUP BY abs(a)");
        assertUngrouped("gbv_p.b", "SELECT b FROM gbv_p GROUP BY lower('x')");
        assertUngrouped("gbv_p.a", "SELECT a FROM gbv_p GROUP BY a + 0");
        assertUngrouped("gbv_p.a", "SELECT a FROM gbv_p GROUP BY CASE WHEN a > 0 THEN a ELSE 0 END");
        assertUngrouped("gbv_p.tx", "SELECT tx FROM gbv_p GROUP BY upper(tx)");
        assertUngrouped("gbv_t.id", "SELECT id, other FROM gbv_t GROUP BY id + 0");
    }

    @Test
    void aGroupedExpressionIsAvailableWholeAndInsideLargerExpressions() throws Exception {
        assertRows("SELECT a + 0 FROM gbv_p GROUP BY a + 0 ORDER BY 1", "1", "2");
        assertRows("SELECT (a + 0) * 2 FROM gbv_p GROUP BY a + 0 ORDER BY 1", "2", "4");
        assertRows("SELECT abs(a) + 1, count(*) FROM gbv_p GROUP BY abs(a) ORDER BY 1", "2|2", "3|1");
        assertRows("SELECT length(upper(tx)) FROM gbv_p GROUP BY upper(tx) ORDER BY 1", "1", "1", "1");
        assertRows("SELECT a::text || 'x' FROM gbv_p GROUP BY a::text ORDER BY 1", "1x", "2x");
    }

    @Test
    void anUngroupedColumnIsRejectedInEveryClause() {
        assertUngrouped("gbv_p.b", "SELECT a, b FROM gbv_p GROUP BY a");
        assertUngrouped("gbv_p.b", "SELECT count(*) FROM gbv_p GROUP BY a HAVING b > 0");
        assertUngrouped("gbv_p.b", "SELECT count(*) FROM gbv_p GROUP BY a ORDER BY b");
        assertUngrouped("gbv_p.b", "SELECT count(*) FROM gbv_p GROUP BY a ORDER BY b + 1");
        assertUngrouped("gbv_p.tx", "SELECT count(*) FROM gbv_p GROUP BY a ORDER BY upper(tx)");
        assertUngrouped("gbv_p.a", "SELECT count(*) FROM gbv_p GROUP BY a + 0 HAVING a > 0");
        assertUngrouped("gbv_p.a", "SELECT count(*) FROM gbv_p GROUP BY a + 0 ORDER BY a");
        assertUngrouped("gbv_p.a", "SELECT count(*) FROM gbv_p HAVING a > 0");
    }

    /** PostgreSQL names the offending column, not the expression that reads it. */
    @Test
    void theErrorNamesTheColumnQualifiedByItsRelation() {
        assertUngrouped("gbv_p.tx", "SELECT upper(tx) FROM gbv_p GROUP BY a");
        assertUngrouped("gbv_p.b", "SELECT b + 1 FROM gbv_p GROUP BY a");
        assertUngrouped("p.b", "SELECT p.b FROM gbv_p p GROUP BY p.a");
        assertUngrouped("p.b", "SELECT count(*) FROM gbv_p p GROUP BY p.a ORDER BY p.b");
        assertUngrouped("t.n", "SELECT count(*) FROM gbv_p p, gbv_t t GROUP BY p.a HAVING t.n > 0");
    }

    // ---- GROUP BY items that are not expressions at all ----

    @Test
    void anOrdinalOutsideTheSelectListIsRejected() {
        assertError("42P10", "GROUP BY position 3 is not in select list",
                "SELECT count(*) FROM gbv_p GROUP BY 3");
        assertError("42P10", "GROUP BY position 0 is not in select list",
                "SELECT count(*) FROM gbv_p GROUP BY 0");
        assertError("42P10", "GROUP BY position -1 is not in select list",
                "SELECT count(*) FROM gbv_p GROUP BY -1");
        assertError("42P10", "GROUP BY position 4 is not in select list",
                "SELECT * FROM gbv_t GROUP BY 4");
    }

    @Test
    void aNonIntegerConstantIsRejected() {
        assertError("42601", "non-integer constant in GROUP BY",
                "SELECT count(*) FROM gbv_p GROUP BY 'x'");
        assertError("42601", "non-integer constant in GROUP BY",
                "SELECT count(*) FROM gbv_p GROUP BY NULL");
        assertError("42601", "non-integer constant in GROUP BY",
                "SELECT count(*) FROM gbv_p GROUP BY true");
        assertError("42601", "non-integer constant in GROUP BY",
                "SELECT count(*) FROM gbv_p GROUP BY 1.5");
    }

    /** Only a bare integer is a position; a constant reached through anything else is a value. */
    @Test
    void aConstantExpressionIsGroupedAsAValueNotAPosition() throws Exception {
        assertUngrouped("gbv_p.a", "SELECT a, count(*) FROM gbv_p GROUP BY 1 + 0");
        assertUngrouped("gbv_p.a", "SELECT a, count(*) FROM gbv_p GROUP BY 1::int");
        assertUngrouped("gbv_p.a", "SELECT a, count(*) FROM gbv_p GROUP BY 'x'::text");
        assertRows("SELECT count(*) FROM gbv_p GROUP BY now()", "3");
        assertRows("SELECT count(*) FROM gbv_p GROUP BY DATE '2020-01-01'", "3");
        // Parentheses do not hide a position.
        assertRows("SELECT a, count(*) FROM gbv_p GROUP BY (1) ORDER BY 1", "1|2", "2|1");
    }

    @Test
    void anAggregateOrWindowFunctionIsNotAGroupingExpression() {
        assertError("42803", "aggregate functions are not allowed in GROUP BY",
                "SELECT a FROM gbv_p GROUP BY sum(a)");
        assertError("42803", "aggregate functions are not allowed in GROUP BY",
                "SELECT a FROM gbv_p GROUP BY count(*)");
        assertError("42803", "aggregate functions are not allowed in GROUP BY",
                "SELECT count(*) FROM gbv_p GROUP BY 1");
        assertError("42803", "aggregate functions are not allowed in GROUP BY",
                "SELECT a, count(*) FROM gbv_p GROUP BY 2");
        assertError("42P20", "window functions are not allowed in GROUP BY",
                "SELECT a FROM gbv_p GROUP BY row_number() OVER ()");
    }

    @Test
    void anAggregateMayNotBeNestedInAnother() {
        assertError("42803", "aggregate function calls cannot be nested",
                "SELECT sum(sum(b)) FROM gbv_p");
        assertError("42803", "aggregate function calls cannot be nested",
                "SELECT a, count(sum(b)) FROM gbv_p GROUP BY a");
    }

    /** A GROUP BY name is the FROM column when one has it, so an alias never shadows a column. */
    @Test
    void aGroupByNameResolvesAgainstTheTableBeforeTheOutputAlias() throws Exception {
        assertUngrouped("gbv_p.b", "SELECT b AS a, count(*) FROM gbv_p GROUP BY a");
        assertUngrouped("gbv_p.b", "SELECT b AS a FROM gbv_p GROUP BY a ORDER BY 1");
        // Grouping both resolves it, and it is a, not b, that the query groups on.
        assertRows("SELECT b AS a, count(*) FROM gbv_p GROUP BY a, b ORDER BY 1",
                "10|1", "20|1", "30|1");
        // A name no relation exposes still falls back to the output alias.
        assertRows("SELECT a + 0 AS z, count(*) FROM gbv_p GROUP BY z ORDER BY 1", "1|2", "2|1");
        assertRows("SELECT a AS z, count(*) FROM gbv_p GROUP BY z ORDER BY 1", "1|2", "2|1");
    }

    // ---- Shapes that have to keep working ----

    @Test
    void anAggregateWithoutGroupByStillTakesOnlyAggregatesAndConstants() throws Exception {
        assertRows("SELECT count(*) FROM gbv_p", "3");
        assertRows("SELECT max(a), min(b) FROM gbv_p", "2|10");
        assertRows("SELECT count(*) FROM gbv_p HAVING count(*) > 0", "3");
        assertRows("SELECT count(*) FROM gbv_p GROUP BY ()", "3");
        assertUngrouped("gbv_p.a", "SELECT a, count(*) FROM gbv_p GROUP BY ()");
    }

    @Test
    void ordinaryGroupedQueriesAreUnaffected() throws Exception {
        assertRows("SELECT a, count(*) FROM gbv_p GROUP BY a ORDER BY 1", "1|2", "2|1");
        assertRows("SELECT a, b, count(*) FROM gbv_p GROUP BY 1, 2 ORDER BY 1, 2",
                "1|10|1", "1|30|1", "2|20|1");
        assertRows("SELECT a, count(*) FROM gbv_p GROUP BY a HAVING a > 1 ORDER BY 1", "2|1");
        assertRows("SELECT a, count(*) AS ct FROM gbv_p GROUP BY a ORDER BY ct DESC, a", "1|2", "2|1");
        assertRows("SELECT a, count(*) FROM gbv_p GROUP BY a ORDER BY count(*) DESC, a", "1|2", "2|1");
        assertRows("SELECT DISTINCT a, count(*) FROM gbv_p GROUP BY a ORDER BY 1", "1|2", "2|1");
        assertRows("SELECT a, string_agg(tx, ',' ORDER BY tx) FROM gbv_p GROUP BY a ORDER BY 1",
                "1|p,r", "2|q");
        assertRows("SELECT a, count(DISTINCT b) FROM gbv_p GROUP BY a ORDER BY 1", "1|2", "2|1");
        assertRows("SELECT a, sum(b) FILTER (WHERE b > 10) FROM gbv_p GROUP BY a ORDER BY 1",
                "1|30", "2|20");
        assertRows("SELECT a, b, count(*) FROM gbv_p GROUP BY ROLLUP (a, b) ORDER BY 1, 2",
                "1|10|1", "1|30|1", "1|null|2", "2|20|1", "2|null|1", "null|null|3");
    }

    @Test
    void groupingWorksThroughDerivedRelationsAndLateral() throws Exception {
        assertRows("SELECT sub.x, count(*) FROM (SELECT a AS x FROM gbv_p) sub GROUP BY sub.x ORDER BY 1",
                "1|2", "2|1");
        assertRows("WITH g AS (SELECT a, count(*) c FROM gbv_p GROUP BY a) SELECT * FROM g ORDER BY 1",
                "1|2", "2|1");
        assertRows("SELECT v.id, v.other FROM gbv_v v GROUP BY v.id, v.other ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT t.id, sum(c.amt) FROM gbv_t t"
                        + " LEFT JOIN LATERAL (SELECT * FROM gbv_child c WHERE c.tid = t.id) c ON true"
                        + " GROUP BY t.id ORDER BY 1",
                "1|11", "2|7");
        assertRows("SELECT x FROM (VALUES (1),(2)) v(x) GROUP BY x ORDER BY 1", "1", "2");
        assertRows("SELECT gs, count(*) FROM generate_series(1,3) gs GROUP BY gs ORDER BY 1",
                "1|1", "2|1", "3|1");
    }

    /**
     * A derived column carries no type or provenance the check may lean on: this rejected
     * {@code sub.rn >= 1} once because it judged the column rather than the grouping.
     */
    @Test
    void aDerivedWindowColumnGroupsAndFiltersLikeAnyOther() throws Exception {
        assertRows("SELECT sub.rn FROM (SELECT row_number() OVER () AS rn FROM gbv_p) sub"
                        + " GROUP BY sub.rn HAVING sub.rn >= 1 ORDER BY 1",
                "1", "2", "3");
    }

    @Test
    void windowFunctionsOverAGroupedQueryKeepWorking() throws Exception {
        assertRows("SELECT a, count(*) OVER () FROM gbv_p GROUP BY a ORDER BY 1", "1|2", "2|2");
        assertRows("SELECT a, row_number() OVER (PARTITION BY a) FROM gbv_p GROUP BY a ORDER BY 1",
                "1|1", "2|1");
        assertUngrouped("gbv_p.b", "SELECT a, sum(b) OVER () FROM gbv_p GROUP BY a");
        assertUngrouped("gbv_p.b", "SELECT a, row_number() OVER (ORDER BY b) FROM gbv_p GROUP BY a");
    }

    /** ORDER BY on a grouped expression the select list does not carry sorts by that expression. */
    @Test
    void orderByAGroupedExpressionOutsideTheSelectListSortsByIt() throws Exception {
        assertRows("SELECT count(*) FROM gbv_p GROUP BY a + 0 ORDER BY a + 0", "2", "1");
        assertRows("SELECT count(*) FROM gbv_p GROUP BY a + 0 ORDER BY a + 0 DESC", "1", "2");
        assertRows("SELECT count(*) FROM gbv_p GROUP BY a HAVING count(*) = 1 ORDER BY a", "1");
    }

    @Test
    void predicatesAndSubscriptsOverGroupedColumnsEvaluatePerGroup() throws Exception {
        assertRows("SELECT a > 1, count(*) FROM gbv_p GROUP BY a ORDER BY 1", "f|2", "t|1");
        assertRows("SELECT a IN (1, 2), count(*) FROM gbv_p GROUP BY a ORDER BY 2", "t|1", "t|2");
        assertRows("SELECT nullif(a, 0), count(*) FROM gbv_p GROUP BY a ORDER BY 1", "1|2", "2|1");
        assertRows("SELECT greatest(a, b), count(*) FROM gbv_p GROUP BY a, b ORDER BY 1",
                "10|1", "20|1", "30|1");
    }
}
