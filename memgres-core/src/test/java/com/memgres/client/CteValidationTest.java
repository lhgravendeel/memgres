package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a WITH clause may say, measured against PostgreSQL 18.
 *
 * <p>Two subjects. The first is the shape of a recursive term. A {@code WITH RECURSIVE} item is
 * evaluated by repeatedly scanning the rows the previous round produced, so the self-reference
 * has to sit where a single such scan makes sense — once, in the FROM clause, not on a side an
 * outer join may null-extend and not inside a sub-select. Anything else asks for the finished
 * result while it is still being built, and PostgreSQL refuses it (42P19) rather than answer a
 * different question. memgres answered all of them, and two of those answers were wrong rather
 * than merely permitted: a second self-reference and an aggregate both stop the recursion at a
 * different row.
 *
 * <p>The second is name resolution. Without RECURSIVE a WITH item sees only the items written
 * before it — not itself, and not what follows — so {@code WITH x AS (SELECT n FROM y), y AS (…)}
 * is an error in PostgreSQL and a stored table named {@code y} is what {@code x} would read if
 * one existed. memgres resolved the whole list first and answered from the later item.
 *
 * <p>Several of the finding's assumptions did not survive being run. A window function in the
 * recursive term is <em>allowed</em> (the case that looked like a window rule was really the
 * column-type rule, because {@code row_number()} is bigint). An aggregate is refused only at the
 * recursive term's own query level — one inside a FROM subquery or a scalar sub-select of it is
 * fine. "Within a subquery" means a sub-select of an expression: a FROM-clause subquery over the
 * recursive name is ordinary SQL. And LIMIT, OFFSET and ORDER BY are refused only on the
 * recursive query itself; the same words inside a parenthesised arm belong to that arm.
 */
class CteValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE cte_t (n int)");
        exec("INSERT INTO cte_t VALUES (1),(2),(3)");
        exec("CREATE TABLE cte_edge (a int, b int)");
        exec("INSERT INTO cte_edge VALUES (1,2),(2,3),(3,4)");
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

    /** Asserts the statement fails with this SQLSTATE and a message containing this text. */
    private static void assertFails(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> rows(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> expected \"" + messagePart + "\" in \"" + e.getMessage() + "\"");
    }

    private static void assertRows(String sql, String... expected) throws SQLException {
        assertEquals(java.util.Arrays.asList(expected), rows(sql), sql);
    }

    // ================================================================
    // 1. The recursive term may name the WITH item exactly once
    // ================================================================

    @Test
    @DisplayName("two self-references in the recursive term are 42P19")
    void twoSelfReferences() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                        + "SELECT a.n + b.n FROM r a, r b WHERE a.n < 4) SELECT n FROM r ORDER BY n",
                "42P19", "recursive reference to query \"r\" must not appear more than once");
    }

    @Test
    @DisplayName("self-references inside FROM subqueries count towards the one allowed")
    void selfReferencesInSubqueriesCount() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT q.n+1 FROM (SELECT n FROM r) q, "
                        + "(SELECT n FROM r) p WHERE q.n < 3 AND p.n = q.n) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear more than once");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT q.n+1 FROM r, (SELECT n FROM r) q "
                        + "WHERE q.n < 3 AND r.n = q.n) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear more than once");
    }

    @Test
    @DisplayName("the non-recursive term may not name the WITH item")
    void selfReferenceInNonRecursiveTerm() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                        + "SELECT n FROM r ORDER BY n",
                "42P19", "recursive reference to query \"r\" must not appear within its non-recursive term");
        // three arms: everything left of the last UNION is the non-recursive term
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3 "
                        + "UNION ALL SELECT n+1 FROM r WHERE n < 2) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within its non-recursive term");
    }

    // ================================================================
    // 2. Not inside a sub-select of an expression
    // ================================================================

    @Test
    @DisplayName("the self-reference may not sit inside IN, EXISTS or a scalar sub-select")
    void selfReferenceInSubLink() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte_t "
                        + "WHERE n IN (SELECT n FROM r) AND n < 3) SELECT n FROM r ORDER BY n",
                "42P19", "recursive reference to query \"r\" must not appear within a subquery");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte_t "
                        + "WHERE EXISTS (SELECT 1 FROM r) AND n < 3) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within a subquery");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte_t "
                        + "WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.n = cte_t.n) AND n < 3) "
                        + "SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within a subquery");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (SELECT max(n) FROM r) + 1 "
                        + "FROM cte_t WHERE n < 2) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within a subquery");
    }

    @Test
    @DisplayName("a FROM-clause subquery over the recursive name is not a subquery reference")
    void selfReferenceInFromSubqueryIsAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT q.n+1 FROM (SELECT n FROM r) q "
                + "WHERE q.n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM "
                + "(WITH q AS (SELECT n FROM r) SELECT n FROM q) z WHERE n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
    }

    // ================================================================
    // 3. Not on a side an outer join may null-extend
    // ================================================================

    @Test
    @DisplayName("the nullable side of an outer join may not hold the self-reference")
    void selfReferenceUnderOuterJoin() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM cte_t x "
                        + "LEFT JOIN r t ON true WHERE t.n < 3 AND x.n = 1) SELECT n FROM r ORDER BY n",
                "42P19", "recursive reference to query \"r\" must not appear within an outer join");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM r t "
                        + "RIGHT JOIN cte_t x ON x.n = t.n WHERE t.n < 3) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within an outer join");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM r t "
                        + "FULL JOIN cte_t x ON x.n = t.n WHERE t.n < 3) SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within an outer join");
        // a subquery over the recursive name on the nullable side counts too
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM cte_t x "
                        + "LEFT JOIN (SELECT n FROM r) t ON true WHERE t.n < 3 AND x.n = 1) "
                        + "SELECT n FROM r ORDER BY n",
                "42P19", "must not appear within an outer join");
    }

    @Test
    @DisplayName("the preserved side of an outer join is ordinary SQL")
    void selfReferenceOnPreservedSideIsAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM r t "
                + "LEFT JOIN cte_t x ON x.n = t.n WHERE t.n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM cte_t x "
                + "RIGHT JOIN r t ON x.n = t.n WHERE t.n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        // an outer join whose nullable side names something else entirely
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n + 1 FROM r, "
                + "(SELECT t.n FROM cte_t t LEFT JOIN cte_t u ON t.n = u.n) q "
                + "WHERE r.n < 2 AND q.n = 1) SELECT n FROM r ORDER BY n", "1", "2");
    }

    // ================================================================
    // 4. No aggregate at the recursive term's own level
    // ================================================================

    @Test
    @DisplayName("an aggregate in the recursive term is 42P19")
    void aggregateInRecursiveTerm() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(n) + 1 FROM r WHERE n < 3) "
                        + "SELECT n FROM r ORDER BY n",
                "42P19", "aggregate functions are not allowed in a recursive query's recursive term");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT count(*)::int + n FROM r "
                        + "WHERE n < 3 GROUP BY n) SELECT n FROM r ORDER BY n",
                "42P19", "aggregate functions are not allowed");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3 "
                        + "GROUP BY n HAVING count(*) > 0) SELECT n FROM r ORDER BY n",
                "42P19", "aggregate functions are not allowed");
    }

    @Test
    @DisplayName("an aggregate one query level down, or in the seed, is allowed")
    void aggregateElsewhereIsAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r, "
                + "(SELECT max(n) AS m FROM cte_t) q WHERE r.n < q.m) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r, "
                + "LATERAL (SELECT max(n) AS m FROM cte_t) q WHERE r.n < q.m) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r "
                + "WHERE n < (SELECT max(n) FROM cte_t)) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT max(n) FROM cte_t UNION ALL SELECT n+1 FROM r "
                + "WHERE n < 5) SELECT n FROM r ORDER BY n", "3", "4", "5");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                + "SELECT max(n) FROM r", "3");
    }

    @Test
    @DisplayName("a window function in the recursive term is allowed")
    void windowFunctionInRecursiveTermIsAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                + "SELECT n + (row_number() OVER ())::int FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT (row_number() OVER ())::int FROM cte_t "
                + "UNION ALL SELECT n+1 FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "2", "3", "3", "3");
    }

    // ================================================================
    // 5. Clauses PostgreSQL has not implemented for a recursive query
    // ================================================================

    @Test
    @DisplayName("ORDER BY, OFFSET, LIMIT and FOR UPDATE on a recursive query are 0A000")
    void unimplementedClauses() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
                        + "ORDER BY n) SELECT n FROM r ORDER BY n",
                "0A000", "ORDER BY in a recursive query is not implemented");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
                        + "OFFSET 0) SELECT n FROM r ORDER BY n",
                "0A000", "OFFSET in a recursive query is not implemented");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
                        + "LIMIT 10) SELECT n FROM r ORDER BY n",
                "0A000", "LIMIT in a recursive query is not implemented");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
                        + "FOR UPDATE) SELECT n FROM r ORDER BY n",
                "0A000", "FOR UPDATE/SHARE in a recursive query is not implemented");
    }

    @Test
    @DisplayName("the same words inside an arm or a subquery belong to that arm")
    void limitInsideAnArmIsAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS ((SELECT 1 LIMIT 1) UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 OFFSET 0)) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r, "
                + "(SELECT n FROM cte_t LIMIT 1) q WHERE r.n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r "
                + "WHERE n < (SELECT n FROM cte_t ORDER BY n DESC LIMIT 1)) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
        // LIMIT on the query that reads the WITH item is untouched
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) "
                + "SELECT n FROM r ORDER BY n LIMIT 2", "1", "2");
    }

    // ================================================================
    // 6. The union form, and column types across the two terms
    // ================================================================

    @Test
    @DisplayName("a recursive item that is not a UNION is 42P19")
    void mustBeAUnion() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 EXCEPT SELECT n FROM r) SELECT n FROM r",
                "42P19", "recursive query \"r\" does not have the form "
                        + "non-recursive-term UNION [ALL] recursive-term");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 INTERSECT SELECT n FROM r) SELECT n FROM r",
                "42P19", "does not have the form non-recursive-term UNION [ALL] recursive-term");
        assertFails("WITH RECURSIVE cte_t AS (SELECT n FROM cte_t) SELECT n FROM cte_t ORDER BY n",
                "42P19", "recursive query \"cte_t\" does not have the form");
    }

    @Test
    @DisplayName("a column the recursive term widens is 42804")
    void columnTypeMustMatchTheSeed() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1::int UNION SELECT (n + 1)::bigint FROM r "
                        + "WHERE n < 3) SELECT n FROM r ORDER BY n",
                "42804", "recursive query \"r\" column 1 has type integer in non-recursive term "
                        + "but type bigint overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1::int UNION ALL SELECT (n + 1)::numeric FROM r "
                        + "WHERE n < 3) SELECT n FROM r ORDER BY n",
                "42804", "has type integer in non-recursive term but type numeric overall");
    }

    @Test
    @DisplayName("a recursive term that narrows, or stays put, is fine")
    void narrowingAndEqualTypesAreAllowed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT (n + 1)::int FROM r "
                + "WHERE n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT n + 1 FROM r "
                + "WHERE n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::varchar UNION ALL SELECT (s || 'a')::text "
                + "FROM r WHERE length(s) < 3) SELECT s FROM r ORDER BY s", "a", "aa", "aaa");
        assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::text UNION ALL SELECT (s || 'a')::varchar "
                + "FROM r WHERE length(s) < 3) SELECT s FROM r ORDER BY s", "a", "aa", "aaa");
    }

    // ================================================================
    // 7. RECURSIVE without a self-reference is an ordinary query
    // ================================================================

    @Test
    @DisplayName("a WITH RECURSIVE item that never names itself is evaluated once")
    void recursiveWithoutSelfReference() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2) SELECT n FROM r ORDER BY n",
                "1", "2");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT 2) SELECT n FROM r ORDER BY n",
                "1", "2");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 EXCEPT SELECT 2) SELECT n FROM r ORDER BY n", "1");
        assertRows("WITH RECURSIVE r(n) AS (VALUES (1),(2)) SELECT n FROM r ORDER BY n", "1", "2");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1) SELECT n FROM r", "1");
    }

    @Test
    @DisplayName("SEARCH or CYCLE on an item that never recurses is 42601")
    void searchNeedsAnActualRecursion() {
        assertFails("WITH RECURSIVE r AS (SELECT 1 AS n) SEARCH DEPTH FIRST BY n SET ord "
                        + "SELECT n FROM r",
                "42601", "WITH query is not recursive");
    }

    // ================================================================
    // 8. SEARCH and CYCLE bind to their own WITH item
    // ================================================================

    @Test
    @DisplayName("a SEARCH or CYCLE clause may be followed by another WITH item")
    void searchThenAnotherCte() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS m) SELECT n FROM r ORDER BY ord",
                "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "SEARCH BREADTH FIRST BY n SET ord, s AS (SELECT 9 AS m) SELECT m FROM s", "9");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "CYCLE n SET is_cycle USING path, s AS (SELECT 9 AS m) "
                + "SELECT n, is_cycle FROM r ORDER BY n", "1|f", "2|f", "3|f");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "SEARCH DEPTH FIRST BY n SET ord CYCLE n SET is_cycle USING path, "
                + "s AS (SELECT 9 AS m) SELECT n FROM r ORDER BY ord", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) "
                + "SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS m), u AS (SELECT 8 AS k) "
                + "SELECT m, k FROM s, u", "9|8");
        assertRows("WITH RECURSIVE a AS (SELECT 7 AS p), r(n) AS (SELECT 1 UNION ALL "
                + "SELECT n+1 FROM r WHERE n<3) SEARCH BREADTH FIRST BY n SET ord, s AS (SELECT 9 AS m) "
                + "SELECT p, m FROM a, s", "7|9");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) "
                + "SEARCH DEPTH FIRST BY n SET ord, q(m) AS (SELECT 1 UNION ALL SELECT m+1 FROM q "
                + "WHERE m<2) SEARCH DEPTH FIRST BY m SET ord2 SELECT n FROM r ORDER BY ord",
                "1", "2", "3");
    }

    @Test
    @DisplayName("SEARCH as the last thing in the WITH clause still works")
    void searchAloneStillWorks() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r ORDER BY ord", "1", "2", "3");
        assertRows("WITH RECURSIVE s AS (SELECT 9 AS m), r(n) AS (SELECT 1 UNION ALL "
                + "SELECT n + 1 FROM r WHERE n < 3) SEARCH DEPTH FIRST BY n SET ord "
                + "SELECT n FROM r ORDER BY ord", "1", "2", "3");
    }

    // ================================================================
    // 9. A plain WITH item sees only the items before it
    // ================================================================

    @Test
    @DisplayName("a forward reference in a plain WITH clause is 42P01")
    void forwardReferenceIsNotARelation() {
        assertFails("WITH x AS (SELECT n FROM y), y AS (SELECT 2 AS n) SELECT * FROM x",
                "42P01", "relation \"y\" does not exist");
        assertFails("WITH x AS (SELECT (SELECT n FROM y) AS n), y AS (SELECT 2 AS n) SELECT * FROM x",
                "42P01", "relation \"y\" does not exist");
        assertFails("WITH x AS (SELECT 1 AS n), y AS (SELECT n FROM z), z AS (SELECT 3 AS n) "
                        + "SELECT n FROM y",
                "42P01", "relation \"z\" does not exist");
        assertFails("WITH x AS (SELECT n FROM x) SELECT * FROM x", "42P01", "relation \"x\" does not exist");
    }

    @Test
    @DisplayName("a backward reference is what a plain WITH clause is for")
    void backwardReferencesResolve() throws Exception {
        assertRows("WITH y AS (SELECT 2 AS n), x AS (SELECT n FROM y) SELECT * FROM x", "2");
        assertRows("WITH x AS (SELECT 1 AS n), y AS (SELECT n FROM x), z AS (SELECT n FROM y) "
                + "SELECT n FROM z", "1");
        // the query itself sees every item, in any order
        assertRows("WITH x AS (SELECT 1 AS n), y AS (SELECT 2 AS n) "
                + "SELECT (SELECT n FROM y) AS a, (SELECT n FROM x) AS b", "2|1");
    }

    @Test
    @DisplayName("with RECURSIVE the whole list is visible to every item")
    void recursiveSeesLaterItems() throws Exception {
        assertRows("WITH RECURSIVE x AS (SELECT n FROM y), y AS (SELECT 2 AS n) SELECT * FROM x", "2");
        assertRows("WITH RECURSIVE y AS (SELECT 2 AS n), x AS (SELECT n FROM y) SELECT * FROM x", "2");
    }

    @Test
    @DisplayName("a hidden WITH name falls through to the stored relation of that name")
    void shadowingAStoredTable() throws Exception {
        // the item before it shadows the table
        assertRows("WITH cte_t AS (SELECT 99 AS n) SELECT n FROM cte_t", "99");
        // a later item does not: x reads the table
        assertRows("WITH x AS (SELECT n FROM cte_t), cte_t AS (SELECT 99 AS n) SELECT n FROM x ORDER BY n",
                "1", "2", "3");
        // nor does the item's own name inside its own body
        assertRows("WITH cte_t AS (SELECT n FROM cte_t) SELECT n FROM cte_t ORDER BY n", "1", "2", "3");
    }

    @Test
    @DisplayName("an inner WITH shadows an outer one and can still read it")
    void nestedWithClauses() throws Exception {
        assertRows("WITH x AS (SELECT 1 AS n) SELECT * FROM (WITH x AS (SELECT 2 AS n) "
                + "SELECT n FROM x) z", "2");
        assertRows("WITH x AS (SELECT 1 AS n) SELECT * FROM (WITH y AS (SELECT n FROM x) "
                + "SELECT n FROM y) z", "1");
    }

    @Test
    @DisplayName("two WITH items may not share a name")
    void duplicateWithName() {
        assertFails("WITH x AS (SELECT 1 AS n), x AS (SELECT 2 AS n) SELECT * FROM x",
                "42712", "WITH query name \"x\" specified more than once");
        assertFails("WITH RECURSIVE x AS (SELECT 1 AS n), x AS (SELECT 2 AS n) SELECT * FROM x",
                "42712", "WITH query name \"x\" specified more than once");
    }

    @Test
    @DisplayName("two WITH items that read each other are 0A000")
    void mutualRecursion() {
        assertFails("WITH RECURSIVE x AS (SELECT n FROM y), y AS (SELECT n FROM x) SELECT * FROM x",
                "0A000", "mutual recursion between WITH items is not implemented");
        assertFails("WITH RECURSIVE x(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM y WHERE n<3), "
                        + "y(n) AS (SELECT n FROM x) SELECT n FROM x ORDER BY n",
                "0A000", "mutual recursion between WITH items is not implemented");
    }

    // ================================================================
    // 10. Ordinary recursion is untouched
    // ================================================================

    @Test
    @DisplayName("the shapes an application actually writes still run")
    void ordinaryRecursionStillRuns() throws Exception {
        assertRows("WITH RECURSIVE p(a,b) AS (SELECT a,b FROM cte_edge WHERE a = 1 UNION ALL "
                + "SELECT e.a, e.b FROM cte_edge e JOIN p ON e.a = p.b) SELECT a,b FROM p ORDER BY a,b",
                "1|2", "2|3", "3|4");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n + 1 FROM r "
                + "JOIN cte_t t ON t.n = r.n WHERE r.n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT z.n+1 FROM r z WHERE z.n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::text UNION ALL SELECT s || 'a' FROM r "
                + "WHERE length(s) < 3) SELECT s FROM r ORDER BY s", "a", "aa", "aaa");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM r "
                + "WHERE n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3 "
                + "GROUP BY n) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3), "
                + "q(m) AS (SELECT 1 UNION ALL SELECT m+1 FROM q WHERE m<2) "
                + "SELECT r.n, q.m FROM r, q ORDER BY 1,2",
                "1|1", "1|2", "2|1", "2|2", "3|1", "3|2");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3), "
                + "s(m) AS (SELECT n FROM r) SELECT m FROM s ORDER BY m", "1", "2", "3");
        assertRows("WITH a AS (SELECT n FROM cte_t), b AS (SELECT n FROM cte_t) "
                + "SELECT a.n FROM a JOIN b ON a.n = b.n ORDER BY 1", "1", "2", "3");
        assertRows("WITH x AS (SELECT n FROM cte_t ORDER BY n LIMIT 2) SELECT n FROM x ORDER BY n",
                "1", "2");
    }
}
