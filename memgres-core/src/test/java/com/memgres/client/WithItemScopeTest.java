package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A WITH item belongs to the statement that declared it, and to the session running it.
 *
 * <p>The rows a recursive item's own name stands for while it is being built were installed as a
 * table in the shared schema, which put one session's half-finished recursion where every other
 * session could read it, and displaced for the length of the recursion whatever stored relation
 * carried the same name. A name declared twice was one entry keyed by that name, so an inner
 * WITH's rows answered for the outer item afterwards. And the scope a set operation opened was
 * left standing when an arm raised, so the next statement on that connection could read an item
 * its own text never declared.
 *
 * <p>SEARCH and CYCLE are settled while PostgreSQL analyses the statement, so an item carrying
 * either is held to it whether or not anything reads the item; and a query reading a WITH item
 * that writes without RETURNING is refused before the write it would have applied happens.
 */
class WithItemScopeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE wsr_edge (id int, parent int)");
            st.execute("INSERT INTO wsr_edge VALUES (1, NULL), (2, 1), (3, 2), (4, 2)");
            // A stored table under the name a recursion below declares for itself.
            st.execute("CREATE TABLE wsr_named (n int)");
            st.execute("INSERT INTO wsr_named VALUES (7)");
            st.execute("CREATE TABLE wsr_log (n int)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void resetLog() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM wsr_log");
            st.execute("INSERT INTO wsr_log VALUES (1), (2), (3)");
        }
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

    private static String scalar(String sql) throws SQLException {
        List<String> r = rows(sql);
        assertEquals(1, r.size(), "expected one row from: " + sql);
        return r.get(0);
    }

    private static String stateOf(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> rows(sql));
        return e.getSQLState();
    }

    private static final String COUNT_TO_FIVE =
            "WITH RECURSIVE wsr_named(n) AS (SELECT 1 UNION ALL"
                    + " SELECT n + 1 FROM wsr_named WHERE n < 5)";

    // ------------------------------------------- the working set is the session's own scratch

    /** The name means the item being built, not the table that happens to carry it. */
    @Test
    void aRecursiveItemsNameMeansTheItemAndNotAStoredTable() throws Exception {
        assertEquals("5", scalar(COUNT_TO_FIVE + " SELECT count(*) FROM wsr_named"));
        assertEquals("7", scalar("SELECT n FROM wsr_named"));
    }

    /** Writing the schema out is a reference to a stored relation and never to the item. */
    @Test
    void aSchemaQualifiedNameReachesTheTableWhileTheRecursionRuns() throws Exception {
        assertEquals("7", scalar(COUNT_TO_FIVE + " SELECT n FROM public.wsr_named"));
    }

    /** No other session sees the rows a recursion is part-way through building. */
    @Test
    void anotherSessionNeverSeesTheWorkingSet() throws Exception {
        try (Connection other = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
             Statement st = other.createStatement()) {
            assertEquals("5", scalar(COUNT_TO_FIVE + " SELECT count(*) FROM wsr_named"));
            try (ResultSet rs = st.executeQuery("SELECT n FROM wsr_named")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    /** A recursion inside another one builds its own rows rather than adding to the outer's. */
    @Test
    void aNestedRecursionKeepsItsOwnWorkingSet() throws Exception {
        assertEquals(List.of("1|3", "2|3", "3|3"), rows(
                "WITH RECURSIVE outer_r(n) AS (SELECT 1 UNION ALL"
                        + " SELECT n + 1 FROM outer_r WHERE n < 3)"
                        + " SELECT n, (WITH RECURSIVE inner_r(m) AS (SELECT 10 UNION ALL"
                        + " SELECT m + 1 FROM inner_r WHERE m < 12) SELECT count(*) FROM inner_r)"
                        + " FROM outer_r ORDER BY n"));
    }

    /** Ordinary recursion over a stored table answers as it always did. */
    @Test
    void recursionOverAStoredTableStillAnswers() throws Exception {
        assertEquals(List.of("1|0", "2|1", "3|2", "4|2"), rows(
                "WITH RECURSIVE t(id, depth) AS ("
                        + "SELECT id, 0 FROM wsr_edge WHERE parent IS NULL UNION ALL"
                        + " SELECT e.id, t.depth + 1 FROM wsr_edge e JOIN t ON e.parent = t.id)"
                        + " SELECT id, depth FROM t ORDER BY id"));
    }

    // ------------------------------------------------------- one name may stand for two items

    /** An inner declaration answers inside the query that made it and nowhere else. */
    @Test
    void aNameDeclaredTwiceIsTwoItems() throws Exception {
        assertEquals("2|1", scalar("WITH x AS (SELECT 1 AS a)"
                + " SELECT (WITH x AS (SELECT 2 AS a) SELECT a FROM x), (SELECT a FROM x)"));
        assertEquals("1|2", scalar("WITH x AS (SELECT 1 AS a)"
                + " SELECT (SELECT a FROM x), (WITH x AS (SELECT 2 AS a) SELECT a FROM x)"));
    }

    /** The same where the inner declaration is a relation of the query above. */
    @Test
    void aNameDeclaredTwiceIsTwoItemsInFromToo() throws Exception {
        assertEquals("2|1", scalar("WITH x AS (SELECT 1 AS a) SELECT s.a, (SELECT a FROM x)"
                + " FROM (WITH x AS (SELECT 2 AS a) SELECT a FROM x) s"));
    }

    // ------------------------------------------------- the scope goes down with the statement

    /** An arm that raises still takes the scope its set operation opened down with it. */
    @Test
    void aSetOperationThatRaisesLeavesNoScopeBehind() {
        assertEquals("22012",
                stateOf("WITH s AS (SELECT 1 AS a) SELECT a FROM s UNION ALL SELECT 1 / 0"));
        assertEquals("42P01", stateOf("SELECT a FROM s"));
        assertEquals("22012",
                stateOf("WITH s AS (SELECT 1 AS a) SELECT 1 / 0 UNION ALL SELECT a FROM s"));
        assertEquals("42P01", stateOf("SELECT a FROM s"));
    }

    // ------------------------------------------------- an item that writes and what reads it

    /** Reading an item that returns nothing is refused, and its write does not happen. */
    @Test
    void readingAWritingItemWithoutReturningIsRefused() throws Exception {
        assertEquals("0A000", stateOf("WITH w AS (DELETE FROM wsr_log) SELECT * FROM w"));
        assertEquals("0A000",
                stateOf("WITH w AS (UPDATE wsr_log SET n = n + 100) SELECT count(*) FROM w"));
        assertEquals("0A000",
                stateOf("WITH w AS (INSERT INTO wsr_log VALUES (99)) SELECT * FROM w"));
        assertEquals(List.of("1", "2", "3"), rows("SELECT n FROM wsr_log ORDER BY n"));
    }

    /** An item nothing reads is not refused, and writes. */
    @Test
    void anUnreadWritingItemStillWrites() throws Exception {
        assertEquals("1", scalar("WITH w AS (INSERT INTO wsr_log VALUES (9)) SELECT 1"));
        assertEquals(List.of("1", "2", "3", "9"), rows("SELECT n FROM wsr_log ORDER BY n"));
    }

    /** RETURNING is what gives a statement that writes a result to read. */
    @Test
    void anItemReadThroughReturningAnswersWithItsRows() throws Exception {
        assertEquals(List.of("2", "3"), rows(
                "WITH w AS (DELETE FROM wsr_log WHERE n > 1 RETURNING n) SELECT n FROM w ORDER BY n"));
        assertEquals(List.of("1"), rows("SELECT n FROM wsr_log ORDER BY n"));
    }

    // --------------------------------------------------- SEARCH and CYCLE, read or not read

    /** A column named twice in a BY list is asked for twice, whether or not the item is read. */
    @Test
    void aByListMayNotNameAColumnTwice() {
        assertEquals("42701", stateOf(recursiveW(" SEARCH DEPTH FIRST BY id, id SET ord") + " SELECT 1"));
        assertEquals("42701", stateOf(recursiveW(" CYCLE id, id SET c USING p") + " SELECT 1"));
    }

    /** A BY list may only name columns the item has. */
    @Test
    void aByListMayOnlyNameColumnsTheItemHas() {
        assertEquals("42601", stateOf(recursiveW(" SEARCH DEPTH FIRST BY nope SET ord") + " SELECT 1"));
        assertEquals("42601", stateOf(recursiveW(" CYCLE nope SET c USING p") + " SELECT 1"));
    }

    /**
     * The column the clause adds is in place while the recursive term is read, so a term naming
     * the colliding column without saying which relation it means is what the statement is
     * refused for.
     */
    @Test
    void anAddedColumnTheRecursiveTermNamesIsAnAmbiguousReference() {
        assertEquals("42702", stateOf(recursiveW(" SEARCH DEPTH FIRST BY id SET id") + " SELECT 1"));
        assertEquals("42702", stateOf(recursiveW(" CYCLE id SET id USING p") + " SELECT 1"));
        assertEquals("42702", stateOf(recursiveW(" CYCLE id SET c USING id") + " SELECT 1"));
    }

    /** Where the recursive term does not name it, the collision between the two columns is. */
    @Test
    void anAddedColumnTheRecursiveTermNeverNamesIsACollision() {
        String item = "WITH RECURSIVE w(id, n) AS (SELECT 1, 1 UNION ALL"
                + " SELECT 9, n + 1 FROM w WHERE n < 3)";
        assertEquals("42601", stateOf(item + " SEARCH DEPTH FIRST BY n SET id SELECT 1"));
        assertEquals("42601", stateOf(item + " CYCLE n SET id USING p SELECT 1"));
        assertEquals("42601", stateOf(item + " CYCLE n SET c USING id SELECT 1"));
    }

    /** The two clauses may not add the same name as each other either. */
    @Test
    void searchAndCycleMayNotAddTheSameName() {
        assertEquals("42601",
                stateOf(recursiveW(" SEARCH DEPTH FIRST BY id SET o CYCLE id SET o USING p")
                        + " SELECT 1"));
    }

    /** What the clauses are for still answers. */
    @Test
    void searchStillOrdersTheRowsItWasAskedTo() throws Exception {
        assertEquals(List.of("1|0", "2|1", "3|2", "4|2"), rows(
                "WITH RECURSIVE t(id, depth) AS ("
                        + "SELECT id, 0 FROM wsr_edge WHERE parent IS NULL UNION ALL"
                        + " SELECT e.id, t.depth + 1 FROM wsr_edge e JOIN t ON e.parent = t.id)"
                        + " SEARCH DEPTH FIRST BY id SET ord SELECT id, depth FROM t ORDER BY ord"));
    }

    /** A recursive item may not be written as a statement that writes. */
    @Test
    void aRecursiveItemThatWritesIsRefused() {
        assertEquals("42P19", stateOf("WITH RECURSIVE w AS ("
                + "INSERT INTO wsr_log SELECT n + 1 FROM w RETURNING n) SELECT n FROM w"));
    }

    private static String recursiveW(String clauses) {
        return "WITH RECURSIVE w(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM w WHERE id < 3)"
                + clauses;
    }
}
