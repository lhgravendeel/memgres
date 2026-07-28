package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL makes a set of clause-level checks before it reads a row: which expressions a
 * grouped query may name, whether DISTINCT ON has a defined answer, whether two FROM items can
 * be told apart, and where a data-modifying statement may appear. Left unmade, each of these
 * turns a query PostgreSQL refuses into one that quietly returns an arbitrary answer — or, for
 * the nested writes, quietly changes data.
 */
class QueryClauseValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE qcv_t (i int, g int, v int, s text)");
        exec("INSERT INTO qcv_t VALUES (1,1,10,'a'),(2,1,20,NULL),(3,2,30,'c'),(4,2,40,'d')");
        exec("CREATE TABLE qcv_a (id int, x int)");
        exec("INSERT INTO qcv_a VALUES (1,10),(2,20),(3,30)");
        exec("CREATE TABLE qcv_b (id int, y int)");
        exec("INSERT INTO qcv_b VALUES (1,100),(2,200),(3,300)");
        exec("CREATE TABLE qcv_ins (i int primary key, j int)");
        exec("CREATE FUNCTION qcv_setof_rec() RETURNS SETOF record AS $$ SELECT 1, 2 $$ LANGUAGE sql");
        exec("CREATE FUNCTION qcv_out_fn(OUT a int, OUT b text) RETURNS SETOF record"
                + " AS $$ SELECT 1, 'x' $$ LANGUAGE sql");
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

    private static int rowCount(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = 0;
            while (rs.next()) n++;
            return n;
        }
    }

    private static SQLException rejects(String sqlState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        return e;
    }

    private static void rejects(String sqlState, String messagePart, String sql) {
        SQLException e = rejects(sqlState, sql);
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- Aggregate placement ----

    @Test
    void anUngroupedColumnInHavingIsRejected() {
        rejects("42803", "column \"qcv_t.i\" must appear in the GROUP BY clause",
                "SELECT count(*) FROM qcv_t HAVING i > 0");
        rejects("42803", "column \"qcv_t.i\" must appear in the GROUP BY clause",
                "SELECT g, count(*) FROM qcv_t GROUP BY g HAVING i > 0");
    }

    @Test
    void anAggregateInOrderByGroupsTheWholeQuery() {
        rejects("42803", "column \"qcv_t.i\" must appear in the GROUP BY clause",
                "SELECT i FROM qcv_t ORDER BY sum(v)");
    }

    @Test
    void anAggregateArgumentMayNotReturnASet() {
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT count(generate_series(1,3))");
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT sum(generate_series(1,3))");
    }

    @Test
    void aNullObjectKeyIsRefusedRatherThanDropped() {
        rejects("22004", "null value not allowed for object key",
                "SELECT json_object_agg(s, i) FROM qcv_t");
        rejects("22023", "field name must not be null",
                "SELECT jsonb_object_agg(s, i) FROM qcv_t");
    }

    @Test
    void ordinaryAggregatesStillWork() throws Exception {
        assertEquals("4", scalar("SELECT count(*) FROM qcv_t HAVING count(*) > 0"));
        assertEquals("2", scalar("SELECT g FROM qcv_t GROUP BY g HAVING g > 1"));
        assertEquals("2", scalar("SELECT g FROM qcv_t GROUP BY g ORDER BY sum(v) DESC"));
        assertEquals("100", scalar("SELECT sum(v) FROM qcv_t ORDER BY sum(v)"));
        assertEquals("1", scalar("SELECT 1 FROM qcv_t ORDER BY sum(v)"));
        assertEquals("4", scalar("SELECT count(*) FROM qcv_t HAVING sum(v) > 0"));
        assertEquals("{\"a\": 1, \"c\": 3, \"d\": 4}",
                scalar("SELECT jsonb_object_agg(s, i) FROM qcv_t WHERE s IS NOT NULL"));
        assertEquals("3", scalar("SELECT count(*) FROM (SELECT generate_series(1,3)) z"));
        // the checks that were already in place stay in place
        rejects("42803", "SELECT count(*) FROM qcv_t WHERE count(*) > 0");
        rejects("42803", "SELECT sum(count(*)) FROM qcv_t");
        rejects("42803", "SELECT count(*) FROM qcv_t GROUP BY count(*)");
        rejects("42803", "SELECT i FROM qcv_t GROUP BY g");
    }

    // ---- DISTINCT ON ----

    @Test
    void distinctOnMustLeadTheOrderBy() {
        rejects("42P10", "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
                "SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY i");
        rejects("42P10", "SELECT DISTINCT ON (i) i FROM qcv_t ORDER BY g");
        rejects("42P10", "SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY 1");
    }

    @Test
    void aMatchingOrDroppedOrderByIsFine() throws Exception {
        assertEquals(2, rowCount("SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY g, i"));
        assertEquals(2, rowCount("SELECT DISTINCT ON (g) i FROM qcv_t"));
        assertEquals(4, rowCount("SELECT DISTINCT ON (g, i) i FROM qcv_t ORDER BY g, i"));
        assertEquals("3", scalar("SELECT DISTINCT ON (g) i FROM qcv_t ORDER BY g DESC, i"));
        assertEquals(2, rowCount("SELECT DISTINCT ON (g+0) i FROM qcv_t ORDER BY g+0, i"));
        // an output alias or a qualified name still names the same sort key
        assertEquals(2, rowCount("SELECT DISTINCT ON (g) g AS grp, i FROM qcv_t ORDER BY grp, i"));
        assertEquals(2, rowCount("SELECT DISTINCT ON (qcv_t.g) i FROM qcv_t ORDER BY g"));
    }

    // ---- Joins ----

    @Test
    void twoFromItemsMayNotShareAName() {
        rejects("42712", "table name \"qcv_a\" specified more than once",
                "SELECT count(*) FROM qcv_a JOIN qcv_a ON true");
        rejects("42712", "table name \"qcv_a\" specified more than once",
                "SELECT count(*) FROM qcv_a, qcv_a");
        rejects("42712", "table name \"t1\" specified more than once",
                "SELECT count(*) FROM qcv_a t1, qcv_b t1");
        rejects("42712", "SELECT count(*) FROM qcv_a, (SELECT 1) qcv_a");
        rejects("42712", "SELECT count(*) FROM public.qcv_a, qcv_a");
        rejects("42712", "SELECT count(*) FROM generate_series(1,2), generate_series(1,2)");
    }

    @Test
    void distinctlyNamedFromItemsAreFine() throws Exception {
        assertEquals("9", scalar("SELECT count(*) FROM qcv_a a1 JOIN qcv_a a2 ON true"));
        assertEquals("9", scalar("SELECT count(*) FROM qcv_a JOIN qcv_b ON true"));
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a JOIN (SELECT 1 AS id) qcv_b ON true"));
        assertEquals("1", scalar("WITH w AS (SELECT 1 AS n) SELECT count(*) FROM w w1, w w2"));
    }

    @Test
    void aJoinConditionMustBeAPredicate() {
        rejects("42804", "argument of JOIN/ON must be type boolean, not type integer",
                "SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id");
        rejects("42804", "SELECT count(*) FROM qcv_a LEFT JOIN qcv_b ON qcv_a.id");
        rejects("42803", "aggregate functions are not allowed in JOIN conditions",
                "SELECT count(*) FROM qcv_a JOIN qcv_b ON count(*) > 0");
        rejects("42P20", "window functions are not allowed in JOIN conditions",
                "SELECT count(*) FROM qcv_a JOIN qcv_b ON row_number() OVER () > 0");
        rejects("42701", "column name \"id\" appears more than once in USING clause",
                "SELECT count(*) FROM qcv_a JOIN qcv_b USING (id, id)");
    }

    @Test
    void ordinaryJoinConditionsStillWork() throws Exception {
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id"));
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a JOIN qcv_b USING (id)"));
        assertEquals("9", scalar("SELECT count(*) FROM qcv_a JOIN qcv_b ON true"));
        assertEquals("9", scalar(
                "SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id > 0 AND qcv_b.id > 0"));
        assertEquals("9", scalar("SELECT count(*) FROM qcv_a JOIN qcv_b ON qcv_a.id::boolean"));
    }

    @Test
    void aNameReachingThroughAJoinAliasMayBeAmbiguous() {
        rejects("42702", "column reference \"id\" is ambiguous",
                "SELECT count(*) FROM (qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j WHERE j.id = 2");
        rejects("42702", "SELECT count(*) FROM"
                + " (SELECT * FROM qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j WHERE j.id = 2");
    }

    @Test
    void anUnambiguousNameThroughAJoinAliasResolves() throws Exception {
        assertEquals("1", scalar(
                "SELECT count(*) FROM (qcv_a JOIN qcv_b ON qcv_a.id = qcv_b.id) AS j WHERE j.x = 10"));
        assertEquals("1", scalar(
                "SELECT count(*) FROM (qcv_a JOIN qcv_b USING (id)) AS j WHERE j.id = 2"));
    }

    @Test
    void aLateralItemMayNotReachAcrossTheNullableSide() {
        rejects("42P10", "invalid reference to FROM-clause entry for table \"t\"",
                "SELECT count(*) FROM qcv_a t RIGHT JOIN LATERAL (SELECT t.x) s ON true");
        rejects("42P10", "SELECT count(*) FROM qcv_a t FULL JOIN LATERAL (SELECT t.x) s ON true");
        rejects("42P10", "SELECT count(*) FROM qcv_a t RIGHT JOIN generate_series(1, t.x) s ON true");
    }

    @Test
    void lateralOverAnInnerOrLeftJoinStillWorks() throws Exception {
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a t LEFT JOIN LATERAL (SELECT t.x) s ON true"));
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a t, LATERAL (SELECT t.x) s"));
        assertEquals("3", scalar("SELECT count(*) FROM qcv_a t RIGHT JOIN LATERAL (SELECT 1 AS z) s ON true"));
    }

    // ---- WITHIN GROUP ----

    @Test
    void withinGroupOnlySuitsAnOrderedSetAggregate() {
        rejects("42883", "function sum(integer, integer) does not exist",
                "SELECT sum(v) WITHIN GROUP (ORDER BY v) FROM qcv_t");
        rejects("42809", "count is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
                "SELECT count(*) WITHIN GROUP (ORDER BY 1) FROM qcv_t");
        rejects("42883", "SELECT string_agg(s, ',') WITHIN GROUP (ORDER BY i) FROM qcv_t");
    }

    @Test
    void theRealOrderedSetAggregatesStillWork() throws Exception {
        assertEquals("25", scalar("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM qcv_t"));
        assertEquals("10", scalar("SELECT mode() WITHIN GROUP (ORDER BY v) FROM qcv_t"));
    }

    // ---- Functions in FROM ----

    @Test
    void aRecordReturningFunctionNeedsExactlyOneDescriptionOfItsColumns() {
        rejects("42601", "a column definition list is required for functions returning \"record\"",
                "SELECT * FROM qcv_setof_rec()");
        rejects("42601", "a column definition list is redundant for a function with OUT parameters",
                "SELECT * FROM qcv_out_fn() AS t(x int, y text)");
        rejects("42P13", "return type mismatch in function declared to return record",
                "SELECT * FROM qcv_setof_rec() AS t(x int, y int, z int)");
        rejects("42703", "could not identify column \"y\" in record data type",
                "SELECT string_agg((qcv_setof_rec()).y, ',')");
    }

    @Test
    void aMatchingColumnDefinitionListResolves() throws Exception {
        assertEquals("1", scalar("SELECT x FROM qcv_setof_rec() AS t(x int, y int)"));
        assertEquals("1", scalar("SELECT a FROM qcv_out_fn()"));
    }

    @Test
    void anyFunctionMayAppearInFrom() throws Exception {
        assertEquals("3", scalar("SELECT * FROM abs(-3)"));
        assertEquals("HI", scalar("SELECT * FROM upper('hi') AS t(u)"));
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM abs(1) WITH ORDINALITY AS t(v, o)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("v"));
            assertEquals(1, rs.getInt("o"));
            assertFalse(rs.next());
        }
        // the column takes the FROM item's name when no column list renames it
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM abs(1) AS t")) {
            assertEquals("t", rs.getMetaData().getColumnLabel(1));
        }
        assertEquals(3, rowCount("SELECT * FROM generate_series(1,3) WITH ORDINALITY AS t(v, o)"));
    }

    // ---- RETURNING ----

    @Test
    void returningIsPerRowSoItTakesNeitherAggregateNorWindow() throws Exception {
        exec("DELETE FROM qcv_ins");
        rejects("42803", "aggregate functions are not allowed in RETURNING",
                "INSERT INTO qcv_ins VALUES (1,1) RETURNING count(*)");
        rejects("42P20", "window functions are not allowed in RETURNING",
                "INSERT INTO qcv_ins VALUES (2,2) RETURNING row_number() OVER ()");
        rejects("42803", "UPDATE qcv_ins SET j = 2 RETURNING count(*)");
        rejects("42803", "DELETE FROM qcv_ins RETURNING count(*)");
        assertEquals("0", scalar("SELECT count(*) FROM qcv_ins"));
    }

    @Test
    void ordinaryReturningStillWorks() throws Exception {
        exec("DELETE FROM qcv_ins");
        assertEquals("5", scalar("INSERT INTO qcv_ins VALUES (5,5) RETURNING i"));
        assertEquals("6", scalar("UPDATE qcv_ins SET j = 6 RETURNING j"));
        assertEquals("5", scalar("DELETE FROM qcv_ins RETURNING i"));
    }

    // ---- Data-modifying statements out of place ----

    @Test
    void aWriteInAFromSubqueryIsASyntaxError() throws Exception {
        exec("DELETE FROM qcv_ins");
        rejects("42601", "syntax error at or near \"INTO\"",
                "SELECT * FROM (INSERT INTO qcv_ins VALUES (9,9) RETURNING j) x");
        rejects("42601", "syntax error at or near \"SET\"",
                "SELECT * FROM (UPDATE qcv_ins SET j=1 RETURNING i) x");
        rejects("42601", "syntax error at or near \"FROM\"",
                "SELECT * FROM (DELETE FROM qcv_ins RETURNING i) x");
        assertEquals("0", scalar("SELECT count(*) FROM qcv_ins"));
    }

    @Test
    void aDataModifyingCteMustBeAtTheTopLevel() throws Exception {
        exec("DELETE FROM qcv_ins");
        rejects("0A000", "WITH clause containing a data-modifying statement must be at the top level",
                "SELECT 1 WHERE EXISTS (WITH x AS (INSERT INTO qcv_ins VALUES (8,8) RETURNING i)"
                        + " SELECT 1 FROM x)");
        rejects("0A000", "WITH a AS (WITH b AS (INSERT INTO qcv_ins VALUES (21,21) RETURNING i)"
                + " SELECT * FROM b) SELECT * FROM a");
        assertEquals("0", scalar("SELECT count(*) FROM qcv_ins"));
        // the top-level form is exactly what PG allows, and it still runs
        assertEquals("7", scalar(
                "WITH x AS (INSERT INTO qcv_ins VALUES (7,7) RETURNING i) SELECT * FROM x"));
        assertEquals("1", scalar("SELECT count(*) FROM qcv_ins"));
    }
}
