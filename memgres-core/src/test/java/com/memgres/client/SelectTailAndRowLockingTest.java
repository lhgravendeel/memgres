package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The tail of a SELECT is one grammar, and a lock sees the rows the query sees.
 *
 * <p>What follows a query's ORDER BY is not a run of independent optional words. PostgreSQL writes
 * the row count and the starting point as a single clause — either order, but always together —
 * and the locking clause sits before that clause or after it, never through the middle of it.
 * memgres read each word as its own optional clause in a fixed sequence, so the order they were
 * written in decided whether the query parsed at all: {@code OFFSET 1 LIMIT 2 FOR UPDATE} was a
 * syntax error where {@code LIMIT 2 OFFSET 1 FOR UPDATE} was not, and nothing could follow the
 * locking clause.
 *
 * <p>The locking clause is spelled out in full or not written at all. Every word after the first
 * was optional, so {@code FOR NO} and {@code FOR KEY} named strengths nobody wrote and
 * {@code FOR UPDATE SKIP} quietly turned on SKIP LOCKED — a worker-queue query answering with
 * fewer rows than its author asked for.
 *
 * <p>A lock is taken on rows a scan produced. A set-returning call in the select list makes rows
 * no scan produced, so PostgreSQL refuses the two together rather than locking some rows once and
 * others several times; ORDER BY counts as part of that list, because that is where PostgreSQL
 * puts the expressions it sorts by.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class SelectTailAndRowLockingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE s9_a (id int, v text)");
            st.execute("INSERT INTO s9_a VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')");
            st.execute("CREATE TABLE s9_part (id int, v text) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE s9_part1 PARTITION OF s9_part FOR VALUES FROM (1) TO (10)");
            st.execute("INSERT INTO s9_part VALUES (1, 'a'), (2, 'b')");
            st.execute("CREATE TABLE s9_parent (id int)");
            st.execute("CREATE TABLE s9_child () INHERITS (s9_parent)");
            st.execute("INSERT INTO s9_parent VALUES (1)");
            st.execute("INSERT INTO s9_child VALUES (2)");
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

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage().getMessage();
    }

    /** The count and the starting point are one clause, written in either order. */
    @Test
    void theRowCountAndTheStartingPointAreOneClauseInEitherOrder() throws Exception {
        List<String> expected = new ArrayList<>();
        expected.add("2");
        expected.add("3");
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id LIMIT 2 OFFSET 1"));
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id OFFSET 1 LIMIT 2"));
        assertEquals(expected,
                rows("SELECT id FROM s9_a ORDER BY id OFFSET 1 FETCH FIRST 2 ROWS ONLY"));
        assertEquals(expected,
                rows("SELECT id FROM s9_a ORDER BY id FETCH FIRST 2 ROWS ONLY OFFSET 1"));
        assertEquals(expected,
                rows("SELECT id FROM s9_a ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY"));
    }

    /** LIMIT ALL is a count too, so it takes an offset on either side of it. */
    @Test
    void limitAllIsACountLikeAnyOther() throws Exception {
        List<String> expected = new ArrayList<>();
        expected.add("3");
        expected.add("4");
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id LIMIT ALL OFFSET 2"));
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id OFFSET 2 LIMIT ALL"));
    }

    /** The locking clause comes before that clause or after it, whichever the author chose. */
    @Test
    void theLockingClauseComesBeforeThatClauseOrAfterIt() throws Exception {
        List<String> expected = new ArrayList<>();
        expected.add("2");
        expected.add("3");
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id LIMIT 2 OFFSET 1 FOR UPDATE"));
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id OFFSET 1 LIMIT 2 FOR UPDATE"));
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE LIMIT 2 OFFSET 1"));
        assertEquals(expected, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE OFFSET 1 LIMIT 2"));
        assertEquals(expected,
                rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE OFFSET 1 ROWS "
                        + "FETCH NEXT 2 ROWS ONLY"));
    }

    /** It is never written through the middle of it. */
    @Test
    void itIsNeverWrittenThroughTheMiddleOfIt() {
        assertEquals("42601",
                stateOf("SELECT id FROM s9_a ORDER BY id LIMIT 2 FOR UPDATE OFFSET 1"));
        assertEquals("42601",
                stateOf("SELECT id FROM s9_a ORDER BY id OFFSET 1 FOR UPDATE LIMIT 2"));
        assertEquals("42601",
                stateOf("SELECT id FROM s9_a ORDER BY id FETCH FIRST 2 ROWS ONLY "
                        + "FOR UPDATE OFFSET 1"));
        assertEquals("42601",
                stateOf("SELECT id FROM s9_a ORDER BY id FOR UPDATE LIMIT 1 FOR SHARE"));
    }

    /** One count and one offset, so a second of either is a syntax error. */
    @Test
    void oneCountAndOneStartingPointIsAllThereIs() {
        assertEquals("42601", stateOf("SELECT id FROM s9_a ORDER BY id LIMIT 1 LIMIT 1"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a ORDER BY id LIMIT 1 OFFSET 1 OFFSET 1"));
    }

    /** Each strength, and each option, is accepted as PostgreSQL spells it. */
    @Test
    void everyStrengthAndOptionIsAcceptedAsSpelled() throws Exception {
        List<String> all = new ArrayList<>();
        all.add("1");
        all.add("2");
        all.add("3");
        all.add("4");
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR NO KEY UPDATE"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR SHARE"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR KEY SHARE"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE OF s9_a"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE NOWAIT"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE SKIP LOCKED"));
        assertEquals(all, rows("SELECT id FROM s9_a ORDER BY id FOR UPDATE OF s9_a NOWAIT"));
    }

    /** A strength that stops part-way through is not a strength. */
    @Test
    void aStrengthThatStopsPartWayThroughIsNotAStrength() {
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR NO"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR NO KEY"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR KEY"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR NO UPDATE"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR KEY UPDATE"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR NO KEY SHARE"));
    }

    /** Neither is an option that stops part-way through, or one written twice over. */
    @Test
    void anOptionThatStopsPartWayThroughIsNotAnOption() {
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR UPDATE SKIP"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR UPDATE LOCKED"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR UPDATE OF"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR UPDATE NOWAIT SKIP LOCKED"));
        assertEquals("42601", stateOf("SELECT id FROM s9_a FOR UPDATE SKIP LOCKED NOWAIT"));
    }

    /** A lock and a set-returning call in the select list are refused together. */
    @Test
    void aLockAndASetReturningCallInTheSelectListAreRefusedTogether() {
        assertEquals("0A000", stateOf("SELECT generate_series(1, 2) FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT id, generate_series(1, 2) FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT generate_series(1, 2) + 1 FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT unnest(ARRAY[1, 2]) FROM s9_a FOR UPDATE"));
        assertEquals("0A000",
                stateOf("SELECT json_array_elements('[1,2]'::json) FROM s9_a FOR SHARE"));
        assertEquals("0A000", stateOf("SELECT generate_series(1, 2) FOR UPDATE"));
        assertEquals("0A000",
                stateOf("SELECT generate_series(1, 2) FROM s9_a FOR UPDATE OF s9_a"));
    }

    /** The refusal names the strength the query asked for. */
    @Test
    void theRefusalNamesTheStrengthTheQueryAskedFor() {
        assertEquals("FOR UPDATE is not allowed with set-returning functions in the target list",
                messageOf("SELECT generate_series(1, 2) FROM s9_a FOR UPDATE"));
        assertEquals("FOR NO KEY UPDATE is not allowed with set-returning functions "
                        + "in the target list",
                messageOf("SELECT generate_series(1, 2) FROM s9_a FOR NO KEY UPDATE"));
        assertEquals("FOR SHARE is not allowed with set-returning functions in the target list",
                messageOf("SELECT generate_series(1, 2) FROM s9_a FOR SHARE"));
        assertEquals("FOR KEY SHARE is not allowed with set-returning functions in the target list",
                messageOf("SELECT generate_series(1, 2) FROM s9_a FOR KEY SHARE"));
    }

    /** ORDER BY is part of that list, and a subquery in FROM carries its own. */
    @Test
    void orderByIsPartOfThatListAndASubqueryCarriesItsOwn() {
        assertEquals("0A000",
                stateOf("SELECT id FROM s9_a ORDER BY generate_series(1, 2) FOR UPDATE"));
        assertEquals("0A000",
                stateOf("SELECT * FROM (SELECT generate_series(1, 2) g FROM s9_a) s FOR UPDATE"));
        assertEquals("0A000",
                stateOf("SELECT * FROM (SELECT generate_series(1, 2) g FROM s9_a) s FOR KEY SHARE"));
    }

    /** A call that produces another query's rows produces them there, not here. */
    @Test
    void aCallThatProducesAnotherQuerysRowsIsNotInThisList() throws Exception {
        List<String> expected = new ArrayList<>();
        expected.add("1");
        expected.add("2");
        assertEquals(expected,
                rows("SELECT id FROM s9_a WHERE id IN (SELECT generate_series(1, 2)) "
                        + "ORDER BY id FOR UPDATE"));
        assertEquals("OK", stateOf("SELECT id FROM s9_a, generate_series(1, 2) g "
                + "ORDER BY id, g FOR UPDATE OF s9_a"));
        assertEquals("OK", stateOf("SELECT generate_series(1, 2) FROM s9_a ORDER BY 1"));
    }

    /** A lock reads the rows the query reads, partitions and children included. */
    @Test
    void aLockReadsTheRowsTheQueryReads() throws Exception {
        List<String> two = new ArrayList<>();
        two.add("1");
        two.add("2");
        assertEquals(two, rows("SELECT id FROM s9_part ORDER BY id FOR UPDATE SKIP LOCKED"));
        assertEquals(two, rows("SELECT id FROM s9_parent ORDER BY id FOR UPDATE SKIP LOCKED"));
        assertEquals(two, rows("SELECT id FROM s9_part ORDER BY id FOR UPDATE NOWAIT"));
        assertEquals(two, rows("SELECT id FROM s9_parent ORDER BY id FOR NO KEY UPDATE"));
    }

    /** What collapses rows still cannot be locked. */
    @Test
    void whatCollapsesRowsCannotBeLocked() {
        assertEquals("0A000", stateOf("SELECT DISTINCT id FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT count(*) FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT id FROM s9_a GROUP BY id FOR UPDATE"));
        assertEquals("0A000",
                stateOf("SELECT id FROM s9_a GROUP BY id HAVING count(*) > 0 FOR SHARE"));
        assertEquals("0A000",
                stateOf("SELECT row_number() OVER (ORDER BY id) FROM s9_a FOR UPDATE"));
        assertEquals("0A000", stateOf("SELECT id FROM (SELECT DISTINCT id FROM s9_a) s FOR UPDATE"));
    }
}
