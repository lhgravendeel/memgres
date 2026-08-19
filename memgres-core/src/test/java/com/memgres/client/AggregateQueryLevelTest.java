package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which query level an aggregate belongs to.
 *
 * <p>PostgreSQL places an aggregate at the query level its argument variables come from, which is
 * not always the level it is written at. {@code SELECT (SELECT sum(a.v)) FROM t a} names only the
 * outer relation's column, so the sum is answered over the outer query's group — once — and the
 * sub-select reads the answer. memgres answered such a query per row, leaving the aggregate to be
 * computed inside a sub-select that has no rows of its own to compute it over.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class AggregateQueryLevelTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_al1 (id int, g int, v int)");
            st.execute("INSERT INTO zz_al1 VALUES (1,1,10),(2,1,20),(3,2,70)");
            st.execute("CREATE TABLE zz_al2 (id int, w int)");
            st.execute("INSERT INTO zz_al2 VALUES (1,100),(2,200),(3,300)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) row.append('|');
                    row.append(rs.getString(i));
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    private static String sqlStateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    @Test
    void anAggregateOverAnOuterColumnIsAnsweredOnceAtTheOuterLevel() throws Exception {
        assertEquals(List.of("100"), rows("SELECT (SELECT sum(a.v)) FROM zz_al1 a"));
        assertEquals(List.of("3"), rows("SELECT (SELECT count(a.v)) FROM zz_al1 a"));
        assertEquals(List.of("70|10"),
                rows("SELECT (SELECT max(a.v)), (SELECT min(a.v)) FROM zz_al1 a"));
    }

    @Test
    void itIsAnsweredOverEachGroupOfTheQueryItBelongsTo() throws Exception {
        assertEquals(List.of("1|30", "2|70"),
                rows("SELECT a.g, (SELECT sum(a.v)) FROM zz_al1 a GROUP BY a.g ORDER BY a.g"));
        assertEquals(List.of("1|30", "2|140"),
                rows("SELECT a.g, (SELECT max(a.v) + min(a.v)) FROM zz_al1 a "
                        + "GROUP BY a.g ORDER BY a.g"));
    }

    @Test
    void anAggregateNamingTheSubSelectsOwnColumnsStaysThere() throws Exception {
        assertEquals(List.of("1|100", "2|100", "3|100"),
                rows("SELECT a.id, (SELECT sum(b.v) FROM zz_al1 b) FROM zz_al1 a ORDER BY a.id"));
        // A bare star names no relation, so it counts the rows it is written over.
        assertEquals(List.of("3", "3", "3"),
                rows("SELECT (SELECT count(*) FROM zz_al2 b) FROM zz_al1 a"));
        // A qualified star names one, and is judged by which one it names.
        assertEquals(List.of("3"),
                rows("SELECT (SELECT count(a.*) FROM zz_al2 b WHERE b.id = 1) FROM zz_al1 a"));
    }

    @Test
    void theAnswerCarriesTheTypeTheAggregateGivesIt() throws Exception {
        assertEquals(List.of("bigint"), rows("SELECT pg_typeof((SELECT sum(a.v))) FROM zz_al1 a"));
        assertEquals(List.of("numeric"), rows("SELECT pg_typeof((SELECT avg(a.v))) FROM zz_al1 a"));
        // Which is the type an operator over it has to be resolved against.
        assertEquals(List.of("101"), rows("SELECT (SELECT sum(a.v)) + 1 FROM zz_al1 a"));
        assertEquals(List.of("t"), rows("SELECT (SELECT max(a.v)) > 5 FROM zz_al1 a"));
    }

    @Test
    void theRuleReachesAsFarDownAsTheSubSelectsGo() throws Exception {
        assertEquals(List.of("100"), rows("SELECT (SELECT (SELECT sum(a.v))) FROM zz_al1 a"));
        assertEquals(List.of("100"),
                rows("SELECT (SELECT sum(a.v) FROM (SELECT 1 AS x) c) FROM zz_al1 a"));
        assertEquals(List.of("100"),
                rows("SELECT (WITH cte AS (SELECT 1 AS x) SELECT sum(a.v) FROM cte) FROM zz_al1 a"));
    }

    @Test
    void everyKindOfAggregateIsPlacedTheSameWay() throws Exception {
        assertEquals(List.of("t"), rows("SELECT (SELECT bool_and(a.v > 5)) FROM zz_al1 a"));
        assertEquals(List.of("2"), rows("SELECT (SELECT count(DISTINCT a.g)) FROM zz_al1 a"));
        assertEquals(List.of("20"), rows("SELECT (SELECT percentile_cont(0.5) "
                + "WITHIN GROUP (ORDER BY a.v)) FROM zz_al1 a"));
        // A window call is not an aggregate, so it stays where it is written and runs per row.
        assertEquals(List.of("10", "20", "70"),
                rows("SELECT (SELECT sum(a.v) OVER ()) FROM zz_al1 a ORDER BY 1"));
    }

    @Test
    void anAggregateOfThisQueryMayNotStandInThisQuerysWhere() {
        assertEquals("42803",
                sqlStateOf("SELECT count(*) FROM zz_al1 a WHERE (SELECT sum(a.v)) > 0"));
        assertEquals("42803", sqlStateOf("UPDATE zz_al2 SET w = (SELECT max(w)) WHERE id = 1"));
        assertEquals("42803", sqlStateOf("DELETE FROM zz_al2 WHERE w > (SELECT max(w))"));
    }

    @Test
    void twoAggregatesOfOneLevelMayNotBeWrittenOneInsideTheOther() {
        assertEquals("42803", sqlStateOf("SELECT sum(count(a.v)) FROM zz_al1 a"));
        assertEquals("42803", sqlStateOf("SELECT sum((SELECT count(a.v))) FROM zz_al1 a"));
        assertEquals("42803", sqlStateOf("SELECT percentile_cont((SELECT min(a.v) / 100.0)) "
                + "WITHIN GROUP (ORDER BY a.v) FROM zz_al1 a"));
        // One the sub-select owns is a value by the time the outer aggregate reads it.
        assertNull(sqlStateOf("SELECT sum((SELECT count(b.v) FROM zz_al1 b)) FROM zz_al1 a"));
    }

    @Test
    void aDirectArgumentIsSettledOnceForTheGroup() throws Exception {
        assertEquals("42803", sqlStateOf("SELECT percentile_cont(a.v / 100.0) "
                + "WITHIN GROUP (ORDER BY a.v) FROM zz_al1 a"));
        assertEquals(List.of("1|15", "2|70"),
                rows("SELECT a.g, percentile_cont(0.5) WITHIN GROUP (ORDER BY a.v) "
                        + "FROM zz_al1 a GROUP BY a.g ORDER BY a.g"));
    }

    @Test
    void aColumnTheOuterQueryDidNotGroupByIsStillUngrouped() {
        assertEquals("42803",
                sqlStateOf("SELECT (SELECT max(a.v) FROM zz_al1 b WHERE b.id = a.id) "
                        + "FROM zz_al1 a"));
        // And a relation an inner FROM shadows is that FROM's, so the outer one is out of reach.
        assertEquals("42703",
                sqlStateOf("SELECT (SELECT sum(a.v) FROM zz_al2 a) FROM zz_al1 a"));
    }
}
