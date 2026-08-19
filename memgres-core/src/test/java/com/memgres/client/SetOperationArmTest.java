package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An arm of a set operation is run as the query it was written as.
 *
 * <p>A WITH clause written in front of a set operation belongs to the whole operation, so the arm
 * carrying its text is run once more with the items taken off. That second query was written out
 * through a shorter constructor, which dropped whatever the constructor had no parameter for: the
 * arm came back without its GROUPING SETS and without its WINDOW definitions, and the operation
 * answered over a query nobody had written.
 */
class SetOperationArmTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE soa_t (g int, v int)");
            st.execute("INSERT INTO soa_t VALUES (1, 10), (1, 20), (2, 30)");
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

    private static final String WITH_C = "WITH c AS (SELECT g, v FROM soa_t) ";

    /** The empty grouping set is the grand total, and an arm that lost it never answers one. */
    @Test
    void anArmKeepsItsGroupingSets() throws Exception {
        assertEquals(List.of("1|30", "2|30", "9|9", "NULL|60"), rows(WITH_C
                + "SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ())"
                + " UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST"));
    }

    /** ROLLUP and CUBE name the same sets in fewer words. */
    @Test
    void anArmKeepsItsRollupAndCube() throws Exception {
        assertEquals(List.of("1|2", "2|1", "9|9", "NULL|3"), rows(WITH_C
                + "SELECT g, count(*) FROM c GROUP BY ROLLUP(g)"
                + " UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST"));
        assertEquals(List.of("1|30", "2|30", "9|9", "NULL|60"), rows(WITH_C
                + "SELECT g, sum(v) FROM c GROUP BY CUBE(g)"
                + " UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST"));
    }

    /** GROUPING says which set a row came from, so it answers only where the sets survived. */
    @Test
    void anArmKeepsWhatGroupingHasToReportOn() throws Exception {
        assertEquals(List.of("1|0", "2|0", "9|9", "NULL|1"), rows(WITH_C
                + "SELECT g, grouping(g) FROM c GROUP BY GROUPING SETS ((g), ())"
                + " UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST"));
    }

    /** What HAVING said is part of the arm too. */
    @Test
    void anArmKeepsItsHaving() throws Exception {
        assertEquals(List.of("1|2", "9|9"), rows(WITH_C
                + "SELECT g, count(*) FROM c GROUP BY g HAVING count(*) > 1"
                + " UNION ALL SELECT 9, 9 ORDER BY 1"));
        assertEquals(List.of("1|2", "9|9", "NULL|3"), rows(WITH_C
                + "SELECT g, count(*) FROM c GROUP BY CUBE(g) HAVING count(*) > 1"
                + " UNION ALL SELECT 9, 9 ORDER BY 1 NULLS LAST"));
    }

    /** A window the arm defined by name is a window the arm still has. */
    @Test
    void anArmKeepsItsWindowDefinitions() throws Exception {
        assertEquals(List.of("1|30", "1|30", "2|30", "9|9"), rows(WITH_C
                + "SELECT g, sum(v) OVER w FROM c WINDOW w AS (PARTITION BY g)"
                + " UNION ALL SELECT 9, 9 ORDER BY 1, 2"));
        assertEquals(List.of("1|1", "1|2", "2|1", "9|9"), rows(WITH_C
                + "SELECT g, rank() OVER w FROM c WINDOW w AS (PARTITION BY g ORDER BY v)"
                + " UNION ALL SELECT 9, 9 ORDER BY 1, 2"));
    }

    /** DISTINCT is the arm's own, whatever the operation does with duplicates afterwards. */
    @Test
    void anArmKeepsItsDistinct() throws Exception {
        assertEquals(List.of("1", "2", "9"), rows(WITH_C
                + "SELECT DISTINCT g FROM c UNION ALL SELECT 9 ORDER BY 1"));
    }

    /** Every operator the two arms may be joined by reads the left arm the same way. */
    @Test
    void theSameHoldsForIntersectAndExcept() throws Exception {
        assertEquals(List.of("1|30", "2|30", "NULL|60"), rows(WITH_C
                + "SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ())"
                + " INTERSECT SELECT g, sum(v) FROM soa_t GROUP BY GROUPING SETS ((g), ())"
                + " ORDER BY 1 NULLS LAST"));
        assertEquals(List.of("1|30", "2|30", "NULL|60"), rows(WITH_C
                + "SELECT g, sum(v) FROM c GROUP BY GROUPING SETS ((g), ())"
                + " EXCEPT SELECT 1, 99 ORDER BY 1 NULLS LAST"));
    }

    /** The items are in scope on the arm that did not declare them. */
    @Test
    void bothArmsReadTheItemsTheClauseDeclared() throws Exception {
        assertEquals(List.of("1", "1", "2", "2"),
                rows("WITH c AS (SELECT g FROM soa_t WHERE g = 1)"
                        + " SELECT g FROM c UNION ALL SELECT g + 1 FROM c ORDER BY 1"));
    }
}
