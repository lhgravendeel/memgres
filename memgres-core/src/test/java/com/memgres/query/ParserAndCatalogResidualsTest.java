package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A bare VALUES list is a query in its own right, braces open a JSON object inside a jsonb
 * array, and a named window can be refined at the call site. Expectations captured from a live
 * PostgreSQL 18.0 server.
 *
 * <p>N67 bare VALUES arms and jsonb array literals, N66 _pg_expandarray,
 * N48 named-window frames and FILTER.
 */
class ParserAndCatalogResidualsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE pcr_w (id int PRIMARY KEY, g int, v int)");
        exec("INSERT INTO pcr_w VALUES (1,1,10),(2,1,20),(3,2,30)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // N67 — a bare VALUES arm, and jsonb array elements
    // ------------------------------------------------------------------

    @Test
    void aBareValuesListIsAValidSetOperationArm() throws Exception {
        assertEquals(Arrays.asList("2"), rows("SELECT count(*) FROM (SELECT 1 UNION ALL VALUES (2)) s"));
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM (SELECT 1 INTERSECT VALUES (1)) s"));
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM (SELECT 1 EXCEPT VALUES (2)) s"));
    }

    /** A VALUES list stands on either side of the operator. */
    @Test
    void aBareValuesListWorksAsTheLeftArmToo() throws Exception {
        assertEquals(Arrays.asList("2"), rows("SELECT count(*) FROM (VALUES (1) UNION ALL SELECT 2) s"));
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM (VALUES (1),(2) INTERSECT SELECT 1) s"));
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM (VALUES (1),(2) EXCEPT SELECT 1) s"));
    }

    @Test
    void bracesInAJsonbArrayElementOpenAnObject() throws Exception {
        assertEquals(Arrays.asList("{\"k\": 1}"),
                rows("SELECT (ARRAY['{\"k\":1}']::jsonb[])[1]::text"));
    }

    /**
     * An array of text is cast element by element, so each element is read as one integer. The
     * braces are part of the text and are not a nested array: an array of two texts cannot become
     * an array of four integers, whatever those texts happen to spell.
     */
    @Test
    void elementsOfATextArrayAreCastOneAtATime() throws Exception {
        SQLException e = org.junit.jupiter.api.Assertions.assertThrows(SQLException.class,
                () -> rows("SELECT (ARRAY['{1,2}','{3,4}']::int[][])::text"));
        assertEquals("22P02", e.getSQLState());
        assertEquals("ERROR: invalid input syntax for type integer: \"{1,2}\"",
                e.getMessage().split("\n")[0]);
    }

    // ------------------------------------------------------------------
    // N66 — _pg_expandarray, which ORM metadata SQL uses
    // ------------------------------------------------------------------

    @Test
    void expandArrayPairsElementsWithTheirIndex() throws Exception {
        assertEquals(Arrays.asList("3|1", "4|2", "5|3"),
                rows("SELECT x, n FROM information_schema._pg_expandarray(ARRAY[3,4,5]) ORDER BY n"));
    }

    // ------------------------------------------------------------------
    // N48 — a named window refined at the call site
    // ------------------------------------------------------------------

    @Test
    void aNamedWindowMayGainAFrameAtTheCallSite() throws Exception {
        assertEquals(Arrays.asList("1|10", "2|30", "3|30"), rows(
                "SELECT id, sum(v) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                        + " FROM pcr_w WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id"));
    }

    @Test
    void filterSurvivesTheNamedWindowSubstitution() throws Exception {
        assertEquals(Arrays.asList("1|0", "2|1", "3|1"), rows(
                "SELECT id, count(*) FILTER (WHERE v > 10) OVER w"
                        + " FROM pcr_w WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id"));
    }

    @Test
    void aPlainNamedWindowReferenceStillWorks() throws Exception {
        assertEquals(Arrays.asList("1|10", "2|30", "3|30"), rows(
                "SELECT id, sum(v) OVER w FROM pcr_w WINDOW w AS (PARTITION BY g ORDER BY id) ORDER BY id"));
    }
}
