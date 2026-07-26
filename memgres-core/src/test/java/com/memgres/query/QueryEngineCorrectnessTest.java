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
 * Query shapes that returned silently wrong answers. Expectations captured from a live
 * PostgreSQL 18.0 server.
 *
 * <p>N7 large VALUES lists, N10 ordinal ORDER BY with a star target, N11 row-constructor
 * subquery comparisons, N33 whole-row references and row NULL semantics, N35 array
 * ordering, N53 union result typing.
 */
class QueryEngineCorrectnessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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
    // N10 — an ordinal ORDER BY counts the expanded star columns
    // ------------------------------------------------------------------

    @Test
    void ordinalOrderByWorksWithStarTarget() throws Exception {
        exec("CREATE TABLE qo (id int, val text)");
        exec("INSERT INTO qo VALUES (2,'b'),(1,'a'),(3,'c')");

        assertEquals(Arrays.asList("3|c", "2|b", "1|a"), rows("SELECT * FROM qo ORDER BY 1 DESC"));
        assertEquals(Arrays.asList("1|a", "2|b", "3|c"), rows("SELECT * FROM qo ORDER BY 1, 2"));
        assertEquals(Arrays.asList("1|a", "2|b", "3|c"), rows("SELECT * FROM qo ORDER BY 2"));
    }

    // ------------------------------------------------------------------
    // N11 — row constructors compared against a subquery
    // ------------------------------------------------------------------

    @Test
    void rowConstructorAgainstSubqueryMatches() throws Exception {
        exec("CREATE TABLE qpts (xi int, yi int)");
        exec("INSERT INTO qpts VALUES (1,2),(3,4)");

        assertEquals(Arrays.asList("t"), rows("SELECT (1,2) = ANY (SELECT xi, yi FROM qpts)"));
        assertEquals(Arrays.asList("t"), rows("SELECT (1,2) IN (SELECT xi, yi FROM qpts)"));
        assertEquals(Arrays.asList("f"), rows("SELECT (9,9) = ANY (SELECT xi, yi FROM qpts)"));
        assertEquals(Arrays.asList("f"), rows("SELECT (9,9) IN (SELECT xi, yi FROM qpts)"));
    }

    // ------------------------------------------------------------------
    // N7 — a long VALUES list is not nested set operations
    // ------------------------------------------------------------------

    @Test
    void longValuesListDoesNotOverflowTheStack() throws Exception {
        StringBuilder sb = new StringBuilder("SELECT count(*) FROM (VALUES ");
        for (int i = 0; i < 2000; i++) {
            if (i > 0) sb.append(',');
            sb.append("(").append(i).append(")");
        }
        sb.append(") v(x)");

        assertEquals(Arrays.asList("2000"), rows(sb.toString()));
    }

    // ------------------------------------------------------------------
    // N33 — whole-row references and row NULL tests
    // ------------------------------------------------------------------

    @Test
    void wholeRowReferenceSelectsTheRow() throws Exception {
        exec("CREATE TABLE qwr (a int, b text)");
        exec("INSERT INTO qwr VALUES (1,'x'),(2,NULL)");

        assertEquals(Arrays.asList("(1,x)", "(2,)"), rows("SELECT t1 FROM qwr t1 ORDER BY a"));
    }

    /** A row is NOT NULL only when every field is; it is NULL only when all fields are. */
    @Test
    void rowNullTestsFollowFieldSemantics() throws Exception {
        exec("CREATE TABLE qrn (a int, b text)");
        exec("INSERT INTO qrn VALUES (1,'x'),(2,NULL)");

        assertEquals(Arrays.asList("t|f", "f|f"),
                rows("SELECT (t1 IS NOT NULL), (t1 IS NULL) FROM qrn t1 ORDER BY a"));
    }

    // ------------------------------------------------------------------
    // N35 — arrays sort element-wise, not as text
    // ------------------------------------------------------------------

    @Test
    void arrayOrderingIsElementWise() throws Exception {
        exec("CREATE TABLE qar (id int, arr int[])");
        exec("INSERT INTO qar VALUES (1,'{1,2}'),(2,'{1}'),(3,'{1,2,3}')");

        assertEquals(Arrays.asList("2", "1", "3"), rows("SELECT id FROM qar ORDER BY arr"));
        assertEquals(Arrays.asList("3", "1", "2"), rows("SELECT id FROM qar ORDER BY arr DESC"));
    }

    // ------------------------------------------------------------------
    // N53 — a union takes the type of the branch that has one
    // ------------------------------------------------------------------

    @Test
    void unionWithNullBranchTakesTheOtherType() throws Exception {
        assertEquals(Arrays.asList("integer"),
                rows("SELECT pg_typeof(x)::text FROM (SELECT NULL UNION ALL SELECT 1) t(x) LIMIT 1"));
    }
}
