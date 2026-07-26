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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A set-returning function belongs in FROM or at the top of a select-list expression, and an
 * aggregate's result type follows its input. Expectations captured from a live PostgreSQL 18.0
 * server.
 *
 * <p>N12 generate_series numeric and step direction, N34 aggregate/operator result types,
 * N49 SRF placement, N50 WITH ORDINALITY aliases, N51 grouping validation, N52 DESC NULL
 * placement, N54 ordered-set and JSON column labels.
 */
class SrfAndAggregateSemanticsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE sag_t (id int PRIMARY KEY, x float8, n numeric, k int, b text)");
        exec("INSERT INTO sag_t VALUES (1, 1.5, 1.5, 3, 'p'), (2, 2.5, 2.5, 1, 'q'), (3, 3.5, 3.5, 2, 'r')");
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

    private static List<String> columnNames(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                out.add(rs.getMetaData().getColumnLabel(i));
            }
        }
        return out;
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N12 — generate_series over numeric bounds and signed steps
    // ------------------------------------------------------------------

    @Test
    void generateSeriesKeepsNumericBoundsAndScale() throws Exception {
        assertEquals(Arrays.asList("1.0", "1.25", "1.50", "1.75", "2.00"),
                rows("SELECT g::text FROM generate_series(1.0, 2.0, 0.25) g"));
        assertEquals(Arrays.asList("1.5", "2.5", "3.5"),
                rows("SELECT g::text FROM generate_series(1.5, 3.5) g"));
    }

    @Test
    void generateSeriesRejectsAZeroStep() {
        assertEquals("22023", state("SELECT count(*) FROM generate_series(1, 3, 0)"));
    }

    @Test
    void aNegativeStepOverAnAscendingRangeYieldsNothing() throws Exception {
        assertEquals(Arrays.asList("0"), rows(
                "SELECT count(*) FROM generate_series("
                        + "timestamp '2024-01-01', timestamp '2024-01-05', interval '-1 day')"));
        assertEquals(Arrays.asList("5"), rows(
                "SELECT count(*) FROM generate_series("
                        + "timestamp '2024-01-05', timestamp '2024-01-01', interval '-1 day')"));
    }

    // ------------------------------------------------------------------
    // N34 — result types follow the input type
    // ------------------------------------------------------------------

    @Test
    void aggregatesOverFloat8ProduceFloat8() throws Exception {
        assertEquals(Arrays.asList("double precision|double precision|double precision|double precision"),
                rows("SELECT pg_typeof(avg(x))::text, pg_typeof(sum(x))::text,"
                        + " pg_typeof(stddev(x))::text, pg_typeof(variance(x))::text FROM sag_t"));
    }

    @Test
    void correlationAggregatesAreAlwaysFloat8() throws Exception {
        assertEquals(Arrays.asList("double precision|double precision"),
                rows("SELECT pg_typeof(corr(x, n::float8))::text,"
                        + " pg_typeof(covar_pop(x, n::float8))::text FROM sag_t"));
    }

    @Test
    void powerIsFloat8UnlessAnOperandIsNumeric() throws Exception {
        assertEquals(Arrays.asList("double precision|8"), rows("SELECT pg_typeof(2^3)::text, (2^3)::text"));
        assertEquals(Arrays.asList("1000.0000000000000|numeric"),
                rows("SELECT (10.0^3)::text, pg_typeof(10.0^3)::text"));
    }

    // ------------------------------------------------------------------
    // N49 — a set-returning function may not hide in a conditional or a filter
    // ------------------------------------------------------------------

    @Test
    void setReturningFunctionsAreRejectedOutsideTheSelectListAndFrom() {
        assertEquals("0A000", state("SELECT CASE WHEN true THEN generate_series(1,3) END"));
        assertEquals("0A000", state("SELECT COALESCE(generate_series(1,3), 0)"));
        assertEquals("0A000", state("SELECT 1 FROM sag_t WHERE generate_series(1,3) > 1"));
    }

    // ------------------------------------------------------------------
    // N50 — WITH ORDINALITY alias handling
    // ------------------------------------------------------------------

    @Test
    void withOrdinalityKeepsItsColumnWhateverTheAliasList() throws Exception {
        assertEquals(Arrays.asList("x|1", "y|2"),
                rows("SELECT a, b FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a, b)"));
        assertEquals(Arrays.asList("a", "ordinality"),
                columnNames("SELECT * FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a)"));
        assertEquals(Arrays.asList("x|1", "y|2"),
                rows("SELECT * FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a)"));
    }

    @Test
    void tooManyColumnAliasesAreRejected() {
        assertEquals("42P10", state("SELECT * FROM unnest(ARRAY['x','y']) WITH ORDINALITY AS t(a, b, c)"));
    }

    // ------------------------------------------------------------------
    // N51 — grouping and DISTINCT validation
    // ------------------------------------------------------------------

    @Test
    void havingCannotReferenceAnUngroupedColumn() {
        assertEquals("42803", state("SELECT b FROM sag_t GROUP BY b HAVING k > 1"));
    }

    @Test
    void havingMayReferenceAGroupedColumn() throws Exception {
        assertEquals(Arrays.asList("q"), rows("SELECT b FROM sag_t GROUP BY b HAVING b = 'q'"));
    }

    @Test
    void distinctAggregateSortKeyMustBeAnArgument() {
        assertEquals("42P10", state("SELECT string_agg(DISTINCT b, ',' ORDER BY k) FROM sag_t"));
    }

    @Test
    void distinctAggregateSortKeyThatIsAnArgumentIsAccepted() throws Exception {
        assertEquals(Arrays.asList("p,q,r"), rows("SELECT string_agg(DISTINCT b, ',' ORDER BY b) FROM sag_t"));
    }

    // ------------------------------------------------------------------
    // N52 — DESC ordering places NULLs first
    // ------------------------------------------------------------------

    @Test
    void descendingAggregateOrderPutsNullsFirst() throws Exception {
        assertEquals(Arrays.asList("{NULL,2,1}"), rows(
                "SELECT array_agg(DISTINCT v ORDER BY v DESC)::text FROM (VALUES (1),(2),(NULL::int)) s(v)"));
        assertEquals(Arrays.asList("{1,2,NULL}"), rows(
                "SELECT array_agg(DISTINCT v ORDER BY v)::text FROM (VALUES (1),(2),(NULL::int)) s(v)"));
    }

    // ------------------------------------------------------------------
    // N54 — ordered-set and JSON functions carry their own column label
    // ------------------------------------------------------------------

    @Test
    void orderedSetAndJsonFunctionsAreLabelledByName() throws Exception {
        assertEquals(Arrays.asList("percentile_cont"),
                columnNames("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM sag_t"));
        assertEquals(Arrays.asList("mode"),
                columnNames("SELECT mode() WITHIN GROUP (ORDER BY k) FROM sag_t"));
        assertEquals(Arrays.asList("percentile_disc"),
                columnNames("SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY k) FROM sag_t"));
        assertEquals(Arrays.asList("json_value"),
                columnNames("SELECT json_value('{\"a\": 1}'::jsonb, '$.a' RETURNING int)"));
    }
}
