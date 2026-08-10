package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The tree EXPLAIN prints, the options it reads, and the statements it will read at all.
 *
 * <p>EXPLAIN used to answer with one flat line — "Memgres in-memory operation" for anything that
 * was not a plain SELECT, INSERT, UPDATE or DELETE — so a sort above a scan, a CTE beside the query
 * that reads it, and a union of two branches were all the same answer. The structured formats
 * carried that one line as their only key, so a client asking for FORMAT JSON got a document with
 * the wrong shape. The options were matched one keyword at a time, which accepted
 * {@code EXPLAIN ANALYZE (COSTS OFF)} — two grammars at once — and refused {@code COSTS 1} and
 * {@code COSTS 'off'}, which are ordinary boolean spellings. And every statement was explainable,
 * so {@code EXPLAIN (ANALYZE) TRUNCATE t} emptied the table to find out how long that took.
 */
class ExplainPlanTreeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        exec("CREATE TABLE zz_ept (id int, v int, t text)");
        exec("INSERT INTO zz_ept VALUES (1,1,'a'),(2,2,'b'),(3,3,'c')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** The plan as a list of lines, one per row, exactly as EXPLAIN returns them. */
    private static List<String> plan(String sql) throws SQLException {
        List<String> lines = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) lines.add(rs.getString(1));
        }
        return lines;
    }

    private static String one(String sql) throws SQLException {
        List<String> lines = plan(sql);
        assertEquals(1, lines.size(), sql + " -> " + lines);
        return lines.get(0);
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    private static void state(String expected, String sql) {
        assertEquals(expected, refused(sql).getSQLState(), sql);
    }

    // ---- the tree ----

    /** A scan, and the qual it filters on, written out rather than elided. */
    @Test
    void aScanCarriesItsFilter() throws Exception {
        assertEquals("Seq Scan on zz_ept", one("EXPLAIN (COSTS OFF) SELECT * FROM zz_ept"));
        assertIterableEquals(
                java.util.Arrays.asList("Seq Scan on zz_ept", "  Filter: (v = 1)"),
                plan("EXPLAIN (COSTS OFF) SELECT * FROM zz_ept WHERE v = 1"));
    }

    /** A sort sits above the scan it orders, and names the key. */
    @Test
    void aSortSitsAboveItsScan() throws Exception {
        assertIterableEquals(
                java.util.Arrays.asList("Sort", "  Sort Key: v", "  ->  Seq Scan on zz_ept"),
                plan("EXPLAIN (COSTS OFF) SELECT * FROM zz_ept ORDER BY v"));
    }

    /** And a limit above the sort, each child indented one step further. */
    @Test
    void nodesNestOneInsideTheNext() throws Exception {
        assertIterableEquals(
                java.util.Arrays.asList("Limit", "  ->  Sort", "        Sort Key: v",
                        "        ->  Seq Scan on zz_ept"),
                plan("EXPLAIN (COSTS OFF) SELECT * FROM zz_ept ORDER BY v LIMIT 2"));
    }

    /** Grouping names its key; an aggregate with no grouping does not. */
    @Test
    void aggregatesNameWhatTheyGroupBy() throws Exception {
        assertIterableEquals(
                java.util.Arrays.asList("Aggregate", "  ->  Seq Scan on zz_ept"),
                plan("EXPLAIN (COSTS OFF) SELECT count(*) FROM zz_ept"));
        assertIterableEquals(
                java.util.Arrays.asList("HashAggregate", "  Group Key: v", "  ->  Seq Scan on zz_ept"),
                plan("EXPLAIN (COSTS OFF) SELECT v, count(*) FROM zz_ept GROUP BY v"));
        assertIterableEquals(
                java.util.Arrays.asList("HashAggregate", "  Group Key: v", "  Filter: (count(*) > 1)",
                        "  ->  Seq Scan on zz_ept"),
                plan("EXPLAIN (COSTS OFF) SELECT v, count(*) FROM zz_ept GROUP BY v HAVING count(*) > 1"));
    }

    /** A CTE is a sub-plan of its own, labelled and indented beside the query that reads it. */
    @Test
    void aCteHangsBesideTheQueryThatReadsIt() throws Exception {
        assertIterableEquals(
                java.util.Arrays.asList("CTE Scan on c", "  CTE c", "    ->  Result"),
                plan("EXPLAIN (COSTS OFF) WITH c AS MATERIALIZED (SELECT 1 AS a) SELECT * FROM c"));
    }

    /** A union appends its branches; without ALL it sorts and drops the repeats. */
    @Test
    void setOperationsHaveTheirOwnNodes() throws Exception {
        assertIterableEquals(
                java.util.Arrays.asList("Append", "  ->  Result", "  ->  Result"),
                plan("EXPLAIN (COSTS OFF) SELECT 1 UNION ALL SELECT 2"));
        assertIterableEquals(
                java.util.Arrays.asList("Unique", "  ->  Sort", "        Sort Key: (1)",
                        "        ->  Append", "              ->  Result", "              ->  Result"),
                plan("EXPLAIN (COSTS OFF) SELECT 1 UNION SELECT 2"));
        assertEquals("SetOp Except", plan("EXPLAIN (COSTS OFF) SELECT 1 EXCEPT SELECT 2").get(0));
        assertEquals("SetOp Intersect", plan("EXPLAIN (COSTS OFF) SELECT 1 INTERSECT SELECT 2").get(0));
    }

    /** A constant table is one scan, however many rows were written. */
    @Test
    void aValuesListIsOneScan() throws Exception {
        assertEquals("Values Scan on \"*VALUES*\"", one("EXPLAIN (COSTS OFF) VALUES (1),(2)"));
    }

    /** A qual that is false needs no scan at all. */
    @Test
    void aFalseQualCollapsesThePlan() throws Exception {
        assertIterableEquals(java.util.Arrays.asList("Result", "  One-Time Filter: false"),
                plan("EXPLAIN (COSTS OFF) SELECT * FROM zz_ept WHERE false"));
    }

    /** Statements that change rows name the relation they change and scan under it. */
    @Test
    void writesNameTheirTargetAndTheirScan() throws Exception {
        assertIterableEquals(java.util.Arrays.asList("Insert on zz_ept", "  ->  Result"),
                plan("EXPLAIN (COSTS OFF) INSERT INTO zz_ept VALUES (9,9,'z')"));
        assertIterableEquals(java.util.Arrays.asList("Update on zz_ept", "  ->  Seq Scan on zz_ept",
                        "        Filter: (id = 2)"),
                plan("EXPLAIN (COSTS OFF) UPDATE zz_ept SET v = 1 WHERE id = 2"));
        assertIterableEquals(java.util.Arrays.asList("Delete on zz_ept", "  ->  Seq Scan on zz_ept",
                        "        Filter: (id = 3)"),
                plan("EXPLAIN (COSTS OFF) DELETE FROM zz_ept WHERE id = 3"));
    }

    /** VERBOSE names the output, with constant arithmetic already worked out. */
    @Test
    void verboseNamesTheOutput() throws Exception {
        assertIterableEquals(java.util.Arrays.asList("Result", "  Output: 2"),
                plan("EXPLAIN (COSTS OFF, VERBOSE) SELECT 1+1"));
    }

    // ---- the structured formats ----

    /** JSON carries the whole tree under PostgreSQL's key names, not one line under one key. */
    @Test
    void jsonCarriesTheTree() throws Exception {
        String json = one("EXPLAIN (COSTS OFF, FORMAT JSON) SELECT 1 UNION ALL SELECT 2");
        assertTrue(json.startsWith("[\n  {\n    \"Plan\": {"), json);
        assertTrue(json.contains("\"Node Type\": \"Append\""), json);
        assertTrue(json.contains("\"Plans\": ["), json);
        assertTrue(json.contains("\"Parent Relationship\": \"Member\""), json);
        assertTrue(json.contains("\"Node Type\": \"Result\""), json);
        assertTrue(json.contains("\"Parallel Aware\": false"), json);
        assertTrue(json.contains("\"Disabled\": false"), json);
    }

    /** XML writes the same map with hyphens where the key had a space. */
    @Test
    void xmlCarriesTheTree() throws Exception {
        String xml = one("EXPLAIN (COSTS OFF, FORMAT XML) VALUES (1),(2)");
        assertTrue(xml.startsWith("<explain xmlns=\"http://www.postgresql.org/2009/explain\">"), xml);
        assertTrue(xml.contains("<Node-Type>Values Scan</Node-Type>"), xml);
        assertTrue(xml.contains("<Parallel-Aware>false</Parallel-Aware>"), xml);
    }

    /** And YAML the same again. */
    @Test
    void yamlCarriesTheTree() throws Exception {
        String yaml = one("EXPLAIN (COSTS OFF, FORMAT YAML) SELECT 1");
        assertTrue(yaml.startsWith("- Plan: "), yaml);
        assertTrue(yaml.contains("Node Type: \"Result\""), yaml);
        assertTrue(yaml.contains("Disabled: false"), yaml);
    }

    /** Costs are printed only when they were asked for. */
    @Test
    void costsAreOptional() throws Exception {
        assertEquals("Result", one("EXPLAIN (COSTS OFF) SELECT 1"));
        assertTrue(one("EXPLAIN (COSTS ON) SELECT 1").startsWith("Result  (cost="));
    }

    // ---- the options ----

    /** A boolean option takes nothing, 0, 1, or one of the four boolean words, however quoted. */
    @Test
    void booleanOptionsTakeTheSpellingsPostgresReads() throws Exception {
        assertEquals("Result", one("EXPLAIN (COSTS 0) SELECT 1"));
        assertEquals("Result", one("EXPLAIN (COSTS 'off') SELECT 1"));
        assertEquals("Result", one("EXPLAIN (COSTS FALSE) SELECT 1"));
        assertTrue(one("EXPLAIN (COSTS 1) SELECT 1").contains("cost="));
        assertTrue(one("EXPLAIN (COSTS TRUE) SELECT 1").contains("cost="));
        assertIterableEquals(java.util.Arrays.asList("Result", "  Output: 1"),
                plan("EXPLAIN (VERBOSE 1, COSTS OFF) SELECT 1"));
    }

    /** Anything else is a syntax error naming the option that could not read it. */
    @Test
    void anotherSpellingIsASyntaxError() {
        for (String sql : new String[]{"EXPLAIN (COSTS yes) SELECT 1", "EXPLAIN (COSTS t) SELECT 1",
                "EXPLAIN (COSTS 2) SELECT 1", "EXPLAIN (COSTS -1) SELECT 1"}) {
            SQLException e = refused(sql);
            assertEquals("42601", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("costs requires a Boolean value"), sql + " -> " + e.getMessage());
        }
        assertTrue(refused("EXPLAIN (VERBOSE bogus) SELECT 1").getMessage()
                .contains("verbose requires a Boolean value"));
    }

    /** An option that does not exist is named as it was written, quotes and case and all. */
    @Test
    void anUnknownOptionIsNamedAsWritten() {
        assertTrue(refused("EXPLAIN (BOGUSOPT) SELECT 1").getMessage()
                .contains("unrecognized EXPLAIN option \"bogusopt\""));
        assertTrue(refused("EXPLAIN (\"COSTS\" OFF) SELECT 1").getMessage()
                .contains("unrecognized EXPLAIN option \"COSTS\""));
        assertTrue(refused("EXPLAIN ('costs' OFF) SELECT 1").getMessage()
                .contains("syntax error at or near \"'costs'\""));
    }

    /** FORMAT and SERIALIZE match their values exactly, so a quoted spelling that differs fails. */
    @Test
    void valuedOptionsMatchExactly() {
        state("42601", "EXPLAIN (COSTS OFF, FORMAT) SELECT 1");
        assertTrue(refused("EXPLAIN (COSTS OFF, FORMAT) SELECT 1").getMessage()
                .contains("format requires a parameter"));
        SQLException bad = refused("EXPLAIN (COSTS OFF, FORMAT BOGUS) SELECT 1");
        assertEquals("22023", bad.getSQLState());
        assertTrue(bad.getMessage().contains("unrecognized value for EXPLAIN option \"format\": \"bogus\""));
        assertTrue(refused("EXPLAIN (COSTS OFF, FORMAT \"JSON\") SELECT 1").getMessage()
                .contains("unrecognized value for EXPLAIN option \"format\": \"JSON\""));
        assertTrue(refused("EXPLAIN (SERIALIZE bogus) SELECT 1").getMessage()
                .contains("unrecognized value for EXPLAIN option \"serialize\": \"bogus\""));
    }

    /** An option list has at least one option in it, and no comma with nothing after it. */
    @Test
    void theOptionListIsWellFormed() {
        state("42601", "EXPLAIN () SELECT 1");
        state("42601", "EXPLAIN (,) SELECT 1");
        state("42601", "EXPLAIN (COSTS OFF,) SELECT 1");
    }

    /** Options that only mean something under ANALYZE say so. */
    @Test
    void someOptionsNeedAnalyze() {
        for (String option : new String[]{"SERIALIZE", "TIMING", "WAL"}) {
            SQLException e = refused("EXPLAIN (" + option + ") SELECT 1");
            assertEquals("22023", e.getSQLState(), option);
            assertTrue(e.getMessage().contains("EXPLAIN option " + option + " requires ANALYZE"),
                    option + " -> " + e.getMessage());
        }
        // SERIALIZE NONE asks for no serialising, so it needs nothing.
        assertDoesNotThrow(() -> exec("EXPLAIN (COSTS OFF, SERIALIZE NONE) SELECT 1"));
    }

    /** A generic plan has no execution to measure, so it cannot be asked for alongside one. */
    @Test
    void genericPlanAndAnalyzeAreExclusive() {
        for (String sql : new String[]{"EXPLAIN (ANALYZE, GENERIC_PLAN) SELECT 1",
                "EXPLAIN (GENERIC_PLAN, ANALYZE) SELECT 1"}) {
            SQLException e = refused(sql);
            assertEquals("22023", e.getSQLState(), sql);
            assertTrue(e.getMessage()
                    .contains("EXPLAIN options ANALYZE and GENERIC_PLAN cannot be used together"), sql);
        }
    }

    /** The old spelling and the parenthesised list are two grammars, not one. */
    @Test
    void theLegacySpellingTakesNoOptionList() {
        state("42601", "EXPLAIN ANALYZE (COSTS OFF) SELECT 1");
        state("42601", "EXPLAIN VERBOSE ANALYZE SELECT 1");
        // ANALYSE is the same word, and a parenthesised query is still a query.
        assertDoesNotThrow(() -> exec("EXPLAIN ANALYSE SELECT 1"));
        assertDoesNotThrow(() -> exec("EXPLAIN ANALYSE VERBOSE SELECT 1"));
        assertDoesNotThrow(() -> exec("EXPLAIN ANALYSE (SELECT 1)"));
        assertDoesNotThrow(() -> exec("EXPLAIN (SELECT 1)"));
    }

    // ---- what may be explained ----

    /** Only a statement with a plan can be explained; the rest fail at the word that begins them. */
    @Test
    void onlyPlannableStatementsAreExplained() {
        for (String sql : new String[]{"EXPLAIN DROP TABLE zz_ept", "EXPLAIN SET work_mem = '4MB'",
                "EXPLAIN CHECKPOINT", "EXPLAIN DO $$ BEGIN NULL; END $$",
                "EXPLAIN GRANT SELECT ON zz_ept TO PUBLIC", "EXPLAIN COPY zz_ept TO STDOUT",
                "EXPLAIN CREATE VIEW zz_v9 AS SELECT 1", "EXPLAIN ALTER TABLE zz_ept ADD COLUMN q int",
                "EXPLAIN EXPLAIN SELECT 1", "EXPLAIN (COSTS OFF) EXPLAIN (COSTS OFF) SELECT 1"}) {
            state("42601", sql);
        }
        // CREATE TABLE is explainable only as CREATE TABLE ... AS, whose column list is names only.
        state("42601", "EXPLAIN CREATE TABLE zz_nope (x int)");
        assertDoesNotThrow(() -> exec("EXPLAIN (COSTS OFF) CREATE TABLE zz_ct9 AS SELECT 1"));
    }

    /** Reading a statement is not running it: nothing it would have done has happened. */
    @Test
    void explainingAStatementDoesNotRunIt() throws Exception {
        exec("EXPLAIN (COSTS OFF) CREATE TABLE zz_ct8 AS SELECT 1");
        assertEquals(0, count("SELECT count(*) FROM pg_tables WHERE tablename = 'zz_ct8'"));
        exec("BEGIN");
        try {
            exec("EXPLAIN (COSTS OFF) DECLARE zz_c8 CURSOR FOR SELECT 1");
            assertEquals(0, count("SELECT count(*) FROM pg_cursors WHERE name = 'zz_c8'"));
        } finally {
            exec("ROLLBACK");
        }
        exec("CREATE TABLE zz_trunc (a int)");
        try {
            exec("INSERT INTO zz_trunc VALUES (1),(2)");
            state("42601", "EXPLAIN (ANALYZE) TRUNCATE zz_trunc");
            assertEquals(2, count("SELECT count(*) FROM zz_trunc"));
        } finally {
            exec("DROP TABLE zz_trunc");
        }
    }

    private static int count(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    /** The statement is analysed before the options are, so a missing relation is reported first. */
    @Test
    void theStatementIsReadBeforeTheOptions() {
        assertEquals("42P01", refused("EXPLAIN (COSTS OFF) SELECT * FROM zz_nosuch").getSQLState());
        assertEquals("42P01", refused("EXPLAIN (BOGUSOPT) SELECT * FROM zz_nosuch").getSQLState());
        assertEquals("42P01", refused("EXPLAIN (WAL) SELECT * FROM zz_nosuch").getSQLState());
        assertEquals("42703", refused("EXPLAIN (COSTS OFF) SELECT nosuchcol FROM zz_ept").getSQLState());
        // A schema-qualified name is reported the way it was written.
        assertTrue(refused("EXPLAIN (COSTS OFF) SELECT * FROM zz_nosch.t").getMessage()
                .contains("relation \"zz_nosch.t\" does not exist"));
    }

    /** The catalogues are relations too, and a plan may be asked for over one. */
    @Test
    void catalogsCanBeExplained() throws Exception {
        assertIterableEquals(java.util.Arrays.asList("Limit", "  ->  Seq Scan on pg_class"),
                plan("EXPLAIN (COSTS OFF) SELECT relname FROM pg_class LIMIT 1"));
    }

    /** SETTINGS names the plan-affecting settings this session changed, and nothing else. */
    @Test
    void settingsNamesOnlyWhatWasChanged() throws Exception {
        assertNull(settingsLine());
        exec("SET enable_seqscan = off");
        try {
            assertEquals("Settings: enable_seqscan = 'off'", settingsLine());
        } finally {
            exec("RESET enable_seqscan");
        }
    }

    private static String settingsLine() throws SQLException {
        for (String line : plan("EXPLAIN (SETTINGS, COSTS OFF) SELECT 1")) {
            if (line != null && line.startsWith("Settings:")) return line;
        }
        return null;
    }

    /** Under ANALYZE the row count is always given; the timings only when TIMING is on. */
    @Test
    void analyzeReportsWhatItMeasured() throws Exception {
        List<String> timed = plan("EXPLAIN (ANALYZE, COSTS OFF) SELECT 1");
        assertTrue(timed.get(0).matches("Result \\(actual time=[0-9.]+\\.\\.[0-9.]+ rows=1\\.00 loops=1\\)"),
                timed.toString());
        assertTrue(timed.get(1).startsWith("Planning Time: "), timed.toString());
        assertTrue(timed.get(2).startsWith("Execution Time: "), timed.toString());

        List<String> untimed = plan("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT 1");
        assertIterableEquals(java.util.Arrays.asList("Result (actual rows=1.00 loops=1)"), untimed);
    }

    /** A CALL takes no clause after its arguments. */
    @Test
    void aCallStatementHasToEnd() {
        state("42601", "CALL zz_nosuchproc() RETURNING 1");
        state("42601", "CALL zz_nosuchproc() 1");
    }
}
