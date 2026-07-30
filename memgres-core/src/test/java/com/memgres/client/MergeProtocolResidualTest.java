package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Statements that said nothing, and statements that reported work they had not done.
 *
 * <p>A MERGE could be written with a WHEN clause that no row could ever reach, with an INSERT arm
 * naming a column twice, or with WITH RECURSIVE, which MERGE does not take. Its NOT MATCHED BY
 * SOURCE arm read "no arm has acted on this row yet" rather than "no source row paired with it",
 * so it rewrote rows the ON clause had matched. A statement a DO INSTEAD rule replaced reported
 * the row count of the statement that never ran. A driver asking what a prepared statement's
 * parameters are was told text whatever they were compared to. A COPY whose CSV ended inside a
 * quoted field stored the unterminated remainder as a value. A PL/pgSQL cursor query written over
 * its own parameters lost their names; CLOSE on a cursor nobody opened did nothing at all; a row
 * constructor returned as a composite kept text in fields declared numeric; and a body that did
 * not parse came back as an internal error rather than a syntax error.
 *
 * <p>Every expectation here was measured against PostgreSQL 18, and each rule is accompanied by
 * the ordinary shapes around it, which must keep working.
 */
class MergeProtocolResidualTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.setQueryTimeout(10); st.execute(sql); }
    }

    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.setQueryTimeout(10); return st.executeUpdate(sql); }
    }

    /** Every row of the query, one string per row, sorted so the order of scan cannot matter. */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
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
                Collections.sort(out);
                return out;
            }
        }
    }

    private static void rejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    private static void resetMergeTables() throws SQLException {
        exec("DROP TABLE IF EXISTS mpr_mt CASCADE");
        exec("DROP TABLE IF EXISTS mpr_ms CASCADE");
        exec("CREATE TABLE mpr_mt (id int PRIMARY KEY, v int, w int)");
        exec("CREATE TABLE mpr_ms (id int PRIMARY KEY, v int)");
        exec("INSERT INTO mpr_mt VALUES (1,1,1),(2,2,2),(3,3,3)");
        exec("INSERT INTO mpr_ms VALUES (2,20),(4,40)");
    }

    // =========================================================================
    // MERGE: a WHEN clause that no row can reach
    // =========================================================================

    @Test
    void aWhenClauseAfterAnUnconditionalOneOfItsOwnKindIsUnreachable() throws Exception {
        resetMergeTables();
        String unreachable = "unreachable WHEN clause specified after unconditional WHEN clause";
        rejected("42601", unreachable, "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = s.v WHEN MATCHED AND t.v > 0 THEN UPDATE SET v = 1");
        rejected("42601", unreachable, "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = 1 WHEN MATCHED THEN UPDATE SET v = 2");
        rejected("42601", unreachable, "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v) "
                + "WHEN NOT MATCHED AND s.v > 0 THEN INSERT (id,v) VALUES (s.id,0)");
        rejected("42601", unreachable, "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY TARGET THEN INSERT (id,v) VALUES (s.id,1) "
                + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,2)");
        rejected("42601", unreachable, "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0 "
                + "WHEN NOT MATCHED BY SOURCE AND t.v > 0 THEN DELETE");
        // Nothing ran: the check is made before any row is read.
        assertEquals(List.of("1|1|1", "2|2|2", "3|3|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    @Test
    void theThreeKindsOfWhenClauseAreIndependentOfEachOther() throws Exception {
        resetMergeTables();
        assertEquals(4, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = s.v "
                + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id, s.v) "
                + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0"));
        assertEquals(List.of("1|0|1", "2|20|2", "3|0|3", "4|40|null"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    @Test
    void aConditionalClauseMayStillPrecedeAnUnconditionalOne() throws Exception {
        resetMergeTables();
        assertEquals(1, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN MATCHED AND t.v > 100 THEN UPDATE SET v = 1 "
                + "WHEN MATCHED THEN UPDATE SET v = 7"));
        assertEquals(List.of("1|1|1", "2|7|2", "3|3|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    // =========================================================================
    // MERGE: an INSERT arm names each column once
    // =========================================================================

    @Test
    void aMergeInsertArmMayNotNameAColumnTwice() throws Exception {
        resetMergeTables();
        rejected("42701", "column \"id\" specified more than once",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED THEN INSERT (id, id) VALUES (s.id, s.v)");
        rejected("42701", "column \"v\" specified more than once",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED THEN INSERT (id, v, v) VALUES (s.id, s.v, 1)");
        // Distinct columns, and no column list at all, are unaffected.
        assertEquals(1, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED THEN INSERT (id, v, w) VALUES (s.id, s.v, s.v)"));
        assertEquals(List.of("1|1|1", "2|2|2", "3|3|3", "4|40|40"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    @Test
    void anInsertOfItsOwnMayNotNameAColumnTwiceEither() throws Exception {
        resetMergeTables();
        rejected("42701", "column \"id\" specified more than once",
                "INSERT INTO mpr_mt (id, id) VALUES (9, 9)");
        exec("INSERT INTO mpr_mt (id, v, w) VALUES (9, 9, 9)");
        assertEquals(List.of("9|9|9"), rows("SELECT id,v,w FROM mpr_mt WHERE id = 9"));
    }

    // =========================================================================
    // MERGE: WITH RECURSIVE
    // =========================================================================

    @Test
    void mergeDoesNotTakeWithRecursive() throws Exception {
        resetMergeTables();
        String refused = "WITH RECURSIVE is not supported for MERGE statement";
        rejected("42601", refused, "WITH RECURSIVE c(n) AS (SELECT 1) "
                + "MERGE INTO mpr_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 7");
        rejected("42601", refused,
                "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 3) "
                        + "MERGE INTO mpr_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 7");
        // The same statement without RECURSIVE runs.
        assertEquals(1, update("WITH c(n) AS (SELECT 1) "
                + "MERGE INTO mpr_mt t USING c ON t.id = c.n WHEN MATCHED THEN UPDATE SET v = 5"));
        assertEquals(List.of("1|5|1", "2|2|2", "3|3|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    // =========================================================================
    // MERGE: each arm sees only the relation it has a row from
    // =========================================================================

    @Test
    void anArmThatNamesTheRelationItHasNoRowFromIsRefused() throws Exception {
        resetMergeTables();
        rejected("42P01", "invalid reference to FROM-clause entry for table \"s\"",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = s.v");
        rejected("42P01", "invalid reference to FROM-clause entry for table \"s\"",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED BY SOURCE AND s.v > 0 THEN DELETE");
        rejected("42P01", "invalid reference to FROM-clause entry for table \"t\"",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id, t.v)");
        rejected("42P01", "invalid reference to FROM-clause entry for table \"t\"",
                "MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                        + "WHEN NOT MATCHED AND t.v > 0 THEN INSERT (id,v) VALUES (s.id, s.v)");
    }

    @Test
    void aSubqueryInAnArmBringsItsOwnFromList() throws Exception {
        resetMergeTables();
        assertEquals(2, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = (SELECT max(s.v) FROM mpr_ms s)"));
        assertEquals(List.of("1|40|1", "2|2|2", "3|40|3"), rows("SELECT id,v,w FROM mpr_mt"));
        // The ON clause and a MATCHED arm may name both relations.
        assertEquals(1, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET v = t.v + s.v"));
        assertEquals(List.of("1|40|1", "2|22|2", "3|40|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    // =========================================================================
    // MERGE: NOT MATCHED BY SOURCE is about the ON clause
    // =========================================================================

    @Test
    void notMatchedBySourceSkipsEveryRowSomeSourceRowPairedWith() throws Exception {
        resetMergeTables();
        assertEquals(2, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 99"));
        assertEquals(List.of("1|99|1", "2|2|2", "3|99|3"), rows("SELECT id,v,w FROM mpr_mt"));

        resetMergeTables();
        assertEquals(2, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY SOURCE THEN DELETE"));
        assertEquals(List.of("2|2|2"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    @Test
    void aDoNothingArmCountsNothing() throws Exception {
        resetMergeTables();
        assertEquals(0, update("MERGE INTO mpr_mt t USING mpr_ms s ON t.id = s.id "
                + "WHEN NOT MATCHED BY SOURCE THEN DO NOTHING"));
        assertEquals(List.of("1|1|1", "2|2|2", "3|3|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    @Test
    void everyTargetRowIsUnmatchedWhenTheOnConditionIsNeverTrue() throws Exception {
        resetMergeTables();
        assertEquals(3, update("MERGE INTO mpr_mt t USING mpr_ms s ON false "
                + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 5"));
        assertEquals(List.of("1|5|1", "2|5|2", "3|5|3"), rows("SELECT id,v,w FROM mpr_mt"));
    }

    // =========================================================================
    // Rules: a statement a DO INSTEAD rule replaces
    // =========================================================================

    @Test
    void aStatementReplacedByARuleReportsWhatTheRuleDid() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_t173 CASCADE");
        exec("DROP TABLE IF EXISTS mpr_log173 CASCADE");
        exec("CREATE TABLE mpr_t173 (i int PRIMARY KEY)");
        exec("CREATE TABLE mpr_log173 (m text)");
        exec("INSERT INTO mpr_t173 VALUES (1),(2)");
        exec("CREATE RULE mpr_r173 AS ON DELETE TO mpr_t173 DO INSTEAD "
                + "( INSERT INTO mpr_log173 VALUES ('d1'); INSERT INTO mpr_log173 VALUES ('d2'); )");

        // The DELETE deleted nothing, so it reports nothing, but both actions ran.
        assertEquals(0, update("DELETE FROM mpr_t173 WHERE i = 1"));
        assertEquals(List.of("1", "2"), rows("SELECT i FROM mpr_t173"));
        assertEquals(List.of("d1", "d2"), rows("SELECT m FROM mpr_log173"));
    }

    @Test
    void aSingleActionInsteadRuleReportsNothingForADeleteEither() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_u173 CASCADE");
        exec("DROP TABLE IF EXISTS mpr_ulog173 CASCADE");
        exec("CREATE TABLE mpr_u173 (i int PRIMARY KEY, v int)");
        exec("CREATE TABLE mpr_ulog173 (m text)");
        exec("INSERT INTO mpr_u173 VALUES (1,1),(2,2)");
        exec("CREATE RULE mpr_ru173 AS ON DELETE TO mpr_u173 DO INSTEAD "
                + "INSERT INTO mpr_ulog173 VALUES ('x')");
        exec("CREATE RULE mpr_rw173 AS ON UPDATE TO mpr_u173 DO INSTEAD "
                + "INSERT INTO mpr_ulog173 VALUES ('u')");

        assertEquals(0, update("DELETE FROM mpr_u173 WHERE i = 1"));
        assertEquals(0, update("UPDATE mpr_u173 SET v = 9 WHERE i = 1"));
        assertEquals(List.of("1|1", "2|2"), rows("SELECT i,v FROM mpr_u173"));
        assertEquals(List.of("u", "x"), rows("SELECT m FROM mpr_ulog173"));
    }

    @Test
    void anInsertReplacedByAnInsertReportsThatInsertsRows() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_v173 CASCADE");
        exec("DROP TABLE IF EXISTS mpr_vlog173 CASCADE");
        exec("CREATE TABLE mpr_v173 (i int PRIMARY KEY)");
        exec("CREATE TABLE mpr_vlog173 (m text)");
        exec("CREATE RULE mpr_rv173 AS ON INSERT TO mpr_v173 DO INSTEAD "
                + "INSERT INTO mpr_vlog173 VALUES ('i')");

        assertEquals(1, update("INSERT INTO mpr_v173 VALUES (1)"));
        assertEquals(List.of(), rows("SELECT i FROM mpr_v173"));
        assertEquals(List.of("i"), rows("SELECT m FROM mpr_vlog173"));
    }

    @Test
    void aStatementWithNoRuleStillReportsItsOwnRows() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_z173 CASCADE");
        exec("CREATE TABLE mpr_z173 (i int PRIMARY KEY)");
        exec("INSERT INTO mpr_z173 VALUES (1),(2),(3)");
        assertEquals(2, update("DELETE FROM mpr_z173 WHERE i <= 2"));
        assertEquals(List.of("3"), rows("SELECT i FROM mpr_z173"));
    }

    // =========================================================================
    // Protocol: what a driver is told a statement's parameters are
    // =========================================================================

    @Test
    void aParameterIsDescribedAsWhatTheStatementComparesItTo() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_pm CASCADE");
        exec("CREATE TABLE mpr_pm (n int, s text, d numeric(10,2), t timestamp, b boolean, "
                + "u uuid, bi bigint, r real, sm smallint, dt date, ba bytea, vc varchar(10))");

        assertParams("SELECT * FROM mpr_pm WHERE n = ?", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE d = ?", "numeric:2");
        assertParams("SELECT * FROM mpr_pm WHERE t = ?", "timestamp:93");
        assertParams("SELECT * FROM mpr_pm WHERE b = ?", "bool:-7");
        assertParams("SELECT * FROM mpr_pm WHERE u = ?", "uuid:1111");
        assertParams("SELECT * FROM mpr_pm WHERE bi = ?", "int8:-5");
        assertParams("SELECT * FROM mpr_pm WHERE r = ?", "float4:7");
        assertParams("SELECT * FROM mpr_pm WHERE sm = ?", "int2:5");
        assertParams("SELECT * FROM mpr_pm WHERE dt = ?", "date:91");
        assertParams("SELECT * FROM mpr_pm WHERE ba = ?", "bytea:-2");
        assertParams("SELECT * FROM mpr_pm WHERE s = ?", "text:12");
        // A varchar column compares with text's operators, so the parameter is text.
        assertParams("SELECT * FROM mpr_pm WHERE vc = ?", "text:12");

        assertParams("INSERT INTO mpr_pm (n, s) VALUES (?, ?)", "int4:4", "text:12");
        assertParams("UPDATE mpr_pm SET n = ? WHERE s = ?", "int4:4", "text:12");
        assertParams("DELETE FROM mpr_pm WHERE n = ?", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE n = ? AND s = ?", "int4:4", "text:12");
        assertParams("SELECT * FROM mpr_pm WHERE n IN (?, ?)", "int4:4", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE n BETWEEN ? AND ?", "int4:4", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE ? = n", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE NOT (n = ?)", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE n = (SELECT n FROM mpr_pm WHERE bi = ?)", "int8:-5");
        assertParams("WITH c AS (SELECT n FROM mpr_pm WHERE n = ?) SELECT * FROM c", "int4:4");
        assertParams("SELECT * FROM mpr_pm ORDER BY n LIMIT ?", "int8:-5");
        assertParams("SELECT ? + 1", "int4:4");
        assertParams("SELECT * FROM mpr_pm WHERE n = ?::bigint", "int8:-5");

        // Nothing in the statement says what these are, so they stay text — as they were.
        assertParams("SELECT * FROM mpr_pm WHERE lower(s) = ?", "text:12");
        assertParams("SELECT ? || 'x'", "text:12");
        assertParams("SELECT * FROM mpr_pm");
    }

    @Test
    void describingAStatementDoesNotChangeWhatExecutingItDoes() throws Exception {
        exec("DROP TABLE IF EXISTS mpr_rt CASCADE");
        exec("CREATE TABLE mpr_rt (n int PRIMARY KEY, s text, d numeric(10,2), b boolean)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO mpr_rt (n, s, d, b) VALUES (?, ?, ?, ?)")) {
            ps.setQueryTimeout(10);
            assertEquals(4, ps.getParameterMetaData().getParameterCount());
            ps.setInt(1, 7);
            ps.setString(2, "hi");
            ps.setBigDecimal(3, new java.math.BigDecimal("1.25"));
            ps.setBoolean(4, true);
            assertEquals(1, ps.executeUpdate());
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT s FROM mpr_rt WHERE n = ? AND b = ?")) {
            ps.setQueryTimeout(10);
            ps.getParameterMetaData();
            ps.setInt(1, 7);
            ps.setBoolean(2, true);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hi", rs.getString(1));
            }
        }
    }

    /** Assert the driver reports these type names and JDBC types, in order, for the statement. */
    private static void assertParams(String sql, String... expected) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setQueryTimeout(10);
            ParameterMetaData md = ps.getParameterMetaData();
            assertEquals(expected.length, md.getParameterCount(), "parameter count for: " + sql);
            for (int i = 1; i <= expected.length; i++) {
                assertEquals(expected[i - 1], md.getParameterTypeName(i) + ":" + md.getParameterType(i),
                        "parameter " + i + " of: " + sql);
            }
        }
    }

    // =========================================================================
    // Protocol: COPY of CSV that ends inside a quoted field
    // =========================================================================

    @Test
    void aCsvThatEndsInsideAQuotedFieldIsRefused() throws Exception {
        assertCopyRefused("1,\"abc,1\n");
        assertCopyRefused("1,\"abc\n");
        assertCopyRefused("1,\"abc");
        assertCopyRefused("n,t\n1,\"abc\n", "WITH (FORMAT csv, HEADER)");
        assertCopyRefused("1,~abc\n", "WITH (FORMAT csv, QUOTE '~')");
    }

    @Test
    void wellFormedCsvIsStillCopied() throws Exception {
        assertEquals(List.of("1|ab\"c"), copyRows("1,\"ab\"\"c\"\n"));
        assertEquals(List.of("1|line1\nline2"), copyRows("1,\"line1\nline2\"\n"));
        assertEquals(List.of("1|abc"), copyRows("1,abc\n"));
        assertEquals(List.of("1|"), copyRows("1,\"\"\n"));
        assertEquals(List.of("1|abc"), copyRows("1,~abc~\n", "WITH (FORMAT csv, QUOTE '~')"));
        assertEquals(List.of("1|ab\"c"), copyRows("1\tab\"c\n", "WITH (FORMAT text)"));
        assertEquals(List.of(), copyRows(""));
    }

    private static void assertCopyRefused(String payload) throws Exception {
        assertCopyRefused(payload, "WITH (FORMAT csv)");
    }

    private static void assertCopyRefused(String payload, String options) throws Exception {
        freshCopyTable();
        SQLException e = assertThrows(SQLException.class, () -> copyIn(payload, options));
        assertEquals("22P04", e.getSQLState(), e.getMessage());
        assertTrue(e.getMessage().contains("unterminated CSV quoted field"), e.getMessage());
        // Nothing of the malformed input was stored.
        assertEquals(List.of(), rows("SELECT n, t FROM mpr_cq"));
    }

    private static List<String> copyRows(String payload) throws Exception {
        return copyRows(payload, "WITH (FORMAT csv)");
    }

    private static List<String> copyRows(String payload, String options) throws Exception {
        freshCopyTable();
        copyIn(payload, options);
        return rows("SELECT n, t FROM mpr_cq");
    }

    private static void freshCopyTable() throws SQLException {
        exec("DROP TABLE IF EXISTS mpr_cq CASCADE");
        exec("CREATE TABLE mpr_cq (n int, t text)");
    }

    private static void copyIn(String payload, String options) throws Exception {
        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
        cm.copyIn("COPY mpr_cq FROM STDIN " + options,
                new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
    }

    // =========================================================================
    // PL/pgSQL: cursors
    // =========================================================================

    @Test
    void aCursorQueryKeepsTheNamesItIsWrittenWith() throws Exception {
        exec("DO $$\n"
                + "declare c1 cursor (param1 int, param2 int) for select param1, param2;\n"
                + "  r record;\n"
                + "begin\n"
                + "  open c1 (param1 := 20, param2 := 21);\n"
                + "  fetch c1 into r;\n"
                + "  assert r.param1 = 20 and r.param2 = 21;\n"
                + "  close c1;\n"
                + "end$$");
        exec("DO $$\n"
                + "declare c1 cursor (param1 int, param2 int) for select param1, param2;\n"
                + "  r record;\n"
                + "begin\n"
                + "  open c1 (20, 21);\n"
                + "  fetch c1 into r;\n"
                + "  assert r.param1 = 20 and r.param2 = 21;\n"
                + "  close c1;\n"
                + "end$$");
    }

    @Test
    void aCursorParameterMayBeAliasedToItsOwnName() throws Exception {
        exec("DO $$\n"
                + "declare c1 cursor (param1 int) for select param1 AS param1;\n"
                + "  r record;\n"
                + "begin\n"
                + "  open c1 (param1 := 20);\n"
                + "  fetch c1 into r;\n"
                + "  assert r.param1 = 20;\n"
                + "  close c1;\n"
                + "end$$");
        // and to a different one, and used in a WHERE clause, as before.
        exec("DO $$\n"
                + "declare c1 cursor (param1 int, param2 int) for select param1 + param2 AS s;\n"
                + "  r record;\n"
                + "begin\n"
                + "  open c1 (param1 := 20, param2 := 21);\n"
                + "  fetch c1 into r;\n"
                + "  assert r.s = 41;\n"
                + "  close c1;\n"
                + "end$$");
        exec("DROP TABLE IF EXISTS mpr_cur CASCADE");
        exec("CREATE TABLE mpr_cur (a int PRIMARY KEY, b text)");
        exec("INSERT INTO mpr_cur VALUES (1,'x')");
        exec("DO $$\n"
                + "declare c1 cursor (p int) for select a, b from mpr_cur where a = p;\n"
                + "  r record;\n"
                + "begin\n"
                + "  open c1 (p := 1);\n"
                + "  fetch c1 into r;\n"
                + "  assert r.a = 1 and r.b = 'x';\n"
                + "  close c1;\n"
                + "end$$");
    }

    @Test
    void closingACursorThatWasNeverOpenedIsAnError() {
        rejected("22004", "cursor variable \"c\" is null",
                "DO $$ declare c cursor for select 1; begin close c; end $$");
        rejected("22004", "cursor variable \"c\" is null",
                "DO $$ declare c refcursor; begin close c; end $$");
    }

    @Test
    void openingFirstIsWhatMakesCloseMeanSomething() throws Exception {
        exec("DO $$ declare c cursor for select 1; v int; "
                + "begin open c; fetch c into v; close c; assert v = 1; end $$");
        exec("DO $$ declare c refcursor; v int; "
                + "begin open c for select 7; fetch c into v; close c; assert v = 7; end $$");
        // A second CLOSE names a portal that has gone, which is a different complaint.
        rejected("34000", "does not exist",
                "DO $$ declare c cursor for select 1; begin open c; close c; close c; end $$");
    }

    // =========================================================================
    // PL/pgSQL: a row constructor returned as a composite
    // =========================================================================

    @Test
    void aReturnedRowMustHoldTheDeclaredTypesKindOfValue() throws Exception {
        exec("DROP TYPE IF EXISTS mpr_two_int8s CASCADE");
        exec("DROP TYPE IF EXISTS mpr_two_texts CASCADE");
        exec("CREATE TYPE mpr_two_int8s AS (q1 bigint, q2 bigint)");
        exec("CREATE TYPE mpr_two_texts AS (t1 text, t2 text)");
        exec("CREATE FUNCTION mpr_retc3(x int) RETURNS mpr_two_int8s AS "
                + "$body$ begin return row(x::text, x::text); end $body$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION mpr_retc4(x int) RETURNS mpr_two_int8s AS "
                + "$body$ begin return row('abc', 'def'); end $body$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION mpr_rettn() RETURNS mpr_two_texts AS "
                + "$body$ begin return row(1, 2); end $body$ LANGUAGE plpgsql");

        String mismatch = "returned record type does not match expected record type";
        rejected("42804", mismatch, "SELECT (mpr_retc3(42)).q1");
        rejected("42804", mismatch, "SELECT mpr_retc3(42)::text");
        rejected("42804", mismatch, "SELECT mpr_retc4(42)::text");
        rejected("42804", mismatch, "SELECT mpr_rettn()::text");
    }

    @Test
    void aReturnedRowOfTheDeclaredKindIsStillAccepted() throws Exception {
        exec("DROP TYPE IF EXISTS mpr_ok_int8s CASCADE");
        exec("DROP TYPE IF EXISTS mpr_ok_texts CASCADE");
        exec("CREATE TYPE mpr_ok_int8s AS (q1 bigint, q2 bigint)");
        exec("CREATE TYPE mpr_ok_texts AS (t1 text, t2 text)");
        exec("CREATE FUNCTION mpr_retbig() RETURNS mpr_ok_int8s AS "
                + "$body$ begin return row(1::bigint, 2::bigint); end $body$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION mpr_rettt() RETURNS mpr_ok_texts AS "
                + "$body$ begin return row('a'::text, 'b'::text); end $body$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION mpr_retvar(x int) RETURNS mpr_ok_int8s AS "
                + "$body$ declare r mpr_ok_int8s; begin r.q1 := '42'; r.q2 := x; return r; end $body$ "
                + "LANGUAGE plpgsql");

        assertEquals(List.of("2"), rows("SELECT (mpr_retbig()).q2"));
        assertEquals(List.of("a"), rows("SELECT (mpr_rettt()).t1"));
        assertEquals(List.of("42"), rows("SELECT (mpr_retvar(3)).q1"));
    }

    // =========================================================================
    // A column definition list that does not describe the result
    // =========================================================================

    @Test
    void aColumnDefinitionListThatDoesNotDescribeTheResultIsRefused() throws Exception {
        exec("DROP FUNCTION IF EXISTS mpr_srf()");
        exec("CREATE FUNCTION mpr_srf() RETURNS SETOF record AS "
                + "$body$ begin return query select 1, 2; end $body$ LANGUAGE plpgsql");

        String mismatch = "structure of query does not match function result type";
        rejected("42804", mismatch, "SELECT * FROM mpr_srf() AS t(x int, y int, z int)");
        rejected("42804", mismatch, "SELECT * FROM mpr_srf() AS t(x int)");
        rejected("42804", mismatch, "SELECT * FROM mpr_srf() AS t(x text, y text)");
        assertEquals(List.of("1|2"), rows("SELECT * FROM mpr_srf() AS t(x int, y int)"));
    }

    // =========================================================================
    // A PL/pgSQL body that does not parse
    // =========================================================================

    @Test
    void aBodyThatDoesNotParseIsASyntaxErrorNotAnInternalOne() {
        rejected("42601", "syntax error at or near \"42\"",
                "CREATE FUNCTION mpr_bad1() RETURNS int AS "
                        + "$body$ declare 42 int; begin return 1; end $body$ LANGUAGE plpgsql");
        rejected("42601", "syntax error at or near \"42\"",
                "DO $$ declare 42 int; begin null; end $$");
        rejected("42601", "syntax error at or near \"1\"",
                "CREATE FUNCTION mpr_bad3() RETURNS int AS "
                        + "$body$ declare x int; return 1; end $body$ LANGUAGE plpgsql");
    }
}
