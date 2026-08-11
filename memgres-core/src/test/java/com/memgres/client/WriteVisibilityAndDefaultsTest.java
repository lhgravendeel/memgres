package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Four things a statement has to get right about the values it reads and writes: where a VIRTUAL
 * generated column is worked out, what the DEFAULT keyword resolves to, which row a subquery reads
 * the enclosing query's columns from, and what a statement that had to wait for another session
 * still sees.
 *
 * <p>None held. A derived table, a view body and a WITH item were read without the enclosing
 * query's qualification, so a generation expression ran for rows the statement had already
 * discarded — and the three write paths that bring in a second relation resolved it with no demand
 * at all, so every VIRTUAL column of that relation was worked out whether the statement named it
 * or not. An assignment of DEFAULT stored NULL for an identity column, ignored the column type's
 * domain default, read the base relation's default through a view instead of the view's own, and
 * was refused outright on a relation carrying a rule; written somewhere it does not belong it was
 * accepted as a no-op whenever no row reached the evaluator, and a statement wrong in two ways at
 * once was reported for whichever fault the reading did not reach first. A subquery resolved a name
 * its own FROM clause has not got against the query around it in a WHERE clause but not in a select
 * list. And a blocked UPDATE or DELETE re-read every relation but its target with a fresh snapshot
 * after the wait, while UPDATE ... FROM and DELETE ... USING never waited and never re-judged at all.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class WriteVisibilityAndDefaultsTest {

    static Memgres memgres;
    static Connection conn;

    /** How long a statement started on a session of its own is given before it counts as waiting. */
    private static final long GRACE_MS = 800L;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The number of rows a write reports having touched. */
    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            return st.executeUpdate(sql);
        }
    }

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** The one value the query returns, read as the number it is. */
    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    /** Every value of the first column, in order, joined with a comma. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    /** The label a client reads for the first column of a query's answer. */
    private static String labelOf(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnLabel(1);
        }
    }

    /**
     * Every row a statement answers with, cells joined with "|" and rows with " / ", with NULL
     * spelled out. A write's RETURNING list reads the same way as a query's target list.
     */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            if (!st.execute(sql)) return "";
            try (ResultSet rs = st.getResultSet()) {
                return readRows(rs);
            }
        }
    }

    private static String readRows(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> got = new ArrayList<>();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                String v = rs.getString(i);
                sb.append(v == null ? "NULL" : v);
            }
            got.add(sb.toString());
        }
        return String.join(" / ", got);
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof PSQLException, "expected a server error from: " + sql);
        return ((PSQLException) thrown).getServerErrorMessage();
    }

    /**
     * The primary message of the error a statement raises. PostgreSQL sends severity in its own
     * field, so the message on the wire never carries an "ERROR: " prefix.
     */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    /** The hint the error carries, which PostgreSQL sends in a field of its own. */
    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    /**
     * The character offset the error points at, 1-based. PostgreSQL leaves the field out entirely
     * for an error that has no place in the statement text, and a client reads that as 0.
     */
    private static int positionOf(String sql) {
        return fieldsOf(sql).getPosition();
    }

    private static long copyIn(String sql, String data) throws Exception {
        CopyManager cm = new CopyManager((BaseConnection) conn);
        return cm.copyIn(sql, new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
    }

    // ------------------------------------------------------------ a VIRTUAL generated column is
    // ------------------------------------------------------------ worked out where it is read

    // The generation expression below is 10/a and the relation holds a row with a = 0, so every
    // statement is really asking "was the expression evaluated for that row". PostgreSQL pulls a
    // derived table, a view body and an inlined WITH item up into the query that reads them, so a
    // qualification written above becomes a scan qualification and the discarded row is never
    // reached.

    @Test
    void aQualificationInTheEnclosingQueryReachesTheRelationUnderIt() throws Exception {
        exec("CREATE TABLE wgv_a_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_a_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE VIEW wgv_a_v AS SELECT * FROM wgv_a_g");

        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wgv_a_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT v.g FROM wgv_a_v v WHERE v.a = 5"));
        assertEquals("5|five|2", rows("SELECT * FROM wgv_a_v WHERE a = 5"));
        assertEquals("2", scalar("WITH c AS (SELECT * FROM wgv_a_g) SELECT g FROM c WHERE a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT a,k,g FROM wgv_a_g) s WHERE s.a = 5"));
        // a column the derived table renames still carries the qualification down
        assertEquals("2", scalar("SELECT s.g FROM (SELECT a AS aa, k, g FROM wgv_a_g) s WHERE s.aa = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wgv_a_g) s WHERE s.a = 5 AND s.k = 'five'"));
        assertEquals("2|five", rows("SELECT s.g, s.k FROM (SELECT * FROM wgv_a_g) s WHERE s.a > 1"));
    }

    @Test
    void aQualificationOnAJoinedDerivedRelationReachesIt() throws Exception {
        exec("CREATE TABLE wgv_b_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_b_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE TABLE wgv_b_o (a int, note text)");
        exec("INSERT INTO wgv_b_o VALUES (5,'x'),(0,'y')");

        assertEquals("2", scalar(
                "SELECT s.g FROM (SELECT * FROM wgv_b_g) s JOIN wgv_b_o o ON o.a = s.a WHERE s.a = 5"));
        // an inner join's ON condition qualifies it the same way
        assertEquals("2", scalar(
                "SELECT s.g FROM (SELECT * FROM wgv_b_g) s JOIN wgv_b_o o ON o.a = s.a AND s.a = 5"));
        assertEquals("2", scalar("WITH c AS (SELECT * FROM wgv_b_g)"
                + " SELECT c.g FROM c JOIN wgv_b_o o ON o.a = c.a WHERE c.a = 5"));
    }

    @Test
    void anAggregateOverAQualifiedDerivedRelationReadsOnlyTheRowsItKept() throws Exception {
        exec("CREATE TABLE wgv_c_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_c_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals(2L, num("SELECT max(s.g) FROM (SELECT * FROM wgv_c_g) s WHERE s.a = 5"));
        assertEquals(1L, num("SELECT count(s.g) FROM (SELECT * FROM wgv_c_g) s WHERE s.a = 5"));
        assertEquals(2L, num("SELECT sum(s.g) FROM (SELECT * FROM wgv_c_g) s WHERE s.a = 5 GROUP BY s.k"));
    }

    @Test
    void orderByAndLimitAboveAQualifiedDerivedRelationKeepTheQualification() throws Exception {
        exec("CREATE TABLE wgv_d_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_d_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wgv_d_g) s WHERE s.a = 5 ORDER BY s.g"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wgv_d_g) s WHERE s.a = 5 LIMIT 1"));
    }

    @Test
    void distinctGroupByOrderByAndASetOperationInsideTheDerivedRelationDoNotStopIt() throws Exception {
        exec("CREATE TABLE wgv_e_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_e_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals("2", scalar("SELECT s.g FROM (SELECT DISTINCT a,k,g FROM wgv_e_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT a,k,g FROM wgv_e_g GROUP BY a,k,g) s WHERE s.a = 5"));
        // each arm of the set operation takes the qualification
        assertEquals("2,2", column("SELECT s.g FROM (SELECT * FROM wgv_e_g UNION ALL"
                + " SELECT * FROM wgv_e_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wgv_e_g ORDER BY a) s WHERE s.a = 5"));
    }

    @Test
    void anInsertSelectThroughAQualifiedDerivedRelationStoresTheOneRow() throws Exception {
        exec("CREATE TABLE wgv_f_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_f_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE TABLE wgv_f_t2 (g int)");

        assertEquals(1, update("INSERT INTO wgv_f_t2 SELECT s.g FROM (SELECT * FROM wgv_f_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT g FROM wgv_f_t2"));
    }

    @Test
    void withNoQualificationTheExpressionIsEvaluatedForEveryRow() throws Exception {
        exec("CREATE TABLE wgv_g_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_g_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wgv_g_g) s"));
        assertEquals("division by zero", messageOf("SELECT s.g FROM (SELECT * FROM wgv_g_g) s"));
        // ORDER BY ... LIMIT above the derived relation qualifies nothing
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wgv_g_g) s ORDER BY s.a LIMIT 1"));
    }

    @Test
    void aQualificationNamingTheVirtualColumnIsAScanQualification() throws Exception {
        exec("CREATE TABLE wgv_h_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_h_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE VIEW wgv_h_v AS SELECT * FROM wgv_h_g");
        exec("CREATE TABLE wgv_h_o (a int, note text)");
        exec("INSERT INTO wgv_h_o VALUES (5,'x'),(0,'y')");

        assertEquals("22012", stateOf("SELECT s.k FROM (SELECT * FROM wgv_h_g) s WHERE s.g = 2"));
        assertEquals("22012", stateOf("SELECT v.k FROM wgv_h_v v WHERE v.g = 2"));
        assertEquals("22012", stateOf("SELECT o.note FROM wgv_h_o o WHERE EXISTS"
                + " (SELECT 1 FROM (SELECT * FROM wgv_h_g) s WHERE s.a = o.a AND s.g = 2)"));
        assertEquals("22012", stateOf("SELECT o.note FROM wgv_h_o o,"
                + " LATERAL (SELECT * FROM wgv_h_g x WHERE x.a = o.a) s WHERE s.g = 2"));
    }

    @Test
    void aLimitOrOffsetInsideTheDerivedRelationKeepsTheQualificationAboveIt() throws Exception {
        exec("CREATE TABLE wgv_i_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_i_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wgv_i_g LIMIT 10) s WHERE s.a = 5"));
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wgv_i_g OFFSET 0) s WHERE s.a = 5"));
    }

    @Test
    void aColumnNobodyNamesIsNeverWorkedOut() throws Exception {
        exec("CREATE TABLE wgv_j_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_j_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE VIEW wgv_j_v AS SELECT * FROM wgv_j_g");
        exec("CREATE TABLE wgv_j_o (a int, note text)");
        exec("INSERT INTO wgv_j_o VALUES (5,'x'),(0,'y')");

        assertEquals(2L, num("SELECT count(*) FROM (SELECT * FROM wgv_j_g) s"));
        assertEquals(2L, num("SELECT count(*) FROM wgv_j_v"));
        assertEquals("five,zero", column("SELECT s.k FROM (SELECT * FROM wgv_j_g) s"));
        assertEquals("x", scalar("SELECT o.note FROM wgv_j_o o WHERE o.a IN"
                + " (SELECT s.a FROM (SELECT * FROM wgv_j_g) s WHERE s.a = 5)"));
    }

    @Test
    void mergeUpdateFromAndDeleteUsingReadOnlyTheColumnsTheStatementNames() throws Exception {
        exec("CREATE TABLE wgv_k_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_k_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE TABLE wgv_k_o (a int, note text)");
        exec("INSERT INTO wgv_k_o VALUES (5,'x'),(0,'y')");

        assertEquals(2, update("MERGE INTO wgv_k_o o USING wgv_k_g t ON o.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.k"));
        assertEquals("0|zero / 5|five", rows("SELECT a, note FROM wgv_k_o ORDER BY a"));

        exec("DELETE FROM wgv_k_o");
        exec("INSERT INTO wgv_k_o VALUES (5,'x'),(0,'y')");
        assertEquals(2, update("UPDATE wgv_k_o o SET note = 'u' FROM wgv_k_g t WHERE t.a = o.a"));
        assertEquals("0|u / 5|u", rows("SELECT a, note FROM wgv_k_o ORDER BY a"));

        exec("DELETE FROM wgv_k_o");
        exec("INSERT INTO wgv_k_o VALUES (5,'x'),(0,'y')");
        assertEquals(0, update("DELETE FROM wgv_k_o o USING wgv_k_g t WHERE t.a = o.a AND o.a = 99"));
        assertEquals("0|y / 5|x", rows("SELECT a, note FROM wgv_k_o ORDER BY a"));
    }

    @Test
    void aWritePathThatNamesTheVirtualColumnStillEvaluatesIt() throws Exception {
        exec("CREATE TABLE wgv_l_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wgv_l_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE TABLE wgv_l_o (a int, note text)");
        exec("INSERT INTO wgv_l_o VALUES (5,'x'),(0,'y')");

        assertEquals("22012", stateOf("MERGE INTO wgv_l_o o USING wgv_l_g t ON o.a = t.a AND t.g = 2"
                + " WHEN MATCHED THEN UPDATE SET note = 'm2'"));
        assertEquals("22012", stateOf("DELETE FROM wgv_l_o o USING wgv_l_g t"
                + " WHERE t.a = o.a AND t.g = 2"));
        // neither of them wrote anything
        assertEquals("0|y / 5|x", rows("SELECT a, note FROM wgv_l_o ORDER BY a"));
    }

    // Pushing a qualification down decides only WHICH rows the column is worked out for; it never
    // changes a value. The generation expression below cannot raise, so every row that comes back
    // has to carry the right one.

    @Test
    void pushingAQualificationDownNeverChangesAValue() throws Exception {
        exec("CREATE TABLE wgv_m_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wgv_m_h VALUES (1,'one'),(2,'two'),(3,'three')");

        assertEquals("1|10 / 2|20", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.a = 1 OR s.k = 'two' ORDER BY s.a"));
        assertEquals("2|20 / 3|30", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE NOT (s.a = 1) ORDER BY s.a"));
        assertEquals("1|10 / 3|30", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.a <> 2 ORDER BY s.a"));
        // a qualification holding a function call or a subquery drops no row, and the value stands
        assertEquals("1|10", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.a = abs(-1) ORDER BY s.a"));
        assertEquals("1|10", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.a = (SELECT min(a) FROM wgv_m_h) ORDER BY s.a"));
        assertEquals("1|10 / 3|30", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.a IN (1,3) ORDER BY s.a"));
        assertEquals("2|20 / 3|30", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_m_h) s"
                + " WHERE s.k LIKE 't%' ORDER BY s.a"));
        assertEquals(1L, num("SELECT count(*) FROM (SELECT * FROM wgv_m_h) s WHERE s.g = 20"));
    }

    @Test
    void anOuterJoinsNullPaddedRowHasNoValueToWorkOut() throws Exception {
        exec("CREATE TABLE wgv_n_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wgv_n_h VALUES (1,'one'),(2,'two'),(3,'three')");
        exec("CREATE TABLE wgv_n_p (a int, note text)");
        exec("INSERT INTO wgv_n_p VALUES (1,'p1'),(9,'p9')");

        assertEquals("1|10 / 9|NULL", rows("SELECT p.a, s.g FROM wgv_n_p p"
                + " LEFT JOIN (SELECT * FROM wgv_n_h) s ON p.a = s.a ORDER BY p.a"));
        assertEquals("1|10 / 9|NULL", rows("SELECT p.a, s.g FROM wgv_n_p p"
                + " LEFT JOIN (SELECT * FROM wgv_n_h) s ON p.a = s.a WHERE p.a >= 1 ORDER BY p.a"));
        assertEquals("1|10 / NULL|20 / NULL|30", rows("SELECT p.a, s.g FROM wgv_n_p p"
                + " RIGHT JOIN (SELECT * FROM wgv_n_h) s ON p.a = s.a ORDER BY s.a"));
        assertEquals("1|10", rows("SELECT s.a, s.g FROM (SELECT * FROM wgv_n_h) s, wgv_n_p p"
                + " WHERE s.a = p.a ORDER BY s.a"));
    }

    @Test
    void aWithItemReadTwiceAnswersEachReferenceUnderItsOwnQualification() throws Exception {
        exec("CREATE TABLE wgv_o_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wgv_o_h VALUES (1,'one'),(2,'two'),(3,'three')");
        exec("CREATE VIEW wgv_o_hv AS SELECT * FROM wgv_o_h");

        assertEquals("1|10 / 2|20", rows("WITH c AS (SELECT * FROM wgv_o_h)"
                + " SELECT c.a, c.g FROM c WHERE c.a = 1"
                + " UNION ALL SELECT c.a, c.g FROM c WHERE c.a = 2"));
        assertEquals("1|10 / 2|20 / 3|30", rows("WITH c AS (SELECT * FROM wgv_o_h)"
                + " SELECT c.a, c.g FROM c ORDER BY c.a"));
        assertEquals("2|20", rows("SELECT v.a, v.g FROM wgv_o_hv v WHERE v.a = 2"));
        assertEquals("1|10 / 2|20 / 3|30", rows("SELECT v.a, v.g FROM wgv_o_hv v ORDER BY v.a"));
    }

    @Test
    void aWritePathStillReadsTheValuesOfTheColumnsItNames() throws Exception {
        exec("CREATE TABLE wgv_p_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wgv_p_h VALUES (1,'one'),(2,'two'),(3,'three')");
        exec("CREATE TABLE wgv_p_p (a int, note text)");
        exec("INSERT INTO wgv_p_p VALUES (1,'p1'),(9,'p9')");

        assertEquals(1, update("UPDATE wgv_p_p p SET note = t.k FROM wgv_p_h t WHERE t.a = p.a"));
        assertEquals("1|one / 9|p9", rows("SELECT a, note FROM wgv_p_p ORDER BY a"));
        assertEquals(1, update("MERGE INTO wgv_p_p p USING wgv_p_h t ON p.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("1|10 / 9|p9", rows("SELECT a, note FROM wgv_p_p ORDER BY a"));
    }

    // ------------------------------------------------------------ what the DEFAULT keyword
    // ------------------------------------------------------------ resolves to, and where it may stand

    @Test
    void updatingAnIdentityColumnToDefaultDrawsTheNextSequenceValue() throws Exception {
        exec("CREATE TABLE wdf_id (id int, c int GENERATED ALWAYS AS IDENTITY,"
                + " d int GENERATED BY DEFAULT AS IDENTITY)");
        exec("INSERT INTO wdf_id (id) VALUES (1)");
        assertEquals("1|1|1", rows("SELECT id, c, d FROM wdf_id"));

        // DEFAULT is the one thing that may be assigned to a GENERATED ALWAYS identity
        assertEquals(1, update("UPDATE wdf_id SET c = DEFAULT"));
        assertEquals("1|2|1", rows("SELECT id, c, d FROM wdf_id"));
        assertEquals(1, update("UPDATE wdf_id SET d = DEFAULT"));
        assertEquals("1|2|2", rows("SELECT id, c, d FROM wdf_id"));
        assertEquals(1, update("UPDATE wdf_id SET c = DEFAULT, d = DEFAULT"));
        assertEquals("1|3|3", rows("SELECT id, c, d FROM wdf_id"));
    }

    @Test
    void updatingASerialColumnToDefaultDrawsFromItsSequence() throws Exception {
        exec("CREATE TABLE wdf_ser (id serial, v int)");
        exec("INSERT INTO wdf_ser (v) VALUES (10)");
        assertEquals(1, update("UPDATE wdf_ser SET id = DEFAULT"));
        assertEquals("2|10", rows("SELECT id, v FROM wdf_ser"));
    }

    @Test
    void assigningDefaultFallsBackToTheDomainDefaultOfTheColumnsType() throws Exception {
        exec("CREATE DOMAIN wdf_dm AS int DEFAULT 42");
        exec("CREATE TABLE wdf_dt (id int, v wdf_dm, w wdf_dm DEFAULT 7)");
        exec("INSERT INTO wdf_dt (id,v,w) VALUES (1,1,1)");

        // the column's own default wins; the domain's stands behind it
        assertEquals(1, update("UPDATE wdf_dt SET v = DEFAULT, w = DEFAULT"));
        assertEquals("1|42|7", rows("SELECT id, v, w FROM wdf_dt"));
        exec("INSERT INTO wdf_dt (id) VALUES (2)");
        assertEquals("1|42|7 / 2|42|7", rows("SELECT id, v, w FROM wdf_dt ORDER BY id"));
    }

    @Test
    void copyFillsInADomainDefaultForAColumnItWasNotGiven() throws Exception {
        exec("CREATE DOMAIN wdf_cd AS int DEFAULT 42");
        exec("CREATE TABLE wdf_cp (id int, v wdf_cd, w wdf_cd DEFAULT 7)");

        assertEquals(2L, copyIn("COPY wdf_cp (id) FROM STDIN", "1\n2\n"));
        assertEquals("1|42|7 / 2|42|7", rows("SELECT id, v, w FROM wdf_cp ORDER BY id"));
    }

    @Test
    void theMergeInsertArmAndAnInsertRuleReadTheDomainDefaultToo() throws Exception {
        exec("CREATE DOMAIN wdf_dm2 AS int DEFAULT 42");
        exec("CREATE TABLE wdf_mg (id int PRIMARY KEY, v wdf_dm2)");
        exec("CREATE TABLE wdf_ms (id int)");
        exec("INSERT INTO wdf_ms VALUES (1),(2)");

        assertEquals(2, update("MERGE INTO wdf_mg t USING wdf_ms s ON t.id = s.id"
                + " WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, DEFAULT)"));
        assertEquals("1|42 / 2|42", rows("SELECT id, v FROM wdf_mg ORDER BY id"));

        exec("CREATE TABLE wdf_il (v int)");
        exec("CREATE TABLE wdf_it (id int, v wdf_dm2)");
        exec("CREATE RULE wdf_ir AS ON INSERT TO wdf_it DO ALSO INSERT INTO wdf_il VALUES (NEW.v)");
        exec("INSERT INTO wdf_it (id) VALUES (1)");
        exec("INSERT INTO wdf_it (id, v) VALUES (2, DEFAULT)");
        assertEquals("1|42 / 2|42", rows("SELECT id, v FROM wdf_it ORDER BY id"));
        assertEquals("42,42", column("SELECT v FROM wdf_il ORDER BY v"));
    }

    @Test
    void onConflictDoUpdateSettingAColumnToDefaultIsAnUpdate() throws Exception {
        exec("CREATE DOMAIN wdf_dm3 AS int DEFAULT 42");
        exec("CREATE TABLE wdf_oc (id int PRIMARY KEY, v wdf_dm3,"
                + " c int GENERATED BY DEFAULT AS IDENTITY)");
        exec("INSERT INTO wdf_oc (id, v) VALUES (1, 5)");
        assertEquals("1|5|1", rows("SELECT id, v, c FROM wdf_oc"));

        assertEquals(1, update("INSERT INTO wdf_oc (id, v) VALUES (1, 9)"
                + " ON CONFLICT (id) DO UPDATE SET v = DEFAULT"));
        assertEquals("1|42|1", rows("SELECT id, v, c FROM wdf_oc"));

        // c reaches 4: each conflicting attempt drew an identity value of its own before the
        // conflict was found, and the assignment then drew a third
        assertEquals(1, update("INSERT INTO wdf_oc (id, v) VALUES (1, 9)"
                + " ON CONFLICT (id) DO UPDATE SET c = DEFAULT"));
        assertEquals("1|42|4", rows("SELECT id, v, c FROM wdf_oc"));
    }

    @Test
    void assigningDefaultThroughAViewAsksTheViewAndNotTheBaseRelation() throws Exception {
        exec("CREATE TABLE wdf_vt (id int, a text DEFAULT 'BASE')");
        exec("INSERT INTO wdf_vt VALUES (1,'o')");
        exec("CREATE VIEW wdf_vv AS SELECT * FROM wdf_vt");

        // the view declares no default of its own, so the assignment writes NULL: the base
        // relation's default is not a fallback for an UPDATE
        assertEquals(1, update("UPDATE wdf_vv SET a = DEFAULT"));
        assertEquals("1|NULL", rows("SELECT id, a FROM wdf_vt"));

        exec("ALTER VIEW wdf_vv ALTER COLUMN a SET DEFAULT 'VIEWD'");
        assertEquals(1, update("UPDATE wdf_vv SET a = DEFAULT"));
        assertEquals("1|VIEWD", rows("SELECT id, a FROM wdf_vt"));

        exec("INSERT INTO wdf_vv (id,a) VALUES (2,DEFAULT)");
        assertEquals("1|VIEWD / 2|VIEWD", rows("SELECT id, a FROM wdf_vt ORDER BY id"));
    }

    @Test
    void anInsertThroughADefaultlessViewStillTakesTheBaseRelationsDefault() throws Exception {
        exec("CREATE TABLE wdf_ivt (id int, a text DEFAULT 'BASE')");
        exec("CREATE VIEW wdf_ivv AS SELECT * FROM wdf_ivt");

        // after the rewrite the INSERT asks the base relation for the defaults of columns the
        // view left it none for
        exec("INSERT INTO wdf_ivv (id,a) VALUES (2,DEFAULT)");
        exec("INSERT INTO wdf_ivv (id) VALUES (3)");
        assertEquals("2|BASE / 3|BASE", rows("SELECT id, a FROM wdf_ivt ORDER BY id"));
    }

    @Test
    void aDomainDefaultStandsBehindAViewColumn() throws Exception {
        exec("CREATE DOMAIN wdf_dm4 AS int DEFAULT 42");
        exec("CREATE TABLE wdf_vd (id int, v wdf_dm4)");
        exec("INSERT INTO wdf_vd VALUES (1,5)");
        exec("CREATE VIEW wdf_vdv AS SELECT * FROM wdf_vd");

        assertEquals(1, update("UPDATE wdf_vdv SET v = DEFAULT"));
        assertEquals("1|42", rows("SELECT id, v FROM wdf_vd"));
    }

    @Test
    void anIdentitySequenceIsNotReachedThroughAView() throws Exception {
        exec("CREATE TABLE wdf_vi (id int, c int GENERATED BY DEFAULT AS IDENTITY)");
        exec("INSERT INTO wdf_vi (id) VALUES (1)");
        exec("CREATE VIEW wdf_viv AS SELECT * FROM wdf_vi");

        assertEquals("23502", stateOf("UPDATE wdf_viv SET c = DEFAULT"));
        assertEquals("null value in column \"c\" of relation \"wdf_vi\" violates not-null constraint",
                messageOf("UPDATE wdf_viv SET c = DEFAULT"));
        assertEquals("Failing row contains (1, null).", detailOf("UPDATE wdf_viv SET c = DEFAULT"));
        assertEquals("1|1", rows("SELECT id, c FROM wdf_vi"));
    }

    @Test
    void aBeforeTriggerOnTheBaseRelationSeesTheValueTheViewResolved() throws Exception {
        exec("CREATE TABLE wdf_tlog (v text)");
        exec("CREATE TABLE wdf_tvt (id int, a text DEFAULT 'BASE')");
        exec("INSERT INTO wdf_tvt VALUES (1,'o')");
        exec("CREATE VIEW wdf_tvv AS SELECT * FROM wdf_tvt");
        exec("ALTER VIEW wdf_tvv ALTER COLUMN a SET DEFAULT 'VIEWD'");
        exec("CREATE FUNCTION wdf_tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO wdf_tlog VALUES (coalesce(NEW.a,'<null>')); RETURN NEW; END $$");
        exec("CREATE TRIGGER wdf_tg BEFORE UPDATE ON wdf_tvt FOR EACH ROW EXECUTE FUNCTION wdf_tf()");

        assertEquals(1, update("UPDATE wdf_tvv SET a = DEFAULT"));
        assertEquals("1|VIEWD", rows("SELECT id, a FROM wdf_tvt"));
        assertEquals("VIEWD", scalar("SELECT v FROM wdf_tlog"));
    }

    @Test
    void aRelationCarryingARuleReadsTheDefaultTheStatementWouldWrite() throws Exception {
        exec("CREATE TABLE wdf_rl1 (v text)");
        exec("CREATE TABLE wdf_rt1 (id int, a text DEFAULT 'DD')");
        exec("INSERT INTO wdf_rt1 VALUES (1,'o'),(2,'p')");
        exec("CREATE RULE wdf_rr1 AS ON UPDATE TO wdf_rt1 DO ALSO INSERT INTO wdf_rl1 VALUES (NEW.a)");

        assertEquals(2, update("UPDATE wdf_rt1 SET a = DEFAULT"));
        assertEquals("1|DD / 2|DD", rows("SELECT id, a FROM wdf_rt1 ORDER BY id"));
        // NEW.a carries the default, once per row
        assertEquals("DD,DD", column("SELECT v FROM wdf_rl1"));
    }

    @Test
    void aDoInsteadRuleReadsItAndTheStatementItselfWritesNothing() throws Exception {
        exec("CREATE TABLE wdf_rl2 (v text)");
        exec("CREATE TABLE wdf_rt2 (id int, a text DEFAULT 'EE')");
        exec("INSERT INTO wdf_rt2 VALUES (1,'o')");
        exec("CREATE RULE wdf_rr2 AS ON UPDATE TO wdf_rt2 DO INSTEAD"
                + " INSERT INTO wdf_rl2 VALUES (NEW.a)");

        assertEquals(0, update("UPDATE wdf_rt2 SET a = DEFAULT"));
        assertEquals("1|o", rows("SELECT id, a FROM wdf_rt2"));
        assertEquals("EE", scalar("SELECT v FROM wdf_rl2"));
    }

    @Test
    void aSequenceBackedDefaultIsDrawnAgainForTheRulesOwnQuery() throws Exception {
        exec("CREATE TABLE wdf_rl3 (v int)");
        exec("CREATE TABLE wdf_rt3 (id int, s serial)");
        exec("INSERT INTO wdf_rt3 (id) VALUES (1),(2)");
        exec("CREATE RULE wdf_rr3 AS ON UPDATE TO wdf_rt3 DO ALSO INSERT INTO wdf_rl3 VALUES (NEW.s)");

        // the rule's query runs first and draws 3 and 4; the statement then draws 5 and 6
        assertEquals(2, update("UPDATE wdf_rt3 SET s = DEFAULT"));
        assertEquals("1|5 / 2|6", rows("SELECT id, s FROM wdf_rt3 ORDER BY id"));
        assertEquals("3,4", column("SELECT v FROM wdf_rl3 ORDER BY v"));
        assertEquals(6L, num("SELECT last_value FROM wdf_rt3_s_seq"));
    }

    @Test
    void insertDefaultValuesAndADataModifyingWithItemAreUntouchedByTheCheck() throws Exception {
        exec("CREATE TABLE wdf_dv (id int, a text DEFAULT 'CD')");
        exec("INSERT INTO wdf_dv VALUES (1,'o'),(2,'p')");

        assertEquals(1L, num("WITH w AS (UPDATE wdf_dv SET a = DEFAULT WHERE id = 1 RETURNING id)"
                + " SELECT count(*) FROM w"));
        assertEquals("1|CD / 2|p", rows("SELECT id, a FROM wdf_dv ORDER BY id"));

        exec("INSERT INTO wdf_dv DEFAULT VALUES");
        assertEquals(3L, num("SELECT count(*) FROM wdf_dv"));
        // SET <name> = DEFAULT is a GUC assignment and builds no DEFAULT value at all
        exec("SET datestyle = DEFAULT");
    }

    // A misplaced DEFAULT is refused while the statement is analysed, so an empty relation is
    // refused exactly as a full one is. The statements are written out as they are sent, because
    // Position is a character offset and depends on the spelling: PostgreSQL points at the first
    // character of the DEFAULT keyword, 1-based.

    @Test
    void aMisplacedDefaultIsRefusedWhileTheStatementIsAnalysedEvenOverAnEmptyRelation()
            throws Exception {
        exec("CREATE TABLE wdefk_dz (a text DEFAULT 'D', b int)");
        assertEquals(0L, num("SELECT count(*) FROM wdefk_dz"));

        String[] refused = {
                "UPDATE wdefk_dz SET a = DEFAULT || 'x'",
                "UPDATE wdefk_dz SET a = 'q' WHERE b = DEFAULT",
                "SELECT a FROM wdefk_dz WHERE a = DEFAULT",
                "DELETE FROM wdefk_dz WHERE b = DEFAULT",
                "INSERT INTO wdefk_dz (a,b) SELECT DEFAULT, b FROM wdefk_dz",
                "INSERT INTO wdefk_dz (a,b) VALUES (DEFAULT || 'y', 1)",
                "VALUES (DEFAULT)",
                "SELECT * FROM wdefk_dz ORDER BY DEFAULT",
                "UPDATE wdefk_dz SET a = CASE WHEN true THEN DEFAULT ELSE 'x' END",
                "SELECT DEFAULT",
                "INSERT INTO wdefk_dz (a,b) VALUES ((SELECT DEFAULT), 1)",
        };
        int[] positions = {25, 39, 34, 32, 35, 36, 9, 33, 45, 8, 44};
        for (int i = 0; i < refused.length; i++) {
            assertEquals("42601", stateOf(refused[i]), refused[i]);
            assertEquals("DEFAULT is not allowed in this context", messageOf(refused[i]), refused[i]);
            assertEquals(positions[i], positionOf(refused[i]), refused[i]);
        }
        // and none of them wrote anything
        assertEquals(0L, num("SELECT count(*) FROM wdefk_dz"));
    }

    @Test
    void aMisplacedDefaultInAConflictOrMergeUpdateArmIsRefusedTheSameWay() throws Exception {
        exec("CREATE TABLE wdefk_c (a int PRIMARY KEY, b text DEFAULT 'q')");
        exec("INSERT INTO wdefk_c VALUES (1,'x')");

        String conflict = "INSERT INTO wdefk_c (a,b) VALUES (1,'y')"
                + " ON CONFLICT (a) DO UPDATE SET b = DEFAULT || 'z'";
        assertEquals("42601", stateOf(conflict));
        assertEquals("DEFAULT is not allowed in this context", messageOf(conflict));
        assertEquals(76, positionOf(conflict));

        String merge = "MERGE INTO wdefk_c t USING (SELECT 9 AS k) s ON t.a = s.k"
                + " WHEN MATCHED THEN UPDATE SET b = DEFAULT || 'w'";
        assertEquals("42601", stateOf(merge));
        assertEquals("DEFAULT is not allowed in this context", messageOf(merge));
        assertEquals(92, positionOf(merge));

        assertEquals("1|x", rows("SELECT a, b FROM wdefk_c ORDER BY a"));
    }

    @Test
    void aRelationTheStatementCannotOpenIsReportedBeforeTheKeyword() throws Exception {
        // the range table is built before the statement's expressions are analysed
        assertEquals("42P01", stateOf("UPDATE wdefk_dnosuch SET a = DEFAULT || 'x'"));
        assertEquals("relation \"wdefk_dnosuch\" does not exist",
                messageOf("UPDATE wdefk_dnosuch SET a = DEFAULT || 'x'"));
        assertEquals(8, positionOf("UPDATE wdefk_dnosuch SET a = DEFAULT || 'x'"));

        assertEquals("42P01", stateOf("SELECT DEFAULT FROM wdefk_nosuch"));
        assertEquals("relation \"wdefk_nosuch\" does not exist",
                messageOf("SELECT DEFAULT FROM wdefk_nosuch"));
        assertEquals(21, positionOf("SELECT DEFAULT FROM wdefk_nosuch"));
    }

    @Test
    void parenthesesDoNotMoveTheKeywordOutOfItsPlace() throws Exception {
        exec("CREATE TABLE wdefk_ok (a text DEFAULT 'D', b int)");

        exec("INSERT INTO wdefk_ok (a,b) VALUES ((DEFAULT), 1)");
        assertEquals(1, update("UPDATE wdefk_ok SET a = (DEFAULT)"));
        exec("INSERT INTO wdefk_ok (a,b) VALUES (DEFAULT,2)");
        assertEquals(2, update("UPDATE wdefk_ok SET (a,b) = (DEFAULT, 3)"));
        assertEquals("D|3 / D|3", rows("SELECT a, b FROM wdefk_ok ORDER BY a, b"));

        exec("CREATE TABLE wdefk_ok2 (a int PRIMARY KEY, b text DEFAULT 'q')");
        exec("INSERT INTO wdefk_ok2 VALUES (1,'x')");
        assertEquals(1, update("INSERT INTO wdefk_ok2 (a,b) VALUES (1,'y')"
                + " ON CONFLICT (a) DO UPDATE SET b = DEFAULT"));
        assertEquals("1|q", rows("SELECT a, b FROM wdefk_ok2 ORDER BY a"));

        assertEquals(1, update("MERGE INTO wdefk_ok2 t USING (SELECT 9 AS k) s ON t.a = s.k"
                + " WHEN NOT MATCHED THEN INSERT (a,b) VALUES (s.k, DEFAULT)"));
        assertEquals("1|q / 9|q", rows("SELECT a, b FROM wdefk_ok2 ORDER BY a"));

        assertEquals(1, update("MERGE INTO wdefk_ok2 t USING (SELECT 9 AS k) s ON t.a = s.k"
                + " WHEN MATCHED THEN UPDATE SET b = DEFAULT"));
        assertEquals("1|q / 9|q", rows("SELECT a, b FROM wdefk_ok2 ORDER BY a"));
    }

    // An integrity-constraint violation has no place in the statement text, so PostgreSQL sends no
    // Position field at all and a client reads 0. A value the parser had to coerce does have one.

    @Test
    void anIntegrityViolationCarriesNoParseLocation() throws Exception {
        exec("CREATE TABLE wdf_pz (a int NOT NULL DEFAULT 3, b int NOT NULL,"
                + " c int GENERATED ALWAYS AS IDENTITY, d int GENERATED ALWAYS AS (a*2) STORED)");

        assertEquals("23502", stateOf("INSERT INTO wdf_pz (a,b) VALUES (1, NULL)"));
        assertEquals("null value in column \"b\" of relation \"wdf_pz\" violates not-null constraint",
                messageOf("INSERT INTO wdf_pz (a,b) VALUES (1, NULL)"));
        assertEquals(0, positionOf("INSERT INTO wdf_pz (a,b) VALUES (1, NULL)"));

        assertEquals("428C9", stateOf("INSERT INTO wdf_pz (a,b,c) VALUES (1, 2, 5)"));
        assertEquals("cannot insert a non-DEFAULT value into column \"c\"",
                messageOf("INSERT INTO wdf_pz (a,b,c) VALUES (1, 2, 5)"));
        assertEquals("Column \"c\" is an identity column defined as GENERATED ALWAYS.",
                detailOf("INSERT INTO wdf_pz (a,b,c) VALUES (1, 2, 5)"));
        assertEquals("Use OVERRIDING SYSTEM VALUE to override.",
                hintOf("INSERT INTO wdf_pz (a,b,c) VALUES (1, 2, 5)"));
        assertEquals(0, positionOf("INSERT INTO wdf_pz (a,b,c) VALUES (1, 2, 5)"));

        assertEquals("428C9", stateOf("INSERT INTO wdf_pz (a,b,d) VALUES (1, 2, 5)"));
        assertEquals("Column \"d\" is a generated column.",
                detailOf("INSERT INTO wdf_pz (a,b,d) VALUES (1, 2, 5)"));
        assertEquals(0, positionOf("INSERT INTO wdf_pz (a,b,d) VALUES (1, 2, 5)"));

        exec("INSERT INTO wdf_pz (a,b) VALUES (1, 2)");

        assertEquals("column \"c\" can only be updated to DEFAULT", messageOf("UPDATE wdf_pz SET c = 9"));
        assertEquals(0, positionOf("UPDATE wdf_pz SET c = 9"));
        assertEquals("column \"d\" can only be updated to DEFAULT", messageOf("UPDATE wdf_pz SET d = 9"));
        assertEquals(0, positionOf("UPDATE wdf_pz SET d = 9"));
        // the old reading found the "d" inside the word UPDATE and pointed at it
        assertEquals("23502", stateOf("UPDATE wdf_pz SET b = NULL"));
        assertEquals(0, positionOf("UPDATE wdf_pz SET b = NULL"));
    }

    @Test
    void everyOtherIntegrityViolationCarriesNoParseLocationEither() throws Exception {
        exec("CREATE TABLE wdf_pa (k int PRIMARY KEY, q int CHECK (q < 100))");
        exec("INSERT INTO wdf_pa VALUES (1, 1)");

        assertEquals("23505", stateOf("INSERT INTO wdf_pa VALUES (1, 1)"));
        assertEquals("duplicate key value violates unique constraint \"wdf_pa_pkey\"",
                messageOf("INSERT INTO wdf_pa VALUES (1, 1)"));
        assertEquals(0, positionOf("INSERT INTO wdf_pa VALUES (1, 1)"));

        assertEquals("23514", stateOf("INSERT INTO wdf_pa VALUES (2, 500)"));
        assertEquals(0, positionOf("INSERT INTO wdf_pa VALUES (2, 500)"));

        exec("CREATE TABLE wdf_chi (j int REFERENCES wdf_pa(k))");
        assertEquals("23503", stateOf("INSERT INTO wdf_chi VALUES (77)"));
        assertEquals(0, positionOf("INSERT INTO wdf_chi VALUES (77)"));

        exec("INSERT INTO wdf_chi VALUES (1)");
        assertEquals("23503", stateOf("DELETE FROM wdf_pa WHERE k = 1"));
        assertEquals(0, positionOf("DELETE FROM wdf_pa WHERE k = 1"));

        exec("CREATE TABLE wdf_nn (b int)");
        exec("INSERT INTO wdf_nn VALUES (NULL)");
        assertEquals("23502", stateOf("ALTER TABLE wdf_nn ALTER COLUMN b SET NOT NULL"));
        assertEquals("column \"b\" of relation \"wdf_nn\" contains null values",
                messageOf("ALTER TABLE wdf_nn ALTER COLUMN b SET NOT NULL"));
        assertEquals(0, positionOf("ALTER TABLE wdf_nn ALTER COLUMN b SET NOT NULL"));

        exec("CREATE TABLE wdf_ck (b int)");
        exec("INSERT INTO wdf_ck VALUES (500)");
        assertEquals("23514", stateOf("ALTER TABLE wdf_ck ADD CONSTRAINT wdf_ckc CHECK (b < 100)"));
        assertEquals("check constraint \"wdf_ckc\" of relation \"wdf_ck\" is violated by some row",
                messageOf("ALTER TABLE wdf_ck ADD CONSTRAINT wdf_ckc CHECK (b < 100)"));
        assertEquals(0, positionOf("ALTER TABLE wdf_ck ADD CONSTRAINT wdf_ckc CHECK (b < 100)"));
    }

    @Test
    void aValueTheParserHadToCoerceKeepsItsPlaceInTheText() throws Exception {
        exec("CREATE TABLE wdf_co (a int, b int)");

        assertEquals("22P02", stateOf("SELECT 'x'::int"));
        assertTrue(positionOf("SELECT 'x'::int") > 0, "a coerced literal has a place in the text");
        assertEquals("22P02", stateOf("INSERT INTO wdf_co VALUES ('y', 1)"));
        assertTrue(positionOf("INSERT INTO wdf_co VALUES ('y', 1)") > 0);
        assertEquals("22008", stateOf("SELECT CAST('2020-13-01' AS date)"));
        assertTrue(positionOf("SELECT CAST('2020-13-01' AS date)") > 0);
        assertEquals("Perhaps you need a different \"DateStyle\" setting.",
                hintOf("SELECT CAST('2020-13-01' AS date)"));
        // a division that only fails once the rows are read does not
        assertEquals("22012", stateOf("SELECT 1/0"));
        assertEquals(0, positionOf("SELECT 1/0"));
    }

    // ------------------------------------------------------------ what a statement that waited
    // ------------------------------------------------------------ still sees

    @Test
    void aBlockedUpdateReadsANonTargetRelationAsItFoundIt() throws Exception {
        exec("CREATE TABLE wws_a_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_a_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_a_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_a_u VALUES (1,10),(2,20)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_a_u SET v = 99 WHERE i = 1",
                             "UPDATE wws_a_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_a_t SET s = 'n' WHERE i IN (SELECT i FROM wws_a_u WHERE v = 10)",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals("OK", b.state);
        assertEquals(1, b.count);
        assertEquals("1|99|n / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_a_t ORDER BY i"));
    }

    @Test
    void aBlockedDeleteReadsANonTargetRelationAsItFoundIt() throws Exception {
        exec("CREATE TABLE wws_b_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_b_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_b_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_b_u VALUES (1,10),(2,20)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_b_u SET v = 99 WHERE i = 1",
                             "UPDATE wws_b_t SET v = 99 WHERE i = 1"},
                "DELETE FROM wws_b_t WHERE i IN (SELECT i FROM wws_b_u WHERE v = 10)",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_b_t ORDER BY i"));
    }

    @Test
    void aBlockedUpdateDoesNotWidenItsRowSetAfterTheWait() throws Exception {
        exec("CREATE TABLE wws_c_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_c_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_c_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_c_u VALUES (1,10),(2,20)");

        // the blocker makes u(2) qualify too; the waiting statement's snapshot never showed that
        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_c_u SET v = 10 WHERE i = 2",
                             "UPDATE wws_c_t SET s = 'blk' WHERE i = 1"},
                "UPDATE wws_c_t SET s = 'q' WHERE i IN (SELECT i FROM wws_c_u WHERE v = 10)",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("1|10|q / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_c_t ORDER BY i"));
    }

    @Test
    void aBlockedDeleteDoesNotWidenItsRowSetAfterTheWait() throws Exception {
        exec("CREATE TABLE wws_d_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_d_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_d_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_d_u VALUES (1,10),(2,20)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_d_u SET v = 10 WHERE i = 2",
                             "UPDATE wws_d_t SET s = 'blk' WHERE i = 1"},
                "DELETE FROM wws_d_t WHERE i IN (SELECT i FROM wws_d_u WHERE v = 10)",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_d_t ORDER BY i"));
    }

    @Test
    void aSetExpressionIsEvaluatedUnderTheStatementsOwnSnapshot() throws Exception {
        exec("CREATE TABLE wws_e_t (i int PRIMARY KEY, v int, s text)");
        exec("INSERT INTO wws_e_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");

        // the blocker takes row 2 out of v = 20 before it commits; the count still has to be 1
        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_e_t SET v = 77 WHERE i = 2",
                             "UPDATE wws_e_t SET s = 'blk' WHERE i = 1"},
                "UPDATE wws_e_t SET s = (SELECT count(*)::text FROM wws_e_t WHERE v = 20) WHERE i = 1",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("1|10|1 / 2|77|b / 3|30|c", rows("SELECT i, v, s FROM wws_e_t ORDER BY i"));
    }

    @Test
    void aSetExpressionOverAnotherRelationReadsTheSameSnapshot() throws Exception {
        exec("CREATE TABLE wws_f_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_f_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_f_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_f_u VALUES (1,10),(2,20)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_f_u SET v = 77 WHERE i = 1",
                             "UPDATE wws_f_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_f_t SET s = (SELECT v::text FROM wws_f_u WHERE wws_f_u.i = wws_f_t.i)"
                        + " WHERE i = 1",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("1|99|10 / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_f_t ORDER BY i"));
    }

    @Test
    void aReturningSubqueryIsEvaluatedUnderTheStatementsOwnSnapshot() throws Exception {
        exec("CREATE TABLE wws_g_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_g_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_g_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_g_u VALUES (1,10),(2,20)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_g_u SET v = 88 WHERE i = 2",
                             "UPDATE wws_g_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_g_t SET s = 'ret' WHERE i = 1"
                        + " RETURNING i, (SELECT count(*) FROM wws_g_u WHERE v = 20)",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals("1|1", b.rows);
        assertEquals("1|99|ret / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_g_t ORDER BY i"));
    }

    @Test
    void updateFromWaitsForTheRowItWantsAndTheBlockersRollbackStands() throws Exception {
        exec("CREATE TABLE wws_h_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_h_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_h_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_h_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_h_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_h_t SET s = 'j' FROM wws_h_u"
                        + " WHERE wws_h_u.i = wws_h_t.i AND wws_h_u.v = 10",
                "ROLLBACK");

        assertTrue(b.stillRunning, "the second session should have had to wait for the row lock");
        assertEquals(1, b.count);
        // the write is made on top of the rolled-back row, so v is 10 again
        assertEquals("1|10|j / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_h_t ORDER BY i"));
    }

    @Test
    void updateFromDoesNotReadAnotherSessionsUncommittedValues() throws Exception {
        exec("CREATE TABLE wws_i_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_i_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_i_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_i_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_i_t SET v = 10 WHERE i = 2"},
                "UPDATE wws_i_t SET s = 'X' FROM wws_i_u"
                        + " WHERE wws_i_u.i = wws_i_t.i AND wws_i_t.v = 10",
                "ROLLBACK");

        // nothing it wants is locked, so it never waits
        assertFalse(b.stillRunning, "nothing the statement wants is locked");
        assertEquals(1, b.count);
        assertEquals("1|10|X / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_i_t ORDER BY i"));
    }

    @Test
    void updateFromWaitsForARowHiddenByAnUncommittedDelete() throws Exception {
        exec("CREATE TABLE wws_j_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_j_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_j_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_j_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"DELETE FROM wws_j_t WHERE i = 1"},
                "UPDATE wws_j_t SET s = 'Z' FROM wws_j_u"
                        + " WHERE wws_j_u.i = wws_j_t.i AND wws_j_t.v = 10",
                "ROLLBACK");

        assertTrue(b.stillRunning, "the row is hidden by an uncommitted delete, not gone");
        assertEquals(1, b.count);
        assertEquals("1|10|Z / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_j_t ORDER BY i"));
    }

    @Test
    void deleteUsingDoesNotReadAnotherSessionsUncommittedValues() throws Exception {
        exec("CREATE TABLE wws_k_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_k_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_k_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_k_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_k_t SET v = 10 WHERE i = 2"},
                "DELETE FROM wws_k_t USING wws_k_u WHERE wws_k_u.i = wws_k_t.i AND wws_k_t.v = 10",
                "ROLLBACK");

        assertFalse(b.stillRunning, "nothing the statement wants is locked");
        assertEquals(1, b.count);
        assertEquals("2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_k_t ORDER BY i"));
    }

    @Test
    void deleteUsingWaitsForARowHiddenByAnUncommittedDelete() throws Exception {
        exec("CREATE TABLE wws_l_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_l_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_l_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_l_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"DELETE FROM wws_l_t WHERE i = 1"},
                "DELETE FROM wws_l_t USING wws_l_u WHERE wws_l_u.i = wws_l_t.i AND wws_l_t.v = 10",
                "ROLLBACK");

        assertTrue(b.stillRunning, "the row is hidden by an uncommitted delete, not gone");
        assertEquals(1, b.count);
        assertEquals("2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_l_t ORDER BY i"));
    }

    @Test
    void ctidInsideAStatementsCommittedImageIsTheRowsOwn() throws Exception {
        exec("CREATE TABLE wws_m_g (i int PRIMARY KEY, v int, s text)");
        exec("INSERT INTO wws_m_g VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        assertEquals("(0,1)|1 / (0,2)|2 / (0,3)|3", rows("SELECT ctid, i FROM wws_m_g ORDER BY i"));

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_m_g SET v = 99 WHERE i = 1"},
                "UPDATE wws_m_g SET s = 'q' WHERE i IN"
                        + " (SELECT i FROM wws_m_g WHERE ctid = '(0,1)')",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals(1, b.count);
        assertEquals("1|99|q / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_m_g ORDER BY i"));
    }

    @Test
    void updateFromHoldsTheRowLockAConcurrentSelectAsksFor() throws Exception {
        exec("CREATE TABLE wws_n_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_n_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_n_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_n_u VALUES (1,10),(2,20),(3,30)");

        whileAnotherSessionHolds(
                new String[]{"UPDATE wws_n_t SET s = 'j' FROM wws_n_u"
                        + " WHERE wws_n_u.i = wws_n_t.i AND wws_n_u.i = 1"},
                () -> {
                    assertEquals("55P03", stateOf("SELECT i FROM wws_n_t WHERE i = 1 FOR UPDATE NOWAIT"));
                    assertEquals("could not obtain lock on row in relation \"wws_n_t\"",
                            messageOf("SELECT i FROM wws_n_t WHERE i = 1 FOR UPDATE NOWAIT"));
                    // the rows it did not touch are free
                    assertEquals("2", scalar("SELECT i FROM wws_n_t WHERE i = 2 FOR UPDATE NOWAIT"));
                });

        assertEquals("1|10|a / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_n_t ORDER BY i"));
    }

    @Test
    void aBlockedDeleteUsingAnswersItsReturningFromTheRowItDeleted() throws Exception {
        exec("CREATE TABLE wws_o_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_o_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_o_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_o_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_o_t SET v = 99 WHERE i = 1"},
                "DELETE FROM wws_o_t USING wws_o_u WHERE wws_o_u.i = wws_o_t.i AND wws_o_t.i = 1"
                        + " RETURNING wws_o_t.i, wws_o_t.v, wws_o_t.s",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        // the row it deleted is the one the blocker left behind
        assertEquals("1|99|a", b.rows);
        assertEquals("2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_o_t ORDER BY i"));
    }

    @Test
    void aBlockedUpdateFromAnswersItsReturningFromTheRowItWrote() throws Exception {
        exec("CREATE TABLE wws_p_t (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_p_u (i int PRIMARY KEY, v int)");
        exec("INSERT INTO wws_p_t VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
        exec("INSERT INTO wws_p_u VALUES (1,10),(2,20),(3,30)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_p_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_p_t SET s = 'w' FROM wws_p_u"
                        + " WHERE wws_p_u.i = wws_p_t.i AND wws_p_t.i = 1"
                        + " RETURNING wws_p_t.i, wws_p_t.v, wws_p_t.s",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals("1|99|w", b.rows);
        assertEquals("1|99|w / 2|20|b / 3|30|c", rows("SELECT i, v, s FROM wws_p_t ORDER BY i"));
    }

    @Test
    void theForeignKeyCheckOfABlockedDeleteUsingReadsTheRowsItReJudged() throws Exception {
        exec("CREATE TABLE wws_q_p (i int PRIMARY KEY, v int)");
        exec("CREATE TABLE wws_q_c (j int REFERENCES wws_q_p(i))");
        exec("CREATE TABLE wws_q_k (i int)");
        exec("INSERT INTO wws_q_p VALUES (1,10),(2,20)");
        exec("INSERT INTO wws_q_c VALUES (1)");
        exec("INSERT INTO wws_q_k VALUES (1),(2)");

        Waited b = whileAnotherSessionHolds(
                new String[]{"UPDATE wws_q_p SET v = 99 WHERE i = 1"},
                "DELETE FROM wws_q_p USING wws_q_k WHERE wws_q_k.i = wws_q_p.i AND wws_q_p.i = 1",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals("23503", b.state);
        assertEquals("1|99 / 2|20", rows("SELECT i, v FROM wws_q_p ORDER BY i"));
    }

    @Test
    void aBlockedUpdateInsideRepeatableReadCannotSerialize() throws Exception {
        exec("CREATE TABLE wws_r_t (i int PRIMARY KEY, v int, s text)");
        exec("INSERT INTO wws_r_t VALUES (1,10,'a'),(2,20,'b')");

        Waited b = whileAnotherSessionHolds(
                new String[]{"BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT count(*) FROM wws_r_t"},
                new String[]{"UPDATE wws_r_t SET v = 99 WHERE i = 1"},
                "UPDATE wws_r_t SET s = 'rr' WHERE i = 1",
                "COMMIT");

        assertTrue(b.stillRunning, "the second session should have had to wait");
        assertEquals("40001", b.state);
        assertEquals("could not serialize access due to concurrent update", b.message);
        assertEquals("1|99|a / 2|20|b", rows("SELECT i, v, s FROM wws_r_t ORDER BY i"));
    }

    @Test
    void systemColumnsReadThroughASnapshotAreTheRowsOwn() throws Exception {
        exec("CREATE TABLE wws_e (i int, s text)");
        exec("INSERT INTO wws_e VALUES (1,'a'),(2,'b'),(3,'c')");
        assertEquals("(0,1)|1 / (0,2)|2 / (0,3)|3", rows("SELECT ctid, i FROM wws_e ORDER BY i"));

        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertEquals("(0,1)|1 / (0,2)|2 / (0,3)|3", rows("SELECT ctid, i FROM wws_e ORDER BY i"));
        assertEquals(3L, num("SELECT count(DISTINCT ctid) FROM wws_e"));
        assertEquals("2", scalar("SELECT i FROM wws_e WHERE ctid = '(0,2)'"));
        assertEquals("t", scalar("SELECT bool_and(xmin::text <> '0') FROM wws_e"));
        assertEquals("t", scalar("SELECT bool_and(cmin::text = '0') FROM wws_e"));
        assertEquals("t", scalar("SELECT bool_and(xmax::text = '0') FROM wws_e"));
        assertEquals("t", scalar("SELECT bool_and(tableoid = 'wws_e'::regclass) FROM wws_e"));
        exec("COMMIT");

        exec("BEGIN ISOLATION LEVEL SERIALIZABLE");
        assertEquals("(0,1)|1 / (0,2)|2 / (0,3)|3", rows("SELECT ctid, i FROM wws_e ORDER BY i"));
        assertEquals(3L, num("SELECT count(DISTINCT ctid) FROM wws_e"));
        assertEquals("2", scalar("SELECT i FROM wws_e WHERE ctid = '(0,2)'"));
        exec("COMMIT");

        exec("BEGIN");
        exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        assertEquals("(0,1)|1 / (0,2)|2 / (0,3)|3", rows("SELECT ctid, i FROM wws_e ORDER BY i"));
        assertEquals(3L, num("SELECT count(DISTINCT ctid) FROM wws_e"));
        assertEquals("2", scalar("SELECT i FROM wws_e WHERE ctid = '(0,2)'"));
        exec("COMMIT");
    }

    @Test
    void updateFromAndDeleteUsingActOnATargetRowOnceWithNobodyElseInTheWay() throws Exception {
        exec("CREATE TABLE wws_ft (i int PRIMARY KEY, v int, s text)");
        exec("CREATE TABLE wws_fu (j int, w int, t text)");
        exec("INSERT INTO wws_ft VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d')");
        exec("INSERT INTO wws_fu VALUES (1,100,'x'),(2,200,'y'),(2,201,'y2'),(5,500,'z')");

        // row 2 joins two rows of wws_fu and is written once
        assertEquals(2, update("UPDATE wws_ft SET v = wws_fu.w FROM wws_fu WHERE wws_fu.j = wws_ft.i"));
        assertEquals("1|100|a / 2|200|b / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        assertEquals("2|200|y", rows("UPDATE wws_ft SET s = u.t FROM wws_fu u"
                + " WHERE u.j = wws_ft.i AND u.w > 150 RETURNING i, v, s"));
        assertEquals("1|100|a / 2|200|y / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        assertEquals(0, update("UPDATE wws_ft SET v = v + 1 FROM wws_fu WHERE wws_fu.j = 999"));
        assertEquals("1|100|a / 2|200|y / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        // the subquery on the right of SET is read over the whole relation, not over the join
        assertEquals("1|500 / 2|500", rows("UPDATE wws_ft SET v = (SELECT max(w) FROM wws_fu)"
                + " FROM wws_fu z WHERE z.j = wws_ft.i RETURNING i, v"));
        assertEquals("1|500|a / 2|500|y / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        assertEquals("1|500", rows("DELETE FROM wws_ft USING wws_fu"
                + " WHERE wws_fu.j = wws_ft.i AND wws_fu.w = 100 RETURNING i, v"));
        assertEquals("2|500|y / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        assertEquals(0, update("DELETE FROM wws_ft USING wws_fu WHERE wws_fu.j = 999"));
        assertEquals("2|500|y / 3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));

        // and the row that joins two USING rows is deleted once
        assertEquals(1, update("DELETE FROM wws_ft USING wws_fu WHERE wws_fu.j = wws_ft.i"));
        assertEquals("3|30|c / 4|40|d", rows("SELECT i, v, s FROM wws_ft ORDER BY i"));
    }

    // ------------------------------------------------------------ a WITH item is worked out
    // ------------------------------------------------------------ inside the query that reads it

    // A WITH item is pulled up into the query that reads it when that query names it once and the
    // item is left to itself; written MATERIALIZED, named twice, or holding a volatile call, it is
    // computed on its own instead and every row of it reaches the generation expression. The
    // expression below is 10/a over a relation holding a row with a = 0, so each case is really
    // asking which rows it was worked out for.

    @Test
    void anInlinedWithItemTakesTheQualificationOfTheQueryThatReadsIt() throws Exception {
        exec("CREATE TABLE wvq_a_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_a_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE VIEW wvq_a_v AS WITH c AS (SELECT * FROM wvq_a_g) SELECT * FROM c");

        assertEquals("2", scalar("WITH c AS (SELECT * FROM wvq_a_g) SELECT g FROM c WHERE a = 5"));
        assertEquals("2", scalar("WITH c AS NOT MATERIALIZED (SELECT * FROM wvq_a_g) SELECT g FROM c WHERE a = 5"));
        // RECURSIVE written but never used leaves the item an ordinary one
        assertEquals("2", scalar("WITH RECURSIVE c AS (SELECT * FROM wvq_a_g) SELECT g FROM c WHERE a = 5"));
        // the qualification reaches through one item into another
        assertEquals("2", scalar("WITH c AS (SELECT * FROM wvq_a_g), d AS (SELECT * FROM c)"
                + " SELECT g FROM d WHERE a = 5"));
        // now() is stable, not volatile, so the item is still pulled up
        assertEquals("2", scalar("WITH c AS (SELECT now()::text AS n, * FROM wvq_a_g)"
                + " SELECT g FROM c WHERE a = 5"));
        // one arm of a set operation naming it is still naming it once
        assertEquals("2,1", column("WITH c AS (SELECT * FROM wvq_a_g) SELECT g FROM c WHERE a = 5"
                + " UNION ALL SELECT 1"));
        // written NOT MATERIALIZED it is pulled up into both references
        assertEquals("2,2", column("WITH c AS NOT MATERIALIZED (SELECT * FROM wvq_a_g) SELECT g FROM c WHERE a = 5"
                + " UNION ALL SELECT g FROM c WHERE a = 5"));
        // and a view whose body reads an item takes the qualification through both
        assertEquals("2", scalar("SELECT g FROM wvq_a_v WHERE a = 5"));
    }

    @Test
    void aWithItemTheQueryKeepsApartIsWorkedOutOverEveryRowOfItsOwn() throws Exception {
        exec("CREATE TABLE wvq_b_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_b_g (a,k) VALUES (5,'five'),(0,'zero')");

        String materialized = "WITH c AS MATERIALIZED (SELECT * FROM wvq_b_g) SELECT g FROM c WHERE a = 5";
        assertEquals("22012", stateOf(materialized));
        assertEquals("division by zero", messageOf(materialized));
        // named twice, the item is computed on its own
        assertEquals("22012", stateOf("WITH c AS (SELECT * FROM wvq_b_g) SELECT g FROM c WHERE a = 5"
                + " UNION ALL SELECT g FROM c WHERE a = 5"));
        assertEquals("22012", stateOf("WITH c AS MATERIALIZED (SELECT * FROM wvq_b_g) SELECT g FROM c WHERE a = 5"
                + " UNION ALL SELECT g FROM c WHERE a = 5"));
        // a volatile call in the item's select list or in its own qualification keeps it apart
        assertEquals("22012", stateOf("WITH c AS (SELECT *, random() AS r FROM wvq_b_g)"
                + " SELECT g FROM c WHERE a = 5"));
        assertEquals("22012", stateOf("WITH c AS (SELECT * FROM wvq_b_g WHERE random() < 2)"
                + " SELECT g FROM c WHERE a = 5"));
        // and NOT MATERIALIZED does not override a volatile call
        assertEquals("22012", stateOf("WITH c AS NOT MATERIALIZED (SELECT *, random() AS r FROM wvq_b_g)"
                + " SELECT g FROM c WHERE a = 5"));
    }

    @Test
    void aWithItemNobodyReadsTheGeneratedColumnOfIsReadWhole() throws Exception {
        exec("CREATE TABLE wvq_c_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_c_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals(2L, num("WITH c AS (SELECT * FROM wvq_c_g) SELECT count(*) FROM c"));
        assertEquals("five,zero", column("WITH c AS (SELECT * FROM wvq_c_g) SELECT k FROM c ORDER BY k"));
    }

    // Pulling an item up decides only WHICH rows the column is worked out for; it never changes a
    // value. The generation expression below cannot raise, so every reading has to carry the same one.

    @Test
    void inliningAWithItemNeverChangesTheValueTheColumnCarries() throws Exception {
        exec("CREATE TABLE wvq_d_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wvq_d_h VALUES (1,'one'),(2,'two'),(3,'three')");

        assertEquals("1|10", rows("WITH c AS (SELECT * FROM wvq_d_h) SELECT a, g FROM c WHERE a = 1"));
        assertEquals("1|10", rows("WITH c AS MATERIALIZED (SELECT * FROM wvq_d_h) SELECT a, g FROM c WHERE a = 1"));
        assertEquals("1|10", rows("WITH c AS NOT MATERIALIZED (SELECT * FROM wvq_d_h)"
                + " SELECT a, g FROM c WHERE a = 1"));
        assertEquals("1|10", rows("WITH c AS (SELECT *, random() AS r FROM wvq_d_h) SELECT a, g FROM c WHERE a = 1"));
        assertEquals("1|10 / 2|20", rows("WITH c AS (SELECT * FROM wvq_d_h) SELECT a, g FROM c WHERE a = 1"
                + " UNION ALL SELECT a, g FROM c WHERE a = 2"));
    }

    // ------------------------------------------------------------ the qualification's parts decide
    // ------------------------------------------------------------ the row before a generation
    // ------------------------------------------------------------ expression does

    @Test
    void aConjunctOverAStoredColumnDecidesTheRowBeforeTheGeneratedOneIsWorkedOut() throws Exception {
        exec("CREATE TABLE wvq_e_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_e_g (a,k) VALUES (5,'five'),(0,'zero')");

        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE a = 5 AND g = 2"));
        // written the other way round, the stored column still decides first
        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE g = 2 AND a = 5"));
        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE a = 5 AND g = 2 AND k = 'five'"));
        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE a <> 0 AND g = 2"));
        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE k = 'five' AND g = 2"));
        // the same qualification written out rather than through the column
        assertEquals("five", scalar("SELECT k FROM wvq_e_g WHERE a = 5 AND 10/a = 2"));
        assertEquals(1L, num("SELECT count(*) FROM wvq_e_g WHERE a = 5 AND g = 2"));
        // the column is read again above the qualification
        assertEquals("5", scalar("SELECT a FROM wvq_e_g WHERE a = 5 AND g = 2 ORDER BY g"));
        assertEquals("five", scalar("SELECT s.k FROM (SELECT * FROM wvq_e_g) s WHERE s.a = 5 AND s.g = 2"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvq_e_g) s WHERE s.a = 5 AND s.g = 2"));
    }

    @Test
    void aQualificationThatDecidesNothingFirstReachesEveryRow() throws Exception {
        exec("CREATE TABLE wvq_f_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_f_g (a,k) VALUES (5,'five'),(0,'zero')");

        // one side of an OR decides nothing on its own
        assertEquals("22012", stateOf("SELECT k FROM wvq_f_g WHERE a = 5 OR g = 2"));
        assertEquals("division by zero", messageOf("SELECT k FROM wvq_f_g WHERE a = 5 OR g = 2"));
        assertEquals("22012", stateOf("SELECT k FROM wvq_f_g WHERE g = 2"));
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wvq_f_g) s WHERE s.g = 2"));
    }

    @Test
    void theWritePathsTakeTheirQualificationTheSameWay() throws Exception {
        exec("CREATE TABLE wvq_g_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvq_g_g (a,k) VALUES (5,'five'),(0,'zero')");
        exec("CREATE TABLE wvq_g_o (a int, note text)");
        exec("INSERT INTO wvq_g_o VALUES (5,'x'),(0,'y')");

        assertEquals(1, update("UPDATE wvq_g_g SET k = 'F' WHERE a = 5 AND g = 2"));
        assertEquals("0|zero / 5|F", rows("SELECT a, k FROM wvq_g_g ORDER BY a"));
        assertEquals(0, update("DELETE FROM wvq_g_g WHERE a = 99 AND g = 2"));
        assertEquals("0|zero / 5|F", rows("SELECT a, k FROM wvq_g_g ORDER BY a"));
        assertEquals("5|five", rows("UPDATE wvq_g_g SET k = 'five' WHERE a = 5 AND g = 2 RETURNING a, k"));

        // a generated column of the relation the statement brought in is worked out above the join
        assertEquals(1, update("UPDATE wvq_g_o o SET note = t.g::text FROM wvq_g_g t WHERE t.a = o.a AND o.a = 5"));
        assertEquals("0|y / 5|2", rows("SELECT a, note FROM wvq_g_o ORDER BY a"));
        // with nothing narrowing the join, every paired row reaches it and nothing is written
        assertEquals("22012", stateOf("UPDATE wvq_g_o o SET note = t.g::text FROM wvq_g_g t WHERE t.a = o.a"));
        assertEquals("0|y / 5|2", rows("SELECT a, note FROM wvq_g_o ORDER BY a"));
    }

    // ------------------------------------------------------------ a generated column of a joined
    // ------------------------------------------------------------ relation is worked out above
    // ------------------------------------------------------------ the join that kept its row

    @Test
    void aDerivedRelationQualifiedOnlyThroughAJoinIsWorkedOutAboveIt() throws Exception {
        exec("CREATE TABLE wvj_a_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_a_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_a_o (a int, note text)");
        exec("INSERT INTO wvj_a_o VALUES (0,'y'),(5,'y')");
        exec("CREATE VIEW wvj_a_v AS SELECT * FROM wvj_a_g");

        assertEquals("2", scalar("SELECT s.g FROM wvj_a_o o LEFT JOIN (SELECT * FROM wvj_a_g) s"
                + " ON o.a = s.a WHERE o.a = 5"));
        // the restriction written into the ON condition narrows it just as well
        assertEquals("2", scalar("SELECT s.g FROM wvj_a_o o JOIN (SELECT * FROM wvj_a_g) s"
                + " ON o.a = s.a AND o.a = 5"));
        assertEquals("5|2", rows("SELECT o.a, s.g FROM wvj_a_o o LEFT JOIN (SELECT * FROM wvj_a_g) s"
                + " ON o.a = s.a WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvj_a_g) s, wvj_a_o o"
                + " WHERE o.a = s.a AND o.a = 5"));
        assertEquals("2", scalar("SELECT v.g FROM wvj_a_o o LEFT JOIN wvj_a_v v ON o.a = v.a WHERE o.a = 5"));
        assertEquals("2", scalar("WITH c AS (SELECT * FROM wvj_a_g)"
                + " SELECT c.g FROM wvj_a_o o LEFT JOIN c ON o.a = c.a WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM wvj_a_o o LEFT JOIN"
                + " (WITH q AS (SELECT * FROM wvj_a_g) SELECT * FROM q) s ON o.a = s.a WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM wvj_a_o o RIGHT JOIN (SELECT * FROM wvj_a_g) s"
                + " ON o.a = s.a WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM wvj_a_o o FULL JOIN (SELECT * FROM wvj_a_g) s"
                + " ON o.a = s.a WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT t1.g FROM (SELECT * FROM wvj_a_g) t1, (SELECT * FROM wvj_a_g) t2"
                + " WHERE t1.a = 5 AND t2.a = 5"));
    }

    @Test
    void aJoinThatKeepsEveryRowOfTheRelationStillReachesTheRowItCannotWorkOut() throws Exception {
        exec("CREATE TABLE wvj_b_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_b_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_b_o (a int, note text)");
        exec("INSERT INTO wvj_b_o VALUES (0,'y'),(5,'y')");

        assertEquals("22012", stateOf("SELECT s.g FROM wvj_b_o o LEFT JOIN (SELECT * FROM wvj_b_g) s"
                + " ON o.a = s.a"));
        assertEquals("22012", stateOf("SELECT s.g FROM wvj_b_o o, LATERAL (SELECT * FROM wvj_b_g) s"));
        // nothing about the generated column is read here, so nothing raises
        assertEquals("five,zero", column("SELECT s.k FROM wvj_b_o o LEFT JOIN (SELECT * FROM wvj_b_g) s"
                + " ON o.a = s.a ORDER BY s.k"));
    }

    // A column alias list renames every column the relation exposes, whatever kind of relation it
    // is; the query above writes those names and the query underneath still answers to its own.

    @Test
    void aColumnAliasListRenamesEveryColumnTheRelationExposes() throws Exception {
        exec("CREATE TABLE wvj_c_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_c_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_c_o (a int, note text)");
        exec("INSERT INTO wvj_c_o VALUES (0,'y'),(5,'y')");
        exec("CREATE VIEW wvj_c_v AS SELECT * FROM wvj_c_g");
        exec("CREATE VIEW wvj_c_vo AS SELECT * FROM wvj_c_o");

        assertEquals("2", scalar("SELECT s.z FROM (SELECT * FROM wvj_c_g) s(x,y,z) WHERE s.x = 5"));
        assertEquals("5|2", rows("SELECT s.x, s.z FROM (SELECT * FROM wvj_c_g) s(x,y,z) WHERE s.x = 5"));
        assertEquals("five", scalar("SELECT s.y FROM (SELECT * FROM wvj_c_g) s(x,y,z) WHERE s.x = 5"));
        assertEquals("2", scalar("SELECT s.z FROM (SELECT * FROM wvj_c_g) s(x,y,z)"
                + " WHERE s.x = 5 AND s.y = 'five'"));
        assertEquals("2", scalar("SELECT s.z FROM (SELECT a, k, g FROM wvj_c_g) s(x,y,z) WHERE s.x = 5"));
        assertEquals("2", scalar("SELECT s.r FROM (SELECT * FROM wvj_c_g) AS s (p, q, r) WHERE s.p = 5"));
        assertEquals("2", scalar("SELECT s.z FROM wvj_c_o o LEFT JOIN (SELECT * FROM wvj_c_g) s(x,y,z)"
                + " ON o.a = s.x WHERE o.a = 5"));
        // a list that reaches only the first column leaves the rest their own names
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvj_c_g) s(x) WHERE s.x = 5"));
        assertEquals(2L, num("SELECT count(*) FROM (SELECT * FROM wvj_c_g) s(x,y,z)"));
        // and a view is renamed the same way
        assertEquals("y,y", column("SELECT s.y FROM wvj_c_vo s(x,y) ORDER BY s.x"));
        assertEquals("2", scalar("SELECT s.z FROM wvj_c_v s(x,y,z) WHERE s.x = 5"));
        assertEquals("five", scalar("SELECT s.y FROM wvj_c_v s(x,y,z) WHERE s.x = 5"));
        assertEquals("5|2", rows("SELECT s.x, s.z FROM wvj_c_v s(x,y,z) WHERE s.x = 5"));
        assertEquals(2L, num("SELECT count(*) FROM wvj_c_v s(x,y,z)"));
    }

    @Test
    void aNameAnAliasListDidNotGiveIsNotAColumnOfTheRelation() throws Exception {
        exec("CREATE TABLE wvj_d_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_d_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_d_o (a int, note text)");
        exec("INSERT INTO wvj_d_o VALUES (0,'y'),(5,'y')");
        exec("CREATE VIEW wvj_d_v AS SELECT * FROM wvj_d_g");
        exec("CREATE VIEW wvj_d_vo AS SELECT * FROM wvj_d_o");

        String partial = "SELECT s.z FROM (SELECT * FROM wvj_d_g) s(x) WHERE s.x = 5";
        assertEquals("42703", stateOf(partial));
        assertEquals("column s.z does not exist", messageOf(partial));
        // the relation answers to the names the list gave it and to no others
        String renamed = "SELECT s.g FROM (SELECT * FROM wvj_d_g) s(x,y,z) WHERE s.x = 5";
        assertEquals("42703", stateOf(renamed));
        assertEquals("column s.g does not exist", messageOf(renamed));
        assertEquals("42703", stateOf("SELECT s.g FROM (SELECT * FROM wvj_d_g) AS s (p, q, r) WHERE s.p = 5"));
        assertEquals("42703", stateOf("SELECT s.g FROM wvj_d_v s(x,y,z) WHERE s.x = 5"));
        // a list longer than the relation is a list the relation cannot answer to
        String tooLong = "SELECT * FROM wvj_d_vo s(x,y,z)";
        assertEquals("42P10", stateOf(tooLong));
        assertEquals("table \"s\" has 2 columns available but 3 columns specified", messageOf(tooLong));
    }

    @Test
    void aQualificationNamingTheRenamedColumnIsStillAScanQualification() throws Exception {
        exec("CREATE TABLE wvj_e_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_e_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE VIEW wvj_e_v AS SELECT * FROM wvj_e_g");

        assertEquals("22012", stateOf("SELECT s.z FROM (SELECT * FROM wvj_e_g) s(x,y,z) WHERE s.z = 2"));
        assertEquals("22012", stateOf("SELECT s.z FROM wvj_e_v s(x,y,z)"));
    }

    // A LATERAL sub-select is pulled up into the query reading it just as a plain derived table is,
    // so a reference to a generated column of a relation underneath stands in the query above.

    @Test
    void aLateralRelationIsWorkedOutInTheQueryThatReadsIt() throws Exception {
        exec("CREATE TABLE wvj_f_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_f_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_f_o (a int, note text)");
        exec("INSERT INTO wvj_f_o VALUES (0,'y'),(5,'y')");

        assertEquals("2", scalar("SELECT s.g FROM wvj_f_o o, LATERAL (SELECT * FROM wvj_f_g WHERE a = o.a) s"
                + " WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM wvj_f_o o CROSS JOIN LATERAL"
                + " (SELECT * FROM wvj_f_g z WHERE z.a = o.a) s WHERE o.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM wvj_f_o o LEFT JOIN LATERAL"
                + " (SELECT * FROM wvj_f_g z WHERE z.a = o.a) s ON true WHERE o.a = 5"));
        // an uncorrelated one narrowed from above answers the same way
        assertEquals("2", scalar("SELECT s.g FROM wvj_f_o o, LATERAL (SELECT * FROM wvj_f_g) s"
                + " WHERE o.a = 5 AND s.a = 5"));
        assertEquals("zero,five", column("SELECT s.k FROM wvj_f_o o, LATERAL (SELECT * FROM wvj_f_g) s"
                + " WHERE o.a = 5 ORDER BY s.a"));
        assertEquals(4L, num("SELECT count(*) FROM wvj_f_o o, LATERAL (SELECT * FROM wvj_f_g) s"));
        // a qualification naming the column is a qualification of the scan itself
        assertEquals("22012", stateOf("SELECT s.g FROM wvj_f_o o,"
                + " LATERAL (SELECT * FROM wvj_f_g WHERE a = o.a) s WHERE s.g = 2"));
    }

    // Two columns compared for equality, one of them compared with a written constant, stand in one
    // class: s.a = o.a AND o.a = 5 says s.a = 5 as surely as it says either of the two, and that
    // restriction decides s's own rows before the generation expression is reached.

    @Test
    void aRestrictionTheEqualitiesDeriveBetweenThemDecidesTheScan() throws Exception {
        exec("CREATE TABLE wvj_g_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_g_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_g_o (a int, note text)");
        exec("INSERT INTO wvj_g_o VALUES (0,'y'),(5,'y')");

        assertEquals("1", scalar("SELECT 1 FROM wvj_g_g s, wvj_g_o o WHERE s.a = o.a AND o.a = 5 AND s.g = 2"));
        assertEquals("1", scalar("SELECT 1 FROM (SELECT * FROM wvj_g_g) s, wvj_g_o o"
                + " WHERE s.a = o.a AND o.a = 5 AND s.g = 2"));
        // nothing is derived from an equality with no constant anywhere in its class
        assertEquals("22012", stateOf("SELECT 1 FROM wvj_g_g s, wvj_g_o o WHERE s.a = o.a AND s.g = 2"));

        // the outer row stands still while a correlated subquery runs, so it decides its rows too
        assertEquals("5", scalar("SELECT a FROM wvj_g_g t1 WHERE a = 5"
                + " AND EXISTS (SELECT 1 FROM wvj_g_g t2 WHERE t2.a = t1.a AND t2.g = 2)"));
        assertEquals("22012", stateOf("SELECT a FROM wvj_g_g t1"
                + " WHERE EXISTS (SELECT 1 FROM wvj_g_g t2 WHERE t2.a = t1.a AND t2.g = 2)"));
        assertEquals("5", scalar("SELECT o.a FROM wvj_g_o o WHERE o.a = 5"
                + " AND EXISTS (SELECT 1 FROM (SELECT * FROM wvj_g_g) s WHERE s.a = o.a AND s.g = 2)"));
        assertEquals("22012", stateOf("SELECT o.a FROM wvj_g_o o"
                + " WHERE EXISTS (SELECT 1 FROM (SELECT * FROM wvj_g_g) s WHERE s.g = 2)"));

        assertEquals(1, update("DELETE FROM wvj_g_o o USING wvj_g_g t WHERE t.a = o.a AND o.a = 5 AND t.g = 2"));
        assertEquals("0|y", rows("SELECT a, note FROM wvj_g_o ORDER BY a"));
    }

    // What a relation settles for itself — a LIMIT, an OFFSET, a sort of its own — it settles
    // before the query above is read, so the qualification stays where it was written.

    @Test
    void whatARelationSettlesForItselfIsStillWorkedOutBelowTheQuery() throws Exception {
        exec("CREATE TABLE wvj_h_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_h_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE VIEW wvj_h_v AS SELECT * FROM wvj_h_g");

        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wvj_h_g) s"));
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wvj_h_g) s ORDER BY s.g"));
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wvj_h_g LIMIT 5) s WHERE s.a = 5"));
        assertEquals("22012", stateOf("SELECT s.g FROM (SELECT * FROM wvj_h_g OFFSET 0) s WHERE s.a = 5"));
        assertEquals("22012", stateOf("SELECT s.a FROM (SELECT * FROM wvj_h_g ORDER BY g) s"));
        assertEquals("22012", stateOf("SELECT g FROM wvj_h_v"));

        // what it does not settle for itself is decided above
        assertEquals("2", scalar("SELECT g FROM wvj_h_v WHERE a = 5"));
        assertEquals("5|five|2", rows("SELECT * FROM (SELECT * FROM wvj_h_g) s WHERE s.a = 5"));
        assertEquals(2L, num("SELECT count(*) FROM (SELECT * FROM wvj_h_g) s"));
        assertEquals(2L, num("SELECT max(s.g) FROM (SELECT * FROM wvj_h_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT DISTINCT a, k, g FROM wvj_h_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT DISTINCT * FROM wvj_h_g) s WHERE s.a = 5"));
        assertEquals("5", scalar("SELECT s.a FROM (SELECT * FROM wvj_h_g GROUP BY a, k, g) s WHERE s.a = 5"));
        assertEquals("2,2", column("SELECT s.g FROM (SELECT * FROM wvj_h_g UNION ALL SELECT * FROM wvj_h_g) s"
                + " WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvj_h_g UNION SELECT * FROM wvj_h_g) s"
                + " WHERE s.a = 5"));

        // one arm of a set operation answers for the column itself and the other for the relation's
        assertEquals("7", scalar("SELECT s.g FROM (SELECT * FROM wvj_h_g UNION ALL SELECT 1, 'one', 7) s"
                + " WHERE s.a = 1"));
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvj_h_g UNION ALL SELECT 1, 'one', 7) s"
                + " WHERE s.a = 5"));
        assertEquals("22012", stateOf("SELECT s.a, s.g FROM (SELECT * FROM wvj_h_g UNION ALL SELECT 1, 'one', 7) s"
                + " ORDER BY s.a"));
    }

    @Test
    void aColumnExposedUnderAnotherNameIsStillWorkedOutForTheRowsTheQueryKept() throws Exception {
        exec("CREATE TABLE wvj_i_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_i_g (a,k) VALUES (0,'zero'),(5,'five')");

        assertEquals("2", scalar("SELECT s.g FROM (SELECT g, a, k FROM wvj_i_g) s WHERE s.a = 5"));
        assertEquals("2", scalar("SELECT s.gg FROM (SELECT a, k, g AS gg FROM wvj_i_g) s WHERE s.a = 5"));
        // the column the expression reads is exposed under another name
        assertEquals("2", scalar("SELECT s.g FROM (SELECT a AS aa, k, g FROM wvj_i_g) s WHERE s.aa = 5"));
        assertEquals("2|2", rows("SELECT s.g, s.g2 FROM (SELECT a, g, g AS g2 FROM wvj_i_g) s WHERE s.a = 5"));
        // the relation qualifies itself
        assertEquals("2", scalar("SELECT s.g FROM (SELECT * FROM wvj_i_g s1 WHERE s1.a = 5) s"));
        assertEquals("2", scalar("WITH c AS (SELECT * FROM wvj_i_g) SELECT x.g FROM c x WHERE x.a = 5"));
    }

    // What a relation hands on is a value, not a generation expression: a relation built from one
    // carries a plain column that can be written to.

    @Test
    void whatARelationHandsOnIsAValueAndNotAGenerationExpression() throws Exception {
        exec("CREATE TABLE wvj_j_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvj_j_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvj_j_o (a int, note text)");
        exec("INSERT INTO wvj_j_o VALUES (0,'y'),(5,'y')");

        exec("CREATE TABLE wvj_j_c AS SELECT * FROM (SELECT * FROM wvj_j_g) s WHERE s.a = 5");
        assertEquals("5|five|2", rows("SELECT a, k, g FROM wvj_j_c"));
        assertEquals(1, update("INSERT INTO wvj_j_c SELECT 1, 'one', 99"));
        assertEquals("1|one|99 / 5|five|2", rows("SELECT a, k, g FROM wvj_j_c ORDER BY a"));

        assertEquals(1, update("INSERT INTO wvj_j_o SELECT s.a, s.g::text FROM (SELECT * FROM wvj_j_g) s"
                + " WHERE s.a = 5"));
        assertEquals("0|y / 5|2 / 5|y", rows("SELECT a, note FROM wvj_j_o ORDER BY a, note"));
    }

    // Deciding where the column is worked out decides only WHICH rows it is worked out for. The
    // generation expression below cannot raise, so every reading has to carry the right value.

    @Test
    void qualifyingAJoinedRelationNeverChangesTheValueItAnswersWith() throws Exception {
        exec("CREATE TABLE wvj_k_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wvj_k_h VALUES (1,'one'),(2,'two'),(3,'three')");
        exec("CREATE TABLE wvj_k_p (a int, note text)");
        exec("INSERT INTO wvj_k_p VALUES (1,'p1'),(9,'p9')");
        exec("CREATE VIEW wvj_k_hv AS SELECT * FROM wvj_k_h");

        assertEquals("1|10 / 9|NULL", rows("SELECT p.a, s.g FROM wvj_k_p p LEFT JOIN (SELECT * FROM wvj_k_h) s"
                + " ON p.a = s.a WHERE p.a >= 1 ORDER BY p.a"));
        assertEquals("1|10", rows("SELECT p.a, s.g FROM wvj_k_p p JOIN (SELECT * FROM wvj_k_h) s"
                + " ON p.a = s.a AND p.a >= 1 ORDER BY p.a"));
        assertEquals("2|20 / 3|30", rows("SELECT s.x, s.z FROM (SELECT * FROM wvj_k_h) s(x,y,z)"
                + " WHERE s.x >= 2 ORDER BY s.x"));
        assertEquals("one|10 / two|20 / three|30", rows("SELECT s.y, s.z FROM (SELECT * FROM wvj_k_h) s(x,y,z)"
                + " ORDER BY s.z"));
        assertEquals("2|20 / 3|30", rows("SELECT s.x, s.z FROM wvj_k_hv s(x,y,z) WHERE s.x >= 2 ORDER BY s.x"));
        assertEquals("3|60", rows("SELECT count(*), sum(s.z) FROM wvj_k_hv s(x,y,z)"));
        assertEquals("1|10", rows("SELECT o.a, s.g FROM wvj_k_p o, LATERAL (SELECT * FROM wvj_k_h WHERE a = o.a) s"
                + " ORDER BY o.a"));
        assertEquals("1|10 / 9|NULL", rows("SELECT o.a, s.g FROM wvj_k_p o LEFT JOIN LATERAL"
                + " (SELECT * FROM wvj_k_h z WHERE z.a = o.a) s ON true ORDER BY o.a"));
        assertEquals("1|10", rows("SELECT s.a, s.g FROM wvj_k_h s, wvj_k_p o WHERE s.a = o.a AND o.a = 1"));
        assertEquals("1|10", rows("SELECT s.a, s.g FROM (SELECT * FROM wvj_k_h) s, wvj_k_p o"
                + " WHERE s.a = o.a AND o.a = 1"));
        // the row an outer join padded with nulls has no value to work out
        assertEquals("10 / NULL", rows("SELECT s.z FROM wvj_k_p o LEFT JOIN (SELECT * FROM wvj_k_h) s(x,y,z)"
                + " ON o.a = s.x ORDER BY s.z"));
    }

    // ------------------------------------------------------------ what a MERGE works out of the
    // ------------------------------------------------------------ relation it reads from

    @Test
    void aMergeSourcesGeneratedColumnIsWorkedOutForTheRowsTheJoinKept() throws Exception {
        exec("CREATE TABLE wvm_a_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_a_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_a_o (a int, note text)");
        exec("INSERT INTO wvm_a_o VALUES (0,'y'),(5,'y')");

        assertEquals(1, update("MERGE INTO wvm_a_o o USING wvm_a_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("0|y / 5|2", rows("SELECT a, note FROM wvm_a_o ORDER BY a, note"));
        exec("UPDATE wvm_a_o SET note = 'y'");

        // the source read through a query of its own is the same relation
        assertEquals(1, update("MERGE INTO wvm_a_o o USING (SELECT * FROM wvm_a_g) t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("0|y / 5|2", rows("SELECT a, note FROM wvm_a_o ORDER BY a, note"));
        exec("UPDATE wvm_a_o SET note = 'y'");

        // the NOT MATCHED arm reads the source row the ON condition paired with nothing
        assertEquals(2, update("MERGE INTO wvm_a_o o USING wvm_a_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"
                + " WHEN NOT MATCHED THEN INSERT VALUES (t.a, 'n')"));
        assertEquals("0|n / 0|y / 5|2", rows("SELECT a, note FROM wvm_a_o ORDER BY a, note"));
    }

    @Test
    void withNothingNarrowingTheJoinTheMergeSourceIsReadWhole() throws Exception {
        exec("CREATE TABLE wvm_b_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_b_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_b_o (a int, note text)");
        exec("INSERT INTO wvm_b_o VALUES (0,'y'),(5,'y')");

        assertEquals("22012", stateOf("MERGE INTO wvm_b_o o USING wvm_b_g t ON o.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("22012", stateOf("MERGE INTO wvm_b_o o USING (SELECT * FROM wvm_b_g) t ON o.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("0|y / 5|y", rows("SELECT a, note FROM wvm_b_o ORDER BY a, note"));
    }

    // A WHEN NOT MATCHED BY SOURCE arm that acts has to answer every target row whether or not a
    // source row paired with it, so the target is preserved and the source is the side that may be
    // padded away. Nothing above such a join can work out a column generated from a source row.

    @Test
    void aMergeThatMayPadItsSourceAwayWorksOutEveryRowOfIt() throws Exception {
        exec("CREATE TABLE wvm_c_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_c_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_c_o (a int, note text)");
        exec("INSERT INTO wvm_c_o VALUES (0,'y'),(5,'y')");

        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        // an arm that never names the source's generated column raises just the same
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = 'm'"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN NOT MATCHED BY SOURCE THEN DELETE"));
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN DO NOTHING"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING wvm_c_g t ON o.a = t.a AND o.a = 5"
                + " WHEN NOT MATCHED BY SOURCE AND o.note = 'y' THEN UPDATE SET note = 'ns'"));
        assertEquals("22012", stateOf("MERGE INTO wvm_c_o o USING (SELECT * FROM wvm_c_g) t"
                + " ON o.a = t.a AND o.a = 5 WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        // and none of them wrote anything
        assertEquals("0|y / 5|y", rows("SELECT a, note FROM wvm_c_o ORDER BY a, note"));
    }

    @Test
    void aSourceQueryThatDoesNotExposeTheColumnHasNothingToWorkOut() throws Exception {
        exec("CREATE TABLE wvm_d_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_d_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_d_o (a int, note text)");
        exec("INSERT INTO wvm_d_o VALUES (0,'y'),(5,'y')");

        assertEquals(1, update("MERGE INTO wvm_d_o o USING (SELECT a, k FROM wvm_d_g) t"
                + " ON o.a = t.a AND o.a = 5 WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("0|ns / 5|y", rows("SELECT a, note FROM wvm_d_o ORDER BY a, note"));
        exec("UPDATE wvm_d_o SET note = 'y'");

        // an arm that does nothing asks nothing of the join, which stays an inner one
        assertEquals(0, update("MERGE INTO wvm_d_o o USING wvm_d_g t ON o.a = t.a AND o.a = 5"
                + " WHEN NOT MATCHED BY SOURCE THEN DO NOTHING"));
        assertEquals("0|y / 5|y", rows("SELECT a, note FROM wvm_d_o ORDER BY a, note"));
        assertEquals(1, update("MERGE INTO wvm_d_o o USING wvm_d_g t ON o.a = t.a AND o.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = 'm'"
                + " WHEN NOT MATCHED BY SOURCE THEN DO NOTHING"));
        assertEquals("0|y / 5|m", rows("SELECT a, note FROM wvm_d_o ORDER BY a, note"));
    }

    @Test
    void aRestrictionOnTheSourceItselfNarrowsTheSourcesScan() throws Exception {
        exec("CREATE TABLE wvm_e_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_e_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_e_o (a int, note text)");
        exec("INSERT INTO wvm_e_o VALUES (0,'y'),(5,'y')");

        assertEquals(2, update("MERGE INTO wvm_e_o o USING wvm_e_g t ON o.a = t.a AND t.a = 5"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("0|ns / 5|2", rows("SELECT a, note FROM wvm_e_o ORDER BY a, note"));
        exec("UPDATE wvm_e_o SET note = 'y'");

        // a restriction on any column of the source narrows it
        assertEquals(1, update("MERGE INTO wvm_e_o o USING wvm_e_g t ON o.a = t.a AND t.k = 'five'"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("0|ns / 5|y", rows("SELECT a, note FROM wvm_e_o ORDER BY a, note"));
    }

    @Test
    void aWritingStatementThatNeverNamesTheColumnNeverWorksItOut() throws Exception {
        exec("CREATE TABLE wvm_f_g (a int, k text, g int GENERATED ALWAYS AS (10/a) VIRTUAL)");
        exec("INSERT INTO wvm_f_g (a,k) VALUES (0,'zero'),(5,'five')");
        exec("CREATE TABLE wvm_f_o (a int, note text)");
        exec("INSERT INTO wvm_f_o VALUES (0,'y'),(5,'y')");

        assertEquals(2, update("UPDATE wvm_f_o o SET note = 'x' FROM (SELECT * FROM wvm_f_g) s WHERE s.a = o.a"));
        assertEquals("0|x / 5|x", rows("SELECT a, note FROM wvm_f_o ORDER BY a, note"));
    }

    @Test
    void aMergeAndAnUpdateFromKeepTheValueTheColumnCarries() throws Exception {
        exec("CREATE TABLE wvm_g_h (a int, k text, g int GENERATED ALWAYS AS (a*10) VIRTUAL)");
        exec("INSERT INTO wvm_g_h VALUES (1,'one'),(2,'two'),(3,'three')");
        exec("CREATE TABLE wvm_g_p (a int, note text)");
        exec("INSERT INTO wvm_g_p VALUES (1,'p1'),(9,'p9')");

        assertEquals(2, update("MERGE INTO wvm_g_p o USING wvm_g_h t ON o.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET note = 'ns'"));
        assertEquals("1|10 / 9|ns", rows("SELECT a, note FROM wvm_g_p ORDER BY a"));
        exec("UPDATE wvm_g_p SET note = 'p' || a::text");

        assertEquals(1, update("MERGE INTO wvm_g_p o USING (SELECT * FROM wvm_g_h) t ON o.a = t.a"
                + " WHEN MATCHED THEN UPDATE SET note = t.g::text"));
        assertEquals("1|10 / 9|p9", rows("SELECT a, note FROM wvm_g_p ORDER BY a"));
        exec("UPDATE wvm_g_p SET note = 'p' || a::text");

        assertEquals(1, update("UPDATE wvm_g_p o SET note = s.g::text FROM (SELECT * FROM wvm_g_h) s"
                + " WHERE s.a = o.a"));
        assertEquals("1|10 / 9|p9", rows("SELECT a, note FROM wvm_g_p ORDER BY a"));
        exec("UPDATE wvm_g_p SET note = 'p' || a::text");

        assertEquals(1, update("DELETE FROM wvm_g_p o USING wvm_g_h t WHERE t.a = o.a AND o.a = 1 AND t.g = 10"));
        assertEquals("9|p9", rows("SELECT a, note FROM wvm_g_p ORDER BY a"));
    }

    // ------------------------------------------------------------ which of a statement's faults
    // ------------------------------------------------------------ is reported, and which DEFAULT
    // ------------------------------------------------------------ the complaint points at

    // A statement can be wrong in more than one way at once and exactly one fault is reported: the
    // first met while the statement is read. Within an expression the reading is left to right, so
    // the same two mistakes written in the two orders give two different errors — at the same
    // offset both times, because the two faults stand in the same place. The relations below are
    // named to a fixed width because Position is a character offset into the statement text.

    @Test
    void theFaultStandingFirstInTheReadingIsTheOneReported() throws Exception {
        exec("CREATE TABLE wdko5_zz (a text DEFAULT 'D', b int)");

        // the two operands of one operator, in both orders
        assertMissingColumnIsReported("SELECT a FROM wdko5_zz WHERE nosuchcol = DEFAULT", 30);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko5_zz WHERE DEFAULT = nosuchcol", 30);
        // a call's arguments are read before the call itself is looked up
        assertMissingColumnIsReported("SELECT a FROM wdko5_zz WHERE upper(nosuchcol) = DEFAULT", 36);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko5_zz WHERE upper(DEFAULT) = nosuchcol", 36);
        assertMissingColumnIsReported("SELECT concat(nosuchcol, DEFAULT) FROM wdko5_zz", 15);
        assertMisplacedKeywordIsReported("SELECT concat(DEFAULT, nosuchcol) FROM wdko5_zz", 15);
        // parentheses reorder nothing
        assertMissingColumnIsReported("SELECT a FROM wdko5_zz WHERE (b + nosuchcol) = (1 + DEFAULT)", 35);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko5_zz WHERE (b + DEFAULT) = (1 + nosuchcol)", 35);

        // and the clauses in the order the query is read: the select list, then WHERE, then HAVING,
        // then the sort clause, and the grouping items last of all
        assertMissingColumnIsReported("SELECT nosuchcol FROM wdko5_zz WHERE DEFAULT", 8);
        assertMisplacedKeywordIsReported("SELECT DEFAULT FROM wdko5_zz WHERE nosuchcol", 8);
        assertMissingColumnIsReported("SELECT a FROM wdko5_zz GROUP BY a HAVING nosuchcol AND DEFAULT", 42);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko5_zz GROUP BY a HAVING DEFAULT AND nosuchcol", 42);
        assertMissingColumnIsReported("SELECT a FROM wdko5_zz WHERE nosuchcol ORDER BY DEFAULT", 30);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko5_zz WHERE DEFAULT ORDER BY nosuchcol", 30);
        assertMissingColumnIsReported("SELECT nosuchcol FROM wdko5_zz GROUP BY DEFAULT", 8);
        assertMisplacedKeywordIsReported("SELECT DEFAULT FROM wdko5_zz GROUP BY nosuchcol", 8);
    }

    @Test
    void theKeywordReportedIsTheOneThatWasRefusedAndNotTheFirstWritten() throws Exception {
        exec("CREATE TABLE wdko6_zz (a text DEFAULT 'D', b int)");

        // a legal DEFAULT standing in front of the refused one does not count
        assertMisplacedKeywordIsReported("INSERT INTO wdko6_zz (a,b) VALUES (DEFAULT,1) RETURNING DEFAULT", 57);
        assertMisplacedKeywordIsReported(
                "INSERT INTO wdko6_zz (a,b) VALUES (DEFAULT, DEFAULT) RETURNING DEFAULT", 64);
        assertMisplacedKeywordIsReported("UPDATE wdko6_zz SET a = DEFAULT RETURNING DEFAULT", 43);
        assertMisplacedKeywordIsReported("UPDATE wdko6_zz SET a = DEFAULT, b = DEFAULT + 1", 38);

        // a refused one standing in front of a legal one
        assertMisplacedKeywordIsReported("UPDATE wdko6_zz SET a = DEFAULT || 'x', b = DEFAULT", 25);
        assertMisplacedKeywordIsReported(
                "INSERT INTO wdko6_zz (a,b) VALUES (DEFAULT || 'x', 1) RETURNING DEFAULT", 36);

        // two refused ones: the one read first, which is not the one written first
        assertMisplacedKeywordIsReported("UPDATE wdko6_zz SET a = DEFAULT || 'x' WHERE b = DEFAULT", 50);
        assertMisplacedKeywordIsReported("SELECT DEFAULT FROM (SELECT DEFAULT) x", 29);

        // and none of them wrote anything
        assertEquals(0L, num("SELECT count(*) FROM wdko6_zz"));
    }

    // The order the clauses are read in is not the order the text spells them: a query's WITH items
    // and its FROM items are read before anything it selects, an UPDATE's FROM and WHERE before the
    // values it assigns and the columns it assigns to, while an INSERT's column list is resolved
    // before its values. The relations a clause names are resolved first of all.

    @Test
    void theClausesAreReadInTheOrderPostgresReadsThemNotTheOrderTheTextSpells() throws Exception {
        exec("CREATE TABLE wdko7_zz (a text DEFAULT 'D', b int)");
        exec("CREATE TABLE wdko7_uu (b int, c text)");

        // a FROM item is read before the select list
        assertMisplacedKeywordIsReported("SELECT DEFAULT FROM wdko7_zz JOIN wdko7_uu ON DEFAULT", 47);
        assertMisplacedKeywordIsReported("SELECT nosuchcol FROM wdko7_zz JOIN wdko7_uu ON DEFAULT", 49);
        assertMisplacedKeywordIsReported("SELECT a FROM wdko7_zz t1, (SELECT DEFAULT) s WHERE DEFAULT", 36);
        assertMisplacedKeywordIsReported("UPDATE wdko7_zz SET a = DEFAULT || 'x' FROM (SELECT DEFAULT) s", 53);
        // and a WITH item before the query that reads from it
        assertMisplacedKeywordIsReported("WITH c AS (SELECT DEFAULT) SELECT DEFAULT FROM wdko7_zz", 19);
        // DISTINCT ON is read after WHERE, however early it is written
        assertMisplacedKeywordIsReported("SELECT DISTINCT ON (DEFAULT) a FROM wdko7_zz WHERE DEFAULT", 52);

        // the range table is built before any of it, so a relation that is not there wins
        String joined = "SELECT a FROM wdko7_zz JOIN wdko7_qq ON DEFAULT";
        assertEquals("42P01", stateOf(joined));
        assertEquals("relation \"wdko7_qq\" does not exist", messageOf(joined));
        assertEquals(29, positionOf(joined));
        String withItem = "WITH w AS (SELECT DEFAULT FROM wdko7_qq) SELECT 1";
        assertEquals("42P01", stateOf(withItem));
        assertEquals("relation \"wdko7_qq\" does not exist", messageOf(withItem));
        assertEquals(32, positionOf(withItem));
        // a column of a WITH item is resolved while the item is read, before a keyword in the query
        // standing around it
        assertMissingColumnIsReported("WITH w AS (SELECT nosuchcol FROM wdko7_zz) SELECT DEFAULT", 19);

        // an UPDATE settles what every assignment writes before it resolves any column written to
        assertWrittenColumnIsReported("UPDATE wdko7_zz SET nosuchcol = DEFAULT", 21);
        assertMisplacedKeywordIsReported("UPDATE wdko7_zz SET nosuchcol = DEFAULT || 'x'", 33);
        assertMisplacedKeywordIsReported("UPDATE wdko7_zz SET nosuchcol = 1, a = DEFAULT || 'x'", 40);
        // an INSERT is the other way round: its column list is resolved before its values
        assertWrittenColumnIsReported("INSERT INTO wdko7_zz (nosuchcol) VALUES (DEFAULT || 'x')", 23);

        // none of them wrote anything, and the legal placements are untouched
        assertEquals(0L, num("SELECT count(*) FROM wdko7_zz"));
        exec("INSERT INTO wdko7_zz (a,b) VALUES (DEFAULT, 1)");
        assertEquals(1, update("UPDATE wdko7_zz SET a = DEFAULT, b = 2"));
        assertEquals("D|2", rows("SELECT a, b FROM wdko7_zz"));
    }

    /** The column is reported: it stands earlier in the reading than the keyword does. */
    private static void assertMissingColumnIsReported(String sql, int position) {
        assertEquals("42703", stateOf(sql), sql);
        assertEquals("column \"nosuchcol\" does not exist", messageOf(sql), sql);
        assertEquals(position, positionOf(sql), sql);
    }

    private static void assertMisplacedKeywordIsReported(String sql, int position) {
        assertEquals("42601", stateOf(sql), sql);
        assertEquals("DEFAULT is not allowed in this context", messageOf(sql), sql);
        assertEquals(position, positionOf(sql), sql);
    }

    /**
     * A write names the relation alongside the column, because a column list is only wrong relative
     * to the relation it was written for.
     */
    private static void assertWrittenColumnIsReported(String sql, int position) {
        assertEquals("42703", stateOf(sql), sql);
        assertEquals("column \"nosuchcol\" of relation \"wdko7_zz\" does not exist", messageOf(sql), sql);
        assertEquals(position, positionOf(sql), sql);
    }

    // ------------------------------------------------------------ a subquery reads the row it
    // ------------------------------------------------------------ stands in

    // A subquery is written inside the scope of the query around it, so a name its own FROM clause
    // has not got is resolved against that query — in its select list exactly as in its WHERE.

    @Test
    void aSubquerySelectListReadsTheRowTheSubqueryStandsBeside() throws Exception {
        exec("CREATE TABLE sqw5_a_o (i int, j text)");
        exec("CREATE TABLE sqw5_a_x (k int)");
        exec("INSERT INTO sqw5_a_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_a_x VALUES (7)");

        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_a_x x) FROM sqw5_a_o o ORDER BY 1"));
        // a bare name the subquery's own relations have not got is the enclosing query's
        assertEquals("a,b", column("SELECT (SELECT j FROM sqw5_a_x x) FROM sqw5_a_o o ORDER BY 1"));
        assertEquals("8,9", column("SELECT (SELECT o.i + x.k FROM sqw5_a_x x) FROM sqw5_a_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_a_x x GROUP BY o.i) FROM sqw5_a_o o ORDER BY 1"));
        assertEquals("7,7", column("SELECT (SELECT x.k FROM sqw5_a_x x ORDER BY o.i) FROM sqw5_a_o o"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_a_x x LIMIT 1) FROM sqw5_a_o o ORDER BY 1"));
        // a subquery that matches nothing still answers null, once per row of the query around it
        assertEquals("NULL / NULL", rows("SELECT (SELECT o.i FROM sqw5_a_x x WHERE false) FROM sqw5_a_o o"));
    }

    @Test
    void theSameOuterReferenceReadsThroughExistsInAnyAllAndArray() throws Exception {
        exec("CREATE TABLE sqw5_b_o (i int, j text)");
        exec("CREATE TABLE sqw5_b_x (k int)");
        exec("INSERT INTO sqw5_b_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_b_x VALUES (7)");

        assertEquals("1,2", column("SELECT o.i FROM sqw5_b_o o WHERE EXISTS (SELECT o.i FROM sqw5_b_x x)"
                + " ORDER BY 1"));
        assertEquals("1,2", column("SELECT s.i FROM sqw5_b_o s WHERE s.i IN (SELECT s.i FROM sqw5_b_x)"
                + " ORDER BY 1"));
        assertEquals("1,2", column("SELECT o.i FROM sqw5_b_o o WHERE o.i = ANY (SELECT o.i FROM sqw5_b_x x)"
                + " ORDER BY 1"));
        assertEquals("1,2", column("SELECT o.i FROM sqw5_b_o o WHERE o.i > ALL (SELECT o.i - 1 FROM sqw5_b_x x)"
                + " ORDER BY 1"));
        assertEquals("{1},{2}", column("SELECT ARRAY(SELECT o.i FROM sqw5_b_x x) FROM sqw5_b_o o ORDER BY 1"));
    }

    @Test
    void aLateralItemReadsTheItemToItsLeftFromItsOwnSelectList() throws Exception {
        exec("CREATE TABLE sqw5_c_o (i int, j text)");
        exec("CREATE TABLE sqw5_c_x (k int)");
        exec("INSERT INTO sqw5_c_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_c_x VALUES (7)");

        assertEquals("1,2", column("SELECT b.z FROM sqw5_c_o a, LATERAL (SELECT a.i AS z FROM sqw5_c_x x) b"
                + " ORDER BY 1"));
        assertEquals("1,2", column("SELECT b.z FROM sqw5_c_o a JOIN LATERAL (SELECT a.i AS z FROM sqw5_c_x x) b"
                + " ON true ORDER BY 1"));
        assertEquals("1|7 / 2|7", rows("SELECT b.i, b.k FROM sqw5_c_o a,"
                + " LATERAL (SELECT a.*, x.k FROM sqw5_c_x x) b ORDER BY 1"));
        // and it reaches that item through a query of its own
        assertEquals("1,2", column("SELECT b.z FROM sqw5_c_o a, LATERAL (SELECT (SELECT a.i) AS z) b ORDER BY 1"));
        assertEquals("1,2", column("SELECT b.z FROM sqw5_c_o a,"
                + " LATERAL (SELECT s.q AS z FROM (SELECT a.i AS q) s) b ORDER BY 1"));
    }

    @Test
    void theInnermostQueryANameBelongsToIsTheOneThatAnswersIt() throws Exception {
        exec("CREATE TABLE sqw5_d_o (i int, j text)");
        exec("CREATE TABLE sqw5_d_x (k int)");
        exec("CREATE TABLE sqw5_d_y (i int)");
        exec("INSERT INTO sqw5_d_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_d_x VALUES (7)");
        exec("INSERT INTO sqw5_d_y VALUES (99)");

        // the reference reaches out through as many levels as it has to
        assertEquals("1,2", column("SELECT (SELECT (SELECT o.i FROM sqw5_d_x y) FROM sqw5_d_x x)"
                + " FROM sqw5_d_o o ORDER BY 1"));
        assertEquals("7,7", column("SELECT (SELECT (SELECT x.k FROM sqw5_d_y y) FROM sqw5_d_x x) FROM sqw5_d_o o"));
        assertEquals("1,2", column("SELECT (SELECT s.z FROM (SELECT o.i AS z FROM sqw5_d_x x) s)"
                + " FROM sqw5_d_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (WITH w AS (SELECT o.i AS z FROM sqw5_d_x x) SELECT w.z FROM w)"
                + " FROM sqw5_d_o o ORDER BY 1"));

        // but a query that has the name answers it itself
        assertEquals("99,99", column("SELECT (SELECT i FROM sqw5_d_y x) FROM sqw5_d_o o"));
        assertEquals("99,99", column("SELECT (SELECT x.i FROM sqw5_d_y x) FROM sqw5_d_o o"));
        assertEquals("7,7", column("SELECT (SELECT o.k FROM sqw5_d_x o) FROM sqw5_d_o o"));
        // and a qualifier the inner query answers to is not looked for outside
        String shadowed = "SELECT (SELECT o.i FROM sqw5_d_x o) FROM sqw5_d_o o";
        assertEquals("42703", stateOf(shadowed));
        assertEquals("column o.i does not exist", messageOf(shadowed));
    }

    @Test
    void aWholeRowAndAQualifiedStarReadTheEnclosingQueryToo() throws Exception {
        exec("CREATE TABLE sqw5_e_o (i int, j text)");
        exec("CREATE TABLE sqw5_e_x (k int)");
        exec("CREATE TABLE sqw5_e_y (i int)");
        exec("INSERT INTO sqw5_e_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_e_x VALUES (7)");
        exec("INSERT INTO sqw5_e_y VALUES (99)");

        assertEquals("(1,a) / (2,b)", rows("SELECT (SELECT o FROM sqw5_e_x x) FROM sqw5_e_o o ORDER BY 1"));
        assertEquals("99,99", column("SELECT (SELECT y.* FROM sqw5_e_x x) AS c FROM sqw5_e_o o, sqw5_e_y y"));
        assertEquals("t,t", column("SELECT EXISTS (SELECT o.* FROM sqw5_e_x x) FROM sqw5_e_o o"));
        // a star of more than one column is still too wide for one value
        String wide = "SELECT (SELECT o.* FROM sqw5_e_x x) FROM sqw5_e_o o";
        assertEquals("42601", stateOf(wide));
        assertEquals("subquery must return only one column", messageOf(wide));
        // standing where a value is expected, the star is the outer row
        assertEquals("1,1", column("SELECT (SELECT num_nonnulls(o.*) FROM sqw5_e_x x) FROM sqw5_e_o o"));
        assertEquals("a,b", column("SELECT (SELECT to_jsonb(o.*)->>'j' FROM sqw5_e_x x) FROM sqw5_e_o o ORDER BY 1"));
        assertEquals("1", scalar("SELECT (SELECT count(o.*) FROM sqw5_e_x x) FROM sqw5_e_o o WHERE o.i = 1"));
        // and such a subquery is labelled after the relation the star names
        assertEquals("k", labelOf("SELECT (SELECT a.* FROM sqw5_e_x a, sqw5_e_y b) FROM sqw5_e_o o"));
        assertEquals("i", labelOf("SELECT (SELECT b.* FROM sqw5_e_x a, sqw5_e_y b) FROM sqw5_e_o o"));
        assertEquals("k", labelOf("SELECT (SELECT x.* FROM sqw5_e_x x JOIN sqw5_e_y y ON true) FROM sqw5_e_o o"));
    }

    @Test
    void theWritingPathsGiveTheSubqueryTheSameScope() throws Exception {
        exec("CREATE TABLE sqw5_f_o (i int, j text)");
        exec("CREATE TABLE sqw5_f_x (k int)");
        exec("CREATE TABLE sqw5_f_y (i int)");
        exec("INSERT INTO sqw5_f_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_f_x VALUES (7)");

        assertEquals("1 / 2", rows("UPDATE sqw5_f_o o SET j = j RETURNING (SELECT o.i FROM sqw5_f_x x)"));
        assertEquals("", rows("DELETE FROM sqw5_f_o d WHERE d.i = 999 RETURNING (SELECT d.i FROM sqw5_f_x x)"));
        assertEquals(2, update("INSERT INTO sqw5_f_y SELECT (SELECT o.i FROM sqw5_f_x x) FROM sqw5_f_o o"));
        assertEquals("1,2", column("SELECT i FROM sqw5_f_y ORDER BY i"));
        assertEquals(2, update("UPDATE sqw5_f_o o SET i = (SELECT o.i + 100 FROM sqw5_f_x x)"));
        assertEquals("101,102", column("SELECT i FROM sqw5_f_o ORDER BY i"));
        exec("CREATE VIEW sqw5_f_v AS SELECT (SELECT o.i FROM sqw5_f_x x) AS c FROM sqw5_f_o o");
        assertEquals("101,102", column("SELECT c FROM sqw5_f_v ORDER BY c"));
    }

    @Test
    void aNameNoQueryInScopeAnswersToIsStillRefused() throws Exception {
        exec("CREATE TABLE sqw5_g_o (i int, j text)");
        exec("CREATE TABLE sqw5_g_x (k int)");
        exec("CREATE TABLE sqw5_g_y (i int)");
        exec("INSERT INTO sqw5_g_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_g_x VALUES (7)");
        exec("INSERT INTO sqw5_g_y VALUES (99)");

        String nope = "SELECT (SELECT nope.i FROM sqw5_g_x x) FROM sqw5_g_o o";
        assertEquals("42P01", stateOf(nope));
        assertEquals("missing FROM-clause entry for table \"nope\"", messageOf(nope));
        String lateralNope = "SELECT b.i FROM sqw5_g_o a, LATERAL (SELECT s.* FROM sqw5_g_x x) b";
        assertEquals("42P01", stateOf(lateralNope));
        assertEquals("missing FROM-clause entry for table \"s\"", messageOf(lateralNope));
        String noColumn = "SELECT (SELECT o.nosuch FROM sqw5_g_x x) FROM sqw5_g_o o";
        assertEquals("42703", stateOf(noColumn));
        assertEquals("column o.nosuch does not exist", messageOf(noColumn));
        String ambiguous = "SELECT (SELECT i FROM sqw5_g_o a, sqw5_g_y b) FROM sqw5_g_o o";
        assertEquals("42702", stateOf(ambiguous));
        assertEquals("column reference \"i\" is ambiguous", messageOf(ambiguous));

        // an alias renames the relation for the whole query, so its own name reaches nothing
        String renamed = "SELECT (SELECT sqw5_g_o.i FROM sqw5_g_x x) FROM sqw5_g_o o";
        assertEquals("42P01", stateOf(renamed));
        assertEquals("invalid reference to FROM-clause entry for table \"sqw5_g_o\"", messageOf(renamed));
        assertEquals("Perhaps you meant to reference the table alias \"o\".", hintOf(renamed));
        assertEquals("42P01", stateOf("SELECT b.z FROM sqw5_g_o o,"
                + " LATERAL (SELECT sqw5_g_o.i AS z FROM sqw5_g_x x) b"));
        // with no alias in the way the relation answers to its own name
        assertEquals("1,2", column("SELECT (SELECT sqw5_g_o.i FROM sqw5_g_x x) FROM sqw5_g_o ORDER BY 1"));
    }

    @Test
    void theRelationANameCouldNotReachIsNamedInTheDetail() throws Exception {
        exec("CREATE TABLE sqw5_h_o (i int, j text)");
        exec("CREATE TABLE sqw5_h_x (k int)");
        exec("CREATE TABLE sqw5_h_n (ix int)");
        exec("INSERT INTO sqw5_h_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_h_x VALUES (7)");
        exec("INSERT INTO sqw5_h_n VALUES (5)");

        assertEquals("There is a column named \"i\" in table \"o\", but it cannot be referenced"
                        + " from this part of the query.",
                detailOf("SELECT (SELECT o.i FROM sqw5_h_n o) FROM sqw5_h_o o"));
        assertEquals("There is a column named \"ix\" in table \"o\", but it cannot be referenced"
                        + " from this part of the query.",
                detailOf("SELECT (SELECT o.ix FROM sqw5_h_o o) FROM sqw5_h_n o"));
        // a column no query in scope holds is refused with nothing to add
        assertEquals(null, detailOf("SELECT (SELECT o.nope FROM sqw5_h_n o) FROM sqw5_h_o o"));
    }

    @Test
    void aSetOperationARecursiveItemAndAValuesListReadTheOuterRowTheSameWay() throws Exception {
        exec("CREATE TABLE sqw5_i_o (i int, j text)");
        exec("CREATE TABLE sqw5_i_x (k int)");
        exec("INSERT INTO sqw5_i_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_i_x VALUES (7)");

        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_i_x x UNION ALL SELECT o.i FROM sqw5_i_x y"
                + " LIMIT 1) AS c FROM sqw5_i_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_i_x x UNION SELECT 9 ORDER BY 1 LIMIT 1)"
                + " AS c FROM sqw5_i_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_i_x x INTERSECT SELECT o.i FROM sqw5_i_x y)"
                + " AS c FROM sqw5_i_o o ORDER BY 1"));
        assertEquals("3,3", column("SELECT (WITH RECURSIVE w(n) AS (SELECT o.i UNION ALL SELECT n+1 FROM w"
                + " WHERE n < 3) SELECT max(n) FROM w) FROM sqw5_i_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM (VALUES (1)) v(c)) FROM sqw5_i_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT (SELECT o.i FROM sqw5_i_x x LIMIT (SELECT 1)) FROM sqw5_i_o o"
                + " ORDER BY 1"));
        // and a subquery standing for one value is still held to one row
        String many = "SELECT (SELECT o.i FROM generate_series(1,2) g) FROM sqw5_i_o o";
        assertEquals("21000", stateOf(many));
        assertEquals("more than one row returned by a subquery used as an expression", messageOf(many));
    }

    @Test
    void theOuterRowReachesASubqueryInEveryClauseTheQueryHas() throws Exception {
        exec("CREATE TABLE sqw5_j_o (i int, j text)");
        exec("CREATE TABLE sqw5_j_x (k int)");
        exec("CREATE TABLE sqw5_j_y (i int)");
        exec("INSERT INTO sqw5_j_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_j_x VALUES (7)");
        exec("INSERT INTO sqw5_j_y VALUES (99)");

        assertEquals("1,2", column("SELECT o.i FROM sqw5_j_o o GROUP BY o.i"
                + " HAVING (SELECT o.i FROM sqw5_j_x x) > 0 ORDER BY 1"));
        assertEquals("2,1", column("SELECT o.i FROM sqw5_j_o o ORDER BY (SELECT o.i FROM sqw5_j_x x) DESC"));
        assertEquals("1,2", column("SELECT CASE WHEN true THEN (SELECT o.i FROM sqw5_j_x x) ELSE 0 END"
                + " FROM sqw5_j_o o ORDER BY 1"));
        assertEquals("1,2", column("SELECT abs((SELECT o.i FROM sqw5_j_x x)) FROM sqw5_j_o o ORDER BY 1"));
        assertEquals(2L, num("SELECT max((SELECT o.i FROM sqw5_j_x x)) FROM sqw5_j_o o"));
        assertEquals("1,2", column("SELECT DISTINCT ON ((SELECT o.i FROM sqw5_j_x x)) o.i FROM sqw5_j_o o"
                + " ORDER BY (SELECT o.i FROM sqw5_j_x x), o.i"));
        assertEquals("1,2", column("SELECT DISTINCT (SELECT o.i FROM sqw5_j_x x) FROM sqw5_j_o o ORDER BY 1"));
        assertEquals("1,2,7", column("SELECT (SELECT o.i FROM sqw5_j_x x) FROM sqw5_j_o o"
                + " UNION ALL SELECT k FROM sqw5_j_x ORDER BY 1"));
        assertEquals("a7,b7", column("SELECT (SELECT o.j || x.k::text FROM sqw5_j_x x) FROM sqw5_j_o o ORDER BY 1"));
        // the row an outer join padded with nulls is the row the subquery reads
        assertEquals("NULL / NULL", rows("SELECT (SELECT y.i FROM sqw5_j_x x) FROM sqw5_j_o o"
                + " LEFT JOIN sqw5_j_y y ON false ORDER BY 1"));
    }

    @Test
    void aGroupedQueryLendsTheSubqueryOnlyWhatItGroupedBy() throws Exception {
        exec("CREATE TABLE sqw5_k_o (i int, j text)");
        exec("CREATE TABLE sqw5_k_x (k int)");
        exec("CREATE TABLE sqw5_k_y (i int)");
        exec("INSERT INTO sqw5_k_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_k_x VALUES (7)");
        exec("INSERT INTO sqw5_k_y VALUES (99)");

        String ungrouped = "SELECT count(*), (SELECT o.i FROM sqw5_k_x x) FROM sqw5_k_o o";
        assertEquals("42803", stateOf(ungrouped));
        assertEquals("subquery uses ungrouped column \"o.i\" from outer query", messageOf(ungrouped));
        String ungroupedHaving = "SELECT count(*) FROM sqw5_k_o o GROUP BY o.i"
                + " HAVING (SELECT o.j FROM sqw5_k_x x) IS NOT NULL";
        assertEquals("42803", stateOf(ungroupedHaving));
        assertEquals("subquery uses ungrouped column \"o.j\" from outer query", messageOf(ungroupedHaving));
        assertEquals("1|1 / 1|2", rows("SELECT count(*), (SELECT o.i FROM sqw5_k_x x) FROM sqw5_k_o o"
                + " GROUP BY o.i ORDER BY 2"));
        // a bare name two relations of the enclosing query answer to is ambiguous there as here
        assertEquals("42702", stateOf("SELECT (SELECT i FROM sqw5_k_x x) FROM sqw5_k_o o, sqw5_k_y y"));
    }

    @Test
    void aRelationBesideThisOneIsOutOfScopeWithoutLateral() throws Exception {
        exec("CREATE TABLE sqw5_l_o (i int, j text)");
        exec("CREATE TABLE sqw5_l_x (k int)");
        exec("INSERT INTO sqw5_l_o VALUES (1,'a'),(2,'b')");
        exec("INSERT INTO sqw5_l_x VALUES (7)");

        String beside = "SELECT s.z FROM sqw5_l_o o, (SELECT o.i AS z FROM sqw5_l_x x) s";
        assertEquals("42P01", stateOf(beside));
        assertEquals("invalid reference to FROM-clause entry for table \"o\"", messageOf(beside));
        assertEquals("There is an entry for table \"o\", but it cannot be referenced from this part"
                + " of the query.", detailOf(beside));
        assertEquals("To reference that table, you must mark this subquery with LATERAL.", hintOf(beside));
        assertEquals("42P01", stateOf("SELECT s.z FROM sqw5_l_o o JOIN (SELECT o.i AS z FROM sqw5_l_x x) s"
                + " ON true"));
        String item = "WITH w AS (SELECT o.i AS z FROM sqw5_l_x x) SELECT w.z FROM sqw5_l_o o, w";
        assertEquals("42P01", stateOf(item));
        assertEquals("missing FROM-clause entry for table \"o\"", messageOf(item));
        // the same query written where the enclosing query really is above answers
        assertEquals("1,2", column("SELECT o.i FROM sqw5_l_o o WHERE o.i IN"
                + " (SELECT s.z FROM (SELECT o.i AS z FROM sqw5_l_x x) s) ORDER BY 1"));
    }

    // ------------------------------------------------------------ two sessions at once

    /** What a statement run on a session of its own answered, and whether it had to wait. */
    private static final class Waited {
        boolean stillRunning;
        int count = -1;
        String rows = "";
        String state = "OK";
        String message;
    }

    private interface Body {
        void run() throws Exception;
    }

    /**
     * Runs {@code holding} in a transaction a second session does not finish, runs {@code body}
     * while that transaction is open, and rolls the second session back afterwards.
     */
    private static void whileAnotherSessionHolds(String[] holding, Body body) throws Exception {
        try (Connection holder = open()) {
            try (Statement s = holder.createStatement()) {
                s.execute("BEGIN");
                for (String sql : holding) s.execute(sql);
            }
            body.run();
            try (Statement s = holder.createStatement()) {
                s.execute("ROLLBACK");
            }
        }
    }

    private static Waited whileAnotherSessionHolds(String[] holding, String waiting, String ending)
            throws Exception {
        return whileAnotherSessionHolds(new String[0], holding, waiting, ending);
    }

    /**
     * Runs {@code waiterFirst} on a session of its own, opens a transaction on a second session and
     * runs {@code holding} in it, then starts {@code waiting} back on the first session and lets it
     * run for a grace period. The holder is then finished with {@code ending} — COMMIT or ROLLBACK
     * — and the answer says what the waiting statement did and whether it was still running when
     * the holder was told to finish.
     */
    private static Waited whileAnotherSessionHolds(String[] waiterFirst, String[] holding,
                                                   String waiting, String ending) throws Exception {
        Waited answer = new Waited();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection waiter = open(); Connection holder = open()) {
            try (Statement s = waiter.createStatement()) {
                for (String sql : waiterFirst) s.execute(sql);
            }
            try (Statement s = holder.createStatement()) {
                s.execute("BEGIN");
                for (String sql : holding) s.execute(sql);
            }
            CountDownLatch started = new CountDownLatch(1);
            Future<?> pending = pool.submit(new Callable<Void>() {
                @Override
                public Void call() {
                    try (Statement s = waiter.createStatement()) {
                        started.countDown();
                        boolean isResultSet = s.execute(waiting);
                        if (isResultSet) {
                            try (ResultSet rs = s.getResultSet()) {
                                answer.rows = readRows(rs);
                            }
                        } else {
                            answer.count = s.getUpdateCount();
                        }
                    } catch (SQLException e) {
                        answer.state = e.getSQLState();
                        ServerErrorMessage fields = e instanceof PSQLException
                                ? ((PSQLException) e).getServerErrorMessage() : null;
                        answer.message = fields != null ? fields.getMessage() : e.getMessage();
                    }
                    return null;
                }
            });
            assertTrue(started.await(10, TimeUnit.SECONDS),
                    "the second session never started: " + waiting);
            long deadline = System.currentTimeMillis() + GRACE_MS;
            while (!pending.isDone() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            answer.stillRunning = !pending.isDone();
            try (Statement s = holder.createStatement()) {
                s.execute(ending);
            }
            pending.get(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        return answer;
    }
}
