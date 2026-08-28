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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A row trigger belongs to the relation the row is really stored in, and a MERGE reads its target
 * the way an UPDATE and a DELETE read theirs.
 *
 * <p>Neither held. A FOR EACH ROW trigger written on a partition never ran for a statement that
 * named the partitioned table above it, so the guards a partition kept for itself were skipped by
 * every write that came through the parent, including the BEFORE DELETE and BEFORE INSERT pair a
 * cross-partition UPDATE is carried out as. And MERGE scanned the relation as it stood rather than
 * as the other transactions had committed it: it answered at once while another session held a row
 * it would act on, paired its source against values nobody had committed, and so inserted a key
 * that session was about to give back and passed over one it had only just taken.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class PartitionTriggersAndMergeConcurrencyTest {

    /** How long a statement is given to reach the row it is expected to block on. */
    private static final long BLOCKED_MS = 600;

    static Memgres memgres;
    static Connection conn;
    static ExecutorService pool;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        pool = Executors.newCachedThreadPool();
        exec("CREATE TABLE ptg_log (n serial PRIMARY KEY, m text)");
        exec("CREATE FUNCTION ptg_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_NAME || '/' || TG_WHEN || '/' || TG_OP"
                + " || '/' || TG_LEVEL || '/' || TG_TABLE_NAME);"
                + " IF TG_LEVEL = 'STATEMENT' THEN RETURN NULL; END IF;"
                + " IF TG_OP = 'DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$");
        exec("CREATE FUNCTION ptg_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES ('VETO/' || TG_NAME || '/' || TG_OP"
                + " || '/' || TG_TABLE_NAME); RETURN NULL; END $$");
        exec("CREATE FUNCTION ptg_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.s := NEW.s || '+m'; RETURN NEW; END $$");
        exec("CREATE FUNCTION ptg_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " IF NEW.i > 10 THEN RETURN NULL; END IF; RETURN NEW; END $$");
        exec("CREATE FUNCTION ptg_cnote() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " IF TG_LEVEL = 'ROW' THEN"
                + "  INSERT INTO ptg_log(m) VALUES (TG_NAME || '/' || TG_WHEN || '/ROW/'"
                + "   || TG_TABLE_NAME || '/' || NEW.i); RETURN NEW; END IF;"
                + " INSERT INTO ptg_log(m) VALUES (TG_NAME || '/' || TG_WHEN || '/STATEMENT/'"
                + "  || TG_TABLE_NAME); RETURN NULL; END $$");
        exec("CREATE FUNCTION ptg_cnull() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_NAME || '/' || TG_WHEN || '/ROW/'"
                + "  || TG_TABLE_NAME || '/' || NEW.i || '/NULL'); RETURN NULL; END $$");
        exec("CREATE FUNCTION ptg_cseen() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE n int;"
                + " BEGIN SELECT count(*) INTO n FROM ptg_cnew;"
                + " INSERT INTO ptg_log(m) VALUES (TG_NAME || '/sees/' || n || '/'"
                + "  || coalesce((SELECT string_agg(i::text, ',' ORDER BY i) FROM ptg_cnew), '-'));"
                + " RETURN NULL; END $$");
        exec("CREATE FUNCTION ptg_craise() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RAISE EXCEPTION 'ptg refused'; END $$");
        exec("CREATE FUNCTION ptg_rnote() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_WHEN || '/' || TG_OP || '/' || TG_TABLE_NAME"
                + "  || '/' || coalesce(OLD.i::text,'-') || '/' || coalesce(NEW.i::text,'-'));"
                + " IF TG_WHEN = 'BEFORE' AND TG_OP = 'DELETE' THEN RETURN OLD; END IF;"
                + " IF TG_WHEN = 'BEFORE' THEN RETURN NEW; END IF; RETURN NULL; END $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (pool != null) pool.shutdownNow();
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ------------------------------------------------------------ helpers

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

    /** The number of rows a write reports having touched. */
    private static int update(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            return st.executeUpdate(sql);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The first column of every row the query answers, joined with commas. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    /** Everything written to the log so far, in the order it was written. */
    private static String firings() throws SQLException {
        return column("SELECT m FROM ptg_log ORDER BY n");
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

    // ------------------------------------------------------------ helpers for a second session

    private static Connection openSession() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void execOn(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /**
     * What a statement answers: its rows, one per {@code ;}, or {@code [n rows]} when it only
     * reports a count, or {@code ERR[sqlstate] message} when it raises.
     */
    private static String answerOf(Connection c, String sql) {
        StringBuilder out = new StringBuilder();
        try (Statement st = c.createStatement()) {
            if (st.execute(sql)) {
                try (ResultSet rs = st.getResultSet()) {
                    int cols = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) out.append('|');
                            out.append(rs.getString(i));
                        }
                        out.append(';');
                    }
                }
            } else {
                out.append('[').append(st.getUpdateCount()).append(" rows]");
            }
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage();
            int nl = message.indexOf('\n');
            out.append("ERR[").append(e.getSQLState()).append("] ")
                    .append(nl < 0 ? message : message.substring(0, nl));
        }
        return out.toString();
    }

    /** Every row of the query, columns joined with {@code |} and rows with commas. */
    private static String rows(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                got.add(sb.toString());
            }
        }
        return String.join(",", got);
    }

    private static String answerWithin(Future<String> pending) throws Exception {
        try {
            return pending.get(20, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return "STILL-BLOCKED";
        }
    }

    /**
     * Runs {@code statement} on a session of its own while another session holds {@code holding}
     * inside a transaction it has not finished, asserts the statement has not answered, ends that
     * transaction with {@code finish}, and returns what the statement finally answered.
     */
    private static String whileAnotherSessionHolds(String holding, String finish, String statement)
            throws Exception {
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, holding);
            Future<String> pending = pool.submit(() -> answerOf(actor, statement));
            Thread.sleep(BLOCKED_MS);
            assertFalse(pending.isDone(),
                    "answered while another session still held the row: " + statement);
            execOn(holder, finish);
            return answerWithin(pending);
        }
    }

    /**
     * The same, for a statement that is expected to answer while the other session is still
     * holding: nothing it writes is a row that session has taken.
     */
    private static String withoutWaitingWhileAnotherSessionHolds(String holding, String finish,
            String statement) throws Exception {
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, holding);
            Future<String> pending = pool.submit(() -> answerOf(actor, statement));
            Thread.sleep(BLOCKED_MS);
            assertTrue(pending.isDone(),
                    "waited for a row it does not write: " + statement);
            String answer = answerWithin(pending);
            execOn(holder, finish);
            return answer;
        }
    }

    /**
     * The same again, for an actor reading from a snapshot: it opens a transaction at
     * {@code level} and takes its snapshot with {@code snapshotQuery} before the other session
     * finishes.
     */
    private static String underSnapshotWhileAnotherSessionHolds(String level, String snapshotQuery,
            String holding, String finish, String statement) throws Exception {
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, holding);
            execOn(actor, "BEGIN ISOLATION LEVEL " + level);
            execOn(actor, snapshotQuery);
            Future<String> pending = pool.submit(() -> answerOf(actor, statement));
            Thread.sleep(BLOCKED_MS);
            assertFalse(pending.isDone(),
                    "answered while another session still held the row: " + statement);
            execOn(holder, finish);
            String answer = answerWithin(pending);
            try {
                execOn(actor, "COMMIT");
            } catch (SQLException ignored) {
                // an aborted transaction ends either way
            }
            return answer;
        }
    }

    /**
     * The same again for a statement whose other session has already finished: the actor takes its
     * snapshot at {@code level} with {@code snapshotQuery}, the other session runs {@code committed}
     * and commits it at once, and nothing is waited for.
     */
    private static String underSnapshotAfterAnotherSessionCommitted(String level,
            String snapshotQuery, String committed, String statement) throws Exception {
        try (Connection other = openSession(); Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL " + level);
            execOn(actor, snapshotQuery);
            execOn(other, committed);
            String answer = answerOf(actor, statement);
            try {
                execOn(actor, "ROLLBACK");
            } catch (SQLException ignored) {
                // an aborted transaction ends either way
            }
            return answer;
        }
    }

    /**
     * Runs {@code writing} inside a transaction of its own, checks it answered {@code expected},
     * and lets the body ask a third session about the rows it is holding before giving it back.
     */
    private static void whileAnotherSessionIsWriting(String writing, String expected, Asked body)
            throws Exception {
        try (Connection writer = openSession(); Connection asker = openSession()) {
            execOn(writer, "BEGIN");
            assertEquals(expected, answerOf(writer, writing));
            try {
                body.ask(asker);
            } finally {
                execOn(writer, "ROLLBACK");
            }
        }
    }

    private interface Asked {
        void ask(Connection other) throws Exception;
    }

    private static void plainTarget(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int PRIMARY KEY, v int, s text)");
        exec("INSERT INTO " + name + " VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
    }

    private static void partitionedTarget(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int PRIMARY KEY, v int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE " + name + "a PARTITION OF " + name + " FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE " + name + "b PARTITION OF " + name + " FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO " + name + " VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
    }

    private static void source(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int, v int, s text)");
        exec("INSERT INTO " + name + " VALUES (2,200,'x'),(4,400,'y')");
    }

    private static String mergeInto(String target, String src) {
        return "MERGE INTO " + target + " t USING " + src + " u ON t.i = u.i"
                + " WHEN MATCHED THEN UPDATE SET s = u.s"
                + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)";
    }

    // ============================================================ a partition fires its own triggers

    @Test
    void aRowTriggerOnAPartitionFiresForAWriteThatNamesThePartitionedTable() throws SQLException {
        exec("CREATE TABLE ptg_p (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_p1 PARTITION OF ptg_p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_p2 PARTITION OF ptg_p FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_a_par BEFORE INSERT OR UPDATE OR DELETE ON ptg_p"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_prt BEFORE INSERT OR UPDATE OR DELETE ON ptg_p1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_c_par AFTER INSERT OR UPDATE OR DELETE ON ptg_p"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_d_prt AFTER INSERT OR UPDATE OR DELETE ON ptg_p1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_e_pars BEFORE INSERT OR UPDATE OR DELETE ON ptg_p"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_f_prts BEFORE INSERT OR UPDATE OR DELETE ON ptg_p1"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_g_pars AFTER INSERT OR UPDATE OR DELETE ON ptg_p"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_h_prts AFTER INSERT OR UPDATE OR DELETE ON ptg_p1"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");

        // The statement named the parent, so the parent's statement triggers run; the row is
        // stored in the partition, so the partition's own row trigger runs beside its copy of the
        // parent's, in trigger-name order, and both report the partition in TG_TABLE_NAME.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_p VALUES (1,'a')");
        assertEquals("ptg_e_pars/BEFORE/INSERT/STATEMENT/ptg_p,"
                + "ptg_a_par/BEFORE/INSERT/ROW/ptg_p1,"
                + "ptg_b_prt/BEFORE/INSERT/ROW/ptg_p1,"
                + "ptg_c_par/AFTER/INSERT/ROW/ptg_p1,"
                + "ptg_d_prt/AFTER/INSERT/ROW/ptg_p1,"
                + "ptg_g_pars/AFTER/INSERT/STATEMENT/ptg_p", firings());

        exec("DELETE FROM ptg_log");
        exec("UPDATE ptg_p SET s='b' WHERE i=1");
        assertEquals("ptg_e_pars/BEFORE/UPDATE/STATEMENT/ptg_p,"
                + "ptg_a_par/BEFORE/UPDATE/ROW/ptg_p1,"
                + "ptg_b_prt/BEFORE/UPDATE/ROW/ptg_p1,"
                + "ptg_c_par/AFTER/UPDATE/ROW/ptg_p1,"
                + "ptg_d_prt/AFTER/UPDATE/ROW/ptg_p1,"
                + "ptg_g_pars/AFTER/UPDATE/STATEMENT/ptg_p", firings());

        exec("DELETE FROM ptg_log");
        exec("DELETE FROM ptg_p WHERE i=1");
        assertEquals("ptg_e_pars/BEFORE/DELETE/STATEMENT/ptg_p,"
                + "ptg_a_par/BEFORE/DELETE/ROW/ptg_p1,"
                + "ptg_b_prt/BEFORE/DELETE/ROW/ptg_p1,"
                + "ptg_c_par/AFTER/DELETE/ROW/ptg_p1,"
                + "ptg_d_prt/AFTER/DELETE/ROW/ptg_p1,"
                + "ptg_g_pars/AFTER/DELETE/STATEMENT/ptg_p", firings());

        // Naming the partition runs the same row triggers, and the partition's own statement
        // triggers rather than the parent's.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_p1 VALUES (2,'a')");
        assertEquals("ptg_f_prts/BEFORE/INSERT/STATEMENT/ptg_p1,"
                + "ptg_a_par/BEFORE/INSERT/ROW/ptg_p1,"
                + "ptg_b_prt/BEFORE/INSERT/ROW/ptg_p1,"
                + "ptg_c_par/AFTER/INSERT/ROW/ptg_p1,"
                + "ptg_d_prt/AFTER/INSERT/ROW/ptg_p1,"
                + "ptg_h_prts/AFTER/INSERT/STATEMENT/ptg_p1", firings());

        // A partition carrying no triggers of its own runs only its copies.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_p VALUES (12,'b')");
        assertEquals("ptg_e_pars/BEFORE/INSERT/STATEMENT/ptg_p,"
                + "ptg_a_par/BEFORE/INSERT/ROW/ptg_p2,"
                + "ptg_c_par/AFTER/INSERT/ROW/ptg_p2,"
                + "ptg_g_pars/AFTER/INSERT/STATEMENT/ptg_p", firings());

        assertEquals("2@ptg_p1,12@ptg_p2",
                column("SELECT i || '@' || tableoid::regclass::text FROM ptg_p ORDER BY i"));
        exec("DROP TABLE ptg_p CASCADE");
    }

    @Test
    void aRowThatChangesPartitionFiresADeleteAndAnInsertRatherThanAnUpdate() throws SQLException {
        exec("CREATE TABLE ptg_mv (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_mv1 PARTITION OF ptg_mv FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_mv2 PARTITION OF ptg_mv FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_a_all BEFORE INSERT OR UPDATE OR DELETE ON ptg_mv"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_one BEFORE INSERT OR UPDATE OR DELETE ON ptg_mv1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_c_all AFTER INSERT OR UPDATE OR DELETE ON ptg_mv"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_e_stmt BEFORE INSERT OR UPDATE OR DELETE ON ptg_mv"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_g_stmt AFTER INSERT OR UPDATE OR DELETE ON ptg_mv"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("INSERT INTO ptg_mv VALUES (1,'a')");

        // The source's BEFORE UPDATE runs first, then the source's BEFORE DELETE and the
        // destination's BEFORE INSERT -- the partition's own trigger among them. No AFTER UPDATE
        // fires for the row at all.
        exec("DELETE FROM ptg_log");
        assertEquals(1, update("UPDATE ptg_mv SET i=12 WHERE i=1"));
        assertEquals("ptg_e_stmt/BEFORE/UPDATE/STATEMENT/ptg_mv,"
                + "ptg_a_all/BEFORE/UPDATE/ROW/ptg_mv1,"
                + "ptg_b_one/BEFORE/UPDATE/ROW/ptg_mv1,"
                + "ptg_a_all/BEFORE/DELETE/ROW/ptg_mv1,"
                + "ptg_b_one/BEFORE/DELETE/ROW/ptg_mv1,"
                + "ptg_a_all/BEFORE/INSERT/ROW/ptg_mv2,"
                + "ptg_c_all/AFTER/DELETE/ROW/ptg_mv1,"
                + "ptg_c_all/AFTER/INSERT/ROW/ptg_mv2,"
                + "ptg_g_stmt/AFTER/UPDATE/STATEMENT/ptg_mv", firings());
        assertEquals(0, num("SELECT count(*) FROM ptg_mv1"));
        assertEquals(1, num("SELECT count(*) FROM ptg_mv2"));
        assertEquals("12/a", scalar("SELECT i || '/' || s FROM ptg_mv2"));
        exec("DROP TABLE ptg_mv CASCADE");
    }

    @Test
    void whatTheSourcesBeforeUpdateLeavesInNewIsWhatTheDestinationStores() throws SQLException {
        exec("CREATE TABLE ptg_mk (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_mk1 PARTITION OF ptg_mk FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_mk2 PARTITION OF ptg_mk FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_z_mark BEFORE UPDATE ON ptg_mk1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_mark()");
        exec("INSERT INTO ptg_mk VALUES (1,'a')");
        assertEquals(1, update("UPDATE ptg_mk SET i=12 WHERE i=1"));
        assertEquals("12/a+m", scalar("SELECT i || '/' || s FROM ptg_mk2"));
        exec("DROP TABLE ptg_mk CASCADE");
    }

    @Test
    void aBeforeDeleteOnTheSourcePartitionThatReturnsNullKeepsTheRowWhereItWas()
            throws SQLException {
        exec("CREATE TABLE ptg_vd (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_vd1 PARTITION OF ptg_vd FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_vd2 PARTITION OF ptg_vd FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_z_veto BEFORE DELETE ON ptg_vd1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_veto()");
        exec("INSERT INTO ptg_vd VALUES (1,'a')");
        exec("DELETE FROM ptg_log");
        // The move is the source's delete, so the source's BEFORE DELETE can refuse it.
        assertEquals(0, update("UPDATE ptg_vd SET i=12 WHERE i=1"));
        assertEquals("VETO/ptg_z_veto/DELETE/ptg_vd1", firings());
        assertEquals("1@ptg_vd1",
                column("SELECT i || '@' || tableoid::regclass::text FROM ptg_vd ORDER BY i"));
        exec("DROP TABLE ptg_vd CASCADE");
    }

    @Test
    void aBeforeInsertOnTheDestinationPartitionThatReturnsNullLeavesTheRowStoredNowhere()
            throws SQLException {
        exec("CREATE TABLE ptg_vi (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_vi1 PARTITION OF ptg_vi FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_vi2 PARTITION OF ptg_vi FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_z_veto BEFORE INSERT ON ptg_vi2"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_veto()");
        exec("INSERT INTO ptg_vi VALUES (1,'a')");
        exec("DELETE FROM ptg_log");
        // PostgreSQL has already carried out the delete by the time the destination refuses the
        // row, so the row is gone and the statement still reports none.
        assertEquals(0, update("UPDATE ptg_vi SET i=12 WHERE i=1"));
        assertEquals("VETO/ptg_z_veto/INSERT/ptg_vi2", firings());
        assertEquals(0, num("SELECT count(*) FROM ptg_vi"));
        exec("DROP TABLE ptg_vi CASCADE");
    }

    @Test
    void theCopiesReachAPartitionOfAPartitionAndItIsTheLeafThatFiresThem() throws SQLException {
        exec("CREATE TABLE ptg_q (i int, j int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_q1 PARTITION OF ptg_q FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (j)");
        exec("CREATE TABLE ptg_q1a PARTITION OF ptg_q1 FOR VALUES FROM (0) TO (10)");
        exec("CREATE TRIGGER ptg_a_top BEFORE INSERT OR UPDATE OR DELETE ON ptg_q"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_mid BEFORE INSERT OR UPDATE OR DELETE ON ptg_q1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_c_leaf BEFORE INSERT OR UPDATE OR DELETE ON ptg_q1a"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_f_mids BEFORE INSERT OR UPDATE OR DELETE ON ptg_q1"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("INSERT INTO ptg_q VALUES (1,1,'a')");

        // The statement named the middle relation, so its statement trigger runs; the row lives in
        // the leaf, so the leaf runs all three row triggers and names itself in each.
        exec("DELETE FROM ptg_log");
        exec("UPDATE ptg_q1 SET s='c' WHERE i=1");
        assertEquals("ptg_f_mids/BEFORE/UPDATE/STATEMENT/ptg_q1,"
                + "ptg_a_top/BEFORE/UPDATE/ROW/ptg_q1a,"
                + "ptg_b_mid/BEFORE/UPDATE/ROW/ptg_q1a,"
                + "ptg_c_leaf/BEFORE/UPDATE/ROW/ptg_q1a", firings());
        exec("DROP TABLE ptg_q CASCADE");
    }

    @Test
    void anInheritanceChildIsGivenNoCopyAndFiresOnlyItsOwnRowTriggers() throws SQLException {
        exec("CREATE TABLE ptg_ip (i int, s text)");
        exec("CREATE TABLE ptg_ic (i int, s text) INHERITS (ptg_ip)");
        exec("CREATE TRIGGER ptg_a_ipar BEFORE INSERT OR UPDATE OR DELETE ON ptg_ip"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_ichd BEFORE INSERT OR UPDATE OR DELETE ON ptg_ic"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_e_ipars BEFORE INSERT OR UPDATE OR DELETE ON ptg_ip"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_note()");
        exec("INSERT INTO ptg_ic VALUES (1,'a')");

        exec("DELETE FROM ptg_log");
        exec("UPDATE ptg_ip SET s='b' WHERE i=1");
        assertEquals("ptg_e_ipars/BEFORE/UPDATE/STATEMENT/ptg_ip,"
                + "ptg_b_ichd/BEFORE/UPDATE/ROW/ptg_ic", firings());

        exec("DELETE FROM ptg_log");
        exec("DELETE FROM ptg_ip WHERE i=1");
        assertEquals("ptg_e_ipars/BEFORE/DELETE/STATEMENT/ptg_ip,"
                + "ptg_b_ichd/BEFORE/DELETE/ROW/ptg_ic", firings());
        exec("DROP TABLE ptg_ip CASCADE");
    }

    @Test
    void aPartitionAttachedLaterIsGivenTheCopyAndADetachedOneGivesItUp() throws SQLException {
        exec("CREATE TABLE ptg_at (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_at1 PARTITION OF ptg_at FOR VALUES FROM (0) TO (10)");
        exec("CREATE TRIGGER ptg_a_at BEFORE INSERT ON ptg_at"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TABLE ptg_at2 (i int, s text)");
        exec("ALTER TABLE ptg_at ATTACH PARTITION ptg_at2 FOR VALUES FROM (10) TO (20)");
        assertEquals("ptg_at,ptg_at1,ptg_at2", column("SELECT tgrelid::regclass::text"
                + " FROM pg_trigger WHERE tgname = 'ptg_a_at' ORDER BY 1"));

        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_at VALUES (12,'a')");
        assertEquals("ptg_a_at/BEFORE/INSERT/ROW/ptg_at2", firings());

        // The copy is not the partition's to drop.
        assertEquals("2BP01", stateOf("DROP TRIGGER ptg_a_at ON ptg_at1"));
        assertEquals("cannot drop trigger ptg_a_at on table ptg_at1 because trigger ptg_a_at"
                        + " on table ptg_at requires it",
                messageOf("DROP TRIGGER ptg_a_at ON ptg_at1"));
        assertEquals("You can drop trigger ptg_a_at on table ptg_at instead.",
                hintOf("DROP TRIGGER ptg_a_at ON ptg_at1"));

        exec("ALTER TABLE ptg_at DETACH PARTITION ptg_at2");
        assertEquals("ptg_at,ptg_at1", column("SELECT tgrelid::regclass::text"
                + " FROM pg_trigger WHERE tgname = 'ptg_a_at' ORDER BY 1"));
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_at2 VALUES (13,'b')");
        assertEquals(0, num("SELECT count(*) FROM ptg_log"));

        exec("DROP TABLE ptg_at2");
        exec("DROP TABLE ptg_at CASCADE");
    }

    @Test
    void eachRowOfAWriteOverTwoPartitionsRunsOnlyItsOwnPartitionsTriggers() throws SQLException {
        exec("CREATE TABLE ptg_tw (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_tw1 PARTITION OF ptg_tw FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_tw2 PARTITION OF ptg_tw FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_a_tw BEFORE INSERT OR UPDATE OR DELETE ON ptg_tw"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_tw1 BEFORE INSERT OR UPDATE OR DELETE ON ptg_tw1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_c_tw2 BEFORE INSERT OR UPDATE OR DELETE ON ptg_tw2"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("INSERT INTO ptg_tw VALUES (1,'a'),(12,'b')");

        // Which partition the statement reaches first is the plan's business, so each is read on
        // its own; what is fixed is that each row runs the trigger of the partition it is stored
        // in, and its copy of the parent's, and nothing belonging to the other partition.
        exec("DELETE FROM ptg_log");
        assertEquals(2, update("UPDATE ptg_tw SET s = s || '!'"));
        assertEquals("ptg_a_tw/BEFORE/UPDATE/ROW/ptg_tw1,ptg_b_tw1/BEFORE/UPDATE/ROW/ptg_tw1",
                column("SELECT m FROM ptg_log WHERE m LIKE '%/ptg_tw1' ORDER BY n"));
        assertEquals("ptg_a_tw/BEFORE/UPDATE/ROW/ptg_tw2,ptg_c_tw2/BEFORE/UPDATE/ROW/ptg_tw2",
                column("SELECT m FROM ptg_log WHERE m LIKE '%/ptg_tw2' ORDER BY n"));
        exec("DROP TABLE ptg_tw CASCADE");
    }

    @Test
    void mergeAndOnConflictAgainstTheParentFireThePartitionsRowTriggers() throws SQLException {
        exec("CREATE TABLE ptg_m (i int PRIMARY KEY, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_m1 PARTITION OF ptg_m FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_m2 PARTITION OF ptg_m FOR VALUES FROM (10) TO (20)");
        exec("CREATE TABLE ptg_ms (i int, s text)");
        exec("INSERT INTO ptg_ms VALUES (1,'m1'),(3,'m3'),(13,'m13')");
        exec("CREATE TRIGGER ptg_a_mpar BEFORE INSERT OR UPDATE ON ptg_m"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_m1 BEFORE INSERT OR UPDATE ON ptg_m1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("INSERT INTO ptg_m VALUES (1,'old')");

        exec("DELETE FROM ptg_log");
        assertEquals(3, update("MERGE INTO ptg_m t USING ptg_ms u ON t.i = u.i"
                + " WHEN MATCHED THEN UPDATE SET s = u.s"
                + " WHEN NOT MATCHED THEN INSERT (i,s) VALUES (u.i,u.s)"));
        assertEquals("ptg_a_mpar/BEFORE/UPDATE/ROW/ptg_m1,"
                + "ptg_b_m1/BEFORE/UPDATE/ROW/ptg_m1,"
                + "ptg_a_mpar/BEFORE/INSERT/ROW/ptg_m1,"
                + "ptg_b_m1/BEFORE/INSERT/ROW/ptg_m1,"
                + "ptg_a_mpar/BEFORE/INSERT/ROW/ptg_m2", firings());
        assertEquals("1/m1,3/m3,13/m13", column("SELECT i || '/' || s FROM ptg_m ORDER BY i"));

        exec("DELETE FROM ptg_log");
        assertEquals(1, update("INSERT INTO ptg_m VALUES (1,'again')"
                + " ON CONFLICT (i) DO UPDATE SET s = 'conflicted'"));
        assertEquals("ptg_a_mpar/BEFORE/INSERT/ROW/ptg_m1,"
                + "ptg_b_m1/BEFORE/INSERT/ROW/ptg_m1,"
                + "ptg_a_mpar/BEFORE/UPDATE/ROW/ptg_m1,"
                + "ptg_b_m1/BEFORE/UPDATE/ROW/ptg_m1", firings());
        assertEquals("conflicted", scalar("SELECT s FROM ptg_m WHERE i = 1"));

        exec("DROP TABLE ptg_m CASCADE");
        exec("DROP TABLE ptg_ms");
    }

    // ============================================================ COPY into the parent

    @Test
    void copyIntoTheParentFiresThePartitionsRowTriggers() throws Exception {
        exec("CREATE TABLE ptg_cp (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_cp1 PARTITION OF ptg_cp FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_cp2 PARTITION OF ptg_cp FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_a_cpar BEFORE INSERT ON ptg_cp"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("CREATE TRIGGER ptg_b_cone BEFORE INSERT ON ptg_cp1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_note()");
        exec("DELETE FROM ptg_log");

        CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
        assertEquals(2L, copies.copyIn("COPY ptg_cp FROM STDIN", new StringReader("1\ta\n12\tb\n")));

        assertEquals("ptg_a_cpar/BEFORE/INSERT/ROW/ptg_cp1,"
                + "ptg_b_cone/BEFORE/INSERT/ROW/ptg_cp1,"
                + "ptg_a_cpar/BEFORE/INSERT/ROW/ptg_cp2", firings());
        assertEquals("1@ptg_cp1,12@ptg_cp2",
                column("SELECT i || '@' || tableoid::regclass::text FROM ptg_cp ORDER BY i"));
        exec("DROP TABLE ptg_cp CASCADE");
    }

    @Test
    void copyIntoTheParentDropsARowThePartitionsBeforeTriggerRefuses() throws Exception {
        exec("CREATE TABLE ptg_cv (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE ptg_cv1 PARTITION OF ptg_cv FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_cv2 PARTITION OF ptg_cv FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_z_skip BEFORE INSERT ON ptg_cv"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_skip()");

        CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
        // The row the trigger returned NULL for is not stored, and COPY does not count it.
        assertEquals(1L, copies.copyIn("COPY ptg_cv FROM STDIN", new StringReader("1\ta\n12\tb\n")));
        assertEquals("1", column("SELECT i::text FROM ptg_cv ORDER BY i"));
        exec("DROP TABLE ptg_cv CASCADE");
    }

    // ============================================================ MERGE waits for the writer it collides with

    @Test
    void mergeWaitsForAnUncommittedUpdateOfTheRowItMatches() throws Exception {
        plainTarget("mcw_u1");
        source("mcw_u1s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_u1 SET v = 99 WHERE i = 2", "COMMIT",
                mergeInto("mcw_u1", "mcw_u1s")));
        // The arm ran against the version the other transaction left behind, so its v stands.
        assertEquals("1|10|a,2|99|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_u1 ORDER BY i"));
        exec("DROP TABLE mcw_u1");
        exec("DROP TABLE mcw_u1s");
    }

    @Test
    void mergeActsOnTheRowAnAbortedUpdateGaveBack() throws Exception {
        plainTarget("mcw_u2");
        source("mcw_u2s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_u2 SET v = 99 WHERE i = 2", "ROLLBACK",
                mergeInto("mcw_u2", "mcw_u2s")));
        assertEquals("1|10|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_u2 ORDER BY i"));
        exec("DROP TABLE mcw_u2");
        exec("DROP TABLE mcw_u2s");
    }

    @Test
    void aRowTheOtherSessionReallyDeletedLeavesItsSourceRowUnmatched() throws Exception {
        plainTarget("mcw_d1");
        source("mcw_d1s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "DELETE FROM mcw_d1 WHERE i = 2", "COMMIT", mergeInto("mcw_d1", "mcw_d1s")));
        // The delete stood, so the source row took the NOT MATCHED arm and brought its own v.
        assertEquals("1|10|a,2|200|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_d1 ORDER BY i"));
        exec("DROP TABLE mcw_d1");
        exec("DROP TABLE mcw_d1s");
    }

    @Test
    void aRowHiddenByADeleteThatWasRolledBackIsStillMatched() throws Exception {
        plainTarget("mcw_d2");
        source("mcw_d2s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "DELETE FROM mcw_d2 WHERE i = 2", "ROLLBACK", mergeInto("mcw_d2", "mcw_d2s")));
        // Answering without the wait would have inserted a second row 2 beside the one the
        // rollback put back.
        assertEquals("1|10|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_d2 ORDER BY i"));
        exec("DROP TABLE mcw_d2");
        exec("DROP TABLE mcw_d2s");
    }

    @Test
    void aRowSteeredOutOfTheOnConditionLeavesItsSourceRowToTheNotMatchedArm() throws Exception {
        plainTarget("mcw_j1");
        exec("CREATE TABLE mcw_j1s (i int, v int, s text)");
        exec("INSERT INTO mcw_j1s VALUES (2,200,'x')");
        assertEquals("[1 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_j1 SET i = 5 WHERE i = 2", "COMMIT", mergeInto("mcw_j1", "mcw_j1s")));
        assertEquals("1|10|a,2|200|x,3|30|c,5|20|b", rows("SELECT * FROM mcw_j1 ORDER BY i"));
        exec("DROP TABLE mcw_j1");
        exec("DROP TABLE mcw_j1s");
    }

    @Test
    void theDeleteArmWaitsForTheRowItWouldDelete() throws Exception {
        plainTarget("mcw_da");
        source("mcw_das");
        assertEquals("[1 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_da SET v = 99 WHERE i = 2", "COMMIT",
                "MERGE INTO mcw_da t USING mcw_das u ON t.i = u.i WHEN MATCHED THEN DELETE"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM mcw_da ORDER BY i"));
        exec("DROP TABLE mcw_da");
        exec("DROP TABLE mcw_das");
    }

    @Test
    void theNotMatchedBySourceArmWaitsForTheRowItWrites() throws Exception {
        plainTarget("mcw_ns");
        source("mcw_nss");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_ns SET v = 99 WHERE i = 1", "COMMIT",
                "MERGE INTO mcw_ns t USING mcw_nss u ON t.i = u.i"
                        + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 'ns'"));
        assertEquals("1|99|ns,2|20|b,3|30|ns", rows("SELECT * FROM mcw_ns ORDER BY i"));
        exec("DROP TABLE mcw_ns");
        exec("DROP TABLE mcw_nss");
    }

    @Test
    void anArmWhoseConditionFailsWaitsForNobody() throws Exception {
        plainTarget("mcw_n1");
        source("mcw_n1s");
        assertEquals("[0 rows]", withoutWaitingWhileAnotherSessionHolds(
                "UPDATE mcw_n1 SET v = 99 WHERE i = 2", "COMMIT",
                "MERGE INTO mcw_n1 t USING mcw_n1s u ON t.i = u.i"
                        + " WHEN MATCHED AND t.v = 77 THEN UPDATE SET s = u.s"));
        assertEquals("1|10|a,2|99|b,3|30|c", rows("SELECT * FROM mcw_n1 ORDER BY i"));
        exec("DROP TABLE mcw_n1");
        exec("DROP TABLE mcw_n1s");
    }

    @Test
    void aDoNothingArmWaitsForNobody() throws Exception {
        plainTarget("mcw_n2");
        source("mcw_n2s");
        assertEquals("[0 rows]", withoutWaitingWhileAnotherSessionHolds(
                "UPDATE mcw_n2 SET v = 99 WHERE i = 2", "COMMIT",
                "MERGE INTO mcw_n2 t USING mcw_n2s u ON t.i = u.i WHEN MATCHED THEN DO NOTHING"));
        exec("DROP TABLE mcw_n2");
        exec("DROP TABLE mcw_n2s");
    }

    @Test
    void aRowNoArmOfTheMergeWritesIsNotWaitedFor() throws Exception {
        plainTarget("mcw_w1");
        source("mcw_w1s");
        assertEquals("[2 rows]", withoutWaitingWhileAnotherSessionHolds(
                "UPDATE mcw_w1 SET v = 99 WHERE i = 1", "COMMIT",
                mergeInto("mcw_w1", "mcw_w1s")));
        assertEquals("1|99|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_w1 ORDER BY i"));
        exec("DROP TABLE mcw_w1");
        exec("DROP TABLE mcw_w1s");
    }

    @Test
    void theInsertArmIsRefusedAKeyAnotherSessionCommittedWhileItWaited() throws Exception {
        plainTarget("mcw_i1");
        source("mcw_i1s");
        String answer = whileAnotherSessionHolds("INSERT INTO mcw_i1 VALUES (4,40,'d')", "COMMIT",
                mergeInto("mcw_i1", "mcw_i1s"));
        assertTrue(answer.startsWith("ERR[23505] "), answer);
        assertTrue(answer.contains("duplicate key value violates unique constraint"
                + " \"mcw_i1_pkey\""), answer);
        // The whole MERGE is taken back, so the UPDATE arm's write goes with it.
        assertEquals("1|10|a,2|20|b,3|30|c,4|40|d", rows("SELECT * FROM mcw_i1 ORDER BY i"));
        exec("DROP TABLE mcw_i1");
        exec("DROP TABLE mcw_i1s");
    }

    @Test
    void theInsertArmTakesAKeyTheOtherSessionGaveBack() throws Exception {
        plainTarget("mcw_i2");
        source("mcw_i2s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "INSERT INTO mcw_i2 VALUES (4,40,'d')", "ROLLBACK", mergeInto("mcw_i2", "mcw_i2s")));
        assertEquals("1|10|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_i2 ORDER BY i"));
        exec("DROP TABLE mcw_i2");
        exec("DROP TABLE mcw_i2s");
    }

    @Test
    void returningReadsTheVersionTheOtherSessionCommitted() throws Exception {
        plainTarget("mcw_r1");
        source("mcw_r1s");
        assertEquals("UPDATE|2|99|x|200;INSERT|4|400|y|400;", whileAnotherSessionHolds(
                "UPDATE mcw_r1 SET v = 99 WHERE i = 2", "COMMIT",
                "MERGE INTO mcw_r1 t USING mcw_r1s u ON t.i = u.i"
                        + " WHEN MATCHED THEN UPDATE SET s = u.s"
                        + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)"
                        + " RETURNING merge_action(), t.i, t.v, t.s, u.v"));
        exec("DROP TABLE mcw_r1");
        exec("DROP TABLE mcw_r1s");
    }

    // ============================================================ a partitioned target

    @Test
    void aRowMovedToAnotherPartitionWhileTheMergeWaitedEndsTheMerge() throws Exception {
        partitionedTarget("mcw_p1");
        source("mcw_p1s");
        String answer = whileAnotherSessionHolds("UPDATE mcw_p1 SET i = 12 WHERE i = 2", "COMMIT",
                mergeInto("mcw_p1", "mcw_p1s"));
        assertTrue(answer.startsWith("ERR[40001] "), answer);
        assertTrue(answer.contains("tuple to be locked was already moved to another partition"
                + " due to concurrent update"), answer);
        assertEquals("1|10|a,3|30|c,12|20|b", rows("SELECT * FROM mcw_p1 ORDER BY i"));
        exec("DROP TABLE mcw_p1 CASCADE");
        exec("DROP TABLE mcw_p1s");
    }

    @Test
    void aPartitionedTargetGivesBackTheRowWhoseMoveWasRolledBack() throws Exception {
        partitionedTarget("mcw_p2");
        source("mcw_p2s");
        assertEquals("[2 rows]", whileAnotherSessionHolds(
                "UPDATE mcw_p2 SET i = 12 WHERE i = 2", "ROLLBACK", mergeInto("mcw_p2", "mcw_p2s")));
        assertEquals("1|10|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_p2 ORDER BY i"));
        exec("DROP TABLE mcw_p2 CASCADE");
        exec("DROP TABLE mcw_p2s");
    }

    // ============================================================ the rows a MERGE holds

    @Test
    void theRowsAMergeWritesAreHeldAgainstForUpdateNowait() throws Exception {
        plainTarget("mcw_l1");
        exec("CREATE TABLE mcw_l1s (i int, v int, s text)");
        exec("INSERT INTO mcw_l1s VALUES (2,200,'x')");
        whileAnotherSessionIsWriting(
                "MERGE INTO mcw_l1 t USING mcw_l1s u ON t.i = u.i"
                        + " WHEN MATCHED THEN UPDATE SET s = u.s",
                "[1 rows]",
                asker -> {
                    assertTrue(answerOf(asker, "SELECT * FROM mcw_l1 WHERE i = 2"
                            + " FOR UPDATE NOWAIT").startsWith("ERR[55P03] "));
                    assertEquals("1|10|a;", answerOf(asker, "SELECT * FROM mcw_l1 WHERE i = 1"
                            + " FOR UPDATE NOWAIT"));
                });
        exec("DROP TABLE mcw_l1");
        exec("DROP TABLE mcw_l1s");
    }

    @Test
    void aDoNothingArmHoldsNoRow() throws Exception {
        plainTarget("mcw_l2");
        exec("CREATE TABLE mcw_l2s (i int, v int, s text)");
        exec("INSERT INTO mcw_l2s VALUES (2,200,'x')");
        whileAnotherSessionIsWriting(
                "MERGE INTO mcw_l2 t USING mcw_l2s u ON t.i = u.i WHEN MATCHED THEN DO NOTHING",
                "[0 rows]",
                asker -> assertEquals("2|20|b;", answerOf(asker,
                        "SELECT * FROM mcw_l2 WHERE i = 2 FOR UPDATE NOWAIT")));
        exec("DROP TABLE mcw_l2");
        exec("DROP TABLE mcw_l2s");
    }

    @Test
    void theInsertArmHoldsTheKeyItTookUntilItsTransactionEnds() throws Exception {
        plainTarget("mcw_l3");
        source("mcw_l3s");
        try (Connection writer = openSession(); Connection asker = openSession()) {
            execOn(writer, "BEGIN");
            assertEquals("[2 rows]", answerOf(writer, mergeInto("mcw_l3", "mcw_l3s")));
            Future<String> pending = pool.submit(
                    () -> answerOf(asker, "INSERT INTO mcw_l3 VALUES (4,44,'z')"));
            Thread.sleep(BLOCKED_MS);
            assertFalse(pending.isDone(), "the key the INSERT arm took was not held");
            execOn(writer, "ROLLBACK");
            assertEquals("[1 rows]", answerWithin(pending));
        }
        assertEquals("1|10|a,2|20|b,3|30|c,4|44|z", rows("SELECT * FROM mcw_l3 ORDER BY i"));
        exec("DROP TABLE mcw_l3");
        exec("DROP TABLE mcw_l3s");
    }

    // ============================================================ a merger reading from a snapshot

    @Test
    void aReadCommittedMergerRunsItsArmAgainstTheRowTheOtherSessionCommitted() throws Exception {
        plainTarget("mcw_z1");
        source("mcw_z1s");
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM mcw_z1",
                "UPDATE mcw_z1 SET v = 99 WHERE i = 2", "COMMIT", mergeInto("mcw_z1", "mcw_z1s")));
        assertEquals("1|10|a,2|99|x,3|30|c,4|400|y", rows("SELECT * FROM mcw_z1 ORDER BY i"));
        exec("DROP TABLE mcw_z1");
        exec("DROP TABLE mcw_z1s");
    }

    @Test
    void aRepeatableReadMergerIsEndedByARowMovedToAnotherPartition() throws Exception {
        partitionedTarget("mcw_z2");
        source("mcw_z2s");
        String answer = underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM mcw_z2",
                "UPDATE mcw_z2 SET i = 12 WHERE i = 2", "COMMIT", mergeInto("mcw_z2", "mcw_z2s"));
        // PostgreSQL blames the tuple's move here whatever the reader is reading from, rather than
        // the write it lost.
        assertTrue(answer.startsWith("ERR[40001] "), answer);
        assertTrue(answer.contains("tuple to be locked was already moved to another partition"
                + " due to concurrent update"), answer);
        exec("DROP TABLE mcw_z2 CASCADE");
        exec("DROP TABLE mcw_z2s");
    }

    @Test
    void aSerializableMergerIsEndedByARowMovedToAnotherPartition() throws Exception {
        partitionedTarget("mcw_z3");
        source("mcw_z3s");
        String answer = underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM mcw_z3",
                "UPDATE mcw_z3 SET i = 12 WHERE i = 2", "COMMIT", mergeInto("mcw_z3", "mcw_z3s"));
        assertTrue(answer.startsWith("ERR[40001] "), answer);
        assertTrue(answer.contains("tuple to be locked was already moved to another partition"
                + " due to concurrent update"), answer);
        exec("DROP TABLE mcw_z3 CASCADE");
        exec("DROP TABLE mcw_z3s");
    }

    @Test
    void aRepeatableReadMergersInsertArmIsStillRefusedTheKeyAnotherSessionTook() throws Exception {
        plainTarget("mcw_z4");
        source("mcw_z4s");
        String answer = underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM mcw_z4",
                "INSERT INTO mcw_z4 VALUES (4,40,'d')", "COMMIT", mergeInto("mcw_z4", "mcw_z4s"));
        // The key is a key whatever the snapshot says, so this is the unique violation and not a
        // serialization failure.
        assertTrue(answer.startsWith("ERR[23505] "), answer);
        assertTrue(answer.contains("duplicate key value violates unique constraint"
                + " \"mcw_z4_pkey\""), answer);
        assertEquals("1|10|a,2|20|b,3|30|c,4|40|d", rows("SELECT * FROM mcw_z4 ORDER BY i"));
        exec("DROP TABLE mcw_z4");
        exec("DROP TABLE mcw_z4s");
    }

    @Test
    void aSerializableMergersInsertArmIsStillRefusedTheKeyAnotherSessionTook() throws Exception {
        plainTarget("mcw_z5");
        source("mcw_z5s");
        String answer = underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM mcw_z5",
                "INSERT INTO mcw_z5 VALUES (4,40,'d')", "COMMIT", mergeInto("mcw_z5", "mcw_z5s"));
        assertTrue(answer.startsWith("ERR[23505] "), answer);
        assertTrue(answer.contains("duplicate key value violates unique constraint"
                + " \"mcw_z5_pkey\""), answer);
        assertEquals("1|10|a,2|20|b,3|30|c,4|40|d", rows("SELECT * FROM mcw_z5 ORDER BY i"));
        exec("DROP TABLE mcw_z5");
        exec("DROP TABLE mcw_z5s");
    }

    // ============================================================ what a MERGE writes on its own

    @Test
    void aMergeThatRaisesLeavesNothingOfItsOtherArmsBehind() throws Exception {
        exec("CREATE TABLE mcw_e1 (i int PRIMARY KEY, v int, s text)");
        exec("INSERT INTO mcw_e1 VALUES (1,10,'a'),(2,20,'b'),(4,40,'d')");
        exec("CREATE TABLE mcw_e1s (i int, v int, s text)");
        exec("INSERT INTO mcw_e1s VALUES (2,200,'x'),(4,400,'y')");
        assertEquals("23505", stateOf("MERGE INTO mcw_e1 t USING mcw_e1s u ON t.i = u.i + 100"
                + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)"));
        assertEquals("Key (i)=(2) already exists.",
                detailOf("MERGE INTO mcw_e1 t USING mcw_e1s u ON t.i = u.i + 100"
                        + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v, u.s)"));
        assertEquals("1|10|a,2|20|b,4|40|d", rows("SELECT * FROM mcw_e1 ORDER BY i"));
        exec("DROP TABLE mcw_e1");
        exec("DROP TABLE mcw_e1s");
    }

    // ============================================================ what a MERGE's arms fire

    @Test
    void theDeleteArmFiresTheDeleteRowTriggersAndHoldsTheirAfterHalvesBack() throws SQLException {
        exec("CREATE TABLE mtr_d (i int PRIMARY KEY, v int)");
        exec("INSERT INTO mtr_d VALUES (1,10),(2,20),(3,30)");
        exec("CREATE TABLE mtr_ds (i int, v int)");
        exec("INSERT INTO mtr_ds VALUES (1,100),(2,200)");
        exec("CREATE TRIGGER mtr_d_b BEFORE DELETE ON mtr_d"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");
        exec("CREATE TRIGGER mtr_d_a AFTER DELETE ON mtr_d"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");

        // Both BEFORE halves run while the statement writes and both AFTER halves once it has:
        // not BEFORE 1, AFTER 1, BEFORE 2, AFTER 2.
        exec("DELETE FROM ptg_log");
        assertEquals(2, update("MERGE INTO mtr_d t USING mtr_ds u ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE"));
        assertEquals("BEFORE/DELETE/mtr_d/1/-,BEFORE/DELETE/mtr_d/2/-,"
                + "AFTER/DELETE/mtr_d/1/-,AFTER/DELETE/mtr_d/2/-", firings());
        assertEquals("3|30", rows("SELECT i, v FROM mtr_d ORDER BY i"));

        // The arm that reaches the rows the source paired with nothing fires them too.
        exec("INSERT INTO mtr_d VALUES (1,10),(2,20)");
        exec("DELETE FROM ptg_log");
        assertEquals(1, update("MERGE INTO mtr_d t USING mtr_ds u ON t.i = u.i"
                + " WHEN NOT MATCHED BY SOURCE THEN DELETE"));
        assertEquals("BEFORE/DELETE/mtr_d/3/-,AFTER/DELETE/mtr_d/3/-", firings());

        // A BEFORE DELETE that answers with nothing keeps the row, the arm counts it as
        // nothing, and no AFTER half is owed for it.
        exec("DROP TRIGGER mtr_d_b ON mtr_d");
        exec("CREATE TRIGGER mtr_d_b BEFORE DELETE ON mtr_d"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_veto()");
        exec("DELETE FROM ptg_log");
        assertEquals(0, update("MERGE INTO mtr_d t USING mtr_ds u ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE"));
        assertEquals("VETO/mtr_d_b/DELETE/mtr_d,VETO/mtr_d_b/DELETE/mtr_d", firings());
        assertEquals("1|10,2|20", rows("SELECT i, v FROM mtr_d ORDER BY i"));

        exec("DROP TABLE mtr_d CASCADE");
        exec("DROP TABLE mtr_ds");
    }

    @Test
    void anAfterRowTriggerOfAMergeReadsTheRelationTheWholeStatementLeftBehind()
            throws SQLException {
        exec("CREATE TABLE mtr_a (i int PRIMARY KEY, v int)");
        exec("INSERT INTO mtr_a VALUES (1,10),(2,20),(3,30)");
        exec("CREATE FUNCTION mtr_af() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_OP || '/left=' || (SELECT count(*) FROM mtr_a)"
                + "  || '/max=' || coalesce((SELECT max(v)::text FROM mtr_a),'-'));"
                + " RETURN NULL; END $$");
        exec("CREATE TRIGGER mtr_a_ad AFTER DELETE ON mtr_a"
                + " FOR EACH ROW EXECUTE FUNCTION mtr_af()");
        exec("CREATE TRIGGER mtr_a_au AFTER UPDATE ON mtr_a"
                + " FOR EACH ROW EXECUTE FUNCTION mtr_af()");

        // Each of the three sees the one row left and the update's own value already stored, so
        // none of them ran before the statement had finished writing.
        exec("DELETE FROM ptg_log");
        assertEquals(3, update("MERGE INTO mtr_a t USING (VALUES (1),(2)) u(i) ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 99"));
        assertEquals("DELETE/left=1/max=99,DELETE/left=1/max=99,UPDATE/left=1/max=99", firings());
        assertEquals("3|99", rows("SELECT i, v FROM mtr_a ORDER BY i"));

        exec("DROP TABLE mtr_a CASCADE");
        exec("DROP FUNCTION mtr_af()");
    }

    @Test
    void aRowAMergeMovesToAnotherPartitionIsReportedAsADeleteAndAnInsert() throws SQLException {
        exec("CREATE TABLE mtr_p (i int PRIMARY KEY, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE mtr_p1 PARTITION OF mtr_p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE mtr_p2 PARTITION OF mtr_p FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO mtr_p VALUES (1,'a'),(2,'b')");
        exec("CREATE TABLE mtr_ps (i int, j int, s text)");
        exec("INSERT INTO mtr_ps VALUES (2,12,'moved')");
        exec("CREATE TRIGGER mtr_p1_b BEFORE INSERT OR UPDATE OR DELETE ON mtr_p1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");
        exec("CREATE TRIGGER mtr_p1_a AFTER INSERT OR UPDATE OR DELETE ON mtr_p1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");
        exec("CREATE TRIGGER mtr_p2_b BEFORE INSERT OR UPDATE OR DELETE ON mtr_p2"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");
        exec("CREATE TRIGGER mtr_p2_a AFTER INSERT OR UPDATE OR DELETE ON mtr_p2"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_rnote()");

        // The partition the row left is asked first as an update and then as a delete, the one
        // it reached for an insert, and neither is given an AFTER UPDATE for it.
        exec("DELETE FROM ptg_log");
        assertEquals(1, update("MERGE INTO mtr_p t USING mtr_ps u ON t.i = u.i"
                + " WHEN MATCHED THEN UPDATE SET i = u.j, s = u.s"));
        assertEquals("BEFORE/UPDATE/mtr_p1/2/12,BEFORE/DELETE/mtr_p1/2/-,"
                + "BEFORE/INSERT/mtr_p2/-/12,AFTER/DELETE/mtr_p1/2/-,AFTER/INSERT/mtr_p2/-/12",
                firings());
        assertEquals("1|1", rows("SELECT (SELECT count(*) FROM mtr_p1),"
                + " (SELECT count(*) FROM mtr_p2)"));
        assertEquals("1|a,12|moved", rows("SELECT i, s FROM mtr_p ORDER BY i"));

        // A row that stays where it is is still an update, and only its own partition hears it.
        exec("DELETE FROM ptg_log");
        assertEquals(1, update("MERGE INTO mtr_p t USING (VALUES (12,15)) u(i,j) ON t.i = u.i"
                + " WHEN MATCHED THEN UPDATE SET i = u.j"));
        assertEquals("BEFORE/UPDATE/mtr_p2/12/15,AFTER/UPDATE/mtr_p2/12/15", firings());

        // And the delete arm through the parent is the storing partition's delete.
        exec("DELETE FROM ptg_log");
        assertEquals(1, update("MERGE INTO mtr_p t USING (VALUES (15)) u(i) ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE"));
        assertEquals("BEFORE/DELETE/mtr_p2/15/-,AFTER/DELETE/mtr_p2/15/-", firings());

        exec("DROP TABLE mtr_p CASCADE");
        exec("DROP TABLE mtr_ps");
    }

    // ============================================================ what a COPY fires, and when

    /** Sends a COPY FROM STDIN and answers with the number of rows the server reported. */
    private static long copyIn(String sql, String data) throws Exception {
        CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
        return copies.copyIn(sql, new StringReader(data));
    }

    @Test
    void copyFiresTheStatementTriggersOnceAndHoldsEveryAfterRowBackToTheEnd() throws Exception {
        exec("CREATE TABLE ptg_ct (i int, k int)");
        exec("CREATE TRIGGER ptg_ct_bs BEFORE INSERT ON ptg_ct"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_ct_br BEFORE INSERT ON ptg_ct"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_ct_ar AFTER INSERT ON ptg_ct"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_ct_as AFTER INSERT ON ptg_ct"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");

        // A COPY is one statement: the BEFORE statement trigger runs once before any row is
        // read, both AFTER row halves wait for the last row, and the AFTER statement trigger
        // closes it. Not BEFORE 1, AFTER 1, BEFORE 2, AFTER 2.
        exec("DELETE FROM ptg_log");
        assertEquals(2L, copyIn("COPY ptg_ct FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/1,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/2,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/1,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/2,"
                + "ptg_ct_as/AFTER/STATEMENT/ptg_ct", firings());

        // Which is the sequence a multi-row INSERT into the same relation fires.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_ct VALUES (7,5),(8,15)");
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/7,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/8,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/7,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/8,"
                + "ptg_ct_as/AFTER/STATEMENT/ptg_ct", firings());

        // A copy carrying no data at all still fires both statement-level triggers.
        exec("DELETE FROM ptg_log");
        assertEquals(0L, copyIn("COPY ptg_ct FROM STDIN", ""));
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,ptg_ct_as/AFTER/STATEMENT/ptg_ct",
                firings());

        // A row the WHERE leaves out never reaches a row trigger, and the statement's own
        // triggers still stand around what is left.
        exec("DELETE FROM ptg_log");
        assertEquals(1L, copyIn("COPY ptg_ct FROM STDIN WHERE k > 10", "31\t5\n32\t15\n"));
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/32,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/32,"
                + "ptg_ct_as/AFTER/STATEMENT/ptg_ct", firings());

        // Writing the schema, or a column list, changes none of it.
        exec("DELETE FROM ptg_log");
        assertEquals(1L, copyIn("COPY public.ptg_ct FROM STDIN", "41\t5\n"));
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/41,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/41,"
                + "ptg_ct_as/AFTER/STATEMENT/ptg_ct", firings());

        exec("DELETE FROM ptg_log");
        assertEquals(1L, copyIn("COPY ptg_ct (i) FROM STDIN", "51\n"));
        assertEquals("ptg_ct_bs/BEFORE/STATEMENT/ptg_ct,"
                + "ptg_ct_br/BEFORE/ROW/ptg_ct/51,"
                + "ptg_ct_ar/AFTER/ROW/ptg_ct/51,"
                + "ptg_ct_as/AFTER/STATEMENT/ptg_ct", firings());

        exec("DROP TABLE ptg_ct CASCADE");
    }

    @Test
    void copyIntoAPartitionedParentFiresTheParentsStatementTriggersAndTheLeavesRowTriggers()
            throws Exception {
        exec("CREATE TABLE ptg_cq (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE ptg_cq0 PARTITION OF ptg_cq FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE ptg_cq1 PARTITION OF ptg_cq FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER ptg_cq_bs BEFORE INSERT ON ptg_cq"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cq_as AFTER INSERT ON ptg_cq"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cq0_br BEFORE INSERT ON ptg_cq0"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cq0_ar AFTER INSERT ON ptg_cq0"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cq1_br BEFORE INSERT ON ptg_cq1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cq1_ar AFTER INSERT ON ptg_cq1"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");

        // The statement named the parent, so the parent's statement triggers bracket the whole
        // copy; each row is stored in a partition, so the row triggers are the partitions' own
        // -- and every AFTER half of all four rows stands after every BEFORE half.
        exec("DELETE FROM ptg_log");
        assertEquals(4L, copyIn("COPY ptg_cq FROM STDIN", "1\t5\n2\t15\n3\t6\n4\t16\n"));
        assertEquals("ptg_cq_bs/BEFORE/STATEMENT/ptg_cq,"
                + "ptg_cq0_br/BEFORE/ROW/ptg_cq0/1,"
                + "ptg_cq1_br/BEFORE/ROW/ptg_cq1/2,"
                + "ptg_cq0_br/BEFORE/ROW/ptg_cq0/3,"
                + "ptg_cq1_br/BEFORE/ROW/ptg_cq1/4,"
                + "ptg_cq0_ar/AFTER/ROW/ptg_cq0/1,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/2,"
                + "ptg_cq0_ar/AFTER/ROW/ptg_cq0/3,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/4,"
                + "ptg_cq_as/AFTER/STATEMENT/ptg_cq", firings());

        // Which is the sequence a multi-row INSERT through the parent fires.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_cq VALUES (11,5),(12,15),(13,6),(14,16)");
        assertEquals("ptg_cq_bs/BEFORE/STATEMENT/ptg_cq,"
                + "ptg_cq0_br/BEFORE/ROW/ptg_cq0/11,"
                + "ptg_cq1_br/BEFORE/ROW/ptg_cq1/12,"
                + "ptg_cq0_br/BEFORE/ROW/ptg_cq0/13,"
                + "ptg_cq1_br/BEFORE/ROW/ptg_cq1/14,"
                + "ptg_cq0_ar/AFTER/ROW/ptg_cq0/11,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/12,"
                + "ptg_cq0_ar/AFTER/ROW/ptg_cq0/13,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/14,"
                + "ptg_cq_as/AFTER/STATEMENT/ptg_cq", firings());

        // A copy that names the partition fires that partition's row triggers and none of the
        // parent's statement triggers, which belong to a statement that named the parent.
        exec("DELETE FROM ptg_log");
        assertEquals(2L, copyIn("COPY ptg_cq1 FROM STDIN", "21\t11\n22\t12\n"));
        assertEquals("ptg_cq1_br/BEFORE/ROW/ptg_cq1/21,"
                + "ptg_cq1_br/BEFORE/ROW/ptg_cq1/22,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/21,"
                + "ptg_cq1_ar/AFTER/ROW/ptg_cq1/22", firings());

        exec("DROP TABLE ptg_cq CASCADE");
    }

    @Test
    void aPartitionsOwnStatementTriggersBelongToAStatementThatNamedThePartition()
            throws Exception {
        exec("CREATE TABLE ptg_cs (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE ptg_cs0 PARTITION OF ptg_cs FOR VALUES FROM (0) TO (10)");
        exec("CREATE TRIGGER ptg_cs_bs BEFORE INSERT ON ptg_cs"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cs0_bs BEFORE INSERT ON ptg_cs0"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cs0_as AFTER INSERT ON ptg_cs0"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");

        exec("DELETE FROM ptg_log");
        assertEquals(1L, copyIn("COPY ptg_cs FROM STDIN", "1\t5\n"));
        assertEquals("ptg_cs_bs/BEFORE/STATEMENT/ptg_cs", firings());

        exec("DELETE FROM ptg_log");
        assertEquals(1L, copyIn("COPY ptg_cs0 FROM STDIN", "2\t6\n"));
        assertEquals("ptg_cs0_bs/BEFORE/STATEMENT/ptg_cs0,ptg_cs0_as/AFTER/STATEMENT/ptg_cs0",
                firings());

        exec("DROP TABLE ptg_cs CASCADE");
    }

    @Test
    void aBeforeRowTriggerThatAnswersWithNothingStillLeavesTheStatementTriggers()
            throws Exception {
        exec("CREATE TABLE ptg_cn (i int, k int)");
        exec("CREATE TRIGGER ptg_cn_bs BEFORE INSERT ON ptg_cn"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cn_br BEFORE INSERT ON ptg_cn"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnull()");
        exec("CREATE TRIGGER ptg_cn_ar AFTER INSERT ON ptg_cn"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_cn_as AFTER INSERT ON ptg_cn"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cnote()");

        // Each row is kept out and owes no AFTER half, and the copy counts none of them; the
        // statement's own triggers are owed either way.
        exec("DELETE FROM ptg_log");
        assertEquals(0L, copyIn("COPY ptg_cn FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals("ptg_cn_bs/BEFORE/STATEMENT/ptg_cn,"
                + "ptg_cn_br/BEFORE/ROW/ptg_cn/1/NULL,"
                + "ptg_cn_br/BEFORE/ROW/ptg_cn/2/NULL,"
                + "ptg_cn_as/AFTER/STATEMENT/ptg_cn", firings());
        assertEquals(0, num("SELECT count(*) FROM ptg_cn"));

        exec("DROP TABLE ptg_cn CASCADE");
    }

    @Test
    void severalRowTriggersOfOneTimingRunInNameOrderAcrossTheWholeCopy() throws Exception {
        exec("CREATE TABLE ptg_co (i int, k int)");
        exec("CREATE TRIGGER ptg_co_b2 BEFORE INSERT ON ptg_co"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_co_b1 BEFORE INSERT ON ptg_co"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_co_a2 AFTER INSERT ON ptg_co"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");
        exec("CREATE TRIGGER ptg_co_a1 AFTER INSERT ON ptg_co"
                + " FOR EACH ROW EXECUTE FUNCTION ptg_cnote()");

        // Triggers of one timing run in name order for each row, and every BEFORE half of every
        // row still stands before the first AFTER half of any row.
        exec("DELETE FROM ptg_log");
        assertEquals(2L, copyIn("COPY ptg_co FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals("ptg_co_b1/BEFORE/ROW/ptg_co/1,"
                + "ptg_co_b2/BEFORE/ROW/ptg_co/1,"
                + "ptg_co_b1/BEFORE/ROW/ptg_co/2,"
                + "ptg_co_b2/BEFORE/ROW/ptg_co/2,"
                + "ptg_co_a1/AFTER/ROW/ptg_co/1,"
                + "ptg_co_a2/AFTER/ROW/ptg_co/1,"
                + "ptg_co_a1/AFTER/ROW/ptg_co/2,"
                + "ptg_co_a2/AFTER/ROW/ptg_co/2", firings());

        exec("DROP TABLE ptg_co CASCADE");
    }

    @Test
    void theTransitionTableOfAnAfterStatementTriggerHoldsEveryRowTheCopyWrote() throws Exception {
        exec("CREATE TABLE ptg_cr (i int, k int)");
        exec("CREATE TRIGGER ptg_cr_as AFTER INSERT ON ptg_cr REFERENCING NEW TABLE AS ptg_cnew"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_cseen()");

        exec("DELETE FROM ptg_log");
        assertEquals(2L, copyIn("COPY ptg_cr FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals("ptg_cr_as/sees/2/1,2", firings());

        // Which is what it holds for an INSERT of the same two rows.
        exec("DELETE FROM ptg_log");
        exec("INSERT INTO ptg_cr VALUES (7,5),(8,15)");
        assertEquals("ptg_cr_as/sees/2/7,8", firings());

        exec("DROP TABLE ptg_cr CASCADE");
    }

    @Test
    void aStatementTriggerThatRaisesTakesTheWholeCopyWithIt() throws Exception {
        exec("CREATE TABLE ptg_cb (i int, k int)");
        exec("CREATE TRIGGER ptg_cb_bs BEFORE INSERT ON ptg_cb"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_craise()");
        // A BEFORE statement trigger that raises stops the copy before a row is read.
        assertThrows(Exception.class, () -> copyIn("COPY ptg_cb FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals(0, num("SELECT count(*) FROM ptg_cb"));
        exec("DROP TABLE ptg_cb CASCADE");

        exec("CREATE TABLE ptg_ca (i int, k int)");
        exec("CREATE TRIGGER ptg_ca_as AFTER INSERT ON ptg_ca"
                + " FOR EACH STATEMENT EXECUTE FUNCTION ptg_craise()");
        // And an AFTER one takes back every row the copy had already stored.
        assertThrows(Exception.class, () -> copyIn("COPY ptg_ca FROM STDIN", "1\t5\n2\t15\n"));
        assertEquals(0, num("SELECT count(*) FROM ptg_ca"));
        exec("DROP TABLE ptg_ca CASCADE");
    }

    // ============================================================ a MERGE reading from a snapshot

    @Test
    void aRepeatableReadMergerRunsItsArmAgainstTheRowTheOtherSessionCommitted() throws Exception {
        plainTarget("msn_u1");
        source("msn_u1s");
        // PostgreSQL's MERGE re-reads a row another transaction has written and runs its arm
        // against that version rather than ending the transaction over the write it lost.
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM msn_u1",
                "UPDATE msn_u1 SET v = 99 WHERE i = 2", "COMMIT", mergeInto("msn_u1", "msn_u1s")));
        assertEquals("1|10|a,2|99|x,3|30|c,4|400|y", rows("SELECT * FROM msn_u1 ORDER BY i"));
        exec("DROP TABLE msn_u1");
        exec("DROP TABLE msn_u1s");
    }

    @Test
    void aSerializableMergerRunsItsArmAgainstTheRowTheOtherSessionCommitted() throws Exception {
        plainTarget("msn_u2");
        source("msn_u2s");
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM msn_u2",
                "UPDATE msn_u2 SET v = 99 WHERE i = 2", "COMMIT", mergeInto("msn_u2", "msn_u2s")));
        assertEquals("1|10|a,2|99|x,3|30|c,4|400|y", rows("SELECT * FROM msn_u2 ORDER BY i"));
        exec("DROP TABLE msn_u2");
        exec("DROP TABLE msn_u2s");
    }

    @Test
    void aMergerReadingFromASnapshotTakesAnUpdateCommittedAfterItLooked() throws Exception {
        plainTarget("msn_u3");
        source("msn_u3s");
        assertEquals("[2 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM msn_u3",
                "UPDATE msn_u3 SET v = 99 WHERE i = 2", mergeInto("msn_u3", "msn_u3s")));
        // The merging transaction rolled back, so what stands is the other session's own write.
        assertEquals("1|10|a,2|99|b,3|30|c", rows("SELECT * FROM msn_u3 ORDER BY i"));
        exec("DROP TABLE msn_u3");
        exec("DROP TABLE msn_u3s");
    }

    @Test
    void aRepeatableReadMergerIsEndedByTheRowTheOtherSessionDeleted() throws Exception {
        plainTarget("msn_d1");
        source("msn_d1s");
        // There is no version of the row left for the arm to write, and taking the NOT MATCHED
        // arm for its source row would insert a key the relation held a moment ago -- so this is
        // the one case PostgreSQL ends the statement over rather than re-reading.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM msn_d1",
                        "DELETE FROM msn_d1 WHERE i = 2", "COMMIT",
                        mergeInto("msn_d1", "msn_d1s")));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM msn_d1 ORDER BY i"));
        exec("DROP TABLE msn_d1");
        exec("DROP TABLE msn_d1s");
    }

    @Test
    void aSerializableDeleteArmIsEndedByTheRowTheOtherSessionDeleted() throws Exception {
        plainTarget("msn_d2");
        source("msn_d2s");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "SERIALIZABLE", "SELECT count(*) FROM msn_d2",
                        "DELETE FROM msn_d2 WHERE i = 2", "COMMIT",
                        "MERGE INTO msn_d2 t USING msn_d2s u ON t.i = u.i"
                                + " WHEN MATCHED THEN DELETE"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM msn_d2 ORDER BY i"));
        exec("DROP TABLE msn_d2");
        exec("DROP TABLE msn_d2s");
    }

    @Test
    void theNotMatchedBySourceArmIsHeldToTheSameTwoRules() throws Exception {
        plainTarget("msn_n1");
        source("msn_n1s");
        // The arm runs for the rows the source paired with nothing, and one of them is the row
        // the other session wrote: it is re-read like any other.
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM msn_n1",
                "UPDATE msn_n1 SET v = 91 WHERE i = 1", "COMMIT",
                "MERGE INTO msn_n1 t USING msn_n1s u ON t.i = u.i"
                        + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 'ns'"));
        assertEquals("1|91|ns,2|20|b,3|30|ns", rows("SELECT * FROM msn_n1 ORDER BY i"));
        // And the row the other session took away ends the statement, as it does for any arm.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM msn_n1",
                        "DELETE FROM msn_n1 WHERE i = 1", "COMMIT",
                        "MERGE INTO msn_n1 t USING msn_n1s u ON t.i = u.i"
                                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET s = 'nd'"));
        assertEquals("2|20|b,3|30|ns", rows("SELECT * FROM msn_n1 ORDER BY i"));
        exec("DROP TABLE msn_n1");
        exec("DROP TABLE msn_n1s");
    }

    @Test
    void aReadCommittedMergerLeavesTheSourceRowOfADeletedRowToTheNotMatchedArm() throws Exception {
        plainTarget("msn_r1");
        source("msn_r1s");
        // A transaction that reads each statement afresh really does find the row gone, so its
        // source row is genuinely unpaired and the NOT MATCHED arm is the one that runs.
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM msn_r1",
                "DELETE FROM msn_r1 WHERE i = 2", "COMMIT", mergeInto("msn_r1", "msn_r1s")));
        assertEquals("1|10|a,2|200|x,3|30|c,4|400|y", rows("SELECT * FROM msn_r1 ORDER BY i"));
        exec("DROP TABLE msn_r1");
        exec("DROP TABLE msn_r1s");
    }

    @Test
    void aMergerReadingFromASnapshotIsNotEndedByADeleteThatWasRolledBack() throws Exception {
        plainTarget("msn_d3");
        source("msn_d3s");
        assertEquals("[2 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM msn_d3",
                "DELETE FROM msn_d3 WHERE i = 2", "ROLLBACK", mergeInto("msn_d3", "msn_d3s")));
        assertEquals("1|10|a,2|20|x,3|30|c,4|400|y", rows("SELECT * FROM msn_d3 ORDER BY i"));
        exec("DROP TABLE msn_d3");
        exec("DROP TABLE msn_d3s");
    }

    @Test
    void anOrdinaryUpdateOrDeleteReadingFromASnapshotIsStillRefusedTheWriteItLost()
            throws Exception {
        plainTarget("msn_p1");
        // Nothing here changes for an UPDATE or a DELETE of its own: only MERGE re-reads.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM msn_p1",
                        "UPDATE msn_p1 SET v = 99 WHERE i = 2", "COMMIT",
                        "UPDATE msn_p1 SET s = 'z' WHERE i = 2"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM msn_p1",
                        "UPDATE msn_p1 SET v = 98 WHERE i = 3", "COMMIT",
                        "DELETE FROM msn_p1 WHERE i = 3"));
        assertEquals("1|10|a,2|99|b,3|98|c", rows("SELECT * FROM msn_p1 ORDER BY i"));
        exec("DROP TABLE msn_p1");
    }

    // ============================================================ a write reading from a snapshot,
    // and the row another session took away

    @Test
    void anUpdateReadingFromASnapshotIsRefusedTheRowAConcurrentDeleteTook() throws Exception {
        plainTarget("snw_u1");
        // There is no version of the row left that both answers the statement and belongs to this
        // transaction's snapshot, so PostgreSQL ends the transaction rather than let the statement
        // report that it wrote nothing.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_u1",
                        "DELETE FROM snw_u1 WHERE i = 2", "COMMIT",
                        "UPDATE snw_u1 SET s = 'z' WHERE i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_u1 ORDER BY i"));
        exec("DROP TABLE snw_u1");
    }

    @Test
    void aDeleteReadingFromASnapshotIsRefusedTheRowAConcurrentDeleteTook() throws Exception {
        plainTarget("snw_d1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_d1",
                        "DELETE FROM snw_d1 WHERE i = 2", "COMMIT",
                        "DELETE FROM snw_d1 WHERE i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_d1 ORDER BY i"));
        exec("DROP TABLE snw_d1");
    }

    @Test
    void aSerializableUpdateAndDeleteAreRefusedTheSameRow() throws Exception {
        plainTarget("snw_s1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "SERIALIZABLE", "SELECT count(*) FROM snw_s1",
                        "DELETE FROM snw_s1 WHERE i = 2", "COMMIT",
                        "UPDATE snw_s1 SET s = 'z' WHERE i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_s1 ORDER BY i"));
        exec("DROP TABLE snw_s1");

        plainTarget("snw_s2");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "SERIALIZABLE", "SELECT count(*) FROM snw_s2",
                        "DELETE FROM snw_s2 WHERE i = 2", "COMMIT",
                        "DELETE FROM snw_s2 WHERE i = 2"));
        exec("DROP TABLE snw_s2");
    }

    @Test
    void theRefusalIsOwedEvenWithNothingLeftToWaitFor() throws Exception {
        plainTarget("snw_c1");
        // The delete was committed before the write began: this transaction was shown the row when
        // it took its snapshot, and the relation no longer holds it.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotAfterAnotherSessionCommitted(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_c1",
                        "DELETE FROM snw_c1 WHERE i = 2",
                        "UPDATE snw_c1 SET s = 'z' WHERE i = 2"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotAfterAnotherSessionCommitted(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_c1",
                        "DELETE FROM snw_c1 WHERE i = 3",
                        "DELETE FROM snw_c1 WHERE i = 3"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotAfterAnotherSessionCommitted(
                        "SERIALIZABLE", "SELECT count(*) FROM snw_c1",
                        "DELETE FROM snw_c1 WHERE i = 1",
                        "UPDATE snw_c1 SET s = 'z' WHERE i = 1"));
        exec("DROP TABLE snw_c1");
    }

    @Test
    void aDeleteThatWasRolledBackLeavesTheRowToBeWritten() throws Exception {
        plainTarget("snw_r1");
        // The delete never happened, so the row this transaction was shown is still its own.
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM snw_r1",
                "DELETE FROM snw_r1 WHERE i = 2", "ROLLBACK",
                "UPDATE snw_r1 SET s = 'z' WHERE i = 2"));
        assertEquals("1|10|a,2|20|z,3|30|c", rows("SELECT * FROM snw_r1 ORDER BY i"));
        exec("DROP TABLE snw_r1");

        plainTarget("snw_r2");
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM snw_r2",
                "DELETE FROM snw_r2 WHERE i = 2", "ROLLBACK",
                "DELETE FROM snw_r2 WHERE i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_r2 ORDER BY i"));
        exec("DROP TABLE snw_r2");
    }

    @Test
    void aWriteThatNeverReachesTheRowTheOtherSessionTookIsNotRefused() throws Exception {
        plainTarget("snw_q1");
        // The qualification settles this as it settles everything else: a row the statement was
        // never going to write is not a row it lost.
        assertEquals("[1 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM snw_q1",
                "DELETE FROM snw_q1 WHERE i = 2", "UPDATE snw_q1 SET s = 'z' WHERE i = 1"));
        assertEquals("[0 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM snw_q1",
                "DELETE FROM snw_q1 WHERE i = 3", "UPDATE snw_q1 SET s = 'z' WHERE i = 9"));
        exec("DROP TABLE snw_q1");
    }

    @Test
    void aWriteThroughAJoinIsRefusedTheRowAConcurrentDeleteTook() throws Exception {
        plainTarget("snw_j1");
        exec("CREATE TABLE snw_j1s (i int)");
        exec("INSERT INTO snw_j1s VALUES (1),(2),(3)");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_j1",
                        "DELETE FROM snw_j1 WHERE i = 2", "COMMIT",
                        "UPDATE snw_j1 t SET s = 'z' FROM snw_j1s u WHERE t.i = u.i AND u.i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_j1 ORDER BY i"));
        exec("DROP TABLE snw_j1");

        plainTarget("snw_j2");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_j2",
                        "DELETE FROM snw_j2 WHERE i = 2", "COMMIT",
                        "DELETE FROM snw_j2 t USING snw_j1s u WHERE t.i = u.i AND u.i = 2"));
        exec("DROP TABLE snw_j2");
        exec("DROP TABLE snw_j1s");
    }

    /**
     * The same, with the delete already committed before the write begins, so nothing waits for
     * anything and the outcome does not depend on how the two sessions interleave.
     *
     * <p>A write through a join reads its qualification over the other relation as well as the
     * target, and the row it lost has to be judged the same way. Judged against the target alone
     * the other relation's names resolved to nothing, the qualification could not be read at
     * all, and being unable to read it was taken for the row not matching -- so the statement
     * reported that it wrote nothing about a row it was entitled to write.
     */
    @Test
    void aWriteThroughAJoinIsRefusedTheRowAnAlreadyCommittedDeleteTook() throws Exception {
        exec("CREATE TABLE snw_k1s (i int)");
        exec("INSERT INTO snw_k1s VALUES (1),(2),(3)");

        plainTarget("snw_k1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotAfterAnotherSessionCommitted(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_k1",
                        "DELETE FROM snw_k1 WHERE i = 2",
                        "UPDATE snw_k1 t SET s = 'z' FROM snw_k1s u WHERE t.i = u.i AND u.i = 2"));
        exec("DROP TABLE snw_k1");

        plainTarget("snw_k2");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotAfterAnotherSessionCommitted(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_k2",
                        "DELETE FROM snw_k2 WHERE i = 2",
                        "DELETE FROM snw_k2 t USING snw_k1s u WHERE t.i = u.i AND u.i = 2"));
        exec("DROP TABLE snw_k2");

        // A row the join was never going to write is not a row the statement lost.
        plainTarget("snw_k3");
        assertEquals("[1 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM snw_k3",
                "DELETE FROM snw_k3 WHERE i = 2",
                "UPDATE snw_k3 t SET s = 'z' FROM snw_k1s u WHERE t.i = u.i AND u.i = 1"));
        exec("DROP TABLE snw_k3");

        plainTarget("snw_k4");
        assertEquals("[1 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM snw_k4",
                "DELETE FROM snw_k4 WHERE i = 2",
                "DELETE FROM snw_k4 t USING snw_k1s u WHERE t.i = u.i AND u.i = 3"));
        exec("DROP TABLE snw_k4");

        exec("DROP TABLE snw_k1s");
    }

    @Test
    void aRowOfAPartitionedRelationIsHeldToTheSameRule() throws Exception {
        partitionedTarget("snw_p1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_p1",
                        "DELETE FROM snw_p1 WHERE i = 2", "COMMIT",
                        "DELETE FROM snw_p1 WHERE i = 2"));
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_p1 ORDER BY i"));
        exec("DROP TABLE snw_p1 CASCADE");
    }

    @Test
    void theWholeTransactionEndsSoNoOtherRowOfTheStatementIsWrittenEither() throws Exception {
        plainTarget("snw_a1");
        try (Connection other = openSession(); Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM snw_a1");
            execOn(other, "DELETE FROM snw_a1 WHERE i = 2");
            // One of the two rows the statement was going to write was taken from it, and the
            // statement is refused whole: the other row is not written either.
            assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                    answerOf(actor, "UPDATE snw_a1 SET s = 'z' WHERE i < 3"));
            assertTrue(answerOf(actor, "SELECT count(*) FROM snw_a1").startsWith("ERR[25P02] "),
                    "the transaction was left able to run another statement");
            execOn(actor, "ROLLBACK");
            assertEquals("2;", answerOf(actor, "SELECT count(*) FROM snw_a1"));
        }
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_a1 ORDER BY i"));
        exec("DROP TABLE snw_a1");
    }

    @Test
    void theRowIsStillReadFromTheSnapshotThatMayNotWriteIt() throws Exception {
        plainTarget("snw_v1");
        try (Connection other = openSession(); Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM snw_v1");
            execOn(other, "DELETE FROM snw_v1 WHERE i = 2");
            // The snapshot still holds the row and answers a query with it; it is only the write
            // that has nothing left to act on.
            assertEquals("1|10|a;2|20|b;3|30|c;",
                    answerOf(actor, "SELECT * FROM snw_v1 ORDER BY i"));
            assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent delete",
                    answerOf(actor, "UPDATE snw_v1 SET s = 'z' WHERE i = 2"));
            execOn(actor, "ROLLBACK");
        }
        exec("DROP TABLE snw_v1");
    }

    // ============================================================ what the refusal must not
    // over-reach into

    @Test
    void aConcurrentCommittedUpdateIsStillReportedAsAConcurrentUpdate() throws Exception {
        plainTarget("snw_w1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_w1",
                        "UPDATE snw_w1 SET v = 99 WHERE i = 2", "COMMIT",
                        "UPDATE snw_w1 SET s = 'z' WHERE i = 2"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_w1",
                        "UPDATE snw_w1 SET v = 98 WHERE i = 3", "COMMIT",
                        "DELETE FROM snw_w1 WHERE i = 3"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "SERIALIZABLE", "SELECT count(*) FROM snw_w1",
                        "UPDATE snw_w1 SET v = 97 WHERE i = 1", "COMMIT",
                        "UPDATE snw_w1 SET s = 'z' WHERE i = 1"));
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "SERIALIZABLE", "SELECT count(*) FROM snw_w1",
                        "UPDATE snw_w1 SET v = 96 WHERE i = 2", "COMMIT",
                        "DELETE FROM snw_w1 WHERE i = 2"));
        assertEquals("1|97|a,2|96|b,3|98|c", rows("SELECT * FROM snw_w1 ORDER BY i"));
        exec("DROP TABLE snw_w1");
    }

    @Test
    void anUpdateThatWasRolledBackAlsoLeavesTheRowToBeWritten() throws Exception {
        plainTarget("snw_w2");
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM snw_w2",
                "UPDATE snw_w2 SET v = 99 WHERE i = 2", "ROLLBACK",
                "UPDATE snw_w2 SET s = 'z' WHERE i = 2"));
        assertEquals("1|10|a,2|20|z,3|30|c", rows("SELECT * FROM snw_w2 ORDER BY i"));
        exec("DROP TABLE snw_w2");
    }

    @Test
    void readCommittedFindsTheRowGoneAndReportsThatItWroteNothing() throws Exception {
        plainTarget("snw_k1");
        // A transaction that reads each statement afresh really does find the row gone by the time
        // it looks again, and nothing was taken from it.
        assertEquals("[0 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM snw_k1",
                "DELETE FROM snw_k1 WHERE i = 2", "COMMIT",
                "UPDATE snw_k1 SET s = 'z' WHERE i = 2"));
        assertEquals("[0 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM snw_k1",
                "DELETE FROM snw_k1 WHERE i = 3", "COMMIT",
                "DELETE FROM snw_k1 WHERE i = 3"));
        assertEquals("[0 rows]", underSnapshotAfterAnotherSessionCommitted(
                "READ COMMITTED", "SELECT count(*) FROM snw_k1",
                "DELETE FROM snw_k1 WHERE i = 1", "UPDATE snw_k1 SET s = 'z' WHERE i = 1"));
        assertEquals("", rows("SELECT * FROM snw_k1 ORDER BY i"));
        exec("DROP TABLE snw_k1");
    }

    @Test
    void readCommittedRunsItsWriteAgainstTheVersionTheOtherSessionCommitted() throws Exception {
        plainTarget("snw_k2");
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM snw_k2",
                "UPDATE snw_k2 SET v = 99 WHERE i = 2", "COMMIT",
                "UPDATE snw_k2 SET s = 'z' WHERE i = 2"));
        assertEquals("1|10|a,2|99|z,3|30|c", rows("SELECT * FROM snw_k2 ORDER BY i"));
        exec("DROP TABLE snw_k2");
    }

    @Test
    void aDeleteCommittedBeforeTheSnapshotWasTakenLeavesNothingToRefuse() throws Exception {
        plainTarget("snw_b1");
        try (Connection other = openSession(); Connection actor = openSession()) {
            execOn(other, "DELETE FROM snw_b1 WHERE i = 2");
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM snw_b1");
            // The snapshot was never shown the row, so the write that reaches nothing simply
            // reaches nothing.
            assertEquals("[0 rows]", answerOf(actor, "UPDATE snw_b1 SET s = 'z' WHERE i = 2"));
            assertEquals("[0 rows]", answerOf(actor, "DELETE FROM snw_b1 WHERE i = 2"));
            assertEquals("2;", answerOf(actor, "SELECT count(*) FROM snw_b1"));
            execOn(actor, "COMMIT");
        }
        exec("DROP TABLE snw_b1");
    }

    @Test
    void aTransactionIsNotRefusedARowItTookAwayItself() throws Exception {
        plainTarget("snw_o1");
        try (Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM snw_o1");
            execOn(actor, "DELETE FROM snw_o1 WHERE i = 2");
            assertEquals("[0 rows]", answerOf(actor, "UPDATE snw_o1 SET s = 'z' WHERE i = 2"));
            assertEquals("[0 rows]", answerOf(actor, "DELETE FROM snw_o1 WHERE i = 2"));
            execOn(actor, "COMMIT");
        }
        assertEquals("1|10|a,3|30|c", rows("SELECT * FROM snw_o1 ORDER BY i"));
        exec("DROP TABLE snw_o1");
    }

    @Test
    void lockingTheRowRatherThanWritingItIsToldOfAsAConcurrentUpdate() throws Exception {
        plainTarget("snw_l1");
        // The lock is taken on the row rather than on the snapshot's copy of it, so the query waits
        // for the session holding the row and is then answered from what became of it.
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_l1",
                        "DELETE FROM snw_l1 WHERE i = 2", "COMMIT",
                        "SELECT i, v FROM snw_l1 WHERE i = 2 FOR UPDATE"));
        exec("DROP TABLE snw_l1");

        plainTarget("snw_l2");
        assertEquals("2|20;", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM snw_l2",
                "DELETE FROM snw_l2 WHERE i = 2", "ROLLBACK",
                "SELECT i, v FROM snw_l2 WHERE i = 2 FOR UPDATE"));
        exec("DROP TABLE snw_l2");
    }

    @Test
    void aRowMovedToAnotherPartitionIsToldOfInTheWordsItsIsolationLevelEarns() throws Exception {
        partitionedTarget("snw_m1");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "REPEATABLE READ", "SELECT count(*) FROM snw_m1",
                        "UPDATE snw_m1 SET i = 12 WHERE i = 2", "COMMIT",
                        "UPDATE snw_m1 SET s = 'z' WHERE i = 2"));
        exec("DROP TABLE snw_m1 CASCADE");

        partitionedTarget("snw_m2");
        assertEquals("ERR[40001] ERROR: tuple to be locked was already moved to another partition"
                        + " due to concurrent update",
                underSnapshotWhileAnotherSessionHolds(
                        "READ COMMITTED", "SELECT count(*) FROM snw_m2",
                        "UPDATE snw_m2 SET i = 12 WHERE i = 2", "COMMIT",
                        "UPDATE snw_m2 SET s = 'z' WHERE i = 2"));
        exec("DROP TABLE snw_m2 CASCADE");
    }

    @Test
    void theKeyAConcurrentCommittedDeleteFreedMayStillBeTaken() throws Exception {
        plainTarget("snw_i1");
        // An INSERT acts on no version of anything, so a snapshot has nothing to refuse it with.
        assertEquals("[1 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM snw_i1",
                "DELETE FROM snw_i1 WHERE i = 2", "INSERT INTO snw_i1 VALUES (2,22,'n')"));
        exec("DROP TABLE snw_i1");
    }

    // ============================================================ a row a trigger rewrote stays in
    // the partition it was put in

    /** The trigger functions the partition-key tests below are written with. */
    private static void keyRewritingFunctions() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION pkr_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE OR REPLACE FUNCTION pkr_touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.s := NEW.s || '!'; RETURN NEW; END $$");
        exec("CREATE OR REPLACE FUNCTION pkr_pass() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RETURN NEW; END $$");
        exec("CREATE OR REPLACE FUNCTION pkr_veto() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RETURN NULL; END $$");
        exec("CREATE OR REPLACE FUNCTION pkr_settle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := 5; RETURN NEW; END $$");
    }

    @Test
    void aTriggerOnThePartitionLeavesTheRowToFailThatPartitionsBound() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_p (i int, k int, s text) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_p0 PARTITION OF pkr_p FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_p1 PARTITION OF pkr_p FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_p0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");

        // Where the row is stored was settled before the trigger ran, so the row the trigger left
        // is a row that partition may not hold -- and the partition is named however the statement
        // reached it.
        ServerErrorMessage viaParent = fieldsOf("INSERT INTO pkr_p VALUES (1, 5, 'a')");
        assertEquals("23514", viaParent.getSQLState());
        assertEquals("new row for relation \"pkr_p0\" violates partition constraint",
                viaParent.getMessage());
        assertEquals("Failing row contains (1, 15, a).", viaParent.getDetail());

        ServerErrorMessage viaPartition = fieldsOf("INSERT INTO pkr_p0 VALUES (2, 6, 'b')");
        assertEquals("23514", viaPartition.getSQLState());
        assertEquals("new row for relation \"pkr_p0\" violates partition constraint",
                viaPartition.getMessage());
        assertEquals("Failing row contains (2, 16, b).", viaPartition.getDetail());

        assertEquals(0, num("SELECT count(*) FROM pkr_p"));
        exec("DROP TABLE pkr_p");
    }

    @Test
    void routingReadsTheKeyTheStatementWroteRatherThanTheOneATriggerLeaves() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_g (i int, k int, s text) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_g0 PARTITION OF pkr_g FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_g1 PARTITION OF pkr_g FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_g1"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");

        // Nothing tests the bound of a partition the statement named until that partition's
        // triggers are done with the row, so a trigger there may settle a key it does hold.
        exec("INSERT INTO pkr_g1 VALUES (3, 1, 'c')");
        // And a row routed by the partitioned table goes where the key it was written with says,
        // whatever the trigger of the other partition would have made of it.
        exec("INSERT INTO pkr_g VALUES (4, 1, 'd')");
        assertEquals("pkr_g1|3|11|c,pkr_g0|4|1|d",
                rows("SELECT tableoid::regclass::text, i, k, s FROM pkr_g ORDER BY i"));
        exec("DROP TABLE pkr_g");
    }

    @Test
    void aCopyOfThePartitionedTablesTriggerMayNotMoveTheRow() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_q (i int, k int, s text) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_q0 PARTITION OF pkr_q FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_q1 PARTITION OF pkr_q FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER pkr_b_move BEFORE INSERT ON pkr_q"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");

        // The copy runs as the partition's own trigger, and the row it rewrote is stopped at the
        // trigger rather than at the bound, naming the trigger and where the row was to be.
        for (String statement : new String[] {"INSERT INTO pkr_q VALUES (1, 5, 'a')",
                "INSERT INTO pkr_q0 VALUES (2, 6, 'b')"}) {
            ServerErrorMessage m = fieldsOf(statement);
            assertEquals("0A000", m.getSQLState());
            assertEquals("moving row to another partition during a BEFORE FOR EACH ROW trigger"
                    + " is not supported", m.getMessage());
            assertEquals("Before executing trigger \"pkr_b_move\", the row was to be in partition"
                    + " \"public.pkr_q0\".", m.getDetail());
        }

        // A copy that leaves the row where it belongs moved nothing.
        exec("INSERT INTO pkr_q1 VALUES (3, 1, 'c')");
        assertEquals("pkr_q1|3|11|c",
                rows("SELECT tableoid::regclass::text, i, k, s FROM pkr_q ORDER BY i"));
        exec("DROP TABLE pkr_q");
    }

    @Test
    void theCopyThatRewroteTheRowIsTheOneTheMoveIsReportedAgainst() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_w (i int, k int, s text) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_w0 PARTITION OF pkr_w FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_w1 PARTITION OF pkr_w FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_w0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        exec("CREATE TRIGGER pkr_b_touch BEFORE INSERT ON pkr_w"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_touch()");

        // The partition's own trigger moved the key and the copy that ran after it changed a
        // column that has nothing to do with the key: the copy is still what is reported.
        ServerErrorMessage moved = fieldsOf("INSERT INTO pkr_w VALUES (1, 5, 'a')");
        assertEquals("0A000", moved.getSQLState());
        assertEquals("Before executing trigger \"pkr_b_touch\", the row was to be in partition"
                + " \"public.pkr_w0\".", moved.getDetail());

        exec("DROP TRIGGER pkr_b_touch ON pkr_w");
        exec("CREATE TRIGGER pkr_b_pass BEFORE INSERT ON pkr_w"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_pass()");

        // A copy that hands the row back untouched rewrote nothing, so the row is left to fail the
        // bound of the partition it was routed to.
        ServerErrorMessage bound = fieldsOf("INSERT INTO pkr_w VALUES (2, 5, 'b')");
        assertEquals("23514", bound.getSQLState());
        assertEquals("new row for relation \"pkr_w0\" violates partition constraint",
                bound.getMessage());
        assertEquals("Failing row contains (2, 15, b).", bound.getDetail());

        // A copy that changes a column outside the key leaves the row where it is.
        exec("DROP TRIGGER pkr_a_move ON pkr_w0");
        exec("DROP TRIGGER pkr_b_pass ON pkr_w");
        exec("CREATE TRIGGER pkr_c_touch BEFORE INSERT ON pkr_w"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_touch()");
        exec("INSERT INTO pkr_w VALUES (3, 5, 'c')");
        assertEquals("pkr_w0|3|5|c!",
                rows("SELECT tableoid::regclass::text, i, k, s FROM pkr_w ORDER BY i"));
        exec("DROP TABLE pkr_w");
    }

    @Test
    void aTriggerThatAnswersWithNothingKeepsTheRowOutAndIsNoMove() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_v (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_v0 PARTITION OF pkr_v FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_v1 PARTITION OF pkr_v FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER pkr_a_veto BEFORE INSERT ON pkr_v0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_veto()");
        exec("INSERT INTO pkr_v VALUES (1, 5)");
        exec("INSERT INTO pkr_v VALUES (2, 15)");
        assertEquals("pkr_v1|2|15",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_v ORDER BY i"));
        exec("DROP TABLE pkr_v");
    }

    @Test
    void aSubPartitionedHierarchyNamesTheLeafTheRowWasRoutedTo() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_s (i int, k int, j int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_s0 PARTITION OF pkr_s FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (j)");
        exec("CREATE TABLE pkr_s00 PARTITION OF pkr_s0 FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE pkr_s1 PARTITION OF pkr_s FOR VALUES FROM (10) TO (20)"
                + " PARTITION BY RANGE (j)");
        exec("CREATE TABLE pkr_s10 PARTITION OF pkr_s1 FOR VALUES FROM (0) TO (100)");

        // The leaf's own trigger leaves a row that fails a bound belonging to the level above it,
        // and the leaf is what the report names.
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_s00"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage own = fieldsOf("INSERT INTO pkr_s VALUES (1, 5, 7)");
        assertEquals("23514", own.getSQLState());
        assertEquals("new row for relation \"pkr_s00\" violates partition constraint",
                own.getMessage());
        assertEquals("Failing row contains (1, 15, 7).", own.getDetail());
        exec("DROP TRIGGER pkr_a_move ON pkr_s00");

        // A copy handed down from the level between names the leaf it fired on, and so does one
        // handed down from the root.
        exec("CREATE TRIGGER pkr_b_move BEFORE INSERT ON pkr_s0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage middle = fieldsOf("INSERT INTO pkr_s VALUES (2, 5, 7)");
        assertEquals("0A000", middle.getSQLState());
        assertEquals("Before executing trigger \"pkr_b_move\", the row was to be in partition"
                + " \"public.pkr_s00\".", middle.getDetail());
        exec("DROP TRIGGER pkr_b_move ON pkr_s0");

        exec("CREATE TRIGGER pkr_c_move BEFORE INSERT ON pkr_s"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage root = fieldsOf("INSERT INTO pkr_s VALUES (3, 5, 7)");
        assertEquals("0A000", root.getSQLState());
        assertEquals("Before executing trigger \"pkr_c_move\", the row was to be in partition"
                + " \"public.pkr_s00\".", root.getDetail());
        exec("DROP TRIGGER pkr_c_move ON pkr_s");

        assertEquals(0, num("SELECT count(*) FROM pkr_s"));
        exec("DROP TABLE pkr_s");
    }

    @Test
    void aPartitionedPartitionTestsItsOwnBoundBeforeItRoutes() throws Exception {
        exec("CREATE TABLE pkr_x (i int, k int, j int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_x0 PARTITION OF pkr_x FOR VALUES FROM (0) TO (10)"
                + " PARTITION BY RANGE (j)");
        exec("CREATE TABLE pkr_x00 PARTITION OF pkr_x0 FOR VALUES FROM (0) TO (100)");

        ServerErrorMessage bound = fieldsOf("INSERT INTO pkr_x0 VALUES (1, 99, 7)");
        assertEquals("23514", bound.getSQLState());
        assertEquals("new row for relation \"pkr_x0\" violates partition constraint",
                bound.getMessage());
        assertEquals("Failing row contains (1, 99, 7).", bound.getDetail());

        ServerErrorMessage none = fieldsOf("INSERT INTO pkr_x0 VALUES (2, 5, 999)");
        assertEquals("23514", none.getSQLState());
        assertEquals("no partition of relation \"pkr_x0\" found for row", none.getMessage());
        assertEquals("Partition key of the failing row contains (j) = (999).", none.getDetail());

        exec("DROP TABLE pkr_x");
    }

    @Test
    void noTriggerGetsToSettleAKeyNoPartitionWillTake() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_n (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_n0 PARTITION OF pkr_n FOR VALUES FROM (0) TO (10)");
        exec("CREATE TRIGGER pkr_a_settle BEFORE INSERT ON pkr_n"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_settle()");

        // Routing runs first, so the row is refused before the trigger that would have rewritten
        // its key into one a partition holds.
        ServerErrorMessage m = fieldsOf("INSERT INTO pkr_n VALUES (1, 99)");
        assertEquals("23514", m.getSQLState());
        assertEquals("no partition of relation \"pkr_n\" found for row", m.getMessage());
        assertEquals("Partition key of the failing row contains (k) = (99).", m.getDetail());
        assertEquals(0, num("SELECT count(*) FROM pkr_n"));
        exec("DROP TABLE pkr_n");
    }

    @Test
    void theDefaultPartitionIsReadAsTheNegationOfItsSiblings() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_d (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_d0 PARTITION OF pkr_d FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_dd PARTITION OF pkr_d DEFAULT");
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_dd"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");

        // A key no sibling claims before the trigger and none claims after it leaves the row where
        // it was put.
        exec("INSERT INTO pkr_d VALUES (1, 50)");
        assertEquals("pkr_dd|1|60",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_d ORDER BY i"));

        // A trigger on a bounded partition may not hand the row to the default one.
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_d0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage m = fieldsOf("INSERT INTO pkr_d VALUES (2, 5)");
        assertEquals("23514", m.getSQLState());
        assertEquals("new row for relation \"pkr_d0\" violates partition constraint",
                m.getMessage());
        assertEquals("Failing row contains (2, 15).", m.getDetail());
        assertEquals("pkr_dd|1|60",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_d ORDER BY i"));
        exec("DROP TABLE pkr_d");
    }

    @Test
    void aBeforeUpdateTriggerMovesTheRowWhoseKeyItChanged() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_u (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_u0 PARTITION OF pkr_u FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_u1 PARTITION OF pkr_u FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO pkr_u VALUES (1, 5), (2, 6)");
        exec("CREATE TRIGGER pkr_a_move BEFORE UPDATE ON pkr_u0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");

        // A row whose key an UPDATE changes is meant to move, and it moves.
        exec("UPDATE pkr_u SET i = i WHERE i = 1");
        assertEquals("pkr_u1|1|15,pkr_u0|2|6",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_u ORDER BY i"));

        // An UPDATE that named the partition has nowhere to move the row to.
        ServerErrorMessage m = fieldsOf("UPDATE pkr_u0 SET i = i WHERE i = 2");
        assertEquals("23514", m.getSQLState());
        assertEquals("new row for relation \"pkr_u0\" violates partition constraint",
                m.getMessage());
        assertEquals("Failing row contains (2, 16).", m.getDetail());

        // and a copy handed down from the partitioned table moves the row just as readily.
        exec("DROP TRIGGER pkr_a_move ON pkr_u0");
        exec("CREATE TRIGGER pkr_b_move BEFORE UPDATE ON pkr_u"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        exec("UPDATE pkr_u SET i = i WHERE i = 2");
        assertEquals("pkr_u1|1|15,pkr_u1|2|16",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_u ORDER BY i"));
        exec("DROP TABLE pkr_u");
    }

    @Test
    void theInsertHalfOfARowMoveIsAnInsertLikeAnyOther() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_r (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_r0 PARTITION OF pkr_r FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_r1 PARTITION OF pkr_r FOR VALUES FROM (10) TO (20)");
        exec("CREATE TABLE pkr_r2 PARTITION OF pkr_r FOR VALUES FROM (20) TO (30)");
        exec("INSERT INTO pkr_r VALUES (1, 5)");

        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_r1"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage bound = fieldsOf("UPDATE pkr_r SET k = 15 WHERE i = 1");
        assertEquals("23514", bound.getSQLState());
        assertEquals("new row for relation \"pkr_r1\" violates partition constraint",
                bound.getMessage());
        assertEquals("Failing row contains (1, 25).", bound.getDetail());

        exec("DROP TRIGGER pkr_a_move ON pkr_r1");
        exec("CREATE TRIGGER pkr_b_move BEFORE INSERT ON pkr_r"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage moved = fieldsOf("UPDATE pkr_r SET k = 15 WHERE i = 1");
        assertEquals("0A000", moved.getSQLState());
        assertEquals("Before executing trigger \"pkr_b_move\", the row was to be in partition"
                + " \"public.pkr_r1\".", moved.getDetail());

        assertEquals("pkr_r0|1|5",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_r ORDER BY i"));
        exec("DROP TABLE pkr_r");
    }

    @Test
    void aMergeInsertArmAndAnOnConflictInsertAreReadTheSameWay() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_ms (i int, k int)");
        exec("INSERT INTO pkr_ms VALUES (1, 5), (2, 5)");
        exec("CREATE TABLE pkr_m (i int, k int, PRIMARY KEY (i, k)) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_m0 PARTITION OF pkr_m FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_m1 PARTITION OF pkr_m FOR VALUES FROM (10) TO (20)");
        String merge = "MERGE INTO pkr_m t USING pkr_ms u ON t.i = u.i"
                + " WHEN NOT MATCHED THEN INSERT (i, k) VALUES (u.i, u.k)";

        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_m0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage armBound = fieldsOf(merge);
        assertEquals("23514", armBound.getSQLState());
        assertEquals("new row for relation \"pkr_m0\" violates partition constraint",
                armBound.getMessage());
        assertEquals("Failing row contains (1, 15).", armBound.getDetail());

        exec("DROP TRIGGER pkr_a_move ON pkr_m0");
        exec("CREATE TRIGGER pkr_b_move BEFORE INSERT ON pkr_m"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        for (String statement : new String[] {merge,
                "INSERT INTO pkr_m VALUES (9, 5) ON CONFLICT (i, k) DO NOTHING"}) {
            ServerErrorMessage m = fieldsOf(statement);
            assertEquals("0A000", m.getSQLState());
            assertEquals("Before executing trigger \"pkr_b_move\", the row was to be in partition"
                    + " \"public.pkr_m0\".", m.getDetail());
        }

        // An ON CONFLICT arm that would have passed the row over is reached only once the row has
        // been found to belong where it was put.
        exec("DROP TRIGGER pkr_b_move ON pkr_m");
        exec("INSERT INTO pkr_m VALUES (1, 5)");
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_m0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        ServerErrorMessage conflicted =
                fieldsOf("INSERT INTO pkr_m VALUES (1, 5) ON CONFLICT (i, k) DO NOTHING");
        assertEquals("23514", conflicted.getSQLState());
        assertEquals("Failing row contains (1, 15).", conflicted.getDetail());
        assertEquals("pkr_m0|1|5",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_m ORDER BY i"));

        exec("DROP TABLE pkr_m");
        exec("DROP TABLE pkr_ms");
    }

    @Test
    void copyIsRoutedBeforeThePartitionsTriggersToo() throws Exception {
        keyRewritingFunctions();
        exec("CREATE TABLE pkr_c (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE pkr_c0 PARTITION OF pkr_c FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE pkr_c1 PARTITION OF pkr_c FOR VALUES FROM (10) TO (20)");

        // A COPY reaches the partitions the way an INSERT does, so the same two reports come out
        // of it. Here it is the state and the message that are read back; the whole field set a
        // refused COPY carries is read further down.
        exec("CREATE TRIGGER pkr_a_move BEFORE INSERT ON pkr_c0"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        assertEquals("23514/new row for relation \"pkr_c0\" violates partition constraint",
                copyReport("COPY pkr_c (i,k) FROM STDIN", "1\t5\n"));
        assertEquals("23514/new row for relation \"pkr_c0\" violates partition constraint",
                copyReport("COPY pkr_c0 (i,k) FROM STDIN", "2\t6\n"));
        assertEquals(0, num("SELECT count(*) FROM pkr_c"));

        exec("DROP TRIGGER pkr_a_move ON pkr_c0");
        exec("CREATE TRIGGER pkr_b_move BEFORE INSERT ON pkr_c"
                + " FOR EACH ROW EXECUTE FUNCTION pkr_bump()");
        String moved = "0A000/moving row to another partition during a BEFORE FOR EACH ROW trigger"
                + " is not supported";
        assertEquals(moved, copyReport("COPY pkr_c (i,k) FROM STDIN", "3\t5\n"));
        assertEquals(moved, copyReport("COPY pkr_c0 (i,k) FROM STDIN", "4\t6\n"));

        // and a copy straight at the partition that claims the rewritten key is no move.
        assertEquals("rows=1", copyReport("COPY pkr_c1 (i,k) FROM STDIN", "5\t1\n"));
        assertEquals("pkr_c1|5|11",
                rows("SELECT tableoid::regclass::text, i, k FROM pkr_c ORDER BY i"));
        exec("DROP TABLE pkr_c");
    }

    /** The rows a COPY reported, or the SQLSTATE and message of the error it raised. */
    private static String copyReport(String sql, String data) {
        try {
            return "rows=" + copyIn(sql, data);
        } catch (Exception e) {
            if (e instanceof PSQLException
                    && ((PSQLException) e).getServerErrorMessage() != null) {
                ServerErrorMessage m = ((PSQLException) e).getServerErrorMessage();
                return m.getSQLState() + "/" + m.getMessage();
            }
            return "EX " + e;
        }
    }

    // ============================================================ what a COPY says when it will
    // not take a row

    /** The trigger functions the COPY refusal tests below are written with. */
    private static void copyRefusalFunctions() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION crf_bump() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " NEW.k := NEW.k + 10; RETURN NEW; END $$");
        exec("CREATE OR REPLACE FUNCTION crf_raise() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " RAISE EXCEPTION 'no'; END $$");
    }

    /**
     * What a COPY FROM STDIN answers: the rows it stored, or the whole field set of the error it
     * raised -- the state, the primary message, the DETAIL and the field the protocol calls Where,
     * which a client prints as CONTEXT.
     */
    private static String copyFields(String sql, String data) {
        return copyFields(sql, data.getBytes(StandardCharsets.UTF_8));
    }

    private static String copyFields(String sql, byte[] data) {
        try {
            CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
            return "rows=" + copies.copyIn(sql, new ByteArrayInputStream(data));
        } catch (Exception e) {
            if (!(e instanceof PSQLException)
                    || ((PSQLException) e).getServerErrorMessage() == null) {
                return "EX " + e;
            }
            ServerErrorMessage m = ((PSQLException) e).getServerErrorMessage();
            return m.getSQLState() + "/" + m.getMessage() + "/" + m.getDetail() + "/" + m.getWhere();
        }
    }

    /** A PGCOPY stream of rows of int4, for the binary copies below. */
    private static byte[] binaryRows(int[][] rows) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(new byte[] {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0});
        out.write(new byte[] {0, 0, 0, 0});
        out.write(new byte[] {0, 0, 0, 0});
        for (int[] row : rows) {
            out.write(new byte[] {0, (byte) row.length});
            for (int v : row) {
                out.write(new byte[] {0, 0, 0, 4});
                out.write(new byte[] {
                        (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v});
            }
        }
        out.write(new byte[] {(byte) 0xFF, (byte) 0xFF});
        return out.toByteArray();
    }

    /**
     * A refused row is answered with more than a message: the row itself stands in the error's
     * DETAIL, and the CONTEXT names the relation, the line of the input the copy had reached and
     * that line as the sender wrote it -- for a sender of thousands of lines the only way to tell
     * which one was refused.
     */
    @Test
    void aRefusedRowCarriesTheFailingRowAndTheLineOfTheInputItCameFrom() throws Exception {
        exec("CREATE TABLE crf_c (i int, k int, CONSTRAINT crf_c_ck CHECK (k < 10))");
        String broken = "23514/new row for relation \"crf_c\" violates check constraint"
                + " \"crf_c_ck\"";

        assertEquals(broken + "/Failing row contains (1, 15)./COPY crf_c, line 1: \"1\t15\"",
                copyFields("COPY crf_c FROM STDIN", "1\t15\n"));
        // A line that was taken is still a line, so the one after it is line 2.
        assertEquals(broken + "/Failing row contains (2, 15)./COPY crf_c, line 2: \"2\t15\"",
                copyFields("COPY crf_c FROM STDIN", "1\t5\n2\t15\n"));
        // So is a header, which is why the first row of a CSV HEADER copy is line 2.
        assertEquals(broken + "/Failing row contains (1, 15)./COPY crf_c, line 2: \"1,15\"",
                copyFields("COPY crf_c FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,15\n"));
        assertEquals(broken + "/Failing row contains (2, 15)./COPY crf_c, line 3: \"2,15\"",
                copyFields("COPY crf_c FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,5\n2,15\n"));
        // The DETAIL is the row as the relation holds it and the CONTEXT the line as it was sent,
        // so a column list that reorders the fields shows in the one and not in the other.
        assertEquals(broken + "/Failing row contains (9, 15)./COPY crf_c, line 1: \"15\t9\"",
                copyFields("COPY crf_c (k,i) FROM STDIN", "15\t9\n"));
        // The relation is named as it is stored, not as the statement wrote it.
        assertEquals(broken + "/Failing row contains (1, 15)./COPY crf_c, line 1: \"1\t15\"",
                copyFields("COPY public.crf_c FROM STDIN", "1\t15\n"));

        // Nothing a refused copy read is left behind, not even the lines it had taken.
        assertEquals(0, num("SELECT count(*) FROM crf_c"));
        exec("DROP TABLE crf_c");
    }

    /** A column with nothing in it is the word null in the failing row. */
    @Test
    void aNotNullRefusalWritesTheEmptyColumnAsNullInTheFailingRow() throws Exception {
        exec("CREATE TABLE crf_n (i int, k int NOT NULL)");
        String broken = "23502/null value in column \"k\" of relation \"crf_n\""
                + " violates not-null constraint";

        assertEquals(broken + "/Failing row contains (1, null)./COPY crf_n, line 1: \"1\t\\N\"",
                copyFields("COPY crf_n FROM STDIN", "1\t\\N\n"));
        assertEquals(broken + "/Failing row contains (3, null)./COPY crf_n, line 2: \"3\t\\N\"",
                copyFields("COPY crf_n FROM STDIN", "1\t2\n3\t\\N\n"));
        // An empty CSV field is the same nothing, and the header is still line 1.
        assertEquals(broken + "/Failing row contains (1, null)./COPY crf_n, line 2: \"1,\"",
                copyFields("COPY crf_n FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,\n"));
        exec("DROP TABLE crf_n");
    }

    /**
     * A value a type's own reader will not take names the column and the value rather than the
     * line: the value is what the sender has to correct. The text quoted back is the one the
     * reader was handed, so an escape has been resolved and a CSV quote taken off by then.
     */
    @Test
    void aValueTheTypesReaderRefusesNamesTheColumnAndTheValueAndCarriesNoDetail() throws Exception {
        exec("CREATE TABLE crf_b (i int, k int)");

        assertEquals("22P02/invalid input syntax for type integer: \"abc\""
                        + "/null/COPY crf_b, line 1, column k: \"abc\"",
                copyFields("COPY crf_b FROM STDIN", "1\tabc\n"));
        assertEquals("22P02/invalid input syntax for type integer: \"xyz\""
                        + "/null/COPY crf_b, line 2, column i: \"xyz\"",
                copyFields("COPY crf_b FROM STDIN", "1\t2\nxyz\t3\n"));
        assertEquals("22P02/invalid input syntax for type integer: \"abc\""
                        + "/null/COPY crf_b, line 2, column k: \"abc\"",
                copyFields("COPY crf_b FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,abc\n"));
        assertEquals("22P02/invalid input syntax for type integer: \"a,b\""
                        + "/null/COPY crf_b, line 1, column k: \"a,b\"",
                copyFields("COPY crf_b FROM STDIN WITH (FORMAT csv)", "1,\"a,b\"\n"));
        // An empty line has one empty field, and it is the first column's reader that says so.
        assertEquals("22P02/invalid input syntax for type integer: \"\""
                        + "/null/COPY crf_b, line 2, column i: \"\"",
                copyFields("COPY crf_b FROM STDIN", "1\t2\n\n"));
        exec("DROP TABLE crf_b");
    }

    /** A line of the wrong width is the line's own complaint, so the line is what is quoted. */
    @Test
    void aLineOfTheWrongWidthIsQuotedWholeAndCarriesNoDetail() throws Exception {
        exec("CREATE TABLE crf_w (i int, k int)");
        assertEquals("22P04/missing data for column \"k\"/null/COPY crf_w, line 1: \"1\"",
                copyFields("COPY crf_w FROM STDIN", "1\n"));
        assertEquals("22P04/extra data after last expected column"
                        + "/null/COPY crf_w, line 1: \"1\t2\t3\"",
                copyFields("COPY crf_w FROM STDIN", "1\t2\t3\n"));
        assertEquals("22P04/missing data for column \"k\"/null/COPY crf_w, line 2: \"1\"",
                copyFields("COPY crf_w FROM STDIN", "1\t2\n1\n"));
        exec("DROP TABLE crf_w");
    }

    /**
     * A unique index says which line the row came from and not what was on it: the copy stores
     * its rows in batches and maintains the index as a batch goes in, by which time the line it
     * came from has been read over.
     */
    @Test
    void aDuplicateKeyNamesTheLineButNotTheTextThatWasOnIt() throws Exception {
        exec("CREATE TABLE crf_u (i int PRIMARY KEY, k int)");
        String broken = "23505/duplicate key value violates unique constraint \"crf_u_pkey\"";

        assertEquals(broken + "/Key (i)=(1) already exists./COPY crf_u, line 2",
                copyFields("COPY crf_u FROM STDIN", "1\t1\n1\t2\n"));
        // A row that was already stored is met on the line that collides with it.
        exec("INSERT INTO crf_u VALUES (7,7)");
        assertEquals(broken + "/Key (i)=(7) already exists./COPY crf_u, line 1",
                copyFields("COPY crf_u FROM STDIN", "7\t9\n"));
        exec("DROP TABLE crf_u");
    }

    /** A row no partition will take: the partition key is the DETAIL and the line the CONTEXT. */
    @Test
    void aRowNoPartitionWillTakeCarriesThePartitionKeyAndTheLine() throws Exception {
        exec("CREATE TABLE crf_r (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE crf_r0 PARTITION OF crf_r FOR VALUES FROM (0) TO (10)");
        String none = "23514/no partition of relation \"crf_r\" found for row"
                + "/Partition key of the failing row contains (k) = (50).";

        assertEquals(none + "/COPY crf_r, line 1: \"1\t50\"",
                copyFields("COPY crf_r FROM STDIN", "1\t50\n"));
        assertEquals(none + "/COPY crf_r, line 2: \"2\t50\"",
                copyFields("COPY crf_r FROM STDIN", "1\t5\n2\t50\n"));
        assertEquals(none + "/COPY crf_r, line 2: \"1,50\"",
                copyFields("COPY crf_r FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,50\n"));
        assertEquals(0, num("SELECT count(*) FROM crf_r"));
        exec("DROP TABLE crf_r");
    }

    /**
     * A BEFORE row trigger that rewrites the key out of the partition the row was routed to. The
     * error carries the second kind of DETAIL line a partitioned relation raises -- the trigger
     * that rewrote the row and the partition the row was going to -- and the CONTEXT names the
     * relation the statement was written against, which is the parent when the copy went through
     * it.
     */
    @Test
    void aTriggerThatMovesTheRowNamesThePartitionItWasBoundForAndTheLineItCameFrom()
            throws Exception {
        copyRefusalFunctions();
        exec("CREATE TABLE crf_m (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE crf_m0 PARTITION OF crf_m FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE crf_m1 PARTITION OF crf_m FOR VALUES FROM (10) TO (20)");
        exec("CREATE TRIGGER crf_move BEFORE INSERT ON crf_m"
                + " FOR EACH ROW EXECUTE FUNCTION crf_bump()");
        String moved = "0A000/moving row to another partition during a BEFORE FOR EACH ROW"
                + " trigger is not supported";
        String toM0 = "/Before executing trigger \"crf_move\", the row was to be in partition"
                + " \"public.crf_m0\".";

        assertEquals(moved + toM0 + "/COPY crf_m, line 1: \"3\t5\"",
                copyFields("COPY crf_m FROM STDIN", "3\t5\n"));
        assertEquals(moved + toM0 + "/COPY crf_m0, line 1: \"4\t6\"",
                copyFields("COPY crf_m0 FROM STDIN", "4\t6\n"));
        // The partition named is the one the row was bound for, not the one it was rewritten into.
        assertEquals(moved
                        + "/Before executing trigger \"crf_move\", the row was to be in partition"
                        + " \"public.crf_m1\"./COPY crf_m, line 1: \"5\t11\"",
                copyFields("COPY crf_m FROM STDIN", "5\t11\n6\t5\n"));
        // A header is a line here too.
        assertEquals(moved + toM0 + "/COPY crf_m, line 2: \"7,5\"",
                copyFields("COPY crf_m FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n7,5\n"));
        assertEquals(0, num("SELECT count(*) FROM crf_m"));
        exec("DROP TABLE crf_m CASCADE");
    }

    /**
     * A leaf's own constraint reached through the parent: the message names the leaf that refused
     * the row and the CONTEXT the relation the copy was written against.
     */
    @Test
    void aLeafsOwnConstraintIsNamedByTheLeafAndTheLineByTheCopy() throws Exception {
        copyRefusalFunctions();
        exec("CREATE TABLE crf_g (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE crf_g0 PARTITION OF crf_g FOR VALUES FROM (0) TO (10)");
        exec("ALTER TABLE crf_g0 ADD CONSTRAINT crf_g0_ck CHECK (i < 100)");
        String broken = "23514/new row for relation \"crf_g0\" violates check constraint"
                + " \"crf_g0_ck\"/Failing row contains (500, 5).";

        assertEquals(broken + "/COPY crf_g, line 1: \"500\t5\"",
                copyFields("COPY crf_g FROM STDIN", "500\t5\n"));
        assertEquals(broken + "/COPY crf_g0, line 1: \"500\t5\"",
                copyFields("COPY crf_g0 FROM STDIN", "500\t5\n"));
        exec("DROP TABLE crf_g CASCADE");

        // A trigger of the partition's own runs once the row is already bound to it, so the row it
        // leaves behind breaks that partition's bound rather than moving anywhere. The DETAIL is
        // the row the trigger made and the CONTEXT the line as it was sent.
        exec("CREATE TABLE crf_t (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE crf_t0 PARTITION OF crf_t FOR VALUES FROM (0) TO (10)");
        exec("CREATE TRIGGER crf_move BEFORE INSERT ON crf_t0"
                + " FOR EACH ROW EXECUTE FUNCTION crf_bump()");
        assertEquals("23514/new row for relation \"crf_t0\" violates partition constraint"
                        + "/Failing row contains (1, 15)./COPY crf_t, line 1: \"1\t5\"",
                copyFields("COPY crf_t FROM STDIN", "1\t5\n"));
        exec("DROP TABLE crf_t CASCADE");
    }

    /**
     * A trigger that raises stands its own frame in front of the copy's, the way PostgreSQL
     * stacks the frames of everything running on a statement's behalf.
     */
    @Test
    void aTriggerThatRaisesStandsItsOwnFrameInFrontOfTheCopys() throws Exception {
        copyRefusalFunctions();
        exec("CREATE TABLE crf_rb (i int)");
        exec("CREATE TRIGGER crf_b BEFORE INSERT ON crf_rb"
                + " FOR EACH ROW EXECUTE FUNCTION crf_raise()");
        assertEquals("P0001/no/null/PL/pgSQL function crf_raise() line 1 at RAISE\n"
                        + "COPY crf_rb, line 1: \"1\"",
                copyFields("COPY crf_rb FROM STDIN", "1\n2\n"));
        exec("DROP TABLE crf_rb CASCADE");
    }

    /** Binary data has no text a sender could be shown, so the line is counted and not quoted. */
    @Test
    void aBinaryCopyCountsTheLineAndQuotesNothingOfIt() throws Exception {
        exec("CREATE TABLE crf_bn (i int, k int, CONSTRAINT crf_bn_ck CHECK (k < 10))");
        String broken = "23514/new row for relation \"crf_bn\" violates check constraint"
                + " \"crf_bn_ck\"";

        assertEquals(broken + "/Failing row contains (1, 15)./COPY crf_bn, line 1",
                copyFields("COPY crf_bn FROM STDIN WITH (FORMAT binary)",
                        binaryRows(new int[][] {{1, 15}})));
        assertEquals(broken + "/Failing row contains (2, 15)./COPY crf_bn, line 2",
                copyFields("COPY crf_bn FROM STDIN WITH (FORMAT binary)",
                        binaryRows(new int[][] {{1, 5}, {2, 15}})));
        // A stream that is not a copy at all was refused before any row was read, so there is no
        // line to name and nothing to carry in a DETAIL.
        assertEquals("22P04/COPY file signature not recognized/null/null",
                copyFields("COPY crf_bn FROM STDIN WITH (FORMAT binary)",
                        "garbage".getBytes(StandardCharsets.UTF_8)));
        exec("DROP TABLE crf_bn");
    }

    /**
     * The rows of a COPY FROM STDIN never reach the server inside the statement. The lines that
     * follow it in a script, and the {@code \.} that ends them, are read by the client and turned
     * into the copy's own messages; a server handed one of them as SQL answers for the first word
     * of it. A copy the sender never writes to is refused where the data would have gone, and the
     * refusal names the line the server was waiting for -- PostgreSQL counts a line as it starts
     * on it, so a copy nothing was sent to is waiting on line 1.
     */
    @Test
    void theRowsOfACopyFromStdinNeverReachTheServerInsideTheStatement() throws Exception {
        exec("CREATE TABLE crf_in (i int, k int)");

        ServerErrorMessage text = fieldsOf("COPY crf_in FROM stdin;\n1\t2\n3\t4\n\\.\n");
        assertEquals("42601", text.getSQLState());
        assertEquals("syntax error at or near \"1\"", text.getMessage());
        ServerErrorMessage csv =
                fieldsOf("COPY crf_in (i,k) FROM STDIN WITH (FORMAT csv);\n5,6\n\\.\n");
        assertEquals("42601", csv.getSQLState());
        assertEquals("syntax error at or near \"5\"", csv.getMessage());
        assertEquals(0, num("SELECT count(*) FROM crf_in"));

        ServerErrorMessage nothingSent = fieldsOf("COPY crf_in FROM STDIN");
        assertEquals("57014", nothingSent.getSQLState());
        assertEquals("COPY from stdin failed:"
                + " COPY commands are only supported using the CopyManager API.",
                nothingSent.getMessage());
        assertEquals("COPY crf_in, line 1", nothingSent.getWhere());

        // The relation and the column list are settled before the copy is opened at all.
        assertEquals("42P01", stateOf("COPY crf_nosuch FROM STDIN"));
        assertEquals("42703", stateOf("COPY crf_in (i,nosuch) FROM STDIN"));
        assertEquals(0, num("SELECT count(*) FROM crf_in"));
        exec("DROP TABLE crf_in");
    }

    // ============================================================ what a MERGE arm sees while it
    // runs

    /** The trigger functions the MERGE arm tests below are written with. */
    private static void mergeArmFunctions() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION mar_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_WHEN || ' ' || TG_OP || ' ' || OLD.i"
                + "  || ' left=' || (SELECT count(*) FROM mar_d));"
                + " IF TG_WHEN = 'BEFORE' THEN RETURN OLD; END IF; RETURN NULL; END $$");
        exec("CREATE OR REPLACE FUNCTION mar_sum() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN"
                + " INSERT INTO ptg_log(m) VALUES (TG_WHEN || ' ' || TG_OP || ' ' || OLD.i"
                + "  || ' sum=' || (SELECT coalesce(sum(v),0) FROM mar_d));"
                + " IF TG_WHEN = 'BEFORE' THEN RETURN NEW; END IF; RETURN NULL; END $$");
    }

    /** The three rows the arm tests below are run over, with nothing logged yet. */
    private static void mergeArmTarget() throws SQLException {
        exec("TRUNCATE mar_d");
        exec("INSERT INTO mar_d VALUES (1,10),(2,20),(3,30)");
        exec("DELETE FROM ptg_log");
    }

    /**
     * A row goes out of the relation where the arm acts on it rather than when the statement ends,
     * so the BEFORE half of the row after it is told the one before has gone: over a two-row arm
     * the first row's trigger reads three rows and the second's reads two. Both AFTER halves run
     * once the statement has finished writing, so both read the one row it left behind.
     */
    @Test
    void aMergeDeleteArmTakesEachRowOutBeforeTheNextRowsBeforeHalfRuns() throws SQLException {
        exec("CREATE TABLE mar_d (i int PRIMARY KEY, v int)");
        exec("CREATE TABLE mar_ds (i int, v int)");
        exec("INSERT INTO mar_ds VALUES (1,100),(2,200)");
        mergeArmFunctions();
        exec("CREATE TRIGGER mar_d_b BEFORE DELETE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_note()");
        exec("CREATE TRIGGER mar_d_a AFTER DELETE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_note()");

        mergeArmTarget();
        assertEquals(2, update("MERGE INTO mar_d t USING mar_ds u ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE"));
        assertEquals("BEFORE DELETE 1 left=3,BEFORE DELETE 2 left=2,"
                + "AFTER DELETE 1 left=1,AFTER DELETE 2 left=1", firings());
        assertEquals("3|30", rows("SELECT i, v FROM mar_d ORDER BY i"));

        exec("DROP TABLE mar_d CASCADE");
        exec("DROP TABLE mar_ds");
    }

    /** The plain DELETE path counts down the same way, however many rows it takes. */
    @Test
    void aPlainDeleteTakesEachRowOutBeforeTheNextRowsBeforeHalfRuns() throws SQLException {
        exec("CREATE TABLE mar_d (i int PRIMARY KEY, v int)");
        mergeArmFunctions();
        exec("CREATE TRIGGER mar_d_b BEFORE DELETE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_note()");
        exec("CREATE TRIGGER mar_d_a AFTER DELETE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_note()");

        mergeArmTarget();
        assertEquals(3, update("DELETE FROM mar_d WHERE i <= 3"));
        assertEquals("BEFORE DELETE 1 left=3,BEFORE DELETE 2 left=2,BEFORE DELETE 3 left=1,"
                + "AFTER DELETE 1 left=0,AFTER DELETE 2 left=0,AFTER DELETE 3 left=0", firings());

        // The rows the WHERE leaves out are there for every half to count, before and after.
        exec("TRUNCATE mar_d");
        exec("INSERT INTO mar_d VALUES (1,10),(2,20),(3,30),(4,40),(5,50),(6,60)");
        exec("DELETE FROM ptg_log");
        assertEquals(4, update("DELETE FROM mar_d WHERE i <= 4"));
        assertEquals("BEFORE DELETE 1 left=6,BEFORE DELETE 2 left=5,BEFORE DELETE 3 left=4,"
                + "BEFORE DELETE 4 left=3,AFTER DELETE 1 left=2,AFTER DELETE 2 left=2,"
                + "AFTER DELETE 3 left=2,AFTER DELETE 4 left=2", firings());

        exec("DROP TABLE mar_d CASCADE");
    }

    /**
     * An UPDATE arm's BEFORE half reads the rows the statement has already written, and its AFTER
     * halves the relation the whole statement left behind -- which the plain UPDATE path answers
     * the same way.
     */
    @Test
    void anUpdateArmsBeforeHalfReadsWhatTheStatementHasAlreadyWritten() throws SQLException {
        exec("CREATE TABLE mar_d (i int PRIMARY KEY, v int)");
        exec("CREATE TABLE mar_ds (i int, v int)");
        exec("INSERT INTO mar_ds VALUES (1,100),(2,200)");
        mergeArmFunctions();
        exec("CREATE TRIGGER mar_d_b BEFORE UPDATE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_sum()");
        exec("CREATE TRIGGER mar_d_a AFTER UPDATE ON mar_d"
                + " FOR EACH ROW EXECUTE FUNCTION mar_sum()");

        mergeArmTarget();
        assertEquals(2, update("MERGE INTO mar_d t USING mar_ds u ON t.i = u.i"
                + " WHEN MATCHED THEN UPDATE SET v = t.v + 1000"));
        assertEquals("BEFORE UPDATE 1 sum=60,BEFORE UPDATE 2 sum=1060,"
                + "AFTER UPDATE 1 sum=2060,AFTER UPDATE 2 sum=2060", firings());
        assertEquals("1|1010,2|1020,3|30", rows("SELECT i, v FROM mar_d ORDER BY i"));

        mergeArmTarget();
        assertEquals(2, update("UPDATE mar_d SET v = v + 1000 WHERE i <= 2"));
        assertEquals("BEFORE UPDATE 1 sum=60,BEFORE UPDATE 2 sum=1060,"
                + "AFTER UPDATE 1 sum=2060,AFTER UPDATE 2 sum=2060", firings());
        assertEquals("1|1010,2|1020,3|30", rows("SELECT i, v FROM mar_d ORDER BY i"));

        exec("DROP TABLE mar_d CASCADE");
        exec("DROP TABLE mar_ds");
    }

    /**
     * A RETURNING clause of a write that brings in a second relation stands in the scope of both,
     * so a bare column name they both hold answers to neither. PostgreSQL reads the clause while
     * it analyses the statement, so the refusal is owed whether or not a row would have been
     * written and whichever arm would have written it.
     */
    @Test
    void aBareReturningNameBothRelationsHoldIsRefused() throws SQLException {
        exec("CREATE TABLE mar_k (i int PRIMARY KEY, v text)");
        exec("CREATE TABLE mar_ks (i int, v text)");
        exec("CREATE TABLE mar_ke (i int, v text)");
        exec("CREATE TABLE mar_kd (j int, w text)");
        exec("INSERT INTO mar_k VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("INSERT INTO mar_ks VALUES (1,'x'),(2,'y'),(4,'z')");
        exec("INSERT INTO mar_kd VALUES (1,'x'),(2,'y')");

        for (String statement : new String[] {
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN DELETE RETURNING i, v",
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN UPDATE SET v = u.v RETURNING i, v",
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.v) RETURNING i, v",
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 'q' RETURNING i, v",
                // an arm that writes nothing, and an ON condition that pairs nothing
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN DO NOTHING RETURNING i",
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i AND t.i > 100"
                        + " WHEN MATCHED THEN DELETE RETURNING i",
                // a source holding no row at all
                "MERGE INTO mar_k t USING mar_ke u ON t.i = u.i"
                        + " WHEN MATCHED THEN DELETE RETURNING i",
                // a source that is a query of its own, a VALUES list, the target read again
                "MERGE INTO mar_k t USING (SELECT i, v FROM mar_ks) u ON t.i = u.i"
                        + " WHEN MATCHED THEN DO NOTHING RETURNING i",
                "MERGE INTO mar_k t USING (VALUES (1,'x')) AS u(i,v) ON t.i = u.i"
                        + " WHEN MATCHED THEN DO NOTHING RETURNING i",
                "MERGE INTO mar_k t USING mar_k u ON t.i = u.i"
                        + " WHEN MATCHED THEN DO NOTHING RETURNING i",
                // a name under an operator, and one beside a clause the statement did settle
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN DELETE RETURNING i + 1",
                "MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN DELETE RETURNING merge_action(), i",
                // and the ordinary UPDATE and DELETE paths, which are read the same way
                "UPDATE mar_k t SET v = 'p' FROM mar_ks u WHERE t.i = u.i RETURNING i, v",
                "UPDATE mar_k t SET v = 'p' FROM mar_ks u"
                        + " WHERE t.i = u.i AND t.i > 100 RETURNING i",
                "UPDATE mar_k t SET v = 'p' FROM mar_ke u WHERE t.i = u.i RETURNING i",
                "DELETE FROM mar_k t USING mar_ks u WHERE t.i = u.i RETURNING i, v",
                "DELETE FROM mar_k t USING mar_ks u WHERE t.i = u.i AND t.i > 100 RETURNING i",
                "DELETE FROM mar_k t USING mar_ke u WHERE t.i = u.i RETURNING i"}) {
            ServerErrorMessage m = fieldsOf(statement);
            assertEquals("42702", m.getSQLState(), statement);
            assertEquals("column reference \"i\" is ambiguous", m.getMessage(), statement);
        }
        assertEquals("column reference \"v\" is ambiguous",
                messageOf("MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                        + " WHEN MATCHED THEN DELETE RETURNING v"));

        // Nothing any of the refusals wrote or took away.
        assertEquals("1|a,2|b,3|c", rows("SELECT i, v FROM mar_k ORDER BY i"));

        // What the rule does not reach: a second relation holding neither name, a name written out
        // against one of the two, and a star, which stands for the columns of both in turn.
        assertEquals("", rows("MERGE INTO mar_k t USING mar_kd d ON t.i = d.j"
                + " WHEN MATCHED THEN DO NOTHING RETURNING i"));
        assertEquals("", rows("DELETE FROM mar_k t USING mar_kd d"
                + " WHERE t.i = d.j AND t.i > 100 RETURNING i"));
        assertEquals("1|x|1|a,2|y|2|b", rows("MERGE INTO mar_k t USING mar_ks u ON t.i = u.i"
                + " WHEN MATCHED THEN DELETE RETURNING *"));
        assertEquals("3|c", rows("DELETE FROM mar_k WHERE i = 3 RETURNING i, v"));
        assertEquals(0, num("SELECT count(*) FROM mar_k"));

        exec("DROP TABLE mar_k");
        exec("DROP TABLE mar_ks");
        exec("DROP TABLE mar_ke");
        exec("DROP TABLE mar_kd");
    }

    // ============================================================ what a refusal writes the row as

    /**
     * Every value PostgreSQL prints inside a DETAIL is written by the column's own output function
     * -- the string a query would have returned for it. An array reads in braces, a boolean as one
     * letter, a bytea in hex, a timestamp with a space between its date and its time, a numeric to
     * the scale it was declared with. A row printed any other way names a row nothing could be
     * looked up by.
     */
    @Test
    void aFailingRowInADetailIsWrittenTheWayAQueryWouldHaveReturnedIt() throws SQLException {
        exec("CREATE TABLE frd_ck (i int, a int[], CONSTRAINT frd_ck_ck CHECK (i < 10))");
        assertEquals("new row for relation \"frd_ck\" violates check constraint \"frd_ck_ck\"",
                messageOf("INSERT INTO frd_ck VALUES (50, '{1,2}')"));
        assertEquals("Failing row contains (50, {1,2}).",
                detailOf("INSERT INTO frd_ck VALUES (50, '{1,2}')"));
        assertEquals("Failing row contains (50, {}).",
                detailOf("INSERT INTO frd_ck VALUES (50, '{}')"));
        assertEquals("Failing row contains (50, null).",
                detailOf("INSERT INTO frd_ck VALUES (50, NULL)"));
        assertEquals("Failing row contains (50, {{1,2},{3,4}}).",
                detailOf("INSERT INTO frd_ck VALUES (50, '{{1,2},{3,4}}')"));
        // A column the statement never named is nothing, and reads as the word null.
        assertEquals("Failing row contains (50, null).",
                detailOf("INSERT INTO frd_ck (i) VALUES (50)"));
        assertEquals(0, num("SELECT count(*) FROM frd_ck"));
        exec("DROP TABLE frd_ck");

        // Every other type whose Java rendering is not PostgreSQL's, in one row.
        exec("CREATE TABLE frd_wt (i int, b bool, y bytea, t time, d date, s timestamp,"
                + " f float8, r real, n numeric, a text[], CONSTRAINT frd_wt_ck CHECK (i < 10))");
        assertEquals("Failing row contains (50, t, \\x0102, 03:04:00, 2020-01-02,"
                        + " 2020-01-02 03:04:05, 1, 2, 1.50, {a,\"b c\"}).",
                detailOf("INSERT INTO frd_wt VALUES (50, true, '\\x0102', '03:04:00',"
                        + " '2020-01-02', '2020-01-02 03:04:05', 1.0, 2.0, 1.50, '{a,\"b c\"}')"));
        exec("DROP TABLE frd_wt");

        // A NOT NULL prints the row the same way, and the column that has nothing in it as null.
        exec("CREATE TABLE frd_nn (i int, a int[], k int NOT NULL)");
        assertEquals("null value in column \"k\" of relation \"frd_nn\""
                        + " violates not-null constraint",
                messageOf("INSERT INTO frd_nn VALUES (1, '{3,4}', NULL)"));
        assertEquals("Failing row contains (1, {3,4}, null).",
                detailOf("INSERT INTO frd_nn VALUES (1, '{3,4}', NULL)"));
        exec("DROP TABLE frd_nn");

        // So does a partition's own bound, met by writing straight at the partition.
        exec("CREATE TABLE frd_pp (i int, a int[]) PARTITION BY RANGE (i)");
        exec("CREATE TABLE frd_pp0 PARTITION OF frd_pp FOR VALUES FROM (0) TO (10)");
        assertEquals("new row for relation \"frd_pp0\" violates partition constraint",
                messageOf("INSERT INTO frd_pp0 VALUES (50, '{1,2}')"));
        assertEquals("Failing row contains (50, {1,2}).",
                detailOf("INSERT INTO frd_pp0 VALUES (50, '{1,2}')"));
        exec("DROP TABLE frd_pp CASCADE");

        // And so does a view's check option, over the row it would have written.
        exec("CREATE TABLE frd_vt (i int, a int[])");
        exec("CREATE VIEW frd_vv AS SELECT * FROM frd_vt WHERE i < 10 WITH CHECK OPTION");
        assertEquals("44000", stateOf("INSERT INTO frd_vv VALUES (50, '{1,2}')"));
        assertEquals("new row violates check option for view \"frd_vv\"",
                messageOf("INSERT INTO frd_vv VALUES (50, '{1,2}')"));
        assertEquals("Failing row contains (50, {1,2}).",
                detailOf("INSERT INTO frd_vv VALUES (50, '{1,2}')"));
        assertEquals(0, num("SELECT count(*) FROM frd_vt"));
        exec("DROP VIEW frd_vv");
        exec("DROP TABLE frd_vt");
    }

    /**
     * A key inside a DETAIL is written by the same hand as a row: an index's key, a foreign key's,
     * an exclusion constraint's, and the partition key of a row no partition will take. A key over
     * several columns names all of them in one list and writes each value with its own column's
     * output function.
     */
    @Test
    void aKeyInADetailIsWrittenTheSameWayARowIs() throws SQLException {
        exec("CREATE TABLE frd_ux (a int[] PRIMARY KEY)");
        exec("INSERT INTO frd_ux VALUES ('{1,2}')");
        assertEquals("duplicate key value violates unique constraint \"frd_ux_pkey\"",
                messageOf("INSERT INTO frd_ux VALUES ('{1,2}')"));
        assertEquals("Key (a)=({1,2}) already exists.",
                detailOf("INSERT INTO frd_ux VALUES ('{1,2}')"));
        exec("DROP TABLE frd_ux");

        exec("CREATE TABLE frd_kk (b bool, y bytea, s timestamp, n numeric, f float8, a text[],"
                + " PRIMARY KEY (b,y,s,n,f,a))");
        String row = "(true, '\\x0102', '2020-01-02 03:04:05', 1.50, 1.0, '{a,\"b c\"}')";
        exec("INSERT INTO frd_kk VALUES " + row);
        assertEquals("Key (b, y, s, n, f, a)=(t, \\x0102, 2020-01-02 03:04:05, 1.50, 1,"
                        + " {a,\"b c\"}) already exists.",
                detailOf("INSERT INTO frd_kk VALUES " + row));
        exec("DROP TABLE frd_kk");

        exec("CREATE TABLE frd_fp (b bool, a int[], PRIMARY KEY (b,a))");
        exec("CREATE TABLE frd_fc (b bool, a int[], FOREIGN KEY (b,a) REFERENCES frd_fp)");
        assertEquals("insert or update on table \"frd_fc\" violates foreign key constraint"
                        + " \"frd_fc_b_a_fkey\"",
                messageOf("INSERT INTO frd_fc VALUES (false, '{7,8}')"));
        assertEquals("Key (b, a)=(f, {7,8}) is not present in table \"frd_fp\".",
                detailOf("INSERT INTO frd_fc VALUES (false, '{7,8}')"));
        exec("DROP TABLE frd_fc");
        exec("DROP TABLE frd_fp");

        exec("CREATE TABLE frd_ex (a int[], EXCLUDE (a WITH =))");
        exec("INSERT INTO frd_ex VALUES ('{1,2}')");
        assertEquals("23P01", stateOf("INSERT INTO frd_ex VALUES ('{1,2}')"));
        assertEquals("conflicting key value violates exclusion constraint \"frd_ex_a_excl\"",
                messageOf("INSERT INTO frd_ex VALUES ('{1,2}')"));
        assertEquals("Key (a)=({1,2}) conflicts with existing key (a)=({1,2}).",
                detailOf("INSERT INTO frd_ex VALUES ('{1,2}')"));
        exec("DROP TABLE frd_ex");

        exec("CREATE TABLE frd_pa (a int[], k int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE frd_pa0 PARTITION OF frd_pa FOR VALUES FROM ('{0}') TO ('{9}')");
        assertEquals("no partition of relation \"frd_pa\" found for row",
                messageOf("INSERT INTO frd_pa VALUES ('{50,2}', 1)"));
        assertEquals("Partition key of the failing row contains (a) = ({50,2}).",
                detailOf("INSERT INTO frd_pa VALUES ('{50,2}', 1)"));
        exec("DROP TABLE frd_pa CASCADE");
    }

    /**
     * A COPY writes the same DETAIL the statement that stores the row does. The DETAIL is the row
     * as the relation holds it and the CONTEXT the line as the sender wrote it, so the two are the
     * same values in two spellings: the copy's own for the input, the columns' own for the row.
     */
    @Test
    void aCopyWritesTheFailingRowTheWayTheColumnsDoAndTheLineTheWayItWasSent() throws Exception {
        exec("CREATE TABLE frd_cc (i int, a int[], CONSTRAINT frd_cc_ck CHECK (i < 10))");
        String broken = "23514/new row for relation \"frd_cc\" violates check constraint"
                + " \"frd_cc_ck\"";
        assertEquals(broken + "/Failing row contains (50, {1,2})./COPY frd_cc, line 1:"
                        + " \"50\t{1,2}\"",
                copyFields("COPY frd_cc FROM STDIN", "50\t{1,2}\n"));
        assertEquals(broken + "/Failing row contains (50, {1,2})./COPY frd_cc, line 1:"
                        + " \"50,\"{1,2}\"\"",
                copyFields("COPY frd_cc FROM STDIN WITH (FORMAT csv)", "50,\"{1,2}\"\n"));
        exec("DROP TABLE frd_cc");

        exec("CREATE TABLE frd_cw (i int, b bool, y bytea, t time, d date, s timestamp,"
                + " f float8, r real, n numeric, a text[], CONSTRAINT frd_cw_ck CHECK (i < 10))");
        assertEquals("23514/new row for relation \"frd_cw\" violates check constraint"
                        + " \"frd_cw_ck\""
                        + "/Failing row contains (50, t, \\x0102, 03:04:00, 2020-01-02,"
                        + " 2020-01-02 03:04:05, 1, 2, 1.50, {a,\"b c\"})."
                        + "/COPY frd_cw, line 1: \"50\tt\t\\\\x0102\t03:04:00\t2020-01-02"
                        + "\t2020-01-02 03:04:05\t1.0\t2.0\t1.50\t{a,\"b c\"}\"",
                copyFields("COPY frd_cw FROM STDIN", "50\tt\t\\\\x0102\t03:04:00\t2020-01-02"
                        + "\t2020-01-02 03:04:05\t1.0\t2.0\t1.50\t{a,\"b c\"}\n"));
        exec("DROP TABLE frd_cw");

        exec("CREATE TABLE frd_cn (i int, a int[], k int NOT NULL)");
        assertEquals("23502/null value in column \"k\" of relation \"frd_cn\""
                        + " violates not-null constraint"
                        + "/Failing row contains (1, {3,4}, null)."
                        + "/COPY frd_cn, line 1: \"1\t{3,4}\t\\N\"",
                copyFields("COPY frd_cn FROM STDIN", "1\t{3,4}\t\\N\n"));
        exec("DROP TABLE frd_cn");

        // A key the index maintains over a batch names the line and no longer the text on it.
        exec("CREATE TABLE frd_cu (a int[] PRIMARY KEY)");
        assertEquals("23505/duplicate key value violates unique constraint \"frd_cu_pkey\""
                        + "/Key (a)=({1,2}) already exists./COPY frd_cu, line 2",
                copyFields("COPY frd_cu FROM STDIN", "{1,2}\n{1,2}\n"));
        exec("DROP TABLE frd_cu");

        exec("CREATE TABLE frd_ce (a int[], EXCLUDE (a WITH =))");
        assertEquals("23P01/conflicting key value violates exclusion constraint"
                        + " \"frd_ce_a_excl\""
                        + "/Key (a)=({3,4}) conflicts with existing key (a)=({3,4})."
                        + "/COPY frd_ce, line 2",
                copyFields("COPY frd_ce FROM STDIN", "{3,4}\n{3,4}\n"));
        exec("DROP TABLE frd_ce");

        // A foreign key is checked once the copy has finished reading, so it names no line at all.
        exec("CREATE TABLE frd_cp (a int[] PRIMARY KEY)");
        exec("CREATE TABLE frd_cf (a int[] REFERENCES frd_cp)");
        assertEquals("23503/insert or update on table \"frd_cf\" violates foreign key constraint"
                        + " \"frd_cf_a_fkey\""
                        + "/Key (a)=({9,8}) is not present in table \"frd_cp\"./null",
                copyFields("COPY frd_cf FROM STDIN", "{9,8}\n"));
        exec("DROP TABLE frd_cf");
        exec("DROP TABLE frd_cp");

        exec("CREATE TABLE frd_cr (a int[], k int) PARTITION BY RANGE (a)");
        exec("CREATE TABLE frd_cr0 PARTITION OF frd_cr FOR VALUES FROM ('{0}') TO ('{9}')");
        assertEquals("23514/no partition of relation \"frd_cr\" found for row"
                        + "/Partition key of the failing row contains (a) = ({50,2})."
                        + "/COPY frd_cr, line 1: \"{50,2}\t1\"",
                copyFields("COPY frd_cr FROM STDIN", "{50,2}\t1\n"));
        assertEquals(0, num("SELECT count(*) FROM frd_cr"));
        exec("DROP TABLE frd_cr CASCADE");
    }

    /**
     * A row no partition will take names the relation in the error's own schema and table fields,
     * the way a constraint of the relation's own does -- a client that branches on those fields is
     * told nothing by the message. The relation named is the one the write was addressed to.
     */
    @Test
    void aRoutingFailureNamesItsRelationInTheErrorsSchemaAndTableFields() throws Exception {
        exec("CREATE TABLE frd_sr (i int, k int) PARTITION BY RANGE (k)");
        exec("CREATE TABLE frd_sr0 PARTITION OF frd_sr FOR VALUES FROM (0) TO (10)");

        ServerErrorMessage written = fieldsOf("INSERT INTO frd_sr VALUES (1, 50)");
        assertEquals("23514", written.getSQLState());
        assertEquals("no partition of relation \"frd_sr\" found for row", written.getMessage());
        assertEquals("Partition key of the failing row contains (k) = (50).", written.getDetail());
        assertEquals("public", written.getSchema());
        assertEquals("frd_sr", written.getTable());
        assertNull(written.getConstraint());
        assertNull(written.getColumn());

        ServerErrorMessage copied = copyErrorOf("COPY frd_sr FROM STDIN", "1\t50\n");
        assertEquals("23514", copied.getSQLState());
        assertEquals("no partition of relation \"frd_sr\" found for row", copied.getMessage());
        assertEquals("Partition key of the failing row contains (k) = (50).", copied.getDetail());
        assertEquals("public", copied.getSchema());
        assertEquals("frd_sr", copied.getTable());
        assertNull(copied.getConstraint());
        assertEquals("COPY frd_sr, line 1: \"1\t50\"", copied.getWhere());

        // A copy written against the partition is refused by the partition's own bound, which
        // names the partition in the same two fields.
        ServerErrorMessage leaf = copyErrorOf("COPY frd_sr0 FROM STDIN", "1\t50\n");
        assertEquals("new row for relation \"frd_sr0\" violates partition constraint",
                leaf.getMessage());
        assertEquals("public", leaf.getSchema());
        assertEquals("frd_sr0", leaf.getTable());

        // The fields a constraint of the relation's own fills, for comparison.
        exec("CREATE TABLE frd_sc (i int, k int NOT NULL, CONSTRAINT frd_sc_ck CHECK (i < 10))");
        ServerErrorMessage check = copyErrorOf("COPY frd_sc FROM STDIN", "50\t1\n");
        assertEquals("public", check.getSchema());
        assertEquals("frd_sc", check.getTable());
        assertEquals("frd_sc_ck", check.getConstraint());
        assertNull(check.getColumn());

        ServerErrorMessage notNull = copyErrorOf("COPY frd_sc FROM STDIN", "1\t\\N\n");
        assertEquals("public", notNull.getSchema());
        assertEquals("frd_sc", notNull.getTable());
        assertNull(notNull.getConstraint());
        assertEquals("k", notNull.getColumn());

        exec("DROP TABLE frd_sc");
        exec("DROP TABLE frd_sr CASCADE");
    }

    /**
     * A CSV field whose closing quote the sender never wrote.
     *
     * <p>PostgreSQL counts a newline carried through a quoted field as a line of its own, but only
     * once it has learned what a line terminator looks like in this input -- which it does from the
     * first one it reads outside quotes. An input whose only newline sits inside the unterminated
     * field is therefore still line 1, while the same field on the second line of an input is line
     * 3. What is quoted back is everything the reader took, embedded newlines and all.
     */
    @Test
    void anUnterminatedQuotedCsvFieldNamesTheLineItRanOutOn() throws Exception {
        exec("CREATE TABLE frd_q (i int, k text)");
        String broken = "22P04/unterminated CSV quoted field/null/COPY frd_q, line ";

        assertEquals(broken + "1: \"1,\"abc\"", unterminated("1,\"abc"));
        assertEquals(broken + "1: \"1,\"abc\n\"", unterminated("1,\"abc\n"));
        assertEquals(broken + "1: \"1,\"abc\n\n\"", unterminated("1,\"abc\n\n"));
        assertEquals(broken + "1: \"1,\"\"", unterminated("1,\""));
        assertEquals(broken + "1: \"1,\"a\nb\nc\"", unterminated("1,\"a\nb\nc"));
        // A quote opened inside an unquoted field is an unterminated field too.
        assertEquals(broken + "1: \"1,ab\"cd\n\"", unterminated("1,ab\"cd\n"));
        // A line that was read whole is a line, so the next one is line 2 -- and the newline the
        // field then carries away is a line of its own on top of it.
        assertEquals(broken + "2: \"2,\"abc\"", unterminated("1,ok\n2,\"abc"));
        assertEquals(broken + "3: \"2,\"abc\n\"", unterminated("1,ok\n2,\"abc\n"));
        assertEquals(broken + "4: \"3,\"abc\n\"", unterminated("1,a\n2,b\n3,\"abc\n"));
        // A header is a line like any other, so the first row of a HEADER copy is line 2.
        assertEquals(broken + "3: \"1,\"abc\n\"",
                copyFields("COPY frd_q FROM STDIN WITH (FORMAT csv, HEADER)", "i,k\n1,\"abc\n"));

        // Nothing any of the refusals read is left behind, and a field the sender did close is
        // taken as it stands.
        assertEquals(0, num("SELECT count(*) FROM frd_q"));
        assertEquals("rows=1", copyFields("COPY frd_q FROM STDIN WITH (FORMAT csv)", "1,\"abc\"\n"));
        assertEquals("1|abc", rows("SELECT i, k FROM frd_q"));
        exec("DROP TABLE frd_q");
    }

    private static String unterminated(String data) {
        return copyFields("COPY frd_q FROM STDIN WITH (FORMAT csv)", data);
    }

    /**
     * STDIN and STDOUT are one thing to PostgreSQL's grammar -- the absence of a file name -- and
     * the direction is read from the FROM or the TO alone. So {@code FROM STDOUT} opens a copy in
     * and reads the client's rows, and {@code TO STDIN} opens a copy out and writes to it.
     */
    @Test
    void theDirectionOfACopyIsReadFromTheFromOrTheToAlone() throws Exception {
        exec("CREATE TABLE frd_d (i int, k int)");

        // A copy in, however the statement spelled the stream.
        assertEquals("rows=1", copyFields("COPY frd_d FROM STDOUT", "1\t2\n"));
        assertEquals("rows=1",
                copyFields("COPY frd_d FROM STDOUT WITH (FORMAT csv)", "3,4\n"));
        assertEquals("rows=1", copyFields("COPY frd_d (i,k) FROM STDOUT", "5\t6\n"));
        assertEquals("1|2,3|4,5|6", rows("SELECT i, k FROM frd_d ORDER BY i"));

        // A copy out, however the statement spelled the stream.
        assertEquals("3 rows: 1\t2\n3\t4\n5\t6\n", copyOutOf("COPY frd_d TO STDOUT"));
        assertEquals("3 rows: 1\t2\n3\t4\n5\t6\n", copyOutOf("COPY frd_d TO STDIN"));
        assertEquals("3 rows: 1,2\n3,4\n5,6\n",
                copyOutOf("COPY frd_d TO STDIN WITH (FORMAT csv)"));
        assertEquals("3 rows: 2\n4\n6\n", copyOutOf("COPY frd_d (k) TO STDIN"));
        assertEquals("3 rows: 1\n3\n5\n",
                copyOutOf("COPY (SELECT i FROM frd_d ORDER BY i) TO STDIN"));

        // A row a copy out is to leave behind is the query's to leave behind: the clause a copy in
        // reads its rows through is not a clause a copy out has.
        assertEquals("42601", stateOf("COPY frd_d TO STDOUT WHERE i < 5"));
        assertEquals("WHERE clause not allowed with COPY TO",
                messageOf("COPY frd_d TO STDOUT WHERE i < 5"));

        // A plain statement cannot send a copy's data, so the client gives up on the copy the
        // moment the server asks for it -- and it asks just the same when the statement said
        // STDOUT. The server names the line it was waiting for.
        for (String sql : new String[] {"COPY frd_d FROM STDIN", "COPY frd_d FROM STDOUT",
                "COPY frd_d FROM STDOUT WITH (FORMAT csv)", "COPY frd_d (i,k) FROM STDOUT"}) {
            ServerErrorMessage m = fieldsOf(sql);
            assertEquals("57014", m.getSQLState(), sql);
            assertEquals("COPY from stdin failed:"
                    + " COPY commands are only supported using the CopyManager API.",
                    m.getMessage(), sql);
            assertEquals("COPY frd_d, line 1", m.getWhere(), sql);
        }

        // A copy out handed to a plain statement is refused on the client side, before a row of it
        // has been read -- so there is no server error at all to read fields off.
        for (String sql : new String[] {"COPY frd_d TO STDOUT", "COPY frd_d TO STDIN",
                "COPY frd_d TO STDIN WITH (FORMAT csv)", "COPY (SELECT 1) TO STDIN"}) {
            SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
            assertEquals("0A000", e.getSQLState(), sql);
            assertNull(((PSQLException) e).getServerErrorMessage(), sql);
            assertEquals("COPY commands are only supported using the CopyManager API.",
                    e.getMessage(), sql);
        }

        // A query is a thing to read out of and never one to write into, and that is the grammar's
        // answer rather than the direction word's.
        assertEquals("42601", stateOf("COPY (SELECT 1) FROM STDIN"));
        assertEquals("syntax error at or near \"FROM\"", messageOf("COPY (SELECT 1) FROM STDIN"));
        assertEquals(3, num("SELECT count(*) FROM frd_d"));
        exec("DROP TABLE frd_d");
    }

    /** The whole field set of the error a COPY FROM STDIN raises. */
    private static ServerErrorMessage copyErrorOf(String sql, String data) {
        try {
            CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
            copies.copyIn(sql, new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            assertTrue(e instanceof PSQLException, "expected a server error from: " + sql);
            ServerErrorMessage m = ((PSQLException) e).getServerErrorMessage();
            assertTrue(m != null, "expected a server error from: " + sql);
            return m;
        }
        throw new AssertionError("the copy was not refused: " + sql);
    }

    /** The rows a COPY TO wrote, and the text it wrote them as. */
    private static String copyOutOf(String sql) throws Exception {
        CopyManager copies = new CopyManager(conn.unwrap(BaseConnection.class));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long rows = copies.copyOut(sql, out);
        return rows + " rows: " + new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    // ============================================================ what a RETURNING list resolves

    /**
     * A write that brings in a second relation reads its RETURNING list in the scope of both, so a
     * bare name only the second relation holds answers to that relation. A name both hold answers
     * to neither and a name neither holds is reported missing -- and PostgreSQL owes those two
     * answers whether or not the statement would have written a row.
     */
    @Test
    void aBareReturningNameOnlyTheSecondRelationHoldsResolvesToThatRelation() throws SQLException {
        returningTargets();

        assertEquals("1|p,2|q", rows("UPDATE rtn_x t SET v='z' FROM rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, w"));
        // written out against the relation it comes from, the same name answers the same
        assertEquals("1|p,2|q", rows("UPDATE rtn_x t SET v='y' FROM rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, u.w"));
        assertEquals("1|1,2|2", rows("UPDATE rtn_x t SET v='y' FROM rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, j"));
        // a name under an operator is a name
        assertEquals("1|p!,2|q!", rows("UPDATE rtn_x t SET v='z' FROM rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, w || '!'"));
        // two second relations, each supplying its own
        assertEquals("1|p|P,2|q|Q", rows("UPDATE rtn_x t SET v='z' FROM rtn_xs u, rtn_xt s"
                + " WHERE t.i = u.j AND t.i = s.k RETURNING i, w, y"));
        // a pairing that reaches no row still answers with the clause's own columns
        assertEquals("", rows("UPDATE rtn_x t SET v='y' FROM rtn_xs u WHERE false RETURNING i, w"));

        // A name both relations hold answers to neither.
        assertEquals("42702", stateOf("UPDATE rtn_x t SET v='y' FROM rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, c"));
        assertEquals("column reference \"c\" is ambiguous",
                messageOf("UPDATE rtn_x t SET v='y' FROM rtn_xs u"
                        + " WHERE t.i = u.j RETURNING i, c"));
        // A name neither holds is reported, whether or not a row would have been written.
        for (String statement : new String[] {
                "UPDATE rtn_x t SET v='y' FROM rtn_xs u WHERE t.i = u.j RETURNING i, nosuch",
                "UPDATE rtn_x t SET v='y' FROM rtn_xs u WHERE false RETURNING i, nosuch",
                "UPDATE rtn_x SET v='z' RETURNING nosuch",
                "INSERT INTO rtn_x VALUES (9,'n','L9') RETURNING i, nosuch"}) {
            assertEquals("42703", stateOf(statement), statement);
            assertEquals("column \"nosuch\" does not exist", messageOf(statement), statement);
        }

        // DELETE reads the relation of its USING clause exactly as UPDATE reads its FROM.
        assertEquals("1|p,2|q", rows("DELETE FROM rtn_x t USING rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, w"));
        exec("INSERT INTO rtn_x VALUES (1,'a','L1'),(2,'b','L2')");
        assertEquals("42702", stateOf("DELETE FROM rtn_x t USING rtn_xs u"
                + " WHERE t.i = u.j RETURNING i, c"));
        assertEquals("3|c", rows("SELECT i, v FROM rtn_x WHERE i = 3"));
        dropReturningTargets();
    }

    /**
     * A second relation supplies its names however it was written: a query of its own, a VALUES
     * list, a set-returning call. What such an item does not hold is reported missing just as a
     * table's missing column is, and an item that produces no row supplies its names all the same.
     */
    @Test
    void aDerivedItemAValuesListAndACallSupplyTheirNamesToTheReturningList() throws SQLException {
        returningTargets();

        assertEquals("1|p,2|q", rows("UPDATE rtn_x t SET v='z' FROM (SELECT j, w FROM rtn_xs) u"
                + " WHERE t.i = u.j RETURNING i, w"));
        assertEquals("1|k", rows("UPDATE rtn_x t SET v='z' FROM (VALUES (1,'k')) u(j,w)"
                + " WHERE t.i = u.j RETURNING i, w"));
        assertEquals("1|1,2|2", rows("UPDATE rtn_x t SET v='z' FROM generate_series(1,2) g"
                + " WHERE t.i = g RETURNING i, g"));
        assertEquals("1|1,2|2", rows("DELETE FROM rtn_x t USING (SELECT j FROM rtn_xs) u"
                + " WHERE t.i = u.j RETURNING i, j"));
        exec("INSERT INTO rtn_x VALUES (1,'a','L1'),(2,'b','L2')");

        for (String statement : new String[] {
                "UPDATE rtn_x t SET v='z' FROM (SELECT j, w FROM rtn_xs) u"
                        + " WHERE t.i = u.j RETURNING i, nosuch",
                "UPDATE rtn_x t SET v='z' FROM (SELECT j, w FROM rtn_xs WHERE false) u"
                        + " WHERE t.i = u.j RETURNING i, nosuch"}) {
            assertEquals("42703", stateOf(statement), statement);
            assertEquals("column \"nosuch\" does not exist", messageOf(statement), statement);
        }
        dropReturningTargets();
    }

    /**
     * MERGE reads its source the same way, in every arm: the arm that updates, the one that
     * deletes, the one that inserts, and the one that acts on a target row no source row paired
     * with -- which reads the source's columns as nothing at all.
     */
    @Test
    void aMergeSourceSuppliesItsNamesToTheReturningListOfEveryArm() throws SQLException {
        returningTargets();

        assertEquals("1|p,2|q", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, w"));
        assertEquals("1|1,2|2", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, j"));
        assertEquals("3|null", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v='s' RETURNING i, w"));
        assertEquals("4|r", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN NOT MATCHED THEN INSERT VALUES (u.j, u.w) RETURNING i, w"));
        // an arm that writes nothing answers with the clause's own columns
        assertEquals("", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN DO NOTHING RETURNING i, w"));
        assertEquals("1|p,2|q,4|r", rows("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN DELETE RETURNING i, w"));
        assertEquals("3|s|L3", rows("SELECT i, v, c FROM rtn_x ORDER BY i"));

        // A source written as a query of its own or as a VALUES list supplies its names too.
        exec("INSERT INTO rtn_x VALUES (1,'a','L1'),(2,'b','L2')");
        assertEquals("1|p,2|q", rows("MERGE INTO rtn_x t USING (SELECT j, w FROM rtn_xs) u"
                + " ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, w"));
        assertEquals("1|k", rows("MERGE INTO rtn_x t USING (VALUES (1,'k')) u(j,w)"
                + " ON t.i = u.j WHEN MATCHED THEN UPDATE SET v='n' RETURNING i, w"));

        // The controls, in the arm that would have written.
        assertEquals("42702", stateOf("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, c"));
        assertEquals("column reference \"c\" is ambiguous",
                messageOf("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                        + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, c"));
        assertEquals("42703", stateOf("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, nosuch"));
        assertEquals("column \"nosuch\" does not exist",
                messageOf("MERGE INTO rtn_x t USING rtn_xs u ON t.i = u.j"
                        + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING i, nosuch"));
        // A source that is not a relation at all is reported ahead of the missing name.
        assertEquals("42P01", stateOf("MERGE INTO rtn_x t USING rtn_nosuch u ON t.i = u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING nosuch"));
        assertEquals("relation \"rtn_nosuch\" does not exist",
                messageOf("MERGE INTO rtn_x t USING rtn_nosuch u ON t.i = u.j"
                        + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING nosuch"));

        assertEquals("1|n|L1,2|m|L2,3|s|L3", rows("SELECT i, v, c FROM rtn_x ORDER BY i"));
        dropReturningTargets();
    }

    /** A target and two relations beside it: one sharing a column name with the target, one not. */
    private static void returningTargets() throws SQLException {
        exec("CREATE TABLE rtn_x (i int PRIMARY KEY, v text, c text)");
        exec("CREATE TABLE rtn_xs (j int, w text, c text)");
        exec("CREATE TABLE rtn_xt (k int, y text)");
        exec("INSERT INTO rtn_x VALUES (1,'a','L1'),(2,'b','L2'),(3,'c','L3')");
        exec("INSERT INTO rtn_xs VALUES (1,'p','R1'),(2,'q','R2'),(4,'r','R4')");
        exec("INSERT INTO rtn_xt VALUES (1,'P'),(2,'Q')");
    }

    private static void dropReturningTargets() throws SQLException {
        exec("DROP TABLE rtn_x");
        exec("DROP TABLE rtn_xs");
        exec("DROP TABLE rtn_xt");
    }

    // ============================================================ what an arbiter predicate implies

    private static final String NO_ARBITER =
            "there is no unique or exclusion constraint matching the ON CONFLICT specification";

    /**
     * A predicate written beside a conflict target names a partial index, and PostgreSQL takes the
     * index only when the index's own predicate follows from the one written. It asks that of the
     * two predicates as the planner leaves them, so the proof reaches through the forms one clause
     * can be written in: a NOT is the comparison it negates, BETWEEN is the conjunction it stands
     * for and IN the disjunction.
     */
    @Test
    void aPredicateWrittenAnotherWayRoundReachesTheSameIndex() throws SQLException {
        exec("CREATE TABLE arb_a (i int, f boolean, s text)");
        exec("CREATE UNIQUE INDEX arb_a_u ON arb_a (i) WHERE i > 0");
        exec("INSERT INTO arb_a VALUES (1,true,'a')");

        for (String predicate : new String[] {
                "NOT (i <= 0)",
                "i BETWEEN 1 AND 10",
                "i IN (1,5)",
                "i = 1",
                "i >= 1",
                "i > 5 OR i > 2",
                "i > 0 AND s = 'x'",
                "i > 0 AND i BETWEEN 1 AND 10"}) {
            exec("INSERT INTO arb_a VALUES (1,true,'b') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING");
        }
        assertEquals("1|t|a", rows("SELECT i, f, s FROM arb_a"));

        // and a predicate that admits a row the index does not hold reaches nothing
        for (String predicate : new String[] {
                "NOT (i < 0)",
                "NOT (i BETWEEN -5 AND 0)",
                "i BETWEEN -5 AND 10",
                "i IN (0,5)",
                "i NOT IN (0,-1)",
                "i > 0 OR s = 'x'",
                "i > 0 AND false",
                "true"}) {
            String sql = "INSERT INTO arb_a VALUES (1,true,'b') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_a"));
        exec("DROP TABLE arb_a");
    }

    /**
     * A test against a boolean constant is the operand it tests, so an index over a bare boolean
     * column is reached by every way of writing that the column holds. A boolean test is not the
     * operand: it answers where the operand is null, which is a row the index does not hold.
     */
    @Test
    void aBooleanComparedWithTrueIsTheOperandItCompares() throws SQLException {
        exec("CREATE TABLE arb_b (i int, f boolean)");
        exec("CREATE UNIQUE INDEX arb_b_u ON arb_b (i) WHERE f");
        exec("INSERT INTO arb_b VALUES (1,true)");

        for (String predicate : new String[] {
                "f", "f = true", "true = f", "f <> false", "NOT (f = false)", "NOT NOT f",
                "f AND i > 0"}) {
            exec("INSERT INTO arb_b VALUES (1,true) ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING");
        }
        for (String predicate : new String[] {"f IS TRUE", "f IS NOT NULL", "NOT f"}) {
            String sql = "INSERT INTO arb_b VALUES (1,true) ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_b"));
        exec("DROP TABLE arb_b");
    }

    /**
     * A strict operator or function answers nothing where its argument is null, so a clause written
     * over one of them proves the argument has a value -- which is what an index over IS NOT NULL
     * asks for. COALESCE, NULLIF and CASE answer for a row whose column is null, so a clause
     * written over one of them says nothing about the column.
     */
    @Test
    void aStrictCallProvesItsArgumentHasAValue() throws SQLException {
        exec("CREATE TABLE arb_d (i int, s text)");
        exec("CREATE UNIQUE INDEX arb_d_u ON arb_d (i) WHERE s IS NOT NULL");
        exec("INSERT INTO arb_d VALUES (1,'x')");

        for (String predicate : new String[] {
                "length(s) > 0",
                "upper(s) = 'X'",
                "substr(s,1,1) = 'x'",
                "s || 'y' = 'xy'",
                "s LIKE 'a%'",
                "s::int > 0",
                "NOT (s = 'q')",
                "NOT (s IS NULL)",
                "s IN ('a','b')",
                "s BETWEEN 'a' AND 'z'",
                "length(s) > 0 OR s = 'q'",
                "length(s) > 0 AND i > 0"}) {
            exec("INSERT INTO arb_d VALUES (1,'y') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING");
        }
        for (String predicate : new String[] {
                "coalesce(s,'y') = 'x'",
                "nullif(s,'q') = 'x'",
                "CASE WHEN s = 'x' THEN true ELSE false END",
                "s IS NULL",
                "i > 0"}) {
            String sql = "INSERT INTO arb_d VALUES (1,'y') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals("1|x", rows("SELECT i, s FROM arb_d"));
        exec("DROP TABLE arb_d");

        // A boolean column holding for a row is a row where it has a value; a NOT over it is no
        // strict thing at all, and neither is a boolean test.
        exec("CREATE TABLE arb_e (i int, f boolean)");
        exec("CREATE UNIQUE INDEX arb_e_u ON arb_e (i) WHERE f IS NOT NULL");
        exec("INSERT INTO arb_e VALUES (1,true)");
        exec("INSERT INTO arb_e VALUES (1,true) ON CONFLICT (i) WHERE f DO NOTHING");
        for (String predicate : new String[] {"NOT f", "f IS TRUE"}) {
            String sql = "INSERT INTO arb_e VALUES (1,true) ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_e"));
        exec("DROP TABLE arb_e");
    }

    /**
     * A bound proves an inequality where it rules the value out, and a bound over text is a bound
     * like any other. An index whose predicate is a disjunction is reached by whatever entails
     * either branch of it.
     */
    @Test
    void aBoundProvesAnInequalityWhereItRulesTheValueOut() throws SQLException {
        exec("CREATE TABLE arb_c (i int)");
        exec("CREATE UNIQUE INDEX arb_c_u ON arb_c (i) WHERE i <> 0");
        exec("INSERT INTO arb_c VALUES (1)");
        for (String predicate : new String[] {"i <> 0", "i > 0", "i >= 1", "i < 0", "i = 1"}) {
            exec("INSERT INTO arb_c VALUES (1) ON CONFLICT (i) WHERE " + predicate + " DO NOTHING");
        }
        for (String predicate : new String[] {"i > -1", "i >= 0"}) {
            String sql = "INSERT INTO arb_c VALUES (1) ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_c"));
        exec("DROP TABLE arb_c");

        exec("CREATE TABLE arb_g (i int, s text)");
        exec("CREATE UNIQUE INDEX arb_g_u ON arb_g (i) WHERE s > 'a'");
        exec("INSERT INTO arb_g VALUES (1,'x')");
        for (String predicate : new String[] {"s > 'b'", "s >= 'b'", "s = 'b'", "s > 'aa'",
                "s BETWEEN 'b' AND 'z'", "s IN ('b','c')", "NOT (s <= 'a')"}) {
            exec("INSERT INTO arb_g VALUES (1,'y') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING");
        }
        for (String predicate : new String[] {"s < 'b'", "s = 'a'", "s >= 'a'"}) {
            String sql = "INSERT INTO arb_g VALUES (1,'y') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals("1|x", rows("SELECT i, s FROM arb_g"));
        exec("DROP TABLE arb_g");

        exec("CREATE TABLE arb_f (i int)");
        exec("CREATE UNIQUE INDEX arb_f_u ON arb_f (i) WHERE i > 0 OR i < -10");
        exec("INSERT INTO arb_f VALUES (1)");
        for (String predicate : new String[] {"i > 5", "i < -20", "i > 0 OR i < -10",
                "i BETWEEN 1 AND 10"}) {
            exec("INSERT INTO arb_f VALUES (1) ON CONFLICT (i) WHERE " + predicate + " DO NOTHING");
        }
        String neither = "INSERT INTO arb_f VALUES (1) ON CONFLICT (i) WHERE i > -5 DO NOTHING";
        assertEquals("42P10", stateOf(neither));
        assertEquals(NO_ARBITER, messageOf(neither));
        assertEquals(1, num("SELECT count(*) FROM arb_f"));
        exec("DROP TABLE arb_f");
    }

    /**
     * Every part of the index's predicate has to be proved, and an index with no predicate at all
     * has nothing to prove -- so it takes any predicate and the predicate decides nothing about
     * which rows collide. A partial index, on the other hand, goes on refusing a conflict target
     * written with no predicate beside it.
     */
    @Test
    void everyPartOfTheIndexesPredicateHasToBeProvedAndAnIndexWithNoneTakesAnything()
            throws SQLException {
        exec("CREATE TABLE arb_i (i int, k int, s text)");
        exec("CREATE UNIQUE INDEX arb_i_u ON arb_i (i) WHERE i > 0 AND s IS NOT NULL");
        exec("INSERT INTO arb_i VALUES (1,1,'a')");
        exec("INSERT INTO arb_i VALUES (1,2,'b') ON CONFLICT (i) WHERE i > 5 AND s = 'x'"
                + " DO NOTHING");
        exec("INSERT INTO arb_i VALUES (1,2,'b')"
                + " ON CONFLICT (i) WHERE length(s) > 0 AND i BETWEEN 2 AND 3 DO NOTHING");
        for (String predicate : new String[] {"i > 5", "s IS NOT NULL"}) {
            String sql = "INSERT INTO arb_i VALUES (1,2,'b') ON CONFLICT (i) WHERE " + predicate
                    + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_i"));
        exec("DROP TABLE arb_i");

        exec("CREATE TABLE arb_j (i int PRIMARY KEY, k int)");
        exec("INSERT INTO arb_j VALUES (1,1)");
        exec("INSERT INTO arb_j VALUES (1,2) ON CONFLICT (i) WHERE false DO NOTHING");
        exec("INSERT INTO arb_j VALUES (1,3) ON CONFLICT (i) WHERE k > 0 DO NOTHING");
        assertEquals(1, update("INSERT INTO arb_j VALUES (1,4)"
                + " ON CONFLICT (i) WHERE i IN (1,5) DO UPDATE SET k = 9"));
        assertEquals("1|9", rows("SELECT i, k FROM arb_j"));
        exec("DROP TABLE arb_j");

        exec("CREATE TABLE arb_k (i int, k int)");
        exec("CREATE UNIQUE INDEX arb_k_u ON arb_k (i) WHERE i > 0");
        exec("INSERT INTO arb_k VALUES (1,1)");
        for (String target : new String[] {"(i)", "(i) WHERE false", "(i) WHERE k > 0"}) {
            String sql = "INSERT INTO arb_k VALUES (1,2) ON CONFLICT " + target + " DO NOTHING";
            assertEquals("42P10", stateOf(sql), sql);
            assertEquals(NO_ARBITER, messageOf(sql), sql);
        }
        assertEquals(1, num("SELECT count(*) FROM arb_k"));
        exec("DROP TABLE arb_k");
    }

    /**
     * The calls an arbiter predicate holds are resolved where the statement stands, and the
     * predicate is read and never evaluated -- so a call that names nothing is refused whatever it
     * would have answered, and a call that names something stands whatever it would have answered.
     * Which fault is reported follows from where it was written: the arguments before the name,
     * the predicate left to right, the target's own columns before the predicate and the action
     * after it.
     */
    @Test
    void aCallInAnArbiterPredicateIsResolvedWhereTheStatementStands() throws SQLException {
        exec("CREATE TABLE arb_h (i int PRIMARY KEY, s text)");
        exec("INSERT INTO arb_h VALUES (1,'a')");

        String missing = "INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchfunc(i) > 0 DO NOTHING";
        assertEquals("42883", stateOf(missing));
        assertEquals("function nosuchfunc(integer) does not exist", messageOf(missing));
        assertEquals("No function matches the given name and argument types."
                + " You might need to add explicit type casts.", hintOf(missing));

        assertEquals("function nosuchfunc() does not exist",
                messageOf("INSERT INTO arb_h VALUES (1,'b')"
                        + " ON CONFLICT (i) WHERE nosuchfunc() > 0 DO NOTHING"));
        assertEquals("function nosuchfunc(unknown) does not exist",
                messageOf("INSERT INTO arb_h VALUES (1,'b')"
                        + " ON CONFLICT (i) WHERE nosuchfunc('lit') > 0 DO NOTHING"));
        assertEquals("function pg_catalog.nosuchfunc(integer) does not exist",
                messageOf("INSERT INTO arb_h VALUES (1,'b')"
                        + " ON CONFLICT (i) WHERE pg_catalog.nosuchfunc(i) > 0 DO NOTHING"));
        // A name that exists but not for these arguments is refused the same way.
        assertEquals("function length(integer) does not exist",
                messageOf("INSERT INTO arb_h VALUES (1,'b')"
                        + " ON CONFLICT (i) WHERE length(i) > 0 DO NOTHING"));
        // A name that is a schema's to answer for is answered by the schema.
        String qualified = "INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchschema.nosuchfunc(i) > 0 DO NOTHING";
        assertEquals("3F000", stateOf(qualified));
        assertEquals("schema \"nosuchschema\" does not exist", messageOf(qualified));
        assertNull(hintOf(qualified));

        // The arguments are read before the name is looked for, and the predicate left to right.
        String inside = "INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchfunc(nosuchcol) > 0 DO NOTHING";
        assertEquals("42703", stateOf(inside));
        assertEquals("column \"nosuchcol\" does not exist", messageOf(inside));
        assertEquals("42703", stateOf("INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchcol > 0 AND nosuchfunc(i) > 0 DO NOTHING"));
        assertEquals("42883", stateOf("INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchfunc(i) > 0 AND nosuchcol > 0 DO NOTHING"));

        // The target's own columns are read first, and the action after the predicate.
        assertEquals("42703", stateOf("INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (nosuchcol) WHERE nosuchfunc(i) > 0 DO NOTHING"));
        assertEquals("42883", stateOf("INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (i) WHERE nosuchfunc(i) > 0 DO UPDATE SET s = nosuchcol"));
        // And the call is refused whether or not any index would have arbitrated.
        assertEquals("42883", stateOf("INSERT INTO arb_h VALUES (1,'b')"
                + " ON CONFLICT (s) WHERE nosuchfunc(i) > 0 DO NOTHING"));

        // A call that does name a function stands: the predicate is read, never evaluated.
        exec("INSERT INTO arb_h VALUES (1,'b') ON CONFLICT (i) WHERE random() > 0.5 DO NOTHING");
        assertEquals("1|a", rows("SELECT i, s FROM arb_h"));
        exec("DROP TABLE arb_h");
    }

    // ------------------------------------------------------------ helpers for a snapshot's writes

    /** What a statement is refused with when the row it meant to write was written under it. */
    private static final String CONCURRENT_UPDATE =
            "ERR[40001] ERROR: could not serialize access due to concurrent update";

    /** And what it is refused with when the row was taken away instead. */
    private static final String CONCURRENT_DELETE =
            "ERR[40001] ERROR: could not serialize access due to concurrent delete";

    /** One row, holding the value every qualification below is written against. */
    private static void qualifiedTarget(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int PRIMARY KEY, v int)");
        exec("INSERT INTO " + name + " VALUES (22,5)");
    }

    /**
     * The same as {@link #underSnapshotWhileAnotherSessionHolds}, for an actor that is expected to
     * answer while the other session is still holding: the row it waits for is not one it writes.
     * That it answered at all is what is read here, and the other session has not finished, so a
     * statement that had waited for it could not have answered whatever else the machine was busy
     * with.
     */
    private static String underSnapshotWithoutWaiting(String level, String snapshotQuery,
            String holding, String finish, String statement) throws Exception {
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, holding);
            execOn(actor, "BEGIN ISOLATION LEVEL " + level);
            execOn(actor, snapshotQuery);
            Future<String> pending = pool.submit(() -> answerOf(actor, statement));
            String answer = answerWithin(pending);
            execOn(holder, finish);
            try {
                execOn(actor, "COMMIT");
            } catch (SQLException ignored) {
                // an aborted transaction ends either way
            }
            return answer;
        }
    }

    /**
     * The same again for an actor with no statement before its write, so the snapshot the write is
     * judged against is the one the write itself fixed.
     */
    private static String underASnapshotTheWriteFixesItself(String level, String holding,
            String finish, String statement) throws Exception {
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, holding);
            execOn(actor, "BEGIN ISOLATION LEVEL " + level);
            Future<String> pending = pool.submit(() -> answerOf(actor, statement));
            Thread.sleep(BLOCKED_MS);
            assertFalse(pending.isDone(),
                    "answered while another session still held the row: " + statement);
            execOn(holder, finish);
            String answer = answerWithin(pending);
            try {
                execOn(actor, "COMMIT");
            } catch (SQLException ignored) {
                // an aborted transaction ends either way
            }
            return answer;
        }
    }

    // ============================================================ a write reading from a snapshot,
    // and the row another session wrote under it

    @Test
    void aWriteIsRefusedTheRowAConcurrentUpdateSteeredOutOfItsQualification() throws Exception {
        qualifiedTarget("swq_a1");
        // The version this transaction may act on answers the qualification and the version the
        // other session committed does not. PostgreSQL will not let the statement report that it
        // wrote nothing over a row that was taken from it, so it ends the transaction instead.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a1",
                "UPDATE swq_a1 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_a1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_a1"));
        exec("DROP TABLE swq_a1");
    }

    @Test
    void aDeleteIsRefusedThatRowInTheSameWordsAsAnUpdate() throws Exception {
        qualifiedTarget("swq_a2");
        // It is the row's version that moved and not the row that went, so PostgreSQL says
        // "concurrent update" here as well.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a2",
                "UPDATE swq_a2 SET v = -1 WHERE i = 22", "COMMIT",
                "DELETE FROM swq_a2 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_a2"));
        exec("DROP TABLE swq_a2");
    }

    @Test
    void aSerializableWriteIsRefusedThatRowTheSameWay() throws Exception {
        qualifiedTarget("swq_a3");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM swq_a3",
                "UPDATE swq_a3 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_a3 SET v = 9 WHERE i = 22 AND v > 0"));
        exec("DROP TABLE swq_a3");

        qualifiedTarget("swq_a4");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM swq_a4",
                "UPDATE swq_a4 SET v = -1 WHERE i = 22", "COMMIT",
                "DELETE FROM swq_a4 WHERE i = 22 AND v > 0"));
        exec("DROP TABLE swq_a4");
    }

    @Test
    void aWriteNamingNoQualificationAtAllIsRefusedThatRowToo() throws Exception {
        qualifiedTarget("swq_a5");
        // The refusal is owed to the row's version having moved, not to what the qualification
        // made of it, so a statement carrying none at all is refused just the same.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a5",
                "UPDATE swq_a5 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_a5 SET v = 9"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_a5"));
        exec("DROP TABLE swq_a5");
    }

    @Test
    void neitherRowIsWrittenWhenOneOfTheTwoWasSteeredOutOfTheQualification() throws Exception {
        qualifiedTarget("swq_a6");
        exec("INSERT INTO swq_a6 VALUES (23,5)");
        // The statement is refused whole: the row the other session never touched is not written
        // either, which is what a statement that quietly passed the lost row over would have done.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a6",
                "UPDATE swq_a6 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_a6 SET v = 9 WHERE v > 0"));
        assertEquals("22|-1,23|5", rows("SELECT i, v FROM swq_a6 ORDER BY i"));
        exec("DROP TABLE swq_a6");
    }

    @Test
    void theRefusalIsOwedWithNothingLeftToWaitForEither() throws Exception {
        qualifiedTarget("swq_a7");
        // The other session's transaction was over before this statement began, so nothing blocks
        // -- and what this transaction is entitled to act on is still the version it was shown.
        assertEquals(CONCURRENT_UPDATE, underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a7",
                "UPDATE swq_a7 SET v = -1 WHERE i = 22",
                "UPDATE swq_a7 SET v = 9 WHERE i = 22 AND v > 0"));
        exec("UPDATE swq_a7 SET v = 5 WHERE i = 22");
        assertEquals(CONCURRENT_UPDATE, underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM swq_a7",
                "UPDATE swq_a7 SET v = -1 WHERE i = 22",
                "DELETE FROM swq_a7 WHERE i = 22 AND v > 0"));
        exec("UPDATE swq_a7 SET v = 5 WHERE i = 22");
        assertEquals(CONCURRENT_UPDATE, underSnapshotAfterAnotherSessionCommitted(
                "SERIALIZABLE", "SELECT count(*) FROM swq_a7",
                "UPDATE swq_a7 SET v = -1 WHERE i = 22",
                "UPDATE swq_a7 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_a7"));
        exec("DROP TABLE swq_a7");
    }

    @Test
    void aRowOfAPartitionedRelationIsRefusedWhicheverRelationTheStatementNames() throws Exception {
        exec("CREATE TABLE swq_p1 (i int, v int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE swq_p1a PARTITION OF swq_p1 FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE swq_p1b PARTITION OF swq_p1 FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO swq_p1 VALUES (2,5)");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_p1",
                "UPDATE swq_p1 SET v = -1 WHERE i = 2", "COMMIT",
                "UPDATE swq_p1 SET v = 9 WHERE i = 2 AND v > 0"));
        assertEquals("2|-1", rows("SELECT i, v FROM swq_p1"));

        // The same written against the partition the row is really stored in.
        exec("UPDATE swq_p1 SET v = 5 WHERE i = 2");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_p1a",
                "UPDATE swq_p1a SET v = -1 WHERE i = 2", "COMMIT",
                "UPDATE swq_p1a SET v = 9 WHERE i = 2 AND v > 0"));

        // And a row the other session moved into the other partition, which is the same rule read
        // through a second relation.
        exec("UPDATE swq_p1 SET v = 5 WHERE i = 2");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_p1",
                "UPDATE swq_p1 SET i = 12 WHERE i = 2", "COMMIT",
                "UPDATE swq_p1 SET v = 9 WHERE i = 2 AND v > 0"));
        assertEquals("12|5", rows("SELECT i, v FROM swq_p1"));
        exec("DROP TABLE swq_p1 CASCADE");
    }

    @Test
    void aRowReachedThroughAnInheritanceParentIsRefusedTheSameWay() throws Exception {
        exec("CREATE TABLE swq_h1 (i int, v int)");
        exec("CREATE TABLE swq_h1c (extra text) INHERITS (swq_h1)");
        exec("INSERT INTO swq_h1c VALUES (3,5,'x')");
        // The child carries a column its parent never declared, and the row is still the row.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_h1",
                "UPDATE swq_h1 SET v = -1 WHERE i = 3", "COMMIT",
                "UPDATE swq_h1 SET v = 9 WHERE i = 3 AND v > 0"));
        assertEquals("3|-1|x", rows("SELECT i, v, extra FROM swq_h1c"));
        exec("DROP TABLE swq_h1c");
        exec("DROP TABLE swq_h1");
    }

    @Test
    void aWriteJoinedToAnotherRelationIsRefusedItsOwnTargetRow() throws Exception {
        exec("CREATE TABLE swq_j1 (i int PRIMARY KEY, v int)");
        exec("CREATE TABLE swq_j1f (i int, w int)");
        exec("INSERT INTO swq_j1 VALUES (1,5)");
        exec("INSERT INTO swq_j1f VALUES (1,100)");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_j1",
                "UPDATE swq_j1 SET v = -1 WHERE i = 1", "COMMIT",
                "UPDATE swq_j1 SET v = swq_j1f.w FROM swq_j1f"
                        + " WHERE swq_j1f.i = swq_j1.i AND swq_j1.v > 0"));
        assertEquals("1|-1", rows("SELECT i, v FROM swq_j1"));

        exec("UPDATE swq_j1 SET v = 5 WHERE i = 1");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_j1",
                "UPDATE swq_j1 SET v = -1 WHERE i = 1", "COMMIT",
                "DELETE FROM swq_j1 USING swq_j1f"
                        + " WHERE swq_j1f.i = swq_j1.i AND swq_j1.v > 0"));
        assertEquals("1|-1", rows("SELECT i, v FROM swq_j1"));
        exec("DROP TABLE swq_j1");
        exec("DROP TABLE swq_j1f");
    }

    @Test
    void whicheverStatementFixedTheSnapshotTheRefusalIsTheSame() throws Exception {
        qualifiedTarget("swq_s1");
        exec("INSERT INTO swq_s1 VALUES (24,5)");
        // Reading the rows themselves rather than counting them.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT i, v FROM swq_s1 ORDER BY i",
                "UPDATE swq_s1 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_s1 SET v = 9 WHERE i = 22 AND v > 0"));

        // Locking a row the write never reaches.
        exec("UPDATE swq_s1 SET v = 5 WHERE i = 22");
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT i FROM swq_s1 WHERE i = 24 FOR UPDATE",
                "UPDATE swq_s1 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_s1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1,24|5", rows("SELECT i, v FROM swq_s1 ORDER BY i"));
        exec("DROP TABLE swq_s1");
    }

    @Test
    void aQualificationReadingASecondRelationIsStillRefusedForItsTargetRow() throws Exception {
        qualifiedTarget("swq_s2");
        exec("CREATE TABLE swq_s2u (i int, w int)");
        exec("INSERT INTO swq_s2u VALUES (22,1)");
        // The other session writes both relations. The subquery is read from the snapshot as it
        // always was; the refusal is owed to the target row and to nothing the subquery saw.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_s2",
                "UPDATE swq_s2 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_s2 SET v = 9 WHERE i = 22 AND v > 0"
                        + " AND EXISTS (SELECT 1 FROM swq_s2u WHERE swq_s2u.i = swq_s2.i"
                        + " AND w > 0)"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_s2"));
        exec("DROP TABLE swq_s2");
        exec("DROP TABLE swq_s2u");
    }

    @Test
    void theTransactionTheRefusedWriteStoodInTakesNothingFurther() throws Exception {
        qualifiedTarget("swq_e1");
        try (Connection holder = openSession(); Connection actor = openSession()) {
            execOn(holder, "BEGIN");
            execOn(holder, "UPDATE swq_e1 SET v = -1 WHERE i = 22");
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM swq_e1");
            Future<String> pending = pool.submit(
                    () -> answerOf(actor, "UPDATE swq_e1 SET v = 9 WHERE i = 22 AND v > 0"));
            Thread.sleep(BLOCKED_MS);
            assertFalse(pending.isDone(), "answered while the other session still held the row");
            execOn(holder, "COMMIT");
            assertEquals(CONCURRENT_UPDATE, answerWithin(pending));
            assertTrue(answerOf(actor, "SELECT count(*) FROM swq_e1").startsWith("ERR[25P02] "),
                    "the transaction was left able to run another statement");
            execOn(actor, "ROLLBACK");
            assertEquals("1;", answerOf(actor, "SELECT count(*) FROM swq_e1"));
        }
        assertEquals("22|-1", rows("SELECT i, v FROM swq_e1"));
        exec("DROP TABLE swq_e1");
    }

    // ============================================================ what that refusal lets through

    @Test
    void aBlockerThatRollsBackLeavesTheRowToBeWrittenAfterAll() throws Exception {
        qualifiedTarget("swq_b1");
        // The other session's write never happened, so the version this transaction was shown is
        // still the relation's own and the qualification still holds for it.
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_b1",
                "UPDATE swq_b1 SET v = -1 WHERE i = 22", "ROLLBACK",
                "UPDATE swq_b1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|9", rows("SELECT i, v FROM swq_b1"));
        exec("DROP TABLE swq_b1");

        qualifiedTarget("swq_b2");
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "SERIALIZABLE", "SELECT count(*) FROM swq_b2",
                "UPDATE swq_b2 SET v = -1 WHERE i = 22", "ROLLBACK",
                "DELETE FROM swq_b2 WHERE i = 22 AND v > 0"));
        assertEquals("", rows("SELECT i, v FROM swq_b2"));
        exec("DROP TABLE swq_b2");
    }

    @Test
    void aQualificationTheCommittedVersionStillSatisfiesIsRefusedAllTheSame() throws Exception {
        qualifiedTarget("swq_b3");
        // The version moved, whatever it moved to: this write would have found a row to act on
        // either way, and it is still not the row this transaction was shown.
        assertEquals(CONCURRENT_UPDATE, underSnapshotWhileAnotherSessionHolds(
                "REPEATABLE READ", "SELECT count(*) FROM swq_b3",
                "UPDATE swq_b3 SET v = 7 WHERE i = 22", "COMMIT",
                "UPDATE swq_b3 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|7", rows("SELECT i, v FROM swq_b3"));
        exec("DROP TABLE swq_b3");
    }

    @Test
    void readCommittedReReadsTheRowAndFindsItNoLongerQualifies() throws Exception {
        qualifiedTarget("swq_k1");
        // A transaction that reads each statement afresh has no snapshot to judge a write by: it
        // reads what the row has become, finds the qualification does not hold, and says so.
        assertEquals("[0 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM swq_k1",
                "UPDATE swq_k1 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_k1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_k1"));

        exec("UPDATE swq_k1 SET v = 5 WHERE i = 22");
        assertEquals("[0 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM swq_k1",
                "UPDATE swq_k1 SET v = -1 WHERE i = 22", "COMMIT",
                "DELETE FROM swq_k1 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_k1"));

        exec("UPDATE swq_k1 SET v = 5 WHERE i = 22");
        assertEquals("[0 rows]", underSnapshotAfterAnotherSessionCommitted(
                "READ COMMITTED", "SELECT count(*) FROM swq_k1",
                "UPDATE swq_k1 SET v = -1 WHERE i = 22",
                "UPDATE swq_k1 SET v = 9 WHERE i = 22 AND v > 0"));
        exec("DROP TABLE swq_k1");
    }

    @Test
    void readCommittedWritesTheVersionTheOtherSessionCommittedWhenItStillQualifies()
            throws Exception {
        qualifiedTarget("swq_k2");
        assertEquals("[1 rows]", underSnapshotWhileAnotherSessionHolds(
                "READ COMMITTED", "SELECT count(*) FROM swq_k2",
                "UPDATE swq_k2 SET v = 7 WHERE i = 22", "COMMIT",
                "UPDATE swq_k2 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|9", rows("SELECT i, v FROM swq_k2"));
        exec("DROP TABLE swq_k2");
    }

    @Test
    void aVersionTheQualificationNeverHeldForIsNoRowToBeRefusedOver() throws Exception {
        exec("CREATE TABLE swq_v1 (i int PRIMARY KEY, v int)");
        exec("INSERT INTO swq_v1 VALUES (22,-1)");
        // The version this transaction may act on is the one holding -1, which its qualification
        // never held for. There is no row for it to be refused over, whatever the other session
        // has since committed -- and no row for it to wait on, either.
        assertEquals("[0 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM swq_v1",
                "UPDATE swq_v1 SET v = 5 WHERE i = 22",
                "UPDATE swq_v1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|5", rows("SELECT i, v FROM swq_v1"));

        exec("UPDATE swq_v1 SET v = -1 WHERE i = 22");
        assertEquals("[0 rows]", underSnapshotAfterAnotherSessionCommitted(
                "REPEATABLE READ", "SELECT count(*) FROM swq_v1",
                "UPDATE swq_v1 SET v = 5 WHERE i = 22",
                "DELETE FROM swq_v1 WHERE i = 22 AND v > 0"));
        assertEquals("22|5", rows("SELECT i, v FROM swq_v1"));

        exec("UPDATE swq_v1 SET v = -1 WHERE i = 22");
        assertEquals("[0 rows]", underSnapshotWithoutWaiting(
                "REPEATABLE READ", "SELECT count(*) FROM swq_v1",
                "UPDATE swq_v1 SET v = 5 WHERE i = 22", "COMMIT",
                "UPDATE swq_v1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|5", rows("SELECT i, v FROM swq_v1"));
        exec("DROP TABLE swq_v1");
    }

    @Test
    void aWriteThatNeverReachesTheOtherSessionsRowIsNeitherBlockedNorRefused() throws Exception {
        qualifiedTarget("swq_v2");
        exec("INSERT INTO swq_v2 VALUES (23,5)");
        assertEquals("[1 rows]", underSnapshotWithoutWaiting(
                "REPEATABLE READ", "SELECT count(*) FROM swq_v2",
                "UPDATE swq_v2 SET v = -1 WHERE i = 23", "COMMIT",
                "UPDATE swq_v2 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|9,23|-1", rows("SELECT i, v FROM swq_v2 ORDER BY i"));
        exec("DROP TABLE swq_v2");
    }

    @Test
    void twoSnapshotsWritingDifferentRowsBothKeepTheirWrites() throws Exception {
        exec("CREATE TABLE swq_v3 (i int PRIMARY KEY, v int)");
        exec("INSERT INTO swq_v3 VALUES (1,1),(2,2)");
        try (Connection one = openSession(); Connection two = openSession()) {
            execOn(one, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(one, "SELECT count(*) FROM swq_v3");
            execOn(two, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(two, "SELECT count(*) FROM swq_v3");
            // Neither waits for the other and neither is refused: they write different rows.
            assertEquals("[1 rows]", answerOf(one, "UPDATE swq_v3 SET v = 11 WHERE i = 1"));
            assertEquals("[1 rows]", answerOf(two, "UPDATE swq_v3 SET v = 22 WHERE i = 2"));
            execOn(one, "COMMIT");
            execOn(two, "COMMIT");
        }
        assertEquals("1|11,2|22", rows("SELECT i, v FROM swq_v3 ORDER BY i"));
        exec("DROP TABLE swq_v3");
    }

    @Test
    void aTransactionGoesOnReadingItsOwnSnapshotWhileAnotherSessionWritesAndCommits()
            throws Exception {
        exec("CREATE TABLE swq_v4 (i int PRIMARY KEY, v int)");
        exec("INSERT INTO swq_v4 VALUES (1,11),(2,22)");
        try (Connection other = openSession(); Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            assertEquals("1|11;2|22;", answerOf(actor, "SELECT i, v FROM swq_v4 ORDER BY i"));
            execOn(other, "INSERT INTO swq_v4 VALUES (3,33)");
            execOn(other, "UPDATE swq_v4 SET v = 99 WHERE i = 1");
            // What the other session committed is nothing to this transaction until it ends.
            assertEquals("1|11;2|22;", answerOf(actor, "SELECT i, v FROM swq_v4 ORDER BY i"));
            execOn(actor, "COMMIT");
            assertEquals("1|99;2|22;3|33;",
                    answerOf(actor, "SELECT i, v FROM swq_v4 ORDER BY i"));
        }
        exec("DROP TABLE swq_v4");
    }

    @Test
    void aRowThisTransactionSteeredOutOfItsOwnQualificationIsOnlyPassedOver() throws Exception {
        qualifiedTarget("swq_v5");
        try (Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM swq_v5");
            // The row's version has moved, and it moved by this transaction's own hand: it reads
            // it as it left it, and there is nothing here to refuse.
            assertEquals("[1 rows]", answerOf(actor, "UPDATE swq_v5 SET v = -1 WHERE i = 22"));
            assertEquals("[0 rows]",
                    answerOf(actor, "UPDATE swq_v5 SET v = 9 WHERE i = 22 AND v > 0"));
            assertEquals("[0 rows]",
                    answerOf(actor, "DELETE FROM swq_v5 WHERE i = 22 AND v > 0"));
            assertEquals("[1 rows]",
                    answerOf(actor, "UPDATE swq_v5 SET v = 8 WHERE i = 22 AND v < 0"));
            execOn(actor, "COMMIT");
        }
        assertEquals("22|8", rows("SELECT i, v FROM swq_v5"));
        exec("DROP TABLE swq_v5");
    }

    // ============================================================ a row the snapshot was never
    // shown, and a snapshot the write fixed itself

    @Test
    void aRowCommittedAfterTheSnapshotIsPassedOverWithoutWaitingForTheSessionHoldingIt()
            throws Exception {
        qualifiedTarget("swq_n1");
        try (Connection committer = openSession(); Connection holder = openSession();
                Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM swq_n1");
            execOn(committer, "INSERT INTO swq_n1 VALUES (23,5)");
            execOn(holder, "BEGIN");
            execOn(holder, "DELETE FROM swq_n1 WHERE i = 23");
            try {
                // Row 23 is not in this transaction's snapshot, so its scan never reaches it and
                // whatever becomes of the other session's delete is nothing to it. Both of these
                // answer while that session is still holding the row, which is what says they
                // never waited for it.
                Future<String> pending = pool.submit(
                        () -> answerOf(actor, "UPDATE swq_n1 SET v = 10 WHERE i = 23"));
                assertEquals("[0 rows]", answerWithin(pending));

                Future<String> alsoPending = pool.submit(
                        () -> answerOf(actor, "DELETE FROM swq_n1 WHERE i = 23"));
                assertEquals("[0 rows]", answerWithin(alsoPending));
            } finally {
                execOn(holder, "ROLLBACK");
            }
            execOn(actor, "COMMIT");
        }
        assertEquals("22|5,23|5", rows("SELECT i, v FROM swq_n1 ORDER BY i"));
        exec("DROP TABLE swq_n1");
    }

    @Test
    void theSameRowHeldByAnUpdateRatherThanADeleteIsPassedOverTheSameWay() throws Exception {
        qualifiedTarget("swq_n2");
        try (Connection committer = openSession(); Connection holder = openSession();
                Connection actor = openSession()) {
            execOn(actor, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            execOn(actor, "SELECT count(*) FROM swq_n2");
            execOn(committer, "INSERT INTO swq_n2 VALUES (23,5)");
            execOn(holder, "BEGIN");
            execOn(holder, "UPDATE swq_n2 SET v = -1 WHERE i = 23");
            try {
                // It answers while the other session is still holding, so it never waited for it.
                Future<String> pending = pool.submit(
                        () -> answerOf(actor, "UPDATE swq_n2 SET v = 10 WHERE i = 23"));
                assertEquals("[0 rows]", answerWithin(pending));
            } finally {
                execOn(holder, "ROLLBACK");
            }
            execOn(actor, "COMMIT");
        }
        exec("DROP TABLE swq_n2");
    }

    @Test
    void readCommittedDoesReachThatRowAndWaitsForTheSessionHoldingIt() throws Exception {
        qualifiedTarget("swq_n3");
        try (Connection committer = openSession(); Connection holder = openSession();
                Connection actor = openSession()) {
            execOn(actor, "BEGIN");
            execOn(actor, "SELECT count(*) FROM swq_n3");
            execOn(committer, "INSERT INTO swq_n3 VALUES (23,5)");
            execOn(holder, "BEGIN");
            execOn(holder, "DELETE FROM swq_n3 WHERE i = 23");
            Future<String> pending = pool.submit(
                    () -> answerOf(actor, "UPDATE swq_n3 SET v = 10 WHERE i = 23"));
            try {
                Thread.sleep(BLOCKED_MS);
                assertFalse(pending.isDone(), "answered without waiting for the row it writes");
            } finally {
                execOn(holder, "ROLLBACK");
            }
            assertEquals("[1 rows]", answerWithin(pending));
            execOn(actor, "COMMIT");
        }
        assertEquals("22|5,23|10", rows("SELECT i, v FROM swq_n3 ORDER BY i"));
        exec("DROP TABLE swq_n3");
    }

    @Test
    void aWriteThatFixedItsOwnSnapshotIsRefusedTheRowAConcurrentDeleteTookAway()
            throws Exception {
        qualifiedTarget("swq_f1");
        // Nothing ran in this transaction before the write, so the snapshot the write is judged
        // against is the one the write itself fixed -- and it is judged against it all the same.
        assertEquals(CONCURRENT_DELETE, underASnapshotTheWriteFixesItself(
                "REPEATABLE READ", "DELETE FROM swq_f1 WHERE i = 22", "COMMIT",
                "UPDATE swq_f1 SET v = 10 WHERE i = 22"));
        assertEquals("", rows("SELECT i, v FROM swq_f1"));

        exec("INSERT INTO swq_f1 VALUES (22,5)");
        assertEquals(CONCURRENT_DELETE, underASnapshotTheWriteFixesItself(
                "SERIALIZABLE", "DELETE FROM swq_f1 WHERE i = 22", "COMMIT",
                "DELETE FROM swq_f1 WHERE i = 22"));

        // And with the concurrent writer replacing the row rather than taking it away.
        exec("INSERT INTO swq_f1 VALUES (22,5)");
        assertEquals(CONCURRENT_UPDATE, underASnapshotTheWriteFixesItself(
                "REPEATABLE READ", "UPDATE swq_f1 SET v = -1 WHERE i = 22", "COMMIT",
                "UPDATE swq_f1 SET v = 9 WHERE i = 22 AND v > 0"));
        assertEquals("22|-1", rows("SELECT i, v FROM swq_f1"));
        exec("DROP TABLE swq_f1");
    }

    @Test
    void aWriteThatFixedItsOwnSnapshotIsNotRefusedAtReadCommittedOrAfterARollback()
            throws Exception {
        qualifiedTarget("swq_f2");
        assertEquals("[0 rows]", underASnapshotTheWriteFixesItself(
                "READ COMMITTED", "DELETE FROM swq_f2 WHERE i = 22", "COMMIT",
                "UPDATE swq_f2 SET v = 10 WHERE i = 22"));
        assertEquals("", rows("SELECT i, v FROM swq_f2"));

        exec("INSERT INTO swq_f2 VALUES (22,5)");
        assertEquals("[1 rows]", underASnapshotTheWriteFixesItself(
                "REPEATABLE READ", "DELETE FROM swq_f2 WHERE i = 22", "ROLLBACK",
                "UPDATE swq_f2 SET v = 10 WHERE i = 22"));
        assertEquals("22|10", rows("SELECT i, v FROM swq_f2"));
        exec("DROP TABLE swq_f2");
    }

    // ------------------------------------------------------------ helpers for the raw protocol

    /** A raw protocol session, opened and authenticated. */
    private static RawWireClient rawSession() throws IOException {
        RawWireClient c = new RawWireClient(memgres.getPort());
        c.startup(memgres.getUser(), "memgres");
        return c;
    }

    /** Parse, Bind, Describe Portal, Execute and Sync, pipelined in one batch. */
    private static void askOverExtended(RawWireClient c, String sql) throws IOException {
        c.write(RawWireClient.parse(sql));
        c.write(RawWireClient.bind());
        c.write(RawWireClient.describePortal());
        c.write(RawWireClient.execute());
        c.write(RawWireClient.sync());
    }

    /**
     * Everything the server said, as one string. It ends in {@code <waiting>} once the server has
     * fallen silent, so a message the client was never sent shows as a message that is not there
     * and a message it was sent twice shows as one written twice.
     */
    private static String frames(RawWireClient c) {
        return String.join(" ", c.readUntilQuiet());
    }

    /** The reply to one statement, read up to and including its ReadyForQuery. */
    private static String readToReady(RawWireClient c) {
        StringBuilder said = new StringBuilder();
        try {
            while (true) {
                RawWireClient.Msg m = c.read();
                if (said.length() > 0) said.append(' ');
                if (m == null) {
                    said.append("<closed>");
                    break;
                }
                said.append(m);
                if (m.type == 'Z') break;
            }
        } catch (IOException e) {
            if (said.length() > 0) said.append(' ');
            said.append("<waiting>");
        }
        return said.toString();
    }

    /** Run a statement on the raw session in simple query mode and read through its reply. */
    private static void queryOn(RawWireClient c, String sql) throws IOException {
        c.write(RawWireClient.query(sql));
        readToReady(c);
    }

    /** How many times one string is written inside another. */
    private static int occurrences(String said, String part) {
        int seen = 0;
        for (int at = said.indexOf(part); at >= 0; at = said.indexOf(part, at + part.length())) {
            seen++;
        }
        return seen;
    }

    private static void lockedRow(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int PRIMARY KEY, s text)");
        exec("INSERT INTO " + name + " VALUES (1,'a')");
    }

    private static void movingRow(String name) throws SQLException {
        exec("CREATE TABLE " + name + " (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE " + name + "a PARTITION OF " + name + " FOR VALUES FROM (0) TO (10)");
        exec("CREATE TABLE " + name + "b PARTITION OF " + name + " FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO " + name + " VALUES (2,'a')");
    }

    // ============================================================ the frames a refusal arrives in

    @Test
    void aForUpdateWhoseRowMovedToAnotherPartitionIsRefusedAfterItsRowDescription()
            throws Exception {
        movingRow("dpr_m1");
        try (Connection writer = openSession(); RawWireClient c = rawSession()) {
            execOn(writer, "BEGIN");
            execOn(writer, "UPDATE dpr_m1 SET i = 12 WHERE i = 2");
            askOverExtended(c, "SELECT * FROM dpr_m1 WHERE i = 2 FOR UPDATE");
            Thread.sleep(BLOCKED_MS);
            execOn(writer, "COMMIT");
            // The portal's shape is settled without running the statement, so the row description
            // stands in front of the refusal rather than being lost with it.
            assertEquals("1 2 T E[40001] tuple to be locked was already moved to another partition"
                    + " due to concurrent update Z[I] <waiting>", frames(c));
        }
        assertEquals("12|a", rows("SELECT i, s FROM dpr_m1"));
        exec("DROP TABLE dpr_m1 CASCADE");
    }

    @Test
    void theBlockARefusedLockStoodInIsLeftAbortedAtEveryIsolationLevel() throws Exception {
        movingRow("dpr_m2");
        String[][] levels = {
            {"READ COMMITTED", "tuple to be locked was already moved to another partition"
                    + " due to concurrent update"},
            {"REPEATABLE READ", "could not serialize access due to concurrent update"},
            {"SERIALIZABLE", "could not serialize access due to concurrent update"},
        };
        for (String[] level : levels) {
            try (Connection writer = openSession(); RawWireClient c = rawSession()) {
                queryOn(c, "BEGIN ISOLATION LEVEL " + level[0]);
                queryOn(c, "SELECT count(*) FROM dpr_m2");
                execOn(writer, "BEGIN");
                execOn(writer, "UPDATE dpr_m2 SET i = 12 WHERE i = 2");
                askOverExtended(c, "SELECT * FROM dpr_m2 WHERE i = 2 FOR UPDATE");
                Thread.sleep(BLOCKED_MS);
                execOn(writer, "COMMIT");
                // The refusal ends the block the statement stood in, so the ReadyForQuery that
                // follows says the block is aborted rather than open.
                assertEquals("1 2 T E[40001] " + level[1] + " Z[E] <waiting>", frames(c),
                        level[0]);
                queryOn(c, "ROLLBACK");
            }
            exec("UPDATE dpr_m2 SET i = 2 WHERE i = 12");
        }
        assertEquals("2|a", rows("SELECT i, s FROM dpr_m2"));
        exec("DROP TABLE dpr_m2 CASCADE");
    }

    @Test
    void aLockTimeoutIsReportedAfterTheRowDescriptionAndNotBeforeIt() throws Exception {
        lockedRow("dpr_l1");
        try (Connection writer = openSession(); RawWireClient c = rawSession()) {
            execOn(writer, "BEGIN");
            execOn(writer, "UPDATE dpr_l1 SET s = 'x' WHERE i = 1");
            queryOn(c, "SET lock_timeout = '400ms'");
            askOverExtended(c, "SELECT * FROM dpr_l1 WHERE i = 1 FOR UPDATE");
            assertEquals("1 2 T E[55P03] canceling statement due to lock timeout Z[I] <waiting>",
                    frames(c));
            execOn(writer, "ROLLBACK");
        }
        assertEquals("1|a", rows("SELECT i, s FROM dpr_l1"));
        exec("DROP TABLE dpr_l1");
    }

    @Test
    void aNowaitRefusalIsReportedAfterTheRowDescriptionToo() throws Exception {
        lockedRow("dpr_l2");
        try (Connection writer = openSession(); RawWireClient c = rawSession()) {
            execOn(writer, "BEGIN");
            execOn(writer, "UPDATE dpr_l2 SET s = 'y' WHERE i = 1");
            askOverExtended(c, "SELECT * FROM dpr_l2 WHERE i = 1 FOR UPDATE NOWAIT");
            assertEquals("1 2 T E[55P03] could not obtain lock on row in relation \"dpr_l2\""
                    + " Z[I] <waiting>", frames(c));
            execOn(writer, "ROLLBACK");
        }
        assertEquals("1|a", rows("SELECT i, s FROM dpr_l2"));
        exec("DROP TABLE dpr_l2");
    }

    @Test
    void describingAPortalUnderALockIsRefusedNothingBecauseItRunsNothing() throws Exception {
        lockedRow("dpr_l3");
        try (Connection writer = openSession(); RawWireClient c = rawSession()) {
            execOn(writer, "BEGIN");
            execOn(writer, "UPDATE dpr_l3 SET s = 'w' WHERE i = 1");
            queryOn(c, "SET lock_timeout = '400ms'");
            // A client that describes a portal and does not run it hears nothing of the lock: the
            // shape of the answer is settled without the statement being carried out.
            c.write(RawWireClient.parse("SELECT * FROM dpr_l3 WHERE i = 1 FOR UPDATE"));
            c.write(RawWireClient.bind());
            c.write(RawWireClient.describePortal());
            c.write(RawWireClient.sync());
            assertEquals("1 2 T Z[I] <waiting>", frames(c));
            execOn(writer, "ROLLBACK");
        }
        assertEquals("1|a", rows("SELECT i, s FROM dpr_l3"));
        exec("DROP TABLE dpr_l3");
    }

    @Test
    void aMergeRefusedTheRowThatMovedSaysSoAfterTheShapeOfItsAnswer() throws Exception {
        movingRow("dpr_m3");
        try (Connection writer = openSession(); RawWireClient c = rawSession()) {
            execOn(writer, "BEGIN");
            execOn(writer, "UPDATE dpr_m3 SET i = 12 WHERE i = 2");
            askOverExtended(c, "MERGE INTO dpr_m3 t USING (SELECT 2 AS i) u ON t.i = u.i"
                    + " WHEN MATCHED THEN UPDATE SET s = 'm'");
            Thread.sleep(BLOCKED_MS);
            execOn(writer, "COMMIT");
            // A MERGE answers no rows, so what stands in front of the refusal is NoData.
            assertEquals("1 2 n E[40001] tuple to be locked was already moved to another partition"
                    + " due to concurrent update Z[I] <waiting>", frames(c));
        }
        assertEquals("12|a", rows("SELECT i, s FROM dpr_m3"));
        exec("DROP TABLE dpr_m3 CASCADE");
    }

    // ============================================================ a described statement runs once

    @Test
    void theStatementADescribedPortalStandsForIsCarriedOutExactlyOnce() throws Exception {
        exec("CREATE TABLE dpr_o1 (i int PRIMARY KEY, s text)");
        exec("CREATE SEQUENCE dpr_o1seq");
        exec("INSERT INTO dpr_o1 VALUES (1,'a')");
        try (RawWireClient c = rawSession()) {
            // A write with nothing to return: one row written, and one CommandComplete for it.
            askOverExtended(c, "INSERT INTO dpr_o1 VALUES (9,'z')");
            assertEquals("1 2 n C[INSERT 0 1] Z[I] <waiting>", frames(c));
            assertEquals("1|a,9|z", rows("SELECT i, s FROM dpr_o1 ORDER BY i"));

            // A write whose effect would read differently had it been carried out twice.
            askOverExtended(c, "UPDATE dpr_o1 SET s = s || '!' WHERE i = 1");
            assertEquals("1 2 n C[UPDATE 1] Z[I] <waiting>", frames(c));
            assertEquals("a!", scalar("SELECT s FROM dpr_o1 WHERE i = 1"));

            // And a read that leaves a mark of its own behind.
            askOverExtended(c, "SELECT nextval('dpr_o1seq')");
            assertEquals("1 2 T D C[SELECT 1] Z[I] <waiting>", frames(c));
            assertEquals(1, num("SELECT last_value FROM dpr_o1seq"));

            askOverExtended(c, "DELETE FROM dpr_o1 WHERE i = 9 RETURNING i");
            assertEquals("1 2 T D C[DELETE 1] Z[I] <waiting>", frames(c));
            assertEquals("1|a!", rows("SELECT i, s FROM dpr_o1 ORDER BY i"));
        }
        exec("DROP TABLE dpr_o1");
        exec("DROP SEQUENCE dpr_o1seq");
    }

    @Test
    void aRefusalTheStatementRanIntoIsReportedOnceAndLeavesTheRelationAsItWas() throws Exception {
        lockedRow("dpr_o2");
        try (RawWireClient c = rawSession()) {
            // The RETURNING list names a column the relation has not. Whether that is reported
            // while the statement is read or while it is run is a difference of its own; what it
            // may not be is reported twice, or swallowed and the write carried out anyway.
            askOverExtended(c, "INSERT INTO dpr_o2 VALUES (9,'z') RETURNING nosuchcol");
            String said = frames(c);
            assertEquals(1, occurrences(said, "E[42703] column \"nosuchcol\" does not exist"),
                    said);
            assertTrue(said.endsWith("Z[I] <waiting>"), said);
            assertEquals("1|a", rows("SELECT i, s FROM dpr_o2"));

            // The same for a RETURNING expression that cannot be worked out.
            askOverExtended(c, "INSERT INTO dpr_o2 VALUES (9,'z') RETURNING i / 0");
            said = frames(c);
            assertEquals(1, occurrences(said, "E[22012] division by zero"), said);
            assertTrue(said.endsWith("Z[I] <waiting>"), said);
            assertEquals("1|a", rows("SELECT i, s FROM dpr_o2"));
        }
        exec("DROP TABLE dpr_o2");
    }

    @Test
    void aWriteRefusedForTheRowItWroteIsRefusedAfterTheShapeOfItsAnswer() throws Exception {
        lockedRow("dpr_o3");
        try (RawWireClient c = rawSession()) {
            String duplicate = "E[23505] duplicate key value violates unique constraint"
                    + " \"dpr_o3_pkey\"";
            askOverExtended(c, "INSERT INTO dpr_o3 VALUES (1,'b')");
            assertEquals("1 2 n " + duplicate + " Z[I] <waiting>", frames(c));
            askOverExtended(c, "INSERT INTO dpr_o3 VALUES (1,'b') RETURNING i");
            assertEquals("1 2 T " + duplicate + " Z[I] <waiting>", frames(c));
            assertEquals("1|a", rows("SELECT i, s FROM dpr_o3"));
        }
        exec("DROP TABLE dpr_o3");
    }
}
