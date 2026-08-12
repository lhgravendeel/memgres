package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.io.StringReader;
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
        // of it. PostgreSQL carries the failing row in the error's DETAIL and memgres sends none
        // for a COPY, so it is the state and the message that are read back here.
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
}
