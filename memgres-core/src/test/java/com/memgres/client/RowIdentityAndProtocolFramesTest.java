package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.memgres.client.RawWireClient.bind;
import static com.memgres.client.RawWireClient.concat;
import static com.memgres.client.RawWireClient.cstring;
import static com.memgres.client.RawWireClient.execute;
import static com.memgres.client.RawWireClient.frame;
import static com.memgres.client.RawWireClient.int16;
import static com.memgres.client.RawWireClient.int32;
import static com.memgres.client.RawWireClient.parse;
import static com.memgres.client.RawWireClient.query;
import static com.memgres.client.RawWireClient.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A row is a row of the relation that stores it, and a statement is settled while it is read.
 *
 * <p>Where a row lives and which relation it belongs to are properties of the relation that stores
 * it, not of the one it was reached through: ctid and tableoid are answered from the partition or
 * the inheritance child however the row was reached, a write qualified on either reaches every
 * relation the parent stands for, and an UPDATE that changes the partition key is a delete and an
 * insert rather than a rewrite where the row stands — so an abort leaves the row where it came
 * from, another session goes on reading it there, and a statement that waited for the lock is told
 * the tuple moved.
 *
 * <p>PostgreSQL also settles a statement while it reads it, so a column no relation in the query
 * has is answered for at Parse — before the client has been told the statement was good — and a
 * Bind whose result format list does not fit the statement's width, or whose parameter value will
 * not read as its type, is refused at Bind, before anything runs. The frame is under test in the
 * second half, so the conversation is driven message by message: only a raw client can see which
 * message an answer arrives instead of.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class RowIdentityAndProtocolFramesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE pbf_t (i int, j text)");
        exec("INSERT INTO pbf_t VALUES (1, 'a')");
        exec("CREATE TABLE pbf_u (i int, k text)");
        exec("INSERT INTO pbf_u VALUES (1, 'b')");
        exec("CREATE VIEW pbf_v AS SELECT i, j FROM pbf_t");
        exec("CREATE VIEW pbf_v2 (a, b) AS SELECT i, j FROM pbf_t");
        exec("CREATE MATERIALIZED VIEW pbf_m AS SELECT i, j FROM pbf_t");
        exec("CREATE TYPE pbf_c AS (a int, b text)");
        exec("CREATE TABLE pbf_ca (cs pbf_c[])");
        exec("INSERT INTO pbf_ca VALUES (ARRAY[(1,'x')::pbf_c])");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ------------------------------------------------------------ helpers

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
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

    /** The number of rows a write reports having touched. */
    private static int update(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            return st.executeUpdate(sql);
        }
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
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /**
     * The primary message of the error a statement raises. PostgreSQL sends severity in its own
     * field, so the message on the wire never carries an "ERROR: " prefix.
     */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    /** The hint the error carries, which PostgreSQL sends in a field of its own. */
    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    /** Every value of the first column, in order, joined with a comma. */
    private static String column(String sql) throws SQLException {
        List<String> got = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) got.add(rs.getString(1));
        }
        return String.join(",", got);
    }

    /** A second session of its own, so a transaction is never left on the shared connection. */
    private static Connection session() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    /** Every row of a query as "col|col|col", nulls as NULL. */
    private static List<String> rows(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    String v = rs.getString(i);
                    sb.append(v == null ? "NULL" : v);
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static List<String> rows(String sql) throws SQLException {
        return rows(conn, sql);
    }

    /** The place, the relation and the values of every row, ordered by relation and key. */
    private static List<String> places(Connection c, String relation) throws SQLException {
        return rows(c, "SELECT ctid::text, tableoid::regclass::text, i, s FROM " + relation
                + " ORDER BY tableoid::regclass::text, i");
    }

    private static void run(Connection c, String... sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            for (String one : sql) s.execute(one);
        }
    }

    private static RawWireClient open() throws IOException {
        RawWireClient client = new RawWireClient(memgres.getPort());
        client.startup(memgres.getUser(), "memgres");
        return client;
    }

    /** Everything the server says up to the ReadyForQuery that ends one round. */
    private static String said(RawWireClient client) throws IOException {
        StringBuilder out = new StringBuilder();
        while (true) {
            RawWireClient.Msg m = client.read();
            if (m == null) return out.append(out.length() > 0 ? " " : "").append("<closed>")
                    .toString();
            if (out.length() > 0) out.append(' ');
            out.append(m);
            if (m.type == 'Z') return out.toString();
        }
    }

    /** One extended-query cycle over the unnamed statement, and everything the server said. */
    private static String cycle(String sql) throws IOException {
        try (RawWireClient client = open()) {
            client.write(concat(parse(sql), bind(), execute(), sync()));
            return said(client);
        }
    }

    /** The same, with a Bind carrying {@code formats} result format codes, all of them text. */
    private static String withFormats(String sql, int formats) throws IOException {
        byte[] codes = new byte[0];
        for (int i = 0; i < formats; i++) codes = concat(codes, int16(0));
        byte[] message = frame('B', concat(cstring(""), cstring(""), int16(0), int16(0),
                int16(formats), codes));
        try (RawWireClient client = open()) {
            client.write(concat(parse(sql), message, execute(), sync()));
            return said(client);
        }
    }

    /** Parse the unnamed statement with the parameter types the client declares for it. */
    private static byte[] parseTyped(String sql, int... oids) {
        byte[] types = int16(oids.length);
        for (int oid : oids) types = concat(types, int32(oid));
        return frame('P', concat(cstring(""), cstring(sql), types));
    }

    /** Bind the unnamed portal with one text value, or with no value at all when it is null. */
    private static byte[] bindText(String value) {
        byte[] body = concat(cstring(""), cstring(""), int16(0), int16(1));
        body = value == null ? concat(body, int32(-1))
                : concat(body, int32(value.getBytes(StandardCharsets.UTF_8).length),
                        value.getBytes(StandardCharsets.UTF_8));
        return frame('B', concat(body, int16(0)));
    }

    /** The same with one binary value. */
    private static byte[] bindBinary(byte[] value) {
        return frame('B', concat(cstring(""), cstring(""), int16(1), int16(1), int16(1),
                int32(value.length), value, int16(0)));
    }

    /** Parse with a declared type, bind one text value, execute. */
    private static String bound(String sql, int oid, String value) throws IOException {
        try (RawWireClient client = open()) {
            client.write(concat(oid == 0 ? parse(sql) : parseTyped(sql, oid), bindText(value),
                    execute(), sync()));
            return said(client);
        }
    }

    /** The same with a binary value. */
    private static String binary(String sql, int oid, byte[] value) throws IOException {
        try (RawWireClient client = open()) {
            client.write(concat(parseTyped(sql, oid), bindBinary(value), execute(), sync()));
            return said(client);
        }
    }

    /** The first value of the first row a binary parameter is answered with. */
    private static String firstValue(String sql, int oid, byte[] value) throws IOException {
        try (RawWireClient client = open()) {
            client.write(concat(parseTyped(sql, oid), bindBinary(value), execute(), sync()));
            while (true) {
                RawWireClient.Msg m = client.read();
                if (m == null) return "<closed>";
                if (m.type == 'D') {
                    int length = ((m.body[2] & 0xFF) << 24) | ((m.body[3] & 0xFF) << 16)
                            | ((m.body[4] & 0xFF) << 8) | (m.body[5] & 0xFF);
                    return new String(m.body, 6, length, StandardCharsets.UTF_8);
                }
                if (m.type == 'E') return m.toString();
                if (m.type == 'Z') return "<no row>";
            }
        }
    }

    // ============================================================ A row read through a parent
    // keeps its own place and its own name

    @Test
    void ctidAndTableoidThroughAPartitionedParentNameThePartition() throws SQLException {
        run(conn, "CREATE TABLE rrp_p (i int, s text) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_p0 PARTITION OF rrp_p FOR VALUES FROM (0) TO (10)",
                "CREATE TABLE rrp_p1 PARTITION OF rrp_p FOR VALUES FROM (10) TO (20)",
                "INSERT INTO rrp_p VALUES (1,'a'),(2,'b'),(11,'c'),(12,'d')");
        // Each partition numbers its own tuples from one.
        assertEquals(List.of("(0,1)|rrp_p0|1", "(0,2)|rrp_p0|2",
                        "(0,1)|rrp_p1|11", "(0,2)|rrp_p1|12"),
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_p ORDER BY i"));
        assertEquals(2, num("SELECT count(DISTINCT ctid) FROM rrp_p"));
        assertEquals("2,12", column("SELECT i FROM rrp_p WHERE ctid = '(0,2)' ORDER BY i"));
        assertEquals(List.of("t|t|t"),
                rows("SELECT bool_and(xmin::text <> '0'), bool_and(xmax::text = '0'),"
                        + " bool_and(cmin::text = '0') FROM rrp_p"));
        // A partitioned table stores nothing of its own.
        assertEquals(0, num("SELECT count(*) FROM ONLY rrp_p"));
        run(conn, "DROP TABLE rrp_p");
    }

    @Test
    void ctidAndTableoidThroughAnInheritanceParentNameTheChild() throws SQLException {
        run(conn, "CREATE TABLE rrp_h (i int, s text)",
                "CREATE TABLE rrp_h0 (x int) INHERITS (rrp_h)",
                "INSERT INTO rrp_h VALUES (1,'a'),(2,'b')",
                "INSERT INTO rrp_h0 VALUES (3,'c',30),(4,'d',40)");
        assertEquals(List.of("(0,1)|rrp_h|1", "(0,2)|rrp_h|2",
                        "(0,1)|rrp_h0|3", "(0,2)|rrp_h0|4"),
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_h ORDER BY i"));
        assertEquals(2, num("SELECT count(DISTINCT ctid) FROM rrp_h"));
        assertEquals("1,3", column("SELECT i FROM rrp_h WHERE ctid = '(0,1)' ORDER BY i"));
        // ONLY reads what the parent stores itself.
        assertEquals(List.of("(0,1)|rrp_h|1", "(0,2)|rrp_h|2"),
                rows("SELECT ctid, tableoid::regclass::text, i FROM ONLY rrp_h ORDER BY i"));
        run(conn, "DROP TABLE rrp_h CASCADE");
    }

    @Test
    void aSubPartitionAndAnInheritanceChainAreNamedAtEveryLevel() throws SQLException {
        run(conn, "CREATE TABLE rrp_a (i int, s text) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_a0 PARTITION OF rrp_a FOR VALUES FROM (0) TO (100)"
                        + " PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_a00 PARTITION OF rrp_a0 FOR VALUES FROM (0) TO (10)",
                "CREATE TABLE rrp_a01 PARTITION OF rrp_a0 FOR VALUES FROM (10) TO (100)",
                "INSERT INTO rrp_a VALUES (1,'a'),(2,'b'),(11,'c')");
        List<String> leaves = List.of("(0,1)|rrp_a00|1", "(0,2)|rrp_a00|2", "(0,1)|rrp_a01|11");
        assertEquals(leaves,
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_a ORDER BY i"));
        // The same rows read at the level between them.
        assertEquals(leaves,
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_a0 ORDER BY i"));
        run(conn, "DROP TABLE rrp_a");

        run(conn, "CREATE TABLE rrp_b (i int, s text)",
                "CREATE TABLE rrp_b0 (x int) INHERITS (rrp_b)",
                "CREATE TABLE rrp_b00 (y int) INHERITS (rrp_b0)",
                "INSERT INTO rrp_b VALUES (1,'a')",
                "INSERT INTO rrp_b0 VALUES (2,'b',20)",
                "INSERT INTO rrp_b00 VALUES (3,'c',30,300)");
        assertEquals(List.of("(0,1)|rrp_b|1", "(0,1)|rrp_b0|2", "(0,1)|rrp_b00|3"),
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_b ORDER BY i"));
        assertEquals(List.of("(0,1)|rrp_b0|2", "(0,1)|rrp_b00|3"),
                rows("SELECT ctid, tableoid::regclass::text, i FROM rrp_b0 ORDER BY i"));
        run(conn, "DROP TABLE rrp_b CASCADE");
    }

    @Test
    void aPartitionWithItsOwnColumnOrderKeepsItsRowsPlaces() throws SQLException {
        run(conn, "CREATE TABLE rrp_c (i int, s text) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_c0 (s text, i int)",
                "INSERT INTO rrp_c0 VALUES ('x',1),('y',2)",
                "ALTER TABLE rrp_c ATTACH PARTITION rrp_c0 FOR VALUES FROM (0) TO (10)");
        assertEquals(List.of("(0,1)|rrp_c0|1|x", "(0,2)|rrp_c0|2|y"),
                rows("SELECT ctid, tableoid::regclass::text, i, s FROM rrp_c ORDER BY i"));
        run(conn, "DROP TABLE rrp_c");
    }

    @Test
    void aDerivedTableOverAParentCarriesTheRowsOwnPlace() throws SQLException {
        run(conn, "CREATE TABLE rrp_dp (i int) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_dp0 PARTITION OF rrp_dp FOR VALUES FROM (0) TO (10)",
                "CREATE TABLE rrp_dp1 PARTITION OF rrp_dp FOR VALUES FROM (10) TO (20)",
                "INSERT INTO rrp_dp VALUES (1),(2),(11)");
        // The derived relation hands on the place its rows came with, rather than numbering them
        // again from where it put them.
        assertEquals(List.of("(0,1)", "(0,2)", "(0,1)"),
                rows("SELECT x.ctid::text FROM (SELECT ctid, i FROM rrp_dp) x ORDER BY i"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,1)"),
                rows("SELECT ctid::text FROM rrp_dp ORDER BY i"));
        run(conn, "DROP TABLE rrp_dp");
    }

    // ============================================================ Writing through a parent

    @Test
    void anUpdateThroughAParentMovesTheRowInTheRelationThatStoresIt() throws SQLException {
        run(conn, "CREATE TABLE rrp_k (i int, s text)",
                "CREATE TABLE rrp_k0 () INHERITS (rrp_k)",
                "INSERT INTO rrp_k VALUES (1,'a')",
                "INSERT INTO rrp_k0 VALUES (2,'b'),(3,'c')",
                "UPDATE rrp_k SET s = 'z' WHERE i = 2");
        // The new version is written onto the child's page, so it takes the child's next place.
        assertEquals(List.of("(0,3)|2|z", "(0,2)|3|c"),
                rows("SELECT ctid, i, s FROM rrp_k0 ORDER BY i"));
        assertEquals(List.of("(0,1)|rrp_k|1|a", "(0,3)|rrp_k0|2|z", "(0,2)|rrp_k0|3|c"),
                rows("SELECT ctid, tableoid::regclass::text, i, s FROM rrp_k ORDER BY i"));
        // The parent's own numbering is untouched by a write to a row it does not store.
        assertEquals(List.of("(0,1)|1|a"), rows("SELECT ctid, i, s FROM ONLY rrp_k ORDER BY i"));
        run(conn, "DROP TABLE rrp_k CASCADE");
    }

    @Test
    void anUpdateThroughAPartitionedParentMovesTheRowInItsPartition() throws SQLException {
        run(conn, "CREATE TABLE rrp_m (i int, s text) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_m0 PARTITION OF rrp_m FOR VALUES FROM (0) TO (10)",
                "CREATE TABLE rrp_m1 PARTITION OF rrp_m FOR VALUES FROM (10) TO (20)",
                "INSERT INTO rrp_m VALUES (1,'a'),(11,'c')");
        assertEquals(1, update(conn, "UPDATE rrp_m SET s = 'Q' WHERE tableoid = 'rrp_m1'::regclass"));
        assertEquals(List.of("(0,1)|rrp_m0|1|a", "(0,2)|rrp_m1|11|Q"),
                rows("SELECT ctid, tableoid::regclass::text, i, s FROM rrp_m ORDER BY i"));
        assertEquals(List.of("(0,2)|11|Q"), rows("SELECT ctid, i, s FROM rrp_m1 ORDER BY i"));
        run(conn, "DROP TABLE rrp_m");
    }

    @Test
    void aWriteQualifiedOnCtidReachesEveryPartition() throws SQLException {
        run(conn, "CREATE TABLE rrp_g (i int, s text) PARTITION BY RANGE (i)",
                "CREATE TABLE rrp_g0 PARTITION OF rrp_g FOR VALUES FROM (0) TO (10)",
                "CREATE TABLE rrp_g1 PARTITION OF rrp_g FOR VALUES FROM (10) TO (20)",
                "INSERT INTO rrp_g VALUES (1,'a'),(2,'b'),(11,'c'),(12,'d')");
        // One tuple per partition answers to (0,2), so two rows are deleted.
        assertEquals(2, update(conn, "DELETE FROM rrp_g WHERE ctid = '(0,2)'"));
        assertEquals(List.of("1|a", "11|c"), rows("SELECT i, s FROM rrp_g ORDER BY i"));
        assertEquals(2, update(conn, "UPDATE rrp_g SET s = 'Y' WHERE ctid = '(0,1)'"));
        assertEquals(List.of("1|Y", "11|Y"), rows("SELECT i, s FROM rrp_g ORDER BY i"));
        run(conn, "DROP TABLE rrp_g");
    }

    @Test
    void aWriteQualifiedOnTableoidReachesTheRelationItNames() throws SQLException {
        run(conn, "CREATE TABLE rrp_r (i int, s text)",
                "CREATE TABLE rrp_r0 () INHERITS (rrp_r)",
                "INSERT INTO rrp_r VALUES (1,'a')",
                "INSERT INTO rrp_r0 VALUES (2,'b'),(3,'c')");
        assertEquals(2, update(conn,
                "UPDATE rrp_r SET s = 'q' WHERE tableoid = 'rrp_r0'::regclass"));
        assertEquals(List.of("1|a", "2|q", "3|q"), rows("SELECT i, s FROM rrp_r ORDER BY i"));
        assertEquals(2, update(conn, "DELETE FROM rrp_r WHERE tableoid = 'rrp_r0'::regclass"));
        assertEquals(List.of("1|a"), rows("SELECT i, s FROM rrp_r ORDER BY i"));
        run(conn, "DROP TABLE rrp_r CASCADE");
    }

    // ============================================================ Reading through a parent's
    // snapshot

    @Test
    void aRepeatableReadSnapshotOfAParentKeepsThePlacesAndTheNames() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rrp_v (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE rrp_v0 PARTITION OF rrp_v FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE rrp_v1 PARTITION OF rrp_v FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO rrp_v VALUES (1,'a'),(2,'b'),(11,'c')",
                    "CREATE TABLE rrp_w (i int, s text)",
                    "CREATE TABLE rrp_w0 (x int) INHERITS (rrp_w)",
                    "INSERT INTO rrp_w VALUES (1,'a')",
                    "INSERT INTO rrp_w0 VALUES (2,'b',20),(3,'c',30)",
                    "BEGIN ISOLATION LEVEL REPEATABLE READ");
            assertEquals(List.of("(0,1)|rrp_v0|1", "(0,2)|rrp_v0|2", "(0,1)|rrp_v1|11"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_v ORDER BY i"));
            assertEquals(List.of("2"), rows(c, "SELECT count(DISTINCT ctid) FROM rrp_v"));
            assertEquals(List.of("2"), rows(c, "SELECT i FROM rrp_v WHERE ctid = '(0,2)'"));
            assertEquals(List.of("t|t|t"),
                    rows(c, "SELECT bool_and(xmin::text <> '0'), bool_and(xmax::text = '0'),"
                            + " bool_and(cmin::text = '0') FROM rrp_v"));
            assertEquals(List.of("(0,1)|rrp_w|1", "(0,1)|rrp_w0|2", "(0,2)|rrp_w0|3"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_w ORDER BY i"));
            assertEquals(List.of("(0,1)|rrp_w|1"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM ONLY rrp_w ORDER BY i"));

            // What this transaction writes itself it reads at the place its own relation gave it.
            run(c, "INSERT INTO rrp_v VALUES (5,'e')", "INSERT INTO rrp_w0 VALUES (7,'g',70)");
            assertEquals(List.of("(0,1)|rrp_v0|1", "(0,2)|rrp_v0|2", "(0,3)|rrp_v0|5",
                            "(0,1)|rrp_v1|11"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_v ORDER BY i"));
            assertEquals(List.of("(0,1)|rrp_w|1", "(0,1)|rrp_w0|2", "(0,2)|rrp_w0|3",
                            "(0,3)|rrp_w0|7"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_w ORDER BY i"));
            run(c, "UPDATE rrp_v SET s = 'Z' WHERE i = 1");
            assertEquals(List.of("(0,4)|rrp_v0|1|Z", "(0,2)|rrp_v0|2|b", "(0,3)|rrp_v0|5|e",
                            "(0,1)|rrp_v1|11|c"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i, s FROM rrp_v ORDER BY i"));
            run(c, "DELETE FROM rrp_w WHERE i = 3");
            assertEquals(List.of("(0,1)|rrp_w|1", "(0,1)|rrp_w0|2", "(0,3)|rrp_w0|7"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_w ORDER BY i"));
            run(c, "COMMIT");
            assertEquals(List.of("(0,4)|rrp_v0|1|Z", "(0,2)|rrp_v0|2|b", "(0,3)|rrp_v0|5|e",
                            "(0,1)|rrp_v1|11|c"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i, s FROM rrp_v ORDER BY i"));
            run(c, "DROP TABLE rrp_v", "DROP TABLE rrp_w CASCADE");
        }
    }

    @Test
    void aRelationMadeInsideTheTransactionIsReadTheSameWay() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT 1",
                    "CREATE TABLE rrp_t (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE rrp_t0 PARTITION OF rrp_t FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE rrp_t1 PARTITION OF rrp_t FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO rrp_t VALUES (1,'a'),(2,'b'),(11,'c')");
            assertEquals(List.of("(0,1)|rrp_t0|1", "(0,2)|rrp_t0|2", "(0,1)|rrp_t1|11"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_t ORDER BY i"));
            run(c, "CREATE TABLE rrp_u (i int, s text)",
                    "CREATE TABLE rrp_u0 (x int) INHERITS (rrp_u)",
                    "INSERT INTO rrp_u VALUES (1,'a')",
                    "INSERT INTO rrp_u0 VALUES (2,'b',20)");
            assertEquals(List.of("(0,1)|rrp_u|1", "(0,1)|rrp_u0|2"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_u ORDER BY i"));
            assertEquals(List.of("t|t"),
                    rows(c, "SELECT bool_and(xmin::text <> '0'), bool_and(xmax::text = '0')"
                            + " FROM rrp_t"));
            run(c, "COMMIT");
            assertEquals(List.of("(0,1)|rrp_t0|1", "(0,2)|rrp_t0|2", "(0,1)|rrp_t1|11"),
                    rows(c, "SELECT ctid, tableoid::regclass::text, i FROM rrp_t ORDER BY i"));
            run(c, "DROP TABLE rrp_t", "DROP TABLE rrp_u CASCADE");
        }
    }

    @Test
    void aCommittedUpdateByAnotherSessionDoesNotMoveWhatTheSnapshotShows() throws SQLException {
        try (Connection reader = session(); Connection writer = session()) {
            run(reader, "CREATE TABLE rrp_d (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE rrp_d0 PARTITION OF rrp_d FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE rrp_d1 PARTITION OF rrp_d FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO rrp_d VALUES (1,'a'),(2,'b'),(11,'c')",
                    "BEGIN ISOLATION LEVEL REPEATABLE READ");
            List<String> before = places(reader, "rrp_d");
            assertEquals(List.of("(0,1)|rrp_d0|1|a", "(0,2)|rrp_d0|2|b", "(0,1)|rrp_d1|11|c"),
                    before);
            run(writer, "UPDATE rrp_d SET s = 'Z' WHERE i = 1");
            // The reader is entitled to the version it started with, at the place it has.
            assertEquals(before, places(reader, "rrp_d"));
            run(reader, "COMMIT");
            // Once the transaction is over the committed write shows, at its new place.
            assertEquals(List.of("(0,3)|rrp_d0|1|Z", "(0,2)|rrp_d0|2|b", "(0,1)|rrp_d1|11|c"),
                    places(reader, "rrp_d"));
            run(reader, "DROP TABLE rrp_d");
        }
    }

    @Test
    void aRowAnotherSessionHasDeletedWithoutCommittingKeepsItsPlace() throws SQLException {
        try (Connection reader = session(); Connection writer = session()) {
            run(reader, "CREATE TABLE rrp_e (i int, s text)",
                    "CREATE TABLE rrp_e0 (x int) INHERITS (rrp_e)",
                    "INSERT INTO rrp_e VALUES (1,'a')",
                    "INSERT INTO rrp_e0 VALUES (2,'b',20)");
            run(writer, "BEGIN", "DELETE FROM rrp_e WHERE i = 2");
            run(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            // The delete is not committed, so the row is still one of the child's rows.
            assertEquals(List.of("(0,1)|rrp_e|1|a", "(0,1)|rrp_e0|2|b"), places(reader, "rrp_e"));
            run(reader, "COMMIT");
            run(writer, "ROLLBACK");
            run(reader, "DROP TABLE rrp_e CASCADE");
        }
    }

    @Test
    void anUncommittedInsertIntoAPartitionIsNotShownThroughTheParent() throws SQLException {
        try (Connection reader = session(); Connection writer = session()) {
            run(reader, "CREATE TABLE rrp_f (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE rrp_f0 PARTITION OF rrp_f FOR VALUES FROM (0) TO (10)",
                    "INSERT INTO rrp_f VALUES (1,'a')");
            run(writer, "BEGIN", "INSERT INTO rrp_f VALUES (2,'b')");
            run(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            assertEquals(List.of("(0,1)|rrp_f0|1|a"), places(reader, "rrp_f"));
            run(reader, "COMMIT");
            run(writer, "ROLLBACK");
            run(reader, "DROP TABLE rrp_f");
        }
    }

    // ============================================================ A partition-key change moves
    // the row

    @Test
    void anUpdateThatChangesThePartitionKeyWritesTheRowIntoItsNewPartition() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_a (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_a0 PARTITION OF pkm_a FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_a1 PARTITION OF pkm_a FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_a VALUES (1,'a'),(2,'b'),(11,'c')");
            assertEquals(List.of("12|b"), rows(c, "UPDATE pkm_a SET i = 12 WHERE i = 2"
                            + " RETURNING i, s"),
                    "RETURNING reports the row the statement wrote, not the one it replaced");
            assertEquals(List.of("(0,1)|pkm_a0|1|a", "(0,1)|pkm_a1|11|c", "(0,2)|pkm_a1|12|b"),
                    places(c, "pkm_a"));
            run(c, "DROP TABLE pkm_a");
        }
    }

    @Test
    void anAbortedMoveLeavesTheRowInThePartitionItCameFrom() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_b (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_b0 PARTITION OF pkm_b FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_b1 PARTITION OF pkm_b FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_b VALUES (1,'a'),(2,'b'),(11,'c')",
                    "BEGIN", "UPDATE pkm_b SET i = 12 WHERE i = 2");
            assertEquals(List.of("(0,1)|pkm_b0|1|a", "(0,1)|pkm_b1|11|c", "(0,2)|pkm_b1|12|b"),
                    places(c, "pkm_b"));
            run(c, "ROLLBACK");
            assertEquals(List.of("(0,1)|pkm_b0|1|a", "(0,2)|pkm_b0|2|b", "(0,1)|pkm_b1|11|c"),
                    places(c, "pkm_b"));
            // The place the aborted move took in the destination is not handed out again.
            run(c, "UPDATE pkm_b SET i = 12 WHERE i = 2");
            assertEquals(List.of("(0,1)|pkm_b0|1|a", "(0,1)|pkm_b1|11|c", "(0,3)|pkm_b1|12|b"),
                    places(c, "pkm_b"));
            run(c, "DROP TABLE pkm_b");
        }
    }

    @Test
    void aSavepointUndoesAMoveTheSameWay() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_c (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_c0 PARTITION OF pkm_c FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_c1 PARTITION OF pkm_c FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_c VALUES (2,'b')",
                    "BEGIN", "SAVEPOINT sp", "UPDATE pkm_c SET i = 12 WHERE i = 2");
            assertEquals(List.of("(0,1)|pkm_c1|12|b"), places(c, "pkm_c"));
            run(c, "ROLLBACK TO SAVEPOINT sp");
            assertEquals(List.of("(0,1)|pkm_c0|2|b"), places(c, "pkm_c"));
            run(c, "COMMIT");
            assertEquals(List.of("(0,1)|pkm_c0|2|b"), places(c, "pkm_c"));
            run(c, "DROP TABLE pkm_c");
        }
    }

    @Test
    void updateFromAndAMergeArmMoveARowTheSameWay() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_d (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_d0 PARTITION OF pkm_d FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_d1 PARTITION OF pkm_d FOR VALUES FROM (10) TO (20)",
                    "CREATE TABLE pkm_ds (k int, nv int)",
                    "INSERT INTO pkm_d VALUES (1,'a'),(2,'b')",
                    "INSERT INTO pkm_ds VALUES (2,15)");
            assertEquals(List.of("15|b"),
                    rows(c, "UPDATE pkm_d SET i = u.nv FROM pkm_ds u WHERE u.k = pkm_d.i"
                            + " RETURNING pkm_d.i, pkm_d.s"));
            assertEquals(List.of("(0,1)|pkm_d0|1|a", "(0,1)|pkm_d1|15|b"), places(c, "pkm_d"));
            // The arm reports the place and the relation the row it wrote now lives at.
            assertEquals(List.of("17|a|(0,2)|pkm_d1"),
                    rows(c, "MERGE INTO pkm_d t USING (SELECT 1 AS k) q ON t.i = q.k"
                            + " WHEN MATCHED THEN UPDATE SET i = 17"
                            + " RETURNING t.i, t.s, t.ctid::text, t.tableoid::regclass::text"));
            assertEquals(List.of("(0,1)|pkm_d1|15|b", "(0,2)|pkm_d1|17|a"), places(c, "pkm_d"));
            run(c, "DROP TABLE pkm_d", "DROP TABLE pkm_ds");
        }
    }

    @Test
    void aMoveBetweenSubPartitionsGoesToTheLeafTheValuesRouteTo() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_e (i int, j int) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_e0 PARTITION OF pkm_e FOR VALUES FROM (0) TO (10)"
                            + " PARTITION BY RANGE (j)",
                    "CREATE TABLE pkm_e00 PARTITION OF pkm_e0 FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_e01 PARTITION OF pkm_e0 FOR VALUES FROM (10) TO (20)",
                    "CREATE TABLE pkm_e1 PARTITION OF pkm_e FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_e VALUES (1,1),(11,5)",
                    "UPDATE pkm_e SET j = 15 WHERE i = 1");
            assertEquals(List.of("(0,1)|pkm_e01|1|15", "(0,1)|pkm_e1|11|5"),
                    rows(c, "SELECT ctid::text, tableoid::regclass::text, i, j FROM pkm_e"
                            + " ORDER BY tableoid::regclass::text, i"));
            run(c, "UPDATE pkm_e SET i = 15 WHERE i = 1");
            assertEquals(List.of("(0,1)|pkm_e1|11|5", "(0,2)|pkm_e1|15|15"),
                    rows(c, "SELECT ctid::text, tableoid::regclass::text, i, j FROM pkm_e"
                            + " ORDER BY tableoid::regclass::text, i"));
            run(c, "DROP TABLE pkm_e");
        }
    }

    // ============================================================ What another session sees of a
    // move

    @Test
    void anUncommittedMoveIsNotVisibleToAnotherSession() throws SQLException {
        try (Connection a = session(); Connection c = session()) {
            run(a, "CREATE TABLE pkm_f (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_f0 PARTITION OF pkm_f FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_f1 PARTITION OF pkm_f FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_f VALUES (1,'a'),(2,'b'),(11,'c')",
                    "BEGIN", "UPDATE pkm_f SET i = 12 WHERE i = 2");
            // The writer reads its own move.
            assertEquals(List.of("(0,1)|pkm_f0|1|a", "(0,1)|pkm_f1|11|c", "(0,2)|pkm_f1|12|b"),
                    places(a, "pkm_f"));

            List<String> old = List.of("(0,1)|pkm_f0|1|a", "(0,2)|pkm_f0|2|b",
                    "(0,1)|pkm_f1|11|c");
            assertEquals(old, places(c, "pkm_f"), "an uncommitted move is not a row to show");
            for (String isolation : List.of("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")) {
                run(c, "BEGIN ISOLATION LEVEL " + isolation);
                assertEquals(old, places(c, "pkm_f"), isolation);
                run(c, "COMMIT");
            }
            // Reading the two partitions themselves says the same thing.
            assertEquals(List.of("(0,1)|1|a", "(0,2)|2|b"),
                    rows(c, "SELECT ctid::text, i, s FROM pkm_f0 ORDER BY i"));
            assertEquals(List.of("(0,1)|11|c"),
                    rows(c, "SELECT ctid::text, i, s FROM pkm_f1 ORDER BY i"));

            run(a, "ROLLBACK");
            assertEquals(old, places(c, "pkm_f"));
            run(a, "BEGIN", "UPDATE pkm_f SET i = 12 WHERE i = 2", "COMMIT");
            assertEquals(List.of("(0,1)|pkm_f0|1|a", "(0,1)|pkm_f1|11|c", "(0,3)|pkm_f1|12|b"),
                    places(c, "pkm_f"));
            run(a, "DROP TABLE pkm_f");
        }
    }

    @Test
    void aStatementThatWaitedForACommittedMoveIsToldTheTupleMoved() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection a = session(); Connection c = session()) {
            run(a, "CREATE TABLE pkm_g (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_g0 PARTITION OF pkm_g FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_g1 PARTITION OF pkm_g FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_g VALUES (2,'b')",
                    "BEGIN", "UPDATE pkm_g SET i = 12 WHERE i = 2");

            Future<SQLException> blocked = pool.submit(new Callable<SQLException>() {
                @Override
                public SQLException call() {
                    try {
                        run(c, "UPDATE pkm_g SET s = 'q' WHERE i = 2");
                        return null;
                    } catch (SQLException e) {
                        return e;
                    }
                }
            });
            Thread.sleep(400);
            assertFalse(blocked.isDone(), "the writer holds the row the other statement wants");
            run(a, "COMMIT");
            SQLException raised = blocked.get(20, TimeUnit.SECONDS);
            assertNotNull(raised, "a row is not followed into another partition");
            assertEquals("40001", raised.getSQLState());
            assertTrue(raised.getMessage().contains("tuple to be locked was already moved to"
                    + " another partition due to concurrent update"), raised.getMessage());
            assertEquals(List.of("(0,1)|pkm_g1|12|b"), places(c, "pkm_g"));
            run(a, "DROP TABLE pkm_g");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void aStatementThatWaitedForAnAbortedMoveActsOnTheRowItWaitedFor() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection a = session(); Connection c = session()) {
            run(a, "CREATE TABLE pkm_h (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_h0 PARTITION OF pkm_h FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_h1 PARTITION OF pkm_h FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_h VALUES (2,'b')",
                    "BEGIN", "UPDATE pkm_h SET i = 12 WHERE i = 2");

            Future<Integer> blocked = pool.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws SQLException {
                    return update(c, "DELETE FROM pkm_h WHERE i = 2");
                }
            });
            Thread.sleep(400);
            assertFalse(blocked.isDone());
            run(a, "ROLLBACK");
            assertEquals(Integer.valueOf(1), blocked.get(20, TimeUnit.SECONDS));
            assertEquals(List.of(), places(c, "pkm_h"));
            run(a, "DROP TABLE pkm_h");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void forUpdateThroughAParentLocksAndReportsEveryRow() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE pkm_l (i int, s text) PARTITION BY RANGE (i)",
                    "CREATE TABLE pkm_l0 PARTITION OF pkm_l FOR VALUES FROM (0) TO (10)",
                    "CREATE TABLE pkm_l1 PARTITION OF pkm_l FOR VALUES FROM (10) TO (20)",
                    "INSERT INTO pkm_l VALUES (1,'a'),(2,'b'),(11,'c')",
                    "BEGIN");
            List<String> all = List.of("1|a", "2|b", "11|c");
            assertEquals(all, rows(c, "SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE"));
            assertEquals(List.of("2|b"), rows(c, "SELECT i, s FROM pkm_l WHERE i = 2 FOR UPDATE"));
            assertEquals(all, rows(c, "SELECT i, s FROM pkm_l ORDER BY i FOR SHARE"));
            assertEquals(all, rows(c, "SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE NOWAIT"));
            assertEquals(all, rows(c, "SELECT i, s FROM pkm_l ORDER BY i FOR UPDATE SKIP LOCKED"));
            // A partitioned table stores nothing of its own, so ONLY locks nothing.
            assertEquals(List.of(), rows(c, "SELECT i, s FROM ONLY pkm_l ORDER BY i FOR UPDATE"));
            run(c, "COMMIT", "DROP TABLE pkm_l");

            run(c, "CREATE TABLE pkm_m (i int, s text)",
                    "CREATE TABLE pkm_m0 (extra int) INHERITS (pkm_m)",
                    "INSERT INTO pkm_m VALUES (1,'a')",
                    "INSERT INTO pkm_m0 VALUES (2,'b',7)",
                    "BEGIN");
            assertEquals(List.of("1|a", "2|b"),
                    rows(c, "SELECT i, s FROM pkm_m ORDER BY i FOR UPDATE"));
            assertEquals(List.of("1|a"),
                    rows(c, "SELECT i, s FROM ONLY pkm_m ORDER BY i FOR UPDATE"));
            assertEquals(List.of("2|b"), rows(c, "SELECT i, s FROM pkm_m WHERE i = 2 FOR SHARE"));
            assertEquals(List.of("1|a", "2|b"),
                    rows(c, "SELECT i, s FROM pkm_m ORDER BY i FOR UPDATE SKIP LOCKED"));
            run(c, "COMMIT", "DROP TABLE pkm_m CASCADE");
        }
    }

    @Test
    void aRoutedInsertCannotReportTheTuplesOwnTransaction() throws SQLException {
        exec("CREATE TABLE pkm_r (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE pkm_r0 PARTITION OF pkm_r FOR VALUES FROM (0) TO (100)");
        // Where the row went and which relation took it are still answerable.
        assertEquals(List.of("1|(0,1)|pkm_r0"),
                rows("INSERT INTO pkm_r VALUES (1,'a')"
                        + " RETURNING i, ctid::text, tableoid::regclass::text"));
        for (String column : new String[]{"cmin", "cmax", "xmin", "xmax::text"}) {
            assertEquals("0A000", stateOf("INSERT INTO pkm_r VALUES (9,'z') RETURNING i, "
                    + column), column);
        }
        assertEquals("cannot retrieve a system column in this context",
                messageOf("INSERT INTO pkm_r VALUES (9,'z') RETURNING i, cmin"));
        // A MERGE that can insert is the same statement in another spelling.
        assertEquals("0A000", stateOf("MERGE INTO pkm_r t USING (SELECT 6 AS k) q ON t.i = q.k"
                + " WHEN NOT MATCHED THEN INSERT VALUES (q.k,'f') RETURNING t.i, t.cmin"));
        // Naming the partition asks nothing of the routing, and neither does a later write.
        assertEquals(List.of("7|0"), rows("INSERT INTO pkm_r0 VALUES (7,'g') RETURNING i, cmin"));
        assertEquals(List.of("7|0"), rows("UPDATE pkm_r SET s = 'z' WHERE i = 7"
                + " RETURNING i, cmin"));
        assertEquals(List.of("7|0"), rows("DELETE FROM pkm_r WHERE i = 7 RETURNING i, cmin"));
        assertEquals(List.of("1|a"), rows("SELECT i, s FROM pkm_r ORDER BY i"));
        exec("DROP TABLE pkm_r");
    }

    // ============================================================ A qualified name is settled at
    // Parse

    @Test
    void aQualifiedColumnARelationHasNotIsRefusedAtParse() throws Exception {
        assertEquals("E[42703] column t.nosuchcol does not exist Z[I]",
                cycle("SELECT t.nosuchcol FROM pbf_t t"));
        assertEquals("E[42703] column pbf_t.nosuchcol does not exist Z[I]",
                cycle("SELECT pbf_t.nosuchcol FROM pbf_t"));
        assertEquals("E[42703] column a.nosuchcol does not exist Z[I]",
                cycle("SELECT a.nosuchcol FROM pbf_t a JOIN pbf_t b ON a.i = b.i"));
        assertEquals("E[42703] column t.nosuchcol does not exist Z[I]",
                cycle("SELECT i FROM pbf_t t WHERE t.nosuchcol = 1"));
        assertEquals("E[42703] column t.nosuchcol does not exist Z[I]",
                cycle("SELECT i FROM pbf_t t ORDER BY t.nosuchcol"));
        assertEquals("E[42703] column t.nosuchcol does not exist Z[I]",
                cycle("SELECT count(*) FROM pbf_t t GROUP BY t.nosuchcol"));
    }

    @Test
    void aQualifierNoRelationAnswersToIsRefusedAtParse() throws Exception {
        assertEquals("E[42P01] missing FROM-clause entry for table \"x\" Z[I]",
                cycle("SELECT x.i FROM pbf_t t"));
        assertEquals("E[42P01] invalid reference to FROM-clause entry for table \"pbf_t\" Z[I]",
                cycle("SELECT pbf_t.i FROM pbf_t t"));
        assertEquals("E[42P01] missing FROM-clause entry for table \"nosuchrel\" Z[I]",
                cycle("SELECT nosuchrel.i FROM (pbf_t JOIN pbf_u USING (i)) AS j"));
    }

    @Test
    void aQualifierThatResolvesIsLeftAlone() throws Exception {
        String oneRow = "1 2 D C[SELECT 1] Z[I]";
        assertEquals(oneRow, cycle("SELECT t.i FROM pbf_t t"));
        assertEquals(oneRow, cycle("SELECT t.* FROM pbf_t t"));
        assertEquals(oneRow, cycle("SELECT t.ctid FROM pbf_t t"));
        // The relation's own name reaches the entry that was not renamed, and only that one.
        assertEquals(oneRow, cycle("SELECT pbf_t.i FROM pbf_t, pbf_t x"));
        assertEquals(oneRow, cycle("SELECT j.i FROM (pbf_t JOIN pbf_u USING (i)) AS j"));
    }

    @Test
    void aQualifiedColumnOverAViewIsRefusedAtParse() throws Exception {
        assertEquals("E[42703] column v.nosuchcol does not exist Z[I]",
                cycle("SELECT v.nosuchcol FROM pbf_v v"));
        assertEquals("E[42703] column pbf_v.nosuchcol does not exist Z[I]",
                cycle("SELECT pbf_v.nosuchcol FROM pbf_v"));
        // A view's columns are the ones it was defined with, whatever the query underneath calls
        // them, and a view stores nothing, so it has no system columns.
        assertEquals("E[42703] column v.i does not exist Z[I]", cycle("SELECT v.i FROM pbf_v2 v"));
        assertEquals("E[42703] column v.ctid does not exist Z[I]",
                cycle("SELECT v.ctid FROM pbf_v v"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT v.a FROM pbf_v2 v"));
        // A materialized view is written out like a table, so it answers for ctid as a table does.
        assertEquals("E[42703] column m.nosuchcol does not exist Z[I]",
                cycle("SELECT m.nosuchcol FROM pbf_m m"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT m.ctid FROM pbf_m m"));
    }

    @Test
    void aQualifiedColumnOverASubSelectOrAWithItemIsRefusedAtParse() throws Exception {
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("SELECT s.nosuchcol FROM (SELECT i FROM pbf_t) s"));
        assertEquals("E[42703] column s.j does not exist Z[I]",
                cycle("SELECT s.j FROM (SELECT i FROM pbf_t) s"));
        // An alias list renames the columns, so the old names are gone.
        assertEquals("E[42703] column s.i does not exist Z[I]",
                cycle("SELECT s.i FROM (SELECT i, j FROM pbf_t) s(y, z)"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT s.z FROM (SELECT i, j FROM pbf_t) s(y, z)"));
        assertEquals("E[42703] column w.nosuchcol does not exist Z[I]",
                cycle("WITH w AS (SELECT i FROM pbf_t) SELECT w.nosuchcol FROM w"));
        assertEquals("E[42703] column w.i does not exist Z[I]",
                cycle("WITH w(x) AS (SELECT i FROM pbf_t) SELECT w.i FROM w"));
        assertEquals("E[42703] column q.nosuchcol does not exist Z[I]",
                cycle("WITH w AS (SELECT i FROM pbf_t) SELECT q.nosuchcol FROM w q"));
        // A WITH item that writes rows answers for its RETURNING list, and nothing runs.
        assertEquals("E[42703] column w.nosuch does not exist Z[I]",
                cycle("WITH w AS (INSERT INTO pbf_u VALUES (9,'z') RETURNING i)"
                        + " SELECT w.nosuch FROM w"));
        assertEquals("1", scalar("SELECT count(*) FROM pbf_u"));
    }

    @Test
    void aQualifiedColumnOverACallInFromIsRefusedAtParse() throws Exception {
        assertEquals("E[42703] column g.nosuchcol does not exist Z[I]",
                cycle("SELECT g.nosuchcol FROM generate_series(1,3) g"));
        // The call's column is named after the alias the clause gave it, not after the call.
        assertEquals("E[42703] column g.generate_series does not exist Z[I]",
                cycle("SELECT g.generate_series FROM generate_series(1,3) g"));
        assertEquals("E[42703] column g.g does not exist Z[I]",
                cycle("SELECT g.g FROM generate_series(1,3) g(x)"));
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("SELECT s.nosuchcol FROM string_to_table('a,b', ',') s"));
        assertEquals("E[42703] column e.nosuchcol does not exist Z[I]",
                cycle("SELECT e.nosuchcol FROM json_array_elements('[1,2]'::json) e"));
        assertEquals("1 2 D D D C[SELECT 3] Z[I]", cycle("SELECT g.g FROM generate_series(1,3) g"));
        assertEquals("1 2 D D D C[SELECT 3] Z[I]",
                cycle("SELECT generate_series FROM generate_series(1,3)"));
        assertEquals("1 2 D D C[SELECT 2] Z[I]",
                cycle("SELECT g.ordinality FROM generate_series(1,2) WITH ORDINALITY g"));
        assertEquals("1 2 D D C[SELECT 2] Z[I]",
                cycle("SELECT e.value FROM json_array_elements('[1,2]'::json) e"));
    }

    @Test
    void theColumnsJsonEachAndUnnestAnswerWithAreSettledAtParse() throws Exception {
        // json_each and its kin hand back a pair per row under the names their signature gives
        // them, whatever the FROM clause calls the call.
        assertEquals("E[42703] column e.nosuchcol does not exist Z[I]",
                cycle("SELECT e.nosuchcol FROM json_each('{\"a\":1}'::json) e"));
        assertEquals("E[42703] column e.e does not exist Z[I]",
                cycle("SELECT e.e FROM json_each('{\"a\":1}'::json) e"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT e.key FROM json_each('{\"a\":1}'::json) e"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT e.k FROM json_each('{\"a\":1}'::json) e(k, v)"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT e.value FROM jsonb_each('{\"a\":1}'::jsonb) e"));
        // An array written out of literals is unnested into one column, named after the alias.
        assertEquals("E[42703] column u.unnest does not exist Z[I]",
                cycle("SELECT u.unnest FROM unnest(ARRAY[1,2]) u"));
        assertEquals("E[42703] column u.nosuchcol does not exist Z[I]",
                cycle("SELECT u.nosuchcol FROM unnest('{1,2}'::int[]) u"));
        assertEquals("1 2 D D C[SELECT 2] Z[I]", cycle("SELECT u.u FROM unnest(ARRAY[1,2]) u"));
        assertEquals("1 2 D D C[SELECT 2] Z[I]", cycle("SELECT u.u FROM unnest('{1,2}'::int[]) u"));
        // An array of a composite type is unnested into one column per field.
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT u.a FROM pbf_ca, unnest(cs) AS u"));
        // A call whose rows are one value answers under the name the clause gave it.
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT k.k FROM json_object_keys('{\"a\":1}'::json) k"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT p.p FROM jsonb_path_query('{\"a\":1}'::jsonb, '$.a') p"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT json_object_keys FROM json_object_keys('{\"a\":1}'::json)"));
    }

    @Test
    void aSchemaWrittenInFrontOfAQualifierIsSettledAtParse() throws Exception {
        assertEquals("E[42703] column pbf_t.nosuchcol does not exist Z[I]",
                cycle("SELECT public.pbf_t.nosuchcol FROM pbf_t"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT public.pbf_t.i FROM pbf_t"));
        assertEquals("E[42P01] invalid reference to FROM-clause entry for table \"pbf_t\" Z[I]",
                cycle("SELECT nosuchschema.pbf_t.i FROM pbf_t"));
        // A WITH item and a sub-select live in no schema at all, so no schema reaches them.
        assertEquals("E[42P01] invalid reference to FROM-clause entry for table \"w\" Z[I]",
                cycle("WITH w AS (SELECT i FROM pbf_t) SELECT public.w.i FROM w"));
        assertEquals("E[42P01] invalid reference to FROM-clause entry for table \"s\" Z[I]",
                cycle("SELECT public.s.i FROM (SELECT i FROM pbf_t) s"));
        // A relation the query does not read at all is missing rather than out of reach.
        assertEquals("E[42P01] missing FROM-clause entry for table \"pbf_u\" Z[I]",
                cycle("SELECT public.pbf_u.i FROM pbf_t"));
    }

    @Test
    void aColumnASubqueryHasNotIsRefusedAtParse() throws Exception {
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT (SELECT nosuchcol FROM pbf_t)"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT (SELECT nosuchcol FROM pbf_t) FROM pbf_t o"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT 1 WHERE EXISTS (SELECT nosuchcol FROM pbf_t)"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT i FROM pbf_t WHERE i IN (SELECT nosuchcol FROM pbf_t)"));
        assertEquals("E[42P01] relation \"pbf_nosuchrel\" does not exist Z[I]",
                cycle("SELECT (SELECT i FROM pbf_nosuchrel)"));
    }

    @Test
    void aCorrelatedReferenceReachesTheQueryAround() throws Exception {
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT (SELECT max(x.i) FROM pbf_t x WHERE x.i = o.i) FROM pbf_t o"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT (SELECT count(*) FROM pbf_t x WHERE x.i = o.i) FROM pbf_t o"));
        // A column the query around it has not got is still a column that does not exist.
        assertEquals("E[42703] column o.nosuchcol does not exist Z[I]",
                cycle("SELECT (SELECT o.nosuchcol FROM pbf_t x LIMIT 1) FROM pbf_t o"));
        assertEquals("E[42P01] missing FROM-clause entry for table \"q\" Z[I]",
                cycle("SELECT (SELECT q.i FROM pbf_t x LIMIT 1) FROM pbf_t o"));
    }

    @Test
    void countStarDoesNotHideAFaultWrittenAfterIt() throws Exception {
        // count(*) is the one call PostgreSQL settles without consulting anything, so a fault
        // written after it is still the fault it reports first.
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT count(*) FROM pbf_t GROUP BY nosuchcol"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT count(*), nosuchcol FROM pbf_t"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("SELECT count(*) FROM pbf_t HAVING nosuchcol > 1"));
        // WHERE and the sort clause are both settled before the grouping items.
        assertEquals("E[42703] column \"nosuchw\" does not exist Z[I]",
                cycle("SELECT count(*) FROM pbf_t WHERE nosuchw = 1 GROUP BY nosuchg"));
        assertEquals("E[42703] column \"nosucho\" does not exist Z[I]",
                cycle("SELECT count(*) FROM pbf_t GROUP BY nosuchg ORDER BY nosucho"));
    }

    @Test
    void aNearMissSuggestsEveryRelationInScope() throws Exception {
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT (SELECT ii FROM pbf_t x) FROM pbf_t o"), sync()));
            RawWireClient.Msg error = client.readErrorResponse();
            assertEquals("42703", error.sqlState());
            assertEquals("Perhaps you meant to reference the column \"x.i\""
                    + " or the column \"o.i\".", error.fields().get('H'));
        }
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT x.ii FROM pbf_t x"), sync()));
            RawWireClient.Msg error = client.readErrorResponse();
            assertEquals("42703", error.sqlState());
            assertEquals("Perhaps you meant to reference the column \"x.i\".",
                    error.fields().get('H'));
        }
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT pbf_t.i FROM pbf_t t"), sync()));
            RawWireClient.Msg error = client.readErrorResponse();
            assertEquals("42P01", error.sqlState());
            assertEquals("Perhaps you meant to reference the table alias \"t\".",
                    error.fields().get('H'));
        }
    }

    @Test
    void aNearMissCarriesItsSuggestionToTheClientThatRanTheStatement() throws SQLException {
        // The suggestion travels in the hint field whichever protocol the statement arrived over.
        exec("CREATE TABLE pbf_n (i int, j text)");
        assertEquals("column t.ii does not exist", messageOf("SELECT t.ii FROM pbf_n t"));
        assertEquals("Perhaps you meant to reference the column \"t.i\".",
                hintOf("SELECT t.ii FROM pbf_n t"));
        assertEquals("invalid reference to FROM-clause entry for table \"pbf_n\"",
                messageOf("SELECT pbf_n.i FROM pbf_n t"));
        assertEquals("Perhaps you meant to reference the table alias \"t\".",
                hintOf("SELECT pbf_n.i FROM pbf_n t"));
        exec("DROP TABLE pbf_n");
    }

    @Test
    void aDerivedRelationIsNamedTheWayItsOwnTargetListIs() throws Exception {
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("SELECT s.nosuchcol FROM (SELECT i + 1 FROM pbf_t) s"));
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("SELECT s.nosuchcol FROM (SELECT lower(j) FROM pbf_t) s"));
        // A cast's column is named after the type it ends at.
        assertEquals("E[42703] column s.text does not exist Z[I]",
                cycle("SELECT s.text FROM (SELECT i::text FROM pbf_t) s"));
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("SELECT s.nosuchcol FROM (SELECT count(*) FROM pbf_t) s"));
        assertEquals("E[42703] column w.nosuchcol does not exist Z[I]",
                cycle("WITH w AS (SELECT i + 1 FROM pbf_t) SELECT w.nosuchcol FROM w"));
        // What nothing names at all is ?column?.
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT s.\"?column?\" FROM (SELECT i + 1 FROM pbf_t) s"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT s.lower FROM (SELECT lower(j) FROM pbf_t) s"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("SELECT s.case FROM (SELECT CASE WHEN i > 0 THEN 1 END FROM pbf_t) s"));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                cycle("WITH w AS (SELECT lower(j) FROM pbf_t) SELECT w.lower FROM w"));
    }

    @Test
    void anAliasListLongerThanTheRelationIsRefusedAtParse() throws Exception {
        assertEquals("E[42P10] table \"z\" has 2 columns available but 3 columns specified Z[I]",
                cycle("SELECT * FROM pbf_t AS z (a, b, c)"));
        assertEquals("E[42P10] table \"s\" has 1 columns available but 2 columns specified Z[I]",
                cycle("SELECT * FROM (SELECT i FROM pbf_t) s (a, b)"));
        assertEquals("E[42P10] table \"x\" has 1 columns available but 2 columns specified Z[I]",
                cycle("WITH w AS (SELECT i FROM pbf_t) SELECT * FROM w x (a, b)"));
        assertEquals("E[42P10] table \"g\" has 2 columns available but 3 columns specified Z[I]",
                cycle("SELECT * FROM generate_series(1,2) WITH ORDINALITY g (a, b, c)"));
        assertEquals("E[42P10] table \"e\" has 2 columns available but 3 columns specified Z[I]",
                cycle("SELECT * FROM json_each('{\"a\":1}'::json) e (a, b, c)"));
        // The shape of the clause is settled before a single column is resolved.
        assertEquals("E[42P10] table \"z\" has 2 columns available but 3 columns specified Z[I]",
                cycle("SELECT * FROM pbf_t z (a, b, c) WHERE nosuchcol = 1"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT z.a FROM pbf_t AS z (a, b)"));
    }

    @Test
    void theStatementExplainWasGivenIsReadAtParse() throws Exception {
        assertEquals("E[42703] column s.nosuchcol does not exist Z[I]",
                cycle("EXPLAIN (COSTS OFF) SELECT s.nosuchcol FROM (SELECT i + 1 FROM pbf_t) s"));
        assertEquals("E[42703] column \"nosuchcol\" does not exist Z[I]",
                cycle("EXPLAIN SELECT nosuchcol FROM pbf_t"));
        assertEquals("E[42703] column \"nosuchcol\" of relation \"pbf_t\" does not exist Z[I]",
                cycle("EXPLAIN INSERT INTO pbf_t (nosuchcol) VALUES (1)"));
    }

    @Test
    void whichStatementAnExecuteNamesIsSettledWhenItRuns() throws Exception {
        // Which prepared statement an EXECUTE names is not settled by its text, so the client is
        // told at Execute — one message later than every other fault this reader answers for.
        assertEquals("1 2 E[26000] prepared statement \"pbf_nosuchprep\" does not exist Z[I]",
                cycle("EXECUTE pbf_nosuchprep(1)"));
    }

    @Test
    void oidIsNotASystemColumn() throws Exception {
        // A tuple has a ctid and a tableoid; it has had no oid since PostgreSQL stopped giving one
        // to ordinary rows. A relation that answers to the name declares it like any other column.
        assertEquals("E[42703] column \"oid\" does not exist Z[I]", cycle("SELECT oid FROM pbf_t"));
        assertEquals("E[42703] column pbf_t.oid does not exist Z[I]",
                cycle("SELECT pbf_t.oid FROM pbf_t"));
        assertEquals("E[42703] column \"oid\" of relation \"pbf_t\" does not exist Z[I]",
                cycle("INSERT INTO pbf_t (oid) VALUES (1)"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT ctid FROM pbf_t"));
        assertEquals("1 2 D C[SELECT 1] Z[I]", cycle("SELECT oid FROM pg_class LIMIT 1"));
    }

    @Test
    void showOfANameNoSettingAnswersToIsRefusedAtParse() throws Exception {
        assertEquals("E[42704] unrecognized configuration parameter \"pbf_nosuchguc\" Z[I]",
                cycle("SHOW pbf_nosuchguc"));
        // A name with a schema in front of it is a custom parameter, and there is no such
        // parameter until something sets one — the placeholder is made by the SET.
        assertEquals("E[42704] unrecognized configuration parameter \"pbf.nosuchcustom\" Z[I]",
                cycle("SHOW pbf.nosuchcustom"));
        try (RawWireClient client = open()) {
            client.write(query("SET pbf.own = '10'"));
            said(client);
            client.write(concat(parse("SHOW pbf.own"), bind(), execute(), sync()));
            assertTrue(said(client).startsWith("1 2 D "), "the setting the session made is shown");
        }
    }

    @Test
    void whatSearchAndCycleAddToAWithItemIsInScope() throws Exception {
        assertEquals("1 2 D D D C[SELECT 3] Z[I]",
                cycle("WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n < 3)"
                        + " SEARCH DEPTH FIRST BY n SET ord SELECT n, ord FROM r ORDER BY n"));
        assertEquals("1 2 D D D C[SELECT 3] Z[I]",
                cycle("WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n < 3)"
                        + " CYCLE n SET c USING p SELECT n, c FROM r ORDER BY n"));
        assertEquals("1 2 D D D C[SELECT 3] Z[I]",
                cycle("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3)"
                        + " SEARCH BREADTH FIRST BY n SET ord CYCLE n SET c USING p"
                        + " SELECT r.n, r.c FROM r ORDER BY n"));
    }

    // ============================================================ What the unnamed statement and
    // the unnamed portal stand for

    @Test
    void theUnnamedPreparedStatementIsNamedForWhatItIs() throws Exception {
        // The unnamed prepared statement has no name to write down, so PostgreSQL describes it
        // rather than quoting an empty one.
        try (RawWireClient client = open()) {
            client.write(concat(bind(), sync()));
            assertEquals("E[26000] unnamed prepared statement does not exist Z[I]", said(client));
        }
        try (RawWireClient client = open()) {
            client.write(concat(frame('D', concat(new byte[]{'S'}, cstring(""))), sync()));
            assertEquals("E[26000] unnamed prepared statement does not exist Z[I]", said(client));
        }
        try (RawWireClient client = open()) {
            byte[] bindNamed = frame('B', concat(cstring(""), cstring("pbf_none"),
                    int16(0), int16(0), int16(0)));
            client.write(concat(bindNamed, sync()));
            assertEquals("E[26000] prepared statement \"pbf_none\" does not exist Z[I]",
                    said(client));
        }
    }

    @Test
    void aSimpleQueryTakesTheUnnamedStatementAndPortalWithIt() throws Exception {
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 1"), sync()));
            assertEquals("1 Z[I]", said(client));
            client.write(query("SELECT 2"));
            assertEquals("T D C[SELECT 1] Z[I]", said(client));
            client.write(concat(bind(), sync()));
            assertEquals("E[26000] unnamed prepared statement does not exist Z[I]", said(client));
        }
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 1"), bind(), sync()));
            assertEquals("1 2 Z[I]", said(client));
            client.write(query("SELECT 2"));
            assertEquals("T D C[SELECT 1] Z[I]", said(client));
            client.write(concat(execute(), sync()));
            assertEquals("E[34000] portal \"\" does not exist Z[I]", said(client));
        }
    }

    @Test
    void executeWithoutAPortalIsRefused() throws Exception {
        // Only a portal a Bind made is executed; the unnamed prepared statement is not one.
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 3"), execute(), sync()));
            assertEquals("1 E[34000] portal \"\" does not exist Z[I]", said(client));
        }
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 8"), bind(),
                    frame('C', concat(new byte[]{'P'}, cstring(""))), execute(), sync()));
            assertEquals("1 2 3 E[34000] portal \"\" does not exist Z[I]", said(client));
        }
    }

    @Test
    void aPortalABlockHoldsOutlivesTheStatementItCameFrom() throws Exception {
        try (RawWireClient client = open()) {
            client.write(query("BEGIN"));
            assertEquals("C[BEGIN] Z[T]", said(client));
            client.write(concat(parse("SELECT 1"), bind(), sync()));
            assertEquals("1 2 Z[T]", said(client));
            // The portal runs the statement it was built from, not the one Parse just wrote.
            client.write(concat(parse("SELECT 2"), execute(), sync()));
            assertEquals("1 D C[SELECT 1] Z[T]", said(client));
            client.write(query("ROLLBACK"));
            assertEquals("C[ROLLBACK] Z[I]", said(client));
        }
    }

    // ============================================================ What Bind makes of the message
    // it is given

    @Test
    void aResultFormatListLongerThanTheColumnsIsRefusedAtBind() throws Exception {
        // The result formats are applied to the portal's row description, so a list naming more of
        // them than the statement has columns is a message that cannot be carried out.
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("SELECT 1", 3));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("SELECT 1", 2));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT 1, 2", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT * FROM pbf_t", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("TABLE pbf_t", 3));
        assertEquals("1 E[08P01] bind message has 4 result formats but query has 3 columns Z[I]",
                withFormats("SELECT *, 1 FROM pbf_t", 4));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 4 columns Z[I]",
                withFormats("SELECT * FROM pbf_t a, pbf_t b", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("SELECT 1 UNION SELECT 2", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("VALUES (1, 2)", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("EXPLAIN SELECT 1", 3));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("DELETE FROM pbf_t WHERE false RETURNING i", 2));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("UPDATE pbf_t SET i = i WHERE false RETURNING *", 3));
    }

    @Test
    void theWidthAResultFormatListIsHeldAgainstComesOffTheText() throws Exception {
        // SHOW writes one column and SHOW ALL three; a star over a WITH item or a sub-select is as
        // wide as the query underneath; a call in FROM is as wide as what it answers with; and a
        // join on USING or NATURAL writes the columns it merged only once.
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("SHOW client_min_messages", 3));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 3 columns Z[I]",
                withFormats("SHOW ALL", 2));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("WITH w AS (SELECT i FROM pbf_t) SELECT * FROM w", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 1 columns Z[I]",
                withFormats("SELECT * FROM (SELECT i FROM pbf_t) s", 3));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("SELECT * FROM (SELECT i + 1 FROM pbf_t) s", 2));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("WITH w AS (SELECT lower(j) FROM pbf_t) SELECT * FROM w", 2));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("SELECT * FROM generate_series(1,2)", 2));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT * FROM generate_series(1,2) WITH ORDINALITY", 3));
        assertEquals("1 E[08P01] bind message has 2 result formats but query has 1 columns Z[I]",
                withFormats("SELECT * FROM unnest(ARRAY[1,2]) u", 2));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT * FROM json_each('{\"a\":1}'::json) e", 3));
        assertEquals("1 E[08P01] bind message has 4 result formats but query has 3 columns Z[I]",
                withFormats("SELECT * FROM pbf_t t JOIN pbf_u u USING (i)", 4));
        assertEquals("1 E[08P01] bind message has 4 result formats but query has 3 columns Z[I]",
                withFormats("SELECT * FROM pbf_t t NATURAL JOIN pbf_u u", 4));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT * FROM pbf_v", 3));
        assertEquals("1 E[08P01] bind message has 3 result formats but query has 2 columns Z[I]",
                withFormats("SELECT * FROM pbf_m", 3));
    }

    @Test
    void aResultFormatListThatFitsIsAccepted() throws Exception {
        // A list of one stands for every column however many there are, and an empty list for none
        // of them, so neither is held against a count.
        assertEquals("1 2 D C[SELECT 1] Z[I]", withFormats("SELECT 1, 2", 2));
        assertEquals("1 2 D C[SELECT 1] Z[I]", withFormats("SELECT 1, 2", 1));
        assertEquals("1 2 D C[SELECT 1] Z[I]", withFormats("SELECT 1, 2", 0));
        assertEquals("1 2 D C[SELECT 1] Z[I]", withFormats("SELECT * FROM pbf_t", 2));
        assertEquals("1 2 D C[SELECT 1] Z[I]", withFormats("SELECT * FROM pbf_v", 2));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                withFormats("WITH w AS (SELECT i, j FROM pbf_t) SELECT * FROM w", 2));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                withFormats("SELECT * FROM pbf_t t JOIN pbf_u u USING (i)", 3));
        assertEquals("1 2 D D C[SELECT 2] Z[I]",
                withFormats("SELECT * FROM generate_series(1,2) WITH ORDINALITY", 2));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                withFormats("SELECT * FROM json_each('{\"a\":1}'::json) e", 2));
        // A statement that answers with no rows has no description to apply them to.
        assertEquals("1 2 C[UPDATE 0] Z[I]",
                withFormats("UPDATE pbf_t SET i = i WHERE false", 3));
        assertEquals("1 2 C[SET] Z[I]", withFormats("SET client_min_messages = notice", 3));
        assertEquals("1 2 C[BEGIN] Z[T]", withFormats("BEGIN", 3));
    }

    @Test
    void everyOtherBindComplaintComesFirst() throws Exception {
        // The count is the last thing Bind settles, so every other complaint the message carries is
        // the one answered.
        byte[] tooManyParameters = frame('B', concat(cstring(""), cstring(""), int16(0), int16(1),
                int32(1), new byte[]{'1'}, int16(3), int16(0), int16(0), int16(0)));
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 1"), tooManyParameters, sync()));
            assertEquals("1 E[08P01] bind message supplies 1 parameters, but prepared statement"
                    + " \"\" requires 0 Z[I]", said(client));
        }
        byte[] badValue = frame('B', concat(cstring(""), cstring(""), int16(0), int16(1),
                int32(8), "notanint".getBytes(StandardCharsets.UTF_8), int16(3),
                int16(0), int16(0), int16(0)));
        try (RawWireClient client = open()) {
            client.write(concat(parseTyped("SELECT $1::int", 23), badValue, execute(), sync()));
            assertEquals("1 E[22P02] invalid input syntax for type integer: \"notanint\" Z[I]",
                    said(client));
        }
        // A code that is neither text nor binary, with a list the right length, is Execute's to
        // report, which is where it is reported once the count fits.
        byte[] badCode = frame('B', concat(cstring(""), cstring(""), int16(0), int16(0),
                int16(2), int16(0), int16(7)));
        try (RawWireClient client = open()) {
            client.write(concat(parse("SELECT 1, 2"), badCode, execute(), sync()));
            assertEquals("1 2 E[22023] unsupported format code: 7 Z[I]", said(client));
        }
        // A portal a block still holds is not one a second Bind may make again.
        try (RawWireClient client = open()) {
            client.write(query("BEGIN"));
            said(client);
            byte[] named = frame('B', concat(cstring("pbf_p"), cstring(""), int16(0), int16(0),
                    int16(0)));
            client.write(concat(parse("SELECT 1"), named, sync()));
            assertEquals("1 2 Z[T]", said(client));
            byte[] again = frame('B', concat(cstring("pbf_p"), cstring(""), int16(0), int16(1),
                    int32(8), "notanint".getBytes(StandardCharsets.UTF_8), int16(0)));
            client.write(concat(parseTyped("SELECT $1::int", 23), again, sync()));
            assertEquals("1 E[42P03] cursor \"pbf_p\" already exists Z[E]", said(client));
            client.write(query("ROLLBACK"));
            said(client);
        }
    }

    @Test
    void aParameterValueThatWillNotReadIsRefusedAtBind() throws Exception {
        // Every parameter is read against the type it resolved to while Bind is processed, so a
        // value that will not read as that type is refused there — before BindComplete.
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"notanint\" Z[I]",
                bound("SELECT $1::int", 23, "notanint"));
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"\" Z[I]",
                bound("SELECT $1::int", 23, ""));
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"1.5\" Z[I]",
                bound("SELECT $1::int", 23, "1.5"));
        assertEquals("1 E[22P02] invalid input syntax for type boolean: \"notabool\" Z[I]",
                bound("SELECT $1::bool", 16, "notabool"));
        assertEquals("1 E[22P02] invalid input syntax for type numeric: \"x\" Z[I]",
                bound("SELECT $1::numeric", 1700, "x"));
        assertEquals("1 E[22007] invalid input syntax for type date: \"notadate\" Z[I]",
                bound("SELECT $1::date", 1082, "notadate"));
        assertEquals("1 E[22P02] invalid input syntax for type cidr: \"notacidr\" Z[I]",
                bound("SELECT $1::cidr", 650, "notacidr"));
        assertEquals("1 E[22P02] malformed array literal: \"notanarray\" Z[I]",
                bound("SELECT $1::int[]", 1007, "notanarray"));
        assertEquals("1 E[22P02] invalid input syntax for type json Z[I]",
                bound("SELECT $1::json", 114, "{"));
        assertEquals("1 E[22P02] invalid input syntax for type uuid: \"nope\" Z[I]",
                bound("SELECT $1::uuid", 2950, "nope"));
        assertEquals("1 E[22003] value \"99999999999\" is out of range for type integer Z[I]",
                bound("SELECT $1::int", 23, "99999999999"));
        assertEquals("1 E[22003] value \"99999999999999999999\" is out of range for type bigint"
                + " Z[I]", bound("SELECT $1::bigint", 20, "99999999999999999999"));
        assertEquals("1 E[22P02] \"2\" is not a valid binary digit Z[I]",
                bound("SELECT $1::bit", 1560, "2"));
        assertEquals("1 E[22P02] invalid input syntax for type point: \"notapoint\" Z[I]",
                bound("SELECT $1::point", 600, "notapoint"));
        assertEquals("1 E[22P02] malformed range literal: \"notarange\" Z[I]",
                bound("SELECT $1::int4range", 3904, "notarange"));
        assertEquals("1 E[22P02] malformed array literal: \"notanarray\" Z[I]",
                bound("SELECT $1::text[]", 1009, "notanarray"));
        assertEquals("1 E[22P02] invalid input syntax for type pg_lsn: \"notanlsn\" Z[I]",
                bound("SELECT $1::pg_lsn", 3220, "notanlsn"));
        assertEquals("1 E[22P02] invalid input syntax for type macaddr8: \"notamac8\" Z[I]",
                bound("SELECT $1::macaddr8", 774, "notamac8"));
    }

    @Test
    void aParameterWithNoDeclaredTypeIsReadAsTheTypeTheStatementSettles() throws Exception {
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"notanint\" Z[I]",
                bound("SELECT $1::int", 0, "notanint"));
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"notanint\" Z[I]",
                bound("SELECT i FROM pbf_t WHERE i = $1", 0, "notanint"));
        assertEquals("1 E[22P02] invalid input syntax for type integer: \"notanint\" Z[I]",
                bound("SELECT $1", 23, "notanint"));
    }

    @Test
    void aParameterValueThatReadsIsBound() throws Exception {
        String oneRow = "1 2 D C[SELECT 1] Z[I]";
        assertEquals(oneRow, bound("SELECT $1::int", 23, "42"));
        assertEquals(oneRow, bound("SELECT $1::int", 23, " 42 "));
        // A value that is not there at all is bound as it always was.
        assertEquals(oneRow, bound("SELECT $1::int", 23, null));
        assertEquals(oneRow, bound("SELECT $1::bool", 16, "t"));
        assertEquals(oneRow, bound("SELECT $1::numeric", 1700, "NaN"));
        assertEquals(oneRow, bound("SELECT $1::text", 25, "anything"));
        assertEquals(oneRow, bound("SELECT $1::date", 1082, "2020-01-01"));
        assertEquals(oneRow, bound("SELECT $1::uuid", 2950,
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
        assertEquals(oneRow, bound("SELECT $1::text[]", 1009, "{a,b}"));
    }

    @Test
    void aBinaryParameterOfTheWrongLengthIsRefusedAtBind() throws Exception {
        // A receive function reads a fixed number of bytes and insists the message holds nothing
        // more, so a value too short runs the reader off the end of the message and one too long
        // leaves data behind. A type read one byte at a time runs out of message rather than short
        // of it, which is worded differently again.
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 23, new byte[]{0, 1}));
        assertEquals("1 E[22P03] incorrect binary data format in bind parameter 1 Z[I]",
                binary("SELECT $1", 23, new byte[]{0, 0, 0, 0, 1}));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 20, new byte[]{0, 0, 0, 7}));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 21, new byte[]{1}));
        assertEquals("1 E[22P03] incorrect binary data format in bind parameter 1 Z[I]",
                binary("SELECT $1", 16, new byte[]{0, 1}));
        assertEquals("1 E[08P01] no data left in message Z[I]",
                binary("SELECT $1", 16, new byte[0]));
        assertEquals("1 E[08P01] no data left in message Z[I]",
                binary("SELECT $1", 829, new byte[5]));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 2950, new byte[15]));
        assertEquals("1 E[22P03] incorrect binary data format in bind parameter 1 Z[I]",
                binary("SELECT $1", 1082, new byte[5]));
        assertEquals("1 2 D C[SELECT 1] Z[I]", binary("SELECT $1", 23, new byte[]{0, 0, 0, 7}));
        assertEquals("1 2 D C[SELECT 1] Z[I]", binary("SELECT $1", 25, new byte[]{97, 98}));
    }

    @Test
    void aBinaryNumericOfTheWrongLengthIsRefusedAtBind() throws Exception {
        // A numeric says how wide it is in its own first two bytes — four fields and then one for
        // each group of digits.
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 1700, new byte[0]));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 1700, new byte[2]));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 1700, new byte[7]));
        assertEquals("1 E[22P03] incorrect binary data format in bind parameter 1 Z[I]",
                binary("SELECT $1", 1700, new byte[9]));
        assertEquals("1 E[22P03] incorrect binary data format in bind parameter 1 Z[I]",
                binary("SELECT $1", 1700, new byte[12]));
        assertEquals("1 E[08P01] insufficient data left in message Z[I]",
                binary("SELECT $1", 1700, new byte[]{0, 3, 0, 0, 0, 0, 0, 0, 0, 5}));
        assertEquals("1 2 D C[SELECT 1] Z[I]", binary("SELECT $1", 1700, new byte[8]));
        assertEquals("1 2 D C[SELECT 1] Z[I]",
                binary("SELECT $1", 1700, new byte[]{0, 1, 0, 0, 0, 0, 0, 0, 0, 5}));
    }

    @Test
    void aBinaryValueIsReadIntoTheTextItsTypePrints() throws Exception {
        // A value read out of a binary parameter is handed on as text, and the text has to be the
        // text the type prints: a zero seconds field and a zero fraction are written out, not left
        // off.
        assertEquals("2000-01-01 00:00:00", firstValue("SELECT $1", 1114, new byte[8]));
        assertEquals("2000-01-01 00:00:00.000001",
                firstValue("SELECT $1", 1114, new byte[]{0, 0, 0, 0, 0, 0, 0, 1}));
        assertEquals("2000-01-01 00:00:00.5",
                firstValue("SELECT $1", 1114, new byte[]{0, 0, 0, 0, 0, 7, -95, 32}));
        assertEquals("00:00:00", firstValue("SELECT $1", 1083, new byte[8]));
        assertEquals("2000-01-01", firstValue("SELECT $1", 1082, new byte[4]));
        assertEquals("00:00:00", firstValue("SELECT $1", 1186, new byte[16]));
        assertEquals("1 day", firstValue("SELECT $1", 1186,
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}));
        assertEquals("1 mon", firstValue("SELECT $1", 1186,
                new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}));
        assertEquals("(1,2)", firstValue("SELECT $1", 27, new byte[]{0, 0, 0, 1, 0, 2}));
        assertEquals("01:02:03:04:05:06",
                firstValue("SELECT $1", 829, new byte[]{1, 2, 3, 4, 5, 6}));
        assertEquals("1/2", firstValue("SELECT $1", 3220, new byte[]{0, 0, 0, 1, 0, 0, 0, 2}));
        assertEquals("1", firstValue("SELECT $1", 700, new byte[]{0x3F, (byte) 0x80, 0, 0}));
        assertEquals("1", firstValue("SELECT $1", 701,
                new byte[]{0x3F, (byte) 0xF0, 0, 0, 0, 0, 0, 0}));
    }

    @Test
    void aStatementTheDriverPreparedOnTheServerIsStillBoundOneFormatPerColumn() throws Exception {
        // Once a statement is prepared server-side the driver sends one result format code per
        // field, so the width Bind holds a list against is worked out on ordinary client traffic:
        // a width read wrongly would refuse every such Bind.
        try (Connection c = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword())) {
            run(c, "CREATE TABLE pbf_x (i int, j text, k int)",
                    "INSERT INTO pbf_x VALUES (1,'a',10),(2,'b',20),(3,'c',30)");
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT i, j, k FROM pbf_x WHERE i = ? ORDER BY i")) {
                for (int round = 1; round <= 8; round++) {
                    ps.setInt(1, 2);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "round " + round);
                        assertEquals("2|b|20", rs.getString(1) + "|" + rs.getString(2) + "|"
                                + rs.getString(3), "round " + round);
                        assertFalse(rs.next(), "round " + round);
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM pbf_x ORDER BY i")) {
                for (int round = 1; round <= 8; round++) {
                    try (ResultSet rs = ps.executeQuery()) {
                        assertEquals(3, rs.getMetaData().getColumnCount(), "round " + round);
                        int seen = 0;
                        while (rs.next()) seen++;
                        assertEquals(3, seen, "round " + round);
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT a.i, b.j FROM pbf_x a JOIN pbf_x b USING (i) ORDER BY a.i")) {
                for (int round = 1; round <= 8; round++) {
                    try (ResultSet rs = ps.executeQuery()) {
                        int seen = 0;
                        while (rs.next()) seen++;
                        assertEquals(3, seen, "round " + round);
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE pbf_x SET j = ? WHERE i = ? RETURNING i, j")) {
                for (int round = 1; round <= 8; round++) {
                    ps.setString(1, "r" + round);
                    ps.setInt(2, 3);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next(), "round " + round);
                        assertEquals("3|r" + round, rs.getString(1) + "|" + rs.getString(2));
                        assertFalse(rs.next(), "round " + round);
                    }
                }
            }
            // A value the type cannot read is refused however the driver sent it.
            try (PreparedStatement ps = c.prepareStatement("SELECT i FROM pbf_x WHERE i = ?::int")) {
                ps.setString(1, "notanint");
                SQLException raised = assertThrows(SQLException.class, ps::executeQuery);
                assertEquals("22P02", raised.getSQLState());
            }
            run(c, "DROP TABLE pbf_x");
        }
    }
}
