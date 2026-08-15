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
import static org.junit.jupiter.api.Assertions.assertNull;
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

    // ------------------------------------------------------------ helpers for the sections below

    /** The SQLSTATE a statement raises on a session of its own, or "OK" when it does not raise. */
    private static String stateOn(Connection c, String sql) {
        try {
            run(c, sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The primary message of a server error, without the severity or the position. */
    private static String primary(SQLException e) {
        if (e instanceof org.postgresql.util.PSQLException
                && ((org.postgresql.util.PSQLException) e).getServerErrorMessage() != null) {
            return ((org.postgresql.util.PSQLException) e).getServerErrorMessage().getMessage();
        }
        return e.getMessage();
    }

    /**
     * A reader at {@code level} takes its snapshot, a second session writes the row the reader is
     * about to lock, and the lock is issued while that write is still uncommitted. Returns what
     * the lock answered once the writer finished -- its rows, or "ERR[sqlstate] message" -- having
     * first established that the lock really waited for the writer rather than answering off the
     * snapshot's own copy of the row.
     */
    private static String whileAnotherSessionHolds(String level, String snapshot, String write,
            boolean writerCommits, String lock) throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection reader = session(); Connection writer = session()) {
            run(reader, "BEGIN ISOLATION LEVEL " + level);
            rows(reader, snapshot);
            run(writer, "BEGIN", write);
            Future<String> waiting = pool.submit(new Callable<String>() {
                @Override
                public String call() {
                    try {
                        return String.join(",", rows(reader, lock));
                    } catch (SQLException e) {
                        return "ERR[" + e.getSQLState() + "] " + primary(e);
                    }
                }
            });
            Thread.sleep(400);
            assertFalse(waiting.isDone(), "the lock did not wait for the writer");
            run(writer, writerCommits ? "COMMIT" : "ROLLBACK");
            String answer = waiting.get(20, TimeUnit.SECONDS);
            run(reader, "ROLLBACK");
            return answer;
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The same shape for a lock that is not supposed to wait at all, so it is asked in the
     * foreground while the writer still holds the row. The writer then takes its write back.
     */
    private static String withoutWaitingFor(String level, String snapshot, String write,
            String lock) throws SQLException {
        try (Connection reader = session(); Connection writer = session()) {
            run(reader, "BEGIN ISOLATION LEVEL " + level);
            rows(reader, snapshot);
            run(writer, "BEGIN", write);
            String answer = promptly(reader, lock);
            run(writer, "ROLLBACK");
            run(reader, "ROLLBACK");
            return answer;
        }
    }

    /** A statement asked with a deadline, so a lock that waits fails rather than hangs. */
    private static String promptly(Connection c, String sql) {
        try (Statement s = c.createStatement()) {
            s.setQueryTimeout(10);
            try (ResultSet rs = s.executeQuery(sql)) {
                List<String> got = new ArrayList<>();
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append('|');
                        sb.append(rs.getString(i));
                    }
                    got.add(sb.toString());
                }
                return got.isEmpty() ? "(no rows)" : String.join(",", got);
            }
        } catch (SQLException e) {
            return "ERR[" + e.getSQLState() + "] " + primary(e);
        }
    }

    // ============================================================ The line pointers a relation
    // hands out are its own, and begin at one

    @Test
    void aRelationCreatedUnderAUsedNameNumbersItsTuplesFromOne() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_a (i int)", "INSERT INTO lpr_a VALUES (1),(2)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                    rows(c, "SELECT ctid::text, i FROM lpr_a ORDER BY i"));
            // The file belongs to the relation, not to the name: a new relation has a new file.
            run(c, "DROP TABLE lpr_a", "CREATE TABLE lpr_a (i int)",
                    "INSERT INTO lpr_a VALUES (1),(2)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                    rows(c, "SELECT ctid::text, i FROM lpr_a ORDER BY i"));
            // The same inside one transaction: the name reaches a different relation afterwards.
            run(c, "BEGIN", "DROP TABLE lpr_a", "CREATE TABLE lpr_a (i int)",
                    "INSERT INTO lpr_a VALUES (7)", "COMMIT");
            assertEquals(List.of("(0,1)|7"), rows(c, "SELECT ctid::text, i FROM lpr_a ORDER BY i"));
            run(c, "DROP TABLE lpr_a");
        }
    }

    @Test
    void aDropThatIsRolledBackLeavesTheNumberingWhereItWas() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_b (i int)", "INSERT INTO lpr_b VALUES (1),(2)",
                    "BEGIN", "DROP TABLE lpr_b", "ROLLBACK", "INSERT INTO lpr_b VALUES (3)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3"),
                    rows(c, "SELECT ctid::text, i FROM lpr_b ORDER BY i"));
            run(c, "DROP TABLE lpr_b");
            // A drop and a re-create rolled back together leave the first relation's file, and its
            // numbering, exactly where they were -- the place (0,1) is not handed out twice.
            run(c, "CREATE TABLE lpr_c (i int)", "INSERT INTO lpr_c VALUES (1),(2)",
                    "BEGIN", "DROP TABLE lpr_c", "CREATE TABLE lpr_c (i int)",
                    "INSERT INTO lpr_c VALUES (9)");
            assertEquals(List.of("(0,1)|9"), rows(c, "SELECT ctid::text, i FROM lpr_c ORDER BY i"));
            run(c, "ROLLBACK", "INSERT INTO lpr_c VALUES (3)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3"),
                    rows(c, "SELECT ctid::text, i FROM lpr_c ORDER BY i"));
            run(c, "DROP TABLE lpr_c");
        }
    }

    @Test
    void aRenameAndAMoveToAnotherSchemaCarryTheNumberingAlong() throws SQLException {
        exec("CREATE TABLE lpr_d (i int)");
        exec("INSERT INTO lpr_d VALUES (1),(2)");
        exec("ALTER TABLE lpr_d RENAME TO lpr_d2");
        exec("INSERT INTO lpr_d2 VALUES (3)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3"),
                rows("SELECT ctid::text, i FROM lpr_d2 ORDER BY i"));
        exec("CREATE SCHEMA lpr_s");
        exec("ALTER TABLE lpr_d2 SET SCHEMA lpr_s");
        exec("INSERT INTO lpr_s.lpr_d2 VALUES (4)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3", "(0,4)|4"),
                rows("SELECT ctid::text, i FROM lpr_s.lpr_d2 ORDER BY i"));
        exec("DROP TABLE lpr_s.lpr_d2");
        exec("DROP SCHEMA lpr_s");
    }

    @Test
    void twoRelationsOfOneNameInTwoSchemasNumberTheirTuplesApart() throws SQLException {
        exec("CREATE SCHEMA lpr_t");
        exec("CREATE TABLE lpr_e (i int)");
        exec("CREATE TABLE lpr_t.lpr_e (i int)");
        exec("INSERT INTO lpr_e VALUES (1),(2),(3)");
        exec("INSERT INTO lpr_t.lpr_e VALUES (1)");
        // Dropping one leaves the other's numbering exactly where it stood.
        exec("DROP TABLE lpr_e");
        exec("INSERT INTO lpr_t.lpr_e VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_t.lpr_e ORDER BY i"));
        exec("CREATE TABLE lpr_e (i int)");
        exec("INSERT INTO lpr_e VALUES (7)");
        assertEquals(List.of("(0,1)|7"), rows("SELECT ctid::text, i FROM lpr_e ORDER BY i"));
        exec("DROP TABLE lpr_e");
        exec("DROP SCHEMA lpr_t CASCADE");
    }

    @Test
    void aPartitionedRelationReCreatedUnderTheSameNameBeginsAtOne() throws SQLException {
        exec("CREATE TABLE lpr_f (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE lpr_f0 PARTITION OF lpr_f FOR VALUES FROM (0) TO (100)");
        exec("INSERT INTO lpr_f VALUES (1),(2)");
        exec("DROP TABLE lpr_f");
        exec("CREATE TABLE lpr_f (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE lpr_f0 PARTITION OF lpr_f FOR VALUES FROM (0) TO (100)");
        exec("INSERT INTO lpr_f VALUES (3),(4)");
        assertEquals(List.of("(0,1)|3", "(0,2)|4"),
                rows("SELECT ctid::text, i FROM lpr_f ORDER BY i"));
        exec("DROP TABLE lpr_f");
    }

    @Test
    void truncateHandsTheRelationANewFileAndADeleteOfEveryRowDoesNot() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_g (i int)", "INSERT INTO lpr_g VALUES (1),(2)",
                    "TRUNCATE lpr_g", "INSERT INTO lpr_g VALUES (3),(4)");
            assertEquals(List.of("(0,1)|3", "(0,2)|4"),
                    rows(c, "SELECT ctid::text, i FROM lpr_g ORDER BY i"));
            run(c, "BEGIN", "TRUNCATE lpr_g", "ROLLBACK", "INSERT INTO lpr_g VALUES (5)");
            assertEquals(List.of("(0,1)|3", "(0,2)|4", "(0,3)|5"),
                    rows(c, "SELECT ctid::text, i FROM lpr_g ORDER BY i"));
            // A DELETE of every row leaves the file alone, so the numbering goes on.
            run(c, "DELETE FROM lpr_g", "INSERT INTO lpr_g VALUES (6)");
            assertEquals(List.of("(0,4)|6"), rows(c, "SELECT ctid::text, i FROM lpr_g ORDER BY i"));
            run(c, "DROP TABLE lpr_g");
        }
    }

    // ============================================================ A write refused after its row
    // was written has already spent the place

    @Test
    void aWriteRefusedByAnIndexHasAlreadySpentItsLinePointer() throws SQLException {
        exec("CREATE TABLE lpr_h (i int PRIMARY KEY)");
        exec("INSERT INTO lpr_h VALUES (1)");
        assertEquals("23505", stateOf("INSERT INTO lpr_h VALUES (1)"));
        exec("INSERT INTO lpr_h VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                rows("SELECT ctid::text, i FROM lpr_h ORDER BY i"));
        // Every row the statement had already written took a place as well.
        assertEquals("23505", stateOf("INSERT INTO lpr_h VALUES (5),(1),(6)"));
        exec("INSERT INTO lpr_h VALUES (3)");
        assertEquals(List.of("(0,1)|1", "(0,3)|2", "(0,6)|3"),
                rows("SELECT ctid::text, i FROM lpr_h ORDER BY i"));
        exec("DROP TABLE lpr_h");
        // An INSERT ... SELECT is the same write in another spelling.
        exec("CREATE TABLE lpr_j (i int PRIMARY KEY)");
        exec("CREATE TABLE lpr_js (i int)");
        exec("INSERT INTO lpr_j VALUES (1)");
        exec("INSERT INTO lpr_js VALUES (5),(1),(6)");
        assertEquals("23505", stateOf("INSERT INTO lpr_j SELECT i FROM lpr_js ORDER BY i"));
        exec("INSERT INTO lpr_j VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,3)|9"),
                rows("SELECT ctid::text, i FROM lpr_j ORDER BY i"));
        exec("DROP TABLE lpr_j");
        exec("DROP TABLE lpr_js");
    }

    @Test
    void aWriteRefusedForAMissingParentRowHasAlreadySpentItsLinePointer() throws SQLException {
        exec("CREATE TABLE lpr_kp (i int PRIMARY KEY)");
        exec("INSERT INTO lpr_kp VALUES (1)");
        exec("CREATE TABLE lpr_k (i int REFERENCES lpr_kp(i))");
        exec("INSERT INTO lpr_k VALUES (1)");
        assertEquals("23503", stateOf("INSERT INTO lpr_k VALUES (9)"));
        exec("INSERT INTO lpr_k VALUES (1)");
        assertEquals(List.of("(0,1)|1", "(0,3)|1"),
                rows("SELECT ctid::text, i FROM lpr_k ORDER BY ctid"));
        exec("DROP TABLE lpr_k");
        exec("DROP TABLE lpr_kp");
    }

    @Test
    void aReferenceLeftToTheEndOfTheTransactionSpendsThePlaceJustTheSame() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_mp (i int PRIMARY KEY)", "INSERT INTO lpr_mp VALUES (1)",
                    "CREATE TABLE lpr_m (i int REFERENCES lpr_mp(i) DEFERRABLE INITIALLY DEFERRED)",
                    "INSERT INTO lpr_m VALUES (1)",
                    "BEGIN", "INSERT INTO lpr_m VALUES (9)");
            // The row was written when the statement ran; only the reference waited.
            assertEquals("23503", stateOn(c, "COMMIT"));
            run(c, "INSERT INTO lpr_m VALUES (1)");
            assertEquals(List.of("(0,1)|1", "(0,3)|1"),
                    rows(c, "SELECT ctid::text, i FROM lpr_m ORDER BY ctid"));
            run(c, "DROP TABLE lpr_m", "DROP TABLE lpr_mp");
        }
    }

    @Test
    void aPartitionsOwnIndexAndAMergeInsertSpendThePlaceTheRowSatOn() throws SQLException {
        exec("CREATE TABLE lpr_n (i int PRIMARY KEY) PARTITION BY RANGE (i)");
        exec("CREATE TABLE lpr_n0 PARTITION OF lpr_n FOR VALUES FROM (0) TO (100)");
        exec("INSERT INTO lpr_n VALUES (1)");
        assertEquals("23505", stateOf("INSERT INTO lpr_n VALUES (1)"));
        exec("INSERT INTO lpr_n VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                rows("SELECT ctid::text, i FROM lpr_n ORDER BY i"));
        exec("DROP TABLE lpr_n");

        exec("CREATE TABLE lpr_p (i int PRIMARY KEY, s text)");
        exec("CREATE TABLE lpr_ps (i int, s text)");
        exec("INSERT INTO lpr_p VALUES (1,'a')");
        exec("INSERT INTO lpr_ps VALUES (1,'b')");
        assertEquals("23505", stateOf("MERGE INTO lpr_p t USING lpr_ps u ON t.i = u.i + 100"
                + " WHEN NOT MATCHED THEN INSERT VALUES (u.i, u.s)"));
        exec("INSERT INTO lpr_p VALUES (2,'c')");
        assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                rows("SELECT ctid::text, i FROM lpr_p ORDER BY i"));
        exec("DROP TABLE lpr_p");
        exec("DROP TABLE lpr_ps");

        // An exclusion constraint is read off an index too, so it is found after the write.
        exec("CREATE TABLE lpr_q (i int, EXCLUDE (i WITH =))");
        exec("INSERT INTO lpr_q VALUES (1)");
        assertEquals("23P01", stateOf("INSERT INTO lpr_q VALUES (1)"));
        exec("INSERT INTO lpr_q VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                rows("SELECT ctid::text, i FROM lpr_q ORDER BY i"));
        exec("DROP TABLE lpr_q");
    }

    @Test
    void anAfterTriggerSpendsThePlaceAndABeforeTriggerDoesNot() throws SQLException {
        exec("CREATE FUNCTION lpr_raise() RETURNS trigger AS $$ BEGIN"
                + " IF NEW.i = 99 THEN RAISE EXCEPTION 'refused by the trigger'; END IF;"
                + " RETURN NEW; END $$ LANGUAGE plpgsql");
        // An AFTER trigger runs once the row is in the relation.
        exec("CREATE TABLE lpr_r (i int)");
        exec("CREATE TRIGGER lpr_r_after AFTER INSERT ON lpr_r"
                + " FOR EACH ROW EXECUTE FUNCTION lpr_raise()");
        exec("INSERT INTO lpr_r VALUES (1)");
        assertEquals("P0001", stateOf("INSERT INTO lpr_r VALUES (99)"));
        exec("INSERT INTO lpr_r VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                rows("SELECT ctid::text, i FROM lpr_r ORDER BY i"));
        exec("DROP TABLE lpr_r");
        // A BEFORE trigger runs first, so the row never reached the relation.
        exec("CREATE TABLE lpr_u (i int)");
        exec("CREATE TRIGGER lpr_u_before BEFORE INSERT ON lpr_u"
                + " FOR EACH ROW EXECUTE FUNCTION lpr_raise()");
        exec("INSERT INTO lpr_u VALUES (1)");
        assertEquals("P0001", stateOf("INSERT INTO lpr_u VALUES (99)"));
        exec("INSERT INTO lpr_u VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_u ORDER BY i"));
        exec("DROP TABLE lpr_u");
        exec("DROP FUNCTION lpr_raise()");
    }

    @Test
    void aWriteRefusedBeforeItReachedTheRelationSpendsNothing() throws SQLException {
        exec("CREATE TABLE lpr_v (i int NOT NULL CHECK (i > 0))");
        exec("INSERT INTO lpr_v VALUES (1)");
        assertEquals("23502", stateOf("INSERT INTO lpr_v VALUES (NULL)"));
        assertEquals("23514", stateOf("INSERT INTO lpr_v VALUES (-1)"));
        assertEquals("22P02", stateOf("INSERT INTO lpr_v VALUES ('x')"));
        exec("INSERT INTO lpr_v VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_v ORDER BY i"));
        exec("DROP TABLE lpr_v");
        // A row that belongs in no partition never reached a relation either.
        exec("CREATE TABLE lpr_w (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE lpr_w0 PARTITION OF lpr_w FOR VALUES FROM (0) TO (10)");
        exec("INSERT INTO lpr_w VALUES (1)");
        assertEquals("23514", stateOf("INSERT INTO lpr_w VALUES (50)"));
        exec("INSERT INTO lpr_w VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_w ORDER BY i"));
        exec("DROP TABLE lpr_w");
        // A generation expression is worked out before the row is written.
        exec("CREATE TABLE lpr_x (i int, j int GENERATED ALWAYS AS (10 / i) STORED)");
        exec("INSERT INTO lpr_x VALUES (1)");
        assertEquals("22012", stateOf("INSERT INTO lpr_x VALUES (0)"));
        exec("INSERT INTO lpr_x VALUES (2)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_x ORDER BY i"));
        exec("DROP TABLE lpr_x");
    }

    @Test
    void whatTheTransactionBecomesDoesNotGiveThePlaceBack() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_y (i int PRIMARY KEY)", "INSERT INTO lpr_y VALUES (1)",
                    "BEGIN", "SAVEPOINT sp");
            assertEquals("23505", stateOn(c, "INSERT INTO lpr_y VALUES (1)"));
            run(c, "ROLLBACK TO SAVEPOINT sp", "INSERT INTO lpr_y VALUES (2)", "COMMIT");
            assertEquals(List.of("(0,1)|1", "(0,3)|2"),
                    rows(c, "SELECT ctid::text, i FROM lpr_y ORDER BY i"));
            run(c, "DROP TABLE lpr_y");
            // A row a whole transaction takes back leaves its place spent as well.
            run(c, "CREATE TABLE lpr_z (i int PRIMARY KEY)", "INSERT INTO lpr_z VALUES (1)",
                    "BEGIN", "INSERT INTO lpr_z VALUES (2)", "ROLLBACK",
                    "INSERT INTO lpr_z VALUES (3)");
            assertEquals(List.of("(0,1)|1", "(0,3)|3"),
                    rows(c, "SELECT ctid::text, i FROM lpr_z ORDER BY i"));
            run(c, "DROP TABLE lpr_z");
        }
    }

    @Test
    void anUpdateWritesItsNewVersionElsewhereAndARefusedOneSpendsThatPlace() throws SQLException {
        exec("CREATE TABLE lpr_aa (i int UNIQUE)");
        exec("INSERT INTO lpr_aa VALUES (1),(2)");
        assertEquals("23505", stateOf("UPDATE lpr_aa SET i = 1 WHERE i = 2"));
        exec("INSERT INTO lpr_aa VALUES (3)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,4)|3"),
                rows("SELECT ctid::text, i FROM lpr_aa ORDER BY i"));
        exec("UPDATE lpr_aa SET i = i * 10 WHERE i = 1");
        exec("INSERT INTO lpr_aa VALUES (4)");
        assertEquals(List.of("(0,2)|2", "(0,4)|3", "(0,6)|4", "(0,5)|10"),
                rows("SELECT ctid::text, i FROM lpr_aa ORDER BY i"));
        exec("DROP TABLE lpr_aa");
    }

    @Test
    void onConflictPassesOverARowForNothingAndRewritesOneSomewhereElse() throws SQLException {
        exec("CREATE TABLE lpr_ab (i int PRIMARY KEY, s text)");
        exec("INSERT INTO lpr_ab VALUES (1,'a')");
        exec("INSERT INTO lpr_ab VALUES (1,'b') ON CONFLICT DO NOTHING");
        exec("INSERT INTO lpr_ab VALUES (2,'c')");
        assertEquals(List.of("(0,1)|1|a", "(0,2)|2|c"),
                rows("SELECT ctid::text, i, s FROM lpr_ab ORDER BY i"));
        exec("INSERT INTO lpr_ab VALUES (1,'d') ON CONFLICT (i) DO UPDATE SET s = 'e'");
        exec("INSERT INTO lpr_ab VALUES (3,'f')");
        assertEquals(List.of("(0,3)|1|e", "(0,2)|2|c", "(0,4)|3|f"),
                rows("SELECT ctid::text, i, s FROM lpr_ab ORDER BY i"));
        exec("DROP TABLE lpr_ab");
    }

    // ============================================================ Who removed the row

    @Test
    void deleteReturningXmaxNamesTheDeletingTransaction() throws SQLException {
        exec("CREATE TABLE xmk_a (i int)");
        exec("INSERT INTO xmk_a VALUES (1),(2),(3),(4)");
        // A row nobody has removed is marked with nobody.
        assertEquals(List.of("1|0", "2|0", "3|0", "4|0"),
                rows("SELECT i, xmax::text FROM xmk_a ORDER BY i"));
        assertEquals(List.of("1|t"), rows("DELETE FROM xmk_a WHERE i = 1"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        // Every row one DELETE takes is marked by the same transaction.
        assertEquals(List.of("2|t", "3|t"), rows("DELETE FROM xmk_a WHERE i IN (2,3)"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        // A DELETE read through a modifying WITH item is marked the same way.
        assertEquals(List.of("4|t"),
                rows("WITH d AS (DELETE FROM xmk_a WHERE i = 4 RETURNING i, xmax)"
                        + " SELECT i, xmax = pg_current_xact_id()::text::xid AS mine FROM d"));
        exec("DROP TABLE xmk_a");
    }

    @Test
    void insertAndUpdateReturningXmaxAnswerZero() throws SQLException {
        // The control for the DELETE above: what an INSERT and an UPDATE report is the version
        // they wrote, whose xmax is nobody's yet.
        exec("CREATE TABLE xmk_b (i int)");
        assertEquals(List.of("5|0"), rows("INSERT INTO xmk_b VALUES (5) RETURNING i, xmax::text"));
        assertEquals(List.of("20|0"),
                rows("UPDATE xmk_b SET i = 20 WHERE i = 5 RETURNING i, xmax::text"));
        try (Connection c = session()) {
            run(c, "BEGIN");
            assertEquals(List.of("6|0"),
                    rows(c, "INSERT INTO xmk_b VALUES (6) RETURNING i, xmax::text"));
            assertEquals(List.of("21|0"),
                    rows(c, "UPDATE xmk_b SET i = 21 WHERE i = 20 RETURNING i, xmax::text"));
            run(c, "COMMIT");
        }
        assertEquals(List.of("6|f", "21|f"),
                rows("SELECT i, xmax::text <> '0' AS marked FROM xmk_b ORDER BY i"));
        exec("DROP TABLE xmk_b");
    }

    @Test
    void theMarkStaysOnTheVersionARolledBackDeletePutBack() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE xmk_c (i int)", "INSERT INTO xmk_c VALUES (1),(2)",
                    "BEGIN", "DELETE FROM xmk_c WHERE i = 1", "ROLLBACK");
            assertEquals(List.of("1|t", "2|f"),
                    rows(c, "SELECT i, xmax::text <> '0' AS marked FROM xmk_c ORDER BY i"));
            run(c, "DROP TABLE xmk_c");
        }
    }

    @Test
    void aRowRemovedThroughAParentIsMarkedWhereItLives() throws SQLException {
        exec("CREATE TABLE xmk_dp (i int, s text)");
        exec("CREATE TABLE xmk_dc (extra int) INHERITS (xmk_dp)");
        exec("INSERT INTO xmk_dp VALUES (1,'a')");
        exec("INSERT INTO xmk_dc VALUES (2,'b',7)");
        assertEquals(List.of("1|t"), rows("DELETE FROM xmk_dp WHERE i = 1"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        // The child's row, taken through the parent.
        assertEquals(List.of("2|t"), rows("DELETE FROM xmk_dp WHERE i = 2"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        exec("INSERT INTO xmk_dc VALUES (3,'c',8)");
        assertEquals(List.of("3|t"), rows("DELETE FROM xmk_dc WHERE i = 3"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        exec("DROP TABLE xmk_dc");
        exec("DROP TABLE xmk_dp");

        exec("CREATE TABLE xmk_e (i int, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE xmk_e0 PARTITION OF xmk_e FOR VALUES FROM (0) TO (100)");
        exec("INSERT INTO xmk_e VALUES (1,'a'),(2,'b'),(3,'c')");
        assertEquals(List.of("1|t"), rows("DELETE FROM xmk_e WHERE i = 1"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        assertEquals(List.of("2|t"), rows("DELETE FROM xmk_e0 WHERE i = 2"
                + " RETURNING i, xmax = pg_current_xact_id()::text::xid AS mine"));
        assertEquals(List.of("3|0"),
                rows("UPDATE xmk_e SET s = 'z' WHERE i = 3 RETURNING i, xmax::text"));
        exec("DROP TABLE xmk_e");
    }

    // ============================================================ The command identifiers a
    // statement takes

    @Test
    void cminCountsTheCatalogueRowsADdlStatementWrote() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN");
            try {
                run(c, "CREATE TABLE cid_a (i int)", "CREATE TABLE cid_a1 (i int)",
                        "INSERT INTO cid_a VALUES (1)",
                        "DROP TABLE cid_a1", "INSERT INTO cid_a VALUES (2)",
                        "CREATE VIEW cid_av AS SELECT 1 AS x", "INSERT INTO cid_a VALUES (3)",
                        "DROP VIEW cid_av", "INSERT INTO cid_a VALUES (4)",
                        "CREATE SEQUENCE cid_as", "INSERT INTO cid_a VALUES (5)",
                        "DROP SEQUENCE cid_as", "INSERT INTO cid_a VALUES (6)",
                        "CREATE TABLE cid_a2 (i int)", "INSERT INTO cid_a VALUES (7)",
                        "ALTER TABLE cid_a2 ADD COLUMN j int", "INSERT INTO cid_a VALUES (8)",
                        // A DROP over a name nothing answers to writes no catalogue row at all.
                        "DROP TABLE IF EXISTS cid_nothere", "INSERT INTO cid_a VALUES (9)");
                assertEquals(
                        List.of("1|2", "2|6", "3|9", "4|14", "5|17", "6|19", "7|21", "8|23",
                                "9|24"),
                        rows(c, "SELECT i, cmin::text FROM cid_a ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
        }
    }

    @Test
    void aStatementThatReadsTakesNoCommandIdentifierAndOneThatWritesTakesOne() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN");
            try {
                // A write that touched no row still took an identifier; a read took none.
                run(c, "CREATE TABLE cid_b (i int)", "INSERT INTO cid_b VALUES (1)",
                        "SELECT 1", "INSERT INTO cid_b VALUES (2)",
                        "UPDATE cid_b SET i = i WHERE false", "INSERT INTO cid_b VALUES (3)",
                        "DELETE FROM cid_b WHERE false", "INSERT INTO cid_b VALUES (4)");
                assertEquals(List.of("1|1", "2|2", "3|4", "4|6"),
                        rows(c, "SELECT i, cmin::text FROM cid_b ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
        }
    }

    @Test
    void aCommandIdentifierARolledBackSavepointSpentIsNotHandedOutAgain() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN", "CREATE TABLE cid_c (i int)", "INSERT INTO cid_c VALUES (1)",
                    "SAVEPOINT sp", "INSERT INTO cid_c VALUES (2)", "ROLLBACK TO SAVEPOINT sp",
                    "INSERT INTO cid_c VALUES (3)");
            assertEquals(List.of("1|1", "3|3"),
                    rows(c, "SELECT i, cmin::text FROM cid_c ORDER BY i"));
            run(c, "COMMIT");
            // The identifier is written on the version, so it reads the same afterwards.
            assertEquals(List.of("1|1", "3|3"),
                    rows(c, "SELECT i, cmin::text FROM cid_c ORDER BY i"));
            run(c, "DROP TABLE cid_c");
        }
    }

    @Test
    void cminAndCmaxOfAVersionThisTransactionWroteAreTheSameCommand() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN", "CREATE TABLE cid_d (i int)", "INSERT INTO cid_d VALUES (1)",
                    "UPDATE cid_d SET i = 2");
            assertEquals(List.of("2|2|2"),
                    rows(c, "SELECT i, cmin::text, cmax::text FROM cid_d ORDER BY i"));
            run(c, "INSERT INTO cid_d VALUES (3)");
            assertEquals(List.of("2|2|2", "3|3|3"),
                    rows(c, "SELECT i, cmin::text, cmax::text FROM cid_d ORDER BY i"));
            run(c, "COMMIT", "DROP TABLE cid_d");
        }
    }

    // ============================================================ Locking a row a second session
    // is writing

    @Test
    void forUpdateAtSerializableWaitsForAConcurrentWriterAndThenRefuses() throws Exception {
        exec("CREATE TABLE flk_a (i int, s text)");
        exec("INSERT INTO flk_a VALUES (1,'a'),(2,'a')");
        assertEquals("ERR[40001] could not serialize access due to concurrent update",
                whileAnotherSessionHolds("SERIALIZABLE", "SELECT count(*) FROM flk_a",
                        "UPDATE flk_a SET s = 'b' WHERE i = 2", true,
                        "SELECT i, s FROM flk_a WHERE i = 2 FOR UPDATE"));
        // FOR SHARE is refused on the same ground.
        assertEquals("ERR[40001] could not serialize access due to concurrent update",
                whileAnotherSessionHolds("SERIALIZABLE", "SELECT count(*) FROM flk_a",
                        "UPDATE flk_a SET s = 'c' WHERE i = 2", true,
                        "SELECT i, s FROM flk_a WHERE i = 2 FOR SHARE"));
        exec("DROP TABLE flk_a");
    }

    @Test
    void forUpdateAtSerializableWaitsForAnUncommittedCrossPartitionMove() throws Exception {
        exec("CREATE TABLE flk_b (i int, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE flk_b_a PARTITION OF flk_b FOR VALUES IN ('a')");
        exec("CREATE TABLE flk_b_b PARTITION OF flk_b FOR VALUES IN ('b')");
        exec("INSERT INTO flk_b VALUES (1,'a'),(2,'a')");
        // The move takes the row out of one partition and writes it into another; the lock must
        // still be taken on the row rather than on the snapshot's own copy of it.
        assertEquals("ERR[40001] could not serialize access due to concurrent update",
                whileAnotherSessionHolds("SERIALIZABLE", "SELECT count(*) FROM flk_b",
                        "UPDATE flk_b SET s = 'b' WHERE i = 2", true,
                        "SELECT i, s FROM flk_b WHERE i = 2 FOR UPDATE"));
        assertEquals(List.of("1|flk_b_a|a", "2|flk_b_b|b"),
                rows("SELECT i, tableoid::regclass::text, s FROM flk_b ORDER BY i"));
        exec("DROP TABLE flk_b");
    }

    @Test
    void forUpdateAtRepeatableReadWaitsForAConcurrentDeleteAndThenRefuses() throws Exception {
        exec("CREATE TABLE flk_c (i int, s text)");
        exec("INSERT INTO flk_c VALUES (1,'a'),(2,'a')");
        assertEquals("ERR[40001] could not serialize access due to concurrent update",
                whileAnotherSessionHolds("REPEATABLE READ", "SELECT count(*) FROM flk_c",
                        "DELETE FROM flk_c WHERE i = 2", true,
                        "SELECT i, s FROM flk_c WHERE i = 2 FOR UPDATE"));
        exec("DROP TABLE flk_c");
    }

    @Test
    void forUpdateAtReadCommittedWaitsAndThenLocksTheVersionTheWriterLeft() throws Exception {
        exec("CREATE TABLE flk_d (i int, s text)");
        exec("INSERT INTO flk_d VALUES (1,'a'),(2,'a')");
        // A statement at READ COMMITTED takes its snapshot afresh, so it follows the writer's
        // version rather than being refused.
        assertEquals("2|b",
                whileAnotherSessionHolds("READ COMMITTED", "SELECT count(*) FROM flk_d",
                        "UPDATE flk_d SET s = 'b' WHERE i = 2", true,
                        "SELECT i, s FROM flk_d WHERE i = 2 FOR UPDATE"));
        exec("DROP TABLE flk_d");
    }

    @Test
    void forUpdateThatWaitedForACommittedCrossPartitionMoveIsToldTheTupleMoved() throws Exception {
        exec("CREATE TABLE flk_e (i int, s text) PARTITION BY LIST (s)");
        exec("CREATE TABLE flk_e_a PARTITION OF flk_e FOR VALUES IN ('a')");
        exec("CREATE TABLE flk_e_b PARTITION OF flk_e FOR VALUES IN ('b')");
        exec("INSERT INTO flk_e VALUES (1,'a'),(2,'a')");
        // A row is not followed into another partition, so the wording says so.
        assertEquals("ERR[40001] tuple to be locked was already moved to another partition due to"
                        + " concurrent update",
                whileAnotherSessionHolds("READ COMMITTED", "SELECT count(*) FROM flk_e",
                        "UPDATE flk_e SET s = 'b' WHERE i = 2", true,
                        "SELECT i, s FROM flk_e WHERE i = 2 FOR UPDATE"));
        exec("DROP TABLE flk_e");
    }

    @Test
    void forUpdateAfterTheWriterAbortsLocksTheRowItWaitedFor() throws Exception {
        exec("CREATE TABLE flk_f (i int, s text)");
        exec("INSERT INTO flk_f VALUES (1,'a'),(2,'a')");
        assertEquals("2|a",
                whileAnotherSessionHolds("SERIALIZABLE", "SELECT count(*) FROM flk_f",
                        "UPDATE flk_f SET s = 'b' WHERE i = 2", false,
                        "SELECT i, s FROM flk_f WHERE i = 2 FOR UPDATE"));
        assertEquals(List.of("1|a", "2|a"), rows("SELECT i, s FROM flk_f ORDER BY i"));
        exec("DROP TABLE flk_f");
    }

    @Test
    void nowaitAndSkipLockedDoNotWaitForTheWriterAtAll() throws SQLException {
        exec("CREATE TABLE flk_g (i int, s text)");
        exec("INSERT INTO flk_g VALUES (1,'a'),(2,'a')");
        String snapshot = "SELECT count(*) FROM flk_g";
        String write = "UPDATE flk_g SET s = 'b' WHERE i = 2";
        assertEquals("ERR[55P03] could not obtain lock on row in relation \"flk_g\"",
                withoutWaitingFor("SERIALIZABLE", snapshot, write,
                        "SELECT i, s FROM flk_g WHERE i = 2 FOR UPDATE NOWAIT"));
        assertEquals("(no rows)",
                withoutWaitingFor("SERIALIZABLE", snapshot, write,
                        "SELECT i, s FROM flk_g WHERE i = 2 FOR UPDATE SKIP LOCKED"));
        // A row the writer never touched is not held against the lock at all.
        assertEquals("1|a",
                withoutWaitingFor("SERIALIZABLE", snapshot, write,
                        "SELECT i, s FROM flk_g WHERE i = 1 FOR UPDATE"));
        assertEquals(List.of("1|a", "2|a"), rows("SELECT i, s FROM flk_g ORDER BY i"));
        exec("DROP TABLE flk_g");
    }

    // ============================================================ The line pointers a relation
    // built by a query hands out are the relation's own

    @Test
    void aRelationFilledByAQueryHandsOutTheLinePointersItUsed() throws SQLException {
        // CREATE TABLE ... AS fills the new relation through the routine an INSERT writes through,
        // so the rows the query wrote take the places in order and the relation goes on from there
        // rather than handing the first place out a second time.
        exec("CREATE TABLE lpr_q1 AS SELECT 8 AS i");
        exec("INSERT INTO lpr_q1 VALUES (9)");
        assertEquals(List.of("(0,1)|8", "(0,2)|9"),
                rows("SELECT ctid::text, i FROM lpr_q1 ORDER BY i"));
        exec("DROP TABLE lpr_q1");

        exec("CREATE TABLE lpr_q2 AS SELECT g FROM generate_series(1,3) g");
        exec("INSERT INTO lpr_q2 VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3", "(0,4)|9"),
                rows("SELECT ctid::text, g FROM lpr_q2 ORDER BY g"));
        // A place a delete gave up is not handed out again, and a tuple id names one row.
        exec("DELETE FROM lpr_q2 WHERE g = 2");
        exec("INSERT INTO lpr_q2 VALUES (10)");
        assertEquals(List.of("(0,1)|1", "(0,3)|3", "(0,4)|9", "(0,5)|10"),
                rows("SELECT ctid::text, g FROM lpr_q2 ORDER BY g"));
        assertEquals(4L, num("SELECT count(DISTINCT ctid) FROM lpr_q2"));
        assertEquals("9", scalar("SELECT g FROM lpr_q2 WHERE ctid = '(0,4)'"));
        exec("DROP TABLE lpr_q2");
    }

    @Test
    void aQueryThatWroteNothingLeavesTheFirstPlaceToTheNextRow() throws SQLException {
        exec("CREATE TABLE lpr_q3 AS SELECT 1 AS a WHERE false");
        exec("INSERT INTO lpr_q3 VALUES (5)");
        assertEquals(List.of("(0,1)|5"), rows("SELECT ctid::text, a FROM lpr_q3 ORDER BY a"));
        exec("DROP TABLE lpr_q3");

        // WITH NO DATA runs the query for its shape only, so no place has been handed out yet.
        exec("CREATE TABLE lpr_q4 AS SELECT g FROM generate_series(1,3) g WITH NO DATA");
        assertEquals(0L, num("SELECT count(*) FROM lpr_q4"));
        exec("INSERT INTO lpr_q4 VALUES (7)");
        exec("INSERT INTO lpr_q4 VALUES (8)");
        assertEquals(List.of("(0,1)|7", "(0,2)|8"),
                rows("SELECT ctid::text, g FROM lpr_q4 ORDER BY g"));
        exec("DROP TABLE lpr_q4");
    }

    @Test
    void selectIntoNumbersItsRowsTheSameWay() throws SQLException {
        exec("SELECT g INTO lpr_q5 FROM generate_series(1,2) g");
        exec("INSERT INTO lpr_q5 VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|9"),
                rows("SELECT ctid::text, g FROM lpr_q5 ORDER BY g"));
        exec("DELETE FROM lpr_q5 WHERE g = 1");
        exec("INSERT INTO lpr_q5 VALUES (11)");
        assertEquals(List.of("(0,2)|2", "(0,3)|9", "(0,4)|11"),
                rows("SELECT ctid::text, g FROM lpr_q5 ORDER BY g"));
        exec("DROP TABLE lpr_q5");

        // A SELECT ... INTO whose query wrote nothing leaves the first place to the next row.
        exec("SELECT g INTO lpr_q6 FROM generate_series(1,2) g WHERE false");
        exec("INSERT INTO lpr_q6 VALUES (5)");
        assertEquals(List.of("(0,1)|5"), rows("SELECT ctid::text, g FROM lpr_q6 ORDER BY g"));
        exec("DROP TABLE lpr_q6");
    }

    @Test
    void everyLaterWriteToARelationAQueryBuiltGoesOnFromWhereTheQueryLeftOff()
            throws SQLException {
        exec("CREATE TABLE lpr_q7 AS SELECT g FROM generate_series(1,3) g");
        exec("INSERT INTO lpr_q7 SELECT g FROM generate_series(4,6) g");
        exec("DELETE FROM lpr_q7 WHERE g IN (2,5)");
        exec("INSERT INTO lpr_q7 VALUES (7),(8)");
        assertEquals(List.of("(0,1)|1", "(0,3)|3", "(0,4)|4", "(0,6)|6", "(0,7)|7", "(0,8)|8"),
                rows("SELECT ctid::text, g FROM lpr_q7 ORDER BY g"));
        // No two rows answer to one place.
        assertEquals(List.of("6|6"),
                rows("SELECT count(*), count(DISTINCT ctid) FROM lpr_q7"));
        exec("DROP TABLE lpr_q7");
    }

    @Test
    void aDeleteByTupleIdAndAnUpdateOverRowsAQueryWrote() throws SQLException {
        exec("CREATE TABLE lpr_q8 AS SELECT g AS i, 'r' || g AS s FROM generate_series(1,4) g");
        // The tuple id the query handed the third row is the one that names it to a delete.
        assertEquals(1, update(conn, "DELETE FROM lpr_q8 WHERE ctid = '(0,3)'"));
        // An update writes a new version of the row, which lives at a new place.
        exec("UPDATE lpr_q8 SET s = 'z' WHERE i = 1");
        exec("INSERT INTO lpr_q8 VALUES (9,'n')");
        assertEquals(List.of("(0,5)|1|z", "(0,2)|2|r2", "(0,4)|4|r4", "(0,6)|9|n"),
                rows("SELECT ctid::text, i, s FROM lpr_q8 ORDER BY i"));
        exec("DROP TABLE lpr_q8");
    }

    @Test
    void aPlaceARowInARelationAQueryBuiltGaveUpIsNeverHandedOutAgain() throws SQLException {
        exec("CREATE TABLE lpr_q9 AS SELECT g FROM generate_series(1,5) g");
        exec("DELETE FROM lpr_q9");
        exec("INSERT INTO lpr_q9 VALUES (9)");
        assertEquals(List.of("(0,6)|9"), rows("SELECT ctid::text, g FROM lpr_q9 ORDER BY g"));
        exec("DROP TABLE lpr_q9");

        // A write that was rolled back had already spent its place, so the next row skips it.
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_qa AS SELECT g FROM generate_series(1,2) g",
                    "BEGIN", "INSERT INTO lpr_qa VALUES (8)", "ROLLBACK",
                    "INSERT INTO lpr_qa VALUES (9)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,4)|9"),
                    rows(c, "SELECT ctid::text, g FROM lpr_qa ORDER BY g"));
            run(c, "DROP TABLE lpr_qa");
        }
    }

    @Test
    void aMaterializedViewNumbersItsRowsFromOneAndARefreshBeginsAgain() throws SQLException {
        exec("CREATE TABLE lpr_qsrc (i int)");
        exec("INSERT INTO lpr_qsrc VALUES (1),(2),(3)");
        exec("CREATE MATERIALIZED VIEW lpr_qmv AS SELECT i FROM lpr_qsrc");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3"),
                rows("SELECT ctid::text, i FROM lpr_qmv ORDER BY i"));
        assertEquals(List.of("3|3"),
                rows("SELECT count(*), count(DISTINCT ctid) FROM lpr_qmv"));
        assertEquals("2", scalar("SELECT i FROM lpr_qmv WHERE ctid = '(0,2)'"));
        // A refresh fills the relation again from the first place, so a row the source lost does
        // not leave a gap behind it.
        exec("DELETE FROM lpr_qsrc WHERE i = 2");
        exec("REFRESH MATERIALIZED VIEW lpr_qmv");
        assertEquals(List.of("(0,1)|1", "(0,2)|3"),
                rows("SELECT ctid::text, i FROM lpr_qmv ORDER BY i"));
        exec("INSERT INTO lpr_qsrc VALUES (4),(5)");
        exec("REFRESH MATERIALIZED VIEW lpr_qmv");
        assertEquals(List.of("(0,1)|1", "(0,2)|3", "(0,3)|4", "(0,4)|5"),
                rows("SELECT ctid::text, i FROM lpr_qmv ORDER BY i"));
        assertEquals(4L, num("SELECT count(DISTINCT ctid) FROM lpr_qmv"));
        exec("DROP MATERIALIZED VIEW lpr_qmv");
        exec("DROP TABLE lpr_qsrc");
    }

    @Test
    void aMaterializedViewDefinedWithNoDataIsUnpopulatedUntilItIsRefreshed() throws SQLException {
        exec("CREATE TABLE lpr_qsr2 (i int)");
        exec("INSERT INTO lpr_qsr2 VALUES (7),(8)");
        exec("CREATE MATERIALIZED VIEW lpr_qmw AS SELECT i FROM lpr_qsr2 WITH NO DATA");
        assertEquals("55000", stateOf("SELECT i FROM lpr_qmw"));
        assertEquals("materialized view \"lpr_qmw\" has not been populated",
                messageOf("SELECT i FROM lpr_qmw"));
        assertEquals("Use the REFRESH MATERIALIZED VIEW command.", hintOf("SELECT i FROM lpr_qmw"));
        // A refresh that asks for no data leaves it unpopulated as well.
        exec("REFRESH MATERIALIZED VIEW lpr_qmw WITH NO DATA");
        assertEquals("55000", stateOf("SELECT count(*) FROM lpr_qmw"));
        // The refresh that does fill it hands its rows the places from the first.
        exec("REFRESH MATERIALIZED VIEW lpr_qmw");
        assertEquals(List.of("(0,1)|7", "(0,2)|8"),
                rows("SELECT ctid::text, i FROM lpr_qmw ORDER BY i"));
        exec("DROP MATERIALIZED VIEW lpr_qmw");
        exec("DROP TABLE lpr_qsr2");
    }

    @Test
    void aMaterializedViewTakesNoWriteOfItsOwn() throws SQLException {
        exec("CREATE TABLE lpr_qsr3 (i int)");
        exec("INSERT INTO lpr_qsr3 VALUES (1),(2)");
        exec("CREATE MATERIALIZED VIEW lpr_qmx AS SELECT i FROM lpr_qsr3");
        assertEquals("42809", stateOf("INSERT INTO lpr_qmx VALUES (9)"));
        assertEquals("42809", stateOf("DELETE FROM lpr_qmx WHERE i = 1"));
        assertEquals("42809", stateOf("UPDATE lpr_qmx SET i = 3 WHERE i = 1"));
        assertEquals("cannot change materialized view \"lpr_qmx\"",
                messageOf("DELETE FROM lpr_qmx WHERE i = 1"));
        // TRUNCATE is refused because a materialized view is not a table.
        assertEquals("\"lpr_qmx\" is not a table", messageOf("TRUNCATE lpr_qmx"));
        assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                rows("SELECT ctid::text, i FROM lpr_qmx ORDER BY i"));
        exec("DROP MATERIALIZED VIEW lpr_qmx");
        exec("DROP TABLE lpr_qsr3");
    }

    @Test
    void aRelationBuiltFromAMaterializedViewNumbersItsOwnRowsFromOne() throws SQLException {
        exec("CREATE TABLE lpr_qsr4 (i int)");
        exec("INSERT INTO lpr_qsr4 VALUES (1),(2)");
        exec("CREATE MATERIALIZED VIEW lpr_qmy AS SELECT i FROM lpr_qsr4");
        exec("CREATE TABLE lpr_qb AS SELECT i FROM lpr_qmy");
        exec("INSERT INTO lpr_qb VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|9"),
                rows("SELECT ctid::text, i FROM lpr_qb ORDER BY i"));
        exec("DROP TABLE lpr_qb");
        exec("DROP MATERIALIZED VIEW lpr_qmy");
        exec("DROP TABLE lpr_qsr4");
    }

    @Test
    void truncateHandsARelationAQueryBuiltANewFile() throws SQLException {
        exec("CREATE TABLE lpr_qc AS SELECT g FROM generate_series(1,2) g");
        exec("TRUNCATE lpr_qc");
        exec("INSERT INTO lpr_qc VALUES (5)");
        assertEquals(List.of("(0,1)|5"), rows("SELECT ctid::text, g FROM lpr_qc ORDER BY g"));
        exec("DROP TABLE lpr_qc");
    }

    @Test
    void aRelationAQueryBuiltInAnotherSchemaOrAsATemporaryOneCountsTheSame() throws SQLException {
        exec("CREATE SCHEMA lpr_qs");
        exec("CREATE TABLE lpr_qs.lpr_qd AS SELECT g FROM generate_series(1,2) g");
        exec("INSERT INTO lpr_qs.lpr_qd VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|9"),
                rows("SELECT ctid::text, g FROM lpr_qs.lpr_qd ORDER BY g"));
        exec("DROP SCHEMA lpr_qs CASCADE");

        try (Connection c = session()) {
            run(c, "CREATE TEMP TABLE lpr_qe AS SELECT g FROM generate_series(1,2) g",
                    "INSERT INTO lpr_qe VALUES (9)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|9"),
                    rows(c, "SELECT ctid::text, g FROM lpr_qe ORDER BY g"));
            run(c, "DROP TABLE lpr_qe");
        }
    }

    @Test
    void aRelationTheTransactionNeverLeftBehindBeginsAtOne() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN", "CREATE TABLE lpr_qf AS SELECT 1 AS a",
                    "INSERT INTO lpr_qf VALUES (2)");
            assertEquals(List.of("(0,1)|1", "(0,2)|2"),
                    rows(c, "SELECT ctid::text, a FROM lpr_qf ORDER BY a"));
            // The relation went with the abort, so the name reaches a file of its own afterwards.
            run(c, "ROLLBACK", "CREATE TABLE lpr_qf AS SELECT 5 AS a",
                    "INSERT INTO lpr_qf VALUES (6)");
            assertEquals(List.of("(0,1)|5", "(0,2)|6"),
                    rows(c, "SELECT ctid::text, a FROM lpr_qf ORDER BY a"));
            run(c, "DROP TABLE lpr_qf");
        }
    }

    @Test
    void rowsAQueryWroteWithTheSameValuesStillHavePlacesOfTheirOwn() throws SQLException {
        exec("CREATE TABLE lpr_qg AS SELECT 1 AS a UNION ALL SELECT 1");
        exec("INSERT INTO lpr_qg VALUES (1)");
        assertEquals(List.of("(0,1)|1", "(0,2)|1", "(0,3)|1"),
                rows("SELECT ctid::text, a FROM lpr_qg ORDER BY ctid"));
        assertEquals(3L, num("SELECT count(DISTINCT ctid) FROM lpr_qg"));
        exec("DROP TABLE lpr_qg");
    }

    @Test
    void thePlacesGoOutInTheOrderTheQueryProducedTheRows() throws SQLException {
        exec("CREATE TABLE lpr_qh AS WITH c AS (SELECT g FROM generate_series(1,3) g)"
                + " SELECT g FROM c ORDER BY g DESC");
        exec("INSERT INTO lpr_qh VALUES (9)");
        assertEquals(List.of("(0,1)|3", "(0,2)|2", "(0,3)|1", "(0,4)|9"),
                rows("SELECT ctid::text, g FROM lpr_qh ORDER BY ctid"));
        exec("DROP TABLE lpr_qh");

        exec("CREATE TABLE lpr_qi AS SELECT g FROM generate_series(1,10) g ORDER BY g LIMIT 3");
        exec("INSERT INTO lpr_qi VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3", "(0,4)|9"),
                rows("SELECT ctid::text, g FROM lpr_qi ORDER BY ctid"));
        exec("DROP TABLE lpr_qi");
    }

    @Test
    void theTupleIdsTheQueryReadAreValuesAndNotThePlacesTheNewRowsTake() throws SQLException {
        exec("CREATE TABLE lpr_qj (i int)");
        exec("INSERT INTO lpr_qj VALUES (1),(2),(3)");
        exec("DELETE FROM lpr_qj WHERE i = 1");
        exec("CREATE TABLE lpr_qk AS SELECT ctid::text AS oldplace, i FROM lpr_qj");
        exec("INSERT INTO lpr_qk VALUES ('x', 9)");
        assertEquals(List.of("(0,1)|(0,2)|2", "(0,2)|(0,3)|3", "(0,3)|x|9"),
                rows("SELECT ctid::text, oldplace, i FROM lpr_qk ORDER BY i"));
        exec("DROP TABLE lpr_qk");
        exec("DROP TABLE lpr_qj");
    }

    @Test
    void twoRelationsBuiltByQueriesCountTheirPlacesApart() throws SQLException {
        exec("CREATE TABLE lpr_ql AS SELECT g FROM generate_series(1,2) g");
        exec("CREATE TABLE lpr_qm AS SELECT g FROM generate_series(1,3) g");
        exec("INSERT INTO lpr_ql VALUES (9)");
        exec("INSERT INTO lpr_qm VALUES (9)");
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|9"),
                rows("SELECT ctid::text, g FROM lpr_ql ORDER BY g"));
        assertEquals(List.of("(0,1)|1", "(0,2)|2", "(0,3)|3", "(0,4)|9"),
                rows("SELECT ctid::text, g FROM lpr_qm ORDER BY g"));
        exec("DROP TABLE lpr_ql");
        exec("DROP TABLE lpr_qm");
    }

    @Test
    void aRelationAPreparedStatementBuiltCountsTheSame() throws SQLException {
        exec("PREPARE lpr_qp AS SELECT 3 AS a");
        exec("CREATE TABLE lpr_qn AS EXECUTE lpr_qp");
        exec("INSERT INTO lpr_qn VALUES (4)");
        assertEquals(List.of("(0,1)|3", "(0,2)|4"),
                rows("SELECT ctid::text, a FROM lpr_qn ORDER BY a"));
        exec("DROP TABLE lpr_qn");
        exec("DEALLOCATE lpr_qp");
    }

    @Test
    void theRowsAQueryWroteCarryTheCommandIdentifierAfterTheStatementsOwn() throws SQLException {
        // The counter moves on once the relation's catalogue rows are written, so the rows go in
        // under the identifier after the one the statement began with.
        exec("CREATE TABLE lpr_qo AS SELECT g FROM generate_series(1,2) g");
        assertEquals(List.of("1|1|1|0", "2|1|1|0"),
                rows("SELECT g, cmin::text, cmax::text, xmax::text FROM lpr_qo ORDER BY g"));
        // The INSERT is a transaction of its own, so it starts the counter again.
        exec("INSERT INTO lpr_qo VALUES (7)");
        assertEquals(List.of("1|1", "2|1", "7|0"),
                rows("SELECT g, cmin::text FROM lpr_qo ORDER BY g"));
        exec("DROP TABLE lpr_qo");

        // Inside one transaction the statement spends two identifiers, so the statement after it
        // reads one more than a plain CREATE TABLE would have left.
        try (Connection c = session()) {
            run(c, "CREATE TABLE lpr_qr (a int)", "BEGIN", "INSERT INTO lpr_qr VALUES (0)",
                    "CREATE TABLE lpr_qt AS SELECT 7 AS b", "INSERT INTO lpr_qr VALUES (1)");
            assertEquals(List.of("0|0", "1|3"),
                    rows(c, "SELECT a, cmin::text FROM lpr_qr ORDER BY a"));
            assertEquals(List.of("7|2|(0,1)"),
                    rows(c, "SELECT b, cmin::text, ctid::text FROM lpr_qt"));
            run(c, "COMMIT", "DROP TABLE lpr_qt", "DROP TABLE lpr_qr");
        }
    }

    // ------------------------------------------------------------ helpers for the sections below

    /** The fields of the error a statement raises on a session of its own. */
    private static org.postgresql.util.ServerErrorMessage fieldsOn(Connection c, String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> run(c, sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /**
     * A write issued on one session while a second session holds the row it names, so it blocks.
     * The holder finishes with {@code finish}, and what the write answered once it got through is
     * returned -- "[n rows]" or "ERR[sqlstate] message". Every wait carries a deadline, so a write
     * that never gets through fails the test rather than hanging it.
     */
    private static String blockedBy(Connection blocked, String write, Connection holder,
            String finish) throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<String> waiting = pool.submit(new Callable<String>() {
                @Override
                public String call() {
                    try (Statement s = blocked.createStatement()) {
                        s.setQueryTimeout(20);
                        return "[" + s.executeUpdate(write) + " rows]";
                    } catch (SQLException e) {
                        return "ERR[" + e.getSQLState() + "] " + primary(e);
                    }
                }
            });
            Thread.sleep(400);
            assertFalse(waiting.isDone(), "the write did not wait for the other session");
            run(holder, finish);
            return waiting.get(20, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    /** The fixture the rollback tests are written against: a keyed relation and its journal. */
    private static void journalled(Connection c, String name) throws SQLException {
        run(c, "CREATE TABLE " + name + " (i int PRIMARY KEY, s text)",
                "CREATE TABLE " + name + "_log (i int, tag text)",
                "CREATE FUNCTION " + name + "_note() RETURNS trigger AS $$ BEGIN INSERT INTO "
                        + name + "_log VALUES (NEW.i, 'ins'); RETURN NEW; END $$ LANGUAGE plpgsql",
                "CREATE TRIGGER " + name + "_tr AFTER INSERT ON " + name
                        + " FOR EACH ROW EXECUTE FUNCTION " + name + "_note()",
                "INSERT INTO " + name + " VALUES (2,'base')");
    }

    private static void dropJournalled(Connection c, String name) throws SQLException {
        run(c, "DROP TRIGGER " + name + "_tr ON " + name, "DROP FUNCTION " + name + "_note()",
                "DROP TABLE " + name, "DROP TABLE " + name + "_log");
    }

    // ============================================================ What a ROLLBACK undoes

    @Test
    void aRollbackTakesBackTheRowAStatementThatSucceededWroteBeforeTheFailingOne()
            throws SQLException {
        try (Connection c = session()) {
            journalled(c, "rbu_a");
            // The journal holds what the insert before the block wrote, and goes on holding it.
            assertEquals(List.of("2|ins"), rows(c, "SELECT i, tag FROM rbu_a_log ORDER BY i"));
            run(c, "BEGIN");
            try {
                run(c, "INSERT INTO rbu_a VALUES (9,'nine')");
                org.postgresql.util.ServerErrorMessage e =
                        fieldsOn(c, "INSERT INTO rbu_a VALUES (1,'a'),(2,'b'),(3,'c')");
                assertEquals("23505", e.getSQLState());
                assertEquals("duplicate key value violates unique constraint \"rbu_a_pkey\"",
                        e.getMessage());
                assertEquals("Key (i)=(2) already exists.", e.getDetail());
                assertNull(e.getHint(), "a duplicate key carries no hint");
            } finally {
                run(c, "ROLLBACK");
            }
            // Nothing of the transaction is left: not the row the first statement wrote, and not
            // what its trigger wrote into the other relation.
            assertEquals(List.of("1"), rows(c, "SELECT count(*) FROM rbu_a"));
            assertEquals(List.of("2|base"), rows(c, "SELECT i, s FROM rbu_a ORDER BY i"));
            assertEquals(List.of("2|ins"), rows(c, "SELECT i, tag FROM rbu_a_log ORDER BY i"));
            dropJournalled(c, "rbu_a");
        }
    }

    @Test
    void aTransactionThatCommitsKeepsEveryWriteItMade() throws SQLException {
        try (Connection c = session()) {
            journalled(c, "rbu_b");
            run(c, "BEGIN", "INSERT INTO rbu_b VALUES (30,'p')", "INSERT INTO rbu_b VALUES (31,'q')",
                    "UPDATE rbu_b SET s = 'r' WHERE i = 30", "DELETE FROM rbu_b WHERE i = 31",
                    "COMMIT");
            assertEquals(List.of("2|base", "30|r"), rows(c, "SELECT i, s FROM rbu_b ORDER BY i"));
            assertEquals(List.of("2|ins", "30|ins", "31|ins"),
                    rows(c, "SELECT i, tag FROM rbu_b_log ORDER BY i"));
            dropJournalled(c, "rbu_b");
        }
    }

    @Test
    void aStatementOutsideAnyBlockKeepsTheRowItWroteAndAFailingOneLeavesNothing()
            throws SQLException {
        try (Connection c = session()) {
            journalled(c, "rbu_c");
            run(c, "INSERT INTO rbu_c VALUES (42,'m')");
            assertEquals(List.of("2|base", "42|m"), rows(c, "SELECT i, s FROM rbu_c ORDER BY i"));
            assertEquals(List.of("2|ins", "42|ins"),
                    rows(c, "SELECT i, tag FROM rbu_c_log ORDER BY i"));
            // A statement is a transaction of its own, so the row it wrote before it was refused
            // goes back with it -- and so does the journal entry its trigger made.
            assertEquals("23505", stateOn(c, "INSERT INTO rbu_c VALUES (40,'k'),(2,'dup'),(41,'l')"));
            assertEquals(List.of("2|base", "42|m"), rows(c, "SELECT i, s FROM rbu_c ORDER BY i"));
            assertEquals(List.of("2|ins", "42|ins"),
                    rows(c, "SELECT i, tag FROM rbu_c_log ORDER BY i"));
            dropJournalled(c, "rbu_c");
        }
    }

    @Test
    void aSavepointRolledBackToTakesBackWhatFollowedItAndAReleasedOneKeepsIt() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rbu_d (i int PRIMARY KEY, s text)",
                    "INSERT INTO rbu_d VALUES (2,'base')");
            run(c, "BEGIN");
            try {
                run(c, "INSERT INTO rbu_d VALUES (20,'x')", "SAVEPOINT sp1",
                        "INSERT INTO rbu_d VALUES (21,'y')");
                assertEquals("23505",
                        stateOn(c, "INSERT INTO rbu_d VALUES (22,'f'),(2,'dup'),(23,'g')"));
                run(c, "ROLLBACK TO SAVEPOINT sp1");
                // What the savepoint covered is gone; what came before it is still the
                // transaction's.
                assertEquals(List.of("2", "20"), rows(c, "SELECT i FROM rbu_d ORDER BY i"));
                run(c, "INSERT INTO rbu_d VALUES (24,'z')", "SAVEPOINT sp2",
                        "INSERT INTO rbu_d VALUES (25,'w')", "RELEASE SAVEPOINT sp2");
                assertEquals(List.of("2", "20", "24", "25"),
                        rows(c, "SELECT i FROM rbu_d ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
            // A release settles nothing: the outer ROLLBACK takes the released work back too.
            assertEquals(List.of("1"), rows(c, "SELECT count(*) FROM rbu_d"));
            assertEquals(List.of("2|base"), rows(c, "SELECT i, s FROM rbu_d ORDER BY i"));
            run(c, "DROP TABLE rbu_d");
        }
    }

    @Test
    void aCommitOfATransactionAnErrorHasAbortedTakesNothingWithIt() throws SQLException {
        try (Connection c = session()) {
            journalled(c, "rbu_e");
            run(c, "BEGIN", "INSERT INTO rbu_e VALUES (9,'nine')");
            assertEquals("23505", stateOn(c, "INSERT INTO rbu_e VALUES (1,'a'),(2,'b'),(3,'c')"));
            run(c, "COMMIT");
            assertEquals(List.of("2"), rows(c, "SELECT i FROM rbu_e ORDER BY i"));
            assertEquals(List.of("2"), rows(c, "SELECT i FROM rbu_e_log ORDER BY i"));
            dropJournalled(c, "rbu_e");
        }
    }

    @Test
    void aSavepointTakenBeforeTheFailureLetsTheRestOfTheTransactionCommit() throws SQLException {
        try (Connection c = session()) {
            journalled(c, "rbu_f");
            run(c, "BEGIN", "INSERT INTO rbu_f VALUES (9,'nine')", "SAVEPOINT s1");
            assertEquals("23505", stateOn(c, "INSERT INTO rbu_f VALUES (1,'a'),(2,'b'),(3,'c')"));
            run(c, "ROLLBACK TO SAVEPOINT s1", "INSERT INTO rbu_f VALUES (10,'ten')", "COMMIT");
            assertEquals(List.of("2", "9", "10"), rows(c, "SELECT i FROM rbu_f ORDER BY i"));
            assertEquals(List.of("2", "9", "10"), rows(c, "SELECT i FROM rbu_f_log ORDER BY i"));
            dropJournalled(c, "rbu_f");
        }
    }

    @Test
    void aRolledBackUpdateLeavesEveryRowAtTheLinePointerItAlwaysHad() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rbu_g (i int PRIMARY KEY, s text)",
                    "CREATE FUNCTION rbu_g_stop() RETURNS trigger AS $$ BEGIN IF NEW.s = 'no'"
                            + " THEN RAISE EXCEPTION 'no'; END IF; RETURN NEW; END $$"
                            + " LANGUAGE plpgsql",
                    "INSERT INTO rbu_g VALUES (1,'a'),(2,'b'),(3,'c')",
                    "CREATE TRIGGER rbu_g_tr BEFORE UPDATE ON rbu_g FOR EACH ROW"
                            + " EXECUTE FUNCTION rbu_g_stop()");
            run(c, "BEGIN");
            try {
                run(c, "UPDATE rbu_g SET s = 'x' WHERE i = 1", "INSERT INTO rbu_g VALUES (4,'d')");
                assertEquals("P0001",
                        stateOn(c, "UPDATE rbu_g SET s = CASE WHEN i = 3 THEN 'no' ELSE 'y' END"));
            } finally {
                run(c, "ROLLBACK");
            }
            // An abort renumbers nothing: the version each row had is live again, at the line
            // pointer it always had, still naming the transaction that wrote it.
            assertEquals(List.of("3"), rows(c, "SELECT count(*) FROM rbu_g"));
            assertEquals(List.of("1|a|(0,1)|t", "2|b|(0,2)|t", "3|c|(0,3)|t"),
                    rows(c, "SELECT i, s, ctid, xmin::text <> '0' FROM rbu_g ORDER BY i"));
            run(c, "DROP TRIGGER rbu_g_tr ON rbu_g", "DROP FUNCTION rbu_g_stop()",
                    "DROP TABLE rbu_g");
        }
    }

    // ============================================================ What a second session sees of a
    // statement that failed and was taken back

    @Test
    void aRowAStatementThatFailedWroteIsNeverVisibleToAnotherSession() throws SQLException {
        try (Connection writer = session(); Connection reader = session()) {
            journalled(writer, "rbu_h");
            run(writer, "BEGIN");
            try {
                run(writer, "INSERT INTO rbu_h VALUES (9,'nine')");
                assertEquals("2", promptly(reader, "SELECT i FROM rbu_h ORDER BY i"));
                assertEquals("23505",
                        stateOn(writer, "INSERT INTO rbu_h VALUES (1,'a'),(2,'dup'),(3,'c')"));
                // The failed statement's undo does not hand the other session the uncommitted row
                // its transaction had written before it.
                assertEquals("2", promptly(reader, "SELECT i FROM rbu_h ORDER BY i"));
                assertEquals("1", promptly(reader, "SELECT count(*) FROM rbu_h_log"));
            } finally {
                run(writer, "ROLLBACK");
            }
            assertEquals("2", promptly(writer, "SELECT i FROM rbu_h ORDER BY i"));
            assertEquals("2", promptly(reader, "SELECT i FROM rbu_h ORDER BY i"));
            assertEquals("1", promptly(reader, "SELECT count(*) FROM rbu_h_log"));
            dropJournalled(writer, "rbu_h");
        }
    }

    @Test
    void aWriteThatWaitedForTheAbortedTransactionActsOnTheRowItWaitedFor() throws Exception {
        try (Connection holder = session(); Connection waiter = session()) {
            run(holder, "CREATE TABLE rbu_i (i int PRIMARY KEY, s text)",
                    "INSERT INTO rbu_i VALUES (1,'one')");
            run(holder, "BEGIN", "INSERT INTO rbu_i VALUES (9,'nine')");
            assertEquals("23505",
                    stateOn(holder, "INSERT INTO rbu_i VALUES (2,'a'),(1,'dup'),(3,'c')"));
            run(holder, "ROLLBACK");
            // A write still waits for a row a second session is holding, and still gets through
            // when that session takes its own write back.
            run(holder, "BEGIN", "UPDATE rbu_i SET s = 'x' WHERE i = 1");
            assertEquals("[1 rows]", blockedBy(waiter, "UPDATE rbu_i SET s = 'y' WHERE i = 1",
                    holder, "ROLLBACK"));
            // The waiter's write stands on the row it waited for, and the aborted transaction left
            // nothing beside it.
            assertEquals(List.of("1|y"), rows(holder, "SELECT i, s FROM rbu_i ORDER BY i"));
            assertEquals(List.of("1|y"), rows(waiter, "SELECT i, s FROM rbu_i ORDER BY i"));
            run(holder, "DROP TABLE rbu_i");
        }
    }

    // ============================================================ What a row remembers through a
    // rename

    @Test
    void aRenameLeavesEveryRowsPlaceItsWriterAndItsCommandIdentifierAlone() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rid_a (i int, s text)",
                    "INSERT INTO rid_a VALUES (1,'a'),(2,'b'),(3,'c')",
                    "DELETE FROM rid_a WHERE i = 2", "UPDATE rid_a SET s = 'cc' WHERE i = 3");
            // The update wrote a fourth version, which sits after the three the insert wrote.
            assertEquals(List.of("1|(0,1)|t|0|f|rid_a", "3|(0,4)|t|0|f|rid_a"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', xmax, cmin::text <> '0',"
                            + " tableoid::regclass::text FROM rid_a ORDER BY i"));
            run(c, "ALTER TABLE rid_a RENAME TO rid_a2");
            // A rename rewrites no tuple: every one of the six answers as it did, and only the
            // name the relation goes by has moved.
            assertEquals(List.of("1|(0,1)|t|0|f|rid_a2", "3|(0,4)|t|0|f|rid_a2"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', xmax, cmin::text <> '0',"
                            + " tableoid::regclass::text FROM rid_a2 ORDER BY i"));
            run(c, "DROP TABLE rid_a2");
        }
    }

    @Test
    void aMoveToAnotherSchemaAndARenameOfTheSchemaLeaveThemAloneToo() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rid_b (i int, s text)",
                    "INSERT INTO rid_b VALUES (1,'a'),(2,'b'),(3,'c')",
                    "DELETE FROM rid_b WHERE i = 2", "UPDATE rid_b SET s = 'cc' WHERE i = 3",
                    "CREATE SCHEMA rid_sc", "ALTER TABLE rid_b SET SCHEMA rid_sc");
            assertEquals(List.of("1|(0,1)|t|0|f|rid_sc.rid_b", "3|(0,4)|t|0|f|rid_sc.rid_b"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', xmax, cmin::text <> '0',"
                            + " tableoid::regclass::text FROM rid_sc.rid_b ORDER BY i"));
            run(c, "ALTER SCHEMA rid_sc RENAME TO rid_sc2");
            assertEquals(List.of("1|(0,1)|t|0|f|rid_sc2.rid_b", "3|(0,4)|t|0|f|rid_sc2.rid_b"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', xmax, cmin::text <> '0',"
                            + " tableoid::regclass::text FROM rid_sc2.rid_b ORDER BY i"));
            // The relation goes on numbering from where it left off, and every place is its own.
            run(c, "INSERT INTO rid_sc2.rid_b VALUES (4,'d')");
            assertEquals(List.of("1|(0,1)|t", "3|(0,4)|t", "4|(0,5)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_sc2.rid_b ORDER BY i"));
            assertEquals(List.of("3"), rows(c, "SELECT count(DISTINCT ctid) FROM rid_sc2.rid_b"));
            assertEquals(List.of("3"), rows(c, "SELECT i FROM rid_sc2.rid_b WHERE ctid = '(0,4)'"));
            run(c, "DROP SCHEMA rid_sc2 CASCADE");
        }
    }

    @Test
    void aRenameInsideTheTransactionThatWroteTheRowsKeepsTheirCommandIdentifiers()
            throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN", "CREATE TABLE rid_c (i int)", "INSERT INTO rid_c VALUES (1)",
                    "INSERT INTO rid_c VALUES (2)", "ALTER TABLE rid_c RENAME TO rid_c2");
            assertEquals(List.of("1|(0,1)|t|1", "2|(0,2)|t|2"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', cmin FROM rid_c2 ORDER BY i"));
            run(c, "COMMIT");
            assertEquals(List.of("1|(0,1)|t", "2|(0,2)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_c2 ORDER BY i"));
            run(c, "DROP TABLE rid_c2");
        }
    }

    @Test
    void aRenameThatIsRolledBackLeavesTheRowsAnsweringUnderTheNameTheyHad() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rid_d (i int)", "INSERT INTO rid_d VALUES (1),(2),(3)",
                    "DELETE FROM rid_d WHERE i = 1");
            run(c, "BEGIN", "ALTER TABLE rid_d RENAME TO rid_d2");
            assertEquals(List.of("2|(0,2)|t", "3|(0,3)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_d2 ORDER BY i"));
            run(c, "ROLLBACK");
            assertEquals(List.of("2|(0,2)|t", "3|(0,3)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_d ORDER BY i"));
            // A relation made again under a name that has been used before starts clean.
            run(c, "DROP TABLE rid_d", "CREATE TABLE rid_d (i int)",
                    "INSERT INTO rid_d VALUES (7),(8)");
            assertEquals(List.of("7|(0,1)|t", "8|(0,2)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_d ORDER BY i"));
            run(c, "DROP TABLE rid_d");
        }
    }

    @Test
    void aRenamedInheritanceChildStillNamesItselfThroughItsParent() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rid_e (i int)", "INSERT INTO rid_e VALUES (7),(8)",
                    "CREATE TABLE rid_ek (j int) INHERITS (rid_e)",
                    "INSERT INTO rid_ek VALUES (9, 90)", "ALTER TABLE rid_ek RENAME TO rid_ek2");
            assertEquals(List.of("7|(0,1)|t|rid_e", "8|(0,2)|t|rid_e", "9|(0,1)|t|rid_ek2"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0', tableoid::regclass::text"
                            + " FROM rid_e ORDER BY i"));
            assertEquals(List.of("9|(0,1)|t"),
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_ek2 ORDER BY i"));
            run(c, "DROP TABLE rid_ek2", "DROP TABLE rid_e");
        }
    }

    @Test
    void aSnapshotTakenBeforeARenameGoesOnAnsweringWithThePlacesItHad() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE rid_f (i int, s text)",
                    "INSERT INTO rid_f VALUES (1,'a'),(2,'b'),(3,'c')",
                    "UPDATE rid_f SET s = 'bb' WHERE i = 2",
                    "BEGIN ISOLATION LEVEL REPEATABLE READ");
            List<String> before =
                    rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_f ORDER BY i");
            assertEquals(List.of("1|(0,1)|t", "2|(0,4)|t", "3|(0,3)|t"), before);
            run(c, "ALTER TABLE rid_f RENAME TO rid_f2");
            assertEquals(before, rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_f2 ORDER BY i"));
            run(c, "COMMIT");
            assertEquals(before, rows(c, "SELECT i, ctid, xmin::text <> '0' FROM rid_f2 ORDER BY i"));
            run(c, "DROP TABLE rid_f2");
        }
    }

    // ============================================================ What a definition's shape costs
    // in command identifiers

    @Test
    void cminCountsTheToastTableAColumnsWidthEarnedTheDefinition() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN");
            try {
                // A relation whose widest row would not fit in a quarter of a page is given a
                // TOAST table, which is four identifiers more: varchar(501) still fits and
                // varchar(502) does not.
                run(c, "CREATE TABLE csz_a (i int)",
                        "CREATE TABLE csz_a1 (i int)", "INSERT INTO csz_a VALUES (1)",
                        "CREATE TABLE csz_a2 (t text)", "INSERT INTO csz_a VALUES (2)",
                        "CREATE TABLE csz_a3 (v varchar(10))", "INSERT INTO csz_a VALUES (3)",
                        "CREATE TABLE csz_a4 (n numeric)", "INSERT INTO csz_a VALUES (4)",
                        "CREATE TABLE csz_a5 (n numeric(5,2))", "INSERT INTO csz_a VALUES (5)",
                        "CREATE TABLE csz_a6 (v varchar(501))", "INSERT INTO csz_a VALUES (6)",
                        "CREATE TABLE csz_a7 (v varchar(502))", "INSERT INTO csz_a VALUES (7)",
                        "CREATE TABLE csz_a8 (a int[])", "INSERT INTO csz_a VALUES (8)",
                        "CREATE TABLE csz_a9 (u uuid)", "INSERT INTO csz_a VALUES (9)");
                assertEquals(
                        List.of("1|2", "2|8", "3|10", "4|16", "5|18", "6|20", "7|26", "8|32",
                                "9|34"),
                        rows(c, "SELECT i, cmin::text FROM csz_a ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
        }
    }

    @Test
    void cminCountsTheKeysDefaultsAndSequencesADefinitionWrote() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN");
            try {
                // Two CHECKs cost what one costs, and so do two defaults and two NOT NULLs: each
                // group is written in one stretch. A default and a NOT NULL are written apart, so
                // together they cost two.
                run(c, "CREATE TABLE csz_b (i int)",
                        "CREATE TABLE csz_b1 (i int primary key)", "INSERT INTO csz_b VALUES (1)",
                        "CREATE TABLE csz_b2 (i int unique)", "INSERT INTO csz_b VALUES (2)",
                        "CREATE TABLE csz_b3 (i int check (i > 0), j int check (j > 0))",
                        "INSERT INTO csz_b VALUES (3)",
                        "CREATE TABLE csz_b4 (i int default 1, j int default 2)",
                        "INSERT INTO csz_b VALUES (4)",
                        "CREATE TABLE csz_b5 (i int not null, j int not null)",
                        "INSERT INTO csz_b VALUES (5)",
                        "CREATE TABLE csz_b6 (i int not null default 1)",
                        "INSERT INTO csz_b VALUES (6)",
                        "CREATE TABLE csz_b7 (i int primary key, j int unique)",
                        "INSERT INTO csz_b VALUES (7)",
                        "CREATE TABLE csz_b8 (t text primary key)", "INSERT INTO csz_b VALUES (8)",
                        "CREATE TABLE csz_b9 (i serial)", "INSERT INTO csz_b VALUES (9)",
                        "CREATE TABLE csz_b10 (i int generated always as identity)",
                        "INSERT INTO csz_b VALUES (10)",
                        "CREATE TABLE csz_b11 (i int, exclude (i with =))",
                        "INSERT INTO csz_b VALUES (11)");
                assertEquals(
                        List.of("1|5", "2|9", "3|12", "4|15", "5|18", "6|22", "7|29", "8|38",
                                "9|45", "10|51", "11|55"),
                        rows(c, "SELECT i, cmin::text FROM csz_b ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
        }
    }

    @Test
    void cminCountsWhatAForeignKeyAndAnInheritedDefinitionCost() throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TABLE csz_cr (i int primary key)", "CREATE TABLE csz_cs (i int)");
            run(c, "BEGIN");
            try {
                // A key writes its row and the triggers that enforce it, and marks the relation it
                // points at as carrying triggers -- which the second key pointing at the same
                // relation no longer has to do. What a table takes from its parents is written
                // where its own constraints are, so INHERITS costs what a CHECK costs and the two
                // together cost one.
                run(c, "CREATE TABLE csz_c (i int)",
                        "CREATE TABLE csz_c1 (i int references csz_cr(i))",
                        "INSERT INTO csz_c VALUES (1)",
                        "CREATE TABLE csz_c2 (i int references csz_cr(i),"
                                + " j int references csz_cr(i))",
                        "INSERT INTO csz_c VALUES (2)",
                        "CREATE TABLE csz_c3 () INHERITS (csz_cs)", "INSERT INTO csz_c VALUES (3)",
                        "CREATE TABLE csz_c4 (j int check (j > 0)) INHERITS (csz_cs)",
                        "INSERT INTO csz_c VALUES (4)",
                        "CREATE TABLE csz_c5 (j int default 1) INHERITS (csz_cs)",
                        "INSERT INTO csz_c VALUES (5)");
                assertEquals(List.of("1|8", "2|19", "3|22", "4|25", "5|29"),
                        rows(c, "SELECT i, cmin::text FROM csz_c ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
            run(c, "DROP TABLE csz_cs CASCADE", "DROP TABLE csz_cr");
        }
    }

    @Test
    void theFirstTemporaryRelationASessionMakesWritesTheSchemaItLivesIn() throws SQLException {
        try (Connection c = session()) {
            run(c, "BEGIN");
            try {
                // The schema a session's temporary relations live in comes into being with the
                // first of them, and is written as any other schema is. The second temporary
                // relation finds it already there.
                run(c, "CREATE TABLE csz_d (i int)",
                        "CREATE TEMP TABLE csz_d1 (i int)", "INSERT INTO csz_d VALUES (1)",
                        "CREATE TEMP TABLE csz_d2 (i int)", "INSERT INTO csz_d VALUES (2)",
                        "CREATE TEMP TABLE csz_d3 (t text)", "INSERT INTO csz_d VALUES (3)");
                assertEquals(List.of("1|4", "2|6", "3|12"),
                        rows(c, "SELECT i, cmin::text FROM csz_d ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
        }
    }

    @Test
    void aTemporaryRelationInASessionThatAlreadyHasItsSchemaCostsWhatAnyRelationCosts()
            throws SQLException {
        try (Connection c = session()) {
            run(c, "CREATE TEMP TABLE csz_e0 (i int)");
            run(c, "BEGIN");
            try {
                run(c, "CREATE TABLE csz_e (i int)", "CREATE TEMP TABLE csz_e1 (i int)",
                        "INSERT INTO csz_e VALUES (1)");
                assertEquals(List.of("1|2"), rows(c, "SELECT i, cmin::text FROM csz_e ORDER BY i"));
            } finally {
                run(c, "ROLLBACK");
            }
            run(c, "DROP TABLE csz_e0");
        }
    }
}
