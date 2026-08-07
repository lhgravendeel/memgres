package com.memgres.client;

import com.memgres.client.RawWireClient.Msg;
import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.memgres.client.RawWireClient.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The function-call sub-protocol, and what it puts on the wire.
 *
 * <p>Message type {@code F} is how a JDBC client reaches the large-object API. Treated as a type
 * nothing understood, it was skipped in silence and the client waited for a reply that was never
 * coming. And the answer to one is the type {@code pg_proc} declares for the function, not the one
 * the value happened to be carried in: {@code lo_creat} is declared an oid and answered eight
 * bytes, so a driver reading four of them found a large object it could not open.
 */
class FunctionCallProtocolTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static int oidOf(String name, int nargs) throws SQLException {
        return Integer.parseInt(scalar("SELECT oid FROM pg_catalog.pg_proc WHERE proname = '"
                + name + "' AND pronargs = " + nargs + " ORDER BY oid LIMIT 1").trim());
    }

    private static byte[] functionCall(int oid, byte[][] args, int resultFormat) {
        List<byte[]> parts = new ArrayList<byte[]>();
        parts.add(int32(oid));
        parts.add(int16(0)); // every argument in text
        parts.add(int16(args.length));
        for (byte[] a : args) {
            parts.add(int32(a == null ? -1 : a.length));
            if (a != null) parts.add(a);
        }
        parts.add(int16(resultFormat));
        return frame('F', concat(parts.toArray(new byte[0][])));
    }

    private static RawWireClient open() throws IOException {
        RawWireClient client = new RawWireClient(memgres.getPort());
        client.startup(memgres.getUser(), "memgres");
        return client;
    }

    // ------------------------------------------------------------- calling a function by oid

    /** A call answers with a FunctionCallResponse and then says it is ready again. */
    @Test
    void aCallAnswersWithItsResult() throws Exception {
        try (RawWireClient client = open()) {
            client.write(functionCall(oidOf("abs", 1), new byte[][]{"-5".getBytes("UTF-8")}, 0));
            Msg result = client.read();
            assertEquals('V', result.type);
            assertEquals(1, result.int32(0));
            assertEquals("5", new String(result.body, 4, 1, "UTF-8"));
            assertEquals("Z[I]", client.read().toString());
        }
    }

    /** Several arguments are read in the order they were written. */
    @Test
    void argumentsAreReadInOrder() throws Exception {
        try (RawWireClient client = open()) {
            client.write(functionCall(oidOf("repeat", 2),
                    new byte[][]{"ab".getBytes("UTF-8"), "2".getBytes("UTF-8")}, 0));
            Msg result = client.read();
            assertEquals('V', result.type);
            assertEquals("abab", new String(result.body, 4, result.int32(0), "UTF-8"));
        }
    }

    /** An argument written as nothing is nothing, and so is the answer. */
    @Test
    void aNullArgumentGivesANullResult() throws Exception {
        try (RawWireClient client = open()) {
            client.write(functionCall(oidOf("abs", 1), new byte[][]{null}, 0));
            Msg result = client.read();
            assertEquals('V', result.type);
            assertEquals(-1, result.int32(0));
        }
    }

    /** The result is encoded as the type the function was declared to answer with. */
    @Test
    void theResultIsEncodedAsItsDeclaredType() throws Exception {
        try (RawWireClient client = open()) {
            // lo_creat is declared to answer an oid, which is four bytes and not eight
            client.write(functionCall(oidOf("lo_creat", 1), new byte[][]{"-1".getBytes("UTF-8")}, 1));
            Msg result = client.read();
            assertEquals('V', result.type);
            assertEquals(4, result.int32(0), "an oid is four bytes on the wire");
        }
        try (RawWireClient client = open()) {
            // lo_tell64 is declared to answer a bigint, which is eight
            int fd = Integer.parseInt(scalar("SELECT lo_open(lo_creat(-1), 262144)"));
            client.write(functionCall(oidOf("lo_tell64", 1),
                    new byte[][]{String.valueOf(fd).getBytes("UTF-8")}, 1));
            Msg result = client.read();
            assertEquals('V', result.type);
            assertEquals(8, result.int32(0), "a bigint is eight bytes on the wire");
        }
    }

    // ------------------------------------------------------------- and what it refuses

    /** An oid nothing owns names no function, and is told so before anything else is read. */
    @Test
    void anOidNothingOwnsNamesNoFunction() throws Exception {
        try (RawWireClient client = open()) {
            client.write(functionCall(987654, new byte[0][], 0));
            Msg error = client.readErrorResponse();
            assertEquals("42883", error.sqlState());
            assertEquals("function with OID 987654 does not exist", error.message());
            assertEquals("Z[I]", client.read().toString());
        }
        // Even when the rest of the message is unreadable, the missing function is the complaint.
        try (RawWireClient client = open()) {
            client.write(frame('F', concat(int32(987654), int16(0), int16(1))));
            Msg error = client.readErrorResponse();
            assertEquals("42883", error.sqlState());
            assertEquals("function with OID 987654 does not exist", error.message());
        }
    }

    /** A call carrying the wrong number of arguments is refused with the count it wanted. */
    @Test
    void theArgumentCountMustMatchTheFunction() throws Exception {
        try (RawWireClient client = open()) {
            client.write(functionCall(oidOf("abs", 1), new byte[0][], 0));
            Msg error = client.readErrorResponse();
            assertEquals("08P01", error.sqlState());
            assertEquals("function call message contains 0 arguments but function requires 1",
                    error.message());
            assertEquals("Z[I]", client.read().toString());
        }
    }

    /** A body that does not hold what it declared is a protocol violation like any other. */
    @Test
    void aBodyThatRunsOutIsRefused() throws Exception {
        try (RawWireClient client = open()) {
            client.write(frame('F', concat(int32(oidOf("abs", 1)), int16(0), int16(1))));
            Msg error = client.readErrorResponse();
            assertEquals("08P01", error.sqlState());
            assertEquals("insufficient data left in message", error.message());
            assertEquals("Z[I]", client.read().toString());
        }
        try (RawWireClient client = open()) {
            client.write(frame('F', concat(int32(oidOf("abs", 1)), int16(0), int16(1), int32(2),
                    "-5".getBytes("UTF-8"), int16(0), new byte[]{0x20})));
            Msg error = client.readErrorResponse();
            assertEquals("08P01", error.sqlState());
            assertEquals("invalid message format", error.message());
        }
    }

    // ------------------------------------------------------------- the driver's own use of it

    /** A large object written and read back through the driver's own API. */
    @Test
    void aLargeObjectSurvivesTheDriversApi() throws Exception {
        conn.setAutoCommit(false);
        try {
            org.postgresql.largeobject.LargeObjectManager objects =
                    conn.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
            long oid = objects.createLO();
            org.postgresql.largeobject.LargeObject object = objects.open(oid,
                    org.postgresql.largeobject.LargeObjectManager.READWRITE);
            object.write("hello".getBytes("UTF-8"));
            object.close();

            object = objects.open(oid, org.postgresql.largeobject.LargeObjectManager.READ);
            byte[] read = object.read(5);
            object.close();
            objects.unlink(oid);
            conn.commit();
            assertEquals("hello", new String(read, "UTF-8"));
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // ------------------------------------------------------------- what the calls are declared

    /** The 64-bit spellings of seek and tell exist, and answer in bigint. */
    @Test
    void theSixtyFourBitSpellingsExist() throws Exception {
        assertEquals("bigint", scalar("SELECT pg_get_function_result(oid) FROM pg_catalog.pg_proc "
                + "WHERE proname = 'lo_lseek64'"));
        assertEquals("bigint", scalar("SELECT pg_get_function_result(oid) FROM pg_catalog.pg_proc "
                + "WHERE proname = 'lo_tell64'"));
        String id = scalar("SELECT lo_from_bytea(0, '\\x010203'::bytea)::text");
        assertEquals("3", scalar("SELECT lo_lseek64(lo_open(" + id + ", 262144), 0, 2)"));
        assertEquals("0", scalar("SELECT lo_tell64(lo_open(" + id + ", 262144))"));
        assertEquals("bigint", scalar("SELECT pg_typeof(lo_tell64(lo_open("
                + id + ", 262144)))::text"));
        scalar("SELECT lo_unlink(" + id + ")");
    }

    /** An oid is a bare number and a registry type a bare name; only the declaration tells them. */
    @Test
    void anOidIsNotTheNumberItIsCarriedAs() throws Exception {
        assertEquals("oid", scalar("SELECT pg_typeof(lo_creat(-1))::text"));
        assertEquals("oid", scalar("SELECT pg_typeof(pg_my_temp_schema())::text"));
        assertEquals("regclass", scalar("SELECT pg_typeof(to_regclass('pg_class'))::text"));
        assertEquals("regtype", scalar("SELECT pg_typeof(to_regtype('integer'))::text"));
        assertEquals("regproc", scalar("SELECT pg_typeof(to_regproc('abs'))::text"));
        // A name is a bare string, and says as little about itself.
        assertEquals("name", scalar("SELECT pg_typeof(current_database())::text"));
        // What the value really can witness is still left to the value.
        assertEquals("integer", scalar("SELECT pg_typeof(pg_backend_pid())::text"));
        assertEquals("integer", scalar("SELECT pg_typeof(abs(-1))::text"));
        assertEquals("text", scalar("SELECT pg_typeof(version())::text"));
    }

    /** A large object read back is a bytea, and writing to one answers with void, not with NULL. */
    @Test
    void whatTheReadingAndWritingCallsAnswerWith() throws Exception {
        String id = scalar("SELECT lo_from_bytea(0, '\\x010203'::bytea)::text");
        assertEquals("bytea", scalar("SELECT pg_typeof(lo_get(" + id + "))::text"));
        assertEquals("f", scalar("SELECT lo_put(" + id + ", 0, '\\x09'::bytea) IS NULL"));
        assertEquals("\\x090203", scalar("SELECT lo_get(" + id + ")::text"));
        scalar("SELECT lo_unlink(" + id + ")");
    }

    /** pg_typeof reads its argument once, so a call with an effect happens once. */
    @Test
    void anArgumentWithAnEffectIsEvaluatedOnce() throws Exception {
        String id = scalar("SELECT lo_creat(-1)::text");
        assertEquals("integer", scalar("SELECT pg_typeof(lo_unlink(" + id + "))::text"));
    }
}
