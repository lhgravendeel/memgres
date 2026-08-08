package com.memgres.client;

import com.memgres.client.RawWireClient.Msg;
import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.List;

import static com.memgres.client.RawWireClient.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Every message declares its own length, and nothing is read outside it.
 *
 * <p>Reading straight from the connection buffer let one message's reader run into the next one's
 * bytes, and from then on the stream was read at the wrong offset: a type byte taken for a length,
 * a length for a type, and the connection dying on an {@code IndexOutOfBoundsException} several
 * messages later, far from whatever had first gone wrong. A body shorter or longer than the length
 * above it is a protocol violation, and PostgreSQL both names it and recovers from it in a
 * particular way — an ErrorResponse, then either a ReadyForQuery or silence until the client sends
 * Sync, depending on whether the message was extended-query work.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class WireMessageFramingTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    private static RawWireClient open() throws IOException {
        RawWireClient client = new RawWireClient(memgres.getPort());
        client.startup(memgres.getUser(), "memgres");
        return client;
    }

    private static String conversation(byte[]... writes) throws IOException {
        try (RawWireClient client = open()) {
            for (byte[] w : writes) client.write(w);
            return String.join(" ", client.readUntilQuiet());
        }
    }

    // --------------------------------------------------- a well-formed stream keeps working

    /** However the bytes are cut up by the network, the messages inside them are the same. */
    @Test
    void aWellFormedStreamIsReadTheSameHoweverItArrives() throws Exception {
        byte[] cycle = concat(parse("SELECT 1"), bind(), describePortal(), execute(), sync());
        assertEquals("1 2 T D C[SELECT 1] Z[I] <waiting>", conversation(cycle));

        try (RawWireClient client = open()) {
            client.writeInPieces(cycle, 1);
            assertEquals("1 2 T D C[SELECT 1] Z[I] <waiting>",
                    String.join(" ", client.readUntilQuiet()));
        }
        try (RawWireClient client = open()) {
            client.writeInPieces(cycle, 3);
            assertEquals("1 2 T D C[SELECT 1] Z[I] <waiting>",
                    String.join(" ", client.readUntilQuiet()));
        }
    }

    /** Two messages in one write are two messages, not one long one. */
    @Test
    void twoMessagesInOneWriteStayTwo() throws Exception {
        assertEquals("T D C[SELECT 1] Z[I] T D C[SELECT 1] Z[I] <waiting>",
                conversation(concat(query("SELECT 1"), query("SELECT 2"))));
    }

    // --------------------------------------------------- a body longer than its reader wants

    /** Bytes left over inside the declared length are a malformed message, not padding. */
    @Test
    void bytesLeftOverAreAMalformedMessage() throws Exception {
        for (byte[] message : new byte[][]{
                padded('Q', cstring("SELECT 1"), 1),
                padded('S', new byte[0], 2)}) {
            try (RawWireClient client = open()) {
                client.write(message);
                Msg error = client.readErrorResponse();
                assertNotNull(error);
                assertEquals("08P01", error.sqlState());
                assertEquals("invalid message format", error.message());
                // Neither is extended-query work, so the server is ready again straight away.
                assertEquals("Z[I]", client.read().toString());
            }
        }
    }

    /** An extended-query message that was malformed leaves the server waiting for Sync. */
    @Test
    void aMalformedExtendedMessageWaitsForSync() throws Exception {
        assertEquals("E[08P01] invalid message format Z[I] <waiting>",
                conversation(concat(padded('P', concat(cstring(""), cstring("SELECT 1"), int16(0)), 2),
                        sync())));
        assertEquals("1 E[08P01] invalid message format Z[I] <waiting>",
                conversation(concat(parse("SELECT 1"),
                        padded('B', concat(cstring(""), cstring(""), int16(0), int16(0), int16(0)), 2),
                        sync())));
        // Nothing but Sync ends it: a query sent in between is discarded, not run.
        assertEquals("E[08P01] invalid message format <waiting>",
                conversation(concat(padded('H', new byte[0], 2), query("SELECT 1"))));
    }

    /** A simple query is still run after one before it was refused. */
    @Test
    void aSimpleQueryAfterARefusedOneStillRuns() throws Exception {
        assertEquals("E[08P01] invalid message format Z[I] T D C[SELECT 1] Z[I] <waiting>",
                conversation(concat(padded('Q', cstring("SELECT 1"), 1), query("SELECT 2"))));
    }

    // --------------------------------------------------- a body that runs out under the reader

    /** Each kind of read that runs off the end of a message has its own complaint. */
    @Test
    void aBodyThatRunsOutIsNamedTheWayItRanOut() throws Exception {
        // pq_getmsgbyte: a single byte that was not there
        assertEquals("E[08P01] no data left in message Z[I] <waiting>",
                conversation(concat(frame('D', new byte[0]), sync())));
        assertEquals("E[08P01] no data left in message Z[I] <waiting>",
                conversation(concat(frame('C', new byte[0]), sync())));
        // pq_getmsgstring: no terminator inside the message
        assertEquals("E[08P01] invalid string in message Z[I] <waiting>",
                conversation(frameWithLength('Q', "SELECT 1".getBytes("UTF-8"), 12)));
        assertEquals("E[08P01] invalid string in message Z[I] <waiting>",
                conversation(frameWithLength('Q', new byte[0], 4)));
    }

    /** A count the message does not carry the bytes for is insufficient data, not a crash. */
    @Test
    void aCountThatOverrunsTheMessageIsRefused() throws Exception {
        byte[][] messages = new byte[][]{
                // a Parse claiming four parameter types and sending none
                frame('P', concat(cstring(""), cstring("SELECT 1"), int16(4))),
                // a Bind claiming sixty-four parameter bytes and sending one
                frame('B', concat(cstring(""), cstring(""), int16(0), int16(1), int32(64),
                        new byte[]{'1'}, int16(0))),
                // a Bind whose parameter length is negative
                frame('B', concat(cstring(""), cstring(""), int16(0), int16(1), int32(-7), int16(0))),
                // a Bind with more format codes than it sent
                frame('B', concat(cstring(""), cstring(""), int16(9), int16(0), int16(0))),
                // a Bind whose result formats run out
                frame('B', concat(cstring(""), cstring(""), int16(0), int16(0), int16(3))),
                // an Execute with no row limit written
                frame('E', cstring("")),
        };
        for (byte[] message : messages) {
            try (RawWireClient client = open()) {
                client.write(concat(parse("SELECT $1"), message, sync()));
                Msg error = client.readErrorResponse();
                assertNotNull(error, "expected an error for a message that overruns its length");
                assertEquals("08P01", error.sqlState());
                assertEquals("insufficient data left in message", error.message());
            }
        }
    }

    // --------------------------------------------------- a header that cannot be believed

    /** A type nothing understands ends the connection, before any length is read for it. */
    @Test
    void anUnknownMessageTypeEndsTheConnection() throws Exception {
        try (RawWireClient client = open()) {
            client.write(concat(frame('W', cstring("nonsense")), query("SELECT 1")));
            Msg error = client.readErrorResponse();
            assertNotNull(error);
            assertEquals("08P01", error.sqlState());
            assertEquals("invalid frontend message type 87", error.message());
            assertEquals("FATAL", error.severity());
            assertNull(client.read(), "the connection must be closed, and the query behind it dropped");
        }
    }

    /**
     * A second startup packet begins with a zero byte, which is no message type at all. Waiting
     * for a body sized from the bytes after it left the connection waiting for 7,676 that were
     * never coming.
     */
    @Test
    void aSecondStartupPacketIsNotAMessage() throws Exception {
        try (RawWireClient client = open()) {
            client.write(startupPacket(196608, defaultParams("memgres", "memgres")));
            Msg error = client.readErrorResponse();
            assertNotNull(error);
            assertEquals("invalid frontend message type 0", error.message());
            assertNull(client.read());
        }
    }

    /** A password message once the session is up is a stranger like any other. */
    @Test
    void aPasswordMessageAfterAuthenticationIsNotAccepted() throws Exception {
        try (RawWireClient client = open()) {
            client.write(concat(frame('p', cstring("x")), query("SELECT 1")));
            Msg error = client.readErrorResponse();
            assertNotNull(error);
            assertEquals("invalid frontend message type 112", error.message());
            assertNull(client.read());
        }
    }

    /** A length that does not cover its own field describes nothing, so nothing is said back. */
    @Test
    void aLengthShorterThanItsOwnFieldIsNotAnswered() throws Exception {
        for (int length : new int[]{3, 0, -8}) {
            try (RawWireClient client = open()) {
                client.write(frameWithLength('Q', cstring("SELECT 1"), length));
                assertNull(client.read(), "a header this broken is not worth an answer");
            }
        }
    }

    /**
     * A length nothing will ever satisfy is not waited on. Buffering whatever a client declared
     * meant one header could hold a connection open for good, and hold whatever arrived behind
     * it in memory while it waited.
     */
    @Test
    void aLengthLargerThanTheServerWillHoldIsDropped() throws Exception {
        byte[][] overLimit = new byte[][]{
                // a header-sized message: ten thousand bytes and no more
                frameWithLength('D', new byte[]{'P', 0}, 10001),
                frameWithLength('C', new byte[]{'S', 0}, 10001),
                frameWithLength('E', new byte[0], 10001),
                frameWithLength('S', new byte[0], 10001),
                frameWithLength('H', new byte[0], 10001),
                frameWithLength('X', new byte[0], 10001),
                frameWithLength('c', new byte[0], 10001),
                frameWithLength('f', cstring("x"), 10001),
                // a message that carries a value: a gigabyte less two
                frameWithLength('Q', cstring("SELECT 1"), 0x3FFFFFFF),
                frameWithLength('P', cstring(""), 0x3FFFFFFF),
                frameWithLength('B', new byte[0], 0x3FFFFFFF),
                frameWithLength('d', new byte[]{1}, 0x3FFFFFFF),
        };
        for (byte[] message : overLimit) {
            try (RawWireClient client = open()) {
                client.write(message);
                assertNull(client.read(), "a length past the limit is not waited on: "
                        + (char) message[0]);
            }
        }
    }

    /** A length inside the limit is still waited on, however large the limit is. */
    @Test
    void aLengthInsideTheLimitIsStillWaitedFor() throws Exception {
        byte[][] withinLimit = new byte[][]{
                frameWithLength('D', new byte[]{'P', 0}, 10000),
                frameWithLength('Q', cstring("SELECT 1"), 0x3FFFFFFE),
                frameWithLength('B', new byte[0], 10001),
        };
        for (byte[] message : withinLimit) {
            try (RawWireClient client = open()) {
                client.write(message);
                assertEquals("<waiting>", String.join(" ", client.readUntilQuiet()),
                        "type " + (char) message[0]);
            }
        }
    }

    // --------------------------------------------------- the packet that opens the connection

    /** The parameters are read to the end of the packet, and no further. */
    @Test
    void theStartupPacketIsReadToItsOwnEnd() throws Exception {
        // Scanning to the end of the connection buffer read whatever was pipelined behind it.
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(concat(startupPacket(196608, defaultParams("memgres", "memgres")),
                    query("SELECT 1")));
            Msg error = client.readErrorResponse();
            assertNotNull(error);
            assertEquals("08P01", error.sqlState());
            assertEquals("expected password response, got message type 81", error.message());
        }
    }

    /** A packet with no terminator, or with bytes past it, is not a startup packet. */
    @Test
    void aStartupPacketWithoutItsTerminatorIsRefused() throws Exception {
        byte[][] packets = new byte[][]{
                startupPacket(196608, concat(defaultParams("memgres", "memgres"),
                        new byte[]{0x20, 0x20})),
                startupPacket(196608, concat(cstring("user"), cstring("memgres"))),
                startupPacket(196608, new byte[0]),
        };
        for (byte[] packet : packets) {
            try (RawWireClient client = new RawWireClient(memgres.getPort())) {
                client.write(packet);
                Msg error = client.readErrorResponse();
                assertNotNull(error);
                assertEquals("08P01", error.sqlState());
                assertEquals("invalid startup packet layout: expected terminator as last byte",
                        error.message());
            }
        }
    }

    /** A startup length below its own header, or past what one may hold, is dropped in silence. */
    @Test
    void aStartupLengthOutsideItsBoundsIsDropped() throws Exception {
        for (int declared : new int[]{3, 0, -8, 10005, 100000}) {
            byte[] packet = startupPacket(196608, defaultParams("memgres", "memgres"));
            System.arraycopy(int32(declared), 0, packet, 0, 4);
            try (RawWireClient client = new RawWireClient(memgres.getPort())) {
                client.write(packet);
                assertNull(client.read(), "startup length " + declared);
            }
        }
    }

    /** Neither SSL nor GSSAPI encryption is offered, and each is declined with a single byte. */
    @Test
    void encryptionRequestsAreDeclinedWithOneByte() throws Exception {
        for (int code : new int[]{80877103, 80877104}) {
            try (RawWireClient client = new RawWireClient(memgres.getPort())) {
                client.write(concat(int32(8), int32(code)));
                assertEquals('N', client.readByte(), "request " + code);
                // and the real packet is still expected behind it
                client.write(startupPacket(196608, defaultParams("memgres", "memgres")));
                client.finishStartup("memgres");
            }
        }
    }

    /** A client asking for a protocol this server does not implement is told what it will get. */
    @Test
    void aNewerMinorVersionIsNegotiatedDown() throws Exception {
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(startupPacket((3 << 16) | 8, defaultParams("memgres", "memgres")));
            Msg negotiate = client.read();
            assertEquals('v', negotiate.type);
            assertEquals((3 << 16) | 2, negotiate.int32(0), "the newest minor version there is");
            assertEquals(0, negotiate.int32(4), "no named options were asked for");
            client.finishStartup("memgres");
        }
    }

    /** A protocol option asked for by name that this server does not implement is named back. */
    @Test
    void anUnimplementedProtocolOptionIsNamedBack() throws Exception {
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(startupPacket(196608, concat(cstring("user"), cstring("memgres"),
                    cstring("_pq_.no_such_thing"), cstring("1"), new byte[]{0})));
            Msg negotiate = client.read();
            assertEquals('v', negotiate.type);
            assertEquals(3 << 16, negotiate.int32(0));
            assertEquals(1, negotiate.int32(4));
            assertEquals("_pq_.no_such_thing",
                    new String(negotiate.body, 8, negotiate.body.length - 9, "UTF-8"));
            client.finishStartup("memgres");
        }
    }

    /** A major version this server has no protocol for is refused by name. */
    @Test
    void anUnsupportedMajorVersionIsRefused() throws Exception {
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(startupPacket(4 << 16, defaultParams("memgres", "memgres")));
            Msg error = client.readErrorResponse();
            assertNotNull(error);
            assertEquals("0A000", error.sqlState());
            assertEquals("unsupported frontend protocol 4.0: server supports 3.0 to 3.2",
                    error.message());
            assertNull(client.read());
        }
    }

    /** The cancel key is as long as the protocol in force allows it to be. */
    @Test
    void theCancelKeyIsAsLongAsTheProtocolAllows() throws Exception {
        assertEquals(8, backendKeyLength(196608), "protocol 3.0 has room for four bytes");
        assertEquals(36, backendKeyLength((3 << 16) | 2), "protocol 3.2 asks for thirty-two");
    }

    private static int backendKeyLength(int protocol) throws IOException {
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(startupPacket(protocol, defaultParams("memgres", "memgres")));
            while (true) {
                Msg m = client.read();
                assertNotNull(m, "closed before BackendKeyData");
                if (m.type == 'R' && m.int32(0) == 3) client.write(frame('p', cstring("memgres")));
                if (m.type == 'K') return m.body.length;
            }
        }
    }

    // --------------------------------------------------- messages that are simply out of place

    /** A copy message outside copy mode is ignored, and what follows it still runs. */
    @Test
    void copyMessagesOutsideCopyModeAreIgnored() throws Exception {
        assertEquals("T D C[SELECT 1] Z[I] <waiting>",
                conversation(concat(frame('c', new byte[0]), query("SELECT 1"))));
    }

    /** A flush on its own changes nothing about the query behind it. */
    @Test
    void aFlushBeforeAQueryChangesNothing() throws Exception {
        assertEquals("T D C[SELECT 1] Z[I] <waiting>",
                conversation(concat(frame('H', new byte[0]), query("SELECT 1"))));
    }

    /** A terminate ends the connection whatever else its body held. */
    @Test
    void aTerminateEndsTheConnection() throws Exception {
        try (RawWireClient client = open()) {
            client.write(padded('X', new byte[0], 2));
            assertNull(client.read());
        }
    }

    /** Nothing sent before the password is answered; PG names the type it got instead. */
    @Test
    void nothingRunsBeforeThePasswordArrives() throws Exception {
        try (RawWireClient client = new RawWireClient(memgres.getPort())) {
            client.write(startupPacket(196608, defaultParams("memgres", "memgres")));
            Msg auth = client.read();
            assertEquals('R', auth.type);
            client.write(query("SELECT 1"));
            List<String> said = client.readUntilQuiet();
            assertEquals("E[08P01] expected password response, got message type 81 <closed>",
                    String.join(" ", said));
        }
    }
}
