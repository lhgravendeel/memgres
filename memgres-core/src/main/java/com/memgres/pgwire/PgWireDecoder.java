package com.memgres.pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes the PostgreSQL wire protocol v3 messages from raw bytes.
 *
 * <p>Every message declares its own length, and nothing here reads outside it. Reading straight
 * from the connection buffer instead let one message's reader run into the next one's bytes, and
 * from then on the stream was read at the wrong offset: the type byte of a real message was taken
 * for a length, a length for a type, and the connection died on an {@code IndexOutOfBoundsException}
 * some messages later, far from whatever had first gone wrong. A body that is shorter or longer
 * than the length above it is now the protocol violation PostgreSQL calls it, raised where it
 * happens and recovered from the way PostgreSQL recovers.
 */
public class PgWireDecoder extends ByteToMessageDecoder {

    /** What PG's pq_getmsgbytes and friends say when a message runs out under the reader. */
    private static final String INSUFFICIENT = "insufficient data left in message";

    /** The frontend message types this protocol has; anything else ends the connection. */
    private static final java.util.BitSet KNOWN_TYPES = new java.util.BitSet(256);

    /** The types whose body may be as large as a value can be; every other one is a header. */
    private static final java.util.BitSet LARGE_TYPES = new java.util.BitSet(256);

    /** As much as PostgreSQL will buffer for a message that carries a value. */
    private static final int LARGE_MESSAGE_LIMIT = 0x3FFFFFFE;

    /** And for one that only says what to do next. */
    private static final int SMALL_MESSAGE_LIMIT = 10000;

    /** The most a startup packet may declare beyond its own length field. */
    private static final int MAX_STARTUP_PACKET_LENGTH = 10000;

    static {
        for (char type : "QPBDESHCXpFdcf".toCharArray()) KNOWN_TYPES.set(type);
        for (char type : "QPBFdp".toCharArray()) LARGE_TYPES.set(type);
    }

    /** The newest minor version of protocol 3 this server implements. */
    static final int LATEST_MINOR = 2;

    private final CancelRegistry cancelRegistry;
    private boolean startupDone = false;

    public PgWireDecoder(CancelRegistry cancelRegistry) {
        this.cancelRegistry = cancelRegistry;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!startupDone) {
            decodeStartup(ctx, in, out);
        } else {
            decodeMessage(in, out);
        }
    }

    // ------------------------------------------------------------------ the opening packet

    private void decodeStartup(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 4) {
            return; // Wait for more data
        }

        in.markReaderIndex();
        int length = in.readInt();

        // A length that does not even cover its own field describes nothing, and one that
        // declares more than any startup packet may hold describes nothing either. PG logs both
        // and drops the connection without answering, because there is no message to answer about.
        if (length < 8 || length - 4 > MAX_STARTUP_PACKET_LENGTH) {
            drop(in, out);
            return;
        }

        if (in.readableBytes() < length - 4) {
            in.resetReaderIndex();
            return;
        }

        ByteBuf packet = in.readSlice(length - 4);
        int protocolVersion = packet.readInt();

        // SSL request: protocol = 80877103
        if (protocolVersion == 80877103) {
            out.add(PgWireMessage.sslRequest());
            return;
        }

        // GSSAPI encryption request: protocol = 80877104. Read as an ordinary startup packet it
        // opened a session whose protocol version was nonsense; PG declines it the way it declines
        // SSL, with a single byte, and waits for the real packet.
        if (protocolVersion == 80877104) {
            out.add(PgWireMessage.gssEncRequest());
            return;
        }

        // Cancel request: protocol = 80877102
        if (protocolVersion == 80877102) {
            // The key is as long as the connection being cancelled was given, which under
            // protocol 3.2 is thirty-two bytes rather than four.
            if (packet.readableBytes() >= 4) {
                int pid = packet.readInt();
                byte[] secretKey = new byte[packet.readableBytes()];
                packet.readBytes(secretKey);
                cancelRegistry.cancel(pid, secretKey);
            }
            ctx.close(); // PG protocol: cancel connection is closed immediately
            return;
        }

        int major = protocolVersion >>> 16;
        int minor = protocolVersion & 0xFFFF;
        if (major != 3) {
            out.add(PgWireMessage.protocolError("0A000", "unsupported frontend protocol "
                    + major + "." + minor + ": server supports 3.0 to 3." + LATEST_MINOR,
                    (byte) 0, true));
            in.skipBytes(in.readableBytes());
            return;
        }

        Map<String, String> params = new LinkedHashMap<>();
        try {
            Body body = new Body(packet);
            while (true) {
                String key = body.readCString();
                if (key.isEmpty()) break;
                params.put(key, body.readCString());
            }
            body.end();
        } catch (ProtocolViolation e) {
            // Scanning to the end of the connection buffer instead of the end of the packet read
            // whatever the client had pipelined behind it as further parameters.
            out.add(PgWireMessage.protocolError("08P01",
                    "invalid startup packet layout: expected terminator as last byte",
                    (byte) 0, true));
            in.skipBytes(in.readableBytes());
            return;
        }

        startupDone = true;
        // A client that asks for a minor version this server does not have, or for a protocol
        // option it does not implement, is told so rather than left to assume it was all accepted.
        java.util.List<String> unsupported = unsupportedOptions(params);
        int settled = Math.min(minor, LATEST_MINOR);
        if (minor > LATEST_MINOR || !unsupported.isEmpty()) {
            out.add(PgWireMessage.negotiateProtocolVersion(settled, unsupported));
        }
        out.add(PgWireMessage.startup(params, settled));
    }

    /** The protocol options a client asked for by name that this server does not implement. */
    private static java.util.List<String> unsupportedOptions(Map<String, String> params) {
        java.util.List<String> unsupported = new java.util.ArrayList<String>();
        for (String key : params.keySet()) {
            if (key.startsWith("_pq_.")) unsupported.add(key);
        }
        return unsupported;
    }

    /** How much of a body PostgreSQL will hold for a message of this type. */
    private static int limitFor(byte type) {
        return LARGE_TYPES.get(type & 0xFF) ? LARGE_MESSAGE_LIMIT : SMALL_MESSAGE_LIMIT;
    }

    /** Say nothing and let the connection go, which is what PG does with an unreadable header. */
    private void drop(ByteBuf in, List<Object> out) {
        in.skipBytes(in.readableBytes());
        out.add(PgWireMessage.protocolError(null, null, (byte) 0, true));
    }

    // ------------------------------------------------------------------ everything after it

    private void decodeMessage(ByteBuf in, List<Object> out) {
        while (in.readableBytes() >= 1) {
            byte type = in.getByte(in.readerIndex());
            // The type is judged before the length, as PG judges it. Waiting for the body of a
            // message nothing understands meant waiting on a length read out of bytes that were
            // never a length -- a second startup packet left the connection waiting for 7,676
            // bytes that no client was ever going to send.
            if (!KNOWN_TYPES.get(type & 0xFF)) {
                in.skipBytes(in.readableBytes());
                out.add(PgWireMessage.protocolError("08P01",
                        "invalid frontend message type " + (type & 0xFF), type, true));
                return;
            }
            if (in.readableBytes() < 5) return;

            in.markReaderIndex();
            in.skipBytes(1);
            int length = in.readInt();

            // A length nothing will ever satisfy is not waited on. Buffering whatever a client
            // declared meant a single header could hold the connection open forever, and hold
            // whatever arrived behind it in memory while it waited.
            if (length < 4 || length > limitFor(type)) {
                drop(in, out);
                return;
            }

            if (in.readableBytes() < length - 4) {
                in.resetReaderIndex();
                return;
            }

            // The slice is the message and nothing else, so the reader index below already sits
            // on the next message whatever the branch above it did with the body.
            ByteBuf body = in.readSlice(length - 4);
            try {
                if (!decodeBody(type, new Body(body), out)) return;
            } catch (ProtocolViolation e) {
                out.add(PgWireMessage.protocolError("08P01", e.getMessage(), type, false));
            }
        }
    }

    /** Decode one message body; false when the connection is not to be read any further. */
    private boolean decodeBody(byte type, Body body, List<Object> out) {
        switch (type) {
            case 'Q': {
                String query = body.readCString();
                body.end();
                out.add(PgWireMessage.query(query));
                return true;
            }
            case 'P': {
                String stmtName = body.readCString();
                String query = body.readCString();
                int numParams = body.readShort() & 0xFFFF;
                int[] paramOids = new int[numParams];
                for (int i = 0; i < numParams; i++) {
                    paramOids[i] = body.readInt();
                }
                body.end();
                out.add(PgWireMessage.parse(stmtName, query, paramOids));
                return true;
            }
            case 'B': {
                String portal = body.readCString();
                String stmtName = body.readCString();

                int numFormatCodes = body.readShort() & 0xFFFF;
                short[] formatCodes = new short[numFormatCodes];
                for (int i = 0; i < numFormatCodes; i++) {
                    formatCodes[i] = body.readShort();
                }

                int numParams = body.readShort() & 0xFFFF;
                byte[][] paramValues = new byte[numParams][];
                for (int i = 0; i < numParams; i++) {
                    int paramLen = body.readInt();
                    paramValues[i] = paramLen == -1 ? null : body.readBytes(paramLen);
                }

                int numResultFormats = body.readShort() & 0xFFFF;
                short[] resultFormats = new short[numResultFormats];
                for (int i = 0; i < numResultFormats; i++) {
                    resultFormats[i] = body.readShort();
                }

                body.end();
                out.add(PgWireMessage.bind(portal, stmtName, formatCodes, paramValues, resultFormats));
                return true;
            }
            case 'D': {
                byte descType = body.readByte(); // 'S' for statement, 'P' for portal
                String name = body.readCString();
                body.end();
                out.add(PgWireMessage.describe(descType, name));
                return true;
            }
            case 'E': {
                String portal = body.readCString();
                int maxRows = body.readInt();
                body.end();
                out.add(PgWireMessage.execute(portal, maxRows));
                return true;
            }
            case 'S': {
                body.end();
                out.add(PgWireMessage.sync());
                return true;
            }
            case 'H': {
                body.end();
                out.add(PgWireMessage.flush());
                return true;
            }
            case 'C': {
                byte closeType = body.readByte();
                String name = body.readCString();
                body.end();
                out.add(PgWireMessage.close(closeType, name));
                return true;
            }
            case 'X': {
                out.add(PgWireMessage.terminate());
                return true;
            }
            case 'p': {
                out.add(PgWireMessage.password(body.readCString()));
                return true;
            }
            case 'F': {
                // PG looks the function up before it reads the arguments, so a call naming no
                // function is told so whatever else is wrong with it. The complaint about the
                // rest travels with the message and is raised only once the function is found.
                int functionOid = body.readInt();
                try {
                    int numFormatCodes = body.readShort() & 0xFFFF;
                    short[] formatCodes = new short[numFormatCodes];
                    for (int i = 0; i < numFormatCodes; i++) {
                        formatCodes[i] = body.readShort();
                    }
                    int numArgs = body.readShort() & 0xFFFF;
                    byte[][] argValues = new byte[numArgs][];
                    for (int i = 0; i < numArgs; i++) {
                        int argLen = body.readInt();
                        argValues[i] = argLen == -1 ? null : body.readBytes(argLen);
                    }
                    short resultFormat = body.readShort();
                    body.end();
                    out.add(PgWireMessage.functionCall(functionOid, formatCodes, argValues,
                            resultFormat, null));
                } catch (ProtocolViolation e) {
                    out.add(PgWireMessage.functionCall(functionOid, null, null, (short) 0,
                            e.getMessage()));
                }
                return true;
            }
            case 'd': {
                out.add(PgWireMessage.copyData(body.rest()));
                return true;
            }
            case 'c': {
                out.add(PgWireMessage.copyDone());
                return true;
            }
            case 'f': {
                out.add(PgWireMessage.copyFail(body.readCString()));
                return true;
            }
            default: {
                // Unreachable: the type was checked against KNOWN_TYPES before the body was read.
                out.add(PgWireMessage.protocolError("08P01",
                        "invalid frontend message type " + (type & 0xFF), type, true));
                return false;
            }
        }
    }

    /** The message types PostgreSQL counts as extended-query work, which a Sync ends. */
    static boolean isExtendedQueryMessage(byte type) {
        return type == 'P' || type == 'B' || type == 'D' || type == 'E'
                || type == 'C' || type == 'H';
    }

    /** A message read at an offset its own length does not reach. */
    static final class ProtocolViolation extends RuntimeException {
        ProtocolViolation(String message) {
            super(message, null, false, false);
        }
    }

    /**
     * A reader over one message's own bytes. Each read that would pass the end of the message
     * raises the complaint PostgreSQL raises for that kind of read, rather than taking bytes that
     * belong to the message after it.
     */
    static final class Body {
        private final ByteBuf buf;

        Body(ByteBuf buf) {
            this.buf = buf;
        }

        byte readByte() {
            if (buf.readableBytes() < 1) throw new ProtocolViolation("no data left in message");
            return buf.readByte();
        }

        short readShort() {
            if (buf.readableBytes() < 2) throw new ProtocolViolation(INSUFFICIENT);
            return buf.readShort();
        }

        int readInt() {
            if (buf.readableBytes() < 4) throw new ProtocolViolation(INSUFFICIENT);
            return buf.readInt();
        }

        byte[] readBytes(int count) {
            if (count < 0 || count > buf.readableBytes()) throw new ProtocolViolation(INSUFFICIENT);
            byte[] bytes = new byte[count];
            buf.readBytes(bytes);
            return bytes;
        }

        String readCString() {
            int nul = buf.indexOf(buf.readerIndex(), buf.writerIndex(), (byte) 0);
            if (nul < 0) throw new ProtocolViolation("invalid string in message");
            byte[] bytes = new byte[nul - buf.readerIndex()];
            buf.readBytes(bytes);
            buf.skipBytes(1); // the terminator
            return new String(bytes, StandardCharsets.UTF_8);
        }

        byte[] rest() {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        }

        /** Whatever the message declared, the reader must have wanted all of it and no more. */
        void end() {
            if (buf.isReadable()) throw new ProtocolViolation("invalid message format");
        }
    }
}
