package com.memgres.client;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A hand-rolled protocol v3 client, so the framing itself can be driven and observed.
 *
 * <p>The JDBC driver only ever writes well-formed messages, which is exactly why it cannot be used
 * to test what happens to a malformed one.
 */
public class RawWireClient implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    public RawWireClient(int port) throws IOException {
        socket = new Socket("localhost", port);
        socket.setSoTimeout(1200);
        socket.setTcpNoDelay(true);
        in = socket.getInputStream();
        out = socket.getOutputStream();
    }

    /** One message as it came off the wire. */
    public static final class Msg {
        public final char type;
        public final byte[] body;

        Msg(char type, byte[] body) {
            this.type = type;
            this.body = body;
        }

        /** The fields of an ErrorResponse or NoticeResponse, keyed by their one-letter code. */
        public Map<Character, String> fields() {
            Map<Character, String> fields = new LinkedHashMap<Character, String>();
            int i = 0;
            while (i < body.length && body[i] != 0) {
                char code = (char) body[i++];
                int start = i;
                while (i < body.length && body[i] != 0) i++;
                fields.put(code, new String(body, start, i - start, StandardCharsets.UTF_8));
                i++;
            }
            return fields;
        }

        public String sqlState() {
            return fields().get('C');
        }

        public String message() {
            return fields().get('M');
        }

        public String severity() {
            return fields().get('S');
        }

        public int int32(int at) {
            return ((body[at] & 0xFF) << 24) | ((body[at + 1] & 0xFF) << 16)
                    | ((body[at + 2] & 0xFF) << 8) | (body[at + 3] & 0xFF);
        }

        @Override
        public String toString() {
            if (type == 'E' || type == 'N') return type + "[" + sqlState() + "] " + message();
            if (type == 'C') return "C[" + new String(body, 0, Math.max(body.length - 1, 0),
                    StandardCharsets.UTF_8) + "]";
            if (type == 'Z') return "Z[" + (body.length > 0 ? (char) body[0] : '?') + "]";
            return String.valueOf(type);
        }
    }

    // ---------------------------------------------------------------- framing

    public static byte[] frame(char type, byte[] body) {
        byte[] msg = new byte[5 + body.length];
        msg[0] = (byte) type;
        writeInt(msg, 1, body.length + 4);
        System.arraycopy(body, 0, msg, 5, body.length);
        return msg;
    }

    /** A frame whose declared length is deliberately not the body's own. */
    public static byte[] frameWithLength(char type, byte[] body, int declaredLength) {
        byte[] msg = new byte[5 + body.length];
        msg[0] = (byte) type;
        writeInt(msg, 1, declaredLength);
        System.arraycopy(body, 0, msg, 5, body.length);
        return msg;
    }

    /** A frame with bytes past what its reader will want, still inside its declared length. */
    public static byte[] padded(char type, byte[] body, int extra) {
        byte[] padding = new byte[extra];
        Arrays.fill(padding, (byte) 0x20);
        return frame(type, concat(body, padding));
    }

    public static byte[] cstring(String s) {
        byte[] raw = s.getBytes(StandardCharsets.UTF_8);
        return Arrays.copyOf(raw, raw.length + 1);
    }

    public static byte[] concat(byte[]... parts) {
        int total = 0;
        for (byte[] p : parts) total += p.length;
        byte[] all = new byte[total];
        int at = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, all, at, p.length);
            at += p.length;
        }
        return all;
    }

    public static byte[] int32(int v) {
        byte[] b = new byte[4];
        writeInt(b, 0, v);
        return b;
    }

    public static byte[] int16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static void writeInt(byte[] b, int at, int v) {
        b[at] = (byte) (v >> 24);
        b[at + 1] = (byte) (v >> 16);
        b[at + 2] = (byte) (v >> 8);
        b[at + 3] = (byte) v;
    }

    // ---------------------------------------------------------------- the usual messages

    public static byte[] query(String sql) {
        return frame('Q', cstring(sql));
    }

    public static byte[] parse(String sql) {
        return frame('P', concat(cstring(""), cstring(sql), int16(0)));
    }

    public static byte[] bind() {
        return frame('B', concat(cstring(""), cstring(""), int16(0), int16(0), int16(0)));
    }

    public static byte[] describePortal() {
        return frame('D', concat(new byte[]{'P'}, cstring("")));
    }

    public static byte[] execute() {
        return frame('E', concat(cstring(""), int32(0)));
    }

    public static byte[] sync() {
        return frame('S', new byte[0]);
    }

    public static byte[] startupPacket(int protocol, byte[] params) {
        return concat(int32(8 + params.length), int32(protocol), params);
    }

    public static byte[] defaultParams(String user, String database) {
        return concat(cstring("user"), cstring(user), cstring("database"), cstring(database),
                new byte[]{0});
    }

    // ---------------------------------------------------------------- session

    public void write(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    /** Write the bytes a piece at a time, so the decoder sees a message split across reads. */
    public void writeInPieces(byte[] bytes, int pieceSize) throws IOException {
        for (int i = 0; i < bytes.length; i += pieceSize) {
            out.write(bytes, i, Math.min(pieceSize, bytes.length - i));
            out.flush();
            try { Thread.sleep(5); } catch (InterruptedException ignored) { }
        }
    }

    /** The next message, or null when the peer closed. */
    public Msg read() throws IOException {
        int type = in.read();
        if (type < 0) return null;
        byte[] lenBytes = readFully(4);
        if (lenBytes == null) return null;
        int len = ((lenBytes[0] & 0xFF) << 24) | ((lenBytes[1] & 0xFF) << 16)
                | ((lenBytes[2] & 0xFF) << 8) | (lenBytes[3] & 0xFF);
        byte[] body = len > 4 ? readFully(len - 4) : new byte[0];
        if (body == null) return null;
        return new Msg((char) type, body);
    }

    private byte[] readFully(int n) throws IOException {
        byte[] b = new byte[n];
        int at = 0;
        while (at < n) {
            int r = in.read(b, at, n - at);
            if (r < 0) return null;
            at += r;
        }
        return b;
    }

    /** One raw byte, for the single-byte reply to an SSL or GSSAPI request. */
    public int readByte() throws IOException {
        return in.read();
    }

    /**
     * Everything the server says until it closes or falls silent. The last entry says which of
     * the two it was: a protocol error inside an extended-query message legitimately leaves the
     * peer waiting for Sync rather than answering.
     */
    public List<String> readUntilQuiet() {
        List<String> msgs = new ArrayList<String>();
        try {
            while (true) {
                Msg m = read();
                if (m == null) { msgs.add("<closed>"); break; }
                msgs.add(m.toString());
            }
        } catch (java.net.SocketTimeoutException e) {
            msgs.add("<waiting>");
        } catch (IOException e) {
            msgs.add("<reset>");
        }
        return msgs;
    }

    /** The first ErrorResponse in what the server said next, or null if it sent none. */
    public Msg readErrorResponse() throws IOException {
        while (true) {
            Msg m = read();
            if (m == null || m.type == 'E') return m;
        }
    }

    /** Open a session the ordinary way, and read through to the first ReadyForQuery. */
    public void startup(String user, String database) throws IOException {
        write(startupPacket(196608, defaultParams(user, database)));
        finishStartup("memgres");
    }

    /** Answer the authentication request and read through to ReadyForQuery. */
    public void finishStartup(String password) throws IOException {
        while (true) {
            Msg m = read();
            if (m == null) throw new IOException("closed during startup");
            if (m.type == 'E') throw new IOException(m.toString());
            if (m.type == 'R' && m.int32(0) == 3) write(frame('p', cstring(password)));
            if (m.type == 'Z') return;
        }
    }

    @Override
    public void close() {
        try { socket.close(); } catch (IOException ignored) { }
    }
}
