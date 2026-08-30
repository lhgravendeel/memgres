package com.memgres.wire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A conversation with a PostgreSQL-speaking server, held in the protocol's own terms.
 *
 * <p>A JDBC driver reads the wire and then tells its caller a much smaller story: the command tag
 * becomes an update count, the error fields become a message. What the server actually put on the
 * socket — the tag it chose, the fields it filled in, whether it answered an empty statement at
 * all — cannot be seen through the driver, and those are exactly the things this branch is about.
 * So the tests that check them speak the protocol themselves.
 */
final class WireTalk implements AutoCloseable {

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    private final String password;

    WireTalk(String host, int port, String user, String database) throws IOException {
        this(host, port, user, database, null);
    }

    WireTalk(String host, int port, String user, String database, String password)
            throws IOException {
        socket = new Socket(host, port);
        this.password = password;
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        startup(user, database);
    }

    private void startup(String user, String database) throws IOException {
        byte[] userBytes = user.getBytes(StandardCharsets.UTF_8);
        byte[] dbBytes = database.getBytes(StandardCharsets.UTF_8);
        int length = 4 + 4 + 5 + userBytes.length + 1 + 9 + dbBytes.length + 1 + 1;
        out.writeInt(length);
        out.writeInt(196608); // protocol 3.0
        out.write("user".getBytes(StandardCharsets.UTF_8));
        out.write(0);
        out.write(userBytes);
        out.write(0);
        out.write("database".getBytes(StandardCharsets.UTF_8));
        out.write(0);
        out.write(dbBytes);
        out.write(0);
        out.write(0);
        out.flush();
        // Read to the first ReadyForQuery, answering the one authentication request a trust
        // configuration sends and passing over the parameters and the key data.
        while (true) {
            Message m = read();
            if (m.type == 'Z') return;
            if (m.type == 'R') {
                int kind = ((m.body[0] & 0xff) << 24) | ((m.body[1] & 0xff) << 16)
                        | ((m.body[2] & 0xff) << 8) | (m.body[3] & 0xff);
                if (kind == 3) {
                    byte[] pw = (password == null ? "" : password)
                            .getBytes(StandardCharsets.UTF_8);
                    out.write('p');
                    out.writeInt(4 + pw.length + 1);
                    out.write(pw);
                    out.write(0);
                    out.flush();
                } else if (kind != 0) {
                    throw new IOException("authentication " + kind + " is not one this speaks");
                }
            }
            if (m.type == 'E') throw new IOException("startup refused: " + fields(m).toString());
        }
    }

    /** One protocol message: its type byte and the body after the length word. */
    static final class Message {
        final char type;
        final byte[] body;

        Message(char type, byte[] body) {
            this.type = type;
            this.body = body;
        }
    }

    private Message read() throws IOException {
        int type = in.read();
        if (type < 0) throw new IOException("the server closed the connection");
        int length = in.readInt();
        byte[] body = new byte[length - 4];
        in.readFully(body);
        return new Message((char) type, body);
    }

    /** Send a simple Query and collect every message up to the ReadyForQuery that ends it. */
    List<Message> query(String sql) throws IOException {
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        out.write('Q');
        out.writeInt(4 + bytes.length + 1);
        out.write(bytes);
        out.write(0);
        out.flush();
        List<Message> messages = new ArrayList<>();
        while (true) {
            Message m = read();
            messages.add(m);
            if (m.type == 'Z') return messages;
        }
    }

    /** The command tags a statement produced, in order. */
    List<String> tags(String sql) throws IOException {
        List<String> tags = new ArrayList<>();
        for (Message m : query(sql)) {
            if (m.type == 'C') tags.add(cString(m.body, 0));
        }
        return tags;
    }

    /** The message types a statement produced, in order, as one string. */
    String shape(String sql) throws IOException {
        StringBuilder out = new StringBuilder();
        for (Message m : query(sql)) out.append(m.type);
        return out.toString();
    }

    /** The error fields a statement produced, keyed by their field bytes. */
    java.util.Map<Character, String> error(String sql) throws IOException {
        for (Message m : query(sql)) {
            if (m.type == 'E') return fields(m);
        }
        return java.util.Collections.emptyMap();
    }

    /**
     * Ask the server to run a portal by name, and collect what it answers up to ReadyForQuery.
     * Nothing has bound the name, which is the point: an Execute names a portal that has to be
     * there.
     */
    List<Message> executePortal(String portal) throws IOException {
        byte[] name = portal.getBytes(StandardCharsets.UTF_8);
        out.write('E');
        out.writeInt(4 + name.length + 1 + 4);
        out.write(name);
        out.write(0);
        out.writeInt(0);
        out.write('S');
        out.writeInt(4);
        out.flush();
        List<Message> messages = new ArrayList<>();
        while (true) {
            Message m = read();
            messages.add(m);
            if (m.type == 'Z') return messages;
        }
    }

    /** The error fields of the first ErrorResponse among these messages. */
    static java.util.Map<Character, String> errorFields(List<Message> messages) {
        for (Message m : messages) {
            if (m.type == 'E') return fields(m);
        }
        return java.util.Collections.emptyMap();
    }

    /** The parameters the server reported while running a statement, as "name=value". */
    List<String> reportedParameters(String sql) throws IOException {
        List<String> reported = new ArrayList<>();
        for (Message m : query(sql)) {
            if (m.type != 'S') continue;
            String name = cString(m.body, 0);
            String value = cString(m.body, name.getBytes(StandardCharsets.UTF_8).length + 1);
            reported.add(name + "=" + value);
        }
        return reported;
    }

    private static java.util.Map<Character, String> fields(Message m) {
        java.util.Map<Character, String> out = new java.util.LinkedHashMap<>();
        int at = 0;
        while (at < m.body.length && m.body[at] != 0) {
            char field = (char) m.body[at++];
            String value = cString(m.body, at);
            at += value.getBytes(StandardCharsets.UTF_8).length + 1;
            out.put(field, value);
        }
        return out;
    }

    private static String cString(byte[] body, int from) {
        int end = from;
        while (end < body.length && body[end] != 0) end++;
        return new String(body, from, end - from, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        try {
            out.write('X');
            out.writeInt(4);
            out.flush();
        } catch (IOException alreadyGone) {
            // Saying goodbye to a connection that is already closed is not a failure.
        }
        socket.close();
    }
}
