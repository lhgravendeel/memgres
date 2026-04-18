package com.memgres.pgwire;

import com.memgres.engine.*;
import com.memgres.engine.parser.ast.CopyStmt;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * COPY protocol handling (COPY TO STDOUT and COPY FROM STDIN),
 * including text/CSV/binary format parsing and encoding.
 */
class PgWireCopyHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PgWireCopyHandler.class);

    private final Session session;

    // COPY FROM STDIN state
    boolean inCopyFromMode;
    com.memgres.engine.parser.ast.CopyStmt activeCopyStmt;
    ByteArrayOutputStream copyBuffer;
    int copyRowCount;

    PgWireCopyHandler(Session session) {
        this.session = session;
    }

    // ---- COPY TO STDOUT ----

    /** Send COPY TO STDOUT result: CopyOutResponse, CopyData rows, CopyDone, CommandComplete. */
    void sendCopyOutResult(ChannelHandlerContext ctx, QueryResult result) {
        CopyStmt copyStmt = result.getCopyStmt();
        int numCols = result.getColumns().size();
        boolean isCsv = "csv".equalsIgnoreCase(copyStmt != null ? copyStmt.format() : "text");
        boolean isBinary = "binary".equalsIgnoreCase(copyStmt != null ? copyStmt.format() : "text");
        String delimiter = copyStmt != null ? copyStmt.delimiter() : (isCsv ? "," : "\t");
        String nullString = copyStmt != null ? copyStmt.nullString() : (isCsv ? "" : "\\N");
        boolean header = copyStmt != null && copyStmt.header();
        String quoteChar = copyStmt != null && copyStmt.quote() != null ? copyStmt.quote() : "\"";
        String escapeChar = copyStmt != null && copyStmt.escape() != null ? copyStmt.escape() : quoteChar;
        List<String> forceQuote = copyStmt != null ? copyStmt.forceQuote() : null;

        Set<Integer> forceQuoteIndices = new HashSet<>();
        if (forceQuote != null && isCsv) {
            for (int i = 0; i < numCols; i++) {
                String colName = result.getColumns().get(i).getName();
                if (forceQuote.contains("*") || forceQuote.stream().anyMatch(c -> c.equalsIgnoreCase(colName))) {
                    forceQuoteIndices.add(i);
                }
            }
        }

        if (isBinary) {
            sendBinaryCopyOut(ctx, result, numCols);
            return;
        }

        // Send CopyOutResponse ('H')
        ByteBuf hdr = ctx.alloc().buffer();
        hdr.writeByte('H');
        hdr.writeInt(4 + 1 + 2 + numCols * 2);
        hdr.writeByte(0); // text format
        hdr.writeShort(numCols);
        for (int i = 0; i < numCols; i++) hdr.writeShort(0);
        ctx.write(hdr);

        // Header row
        if (header) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < numCols; i++) {
                if (i > 0) sb.append(delimiter);
                String colName = result.getColumns().get(i).getName();
                if (isCsv) {
                    sb.append(csvQuoteIfNeeded(colName, delimiter, quoteChar, escapeChar, nullString));
                } else {
                    sb.append(colName);
                }
            }
            sb.append('\n');
            sendCopyDataMessage(ctx, sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        // Data rows
        for (Object[] row : result.getRows()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.length; i++) {
                if (i > 0) sb.append(delimiter);
                Object val = row[i];
                if (val == null) {
                    sb.append(nullString);
                } else {
                    String text = PgWireValueFormatter.formatValue(val, null);
                    if (isCsv) {
                        if (forceQuoteIndices.contains(i)) {
                            sb.append(quoteChar).append(text.replace(quoteChar, escapeChar + quoteChar)).append(quoteChar);
                        } else {
                            sb.append(csvQuoteIfNeeded(text, delimiter, quoteChar, escapeChar, nullString));
                        }
                    } else {
                        sb.append(escapeTextCopy(text));
                    }
                }
            }
            sb.append('\n');
            sendCopyDataMessage(ctx, sb.toString().getBytes(StandardCharsets.UTF_8));
        }

        // CopyDone + CommandComplete
        sendCopyDone(ctx);
        PgWireHandler.sendCommandComplete(ctx, "COPY " + result.getRows().size());
    }

    /** Send binary format COPY TO output with PGCOPY header. */
    private void sendBinaryCopyOut(ChannelHandlerContext ctx, QueryResult result, int numCols) {
        ByteBuf hdr = ctx.alloc().buffer();
        hdr.writeByte('H');
        hdr.writeInt(4 + 1 + 2 + numCols * 2);
        hdr.writeByte(1); // binary format
        hdr.writeShort(numCols);
        for (int i = 0; i < numCols; i++) hdr.writeShort(1);
        ctx.write(hdr);

        DataType[] colTypes = new DataType[numCols];
        for (int i = 0; i < numCols; i++) colTypes[i] = result.getColumns().get(i).getType();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // PGCOPY signature
            baos.write(new byte[]{'P','G','C','O','P','Y','\n',(byte)0xFF,'\r','\n',0});
            baos.write(new byte[]{0, 0, 0, 0}); // flags
            baos.write(new byte[]{0, 0, 0, 0}); // header extension length

            for (Object[] row : result.getRows()) {
                PgWireBinaryCodec.writeInt16(baos, numCols);
                for (int i = 0; i < numCols; i++) {
                    Object val = row[i];
                    if (val == null) {
                        PgWireBinaryCodec.writeInt32(baos, -1);
                    } else {
                        byte[] encoded = PgWireBinaryCodec.encodeBinaryValue(val, colTypes[i]);
                        PgWireBinaryCodec.writeInt32(baos, encoded.length);
                        baos.write(encoded);
                    }
                }
            }
            PgWireBinaryCodec.writeInt16(baos, -1); // trailer
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }

        sendCopyDataMessage(ctx, baos.toByteArray());
        sendCopyDone(ctx);
        PgWireHandler.sendCommandComplete(ctx, "COPY " + result.getRows().size());
    }

    // ---- COPY FROM STDIN ----

    /** Send CopyInResponse and enter copy-from mode. */
    void sendCopyInResult(ChannelHandlerContext ctx, QueryResult result) {
        CopyStmt copyStmt = result.getCopyStmt();
        boolean isBinary = "binary".equalsIgnoreCase(copyStmt.format());
        int numCols;
        if (copyStmt.columns() != null && !copyStmt.columns().isEmpty()) {
            numCols = copyStmt.columns().size();
        } else {
            numCols = session.getTableColumnCount(copyStmt.table());
        }

        ByteBuf hdr = ctx.alloc().buffer();
        hdr.writeByte('G');
        hdr.writeInt(4 + 1 + 2 + numCols * 2);
        hdr.writeByte(isBinary ? 1 : 0);
        hdr.writeShort(numCols);
        for (int i = 0; i < numCols; i++) hdr.writeShort(isBinary ? 1 : 0);
        ctx.writeAndFlush(hdr);

        inCopyFromMode = true;
        activeCopyStmt = copyStmt;
        copyBuffer = new ByteArrayOutputStream();
        copyRowCount = 0;
    }

    /** Handle incoming CopyData message. */
    void handleCopyData(ChannelHandlerContext ctx, PgWireMessage msg) {
        byte[] data = msg.getCopyData();
        if (data != null) {
            copyBuffer.write(data, 0, data.length);
        }
    }

    /** Handle CopyDone: parse and insert all collected data. */
    void handleCopyDone(ChannelHandlerContext ctx) {
        try {
            Set<Object[]> insertedRows = Collections.newSetFromMap(new IdentityHashMap<>());
            boolean isBinary = "binary".equalsIgnoreCase(activeCopyStmt.format());

            if (isBinary) {
                parseBinaryCopyData(insertedRows);
            } else {
                java.nio.charset.Charset copyCharset = StandardCharsets.UTF_8;
                if (activeCopyStmt.encoding() != null) {
                    try {
                        copyCharset = java.nio.charset.Charset.forName(activeCopyStmt.encoding());
                    } catch (Exception ignored) { /* fall back to UTF-8 */ }
                }
                String data = new String(copyBuffer.toByteArray(), copyCharset);
                boolean isCsv = "csv".equalsIgnoreCase(activeCopyStmt.format());
                String delimiter = activeCopyStmt.delimiter();
                if (delimiter == null) delimiter = isCsv ? "," : "\t";
                String nullStr = activeCopyStmt.nullString();
                if (nullStr == null) nullStr = isCsv ? "" : "\\N";
                boolean header = activeCopyStmt.header();
                boolean onErrorIgnore = "ignore".equalsIgnoreCase(activeCopyStmt.onError());

                String[] lines;
                if (isCsv) {
                    lines = splitCsvLines(data);
                } else {
                    lines = data.split("\n", -1);
                }

                boolean first = true;
                for (String rawLine : lines) {
                    String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
                    if (line.isEmpty()) continue;
                    if (!isCsv && line.equals("\\.")) break;
                    if (header && first) {
                        first = false;
                        // HEADER MATCH: validate column names match
                        if (activeCopyStmt.headerMatch()) {
                            List<String> headerValues;
                            if (isCsv) {
                                headerValues = parseCsvLine(line, delimiter, nullStr);
                            } else {
                                headerValues = parseTextLine(line, delimiter, nullStr);
                            }
                            List<String> expectedCols = activeCopyStmt.columns();
                            if (expectedCols != null && !expectedCols.isEmpty()) {
                                if (headerValues.size() != expectedCols.size()) {
                                    throw new com.memgres.engine.MemgresException(
                                            "COPY HEADER MATCH: column count mismatch", "22P04");
                                }
                                for (int hi = 0; hi < expectedCols.size(); hi++) {
                                    String expected = expectedCols.get(hi).toLowerCase();
                                    String actual = headerValues.get(hi) != null ? headerValues.get(hi).trim().toLowerCase() : "";
                                    if (!expected.equals(actual)) {
                                        throw new com.memgres.engine.MemgresException(
                                                "column name mismatch in header line field " + (hi + 1) +
                                                ": got \"" + headerValues.get(hi) + "\", expected \"" + expectedCols.get(hi) + "\"",
                                                "22P04");
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    first = false;

                    List<String> values;
                    if (isCsv) {
                        values = parseCsvLine(line, delimiter, nullStr);
                    } else {
                        values = parseTextLine(line, delimiter, nullStr);
                    }

                    try {
                        Object[] insertedRow = session.executeCopyFromRow(activeCopyStmt, values);
                        if (insertedRow != null) insertedRows.add(insertedRow);
                        copyRowCount++;
                    } catch (Exception rowErr) {
                        if (onErrorIgnore) continue;
                        if (!insertedRows.isEmpty()) {
                            try {
                                session.deleteInsertedRows(activeCopyStmt.table(), insertedRows);
                            } catch (Exception rollbackErr) {
                                LOG.error("Error rolling back COPY rows", rollbackErr);
                            }
                        }
                        throw rowErr;
                    }
                }
            }

            PgWireHandler.sendCommandComplete(ctx, "COPY " + copyRowCount);
        } catch (MemgresException e) {
            PgWireHandler.sendErrorSimple(ctx, e.getSqlState(), e.getMessage());
        } catch (Exception e) {
            LOG.error("Error during COPY FROM", e);
            PgWireHandler.sendErrorSimple(ctx, "XX000", "COPY FROM failed: " + e.getMessage());
        } finally {
            resetCopyState();
            PgWireHandler.sendReadyForQuery(ctx, session);
        }
    }

    /** Parse and insert binary format COPY FROM data. */
    private void parseBinaryCopyData(Set<Object[]> insertedRows) {
        byte[] raw = copyBuffer.toByteArray();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(raw);
        buf.position(11); // skip PGCOPY signature
        buf.getInt();     // skip flags
        int extLen = buf.getInt();
        buf.position(buf.position() + extLen);

        DataType[] colTypes = resolveActiveCopyColumnTypes();

        while (buf.remaining() >= 2) {
            short fieldCount = buf.getShort();
            if (fieldCount == -1) break;
            List<String> values = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                int len = buf.getInt();
                if (len == -1) {
                    values.add(null);
                } else {
                    byte[] fieldData = new byte[len];
                    buf.get(fieldData);
                    DataType dt = (colTypes != null && i < colTypes.length) ? colTypes[i] : null;
                    values.add(PgWireBinaryCodec.decodeBinaryField(fieldData, dt));
                }
            }
            Object[] insertedRow = session.executeCopyFromRow(activeCopyStmt, values);
            if (insertedRow != null) insertedRows.add(insertedRow);
            copyRowCount++;
        }
    }

    /** Resolve column types for the active COPY FROM statement. */
    private DataType[] resolveActiveCopyColumnTypes() {
        if (activeCopyStmt == null || activeCopyStmt.table() == null) return null;
        try {
            Table table = session.resolveTable(activeCopyStmt.table());
            if (table == null) return null;
            List<Column> cols = table.getColumns();
            if (activeCopyStmt.columns() != null && !activeCopyStmt.columns().isEmpty()) {
                DataType[] types = new DataType[activeCopyStmt.columns().size()];
                for (int i = 0; i < types.length; i++) {
                    String colName = activeCopyStmt.columns().get(i);
                    for (Column col : cols) {
                        if (col.getName().equalsIgnoreCase(colName)) {
                            types[i] = col.getType();
                            break;
                        }
                    }
                }
                return types;
            }
            DataType[] types = new DataType[cols.size()];
            for (int i = 0; i < cols.size(); i++) types[i] = cols.get(i).getType();
            return types;
        } catch (Exception e) {
            return null;
        }
    }

    /** Handle CopyFail: abort the COPY. */
    void handleCopyFail(ChannelHandlerContext ctx, PgWireMessage msg) {
        String errorMsg = msg.getQuery();
        resetCopyState();
        PgWireHandler.sendErrorSimple(ctx, "57014", errorMsg != null ? errorMsg : "COPY FROM STDIN failed");
    }

    private void resetCopyState() {
        inCopyFromMode = false;
        activeCopyStmt = null;
        copyBuffer = null;
        copyRowCount = 0;
    }

    // ---- Wire helpers ----

    private static void sendCopyDataMessage(ChannelHandlerContext ctx, byte[] data) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('d');
        buf.writeInt(4 + data.length);
        buf.writeBytes(data);
        ctx.write(buf);
    }

    private static void sendCopyDone(ChannelHandlerContext ctx) {
        ByteBuf done = ctx.alloc().buffer();
        done.writeByte('c');
        done.writeInt(4);
        ctx.write(done);
    }

    // ---- Text/CSV format helpers ----

    /** Escape a value for text-format COPY output. */
    static String escapeTextCopy(String val) {
        StringBuilder sb = new StringBuilder(val.length());
        for (int i = 0; i < val.length(); i++) {
            char c = val.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }

    /** Quote a value for CSV-format COPY output if needed. */
    static String csvQuoteIfNeeded(String val, String delimiter, String quoteChar, String escapeChar, String nullString) {
        boolean mustQuoteEmpty = val.isEmpty() && nullString.isEmpty();
        if (mustQuoteEmpty || val.contains(quoteChar) || val.contains(delimiter) || val.contains("\n") || val.contains("\r")) {
            return quoteChar + val.replace(quoteChar, escapeChar + quoteChar) + quoteChar;
        }
        return val;
    }

    /** Parse a line in text COPY format. */
    static List<String> parseTextLine(String line, String delimiter, String nullStr) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < line.length()) {
            if (line.startsWith(delimiter, i)) {
                String val = current.toString();
                values.add(val.equals(nullStr) ? null : val);
                current.setLength(0);
                i += delimiter.length();
            } else if (line.charAt(i) == '\\' && i + 1 < line.length()) {
                char next = line.charAt(i + 1);
                switch (next) {
                    case 'n':
                        current.append('\n');
                        break;
                    case 't':
                        current.append('\t');
                        break;
                    case 'r':
                        current.append('\r');
                        break;
                    case '\\':
                        current.append('\\');
                        break;
                    case 'N':
                        current.append("\\N");
                        break;
                    default: {
                        current.append('\\'); current.append(next); 
                        break;
                    }
                }
                i += 2;
            } else {
                current.append(line.charAt(i));
                i++;
            }
        }
        String val = current.toString();
        values.add(val.equals(nullStr) ? null : val);
        return values;
    }

    /** Parse a CSV line respecting quoting. */
    static List<String> parseCsvLine(String line, String delimiter, String nullStr) {
        List<String> values = new ArrayList<>();
        int i = 0;
        while (i <= line.length()) {
            if (i == line.length()) {
                values.add(null);
                break;
            }
            if (line.charAt(i) == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < line.length()) {
                    if (line.charAt(i) == '"') {
                        if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                            sb.append('"');
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        sb.append(line.charAt(i));
                        i++;
                    }
                }
                values.add(sb.toString());
                if (i < line.length() && line.startsWith(delimiter, i)) {
                    i += delimiter.length();
                } else {
                    break;
                }
            } else {
                int delimIdx = line.indexOf(delimiter, i);
                String field;
                if (delimIdx >= 0) {
                    field = line.substring(i, delimIdx);
                    i = delimIdx + delimiter.length();
                } else {
                    field = line.substring(i);
                    i = line.length();
                }
                if (field.equals(nullStr) || (nullStr.isEmpty() && field.isEmpty())) {
                    values.add(null);
                } else {
                    values.add(field);
                }
                if (delimIdx < 0) break;
            }
        }
        return values;
    }

    /** Split CSV data into lines, respecting quoted fields that may contain newlines. */
    static String[] splitCsvLines(String data) {
        List<String> lines = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < data.length(); i++) {
            char c = data.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
                current.append(c);
            } else if (c == '\n' && !inQuote) {
                if (current.length() > 0) {
                    lines.add(current.toString());
                    current.setLength(0);
                }
            } else if (c == '\r' && !inQuote) {
                // Skip \r
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            lines.add(current.toString());
        }
        return lines.toArray(new String[0]);
    }
}
