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
        char delimC = delimiter != null && !delimiter.isEmpty() ? delimiter.charAt(0) : (isCsv ? ',' : '\t');
        char quoteC = quoteChar.isEmpty() ? '"' : quoteChar.charAt(0);
        char escapeC = escapeChar.isEmpty() ? quoteC : escapeChar.charAt(0);
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
                        if (forceQuoteIndices.contains(i)
                                || (numCols == 1 && "\\.".equals(text))) {
                            // Force-quoted, or a lone \. field that would otherwise
                            // read back as the end-of-data marker.
                            sb.append(csvQuote(text, quoteC, escapeC));
                        } else {
                            sb.append(csvQuoteIfNeeded(text, delimiter, quoteChar, escapeChar, nullString));
                        }
                    } else {
                        sb.append(escapeTextCopy(text, delimC));
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
                String defaultStr = activeCopyStmt.defaultString();
                char quoteC = activeCopyStmt.quote() != null && !activeCopyStmt.quote().isEmpty()
                        ? activeCopyStmt.quote().charAt(0) : '"';
                char escapeC = activeCopyStmt.escape() != null && !activeCopyStmt.escape().isEmpty()
                        ? activeCopyStmt.escape().charAt(0) : quoteC;

                String[] lines;
                if (isCsv) {
                    lines = splitCsvLines(data, quoteC, escapeC);
                } else {
                    lines = data.split("\n", -1);
                    // The segment after the final newline is not a row (a trailing
                    // terminator must not create a phantom empty row); a line
                    // without a trailing newline still counts.
                    if (lines.length > 0 && lines[lines.length - 1].isEmpty()) {
                        lines = Arrays.copyOf(lines, lines.length - 1);
                    }
                }

                boolean first = true;
                for (String rawLine : lines) {
                    // CSV lines have terminators already handled in splitCsvLines;
                    // for text, strip a trailing \r (CRLF input).
                    String line = !isCsv && rawLine.endsWith("\r")
                            ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
                    if (line.equals("\\.")) break; // end-of-data marker (both formats)
                    if (header && first) {
                        first = false;
                        // HEADER MATCH: validate column names match
                        if (activeCopyStmt.headerMatch()) {
                            List<String> headerValues;
                            if (isCsv) {
                                headerValues = parseCsvLine(line, delimiter, nullStr, null, quoteC, escapeC);
                            } else {
                                headerValues = parseTextLine(line, delimiter, nullStr);
                            }
                            List<String> expectedCols = activeCopyStmt.columns();
                            if (expectedCols == null || expectedCols.isEmpty()) {
                                expectedCols = resolveActiveCopyColumnNames();
                            }
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
                        values = parseCsvLine(line, delimiter, nullStr, defaultStr, quoteC, escapeC);
                    } else {
                        values = parseTextLine(line, delimiter, nullStr, defaultStr);
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

    /** Resolve column names for the active COPY FROM statement's table (for HEADER MATCH). */
    private List<String> resolveActiveCopyColumnNames() {
        if (activeCopyStmt == null || activeCopyStmt.table() == null) return null;
        try {
            Table table = session.resolveTable(activeCopyStmt.table());
            if (table == null) return null;
            List<String> names = new ArrayList<>();
            for (Column col : table.getColumns()) {
                names.add(col.getName());
            }
            return names;
        } catch (Exception e) {
            return null;
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

    /** Escape a value for text-format COPY output (default tab delimiter). */
    static String escapeTextCopy(String val) {
        return escapeTextCopy(val, '\t');
    }

    /**
     * Escape a value for text-format COPY output. Matches PostgreSQL's
     * CopyAttributeOutText: backslash, the active delimiter, and the control
     * characters \b \f \n \r \t \v are escaped; everything else passes through.
     */
    static String escapeTextCopy(String val, char delimiter) {
        StringBuilder sb = new StringBuilder(val.length());
        for (int i = 0; i < val.length(); i++) {
            char c = val.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\u000B': // vertical tab
                    sb.append("\\v");
                    break;
                default:
                    if (c == delimiter) {
                        sb.append('\\');
                    }
                    sb.append(c);
                    break;
            }
        }
        return sb.toString();
    }

    /** Quote a value with the given CSV quote/escape chars, escaping embedded quote and escape chars. */
    static String csvQuote(String val, char quote, char escape) {
        StringBuilder sb = new StringBuilder(val.length() + 2);
        sb.append(quote);
        for (int i = 0; i < val.length(); i++) {
            char c = val.charAt(i);
            if (c == quote || c == escape) {
                sb.append(escape);
            }
            sb.append(c);
        }
        sb.append(quote);
        return sb.toString();
    }

    /** Quote a value for CSV-format COPY output if needed. */
    static String csvQuoteIfNeeded(String val, String delimiter, String quoteChar, String escapeChar, String nullString) {
        char quote = quoteChar.isEmpty() ? '"' : quoteChar.charAt(0);
        char escape = escapeChar.isEmpty() ? quote : escapeChar.charAt(0);
        // A data value equal to the NULL marker must be quoted so it round-trips as data.
        // PG only quotes for the QUOTE char, not the ESCAPE char (unless escape == quote).
        boolean needsQuote = val.equals(nullString)
                || val.indexOf(quote) >= 0
                || val.contains(delimiter) || val.indexOf('\n') >= 0 || val.indexOf('\r') >= 0;
        if (needsQuote) {
            return csvQuote(val, quote, escape);
        }
        return val;
    }

    /**
     * Parse a line in text COPY format.
     *
     * <p>As in PostgreSQL, the NULL marker (and DEFAULT marker) are compared against
     * the RAW field text before unescaping: input {@code \N} is NULL, while
     * {@code \\N} unescapes to the literal two-character string {@code \N}.
     * The full PG escape set is supported: {@code \b \f \n \r \t \v \\},
     * octal ({@code \o}, {@code \oo}, {@code \ooo}) and hex ({@code \xh}, {@code \xhh});
     * an unrecognized escaped character is taken literally (backslash dropped),
     * which also makes {@code \<delimiter>} a literal delimiter character.</p>
     */
    static List<String> parseTextLine(String line, String delimiter, String nullStr) {
        return parseTextLine(line, delimiter, nullStr, null);
    }

    static List<String> parseTextLine(String line, String delimiter, String nullStr, String defaultStr) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int fieldStart = 0;
        int i = 0;
        while (i < line.length()) {
            if (line.startsWith(delimiter, i)) {
                addTextField(values, line.substring(fieldStart, i), current.toString(), nullStr, defaultStr);
                current.setLength(0);
                i += delimiter.length();
                fieldStart = i;
            } else if (line.charAt(i) == '\\') {
                if (i + 1 >= line.length()) {
                    // Data ending in a lone backslash: kept literally (as PG does).
                    current.append('\\');
                    i++;
                    continue;
                }
                char next = line.charAt(i + 1);
                i += 2;
                switch (next) {
                    case 'b':
                        current.append('\b');
                        break;
                    case 'f':
                        current.append('\f');
                        break;
                    case 'n':
                        current.append('\n');
                        break;
                    case 'r':
                        current.append('\r');
                        break;
                    case 't':
                        current.append('\t');
                        break;
                    case 'v':
                        current.append('\u000B');
                        break;
                    case '\\':
                        current.append('\\');
                        break;
                    case 'x': {
                        // \xh or \xhh hex escape; \x with no hex digit is a literal 'x'
                        int val = 0;
                        int digits = 0;
                        while (digits < 2 && i < line.length() && isHexDigit(line.charAt(i))) {
                            val = val * 16 + hexValue(line.charAt(i));
                            i++;
                            digits++;
                        }
                        if (digits == 0) {
                            current.append('x');
                        } else {
                            current.append((char) val);
                        }
                        break;
                    }
                    default:
                        if (next >= '0' && next <= '7') {
                            // Octal escape: up to 3 digits total
                            int val = next - '0';
                            int digits = 1;
                            while (digits < 3 && i < line.length()
                                    && line.charAt(i) >= '0' && line.charAt(i) <= '7') {
                                val = (val << 3) + (line.charAt(i) - '0');
                                i++;
                                digits++;
                            }
                            current.append((char) (val & 0xFF));
                        } else {
                            // Unknown escape: the backslash is dropped, the char kept.
                            current.append(next);
                        }
                        break;
                }
            } else {
                current.append(line.charAt(i));
                i++;
            }
        }
        addTextField(values, line.substring(fieldStart), current.toString(), nullStr, defaultStr);
        return values;
    }

    /** Add one text-format field: NULL/DEFAULT markers match against the RAW (pre-unescape) text. */
    private static void addTextField(List<String> values, String raw, String unescaped,
                                     String nullStr, String defaultStr) {
        if (raw.equals(nullStr)) {
            values.add(null);
        } else if (defaultStr != null && raw.equals(defaultStr)) {
            // Preserve the marker verbatim so the executor's DEFAULT substitution matches.
            values.add(defaultStr);
        } else {
            values.add(unescaped);
        }
    }

    private static boolean isHexDigit(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    private static int hexValue(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        return c - 'A' + 10;
    }

    /** Parse a CSV line with default quote/escape ('"'). */
    static List<String> parseCsvLine(String line, String delimiter, String nullStr) {
        return parseCsvLine(line, delimiter, nullStr, null, '"', '"');
    }

    /**
     * Parse a CSV line respecting the configured QUOTE and ESCAPE characters.
     * A field that was quoted (even partially) is never NULL; an unquoted field
     * equal to the NULL marker is NULL. Inside quotes, escape+quote or
     * escape+escape yield the literal character; when escape == quote this is
     * the standard doubled-quote rule.
     */
    static List<String> parseCsvLine(String line, String delimiter, String nullStr,
                                     String defaultStr, char quote, char escape) {
        List<String> values = new ArrayList<>();
        int len = line.length();
        int i = 0;
        while (true) {
            StringBuilder sb = new StringBuilder();
            boolean sawQuote = false;
            int rawStart = i;
            while (i < len) {
                char c = line.charAt(i);
                if (c == quote) {
                    sawQuote = true;
                    i++;
                    while (i < len) {
                        char q = line.charAt(i);
                        if (q == escape && i + 1 < len
                                && (line.charAt(i + 1) == quote || line.charAt(i + 1) == escape)) {
                            sb.append(line.charAt(i + 1));
                            i += 2;
                        } else if (q == quote) {
                            i++;
                            break;
                        } else {
                            sb.append(q);
                            i++;
                        }
                    }
                } else if (line.startsWith(delimiter, i)) {
                    break;
                } else {
                    sb.append(c);
                    i++;
                }
            }
            String raw = line.substring(rawStart, i);
            if (!sawQuote && raw.equals(nullStr)) {
                values.add(null);
            } else if (!sawQuote && defaultStr != null && raw.equals(defaultStr)) {
                values.add(defaultStr);
            } else {
                values.add(sb.toString());
            }
            if (i < len) {
                i += delimiter.length(); // consume delimiter, parse next field
            } else {
                break;
            }
        }
        return values;
    }

    /** Split CSV data into lines with default quote/escape ('"'). */
    static String[] splitCsvLines(String data) {
        return splitCsvLines(data, '"', '"');
    }

    /**
     * Split CSV data into lines, respecting quoted fields that may contain newlines.
     * Empty lines between terminators are preserved (they are data rows in CSV);
     * a trailing final newline does not create a phantom row. Both \n, \r\n and
     * bare \r act as line terminators outside quotes.
     */
    static String[] splitCsvLines(String data, char quote, char escape) {
        List<String> lines = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        int i = 0;
        while (i < data.length()) {
            char c = data.charAt(i);
            if (inQuote) {
                if (c == escape && i + 1 < data.length()
                        && (data.charAt(i + 1) == quote || data.charAt(i + 1) == escape)) {
                    current.append(c).append(data.charAt(i + 1));
                    i += 2;
                    continue;
                }
                if (c == quote) {
                    inQuote = false;
                }
                current.append(c);
                i++;
            } else if (c == '\n' || c == '\r') {
                if (c == '\r' && i + 1 < data.length() && data.charAt(i + 1) == '\n') {
                    i++; // \r\n is a single terminator
                }
                lines.add(current.toString());
                current.setLength(0);
                i++;
            } else {
                if (c == quote) {
                    inQuote = true;
                }
                current.append(c);
                i++;
            }
        }
        if (inQuote) {
            // The input ran out with a quote still open, so the last field never ended. There is
            // no row here to store: the remainder of the input is the inside of a field whose
            // closing quote the sender did not write.
            throw new com.memgres.engine.MemgresException("unterminated CSV quoted field", "22P04");
        }
        if (current.length() > 0) {
            lines.add(current.toString());
        }
        return lines.toArray(new String[0]);
    }
}
