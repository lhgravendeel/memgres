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
    /** The handler this COPY belongs to, so a failure can be answered the way its protocol wants. */
    private final PgWireHandler owner;

    // COPY FROM STDIN state
    boolean inCopyFromMode;
    com.memgres.engine.parser.ast.CopyStmt activeCopyStmt;
    ByteArrayOutputStream copyBuffer;
    int copyRowCount;
    /** Rows ON_ERROR ignore dropped: REJECT_LIMIT caps them and a NOTICE reports them at the end. */
    long copySkippedCount;
    /**
     * How many lines of the input the copy has read, and the last of them as the sender wrote it.
     *
     * <p>PostgreSQL counts every line it reads, the header among them, and a line that a quoted
     * field carried a newline through counts once however many newlines it holds. It keeps the
     * line itself for as long as it may still have to say which one was refused.
     */
    long copyLineNumber;
    String copyLineText;
    /**
     * True when Execute opened this COPY rather than a simple Query. The extended protocol's
     * ReadyForQuery belongs to Sync alone, so a COPY that ends under it must not send one of its
     * own — a client counting them was one message out of step from that point on.
     */
    boolean copyFromExtended;

    PgWireCopyHandler(Session session, PgWireHandler owner) {
        this.session = session;
        this.owner = owner;
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
    void sendCopyInResult(ChannelHandlerContext ctx, QueryResult result, boolean extended) {
        CopyStmt copyStmt = result.getCopyStmt();
        MemgresException refusal = result.getCopyRefusal();
        boolean isBinary = "binary".equalsIgnoreCase(copyStmt.format());
        int numCols;
        if (copyStmt.columns() != null && !copyStmt.columns().isEmpty()) {
            numCols = copyStmt.columns().size();
        } else if (refusal != null) {
            // Nothing is going to be sent, and the relation this names is not one whose columns
            // can be counted here, so the count says nothing rather than guessing.
            numCols = 0;
        } else {
            // A generated column takes no field in the data, so the count the CopyInResponse
            // advertises has to leave it out — the sender writes one field fewer than the
            // relation has columns.
            numCols = nonGeneratedColumnCount(copyStmt.table());
        }

        ByteBuf hdr = ctx.alloc().buffer();
        hdr.writeByte('G');
        hdr.writeInt(4 + 1 + 2 + numCols * 2);
        hdr.writeByte(isBinary ? 1 : 0);
        hdr.writeShort(numCols);
        for (int i = 0; i < numCols; i++) hdr.writeShort(isBinary ? 1 : 0);
        ctx.writeAndFlush(hdr);

        if (refusal != null) {
            // PostgreSQL opens the copy and only then finds it cannot store rows in this relation,
            // so the client is told to send data and told the statement failed one message later.
            // The refusal is raised here, with the CopyInResponse already on its way out, so that
            // it is answered like any other failed statement; copy mode is never entered, and the
            // CopyData, CopyDone or CopyFail the client sends on the strength of the
            // CopyInResponse are read and thrown away as PostgreSQL throws them away.
            throw refusal;
        }

        // PostgreSQL opens the copy before it fires anything, so the relation's BEFORE statement
        // triggers run here rather than while the statement was still being judged: one that
        // refuses the copy is reported after the client has been told to send its data, and copy
        // mode is never entered, so what it sends on the strength of the CopyInResponse is read
        // and thrown away.
        session.beginCopyFrom(copyStmt);

        inCopyFromMode = true;
        activeCopyStmt = copyStmt;
        copyBuffer = new ByteArrayOutputStream();
        copyRowCount = 0;
        copySkippedCount = 0;
        copyLineNumber = 0;
        copyLineText = null;
        copyFromExtended = extended;
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
        boolean extended = copyFromExtended;
        try {
            Set<Object[]> insertedRows = Collections.newSetFromMap(new IdentityHashMap<>());
            boolean isBinary = "binary".equalsIgnoreCase(activeCopyStmt.format());

            if (isBinary) {
                parseBinaryCopyData(insertedRows);
            } else {
                java.nio.charset.Charset copyCharset = activeCopyStmt.encoding() != null
                        ? copyCharset(activeCopyStmt.encoding()) : StandardCharsets.UTF_8;
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
                    try {
                        lines = splitCsvLines(data, quoteC, escapeC);
                    } catch (UnclosedField unclosed) {
                        copyLineNumber = unclosed.line;
                        copyLineText = unclosed.text;
                        throw new MemgresException("unterminated CSV quoted field", "22P04");
                    }
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
                    copyLineNumber++;
                    copyLineText = line;
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
                                            "wrong number of fields in header line: got "
                                            + headerValues.size() + ", expected "
                                            + expectedCols.size(), "22P04");
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

                    insertCopyRow(values, insertedRows, onErrorIgnore);
                }
            }

            // ON_ERROR ignore drops rows and still reports success, so PostgreSQL says how many
            // were left out — the count in the tag does not say that anything was.
            if (copySkippedCount > 0
                    && !"silent".equalsIgnoreCase(activeCopyStmt.logVerbosity())) {
                sendNotice(ctx, copySkippedCount == 1
                        ? "1 row was skipped due to data type incompatibility"
                        : copySkippedCount + " rows were skipped due to data type incompatibility");
            }
            // A COPY is one statement, so what it still owes once its last row has gone in is owed
            // once: every AFTER row trigger it held back, and then the relation's AFTER statement
            // triggers over everything it wrote. A refusal from either is the statement's refusal
            // and takes the rows back out with it.
            // Nothing is being read any more, so an error from here on names no line: PostgreSQL
            // holds its AFTER triggers back until the last row has gone in, by which time the copy
            // has stopped reading and the line it stopped on is no longer anybody's to report.
            copyLineNumber = 0;
            copyLineText = null;
            try {
                session.finishCopyFrom();
            } catch (RuntimeException endOfCopy) {
                rollbackCopyRows(insertedRows);
                throw endOfCopy;
            }
            PgWireHandler.sendCommandComplete(ctx, "COPY " + copyRowCount);
        } catch (MemgresException e) {
            // Everything the error carries goes out with it: a refusal from inside a COPY reaches
            // the sender with no statement text to look at, so its detail and the line it names
            // are all there is to go on.
            PgWireHandler.sendErrorWithDetails(ctx, e, extended, copyErrorContext(e));
            if (extended) owner.setErrorPendingUntilSync(ctx);
        } catch (Exception e) {
            LOG.error("Error during COPY FROM", e);
            PgWireHandler.sendErrorSimple(ctx, "XX000", "COPY FROM failed: " + e.getMessage());
            if (extended) owner.setErrorPendingUntilSync(ctx);
        } finally {
            resetCopyState();
            // Under the extended protocol the CommandComplete belongs to the Execute that opened
            // the COPY, and PostgreSQL writes it without pushing it: the completion reaches the
            // client with the next Flush or Sync it asks for. Flushing it at CopyDone handed the
            // statement's result over a round trip before PostgreSQL hands it over, which is a
            // difference a client counting messages between CopyDone and Sync can see. A failure
            // is the one thing PostgreSQL does push at once, and the refusal above has done that.
            if (!extended) {
                PgWireHandler.sendReadyForQuery(ctx, session);
            }
        }
    }

    /**
     * Store one COPY FROM row, and decide what to do when it will not go in.
     *
     * <p>One routine for the text loop and the binary loop, because the policy is one policy: a
     * row is counted only once it has been stored, ON_ERROR ignore covers only what a type's
     * input function raises, REJECT_LIMIT is a ceiling on the rows that were skipped, and a row
     * that fails for any other reason takes the whole COPY back out again. Written twice, the two
     * formats drifted: binary COPY left every row before the failure permanently stored.
     *
     * @param skippable whether ON_ERROR ignore is in force; binary COPY has no soft errors, so a
     *        binary row is always strict however the option was written
     */
    private void insertCopyRow(List<String> values, Set<Object[]> insertedRows, boolean skippable) {
        try {
            Object[] insertedRow = session.executeCopyFromRow(activeCopyStmt, values);
            // A row the WHERE rejected, or one a BEFORE trigger returned NULL for, was never
            // stored, and PostgreSQL counts only the rows that were.
            if (insertedRow == null) return;
            insertedRows.add(insertedRow);
            copyRowCount++;
        } catch (MemgresException rowErr) {
            if (skippable && isConversionError(rowErr)) {
                copySkippedCount++;
                Long limit = activeCopyStmt.rejectLimit();
                if (limit != null && copySkippedCount > limit.longValue()) {
                    rollbackCopyRows(insertedRows);
                    MemgresException over = new MemgresException("skipped more than REJECT_LIMIT ("
                            + limit + ") rows due to data type incompatibility", "22P02");
                    // The row that took the count past the limit is the one PostgreSQL is still
                    // holding when it gives up, so what it names is that row's field.
                    over.setCopyField(rowErr.getCopyField());
                    throw over;
                }
                return;
            }
            rollbackCopyRows(insertedRows);
            throw rowErr;
        } catch (RuntimeException rowErr) {
            rollbackCopyRows(insertedRows);
            throw rowErr;
        }
    }

    /**
     * What a client prints as CONTEXT under an error a COPY raised.
     *
     * <p>PostgreSQL names the relation and the line of the input it had reached, and quotes that
     * line as the sender wrote it: for a sender of thousands of lines it is the only way to tell
     * which one was refused. A failure inside a field's own reader names the field instead,
     * because the value that could not be read says more than the line it sat on. A row that
     * failed inside something running on the copy's behalf carries what that was first, the way
     * PostgreSQL stacks them.
     */
    private String copyErrorContext(MemgresException e) {
        String reading = copyReadingContext(e);
        if (e.getPgContext() == null) return reading;
        return reading == null ? e.getPgContext() : e.getPgContext() + "\n" + reading;
    }

    /** The frame the copy itself contributes, or null where it is no longer reading a line. */
    private String copyReadingContext(MemgresException e) {
        if (copyLineNumber == 0 || activeCopyStmt == null) return null;
        // A foreign key is checked once the statement has stored everything it is going to store,
        // so by the time it refuses a row the copy has no line in hand to name.
        if ("23503".equals(e.getSqlState())) return null;
        Table relation;
        try {
            relation = session.resolveTable(activeCopyStmt.table());
        } catch (RuntimeException gone) {
            return null;
        }
        StringBuilder sb = new StringBuilder("COPY ");
        sb.append(relation.getName()).append(", line ").append(copyLineNumber);
        if (e.getCopyField() != null) {
            sb.append(", ").append(e.getCopyField());
        } else if (copyLineText != null && !lineReadOverBefore(e, relation)) {
            sb.append(": \"").append(copyLineText).append('"');
        }
        return sb.toString();
    }

    /**
     * True when the line would have been read over before this refusal was raised.
     *
     * <p>PostgreSQL reads a copy's rows ahead into a buffer and stores them together, and a unique
     * or exclusion index is maintained as the buffer goes in rather than as the line is read: the
     * refusal can still say which line the row came from, but no longer what was on it. A relation
     * carrying a BEFORE row trigger is stored a row at a time, because the trigger may look at
     * what the copy has written so far, and there the line is still to hand.
     */
    private boolean lineReadOverBefore(MemgresException e, Table relation) {
        if (!"23505".equals(e.getSqlState()) && !"23P01".equals(e.getSqlState())) return false;
        for (PgTrigger trigger : session.getDatabase().getTriggersForTable(relation.getName())) {
            if (!trigger.isForEachStatement()
                    && trigger.getTiming() == PgTrigger.Timing.BEFORE
                    && trigger.getEvent() == PgTrigger.Event.INSERT) {
                return false;
            }
        }
        return !fillsAVolatileDefault(relation);
    }

    /**
     * Whether the copy has to work out a default for this relation that PostgreSQL calls volatile.
     *
     * <p>Such a default may read the relation it is filling, so PostgreSQL will not buffer rows
     * ahead of one -- the same reason a BEFORE row trigger stops it -- and the line each row came
     * from is still to hand when an index refuses it. Only a column the sender writes nothing for
     * has its default worked out at all, so a volatile default the statement's own column list
     * names is not one of these. A sequence is the exception PostgreSQL makes by name: a serial or
     * an identity column is much the commonest default there is and nextval reads nothing.
     */
    private boolean fillsAVolatileDefault(Table relation) {
        List<String> written = activeCopyStmt.columns();
        // A copy that names no columns is sent a field for every one of them and defaults none.
        if (written == null || written.isEmpty()) return false;
        for (Column col : relation.getColumns()) {
            if (col.isGenerated() || namedAmong(written, col.getName())) continue;
            String def = col.getDefaultValue();
            if (def == null) continue;
            String text = def.toLowerCase().replace(" ", "");
            for (String call : VOLATILE_DEFAULT_CALLS) {
                if (text.contains(call + "(")) return true;
            }
        }
        return false;
    }

    private static boolean namedAmong(List<String> names, String column) {
        for (String name : names) {
            if (name.equalsIgnoreCase(column)) return true;
        }
        return false;
    }

    /** What PostgreSQL records as volatile among the calls a column default is written with. */
    private static final Set<String> VOLATILE_DEFAULT_CALLS = new HashSet<>(Arrays.asList(
            "random", "random_normal", "clock_timestamp", "timeofday", "gen_random_uuid",
            "uuid_generate_v1", "uuid_generate_v4", "uuidv4", "uuidv7", "currval", "setval",
            "txid_current", "pg_current_xact_id"));

    /**
     * True for the failures ON_ERROR ignore is allowed to skip: the ones a type's input function
     * raises, which is the data-exception class. A constraint, a trigger or a row that belongs to
     * no partition is not bad data, and PostgreSQL aborts the COPY for those. Malformed COPY data
     * (22P04) is the reader's complaint rather than a type's, and is not skippable either.
     */
    private static boolean isConversionError(MemgresException e) {
        String state = e.getSqlState();
        return state != null && state.startsWith("22") && !"22P04".equals(state);
    }

    private void rollbackCopyRows(Set<Object[]> insertedRows) {
        if (insertedRows.isEmpty()) return;
        try {
            session.deleteInsertedRows(activeCopyStmt.table(), insertedRows);
        } catch (Exception rollbackErr) {
            LOG.error("Error rolling back COPY rows", rollbackErr);
        }
    }

    /** Parse and insert binary format COPY FROM data. */
    private void parseBinaryCopyData(Set<Object[]> insertedRows) {
        byte[] raw = copyBuffer.toByteArray();
        // The frame is read rather than seeked over. A stream that is not PGCOPY at all, and one
        // that stops in the middle of a field, are both bad COPY data, and PostgreSQL says which
        // of the two it is — seeking past the header reported them as internal Java errors.
        if (raw.length < COPY_SIGNATURE.length + 8 || !startsWithSignature(raw)) {
            throw new MemgresException("COPY file signature not recognized", "22P04");
        }
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(raw);
        buf.position(COPY_SIGNATURE.length);
        int flags = buf.getInt();
        if ((flags & 0xFFFF0000) != 0) {
            throw new MemgresException("unrecognized critical flags in COPY file header", "22P04");
        }
        int extLen = buf.getInt();
        if (extLen < 0 || extLen > buf.remaining()) {
            throw new MemgresException("invalid COPY file header (wrong length)", "22P04");
        }
        buf.position(buf.position() + extLen);

        DataType[] colTypes = resolveActiveCopyColumnTypes();
        int expectedFields = colTypes != null ? colTypes.length : -1;

        // PostgreSQL tolerates a stream that ends without the -1 trailer, so its absence is not
        // an error here either.
        while (buf.remaining() >= 2) {
            short fieldCount = buf.getShort();
            if (fieldCount == -1) break;
            if (expectedFields >= 0 && fieldCount != expectedFields) {
                throw new MemgresException("row field count is " + fieldCount
                        + ", expected " + expectedFields, "22P04");
            }
            List<String> values = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                if (buf.remaining() < 4) throw unexpectedCopyEof();
                int len = buf.getInt();
                if (len == -1) {
                    values.add(null);
                } else if (len < 0 || len > buf.remaining()) {
                    throw unexpectedCopyEof();
                } else {
                    byte[] fieldData = new byte[len];
                    buf.get(fieldData);
                    DataType dt = (colTypes != null && i < colTypes.length) ? colTypes[i] : null;
                    values.add(PgWireBinaryCodec.decodeBinaryField(fieldData, dt));
                }
            }
            copyLineNumber++;
            insertCopyRow(values, insertedRows, false);
        }
    }

    private static final byte[] COPY_SIGNATURE =
            {'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0};

    private static boolean startsWithSignature(byte[] raw) {
        for (int i = 0; i < COPY_SIGNATURE.length; i++) {
            if (raw[i] != COPY_SIGNATURE[i]) return false;
        }
        return true;
    }

    private static MemgresException unexpectedCopyEof() {
        return new MemgresException("unexpected EOF in COPY data", "22P04");
    }

    /** How many fields the data carries: a generated column takes none, in either direction. */
    private int nonGeneratedColumnCount(String tableName) {
        Table table = session.resolveTable(tableName);
        int n = 0;
        for (Column col : table.getColumns()) {
            if (!col.isGenerated()) n++;
        }
        return n;
    }

    /**
     * The Java charset a PostgreSQL encoding name asks for. The names are PostgreSQL's own —
     * LATIN1, WIN1252, SJIS — and {@code Charset.forName} knows none of them, so the option fell
     * back to UTF-8 without saying so and every byte outside ASCII came out as another character.
     */
    private static java.nio.charset.Charset copyCharset(String pgName) {
        StringBuilder key = new StringBuilder(pgName.length());
        for (int i = 0; i < pgName.length(); i++) {
            char c = pgName.charAt(i);
            if (Character.isLetterOrDigit(c)) key.append(Character.toLowerCase(c));
        }
        String javaName = PG_TO_JAVA_CHARSET.get(key.toString());
        try {
            return java.nio.charset.Charset.forName(javaName != null ? javaName : pgName);
        } catch (Exception e) {
            return StandardCharsets.UTF_8;
        }
    }

    private static final Map<String, String> PG_TO_JAVA_CHARSET = new HashMap<>();

    static {
        PG_TO_JAVA_CHARSET.put("utf8", "UTF-8");
        PG_TO_JAVA_CHARSET.put("unicode", "UTF-8");
        // SQL_ASCII does no conversion at all, so the byte-preserving charset is the closest Java
        // has to it: every byte reads back as the character with that code point.
        PG_TO_JAVA_CHARSET.put("sqlascii", "ISO-8859-1");
        PG_TO_JAVA_CHARSET.put("latin1", "ISO-8859-1");
        PG_TO_JAVA_CHARSET.put("iso88591", "ISO-8859-1");
        PG_TO_JAVA_CHARSET.put("latin2", "ISO-8859-2");
        PG_TO_JAVA_CHARSET.put("iso88592", "ISO-8859-2");
        PG_TO_JAVA_CHARSET.put("latin3", "ISO-8859-3");
        PG_TO_JAVA_CHARSET.put("iso88593", "ISO-8859-3");
        PG_TO_JAVA_CHARSET.put("latin4", "ISO-8859-4");
        PG_TO_JAVA_CHARSET.put("iso88594", "ISO-8859-4");
        PG_TO_JAVA_CHARSET.put("iso88595", "ISO-8859-5");
        PG_TO_JAVA_CHARSET.put("iso88596", "ISO-8859-6");
        PG_TO_JAVA_CHARSET.put("iso88597", "ISO-8859-7");
        PG_TO_JAVA_CHARSET.put("iso88598", "ISO-8859-8");
        PG_TO_JAVA_CHARSET.put("latin5", "ISO-8859-9");
        PG_TO_JAVA_CHARSET.put("iso88599", "ISO-8859-9");
        PG_TO_JAVA_CHARSET.put("latin7", "ISO-8859-13");
        PG_TO_JAVA_CHARSET.put("iso885913", "ISO-8859-13");
        PG_TO_JAVA_CHARSET.put("latin9", "ISO-8859-15");
        PG_TO_JAVA_CHARSET.put("iso885915", "ISO-8859-15");
        PG_TO_JAVA_CHARSET.put("koi8", "KOI8-R");
        PG_TO_JAVA_CHARSET.put("koi8r", "KOI8-R");
        PG_TO_JAVA_CHARSET.put("koi8u", "KOI8-U");
        PG_TO_JAVA_CHARSET.put("win", "windows-1251");
        for (int cp = 1250; cp <= 1258; cp++) {
            String name = "windows-" + cp;
            PG_TO_JAVA_CHARSET.put("win" + cp, name);
            PG_TO_JAVA_CHARSET.put("cp" + cp, name);
            PG_TO_JAVA_CHARSET.put("windows" + cp, name);
        }
        PG_TO_JAVA_CHARSET.put("alt", "IBM866");
        PG_TO_JAVA_CHARSET.put("win866", "IBM866");
        PG_TO_JAVA_CHARSET.put("cp866", "IBM866");
        PG_TO_JAVA_CHARSET.put("win874", "x-windows-874");
        PG_TO_JAVA_CHARSET.put("cp874", "x-windows-874");
        PG_TO_JAVA_CHARSET.put("tis620", "TIS-620");
        PG_TO_JAVA_CHARSET.put("thai", "TIS-620");
        PG_TO_JAVA_CHARSET.put("sjis", "Shift_JIS");
        PG_TO_JAVA_CHARSET.put("mskanji", "Shift_JIS");
        PG_TO_JAVA_CHARSET.put("shiftjis", "Shift_JIS");
        PG_TO_JAVA_CHARSET.put("win932", "Shift_JIS");
        PG_TO_JAVA_CHARSET.put("cp932", "Shift_JIS");
        PG_TO_JAVA_CHARSET.put("eucjp", "EUC-JP");
        PG_TO_JAVA_CHARSET.put("euckr", "EUC-KR");
        PG_TO_JAVA_CHARSET.put("uhc", "x-windows-949");
        PG_TO_JAVA_CHARSET.put("win949", "x-windows-949");
        PG_TO_JAVA_CHARSET.put("cp949", "x-windows-949");
        PG_TO_JAVA_CHARSET.put("euccn", "GB2312");
        PG_TO_JAVA_CHARSET.put("gbk", "GBK");
        PG_TO_JAVA_CHARSET.put("win936", "GBK");
        PG_TO_JAVA_CHARSET.put("cp936", "GBK");
        PG_TO_JAVA_CHARSET.put("gb18030", "GB18030");
        PG_TO_JAVA_CHARSET.put("big5", "Big5");
        PG_TO_JAVA_CHARSET.put("bigfive", "Big5");
        PG_TO_JAVA_CHARSET.put("win950", "Big5");
        PG_TO_JAVA_CHARSET.put("cp950", "Big5");
    }

    /**
     * A NoticeResponse. It is how PostgreSQL reports the rows ON_ERROR ignore dropped: the COPY
     * succeeded, and nothing in its tag would tell the client that anything was left out.
     */
    private static void sendNotice(ChannelHandlerContext ctx, String message) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('N');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, "NOTICE");
        buf.writeByte('V');
        PgWireValueFormatter.writeCString(buf, "NOTICE");
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, "00000");
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, message);
        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    /** Resolve column names for the active COPY FROM statement's table (for HEADER MATCH). */
    private List<String> resolveActiveCopyColumnNames() {
        if (activeCopyStmt == null || activeCopyStmt.table() == null) return null;
        try {
            Table table = session.resolveTable(activeCopyStmt.table());
            if (table == null) return null;
            List<String> names = new ArrayList<>();
            for (Column col : table.getColumns()) {
                // A generated column has no field in the data, so it has no header field either.
                if (col.isGenerated()) continue;
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
            List<DataType> types = new ArrayList<>();
            for (Column col : cols) {
                if (col.isGenerated()) continue;
                types.add(col.getType());
            }
            return types.toArray(new DataType[0]);
        } catch (Exception e) {
            return null;
        }
    }

    /** Handle CopyFail: abort the COPY. */
    void handleCopyFail(ChannelHandlerContext ctx, PgWireMessage msg) {
        String errorMsg = msg.getQuery();
        boolean extended = copyFromExtended;
        MemgresException failed = new MemgresException(
                "COPY from stdin failed: " + (errorMsg != null ? errorMsg : ""), "57014");
        // PostgreSQL counts a line as it starts on it rather than as it finishes, so a copy the
        // sender gave up on names the line it was still waiting for.
        String reading = copyFailContext();
        resetCopyState();
        // PostgreSQL says whose failure it was and quotes what the client sent. The error also has
        // to reach the client now: a CopyFail sent on its own is followed by nothing that would
        // flush it, so a client that sends one and waits was waiting on a buffer.
        PgWireHandler.sendErrorWithDetails(ctx, failed, extended, reading);
        if (extended) {
            owner.setErrorPendingUntilSync(ctx);
        } else {
            PgWireHandler.sendReadyForQuery(ctx, session);
        }
    }

    /** The relation and the line a copy was waiting for, for a sender that gave up on it. */
    private String copyFailContext() {
        if (activeCopyStmt == null) return null;
        long waitingFor = lineWaitedFor();
        // A binary copy reads its header before it is reading lines at all, and PostgreSQL has not
        // yet said what the copy is doing when it does: a sender that gives up before writing the
        // header is answered with no context field of any kind.
        if (waitingFor == 0) return null;
        try {
            return "COPY " + session.resolveTable(activeCopyStmt.table()).getName()
                    + ", line " + waitingFor;
        } catch (RuntimeException gone) {
            return null;
        }
    }

    /**
     * The line the copy would have read next, or zero where it had not begun reading lines.
     *
     * <p>PostgreSQL counts a line as it starts on it rather than as it finishes, so a copy nothing
     * was ever sent to is waiting on line 1 and one that was sent two whole rows is waiting on line
     * 3, whatever it has since done with them. Nothing here has been read yet -- the reading is one
     * pass once the copy ends -- so the count is taken over what the sender did write.
     */
    private long lineWaitedFor() {
        byte[] sent = copyBuffer == null ? new byte[0] : copyBuffer.toByteArray();
        if ("binary".equalsIgnoreCase(activeCopyStmt.format())) {
            if (sent.length < COPY_SIGNATURE.length + 8 || !startsWithSignature(sent)) return 0;
            return 1 + wholeBinaryRows(sent);
        }
        java.nio.charset.Charset charset = activeCopyStmt.encoding() != null
                ? copyCharset(activeCopyStmt.encoding()) : StandardCharsets.UTF_8;
        String text = new String(sent, charset);
        boolean isCsv = "csv".equalsIgnoreCase(activeCopyStmt.format());
        char quoteC = activeCopyStmt.quote() != null && !activeCopyStmt.quote().isEmpty()
                ? activeCopyStmt.quote().charAt(0) : '"';
        char escapeC = activeCopyStmt.escape() != null && !activeCopyStmt.escape().isEmpty()
                ? activeCopyStmt.escape().charAt(0) : quoteC;
        long complete = 0;
        boolean inQuote = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (isCsv && inQuote && c == escapeC && i + 1 < text.length()
                    && (text.charAt(i + 1) == quoteC || text.charAt(i + 1) == escapeC)) {
                i++;
            } else if (isCsv && c == quoteC) {
                inQuote = !inQuote;
            } else if (!inQuote && (c == '\n' || (isCsv && c == '\r'))) {
                if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') i++;
                complete++;
            }
        }
        return complete + 1;
    }

    /** How many whole rows a sender's binary stream holds after the header it opened with. */
    private static long wholeBinaryRows(byte[] sent) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(sent);
        buf.position(COPY_SIGNATURE.length + 4);
        int extLen = buf.getInt();
        if (extLen < 0 || extLen > buf.remaining()) return 0;
        buf.position(buf.position() + extLen);
        long rows = 0;
        while (buf.remaining() >= 2) {
            short fieldCount = buf.getShort();
            if (fieldCount < 0) return rows;
            for (int i = 0; i < fieldCount; i++) {
                if (buf.remaining() < 4) return rows;
                int len = buf.getInt();
                if (len == -1) continue;
                if (len < 0 || len > buf.remaining()) return rows;
                buf.position(buf.position() + len);
            }
            rows++;
        }
        return rows;
    }

    private void resetCopyState() {
        // A copy that ended without finishing owes nothing: its rows have gone back out again.
        session.discardCopyFrom();
        inCopyFromMode = false;
        activeCopyStmt = null;
        copyBuffer = null;
        copyRowCount = 0;
        copySkippedCount = 0;
        copyLineNumber = 0;
        copyLineText = null;
        copyFromExtended = false;
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
        // PostgreSQL counts a newline carried through a quoted field as a line of its own, but only
        // once it knows what a line terminator looks like in this input -- which it learns from the
        // first one it reads outside quotes, and takes to be a carriage return until then. That is
        // why an input whose only newline sits inside an unterminated field is still line 1 while
        // the same field on the second line of an input is line 3.
        char embeddedTerminator = '\r';
        boolean terminatorKnown = false;
        long quotedTerminators = 0;
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
                if (c == embeddedTerminator) quotedTerminators++;
                if (c == quote) {
                    inQuote = false;
                }
                current.append(c);
                i++;
            } else if (c == '\n' || c == '\r') {
                if (c == '\r' && i + 1 < data.length() && data.charAt(i + 1) == '\n') {
                    i++; // \r\n is a single terminator
                }
                if (!terminatorKnown) {
                    embeddedTerminator = c;
                    terminatorKnown = true;
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
            throw new UnclosedField(lines.size() + 1 + quotedTerminators, current.toString());
        }
        if (current.length() > 0) {
            lines.add(current.toString());
        }
        return lines.toArray(new String[0]);
    }

    /**
     * The tail of an input whose last field never closed its quote, with the line PostgreSQL says
     * it was on. Only the copy knows what the relation is called, so the splitter hands the two
     * back rather than raising the refusal itself.
     */
    private static final class UnclosedField extends RuntimeException {
        final long line;
        final String text;

        UnclosedField(long line, String text) {
            super(null, null, false, false);
            this.line = line;
            this.text = text;
        }
    }
}
