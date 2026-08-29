package com.memgres.pgwire;

import com.memgres.engine.*;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Value formatting for text-mode PgWire transmission,
 * plus RowDescription metadata helpers.
 */
class PgWireValueFormatter {

    /** extra_float_digits governs how many digits float4/float8 output carries. */
    static int extraFloatDigits(GucSettings guc) {
        if (guc == null) return 1;
        try {
            String v = guc.get("extra_float_digits");
            return v == null ? 1 : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /** Format a value for text-mode PgWire transmission, respecting GUC settings. */
    static String formatValue(Object val, GucSettings guc) {
        if (val instanceof byte[]) {
            byte[] ba = (byte[]) val;
            String byteaOutput = guc != null ? guc.get("bytea_output") : "hex";
            if ("escape".equalsIgnoreCase(byteaOutput)) {
                StringBuilder sb = new StringBuilder();
                for (byte b : ba) {
                    int v = b & 0xFF;
                    if (v == 0x5C) { // backslash
                        sb.append("\\\\");
                    } else if (v >= 32 && v <= 126) {
                        sb.append((char) v);
                    } else {
                        sb.append('\\');
                        sb.append((char) ('0' + ((v >> 6) & 7)));
                        sb.append((char) ('0' + ((v >> 3) & 7)));
                        sb.append((char) ('0' + (v & 7)));
                    }
                }
                return sb.toString();
            }
            StringBuilder sb = new StringBuilder("\\x");
            for (byte b : ba) sb.append(String.format("%02x", b & 0xFF));
            return sb.toString();
        }
        // The two ends of the timestamp range are written as the words they are, not as the
        // instants that stand for them.
        String infinite = com.memgres.engine.TypeCoercion.infinityText(val);
        if (infinite != null) return infinite;
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return b ? "t" : "f";
        } else if (val instanceof LocalTime) {
            LocalTime t = (LocalTime) val;
            // The end of the day is a time PostgreSQL prints as 24:00:00; java.time holds it one
            // nanosecond short, so it would otherwise read back as 23:59:59.999999.
            if (com.memgres.engine.TypeCoercion.isEndOfDay(t)) return "24:00:00";
            return t.getNano() != 0
                    ? stripTrailingFracZeros(t.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                    : t.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        } else if (val instanceof LocalDate) {
            LocalDate ld = (LocalDate) val;
            String datestyle = guc != null ? guc.get("datestyle") : "ISO, MDY";
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return ld.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return ld.format(DateTimeFormatter.ofPattern("MM/dd/yyyy"));
            }
            return com.memgres.engine.TypeCoercion.formatIsoDate(ld);
        } else if (val instanceof LocalDateTime) {
            LocalDateTime dt = (LocalDateTime) val;
            String datestyle = guc != null ? guc.get("datestyle") : "ISO, MDY";
            String timePart = dt.getNano() != 0
                    ? stripTrailingFracZeros(dt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                    : dt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return dt.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")) + " " + timePart;
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return dt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy")) + " " + timePart;
            }
            String datePart = String.format("%04d-%02d-%02d",
                    com.memgres.engine.TypeCoercion.displayYear(dt.getYear()),
                    dt.getMonthValue(), dt.getDayOfMonth());
            String era = com.memgres.engine.TypeCoercion.eraSuffix(dt.getYear());
            return (dt.getNano() != 0
                    ? stripTrailingFracZeros(datePart + " " + dt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                    : datePart + " " + dt.format(DateTimeFormatter.ofPattern("HH:mm:ss"))) + era;
        } else if (val instanceof OffsetDateTime) {
            OffsetDateTime odt = (OffsetDateTime) val;
            if (guc != null) {
                String tz = guc.get("timezone");
                if (tz != null) {
                    try {
                        java.time.ZoneId zone = java.time.ZoneId.of(tz);
                        odt = odt.atZoneSameInstant(zone).toOffsetDateTime();
                    } catch (Exception ignored) {}
                }
            }
            String datestyle = guc != null ? guc.get("datestyle") : "ISO, MDY";
            String timePart = odt.getNano() != 0
                    ? stripTrailingFracZeros(odt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                    : odt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String offsetStr = formatPgOffset(odt.getOffset());
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return odt.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")) + " " + timePart + offsetStr;
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return odt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy")) + " " + timePart + offsetStr;
            }
            // A timestamptz names its era the same way a timestamp does: there is no year zero.
            String isoDate = String.format("%04d-%02d-%02d",
                    com.memgres.engine.TypeCoercion.displayYear(odt.getYear()),
                    odt.getMonthValue(), odt.getDayOfMonth());
            String datePart = odt.getNano() != 0
                    ? stripTrailingFracZeros(isoDate + " "
                            + odt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                    : isoDate + " " + odt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            return datePart + formatPgOffset(odt.getOffset())
                    + com.memgres.engine.TypeCoercion.eraSuffix(odt.getYear());
        } else if (val instanceof PgInterval) {
            PgInterval interval = (PgInterval) val;
            String intervalStyle = guc != null ? guc.get("intervalstyle") : "postgres";
            return interval.toString(intervalStyle);
        } else if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            return bd.toPlainString();
        } else if (val instanceof Float) {
            return com.memgres.engine.PgFloatFormat.float4out((Float) val, extraFloatDigits(guc));
        } else if (val instanceof Double) {
            return com.memgres.engine.PgFloatFormat.float8out((Double) val, extraFloatDigits(guc));
        } else if (val instanceof com.memgres.engine.AstExecutor.PgEnum) {
            com.memgres.engine.AstExecutor.PgEnum enumVal = (com.memgres.engine.AstExecutor.PgEnum) val;
            return enumVal.label();
        } else if (val instanceof java.util.Map<?, ?>) {
            // A composite held as a field map (PL/pgSQL composite variables) renders as a row.
            return formatValue(
                    com.memgres.engine.AstExecutor.PgRow.fromFieldMap((java.util.Map<?, ?>) val), guc);
        } else if (val instanceof com.memgres.engine.AstExecutor.PgRow) {
            com.memgres.engine.AstExecutor.PgRow row = (com.memgres.engine.AstExecutor.PgRow) val;
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < row.values().size(); i++) {
                if (i > 0) sb.append(",");
                Object elem = row.values().get(i);
                // A NULL field prints as nothing at all, which is what tells it from an empty one
                if (elem == null) continue;
                String text = elem instanceof Boolean ? (((Boolean) elem) ? "t" : "f")
                        : formatValue(elem, guc);
                if (needsCompositeQuote(text)) {
                    sb.append('"').append(text.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
                } else {
                    sb.append(text);
                }
            }
            sb.append(")");
            return sb.toString();
        } else if (val instanceof com.memgres.engine.PgVector) {
            com.memgres.engine.PgVector vec = (com.memgres.engine.PgVector) val;
            return vec.toString();
        } else if (val instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) val;
            StringBuilder sb = new StringBuilder(boundsPrefixOf(list)).append("{");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                Object elem = list.get(i);
                if (elem == null) {
                    sb.append("NULL");
                } else if (elem instanceof com.memgres.engine.AstExecutor.PgRow) {
                    // A composite element is quoted by the same rule as any other: a bare (1) has
                    // nothing in it an array reader could mistake for structure, and PostgreSQL
                    // writes it back unquoted. Quoting every composite made {(1),(2)} come out as
                    // {"(1)","(2)"}, which is a different array of a different two strings.
                    String rowStr = formatValue(elem, guc);
                    if (needsArrayQuote(rowStr)) {
                        sb.append("\"").append(rowStr.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    } else {
                        sb.append(rowStr);
                    }
                } else if (elem instanceof String) {
                    String s = (String) elem;
                    // A backslash and the word NULL have to be quoted too: unquoted they read back
                    // as an escape and as the SQL null, so the array would not survive a round trip.
                    if (needsArrayQuote(s)) {
                        sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    } else {
                        sb.append(s);
                    }
                } else if (elem instanceof java.util.List<?>) {
                    // A dimension inside an array is written as its own braces, never quoted.
                    sb.append(formatValue(elem, guc));
                } else {
                    // Every element is quoted by the same rule, whatever its type: a timestamp's
                    // text carries a space, and unquoted it read back as two elements.
                    String text = formatValue(elem, guc);
                    if (needsArrayQuote(text)) {
                        sb.append('"').append(text.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
                    } else {
                        sb.append(text);
                    }
                }
            }
            sb.append("}");
            return sb.toString();
        } else {
            return val.toString();
        }
    }

    /**
     * Format an array column value as PG array text, formatting each element per its declared
     * element type. This differs from {@link #formatValue} in that temporal element values stored
     * as ISO strings (e.g. {@code "2020-01-02T03:04:05"}) are rendered in PG's canonical
     * space-separated form and quoted, so pgjdbc's text-mode array parser (which is what it uses by
     * default for {@code timestamp[]}/etc.) accepts them instead of choking on the {@code 'T'}.
     */
    static String formatArray(Object val, DataType elemType, GucSettings guc) {
        java.util.List<?> list;
        if (val instanceof java.util.List<?>) {
            list = (java.util.List<?>) val;
        } else if (val instanceof String && isTemporalArrayElem(elemType)
                && ((String) val).startsWith("{") && ((String) val).endsWith("}")) {
            // Raw array-literal string form for a temporal element type: parse it so each element
            // can be re-rendered in PG's space-separated, quoted form (pgjdbc's text array parser
            // silently drops the space of an unquoted "2020-01-02 03:04:05", so quoting is required).
            list = parseArrayLiteralElements((String) val);
        } else {
            return formatValue(val, guc);
        }
        StringBuilder sb = new StringBuilder(boundsPrefixOf(list)).append("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(',');
            Object e = list.get(i);
            if (e == null) { sb.append("NULL"); continue; }
            if (e instanceof java.util.List<?>) { sb.append(formatArray(e, elemType, guc)); continue; }
            String d = formatArrayElement(e, elemType, guc);
            if (needsArrayQuote(d)) {
                sb.append('"').append(d.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            } else {
                sb.append(d);
            }
        }
        sb.append('}');
        return sb.toString();
    }

    /** Parse a flat PG array literal ({@code {a,"b c",NULL}}) into element strings (null for NULL). */
    private static java.util.List<Object> parseArrayLiteralElements(String text) {
        java.util.List<Object> elements = new java.util.ArrayList<>();
        String inner = text.substring(1, text.length() - 1);
        if (inner.isEmpty()) return elements;
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean quoted = false;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (inQuotes) {
                if (c == '\\' && i + 1 < inner.length()) current.append(inner.charAt(++i));
                else if (c == '"') inQuotes = false;
                else current.append(c);
            } else if (c == '"') {
                inQuotes = true; quoted = true;
            } else if (c == ',') {
                elements.add((!quoted && current.toString().equalsIgnoreCase("NULL")) ? null : current.toString());
                current.setLength(0); quoted = false;
            } else {
                current.append(c);
            }
        }
        elements.add((!quoted && current.toString().equalsIgnoreCase("NULL")) ? null : current.toString());
        return elements;
    }

    private static String formatArrayElement(Object e, DataType elemType, GucSettings guc) {
        if (e instanceof String) {
            String s = (String) e;
            if (isTemporalArrayElem(elemType)) {
                return s.replaceFirst("^(\\d{4}-\\d{2}-\\d{2})T", "$1 ");
            }
            return s;
        }
        return formatValue(e, guc);
    }

    private static boolean isTemporalArrayElem(DataType t) {
        return t == DataType.DATE || t == DataType.TIMESTAMP || t == DataType.TIMESTAMPTZ
                || t == DataType.TIME || t == DataType.TIMETZ;
    }

    /**
     * The {@code [lb:ub]=} an array whose dimensions do not start at 1 is written with. Without it
     * the client is told an ordinary array, and the bounds it was written with are gone.
     */
    private static String boundsPrefixOf(java.util.List<?> list) {
        if (!(list instanceof com.memgres.engine.PgArray)) return "";
        com.memgres.engine.PgArray array = (com.memgres.engine.PgArray) list;
        return array.hasCustomLowerBounds() ? array.boundsPrefix() : "";
    }

    private static boolean needsArrayQuote(String s) {
        if (s.isEmpty() || s.equalsIgnoreCase("NULL")) return true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' || Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * True if a composite field must be double-quoted in PG's {@code (a,b)} output syntax: an
     * empty field, or one carrying a character that would otherwise be read as structure.
     */
    private static boolean needsCompositeQuote(String s) {
        if (s.isEmpty()) return true;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ',' || c == '(' || c == ')' || c == '"' || c == '\\'
                    || Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }

    /** Strip trailing zeros from the fractional-seconds part of a formatted timestamp/time string. */
    private static String stripTrailingFracZeros(String s) {
        int dotIdx = s.lastIndexOf('.');
        if (dotIdx < 0) return s;
        int end = s.length();
        // Find where non-digit suffix starts (e.g. offset like +00)
        int fracEnd = end;
        for (int i = dotIdx + 1; i < end; i++) {
            if (!Character.isDigit(s.charAt(i))) {
                fracEnd = i;
                break;
            }
        }
        // Strip trailing zeros in the fractional part
        int last = fracEnd;
        while (last > dotIdx + 1 && s.charAt(last - 1) == '0') {
            last--;
        }
        if (last == dotIdx + 1) {
            // All fractional digits are zero — remove the dot too
            return s.substring(0, dotIdx) + s.substring(fracEnd);
        }
        return s.substring(0, last) + s.substring(fracEnd);
    }

    /** Format a timezone offset like PG: +00 when minutes==0, +05:30 otherwise. */
    static String formatPgOffset(ZoneOffset offset) {
        return com.memgres.engine.TypeCoercion.writtenOffset(offset);
    }

    /**
     * Returns the fixed storage size for a PostgreSQL type, or -1 for variable-length types.
     * Used in RowDescription messages.
     */
    static short pgTypeSize(DataType type) {
        switch (type) {
            case BOOLEAN:
                return 1;
            case SMALLINT:
            case SMALLSERIAL:
                return 2;
            case INTEGER:
            case SERIAL:
            case REAL:
            case OID:
                return 4;
            case BIGINT:
            case BIGSERIAL:
            case DOUBLE_PRECISION:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case TIME:
            case DATE:
            case INTERVAL:
                return 8;
            default:
                return -1;
        }
    }

    /**
     * Returns the PostgreSQL type modifier (atttypmod) for a column.
     * For NUMERIC(p,s): typmod = ((p << 16) | s) + 4
     * For VARCHAR(n) / CHAR(n): typmod = n + 4
     * For unconstrained types: -1
     */
    static int pgTypeMod(Column col) {
        if (col.getType() == null) return -1;
        Integer precision = col.getPrecision();
        Integer scale = col.getScale();
        switch (col.getType()) {
            case NUMERIC:
                if (precision != null) {
                    int s = (scale != null) ? scale : 0;
                    return ((precision << 16) | (s & 0xFFFF)) + 4;
                }
                return -1;
            case VARCHAR:
            case CHAR:
                if (precision != null) {
                    return precision + 4;
                }
                return -1;
            default:
                return -1;
        }
    }

    /** Write a RowDescription message to the ByteBuf. */
    static void sendRowDescription(ByteBuf buf, List<Column> columns) {
        sendRowDescription(buf, columns, null);
    }

    /**
     * Write a RowDescription message to the ByteBuf.
     *
     * @param session the session whose {@link Session#resolveOid} can resolve the real,
     *                per-type OID for custom enum columns (may be {@code null}, in which case
     *                enum columns fall back to the unresolvable placeholder OID 0 — pgjdbc's
     *                {@code TypeInfoCache} treats OID 0 as {@code Oid.UNSPECIFIED} and never
     *                even attempts to look it up in {@code pg_type}, which is what caused the
     *                {@code Misuse of castNonNull} crash in {@code PgResultSet.initSqlType}).
     */
    static void sendRowDescription(ByteBuf buf, List<Column> columns, Session session) {
        buf.writeByte('T');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0); // placeholder for length
        buf.writeShort(columns.size());
        for (Column col : columns) {
            writeCString(buf, col.getName());
            buf.writeInt(col.getTableOid());
            buf.writeShort(col.getAttNum());
            DataType colType = col.getType() != null ? col.getType() : DataType.TEXT;
            buf.writeInt(columnTypeOid(colType, col, session));
            buf.writeShort(pgTypeSize(colType));
            buf.writeInt(pgTypeMod(col));
            buf.writeShort(0); // format code (0 = text)
        }
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
    }

    /**
     * Resolves the wire OID to advertise for a column. Custom enum columns must advertise the
     * real, dynamically-allocated OID for their named type (the same OID the session's own
     * {@code pg_type}/{@code pg_attribute} catalog rows use, via {@code oid("type:" + name)}) —
     * not {@link DataType#ENUM}'s generic placeholder OID of 0, which pgjdbc cannot resolve.
     *
     * <p>An <em>array</em> of a custom enum ({@code col.getArrayElementType() == DataType.ENUM})
     * must advertise the array type's own distinct OID ({@code oid("type:" + name + "[]")}), not
     * the element's — reusing the element's OID for the array column made pgjdbc's
     * {@code TypeInfoCache.getArrayDelimiter}/{@code getPGArrayElement} pg_type lookups (which
     * join the advertised oid's row against its {@code typelem} row) find no rows, since the
     * element row's own {@code typelem} is 0 (it isn't an array). See
     * {@code CatalogCoreBuilder.buildPgType} for the matching synthesized pg_type array row.
     */
    static int columnTypeOid(DataType colType, Column col, Session session) {
        if (colType == DataType.ENUM && session != null && col.getEnumTypeName() != null) {
            // The column recorded which schema's enum it was declared with; the OID follows it.
            String key = session.typeOidKey(col.getEnumTypeName());
            if (col.getArrayElementType() == DataType.ENUM) {
                key = key + "[]";
            }
            return session.resolveOid(key);
        }
        // A composite is a type of its own, with a row in pg_type and an OID of its own: the
        // client resolves the column against that row, not against text.
        if (col != null && col.getCompositeTypeName() != null && session != null) {
            int composite = session.resolveOid(session.typeOidKey(col.getCompositeTypeName()));
            if (composite != 0) return composite;
        }
        // For array columns, advertise the array OID instead of scalar
        if (col != null && col.getArrayElementType() != null && col.getArrayElementType() != DataType.ENUM) {
            DataType arrayOid = scalarToArrayOid(col.getArrayElementType());
            if (arrayOid != null) return arrayOid.getOid();
        }
        return colType.getOid();
    }

    /** Map scalar DataType to its array DataType for OID resolution. */
    private static DataType scalarToArrayOid(DataType scalar) {
        if (scalar == null) return null;
        switch (scalar) {
            case BOOLEAN: return DataType.BOOL_ARRAY;
            case SMALLINT: return DataType.INT2_ARRAY;
            case INTEGER: case SERIAL: return DataType.INT4_ARRAY;
            case BIGINT: case BIGSERIAL: return DataType.INT8_ARRAY;
            case REAL: return DataType.FLOAT4_ARRAY;
            case DOUBLE_PRECISION: return DataType.FLOAT8_ARRAY;
            case NUMERIC: return DataType.NUMERIC_ARRAY;
            case TEXT: case NAME: return DataType.TEXT_ARRAY;
            case VARCHAR: return DataType.VARCHAR_ARRAY;
            case CHAR: return DataType.CHAR_ARRAY;
            case DATE: return DataType.DATE_ARRAY;
            case TIMESTAMP: return DataType.TIMESTAMP_ARRAY;
            case TIMESTAMPTZ: return DataType.TIMESTAMPTZ_ARRAY;
            case TIME: return DataType.TIME_ARRAY;
            case TIMETZ: return DataType.TIMETZ_ARRAY;
            case UUID: return DataType.UUID_ARRAY;
            case BYTEA: return DataType.BYTEA_ARRAY;
            case INTERVAL: return DataType.INTERVAL_ARRAY;
            case JSON: return DataType.JSON_ARRAY;
            case HSTORE: return DataType.HSTORE_ARRAY;
            case JSONB: return DataType.JSONB_ARRAY;
            case INET: return DataType.INET_ARRAY;
            default: return null;
        }
    }

    static void writeCString(ByteBuf buf, String s) {
        buf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        buf.writeByte(0);
    }
}
