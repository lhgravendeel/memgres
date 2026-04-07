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

    /** Format a value for text-mode PgWire transmission, respecting GUC settings. */
    static String formatValue(Object val, GucSettings guc) {
        if (val instanceof byte[]) {
            byte[] ba = (byte[]) val;
            StringBuilder sb = new StringBuilder("\\x");
            for (byte b : ba) sb.append(String.format("%02x", b & 0xFF));
            return sb.toString();
        }
        if (val instanceof Boolean) {
            Boolean b = (Boolean) val;
            return b ? "t" : "f";
        } else if (val instanceof LocalTime) {
            LocalTime t = (LocalTime) val;
            return t.getNano() != 0
                    ? t.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"))
                    : t.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        } else if (val instanceof LocalDate) {
            LocalDate ld = (LocalDate) val;
            String datestyle = guc != null ? guc.get("datestyle") : "ISO, MDY";
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return ld.format(DateTimeFormatter.ofPattern("dd.MM.yyyy"));
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return ld.format(DateTimeFormatter.ofPattern("MM/dd/yyyy"));
            }
            return ld.toString();
        } else if (val instanceof LocalDateTime) {
            LocalDateTime dt = (LocalDateTime) val;
            String datestyle = guc != null ? guc.get("datestyle") : "ISO, MDY";
            String timePart = dt.getNano() != 0
                    ? dt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"))
                    : dt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return dt.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")) + " " + timePart;
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return dt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy")) + " " + timePart;
            }
            return dt.getNano() != 0
                    ? dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
                    : dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
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
                    ? odt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"))
                    : odt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String offsetStr = formatPgOffset(odt.getOffset());
            if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                return odt.format(DateTimeFormatter.ofPattern("dd.MM.yyyy")) + " " + timePart + offsetStr;
            } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                return odt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy")) + " " + timePart + offsetStr;
            }
            String datePart = odt.getNano() != 0
                    ? odt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
                    : odt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            return datePart + formatPgOffset(odt.getOffset());
        } else if (val instanceof PgInterval) {
            PgInterval interval = (PgInterval) val;
            String intervalStyle = guc != null ? guc.get("intervalstyle") : "postgres";
            return interval.toString(intervalStyle);
        } else if (val instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) val;
            return bd.toPlainString();
        } else if (val instanceof Float) {
            Float f = (Float) val;
            if (f.isNaN()) return "NaN";
            if (f.isInfinite()) return f > 0 ? "Infinity" : "-Infinity";
            if (f == f.longValue()) return String.valueOf(f.longValue());
            return Float.toString(f);
        } else if (val instanceof Double) {
            Double d = (Double) val;
            if (d.isNaN()) return "NaN";
            if (d.isInfinite()) return d > 0 ? "Infinity" : "-Infinity";
            if (d == d.longValue()) return String.valueOf(d.longValue());
            return Double.toString(d);
        } else if (val instanceof com.memgres.engine.AstExecutor.PgEnum) {
            com.memgres.engine.AstExecutor.PgEnum enumVal = (com.memgres.engine.AstExecutor.PgEnum) val;
            return enumVal.label();
        } else if (val instanceof com.memgres.engine.AstExecutor.PgRow) {
            com.memgres.engine.AstExecutor.PgRow row = (com.memgres.engine.AstExecutor.PgRow) val;
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < row.values().size(); i++) {
                if (i > 0) sb.append(",");
                Object elem = row.values().get(i);
                if (elem == null) sb.append("");
                else if (elem instanceof Boolean) sb.append(((Boolean) elem) ? "t" : "f");
                else if (elem instanceof com.memgres.engine.AstExecutor.PgRow) {
                    String inner = formatValue(elem, guc);
                    sb.append("\"").append(inner.replace("\"", "\\\"")).append("\"");
                }
                else if (elem instanceof String && ((String) elem).contains(",")) {
                    String s = (String) elem;
                    sb.append("\"").append(s.replace("\"", "\\\"")).append("\"");
                }
                else sb.append(elem);
            }
            sb.append(")");
            return sb.toString();
        } else if (val instanceof com.memgres.engine.PgVector) {
            com.memgres.engine.PgVector vec = (com.memgres.engine.PgVector) val;
            return vec.toString();
        } else if (val instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) val;
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                Object elem = list.get(i);
                if (elem == null) {
                    sb.append("NULL");
                } else if (elem instanceof com.memgres.engine.AstExecutor.PgRow) {
                    String rowStr = formatValue(elem, guc);
                    sb.append("\"").append(rowStr.replace("\"", "\\\"")).append("\"");
                } else if (elem instanceof String) {
                    String s = (String) elem;
                    if (s.startsWith("(") || s.contains(",") || s.contains("{") || s.contains("}") || s.contains("\"") || s.contains(" ") || s.isEmpty()) {
                        sb.append("\"").append(s.replace("\"", "\\\"")).append("\"");
                    } else {
                        sb.append(s);
                    }
                } else {
                    sb.append(formatValue(elem, guc));
                }
            }
            sb.append("}");
            return sb.toString();
        } else {
            return val.toString();
        }
    }

    /** Format a timezone offset like PG: +00 when minutes==0, +05:30 otherwise. */
    static String formatPgOffset(ZoneOffset offset) {
        int totalSeconds = offset.getTotalSeconds();
        int hours = totalSeconds / 3600;
        int minutes = Math.abs((totalSeconds % 3600) / 60);
        if (minutes == 0) {
            return String.format("%+03d", hours);
        } else {
            return String.format("%+03d:%02d", hours, minutes);
        }
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
        buf.writeByte('T');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0); // placeholder for length
        buf.writeShort(columns.size());
        for (Column col : columns) {
            writeCString(buf, col.getName());
            buf.writeInt(col.getTableOid());
            buf.writeShort(col.getAttNum());
            buf.writeInt(col.getType().getOid());
            buf.writeShort(pgTypeSize(col.getType()));
            buf.writeInt(pgTypeMod(col));
            buf.writeShort(0); // format code (0 = text)
        }
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
    }

    static void writeCString(ByteBuf buf, String s) {
        buf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        buf.writeByte(0);
    }
}
