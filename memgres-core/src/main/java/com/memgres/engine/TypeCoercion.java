package com.memgres.engine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * PostgreSQL-compatible implicit type coercion.
 * Defines which types can be implicitly coerced and performs the conversions.
 */
public final class TypeCoercion {

    private TypeCoercion() {}

    // ---- Type categories (from PG pg_type.typcategory) ----

    public enum TypeCategory {
        NUMERIC,    // N: smallint, integer, bigint, real, double, numeric
        STRING,     // S: text, varchar, char
        BOOLEAN,    // B: boolean
        DATETIME,   // D: date, time, timestamp, timestamptz, interval
        NETWORK,    // I: inet, cidr, macaddr
        BINARY,     // U: bytea
        UUID,       // U: uuid
        JSON,       // U: json, jsonb
        UNKNOWN     // X: everything else
    }

    public static TypeCategory categoryOf(DataType type) {
        if (type == null) return TypeCategory.UNKNOWN;
        switch (type) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case REAL:
            case DOUBLE_PRECISION:
            case NUMERIC:
            case SERIAL:
            case BIGSERIAL:
            case SMALLSERIAL:
            case MONEY:
                return TypeCategory.NUMERIC;
            case VARCHAR:
            case CHAR:
            case TEXT:
            case NAME:
                return TypeCategory.STRING;
            case BOOLEAN:
                return TypeCategory.BOOLEAN;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case INTERVAL:
                return TypeCategory.DATETIME;
            case INET:
            case CIDR:
            case MACADDR:
                return TypeCategory.NETWORK;
            case BYTEA:
                return TypeCategory.BINARY;
            case UUID:
                return TypeCategory.UUID;
            case JSON:
            case JSONB:
                return TypeCategory.JSON;
            case TSVECTOR:
            case TSQUERY:
                return TypeCategory.UNKNOWN;
            case POINT:
            case LINE:
            case LSEG:
            case BOX:
            case PATH:
            case POLYGON:
            case CIRCLE:
                return TypeCategory.UNKNOWN;
            case BIT:
            case VARBIT:
                return TypeCategory.UNKNOWN;
            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case TSTZRANGE:
            case INT4MULTIRANGE:
                return TypeCategory.UNKNOWN;
            case XML:
                return TypeCategory.STRING;
            case TEXT_ARRAY:
            case INT4_ARRAY:
            case ACLITEM_ARRAY:
                return TypeCategory.UNKNOWN;
            case ENUM:
                return TypeCategory.UNKNOWN;
            default:
                throw new IllegalStateException("Unknown data type: " + type);
        }
    }

    // ---- Numeric type ordering for promotion ----

    private static int numericRank(DataType type) {
        switch (type) {
            case SMALLINT:
            case SMALLSERIAL:
                return 1;
            case INTEGER:
            case SERIAL:
                return 2;
            case BIGINT:
            case BIGSERIAL:
                return 3;
            case REAL:
                return 4;
            case DOUBLE_PRECISION:
                return 5;
            case NUMERIC:
            case MONEY:
                return 6;
            default:
                return 0;
        }
    }

    /**
     * Determine the common type for a binary operation between two numeric types.
     * Follows PG's type promotion rules.
     */
    public static DataType promoteNumeric(DataType a, DataType b) {
        if (a == DataType.NUMERIC || b == DataType.NUMERIC) return DataType.NUMERIC;
        if (a == DataType.DOUBLE_PRECISION || b == DataType.DOUBLE_PRECISION) return DataType.DOUBLE_PRECISION;
        if (a == DataType.REAL || b == DataType.REAL) return DataType.DOUBLE_PRECISION;
        if (a == DataType.BIGINT || a == DataType.BIGSERIAL || b == DataType.BIGINT || b == DataType.BIGSERIAL) return DataType.BIGINT;
        return DataType.INTEGER;
    }

    // ---- Implicit coercion check ----

    /**
     * Check if a value of type 'from' can be implicitly coerced to type 'to'.
     */
    public static boolean canImplicitCoerce(DataType from, DataType to) {
        if (from == to) return true;
        if (from == null || to == null) return true; // unknown types are always coercible

        TypeCategory fromCat = categoryOf(from);
        TypeCategory toCat = categoryOf(to);

        // String → anything (PG allows implicit text input)
        if (fromCat == TypeCategory.STRING) return true;

        // Within numeric: always coercible (with possible precision loss)
        if (fromCat == TypeCategory.NUMERIC && toCat == TypeCategory.NUMERIC) return true;

        // Numeric → String
        if (fromCat == TypeCategory.NUMERIC && toCat == TypeCategory.STRING) return true;

        // Boolean ↔ String
        if (fromCat == TypeCategory.BOOLEAN && toCat == TypeCategory.STRING) return true;

        // DateTime conversions
        if (fromCat == TypeCategory.DATETIME && toCat == TypeCategory.DATETIME) {
            return canCoerceDatetime(from, to);
        }
        if (fromCat == TypeCategory.DATETIME && toCat == TypeCategory.STRING) return true;

        return false;
    }

    private static boolean canCoerceDatetime(DataType from, DataType to) {
        // date → timestamp/timestamptz (adds midnight)
        if (from == DataType.DATE && (to == DataType.TIMESTAMP || to == DataType.TIMESTAMPTZ)) return true;
        // timestamp → timestamptz and vice versa
        if ((from == DataType.TIMESTAMP && to == DataType.TIMESTAMPTZ) ||
            (from == DataType.TIMESTAMPTZ && to == DataType.TIMESTAMP)) return true;
        // timestamp/timestamptz → date (truncates time)
        if ((from == DataType.TIMESTAMP || from == DataType.TIMESTAMPTZ) && to == DataType.DATE) return true;
        return from == to;
    }

    // ---- Actual coercion ----

    /**
     * Coerce a value to the target DataType. Returns the coerced value.
     * Throws MemgresException on invalid conversion.
     */
    public static Object coerce(Object value, DataType targetType) {
        if (value == null) return null;
        if (targetType == null) return value;

        switch (targetType) {
            case SMALLINT:
            case SMALLSERIAL:
                return toShort(value);
            case INTEGER:
            case SERIAL:
                return toInteger(value);
            case BIGINT:
            case BIGSERIAL:
                return toLong(value);
            case REAL:
                return toFloat(value);
            case DOUBLE_PRECISION:
                return toDouble(value);
            case NUMERIC: {
                // PG numeric supports NaN, represented as Double.NaN
                if (value instanceof String && ((String) value).trim().equalsIgnoreCase("NaN")) {
                    String s = (String) value;
                    return Double.NaN;
                }
                return toBigDecimal(value);
            }
            case MONEY:
                return toMoney(value);
            case VARCHAR:
            case CHAR:
            case TEXT:
                return toString(value);
            case BOOLEAN:
                return toBoolean(value);
            case DATE:
                return toLocalDate(value);
            case TIME:
                return toLocalTime(value);
            case TIMESTAMP:
                return toLocalDateTime(value);
            case TIMESTAMPTZ:
                return toOffsetDateTime(value);
            case INTERVAL:
                return toInterval(value);
            case UUID:
                return toUUID(value);
            case BYTEA:
                return toBytea(value);
            case JSONB:
                return normalizeJsonb(value.toString());
            case JSON:
                return value.toString();
            case INET:
            case CIDR:
                return NetworkOperations.normalizeAddress(value.toString());
            default:
                return value;
        }
    }

    /**
     * Coerce a value for storage into a column. Less strict than explicit cast.
     */
    public static Object coerceForStorage(Object value, Column column) {
        if (value == null) return null;

        DataType type = column.getType();
        if (type == DataType.SERIAL || type == DataType.BIGSERIAL || type == DataType.SMALLSERIAL) return value; // handled separately

        // If value is already the right Java type, no conversion needed
        if (isCorrectJavaType(value, type)) {
            return applyPrecision(value, type, column);
        }

        // Arrays (List) should be stored as PG array format strings, not Java List.toString()
        if (value instanceof java.util.List<?>) {
            return formatPgArray((java.util.List<?>) value);
        }

        // Composite values (PgRow) should be stored in PG parenthesized text format
        if (value instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) value;
            return toString(row);
        }

        try {
            Object coerced = coerce(value, type);
            return applyPrecision(coerced, type, column);
        } catch (MemgresException e) {
            // If the value looks like an array literal or is a List, store as-is
            if (value instanceof java.util.List || (value instanceof String && ((String) value).trim().startsWith("{"))) {
                String s = (String) value;
                return value;
            }
            throw e;
        } catch (Exception e) {
            // If the value looks like an array literal or is a List, store as-is
            if (value instanceof java.util.List || (value instanceof String && ((String) value).trim().startsWith("{"))) {
                String s = (String) value;
                return value;
            }
            throw new MemgresException(
                    "invalid input syntax for type " + type.getPgName() + ": \"" + value + "\"", "22P02");
        }
    }

    private static boolean isCorrectJavaType(Object value, DataType type) {
        switch (type) {
            case SMALLINT:
            case SMALLSERIAL:
                return value instanceof Short;
            case INTEGER:
            case SERIAL:
                return value instanceof Integer;
            case BIGINT:
            case BIGSERIAL:
                return value instanceof Long;
            case REAL:
                return value instanceof Float;
            case DOUBLE_PRECISION:
                return value instanceof Double;
            case NUMERIC:
                return value instanceof BigDecimal;
            case MONEY:
                return value instanceof BigDecimal;
            case BOOLEAN:
                return value instanceof Boolean;
            case DATE:
                return value instanceof LocalDate;
            case TIME:
                return value instanceof LocalTime;
            case TIMESTAMP:
                return value instanceof LocalDateTime;
            case TIMESTAMPTZ:
                return value instanceof OffsetDateTime;
            case INTERVAL:
                return value instanceof PgInterval;
            case UUID:
                return value instanceof java.util.UUID;
            case BYTEA:
                return value instanceof byte[];
            case JSONB:
                return false;
            case INET:
            case CIDR:
                return false;
            default:
                return value instanceof String;
        }
    }

    private static Object applyPrecision(Object value, DataType type, Column column) {
        // VARCHAR(n) enforcement
        if (type == DataType.VARCHAR && column.getPrecision() != null && value instanceof String) {
            String s = (String) value;
            if (s.length() > column.getPrecision()) {
                throw new MemgresException("value too long for type character varying(" + column.getPrecision() + ")", "22001");
            }
        }
        // CHAR(n) padding
        if (type == DataType.CHAR && column.getPrecision() != null && value instanceof String) {
            String s = (String) value;
            int n = column.getPrecision();
            if (s.length() > n) {
                throw new MemgresException("value too long for type character(" + n + ")", "22001");
            }
            // PG pads CHAR with spaces
            return String.format("%-" + n + "s", s);
        }
        // NUMERIC(p,s)
        if (type == DataType.NUMERIC && column.getScale() != null && value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;
            return bd.setScale(column.getScale(), RoundingMode.HALF_UP);
        }
        return value;
    }

    // ---- Conversion helpers ----

    private static Short toShort(Object val) {
        if (val instanceof Number) {
            Number n = (Number) val;
            long lv = n.longValue();
            if (lv < Short.MIN_VALUE || lv > Short.MAX_VALUE) {
                throw new MemgresException("smallint out of range", "22003");
            }
            return (short) lv;
        }
        if (val instanceof Boolean) return (short) (((Boolean) val) ? 1 : 0);
        String s = val.toString().trim();
        try {
            return Short.parseShort(s);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type smallint: \"" + val + "\"", "22P02");
        }
    }

    public static Integer toInteger(Object val) {
        if (val instanceof RegclassValue) return ((RegclassValue) val).oid();
        if (val instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) val;
            long lv = bd.setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw new MemgresException("integer out of range", "22003");
            return (int) lv;
        }
        if (val instanceof Number) {
            Number n = (Number) val;
            long lv = n.longValue();
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw new MemgresException("integer out of range", "22003");
            return (int) lv;
        }
        if (val instanceof Boolean) return ((Boolean) val) ? 1 : 0;
        String s = val.toString().trim();
        if (s.isEmpty()) throw new MemgresException("invalid input syntax for type integer: \"\"", "22P02");
        try {
            if (s.contains(".")) {
                long lv = Math.round(Double.parseDouble(s));
                if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                    throw new MemgresException("integer out of range", "22003");
                return (int) lv;
            }
            long lv = Long.parseLong(s);
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw new MemgresException("integer out of range", "22003");
            return (int) lv;
        } catch (NumberFormatException e) {
            if (s.matches("[+-]?\\d+")) {
                throw new MemgresException("integer out of range", "22003");
            }
            throw new MemgresException("invalid input syntax for type integer: \"" + val + "\"", "22P02");
        }
    }

    public static Long toLong(Object val) {
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).setScale(0, java.math.RoundingMode.HALF_UP).longValue();
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof Boolean) return ((Boolean) val) ? 1L : 0L;
        String s = val.toString().trim();
        if (s.isEmpty()) throw new MemgresException("invalid input syntax for type bigint: \"\"", "22P02");
        try {
            if (s.contains(".")) return (long) Double.parseDouble(s);
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            // Distinguish out-of-range from bad syntax: if the string looks numeric but overflows, it's 22003
            if (s.matches("[+-]?\\d+")) {
                throw new MemgresException("bigint out of range", "22003");
            }
            throw new MemgresException("invalid input syntax for type bigint: \"" + val + "\"", "22P02");
        }
    }

    static Float toFloat(Object val) {
        if (val instanceof Number) return ((Number) val).floatValue();
        String s = val.toString().trim();
        switch (s) {
            case "Infinity":
                return Float.POSITIVE_INFINITY;
            case "-Infinity":
                return Float.NEGATIVE_INFINITY;
            case "NaN":
                return Float.NaN;
            default:
                return Float.parseFloat(s);
        }
    }

    public static Double toDouble(Object val) {
        if (val instanceof Number) return ((Number) val).doubleValue();
        String s = val.toString().trim();
        switch (s) {
            case "Infinity":
                return Double.POSITIVE_INFINITY;
            case "-Infinity":
                return Double.NEGATIVE_INFINITY;
            case "NaN":
                return Double.NaN;
            default:
                return Double.parseDouble(s);
        }
    }

    public static BigDecimal toMoney(Object val) {
        if (val instanceof BigDecimal) return ((BigDecimal) val).setScale(2, RoundingMode.HALF_UP);
        if (val instanceof Number) return BigDecimal.valueOf(((Number) val).doubleValue()).setScale(2, RoundingMode.HALF_UP);
        String s = val.toString().trim();
        // Strip '$' and ',' from money strings
        s = s.replace("$", "").replace(",", "");
        return new BigDecimal(s).setScale(2, RoundingMode.HALF_UP);
    }

    public static BigDecimal toBigDecimal(Object val) {
        if (val instanceof BigDecimal) return ((BigDecimal) val);
        if (val instanceof Integer) return BigDecimal.valueOf(((Integer) val));
        if (val instanceof Long) return BigDecimal.valueOf(((Long) val));
        if (val instanceof Double) return BigDecimal.valueOf(((Double) val));
        if (val instanceof Float) return BigDecimal.valueOf(((Float) val));
        if (val instanceof Number) return BigDecimal.valueOf(((Number) val).doubleValue());
        String s = val.toString().trim();
        if (s.isEmpty()) throw new MemgresException("invalid input syntax for type numeric: \"\"", "22P02");
        try {
            return new BigDecimal(s);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type numeric: \"" + val + "\"", "22P02");
        }
    }

    private static String toString(Object val) {
        if (val instanceof LocalDate) return ((LocalDate) val).toString();
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).toString();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toString();
        if (val instanceof LocalTime) return ((LocalTime) val).toString();
        if (val instanceof PgInterval) return ((PgInterval) val).toString();
        if (val instanceof java.util.List<?>) return formatPgArray((java.util.List<?>) val);
        if (val instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) val).label();
        if (val instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) val;
            // PG format: (val1,val2,...) with nested composites quoted
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < row.values().size(); i++) {
                if (i > 0) sb.append(",");
                Object v = row.values().get(i);
                if (v == null) sb.append("");
                else if (v instanceof AstExecutor.PgRow) {
                    AstExecutor.PgRow nested = (AstExecutor.PgRow) v;
                    String inner = toString(nested);
                    sb.append("\"").append(inner.replace("\"", "\\\"")).append("\"");
                } else sb.append(v);
            }
            sb.append(")");
            return sb.toString();
        }
        return val.toString();
    }

    /** Normalize JSONB: canonical form with sorted keys, deduplication (last wins), space after : and , (PG behavior). */
    public static String normalizeJsonb(String json) {
        if (json == null) return null;
        json = json.trim();
        if (json.startsWith("{")) {
            // Parse into map (LinkedHashMap preserves insertion order, last put wins = dedup)
            Map<String, String> map = JsonOperations.parseObjectKeys(json);
            // Sort keys alphabetically and normalize values recursively
            TreeMap<String, String> sorted = new TreeMap<>();
            for (Map.Entry<String, String> e : map.entrySet()) {
                sorted.put(e.getKey(), normalizeJsonb(e.getValue()));
            }
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> e : sorted.entrySet()) {
                if (!first) sb.append(", ");
                sb.append("\"").append(e.getKey()).append("\": ").append(e.getValue());
                first = false;
            }
            sb.append("}");
            return sb.toString();
        }
        if (json.startsWith("[")) {
            List<String> elems = JsonOperations.parseArrayElements(json);
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < elems.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(normalizeJsonb(elems.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
        // Scalar values: return as-is (numbers, strings, booleans, null)
        return json;
    }

    /** Format a Java List as a PG array string: {elem1,elem2,...} */
    public static String formatPgArray(java.util.List<?> list) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            Object elem = list.get(i);
            if (elem == null) {
                sb.append("NULL");
            } else if (elem instanceof java.util.List<?>) {
                java.util.List<?> sub = (java.util.List<?>) elem;
                sb.append(formatPgArray(sub));
            } else if (elem instanceof AstExecutor.PgRow) {
                AstExecutor.PgRow row = (AstExecutor.PgRow) elem;
                String rowStr = toString(row);
                sb.append("\"").append(rowStr.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
            } else if (elem instanceof String) {
                String s = (String) elem;
                if (s.startsWith("(") || s.contains(",") || s.contains("{") || s.contains("}") || s.contains("\"") || s.contains(" ") || s.isEmpty()) {
                    sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                } else {
                    sb.append(s);
                }
            } else {
                sb.append(elem);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    public static Boolean toBoolean(Object val) {
        if (val instanceof Boolean) return ((Boolean) val);
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        String s = val.toString().trim().toLowerCase();
        switch (s) {
            case "true":
            case "t":
            case "yes":
            case "y":
            case "on":
            case "1":
                return true;
            case "false":
            case "f":
            case "no":
            case "n":
            case "off":
            case "0":
                return false;
            default:
                throw new MemgresException("invalid input syntax for type boolean: \"" + val + "\"", "22P02");
        }
    }

    // ---- Date/Time conversions ----

    private static final DateTimeFormatter[] DATE_FORMATS = {
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
    };

    public static Object toLocalDateOrBc(Object val) {
        if (val instanceof String) {
            String s = (String) val;
            String trimmed = s.trim();
            if (trimmed.toUpperCase().endsWith(" BC")) {
                // Parse the date part and return with BC suffix
                String datePart = trimmed.substring(0, trimmed.length() - 3).trim();
                LocalDate d = toLocalDate(datePart);
                return d.toString() + " BC";
            }
        }
        return toLocalDate(val);
    }

    public static LocalDate toLocalDate(Object val) {
        if (val instanceof LocalDate) return ((LocalDate) val);
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).toLocalDate();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toLocalDate();
        String s = val.toString().trim();
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try { return LocalDate.parse(s, fmt); } catch (DateTimeParseException e) { /* try next */ }
        }
        // Try parsing as timestamp then extracting date
        try { return LocalDateTime.parse(s).toLocalDate(); } catch (Exception e) { /* ignore */ }
        try { return OffsetDateTime.parse(s).toLocalDate(); } catch (Exception e) { /* ignore */ }
        // Strip trailing timezone offset (e.g. "2024-06-15 +02" from JDBC driver) because PG ignores TZ for date type
        if (s.length() > 10 && s.matches("\\d{4}-\\d{2}-\\d{2}[\\s+].*")) {
            String datePart = s.substring(0, 10);
            for (DateTimeFormatter fmt : DATE_FORMATS) {
                try { return LocalDate.parse(datePart, fmt); } catch (DateTimeParseException e) { /* try next */ }
            }
        }
        // Use 22008 (datetime_field_overflow) for well-formatted but out-of-range dates (e.g. 2023-02-29)
        String errCode = s.matches("\\d{4}-\\d{2}-\\d{2}.*") ? "22008" : "22007";
        throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
    }

    public static LocalTime toLocalTime(Object val) {
        if (val instanceof LocalTime) return ((LocalTime) val);
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).toLocalTime();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toLocalTime();
        String s = val.toString().trim();
        try { return LocalTime.parse(s); } catch (DateTimeParseException e) {
            // Try parsing as time with timezone offset (e.g., "10:30:00+02")
            try {
                return java.time.OffsetTime.parse(s, java.time.format.DateTimeFormatter.ISO_OFFSET_TIME).toLocalTime();
            } catch (DateTimeParseException e2) { /* try more */ }
            // Handle UTC+N / UTC-N format (PG wraps extreme offsets)
            java.util.regex.Matcher utcMatch = java.util.regex.Pattern
                    .compile("^(\\d{1,2}:\\d{2}(?::\\d{2})?)\\s+UTC([+-]\\d+)$", java.util.regex.Pattern.CASE_INSENSITIVE)
                    .matcher(s);
            if (utcMatch.matches()) {
                String timePart = utcMatch.group(1);
                int offsetHours = Integer.parseInt(utcMatch.group(2));
                try {
                    LocalTime base = LocalTime.parse(timePart);
                    return base.minusHours(offsetHours);
                } catch (DateTimeParseException e3) { /* fall through */ }
            }
            // Handle simple offset formats: HH:MM:SS+HH or HH:MM:SS+HHMM
            if (s.contains("+") || (s.lastIndexOf('-') > s.indexOf(':'))) {
                String timePart = s;
                int plusIdx = s.lastIndexOf('+');
                int minusIdx = s.lastIndexOf('-');
                int tzIdx = Math.max(plusIdx, minusIdx);
                if (tzIdx > 0) {
                    timePart = s.substring(0, tzIdx);
                    try { return LocalTime.parse(timePart); } catch (DateTimeParseException e3) { /* fall through */ }
                }
            }
            // Use 22008 for well-formatted but out-of-range times (e.g. 25:00:00)
            String errCode = s.matches("\\d{1,2}:\\d{2}(:\\d{2})?.*") ? "22008" : "22007";
            throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
        }
    }

    /**
     * Parse a TIMETZ literal, preserving the raw time and offset.
     * PG convention: UTC+N displays as -N in the offset portion (sign flip).
     * Returns a formatted string like "HH:MM:SS±offset".
     */
    public static String toTimeTz(Object val) {
        if (val instanceof String) {
            // already formatted timetz string; pass through
        }
        String s = val.toString().trim();

        // Handle UTC+N / UTC-N format: preserve time as-is, flip the sign for display
        java.util.regex.Matcher utcMatch = java.util.regex.Pattern
                .compile("^(\\d{1,2}:\\d{2}(?::\\d{2}(?:\\.\\d+)?)?)\\s+UTC([+-])(\\d+)$", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(s);
        if (utcMatch.matches()) {
            String timePart = utcMatch.group(1);
            String sign = utcMatch.group(2);
            String offsetVal = utcMatch.group(3);
            // Validate the time part parses
            try {
                LocalTime lt = LocalTime.parse(timePart);
                timePart = lt.toString();
                // Ensure seconds are always present (HH:MM:SS)
                if (timePart.length() == 5) timePart += ":00";
            } catch (DateTimeParseException e) {
                String errCode = timePart.matches("\\d{1,2}:\\d{2}(:\\d{2})?.*") ? "22008" : "22007";
                throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
            }
            // Flip sign: UTC+N → display as -N, UTC-N → display as +N
            String displaySign = sign.equals("+") ? "-" : "+";
            // Format offset: strip leading zeros but keep at least two digits
            int offsetNum = Integer.parseInt(offsetVal);
            String formattedOffset = String.format("%02d", offsetNum);
            return timePart + displaySign + formattedOffset;
        }

        // Handle time with explicit offset (e.g., "10:30:00+02", "10:30:00-05:30")
        // Try parsing with Java's OffsetTime for standard offsets (±18 hours)
        try {
            java.time.OffsetTime ot = java.time.OffsetTime.parse(s, java.time.format.DateTimeFormatter.ISO_OFFSET_TIME);
            String timePart = ot.toLocalTime().toString();
            if (timePart.length() == 5) timePart += ":00";
            int totalSeconds = ot.getOffset().getTotalSeconds();
            String sign = totalSeconds >= 0 ? "+" : "-";
            int absSeconds = Math.abs(totalSeconds);
            int hours = absSeconds / 3600;
            int minutes = (absSeconds % 3600) / 60;
            String offsetStr = minutes > 0 ? String.format("%02d:%02d", hours, minutes) : String.format("%02d", hours);
            return timePart + sign + offsetStr;
        } catch (DateTimeParseException e) { /* try more */ }

        // Handle simple offset formats: HH:MM:SS+HH or HH:MM:SS-HH
        if (s.contains("+") || (s.lastIndexOf('-') > s.indexOf(':'))) {
            int plusIdx = s.lastIndexOf('+');
            int minusIdx = s.lastIndexOf('-');
            int tzIdx = Math.max(plusIdx, minusIdx);
            if (tzIdx > 0) {
                String timePart = s.substring(0, tzIdx);
                String offsetPart = s.substring(tzIdx); // includes sign
                try {
                    LocalTime lt = LocalTime.parse(timePart);
                    timePart = lt.toString();
                    if (timePart.length() == 5) timePart += ":00";
                    return timePart + offsetPart;
                } catch (DateTimeParseException e3) { /* fall through */ }
            }
        }

        // Plain time without offset, default to +00 (UTC)
        try {
            LocalTime lt = LocalTime.parse(s);
            String timePart = lt.toString();
            if (timePart.length() == 5) timePart += ":00";
            return timePart + "+00";
        } catch (DateTimeParseException e) { /* fall through */ }

        String errCode = s.matches("\\d{1,2}:\\d{2}(:\\d{2})?.*") ? "22008" : "22007";
        throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
    }

    // Sentinel values for PG 'infinity' / '-infinity' timestamps
    public static final LocalDateTime TIMESTAMP_INFINITY = LocalDateTime.of(9999, 12, 31, 23, 59, 59);
    public static final LocalDateTime TIMESTAMP_NEG_INFINITY = LocalDateTime.of(-4713, 1, 1, 0, 0, 0);

    public static Object toLocalDateTimeOrInfinity(Object val) {
        if (val instanceof String) {
            String s = (String) val;
            String trimmed = s.trim();
            if (trimmed.equalsIgnoreCase("infinity")) return "infinity";
            if (trimmed.equalsIgnoreCase("-infinity")) return "-infinity";
        }
        return toLocalDateTime(val);
    }

    public static LocalDateTime toLocalDateTime(Object val) {
        if (val instanceof LocalDateTime) return ((LocalDateTime) val);
        if (val instanceof LocalDate) return ((LocalDate) val).atStartOfDay();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toLocalDateTime();
        String s = val.toString().trim();
        // Handle PG 'infinity' / '-infinity' special values
        if (s.equalsIgnoreCase("infinity")) return TIMESTAMP_INFINITY;
        if (s.equalsIgnoreCase("-infinity")) return TIMESTAMP_NEG_INFINITY;
        // Handle "YYYY-MM-DD HH:MM:SS" (space instead of T)
        s = s.replace(' ', 'T');
        try { return LocalDateTime.parse(s); } catch (DateTimeParseException e) { /* try more */ }
        // Try date-only
        try { return LocalDate.parse(s).atStartOfDay(); } catch (DateTimeParseException e) { /* try more */ }
        try { return OffsetDateTime.parse(s).toLocalDateTime(); } catch (Exception e) { /* ignore */ }
        // Use 22008 for well-formatted but out-of-range timestamps
        String errCode = val.toString().trim().matches("\\d{4}-\\d{2}-\\d{2}.*") ? "22008" : "22007";
        throw new MemgresException("invalid input syntax for type timestamp: \"" + val + "\"", errCode);
    }

    public static OffsetDateTime toOffsetDateTime(Object val) {
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val);
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).atZone(ZoneId.systemDefault()).toOffsetDateTime();
        if (val instanceof LocalDate) return ((LocalDate) val).atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
        String s = val.toString().trim();
        // Try named timezone: "2024-01-01 13:00:00 Europe/Amsterdam"
        java.util.regex.Matcher tzMatcher = java.util.regex.Pattern
                .compile("^(\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2})\\s+([A-Za-z][A-Za-z0-9_/+-]+)$")
                .matcher(s);
        if (tzMatcher.matches()) {
            try {
                String dtPart = tzMatcher.group(1).replace(' ', 'T');
                String tzName = tzMatcher.group(2);
                java.time.ZoneId zone = java.time.ZoneId.of(tzName);
                return LocalDateTime.parse(dtPart).atZone(zone).toOffsetDateTime();
            } catch (Exception ignore) { /* fall through */ }
        }
        s = s.replace(' ', 'T');
        try { return OffsetDateTime.parse(s); } catch (DateTimeParseException e) { /* try more */ }
        try { return LocalDateTime.parse(s).atZone(ZoneId.systemDefault()).toOffsetDateTime(); } catch (DateTimeParseException e) { /* try more */ }
        try { return LocalDate.parse(s).atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime(); } catch (DateTimeParseException e) { /* ignore */ }
        // Use 22008 for well-formatted but out-of-range dates (e.g., 2024-02-30)
        String errCode = val.toString().trim().matches("\\d{4}-\\d{2}-\\d{2}.*") ? "22008" : "22007";
        throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
    }

    public static PgInterval toInterval(Object val) {
        if (val instanceof PgInterval) return ((PgInterval) val);
        return PgInterval.parse(val.toString());
    }

    public static byte[] toBytea(Object val) {
        if (val instanceof byte[]) return (byte[]) val;
        if (val instanceof String) {
            String s = (String) val;
            // PG hex format: \xDEADBEEF or \\xDEADBEEF
            String hex = s;
            if (hex.startsWith("\\x") || hex.startsWith("\\\\x")) {
                hex = hex.startsWith("\\\\x") ? hex.substring(3) : hex.substring(2);
                byte[] result = new byte[hex.length() / 2];
                for (int i = 0; i < result.length; i++) {
                    result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
                }
                return result;
            }
            // Already a plain string, convert to bytes
            return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        return null;
    }

    private static Object toUUID(Object val) {
        if (val instanceof java.util.UUID) return val;
        if (val instanceof String) {
            String s = (String) val;
            try { return java.util.UUID.fromString(s); } catch (IllegalArgumentException e) {
                throw new MemgresException("invalid input syntax for type uuid: \"" + val + "\"", "22P02");
            }
        }
        return val;
    }

    private static boolean isUUID(String s) {
        try { java.util.UUID.fromString(s); return true; } catch (Exception e) { return false; }
    }

    // ---- Comparison helpers ----

    /**
     * Compare two values with proper type-aware comparison.
     * Handles cross-type numeric comparisons and date/time comparisons.
     */
    @SuppressWarnings("unchecked")
    public static int compare(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        // PgEnum: compare by ordinal when both are PgEnum, fall through to string otherwise
        if (a instanceof AstExecutor.PgEnum && b instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) a).compareTo(((AstExecutor.PgEnum) b));

        // Both numbers: promote and compare
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof BigDecimal || b instanceof BigDecimal) {
                return toBigDecimal(a).compareTo(toBigDecimal(b));
            }
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }

        // Date/time comparisons
        if (a instanceof LocalDate && b instanceof LocalDate) return ((LocalDate) a).compareTo((LocalDate) b);
        if (a instanceof LocalDateTime && b instanceof LocalDateTime) return ((LocalDateTime) a).compareTo((LocalDateTime) b);
        if (a instanceof OffsetDateTime && b instanceof OffsetDateTime) return ((OffsetDateTime) a).toInstant().compareTo(((OffsetDateTime) b).toInstant());
        if (a instanceof LocalTime && b instanceof LocalTime) return ((LocalTime) a).compareTo((LocalTime) b);
        if (a instanceof PgInterval && b instanceof PgInterval) return ((PgInterval) a).compareTo((PgInterval) b);

        // UUID comparisons
        if (a instanceof java.util.UUID && b instanceof java.util.UUID) return ((java.util.UUID) a).compareTo(((java.util.UUID) b));
        if (a instanceof java.util.UUID && b instanceof String) {
            String sb = (String) b;
            java.util.UUID ua = (java.util.UUID) a;
            try { return ua.compareTo(java.util.UUID.fromString(sb)); } catch (Exception e) { /* fall through */ }
        }
        if (b instanceof java.util.UUID && a instanceof String) {
            String sa = (String) a;
            java.util.UUID ub = (java.util.UUID) b;
            try { return java.util.UUID.fromString(sa).compareTo(ub); } catch (Exception e) { /* fall through */ }
        }

        // Byte array comparison
        if (a instanceof byte[] && b instanceof byte[]) {
            byte[] ba = (byte[]) a;
            byte[] bb = (byte[]) b;
            return compareBytes(ba, bb);
        }
        // Byte array vs string: coerce string to byte array
        if (a instanceof byte[] && b instanceof String) {
            byte[] ba = (byte[]) a;
            String sb = (String) b;
            Object coerced = toBytea(sb);
            if (coerced instanceof byte[]) return compareBytes(ba, (byte[]) coerced);
        }
        if (b instanceof byte[] && a instanceof String) {
            byte[] bb = (byte[]) b;
            String sa = (String) a;
            Object coerced = toBytea(sa);
            if (coerced instanceof byte[]) return compareBytes((byte[]) coerced, bb);
        }

        // Cross-type date/time: coerce to common type
        if (isDateTime(a) && isDateTime(b)) {
            return toLocalDateTime(a).compareTo(toLocalDateTime(b));
        }

        // Number vs string: try numeric comparison
        if (a instanceof Number || b instanceof Number) {
            try {
                return Double.compare(toDouble(a), toDouble(b));
            } catch (Exception e) {
                // fall through
            }
        }

        // String comparison: use PostgreSQL-like collation ordering.
        // Alphanumeric characters use standard codepoint ordering (C locale),
        // while non-alphanumeric characters (punctuation, symbols) sort after
        // all alphanumeric characters (as in en_US.UTF-8 locale).
        return pgStringCompare(a.toString(), b.toString());
    }

    /**
     * PostgreSQL-like locale-aware string comparison.
     * Emulates glibc strcoll() behavior for en_US.UTF-8 where:
     * - Letters are compared case-insensitively at primary level
     * - Digits sort after letters at primary level
     * - Punctuation/symbols sort after digits
     * - Case is used as a secondary tiebreaker (uppercase before lowercase)
     * - Original codepoint is the final tiebreaker
     */
    static int pgStringCompare(String a, String b) {
        int len = Math.min(a.length(), b.length());
        for (int i = 0; i < len; i++) {
            char ca = a.charAt(i);
            char cb = b.charAt(i);
            if (ca == cb) continue;
            int wa = pgCharWeight(ca);
            int wb = pgCharWeight(cb);
            if (wa != wb) return Integer.compare(wa, wb);
        }
        return Integer.compare(a.length(), b.length());
    }

    /**
     * Compute a collation weight for a character that emulates PostgreSQL's
     * default locale collation ordering: letters (case-insensitive) < digits < symbols/punctuation.
     */
    private static int pgCharWeight(char c) {
        if (Character.isLetterOrDigit(c)) {
            // Alphanumeric characters: use standard codepoint ordering (C locale).
            // This preserves uppercase-before-lowercase and digit-before-letter ordering.
            return c;
        }
        // Non-alphanumeric (punctuation, symbols, whitespace): sort after all alphanumeric characters.
        // This matches PostgreSQL's en_US.UTF-8 collation where symbols sort after letters/digits.
        return 0x10000 + c;
    }

    /**
     * Type-aware equality check.
     */
    public static boolean areEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a.equals(b) || b.equals(a)) return true;
        // PgEnum comparison: compare by label string
        if (a instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) a).label().equals(b instanceof AstExecutor.PgEnum ? ((AstExecutor.PgEnum) b).label() : b.toString());
        if (b instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) b).label().equals(a.toString());

        // Number comparison
        if (a instanceof Number && b instanceof Number) {
            // Handle NaN/Infinity: these can't be converted to BigDecimal
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            if (Double.isNaN(da) || Double.isNaN(db) || Double.isInfinite(da) || Double.isInfinite(db)) {
                // NaN == NaN is true in PG for storage/comparison purposes
                if (Double.isNaN(da) && Double.isNaN(db)) return true;
                return da == db;
            }
            if (a instanceof BigDecimal || b instanceof BigDecimal) {
                return toBigDecimal(a).compareTo(toBigDecimal(b)) == 0;
            }
            return da == db;
        }

        // Byte array comparison
        if (a instanceof byte[] && b instanceof byte[]) {
            byte[] ba = (byte[]) a;
            byte[] bb = (byte[]) b;
            return java.util.Arrays.equals(ba, bb);
        }
        // Byte array vs hex string: coerce string to byte array and compare
        if (a instanceof byte[] && b instanceof String) {
            byte[] ba = (byte[]) a;
            String sb = (String) b;
            Object coerced = toBytea(sb);
            if (coerced instanceof byte[]) return java.util.Arrays.equals(ba, (byte[]) coerced);
        }
        if (b instanceof byte[] && a instanceof String) {
            byte[] bb = (byte[]) b;
            String sa = (String) a;
            Object coerced = toBytea(sa);
            if (coerced instanceof byte[]) return java.util.Arrays.equals((byte[]) coerced, bb);
        }

        // Date/time equality
        if (isDateTime(a) && isDateTime(b)) {
            try { return compare(a, b) == 0; } catch (Exception e) { /* fall through */ }
        }

        // UUID vs String: parameters often arrive as text strings for UUID columns
        if (a instanceof java.util.UUID && b instanceof String) {
            String sb = (String) b;
            java.util.UUID au = (java.util.UUID) a;
            try { return au.equals(java.util.UUID.fromString(sb)); } catch (Exception e) { /* fall through */ }
        }
        if (b instanceof java.util.UUID && a instanceof String) {
            String sa = (String) a;
            java.util.UUID bu = (java.util.UUID) b;
            try { return bu.equals(java.util.UUID.fromString(sa)); } catch (Exception e) { /* fall through */ }
        }

        // String vs Boolean: "t"/"f"/"true"/"false" from extended query protocol
        if (a instanceof Boolean && b instanceof String) {
            String sb = (String) b;
            Boolean ab = (Boolean) a;
            return ab.equals(toBoolean(sb));
        }
        if (b instanceof Boolean && a instanceof String) {
            String sa = (String) a;
            Boolean bb = (Boolean) b;
            return bb.equals(toBoolean(sa));
        }

        // String vs Number: parameter values arrive as text strings
        if (a instanceof Number && b instanceof String) {
            String sb = (String) b;
            try { return areEqual(a, toBigDecimal(sb)); } catch (Exception e) { /* fall through */ }
        }
        if (b instanceof Number && a instanceof String) {
            String sa = (String) a;
            try { return areEqual(toBigDecimal(sa), b); } catch (Exception e) { /* fall through */ }
        }

        // String vs DateTime: timestamp parameters arrive as text strings
        if (isDateTime(a) && b instanceof String) {
            String sb = (String) b;
            try {
                if (a instanceof LocalDateTime) return a.equals(toLocalDateTime(sb));
                if (a instanceof LocalDate) return a.equals(toLocalDate(sb));
                if (a instanceof OffsetDateTime) return ((OffsetDateTime) a).toInstant().equals(toOffsetDateTime(sb).toInstant());
                if (a instanceof LocalTime) return a.equals(toLocalTime(sb));
            } catch (Exception e) { /* fall through */ }
        }
        if (isDateTime(b) && a instanceof String) {
            String sa = (String) a;
            try {
                if (b instanceof LocalDateTime) return b.equals(toLocalDateTime(sa));
                if (b instanceof LocalDate) return b.equals(toLocalDate(sa));
                if (b instanceof OffsetDateTime) return ((OffsetDateTime) b).toInstant().equals(toOffsetDateTime(sa).toInstant());
                if (b instanceof LocalTime) return b.equals(toLocalTime(sa));
            } catch (Exception e) { /* fall through */ }
        }

        // Fall back to string comparison, using trailing-space-insensitive comparison
        // to handle CHAR(n) values which PG pads with spaces but compares ignoring trailing spaces
        String sa2 = a.toString();
        String sb2 = b.toString();
        if (sa2.equals(sb2)) return true;
        if (sa2.length() != sb2.length()) {
            // Pad shorter to longer length with spaces (CHAR semantics)
            int maxLen = Math.max(sa2.length(), sb2.length());
            return String.format("%-" + maxLen + "s", sa2).equals(String.format("%-" + maxLen + "s", sb2));
        }
        return false;
    }

    private static boolean isDateTime(Object val) {
        return val instanceof LocalDate || val instanceof LocalDateTime ||
               val instanceof OffsetDateTime || val instanceof LocalTime ||
               val instanceof PgInterval;
    }

    /**
     * Infer the DataType of a runtime Java value.
     */
    public static DataType inferType(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return DataType.INTEGER;
        if (value instanceof Long) return DataType.BIGINT;
        if (value instanceof Short) return DataType.SMALLINT;
        if (value instanceof Float) return DataType.REAL;
        if (value instanceof Double) return DataType.DOUBLE_PRECISION;
        if (value instanceof BigDecimal) return DataType.NUMERIC;
        if (value instanceof Boolean) return DataType.BOOLEAN;
        if (value instanceof LocalDate) return DataType.DATE;
        if (value instanceof LocalTime) return DataType.TIME;
        if (value instanceof LocalDateTime) return DataType.TIMESTAMP;
        if (value instanceof OffsetDateTime) return DataType.TIMESTAMPTZ;
        if (value instanceof PgInterval) return DataType.INTERVAL;
        if (value instanceof java.util.UUID) return DataType.UUID;
        if (value instanceof List) return DataType.TEXT; // arrays
        return DataType.TEXT;
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }
}
