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

    /**
     * Whether a value of the named PostgreSQL type can be assigned to a column of the target type
     * without being cast explicitly. PostgreSQL looks for a cast registered in {@code pg_cast} as
     * implicit or assignment, and where there is none it will still read the value through the
     * types' own text forms — but only <em>into</em> a string type, which is the one direction it
     * allows without a registered cast. That is why {@code text DEFAULT 1} stands and
     * {@code integer DEFAULT 'a'||'b'} does not, and why a category rule was too coarse: a
     * timestamp and an interval are both date/time types with no cast between them.
     *
     * <p>A type name this engine does not recognise is left alone: guessing would refuse
     * definitions PostgreSQL accepts.
     */
    public static boolean assignableFrom(String sourceTypeName, DataType target) {
        DataType source = DataType.fromPgName(sourceTypeName);
        if (source == null || target == null) return true;
        return CastLegality.assignable(source, target);
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
            case OID:
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
            case TIMETZ:
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
            case INT8MULTIRANGE:
            case NUMMULTIRANGE:
            case DATEMULTIRANGE:
            case TSMULTIRANGE:
            case TSTZMULTIRANGE:
                return TypeCategory.UNKNOWN;
            case XML:
                return TypeCategory.STRING;
            case BOOL_ARRAY:
            case INT2_ARRAY:
            case INT4_ARRAY:
            case INT8_ARRAY:
            case FLOAT4_ARRAY:
            case FLOAT8_ARRAY:
            case NUMERIC_ARRAY:
            case TEXT_ARRAY:
            case VARCHAR_ARRAY:
            case CHAR_ARRAY:
            case NAME_ARRAY:
            case DATE_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case TIME_ARRAY:
            case TIMETZ_ARRAY:
            case UUID_ARRAY:
            case BYTEA_ARRAY:
            case INTERVAL_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
            case INET_ARRAY:
            case ACLITEM_ARRAY:
                return TypeCategory.UNKNOWN;
            case ENUM:
            case HSTORE:
                return TypeCategory.UNKNOWN;
            // The object-identifier aliases are numbers in PostgreSQL's own type system: regclass
            // is category N and comparing one to an integer is what a catalog join does. They were
            // missing here, and the exception below is why DataType.fromPgName could not answer
            // with them -- a value written ::regclass had to be described to the client as an
            // integer, which is not what it holds.
            case REGPROC:
            case REGCLASS:
            case REGTYPE:
            case XID:
                return TypeCategory.NUMERIC;
            // A vector, a node tree, a composite and the statistics types have no category any
            // assignment rule reads; naming them keeps categoryOf total, which is what its callers
            // assume when they ask about a column type they did not choose.
            case INT2VECTOR:
            case OIDVECTOR:
            case PG_NODE_TREE:
            case PG_LSN:
            case PG_NDISTINCT:
            case PG_DEPENDENCIES:
            case PG_MCV_LIST:
            case ANYARRAY:
            case RECORD:
            case VOID:
            case MACADDR8:
            case OID_ARRAY:
            case RECORD_ARRAY:
            case INTERNAL_CHAR_ARRAY:
                return TypeCategory.UNKNOWN;
            case INTERNAL_CHAR:
                return TypeCategory.STRING;
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
            case OID:
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
     * One value held to the width, length or scale a type declaration names for it. A declaration
     * with no modifier bounds nothing, which is the ordinary case and leaves the value alone.
     *
     * <p>This is what reading a value <em>as</em> a type does, which is not the same as casting to
     * it: a cast to varchar(2) shortens a longer string, while reading one as varchar(2) refuses
     * it. Composite fields, and the RETURNING clause of the SQL/JSON expressions, are read.
     */
    static Object heldToItsType(Object value, String typeSpec) {
        if (value == null || typeSpec == null) return value;
        String spec = typeSpec.trim();
        int open = spec.indexOf('(');
        int close = spec.indexOf(')', open + 1);
        if (open <= 0 || close < 0) return value;
        String base = spec.substring(0, open).trim().toLowerCase(Locale.ROOT);
        String[] args = spec.substring(open + 1, close).split(",");
        int first;
        try {
            first = Integer.parseInt(args[0].trim());
        } catch (NumberFormatException e) {
            return value;
        }
        if (base.equals("varchar") || base.equals("character varying")) {
            String s = value.toString();
            if (s.length() > first) {
                throw new MemgresException(
                        "value too long for type character varying(" + first + ")", "22001");
            }
            return value;
        }
        if (base.equals("char") || base.equals("character") || base.equals("bpchar")) {
            String s = value.toString();
            if (s.length() > first) {
                throw new MemgresException(
                        "value too long for type character(" + first + ")", "22001");
            }
            StringBuilder padded = new StringBuilder(s);
            while (padded.length() < first) padded.append(' ');
            return padded.toString();
        }
        if (base.equals("numeric") || base.equals("decimal")) {
            BigDecimal bd;
            try {
                bd = new BigDecimal(value.toString().trim());
            } catch (NumberFormatException e) {
                return value;
            }
            int scale = 0;
            if (args.length > 1) {
                try {
                    scale = Integer.parseInt(args[1].trim());
                } catch (NumberFormatException e) {
                    return value;
                }
            }
            BigDecimal rounded = bd.setScale(scale, RoundingMode.HALF_UP);
            checkNumericTypmod(rounded, first, scale);
            return rounded;
        }
        return value;
    }

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
            case OID:
                return toInteger(value);
            case BIGINT:
            case BIGSERIAL:
                return toLong(value);
            case REAL: {
                Float f = toFloat(value);
                // A real column that answers Infinity for a stored 1e39 has lost what it was
                // given, so the value is refused on the way in the way PG refuses it.
                if (f.isInfinite() && !isInfiniteInput(value)) {
                    throw outOfFloatRange(value, "real", false);
                }
                if (NumericLimits.underflowedToZero(value, f.doubleValue())) {
                    throw outOfFloatRange(value, "real", true);
                }
                return f;
            }
            case DOUBLE_PRECISION: {
                Double d = toDouble(value);
                if (d.isInfinite() && !isInfiniteInput(value)) {
                    throw outOfFloatRange(value, "double precision", false);
                }
                if (NumericLimits.underflowedToZero(value, d.doubleValue())) {
                    throw outOfFloatRange(value, "double precision", true);
                }
                return d;
            }
            case NUMERIC: {
                // PG's numeric carries NaN and both infinities; memgres holds them as the
                // matching Double, and they must survive a store as readily as a cast.
                Double special = NumericLimits.specialNumericOrNull(value);
                if (special != null) return special;
                return toBigDecimal(value);
            }
            case MONEY:
                return toMoney(value);
            case VARCHAR:
            case CHAR:
            case TEXT:
                // A boolean reaching a string type goes through the cast PostgreSQL registered
                // between the two, which spells the value out in full; the single letter is what
                // boolean's own output function writes, and that is reached only inside an array
                // or a composite, where the letter is what PostgreSQL writes too. Storing the
                // letter left a varchar column answering t where PostgreSQL answers true, and let
                // character(1) hold a value four characters wide.
                if (value instanceof Boolean) return ((Boolean) value) ? "true" : "false";
                return toString(value);
            case BOOLEAN:
                return toBoolean(value);
            case DATE:
                return toLocalDate(value);
            case TIME:
                return toLocalTime(value);
            case TIMETZ:
                return toTimeTz(value);
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
                if (value instanceof InetValue) return value;
                return InetValue.parse(value.toString());
            case CIDR:
                if (value instanceof CidrValue) return value;
                if (value instanceof InetValue) return CidrValue.fromInet((InetValue) value);
                return CidrValue.parse(value.toString());
            case MACADDR:
                if (value instanceof MacaddrValue) return value;
                return MacaddrValue.parse(value.toString());
            case MACADDR8:
                if (value instanceof Macaddr8Value) return value;
                return Macaddr8Value.parse(value.toString());
            case HSTORE:
                if (value instanceof HstoreValue) return value;
                return HstoreValue.parse(value.toString());
            case TSVECTOR:
                if (value instanceof TsVector) return value;
                TsVector tv = TsVector.parseLiteral(value.toString());
                return tv != null ? tv : TsVector.empty();
            case TSQUERY:
                if (value instanceof TsQuery) return value;
                return TsQuery.parse(value.toString());
            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case TSTZRANGE:
                // A range's column type names its element type, so the bounds are read and stored
                // as values of it — the same normalisation an explicit cast performs.
                return RangeOperations.parse(value.toString().trim(), targetType.getPgName())
                        .toString();
            case INT4MULTIRANGE:
            case INT8MULTIRANGE:
            case NUMMULTIRANGE:
            case DATEMULTIRANGE:
            case TSMULTIRANGE:
            case TSTZMULTIRANGE:
                return normalizeMultirangeForStorage(value.toString().trim(), targetType);
            default:
                return value;
        }
    }

    private static String normalizeMultirangeForStorage(String text, DataType targetType) {
        String rangeType = targetType.getPgName().replace("multirange", "range");
        if (text.equalsIgnoreCase("empty")) return "{}";
        if (RangeOperations.isRangeString(text)) {
            RangeOperations.PgRange one = RangeOperations.parse(text, rangeType);
            return one.isEmpty() ? "{}" : "{" + one + "}";
        }
        java.util.List<RangeOperations.PgRange> parts = new java.util.ArrayList<>();
        for (RangeOperations.PgRange r : RangeOperations.parseMultirangeLiteral(text, rangeType)) {
            if (!r.isEmpty()) parts.add(r);
        }
        return RangeOperations.formatMultirange(RangeOperations.mergeAndSort(parts));
    }

    /**
     * Coerce a value for storage into a column. Less strict than explicit cast.
     */
    public static Object coerceForStorage(Object value, Column column) {
        if (value == null) return null;

        DataType type = column.getType();
        if (type == DataType.SERIAL || type == DataType.BIGSERIAL || type == DataType.SMALLSERIAL) return value; // handled separately

        // A log sequence number is text of a particular shape, so being a string is not enough
        // to be one: the check has to come before the value is taken as already stored.
        if (type == DataType.PG_LSN) return checkedLsn(value);

        // A geometric column holds a shape, and the shape is what the type's own reader makes of
        // the text. The reader was not run on the way in at all, so a column took whatever
        // characters it was given: a literal naming no shape was stored, and a box was kept
        // corner-for-corner as written rather than in the one order every box is held in --
        // which then compared unequal to the same box written the other way round.
        if (GeometricOperations.isGeometricType(type)) {
            return GeometricOperations.format(
                    GeometricOperations.parseAs(type.getPgName(), String.valueOf(value)));
        }

        // An array column holds an array. Writing the text of one into the column instead threw
        // away the bounds and the element type, so everything read back out of it was a string.
        if (DataType.isArrayType(type)) {
            PgArray array = PgArray.from(value);
            if (array != null) return coerceArrayElements(array, DataType.elementOf(type), column);
        }

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

        // A numeric special reaches its column as text, and a declared precision has no room for
        // one: PostgreSQL calls that an overflow of the field rather than bad input syntax, and
        // names the field it would not fit in.
        if (type == DataType.NUMERIC && column.getPrecision() != null) {
            Double special = NumericLimits.specialNumericOrNull(value);
            if (special != null) {
                rejectNonFiniteNumeric(special.doubleValue(), column.getPrecision(), column.getScale());
            }
        }
        try {
            Object coerced = coerce(value, type);
            return applyPrecision(coerced, type, column);
        } catch (MemgresException e) {
            if (storesArrayText(value, type)) {
                validateArrayElements(value, type);
                return value;
            }
            throw e;
        } catch (Exception e) {
            if (storesArrayText(value, type)) {
                validateArrayElements(value, type);
                return value;
            }
            throw new MemgresException(
                    "invalid input syntax for type " + type.getPgName() + ": \"" + value + "\"", "22P02");
        }
    }

    /**
     * Every element of an array read as the column's element type, keeping the array's bounds.
     * An element the element type refuses is refused here rather than stored as its own spelling.
     */
    static PgArray coerceArrayElements(PgArray array, DataType elementType) {
        return coerceArrayElements(array, elementType, null);
    }

    /**
     * As above, and every element held to the width the column declared as well.
     *
     * <p>A modifier written on an array belongs to the elements: varchar(4)[] is an array of
     * varchar(4), and PostgreSQL holds each element to the four characters exactly as it holds a
     * scalar column's value to them. Asking the array type for a width found none -- an array type
     * has no modifier of its own -- so a four-character column took a seven-character element and
     * kept it.
     */
    static PgArray coerceArrayElements(PgArray array, DataType elementType, Column column) {
        if (elementType == null) return array;
        // A column that declared no width bounds nothing, and asking is what makes every array
        // column that has none behave exactly as it did.
        Column width = column != null && column.getPrecision() != null ? column : null;
        List<Object> coerced = coerceElementList(array, elementType, width);
        return PgArray.of(coerced, array.lowerBounds(), elementType.getPgName());
    }

    private static List<Object> coerceElementList(List<?> elements, DataType elementType,
                                                  Column width) {
        List<Object> out = new ArrayList<Object>(elements.size());
        for (Object element : elements) {
            if (element == null) out.add(null);
            else if (element instanceof List<?>) {
                out.add(coerceElementList((List<?>) element, elementType, width));
            } else {
                Object one = coerce(element, elementType);
                out.add(width == null ? one : applyPrecision(one, elementType, width));
            }
        }
        return out;
    }

    /** Validate array elements match the expected array element type. */
    /**
     * Whether a value the target type's input function refused should still be stored as the array
     * text it looks like. Only an array column may: the test used to be that the text began with a
     * brace whatever the column was, so '{foo}' was accepted into a date column and stayed there
     * as the string it is, and the conversion error it had already raised was thrown away.
     */
    private static boolean storesArrayText(Object value, DataType type) {
        if (!DataType.isArrayType(type)) return false;
        return value instanceof java.util.List
                || (value instanceof String && ((String) value).trim().startsWith("{"));
    }

    private static void validateArrayElements(Object value, DataType type) {
        if (type == DataType.INT4_ARRAY && value instanceof String) {
            String s = ((String) value).trim();
            if (s.startsWith("{") && s.endsWith("}")) {
                String inner = s.substring(1, s.length() - 1);
                if (!inner.isEmpty()) {
                    for (String elem : inner.split(",")) {
                        elem = elem.trim();
                        if (elem.equalsIgnoreCase("NULL")) continue;
                        try { Long.parseLong(elem); } catch (NumberFormatException e) {
                            throw new MemgresException(
                                    "invalid input syntax for type integer: \"" + elem + "\"", "22P02");
                        }
                    }
                }
            }
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
                // NaN and the infinities have no BigDecimal form and are carried as Doubles;
                // they are already numeric values and need no further conversion.
                return value instanceof BigDecimal || NumericLimits.isSpecial(value);
            case MONEY:
                return value instanceof PgMoney;
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
                return value instanceof InetValue;
            case CIDR:
                return value instanceof CidrValue;
            case MACADDR:
                return value instanceof MacaddrValue;
            case MACADDR8:
                return value instanceof Macaddr8Value;
            case HSTORE:
                return value instanceof HstoreValue;
            case TSVECTOR:
                return value instanceof TsVector;
            case TSQUERY:
                return value instanceof TsQuery;
            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case TSTZRANGE:
            case INT4MULTIRANGE:
            case INT8MULTIRANGE:
            case NUMMULTIRANGE:
            case DATEMULTIRANGE:
            case TSMULTIRANGE:
            case TSTZMULTIRANGE:
                // A range arrives as text and stays text, but the text has to be the canonical
                // form for the column's element type rather than whatever was written.
                return false;
            default:
                return value instanceof String;
        }
    }

    /** A declared numeric(p[,s]) has no room for NaN or an infinity. */
    private static void rejectNonFiniteNumeric(double value, int precision, Integer scale) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            // Refused with the same sentence a cast to numeric(p,s) is refused with, so the field
            // is named however the value arrived at it.
            rejectSpecialForTypmod(value, precision, scale == null ? 0 : scale.intValue());
        }
    }

    /**
     * A date/time value rounded to the fractional-second precision its column declares, or null
     * when the value is not one this applies to. There was no branch for it at all, so a
     * timestamp(0) column kept every digit the value arrived with.
     */
    private static Object roundToPrecision(Object value, DataType type, int precision) {
        if (precision < 0 || precision > 6) return null;
        if (value instanceof LocalDateTime) {
            return isDateTimeInfinity(value) ? value
                    : ((LocalDateTime) value).truncatedTo(java.time.temporal.ChronoUnit.MICROS)
                        .plusNanos(roundingNanos(((LocalDateTime) value).getNano(), precision))
                        .truncatedTo(unitFor(precision));
        }
        if (value instanceof OffsetDateTime) {
            return isDateTimeInfinity(value) ? value
                    : ((OffsetDateTime) value).plusNanos(
                        roundingNanos(((OffsetDateTime) value).getNano(), precision))
                        .truncatedTo(unitFor(precision));
        }
        if (value instanceof LocalTime) {
            return ((LocalTime) value).plusNanos(
                    roundingNanos(((LocalTime) value).getNano(), precision)).truncatedTo(unitFor(precision));
        }
        return null;
    }

    /** How far to nudge a value so that truncating to this precision rounds it to nearest. */
    private static long roundingNanos(int nano, int precision) {
        long unit = 1L;
        for (int i = precision; i < 9; i++) unit *= 10L;
        long remainder = nano % unit;
        return remainder >= unit / 2 ? unit - remainder : -remainder;
    }

    private static java.time.temporal.ChronoUnit unitFor(int precision) {
        if (precision == 0) return java.time.temporal.ChronoUnit.SECONDS;
        if (precision <= 3) return java.time.temporal.ChronoUnit.MILLIS;
        return java.time.temporal.ChronoUnit.MICROS;
    }

    /**
     * A log sequence number, checked and written the way PostgreSQL writes it: two hexadecimal
     * numbers with one slash between them, in capitals. Storing the text unread put anything at
     * all under the type.
     */
    public static String checkedLsn(Object value) {
        String lsn = value.toString().trim();
        int slash = lsn.indexOf('/');
        if (slash <= 0 || slash == lsn.length() - 1 || lsn.indexOf('/', slash + 1) >= 0
                || !isHexRun(lsn.substring(0, slash)) || !isHexRun(lsn.substring(slash + 1))) {
            throw new MemgresException(
                    "invalid input syntax for type pg_lsn: \"" + value + "\"", "22P02");
        }
        return lsn.toUpperCase();
    }

    private static boolean isHexRun(String text) {
        if (text.isEmpty() || text.length() > 8) return false;
        for (int i = 0; i < text.length(); i++) {
            if (Character.digit(text.charAt(i), 16) < 0) return false;
        }
        return true;
    }

    /** Whether this text is a run of bits, so it can be read as the bit string it spells. */
    private static boolean isBitText(String text) {
        if (text.isEmpty()) return false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c != '0' && c != '1') return false;
        }
        return true;
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
        // NUMERIC(p[,s]). The scale defaults to zero when only a precision was declared, and a
        // declared precision has no room for NaN or an infinity. Requiring a scale before looking
        // at anything let numeric(5) hold 123456789 and numeric(5,1) hold Infinity.
        if (type == DataType.NUMERIC && column.getPrecision() != null && value instanceof Number
                && !(value instanceof BigDecimal)) {
            value = toBigDecimal(value);
        }
        if (type == DataType.NUMERIC && column.getPrecision() != null && value instanceof Double) {
            rejectNonFiniteNumeric((Double) value, column.getPrecision(), column.getScale());
        }
        if (type == DataType.NUMERIC && column.getPrecision() != null && value instanceof BigDecimal) {
            int scale = column.getScale() != null ? column.getScale() : 0;
            BigDecimal rounded = ((BigDecimal) value).setScale(scale, RoundingMode.HALF_UP);
            checkNumericTypmod(rounded, column.getPrecision(), scale);
            return rounded;
        }
        if (type == DataType.NUMERIC && column.getScale() != null && value instanceof BigDecimal) {
            return ((BigDecimal) value).setScale(column.getScale(), RoundingMode.HALF_UP);
        }
        // A name is truncated to what a name holds rather than kept whole.
        if (type == DataType.NAME && value instanceof String && ((String) value).length() > 63) {
            return ((String) value).substring(0, 63);
        }
        // timestamp(n) and time(n) keep n fractional digits, rounded as PostgreSQL rounds them.
        if (column.getPrecision() != null) {
            Object rounded = roundToPrecision(value, type, column.getPrecision());
            if (rounded != null) return rounded;
        }
        // interval day to second(n): the column keeps only the fields its qualifier reaches, and
        // only n fractional digits of its seconds — the same modifier a cast to the type applies.
        if (type == DataType.INTERVAL && value instanceof PgInterval
                && (column.getPrecision() != null || column.getIntervalQualifier() != null)) {
            StringBuilder spec = new StringBuilder("interval");
            if (column.getIntervalQualifier() != null) spec.append(' ').append(column.getIntervalQualifier());
            if (column.getPrecision() != null) spec.append('(').append(column.getPrecision()).append(')');
            IntervalTypmod typmod = IntervalTypmod.fromTypeSpec(spec.toString());
            if (typmod != null) return typmod.apply((PgInterval) value);
        }
        // BIT(n) / VARBIT(n) length enforcement. The value may arrive as the text of the bits
        // rather than as a bit string, and checking only the latter let a bit(4) column take
        // '101' and '10101' alike.
        if ((type == DataType.BIT || type == DataType.VARBIT) && value instanceof String
                && column.getPrecision() != null && isBitText((String) value)) {
            value = new AstExecutor.PgBitString((String) value);
        }
        if (type == DataType.BIT && value instanceof AstExecutor.PgBitString && column.getPrecision() != null) {
            String bits = ((AstExecutor.PgBitString) value).bits();
            int n = column.getPrecision();
            // On column assignment PG requires an exact length match for bit(n): a value that
            // is too short or too long is an error (22026). Padding only happens for an
            // explicit CAST (handled in CastEvaluator), not here.
            if (bits.length() != n) {
                throw new MemgresException("bit string length " + bits.length() + " does not match type bit(" + n + ")", "22026");
            }
        }
        if (type == DataType.VARBIT && value instanceof AstExecutor.PgBitString && column.getPrecision() != null) {
            String bits = ((AstExecutor.PgBitString) value).bits();
            int n = column.getPrecision();
            if (bits.length() > n) {
                throw new MemgresException("bit string too long for type bit varying(" + n + ")", "22001");
            }
        }
        return value;
    }

    // ---- Out-of-range exception helpers (with datatype field for wire protocol) ----

    private static MemgresException smallintOutOfRange() {
        return new MemgresException("smallint out of range", "22003");
    }

    private static MemgresException integerOutOfRange() {
        return new MemgresException("integer out of range", "22003");
    }

    private static MemgresException bigintOutOfRange() {
        return new MemgresException("bigint out of range", "22003");
    }

    // ---- Conversion helpers ----

    private static Short toShort(Object val) {
        if (val instanceof Number) {
            Number n = (Number) val;
            long lv = n.longValue();
            if (lv < Short.MIN_VALUE || lv > Short.MAX_VALUE) {
                throw smallintOutOfRange();
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
        if (val instanceof AstExecutor.PgBitString) {
            String bits = ((AstExecutor.PgBitString) val).bits();
            long lv = Long.parseLong(bits, 2);
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw integerOutOfRange();
            return (int) lv;
        }
        if (val instanceof RegclassValue) return ((RegclassValue) val).oid();
        if (val instanceof RegtypeValue) return ((RegtypeValue) val).oid();
        if (val instanceof RegprocValue) return ((RegprocValue) val).oid();
        // PG 18 casts bytea to integer by reading the bytes as the integer's own big-endian
        // representation. Without this the byte array fell through to the text path and the
        // client was shown a Java array identity — "[B@5a688f2e" — as if it were its input.
        if (val instanceof byte[]) {
            byte[] b = (byte[]) val;
            if (b.length != 4) {
                throw new MemgresException("smallint or bigint or integer expected", "22P03");
            }
            return ((b[0] & 0xff) << 24) | ((b[1] & 0xff) << 16) | ((b[2] & 0xff) << 8) | (b[3] & 0xff);
        }
        if (val instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) val;
            long lv;
            try {
                lv = bd.setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();
            } catch (ArithmeticException e) {
                // Past a long the exact conversion gives up, and the ArithmeticException was
                // reported as invalid input syntax — the value is perfectly good input, it is
                // only too big for the column.
                throw integerOutOfRange();
            }
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw integerOutOfRange();
            return (int) lv;
        }
        if (val instanceof Double || val instanceof Float) {
            long lv = roundFloatToLong(((Number) val).doubleValue());
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE) throw integerOutOfRange();
            return (int) lv;
        }
        if (val instanceof Number) {
            Number n = (Number) val;
            long lv = n.longValue();
            if (lv < Integer.MIN_VALUE || lv > Integer.MAX_VALUE)
                throw integerOutOfRange();
            return (int) lv;
        }
        if (val instanceof Boolean) return ((Boolean) val) ? 1 : 0;
        String s = val.toString().trim();
        if (s.isEmpty()) throw new MemgresException("invalid input syntax for type integer: \"\"", "22P02");
        java.math.BigInteger parsed = parseIntegerText(s);
        if (parsed == null) {
            throw new MemgresException("invalid input syntax for type integer: \"" + val + "\"", "22P02");
        }
        if (parsed.bitLength() >= 32) throw integerOutOfRange();
        return parsed.intValue();
    }

    public static Long toLong(Object val) {
        if (val instanceof AstExecutor.PgBitString) {
            String bits = ((AstExecutor.PgBitString) val).bits();
            if (bits.length() <= 63) return Long.parseLong(bits, 2);
            // 64-bit: parse as unsigned then reinterpret as signed (two's complement)
            return Long.parseUnsignedLong(bits.substring(bits.length() - 64), 2);
        }
        if (val instanceof java.math.BigDecimal) {
            // longValue() would wrap modulo 2^64, turning an out-of-range numeric into a
            // plausible number of the wrong sign rather than reporting it.
            java.math.BigDecimal rounded =
                    ((java.math.BigDecimal) val).setScale(0, java.math.RoundingMode.HALF_UP);
            try {
                return rounded.longValueExact();
            } catch (ArithmeticException e) {
                throw bigintOutOfRange();
            }
        }
        if (val instanceof java.math.BigInteger) {
            try {
                return ((java.math.BigInteger) val).longValueExact();
            } catch (ArithmeticException e) {
                throw bigintOutOfRange();
            }
        }
        if (val instanceof Double || val instanceof Float) {
            return roundFloatToLong(((Number) val).doubleValue());
        }
        if (val instanceof Number) return ((Number) val).longValue();
        if (val instanceof Boolean) return ((Boolean) val) ? 1L : 0L;
        String s = val.toString().trim();
        if (s.isEmpty()) throw new MemgresException("invalid input syntax for type bigint: \"\"", "22P02");
        java.math.BigInteger parsed = parseIntegerText(s);
        if (parsed == null) {
            throw new MemgresException("invalid input syntax for type bigint: \"" + val + "\"", "22P02");
        }
        if (parsed.bitLength() >= 64) throw bigintOutOfRange();
        return parsed.longValue();
    }

    /**
     * A float converts to an integer by rounding half to even, which is what PostgreSQL reports:
     * 0.5 and 2.5 both land on 2, 1.5 lands on 2. Truncating instead turns 1.6 into 1. A value
     * with no integer to land on is out of range, not a saturated bound — returning
     * Long.MAX_VALUE would report a plausible wrong number instead of an error.
     */
    private static long roundFloatToLong(double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) throw bigintOutOfRange();
        double rounded = Math.rint(d);
        if (rounded < -9.223372036854776E18 || rounded >= 9.223372036854776E18) {
            throw bigintOutOfRange();
        }
        return (long) rounded;
    }

    /**
     * The out-of-range PG reports for a value that will not fit a float type. A value that
     * arrived as a float is being narrowed by an operator, which PG names by the operation;
     * anything else is an input value, which PG quotes.
     */
    private static MemgresException outOfFloatRange(Object value, String typeName, boolean underflow) {
        if (value instanceof Double || value instanceof Float) {
            return underflow ? NumericLimits.floatUnderflow() : NumericLimits.floatOverflow();
        }
        return NumericLimits.outOfRangeForType(value, typeName);
    }

    /** True when the input was already an infinity, so narrowing it is not an overflow. */
    private static boolean isInfiniteInput(Object val) {
        if (val instanceof Double) return ((Double) val).isInfinite();
        if (val instanceof Float) return ((Float) val).isInfinite();
        if (!(val instanceof String)) return false;
        Double special = NumericLimits.specialNumericOrNull(val);
        return special != null && special.isInfinite();
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
                break;
        }
        // real reads the same spellings float8 does — inf, +inf, -inf, infinity and nan, in any
        // case. Only the canonical three were matched here, so a real column refused input its own
        // double-precision sibling accepted, and the parse failure escaped as an internal error.
        Double special = NumericLimits.specialNumericOrNull(s);
        if (special != null) return special.floatValue();
        try {
            return Float.parseFloat(s);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type real: \"" + s + "\"", "22P02");
        }
    }

    public static Double toDouble(Object val) {
        if (val instanceof Number) return ((Number) val).doubleValue();
        String s = val.toString().trim();
        String lower = s.toLowerCase();
        if (lower.equals("infinity") || lower.equals("inf") || lower.equals("+infinity") || lower.equals("+inf")) {
            return Double.POSITIVE_INFINITY;
        }
        if (lower.equals("-infinity") || lower.equals("-inf")) {
            return Double.NEGATIVE_INFINITY;
        }
        if (lower.equals("nan")) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            // float8's input function accepts the same non-decimal integer forms int4 does
            java.math.BigInteger whole = parseIntegerText(s);
            if (whole != null) return whole.doubleValue();
            throw e;
        }
    }

    public static PgMoney toMoney(Object val) {
        if (val instanceof PgMoney) return (PgMoney) val;
        if (val instanceof BigDecimal) return new PgMoney((BigDecimal) val);
        if (val instanceof Number) return new PgMoney(BigDecimal.valueOf(((Number) val).doubleValue()));
        return PgMoney.parse(val.toString());
    }

    /**
     * Reject a value that no longer fits its declared numeric(p,s) after rounding. PG states
     * the bound it checked, so the message names the precision and scale.
     */
    public static void checkNumericTypmod(BigDecimal rounded, int precision, int scale) {
        int intDigits = precision - scale;
        // A scale wider than the precision leaves a fractional-only field: 10^-5 is still the
        // bound the value must stay under, so the check runs for a negative digit count too.
        BigDecimal limit = BigDecimal.ONE.movePointRight(intDigits);
        if (rounded.abs().compareTo(limit) >= 0) {
            // PG writes the bound as a power of ten except at the exponent zero, where it says 1.
            String bound = intDigits == 0 ? "1" : "10^" + intDigits;
            throw new MemgresException("numeric field overflow"
                    + "\n  Detail: A field with precision " + precision + ", scale " + scale
                    + " must round to an absolute value less than " + bound + ".", "22003");
        }
    }

    /** A declared numeric(p,s) has no room for NaN or an infinity, and PG says which it was. */
    public static void rejectSpecialForTypmod(double special, int precision, int scale) {
        String what = Double.isNaN(special) ? "NaN" : "an infinite value";
        throw new MemgresException("numeric field overflow"
                + "\n  Detail: A field with precision " + precision + ", scale " + scale
                + " cannot hold " + what + ".", "22003");
    }

    /** Longest a varchar or char may declare: PG packs the length into a typmod capped here. */
    private static final int MAX_CHAR_LENGTH = 10485760;

    /**
     * Reject a type modifier outside the range the type's own input function accepts. PG checks
     * these when the declaration is made, so a table it could never hold is never created.
     *
     * @param typeSpec the written type, modifier included, e.g. {@code numeric(1001,2)}
     */
    public static void checkDeclaredTypeLimits(String typeSpec) {
        if (typeSpec == null) return;
        int open = typeSpec.indexOf('(');
        int close = typeSpec.indexOf(')', open + 1);
        if (open < 0 || close < 0) return;
        String name = typeSpec.substring(0, open).trim().toLowerCase(java.util.Locale.ROOT);
        String[] args = typeSpec.substring(open + 1, close).split(",");
        // numeric takes its modifier as an expression rather than as a bare integer token, so a
        // number too wide for the int4 a modifier is held in fails as a bad integer before it is
        // a precision or a scale at all.
        if (name.equals("numeric") || name.equals("decimal")) {
            for (int i = 0; i < args.length; i++) {
                String written = args[i].trim();
                java.math.BigInteger value;
                try {
                    value = new java.math.BigInteger(written);
                } catch (NumberFormatException e) {
                    continue; // not a whole number, so the checks below say what it is instead
                }
                if (value.bitLength() > 31) {
                    throw new MemgresException("value \"" + written
                            + "\" is out of range for type integer", "22003");
                }
            }
        }
        Integer first = parseModifier(args, 0);
        if (first == null) return;

        if (name.equals("varchar") || name.equals("character varying")) {
            checkCharLength(first, "varchar");
        } else if (name.equals("char") || name.equals("character") || name.equals("bpchar")) {
            checkCharLength(first, "char");
        } else if (name.equals("bit")) {
            if (first < 1) throw lengthAtLeastOne("bit");
        } else if (name.equals("varbit") || name.equals("bit varying")) {
            if (first < 1) throw lengthAtLeastOne("varbit");
        } else if (name.equals("numeric") || name.equals("decimal")) {
            if (first < 1 || first > 1000) {
                throw new MemgresException("NUMERIC precision " + first
                        + " must be between 1 and 1000", "22023");
            }
            Integer scale = parseModifier(args, 1);
            if (scale != null && (scale < -1000 || scale > 1000)) {
                throw new MemgresException("NUMERIC scale " + scale
                        + " must be between -1000 and 1000", "22023");
            }
        } else if (name.equals("float")) {
            if (first < 1) {
                throw new MemgresException("precision for type float must be at least 1 bit", "22023");
            }
            if (first > 53) {
                throw new MemgresException("precision for type float must be less than 54 bits", "22023");
            }
        }
    }

    private static void checkCharLength(int length, String typeName) {
        if (length < 1) throw lengthAtLeastOne(typeName);
        if (length > MAX_CHAR_LENGTH) {
            throw new MemgresException("length for type " + typeName
                    + " cannot exceed " + MAX_CHAR_LENGTH, "22023");
        }
    }

    private static MemgresException lengthAtLeastOne(String typeName) {
        return new MemgresException("length for type " + typeName + " must be at least 1", "22023");
    }

    /** The n-th modifier as an integer, or null when it is absent or not a plain number. */
    private static Integer parseModifier(String[] args, int index) {
        if (index >= args.length) return null;
        try {
            return Integer.valueOf(args[index].trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static BigDecimal toBigDecimal(Object val) {
        if (val instanceof PgMoney) return ((PgMoney) val).getValue();
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
            // numeric's input function reads the non-decimal integer forms and the underscore
            // separator exactly as int4's does, so '0x2a'::numeric is 42 and not an error
            java.math.BigInteger whole = parseIntegerText(s);
            if (whole != null) return new BigDecimal(whole);
            throw new MemgresException("invalid input syntax for type numeric: \"" + val + "\"", "22P02");
        }
    }

    /**
     * PG has no year zero: a proleptic year of 0 or less is written as its BC equivalent with
     * an era suffix, so ISO year -43 prints as {@code 0044-... BC}.
     */
    public static String formatIsoDate(LocalDate d) {
        int y = d.getYear();
        return String.format("%04d-%02d-%02d%s", displayYear(y),
                d.getMonthValue(), d.getDayOfMonth(), eraSuffix(y));
    }

    /** The BC suffix PG appends after the time part of a pre-Christian timestamp. */
    /**
     * {@code "infinity"} or {@code "-infinity"} when this value is one of the sentinels, else
     * null. A date and a timestamp both have the two, and a timestamptz carries the timestamp's.
     */
    public static String infinityText(Object val) {
        if (val instanceof LocalDateTime) {
            if (val.equals(TIMESTAMP_INFINITY)) return "infinity";
            if (val.equals(TIMESTAMP_NEG_INFINITY)) return "-infinity";
            return null;
        }
        if (val instanceof LocalDate) {
            if (val.equals(DATE_INFINITY)) return "infinity";
            if (val.equals(DATE_NEG_INFINITY)) return "-infinity";
            return null;
        }
        if (val instanceof OffsetDateTime) {
            return infinityText(((OffsetDateTime) val).toLocalDateTime());
        }
        return null;
    }

    /** Whether this value is one of the date/time infinities. */
    public static boolean isDateTimeInfinity(Object val) {
        return infinityText(val) != null;
    }

    public static String eraSuffix(int prolepticYear) {
        return prolepticYear > 0 ? "" : " BC";
    }

    /** The year PG writes for a proleptic year, ignoring the era. */
    public static int displayYear(int prolepticYear) {
        return prolepticYear > 0 ? prolepticYear : 1 - prolepticYear;
    }

    /**
     * A value written the way PostgreSQL writes it, for the places that build a literal out of
     * several values — an array, a composite — rather than casting one on its own.
     */
    static String toString(Object val) {
        // An infinity is written as the word, not as the instant that stands for it.
        String infinite = infinityText(val);
        if (infinite != null) return infinite;
        // The types whose Java rendering is not PostgreSQL's. A boolean inside a container is one
        // letter, a bytea is its hex form, and a numeric is written in full rather than in Java's
        // exponent notation -- none of which Object.toString() gives.
        if (val instanceof Boolean) return ((Boolean) val) ? "t" : "f";
        if (val instanceof byte[]) return byteaToText((byte[]) val);
        if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).toPlainString();
        if (val instanceof Double) return PgFloatFormat.float8out((Double) val);
        if (val instanceof Float) return PgFloatFormat.float4out((Float) val);
        if (val instanceof LocalDate) return formatIsoDate((LocalDate) val);
        if (val instanceof LocalDateTime) {
            LocalDateTime dt = (LocalDateTime) val;
            String datePart = String.format("%04d-%02d-%02d", displayYear(dt.getYear()),
                    dt.getMonthValue(), dt.getDayOfMonth());
            String era = eraSuffix(dt.getYear());
            if (dt.getNano() != 0) {
                String s = datePart + " " + dt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"));
                // Strip trailing zeros from fractional seconds
                return s.replaceAll("0+$", "").replaceAll("\\.$", "") + era;
            }
            return datePart + " " + dt.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + era;
        }
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toString();
        if (val instanceof LocalTime) {
            LocalTime lt = (LocalTime) val;
            if (isEndOfDay(lt)) return "24:00:00";
            if (lt.getNano() == 0) return lt.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            // Java pads the fraction to a multiple of three digits; PostgreSQL writes only the
            // digits the value has, so 01:02:03.99 stays two digits wide
            String s = lt.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"));
            return s.replaceAll("0+$", "").replaceAll("\\.$", "");
        }
        if (val instanceof PgInterval) return ((PgInterval) val).toString();
        if (val instanceof java.util.List<?>) return formatPgArray((java.util.List<?>) val);
        if (val instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) val).label();
        // A composite has one writer, the one that quotes its fields; a second one here wrote a
        // row whose fields no reader could find again.
        if (val instanceof AstExecutor.PgRow) return ((AstExecutor.PgRow) val).toPgText();
        if (val instanceof java.util.Map<?, ?>) {
            return AstExecutor.PgRow.fromFieldMap((java.util.Map<?, ?>) val).toPgText();
        }
        return val.toString();
    }

    /**
     * A document as jsonb stores and writes it: members sorted by key with one entry per distinct
     * key, numbers as numerics, strings holding what their escapes named, and a space after every
     * colon and comma.
     */
    public static String normalizeJsonb(String json) {
        if (json == null) return null;
        return JsonOperations.normalizeJsonb(json);
    }

    /** Format a Java List as a PG array string: {elem1,elem2,...} */
    /** PG's bytea output in hex format. */
    public static String byteaToText(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2 + 2).append("\\x");
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16)).append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }

    /**
     * An array written the way {@code array_out} writes it. This is the only array writer: the
     * three others that grew beside it each quoted a different set of characters, so the same
     * array had three spellings and only one of them could be read back.
     *
     * <p>An array whose dimensions do not start at 1 states them in front of the braces, because
     * that is the only place its bounds can be kept.
     */
    public static String formatPgArray(java.util.List<?> list) {
        String prefix = "";
        if (list instanceof PgArray) {
            PgArray array = (PgArray) list;
            if (array.hasCustomLowerBounds()) prefix = array.boundsPrefix();
        }
        StringBuilder sb = new StringBuilder(prefix).append("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            Object elem = list.get(i);
            if (elem == null) {
                sb.append("NULL");
            } else if (elem instanceof java.util.List<?>) {
                sb.append(formatPgArray((java.util.List<?>) elem));
            } else {
                appendArrayElement(sb, toString(elem));
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * One element of an array literal. PostgreSQL quotes an element that is empty, that spells
     * NULL in any case, or that carries a brace, the delimiter, a quote, a backslash or any
     * whitespace — and escapes the quote and the backslash inside the quotes.
     */
    static void appendArrayElement(StringBuilder sb, String text) {
        if (!needsArrayQuoting(text)) {
            sb.append(text);
            return;
        }
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\');
            sb.append(c);
        }
        sb.append('"');
    }

    private static boolean needsArrayQuoting(String text) {
        if (text.isEmpty() || text.equalsIgnoreCase("NULL")) return true;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '{' || c == '}' || c == ',' || c == '"' || c == '\\'
                    || Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }

    /** The words boolean input accepts, each matchable by any prefix that names only one of them. */
    private static final String[] BOOLEAN_TRUE_WORDS = {"true", "yes", "on"};
    private static final String[] BOOLEAN_FALSE_WORDS = {"false", "no", "off"};

    public static Boolean toBoolean(Object val) {
        if (val instanceof Boolean) return ((Boolean) val);
        if (val instanceof Number) return ((Number) val).intValue() != 0;
        String s = val.toString().trim().toLowerCase();
        if (s.equals("1")) return true;
        if (s.equals("0")) return false;
        // PG takes any prefix that names exactly one word, so "tr" is true and "fals" is false,
        // while "o" is neither because it starts both "on" and "off".
        Boolean matched = null;
        if (!s.isEmpty()) {
            for (String word : BOOLEAN_TRUE_WORDS) {
                if (word.startsWith(s)) {
                    if (matched != null && !matched) return ambiguousBoolean(val);
                    matched = Boolean.TRUE;
                }
            }
            for (String word : BOOLEAN_FALSE_WORDS) {
                if (word.startsWith(s)) {
                    if (matched != null && matched) return ambiguousBoolean(val);
                    matched = Boolean.FALSE;
                }
            }
        }
        if (matched != null) return matched;
        throw new MemgresException("invalid input syntax for type boolean: \"" + val + "\"", "22P02");
    }

    private static Boolean ambiguousBoolean(Object val) {
        throw new MemgresException("invalid input syntax for type boolean: \"" + val + "\"", "22P02");
    }

    /**
     * Read text as an integer the way PostgreSQL's input function does: an optional sign, then
     * either a radix prefix ({@code 0x}, {@code 0o}, {@code 0b}) or decimal digits, with
     * underscores allowed between digits as separators. A fraction is not integer input, however
     * close to whole it is — {@code '2.5'::int} is an error, while {@code 2.5::numeric::int} is 3.
     *
     * @return the value, or null when the text is not integer input at all
     */
    static java.math.BigInteger parseIntegerText(String raw) {
        String s = raw.trim();
        if (s.isEmpty()) return null;
        boolean negative = false;
        int i = 0;
        char first = s.charAt(0);
        if (first == '+' || first == '-') {
            negative = first == '-';
            i = 1;
        }
        int radix = 10;
        if (i + 1 < s.length() && s.charAt(i) == '0') {
            char kind = Character.toLowerCase(s.charAt(i + 1));
            if (kind == 'x') { radix = 16; i += 2; }
            else if (kind == 'o') { radix = 8; i += 2; }
            else if (kind == 'b') { radix = 2; i += 2; }
        }
        StringBuilder digits = new StringBuilder();
        boolean lastWasDigit = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                // a separator has to sit between two digits, never at either end
                if (!lastWasDigit || i + 1 >= s.length()) return null;
                lastWasDigit = false;
                continue;
            }
            if (Character.digit(c, radix) < 0) return null;
            digits.append(c);
            lastWasDigit = true;
        }
        if (digits.length() == 0 || !lastWasDigit) return null;
        java.math.BigInteger value = new java.math.BigInteger(digits.toString(), radix);
        return negative ? value.negate() : value;
    }

    // ---- Date/Time conversions ----

    /**
     * H37: current DateStyle field order ("MDY", "DMY", or "YMD") for interpreting
     * ambiguous numeric date input. Set per-statement from the session GUC by
     * {@link AstExecutor#execute}. Defaults to PG's default of "MDY".
     */
    private static final ThreadLocal<String> DATE_ORDER = ThreadLocal.withInitial(() -> "MDY");

    /** Set the DateStyle field order used for parsing ambiguous numeric date input. */
    public static void setDateOrder(String order) {
        DATE_ORDER.set((order == null || order.isEmpty()) ? "MDY" : order.toUpperCase());
    }

    /** Current DateStyle field order used for parsing ambiguous numeric date input. */
    public static String getDateOrder() {
        return DATE_ORDER.get();
    }

    /**
     * The session IntervalStyle, published per-statement alongside DateStyle.
     *
     * <p>It says how an interval is written, and also how an ambiguously written one is read:
     * under the SQL standard's rules one sign stands for every field that follows it.
     */
    private static final ThreadLocal<String> INTERVAL_STYLE =
            ThreadLocal.withInitial(() -> "postgres");

    public static void setIntervalStyle(String style) {
        INTERVAL_STYLE.set(style == null || style.isEmpty() ? "postgres" : style);
    }

    public static String getIntervalStyle() {
        return INTERVAL_STYLE.get();
    }

    /**
     * The session TimeZone, published per-statement by {@link AstExecutor#execute} the same way
     * DateStyle is. "Now" in PostgreSQL is a moment in the session's zone, not in the server
     * JVM's: with TimeZone set to UTC, an Amsterdam server is already on the next calendar day
     * for over an hour before {@code CURRENT_DATE} is allowed to move.
     */
    private static final ThreadLocal<ZoneId> SESSION_ZONE = new ThreadLocal<ZoneId>();

    /** Set the session TimeZone used to resolve the current date and time. */
    public static void setSessionZone(ZoneId zone) {
        SESSION_ZONE.set(zone);
    }

    /** The bound zone, or null outside a statement — for saving and restoring it. */
    static ZoneId rawSessionZone() {
        return SESSION_ZONE.get();
    }

    /** The session TimeZone, or the JVM default outside a session. */
    public static ZoneId sessionZone() {
        ZoneId zone = SESSION_ZONE.get();
        return zone != null ? zone : ZoneId.systemDefault();
    }

    /**
     * The instant the current statement reads as "now", published by {@link AstExecutor#execute}.
     * PostgreSQL answers every current date/time from one timestamp, so 'now' and LOCALTIMESTAMP
     * in the same statement cannot land microseconds apart.
     */
    private static final ThreadLocal<OffsetDateTime> SESSION_INSTANT = new ThreadLocal<OffsetDateTime>();

    /** Set the instant that resolves 'now', 'today', 'yesterday' and 'tomorrow'. */
    public static void setSessionInstant(OffsetDateTime instant) {
        SESSION_INSTANT.set(instant);
    }

    /** The bound instant, or null outside a statement — for saving and restoring it. */
    static OffsetDateTime rawSessionInstant() {
        return SESSION_INSTANT.get();
    }

    /** The current statement's instant, or a fresh reading outside a statement. */
    public static OffsetDateTime sessionInstant() {
        OffsetDateTime instant = SESSION_INSTANT.get();
        return instant != null ? instant : OffsetDateTime.now();
    }

    /** "Now" as a wall clock in the session's own zone. */
    private static LocalDateTime nowHere() {
        return sessionInstant().atZoneSameInstant(sessionZone()).toLocalDateTime();
    }

    // Three numeric fields separated by a single '/' or '-' (same separator), e.g. 01/02/2026.
    private static final java.util.regex.Pattern NUMERIC_DATE =
            java.util.regex.Pattern.compile("^(\\d{1,4})([-/])(\\d{1,2})\\2(\\d{1,4})$");

    /**
     * H37: interpret a 3-field slash/dash numeric date according to the DateStyle field
     * order (DMY/YMD/MDY). Returns null if {@code s} isn't such a date or the fields are
     * out of range (so callers fall through to the default formatters).
     * A 4-digit leading field always means year-first (YMD), matching PG.
     */
    private static LocalDate tryOrderedNumericDate(String s, String order) {
        java.util.regex.Matcher m = NUMERIC_DATE.matcher(s);
        if (!m.matches()) return null;
        int f1 = Integer.parseInt(m.group(1));
        int f2 = Integer.parseInt(m.group(3));
        int f3 = Integer.parseInt(m.group(4));
        boolean f1IsYear = m.group(1).length() >= 3; // 3+ digit leading field is the year
        int year, month, day, yearLen;
        if (f1IsYear || "YMD".equals(order)) {
            year = f1; month = f2; day = f3; yearLen = m.group(1).length();
        } else if ("DMY".equals(order)) {
            day = f1; month = f2; year = f3; yearLen = m.group(4).length();
        } else { // MDY
            month = f1; day = f2; year = f3; yearLen = m.group(4).length();
        }
        year = normalizeTwoDigitYear(year, yearLen);
        if (year == 0) return null;
        try {
            return LocalDate.of(year, month, day);
        } catch (java.time.DateTimeException e) {
            return null;
        }
    }

    /** PG two-digit year rule: 00-69 -> 2000-2069, 70-99 -> 1970-1999. */
    private static int normalizeTwoDigitYear(int year, int digits) {
        if (digits <= 2) {
            return year < 70 ? 2000 + year : 1900 + year;
        }
        return year;
    }

    private static final DateTimeFormatter[] DATE_FORMATS = {
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
    };

    // Additional date formats with named months
    private static final DateTimeFormatter[] NAMED_MONTH_FORMATS = {
            DateTimeFormatter.ofPattern("MMMM d, yyyy", java.util.Locale.ENGLISH),  // January 8, 1999
            DateTimeFormatter.ofPattern("MMM d, yyyy", java.util.Locale.ENGLISH),    // Jan 8, 1999
            DateTimeFormatter.ofPattern("yyyy-MMM-dd", java.util.Locale.ENGLISH),    // 1999-Jan-08
            DateTimeFormatter.ofPattern("dd-MMM-yyyy", java.util.Locale.ENGLISH),    // 08-Jan-1999
            DateTimeFormatter.ofPattern("MMM dd yyyy", java.util.Locale.ENGLISH),    // Jan 08 1999
    };

    // Julian Day Number epoch: November 24, 4714 BC in proleptic Gregorian = JD 0
    private static final long JULIAN_EPOCH_JD = 1721426L; // JD of 0001-01-01

    private static LocalDate julianDayToDate(long jd) {
        // Convert Julian Day Number to LocalDate
        // JD 2451545 = 2000-01-01
        // Use the known reference: JD 2451545 = 2000-01-01
        long daysDiff = jd - 2451545L;
        return LocalDate.of(2000, 1, 1).plusDays(daysDiff);
    }

    /**
     * Replace only the first space (date-time separator) with 'T', preserving
     * spaces before timezone offsets. Handles: "YYYY-MM-DD HH:MM:SS +05:00"
     */
    private static String replaceDateTimeSeparator(String s) {
        // Find the space between date and time parts (after YYYY-MM-DD)
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "^(\\d{4}-\\d{2}-\\d{2}) (\\d)"
        ).matcher(s);
        if (m.find()) {
            return s.substring(0, m.start(1) + 10) + "T" + s.substring(m.start(2));
        }
        return s;
    }

    /**
     * Normalize a timezone offset string that may use compact +HHMM format
     * or have a space before the offset. Converts to standard +HH:MM format.
     */
    private static String normalizeTimezoneOffset(String s) {
        // Handle space before offset: "2024-01-08T04:05:06 +00:00" -> "2024-01-08T04:05:06+00:00"
        s = s.replaceAll("\\s+([+-]\\d)", "$1");
        // Handle +HHMM (4-digit offset without colon): "...+0530" -> "...+05:30"
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "([+-])(\\d{2})(\\d{2})$"
        ).matcher(s);
        if (m.find()) {
            s = s.substring(0, m.start()) + m.group(1) + m.group(2) + ":" + m.group(3);
        }
        return s;
    }

    /**
     * Map of common timezone abbreviations to their UTC offsets.
     * PG has a much larger list; we cover the most common ones.
     */
    private static final java.util.Map<String, String> TZ_ABBREVIATIONS = new java.util.HashMap<>();
    static {
        TZ_ABBREVIATIONS.put("UTC", "+00:00");
        TZ_ABBREVIATIONS.put("GMT", "+00:00");
        TZ_ABBREVIATIONS.put("Z", "+00:00");
        TZ_ABBREVIATIONS.put("EST", "-05:00");
        TZ_ABBREVIATIONS.put("EDT", "-04:00");
        TZ_ABBREVIATIONS.put("CST", "-06:00");
        TZ_ABBREVIATIONS.put("CDT", "-05:00");
        TZ_ABBREVIATIONS.put("MST", "-07:00");
        TZ_ABBREVIATIONS.put("MDT", "-06:00");
        TZ_ABBREVIATIONS.put("PST", "-08:00");
        TZ_ABBREVIATIONS.put("PDT", "-07:00");
        TZ_ABBREVIATIONS.put("HST", "-10:00");
        TZ_ABBREVIATIONS.put("AKST", "-09:00");
        TZ_ABBREVIATIONS.put("AKDT", "-08:00");
        TZ_ABBREVIATIONS.put("AST", "-04:00");
        TZ_ABBREVIATIONS.put("NST", "-03:30");
        TZ_ABBREVIATIONS.put("NDT", "-02:30");
        TZ_ABBREVIATIONS.put("CET", "+01:00");
        TZ_ABBREVIATIONS.put("CEST", "+02:00");
        TZ_ABBREVIATIONS.put("EET", "+02:00");
        TZ_ABBREVIATIONS.put("EEST", "+03:00");
        TZ_ABBREVIATIONS.put("WET", "+00:00");
        TZ_ABBREVIATIONS.put("WEST", "+01:00");
        TZ_ABBREVIATIONS.put("IST", "+05:30");
        TZ_ABBREVIATIONS.put("JST", "+09:00");
        TZ_ABBREVIATIONS.put("KST", "+09:00");
        TZ_ABBREVIATIONS.put("CST6CDT", "-06:00");
        TZ_ABBREVIATIONS.put("AEST", "+10:00");
        TZ_ABBREVIATIONS.put("AEDT", "+11:00");
        TZ_ABBREVIATIONS.put("ACST", "+09:30");
        TZ_ABBREVIATIONS.put("ACDT", "+10:30");
        TZ_ABBREVIATIONS.put("AWST", "+08:00");
        TZ_ABBREVIATIONS.put("NZST", "+12:00");
        TZ_ABBREVIATIONS.put("NZDT", "+13:00");
    }

    public static Object toLocalDateOrBc(Object val) {
        if (val instanceof String) {
            String s = (String) val;
            String trimmed = s.trim();
            // Handle date infinity/negative infinity
            if (trimmed.equalsIgnoreCase("infinity")) return "infinity";
            if (trimmed.equalsIgnoreCase("-infinity")) return "-infinity";
            // A BC date is a real date with a proleptic year, not text carrying a suffix:
            // keeping it typed is what lets it be compared and subtracted. The era marker
            // comes back at render time, from formatIsoDate.
        }
        return toLocalDate(val);
    }

    /** True when the text carries a trailing BC era marker. */
    private static boolean endsWithEra(String s) {
        String t = s.trim();
        return t.length() > 3 && t.toUpperCase().endsWith(" BC");
    }

    /** The date/time text with its trailing era marker removed. */
    private static String stripEra(String s) {
        String t = s.trim();
        return t.substring(0, t.length() - 3).trim();
    }

    public static LocalDate toLocalDate(Object val) {
        if (val instanceof LocalDate) {
            LocalDate d = (LocalDate) val;
            if (d.getYear() == 0) throw new MemgresException("date/time field value out of range: \"" + val + "\"", "22008");
            return d;
        }
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).toLocalDate();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).atZoneSameInstant(sessionZone()).toLocalDate();
        String s = val.toString().trim();
        LocalDateTime calendar = parseCalendarLiteral(s, val.toString(), "date", DATE_MAX_YEAR);
        if (calendar != null) return calendar.toLocalDate();
        if (endsWithEra(s)) {
            LocalDate bc = toLocalDate(stripEra(s));
            return bc.withYear(1 - bc.getYear());
        }
        // Handle special keywords
        switch (s.toLowerCase()) {
            case "epoch": return LocalDate.of(1970, 1, 1);
            case "today": return nowHere().toLocalDate();
            case "yesterday": return nowHere().toLocalDate().minusDays(1);
            case "tomorrow": return nowHere().toLocalDate().plusDays(1);
            case "infinity": return DATE_INFINITY;
            case "-infinity": return DATE_NEG_INFINITY;
        }
        // Reject year 0000 - PostgreSQL has no year zero
        if (s.startsWith("0000-")) {
            throw new MemgresException("date/time field value out of range: \"" + val + "\"", "22008");
        }
        // Julian day format: J2451545
        if (s.length() > 1 && (s.charAt(0) == 'J' || s.charAt(0) == 'j') && s.substring(1).matches("\\d+")) {
            // A Julian day beyond what a long holds is a date out of range, not an internal error.
            long jd;
            try {
                jd = Long.parseLong(s.substring(1));
            } catch (NumberFormatException e) {
                throw new MemgresException(
                        "date/time field value out of range: \"" + val + "\"", "22008");
            }
            return julianDayToDate(jd);
        }
        // Compact YYYYMMDD format (exactly 8 digits)
        if (s.matches("\\d{8}")) {
            try {
                return LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE);
            } catch (DateTimeParseException e) { /* fall through */ }
        }
        // H37: apply DateStyle field order (DMY/YMD) to ambiguous numeric date input.
        // For the default MDY order we defer to DATE_FORMATS below to preserve existing behavior.
        String dateOrder = DATE_ORDER.get();
        if (dateOrder != null && !"MDY".equals(dateOrder)) {
            LocalDate ordered = tryOrderedNumericDate(s, dateOrder);
            if (ordered != null) return ordered;
        }
        for (DateTimeFormatter fmt : DATE_FORMATS) {
            try {
                LocalDate d = LocalDate.parse(s, fmt);
                if (d.getYear() == 0) throw new MemgresException("date/time field value out of range: \"" + val + "\"", "22008");
                return d;
            } catch (DateTimeParseException e) { /* try next */ }
        }
        // Try named month formats
        for (DateTimeFormatter fmt : NAMED_MONTH_FORMATS) {
            try {
                LocalDate d = LocalDate.parse(s, fmt);
                return d;
            } catch (DateTimeParseException e) { /* try next */ }
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
        // A date that is written correctly but names a day that does not exist has a field out of
        // range (2023-02-29); text that is not a date at all never had a field to overflow, so PG
        // reports it as input syntax instead.
        if (s.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            throw new MemgresException(
                    "date/time field value out of range: \"" + val + "\"", "22008");
        }
        // A year of five digits or more is written correctly, and date holds years up to 5874897;
        // past that it is the range that fails rather than the spelling
        java.util.regex.Matcher wide = WIDE_YEAR.matcher(s);
        if (wide.matches()) {
            long year = Long.parseLong(wide.group(1));
            if (year > DATE_MAX_YEAR) {
                throw new MemgresException("date out of range: \"" + val + "\"", "22008");
            }
            try {
                return LocalDate.of((int) year, Integer.parseInt(wide.group(2)),
                        Integer.parseInt(wide.group(3)));
            } catch (RuntimeException e) {
                throw new MemgresException(
                        "date/time field value out of range: \"" + val + "\"", "22008");
            }
        }
        throw new MemgresException("invalid input syntax for type date: \"" + val + "\"", "22007");
    }

    /** A date whose year runs to five digits or more, which the ISO parsers will not read. */
    private static final java.util.regex.Pattern WIDE_YEAR =
            java.util.regex.Pattern.compile("^(\\d{5,})-(\\d{1,2})-(\\d{1,2})$");

    /**
     * The same wide year, with the time of day a timestamp literal may carry. A timestamp holds
     * years up to 294276, so its widest legal spellings run past what the ISO parsers will read
     * and have to be taken apart by hand rather than reported as malformed.
     */
    private static final java.util.regex.Pattern WIDE_YEAR_TIMESTAMP =
            java.util.regex.Pattern.compile(
                    "^(\\d{5,})-(\\d{1,2})-(\\d{1,2})(?:[ T](\\d{1,2}):(\\d{1,2})(?::(\\d{1,2}(?:\\.\\d+)?))?)?$");

    /** The last year date can hold, and the last one timestamp can. */
    private static final long DATE_MAX_YEAR = 5874897L;
    private static final long TIMESTAMP_MAX_YEAR = 294276L;

    /**
     * A plain calendar date, optionally with a wall-clock time and a BC era marker, and nothing
     * else: no zone, no offset, no month name. Only a literal of exactly this shape is read by
     * {@link #parseCalendarLiteral}, so everything else keeps the parsing it already had.
     */
    private static final java.util.regex.Pattern CALENDAR_LITERAL =
            java.util.regex.Pattern.compile(
                    "^(\\d{4,7})-(\\d{1,2})-(\\d{1,2})"
                            + "(?:[ T](\\d{1,2}):(\\d{1,2})(?::(\\d{1,2})(\\.\\d+)?)?)?"
                            + "(?:\\s+(?i:BC))?$");

    /** The first moment either date or timestamp can hold: 4714-11-24 BC, Julian day zero. */
    private static final LocalDate CALENDAR_MIN = LocalDate.of(-4713, 11, 24);

    /** A calendar literal with a numeric UTC offset written after it. */
    private static final java.util.regex.Pattern TRAILING_OFFSET =
            java.util.regex.Pattern.compile("^(.*?)\\s*([+-]\\d{1,2}(?::?\\d{2})?)$");

    /**
     * Read a literal of the plain calendar shape, raising PostgreSQL's own errors for a field or a
     * year the type cannot hold, or null when the text is not of that shape at all.
     *
     * <p>Three things separate this from letting {@code LocalDateTime.parse} try: PostgreSQL names
     * a year outside the type's span {@code "<type> out of range"} and a bad month, day or clock
     * field {@code "date/time field value out of range"}, where the ISO parsers can only say the
     * spelling was wrong; it has no year zero; and it reads {@code 24:00:00} and a {@code :60}
     * second as the next day and the next minute rather than refusing them.
     *
     * @param outOfRangeNoun what PostgreSQL calls the type in its "out of range" message
     * @param maxYear the last year that type can hold
     */
    private static LocalDateTime parseCalendarLiteral(String s, String original,
                                                      String outOfRangeNoun, long maxYear) {
        java.util.regex.Matcher m = CALENDAR_LITERAL.matcher(s);
        if (!m.matches()) return null;
        boolean bc = s.toUpperCase().endsWith(" BC");
        long written = Long.parseLong(m.group(1));
        // A BC year is a proleptic one of 1 - the written year, so 1 BC is year 0 and 4714 BC
        // is -4713. Year zero is not a year at all in either era.
        if (written == 0) throw fieldOutOfRange(original);
        long year = bc ? 1 - written : written;
        int month = Integer.parseInt(m.group(2));
        int day = Integer.parseInt(m.group(3));
        int hour = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
        int minute = m.group(5) == null ? 0 : Integer.parseInt(m.group(5));
        int second = m.group(6) == null ? 0 : Integer.parseInt(m.group(6));
        long nanos = m.group(7) == null ? 0
                : new java.math.BigDecimal(m.group(7)).movePointRight(9).longValue();
        if (year > maxYear) throw outOfRange(outOfRangeNoun, original);
        // A month outside 1..12 and a day outside 1..31 are the two mistakes PostgreSQL suspects
        // of being a date written in another field order, and they are the only ones it offers the
        // DateStyle advice for. February the 30th is a real day of some other month, so it is
        // refused with the message alone, as is a year of zero and a clock field out of range.
        if (month < 1 || month > 12) throw misorderedField(original);
        LocalDate date;
        try {
            date = LocalDate.of((int) year, month, 1);
        } catch (RuntimeException e) {
            throw outOfRange(outOfRangeNoun, original);
        }
        if (day < 1 || day > 31) throw misorderedField(original);
        if (day > date.lengthOfMonth()) throw fieldOutOfRange(original);
        // PG reads 24:00:00 as the following midnight and a 60th second as the next minute, but
        // only when nothing finer is written past them.
        if (hour > 24 || (hour == 24 && (minute != 0 || second != 0 || nanos != 0))) {
            throw fieldOutOfRange(original);
        }
        if (minute > 59 || second > 60 || (second == 60 && nanos != 0)) throw fieldOutOfRange(original);
        LocalDateTime result = date.withDayOfMonth(day).atStartOfDay()
                .plusHours(hour).plusMinutes(minute).plusSeconds(second).plusNanos(nanos);
        if (result.toLocalDate().isBefore(CALENDAR_MIN) || result.getYear() > maxYear) {
            throw outOfRange(outOfRangeNoun, original);
        }
        return result;
    }

    /** The moment a timestamptz names has to fall inside the type's span read in UTC. */
    private static OffsetDateTime checkInstantRange(OffsetDateTime moment, String original) {
        OffsetDateTime utc = moment.withOffsetSameInstant(ZoneOffset.UTC);
        if (utc.getYear() > TIMESTAMP_MAX_YEAR || utc.toLocalDate().isBefore(CALENDAR_MIN)) {
            throw outOfRange("timestamp", original);
        }
        return moment;
    }

    private static MemgresException fieldOutOfRange(String original) {
        return new MemgresException(
                "date/time field value out of range: \"" + original + "\"", "22008");
    }

    /** The same, for a field a different field order would have read as a legal one. */
    private static MemgresException misorderedField(String original) {
        MemgresException ex = fieldOutOfRange(original);
        ex.setHint("Perhaps you need a different \"DateStyle\" setting.");
        return ex;
    }

    private static MemgresException outOfRange(String noun, String original) {
        return new MemgresException(noun + " out of range: \"" + original + "\"", "22008");
    }

    /**
     * A clock reading on its own, by the field rules {@link #parseCalendarLiteral} reads the clock
     * part of a timestamp by: an hour, a minute and optionally a second, each written with one
     * digit or two, and optionally a fraction of a second rounded to microseconds.
     *
     * <p>Null for anything that is no clock reading at all, and null for a reading whose fields
     * are out of range -- the caller has an answer of its own for both, and PostgreSQL tells the
     * two apart the same way.
     */
    private static LocalTime clockLiteral(String s) {
        java.util.regex.Matcher m = CLOCK_LITERAL.matcher(s);
        if (!m.matches()) return null;
        int hour = Integer.parseInt(m.group(1));
        int minute = Integer.parseInt(m.group(2));
        int second = m.group(3) == null ? 0 : Integer.parseInt(m.group(3));
        String fraction = m.group(4);
        long nanos = fraction == null || fraction.isEmpty() ? 0
                : new java.math.BigDecimal("0." + fraction).movePointRight(9).longValue();
        // The hour may reach 24 only when nothing follows it, and a 60th second is the next
        // minute -- the same two rules the clock part of a timestamp is read by.
        if (hour > 24 || (hour == 24 && (minute != 0 || second != 0 || nanos != 0))) return null;
        if (minute > 59 || second > 60 || (second == 60 && nanos != 0)) return null;
        long micros = ((hour * 60L + minute) * 60L + second) * 1_000_000L
                + Math.round(nanos / 1000.0);
        return micros >= 86_400_000_000L ? TIME_END_OF_DAY : LocalTime.ofNanoOfDay(micros * 1000L);
    }

    /** An hour, a minute, and optionally a second and a fraction of one. */
    private static final java.util.regex.Pattern CLOCK_LITERAL =
            java.util.regex.Pattern.compile("^(\\d{1,2}):(\\d{1,2})(?::(\\d{1,2})(?:\\.(\\d*))?)?$");

    public static LocalTime toLocalTime(Object val) {
        if (val instanceof LocalTime) return ((LocalTime) val);
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).toLocalTime();
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).toLocalTime();
        String s = val.toString().trim();
        // Handle special keywords
        if (s.equalsIgnoreCase("allballs")) return LocalTime.MIDNIGHT;
        if (s.equalsIgnoreCase("now")) return nowHere().toLocalTime();
        // 24:00:00 is the end of the day, and the only hour-24 time the type takes: 24:00:00.000001
        // is out of range. java.time stops a nanosecond short of it, so it is held as that.
        if (s.matches("24:00(:00(\\.0+)?)?")) return TIME_END_OF_DAY;
        try { return roundedToMicros(LocalTime.parse(s)); } catch (DateTimeParseException e) {
            // A field written with one digit names the same time as one written with two, and the
            // seconds may be left off: PostgreSQL reads '3:4' as three minutes past three. The
            // reader a timestamp's clock part already goes through takes that spelling, so a time
            // standing on its own is not read any more narrowly here.
            LocalTime clock = clockLiteral(s);
            if (clock != null) return clock;
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
                    try { return LocalTime.parse(timePart); } catch (DateTimeParseException e3) {
                        LocalTime written = clockLiteral(timePart);
                        if (written != null) return written;
                    }
                }
            }
            // Use 22008 for well-formatted but out-of-range times (e.g. 25:00:00); text that is
            // no time at all gets PG's 22007 wording, which names the type it would not read as.
            // A clock whose fields are written one digit wide is well formatted too, so a field
            // out of range in one is reported as out of range rather than as a spelling mistake.
            if (!s.matches("\\d{1,2}:\\d{2}(:\\d{2})?.*") && !s.matches(CLOCK_LITERAL.pattern())) {
                throw new MemgresException(
                        "invalid input syntax for type time: \"" + val + "\"", "22007");
            }
            throw new MemgresException("date/time field value out of range: \"" + val + "\"", "22008");
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
                timePart = pgTimeText(LocalTime.parse(timePart));
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
            String timePart = pgTimeText(ot.toLocalTime());
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
                    timePart = pgTimeText(LocalTime.parse(timePart));
                    return timePart + offsetPart;
                } catch (DateTimeParseException e3) { /* fall through */ }
            }
        }

        // The end of the day is a timetz as much as it is a time.
        if (s.matches("24:00(:00(\\.0+)?)?")) return "24:00:00+00";

        // Plain time without offset, default to +00 (UTC)
        try {
            LocalTime lt = LocalTime.parse(s);
            String timePart = lt.toString();
            if (timePart.length() == 5) timePart += ":00";
            return timePart + "+00";
        } catch (DateTimeParseException e) { /* fall through */ }

        // A clock written with one-digit fields, or with the seconds left off, names the same
        // reading as one written in full -- the reader a timestamp's clock part goes through takes
        // those spellings -- so writing it out in full is all a timetz needs to be read the same
        // way. Anything the readers above already took has been answered before this is reached.
        java.util.regex.Matcher loose = LOOSE_TIMETZ.matcher(s);
        if (loose.matches()) {
            LocalTime clock = clockLiteral(loose.group(1));
            String zone = loose.group(2) == null ? "" : loose.group(2);
            if (clock != null && !(toString(clock) + zone).equals(s)) {
                return toTimeTz(toString(clock) + zone);
            }
        }
        String errCode = s.matches("\\d{1,2}:\\d{2}(:\\d{2})?.*")
                || s.matches(CLOCK_LITERAL.pattern()) ? "22008" : "22007";
        throw new MemgresException("date/time field value out of range: \"" + val + "\"", errCode);
    }

    /** A clock reading in any of its spellings, with the offset a timetz may carry written after it. */
    private static final java.util.regex.Pattern LOOSE_TIMETZ =
            java.util.regex.Pattern.compile(
                    "^(\\d{1,2}:\\d{1,2}(?::\\d{1,2}(?:\\.\\d*)?)?)\\s*([+-].*)?$");

    /** True when the value is a timetz -- memgres holds one as its printed HH:MM:SS±TZ text. */
    public static boolean looksLikeTimeTz(Object val) {
        return val instanceof String && isTimeTzString(((String) val).trim());
    }

    /**
     * Rewrite a time against another zone, keeping the instant of day it names: a timetz is not
     * moved, only written against a different offset. A plain time carries no offset of its own,
     * so it takes the session's first, which is how PG reads {@code time AT TIME ZONE}.
     */
    public static String shiftTimeTzToZone(Object val, ZoneId zone) {
        java.time.OffsetTime source = val instanceof LocalTime
                ? ((LocalTime) val).atOffset(offsetOfZoneNow(sessionZone()))
                : parseTimeTzText(toTimeTz(val));
        return formatTimeTz(source.withOffsetSameInstant(offsetOfZoneNow(zone)));
    }

    /** A zone's offset as of the current statement; a named zone's offset moves with the date. */
    private static ZoneOffset offsetOfZoneNow(ZoneId zone) {
        return zone.getRules().getOffset(sessionInstant().toInstant());
    }

    /** Read back the HH:MM:SS±TZ text that {@link #toTimeTz} produces. */
    private static java.time.OffsetTime parseTimeTzText(String s) {
        int signIdx = -1;
        for (int i = s.length() - 1; i > 0; i--) {
            char c = s.charAt(i);
            if (c == '+' || c == '-') { signIdx = i; break; }
        }
        if (signIdx < 0) return LocalTime.parse(s).atOffset(ZoneOffset.UTC);
        return LocalTime.parse(s.substring(0, signIdx))
                .atOffset(ZoneOffset.of(s.substring(signIdx)));
    }

    /** Print a timetz the way PG does: seconds always, offset minutes only when they matter. */
    /**
     * A time of day as PostgreSQL writes it: the second always, and a fraction only to the digits
     * it has. Java's own toString drops a zero second and pads a fraction to a multiple of three,
     * so 03:04:05.5 came back as 03:04:05.500 and 03:04 lost its seconds.
     */
    static String pgTimeText(LocalTime lt) {
        StringBuilder sb = new StringBuilder(15);
        sb.append(String.format("%02d:%02d:%02d", lt.getHour(), lt.getMinute(), lt.getSecond()));
        if (lt.getNano() != 0) {
            String micros = String.format("%06d", lt.getNano() / 1000);
            int end = micros.length();
            while (end > 1 && micros.charAt(end - 1) == '0') end--;
            sb.append('.').append(micros, 0, end);
        }
        return sb.toString();
    }

    private static String formatTimeTz(java.time.OffsetTime ot) {
        String timePart = pgTimeText(ot.toLocalTime());
        int totalSeconds = ot.getOffset().getTotalSeconds();
        String sign = totalSeconds >= 0 ? "+" : "-";
        int absSeconds = Math.abs(totalSeconds);
        int hours = absSeconds / 3600;
        int minutes = (absSeconds % 3600) / 60;
        return timePart + sign + (minutes > 0
                ? String.format("%02d:%02d", hours, minutes)
                : String.format("%02d", hours));
    }

    /**
     * Check if a string looks like a timetz value (HH:MM:SS±offset).
     */
    private static final java.util.regex.Pattern TIMETZ_PATTERN =
            java.util.regex.Pattern.compile("^\\d{1,2}:\\d{2}(:\\d{2}(\\.\\d+)?)?[+-]\\d{2}(:\\d{2})?$");

    static boolean isTimeTzString(String s) {
        return TIMETZ_PATTERN.matcher(s).matches();
    }

    /**
     * Compare two timetz strings. PG compares by UTC-normalized time first,
     * then by zone offset (smaller offset = greater for ordering).
     * For equality, both UTC time AND zone must match.
     */
    static int compareTimeTz(String a, String b) {
        long utcA = timeTzToUtcNanos(a);
        long utcB = timeTzToUtcNanos(b);
        int cmp = Long.compare(utcA, utcB);
        if (cmp != 0) return cmp;
        // Same UTC time: compare by zone offset (PG sorts smaller offset as greater)
        int offA = timeTzOffsetSeconds(a);
        int offB = timeTzOffsetSeconds(b);
        return Integer.compare(offB, offA);
    }

    private static int timeTzOffsetSeconds(String s) {
        int idx = -1;
        for (int i = s.length() - 1; i >= 1; i--) {
            char c = s.charAt(i);
            if (c == '+' || c == '-') { idx = i; break; }
        }
        if (idx < 1) return 0;
        String offsetPart = s.substring(idx);
        int sign = offsetPart.charAt(0) == '-' ? -1 : 1;
        String offVal = offsetPart.substring(1);
        int offHours, offMinutes = 0;
        if (offVal.contains(":")) {
            String[] parts = offVal.split(":");
            offHours = Integer.parseInt(parts[0]);
            offMinutes = Integer.parseInt(parts[1]);
        } else {
            offHours = Integer.parseInt(offVal);
        }
        return sign * (offHours * 3600 + offMinutes * 60);
    }

    private static long timeTzToUtcNanos(String s) {
        // Parse time and offset, normalize to UTC
        int signIdx = s.lastIndexOf('+');
        if (signIdx < 1) signIdx = s.lastIndexOf('-');
        // Find the sign that's part of offset (not part of time)
        // The offset sign is after the time portion
        int idx = -1;
        for (int i = s.length() - 1; i >= 1; i--) {
            char c = s.charAt(i);
            if (c == '+' || c == '-') {
                idx = i;
                break;
            }
        }
        if (idx < 1) return 0;
        String timePart = s.substring(0, idx);
        String offsetPart = s.substring(idx);
        try {
            LocalTime lt = LocalTime.parse(timePart);
            // Parse offset: +HH or +HH:MM
            int sign = offsetPart.charAt(0) == '-' ? -1 : 1;
            String offVal = offsetPart.substring(1);
            int offHours, offMinutes = 0;
            if (offVal.contains(":")) {
                String[] parts = offVal.split(":");
                offHours = Integer.parseInt(parts[0]);
                offMinutes = Integer.parseInt(parts[1]);
            } else {
                offHours = Integer.parseInt(offVal);
            }
            long offsetNanos = sign * (offHours * 3600L + offMinutes * 60L) * 1_000_000_000L;
            return lt.toNanoOfDay() - offsetNanos;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * PostgreSQL's {@code time} runs to 24:00:00 inclusive — one value past the last instant of
     * the day, which it uses to mean the end of it. {@code java.time.LocalTime} stops one
     * nanosecond short of that, so {@link LocalTime#MAX} stands in for it here. Nothing else can
     * reach that value: a time is stored to microsecond precision, so 23:59:59.999999999 is not a
     * value any input produces — PostgreSQL rounds it up to 24:00:00, which is exactly what this
     * represents.
     */
    public static final LocalTime TIME_END_OF_DAY = LocalTime.MAX;

    /**
     * A time held to the microsecond, which is the resolution the type has. PostgreSQL rounds
     * rather than truncates, so 23:59:59.9999999 becomes 24:00:00 — the end-of-day value, and the
     * reason it is reachable at all.
     */
    public static LocalTime roundedToMicros(LocalTime time) {
        long nanos = time.toNanoOfDay();
        long micros = (nanos + 500L) / 1000L;
        return micros >= 86_400_000_000L ? TIME_END_OF_DAY : LocalTime.ofNanoOfDay(micros * 1000L);
    }

    /** True when this is the end-of-day time, which prints as 24:00:00 rather than as a clock. */
    public static boolean isEndOfDay(Object value) {
        return TIME_END_OF_DAY.equals(value);
    }

    /** Microseconds since midnight, counting the end-of-day value as a full day. */
    public static long timeMicros(LocalTime time) {
        return isEndOfDay(time) ? 86_400_000_000L : time.toNanoOfDay() / 1000L;
    }

    /** The time a microsecond count names, wrapping into the day as PostgreSQL does. */
    public static LocalTime timeOfMicros(long micros) {
        long day = 86_400_000_000L;
        long m = ((micros % day) + day) % day;
        return LocalTime.ofNanoOfDay(m * 1000L);
    }

    /**
     * Sentinel values for PostgreSQL's {@code infinity} and {@code -infinity}.
     *
     * <p>They sit outside every instant a timestamp can hold, so no value a user can write is one
     * of them. Standing them on representable instants — 9999-12-31 23:59:59 and 4713-01-01 BC —
     * meant infinity round-tripped as a finite instant, an ordinary timestamp compared equal to
     * it, and isfinite answered true for it.
     */
    public static final LocalDateTime TIMESTAMP_INFINITY = LocalDateTime.MAX;
    public static final LocalDateTime TIMESTAMP_NEG_INFINITY = LocalDateTime.MIN;
    /** date also has infinities, sharing the timestamp sentinels' day so comparisons agree. */
    public static final LocalDate DATE_INFINITY = LocalDate.MAX;
    public static final LocalDate DATE_NEG_INFINITY = LocalDate.MIN;

    /**
     * The span a PostgreSQL timestamp can hold: 4714-11-24 BC to 294276-12-31, which is narrower at
     * the top than {@code date}'s (5874897-12-31) because a timestamp spends its bits on the time
     * of day. Arithmetic that lands outside it has no representable answer, so PostgreSQL raises
     * 22008 rather than returning a value no server could store or send back.
     */
    private static final LocalDateTime TIMESTAMP_RANGE_MAX =
            LocalDateTime.of((int) TIMESTAMP_MAX_YEAR, 12, 31, 23, 59, 59, 999_999_000);
    private static final LocalDateTime TIMESTAMP_RANGE_MIN = LocalDateTime.of(-4713, 11, 24, 0, 0);

    /**
     * Refuse a timestamp outside the type's range. The two infinity sentinels are let through: they
     * stand for infinity rather than for the instant they happen to be spelled as, and
     * {@code -infinity} is deliberately stored below the true lower bound.
     */
    public static LocalDateTime requireTimestampInRange(LocalDateTime value) {
        if (value == null || value.equals(TIMESTAMP_INFINITY) || value.equals(TIMESTAMP_NEG_INFINITY)) {
            return value;
        }
        if (value.isAfter(TIMESTAMP_RANGE_MAX) || value.isBefore(TIMESTAMP_RANGE_MIN)) {
            throw new MemgresException("timestamp out of range", "22008");
        }
        return value;
    }

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
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val).atZoneSameInstant(sessionZone()).toLocalDateTime();
        String s = val.toString().trim();
        LocalDateTime calendar = parseCalendarLiteral(s, val.toString(), "timestamp",
                TIMESTAMP_MAX_YEAR);
        if (calendar != null) return calendar;
        // A BC era suffix means a proleptic year of 1 - the written year: 44 BC is ISO year -43
        if (endsWithEra(s)) {
            String body = stripEra(s);
            LocalDateTime bc = toLocalDateTime(body);
            return bc.withYear(1 - bc.getYear());
        }
        // Handle PG 'infinity' / '-infinity' special values
        if (s.equalsIgnoreCase("infinity")) return TIMESTAMP_INFINITY;
        if (s.equalsIgnoreCase("-infinity")) return TIMESTAMP_NEG_INFINITY;
        // Handle special keywords
        if (s.equalsIgnoreCase("epoch")) return LocalDateTime.of(1970, 1, 1, 0, 0, 0);
        if (s.equalsIgnoreCase("now")) return nowHere();
        if (s.equalsIgnoreCase("today")) return nowHere().toLocalDate().atStartOfDay();
        if (s.equalsIgnoreCase("yesterday")) return nowHere().toLocalDate().minusDays(1).atStartOfDay();
        if (s.equalsIgnoreCase("tomorrow")) return nowHere().toLocalDate().plusDays(1).atStartOfDay();
        // A `timestamp` (without time zone) literal may still carry a trailing zone name/
        // abbreviation (e.g. "2024-01-01 12:00:00 UTC"); PG parses and validates it but
        // otherwise ignores it — the wall-clock value is taken as-is.
        String[] trailingZone = extractTrailingZoneSuffix(s);
        if (trailingZone != null) {
            try {
                return trailingZone[0].contains("T")
                        ? LocalDateTime.parse(trailingZone[0])
                        : LocalDate.parse(trailingZone[0]).atStartOfDay();
            } catch (DateTimeParseException ignore) { /* fall through to normal handling below */ }
        }
        // Handle "YYYY-MM-DD HH:MM:SS" (space instead of T) — only replace the date-time separator
        s = replaceDateTimeSeparator(s);
        // Normalize timezone offset in case there's a space before it or compact +HHMM
        s = normalizeTimezoneOffset(s);
        try { return LocalDateTime.parse(s); } catch (DateTimeParseException e) { /* try more */ }
        // Try date-only
        try { return LocalDate.parse(s).atStartOfDay(); } catch (DateTimeParseException e) { /* try more */ }
        try { return OffsetDateTime.parse(s).toLocalDateTime(); } catch (Exception e) { /* ignore */ }
        // A date may name an offset with no time of day: '2001-01-01+02'. A timestamp without
        // time zone reads the offset and then discards it, the way it does a trailing zone name.
        java.util.regex.Matcher dateOffset = DATE_ONLY_OFFSET.matcher(val.toString().trim());
        if (dateOffset.matches()) {
            try {
                return LocalDate.parse(dateOffset.group(1)).atStartOfDay();
            } catch (DateTimeParseException ignore) { /* fall through to the error below */ }
        }
        // A timestamp holds years up to 294276; past that it is the range that fails rather than
        // the spelling, and PG says so with its own message
        java.util.regex.Matcher wideTs = WIDE_YEAR_TIMESTAMP.matcher(val.toString().trim());
        if (wideTs.matches()) {
            long year = Long.parseLong(wideTs.group(1));
            if (year > TIMESTAMP_MAX_YEAR) {
                throw new MemgresException("timestamp out of range: \"" + val + "\"", "22008");
            }
            try {
                LocalDate day = LocalDate.of((int) year, Integer.parseInt(wideTs.group(2)),
                        Integer.parseInt(wideTs.group(3)));
                if (wideTs.group(4) == null) return day.atStartOfDay();
                int hour = Integer.parseInt(wideTs.group(4));
                int minute = Integer.parseInt(wideTs.group(5));
                int second = 0;
                int nano = 0;
                if (wideTs.group(6) != null) {
                    java.math.BigDecimal secs = new java.math.BigDecimal(wideTs.group(6));
                    second = secs.intValue();
                    nano = secs.subtract(new java.math.BigDecimal(second))
                            .movePointRight(9).setScale(0, java.math.RoundingMode.HALF_UP).intValue();
                }
                return LocalDateTime.of(day, LocalTime.of(hour, minute, second, nano));
            } catch (RuntimeException e) {
                throw new MemgresException(
                        "date/time field value out of range: \"" + val + "\"", "22008");
            }
        }
        // Use 22008 for well-formatted but out-of-range timestamps
        String errCode = val.toString().trim().matches("\\d{4}-\\d{2}-\\d{2}.*") ? "22008" : "22007";
        throw new MemgresException("invalid input syntax for type timestamp: \"" + val + "\"", errCode);
    }

    /** Parse a value as timestamptz, interpreting any zoneless literal in the JVM's default zone. */
    public static OffsetDateTime toOffsetDateTime(Object val) {
        return toOffsetDateTime(val, ZoneId.systemDefault());
    }

    /**
     * Parse a value as timestamptz, interpreting any zoneless literal in {@code zone}
     * (the effective session TimeZone). A literal that carries its own explicit offset or
     * named zone suffix is always interpreted using that offset/zone instead, matching PG.
     */
    public static OffsetDateTime toOffsetDateTime(Object val, ZoneId zone) {
        if (val instanceof OffsetDateTime) return ((OffsetDateTime) val);
        if (val instanceof LocalDateTime) return ((LocalDateTime) val).atZone(zone).toOffsetDateTime();
        if (val instanceof LocalDate) return ((LocalDate) val).atStartOfDay(zone).toOffsetDateTime();
        String s = val.toString().trim();
        // A zoneless literal is a wall clock reading east or west of UTC, so its own year may run
        // one past the type's last: what has to fit is the moment it names, which is why
        // '294277-01-01 00:00:00' is a timestamptz at +01 and not at +00.
        LocalDateTime calendar = parseCalendarLiteral(s, val.toString(), "timestamp",
                TIMESTAMP_MAX_YEAR + 1);
        if (calendar != null) {
            return checkInstantRange(calendar.atZone(zone).toOffsetDateTime(), val.toString());
        }
        // The same reading with an offset written after it: the calendar part is checked exactly
        // as above and the offset put on unchanged. Only a literal that already names a time of
        // day is split here -- after a bare date a trailing "-05" is another date field, not an
        // offset, and DATE_ONLY_OFFSET below is what knows that rule.
        java.util.regex.Matcher off = TRAILING_OFFSET.matcher(s);
        if (off.matches() && off.group(1).indexOf(':') >= 0) {
            LocalDateTime zoned = parseCalendarLiteral(off.group(1), val.toString(), "timestamp",
                    TIMESTAMP_MAX_YEAR + 1);
            if (zoned != null) {
                try {
                    return checkInstantRange(zoned.atOffset(ZoneOffset.of(off.group(2))),
                            val.toString());
                } catch (java.time.DateTimeException ignore) {
                    /* not an offset after all — fall through to the parsers below */
                }
            }
        }
        // Handle special keywords
        if (s.equalsIgnoreCase("epoch")) return OffsetDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        if (s.equalsIgnoreCase("now")) return OffsetDateTime.now(zone);
        if (s.equalsIgnoreCase("today")) return LocalDate.now(zone).atStartOfDay(zone).toOffsetDateTime();
        if (s.equalsIgnoreCase("yesterday")) return LocalDate.now(zone).minusDays(1).atStartOfDay(zone).toOffsetDateTime();
        if (s.equalsIgnoreCase("tomorrow")) return LocalDate.now(zone).plusDays(1).atStartOfDay(zone).toOffsetDateTime();
        if (s.equalsIgnoreCase("infinity")) return OffsetDateTime.of(TIMESTAMP_INFINITY, ZoneOffset.UTC);
        if (s.equalsIgnoreCase("-infinity")) return OffsetDateTime.of(TIMESTAMP_NEG_INFINITY, ZoneOffset.UTC);
        // Try a named timezone region or abbreviation suffix, e.g. "2024-01-01 13:00:00 Europe/Amsterdam",
        // "2024-01-01 12:00 CET", or a date-only "2024-01-01 UTC" (no time-of-day at all).
        String[] trailingZone = extractTrailingZoneSuffix(s);
        if (trailingZone != null) {
            String dtPart = trailingZone[0];
            String tzName = trailingZone[1];
            try {
                LocalDateTime ldt = dtPart.contains("T") ? LocalDateTime.parse(dtPart) : LocalDate.parse(dtPart).atStartOfDay();
                // Try timezone abbreviation first
                String abbrevOffset = TZ_ABBREVIATIONS.get(tzName.toUpperCase());
                if (abbrevOffset != null) {
                    try {
                        return ldt.atOffset(ZoneOffset.of(abbrevOffset));
                    } catch (Exception ignore) { /* fall through */ }
                }
                // Try as ZoneId region
                try {
                    java.time.ZoneId namedZone = java.time.ZoneId.of(tzName);
                    return ldt.atZone(namedZone).toOffsetDateTime();
                } catch (Exception ignore) { /* fall through */ }
            } catch (DateTimeParseException ignore) { /* fall through */ }
        }
        // Replace only the date-time separator space, normalize offset format
        s = replaceDateTimeSeparator(s);
        s = normalizeTimezoneOffset(s);
        try { return OffsetDateTime.parse(s); } catch (DateTimeParseException e) { /* try more */ }
        try { return LocalDateTime.parse(s).atZone(zone).toOffsetDateTime(); } catch (DateTimeParseException e) { /* try more */ }
        try { return LocalDate.parse(s).atStartOfDay(zone).toOffsetDateTime(); } catch (DateTimeParseException e) { /* ignore */ }
        // A date may name its own offset with no time of day at all: '2001-01-01+02' is midnight
        // in +02. Written without a space a leading '-' would be another date field, so PG only
        // reads a negative offset when a space separates it.
        java.util.regex.Matcher dateOffset = DATE_ONLY_OFFSET.matcher(val.toString().trim());
        if (dateOffset.matches()) {
            String offsetText = dateOffset.group(2) != null ? dateOffset.group(2) : dateOffset.group(3);
            try {
                return LocalDate.parse(dateOffset.group(1)).atStartOfDay()
                        .atOffset(ZoneOffset.of(offsetText));
            } catch (RuntimeException ignore) { /* fall through to the error below */ }
        }
        // Use 22008 "out of range" for well-formatted but out-of-range dates (e.g., 2024-02-30);
        // garbage input gets 22007 with PG's "invalid input syntax" wording.
        if (val.toString().trim().matches("\\d{4}-\\d{2}-\\d{2}.*")) {
            // A whole date followed by something that is not a time of day is a syntax problem,
            // not a range one -- PG rejects '2001-01-01-05' with 22007.
            if (isValidDatePrefixWithTrailer(val.toString().trim())) {
                throw new MemgresException(
                        "invalid input syntax for type timestamp with time zone: \"" + val + "\"", "22007");
            }
            throw new MemgresException("date/time field value out of range: \"" + val + "\"", "22008");
        }
        throw new MemgresException("invalid input syntax for type timestamp with time zone: \"" + val + "\"", "22007");
    }

    /** A yyyy-MM-dd with an offset and no time of day; a bare '-' offset needs a space before it. */
    private static final java.util.regex.Pattern DATE_ONLY_OFFSET = java.util.regex.Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2})(?:\\s+([+-]\\d{1,2}(?::?\\d{2})?)|(\\+\\d{1,2}(?::?\\d{2})?))$");

    /** True when the leading ten characters are a real date and something else follows it. */
    private static boolean isValidDatePrefixWithTrailer(String s) {
        if (s.length() <= 10) return false;
        try {
            LocalDate.parse(s.substring(0, 10));
        } catch (DateTimeParseException e) {
            return false;
        }
        // Only an offset-shaped trailer: anything else keeps the range wording it had before
        return s.substring(10).matches("[+-]\\d{1,2}(?::?\\d{2})?");
    }

    /** Pattern for a date, optionally followed by a time-of-day, followed by a trailing zone name/abbreviation. */
    private static final java.util.regex.Pattern TRAILING_ZONE_SUFFIX_PATTERN = java.util.regex.Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2})(?:[T ](\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d+)?)?))?\\s+([A-Za-z][A-Za-z0-9_/+-]*)$");

    /**
     * If {@code s} is a date (optionally with a time-of-day) followed by a recognized trailing
     * zone name or abbreviation (e.g. "2024-01-01 UTC", "2024-01-01 12:00 CET",
     * "2024-01-01 12:00:00 Europe/Amsterdam"), returns a 2-element array of
     * {dtPart (date, or "date'T'time"), zoneName}. Returns {@code null} if there is no such
     * suffix, or the trailing token isn't a recognized zone abbreviation/region (so callers can
     * fall back to normal offset-based parsing, preserving prior error behavior for garbage input).
     */
    private static String[] extractTrailingZoneSuffix(String s) {
        java.util.regex.Matcher m = TRAILING_ZONE_SUFFIX_PATTERN.matcher(s);
        if (!m.matches()) return null;
        String datePart = m.group(1);
        String timePart = m.group(2);
        String tzName = m.group(3);
        boolean recognized = TZ_ABBREVIATIONS.containsKey(tzName.toUpperCase());
        if (!recognized) {
            try {
                java.time.ZoneId.of(tzName);
                recognized = true;
            } catch (Exception ignore) { /* not a recognized zone */ }
        }
        if (!recognized) return null;
        String dtPart = timePart != null ? (datePart + "T" + timePart) : datePart;
        return new String[]{dtPart, tzName};
    }

    public static PgInterval toInterval(Object val) {
        if (val instanceof PgInterval) return ((PgInterval) val);
        return PgInterval.parse(val.toString());
    }

    public static byte[] toBytea(Object val) {
        if (val instanceof byte[]) return (byte[]) val;
        // The other half of PG 18's integer/bytea pair: the bytes are the integer's big-endian
        // representation, two, four or eight of them by its width. These arrived here as null
        // before, so nothing that worked depended on the old answer.
        if (val instanceof Short) return bigEndian(((Short) val).longValue(), 2);
        if (val instanceof Integer) return bigEndian(((Integer) val).longValue(), 4);
        if (val instanceof Long) return bigEndian((Long) val, 8);
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

    /** An integer's PostgreSQL on-the-wire bytes: most significant first, fixed width. */
    private static byte[] bigEndian(long v, int width) {
        byte[] out = new byte[width];
        for (int i = width - 1; i >= 0; i--) {
            out[i] = (byte) (v & 0xff);
            v >>= 8;
        }
        return out;
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
    /**
     * Array column values are held as PG array literals. Sorting has to compare them
     * element-wise, as the array operators do, rather than lexicographically — otherwise
     * {1,2,3} sorts before {1} because ',' precedes '}'.
     */
    public static Object arrayForCompare(Object value) {
        if (!(value instanceof String)) return value;
        if (!PgArray.looksLikeArrayText((String) value)) return value;
        try {
            return PgArray.from(value);
        } catch (MemgresException e) {
            // Text that opens like an array but is not one stays the text it is.
            return value;
        }
    }

    /** The bounds an ordinary array has: one per dimension, each starting at 1. */
    private static int[] defaultBounds(List<?> array) {
        int[] bounds = new int[PgArray.dimensionsOf(array)];
        java.util.Arrays.fill(bounds, 1);
        return bounds;
    }

    /**
     * Two arrays compared as {@code array_cmp} compares them: element by element, with a null
     * element after every value, and — when every element agrees — the shorter array first.
     */
    private static int compareArrays(List<?> a, List<?> b) {
        int shared = Math.min(a.size(), b.size());
        for (int i = 0; i < shared; i++) {
            Object ea = a.get(i);
            Object eb = b.get(i);
            if (ea == null || eb == null) {
                // PostgreSQL sorts a null element after every value, the way NULLS LAST does.
                if (ea == null && eb == null) continue;
                return ea == null ? 1 : -1;
            }
            int cmp = compare(ea, eb);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.size(), b.size());
    }

    public static int compare(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        // bytea orders by unsigned byte value, as PG's byteacmp does
        if (a instanceof byte[] && b instanceof byte[]) {
            byte[] ba = (byte[]) a, bb = (byte[]) b;
            int n = Math.min(ba.length, bb.length);
            for (int i = 0; i < n; i++) {
                int cmp = Integer.compare(ba[i] & 0xFF, bb[i] & 0xFF);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(ba.length, bb.length);
        }

        // Arrays are compared element by element, as array_cmp does. Comparing the text of the
        // literal instead put {1,2,3} before {1}, because a comma sorts before a brace.
        if (a instanceof List<?> || b instanceof List<?>) {
            List<?> la = a instanceof List<?> ? (List<?>) a : null;
            List<?> lb = b instanceof List<?> ? (List<?>) b : null;
            if (la != null && lb != null) return compareArrays(la, lb);
        }

        // PgRow (record) comparison: element-by-element
        if (a instanceof AstExecutor.PgRow && b instanceof AstExecutor.PgRow) {
            List<Object> la = ((AstExecutor.PgRow) a).values;
            List<Object> lb = ((AstExecutor.PgRow) b).values;
            int minLen = Math.min(la.size(), lb.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = compare(la.get(i), lb.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(la.size(), lb.size());
        }

        // PgEnum: compare by ordinal when both are PgEnum, fall through to string otherwise
        if (a instanceof AstExecutor.PgEnum && b instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) a).compareTo(((AstExecutor.PgEnum) b));

        // PgMoney comparison
        if (a instanceof PgMoney && b instanceof PgMoney) return ((PgMoney) a).compareTo((PgMoney) b);
        if (a instanceof PgMoney) return compare(((PgMoney) a).getValue(), b);
        if (b instanceof PgMoney) return compare(a, ((PgMoney) b).getValue());

        // Both numbers: promote and compare
        if (a instanceof Number && b instanceof Number) {
            // Two whole numbers are compared as whole numbers. Promoting them to double first
            // lost the low bits of anything past 2^53, so two distinct bigints compared equal
            // and max() over a column of them answered the smaller one.
            if (isIntegral((Number) a) && isIntegral((Number) b)) {
                return toBigInteger((Number) a).compareTo(toBigInteger((Number) b));
            }
            // Handle NaN and Infinity for numeric type: NaN sorts as greatest (PG semantics)
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            if (Double.isNaN(da) || Double.isNaN(db) || Double.isInfinite(da) || Double.isInfinite(db)) {
                // Use Double.compare which handles NaN (greatest) and Infinity correctly
                return Double.compare(da, db);
            }
            if (a instanceof BigDecimal || b instanceof BigDecimal) {
                return toBigDecimal(a).compareTo(toBigDecimal(b));
            }
            // IEEE says a negative zero equals a positive one, and PostgreSQL's float operators
            // say so too; Double.compare puts them in an order they do not have.
            return da < db ? -1 : da > db ? 1 : 0;
        }

        // Date/time comparisons
        if (a instanceof LocalDate && b instanceof LocalDate) return ((LocalDate) a).compareTo((LocalDate) b);
        if (a instanceof LocalDateTime && b instanceof LocalDateTime) return ((LocalDateTime) a).compareTo((LocalDateTime) b);
        if (a instanceof OffsetDateTime && b instanceof OffsetDateTime) return ((OffsetDateTime) a).toInstant().compareTo(((OffsetDateTime) b).toInstant());
        if (a instanceof LocalTime && b instanceof LocalTime) return ((LocalTime) a).compareTo((LocalTime) b);
        if (a instanceof PgInterval && b instanceof PgInterval) return ((PgInterval) a).compareTo((PgInterval) b);

        // A tid is ordered by its block and then its slot. As text, (0,10) came before (0,9).
        if (a instanceof PgTid && b instanceof PgTid) return ((PgTid) a).compareTo((PgTid) b);

        // UUID comparisons
        if (a instanceof java.util.UUID && b instanceof java.util.UUID) {
            return compareUuids((java.util.UUID) a, (java.util.UUID) b);
        }
        if (a instanceof java.util.UUID && b instanceof String) {
            String sb = (String) b;
            java.util.UUID ua = (java.util.UUID) a;
            try { return compareUuids(ua, java.util.UUID.fromString(sb)); } catch (Exception e) { /* fall through */ }
        }
        if (b instanceof java.util.UUID && a instanceof String) {
            String sa = (String) a;
            java.util.UUID ub = (java.util.UUID) b;
            try { return compareUuids(java.util.UUID.fromString(sa), ub); } catch (Exception e) { /* fall through */ }
        }

        // Network type comparisons (InetValue, MacaddrValue, Macaddr8Value)
        if (a instanceof InetValue && b instanceof InetValue) return ((InetValue) a).compareTo((InetValue) b);
        if (a instanceof MacaddrValue && b instanceof MacaddrValue) return ((MacaddrValue) a).compareTo((MacaddrValue) b);
        if (a instanceof Macaddr8Value && b instanceof Macaddr8Value) return ((Macaddr8Value) a).compareTo((Macaddr8Value) b);

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

        // TimeTZ comparison: normalize to UTC before comparing
        if (a instanceof String && b instanceof String) {
            String sa = (String) a;
            String sb = (String) b;
            if (isTimeTzString(sa) && isTimeTzString(sb)) {
                return compareTimeTz(sa, sb);
            }
        }

        // Range/multirange comparison: compare by lower bound, then upper bound
        // Only trigger when BOTH sides are range-like (avoids false positives with tuple/array strings)
        if (a instanceof String && b instanceof String) {
            String sa = (String) a;
            String sb = (String) b;
            boolean aIsRange = RangeOperations.isRangeString(sa);
            boolean bIsRange = RangeOperations.isRangeString(sb);
            // Use strict isMultirangeString first; if at least one side is a range/multirange,
            // also accept "{}" as empty multirange on the other side
            boolean aIsMr = !aIsRange && RangeOperations.isMultirangeString(sa);
            boolean bIsMr = !bIsRange && RangeOperations.isMultirangeString(sb);
            boolean anyRangeLike = aIsRange || bIsRange || aIsMr || bIsMr;
            if (anyRangeLike) {
                // Accept "{}" as empty multirange when the other side is confirmed range/multirange
                if (!aIsRange && !aIsMr && sa.equals("{}")) aIsMr = true;
                if (!bIsRange && !bIsMr && sb.equals("{}")) bIsMr = true;
            }
            if ((aIsRange || aIsMr) && (bIsRange || bIsMr)) {
                return compareRangeOrMultirange(sa, aIsRange, sb, bIsRange);
            }
        }

        // Cross-type date/time: coerce to common type
        if (isDateTime(a) && isDateTime(b)) {
            return toLocalDateTime(a).compareTo(toLocalDateTime(b));
        }

        // Temporal value vs. an unknown-type text operand (e.g. a bound parameter/literal PG
        // hasn't resolved a concrete type for, such as jdbi's Instant -> setTimestamp -> pgjdbc
        // Oid.UNSPECIFIED text bind): PostgreSQL resolves the untyped text against the other
        // operand's type before comparing. Coerce the text side to the temporal type instead of
        // falling through to lexicographic string comparison below, which silently miscompares
        // ISO 'T'-separated temporal text against PG's ' '-separated format (ordering by ASCII
        // punctuation, not by instant) — every bound timestamptz range filter was silently
        // empty/wrong. Mirrors the UUID-vs-String special case above. Unparseable text raises the
        // same 22007/22008 errors the temporal coercion helpers already raise for bad input,
        // matching PostgreSQL's behavior for invalid literals.
        if (isDateTime(a) && b instanceof String) {
            return compareTemporalToText(a, (String) b);
        }
        if (isDateTime(b) && a instanceof String) {
            return -compareTemporalToText(b, (String) a);
        }

        // Number vs string: try numeric comparison
        if (a instanceof Number || b instanceof Number) {
            try {
                return Double.compare(toDouble(a), toDouble(b));
            } catch (Exception e) {
                // fall through
            }
        }

        // String comparison by codepoint ordering. Trailing blanks count: ignoring them is a
        // rule that belongs to bpchar alone, and applying it to every string made 'abc ' equal
        // to 'abc' and '' equal to ' '. The bpchar rule is applied where a bpchar is known.
        return pgStringCompare(a.toString(), b.toString());
    }

    /**
     * Default string comparison using binary/codepoint ordering.
     * This is the standard comparison used throughout the engine for WHERE clauses,
     * equality checks, ILIKE, SIMILAR TO, IN, etc.
     */
    static int pgStringCompare(String a, String b) {
        return a.compareTo(b);
    }

    /** Whether this number holds a whole value exactly, so it can be compared as one. */
    private static boolean isIntegral(Number n) {
        return n instanceof Integer || n instanceof Long || n instanceof Short
                || n instanceof Byte || n instanceof java.math.BigInteger;
    }

    private static java.math.BigInteger toBigInteger(Number n) {
        return n instanceof java.math.BigInteger
                ? (java.math.BigInteger) n
                : java.math.BigInteger.valueOf(n.longValue());
    }

    /**
     * UUIDs compare as the sixteen unsigned bytes they are. java.util.UUID compares its halves as
     * signed longs, so every UUID from 8000... on sorted below every UUID below it and the
     * greatest UUID of all compared less than the least.
     */
    private static int compareUuids(java.util.UUID a, java.util.UUID b) {
        int high = compareUnsignedLongs(a.getMostSignificantBits(), b.getMostSignificantBits());
        return high != 0 ? high
                : compareUnsignedLongs(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    }

    private static int compareUnsignedLongs(long a, long b) {
        return Long.compare(a + Long.MIN_VALUE, b + Long.MIN_VALUE);
    }

    /** Strip trailing space characters from a string (for CHAR(n) comparison semantics). */
    private static String stripTrailingSpaces(String s) {
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == ' ') end--;
        return end == s.length() ? s : s.substring(0, end);
    }

    /**
     * Locale-aware string comparison that emulates glibc strcoll() behavior
     * for en_US.UTF-8 where:
     * - Letters are compared case-insensitively at primary level
     * - Digits sort after letters at primary level
     * - Punctuation/symbols sort after digits
     * - Case is used as a secondary tiebreaker (lowercase before uppercase)
     * - Original codepoint is the final tiebreaker
     *
     * This should only be used when an explicit COLLATE clause is present.
     */
    static int pgLocaleAwareCompare(String a, String b) {
        int len = Math.min(a.length(), b.length());
        int caseTiebreaker = 0; // first case difference found (secondary level)
        for (int i = 0; i < len; i++) {
            char ca = a.charAt(i);
            char cb = b.charAt(i);
            if (ca == cb) continue;
            int wa = pgCharPrimaryWeight(ca);
            int wb = pgCharPrimaryWeight(cb);
            if (wa != wb) return Integer.compare(wa, wb);
            // Same primary weight but different characters: record case tiebreaker
            if (caseTiebreaker == 0) {
                // In en_US.UTF-8, lowercase sorts before uppercase
                caseTiebreaker = pgCharCaseWeight(ca) - pgCharCaseWeight(cb);
            }
        }
        int lenCmp = Integer.compare(a.length(), b.length());
        if (lenCmp != 0) return lenCmp;
        return caseTiebreaker;
    }

    /**
     * Compute the primary collation weight for a character that emulates PostgreSQL's
     * en_US.UTF-8 locale ordering: letters (case-insensitive) < digits < symbols/punctuation.
     * Letters are folded to lowercase so that 'a' and 'A' have the same primary weight.
     */
    private static int pgCharPrimaryWeight(char c) {
        if (Character.isLetter(c)) {
            // Fold to lowercase for case-insensitive primary comparison.
            return Character.toLowerCase(c);
        }
        if (Character.isDigit(c)) {
            // Digits sort after all letters: offset past the letter range.
            return 0x8000 + c;
        }
        // Non-alphanumeric (punctuation, symbols, whitespace): sort after all alphanumeric characters.
        return 0x10000 + c;
    }

    /**
     * Secondary (case) weight: lowercase sorts before uppercase in en_US.UTF-8.
     */
    private static int pgCharCaseWeight(char c) {
        if (Character.isLowerCase(c)) return 0;
        if (Character.isUpperCase(c)) return 1;
        return 2; // other (digits, symbols)
    }

    /**
     * Returns true if the given collation name represents a binary/C collation.
     */
    static boolean isBinaryCollation(String collation) {
        if (collation == null) return false;
        String lower = collation.toLowerCase().replace("\"", "");
        return lower.equals("c") || lower.equals("posix");
    }

    /**
     * Compare two strings using the specified collation.
     * For "C"/"POSIX" collations, uses binary (codepoint) ordering.
     * For locale-aware collations (en_US.utf8, default, etc.), uses pgStringCompare.
     */
    static int compareStringsWithCollation(String a, String b, String collation) {
        if (isBinaryCollation(collation)) {
            return a.compareTo(b);
        }
        return pgLocaleAwareCompare(a, b);
    }

    /**
     * Type-aware equality check.
     */
    /**
     * A value written so that two values the type calls equal write the same text.
     *
     * <p>Used where an identity has to be built out of text -- a group key, the pre-check that
     * decides whether a unique index can be built. {@code toString} is the Java rendering and
     * not the type's, so a numeric written 1.0 and the same value written 1.00 came out as two
     * different keys, and a byte array came out as its identity hash.
     */
    public static String keyText(Object value) {
        if (value == null) return "\0NULL\0";
        if (value instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) value).stripTrailingZeros().toPlainString();
        }
        if (value instanceof byte[]) return ByteaOperations.encodeHex((byte[]) value);
        if (value instanceof Number && !(value instanceof java.math.BigInteger)) {
            double d = ((Number) value).doubleValue();
            if (d == Math.rint(d) && !Double.isInfinite(d)) {
                return java.math.BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
            }
        }
        return toString(value);
    }

    public static boolean areEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        // Two arrays are equal when their elements are, however each element is spelled: comparing
        // the literal text made a numeric 1.0 differ from the same value written 1.00.
        if (a instanceof List<?> && b instanceof List<?>) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            if (la.size() != lb.size()) return false;
            for (int i = 0; i < la.size(); i++) {
                Object ea = la.get(i);
                Object eb = lb.get(i);
                if (ea == null || eb == null) {
                    if (ea != eb) return false;
                } else if (!areEqual(ea, eb)) {
                    return false;
                }
            }
            // An array states where its dimensions begin, and two arrays holding the same elements
            // at different subscripts are not the same array.
            int[] boundsA = a instanceof PgArray ? ((PgArray) a).lowerBounds() : null;
            int[] boundsB = b instanceof PgArray ? ((PgArray) b).lowerBounds() : null;
            return java.util.Arrays.equals(
                    boundsA == null ? defaultBounds(la) : boundsA,
                    boundsB == null ? defaultBounds(lb) : boundsB);
        }
        if (a.equals(b) || b.equals(a)) return true;
        // A shape is equal to another by the rule its own type has, which is not "the same
        // characters": two circles of one radius are equal wherever they sit, and two lines are
        // equal whenever one's coefficients are the other's scaled.
        if (a instanceof String && b instanceof String) {
            Boolean shapes = GeometricOperations.equalShapes((String) a, (String) b);
            if (shapes != null) return shapes.booleanValue();
        }
        // bytea: compare the bytes, not the array identity
        if (a instanceof byte[] && b instanceof byte[]) {
            return java.util.Arrays.equals((byte[]) a, (byte[]) b);
        }
        // TimeTZ strings: compare by UTC-normalized time
        if (a instanceof String && b instanceof String && isTimeTzString((String) a) && isTimeTzString((String) b)) {
            return compareTimeTz((String) a, (String) b) == 0;
        }
        // PgEnum comparison: compare by label string
        if (a instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) a).label().equals(b instanceof AstExecutor.PgEnum ? ((AstExecutor.PgEnum) b).label() : b.toString());
        if (b instanceof AstExecutor.PgEnum) return ((AstExecutor.PgEnum) b).label().equals(a.toString());

        // Number comparison
        if (a instanceof Number && b instanceof Number) {
            // Two whole numbers are equal only if they are the same whole number; going through
            // double first made every pair past 2^53 that shared a mantissa compare equal.
            if (isIntegral((Number) a) && isIntegral((Number) b)) {
                return toBigInteger((Number) a).equals(toBigInteger((Number) b));
            }
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

        // Fall back to string comparison, blanks and all, so that = and < agree with each other
        // and with what PostgreSQL answers for text and varchar.
        return a.toString().equals(b.toString());
    }

    private static boolean isDateTime(Object val) {
        return val instanceof LocalDate || val instanceof LocalDateTime ||
               val instanceof OffsetDateTime || val instanceof LocalTime ||
               val instanceof PgInterval;
    }

    /**
     * Compares a temporal value against a text operand by coercing the text to the same
     * temporal type first (see the call site in {@link #compare(Object, Object)} for rationale).
     * {@code OffsetDateTime} compares by instant (matching timestamptz semantics: two
     * differently-offset representations of the same instant are equal).
     */
    private static int compareTemporalToText(Object temporal, String text) {
        if (temporal instanceof OffsetDateTime) {
            return ((OffsetDateTime) temporal).toInstant().compareTo(toOffsetDateTime(text).toInstant());
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).compareTo(toLocalDateTime(text));
        }
        if (temporal instanceof LocalDate) {
            return ((LocalDate) temporal).compareTo(toLocalDate(text));
        }
        if (temporal instanceof LocalTime) {
            return ((LocalTime) temporal).compareTo(toLocalTime(text));
        }
        if (temporal instanceof PgInterval) {
            return ((PgInterval) temporal).compareTo(toInterval(text));
        }
        throw new IllegalStateException("unreachable: isDateTime guarded the caller");
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
        if (value instanceof InetValue && !(value instanceof CidrValue)) return DataType.INET;
        if (value instanceof CidrValue) return DataType.CIDR;
        if (value instanceof MacaddrValue) return DataType.MACADDR;
        if (value instanceof PgTid) return DataType.TID;
        if (value instanceof Macaddr8Value) return DataType.MACADDR8;
        if (value instanceof byte[]) return DataType.BYTEA;
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

    /**
     * Compare two range or multirange strings.
     * PG ordering: empty < non-empty; then by lower bound, then by upper bound.
     * For multiranges, compare element-by-element (first sub-range, then second, etc.).
     */
    private static int compareRangeOrMultirange(String sa, boolean aIsRange, String sb, boolean bIsRange) {
        // Convert both to lists of PgRange for uniform comparison
        java.util.List<RangeOperations.PgRange> aRanges;
        java.util.List<RangeOperations.PgRange> bRanges;
        if (aIsRange) {
            RangeOperations.PgRange r = RangeOperations.parse(sa);
            aRanges = r.isEmpty() ? java.util.Collections.emptyList() : java.util.Collections.singletonList(r);
        } else {
            aRanges = RangeOperations.parseMultirange(sa);
        }
        if (bIsRange) {
            RangeOperations.PgRange r = RangeOperations.parse(sb);
            bRanges = r.isEmpty() ? java.util.Collections.emptyList() : java.util.Collections.singletonList(r);
        } else {
            bRanges = RangeOperations.parseMultirange(sb);
        }
        // Compare element by element
        int minLen = Math.min(aRanges.size(), bRanges.size());
        for (int i = 0; i < minLen; i++) {
            int cmp = compareSingleRange(aRanges.get(i), bRanges.get(i));
            if (cmp != 0) return cmp;
        }
        return Integer.compare(aRanges.size(), bRanges.size());
    }

    /** Compare two individual PgRange values. PG: by lower bound, then upper bound. */
    private static int compareSingleRange(RangeOperations.PgRange a, RangeOperations.PgRange b) {
        if (a.isEmpty() && b.isEmpty()) return 0;
        if (a.isEmpty()) return -1;
        if (b.isEmpty()) return 1;
        // Compare lower bounds
        int cmp = compareBound(a.lower(), a.lowerInclusive(), b.lower(), b.lowerInclusive(), true);
        if (cmp != 0) return cmp;
        // Compare upper bounds
        return compareBound(a.upper(), a.upperInclusive(), b.upper(), b.upperInclusive(), false);
    }

    /** Compare two range bounds. null = unbounded (negative infinity for lower, positive infinity for upper). */
    private static int compareBound(Number a, boolean aInc, Number b, boolean bInc, boolean isLower) {
        if (a == null && b == null) return 0;
        if (a == null) return isLower ? -1 : 1;  // unbounded lower is smallest, unbounded upper is largest
        if (b == null) return isLower ? 1 : -1;
        // The bound is compared as the number it is. Narrowing it to a long first made every
        // fractional numrange bound tie with its neighbours, so 1.2, 1.5 and 1.9 all sorted equal.
        int cmp = toBigDecimal(a).compareTo(toBigDecimal(b));
        if (cmp != 0) return cmp;
        // Same value: inclusive vs exclusive matters
        // For lower: inclusive < exclusive (inclusive starts earlier)
        // For upper: exclusive < inclusive (exclusive ends earlier)
        if (aInc == bInc) return 0;
        if (isLower) return aInc ? -1 : 1;
        return aInc ? 1 : -1;
    }

    /**
     * Hold a date/time value to a declared fractional-seconds precision. PostgreSQL rounds to
     * that precision rather than dropping the digits past it, so {@code time(0)} turns
     * 01:02:03.987 into 01:02:04 and not 01:02:03. A precision of 6 or more leaves microsecond
     * values untouched, and anything that is not a time-of-day or timestamp is left alone.
     */
    public static Object roundTemporal(Object value, int precision) {
        if (value == null || precision < 0 || precision >= 9) return value;
        long unit = 1L;
        for (int i = 0; i < 9 - precision; i++) unit *= 10L;
        if (value instanceof LocalTime) {
            return roundNanoOfDay((LocalTime) value, unit);
        }
        if (value instanceof OffsetTime) {
            OffsetTime t = (OffsetTime) value;
            return t.with(roundNanoOfDay(t.toLocalTime(), unit));
        }
        if (value instanceof LocalDateTime) {
            LocalDateTime t = (LocalDateTime) value;
            return roundDateTime(t, unit);
        }
        if (value instanceof OffsetDateTime) {
            OffsetDateTime t = (OffsetDateTime) value;
            return t.with(roundDateTime(t.toLocalDateTime(), unit));
        }
        if (value instanceof java.sql.Timestamp) {
            java.sql.Timestamp ts = (java.sql.Timestamp) value;
            return java.sql.Timestamp.valueOf(roundDateTime(ts.toLocalDateTime(), unit));
        }
        return value;
    }

    private static LocalTime roundNanoOfDay(LocalTime t, long unit) {
        long nanos = t.toNanoOfDay();
        long rounded = ((nanos + unit / 2) / unit) * unit;
        // Rounding up out of the day wraps, which is what PostgreSQL's 24:00:00 boundary does
        return LocalTime.ofNanoOfDay(rounded % 86400000000000L);
    }

    private static LocalDateTime roundDateTime(LocalDateTime t, long unit) {
        long nanos = t.getNano();
        long rounded = ((nanos + unit / 2) / unit) * unit;
        LocalDateTime base = t.withNano(0);
        if (rounded >= 1000000000L) return base.plusSeconds(1);
        return base.withNano((int) rounded);
    }
}
