package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;

import java.util.*;

/**
 * Handles type casting (::type, CAST(x AS type)).
 * Extracted from AstExecutor to separate type coercion concerns.
 */
class CastEvaluator {

    /**
     * What a value that is neither a scalar nor a string is called in a cast rejection: a row is a
     * {@code record}, and a collection of rows a {@code record[]}, which is what the ordering
     * column of a SEARCH DEPTH FIRST clause holds. Anything else collected is an {@code array}.
     */
    private static String compositeSourceName(Object val) {
        if (val instanceof AstExecutor.PgRow) return "record";
        Object first = null;
        if (val instanceof java.util.List<?>) {
            java.util.List<?> list = (java.util.List<?>) val;
            if (!list.isEmpty()) first = list.get(0);
        } else if (val instanceof Object[]) {
            Object[] arr = (Object[]) val;
            if (arr.length > 0) first = arr[0];
        }
        return first instanceof AstExecutor.PgRow ? "record[]" : "array";
    }

    /** The name PostgreSQL puts in a message for an integer type, whichever alias was written. */
    private static String integerTypeDisplayName(String lowerSpec) {
        if (lowerSpec.equals("int") || lowerSpec.equals("int4")) return "integer";
        if (lowerSpec.equals("int8")) return "bigint";
        if (lowerSpec.equals("int2")) return "smallint";
        return lowerSpec;
    }

    private final AstExecutor executor;

    /** Reads pg_proc for the regprocedure cast; built once because it holds only the executor. */
    private CatalogMetadataFunctions procCatalog;

    private CatalogMetadataFunctions procCatalog() {
        if (procCatalog == null) procCatalog = new CatalogMetadataFunctions(executor);
        return procCatalog;
    }

    /**
     * The name PostgreSQL prints for a type OID, or null when nothing here carries that OID.
     *
     * <p>Derived from the same lists pg_type is built from rather than from a second table of
     * names: a type that has a pg_type row but no name here renders as its OID, which is what
     * made {@code castsource::regtype::text} answer "18" where PostgreSQL answers "char".
     */
    private String typeNameForOid(int oid) {
        String known = OID_TO_TYPE.get(oid);
        if (known != null) return known;
        DataType dt = DataType.fromOid(oid);
        if (dt != null) {
            DataType elem = DataType.elementOf(dt);
            return elem != null ? regtypeDisplay(elem) + "[]" : regtypeDisplay(dt);
        }
        int elemOid = CatalogCoreBuilder.arrayElementOid(oid);
        if (elemOid != 0) {
            String elemName = typeNameForOid(elemOid);
            if (elemName != null) return elemName + "[]";
        }
        String builtin = PgInternalTypes.nameForOid(oid);
        if (builtin != null) return builtin;
        String other = CatalogCoreBuilder.otherTypeName(oid);
        if (other != null) return other;
        // A user type (enum, domain, composite) is named in the catalog's OID map.
        String key = executor.systemCatalog.keyForOid(oid);
        if (key != null && key.startsWith("type:")) return key.substring(5);
        return null;
    }

    /** PostgreSQL spells its single-byte flag type with the quotes; everything else as itself. */
    private static String regtypeDisplay(DataType dt) {
        return dt == DataType.INTERNAL_CHAR ? "\"char\"" : dt.toRegtypeDisplay();
    }

    /**
     * The function name an OID was handed out for, or null. Overloads are keyed with a suffix
     * ({@code proc:max#agg3}); the name is what regproc prints, so the suffix is dropped.
     */
    private String procNameForOid(int oid) {
        String key = executor.systemCatalog.keyForOid(oid);
        if (key == null || !key.startsWith("proc:")) return null;
        String name = key.substring(5);
        int suffix = name.indexOf('#');
        return suffix > 0 ? name.substring(0, suffix) : name;
    }

    /** Maps PG OIDs to their canonical type names (used by ::regtype casts). */
    private static final Map<Integer, String> OID_TO_TYPE;

    static {
        // Filled in one entry at a time rather than through a long varargs call:
        // inferring the key and value types across forty-odd nested generic calls
        // is more than some compilers will do, and none of it is needed here.
        Map<Integer, String> oids = new LinkedHashMap<>();
        oids.put(16, "boolean");
        oids.put(17, "bytea");
        oids.put(20, "bigint");
        oids.put(21, "smallint");
        oids.put(23, "integer");
        oids.put(25, "text");
        oids.put(26, "oid");
        oids.put(114, "json");
        oids.put(142, "xml");
        oids.put(700, "real");
        oids.put(701, "double precision");
        oids.put(869, "inet");
        oids.put(650, "cidr");
        oids.put(829, "macaddr");
        oids.put(774, "macaddr8");
        oids.put(790, "money");
        oids.put(1042, "character");
        oids.put(1043, "character varying");
        oids.put(1082, "date");
        oids.put(1083, "time without time zone");
        oids.put(1114, "timestamp without time zone");
        oids.put(1184, "timestamp with time zone");
        oids.put(1186, "interval");
        oids.put(1560, "bit");
        oids.put(1562, "bit varying");
        oids.put(1700, "numeric");
        oids.put(2950, "uuid");
        oids.put(3802, "jsonb");
        oids.put(600, "point");
        oids.put(601, "lseg");
        oids.put(602, "path");
        oids.put(603, "box");
        oids.put(604, "polygon");
        oids.put(628, "line");
        oids.put(718, "circle");
        oids.put(3614, "tsvector");
        oids.put(3615, "tsquery");
        oids.put(1007, "integer[]");
        oids.put(1009, "text[]");
        oids.put(1016, "bigint[]");
        oids.put(1000, "boolean[]");
        for (String polyName : PolymorphicTypes.names()) {
            oids.put(PolymorphicTypes.oid(polyName), polyName);
        }
        OID_TO_TYPE = Collections.unmodifiableMap(oids);
    }

    /**
     * Parse a UUID the way PostgreSQL does: exactly 32 hex digits, optionally wrapped in braces
     * and with hyphens anywhere between digits. Java's own parser is looser in one direction and
     * stricter in the other — it pads a short group instead of rejecting it, which turns a
     * mistyped identifier into a different valid one, and it refuses the undashed form PG takes.
     */
    private static java.util.UUID parseUuid(String raw) {
        String text = raw.trim();
        String body = text;
        if (body.length() >= 2 && body.charAt(0) == '{' && body.charAt(body.length() - 1) == '}') {
            body = body.substring(1, body.length() - 1);
        }
        StringBuilder digits = new StringBuilder(32);
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '-') continue;
            boolean hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!hex) throw invalidUuid(raw);
            digits.append(c);
        }
        if (digits.length() != 32) throw invalidUuid(raw);
        String d = digits.toString();
        return java.util.UUID.fromString(d.substring(0, 8) + "-" + d.substring(8, 12) + "-"
                + d.substring(12, 16) + "-" + d.substring(16, 20) + "-" + d.substring(20));
    }

    /**
     * Bit-string input may carry a radix prefix: {@code b} for binary and {@code x} for
     * hexadecimal, either case. Without expanding it the prefix letter reaches the digit check
     * and the whole literal is refused.
     */
    private static String expandBitRadixPrefix(String text) {
        if (text.length() < 1) return text;
        char prefix = text.charAt(0);
        String rest = text.substring(1);
        if (prefix == 'b' || prefix == 'B') {
            return rest;
        }
        if (prefix == 'x' || prefix == 'X') {
            StringBuilder bits = new StringBuilder(rest.length() * 4);
            for (int i = 0; i < rest.length(); i++) {
                int digit = Character.digit(rest.charAt(i), 16);
                if (digit < 0) return text; // let the digit check report it
                for (int bit = 3; bit >= 0; bit--) {
                    bits.append((digit >> bit) & 1);
                }
            }
            return bits.toString();
        }
        return text;
    }

    /** Bits a bit string may declare: one attribute's worth (PG's MaxAttrSize, in bits). */
    private static final long MAX_BIT_LENGTH = 10485760L * 8;

    /**
     * A bit string is stored in a single attribute, so PostgreSQL bounds the length modifier as
     * it reads it rather than when the value is built. memgres holds the bits in an ordinary
     * string, so an unbounded modifier is an unbounded allocation.
     */
    private static void checkBitTypmod(String typeName, String lowerSpec) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\((-?\\d+)\\)").matcher(lowerSpec);
        if (!m.find()) return;
        // PG names the type by its internal spelling, whichever syntax was written
        String reported = typeName.equals("bit") ? "bit" : "varbit";
        java.math.BigInteger written = new java.math.BigInteger(m.group(1));
        // A type modifier is an int4, so one too wide fails as a bad integer before it is a
        // length at all
        if (written.bitLength() > 31) {
            throw new MemgresException("value \"" + m.group(1)
                    + "\" is out of range for type integer", "22003");
        }
        long n = written.longValue();
        if (n < 1) {
            throw new MemgresException("length for type " + reported + " must be at least 1", "22023");
        }
        if (n > MAX_BIT_LENGTH) {
            throw new MemgresException("length for type " + reported + " cannot exceed "
                    + MAX_BIT_LENGTH, "22023");
        }
    }

    /**
     * An integer range's bounds must be whole numbers within the element type's span. Without
     * this the bound is narrowed on the way in, producing a finite range with completely
     * different endpoints and nothing to say the input was not representable.
     */
    private static void checkRangeBoundsFitElementType(String literal, String rangeType) {
        String element;
        if ("int4range".equals(rangeType)) element = "integer";
        else if ("int8range".equals(rangeType)) element = "bigint";
        else return;
        String text = literal.trim();
        if (text.equalsIgnoreCase("empty")) return;
        // Text that is not a two-bound literal is malformed rather than out of range; leave the
        // range parser to say so, in the words PG uses for it.
        String[] parts = RangeOperations.boundTexts(text);
        if (parts == null) return;
        for (String part : parts) {
            String bound = part.trim();
            if (bound.isEmpty()) continue;
            java.math.BigInteger value;
            try {
                value = new java.math.BigInteger(bound);
            } catch (NumberFormatException e) {
                throw new MemgresException(
                        "invalid input syntax for type " + element + ": \"" + bound + "\"", "22P02");
            }
            java.math.BigInteger min = "integer".equals(element)
                    ? java.math.BigInteger.valueOf(Integer.MIN_VALUE)
                    : java.math.BigInteger.valueOf(Long.MIN_VALUE);
            java.math.BigInteger max = "integer".equals(element)
                    ? java.math.BigInteger.valueOf(Integer.MAX_VALUE)
                    : java.math.BigInteger.valueOf(Long.MAX_VALUE);
            if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                throw new MemgresException("value \"" + bound + "\" is out of range for type "
                        + element, "22003");
            }
        }
    }

    private static MemgresException invalidUuid(String raw) {
        return new MemgresException("invalid input syntax for type uuid: \"" + raw + "\"", "22P02");
    }

    CastEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    /** True when this domain, or any domain it is built on, is declared NOT NULL. */
    private boolean domainChainRejectsNull(DomainType domain) {
        DomainType d = domain;
        for (int guard = 0; d != null && guard < 64; guard++) {
            if (d.isNotNull()) return true;
            String base = d.getBaseTypeName();
            d = base == null ? null : executor.database.getDomain(base);
        }
        return false;
    }

    /**
     * The zone a zoneless timestamptz literal is interpreted in: the session TimeZone, always.
     * It has to be the same zone the value is later read back in, or a literal written as
     * midnight comes back as the previous day — and the session is told over ParameterStatus
     * which zone that is, so anything else would be a lie to the client.
     */
    private java.time.ZoneId sessionInterpretationZone() {
        return TypeCoercion.sessionZone();
    }

    private int extraFloatDigits() {
        if (executor.session == null) return 1;
        try {
            String v = executor.session.getGucSettings().get("extra_float_digits");
            return v == null ? 1 : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    /** The types whose value memgres keeps exactly as it was written; see {@link #applyCast}. */
    private static final java.util.Set<String> KEPT_AS_WRITTEN =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "regcollation", "aclitem", "pg_snapshot", "txid_snapshot", "cid",
                    "cstring", "unknown"));

    /** Whether a type of this name was declared here, in which case it is not PostgreSQL's. */
    private boolean isDeclaredByThisDatabase(String typeName) {
        if (executor.database == null) return false;
        return executor.database.getDomain(typeName) != null
                || executor.database.getCustomEnum(typeName) != null
                || executor.database.getRowType(typeName) != null;
    }

    Object applyCast(Object val, String typeSpec) {
        return applyCast(val, typeSpec, false);
    }

    /**
     * @param fromUnknownLiteral the value was written as a bare quoted literal, so PG reads it with
     *        the target type's input function rather than converting a value of a known type.
     */
    Object applyCast(Object val, String typeSpec, boolean fromUnknownLiteral) {
        if (val == null) {
            // A NOT NULL domain rejects null even through a cast, and the constraint is
            // inherited from every domain it is built on
            DomainType nullDomain = executor.database.getDomain(
                    typeSpec.toLowerCase().replaceAll("\\(.*\\)", "").trim());
            if (nullDomain != null && domainChainRejectsNull(nullDomain)) {
                throw new MemgresException(
                        "domain " + nullDomain.getName() + " does not allow null values", "23502");
            }
            return null;
        }
        // The word "null" is not input for any type: PG reads it as text and its input function
        // refuses it. Treating it as SQL NULL turned a malformed value into a missing one.
        String lowerSpec = typeSpec.toLowerCase().trim();

        // A polymorphic pseudo-type is a placeholder, not a target: the value keeps whatever
        // concrete type the caller passed in.
        if (PolymorphicTypes.isPolymorphic(lowerSpec)) return val;

        // One of information_schema's own domains, which keeps its qualifier because it answers to
        // nothing else. The value becomes the type underneath and is then judged by the domain.
        if (InformationSchemaTypes.isOne(lowerSpec)) {
            DataType base = InformationSchemaTypes.baseTypeOf(lowerSpec);
            return InformationSchemaTypes.check(lowerSpec,
                    applyCast(val, base.getPgName(), fromUnknownLiteral));
        }

        // Reject impossible casts (PG raises 42846 "cannot cast type X to Y")
        if (lowerSpec.equals("uuid")) {
            if (val instanceof Number || val instanceof Boolean) {
                String srcType = val instanceof Integer ? "integer" : val instanceof Long ? "bigint" :
                        val instanceof Boolean ? "boolean" : val.getClass().getSimpleName().toLowerCase();
                throw new MemgresException("cannot cast type " + srcType + " to uuid", "42846");
            }
        }
        if (lowerSpec.equals("integer") || lowerSpec.equals("int") || lowerSpec.equals("int4")
                || lowerSpec.equals("bigint") || lowerSpec.equals("int8")
                || lowerSpec.equals("smallint") || lowerSpec.equals("int2")) {
            if (val instanceof java.util.List<?> || val instanceof Object[]
                    || val instanceof AstExecutor.PgRow) {
                throw new MemgresException("cannot cast type " + compositeSourceName(val)
                        + " to " + integerTypeDisplayName(lowerSpec), "42846");
            }
            // Geometric types cannot be cast to integer
            if (val instanceof String && GeometricOperations.isGeometricString(((String) val).trim())) {
                String sv = (String) val;
                throw new MemgresException("cannot cast type point to " + lowerSpec, "42846");
            }
        }

        TypeCoercion.checkDeclaredTypeLimits(lowerSpec);
        // Handle float(p): p <= 24 means REAL, p >= 25 means DOUBLE PRECISION
        if (lowerSpec.startsWith("float(")) {
            String pStr = lowerSpec.replaceAll(".*\\((\\d+)\\).*", "$1");
            int p = Integer.parseInt(pStr);
            if (p <= 24) return TypeCoercion.toFloat(val);
            else return TypeCoercion.toDouble(val);
        }
        // Handle numeric(precision, scale) and apply scale for proper formatting
        if (lowerSpec.startsWith("numeric(") || lowerSpec.startsWith("decimal(")) {
            // A precision outside 1..1000 is not a field numeric could ever have, and PG says
            // so before it looks at the value
            TypeCoercion.checkDeclaredTypeLimits(lowerSpec);
            Double specialTypmod = NumericLimits.specialNumericOrNull(val);
            String params = lowerSpec.replaceAll(".*\\(([^)]+)\\).*", "$1");
            String[] parts = params.split(",");
            int precision = Integer.parseInt(parts[0].trim());
            int scale = parts.length >= 2 ? Integer.parseInt(parts[1].trim()) : 0;
            if (specialTypmod != null) {
                // NaN fits any numeric(p,s); an infinity is larger than every value the field
                // could round to, so PG reports the same overflow it does for a huge number.
                if (specialTypmod.isNaN()) return specialTypmod;
                TypeCoercion.rejectSpecialForTypmod(specialTypmod.doubleValue(), precision, scale);
            }
            java.math.BigDecimal bd = NumericLimits.check(TypeCoercion.toBigDecimal(val));
            java.math.BigDecimal rounded = bd.setScale(scale, java.math.RoundingMode.HALF_UP);
            // PG checks the typmod after rounding: 99.995 rounds to 100.00, which no longer
            // fits numeric(4,2), so it overflows rather than silently widening
            TypeCoercion.checkNumericTypmod(rounded, precision, scale);
            return rounded;
        }
        // Handle varchar(n) by truncating to length
        if (lowerSpec.startsWith("varchar(") || lowerSpec.startsWith("character varying(")) {
            String nStr = lowerSpec.replaceAll(".*\\((\\d+)\\).*", "$1");
            int n = Integer.parseInt(nStr);
            String s = val.toString();
            return s.length() > n ? s.substring(0, n) : s;
        }
        // Handle char(n) by truncating or padding with spaces
        if ((lowerSpec.startsWith("char(") || lowerSpec.startsWith("character(") || lowerSpec.startsWith("bpchar("))
                && !lowerSpec.startsWith("character varying")) {
            String nStr = lowerSpec.replaceAll(".*\\((\\d+)\\).*", "$1");
            int n = Integer.parseInt(nStr);
            String s = val.toString();
            if (s.length() > n) return s.substring(0, n);
            return String.format("%-" + n + "s", s);
        }
        // A date/time type's precision is a fractional-seconds precision, and it rounds rather
        // than truncates: '01:02:03.987'::time(0) is 01:02:04. The value has to be read as its
        // base type first, so the precision is applied to what comes back.
        Integer timePrecision = temporalPrecision(lowerSpec);
        if (timePrecision != null) {
            String base = typeSpec.substring(0, typeSpec.indexOf('(')).trim()
                    + typeSpec.substring(typeSpec.indexOf(')') + 1);
            return TypeCoercion.roundTemporal(applyCast(val, base.trim(), fromUnknownLiteral),
                    timePrecision);
        }
        // An interval type carries its field qualifier and its fractional-seconds precision in
        // the type name, and both change the value: the qualifier decides what an unlabelled
        // number in the literal counts and which fields survive, the precision how many
        // fractional digits do. Both have to be seen before the text is read, not after.
        if (!lowerSpec.endsWith("[]")) {
            IntervalTypmod intervalTypmod = IntervalTypmod.fromTypeSpec(lowerSpec);
            if (intervalTypmod != null) {
                if (val instanceof PgInterval) return intervalTypmod.apply((PgInterval) val);
                if (val instanceof String) return PgInterval.parse((String) val, intervalTypmod);
                return intervalTypmod.apply(TypeCoercion.toInterval(val));
            }
        }
        String typeName = typeSpec.toLowerCase().replaceAll("\\(.*\\)", "").trim();
        // Handle array casting: when value is a List or PG array literal string, cast each element
        boolean isArrayCast = typeName.contains("[]");
        typeName = typeName.replace("[]", "").trim();
        if (isArrayCast) {
            // Text becomes an array by being written as an array literal, never by being one
            // element of one: PG reads '3'::int[] as a literal and fails to parse it.
            List<?> list;
            ArrayLiteral literal = null;
            if (val instanceof List<?>) {
                list = (List<?>) val;
            } else if (val instanceof String) {
                literal = ArrayLiteral.parse((String) val);
                list = literal.elements();
            } else {
                list = null;
            }
            if (list != null) {
                // The literal parser hands back nested lists for nested braces, so a String element
                // is only ever an element -- a quoted "{1,2}" stays that text rather than becoming
                // a sub-array. A List arriving from elsewhere still needs the older reading.
                List<Object> castList = castArrayElements(list, typeName, literal == null);
                if (literal != null && literal.hasCustomLowerBounds()) {
                    return literal.boundsPrefix() + TypeCoercion.formatPgArray(castList);
                }
                return castList;
            }
        }
        // Types PostgreSQL ships that memgres holds no value class for. A cast to one is legal SQL
        // there, so refusing it as a type that does not exist refused a statement PostgreSQL runs;
        // the text is kept as written, which is what every one of them prints. A type this
        // database has been told about under the same name is its own and answers first.
        if (KEPT_AS_WRITTEN.contains(typeName) && !isDeclaredByThisDatabase(typeName)) {
            return val.toString();
        }
        switch (typeName) {
            case "integer":
            case "int":
            case "int4":
                return TypeCoercion.toInteger(val);
            case "bigint":
            case "int8":
                return TypeCoercion.toLong(val);
            case "smallint":
            case "int2": {
                int iv = TypeCoercion.toInteger(val).intValue();
                if (iv < Short.MIN_VALUE || iv > Short.MAX_VALUE) {
                    throw new MemgresException("smallint out of range", "22003");
                }
                return (short) iv;
            }
            case "real":
            case "float4": {
                double dv = TypeCoercion.toDouble(val).doubleValue();
                float fv = (float) dv;
                boolean broughtInfinity = Double.isInfinite(dv)
                        || (val instanceof String && isInfinityLiteral(((String) val).trim()));
                if (Float.isInfinite(fv) && !broughtInfinity) {
                    // A float8 that no longer fits float4 is a narrowing overflow, which PG
                    // reports by the operation rather than by quoting the value.
                    if (val instanceof Double || val instanceof Float) throw NumericLimits.floatOverflow();
                    throw NumericLimits.outOfRangeForType(val, "real");
                }
                if (NumericLimits.underflowedToZero(val, fv)) {
                    if (val instanceof Double || val instanceof Float) throw NumericLimits.floatUnderflow();
                    throw NumericLimits.outOfRangeForType(val, "real");
                }
                return fv;
            }
            case "double precision":
            case "float8":
            case "float": {
                try {
                    Double dv = TypeCoercion.toDouble(val);
                    boolean broughtInfinity = (val instanceof Double && ((Double) val).isInfinite())
                            || (val instanceof Float && ((Float) val).isInfinite())
                            || (val instanceof String && isInfinityLiteral(((String) val).trim()));
                    if (dv.isInfinite() && !broughtInfinity) {
                        throw NumericLimits.outOfRangeForType(val, "double precision");
                    }
                    if (NumericLimits.underflowedToZero(val, dv.doubleValue())) {
                        throw NumericLimits.outOfRangeForType(val, "double precision");
                    }
                    return dv;
                } catch (NumberFormatException e) {
                    throw new MemgresException("invalid input syntax for type double precision: \"" + val + "\"", "22P02");
                }
            }
            case "numeric":
            case "decimal": {
                Double special = NumericLimits.specialNumericOrNull(val);
                if (special != null) return special;
                return NumericLimits.check(TypeCoercion.toBigDecimal(val));
            }
            case "citext": {
                // citext preserves original case but compares case-insensitively
                if (val instanceof CitextValue) return val;
                return new CitextValue(val.toString());
            }
            case "text":
            case "varchar":
            case "character varying":
            case "char":
            case "character":
            case "name": {
                // PG inet::text uses network_show which always includes /prefix
                if (val instanceof InetValue) {
                    return ((InetValue) val).text();
                }
                // Composites render as (f1,f2), whether held as a row or as a field map.
                if (val instanceof AstExecutor.PgRow) {
                    return ((AstExecutor.PgRow) val).toPgText();
                }
                if (val instanceof java.util.Map<?, ?>) {
                    return AstExecutor.PgRow.fromFieldMap((java.util.Map<?, ?>) val).toPgText();
                }
                if (val instanceof RegclassValue) {
                    RegclassValue rc = (RegclassValue) val;
                    return formatRegclassDisplay(rc.name());
                }
                if (val instanceof RegprocValue) {
                    return ((RegprocValue) val).name();
                }
                if (val instanceof RegtypeValue) {
                    return ((RegtypeValue) val).name();
                }
                if (val instanceof RegnamespaceValue) {
                    return ((RegnamespaceValue) val).name();
                }
                // For OffsetDateTime, apply session timezone conversion (PG behavior for timestamptz::text)
                if (val instanceof java.time.OffsetDateTime && executor.session != null) {
                    java.time.OffsetDateTime odt = (java.time.OffsetDateTime) val;
                    String tz = executor.session.getGucSettings().get("timezone");
                    if (tz != null) {
                        try {
                            java.time.ZoneId zone = java.time.ZoneId.of(tz);
                            odt = odt.atZoneSameInstant(zone).toOffsetDateTime();
                        } catch (Exception ignored) {}
                    }
                    // Format like PG: yyyy-MM-dd HH:mm:ss+ZZ
                    String timePart = odt.getNano() != 0
                            ? stripTrailingFracZeros(odt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")))
                            : odt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                    String offsetStr = odt.getOffset().toString();
                    if (offsetStr.equals("Z")) offsetStr = "+00";
                    return odt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")) + " " + timePart + offsetStr;
                }
                // Java pads a time's fraction to a multiple of three digits; PG writes only the
                // digits the value has, so 01:02:03.99 stays two digits wide
                if (val instanceof java.time.LocalTime) {
                    java.time.LocalTime lt = (java.time.LocalTime) val;
                    return lt.getNano() == 0
                            ? lt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))
                            : stripTrailingFracZeros(lt.format(
                                    java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")));
                }
                // H17: PgVector (int2vector/oidvector) to text: space-separated format
                if (val instanceof PgVector) {
                    return val.toString();
                }
                // Array (List) to text: use PostgreSQL {e1,e2,...} format
                if (val instanceof java.util.List<?>) {
                    return formatListAsPgArray((java.util.List<?>) val);
                }
                // Boolean to text: PG SELECT true::text → "true"
                if (val instanceof Boolean) {
                    return ((Boolean) val) ? "true" : "false";
                }
                // float4/float8 to text goes through PG's own output functions,
                // which honour extra_float_digits.
                if (val instanceof Double) {
                    return PgFloatFormat.float8out((Double) val, extraFloatDigits());
                }
                if (val instanceof Float) {
                    return PgFloatFormat.float4out((Float) val, extraFloatDigits());
                }
                if (val instanceof byte[]) {
                    // PG bytea::text uses the bytea_output format (default: hex, "\x..").
                    byte[] b = (byte[]) val;
                    String byteaOutput = (executor.session != null) ? executor.session.getGucSettings().get("bytea_output") : "hex";
                    if ("escape".equalsIgnoreCase(byteaOutput)) {
                        StringBuilder esc = new StringBuilder();
                        for (byte bb : b) {
                            int v = bb & 0xFF;
                            if (v == 0x5C) { // backslash
                                esc.append("\\\\");
                            } else if (v >= 32 && v <= 126) {
                                esc.append((char) v);
                            } else {
                                esc.append('\\');
                                esc.append((char) ('0' + ((v >> 6) & 7)));
                                esc.append((char) ('0' + ((v >> 3) & 7)));
                                esc.append((char) ('0' + (v & 7)));
                            }
                        }
                        return esc.toString();
                    }
                    StringBuilder bhex = new StringBuilder(2 + b.length * 2);
                    bhex.append("\\x");
                    for (byte bb : b) {
                        String hex = Integer.toHexString(bb & 0xFF);
                        if (hex.length() == 1) bhex.append('0');
                        bhex.append(hex);
                    }
                    return bhex.toString();
                }
                // LocalDateTime to text: PG uses a space separator, not 'T', and marks the era
                if (val instanceof java.time.LocalDateTime) {
                    java.time.LocalDateTime dt = (java.time.LocalDateTime) val;
                    String datePart = String.format("%04d-%02d-%02d",
                            TypeCoercion.displayYear(dt.getYear()), dt.getMonthValue(), dt.getDayOfMonth());
                    String era = TypeCoercion.eraSuffix(dt.getYear());
                    if (dt.getNano() != 0) {
                        String s = datePart + " "
                                + dt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"));
                        return stripTrailingFracZeros(s) + era;
                    }
                    return datePart + " "
                            + dt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")) + era;
                }
                if (val instanceof java.time.LocalDate) {
                    return TypeCoercion.formatIsoDate((java.time.LocalDate) val);
                }
                // PG formats float8/float4 without trailing ".0" when the value is integral
                if (val instanceof Double) {
                    double d = (Double) val;
                    if (Double.isNaN(d)) return "NaN";
                    if (Double.isInfinite(d)) return d > 0 ? "Infinity" : "-Infinity";
                    if (d == Math.floor(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) {
                        return Long.toString((long) d);
                    }
                }
                if (val instanceof Float) {
                    float f = (Float) val;
                    if (Float.isNaN(f)) return "NaN";
                    if (Float.isInfinite(f)) return f > 0 ? "Infinity" : "-Infinity";
                    if (f == Math.floor(f) && !Float.isInfinite(f) && Math.abs(f) < 1e7) {
                        return Long.toString((long) f);
                    }
                }
                // LocalDate to text: respect DateStyle GUC
                if (val instanceof java.time.LocalDate) {
                    java.time.LocalDate ld = (java.time.LocalDate) val;
                    String datestyle = (executor.session != null) ? executor.session.getGucSettings().get("datestyle") : "ISO, MDY";
                    if (datestyle != null && datestyle.toLowerCase().contains("german")) {
                        return ld.format(java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy"));
                    } else if (datestyle != null && datestyle.toLowerCase().contains("sql")) {
                        return ld.format(java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy"));
                    }
                    return ld.toString();
                }
                // PgInterval to text: respect IntervalStyle GUC
                if (val instanceof PgInterval) {
                    String intervalStyle = (executor.session != null) ? executor.session.getGucSettings().get("intervalstyle") : "postgres";
                    return ((PgInterval) val).toString(intervalStyle);
                }
                // PG's numeric output never falls back on exponent notation, which is where
                // BigDecimal goes for a negative scale (round(1.5, -1)) or a large one
                if (val instanceof java.math.BigDecimal) {
                    return ((java.math.BigDecimal) val).toPlainString();
                }
                return val.toString();
            }
            case "boolean":
            case "bool":
                // Only integer converts to boolean; PG ships no cast from the wider numeric types,
                // so a value that "looks true" is a type error rather than a truth value.
                if (val instanceof java.math.BigDecimal || val instanceof Double
                        || val instanceof Float || val instanceof Long || val instanceof Short) {
                    throw new MemgresException("cannot cast type "
                            + AstExecutor.pgTypeNameOf(val) + " to boolean", "42846");
                }
                return TypeCoercion.toBoolean(val);
            case "date":
                return TypeCoercion.toLocalDateOrBc(val);
            case "time":
            case "time without time zone":
                return TypeCoercion.toLocalTime(val);
            case "timetz":
            case "time with time zone":
                return TypeCoercion.toTimeTz(val);
            case "timestamp":
            case "timestamp without time zone":
                return TypeCoercion.toLocalDateTimeOrInfinity(val);
            case "timestamptz":
            case "timestamp with time zone":
                return TypeCoercion.toOffsetDateTime(val, sessionInterpretationZone());
            // "interval", with or without a qualifier, is handled above where the qualifier can
            // still reach the literal's text.
            case "money":
                // PG does not allow direct float→money cast (must go through numeric first)
                if (val instanceof Double || val instanceof Float) {
                    throw new MemgresException("cannot cast type double precision to money", "42846");
                }
                return TypeCoercion.toMoney(val);
            case "bytea": {
                if (val instanceof byte[]) return val;
                String s = val.toString();
                if (s.startsWith("\\x") || s.startsWith("\\X")) {
                    return ByteaOperations.parseHexFormat(s);
                }
                // Check for escape format: contains backslash-octal sequences
                if (s.contains("\\")) {
                    return ByteaOperations.parseEscapeFormat(s);
                }
                // Convert plain string to bytes (PG stores bytea as byte array)
                return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case "bit":
            case "bit varying":
            case "varbit": {
                // A bit string lives in one attribute, so PG bounds the modifier before it builds
                // anything; without this, bit(200000000) allocates two hundred million characters.
                checkBitTypmod(typeName, lowerSpec);
                String bitStr;
                if (val instanceof AstExecutor.PgBitString) {
                    bitStr = ((AstExecutor.PgBitString) val).bits();
                } else if (val instanceof Number) {
                    // Integer/long → bit: convert to two's complement binary
                    long lv = ((Number) val).longValue();
                    // Extract target length from type spec
                    int targetLen = -1;
                    java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\((\\d+)\\)").matcher(lowerSpec);
                    if (m.find()) targetLen = Integer.parseInt(m.group(1));
                    if (targetLen <= 0) targetLen = 32; // PG default for bit without length is 1, but for int casts uses 32
                    if (targetLen <= 32) {
                        // Use 32-bit two's complement, then take last targetLen bits
                        String full = String.format("%32s", Integer.toBinaryString((int) lv)).replace(' ', '0');
                        bitStr = full.substring(32 - targetLen);
                    } else {
                        // Use 64-bit two's complement, then take last targetLen bits
                        String full = String.format("%64s", Long.toBinaryString(lv)).replace(' ', '0');
                        if (targetLen <= 64) {
                            bitStr = full.substring(64 - targetLen);
                        } else {
                            // Pad left with sign bit
                            char signBit = lv < 0 ? '1' : '0';
                            bitStr = Strs.repeat(String.valueOf(signBit), targetLen - 64) + full;
                        }
                    }
                    return new AstExecutor.PgBitString(bitStr);
                } else {
                    bitStr = expandBitRadixPrefix(val.toString());
                }
                // Validate: only '0' and '1' allowed
                for (int i = 0; i < bitStr.length(); i++) {
                    char c = bitStr.charAt(i);
                    if (c != '0' && c != '1') {
                        throw new MemgresException("\"" + val + "\" is not a valid binary digit", "22P02");
                    }
                }
                // Handle bit(N) length enforcement
                java.util.regex.Matcher lenMatcher = java.util.regex.Pattern.compile("\\((\\d+)\\)").matcher(lowerSpec);
                if (lenMatcher.find()) {
                    int n = Integer.parseInt(lenMatcher.group(1));
                    if (typeName.equals("bit")) {
                        // bit(n): must be exactly n bits — pad right or truncate
                        if (bitStr.length() < n) {
                            bitStr = bitStr + Strs.repeat("0", n - bitStr.length());
                        } else if (bitStr.length() > n) {
                            bitStr = bitStr.substring(0, n);
                        }
                    } else {
                        // varbit(n): an explicit cast truncates to n bits (assignment errors instead,
                        // which TypeCoercion handles)
                        if (bitStr.length() > n) {
                            bitStr = bitStr.substring(0, n);
                        }
                    }
                } else if (typeName.equals("bit") && bitStr.length() != 1) {
                    // bare "bit" without length means bit(1)
                    if (bitStr.length() < 1) {
                        bitStr = "0";
                    } else {
                        bitStr = bitStr.substring(0, 1);
                    }
                }
                return new AstExecutor.PgBitString(bitStr);
            }
            case "point":
                return GeometricOperations.format(GeometricOperations.parsePoint(val.toString()));
            case "line":
                return GeometricOperations.format(GeometricOperations.parseLine(val.toString()));
            case "lseg":
                return GeometricOperations.format(GeometricOperations.parseLseg(val.toString()));
            case "box":
                return GeometricOperations.format(GeometricOperations.parseBox(val.toString()));
            case "path":
                return GeometricOperations.format(GeometricOperations.parsePath(val.toString()));
            case "polygon":
                return GeometricOperations.format(GeometricOperations.parsePolygon(val.toString()));
            case "circle":
                return GeometricOperations.format(GeometricOperations.parseCircle(val.toString()));
            case "tsvector": {
                if (val instanceof TsVector) return val;
                String tsInput = val.toString();
                // ::tsvector cast uses tsvector input format (literal parsing), NOT to_tsvector
                TsVector parsed = TsVector.parseLiteral(tsInput);
                return parsed != null ? parsed : TsVector.empty();
            }
            case "tsquery":
                return val instanceof TsQuery ? ((TsQuery) val) : TsQuery.parse(val.toString());
            case "xml":
                return XmlOperations.validateXmlCast(val.toString());
            case "int4range":
            case "int8range":
            case "numrange":
            case "daterange":
            case "tsrange":
            case "tstzrange": {
                String rangeStr = val.toString().trim();
                // Detect multirange-to-range cast (multirange starts with '{')
                if (rangeStr.startsWith("{") && rangeStr.endsWith("}")) {
                    String multirangeType = typeName.replace("range", "multirange");
                    throw new MemgresException("cannot cast type " + multirangeType + " to " + typeName, "42846");
                }
                // The bounds have to fit the range's element type. Narrowing them silently would
                // leave a plausible range whose bounds are not the ones written.
                checkRangeBoundsFitElementType(rangeStr, typeName);
                // The target type names the element type, so the bounds are read and written back
                // as values of it rather than as whatever the written text happened to resemble.
                return RangeOperations.parse(rangeStr, typeName).toString();
            }
            case "int4multirange":
            case "int8multirange":
            case "nummultirange":
            case "datemultirange":
            case "tsmultirange":
            case "tstzmultirange": {
                String s = val.toString().trim();
                String rangeType = typeName.replace("multirange", "range");
                // A range value casts to its multirange type, but a written literal has to be a
                // multirange literal: PG reads the text with the multirange input function, which
                // wants the braces. Only the source can tell those two apart.
                if (!fromUnknownLiteral && RangeOperations.isRangeString(s)) {
                    RangeOperations.PgRange parsed = RangeOperations.parse(s, rangeType);
                    if (parsed.isEmpty()) return "{}";
                    return "{" + parsed.toString() + "}";
                }
                if (s.equalsIgnoreCase("empty")) return "{}";
                // Multirange literal validation and canonicalization. The error names the value as
                // it was written, whitespace and all, so the untrimmed text is what is read.
                java.util.List<RangeOperations.PgRange> parsed = new java.util.ArrayList<>();
                for (RangeOperations.PgRange r
                        : RangeOperations.parseMultirangeLiteral(val.toString(), rangeType)) {
                    if (!r.isEmpty()) parsed.add(r);
                }
                if (parsed.isEmpty()) return "{}";
                return RangeOperations.formatMultirange(RangeOperations.mergeAndSort(parsed));
            }
            case "uuid": {
                if (val instanceof java.util.UUID) return val;
                return parseUuid(val.toString());
            }
            case "json":
            case "jsonb": {
                // hstore → json/jsonb: convert via hstore_to_json semantics (all values as strings)
                if (val instanceof HstoreValue) {
                    HstoreValue h = (HstoreValue) val;
                    StringBuilder sb = new StringBuilder("{");
                    boolean first = true;
                    for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                        if (!first) sb.append(", ");
                        first = false;
                        sb.append("\"").append(e.getKey().replace("\\", "\\\\").replace("\"", "\\\"")).append("\": ");
                        if (e.getValue() == null) sb.append("null");
                        else sb.append("\"").append(e.getValue().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    }
                    sb.append("}");
                    return sb.toString();
                }
                String jsonStr = val.toString();
                String trimmed = jsonStr.trim();
                // Parse it properly rather than balancing brackets: a bracket count cannot see a
                // second document after the first, an unquoted key, or a number JSON has no form for.
                JsonTextValidator.validate(trimmed);
                // JSONB normalizes whitespace and decodes string escapes; JSON preserves input
                if ("jsonb".equals(typeName)) {
                    return TypeCoercion.normalizeJsonb(trimmed);
                }
                return jsonStr;
            }
            case "inet": {
                if (val instanceof InetValue) return val;
                if (val instanceof CidrValue) return val; // cidr is-a InetValue, usable as inet
                return InetValue.parse(val.toString());
            }
            case "cidr": {
                if (val instanceof CidrValue) return val;
                if (val instanceof InetValue) return CidrValue.fromInet((InetValue) val);
                return CidrValue.parse(val.toString());
            }
            case "hstore":
                if (!executor.database.hasExtension("hstore")) {
                    throw new MemgresException("type \"hstore\" does not exist\n"
                            + "  Hint: You need to install the hstore extension: CREATE EXTENSION hstore;", "42704");
                }
                if (val instanceof HstoreValue) return val;
                return HstoreValue.parse(val.toString());
            case "macaddr": {
                if (val instanceof MacaddrValue) return val;
                if (val instanceof Macaddr8Value) return ((Macaddr8Value) val).toMacaddr();
                return MacaddrValue.parse(val.toString());
            }
            case "macaddr8": {
                if (val instanceof Macaddr8Value) return val;
                if (val instanceof MacaddrValue) return ((MacaddrValue) val).toMacaddr8();
                return Macaddr8Value.parse(val.toString());
            }
            case "regconfig":
            case "regdictionary":
            case "regrole":
            case "regoper":
            case "regoperator":
                // reg* OID types — we don't track real OIDs for these internal objects;
                // preserve the input name as-is so the cast round-trips to the same text.
                return val.toString();
            case "pg_lsn":
                // PG log sequence number — preserve the 'X/Y' hex textual form
                return val.toString();
            case "tid":
                // tuple identifier; preserve as-is
                return val.toString();
            case "jsonpath":
                return normalizeJsonpath(val.toString());
            case "oidvector":
            case "int2vector": {
                // The catalogs' 0-based vector types, written as space-separated numbers. Held as
                // a PgVector so a subscript is read from 0 and array_lower answers 0, the way it
                // does for pg_proc.proargtypes.
                if (val instanceof PgVector) return val;
                String vs = val.toString().trim();
                List<Object> elems = new ArrayList<>();
                if (!vs.isEmpty()) {
                    for (String part : vs.split("\\s+")) {
                        if (part.isEmpty()) continue;
                        try {
                            elems.add("oidvector".equals(typeName) ? (Object) Integer.valueOf(part)
                                    : (Object) Short.valueOf(part));
                        } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type " + typeName
                                    + ": \"" + vs + "\"", "22P02");
                        }
                    }
                }
                return new PgVector(elems);
            }
            case "xid": {
                // xid is a transaction ID, essentially an unsigned 32-bit integer
                if (val instanceof Number) return ((Number) val).longValue();
                String s = val.toString().trim();
                try { return Long.parseLong(s); } catch (NumberFormatException e) { return 0L; }
            }
            case "regclass": {
                // Return OID internally but tag for display as name
                if (val instanceof RegclassValue) return val;
                if (val instanceof Number) {
                    Number numVal = (Number) val;
                    // Resolve OID to name by scanning the oidMap
                    int targetOid = numVal.intValue();
                    for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                        if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                            String fullKey = entry.getKey().substring(4); // strip "rel:"
                            String displayName = formatRegclassDisplay(fullKey);
                            return new RegclassValue(targetOid, displayName);
                        }
                    }
                    // If not found, return as-is (number)
                    return val;
                }
                String relName = val.toString().trim();
                // An all-digit string is an OID written out, which PG takes verbatim without
                // looking anything up -- that is how a catalog dump round-trips a regclass
                if (relName.matches("\\d+")) {
                    try {
                        return new RegclassValue(Integer.parseInt(relName), relName);
                    } catch (NumberFormatException ignored) {
                        // too large for an int: fall through to name resolution
                    }
                }
                String schemaPrefix = null;
                if (relName.contains(".")) {
                    int dotIdx = relName.lastIndexOf('.');
                    schemaPrefix = relName.substring(0, dotIdx);
                    relName = relName.substring(dotIdx + 1);
                }
                // M15: Handle quoted identifiers — preserve case if double-quoted
                boolean relQuoted = relName.startsWith("\"") && relName.endsWith("\"") && relName.length() > 1;
                if (relQuoted) {
                    relName = relName.substring(1, relName.length() - 1);
                }
                if (schemaPrefix != null && schemaPrefix.startsWith("\"") && schemaPrefix.endsWith("\"") && schemaPrefix.length() > 1) {
                    schemaPrefix = schemaPrefix.substring(1, schemaPrefix.length() - 1);
                }
                // PG lowercases unquoted identifiers
                String lowerName = relQuoted ? relName : relName.toLowerCase();
                // Validate relation exists before returning OID
                boolean rcExists = false;
                if (schemaPrefix != null) {
                    String lowerSchema = schemaPrefix.toLowerCase();
                    // Check system catalog tables (virtual tables in SystemCatalog)
                    if ("pg_catalog".equals(lowerSchema) && lowerName.startsWith("pg_")) {
                        rcExists = true; // All pg_catalog.pg_* tables are recognized
                    } else if ("information_schema".equals(lowerSchema)) {
                        rcExists = true; // All information_schema tables are recognized
                    }
                    if (!rcExists) {
                        Schema s = executor.database.getSchema(lowerSchema);
                        if (s != null && s.getTable(lowerName) != null) rcExists = true;
                    }
                    if (!rcExists && executor.database.hasView(lowerName)) rcExists = true;
                    if (!rcExists && executor.database.getSequence(lowerName) != null) rcExists = true;
                    if (!rcExists && executor.database.hasIndex(lowerName)) rcExists = true;
                } else {
                    if (lowerName.startsWith("pg_") || lowerName.startsWith("information_schema")) {
                        rcExists = true; // system tables
                    } else {
                        // Search in default schema, then public
                        Schema defSchema = executor.database.getSchema(executor.defaultSchema());
                        if (defSchema != null && defSchema.getTable(lowerName) != null) rcExists = true;
                        if (!rcExists) {
                            Schema pub = executor.database.getSchema("public");
                            if (pub != null && pub.getTable(lowerName) != null) rcExists = true;
                        }
                        if (!rcExists && executor.database.getSequence(lowerName) != null) rcExists = true;
                        if (!rcExists && executor.database.hasIndex(lowerName)) rcExists = true;
                        if (!rcExists && executor.database.hasView(lowerName)) rcExists = true;
                        // A primary-key or unique index is stored as a constraint, not an index,
                        // but it is still a relation with a name
                        if (!rcExists && isConstraintBackedIndex(lowerName)) rcExists = true;
                        // pg_temp is implicitly on the search path ahead of public
                        if (!rcExists && executor.session != null) {
                            Schema tempSchema = executor.database.getSchema(executor.session.getTempSchemaName());
                            if (tempSchema != null && tempSchema.getTable(lowerName) != null) rcExists = true;
                        }
                    }
                }
                if (!rcExists) {
                    throw new MemgresException("relation \"" + val + "\" does not exist", "42P01");
                }
                int regOid;
                String displayName;
                if (schemaPrefix != null) {
                    regOid = executor.systemCatalog.getOid("rel:" + schemaPrefix.toLowerCase() + "." + lowerName);
                    displayName = formatRegclassDisplay(schemaPrefix.toLowerCase() + "." + relName);
                } else if (lowerName.startsWith("pg_")) {
                    regOid = executor.systemCatalog.getOid("rel:pg_catalog." + lowerName);
                    displayName = quoteIdentIfNeeded(relName);
                } else {
                    regOid = executor.systemCatalog.getOid("rel:" + executor.defaultSchema() + "." + lowerName);
                    displayName = quoteIdentIfNeeded(relName);
                }
                return new RegclassValue(regOid, displayName);
            }
            case "regproc":
            case "regprocedure": {
                // ::regproc converts a function name to its OID — and an OID back to the name.
                // A catalog column holding a function reference is read as
                // `oprcode::regproc::text`, and answering with the number is answering with
                // what the reader already had.
                if (val instanceof RegprocValue) return val;
                if (val instanceof Number) {
                    int procOidIn = ((Number) val).intValue();
                    if (procOidIn == 0) return new RegprocValue(0, "-");
                    String known = procNameForOid(procOidIn);
                    return known != null ? new RegprocValue(procOidIn, known) : val;
                }
                // A schema written in front of the name is part of what is being named: a
                // function of that name in another schema is not the one asked for, and no
                // schema of that name at all means no function either. What comes back is
                // written the way PostgreSQL writes it -- qualified only where the schema is
                // off the search path -- rather than the way the query happened to spell it.
                String written = val.toString();
                CatalogMetadataFunctions.ProcLookup lookup = CatalogMetadataFunctions.lookupProc(
                        executor, written, "regprocedure".equals(typeName));
                if (lookup == null) {
                    throw new MemgresException(
                            "function \"" + written.trim() + "\" does not exist", "42883");
                }
                if (lookup.ambiguous) {
                    throw new MemgresException(
                            "more than one function named \"" + written.trim() + "\"", "42725");
                }
                return new RegprocValue(lookup.oid, lookup.display);
            }
            case "regtype": {
                // ::regtype converts a type name to its OID or name
                if (val instanceof RegtypeValue) return val;
                if (val instanceof Number) {
                    int oid = ((Number) val).intValue();
                    String name = typeNameForOid(oid);
                    return new RegtypeValue(oid, name != null ? name : String.valueOf(oid));
                }
                String rtName = val.toString().trim().toLowerCase();
                // Polymorphic pseudo-types are real pg_type rows, so they cast like any other name
                if (PolymorphicTypes.isPolymorphic(rtName)) {
                    return new RegtypeValue(PolymorphicTypes.oid(rtName), rtName);
                }
                // Validate the type exists
                DataType dt = DataType.fromPgName(rtName);
                if (dt == null) {
                    // Check common aliases
                    switch (rtName) {
                        case "int":
                        case "int4":
                        case "integer":
                            dt = DataType.INTEGER;
                            break;
                        case "int2":
                        case "smallint":
                            dt = DataType.SMALLINT;
                            break;
                        case "int8":
                        case "bigint":
                            dt = DataType.BIGINT;
                            break;
                        case "float4":
                        case "real":
                            dt = DataType.REAL;
                            break;
                        case "float8":
                        case "double precision":
                            dt = DataType.DOUBLE_PRECISION;
                            break;
                        case "bool":
                        case "boolean":
                            dt = DataType.BOOLEAN;
                            break;
                        case "varchar":
                        case "character varying":
                            dt = DataType.VARCHAR;
                            break;
                        case "char":
                        case "character":
                            dt = DataType.CHAR;
                            break;
                        case "timestamptz":
                        case "timestamp with time zone":
                            dt = DataType.TIMESTAMPTZ;
                            break;
                        case "timetz":
                        case "time with time zone":
                            dt = DataType.TIMETZ;
                            break;
                        default:
                            dt = null;
                            break;
                    }
                }
                // A composite or range type created by the user is as much a type as an enum is.
                if (dt == null && executor.database.getDomain(rtName) == null
                        && !executor.database.isCustomEnum(rtName)
                        && !executor.database.isCompositeType(rtName)
                        && !executor.database.getRangeTypes().containsKey(rtName)) {
                    throw new MemgresException("type \"" + val + "\" does not exist", "42704");
                }
                // Return RegtypeValue with canonical type name and OID
                if (dt != null) {
                    // Map the DataType back to the canonical PG name
                    String canonical;
                    switch (dt) {
                        case INTEGER:
                            canonical = "integer";
                            break;
                        case SMALLINT:
                            canonical = "smallint";
                            break;
                        case BIGINT:
                            canonical = "bigint";
                            break;
                        case REAL:
                            canonical = "real";
                            break;
                        case DOUBLE_PRECISION:
                            canonical = "double precision";
                            break;
                        case BOOLEAN:
                            canonical = "boolean";
                            break;
                        case VARCHAR:
                            canonical = "character varying";
                            break;
                        case CHAR:
                            canonical = "character";
                            break;
                        case TEXT:
                            canonical = "text";
                            break;
                        case NUMERIC:
                            canonical = "numeric";
                            break;
                        case DATE:
                            canonical = "date";
                            break;
                        case TIME:
                            canonical = "time without time zone";
                            break;
                        case TIMESTAMP:
                            canonical = "timestamp without time zone";
                            break;
                        case TIMESTAMPTZ:
                            canonical = "timestamp with time zone";
                            break;
                        case INTERVAL:
                            canonical = "interval";
                            break;
                        case UUID:
                            canonical = "uuid";
                            break;
                        case JSON:
                            canonical = "json";
                            break;
                        case JSONB:
                            canonical = "jsonb";
                            break;
                        case BYTEA:
                            canonical = "bytea";
                            break;
                        default:
                            canonical = dt.getPgName();
                            break;
                    }
                    return new RegtypeValue(dt.getOid(), canonical);
                }
                // Custom enum or domain — resolve OID from catalog
                int customOid = executor.systemCatalog.getOid("type:" + rtName);
                return new RegtypeValue(customOid, val.toString());
            }
            case "oid": {
                if (val instanceof RegclassValue) return ((RegclassValue) val).oid();
                if (val instanceof RegprocValue) return ((RegprocValue) val).oid();
                if (val instanceof RegtypeValue) return ((RegtypeValue) val).oid();
                return TypeCoercion.toInteger(val);
            }
            case "regnamespace": {
                // ::regnamespace wraps the schema name so it renders as the name
                // but still equals() its OID for comparisons against pg_namespace.oid.
                if (val instanceof RegnamespaceValue) return val;
                if (val instanceof Number) {
                    int nsOid = ((Number) val).intValue();
                    // Reverse-lookup name from OID; fall back to numeric text.
                    String nm = null;
                    for (java.util.Map.Entry<String, Integer> e : executor.systemCatalog.getOidMap().entrySet()) {
                        if (e.getValue() == nsOid && e.getKey().startsWith("ns:")) {
                            nm = e.getKey().substring(3);
                            break;
                        }
                    }
                    return new RegnamespaceValue(nsOid, nm != null ? nm : String.valueOf(nsOid));
                }
                String nsName = val.toString().trim();
                int nsOid = executor.systemCatalog.getOid("ns:" + nsName);
                return new RegnamespaceValue(nsOid, nsName);
            }
            default: {
                // Check if it's a domain type
                DomainType domain = executor.database.getDomain(typeName);
                if (domain != null) {
                    // A domain over a domain inherits its base's constraints, so walk from the
                    // base outwards. PG reports the constraint the base declared, but names the
                    // domain the query actually wrote.
                    List<DomainType> chain = new ArrayList<>();
                    for (DomainType d = domain; d != null && chain.size() < 64; ) {
                        chain.add(0, d);
                        String base = d.getBaseTypeName();
                        d = base == null ? null : executor.database.getDomain(base);
                    }
                    DomainType root = chain.get(0);
                    Object coerced = applyCast(val, root.getBaseType().getPgName());
                    for (DomainType d : chain) {
                        checkDomainConstraints(d, typeName, coerced);
                    }
                    return coerced;
                }
                // Check if it's an enum type. A type another session created in a transaction
                // that has not committed is not one this session can cast to yet.
                CustomEnum customEnum = executor.database.getCustomEnum(typeName);
                if (customEnum != null && !executor.database.isObjectVisibleTo(customEnum, executor.session)) {
                    customEnum = null;
                }
                if (customEnum != null) {
                    String label = val instanceof AstExecutor.PgEnum ? ((AstExecutor.PgEnum) val).label() : val.toString();
                    if (!customEnum.isValidLabel(label)) {
                        throw new MemgresException("invalid input value for enum " + typeName + ": \"" + label + "\"", "22P02");
                    }
                    return new AstExecutor.PgEnum(label, typeName, customEnum.ordinal(label));
                }
                // Cast to "record": ROW values are already record types, return as-is
                if (typeName.equals("record")) {
                    return val;
                }
                // ROW cast to a composite type, which a table also defines; check arity
                List<CreateTypeStmt.CompositeField> rowFields = executor.database.getRowType(typeName);
                if (val instanceof AstExecutor.PgRow && rowFields != null) {
                    AstExecutor.PgRow pr = (AstExecutor.PgRow) val;
                    if (pr.values().size() != rowFields.size()) {
                        throw new MemgresException("cannot cast type record to " + typeName, "42846");
                    }
                }
                // If this type is not a known composite either, it doesn't exist
                if (rowFields == null) {
                    // Check if it looks like a user-defined type name (not a built-in alias we missed)
                    // Known safe aliases that fall through: none should reach here after the switch above
                    // Only throw if the type name looks like an unknown identifier (not a PG built-in)
                    DataType knownType = DataType.fromPgName(typeName);
                    if (knownType == null) {
                        throw new MemgresException("type \"" + typeName + "\" does not exist", "42704");
                    }
                }
                return val;
            }
        }
    }

    /**
     * Run every CHECK a domain carries against a value being cast to it. A constraint added by
     * ALTER DOMAIN counts as much as one written into CREATE DOMAIN, and PostgreSQL enforces one
     * marked NOT VALID on new values too — NOT VALID only excuses the rows already stored.
     *
     * @param reportedName the domain the query named, which is the one PG blames even when the
     *        constraint was inherited from a domain this one is built on
     */
    private void checkDomainConstraints(DomainType domain, String reportedName, Object value) {
        Table valueTable = new Table("_domain_check", Cols.listOf(
                new Column("value", domain.getBaseType(), true, false, null)));
        RowContext checkCtx = new RowContext(valueTable, null, new Object[]{value});
        if (domain.getParsedCheck() != null) {
            failIfViolated(domain.getParsedCheck(), checkCtx, reportedName, domain.getName() + "_check");
        }
        for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
            if (nc.parsedCheck() == null) continue;
            failIfViolated(nc.parsedCheck(), checkCtx, reportedName, nc.name());
        }
    }

    /** A CHECK that answers NULL passes, the way it does on a table; only an explicit false fails. */
    private void failIfViolated(Expression check, RowContext ctx, String domainName, String constraintName) {
        Object result = executor.evalExpr(check, ctx);
        if (result != null && !executor.isTruthy(result)) {
            throw new MemgresException("value for domain " + domainName
                    + " violates check constraint \"" + constraintName + "\"", "23514");
        }
    }

    /**
     * Format a regclass display name, omitting the schema prefix if the schema
     * is in the current search_path (matching PG behavior).
     * Input can be "schema.table" or just "table".
     */
    private String formatRegclassDisplay(String qualifiedName) {
        // An all-digit regclass is an OID rather than an identifier, and prints bare
        if (qualifiedName.matches("\\d+")) return qualifiedName;
        if (!qualifiedName.contains(".")) return quoteIdentIfNeeded(qualifiedName);
        int dotIdx = qualifiedName.indexOf('.');
        String schema = qualifiedName.substring(0, dotIdx);
        String table = qualifiedName.substring(dotIdx + 1);
        // pg_catalog tables are never prefixed
        if ("pg_catalog".equals(schema)) return quoteIdentIfNeeded(table);
        // Check if schema is in the current search_path
        if (executor.session != null) {
            List<String> searchPath = executor.session.getEffectiveSearchPath(false);
            if (searchPath.contains(schema)) return quoteIdentIfNeeded(table);
        } else if ("public".equals(schema)) {
            return quoteIdentIfNeeded(table);
        }
        return quoteIdentIfNeeded(schema) + "." + quoteIdentIfNeeded(table);
    }

    /**
     * Double-quote an identifier for display when it is not a plain lowercase
     * identifier (M15: regclass/regtype output quotes mixed-case names).
     */
    private static String quoteIdentIfNeeded(String ident) {
        if (ident == null || ident.isEmpty()) return ident;
        if (ident.startsWith("\"")) return ident; // already quoted
        boolean needsQuote = !Character.isLowerCase(ident.charAt(0)) && ident.charAt(0) != '_';
        for (int i = 0; i < ident.length() && !needsQuote; i++) {
            char c = ident.charAt(i);
            if (!(Character.isLowerCase(c) || Character.isDigit(c) || c == '_')) needsQuote = true;
        }
        return needsQuote ? "\"" + ident.replace("\"", "\"\"") + "\"" : ident;
    }

    /** Check if a trimmed string is an infinity literal accepted by PostgreSQL. */
    private static boolean isInfinityLiteral(String s) {
        String lower = s.toLowerCase();
        return lower.equals("infinity") || lower.equals("-infinity")
                || lower.equals("+infinity") || lower.equals("inf")
                || lower.equals("-inf") || lower.equals("+inf");
    }

    /**
     * Cast every element of an array to the target element type.
     *
     * @param braceTextIsSubArray true when a String element spelled {@code {...}} should be read as
     *        a nested array. Only values that reached here already parsed need that: the literal
     *        parser has already told nesting apart from a quoted element that happens to look it.
     */
    private List<Object> castArrayElements(List<?> list, String elemType,
                                           boolean braceTextIsSubArray) {
        List<Object> castList = new ArrayList<>();
        for (Object elem : list) {
            if (elem == null) {
                castList.add(null);
            } else if (elem instanceof List<?>) {
                castList.add(castArrayElements((List<?>) elem, elemType, braceTextIsSubArray));
            } else {
                // Do not trim: quoted elements may carry significant leading/trailing whitespace
                // (already normalized by the parser above for unquoted ones).
                String elemStr = elem instanceof String ? (String) elem : elem.toString();
                // For json/jsonb, braces open an object rather than a nested array
                boolean jsonElement = elemType.equals("json") || elemType.equals("jsonb");
                if (braceTextIsSubArray && !jsonElement && elem instanceof String
                        && elemStr.startsWith("{") && elemStr.endsWith("}")) {
                    castList.add(applyCast(elemStr, elemType + "[]"));
                } else {
                    castList.add(applyCast(elem instanceof String ? elemStr : elem, elemType));
                }
            }
        }
        return castList;
    }

    /** Strip trailing zeros from the fractional-seconds part of a formatted timestamp/time string. */
    private static String stripTrailingFracZeros(String s) {
        int dotIdx = s.lastIndexOf('.');
        if (dotIdx < 0) return s;
        int end = s.length();
        int fracEnd = end;
        for (int i = dotIdx + 1; i < end; i++) {
            if (!Character.isDigit(s.charAt(i))) {
                fracEnd = i;
                break;
            }
        }
        int last = fracEnd;
        while (last > dotIdx + 1 && s.charAt(last - 1) == '0') {
            last--;
        }
        if (last == dotIdx + 1) {
            return s.substring(0, dotIdx) + s.substring(fracEnd);
        }
        return s.substring(0, last) + s.substring(fracEnd);
    }

    /** Format a Java List as a PostgreSQL array literal string {e1,e2,...}. */
    private static String formatListAsPgArray(java.util.List<?> list) {
        if (list.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) sb.append(",");
            Object elem = list.get(i);
            if (elem == null) {
                sb.append("NULL");
            } else if (elem instanceof java.util.List<?>) {
                java.util.List<?> nested = (java.util.List<?>) elem;
                sb.append(formatListAsPgArray(nested));
            } else if (elem instanceof String || elem instanceof AstExecutor.PgRow
                    || elem instanceof java.util.Map<?, ?>) {
                // A composite element renders as (f1,f2), whose commas make it need quoting
                String s;
                if (elem instanceof AstExecutor.PgRow) {
                    s = ((AstExecutor.PgRow) elem).toPgText();
                } else if (elem instanceof java.util.Map<?, ?>) {
                    s = AstExecutor.PgRow.fromFieldMap((java.util.Map<?, ?>) elem).toPgText();
                } else {
                    s = (String) elem;
                }
                // Quote strings that contain special chars
                if (s.contains(",") || s.contains("{") || s.contains("}") || s.contains("\"") || s.contains(" ") || s.isEmpty()) {
                    sb.append("\"").append(s.replace("\"", "\\\"")).append("\"");
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

    /**
     * Normalize a jsonpath string to PG format: quote all member accessor keys.
     * E.g. $.store.book[*].author → $."store"."book"[*]."author"
     */
    static String normalizeJsonpath(String jp) {
        if (jp == null || jp.isEmpty()) return jp;
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < jp.length()) {
            char c = jp.charAt(i);
            if (c == '.' && i + 1 < jp.length()) {
                sb.append('.');
                i++;
                char next = jp.charAt(i);
                if (next == '.' || next == '*' || next == '"') {
                    // recursive descent (..), wildcard (.*), or already quoted
                    sb.append(next);
                    i++;
                } else if (next == '[') {
                    sb.append(next);
                    i++;
                } else {
                    // member accessor — read the key name and quote it
                    int start = i;
                    while (i < jp.length() && jp.charAt(i) != '.' && jp.charAt(i) != '[' && jp.charAt(i) != ' ') {
                        i++;
                    }
                    sb.append('"').append(jp, start, i).append('"');
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * The fractional-seconds precision a date/time type spells out, or null when the spec is not
     * one of those types or carries no precision. {@code interval} is left alone: its qualifier
     * is read elsewhere and means more than a precision.
     */
    private static Integer temporalPrecision(String lowerSpec) {
        int paren = lowerSpec.indexOf('(');
        if (paren < 0 || lowerSpec.endsWith("[]")) return null;
        int close = lowerSpec.indexOf(')', paren);
        if (close < 0) return null;
        String base = (lowerSpec.substring(0, paren) + lowerSpec.substring(close + 1)).trim();
        base = base.replaceAll("\\s+", " ");
        if (!TEMPORAL_TYPMOD_TYPES.contains(base)) return null;
        try {
            return Integer.valueOf(lowerSpec.substring(paren + 1, close).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static final java.util.Set<String> TEMPORAL_TYPMOD_TYPES =
            new java.util.HashSet<String>(java.util.Arrays.asList(
                    "timestamp", "timestamptz", "time", "timetz",
                    "timestamp with time zone", "timestamp without time zone",
                    "time with time zone", "time without time zone"));

    /** True when the name belongs to an index PG materialises from a constraint. */
    private boolean isConstraintBackedIndex(String lowerName) {
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table t : schema.getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getName() != null && sc.getName().equalsIgnoreCase(lowerName)) return true;
                }
            }
        }
        return false;
    }

}
