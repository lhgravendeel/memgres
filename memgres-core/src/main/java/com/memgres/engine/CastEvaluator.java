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

    private final AstExecutor executor;

    /** Maps PG OIDs to their canonical type names (used by ::regtype casts). */
    private static final Map<Integer, String> OID_TO_TYPE = Cols.mapOfEntries(
        Cols.entry(16, "boolean"), Cols.entry(17, "bytea"),
        Cols.entry(20, "bigint"), Cols.entry(21, "smallint"),
        Cols.entry(23, "integer"), Cols.entry(25, "text"), Cols.entry(26, "oid"),
        Cols.entry(114, "json"), Cols.entry(142, "xml"),
        Cols.entry(700, "real"), Cols.entry(701, "double precision"),
        Cols.entry(869, "inet"), Cols.entry(650, "cidr"),
        Cols.entry(829, "macaddr"), Cols.entry(790, "money"),
        Cols.entry(1042, "character"), Cols.entry(1043, "character varying"),
        Cols.entry(1082, "date"), Cols.entry(1083, "time without time zone"),
        Cols.entry(1114, "timestamp without time zone"),
        Cols.entry(1184, "timestamp with time zone"),
        Cols.entry(1186, "interval"), Cols.entry(1560, "bit"),
        Cols.entry(1562, "bit varying"), Cols.entry(1700, "numeric"),
        Cols.entry(2950, "uuid"), Cols.entry(3802, "jsonb"),
        Cols.entry(600, "point"), Cols.entry(601, "lseg"),
        Cols.entry(602, "path"), Cols.entry(603, "box"),
        Cols.entry(604, "polygon"), Cols.entry(628, "line"),
        Cols.entry(718, "circle"),
        Cols.entry(3614, "tsvector"), Cols.entry(3615, "tsquery"),
        Cols.entry(1007, "integer[]"), Cols.entry(1009, "text[]"),
        Cols.entry(1016, "bigint[]"), Cols.entry(1000, "boolean[]")
    );

    CastEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    Object applyCast(Object val, String typeSpec) {
        if (val == null) return null;
        // JSON/JSONB null literal → SQL NULL when cast to any other type
        if (val instanceof String && ((String) val).trim().equals("null")) {
            String s = (String) val;
            String lt = typeSpec.toLowerCase().trim();
            if (!lt.equals("json") && !lt.equals("jsonb") && !lt.equals("text") && !lt.equals("varchar")) {
                return null;
            }
        }
        String lowerSpec = typeSpec.toLowerCase().trim();

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
            if (val instanceof java.util.List<?> || val instanceof AstExecutor.PgRow) {
                String srcType = val instanceof AstExecutor.PgRow ? "record" : "array";
                throw new MemgresException("cannot cast type " + srcType + " to " + lowerSpec, "42846");
            }
            // Geometric types cannot be cast to integer
            if (val instanceof String && GeometricOperations.isGeometricString(((String) val).trim())) {
                String sv = (String) val;
                throw new MemgresException("cannot cast type point to " + lowerSpec, "42846");
            }
        }

        // Handle float(p): p <= 24 means REAL, p >= 25 means DOUBLE PRECISION
        if (lowerSpec.startsWith("float(")) {
            String pStr = lowerSpec.replaceAll(".*\\((\\d+)\\).*", "$1");
            int p = Integer.parseInt(pStr);
            if (p <= 24) return TypeCoercion.toFloat(val);
            else return TypeCoercion.toDouble(val);
        }
        // Handle numeric(precision, scale) and apply scale for proper formatting
        if (lowerSpec.startsWith("numeric(") || lowerSpec.startsWith("decimal(")) {
            java.math.BigDecimal bd = TypeCoercion.toBigDecimal(val);
            String params = lowerSpec.replaceAll(".*\\(([^)]+)\\).*", "$1");
            String[] parts = params.split(",");
            if (parts.length >= 2) {
                int scale = Integer.parseInt(parts[1].trim());
                return bd.setScale(scale, java.math.RoundingMode.HALF_UP);
            }
            return bd;
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
        String typeName = typeSpec.toLowerCase().replaceAll("\\(.*\\)", "").trim();
        // Handle array casting: when value is a List or PG array literal string, cast each element
        boolean isArrayCast = typeName.contains("[]");
        typeName = typeName.replace("[]", "").trim();
        if (isArrayCast) {
            String valStr = val instanceof String ? ((String) val).trim() : null;
            // Handle custom lower-bound array: "[lo:hi]={...}", preserved as-is for subscript access
            if (valStr != null && valStr.matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                // Parse bounds and validate
                int eqIdx = valStr.indexOf('=');
                String boundsStr = valStr.substring(0, eqIdx);
                String content = valStr.substring(eqIdx + 1);
                String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                int lo = Integer.parseInt(parts[0].trim());
                int hi = Integer.parseInt(parts[1].trim());
                List<String> elems = parseArrayElements(content.substring(1, content.length() - 1));
                if (lo > hi && !elems.isEmpty()) {
                    throw new MemgresException("array lower bound must be less than or equal to upper bound", "2202E");
                }
                if (lo > hi) throw new MemgresException("wrong number of array subscripts", "2202E");
                // Return as special string preserving bounds info
                return valStr;
            }
            // Parse PG array literal string like '{1,2,3}' into a List
            List<?> list;
            if (val instanceof List<?>) {
                list = (List<?>) val;
            } else if (valStr != null && valStr.startsWith("{") && valStr.endsWith("}")) {
                String inner = valStr.substring(1, valStr.length() - 1).trim();
                if (inner.isEmpty()) {
                    list = Cols.listOf();
                } else {
                    list = java.util.Arrays.asList(inner.split(","));
                }
            } else {
                list = null;
            }
            if (list != null) {
                String elemType = typeName;
                List<Object> castList = new ArrayList<>();
                for (Object elem : list) {
                    if (elem == null) castList.add(null);
                    else castList.add(applyCast(elem instanceof String ? ((String) elem).trim() : elem, elemType));
                }
                return castList;
            }
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
            case "int2":
                return (short) TypeCoercion.toInteger(val).intValue();
            case "real":
            case "float4":
                return (float) TypeCoercion.toDouble(val).doubleValue();
            case "double precision":
            case "float8":
            case "float": {
                try {
                    Double dv = TypeCoercion.toDouble(val);
                    if (dv.isInfinite() && !(val instanceof Double && ((Double) val).isInfinite())
                            && !(val instanceof Float && ((Float) val).isInfinite())
                            && !(val instanceof String && (((String) val).trim().equalsIgnoreCase("infinity") || ((String) val).trim().equalsIgnoreCase("-infinity")))) {
                        throw new MemgresException("\"" + val + "\" is out of range for type double precision", "22003");
                    }
                    return dv;
                } catch (NumberFormatException e) {
                    throw new MemgresException("invalid input syntax for type double precision: \"" + val + "\"", "22P02");
                }
            }
            case "numeric":
            case "decimal": {
                if (val instanceof String && ((String) val).trim().equalsIgnoreCase("NaN")) return Double.NaN;
                return TypeCoercion.toBigDecimal(val);
            }
            case "text":
            case "varchar":
            case "character varying":
            case "char":
            case "character":
            case "name": {
                if (val instanceof RegclassValue) {
                    RegclassValue rc = (RegclassValue) val;
                    return formatRegclassDisplay(rc.name());
                }
                if (val instanceof RegprocValue) {
                    return ((RegprocValue) val).name();
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
                            ? odt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS"))
                            : odt.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                    String offsetStr = odt.getOffset().toString();
                    if (offsetStr.equals("Z")) offsetStr = "+00";
                    return odt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")) + " " + timePart + offsetStr;
                }
                // Array (List) to text: use PostgreSQL {e1,e2,...} format
                if (val instanceof java.util.List<?>) {
                    return formatListAsPgArray((java.util.List<?>) val);
                }
                // Boolean to text: PG cast returns "true"/"false"
                if (val instanceof Boolean) {
                    return ((Boolean) val) ? "true" : "false";
                }
                return val.toString();
            }
            case "boolean":
            case "bool":
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
                return TypeCoercion.toOffsetDateTime(val);
            case "interval":
                return TypeCoercion.toInterval(val);
            case "money":
                return TypeCoercion.toMoney(val);
            case "bytea": {
                if (val instanceof byte[]) return val;
                String s = val.toString();
                if (s.startsWith("\\x") || s.startsWith("\\X")) {
                    return ByteaOperations.parseHexFormat(s);
                }
                // Convert plain string to bytes (PG stores bytea as byte array)
                return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            }
            case "bit":
            case "bit varying":
            case "varbit": {
                String bitStr = val instanceof AstExecutor.PgBitString ? ((AstExecutor.PgBitString) val).bits() : val.toString();
                // Handle bit(N) by padding or truncating to N bits
                if (lowerSpec.matches("bit\\(\\d+\\)")) {
                    int n = Integer.parseInt(lowerSpec.replaceAll(".*\\((\\d+)\\).*", "$1"));
                    if (bitStr.length() < n) {
                        // Pad with zeros on the right (PG behavior)
                        bitStr = bitStr + Strs.repeat("0", n - bitStr.length());
                    } else if (bitStr.length() > n) {
                        bitStr = bitStr.substring(0, n);
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
            case "tsvector":
                return val instanceof TsVector ? ((TsVector) val) : TsVector.fromText(val.toString());
            case "tsquery":
                return val instanceof TsQuery ? ((TsQuery) val) : TsQuery.parse(val.toString());
            case "xml":
                return val.toString();
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
                return RangeOperations.parse(rangeStr).toString();
            }
            case "int4multirange":
            case "int8multirange":
            case "nummultirange":
            case "datemultirange":
            case "tsmultirange":
            case "tstzmultirange": {
                String s = val.toString().trim();
                // Implicit cast: range → multirange (wrap single range)
                if (RangeOperations.isRangeString(s)) {
                    RangeOperations.PgRange parsed = RangeOperations.parse(s);
                    if (parsed.isEmpty()) return "{}";
                    return "{" + parsed.toString() + "}";
                }
                if (s.equalsIgnoreCase("empty")) return "{}";
                // Multirange literal validation and canonicalization
                if (!s.startsWith("{") || !s.endsWith("}")) throw new MemgresException("malformed multirange literal: \"" + s + "\"", "22P02");
                String inner = s.substring(1, s.length() - 1);
                if (inner.isEmpty()) return "{}";
                // Parse, validate, and canonicalize (sort + merge overlapping)
                java.util.List<RangeOperations.PgRange> parsed = new java.util.ArrayList<>();
                for (String part : inner.split(",(?=[\\[\\(])")) {
                    RangeOperations.PgRange r = RangeOperations.parse(part.trim());
                    if (!r.isEmpty()) parsed.add(r);
                }
                if (parsed.isEmpty()) return "{}";
                // Sort and merge overlapping/adjacent
                parsed.sort((a, b) -> Long.compare(a.effectiveLower(), b.effectiveLower()));
                java.util.List<RangeOperations.PgRange> merged = new java.util.ArrayList<>();
                merged.add(parsed.get(0));
                for (int mi = 1; mi < parsed.size(); mi++) {
                    RangeOperations.PgRange last = merged.get(merged.size() - 1);
                    RangeOperations.PgRange curr = parsed.get(mi);
                    if (last.effectiveUpper() >= curr.effectiveLower()) {
                        merged.set(merged.size() - 1, RangeOperations.merge(last, curr));
                    } else {
                        merged.add(curr);
                    }
                }
                return RangeOperations.formatMultirange(merged);
            }
            case "uuid": {
                if (val instanceof java.util.UUID) return val;
                try {
                    return java.util.UUID.fromString(val.toString().trim());
                } catch (IllegalArgumentException e) {
                    throw new MemgresException("invalid input syntax for type uuid: \"" + val + "\"", "22P02");
                }
            }
            case "json":
            case "jsonb": {
                String jsonStr = val.toString();
                // Validate JSON syntax
                String trimmed = jsonStr.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("{") && !trimmed.startsWith("[")
                        && !trimmed.startsWith("\"") && !trimmed.equals("null")
                        && !trimmed.equals("true") && !trimmed.equals("false")) {
                    // Try parsing as a number
                    try { new java.math.BigDecimal(trimmed); } catch (NumberFormatException e) {
                        throw new MemgresException("invalid input syntax for type json", "22P02");
                    }
                } else if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    // Validate JSON: balanced brackets + no trailing commas
                    int depth = 0;
                    boolean inStr = false;
                    char prevNonWs = 0;
                    for (int ci = 0; ci < trimmed.length(); ci++) {
                        char ch = trimmed.charAt(ci);
                        if (inStr) { if (ch == '"' && (ci == 0 || trimmed.charAt(ci-1) != '\\')) inStr = false; }
                        else {
                            if (ch == '"') inStr = true;
                            else if (ch == '{' || ch == '[') depth++;
                            else if (ch == '}' || ch == ']') {
                                // Trailing comma: ,} or ,]
                                if (prevNonWs == ',') throw new MemgresException("invalid input syntax for type json", "22P02");
                                depth--;
                            }
                            if (!Character.isWhitespace(ch)) prevNonWs = ch;
                        }
                    }
                    if (depth != 0) throw new MemgresException("invalid input syntax for type json", "22P02");
                }
                // JSONB normalizes whitespace; JSON preserves input
                if ("jsonb".equals(typeName)) {
                    return TypeCoercion.normalizeJsonb(trimmed);
                }
                return jsonStr;
            }
            case "inet":
            case "cidr": {
                String inetStr = val.toString().trim();
                // Validate inet/cidr format: must be a valid IP address (with optional /prefix)
                if (!inetStr.isEmpty() && !(val instanceof Number)) {
                    String ipPart = inetStr.contains("/") ? inetStr.substring(0, inetStr.indexOf('/')) : inetStr;
                    // Check basic IP format: must contain dots (IPv4) or colons (IPv6)
                    if (!ipPart.contains(".") && !ipPart.contains(":")) {
                        throw new MemgresException("invalid input syntax for type inet: \"" + val + "\"", "22P02");
                    }
                }
                return inetStr;
            }
            case "macaddr":
            case "macaddr8":
                return val.toString();
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
                String relName = val.toString();
                String schemaPrefix = null;
                if (relName.contains(".")) {
                    int dotIdx = relName.lastIndexOf('.');
                    schemaPrefix = relName.substring(0, dotIdx);
                    relName = relName.substring(dotIdx + 1);
                }
                String lowerName = relName.toLowerCase();
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
                    displayName = relName;
                } else {
                    regOid = executor.systemCatalog.getOid("rel:" + executor.defaultSchema() + "." + lowerName);
                    displayName = relName;
                }
                return new RegclassValue(regOid, displayName);
            }
            case "regproc":
            case "regprocedure": {
                // ::regproc converts a function name to its OID (or just keeps it as a string for comparison)
                if (val instanceof Number) return val;
                String procName = val.toString();
                // Strip schema prefix for comparison
                if (procName.contains(".")) {
                    procName = procName.substring(procName.lastIndexOf('.') + 1);
                }
                // For regproc (no signature), check for ambiguous built-in aggregates
                if ("regproc".equals(typeName)) {
                    String lowerProc = procName.toLowerCase();
                    // Built-in aggregates that exist for multiple types (ambiguous via regproc)
                    java.util.Set<String> ambiguousBuiltins = java.util.Set.of(
                            "min", "max", "sum", "avg", "count",
                            "array_agg", "string_agg", "every", "bool_and", "bool_or");
                    if (ambiguousBuiltins.contains(lowerProc)) {
                        throw new MemgresException("more than one function named \"" + procName + "\"", "42725");
                    }
                }
                // For regprocedure (explicit signature lookup like 'fname(int)'::regprocedure),
                // validate that the function exists. regproc is used for system function references
                // (like typinput in pg_type) which may reference built-in PG functions we don't track.
                if ("regprocedure".equals(typeName)) {
                    String lookupName = procName;
                    if (lookupName.contains("(")) {
                        lookupName = lookupName.substring(0, lookupName.indexOf('(')).trim();
                    }
                    if (executor.database.getFunction(lookupName) == null && executor.database.getFunction(lookupName.toLowerCase()) == null) {
                        throw new MemgresException("function \"" + procName + "\" does not exist", "42883");
                    }
                }
                // Try to resolve to an OID for comparison with pg_proc/pg_aggregate
                // Strip argument list if present (e.g. "cfmt_fn(int)" -> "cfmt_fn")
                String oidLookupName = procName;
                if (oidLookupName.contains("(")) {
                    oidLookupName = oidLookupName.substring(0, oidLookupName.indexOf('(')).trim();
                }
                int procOid = executor.systemCatalog.getOid("proc:" + oidLookupName);
                if (procOid == 0) {
                    procOid = executor.systemCatalog.getOid("proc:" + oidLookupName.toLowerCase());
                }
                if (procOid != 0) return procOid;
                return procName;
            }
            case "regtype": {
                // ::regtype converts a type name to its OID or name
                if (val instanceof Number) {
                    int oid = ((Number) val).intValue();
                    String name = OID_TO_TYPE.get(oid);
                    return name != null ? name : String.valueOf(oid);
                }
                String rtName = val.toString().trim().toLowerCase();
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
                            dt = DataType.TIME;
                            break;
                        default:
                            dt = null;
                            break;
                    }
                }
                if (dt == null && executor.database.getDomain(rtName) == null && !executor.database.isCustomEnum(rtName)) {
                    throw new MemgresException("type \"" + val + "\" does not exist", "42704");
                }
                // Return canonical type name (e.g., 'int4' -> 'integer')
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
                    return canonical;
                }
                return val.toString();
            }
            case "oid": {
                if (val instanceof RegclassValue) return ((RegclassValue) val).oid();
                if (val instanceof RegprocValue) return ((RegprocValue) val).oid();
                return TypeCoercion.toInteger(val);
            }
            case "regnamespace": {
                // ::regnamespace converts a schema name to its OID
                if (val instanceof Number) return val;
                String nsName = val.toString().trim();
                int nsOid = executor.systemCatalog.getOid("ns:" + nsName);
                return nsOid;
            }
            default: {
                // Check if it's a domain type
                DomainType domain = executor.database.getDomain(typeName);
                if (domain != null) {
                    Object coerced = applyCast(val, domain.getBaseType().getPgName());
                    // Validate domain CHECK constraint if present
                    Expression checkExpr = domain.getParsedCheck();
                    if (checkExpr != null) {
                        // The check expression uses "value" as a column reference.
                        // Create a single-column table context with column "value"
                        Table valueTable = new Table("_domain_check", Cols.listOf(
                                new Column("value", domain.getBaseType(), true, false, null)));
                        Object[] valueRow = new Object[]{coerced};
                        RowContext checkCtx = new RowContext(valueTable, null, valueRow);
                        Object checkResult = executor.evalExpr(checkExpr, checkCtx);
                        if (checkResult instanceof Boolean && !((Boolean) checkResult)) {
                            Boolean b = (Boolean) checkResult;
                            throw new MemgresException("value for domain " + typeName + " violates check constraint \"" + typeName + "_check\"", "23514");
                        }
                    }
                    return coerced;
                }
                // Check if it's an enum type
                CustomEnum customEnum = executor.database.getCustomEnum(typeName);
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
                // ROW cast to composite type; check arity
                if (val instanceof AstExecutor.PgRow && executor.database.isCompositeType(typeName)) {
                    AstExecutor.PgRow pr = (AstExecutor.PgRow) val;
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
                    if (fields != null && pr.values().size() != fields.size()) {
                        throw new MemgresException("cannot cast type record to " + typeName, "42846");
                    }
                }
                // If this type is not a known composite either, it doesn't exist
                if (!executor.database.isCompositeType(typeName)) {
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
     * Format a regclass display name, omitting the schema prefix if the schema
     * is in the current search_path (matching PG behavior).
     * Input can be "schema.table" or just "table".
     */
    private String formatRegclassDisplay(String qualifiedName) {
        if (!qualifiedName.contains(".")) return qualifiedName;
        int dotIdx = qualifiedName.indexOf('.');
        String schema = qualifiedName.substring(0, dotIdx);
        String table = qualifiedName.substring(dotIdx + 1);
        // pg_catalog tables are never prefixed
        if ("pg_catalog".equals(schema)) return table;
        // Check if schema is in the current search_path
        if (executor.session != null) {
            List<String> searchPath = executor.session.getEffectiveSearchPath(false);
            if (searchPath.contains(schema)) return table;
        } else if ("public".equals(schema)) {
            return table;
        }
        return qualifiedName;
    }

    /** Parse array elements from an inner string (between braces), handling quoted strings. */
    private static List<String> parseArrayElements(String inner) {
        List<String> result = new ArrayList<>();
        if (inner == null || inner.trim().isEmpty()) return result;
        // Simple split by comma (doesn't handle quoted strings perfectly but works for basic cases)
        for (String part : inner.split(",", -1)) {
            result.add(part.trim());
        }
        return result;
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
            } else if (elem instanceof String) {
                String s = (String) elem;
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
}
