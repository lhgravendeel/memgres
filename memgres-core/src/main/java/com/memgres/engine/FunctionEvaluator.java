package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;
import com.memgres.engine.util.Strs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

/**
 * Evaluates SQL function calls. Extracted from AstExecutor to reduce class size.
 */
class FunctionEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionEvaluator.class);

    // CRC-32C (Castagnoli) lookup table — bit-reversed polynomial 0x82F63B78
    private static final int[] CRC32C_TABLE = new int[256];
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++)
                crc = (crc >>> 1) ^ ((crc & 1) != 0 ? 0x82F63B78 : 0);
            CRC32C_TABLE[i] = crc;
        }
    }

    private static long crc32c(byte[] data) {
        int crc = 0xFFFFFFFF;
        for (byte b : data)
            crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ b) & 0xFF];
        return (~crc) & 0xFFFFFFFFL;
    }

    static final Object NOT_HANDLED = new Object();


    private final AstExecutor executor;
    private final MathFunctions mathFunctions;
    private final StringFunctions stringFunctions;
    private final CatalogSystemFunctions catalogSystemFunctions;
    private final JsonFunctions jsonFunctions;
    private final TextSearchFunctions textSearchFunctions;
    private final DateTimeFunctions dateTimeFunctions;
    private final XmlFunctions xmlFunctions;
    private final GeometricFunctions geometricFunctions;
    private final RangeFunctions rangeFunctions;
    private final NetworkFunctions networkFunctions;
    private final ByteaFunctions byteaFunctions;
    private final AdvisoryLockFunctions advisoryLockFunctions;

    FunctionEvaluator(AstExecutor executor) {
        this.executor = executor;
        this.mathFunctions = new MathFunctions(executor);
        this.stringFunctions = new StringFunctions(executor);
        this.catalogSystemFunctions = new CatalogSystemFunctions(executor);
        this.jsonFunctions = new JsonFunctions(executor);
        this.textSearchFunctions = new TextSearchFunctions(executor);
        this.dateTimeFunctions = new DateTimeFunctions(executor);
        this.xmlFunctions = new XmlFunctions(executor);
        this.geometricFunctions = new GeometricFunctions(executor);
        this.rangeFunctions = new RangeFunctions(executor);
        this.networkFunctions = new NetworkFunctions(executor);
        this.byteaFunctions = new ByteaFunctions(executor);
        this.advisoryLockFunctions = new AdvisoryLockFunctions(executor);
    }

    private static java.nio.charset.Charset pgEncodingToCharset(String enc) {
        String upper = enc.toUpperCase().replace("-", "").replace("_", "");
        switch (upper) {
            case "UTF8": case "UTF88": return java.nio.charset.StandardCharsets.UTF_8;
            case "LATIN1": case "ISO88591": return java.nio.charset.StandardCharsets.ISO_8859_1;
            case "LATIN2": case "ISO88592": return java.nio.charset.Charset.forName("ISO-8859-2");
            case "WIN1252": return java.nio.charset.Charset.forName("windows-1252");
            case "SQLASCII": case "ASCII": return java.nio.charset.StandardCharsets.US_ASCII;
            default: return java.nio.charset.Charset.forName(enc);
        }
    }

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types."), "42883");
        }
    }

    /**
     * Checks that a required extension is installed. PG 18 requires CREATE EXTENSION
     * before extension functions become available. Throws 42883 if not installed.
     */
    private void requireExtension(String extensionName, String functionName, int argCount) {
        if (!executor.database.hasExtension(extensionName)) {
            String sig = functionName + "(" + String.join(", ",
                    java.util.Collections.nCopies(argCount, "unknown")) + ")";
            throw new MemgresException(
                    "function " + sig + " does not exist\n" +
                    "  Hint: No function matches the given name and argument types. " +
                    "You might need to add explicit type casts.", "42883");
        }
    }

    Object evalFunction(FunctionCallExpr fn, RowContext ctx) {
        String name = fn.name().toLowerCase();
        // Strip schema prefixes for built-in function resolution
        name = stripSchemaPrefix(name);

        // Reject DEFAULT as a function argument; PG gives 42601 (syntax error)
        for (Expression arg : fn.args()) {
            if (arg instanceof Literal && ((Literal) arg).literalType() == Literal.LiteralType.DEFAULT) {
                Literal lit = (Literal) arg;
                throw new MemgresException("DEFAULT is not allowed in this context", "42601");
            }
        }

        // Expand VARIADIC args: NamedArgExpr("__variadic__", arrayExpr) → expand array to individual args
        boolean callUsedVariadic = fn.args().stream().anyMatch(a -> a instanceof NamedArgExpr && ((NamedArgExpr) a).name().equals("__variadic__"));
        if (callUsedVariadic) {
            List<Expression> expandedArgs = new ArrayList<>();
            for (Expression arg : fn.args()) {
                if (arg instanceof NamedArgExpr && ((NamedArgExpr) arg).name().equals("__variadic__")) {
                    NamedArgExpr na = (NamedArgExpr) arg;
                    // Evaluate the array and expand into individual literal args
                    Object arrVal = executor.evalExpr(na.value(), ctx);
                    if (arrVal != null) {
                        List<Object> elements = FromFunctionResolver.toElementList(arrVal);
                        for (Object elem : elements) {
                            if (elem == null) expandedArgs.add(Literal.ofNull());
                            else expandedArgs.add(Literal.ofString(elem.toString()));
                        }
                    }
                } else {
                    expandedArgs.add(arg);
                }
            }
            fn = new FunctionCallExpr(fn.name(), expandedArgs, fn.distinct(), fn.star());
        }

        // Validate named args: check for duplicates and positional-after-named
        boolean seenNamed = false;
        Set<String> namedArgNames = new java.util.HashSet<>();
        for (Expression arg : fn.args()) {
            if (arg instanceof NamedArgExpr && !((NamedArgExpr) arg).name().equals("__variadic__")) {
                NamedArgExpr na = (NamedArgExpr) arg;
                seenNamed = true;
                if (!namedArgNames.add(na.name())) {
                    throw new MemgresException("argument name \"" + na.name() + "\" used more than once", "42601");
                }
            } else if (seenNamed && !(arg instanceof NamedArgExpr)) {
                throw new MemgresException("positional argument cannot follow named argument", "42601");
            }
        }

        // Ordered-set aggregates require WITHIN GROUP clause; without it, PG gives 42601
        if (name.equals("percentile_cont") || name.equals("percentile_disc") || name.equals("mode")) {
            throw new MemgresException(
                "function " + name + " requires WITHIN GROUP (ORDER BY ...) syntax", "42601");
        }

        // VALUES is not a function; using it as a function argument is a syntax error
        if (name.equals("values")) {
            throw new MemgresException("syntax error at or near \"VALUES\"", "42601");
        }

        // Delegate to category handlers
        Object delegated;
        delegated = mathFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;
        delegated = stringFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;
        delegated = catalogSystemFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;

        switch (name) {
            case "merge_action": {
                // merge_action() returns 'INSERT', 'UPDATE', or 'DELETE' in MERGE RETURNING (PG 17+)
                if (executor.currentMergeAction == null) {
                    throw new MemgresException("merge_action() can only be used in a MERGE RETURNING clause", "42P20");
                }
                return executor.currentMergeAction;
            }
            case "row": {
                // ROW(a, b, c) -> List of evaluated values
                List<Object> row = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    row.add(executor.evalExpr(arg, ctx));
                }
                return row;
            }
            case "now":
            case "current_timestamp": {
                // now()/current_timestamp must be stable within a transaction (transaction timestamp)
                if (executor.session != null && executor.session.getTransactionTimestamp() != null) {
                    return executor.session.getTransactionTimestamp();
                }
                return executor.currentStatementTimestamp != null ? executor.currentStatementTimestamp : OffsetDateTime.now();
            }
            case "current_date":
                return LocalDate.now();
            case "current_time":
            case "localtime":
                return LocalTime.now();
            case "localtimestamp":
                return LocalDateTime.now();
            case "version":
                return "PostgreSQL 18.0";
            case "gen_random_uuid":
                return java.util.UUID.randomUUID();
            case "uuidv4":
                return java.util.UUID.randomUUID();
            case "uuid_generate_v4":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.randomUUID();
            case "uuid_generate_v1": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v1: timestamp + node (MAC) based
                // Use current time since UUID epoch (Oct 15, 1582) in 100-ns intervals
                long uuidEpochOffset = 122192928000000000L; // 100-ns intervals between UUID epoch and Unix epoch
                long timestamp = System.currentTimeMillis() * 10000L + uuidEpochOffset;
                long timeLow = timestamp & 0xFFFFFFFFL;
                long timeMid = (timestamp >>> 32) & 0xFFFFL;
                long timeHi = (timestamp >>> 48) & 0x0FFFL;
                long msb = (timeLow << 32) | (timeMid << 16) | 0x1000L | timeHi; // version 1
                // Clock sequence (random) and node (random, multicast bit set)
                java.security.SecureRandom sr = new java.security.SecureRandom();
                int clockSeq = sr.nextInt(0x3FFF);
                long node = sr.nextLong() & 0xFFFFFFFFFFL | 0x010000000000L; // set multicast bit
                long lsb = ((long)(0x80 | ((clockSeq >>> 8) & 0x3F)) << 56)
                         | ((long)(clockSeq & 0xFF) << 48)
                         | node;
                return new java.util.UUID(msb, lsb);
            }
            case "uuid_generate_v3": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v3: MD5-based namespace UUID
                requireArgs(fn, 2);
                Object nsArg = executor.evalExpr(fn.args().get(0), ctx);
                Object nameArg = executor.evalExpr(fn.args().get(1), ctx);
                if (nsArg == null || nameArg == null) return null;
                java.util.UUID namespace = nsArg instanceof java.util.UUID ? (java.util.UUID) nsArg : java.util.UUID.fromString(nsArg.toString());
                return uuid3(namespace, nameArg.toString());
            }
            case "uuid_generate_v5": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v5: SHA-1-based namespace UUID
                requireArgs(fn, 2);
                Object nsArg = executor.evalExpr(fn.args().get(0), ctx);
                Object nameArg = executor.evalExpr(fn.args().get(1), ctx);
                if (nsArg == null || nameArg == null) return null;
                java.util.UUID namespace = nsArg instanceof java.util.UUID ? (java.util.UUID) nsArg : java.util.UUID.fromString(nsArg.toString());
                return uuid5(namespace, nameArg.toString());
            }
            case "uuid_nil":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("00000000-0000-0000-0000-000000000000");
            case "uuid_ns_dns":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
            case "uuid_ns_url":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
            case "uuidv7": {
                if (!fn.args().isEmpty()) {
                    throw new MemgresException("function uuidv7(" + fn.args().stream()
                            .map(a -> { Object v = executor.evalExpr(a, ctx); return v instanceof Integer ? "integer" : "unknown"; })
                            .collect(java.util.stream.Collectors.joining(", ")) + ") does not exist", "42883");
                }
                // UUID v7: time-ordered UUID (PG18-specific)
                long timestamp = System.currentTimeMillis();
                long msb = (timestamp << 16) | 0x7000L | (long)(Math.random() * 0x0FFF);
                long lsb = 0x8000000000000000L | (long)(Math.random() * 0x3FFFFFFFFFFFFFFFL);
                return new java.util.UUID(msb, lsb);
            }
            case "crc32": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof Number) throw new MemgresException("function crc32(integer) does not exist", "42883");
                java.util.zip.CRC32 crc = new java.util.zip.CRC32();
                crc.update(arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                return crc.getValue();
            }
            case "crc32c": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof Number) throw new MemgresException("function crc32c(integer) does not exist", "42883");
                return crc32c(arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            case "gen_random_bytes": {
                requireArgs(fn, 1);
                int size = TypeCoercion.toInteger(executor.evalExpr(fn.args().get(0), ctx));
                byte[] bytes = new byte[size];
                new java.security.SecureRandom().nextBytes(bytes);
                return bytes;
            }
            case "digest": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: digest(data, type) → bytea hash
                requireArgs(fn, 2);
                Object dataArg = executor.evalExpr(fn.args().get(0), ctx);
                Object typeArg = executor.evalExpr(fn.args().get(1), ctx);
                if (dataArg == null || typeArg == null) return null;
                byte[] data;
                if (dataArg instanceof byte[]) {
                    data = (byte[]) dataArg;
                } else {
                    data = dataArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                String algo = typeArg.toString().toLowerCase().trim();
                String javaAlgo;
                switch (algo) {
                    case "md5": javaAlgo = "MD5"; break;
                    case "sha1": javaAlgo = "SHA-1"; break;
                    case "sha224": javaAlgo = "SHA-224"; break;
                    case "sha256": javaAlgo = "SHA-256"; break;
                    case "sha384": javaAlgo = "SHA-384"; break;
                    case "sha512": javaAlgo = "SHA-512"; break;
                    default: throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
                try {
                    java.security.MessageDigest md = java.security.MessageDigest.getInstance(javaAlgo);
                    return md.digest(data);
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
            }
            case "hmac": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: hmac(data, key, type) → bytea HMAC
                requireArgs(fn, 3);
                Object dataArg = executor.evalExpr(fn.args().get(0), ctx);
                Object keyArg = executor.evalExpr(fn.args().get(1), ctx);
                Object typeArg = executor.evalExpr(fn.args().get(2), ctx);
                if (dataArg == null || keyArg == null || typeArg == null) return null;
                byte[] data;
                if (dataArg instanceof byte[]) {
                    data = (byte[]) dataArg;
                } else {
                    data = dataArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                byte[] key;
                if (keyArg instanceof byte[]) {
                    key = (byte[]) keyArg;
                } else {
                    key = keyArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                String algo = typeArg.toString().toLowerCase().trim();
                String hmacAlgo;
                switch (algo) {
                    case "md5": hmacAlgo = "HmacMD5"; break;
                    case "sha1": hmacAlgo = "HmacSHA1"; break;
                    case "sha224": hmacAlgo = "HmacSHA224"; break;
                    case "sha256": hmacAlgo = "HmacSHA256"; break;
                    case "sha384": hmacAlgo = "HmacSHA384"; break;
                    case "sha512": hmacAlgo = "HmacSHA512"; break;
                    default: throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
                try {
                    javax.crypto.Mac mac = javax.crypto.Mac.getInstance(hmacAlgo);
                    mac.init(new javax.crypto.spec.SecretKeySpec(key, hmacAlgo));
                    return mac.doFinal(data);
                } catch (java.security.NoSuchAlgorithmException | java.security.InvalidKeyException e) {
                    throw new MemgresException("Cannot use \"" + algo + "\": " + e.getMessage(), "22023");
                }
            }
            case "gen_salt": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: gen_salt(type [, iter_count]) → text salt string
                requireArgs(fn, 1);
                Object typeArg = executor.evalExpr(fn.args().get(0), ctx);
                if (typeArg == null) return null;
                String saltType = typeArg.toString().toLowerCase().trim();
                java.security.SecureRandom sr = new java.security.SecureRandom();
                switch (saltType) {
                    case "bf": {
                        int rounds = fn.args().size() > 1 ? TypeCoercion.toInteger(executor.evalExpr(fn.args().get(1), ctx)) : 8;
                        byte[] saltBytes = new byte[16];
                        sr.nextBytes(saltBytes);
                        String encoded = java.util.Base64.getEncoder().encodeToString(saltBytes).substring(0, 22);
                        return "$2a$" + String.format("%02d", rounds) + "$" + encoded;
                    }
                    case "md5": {
                        byte[] saltBytes = new byte[6];
                        sr.nextBytes(saltBytes);
                        StringBuilder sb = new StringBuilder("$1$");
                        for (byte b : saltBytes) sb.append(String.format("%02x", b & 0xFF));
                        return sb.toString();
                    }
                    case "des": {
                        byte[] saltBytes = new byte[2];
                        sr.nextBytes(saltBytes);
                        String chars = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                        return "" + chars.charAt((saltBytes[0] & 0xFF) % 64) + chars.charAt((saltBytes[1] & 0xFF) % 64);
                    }
                    case "xdes": {
                        byte[] saltBytes = new byte[3];
                        sr.nextBytes(saltBytes);
                        String chars = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                        return "_" + chars.charAt((saltBytes[0] & 0xFF) % 64) + chars.charAt((saltBytes[1] & 0xFF) % 64) + chars.charAt((saltBytes[2] & 0xFF) % 64);
                    }
                    default:
                        throw new MemgresException("Unknown salt type: " + saltType, "22023");
                }
            }
            case "pg_notify": {
                // pg_notify(channel, payload): sends notification, returns void
                // Respects transaction boundaries: deferred until COMMIT, discarded on ROLLBACK
                requireArgs(fn, 2);
                Object channel = executor.evalExpr(fn.args().get(0), ctx);
                Object payload = executor.evalExpr(fn.args().get(1), ctx);
                if (channel != null) {
                    if (executor.session != null) {
                        executor.session.queueNotification(
                                channel.toString(), payload != null ? payload.toString() : "");
                    } else {
                        executor.database.getNotificationManager().notify(
                                channel.toString(), payload != null ? payload.toString() : "", 0);
                    }
                }
                return null;
            }
            case "generate_series": {
                if (fn.args().size() < 2) {
                    throw new MemgresException("function generate_series() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object startObj = executor.evalExpr(fn.args().get(0), ctx);
                Object stopObj = executor.evalExpr(fn.args().get(1), ctx);
                Object stepObj = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx) : null;
                // Date/timestamp overload: generate_series(start_date, end_date, interval)
                if (startObj instanceof LocalDate || startObj instanceof LocalDateTime) {
                    LocalDateTime start = startObj instanceof LocalDate ? ((LocalDate) startObj).atStartOfDay() : (LocalDateTime) startObj;
                    LocalDateTime stop = stopObj instanceof LocalDate ? ((LocalDate) stopObj).atStartOfDay() : TypeCoercion.toLocalDateTime(stopObj);
                    PgInterval step = stepObj != null ? TypeCoercion.toInterval(stepObj) : new PgInterval(0, 1, 0);
                    List<Object> result = new ArrayList<>();
                    LocalDateTime cur = start;
                    for (int i = 0; i < 10000 && !cur.isAfter(stop); i++) {
                        result.add(startObj instanceof LocalDate ? cur.toLocalDate() : cur);
                        cur = step.addTo(cur);
                    }
                    return result;
                }
                // Numeric overload, reject non-numeric args
                if (startObj instanceof String && !((String) startObj).isEmpty() && !Character.isDigit(((String) startObj).charAt(0)) && ((String) startObj).charAt(0) != '-') {
                    String s = (String) startObj;
                    throw new MemgresException("function generate_series(unknown, unknown) is not unique", "42725");
                }
                long start = executor.toLong(startObj);
                long stop = executor.toLong(stopObj);
                long step = stepObj != null ? executor.toLong(stepObj) : 1;
                List<Object> result = new ArrayList<>();
                if (step > 0) {
                    for (long v = start; v <= stop; v += step) {
                        result.add((v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v);
                    }
                } else if (step < 0) {
                    for (long v = start; v >= stop; v += step) {
                        result.add((v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v);
                    }
                }
                return result;
            }
            case "generate_subscripts": {
                // generate_subscripts(anyarray, dim [, reverse]) → set of integer subscripts
                if (fn.args().size() < 2) {
                    throw new MemgresException("function generate_subscripts() does not exist", "42883");
                }
                Object arrObj = executor.evalExpr(fn.args().get(0), ctx);
                int dim = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                boolean reverse = fn.args().size() > 2 && executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                int lo = 1;
                int hi;
                // Handle custom lower-bound arrays: "[lb:ub]={...}" format
                if (arrObj instanceof String && ((String) arrObj).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                    String s = (String) arrObj;
                    int eqIdx = s.indexOf('=');
                    String boundsStr = s.substring(0, eqIdx);
                    String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                    lo = Integer.parseInt(parts[0].trim());
                    hi = Integer.parseInt(parts[1].trim());
                } else if (arrObj instanceof List<?>) {
                    List<?> list = (List<?>) arrObj;
                    if (dim == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) {
                        List<?> sub = (List<?>) list.get(0);
                        hi = lo + sub.size() - 1;
                    } else if (dim == 1) {
                        hi = lo + list.size() - 1;
                    } else {
                        return Cols.listOf(); // dimension doesn't exist
                    }
                } else if (arrObj instanceof String && ((String) arrObj).startsWith("{") && ((String) arrObj).endsWith("}")) {
                    String s = (String) arrObj;
                    String inner = s.substring(1, s.length() - 1);
                    if (inner.isEmpty()) return Cols.listOf();
                    // Detect multi-dimensional arrays: inner content starts with '{'
                    if (inner.trim().startsWith("{")) {
                        // Multi-dimensional array like {{1,2},{3,4}}
                        // Count top-level sub-arrays for dim calculation
                        List<String> topLevel = splitTopLevelSubArrays(inner);
                        if (dim == 1) {
                            hi = lo + topLevel.size() - 1;
                        } else if (dim == 2 && !topLevel.isEmpty()) {
                            // Parse the first sub-array to get dimension 2 size
                            String firstSub = topLevel.get(0).trim();
                            if (firstSub.startsWith("{") && firstSub.endsWith("}")) {
                                String subInner = firstSub.substring(1, firstSub.length() - 1);
                                // Check for further nesting
                                if (subInner.trim().startsWith("{")) {
                                    List<String> subLevel = splitTopLevelSubArrays(subInner);
                                    hi = lo + subLevel.size() - 1;
                                } else {
                                    List<Object> subElems = parseSimplePgArray(firstSub);
                                    hi = lo + subElems.size() - 1;
                                }
                            } else {
                                return Cols.listOf();
                            }
                        } else if (dim > 2 && !topLevel.isEmpty()) {
                            // Navigate deeper dimensions
                            String current = topLevel.get(0).trim();
                            for (int d = 2; d < dim; d++) {
                                if (current.startsWith("{") && current.endsWith("}")) {
                                    String ci = current.substring(1, current.length() - 1);
                                    if (ci.trim().startsWith("{")) {
                                        List<String> sub = splitTopLevelSubArrays(ci);
                                        if (sub.isEmpty()) { return Cols.listOf(); }
                                        current = sub.get(0).trim();
                                    } else {
                                        return Cols.listOf(); // dimension doesn't exist
                                    }
                                } else {
                                    return Cols.listOf();
                                }
                            }
                            // current should be an array at the target dimension
                            if (current.startsWith("{") && current.endsWith("}")) {
                                String ci = current.substring(1, current.length() - 1);
                                if (ci.trim().startsWith("{")) {
                                    List<String> sub = splitTopLevelSubArrays(ci);
                                    hi = lo + sub.size() - 1;
                                } else {
                                    List<Object> subElems = parseSimplePgArray(current);
                                    hi = lo + subElems.size() - 1;
                                }
                            } else {
                                return Cols.listOf();
                            }
                        } else {
                            return Cols.listOf();
                        }
                    } else {
                        // 1D array
                        List<Object> elems = parseSimplePgArray(s);
                        if (dim == 1) {
                            hi = lo + elems.size() - 1;
                        } else {
                            return Cols.listOf();
                        }
                    }
                } else {
                    return Cols.listOf();
                }
                List<Object> result = new ArrayList<>();
                if (lo <= hi) {
                    if (reverse) {
                        for (int i = hi; i >= lo; i--) result.add(i);
                    } else {
                        for (int i = lo; i <= hi; i++) result.add(i);
                    }
                }
                return result;
            }
            case "nextval": {
                Object seqArg = executor.evalExpr(fn.args().get(0), ctx);
                String seqName;
                if (seqArg instanceof RegclassValue) {
                    RegclassValue rc = (RegclassValue) seqArg;
                    seqName = rc.name();
                } else if (seqArg instanceof Number) {
                    // OID from ::regclass, look up sequence by trying all sequences
                    int targetOid = ((Number) seqArg).intValue();
                    seqName = null;
                    for (Map.Entry<String, Sequence> entry : executor.database.getSequences().entrySet()) {
                        int seqOid = executor.systemCatalog.getOid("rel:public." + entry.getKey());
                        if (seqOid == targetOid) { seqName = entry.getKey(); break; }
                    }
                    if (seqName == null) seqName = String.valueOf(seqArg); // fallback
                } else {
                    seqName = String.valueOf(seqArg);
                }
                // Strip schema prefix if present (e.g., "public.my_seq" → "my_seq")
                if (seqName.contains(".")) seqName = seqName.substring(seqName.lastIndexOf('.') + 1);
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                long nv;
                if (executor.session != null && seq.getCache() > 1) {
                    nv = executor.session.nextvalCached(seq);
                } else {
                    nv = seq.nextVal();
                }
                executor.lastSequenceValue = nv;
                return nv;
            }
            case "currval": {
                String seqName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                if (seqName.contains(".")) seqName = seqName.substring(seqName.lastIndexOf('.') + 1);
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                return seq.currVal();
            }
            case "lastval": {
                if (executor.lastSequenceValue == null) {
                    throw new MemgresException("lastval is not yet defined in this session");
                }
                return executor.lastSequenceValue;
            }
            case "pg_sequence_last_value": {
                // Returns the last value allocated by the sequence, or NULL if never used.
                Object seqArg = executor.evalExpr(fn.args().get(0), ctx);
                String seqName;
                if (seqArg instanceof RegclassValue) {
                    seqName = ((RegclassValue) seqArg).name();
                } else if (seqArg instanceof Number) {
                    int targetOid = ((Number) seqArg).intValue();
                    seqName = null;
                    for (Map.Entry<String, Sequence> entry : executor.database.getSequences().entrySet()) {
                        int seqOid = executor.systemCatalog.getOid("rel:public." + entry.getKey());
                        if (seqOid == targetOid) { seqName = entry.getKey(); break; }
                    }
                    if (seqName == null) seqName = String.valueOf(seqArg);
                } else {
                    seqName = String.valueOf(seqArg);
                }
                if (seqName.contains(".")) seqName = seqName.substring(seqName.lastIndexOf('.') + 1);
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                try { return seq.currVal(); }
                catch (Exception e) { return null; } // never been used -> null
            }
            case "setval": {
                String seqName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                if (seqName.contains(".")) seqName = seqName.substring(seqName.lastIndexOf('.') + 1);
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                long val = executor.toLong(executor.evalExpr(fn.args().get(1), ctx));
                long result;
                if (fn.args().size() > 2) {
                    boolean isCalled = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                    result = seq.setVal(val, isCalled);
                } else {
                    result = seq.setVal(val);
                }
                // Also sync the table's serial counter if this sequence matches tableName_colName_seq
                // This ensures GENERATED ALWAYS AS IDENTITY / SERIAL columns pick up the new value
                if (seqName.contains("_seq")) {
                    String prefix = seqName.substring(0, seqName.lastIndexOf("_seq"));
                    int lastUnderscore = prefix.lastIndexOf('_');
                    if (lastUnderscore > 0) {
                        String tblName = prefix.substring(0, lastUnderscore);
                        for (Schema schema : executor.database.getSchemas().values()) {
                            Table tbl = schema.getTable(tblName);
                            if (tbl != null) {
                                tbl.resetSerialCounter(val + 1); // +1 because setval sets the last returned value
                                break;
                            }
                        }
                    }
                }
                return result;
            }
            case "coalesce": {
                // PG validates type compatibility at plan time before short-circuit evaluation.
                // Check for json vs jsonb type mismatch (PG rejects mixing these)
                boolean hasJsonFunc = false, hasJsonbCast = false;
                for (Expression arg : fn.args()) {
                    if (arg instanceof FunctionCallExpr) {
                        String fname = ((FunctionCallExpr) arg).name().toLowerCase();
                        if (fname.equals("json_arrayagg") || fname.equals("json_objectagg")
                                || fname.equals("json_array_constructor") || fname.equals("json_object_constructor")) {
                            hasJsonFunc = true;
                        }
                    }
                    if (arg instanceof CastExpr) {
                        String targetType = ((CastExpr) arg).typeName().toLowerCase();
                        if (targetType.equals("jsonb")) hasJsonbCast = true;
                    }
                }
                if (hasJsonFunc && hasJsonbCast) {
                    throw new MemgresException("could not convert type jsonb to json", "42846");
                }
                // Check for obvious mismatches: numeric literal mixed with non-numeric string literal.
                boolean hasNum = false, hasBadStr = false;
                String badVal = null;
                for (Expression arg : fn.args()) {
                    if (arg instanceof Literal) {
                        Literal lit = (Literal) arg;
                        if (lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT) {
                            hasNum = true;
                        } else if (lit.literalType() == Literal.LiteralType.STRING) {
                            try { new java.math.BigDecimal(lit.value()); } catch (NumberFormatException e) {
                                hasBadStr = true; badVal = lit.value();
                            }
                        }
                    }
                }
                if (hasNum && hasBadStr) {
                    throw new MemgresException("invalid input syntax for type integer: \"" + badVal + "\"", "22P02");
                }
                // PG uses short-circuit evaluation: stop at first non-null, don't evaluate remaining args.
                // This means `COALESCE(1, 1/0)` returns 1, not a division-by-zero error.
                Object result = null;
                for (Expression arg : fn.args()) {
                    Object v = executor.evalExpr(arg, ctx);
                    if (v != null) { result = v; break; }
                }
                return result;
            }
            case "nullif": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                validateHomogeneousTypes(Cols.listOf(a != null ? a : "", b != null ? b : ""), "NULLIF");
                return Objects.equals(a, b) ? null : a;
            }
            case "greatest": {
                if (fn.args().isEmpty()) throw new MemgresException("syntax error at or near \")\"", "42601");
                List<Object> vals = new ArrayList<>();
                for (Expression arg : fn.args()) vals.add(executor.evalExpr(arg, ctx));
                validateHomogeneousTypes(vals, "GREATEST");
                Object max = null;
                for (Object val : vals) {
                    if (val != null && (max == null || executor.compareValues(val, max) > 0)) max = val;
                }
                return max;
            }
            case "least": {
                if (fn.args().isEmpty()) throw new MemgresException("syntax error at or near \")\"", "42601");
                List<Object> vals = new ArrayList<>();
                for (Expression arg : fn.args()) vals.add(executor.evalExpr(arg, ctx));
                validateHomogeneousTypes(vals, "LEAST");
                Object min = null;
                for (Object val : vals) {
                    if (val != null && (min == null || executor.compareValues(val, min) < 0)) min = val;
                }
                return min;
            }
            case "num_nulls": {
                int count = 0;
                for (Expression arg : fn.args()) {
                    if (executor.evalExpr(arg, ctx) == null) count++;
                }
                return count;
            }
            case "num_nonnulls": {
                int count = 0;
                for (Expression arg : fn.args()) {
                    if (executor.evalExpr(arg, ctx) != null) count++;
                }
                return count;
            }
            case "age":
            case "date_part":
            case "extract":
            case "date_trunc":
            case "make_date":
            case "make_time":
            case "make_timestamp":
            case "make_timestamptz":
            case "make_interval":
            case "clock_timestamp":
            case "statement_timestamp":
            case "transaction_timestamp":
            case "timeofday":
            case "to_char":
            case "to_date":
            case "to_timestamp":
            case "to_number":
            case "justify_hours":
            case "justify_days":
            case "justify_interval":
            case "isfinite":
            case "date_bin":
                return dateTimeFunctions.eval(name, fn, ctx);
            case "timezone": {
                // timezone(zone, timestamp) → applies timezone conversion
                // Equivalent to: timestamp AT TIME ZONE zone
                requireArgs(fn, 2);
                Object zoneArg = executor.evalExpr(fn.args().get(0), ctx);
                Object tsArg = executor.evalExpr(fn.args().get(1), ctx);
                if (tsArg == null) return null;
                String zoneName = zoneArg != null ? zoneArg.toString() : "UTC";
                java.time.ZoneId zid;
                try {
                    zid = java.time.ZoneId.of(zoneName);
                } catch (java.time.DateTimeException e) {
                    throw new MemgresException("time zone \"" + zoneName + "\" not recognized", "22023");
                }
                if (tsArg instanceof OffsetDateTime) {
                    return ((OffsetDateTime) tsArg).atZoneSameInstant(zid).toLocalDateTime();
                } else if (tsArg instanceof LocalDateTime) {
                    return ((LocalDateTime) tsArg).atZone(zid).toOffsetDateTime();
                } else if (tsArg instanceof LocalTime) {
                    return tsArg;
                }
                return tsArg;
            }
            case "array_length": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                if (!(arr instanceof List<?>) && !(arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}"))) {
                    String typeName = arr instanceof Number ? "integer" : "text";
                    throw new MemgresException("function array_length(" + typeName + ", integer) does not exist", "42804");
                }
                int dim = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim < 1) return null; // dimension 0 doesn't exist
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    if (dim == 1) return list.isEmpty() ? null : list.size();
                    // Dimension 2+: check if elements are sub-arrays
                    if (dim == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) return ((List<?>) list.get(0)).size();
                    return null; // dimension doesn't exist for this array
                }
                // Handle PostgreSQL array string format: {val1,val2,...}
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    if (dim != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (inner.isEmpty()) return null; // PG returns NULL for empty arrays
                    return countArrayElements(inner);
                }
                return null;
            }
            case "array_upper": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                int dim2 = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim2 < 1) return null;
                // Handle custom lower-bound arrays: "[lb:ub]={...}" format
                if (arr instanceof String && ((String) arr).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                    String s = (String) arr;
                    int eqIdx = s.indexOf('=');
                    String boundsStr = s.substring(0, eqIdx);
                    String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                    if (dim2 == 1) return Integer.parseInt(parts[1].trim());
                    return null;
                }
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    if (dim2 == 1) return list.size();
                    if (dim2 == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) return ((List<?>) list.get(0)).size();
                    return null;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    if (dim2 != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (inner.isEmpty()) return 0;
                    return inner.split(",").length;
                }
                return null;
            }
            case "array_lower": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                int dim2 = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim2 < 1) return null;
                // Handle custom lower-bound arrays: "[lb:ub]={...}" format
                if (arr instanceof String && ((String) arr).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                    String s = (String) arr;
                    int eqIdx = s.indexOf('=');
                    String boundsStr = s.substring(0, eqIdx);
                    String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                    if (dim2 == 1) return Integer.parseInt(parts[0].trim());
                    return null;
                }
                if (arr instanceof List<?> && !((List<?>) arr).isEmpty()) {
                    List<?> list = (List<?>) arr;
                    if (dim2 == 1) return 1;
                    if (dim2 == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) return 1;
                    return null;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    if (dim2 != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (!inner.isEmpty()) return 1;
                    return null;
                }
                return null;
            }
            case "array_ndims": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                String s = arr instanceof String ? (String) arr : TypeCoercion.formatPgArray(arr instanceof List<?> ? (List<?>) arr : Cols.listOf(arr));
                if (!s.startsWith("{")) return null;
                int dims = 0;
                for (int ci = 0; ci < s.length(); ci++) {
                    if (s.charAt(ci) == '{') dims++;
                    else break;
                }
                return dims;
            }
            case "array_fill": {
                Object fillVal = executor.evalExpr(fn.args().get(0), ctx);
                Object dimsArg = executor.evalExpr(fn.args().get(1), ctx);
                if (dimsArg == null) return null;
                List<?> dimsList;
                if (dimsArg instanceof List<?>) dimsList = (List<?>) dimsArg;
                else if (dimsArg instanceof String && ((String) dimsArg).startsWith("{")) dimsList = parseSimplePgArray((String) dimsArg);
                else return null;
                String filled = buildFilledArray(fillVal, dimsList, 0);
                if (fn.args().size() > 2) {
                    Object lbArg = executor.evalExpr(fn.args().get(2), ctx);
                    if (lbArg != null) {
                        List<?> lbList;
                        if (lbArg instanceof List<?>) lbList = (List<?>) lbArg;
                        else if (lbArg instanceof String && ((String) lbArg).startsWith("{")) lbList = parseSimplePgArray((String) lbArg);
                        else lbList = null;
                        if (lbList != null) {
                            StringBuilder prefix = new StringBuilder();
                            for (int di = 0; di < dimsList.size(); di++) {
                                int lb = di < lbList.size() ? ((Number) lbList.get(di)).intValue() : 1;
                                int dimSize = ((Number) dimsList.get(di)).intValue();
                                int ub = lb + dimSize - 1;
                                prefix.append("[").append(lb).append(":").append(ub).append("]");
                            }
                            prefix.append("=");
                            filled = prefix.toString() + filled;
                        }
                    }
                }
                return filled;
            }
            case "trim_array": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object nObj = executor.evalExpr(fn.args().get(1), ctx);
                if (arr == null || nObj == null) return null;
                int n = ((Number) nObj).intValue();
                List<Object> list;
                if (arr instanceof List<?>) list = new java.util.ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new java.util.ArrayList<>(parseSimplePgArray((String) arr));
                else return arr;
                if (n < 0 || n > list.size()) throw new MemgresException("number of elements to trim must be between 0 and " + list.size(), "2202E");
                list = list.subList(0, list.size() - n);
                return TypeCoercion.formatPgArray(list);
            }
            case "array_dims": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                List<?> list = null;
                if (arr instanceof List<?>) list = (List<?>) arr;
                else if (arr instanceof String && ((String) arr).startsWith("{")) {
                    String s = (String) arr;
                    list = parseSimplePgArray(s);
                }
                if (list == null || list.isEmpty()) return null;
                StringBuilder dims = new StringBuilder("[1:" + list.size() + "]");
                // Check for multi-dimensional
                if (!list.isEmpty() && list.get(0) instanceof List<?>) {
                    dims.append("[1:").append(((List<?>) list.get(0)).size()).append("]");
                }
                return dims.toString();
            }
            case "array_sort": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                if (arr instanceof Number || arr instanceof Boolean) {
                    throw new MemgresException("function array_sort(integer) does not exist", "42883");
                }
                List<Object> list;
                if (arr instanceof List<?>) list = new ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new ArrayList<>(parseSimplePgArray(((String) arr)));
                else return arr;
                list.sort((a, b) -> TypeCoercion.compare(a, b));
                return TypeCoercion.formatPgArray(list);
            }
            case "array_reverse": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                List<Object> list;
                if (arr instanceof List<?>) list = new ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new ArrayList<>(parseSimplePgArray(((String) arr)));
                else return arr;
                java.util.Collections.reverse(list);
                return TypeCoercion.formatPgArray(list);
            }
            case "array_to_string": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object delim = executor.evalExpr(fn.args().get(1), ctx);
                // Handle string-formatted arrays like {a,b,c}
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    String inner = s.substring(1, s.length() - 1);
                    if (inner.isEmpty()) return "";
                    arr = java.util.Arrays.asList(inner.split(","));
                }
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < list.size(); i++) {
                        if (i > 0) sb.append(delim);
                        Object elem = list.get(i);
                        if (elem != null) sb.append(elem.toString().trim());
                        else if (fn.args().size() > 2) {
                            Object nullStr = executor.evalExpr(fn.args().get(2), ctx);
                            if (nullStr != null) sb.append(nullStr);
                        }
                    }
                    return sb.toString();
                }
                return null;
            }
            case "cardinality": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr instanceof List<?>) return countLeafElements((List<?>) arr);
                if (arr != null) {
                    String s = arr.toString().trim();
                    if (s.equals("{}")) return 0;
                    if (s.startsWith("{") && s.endsWith("}")) {
                        return countLeafElementsFromString(s);
                    }
                }
                return null;
            }
            case "unnest": {
                if (fn.args().isEmpty()) {
                    throw new MemgresException("function unnest() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                // unnest returns set; expand array into individual elements as a List
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr instanceof TsVector) {
                    TsVector tv = (TsVector) arr;
                    List<Object[]> rows = TextSearchOperations.unnestTsVector(tv);
                    StringBuilder sb = new StringBuilder();
                    for (Object[] row : rows) {
                        if (sb.length() > 0) sb.append(",");
                        sb.append("(").append(row[0]).append(",").append(row[1]).append(",").append(row[2]).append(")");
                    }
                    return "(" + sb + ")";
                }
                if (arr instanceof List<?>) {
                    return new ArrayList<>((List<?>) arr); // Return as List for SRF expansion
                }
                // Multirange unnest: expand into individual range strings
                if (arr instanceof String && RangeOperations.isMultirangeOrEmpty(((String) arr))) {
                    java.util.List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(((String) arr));
                    List<Object> result = new ArrayList<>();
                    for (RangeOperations.PgRange r : ranges) {
                        result.add(r.toString());
                    }
                    return result;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    List<Object> parsed = new ArrayList<>(parseSimplePgArray(s));
                    // If this is an enum array, wrap elements as PgEnum for ordinal-based sorting
                    String enumTypeName = resolveEnumTypeFromArrayArg(fn.args().get(0), ctx);
                    if (enumTypeName != null) {
                        CustomEnum ce = executor.database.getCustomEnum(enumTypeName);
                        if (ce != null) {
                            List<Object> enumList = new ArrayList<>();
                            for (Object o : parsed) {
                                String label = o.toString();
                                if (ce.isValidLabel(label)) {
                                    enumList.add(new AstExecutor.PgEnum(label, enumTypeName, ce.ordinal(label)));
                                } else {
                                    enumList.add(o);
                                }
                            }
                            return enumList;
                        }
                    }
                    return parsed;
                }
                // unnest on non-array type should error
                if (arr instanceof Number || arr instanceof Boolean) {
                    throw new MemgresException("function unnest(" +
                            (arr instanceof Integer ? "integer" : arr instanceof Long ? "bigint" : "unknown") +
                            ") does not exist", "42883");
                }
                return arr;
            }
            case "aclexplode": {
                // aclexplode(aclitem[]) -> SETOF record(grantor oid, grantee oid, privilege_type text, is_grantable boolean)
                // Returns empty set for NULL input (which is the common case since Memgres doesn't implement column-level ACLs)
                Object acl = executor.evalExpr(fn.args().get(0), ctx);
                if (acl == null) {
                    return new ArrayList<>(); // NULL ACL = no privileges = 0 rows
                }
                // Parse ACL strings like "{postgres=arwdDxt/postgres,=r/postgres}"
                List<Object> rows = new ArrayList<>();
                String aclStr = acl.toString().trim();
                if (aclStr.startsWith("{")) aclStr = aclStr.substring(1);
                if (aclStr.endsWith("}")) aclStr = aclStr.substring(0, aclStr.length() - 1);
                if (aclStr.isEmpty()) {
                    return rows;
                }
                for (String item : aclStr.split(",")) {
                    item = item.trim();
                    if (item.isEmpty()) continue;
                    // Format: grantee=privs/grantor  (empty grantee = PUBLIC)
                    int eqIdx = item.indexOf('=');
                    int slashIdx = item.indexOf('/');
                    if (eqIdx < 0 || slashIdx < 0) continue;
                    String granteeStr = item.substring(0, eqIdx);
                    String privs = item.substring(eqIdx + 1, slashIdx);
                    String grantorStr = item.substring(slashIdx + 1);
                    long grantorOid = 10; // default superuser OID
                    long granteeOid = granteeStr.isEmpty() ? 0 : 10;
                    for (int i = 0; i < privs.length(); i++) {
                        char c = privs.charAt(i);
                        boolean grantable = (i + 1 < privs.length() && privs.charAt(i + 1) == '*');
                        String privType;
                        switch (c) {
                            case 'r':
                                privType = "SELECT";
                                break;
                            case 'w':
                                privType = "UPDATE";
                                break;
                            case 'a':
                                privType = "INSERT";
                                break;
                            case 'd':
                                privType = "DELETE";
                                break;
                            case 'D':
                                privType = "TRUNCATE";
                                break;
                            case 'x':
                                privType = "REFERENCES";
                                break;
                            case 't':
                                privType = "TRIGGER";
                                break;
                            case 'X':
                                privType = "EXECUTE";
                                break;
                            case 'U':
                                privType = "USAGE";
                                break;
                            case 'C':
                                privType = "CREATE";
                                break;
                            case 'c':
                                privType = "CONNECT";
                                break;
                            case 'T':
                                privType = "TEMPORARY";
                                break;
                            case '*':
                                privType = null;
                                break;
                            default:
                                privType = null;
                                break;
                        }
                        if (privType != null) {
                            // Return as composite row: (grantor, grantee, privilege_type, is_grantable)
                            rows.add("(" + grantorOid + "," + granteeOid + "," + privType + "," + grantable + ")");
                        }
                    }
                }
                return rows;
            }
            case "array_cat": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                List<Object> result = new ArrayList<>();
                if (a instanceof List<?>) result.addAll((List<?>) a);
                if (b instanceof List<?>) result.addAll((List<?>) b);
                return result;
            }
            case "array_append": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                // Type compatibility check: if array has numeric elements, reject text element
                if (arr instanceof List<?> && !((List<?>) arr).isEmpty() && elem != null) {
                    List<?> la = (List<?>) arr;
                    Object first = la.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
                    if (first instanceof Number && elem instanceof String && !(elem instanceof Number)) {
                        try { Double.parseDouble(elem.toString()); } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type integer: \"" + elem + "\"", "22P02");
                        }
                    }
                }
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) result.addAll((List<?>) arr);
                result.add(elem);
                return result;
            }
            case "array_prepend": {
                Object elem = executor.evalExpr(fn.args().get(0), ctx);
                Object arr = executor.evalExpr(fn.args().get(1), ctx);
                List<Object> result = new ArrayList<>();
                result.add(elem);
                if (arr instanceof List<?>) result.addAll((List<?>) arr);
                return result;
            }
            case "array_remove": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) {
                    for (Object o : (List<?>) arr) {
                        if (!TypeCoercion.areEqual(o, elem)) result.add(o);
                    }
                }
                return result;
            }
            case "array_position": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                int startPos = 1;
                if (fn.args().size() > 2) {
                    Object startArg = executor.evalExpr(fn.args().get(2), ctx);
                    if (startArg != null) startPos = ((Number) startArg).intValue();
                }
                if (arr instanceof List<?>) {
                    List<?> la = (List<?>) arr;
                    for (int ai = Math.max(startPos - 1, 0); ai < la.size(); ai++) {
                        if (TypeCoercion.areEqual(la.get(ai), elem)) return ai + 1; // 1-based
                    }
                }
                return null;
            }
            case "array_positions": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                List<Object> positions = new ArrayList<>();
                if (arr instanceof List<?>) {
                    List<?> la = (List<?>) arr;
                    for (int ai = 0; ai < la.size(); ai++) {
                        if (TypeCoercion.areEqual(la.get(ai), elem)) positions.add(ai + 1);
                    }
                }
                return positions;
            }
            case "array_replace": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object oldVal = executor.evalExpr(fn.args().get(1), ctx);
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) {
                    for (Object o : (List<?>) arr) {
                        result.add(TypeCoercion.areEqual(o, oldVal) ? newVal : o);
                    }
                }
                return result;
            }
            case "__similar_to_escape__": {
                // SIMILAR TO with ESCAPE: __similar_to_escape__(str, pattern, escape_char)
                requireArgs(fn, 3);
                Object strVal = executor.evalExpr(fn.args().get(0), ctx);
                Object patVal = executor.evalExpr(fn.args().get(1), ctx);
                Object escVal = executor.evalExpr(fn.args().get(2), ctx);
                if (strVal == null || patVal == null) return null;
                String str = strVal.toString();
                String pat = patVal.toString();
                String esc = escVal != null ? escVal.toString() : "";
                // If escape is empty, disable escaping; convert SIMILAR TO pattern to regex
                String regex;
                if (esc.isEmpty()) {
                    // No escape character: convert % and _ but quote everything else for regex
                    StringBuilder sbNoEsc = new StringBuilder();
                    for (int ci = 0; ci < pat.length(); ci++) {
                        char ch = pat.charAt(ci);
                        if (ch == '%') {
                            sbNoEsc.append(".*");
                        } else if (ch == '_') {
                            sbNoEsc.append(".");
                        } else {
                            sbNoEsc.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
                        }
                    }
                    regex = sbNoEsc.toString();
                } else {
                    char escChar = esc.charAt(0);
                    StringBuilder sb = new StringBuilder();
                    for (int ci = 0; ci < pat.length(); ci++) {
                        char ch = pat.charAt(ci);
                        if (ch == escChar && ci + 1 < pat.length()) {
                            char next = pat.charAt(ci + 1);
                            // Escaped character: emit literal
                            sb.append(java.util.regex.Pattern.quote(String.valueOf(next)));
                            ci++; // skip next
                        } else if (ch == '%') {
                            sb.append(".*");
                        } else if (ch == '_') {
                            sb.append(".");
                        } else {
                            sb.append(ch);
                        }
                    }
                    regex = sb.toString();
                }
                return str.matches("(?s)" + regex);
            }
            case "overlaps": {
                // SQL OVERLAPS: (start1, end1_or_interval) OVERLAPS (start2, end2_or_interval) -> boolean
                // Each argument is a row constructor (ArrayExpr) with 2 elements
                if (fn.args().size() != 2)
                    throw new MemgresException("OVERLAPS requires exactly two range arguments", "42804");
                Expression leftExpr = fn.args().get(0);
                Expression rightExpr = fn.args().get(1);
                Object leftVal = executor.evalExpr(leftExpr, ctx);
                Object rightVal = executor.evalExpr(rightExpr, ctx);
                // Extract start/end from each side (PgRow or List)
                java.time.temporal.Temporal s1, e1, s2, e2;
                List<?> lv = extractRowValues(leftVal);
                s1 = toTemporal(lv.get(0));
                e1 = resolveOverlapEnd(s1, lv.get(1));
                List<?> rv = extractRowValues(rightVal);
                s2 = toTemporal(rv.get(0));
                e2 = resolveOverlapEnd(s2, rv.get(1));
                // SQL standard: two periods overlap iff (s1 < e2) AND (s2 < e1)
                // With the normalization that if start > end, swap them
                if (compareTemporal(s1, e1) > 0) { java.time.temporal.Temporal tmp = s1; s1 = e1; e1 = tmp; }
                if (compareTemporal(s2, e2) > 0) { java.time.temporal.Temporal tmp = s2; s2 = e2; e2 = tmp; }
                // Special case: zero-length period starting at same point as other period's start
                if (compareTemporal(s1, e1) == 0 && compareTemporal(s1, s2) == 0) return true;
                if (compareTemporal(s2, e2) == 0 && compareTemporal(s2, s1) == 0) return true;
                return compareTemporal(s1, e2) < 0 && compareTemporal(s2, e1) < 0;
            }
            case "array_sample": {
                // array_sample(arr, n) - returns n random elements from arr (PG 16+)
                // PG 18 only supports 2-arg form; 3-arg with seed does not exist
                if (fn.args().size() >= 3) {
                    throw new MemgresException(
                            "function array_sample(integer[], integer, integer) does not exist", "42883");
                }
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object nObj = executor.evalExpr(fn.args().get(1), ctx);
                if (arr == null) return null;
                int n = executor.toInt(nObj);
                List<Object> elements;
                if (arr instanceof List<?>) {
                    elements = new ArrayList<>((List<?>) arr);
                } else {
                    elements = new ArrayList<>(parseSimplePgArray(arr.toString()));
                }
                if (n <= 0) return TypeCoercion.formatPgArray(new ArrayList<>());
                if (n >= elements.size()) n = elements.size();
                java.util.Random rng = new java.util.Random();
                java.util.Collections.shuffle(elements, rng);
                return TypeCoercion.formatPgArray(new ArrayList<>(elements.subList(0, n)));
            }
            case "array_shuffle": {
                // array_shuffle(arr) - returns arr with elements in random order (PG 16+)
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                List<Object> elements;
                if (arr instanceof List<?>) {
                    elements = new ArrayList<>((List<?>) arr);
                } else {
                    elements = new ArrayList<>(parseSimplePgArray(arr.toString()));
                }
                java.util.Collections.shuffle(elements, new java.util.Random());
                return TypeCoercion.formatPgArray(elements);
            }
            case "enum_first": {
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                return ce == null ? null : ce.getLabels().get(0);
            }
            case "enum_last": {
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                return ce == null ? null : ce.getLabels().get(ce.getLabels().size() - 1);
            }
            case "enum_range": {
                if (fn.args().size() >= 2) {
                    // Bounded form: enum_range(from, to)
                    String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                    if (enumType == null) enumType = resolveEnumTypeFromArg(fn.args().get(1), ctx);
                    if (enumType == null) return null;
                    CustomEnum ce = executor.database.getCustomEnum(enumType);
                    if (ce == null) return null;
                    Object fromVal = executor.evalExpr(fn.args().get(0), ctx);
                    Object toVal = executor.evalExpr(fn.args().get(1), ctx);
                    String fromStr = fromVal == null ? null : fromVal.toString();
                    String toStr = toVal == null ? null : toVal.toString();
                    java.util.List<String> labels = ce.getLabels();
                    int startIdx = fromStr == null ? 0 : labels.indexOf(fromStr);
                    int endIdx = toStr == null ? labels.size() - 1 : labels.indexOf(toStr);
                    if (startIdx < 0) startIdx = 0;
                    if (endIdx < 0) endIdx = labels.size() - 1;
                    java.util.List<String> range = labels.subList(startIdx, endIdx + 1);
                    return "{" + String.join(",", range) + "}";
                }
                // Unbounded form: enum_range(NULL::type)
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                if (ce == null) return null;
                return "{" + String.join(",", ce.getLabels()) + "}";
            }
            case "json_scalar": {
                if (fn.args().isEmpty()) throw new MemgresException("function json_scalar requires one argument", "42883");
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return "null";
                if (val instanceof Number) return val.toString();
                if (val instanceof Boolean) return val.toString();
                // strings get quoted
                return "\"" + val.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
            }
            case "json_serialize": {
                if (fn.args().isEmpty()) throw new MemgresException("function json_serialize requires one argument", "42883");
                Expression arg = fn.args().get(0);
                Object val = executor.evalExpr(arg, ctx);
                if (val == null) return null;
                String jsonStr = val.toString();
                // JSON_SERIALIZE without RETURNING returns text (SQL standard default)
                String trimmed = jsonStr.trim();
                String normalized;
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    normalized = JsonOperations.normalizeJsonb(trimmed);
                } else {
                    normalized = jsonStr;
                }
                return normalized;
            }
            case "json_array_subquery": {
                // JSON_ARRAY(SELECT ...) — execute subquery and build JSON array from results
                if (fn.args().isEmpty()) throw new MemgresException("json_array_subquery requires a subquery argument", "42883");
                Expression subqExpr = fn.args().get(0);
                if (!(subqExpr instanceof SubqueryExpr)) {
                    throw new MemgresException("json_array_subquery requires a subquery argument", "42883");
                }
                SubqueryExpr sq = (SubqueryExpr) subqExpr;
                QueryResult result = executor.executeStatement(sq.subquery());
                // PG: JSON_ARRAY from empty subquery returns NULL
                if (result.getRows().isEmpty()) return null;
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (Object[] row : result.getRows()) {
                    if (row.length > 0 && row[0] != null) {
                        if (!first) sb.append(", ");
                        first = false;
                        appendJsonValue(sb, row[0]);
                    }
                }
                sb.append("]");
                return sb.toString();
            }
            case "json_array_constructor": {
                // Last arg is the null behavior flag: "absent_on_null" or "null_on_null"
                int argCount = fn.args().size();
                String nullBehavior = "absent"; // default: ABSENT ON NULL
                if (argCount > 0) {
                    Expression lastArg = fn.args().get(argCount - 1);
                    if (lastArg instanceof Literal) {
                        String flag = ((Literal) lastArg).value();
                        if ("absent_on_null".equals(flag) || "null_on_null".equals(flag)) {
                            nullBehavior = flag.startsWith("null") ? "null" : "absent";
                            argCount--;
                        }
                    }
                }
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (int i = 0; i < argCount; i++) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    if (val == null && "absent".equals(nullBehavior)) continue;
                    if (!first) sb.append(", ");
                    first = false;
                    appendJsonValue(sb, val);
                }
                sb.append("]");
                return sb.toString();
            }
            case "json_object_constructor": {
                // Args are key-value pairs, last two args are flags: nullBehavior, uniqueKeys
                int argCount = fn.args().size();
                String nullBehavior = "absent";
                boolean uniqueKeys = false;
                // Parse trailing flags (packed as "absent_on_null"/"null_on_null" and "unique_keys"/"no_unique_keys")
                if (argCount >= 2) {
                    Expression lastArg = fn.args().get(argCount - 1);
                    Expression secondLastArg = fn.args().get(argCount - 2);
                    if (lastArg instanceof Literal && secondLastArg instanceof Literal) {
                        String f1 = ((Literal) secondLastArg).value();
                        String f2 = ((Literal) lastArg).value();
                        if (("absent_on_null".equals(f1) || "null_on_null".equals(f1)) &&
                            ("unique_keys".equals(f2) || "no_unique_keys".equals(f2))) {
                            nullBehavior = f1.startsWith("null") ? "null" : "absent";
                            uniqueKeys = "unique_keys".equals(f2);
                            argCount -= 2;
                        }
                    }
                }
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                Set<String> seenKeys = uniqueKeys ? new HashSet<>() : null;
                for (int i = 0; i + 1 < argCount; i += 2) {
                    Object key = executor.evalExpr(fn.args().get(i), ctx);
                    Object val = executor.evalExpr(fn.args().get(i + 1), ctx);
                    if (key == null) throw new MemgresException("null value not allowed for object key", "22023");
                    if (val == null && "absent".equals(nullBehavior)) continue;
                    String keyStr = key.toString();
                    if (uniqueKeys && seenKeys != null) {
                        if (!seenKeys.add(keyStr)) {
                            throw new MemgresException("duplicate JSON object key value", "22030");
                        }
                    }
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append("\"").append(keyStr.replace("\\", "\\\\").replace("\"", "\\\"")).append("\" : ");
                    appendJsonValue(sb, val);
                }
                sb.append("}");
                return sb.toString();
            }
            case "text": {
                if (fn.args().size() != 1) {
                    throw new MemgresException("function text() requires exactly one argument", "42883");
                }
                Expression argExpr = fn.args().get(0);
                Object val = executor.evalExpr(argExpr, ctx);
                if (val == null) return null;
                // Check if the argument comes from an inet/cidr column and use network text representation
                if (argExpr instanceof ColumnRef && ctx != null) {
                    ColumnRef ref = (ColumnRef) argExpr;
                    Column colDef = ctx.resolveColumnDef(ref.table(), ref.column());
                    if (colDef != null && (colDef.getType() == DataType.INET || colDef.getType() == DataType.CIDR)) {
                        return NetworkOperations.text(val.toString());
                    }
                }
                return val.toString();
            }
            // ---- pg_trgm extension ----
            case "show_trgm": {
                requireExtension("pg_trgm", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().toLowerCase();
                Set<String> trgmSet = new java.util.TreeSet<>();
                // Pad with two spaces on each side (PG convention)
                String padded = "  " + s + " ";
                for (int i = 0; i <= padded.length() - 3; i++) {
                    trgmSet.add(padded.substring(i, i + 3));
                }
                return new ArrayList<>(trgmSet);
            }
            case "similarity": {
                requireExtension("pg_trgm", name, fn.args().size());
                requireArgs(fn, 2);
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null || arg2 == null) return 0.0;
                String s1 = arg1.toString().toLowerCase();
                String s2 = arg2.toString().toLowerCase();
                Set<String> trgm1 = trigramSet(s1);
                Set<String> trgm2 = trigramSet(s2);
                if (trgm1.isEmpty() && trgm2.isEmpty()) return 1.0;
                Set<String> intersection = new HashSet<>(trgm1);
                intersection.retainAll(trgm2);
                Set<String> union = new HashSet<>(trgm1);
                union.addAll(trgm2);
                if (union.isEmpty()) return 0.0;
                return (double) intersection.size() / (double) union.size();
            }
            // ---- cube extension ----
            case "cube": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof java.util.List<?>) {
                    java.util.List<?> list = (java.util.List<?>) arg;
                    double[] coords = new double[list.size()];
                    for (int ci = 0; ci < list.size(); ci++) {
                        Object elem = list.get(ci);
                        if (elem instanceof Number) {
                            coords[ci] = ((Number) elem).doubleValue();
                        } else {
                            coords[ci] = Double.parseDouble(elem.toString());
                        }
                    }
                    return new CubeValue(coords);
                }
                // Single scalar → 1D cube
                double v = (arg instanceof Number) ? ((Number) arg).doubleValue() : Double.parseDouble(arg.toString());
                return new CubeValue(new double[]{v});
            }
            case "cube_dim": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof CubeValue) {
                    return ((CubeValue) arg).dim();
                }
                throw new MemgresException("function cube_dim(cube) does not exist", "42883");
            }
            // ---- fuzzystrmatch extension ----
            case "levenshtein": {
                requireExtension("fuzzystrmatch", name, fn.args().size());
                requireArgs(fn, 2);
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null || arg2 == null) return null;
                return levenshteinDistance(arg1.toString(), arg2.toString());
            }
            case "soundex": {
                requireExtension("fuzzystrmatch", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return computeSoundex(arg.toString());
            }
            // ---- unaccent extension ----
            case "unaccent": {
                requireExtension("unaccent", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                // Use Java's Normalizer to decompose, then strip combining marks
                String normalized = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD);
                return normalized.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
            }
            // ---- unicode functions ----
            case "unicode_version":
                return Character.UnicodeScript.of('A').toString().isEmpty() ? "0.0" : System.getProperty("java.version").startsWith("1") ? "6.2" : "15.0";
            case "unicode_assigned": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String text = arg.toString();
                if (text.isEmpty()) return false;
                int codepoint = text.codePointAt(0);
                return Character.isDefined(codepoint);
            }
            case "__is_normalized__": {
                // IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED
                Object textVal = executor.evalExpr(fn.args().get(0), ctx);
                if (textVal == null) return null;
                String form = executor.evalExpr(fn.args().get(1), ctx).toString();
                boolean expectTrue = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                java.text.Normalizer.Form nf;
                switch (form.toUpperCase()) {
                    case "NFD": nf = java.text.Normalizer.Form.NFD; break;
                    case "NFKC": nf = java.text.Normalizer.Form.NFKC; break;
                    case "NFKD": nf = java.text.Normalizer.Form.NFKD; break;
                    default: nf = java.text.Normalizer.Form.NFC; break;
                }
                boolean normalized = java.text.Normalizer.isNormalized(textVal.toString(), nf);
                boolean result = expectTrue ? normalized : !normalized;
                return result;
            }

            // ---- server file access stubs ----
            case "pg_read_file": {
                // pg_read_file(text) / pg_read_file(text, int, int) — stub, returns empty string
                return "";
            }
            case "pg_read_binary_file": {
                // pg_read_binary_file(text) / pg_read_binary_file(text, int, int) — stub, returns empty bytea
                return new byte[0];
            }
            case "pg_stat_file": {
                // pg_stat_file(text) → record (size, access, modification, change, creation, isdir)
                // Stub: returns a record with zeroed/null fields
                List<Object> record = new ArrayList<>();
                record.add(0L);          // size
                record.add(null);        // access
                record.add(null);        // modification
                record.add(null);        // change
                record.add(null);        // creation
                record.add(false);       // isdir
                return record;
            }
            case "pg_ls_dir": {
                // pg_ls_dir(text) → set of text — stub, returns empty set
                return new ArrayList<>();
            }
            case "pg_ls_logdir":
            case "pg_ls_waldir":
            case "pg_ls_tmpdir":
            case "pg_ls_archive_statusdir": {
                // These all return set of record — stub, return empty set
                return new ArrayList<>();
            }
            case "pg_current_logfile": {
                // pg_current_logfile() → text — stub, returns empty string
                return "";
            }
            case "pg_log_backend_memory_contexts": {
                // pg_log_backend_memory_contexts(int) → boolean — stub, returns true
                return true;
            }
            case "pg_promote": {
                // pg_promote(boolean, integer) → boolean — only valid on a standby server
                throw new MemgresException("recovery is not in progress", "55000");
            }
            case "pg_safe_snapshot_blocking_pids": {
                // pg_safe_snapshot_blocking_pids(int) → int[] — stub, returns empty int array
                return "{}";
            }
            case "pg_partition_ancestors": {
                // pg_partition_ancestors(regclass) → set of regclass — returns the table itself
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return new ArrayList<>();
                List<Object> result = new ArrayList<>();
                result.add(arg);
                return result;
            }
            case "pg_partition_tree": {
                // pg_partition_tree(regclass) → set of (relid, parentrelid, isleaf, level)
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return new ArrayList<>();
                // Return the table itself as a single-entry partition tree
                List<Object> row = new ArrayList<>();
                row.add(arg);   // relid
                row.add(null);  // parentrelid (root has no parent)
                row.add(true);  // isleaf
                row.add(0);     // level
                List<List<Object>> result = new ArrayList<>();
                result.add(row);
                return result;
            }
            case "pg_stat_statements_reset": {
                throw new MemgresException("pg_stat_statements must be loaded via \"shared_preload_libraries\"", "55000");
            }

            // ---- sha224 standalone function ----
            case "sha224": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] data;
                if (arg instanceof byte[]) {
                    data = (byte[]) arg;
                } else {
                    data = arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                try {
                    java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-224");
                    return md.digest(data);
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new MemgresException("SHA-224 not available", "XX000");
                }
            }

            // ---- encoding conversion stub ----
            case "convert": {
                // convert(bytea, src_encoding, dest_encoding) → bytea — encoding conversion
                requireArgs(fn, 3);
                Object input = executor.evalExpr(fn.args().get(0), ctx);
                if (input == null) return null;
                Object srcEncObj = executor.evalExpr(fn.args().get(1), ctx);
                Object dstEncObj = executor.evalExpr(fn.args().get(2), ctx);
                String srcEnc = srcEncObj != null ? srcEncObj.toString() : "UTF8";
                String dstEnc = dstEncObj != null ? dstEncObj.toString() : "UTF8";
                byte[] data;
                if (input instanceof byte[]) {
                    data = (byte[]) input;
                } else {
                    data = input.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                try {
                    java.nio.charset.Charset srcCharset = pgEncodingToCharset(srcEnc);
                    java.nio.charset.Charset dstCharset = pgEncodingToCharset(dstEnc);
                    String decoded = new String(data, srcCharset);
                    return decoded.getBytes(dstCharset);
                } catch (Exception e) {
                    return data;
                }
            }

            // ---- enum comparison ----
            case "enum_cmp": {
                requireArgs(fn, 2);
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                // Compare by position in the enum type
                // Try to resolve the enum type from argument casts
                String enumType = null;
                for (Expression arg : fn.args()) {
                    if (arg instanceof CastExpr) {
                        enumType = ((CastExpr) arg).typeName();
                        break;
                    }
                }
                if (enumType != null) {
                    CustomEnum ce = executor.database.getCustomEnum(enumType);
                    if (ce != null) {
                        List<String> labels = ce.getLabels();
                        int posA = labels.indexOf(a.toString());
                        int posB = labels.indexOf(b.toString());
                        return Integer.compare(posA, posB);
                    }
                }
                // Fallback: lexicographic comparison
                return a.toString().compareTo(b.toString());
            }

            // ---- ICU unicode version ----
            case "icu_unicode_version": {
                // Returns the ICU unicode version string — stub
                return "15.1";
            }

            default: {
                // Delegate to domain-specific function handlers
                Object delegated2;
                delegated2 = jsonFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = textSearchFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = xmlFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = geometricFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = rangeFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = networkFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = byteaFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = advisoryLockFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;

                // Try user-defined function; resolve overloads by argument types
                PgFunction userFunc;
                {
                    // Handle schema-qualified function names (e.g., lib.helper)
                    String lookupName = name;
                    String qualifiedSchema = null;
                    if (name.contains(".")) {
                        int dot = name.indexOf('.');
                        qualifiedSchema = name.substring(0, dot);
                        lookupName = name.substring(dot + 1);
                    }
                    List<PgFunction> overloads = executor.database.getFunctionOverloads(lookupName);
                    // Filter by explicit schema when schema-qualified
                    if (qualifiedSchema != null) {
                        String qs = qualifiedSchema;
                        List<PgFunction> filtered = new ArrayList<>();
                        for (PgFunction f : overloads) {
                            String fSchema = f.getSchemaName() != null ? f.getSchemaName().toLowerCase() : "public";
                            if (qs.equalsIgnoreCase(fSchema)) filtered.add(f);
                        }
                        // For pg_catalog qualification, fall back to unfiltered (built-ins lack schema)
                        if (!filtered.isEmpty() || !"pg_catalog".equalsIgnoreCase(qs)) {
                            overloads = filtered;
                        }
                    } else if (!name.contains(".") && executor.session != null) {
                        Set<String> visibleSchemas = new LinkedHashSet<>();
                        visibleSchemas.add("pg_catalog");
                        String searchPath = executor.session.getGucSettings().get("search_path");
                        if (searchPath != null) {
                            for (String sp : searchPath.split(",")) {
                                visibleSchemas.add(sp.trim().toLowerCase());
                            }
                        }
                        visibleSchemas.add("public");
                        List<PgFunction> filtered = new ArrayList<>();
                        for (PgFunction f : overloads) {
                            String fSchema = f.getSchemaName() != null ? f.getSchemaName().toLowerCase() : "public";
                            if (visibleSchemas.contains(fSchema)) filtered.add(f);
                        }
                        // Apply filter. Fall back to unfiltered only if ALL functions have null schema (built-ins)
                        if (!filtered.isEmpty()) {
                            overloads = filtered;
                        } else {
                            boolean anyHasSchema = overloads.stream().anyMatch(f -> f.getSchemaName() != null);
                            if (anyHasSchema) {
                                overloads = filtered; // empty — function exists but not in search_path
                            }
                            // else: all have null schema (built-ins) — keep unfiltered
                        }
                    }
                    if (overloads.size() >= 1) {
                        // Resolve by evaluating argument types
                        // When named args are present, type hints are in call order which
                        // may differ from param order, so skip hints to avoid false mismatches.
                        boolean callHasNamedArgs = fn.args().stream()
                                .anyMatch(a -> a instanceof NamedArgExpr && !((NamedArgExpr) a).name().equals("__variadic__"));
                        List<String> argTypeHints = new ArrayList<>();
                        if (!callHasNamedArgs) {
                            for (Expression arg : fn.args()) {
                                // Check for explicit cast (e.g., ROW(...)::typename) - use cast type as hint
                                if (arg instanceof CastExpr) {
                                    argTypeHints.add(((CastExpr) arg).typeName());
                                    continue;
                                }
                                try {
                                    Object v = executor.evalExpr(arg, ctx);
                                    if (v instanceof Integer) argTypeHints.add("integer");
                                    else if (v instanceof Long) argTypeHints.add("bigint");
                                    else if (v instanceof String) argTypeHints.add("text");
                                    else if (v instanceof Boolean) argTypeHints.add("boolean");
                                    else if (v instanceof Double || v instanceof Float) argTypeHints.add("double precision");
                                    else argTypeHints.add(null);
                                } catch (Exception e) {
                                    argTypeHints.add(null);
                                }
                            }
                        }
                        userFunc = executor.database.resolveFunction(lookupName, fn.args().size(), argTypeHints);
                        // When explicit VARIADIC was used and the array was empty,
                        // expansion yields 0 variadic args. Resolution rejects this
                        // because it looks like no variadic args were provided.
                        // Retry with argCount+1 to simulate the empty array as one arg.
                        if (userFunc == null && callUsedVariadic && fn.args().isEmpty()) {
                            userFunc = executor.database.resolveFunction(lookupName, 1, argTypeHints);
                        }
                    } else {
                        userFunc = null;
                    }
                }
                if (userFunc != null) {
                    // Procedures cannot be called via SELECT; must use CALL
                    if (userFunc.isProcedure()) {
                        throw new MemgresException(name + " is a procedure\nHint: To call a procedure, use CALL.", "42809");
                    }
                    // Collect input params (excluding OUT)
                    List<PgFunction.Param> inputParams = new ArrayList<>();
                    boolean funcHasVariadic = false;
                    PgFunction.Param variadicParam = null;
                    for (PgFunction.Param p : userFunc.getParams()) {
                        if ("VARIADIC".equalsIgnoreCase(p.mode())) {
                            funcHasVariadic = true;
                            variadicParam = p;
                            inputParams.add(p);
                            continue;
                        }
                        if (!"OUT".equalsIgnoreCase(p.mode())) {
                            inputParams.add(p);
                        }
                    }
                    int requiredParams = 0;
                    for (PgFunction.Param p : inputParams) {
                        if (p.defaultExpr() == null) requiredParams++;
                    }

                    // Check if any arg uses named notation
                    boolean hasNamedNotation = fn.args().stream().anyMatch(a -> a instanceof NamedArgExpr);

                    if (hasNamedNotation) {
                        // Resolve named parameters: reorder args to match parameter positions
                        List<Expression> positionalArgs = new ArrayList<>();
                        Map<String, Expression> namedMap = new LinkedHashMap<>();
                        for (Expression arg : fn.args()) {
                            if (arg instanceof NamedArgExpr) {
                                NamedArgExpr na = (NamedArgExpr) arg;
                                namedMap.put(na.name().toLowerCase(), na.value());
                            } else {
                                positionalArgs.add(arg);
                            }
                        }

                        // Check for positional arg conflicting with named arg
                        for (int i = 0; i < positionalArgs.size(); i++) {
                            if (i < inputParams.size()) {
                                String paramName = inputParams.get(i).name() != null ? inputParams.get(i).name().toLowerCase() : null;
                                if (paramName != null && namedMap.containsKey(paramName)) {
                                    throw new MemgresException("function " + name + "(" +
                                            buildNamedArgTypeList(fn.args(), ctx) +
                                            ") does not exist", "42883");
                                }
                            }
                        }

                        // Check that all named args refer to valid parameter names
                        for (String argName : namedMap.keySet()) {
                            boolean found = false;
                            for (PgFunction.Param p : inputParams) {
                                if (p.name() != null && p.name().equalsIgnoreCase(argName)) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                throw new MemgresException("function " + name + "(" +
                                        buildNamedArgTypeList(fn.args(), ctx) +
                                        ") does not exist", "42883");
                            }
                        }

                        // Build reordered arg list matching parameter positions
                        List<Object> args = new ArrayList<>();
                        for (int i = 0; i < inputParams.size(); i++) {
                            PgFunction.Param p = inputParams.get(i);
                            String pName = p.name() != null ? p.name().toLowerCase() : null;

                            if (i < positionalArgs.size()) {
                                args.add(executor.evalExpr(positionalArgs.get(i), ctx));
                            } else if (pName != null && namedMap.containsKey(pName)) {
                                args.add(executor.evalExpr(namedMap.get(pName), ctx));
                            } else if (p.defaultExpr() != null) {
                                // Evaluate default expression
                                try {
                                    QueryResult defaultResult = executor.execute("SELECT " + p.defaultExpr());
                                    Object defaultVal = (!defaultResult.getRows().isEmpty() && defaultResult.getRows().get(0).length > 0)
                                            ? defaultResult.getRows().get(0)[0] : null;
                                    args.add(defaultVal);
                                } catch (Exception e) {
                                    args.add(null);
                                }
                            } else {
                                // Required parameter missing
                                throw new MemgresException("function " + name + "(" +
                                        buildNamedArgTypeList(fn.args(), ctx) +
                                        ") does not exist", "42883");
                            }
                        }

                        // STRICT: return NULL immediately if any argument is NULL
                        // For set-returning functions, return empty set instead of NULL
                        if (userFunc.isStrict()) {
                            for (Object arg : args) {
                                if (arg == null) {
                                    String rt = userFunc.getReturnType();
                                    if (rt != null && (rt.toUpperCase().startsWith("SETOF") || rt.toUpperCase().startsWith("TABLE"))) {
                                        return new java.util.ArrayList<>();
                                    }
                                    return null;
                                }
                            }
                        }
                        userFunc.incrementCallCount();
                        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                        Object result = plExec.executeFunction(userFunc, args);
                        if (result instanceof List<?>) return (List<?>) result;
                        return result;
                    }

                    // Positional-only args: validate arity
                    int actualArgs = fn.args().size();
                    if (!funcHasVariadic && (actualArgs < requiredParams || actualArgs > inputParams.size())) {
                        throw new MemgresException("function " + name + "(" +
                                fn.args().stream().map(a -> {
                                    Object v = executor.evalExpr(a, ctx);
                                    return v instanceof Integer ? "integer" : v instanceof Long ? "bigint" :
                                           v instanceof String ? "text" : "unknown";
                                }).collect(java.util.stream.Collectors.joining(", ")) +
                                ") does not exist", "42883");
                    }
                    List<Object> args = new ArrayList<>();
                    for (int i = 0; i < fn.args().size(); i++) {
                        Object val = executor.evalExpr(fn.args().get(i), ctx);
                        // Coerce argument to declared parameter type (skip VARIADIC array type)
                        if (val != null && i < inputParams.size()
                                && !"VARIADIC".equalsIgnoreCase(inputParams.get(i).mode())) {
                            String declaredType = inputParams.get(i).typeName();
                            if (declaredType != null) {
                                val = executor.castEvaluator.applyCast(val, declaredType);
                            }
                        }
                        args.add(val);
                    }
                    // STRICT: return NULL immediately if any argument is NULL
                    // For set-returning functions, return empty set instead of NULL
                    if (userFunc.isStrict()) {
                        for (Object arg : args) {
                            if (arg == null) {
                                String rt = userFunc.getReturnType();
                                if (rt != null && (rt.toUpperCase().startsWith("SETOF") || rt.toUpperCase().startsWith("TABLE"))) {
                                    return new java.util.ArrayList<>();
                                }
                                return null;
                            }
                        }
                    }
                    userFunc.incrementCallCount();
                    PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                    Object result = plExec.executeFunction(userFunc, args);
                    // Handle set-returning functions
                    if (result instanceof List<?>) {
                        // When called in scalar context, return the list as-is for FROM processing
                        return (List<?>) result;
                    }
                    return result;
                }
                // Check if it's a known aggregate function; if so, return null
                // (aggregates are handled by SelectExecutor, not FunctionEvaluator)
                Set<String> AGGREGATES = Cols.setOf("count", "sum", "avg", "min", "max",
                        "string_agg", "array_agg", "bool_and", "bool_or", "every",
                        "bit_and", "bit_or", "json_agg", "jsonb_agg",
                        "json_object_agg", "jsonb_object_agg", "xmlagg",
                        "json_arrayagg", "json_objectagg", "any_value");
                if (AGGREGATES.contains(name)) {
                    return null; // Will be handled by aggregate executor
                }
                // grouping() requires GROUPING SETS / ROLLUP / CUBE
                if (name.equals("grouping")) {
                    throw new MemgresException("GROUPING is not supported without GROUPING SETS, ROLLUP, or CUBE", "42803");
                }
                // "open" is not a SQL function; PG gives 42704 (undefined_object)
                if (name.equals("open") || name.equals("close")) {
                    throw new MemgresException("type \"" + name + "\" does not exist", "42704");
                }
                // Unknown function; build argument type list for error message
                StringBuilder argTypes = new StringBuilder();
                for (int ai = 0; ai < fn.args().size(); ai++) {
                    if (ai > 0) argTypes.append(", ");
                    try {
                        Object av = executor.evalExpr(fn.args().get(ai), ctx);
                        argTypes.append(av == null ? "unknown" :
                                av instanceof Integer ? "integer" :
                                av instanceof Long ? "bigint" :
                                av instanceof Double ? "double precision" :
                                av instanceof Boolean ? "boolean" :
                                av instanceof java.math.BigDecimal ? "numeric" :
                                "text");
                    } catch (Exception e) {
                        argTypes.append("unknown");
                    }
                }
                throw new MemgresException(
                        "function " + fn.name() + "(" + argTypes + ") does not exist", "42883");
            }
        }
    }

    // ---- JSON path delegation (used by BinaryOpEvaluator, FromResolver) ----

    String extractJsonKey(String json, String key) {
        return jsonFunctions.extractJsonKey(json, key);
    }

    private void appendJsonValue(StringBuilder sb, Object val) {
        if (val == null) { sb.append("null"); return; }
        if (val instanceof Number || val instanceof Boolean) { sb.append(val); return; }
        String s = val.toString().trim();
        if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
            sb.append(s); // already JSON
        } else {
            sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        }
    }

    List<String> evaluateJsonPathAll(String json, String path) {
        return jsonFunctions.evaluateJsonPathAll(json, path);
    }

    Boolean evaluateJsonPathPredicate(String json, String path) {
        return jsonFunctions.evaluateJsonPathPredicate(json, path);
    }

    boolean evaluateJsonPathExists(String json, String path) {
        return jsonFunctions.evaluateJsonPathExists(json, path);
    }

    // ---- Kept utility methods ----

    /**
     * Build an argument type list string for named-arg function call error messages.
     */
    private String buildNamedArgTypeList(List<Expression> args, RowContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) sb.append(", ");
            Expression arg = args.get(i);
            if (arg instanceof NamedArgExpr) {
                NamedArgExpr na = (NamedArgExpr) arg;
                sb.append(na.name()).append(" => ");
                try {
                    Object v = executor.evalExpr(na.value(), ctx);
                    sb.append(inferArgType(v));
                } catch (Exception e) {
                    sb.append("unknown");
                }
            } else {
                try {
                    Object v = executor.evalExpr(arg, ctx);
                    sb.append(inferArgType(v));
                } catch (Exception e) {
                    sb.append("unknown");
                }
            }
        }
        return sb.toString();
    }

    private static String inferArgType(Object v) {
        if (v == null) return "unknown";
        if (v instanceof Integer) return "integer";
        if (v instanceof Long) return "bigint";
        if (v instanceof Double) return "double precision";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof java.math.BigDecimal) return "numeric";
        return "text";
    }

    // ---- Enum helpers ----

    private String resolveEnumTypeFromArrayArg(Expression arg, RowContext ctx) {
        if (arg instanceof ColumnRef && ctx != null) {
            ColumnRef colRef = (ColumnRef) arg;
            Column colDef = ctx.resolveColumnDef(colRef.table(), colRef.column());
            if (colDef != null && colDef.getEnumTypeName() != null) {
                return colDef.getEnumTypeName();
            }
        }
        return null;
    }

    private String resolveEnumTypeFromArg(Expression arg, RowContext ctx) {
        if (arg instanceof CastExpr) {
            CastExpr cast = (CastExpr) arg;
            return cast.typeName();
        }
        Object val = executor.evalExpr(arg, ctx);
        return val == null ? null : val.toString();
    }

    /**
     * Resolve a sequence by name, checking session temp schema first, then global.
     */
    private Sequence resolveSequence(String seqName) {
        if (executor.session != null) {
            String tempName = executor.session.getTempSchemaName() + "." + seqName;
            Sequence seq = executor.database.getSequence(tempName);
            if (seq != null) return seq;
        }
        return executor.database.getSequence(seqName);
    }

    /**
     * Validate that all non-null values in a list have compatible types.
     */
    private void validateHomogeneousTypes(List<Object> values, String funcName) {
        Object firstNonNull = null;
        for (Object v : values) { if (v != null) { firstNonNull = v; break; } }
        if (firstNonNull == null) return;
        boolean firstIsNumeric = firstNonNull instanceof Number;
        for (Object v : values) {
            if (v == null) continue;
            if (firstIsNumeric && v instanceof String) {
                String s = (String) v;
                try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                    throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02");
                }
            } else if (firstNonNull instanceof String && v instanceof Number) {
                // First was string, second is number; PG would infer text type, numbers coerce to text, that's OK
            }
        }
    }

    /** Returns true if the string can be parsed as a number. */
    static boolean isNumericString(String s) {
        if (s == null || Strs.isBlank(s)) return false;
        String t = s.trim();
        if (t.equalsIgnoreCase("infinity") || t.equalsIgnoreCase("-infinity") || t.equalsIgnoreCase("nan")) return true;
        try { Double.parseDouble(t); return true; } catch (NumberFormatException e) { return false; }
    }

    /** Recursively count all leaf elements in a nested list. */
    private static int countLeafElements(List<?> list) {
        int count = 0;
        for (Object elem : list) {
            if (elem instanceof List<?>) count += countLeafElements((List<?>) elem);
            else count++;
        }
        return count;
    }

    /** Count all leaf elements from a PG array string like {{1,2},{3,4}}. */
    private static int countLeafElementsFromString(String s) {
        // Count commas outside of nested braces at the deepest level
        // Simple approach: strip all braces and count comma-separated elements
        String stripped = s.replaceAll("[{}]", "");
        if (stripped.isEmpty()) return 0;
        return stripped.split(",", -1).length;
    }

    /** Build a filled multi-dimensional array string. */
    private String buildFilledArray(Object fillVal, List<?> dims, int dimIdx) {
        if (dimIdx >= dims.size()) {
            return fillVal == null ? "NULL" : fillVal.toString();
        }
        int size = ((Number) dims.get(dimIdx)).intValue();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < size; i++) {
            if (i > 0) sb.append(",");
            sb.append(buildFilledArray(fillVal, dims, dimIdx + 1));
        }
        sb.append("}");
        return sb.toString();
    }

    /** Count elements in a PG array inner string, respecting quoted strings and braces. */
    static int countArrayElements(String inner) {
        if (inner.isEmpty()) return 0;
        int count = 1;
        boolean inQuote = false;
        int braceDepth = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (inQuote) {
                if (c == '\\' && i + 1 < inner.length()) { i++; continue; }
                if (c == '"') inQuote = false;
            } else {
                if (c == '"') inQuote = true;
                else if (c == '{') braceDepth++;
                else if (c == '}') braceDepth--;
                else if (c == ',' && braceDepth == 0) count++;
            }
        }
        return count;
    }

    /** Parse a simple PG array string like {a,b,c} into a List. */
    static List<Object> parseSimplePgArray(String s) {
        if (s == null || !s.startsWith("{") || !s.endsWith("}")) return Cols.listOf();
        String inner = s.substring(1, s.length() - 1).trim();
        if (inner.isEmpty()) return Cols.listOf();
        List<Object> result = new ArrayList<>();
        for (String elem : inner.split(",", -1)) {
            String trimmed = elem.trim();
            if (trimmed.equalsIgnoreCase("NULL")) result.add(null);
            else if (trimmed.startsWith("\"") && trimmed.endsWith("\""))
                result.add(trimmed.substring(1, trimmed.length() - 1));
            else {
                try { result.add(Integer.parseInt(trimmed)); }
                catch (NumberFormatException e) {
                    try { result.add(Long.parseLong(trimmed)); }
                    catch (NumberFormatException e2) { result.add(trimmed); }
                }
            }
        }
        return result;
    }

    /**
     * Split a string containing top-level comma-separated sub-arrays, respecting brace nesting.
     */
    static List<String> splitTopLevelSubArrays(String s) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') depth--;
            else if (c == ',' && depth == 0) {
                result.add(s.substring(start, i).trim());
                start = i + 1;
            }
        }
        String last = s.substring(start).trim();
        if (!last.isEmpty()) result.add(last);
        return result;
    }

    // ---- OVERLAPS helper methods ----

    private List<?> extractRowValues(Object val) {
        if (val instanceof AstExecutor.PgRow) return ((AstExecutor.PgRow) val).values();
        if (val instanceof List<?>) return (List<?>) val;
        throw new MemgresException("OVERLAPS arguments must be row constructors", "42804");
    }

    private java.time.temporal.Temporal toTemporal(Object val) {
        if (val instanceof java.time.LocalDate) return (java.time.LocalDate) val;
        if (val instanceof java.time.LocalDateTime) return (java.time.LocalDateTime) val;
        if (val instanceof java.time.OffsetDateTime) return (java.time.OffsetDateTime) val;
        if (val instanceof java.time.LocalTime) return ((java.time.LocalTime) val).atDate(java.time.LocalDate.of(1970, 1, 1));
        String s = val.toString().trim();
        try { return java.time.LocalDate.parse(s); } catch (Exception ignored) {}
        try { return java.time.LocalDateTime.parse(s.replace(" ", "T")); } catch (Exception ignored) {}
        try { return java.time.OffsetDateTime.parse(s); } catch (Exception ignored) {}
        throw new MemgresException("cannot convert value to temporal type for OVERLAPS: " + s, "42804");
    }

    private java.time.temporal.Temporal resolveOverlapEnd(java.time.temporal.Temporal start, Object endOrInterval) {
        if (endOrInterval instanceof java.time.LocalDate || endOrInterval instanceof java.time.LocalDateTime
                || endOrInterval instanceof java.time.OffsetDateTime) {
            return toTemporal(endOrInterval);
        }
        // Handle PgInterval: add interval to start date/timestamp
        if (endOrInterval instanceof PgInterval) {
            PgInterval iv = (PgInterval) endOrInterval;
            if (start instanceof java.time.LocalDate) return iv.addTo((java.time.LocalDate) start);
            if (start instanceof java.time.LocalDateTime) return iv.addTo((java.time.LocalDateTime) start);
            if (start instanceof java.time.OffsetDateTime) return iv.addTo((java.time.OffsetDateTime) start);
        }
        // Try as a date/timestamp string
        String s = endOrInterval.toString().trim();
        try { return toTemporal(endOrInterval); } catch (Exception ignored) {}
        // Try parsing as interval string and add to start
        PgInterval iv = PgInterval.parse(s);
        if (start instanceof java.time.LocalDate) return iv.addTo((java.time.LocalDate) start);
        if (start instanceof java.time.LocalDateTime) return iv.addTo((java.time.LocalDateTime) start);
        if (start instanceof java.time.OffsetDateTime) return iv.addTo((java.time.OffsetDateTime) start);
        throw new MemgresException("unsupported temporal type for OVERLAPS", "42804");
    }

    @SuppressWarnings("unchecked")
    private int compareTemporal(java.time.temporal.Temporal a, java.time.temporal.Temporal b) {
        if (a instanceof java.time.LocalDate && b instanceof java.time.LocalDate) {
            return ((java.time.LocalDate) a).compareTo((java.time.LocalDate) b);
        }
        if (a instanceof java.time.LocalDateTime && b instanceof java.time.LocalDateTime) {
            return ((java.time.LocalDateTime) a).compareTo((java.time.LocalDateTime) b);
        }
        if (a instanceof java.time.OffsetDateTime && b instanceof java.time.OffsetDateTime) {
            return ((java.time.OffsetDateTime) a).compareTo((java.time.OffsetDateTime) b);
        }
        // Mixed types: convert both to LocalDateTime for comparison
        java.time.LocalDateTime la = toLocalDateTime(a);
        java.time.LocalDateTime lb = toLocalDateTime(b);
        return la.compareTo(lb);
    }

    private java.time.LocalDateTime toLocalDateTime(java.time.temporal.Temporal t) {
        if (t instanceof java.time.LocalDateTime) return (java.time.LocalDateTime) t;
        if (t instanceof java.time.LocalDate) return ((java.time.LocalDate) t).atStartOfDay();
        if (t instanceof java.time.OffsetDateTime) return ((java.time.OffsetDateTime) t).toLocalDateTime();
        throw new MemgresException("cannot convert to LocalDateTime for comparison", "42804");
    }

    /** Strip well-known schema prefixes (pg_catalog., information_schema.) from a function name. */
    static String stripSchemaPrefix(String name) {
        if (name.startsWith("pg_catalog.")) {
            return name.substring("pg_catalog.".length());
        }
        if (name.startsWith("information_schema.")) {
            return name.substring("information_schema.".length());
        }
        return name;
    }

    // ---- UUID v3/v5 helpers ----

    private static java.util.UUID uuid3(java.util.UUID namespace, String name) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            md.update(uuidToBytes(namespace));
            md.update(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x30); // version 3
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80); // variant RFC4122
            return bytesToUuid(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new MemgresException("MD5 not available", "XX000");
        }
    }

    private static java.util.UUID uuid5(java.util.UUID namespace, String name) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-1");
            md.update(uuidToBytes(namespace));
            md.update(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x50); // version 5
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80); // variant RFC4122
            return bytesToUuid(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new MemgresException("SHA-1 not available", "XX000");
        }
    }

    private static byte[] uuidToBytes(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] out = new byte[16];
        for (int i = 0; i < 8; i++) out[i] = (byte) (msb >>> (56 - i * 8));
        for (int i = 0; i < 8; i++) out[8 + i] = (byte) (lsb >>> (56 - i * 8));
        return out;
    }

    private static java.util.UUID bytesToUuid(byte[] bytes) {
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) msb = (msb << 8) | (bytes[i] & 0xFF);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (bytes[i] & 0xFF);
        return new java.util.UUID(msb, lsb);
    }

    // ---- Trigram helper ----

    private static Set<String> trigramSet(String s) {
        Set<String> trgms = new HashSet<>();
        String padded = "  " + s + " ";
        for (int i = 0; i <= padded.length() - 3; i++) {
            trgms.add(padded.substring(i, i + 3));
        }
        return trgms;
    }

    // ---- Levenshtein distance ----

    private static int levenshteinDistance(String s, String t) {
        int m = s.length(), n = t.length();
        int[] prev = new int[n + 1], curr = new int[n + 1];
        for (int j = 0; j <= n; j++) prev[j] = j;
        for (int i = 1; i <= m; i++) {
            curr[0] = i;
            for (int j = 1; j <= n; j++) {
                int cost = s.charAt(i - 1) == t.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] tmp = prev; prev = curr; curr = tmp;
        }
        return prev[n];
    }

    // ---- Soundex ----

    private static String computeSoundex(String s) {
        if (s == null || s.isEmpty()) return "0000";
        s = s.toUpperCase();
        // Strip non-alpha
        StringBuilder alpha = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c >= 'A' && c <= 'Z') alpha.append(c);
        }
        if (alpha.length() == 0) return "0000";
        char first = alpha.charAt(0);
        String map = "01230120022455012623010202"; // A=0, B=1, C=2, ...
        StringBuilder code = new StringBuilder();
        code.append(first);
        char lastCode = map.charAt(first - 'A');
        for (int i = 1; i < alpha.length() && code.length() < 4; i++) {
            char c = alpha.charAt(i);
            char mc = map.charAt(c - 'A');
            if (mc != '0' && mc != lastCode) {
                code.append(mc);
            }
            lastCode = mc;
        }
        while (code.length() < 4) code.append('0');
        return code.toString();
    }
}
