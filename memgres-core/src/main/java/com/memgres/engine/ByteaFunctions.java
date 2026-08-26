package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CodingErrorAction;

/**
 * Bytea function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class ByteaFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    ByteaFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    private static final java.util.Set<String> VALID_ENCODINGS = Cols.setOf(
            "UTF8", "UTF-8", "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5",
            "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10",
            "SQL_ASCII", "WIN1250", "WIN1251", "WIN1252", "WIN1253", "WIN1254",
            "WIN1255", "WIN1256", "WIN1257", "WIN1258",
            "EUC_JP", "EUC_CN", "EUC_KR", "EUC_TW",
            "SJIS", "BIG5", "GBK", "GB18030", "JOHAB", "UHC",
            "ISO_8859_5", "ISO_8859_6", "ISO_8859_7", "ISO_8859_8",
            "KOI8R", "KOI8U", "MULE_INTERNAL"
    );

    private void validateEncoding(String encoding) {
        String upper = encoding.toUpperCase();
        if (!VALID_ENCODINGS.contains(upper) && !VALID_ENCODINGS.contains(upper.replace("-", ""))) {
            throw new MemgresException("encoding \"" + encoding + "\" does not exist", "22023");
        }
    }

    /**
     * A bit index, which may be wider than an int because {@code get_bit(bytea, bigint)} is a
     * function PostgreSQL declares.
     */
    private static long bitIndex(Object given) {
        return given instanceof Number ? ((Number) given).longValue()
                : Long.parseLong(given.toString().trim());
    }

    /**
     * A byte index, which may not: PostgreSQL declares {@code get_byte} and {@code set_byte} over
     * an int alone, so a wider index names no function to call rather than a byte to read.
     */
    private int byteIndex(Object given, String function, String before, String after) {
        if (given instanceof Number) {
            long value = ((Number) given).longValue();
            if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                MemgresException e = new MemgresException("function " + function + "(" + before
                        + "bigint" + after + ") does not exist", "42883");
                e.setHint("No function matches the given name and argument types. "
                        + "You might need to add explicit type casts.");
                throw e;
            }
            return (int) value;
        }
        return executor.toInt(given);
    }

    /**
     * Check the index before an offset is worked out from it.
     *
     * <p>Dividing first truncated towards zero, so bit -1 landed in byte 0 and passed a check
     * that was only ever looking at the byte: the read answered a bit the caller never named and
     * the write went to one it never named either.
     */
    private static void requireBitInRange(long bit, int byteLength) {
        long bits = (long) byteLength * 8;
        if (bit < 0 || bit >= bits) {
            throw new MemgresException(
                    "index " + bit + " out of valid range, 0.." + (bits - 1), "2202E");
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "sha256": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // The digest is bytes. Handing back the hex text of it made the documented
                // encode(sha256(x),'hex') spell out the hex of the hex.
                return ByteaOperations.sha256(input);
            }
            case "sha384": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // The digest is bytes. Handing back the hex text of it made the documented
                // encode(sha384(x),'hex') spell out the hex of the hex.
                return ByteaOperations.sha384(input);
            }
            case "sha512": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // The digest is bytes. Handing back the hex text of it made the documented
                // encode(sha512(x),'hex') spell out the hex of the hex.
                return ByteaOperations.sha512(input);
            }
            case "get_byte": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object offset = executor.evalExpr(fn.args().get(1), ctx);
                if (data == null || offset == null) return null;
                byte[] bytes = data instanceof byte[] ? (byte[]) data : data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return ByteaOperations.getByte(bytes,
                        byteIndex(offset, "get_byte", "bytea, ", ""));
            }
            case "set_byte": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object offset = executor.evalExpr(fn.args().get(1), ctx);
                Object newByte = executor.evalExpr(fn.args().get(2), ctx);
                if (data == null || offset == null || newByte == null) return null;
                byte[] bytes = data instanceof byte[] ? (byte[]) data : data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                // set_byte(bytea, int, int) returns bytea.
                return ByteaOperations.setByte(bytes,
                        byteIndex(offset, "set_byte", "bytea, ", ", integer"),
                        executor.toInt(newByte));
            }
            case "convert_from": {
                // The encoding says what the bytes spell. Reading them as UTF-8 whatever the
                // caller named answered a different text, or refused bytes that spell a
                // perfectly good character in the encoding they were actually written in.
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object encObj = fn.args().size() > 1
                        ? executor.evalExpr(fn.args().get(1), ctx) : null;
                if (data == null || (fn.args().size() > 1 && encObj == null)) return null;
                String encoding = encObj == null ? "UTF8" : PgEncoding.named(encObj, "source");
                byte[] source = data instanceof byte[]
                        ? (byte[]) data
                        : data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return PgEncoding.decode(source, encoding);
            }
            case "convert_to": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object encObj = fn.args().size() > 1
                        ? executor.evalExpr(fn.args().get(1), ctx) : null;
                if (data == null || (fn.args().size() > 1 && encObj == null)) return null;
                String encoding = encObj == null ? "UTF8" : PgEncoding.named(encObj, "destination");
                return PgEncoding.encode(data.toString(), encoding);
            }
            case "bit_count": {
                // PG: bit_count(bytea|bitstring) -> bigint, number of set bits (popcount)
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                if (data == null) return null;
                byte[] bytes;
                if (data instanceof byte[]) bytes = (byte[]) data;
                else if (data instanceof AstExecutor.PgBitString) {
                    String bits = ((AstExecutor.PgBitString) data).bits();
                    long count = 0;
                    for (int i = 0; i < bits.length(); i++) if (bits.charAt(i) == '1') count++;
                    return count;
                } else {
                    String s = data.toString();
                    if (s.startsWith("\\x") || s.startsWith("\\X")) {
                        bytes = ByteaOperations.parseHexFormat(s);
                    } else {
                        bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    }
                }
                long count = 0;
                for (byte b : bytes) count += Integer.bitCount(b & 0xFF);
                return count;
            }
            case "get_bit": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object pos = executor.evalExpr(fn.args().get(1), ctx);
                if (data == null || pos == null) return null;
                long p = bitIndex(pos);
                if (data instanceof byte[]) {
                    byte[] bytes = (byte[]) data;
                    // bytea get_bit: PG18 numbers bits LSB-first within each byte (bit 0 = LSB of byte 0).
                    requireBitInRange(p, bytes.length);
                    int byteIdx = (int) (p / 8);
                    int bitIdx = (int) (p % 8);
                    return (bytes[byteIdx] >> bitIdx) & 1;
                }
                // For bit strings, direct character indexing
                String s = data instanceof AstExecutor.PgBitString ? ((AstExecutor.PgBitString) data).bits() : data.toString();
                // A bit string is exactly as long as it was written, so an index past either end
                // names no bit at all rather than a bit that happens to be zero.
                if (p < 0 || p >= s.length()) {
                    throw new MemgresException("bit index " + p + " out of valid range (0.."
                            + (s.length() - 1) + ")", "2202E");
                }
                return Character.getNumericValue(s.charAt((int) p));
            }
            case "set_bit": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object pos = executor.evalExpr(fn.args().get(1), ctx);
                Object newBit = executor.evalExpr(fn.args().get(2), ctx);
                if (data == null || pos == null || newBit == null) return null;
                long p = bitIndex(pos);
                int nb = executor.toInt(newBit);
                // A bit holds a 0 or a 1, so there is no third value to write into one. Reading
                // anything else as "clear the bit" quietly did something the caller never asked for.
                if (nb != 0 && nb != 1) {
                    throw new MemgresException("new bit must be 0 or 1", "22023");
                }
                if (data instanceof byte[]) {
                    byte[] bytes = (byte[]) data;
                    // bytea set_bit: PG18 numbers bits LSB-first within each byte (bit 0 = LSB of byte 0).
                    requireBitInRange(p, bytes.length);
                    int byteIdx = (int) (p / 8);
                    int bitIdx = (int) (p % 8);
                    byte[] result = bytes.clone();
                    if (nb == 1) {
                        result[byteIdx] = (byte)(result[byteIdx] | (1 << bitIdx));
                    } else {
                        result[byteIdx] = (byte)(result[byteIdx] & ~(1 << bitIdx));
                    }
                    return result;
                }
                // For bit strings
                String s = data instanceof AstExecutor.PgBitString ? ((AstExecutor.PgBitString) data).bits() : data.toString();
                char[] chars = s.toCharArray();
                if (p >= 0 && p < chars.length) chars[(int) p] = nb == 1 ? '1' : '0';
                return new AstExecutor.PgBitString(new String(chars));
            }
            default:
                return NOT_HANDLED;
        }
    }
}
