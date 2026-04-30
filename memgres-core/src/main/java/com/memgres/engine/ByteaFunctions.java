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

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "sha256": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return ByteaOperations.encodeHex(ByteaOperations.sha256(input));
            }
            case "sha384": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return ByteaOperations.encodeHex(ByteaOperations.sha384(input));
            }
            case "sha512": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] input = arg instanceof byte[] ? (byte[]) arg : arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return ByteaOperations.encodeHex(ByteaOperations.sha512(input));
            }
            case "get_byte": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object offset = executor.evalExpr(fn.args().get(1), ctx);
                if (data == null) return null;
                byte[] bytes = data instanceof byte[] ? (byte[]) data : data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                return ByteaOperations.getByte(bytes, executor.toInt(offset));
            }
            case "set_byte": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object offset = executor.evalExpr(fn.args().get(1), ctx);
                Object newByte = executor.evalExpr(fn.args().get(2), ctx);
                if (data == null) return null;
                byte[] bytes = data instanceof byte[] ? (byte[]) data : data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                byte[] result = ByteaOperations.setByte(bytes, executor.toInt(offset), executor.toInt(newByte));
                return new String(result, java.nio.charset.StandardCharsets.UTF_8);
            }
            case "convert_from": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                if (data == null) return null;
                if (fn.args().size() > 1) {
                    Object enc = executor.evalExpr(fn.args().get(1), ctx);
                    if (enc != null) validateEncoding(enc.toString());
                }
                // convert_from(bytea, encoding) -> text
                if (data instanceof byte[]) {
                    byte[] ba = (byte[]) data;
                    // Validate UTF-8 encoding
                    java.nio.charset.CharsetDecoder decoder = java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                            .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
                    try {
                        return decoder.decode(java.nio.ByteBuffer.wrap(ba)).toString();
                    } catch (java.nio.charset.CharacterCodingException e) {
                        throw new MemgresException("invalid byte sequence for encoding \"UTF8\"", "22021");
                    }
                }
                return data.toString();
            }
            case "convert_to": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                if (data == null) return null;
                String encoding = "UTF-8";
                if (fn.args().size() > 1) {
                    Object enc = executor.evalExpr(fn.args().get(1), ctx);
                    if (enc != null) {
                        validateEncoding(enc.toString());
                        encoding = enc.toString().toUpperCase();
                        if (encoding.equals("UTF8")) encoding = "UTF-8";
                    }
                }
                // convert_to(text, encoding) -> bytea
                return data.toString().getBytes(java.nio.charset.Charset.forName(encoding));
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
                if (data == null) return null;
                int p = executor.toInt(pos);
                if (data instanceof byte[]) {
                    byte[] bytes = (byte[]) data;
                    // bytea get_bit: PG18 numbers bits LSB-first within each byte (bit 0 = LSB of byte 0).
                    int byteIdx = p / 8;
                    int bitIdx = p % 8;
                    if (byteIdx < 0 || byteIdx >= bytes.length) {
                        throw new MemgresException("index " + p + " out of valid range, 0.." + (bytes.length * 8 - 1), "22000");
                    }
                    return (bytes[byteIdx] >> bitIdx) & 1;
                }
                // For bit strings, direct character indexing
                String s = data instanceof AstExecutor.PgBitString ? ((AstExecutor.PgBitString) data).bits() : data.toString();
                return (p >= 0 && p < s.length()) ? Character.getNumericValue(s.charAt(p)) : 0;
            }
            case "set_bit": {
                Object data = executor.evalExpr(fn.args().get(0), ctx);
                Object pos = executor.evalExpr(fn.args().get(1), ctx);
                Object newBit = executor.evalExpr(fn.args().get(2), ctx);
                if (data == null) return null;
                int p = executor.toInt(pos);
                int nb = executor.toInt(newBit);
                if (data instanceof byte[]) {
                    byte[] bytes = (byte[]) data;
                    // bytea set_bit: PG18 numbers bits LSB-first within each byte (bit 0 = LSB of byte 0).
                    int byteIdx = p / 8;
                    int bitIdx = p % 8;
                    if (byteIdx < 0 || byteIdx >= bytes.length) {
                        throw new MemgresException("index " + p + " out of valid range, 0.." + (bytes.length * 8 - 1), "22000");
                    }
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
                if (p >= 0 && p < chars.length) chars[p] = nb == 1 ? '1' : '0';
                return new AstExecutor.PgBitString(new String(chars));
            }
            default:
                return NOT_HANDLED;
        }
    }
}
