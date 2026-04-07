package com.memgres.pgwire;

import com.memgres.engine.DataType;
import com.memgres.engine.PgInterval;
import com.memgres.engine.util.Strs;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Binary encoding/decoding for PostgreSQL wire protocol values.
 * Handles parameter decoding, DataRow encoding, and COPY binary format.
 */
class PgWireBinaryCodec {

    // ---- Decoding: binary parameters from Bind ----

    /**
     * Decode a binary parameter from Bind into a Java object (usually String).
     * Uses the parameter OID from Parse to determine the type.
     */
    static Object decodeBinaryParam(byte[] bytes, int paramOid) {
        // BYTEA (OID 17): keep as raw bytes
        if (paramOid == 17) return bytes.clone();
        // Boolean (OID 16)
        if (paramOid == 16 && bytes.length == 1) return bytes[0] != 0 ? "t" : "f";
        // Int32 (OID 23) or explicit 4-byte int
        if ((paramOid == 23 || paramOid == 0) && bytes.length == 4) {
            return String.valueOf(readInt4(bytes, 0));
        }
        // Int64 (OID 20) or explicit 8-byte long
        if ((paramOid == 20 || paramOid == 0) && bytes.length == 8) {
            return String.valueOf(readInt8(bytes, 0));
        }
        // Int16 (OID 21): 2-byte short
        if (paramOid == 21 && bytes.length == 2) {
            return String.valueOf((short) readInt2(bytes, 0));
        }
        // Float4 (OID 700): 4-byte float
        if (paramOid == 700 && bytes.length == 4) {
            return String.valueOf(Float.intBitsToFloat(readInt4(bytes, 0)));
        }
        // Float8 (OID 701): 8-byte double
        if (paramOid == 701 && bytes.length == 8) {
            return String.valueOf(Double.longBitsToDouble(readInt8(bytes, 0)));
        }
        // Numeric (OID 1700): PG binary numeric format
        if (paramOid == 1700 && bytes.length >= 8) {
            return decodeBinaryNumeric(bytes);
        }
        // Timestamp (OID 1114): 8-byte microseconds since 2000-01-01
        if (paramOid == 1114 && bytes.length == 8) {
            long microsSince2000 = readInt8(bytes, 0);
            LocalDateTime epoch = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
            return epoch.plusNanos(microsSince2000 * 1000).toString();
        }
        // Timestamptz (OID 1184): 8-byte microseconds since 2000-01-01 UTC
        if (paramOid == 1184 && bytes.length == 8) {
            long microsSince2000 = readInt8(bytes, 0);
            OffsetDateTime epoch = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            return epoch.plusNanos(microsSince2000 * 1000).toString();
        }
        // Date (OID 1082): 4-byte days since 2000-01-01
        if (paramOid == 1082 && bytes.length == 4) {
            int days = readInt4(bytes, 0);
            return LocalDate.of(2000, 1, 1).plusDays(days).toString();
        }
        // Time (OID 1083): 8-byte microseconds since midnight
        if (paramOid == 1083 && bytes.length == 8) {
            long micros = readInt8(bytes, 0);
            return LocalTime.ofNanoOfDay(micros * 1000).toString();
        }
        // UUID (OID 2950): 16 bytes
        if (paramOid == 2950 && bytes.length == 16) {
            long msb = readInt8(bytes, 0);
            long lsb = readInt8(bytes, 8);
            return new UUID(msb, lsb).toString();
        }
        // Int4Array (OID 1007): PG binary array format
        if (paramOid == 1007 && bytes.length >= 12) {
            return decodeBinaryInt4Array(bytes);
        }
        // TextArray (OID 1009): PG binary array format
        if (paramOid == 1009 && bytes.length >= 12) {
            return decodeBinaryTextArray(bytes);
        }
        // Fallback: treat as text
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ---- Decoding: binary COPY fields ----

    /** Decode a binary COPY field into a string representation. */
    static String decodeBinaryField(byte[] data, DataType type) {
        if (type == null) return new String(data, StandardCharsets.UTF_8);
        switch (type) {
            case BOOLEAN:
                return data[0] == 1 ? "t" : "f";
            case SMALLINT:
            case SMALLSERIAL:
                return Short.toString((short) readInt2(data, 0));
            case INTEGER:
            case SERIAL:
                return Integer.toString(readInt4(data, 0));
            case BIGINT:
            case BIGSERIAL:
                return Long.toString(readInt8(data, 0));
            case REAL:
                return Float.toString(Float.intBitsToFloat(readInt4(data, 0)));
            case DOUBLE_PRECISION:
                return Double.toString(Double.longBitsToDouble(readInt8(data, 0)));
            case UUID: {
                long msb = readInt8(data, 0);
                long lsb = readInt8(data, 8);
                return new UUID(msb, lsb).toString();
            }
            case NUMERIC:
                return decodeBinaryNumeric(data);
            default:
                return new String(data, StandardCharsets.UTF_8);
        }
    }

    // ---- Decoding: binary numeric ----

    /** Decode PG binary numeric format into a string representation. */
    static String decodeBinaryNumeric(byte[] bytes) {
        int offset = 0;
        int ndigits = readInt2(bytes, offset) & 0xFFFF; offset += 2;
        int weight = (short) readInt2(bytes, offset); offset += 2;
        int sign = readInt2(bytes, offset) & 0xFFFF; offset += 2;
        int dscale = readInt2(bytes, offset) & 0xFFFF; offset += 2;

        if (sign == 0xC000) return "NaN";
        if (ndigits == 0) {
            if (dscale == 0) return "0";
            return "0." + Strs.repeat("0", dscale);
        }

        int[] digits = new int[ndigits];
        for (int i = 0; i < ndigits; i++) {
            digits[i] = readInt2(bytes, offset) & 0xFFFF;
            offset += 2;
        }

        StringBuilder sb = new StringBuilder();
        if (sign == 0x4000) sb.append('-');

        int intGroups = weight + 1;
        for (int i = 0; i < intGroups; i++) {
            int d = (i < ndigits) ? digits[i] : 0;
            if (i == 0) sb.append(d);
            else sb.append(String.format("%04d", d));
        }
        if (intGroups <= 0) sb.append('0');

        if (dscale > 0) {
            sb.append('.');
            int fracDigitsWritten = 0;
            for (int i = Math.max(intGroups, 0); i < ndigits && fracDigitsWritten < dscale; i++) {
                String group = String.format("%04d", digits[i]);
                for (int j = 0; j < 4 && fracDigitsWritten < dscale; j++) {
                    sb.append(group.charAt(j));
                    fracDigitsWritten++;
                }
            }
            while (fracDigitsWritten < dscale) {
                sb.append('0');
                fracDigitsWritten++;
            }
        }

        return sb.toString();
    }

    // ---- Decoding: binary arrays ----

    private static String decodeBinaryInt4Array(byte[] bytes) {
        int offset = 0;
        int ndim = readInt4(bytes, offset); offset += 4;
        offset += 4; // flags
        offset += 4; // element OID
        if (ndim == 0) return "{}";
        int nelems = readInt4(bytes, offset); offset += 4;
        offset += 4; // lower bound
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < nelems; i++) {
            if (i > 0) sb.append(',');
            int len = readInt4(bytes, offset); offset += 4;
            if (len == -1) {
                sb.append("NULL");
            } else {
                int val = readInt4(bytes, offset); offset += len;
                sb.append(val);
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private static String decodeBinaryTextArray(byte[] bytes) {
        int offset = 0;
        int ndim = readInt4(bytes, offset); offset += 4;
        offset += 4; // flags
        offset += 4; // element OID
        if (ndim == 0) return "{}";
        int nelems = readInt4(bytes, offset); offset += 4;
        offset += 4; // lower bound
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < nelems; i++) {
            if (i > 0) sb.append(',');
            int len = readInt4(bytes, offset); offset += 4;
            if (len == -1) {
                sb.append("NULL");
            } else {
                String elem = new String(bytes, offset, len, StandardCharsets.UTF_8);
                sb.append('"').append(elem.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
                offset += len;
            }
        }
        sb.append('}');
        return sb.toString();
    }

    // ---- Encoding: binary values for DataRow (writes to ByteBuf) ----

    /**
     * Write a non-null value in PG binary format to a ByteBuf (for DataRow messages).
     * Writes the 4-byte length prefix followed by the encoded value.
     */
    static void writeBinaryValue(ByteBuf buf, Object val, DataType type) {
        try {
            switch (type) {
                case BOOLEAN: {
                    buf.writeInt(1);
                    boolean b = val instanceof Boolean ? ((Boolean) val) : Boolean.parseBoolean(val.toString());
                    buf.writeByte(b ? 1 : 0);
                    break;
                }
                case SMALLINT: {
                    buf.writeInt(2);
                    short s = val instanceof Number ? ((Number) val).shortValue() : Short.parseShort(val.toString());
                    buf.writeShort(s);
                    break;
                }
                case INTEGER:
                case SERIAL: {
                    buf.writeInt(4);
                    int iv = val instanceof Number ? ((Number) val).intValue() : Integer.parseInt(val.toString());
                    buf.writeInt(iv);
                    break;
                }
                case BIGINT:
                case BIGSERIAL: {
                    buf.writeInt(8);
                    long lv = val instanceof Number ? ((Number) val).longValue() : Long.parseLong(val.toString());
                    buf.writeLong(lv);
                    break;
                }
                case REAL: {
                    buf.writeInt(4);
                    float fv = val instanceof Number ? ((Number) val).floatValue() : Float.parseFloat(val.toString());
                    buf.writeInt(Float.floatToIntBits(fv));
                    break;
                }
                case DOUBLE_PRECISION: {
                    buf.writeInt(8);
                    double dv = val instanceof Number ? ((Number) val).doubleValue() : Double.parseDouble(val.toString());
                    buf.writeLong(Double.doubleToLongBits(dv));
                    break;
                }
                case TIMESTAMP: {
                    buf.writeInt(8);
                    LocalDateTime dt;
                    if (val instanceof LocalDateTime) dt = ((LocalDateTime) val);
                    else dt = LocalDateTime.parse(val.toString().replace(' ', 'T'));
                    LocalDateTime epoch = LocalDateTime.of(2000, 1, 1, 0, 0, 0);
                    long micros = Duration.between(epoch, dt).toNanos() / 1000;
                    buf.writeLong(micros);
                    break;
                }
                case TIMESTAMPTZ: {
                    buf.writeInt(8);
                    OffsetDateTime odt;
                    if (val instanceof OffsetDateTime) odt = ((OffsetDateTime) val);
                    else odt = OffsetDateTime.parse(val.toString());
                    OffsetDateTime epoch = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
                    long micros = Duration.between(epoch, odt).toNanos() / 1000;
                    buf.writeLong(micros);
                    break;
                }
                case DATE: {
                    buf.writeInt(4);
                    LocalDate ld;
                    if (val instanceof LocalDate) ld = ((LocalDate) val);
                    else ld = LocalDate.parse(val.toString());
                    int days = (int) java.time.temporal.ChronoUnit.DAYS.between(
                            LocalDate.of(2000, 1, 1), ld);
                    buf.writeInt(days);
                    break;
                }
                case UUID: {
                    buf.writeInt(16);
                    UUID uuid;
                    if (val instanceof UUID) uuid = ((UUID) val);
                    else uuid = UUID.fromString(val.toString());
                    buf.writeLong(uuid.getMostSignificantBits());
                    buf.writeLong(uuid.getLeastSignificantBits());
                    break;
                }
                case NUMERIC:
                case MONEY: {
                    BigDecimal bd;
                    if (val instanceof BigDecimal) bd = ((BigDecimal) val);
                    else if (val instanceof Number) bd = BigDecimal.valueOf(((Number) val).doubleValue());
                    else bd = new BigDecimal(val.toString());
                    encodeBinaryNumericToBuf(buf, bd);
                    break;
                }
                default: {
                    String text = PgWireValueFormatter.formatValue(val, null);
                    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                    buf.writeInt(bytes.length);
                    buf.writeBytes(bytes);
                    break;
                }
            }
        } catch (Exception e) {
            // If binary encoding fails, fall back to text
            String text = PgWireValueFormatter.formatValue(val, null);
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }
    }

    // ---- Encoding: binary values for COPY (returns byte[]) ----

    /** Encode a non-null value into PG binary format based on column type. Returns byte[]. */
    static byte[] encodeBinaryValue(Object val, DataType type) {
        if (type == null) type = DataType.TEXT;
        switch (type) {
            case BOOLEAN:
                return new byte[]{Boolean.TRUE.equals(val) ? (byte) 1 : (byte) 0};
            case SMALLINT:
            case SMALLSERIAL: {
                short s = (val instanceof Number) ? ((Number) val).shortValue() : Short.parseShort(val.toString());
                return new byte[]{(byte) (s >> 8), (byte) s};
            }
            case INTEGER:
            case SERIAL: {
                int v = (val instanceof Number) ? ((Number) val).intValue() : Integer.parseInt(val.toString());
                return intToBytes(v);
            }
            case BIGINT:
            case BIGSERIAL: {
                long v = (val instanceof Number) ? ((Number) val).longValue() : Long.parseLong(val.toString());
                return longToBytes(v);
            }
            case REAL: {
                float f = (val instanceof Number) ? ((Number) val).floatValue() : Float.parseFloat(val.toString());
                return intToBytes(Float.floatToIntBits(f));
            }
            case DOUBLE_PRECISION: {
                double d = (val instanceof Number) ? ((Number) val).doubleValue() : Double.parseDouble(val.toString());
                return longToBytes(Double.doubleToLongBits(d));
            }
            case NUMERIC:
                return encodeBinaryNumericToBytes(val);
            case UUID: {
                UUID uuid = (val instanceof UUID) ? ((UUID) val) : UUID.fromString(val.toString());
                byte[] b = new byte[16];
                long msb = uuid.getMostSignificantBits();
                long lsb = uuid.getLeastSignificantBits();
                for (int i = 0; i < 8; i++) {
                    b[i] = (byte) (msb >> (56 - i * 8));
                    b[i + 8] = (byte) (lsb >> (56 - i * 8));
                }
                return b;
            }
            case DATE: {
                LocalDate d = (val instanceof LocalDate) ? ((LocalDate) val) : LocalDate.parse(val.toString());
                int days = (int) (d.toEpochDay() - LocalDate.of(2000, 1, 1).toEpochDay());
                return intToBytes(days);
            }
            case TIME: {
                LocalTime t = (val instanceof LocalTime) ? ((LocalTime) val) : LocalTime.parse(val.toString());
                long micros = t.toNanoOfDay() / 1000;
                return longToBytes(micros);
            }
            case TIMESTAMP: {
                LocalDateTime dt = (val instanceof LocalDateTime) ? ((LocalDateTime) val) : LocalDateTime.parse(val.toString());
                long micros = Duration.between(LocalDateTime.of(2000, 1, 1, 0, 0, 0), dt).getSeconds() * 1_000_000L
                        + dt.getNano() / 1000;
                return longToBytes(micros);
            }
            case TIMESTAMPTZ: {
                OffsetDateTime odt = (val instanceof OffsetDateTime) ? ((OffsetDateTime) val) : OffsetDateTime.parse(val.toString());
                OffsetDateTime utc = odt.withOffsetSameInstant(ZoneOffset.UTC);
                long micros = Duration.between(
                        LocalDateTime.of(2000, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC),
                        utc).getSeconds() * 1_000_000L + utc.getNano() / 1000;
                return longToBytes(micros);
            }
            case INTERVAL: {
                PgInterval iv = (val instanceof PgInterval) ? ((PgInterval) val) : PgInterval.parse(val.toString());
                byte[] b = new byte[16];
                System.arraycopy(longToBytes(iv.getMicroseconds()), 0, b, 0, 8);
                System.arraycopy(intToBytes(iv.getDays()), 0, b, 8, 4);
                System.arraycopy(intToBytes(iv.getMonths()), 0, b, 12, 4);
                return b;
            }
            case JSONB: {
                byte[] json = PgWireValueFormatter.formatValue(val, null).getBytes(StandardCharsets.UTF_8);
                byte[] b = new byte[json.length + 1];
                b[0] = 1;
                System.arraycopy(json, 0, b, 1, json.length);
                return b;
            }
            case TEXT_ARRAY:
                return encodeBinaryArray(val, 25);
            case INT4_ARRAY:
                return encodeBinaryArray(val, 23);
            default:
                return PgWireValueFormatter.formatValue(val, null).getBytes(StandardCharsets.UTF_8);
        }
    }

    // ---- Encoding: binary numeric ----

    /** Encode a numeric/decimal value in PG binary numeric format, returning byte[]. */
    private static byte[] encodeBinaryNumericToBytes(Object val) {
        BigDecimal bd;
        if (val instanceof BigDecimal) bd = ((BigDecimal) val);
        else if (val instanceof Double && ((Double) val).isNaN()) {
            Double d = (Double) val;
            return new byte[]{0, 0, 0, 0, (byte) 0xC0, 0, 0, 0};
        }
        else bd = new BigDecimal(val.toString());

        String unscaled = bd.unscaledValue().abs().toString();
        int scale = bd.scale();
        boolean negative = bd.signum() < 0;

        int digitsBeforeDecimal = unscaled.length() - scale;
        int padFront = ((4 - (digitsBeforeDecimal % 4)) % 4);
        String padded = Strs.repeat("0", padFront) + unscaled;
        int padEnd = (4 - (padded.length() % 4)) % 4;
        padded = padded + Strs.repeat("0", padEnd);

        int ndigits = padded.length() / 4;
        while (ndigits > 0 && padded.substring((ndigits - 1) * 4, ndigits * 4).equals("0000")) {
            ndigits--;
        }
        int weight = ndigits == 0 ? 0 : (digitsBeforeDecimal + padFront) / 4 - 1;
        int dscale = Math.max(scale, 0);

        byte[] result = new byte[8 + ndigits * 2];
        result[0] = (byte) (ndigits >> 8);
        result[1] = (byte) ndigits;
        result[2] = (byte) (weight >> 8);
        result[3] = (byte) weight;
        result[4] = (byte) (negative ? 0x40 : 0x00);
        result[5] = 0;
        result[6] = (byte) (dscale >> 8);
        result[7] = (byte) dscale;
        for (int i = 0; i < ndigits; i++) {
            int digit = Integer.parseInt(padded.substring(i * 4, i * 4 + 4));
            result[8 + i * 2] = (byte) (digit >> 8);
            result[8 + i * 2 + 1] = (byte) digit;
        }
        return result;
    }

    /** Encode a BigDecimal in PG binary numeric format, writing to a ByteBuf with length prefix. */
    static void encodeBinaryNumericToBuf(ByteBuf buf, BigDecimal bd) {
        if (bd.signum() == 0) {
            buf.writeInt(8);
            buf.writeShort(0);           // ndigits
            buf.writeShort(0);           // weight
            buf.writeShort(0x0000);      // sign: positive
            buf.writeShort(bd.scale());  // dscale
            return;
        }

        int sign = bd.signum() < 0 ? 0x4000 : 0x0000;
        bd = bd.abs();
        int dscale = bd.scale();

        String unscaled = bd.unscaledValue().toString();
        int intDigitCount = unscaled.length() - dscale;

        int intPad = ((4 - (intDigitCount % 4)) % 4);
        int fracDigitCount = dscale;
        int fracPad = ((4 - (fracDigitCount % 4)) % 4);

        String padded = Strs.repeat("0", intPad) + unscaled + Strs.repeat("0", fracPad);

        int totalGroups = padded.length() / 4;
        int intGroups = (intDigitCount + intPad) / 4;

        short[] digits = new short[totalGroups];
        for (int i = 0; i < totalGroups; i++) {
            digits[i] = Short.parseShort(padded.substring(i * 4, i * 4 + 4));
        }

        int firstNonZero = 0;
        while (firstNonZero < intGroups && digits[firstNonZero] == 0) firstNonZero++;

        int lastNonZero = totalGroups - 1;
        while (lastNonZero >= intGroups && digits[lastNonZero] == 0) lastNonZero--;

        int ndigits = lastNonZero - firstNonZero + 1;
        if (ndigits < 0) ndigits = 0;

        int weight = intGroups - 1 - firstNonZero;

        int payloadSize = 8 + ndigits * 2;
        buf.writeInt(payloadSize);
        buf.writeShort(ndigits);
        buf.writeShort(weight);
        buf.writeShort(sign);
        buf.writeShort(dscale);
        for (int i = firstNonZero; i <= lastNonZero && i < totalGroups; i++) {
            buf.writeShort(digits[i]);
        }
    }

    // ---- Encoding: binary arrays ----

    /** Encode a PG array value in binary format. */
    static byte[] encodeBinaryArray(Object val, int elementOid) {
        String text = PgWireValueFormatter.formatValue(val, null);
        List<String> elements = new ArrayList<>();
        boolean hasNull = false;
        if (text.length() > 2) {
            String inner = text.substring(1, text.length() - 1);
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < inner.length(); i++) {
                char c = inner.charAt(i);
                if (inQuotes) {
                    if (c == '"' && i + 1 < inner.length() && inner.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else if (c == '"') {
                        inQuotes = false;
                    } else {
                        current.append(c);
                    }
                } else if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    String elem = current.toString();
                    if (elem.equalsIgnoreCase("NULL")) {
                        elements.add(null);
                        hasNull = true;
                    } else {
                        elements.add(elem);
                    }
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
            String elem = current.toString();
            if (elem.equalsIgnoreCase("NULL")) {
                elements.add(null);
                hasNull = true;
            } else {
                elements.add(elem);
            }
        }

        ByteArrayOutputStream bufOs = new ByteArrayOutputStream();
        writeInt32(bufOs, elements.isEmpty() ? 0 : 1);
        writeInt32(bufOs, hasNull ? 1 : 0);
        writeInt32(bufOs, elementOid);
        if (!elements.isEmpty()) {
            writeInt32(bufOs, elements.size());
            writeInt32(bufOs, 1); // lower bound
            for (String e : elements) {
                if (e == null) {
                    writeInt32(bufOs, -1);
                } else {
                    byte[] encoded;
                    if (elementOid == 23) {
                        encoded = intToBytes(Integer.parseInt(e));
                    } else {
                        encoded = e.getBytes(StandardCharsets.UTF_8);
                    }
                    writeInt32(bufOs, encoded.length);
                    try { bufOs.write(encoded); } catch (IOException ex) { throw new RuntimeException(ex); }
                }
            }
        }
        return bufOs.toByteArray();
    }

    // ---- Primitive byte helpers ----

    static int readInt2(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    static int readInt4(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3] & 0xFF);
    }

    static long readInt8(byte[] bytes, int offset) {
        long val = 0;
        for (int i = 0; i < 8; i++) val = (val << 8) | (bytes[offset + i] & 0xFF);
        return val;
    }

    static void writeInt16(ByteArrayOutputStream baos, int v) {
        baos.write((v >> 8) & 0xFF);
        baos.write(v & 0xFF);
    }

    static void writeInt32(ByteArrayOutputStream baos, int v) {
        baos.write((v >> 24) & 0xFF);
        baos.write((v >> 16) & 0xFF);
        baos.write((v >> 8) & 0xFF);
        baos.write(v & 0xFF);
    }

    static byte[] intToBytes(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    static byte[] longToBytes(long v) {
        return new byte[]{
            (byte) (v >> 56), (byte) (v >> 48), (byte) (v >> 40), (byte) (v >> 32),
            (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }
}
