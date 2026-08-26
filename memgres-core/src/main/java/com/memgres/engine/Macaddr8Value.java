package com.memgres.engine;

import java.util.Arrays;

/**
 * Typed wrapper for PostgreSQL macaddr8 (8-byte EUI-64 MAC address).
 * Normalizes all input formats to canonical xx:xx:xx:xx:xx:xx:xx:xx lowercase.
 */
public class Macaddr8Value implements Comparable<Macaddr8Value> {
    private final byte[] bytes; // always 8 bytes

    public Macaddr8Value(byte[] bytes) {
        if (bytes.length != 8) throw new IllegalArgumentException("macaddr8 must be 8 bytes");
        this.bytes = bytes.clone();
    }

    /**
     * Parse a macaddr8 from any PG-accepted format:
     * - xx:xx:xx:xx:xx:xx:xx:xx (colon-separated 8 parts)
     * - xx-xx-xx-xx-xx-xx-xx-xx (dash-separated 8 parts)
     * - xxxx.xxxx.xxxx.xxxx (dot-separated groups of 4)
     * - xxxxxxxxxxxxxxxx (no separator, 16 hex chars)
     * Also accepts 6-byte formats (auto-expanded with ff:fe).
     */
    public static Macaddr8Value parse(String input) {
        byte[] bytes = MacaddrGrouping.read(input, "macaddr8", 16, new int[][]{
                {16}, {2, 2, 2, 2, 2, 2, 2, 2}, {8, 8}, {4, 4, 4, 4}, {6, 10},
                // The six-byte spellings, which are widened with ff:fe below.
                {12}, {2, 2, 2, 2, 2, 2}, {6, 6}, {4, 4, 4}});
        if (bytes == null) {
            throw new MemgresException(
                    "invalid input syntax for type macaddr8: \"" + input + "\"", "22P02");
        }
        return new Macaddr8Value(bytes.length == 6 ? expand6to8(bytes) : bytes);
    }

    private static byte[] parseHexParts(String[] parts, int expected, String original) {
        byte[] b = new byte[expected];
        for (int i = 0; i < expected; i++) {
            if (parts[i].length() > 2) {
                throw new MemgresException("invalid input syntax for type macaddr8: \"" + original + "\"", "22P02");
            }
            try {
                b[i] = (byte) Integer.parseInt(parts[i], 16);
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type macaddr8: \"" + original + "\"", "22P02");
            }
        }
        return b;
    }

    private static byte[] parseDotGroups(String[] parts, int expected, String original) {
        int bytesPerGroup = (expected == 4) ? 2 : 2;
        int totalBytes = (expected == 4) ? 8 : 6;
        StringBuilder combined = new StringBuilder();
        for (String p : parts) {
            if (p.length() != 4) {
                throw new MemgresException("invalid input syntax for type macaddr8: \"" + original + "\"", "22P02");
            }
            combined.append(p);
        }
        byte[] b = new byte[totalBytes];
        try {
            String hex = combined.toString();
            for (int i = 0; i < totalBytes; i++) {
                b[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type macaddr8: \"" + original + "\"", "22P02");
        }
        return b;
    }

    /** EUI-48 to EUI-64: insert ff:fe between bytes 3 and 4. */
    private static byte[] expand6to8(byte[] mac6) {
        byte[] result = new byte[8];
        System.arraycopy(mac6, 0, result, 0, 3);
        result[3] = (byte) 0xFF;
        result[4] = (byte) 0xFE;
        System.arraycopy(mac6, 3, result, 5, 3);
        return result;
    }

    public byte[] getBytes() { return bytes.clone(); }
    public byte[] getBytesRef() { return bytes; }

    /** Bitwise NOT. */
    public Macaddr8Value bitwiseNot() {
        byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) result[i] = (byte) ~bytes[i];
        return new Macaddr8Value(result);
    }

    /** Bitwise AND. */
    public Macaddr8Value bitwiseAnd(Macaddr8Value other) {
        byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) result[i] = (byte) (bytes[i] & other.bytes[i]);
        return new Macaddr8Value(result);
    }

    /** Bitwise OR. */
    public Macaddr8Value bitwiseOr(Macaddr8Value other) {
        byte[] result = new byte[8];
        for (int i = 0; i < 8; i++) result[i] = (byte) (bytes[i] | other.bytes[i]);
        return new Macaddr8Value(result);
    }

    /** trunc(macaddr8): set last 5 bytes to zero. */
    public Macaddr8Value trunc() {
        byte[] result = bytes.clone();
        result[3] = 0; result[4] = 0; result[5] = 0; result[6] = 0; result[7] = 0;
        return new Macaddr8Value(result);
    }

    /** macaddr8_set7bit: set bit 6 (universal/local) of first octet. */
    public Macaddr8Value set7bit() {
        byte[] result = bytes.clone();
        result[0] |= 0x02;
        return new Macaddr8Value(result);
    }

    /**
     * Convert to macaddr (6-byte). PG only converts EUI-64 addresses whose middle two
     * bytes are ff:fe (the standard EUI-48 embedding); anything else is out of range.
     */
    public MacaddrValue toMacaddr() {
        if (bytes[3] != (byte) 0xFF || bytes[4] != (byte) 0xFE) {
            // PostgreSQL spells out which addresses do convert, so a caller can tell from the
            // error whether its address could ever have been one of them.
            throw new MemgresException("macaddr8 data out of range to convert to macaddr"
                    + "\n  Hint: Only addresses that have FF and FE as values in the 4th and 5th"
                    + " bytes from the left, for example xx:xx:xx:ff:fe:xx:xx:xx, are eligible to"
                    + " be converted from macaddr8 to macaddr.", "22003");
        }
        byte[] result = new byte[6];
        System.arraycopy(bytes, 0, result, 0, 3);
        System.arraycopy(bytes, 5, result, 3, 3);
        return new MacaddrValue(result);
    }

    /** Canonical PG format: xx:xx:xx:xx:xx:xx:xx:xx lowercase. */
    @Override
    public String toString() {
        return String.format("%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
                bytes[0] & 0xFF, bytes[1] & 0xFF, bytes[2] & 0xFF, bytes[3] & 0xFF,
                bytes[4] & 0xFF, bytes[5] & 0xFF, bytes[6] & 0xFF, bytes[7] & 0xFF);
    }

    @Override
    public int compareTo(Macaddr8Value other) {
        for (int i = 0; i < 8; i++) {
            int cmp = Integer.compare(bytes[i] & 0xFF, other.bytes[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Macaddr8Value)) return false;
        return Arrays.equals(bytes, ((Macaddr8Value) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
