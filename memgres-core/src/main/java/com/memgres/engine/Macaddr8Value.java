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
        String s = input.trim().toLowerCase();
        byte[] bytes = null;

        // Try colon-separated
        if (s.contains(":")) {
            String[] parts = s.split(":");
            if (parts.length == 8) {
                bytes = parseHexParts(parts, 8, input);
            } else if (parts.length == 6) {
                // 6-byte: expand to 8 with ff:fe
                byte[] mac6 = parseHexParts(parts, 6, input);
                bytes = expand6to8(mac6);
            }
        }
        // Try dash-separated
        else if (s.contains("-")) {
            String[] parts = s.split("-");
            if (parts.length == 8) {
                bytes = parseHexParts(parts, 8, input);
            } else if (parts.length == 6) {
                byte[] mac6 = parseHexParts(parts, 6, input);
                bytes = expand6to8(mac6);
            }
        }
        // Try dot-separated (4 groups of 4 hex)
        else if (s.contains(".")) {
            String[] parts = s.split("\\.");
            if (parts.length == 4) {
                bytes = parseDotGroups(parts, 4, input);
            } else if (parts.length == 3) {
                // 6-byte dot format
                byte[] mac6 = parseDotGroups(parts, 3, input);
                bytes = expand6to8(mac6);
            }
        }
        // Try bare hex
        else if (s.length() == 16 && s.matches("[0-9a-f]+")) {
            bytes = new byte[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
            }
        } else if (s.length() == 12 && s.matches("[0-9a-f]+")) {
            byte[] mac6 = new byte[6];
            for (int i = 0; i < 6; i++) {
                mac6[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
            }
            bytes = expand6to8(mac6);
        }

        if (bytes == null) {
            throw new MemgresException("invalid input syntax for type macaddr8: \"" + input + "\"", "22P02");
        }
        return new Macaddr8Value(bytes);
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
            throw new MemgresException("macaddr8 data out of range to convert to macaddr", "22003");
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
