package com.memgres.engine;

import java.util.Arrays;

/**
 * Typed wrapper for PostgreSQL macaddr (6-byte MAC address).
 * Normalizes all PG-accepted input formats to canonical xx:xx:xx:xx:xx:xx lowercase.
 */
public class MacaddrValue implements Comparable<MacaddrValue> {
    private final byte[] bytes; // always 6 bytes

    public MacaddrValue(byte[] bytes) {
        if (bytes.length != 6) throw new IllegalArgumentException("macaddr must be 6 bytes");
        this.bytes = bytes.clone();
    }

    /**
     * Parse a macaddr from any PG-accepted format:
     * - xx:xx:xx:xx:xx:xx (colon-separated)
     * - xx-xx-xx-xx-xx-xx (dash-separated)
     * - xxxx.xxxx.xxxx (dot-separated groups of 4)
     * - xxxxxxxxxxxx (no separator)
     * - xxxxxx:xxxxxx (colon-separated 3-byte groups)
     * - xxxxxx-xxxxxx (dash-separated 3-byte groups)
     */
    public static MacaddrValue parse(String input) {
        String s = input.trim().toLowerCase();
        byte[] bytes = null;

        // Try colon-separated (6 parts)
        if (s.contains(":")) {
            String[] parts = s.split(":");
            if (parts.length == 6) {
                bytes = parseHexParts(parts, input);
            } else if (parts.length == 2) {
                // xxxxxx:xxxxxx
                bytes = parse2Groups(parts, 6, input);
            }
        }
        // Try dash-separated (6 parts)
        else if (s.contains("-")) {
            String[] parts = s.split("-");
            if (parts.length == 6) {
                bytes = parseHexParts(parts, input);
            } else if (parts.length == 2) {
                // xxxxxx-xxxxxx
                bytes = parse2Groups(parts, 6, input);
            }
        }
        // Try dot-separated (3 groups of 4 hex)
        else if (s.contains(".")) {
            String[] parts = s.split("\\.");
            if (parts.length == 3) {
                bytes = parse3Groups(parts, input);
            }
        }
        // Try bare hex (12 chars)
        else if (s.length() == 12 && s.matches("[0-9a-f]+")) {
            bytes = new byte[6];
            for (int i = 0; i < 6; i++) {
                bytes[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
            }
        }

        if (bytes == null) {
            throw new MemgresException("invalid input syntax for type macaddr: \"" + input + "\"", "22P02");
        }
        return new MacaddrValue(bytes);
    }

    private static byte[] parseHexParts(String[] parts, String original) {
        byte[] b = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].length() > 2) {
                throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
            }
            try {
                b[i] = (byte) Integer.parseInt(parts[i], 16);
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
            }
        }
        return b;
    }

    private static byte[] parse2Groups(String[] parts, int expectedLen, String original) {
        if (parts[0].length() != expectedLen || parts[1].length() != expectedLen) {
            throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
        }
        String combined = parts[0] + parts[1];
        byte[] b = new byte[expectedLen];
        try {
            for (int i = 0; i < expectedLen; i++) {
                b[i] = (byte) Integer.parseInt(combined.substring(i * 2, i * 2 + 2), 16);
            }
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
        }
        return b;
    }

    private static byte[] parse3Groups(String[] parts, String original) {
        if (parts[0].length() != 4 || parts[1].length() != 4 || parts[2].length() != 4) {
            throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
        }
        String combined = parts[0] + parts[1] + parts[2];
        byte[] b = new byte[6];
        try {
            for (int i = 0; i < 6; i++) {
                b[i] = (byte) Integer.parseInt(combined.substring(i * 2, i * 2 + 2), 16);
            }
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type macaddr: \"" + original + "\"", "22P02");
        }
        return b;
    }

    public byte[] getBytes() { return bytes.clone(); }
    public byte[] getBytesRef() { return bytes; }

    /** Bitwise NOT (~macaddr). */
    public MacaddrValue bitwiseNot() {
        byte[] result = new byte[6];
        for (int i = 0; i < 6; i++) result[i] = (byte) ~bytes[i];
        return new MacaddrValue(result);
    }

    /** Bitwise AND (macaddr & macaddr). */
    public MacaddrValue bitwiseAnd(MacaddrValue other) {
        byte[] result = new byte[6];
        for (int i = 0; i < 6; i++) result[i] = (byte) (bytes[i] & other.bytes[i]);
        return new MacaddrValue(result);
    }

    /** Bitwise OR (macaddr | macaddr). */
    public MacaddrValue bitwiseOr(MacaddrValue other) {
        byte[] result = new byte[6];
        for (int i = 0; i < 6; i++) result[i] = (byte) (bytes[i] | other.bytes[i]);
        return new MacaddrValue(result);
    }

    /** trunc(macaddr): set last 3 bytes to zero. */
    public MacaddrValue trunc() {
        byte[] result = bytes.clone();
        result[3] = 0; result[4] = 0; result[5] = 0;
        return new MacaddrValue(result);
    }

    /** Convert to macaddr8 by inserting ff:fe between bytes 3 and 4 (EUI-48 → EUI-64). */
    public Macaddr8Value toMacaddr8() {
        byte[] result = new byte[8];
        System.arraycopy(bytes, 0, result, 0, 3);
        result[3] = (byte) 0xFF;
        result[4] = (byte) 0xFE;
        System.arraycopy(bytes, 3, result, 5, 3);
        return new Macaddr8Value(result);
    }

    /** Canonical PG format: xx:xx:xx:xx:xx:xx lowercase. */
    @Override
    public String toString() {
        return String.format("%02x:%02x:%02x:%02x:%02x:%02x",
                bytes[0] & 0xFF, bytes[1] & 0xFF, bytes[2] & 0xFF,
                bytes[3] & 0xFF, bytes[4] & 0xFF, bytes[5] & 0xFF);
    }

    @Override
    public int compareTo(MacaddrValue other) {
        for (int i = 0; i < 6; i++) {
            int cmp = Integer.compare(bytes[i] & 0xFF, other.bytes[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MacaddrValue)) return false;
        return Arrays.equals(bytes, ((MacaddrValue) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
