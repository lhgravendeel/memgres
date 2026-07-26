package com.memgres.engine;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Binary data (bytea) operations.
 */
public final class ByteaOperations {
    private ByteaOperations() {}

    /** Parse bytea hex format: '\x48656c6c6f' -> byte[] */
    public static byte[] parseHexFormat(String input) {
        if (input.startsWith("\\x") || input.startsWith("\\X")) {
            String hex = input.substring(2).replaceAll("\\s+", ""); // PG allows whitespace in hex
            if (hex.length() % 2 != 0) {
                throw new MemgresException("invalid hexadecimal data: odd number of digits", "22023");
            }
            // Validate hex digits
            for (int i = 0; i < hex.length(); i++) {
                if (Character.digit(hex.charAt(i), 16) < 0) {
                    throw new MemgresException("invalid hexadecimal digit: \"" + hex.charAt(i) + "\"", "22023");
                }
            }
            return hexToBytes(hex);
        }
        return input.getBytes(StandardCharsets.UTF_8);
    }

    /** Parse bytea escape format: '\000\047hello' -> byte[] with octal escapes */
    public static byte[] parseEscapeFormat(String input) {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        int i = 0;
        while (i < input.length()) {
            char c = input.charAt(i);
            if (c == '\\') {
                if (i + 1 < input.length() && input.charAt(i + 1) == '\\') {
                    baos.write('\\');
                    i += 2;
                } else if (i + 3 < input.length()
                        && input.charAt(i + 1) >= '0' && input.charAt(i + 1) <= '3'
                        && input.charAt(i + 2) >= '0' && input.charAt(i + 2) <= '7'
                        && input.charAt(i + 3) >= '0' && input.charAt(i + 3) <= '7') {
                    int val = (input.charAt(i + 1) - '0') * 64
                            + (input.charAt(i + 2) - '0') * 8
                            + (input.charAt(i + 3) - '0');
                    baos.write(val);
                    i += 4;
                } else {
                    // PG's escape format only knows \\ and \ooo; anything else is a syntax error
                    throw new MemgresException("invalid input syntax for type bytea", "22P02");
                }
            } else {
                baos.write(c);
                i++;
            }
        }
        return baos.toByteArray();
    }

    /** Encode bytes to hex string */
    public static String encodeHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder("\\x");
        for (byte b : bytes) sb.append(String.format("%02x", b & 0xFF));
        return sb.toString();
    }

    /** get_byte(bytea, int) -> int */
    public static int getByte(byte[] data, int offset) {
        if (offset < 0 || offset >= data.length)
            throw new MemgresException("index " + offset + " out of valid range, 0.." + (data.length - 1), "2202E");
        return data[offset] & 0xFF;
    }

    /** set_byte(bytea, int, int) -> bytea */
    public static byte[] setByte(byte[] data, int offset, int newByte) {
        if (offset < 0 || offset >= data.length)
            throw new MemgresException("index " + offset + " out of valid range, 0.." + (data.length - 1), "2202E");
        byte[] result = data.clone();
        result[offset] = (byte) newByte;
        return result;
    }

    /** MD5 hash of string */
    public static String md5(String input) {
        return md5bytes(input.getBytes(StandardCharsets.UTF_8));
    }

    /** MD5 hash of raw bytes */
    public static String md5bytes(byte[] input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(input);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b & 0xFF));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new MemgresException("MD5 not available");
        }
    }

    /** SHA-256 hash */
    public static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new MemgresException("SHA-256 not available");
        }
    }

    /** SHA-384 hash */
    public static byte[] sha384(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-384").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new MemgresException("SHA-384 not available");
        }
    }

    /** SHA-512 hash */
    public static byte[] sha512(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-512").digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new MemgresException("SHA-512 not available");
        }
    }

    /** substring(bytea, start, count) */
    public static byte[] substring(byte[] data, int start, int count) {
        // PG is 1-based: end = start + count (before clamping)
        // When start < 1, the effective count is reduced (PG behavior)
        int end = start + count;  // 1-based exclusive, before clamping
        int s = Math.max(0, start - 1);  // 0-based inclusive, clamped
        int e = Math.min(data.length, Math.max(0, end - 1));  // 0-based exclusive, clamped
        if (e <= s) return new byte[0];
        byte[] result = new byte[e - s];
        System.arraycopy(data, s, result, 0, result.length);
        return result;
    }

    /** Convert hex string to bytes */
    public static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /** Convert bytes to hex string */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b & 0xFF));
        return sb.toString();
    }
}
