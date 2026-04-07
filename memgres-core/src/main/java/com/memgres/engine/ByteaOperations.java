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
            String hex = input.substring(2);
            return hexToBytes(hex);
        }
        return input.getBytes(StandardCharsets.UTF_8);
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
            throw new MemgresException("index " + offset + " out of valid range, 0.." + (data.length - 1));
        return data[offset] & 0xFF;
    }

    /** set_byte(bytea, int, int) -> bytea */
    public static byte[] setByte(byte[] data, int offset, int newByte) {
        if (offset < 0 || offset >= data.length)
            throw new MemgresException("index " + offset + " out of valid range, 0.." + (data.length - 1));
        byte[] result = data.clone();
        result[offset] = (byte) newByte;
        return result;
    }

    /** MD5 hash */
    public static String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
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
        // PG is 1-based
        int s = Math.max(0, start - 1);
        int e = Math.min(data.length, s + count);
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
