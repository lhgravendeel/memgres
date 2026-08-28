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

    /** The alphabet base64 is written in, and where each character stands in it. */
    private static final String BASE64_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    /** How many characters of base64 go on a line before the next one begins. */
    private static final int BASE64_LINE = 76;

    /**
     * Write bytes as base64, a line at a time.
     *
     * <p>The wrapping is part of the format PostgreSQL writes and reads: its own reader accepts
     * the newlines and a client that stores the result gets the same text back. Writing one
     * unbroken line was shorter by exactly the newlines, so any length taken of the result
     * disagreed, and the text was no longer what the same call on a real server produces.
     */
    public static String encodeBase64(byte[] bytes) {
        StringBuilder out = new StringBuilder();
        int column = 0;
        for (int i = 0; i < bytes.length; i += 3) {
            int remaining = bytes.length - i;
            int block = (bytes[i] & 0xFF) << 16;
            if (remaining > 1) block |= (bytes[i + 1] & 0xFF) << 8;
            if (remaining > 2) block |= bytes[i + 2] & 0xFF;
            char[] four = new char[4];
            four[0] = BASE64_ALPHABET.charAt((block >> 18) & 0x3F);
            four[1] = BASE64_ALPHABET.charAt((block >> 12) & 0x3F);
            four[2] = remaining > 1 ? BASE64_ALPHABET.charAt((block >> 6) & 0x3F) : '=';
            four[3] = remaining > 2 ? BASE64_ALPHABET.charAt(block & 0x3F) : '=';
            for (char c : four) {
                out.append(c);
                if (++column == BASE64_LINE) {
                    out.append('\n');
                    column = 0;
                }
            }
        }
        return out.toString();
    }

    /**
     * Read base64, ignoring the whitespace its own writer puts in and insisting on the padding
     * that says how the last group ends.
     */
    public static byte[] decodeBase64(String text) {
        StringBuilder symbols = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) continue;
            if (c != '=' && BASE64_ALPHABET.indexOf(c) < 0) {
                throw new MemgresException(
                        "invalid symbol \"" + c + "\" found while decoding base64 sequence",
                        "22023");
            }
            symbols.append(c);
        }
        // Four characters spell three bytes, so a count that is not a multiple of four does not
        // say how the last group ends. Filling in the padding for the caller accepted text that
        // no writer of this format produces.
        if (symbols.length() % 4 != 0) {
            MemgresException e = new MemgresException("invalid base64 end sequence", "22023");
            e.setHint("Input data is missing padding, is truncated, or is otherwise corrupted.");
            throw e;
        }
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int block = 0;
        int held = 0;
        for (int i = 0; i < symbols.length(); i++) {
            char c = symbols.charAt(i);
            if (c == '=') {
                // The padding says the group was short: what is already held is all it spelled.
                if (held == 3) {
                    out.write((block >> 10) & 0xFF);
                    out.write((block >> 2) & 0xFF);
                } else if (held == 2) {
                    out.write((block >> 4) & 0xFF);
                }
                return out.toByteArray();
            }
            block = (block << 6) | BASE64_ALPHABET.indexOf(c);
            if (++held == 4) {
                out.write((block >> 16) & 0xFF);
                out.write((block >> 8) & 0xFF);
                out.write(block & 0xFF);
                block = 0;
                held = 0;
            }
        }
        return out.toByteArray();
    }

    /**
     * Read hex digits, ignoring whitespace between them.
     *
     * <p>Each character is checked as it is met, so a character that is no digit at all is
     * reported as that rather than as the odd count it happens to leave behind.
     */
    public static byte[] decodeHexText(String text) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int high = -1;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isWhitespace(c)) continue;
            int digit = Character.digit(c, 16);
            if (digit < 0) {
                throw new MemgresException("invalid hexadecimal digit: \"" + c + "\"", "22023");
            }
            if (high < 0) {
                high = digit;
            } else {
                out.write((high << 4) | digit);
                high = -1;
            }
        }
        if (high >= 0) {
            throw new MemgresException(
                    "invalid hexadecimal data: odd number of digits", "22023");
        }
        return out.toByteArray();
    }

    /**
     * Read the escape format: the text's own bytes, with a backslash introducing either another
     * backslash or three octal digits.
     *
     * <p>The bytes are the ones the text is written in, so a character outside ASCII contributes
     * all of its bytes. Writing one byte per Java character truncated every such character to its
     * low eight bits and lost the rest of it.
     */
    public static byte[] decodeEscape(String text) {
        byte[] source = text.getBytes(StandardCharsets.UTF_8);
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (int i = 0; i < source.length; i++) {
            int c = source[i] & 0xFF;
            if (c != '\\') {
                out.write(c);
                continue;
            }
            if (i + 1 < source.length && source[i + 1] == '\\') {
                out.write('\\');
                i++;
                continue;
            }
            if (i + 3 < source.length && isOctal(source[i + 1]) && isOctal(source[i + 2])
                    && isOctal(source[i + 3])) {
                int value = (source[i + 1] - '0') * 64 + (source[i + 2] - '0') * 8
                        + (source[i + 3] - '0');
                if (value > 255) {
                    throw new MemgresException("invalid input syntax for type bytea", "22P02");
                }
                out.write(value);
                i += 3;
                continue;
            }
            throw new MemgresException("invalid input syntax for type bytea", "22P02");
        }
        return out.toByteArray();
    }

    private static boolean isOctal(byte b) {
        return b >= '0' && b <= '7';
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
