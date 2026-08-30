package com.memgres.engine;

/**
 * How the hexadecimal digits of a hardware address may be grouped.
 *
 * <p>PostgreSQL does not have a rule here so much as a list: the digits may be written all
 * together, or in twos, or in a handful of other groupings, and each grouping is spelled out in
 * its input function. Reading the separators and accepting whatever they produced admitted
 * groupings no server takes, and matching only a few of them by hand refused four spellings the
 * documentation lists.
 */
final class MacaddrGrouping {

    private MacaddrGrouping() {
    }

    /**
     * The bytes an address literal names.
     *
     * @param digits how many hexadecimal digits the address has in total
     * @param groupings the group sizes each accepted spelling has, in order
     * @return the bytes, or {@code null} where the literal is not one of the spellings
     */
    static byte[] read(String input, String typeName, int digits, int[][] groupings) {
        if (input == null) return null;
        String s = input.trim().toLowerCase(java.util.Locale.ROOT);
        // A group written with a sign is a number out of range rather than a spelling this does
        // not know: PostgreSQL reads it, finds it is not a byte, and says so.
        for (int i = 1; i < s.length(); i++) {
            char before = s.charAt(i - 1);
            char c = s.charAt(i);
            if ((c == '-' || c == '+') && (before == ':' || before == '-' || before == '.')) {
                throw new MemgresException("invalid octet value in \"" + typeName
                        + "\" value: \"" + input + "\"", "22003");
            }
        }
        // One kind of separator throughout, and it separates rather than surrounds.
        char separator = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ':' || c == '-' || c == '.') {
                if (separator != 0 && c != separator) return null;
                separator = c;
            } else if (Character.digit(c, 16) < 0) {
                return null;
            }
        }
        String[] parts = separator == 0
                ? new String[]{s} : s.split(java.util.regex.Pattern.quote(String.valueOf(separator)), -1);
        for (int[] grouping : groupings) {
            if (grouping.length != parts.length) continue;
            boolean fits = true;
            for (int i = 0; i < grouping.length; i++) {
                if (parts[i].length() != grouping[i]) {
                    fits = false;
                    break;
                }
            }
            if (!fits) continue;
            StringBuilder hex = new StringBuilder();
            for (String part : parts) hex.append(part);
            if (hex.length() != digits && hex.length() != 12 && hex.length() != 16) return null;
            byte[] out = new byte[hex.length() / 2];
            for (int i = 0; i < out.length; i++) {
                out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
            }
            return out;
        }
        return null;
    }
}
