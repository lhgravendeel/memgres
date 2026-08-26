package com.memgres.engine;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Typed wrapper for PostgreSQL cidr values.
 * Like InetValue but host bits must be zero. Display always includes prefix.
 */
public class CidrValue extends InetValue {

    private CidrValue(byte[] address, int prefixLength) {
        super(address, prefixLength);
    }

    /**
     * Parse a cidr literal. Host bits must be zero (or will be zeroed per PG behavior).
     * PG rejects cidr values with non-zero host bits (22P02), e.g. '192.168.1.5/24'::cidr.
     */
    public static CidrValue parse(String input) {
        String s = input.trim();
        String addrPart = s;
        int prefix = -1;
        int slashIdx = s.indexOf('/');
        if (slashIdx >= 0) {
            addrPart = s.substring(0, slashIdx);
            try {
                prefix = Integer.parseInt(s.substring(slashIdx + 1));
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
            }
        }
        byte[] bytes;
        boolean isIPv6Syntax = addrPart.contains(":");
        if (!isIPv6Syntax) {
            // IPv4 (possibly abbreviated). Unlike inet, cidr accepts 1-4 decimal octets
            // which are left-aligned and zero-padded on the right (e.g. '10' -> 10.0.0.0,
            // '10.1' -> 10.1.0.0). When no explicit prefix is given, it defaults to
            // (number of octets) * 8 (e.g. '10' -> /8, '10.1' -> /16, '192.168.1' -> /24).
            String[] octets = addrPart.split("\\.", -1);
            if (octets.length < 1 || octets.length > 4) {
                throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
            }
            bytes = new byte[4];
            for (int i = 0; i < octets.length; i++) {
                if (octets[i].isEmpty()) {
                    throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
                }
                int v;
                try {
                    v = Integer.parseInt(octets[i]);
                } catch (NumberFormatException e) {
                    throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
                }
                if (v < 0 || v > 255) {
                    throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
                }
                bytes[i] = (byte) v;
            }
            if (prefix == -1) prefix = octets.length * 8;
        } else {
            try {
                bytes = InetAddress.getByName(addrPart).getAddress();
                // Java maps IPv4-compatible/mapped IPv6 addresses to 4-byte arrays;
                // if the user wrote IPv6 syntax (colons), force 16-byte representation
                if (bytes.length == 4) {
                    byte[] ipv6 = new byte[16];
                    ipv6[10] = (byte) 0xFF;
                    ipv6[11] = (byte) 0xFF;
                    System.arraycopy(bytes, 0, ipv6, 12, 4);
                    bytes = ipv6;
                }
            } catch (UnknownHostException e) {
                throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
            }
            if (prefix == -1) prefix = bytes.length * 8;
        }
        int maxPrefix = bytes.length * 8;
        if (prefix < 0 || prefix > maxPrefix) {
            throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
        }
        // Check host bits are zero
        byte[] check = bytes.clone();
        zeroHostBits(check, prefix);
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != check[i]) {
                throw new MemgresException("invalid cidr value: \"" + input + "\"\n"
                        + "  Detail: Value has bits set to right of mask.", "22P02");
            }
        }
        return new CidrValue(bytes, prefix);
    }

    /** Create from an InetValue (zeros host bits). */
    public static CidrValue fromInet(InetValue inet) {
        byte[] addr = inet.getAddress();
        zeroHostBits(addr, inet.getPrefixLength());
        return new CidrValue(addr, inet.getPrefixLength());
    }

    /** cidr always displays with prefix. */
    @Override
    public String abbrev() {
        // PG cidr abbrev: for IPv4, abbreviate trailing zero octets
        if (isIPv4()) {
            return abbrevIPv4();
        }
        return abbrevIPv6(getAddressRef(), getPrefixLength());
    }

    /**
     * Abbreviated IPv6 cidr display, ported from PostgreSQL's inet_cidr_ntop_ipv6.
     * Only the words covered by the netmask are displayed (with trailing zero words
     * dropped), e.g. '2001:db8::/32' -> "2001:db8/32", '2001:db8::ff00:0/104' ->
     * "2001:db8::ff00/104".
     */
    static String abbrevIPv6(byte[] src, int bits) {
        if (bits < 0) bits = 0;
        if (bits > 128) bits = 128;
        StringBuilder out = new StringBuilder();
        if (bits == 0) {
            out.append("::");
        } else {
            // Copy address and zero the host part.
            byte[] in = new byte[16];
            int p = (bits + 7) / 8;
            System.arraycopy(src, 0, in, 0, Math.min(p, 16));
            int b = bits % 8;
            if (b != 0 && p >= 1) {
                in[p - 1] &= (byte) (((~0) << (8 - b)) & 0xFF);
            }
            // Number of 16-bit words to display.
            int words = (bits + 15) / 16;
            if (words == 1) words = 2;
            int[] w = new int[words];
            for (int i = 0; i < words; i++) {
                w[i] = ((in[i * 2] & 0xFF) << 8) | (in[i * 2 + 1] & 0xFF);
            }
            // Find the longest run of zero words for :: compression.
            int zeroStart = -1, zeroLen = 0, curStart = -1, curLen = 0;
            for (int i = 0; i < words; i++) {
                if (w[i] == 0) {
                    if (curLen == 0) curStart = i;
                    curLen++;
                    if (curLen > zeroLen) { zeroStart = curStart; zeroLen = curLen; }
                } else {
                    curLen = 0;
                }
            }
            for (int i = 0; i < words; i++) {
                if (zeroLen != 0 && i >= zeroStart && i < zeroStart + zeroLen) {
                    if (i == zeroStart) out.append(':');
                    if (i == words - 1) out.append(':');
                    continue;
                }
                // Mirror PG: a non-zero word is always preceded by ':' when output is
                // non-empty; after a zero run this second ':' completes the '::' marker.
                if (out.length() != 0) out.append(':');
                out.append(Integer.toHexString(w[i]));
            }
        }
        out.append('/').append(bits);
        return out.toString();
    }

    private String abbrevIPv4() {
        byte[] addr = getAddressRef();
        int prefix = getPrefixLength();
        // PG abbreviates by omitting trailing zero octets
        // e.g., 10.0.0.0/8 -> 10/8, 192.168.0.0/16 -> 192.168/16
        int significantOctets = (prefix + 7) / 8;
        if (significantOctets == 0) significantOctets = 1;
        // But only abbreviate if trailing octets are actually zero
        int lastNonZero = 0;
        for (int i = 0; i < 4; i++) {
            if ((addr[i] & 0xFF) != 0) lastNonZero = i;
        }
        int octetsToShow = Math.max(significantOctets, lastNonZero + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < octetsToShow; i++) {
            if (i > 0) sb.append('.');
            sb.append(addr[i] & 0xFF);
        }
        sb.append('/').append(prefix);
        return sb.toString();
    }

    /** cidr text() always includes prefix. */
    @Override
    public String text() {
        return formatAddress(getAddressRef()) + "/" + getPrefixLength();
    }

    @Override
    public String toString() {
        return formatAddress(getAddressRef()) + "/" + getPrefixLength();
    }

    /** Override set_masklen to return CidrValue (zeros host bits). */
    public CidrValue setCidrMasklen(int newPrefix) {
        int max = maxBits();
        // Minus one is PostgreSQL's way of asking for the whole address: it is the one negative
        // length that names a length, and refusing it left no way to write "all of it".
        if (newPrefix == -1) newPrefix = max;
        if (newPrefix < 0 || newPrefix > max) {
            throw new MemgresException("invalid mask length: " + newPrefix, "22023");
        }
        byte[] addr = getAddress();
        zeroHostBits(addr, newPrefix);
        return new CidrValue(addr, newPrefix);
    }
}
