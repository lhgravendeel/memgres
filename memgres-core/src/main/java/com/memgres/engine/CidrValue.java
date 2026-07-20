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
        try {
            bytes = InetAddress.getByName(addrPart).getAddress();
            // Java maps IPv4-compatible/mapped IPv6 addresses to 4-byte arrays;
            // if the user wrote IPv6 syntax (colons), force 16-byte representation
            if (isIPv6Syntax && bytes.length == 4) {
                byte[] ipv6 = new byte[16];
                ipv6[10] = (byte) 0xFF;
                ipv6[11] = (byte) 0xFF;
                System.arraycopy(bytes, 0, ipv6, 12, 4);
                bytes = ipv6;
            }
        } catch (UnknownHostException e) {
            throw new MemgresException("invalid input syntax for type cidr: \"" + input + "\"", "22P02");
        }
        int maxPrefix = bytes.length * 8;
        if (prefix == -1) prefix = maxPrefix;
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
        return formatAddress(getAddressRef()) + "/" + getPrefixLength();
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
        if (newPrefix < 0 || newPrefix > max) {
            throw new MemgresException("invalid mask length: " + newPrefix, "22023");
        }
        byte[] addr = getAddress();
        zeroHostBits(addr, newPrefix);
        return new CidrValue(addr, newPrefix);
    }
}
