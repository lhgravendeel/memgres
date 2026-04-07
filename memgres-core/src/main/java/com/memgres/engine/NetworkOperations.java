package com.memgres.engine;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Network address type operations for inet, cidr, and macaddr.
 */
public final class NetworkOperations {
    private NetworkOperations() {}

    // Parse an inet/cidr string into address and prefix length
    private static long[] parseInet(String value) {
        // Returns [addressAsLong, prefixLength, byteCount]
        // Handles both "192.168.1.1" and "192.168.1.0/24"
        String addrPart = value;
        int prefix = -1; // -1 means single host (no mask specified)
        int slashIdx = value.indexOf('/');
        if (slashIdx >= 0) {
            addrPart = value.substring(0, slashIdx);
            prefix = Integer.parseInt(value.substring(slashIdx + 1));
        }
        try {
            byte[] bytes = InetAddress.getByName(addrPart).getAddress();
            long addr = 0;
            for (byte b : bytes) {
                addr = (addr << 8) | (b & 0xFF);
            }
            if (prefix == -1) prefix = bytes.length * 8; // host address
            return new long[]{addr, prefix, bytes.length};
        } catch (UnknownHostException e) {
            throw new MemgresException("invalid input syntax for type inet: \"" + value + "\"");
        }
    }

    /** host(inet): extracts IP address as text. */
    public static String host(String inet) {
        int slash = inet.indexOf('/');
        return slash >= 0 ? inet.substring(0, slash) : inet;
    }

    /** text(inet): full text representation (always includes prefix length). */
    public static String text(String inet) {
        if (inet.indexOf('/') >= 0) return inet;
        // Default prefix for host addresses is /32 (IPv4)
        return inet + "/32";
    }

    /** abbrev(inet): abbreviated display (removes /32 for hosts). */
    public static String abbrev(String inet) {
        if (inet.endsWith("/32")) return inet.substring(0, inet.length() - 3);
        return inet;
    }

    /** masklen(inet): returns the prefix length. */
    public static int masklen(String inet) {
        int slash = inet.indexOf('/');
        return slash >= 0 ? Integer.parseInt(inet.substring(slash + 1)) : 32;
    }

    /** set_masklen(inet, int): sets the prefix length (does NOT zero host bits; only network() does that). */
    public static String setMasklen(String inet, int len) {
        long[] parsed = parseInet(inet);
        long addr = parsed[0];
        return longToIp(addr) + "/" + len;
    }

    /** network(inet): returns the network part. */
    public static String network(String inet) {
        long[] parsed = parseInet(inet);
        long addr = parsed[0];
        int prefix = (int) parsed[1];
        // When no explicit mask was provided, use classful default prefix
        if (inet.indexOf('/') < 0) {
            prefix = classfulPrefix(addr);
        }
        long mask = prefix == 0 ? 0 : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
        long net = addr & mask;
        return longToIp(net) + "/" + prefix;
    }

    /** Determine the classful network prefix length for an IPv4 address. */
    private static int classfulPrefix(long addr) {
        int firstOctet = (int) ((addr >> 24) & 0xFF);
        if (firstOctet < 128) return 8;        // Class A: 0.0.0.0 - 127.255.255.255
        if (firstOctet < 192) return 16;       // Class B: 128.0.0.0 - 191.255.255.255
        return 24;                              // Class C: 192.0.0.0 - 223.255.255.255 (and beyond)
    }

    /** netmask(inet): returns the subnet mask. */
    public static String netmask(String inet) {
        int prefix = masklen(inet);
        long mask = prefix == 0 ? 0 : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
        return longToIp(mask);
    }

    /** broadcast(inet): returns the broadcast address. */
    public static String broadcast(String inet) {
        long[] parsed = parseInet(inet);
        long addr = parsed[0];
        int prefix = (int) parsed[1];
        long mask = prefix == 0 ? 0 : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
        long hostMask = ~mask & 0xFFFFFFFFL;
        long bcast = (addr & mask) | hostMask;
        return longToIp(bcast) + "/" + prefix;
    }

    /** inet_same_family(inet, inet): checks if both addresses are in the same family. */
    public static boolean inetSameFamily(String a, String b) {
        // Simplified: check if both are IPv4 (contain dots) or both are IPv6 (contain colons)
        boolean aV4 = a.contains(".");
        boolean bV4 = b.contains(".");
        return aV4 == bV4;
    }

    /** inet_merge(inet, inet): returns the smallest network including both. */
    public static String inetMerge(String a, String b) {
        long[] pa = parseInet(a);
        long[] pb = parseInet(b);
        long addrA = pa[0], addrB = pb[0];
        // Find common prefix
        long diff = addrA ^ addrB;
        int commonBits = diff == 0 ? 32 : Integer.numberOfLeadingZeros((int) diff);
        long mask = commonBits == 0 ? 0 : (0xFFFFFFFFL << (32 - commonBits)) & 0xFFFFFFFFL;
        long network = addrA & mask;
        return longToIp(network) + "/" + commonBits;
    }

    /** Contains (a >> b): does network a strictly contain b? */
    public static boolean contains(String a, String b) {
        long[] pa = parseInet(a);
        long[] pb = parseInet(b);
        int prefA = (int) pa[1], prefB = (int) pb[1];
        if (prefA >= prefB) return false; // must be strictly containing (smaller prefix)
        long maskA = prefA == 0 ? 0 : (0xFFFFFFFFL << (32 - prefA)) & 0xFFFFFFFFL;
        return (pa[0] & maskA) == (pb[0] & maskA);
    }

    /** Contains or equals: a >>= b */
    public static boolean containsOrEquals(String a, String b) {
        long[] pa = parseInet(a);
        long[] pb = parseInet(b);
        int prefA = (int) pa[1], prefB = (int) pb[1];
        if (prefA > prefB) return false;
        long maskA = prefA == 0 ? 0 : (0xFFFFFFFFL << (32 - prefA)) & 0xFFFFFFFFL;
        return (pa[0] & maskA) == (pb[0] & maskA);
    }

    /** Contained by (a << b): is a strictly contained in b? */
    public static boolean containedBy(String a, String b) {
        return contains(b, a);
    }

    /** Contained by or equals: a <<= b */
    public static boolean containedByOrEquals(String a, String b) {
        return containsOrEquals(b, a);
    }

    /** Bitwise AND for inet */
    public static String bitwiseAnd(String a, String b) {
        long[] pa = parseInet(a);
        long[] pb = parseInet(b);
        return longToIp(pa[0] & pb[0]);
    }

    /** Bitwise OR for inet */
    public static String bitwiseOr(String a, String b) {
        long[] pa = parseInet(a);
        long[] pb = parseInet(b);
        return longToIp(pa[0] | pb[0]);
    }

    /** Bitwise NOT for inet */
    public static String bitwiseNot(String a) {
        long[] pa = parseInet(a);
        return longToIp(~pa[0] & 0xFFFFFFFFL);
    }

    /**
     * Normalize an inet/cidr string to match PostgreSQL canonical formatting.
     * Converts IPv4-mapped IPv6 addresses from hex to mixed notation
     * (e.g., ::ffff:0:0 → ::ffff:0.0.0.0).
     */
    public static String normalizeAddress(String value) {
        if (value == null) return null;
        String addrPart = value;
        String suffix = "";
        int slash = value.indexOf('/');
        if (slash >= 0) {
            addrPart = value.substring(0, slash);
            suffix = value.substring(slash);
        }
        // Normalize IPv4-mapped IPv6: ::ffff:HHHH:HHHH → ::ffff:d.d.d.d
        String lower = addrPart.toLowerCase();
        if (lower.startsWith("::ffff:") && !addrPart.contains(".")) {
            String hexPart = addrPart.substring(7); // after "::ffff:"
            String[] groups = hexPart.split(":");
            if (groups.length == 2) {
                try {
                    int hi = Integer.parseInt(groups[0], 16);
                    int lo = Integer.parseInt(groups[1], 16);
                    String dotted = ((hi >> 8) & 0xFF) + "." + (hi & 0xFF) + "."
                                  + ((lo >> 8) & 0xFF) + "." + (lo & 0xFF);
                    return "::ffff:" + dotted + suffix;
                } catch (NumberFormatException e) {
                    // fall through
                }
            }
        }
        return value;
    }

    private static String longToIp(long addr) {
        return String.format("%d.%d.%d.%d",
            (addr >> 24) & 0xFF, (addr >> 16) & 0xFF,
            (addr >> 8) & 0xFF, addr & 0xFF);
    }
}
