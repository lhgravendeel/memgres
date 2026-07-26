package com.memgres.engine;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Typed wrapper for PostgreSQL inet values.
 * Stores address as raw bytes (4 for IPv4, 16 for IPv6) plus prefix length.
 * Implements PG-compatible equality, ordering, and display.
 */
public class InetValue implements Comparable<InetValue> {
    private final byte[] address; // 4 bytes (IPv4) or 16 bytes (IPv6)
    private final int prefixLength;

    public InetValue(byte[] address, int prefixLength) {
        this.address = address.clone();
        this.prefixLength = prefixLength;
    }

    /** Parse a PostgreSQL inet literal (e.g. "192.168.1.1", "192.168.1.0/24", "::1/128", "2001:db8::1"). */
    public static InetValue parse(String input) {
        String s = input.trim();
        String addrPart = s;
        int prefix = -1;
        int slashIdx = s.indexOf('/');
        if (slashIdx >= 0) {
            addrPart = s.substring(0, slashIdx);
            try {
                prefix = Integer.parseInt(s.substring(slashIdx + 1));
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type inet: \"" + input + "\"", "22P02");
            }
        }
        byte[] bytes;
        boolean isIPv6Syntax = addrPart.contains(":");
        try {
            InetAddress addr = InetAddress.getByName(addrPart);
            bytes = addr.getAddress();
            // Java maps IPv4-compatible/mapped IPv6 addresses to 4-byte arrays;
            // if the user wrote IPv6 syntax (colons), force 16-byte representation
            if (isIPv6Syntax && bytes.length == 4) {
                byte[] ipv6 = new byte[16];
                // Map to ::ffff:x.x.x.x
                ipv6[10] = (byte) 0xFF;
                ipv6[11] = (byte) 0xFF;
                System.arraycopy(bytes, 0, ipv6, 12, 4);
                bytes = ipv6;
            }
        } catch (UnknownHostException e) {
            throw new MemgresException("invalid input syntax for type inet: \"" + input + "\"", "22P02");
        }
        int maxPrefix = bytes.length * 8;
        if (prefix == -1) prefix = maxPrefix;
        if (prefix < 0 || prefix > maxPrefix) {
            throw new MemgresException("invalid input syntax for type inet: \"" + input + "\"", "22P02");
        }
        // Validate octets for IPv4 (InetAddress.getByName is lenient with some formats)
        if (bytes.length == 4) {
            validateIPv4Octets(addrPart, input);
        }
        return new InetValue(bytes, prefix);
    }

    /** Parse with strict validation: rejects PG-invalid abbreviated IPv4 forms like "10" (cidr-only). */
    public static InetValue parseStrict(String input) {
        return parse(input);
    }

    private static void validateIPv4Octets(String addrPart, String original) {
        String[] octets = addrPart.split("\\.");
        if (octets.length != 4) {
            // Unlike cidr, PG's inet input requires a full 4-octet IPv4 address;
            // abbreviated forms such as '10', '10.1' or '10.1.2' are rejected (22P02).
            throw new MemgresException("invalid input syntax for type inet: \"" + original + "\"", "22P02");
        }
        for (String o : octets) {
            try {
                int val = Integer.parseInt(o);
                if (val < 0 || val > 255) {
                    throw new MemgresException("invalid input syntax for type inet: \"" + original + "\"", "22P02");
                }
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid input syntax for type inet: \"" + original + "\"", "22P02");
            }
        }
    }

    public byte[] getAddress() { return address.clone(); }
    public byte[] getAddressRef() { return address; }
    public int getPrefixLength() { return prefixLength; }
    public int family() { return address.length == 4 ? 4 : 6; }
    public boolean isIPv4() { return address.length == 4; }
    public boolean isIPv6() { return address.length == 16; }
    public int maxBits() { return address.length * 8; }

    /** Returns the host part of the address (no prefix). */
    public String host() {
        return formatAddress(address);
    }

    /** Returns text representation: address/prefix (always includes prefix). */
    public String text() {
        return formatAddress(address) + "/" + prefixLength;
    }

    /** Returns abbreviated display: omits /32 for IPv4 hosts, /128 for IPv6 hosts. */
    public String abbrev() {
        if (prefixLength == maxBits()) return formatAddress(address);
        return formatAddress(address) + "/" + prefixLength;
    }

    /** Returns the network part (host bits zeroed). */
    public InetValue network() {
        byte[] net = address.clone();
        zeroHostBits(net, prefixLength);
        return new InetValue(net, prefixLength);
    }

    /** Returns the netmask as an InetValue with maxBits prefix. */
    public InetValue netmask() {
        byte[] mask = new byte[address.length];
        for (int i = 0; i < mask.length * 8; i++) {
            if (i < prefixLength) {
                mask[i / 8] |= (byte) (0x80 >> (i % 8));
            }
        }
        return new InetValue(mask, mask.length * 8);
    }

    /** Returns the hostmask (complement of netmask) as an InetValue with maxBits prefix. */
    public InetValue hostmask() {
        byte[] mask = new byte[address.length];
        for (int i = 0; i < mask.length * 8; i++) {
            if (i >= prefixLength) {
                mask[i / 8] |= (byte) (0x80 >> (i % 8));
            }
        }
        return new InetValue(mask, mask.length * 8);
    }

    /** Returns the broadcast address (host bits all 1s). */
    public InetValue broadcast() {
        byte[] bcast = address.clone();
        for (int i = prefixLength; i < bcast.length * 8; i++) {
            bcast[i / 8] |= (byte) (0x80 >> (i % 8));
        }
        return new InetValue(bcast, prefixLength);
    }

    /** set_masklen: returns new InetValue with different prefix (keeps address). */
    public InetValue setMasklen(int newPrefix) {
        int max = maxBits();
        if (newPrefix < 0 || newPrefix > max) {
            throw new MemgresException("invalid mask length: " + newPrefix, "22023");
        }
        return new InetValue(address, newPrefix);
    }

    /** inet + integer: add offset to address. */
    public InetValue add(long offset) {
        BigInteger addr = new BigInteger(1, address);
        BigInteger result = addr.add(BigInteger.valueOf(offset));
        // The address space does not wrap: PG reports a result that leaves it
        if (result.signum() < 0 || result.bitLength() > address.length * 8) {
            throw new MemgresException("result is out of range", "22003");
        }
        byte[] resultBytes = bigIntToBytes(result, address.length);
        return new InetValue(resultBytes, prefixLength);
    }

    /** inet - integer: subtract offset from address. */
    public InetValue subtract(long offset) {
        return add(-offset);
    }

    /** inet - inet: returns the difference as a long. */
    public long subtract(InetValue other) {
        if (this.address.length != other.address.length) {
            throw new MemgresException("cannot subtract inet values of different families", "42883");
        }
        BigInteger a = new BigInteger(1, address);
        BigInteger b = new BigInteger(1, other.address);
        return a.subtract(b).longValueExact();
    }

    /** Bitwise AND. */
    public InetValue bitwiseAnd(InetValue other) {
        checkSameFamily(other);
        byte[] result = new byte[address.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (address[i] & other.address[i]);
        }
        return new InetValue(result, Math.max(prefixLength, other.prefixLength));
    }

    /** Bitwise OR. */
    public InetValue bitwiseOr(InetValue other) {
        checkSameFamily(other);
        byte[] result = new byte[address.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (address[i] | other.address[i]);
        }
        return new InetValue(result, Math.max(prefixLength, other.prefixLength));
    }

    /** Bitwise NOT (~inet). */
    public InetValue bitwiseNot() {
        byte[] result = new byte[address.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) ~address[i];
        }
        return new InetValue(result, prefixLength);
    }

    /** Strictly contains: this >> other (this network contains other). */
    public boolean contains(InetValue other) {
        if (address.length != other.address.length) return false;
        if (prefixLength >= other.prefixLength) return false; // must be strictly wider
        return networkMatch(other);
    }

    /** Contains or equals: this >>= other. */
    public boolean containsOrEquals(InetValue other) {
        if (address.length != other.address.length) return false;
        if (prefixLength > other.prefixLength) return false;
        return networkMatch(other);
    }

    /** Does this network contain other's address? */
    private boolean networkMatch(InetValue other) {
        for (int i = 0; i < prefixLength; i++) {
            int byteIdx = i / 8;
            int bitMask = 0x80 >> (i % 8);
            if ((address[byteIdx] & bitMask) != (other.address[byteIdx] & bitMask)) {
                return false;
            }
        }
        return true;
    }

    /** inet_same_family. */
    public boolean sameFamily(InetValue other) {
        return this.address.length == other.address.length;
    }

    /** inet_merge: smallest network containing both. */
    public InetValue merge(InetValue other) {
        if (address.length != other.address.length) {
            throw new MemgresException("cannot merge addresses from different families", "22023");
        }
        // Find the first differing bit
        int commonBits = 0;
        for (int i = 0; i < address.length * 8; i++) {
            int byteIdx = i / 8;
            int bitMask = 0x80 >> (i % 8);
            if ((address[byteIdx] & bitMask) == (other.address[byteIdx] & bitMask)) {
                commonBits++;
            } else {
                break;
            }
        }
        byte[] net = address.clone();
        zeroHostBits(net, commonBits);
        return new InetValue(net, commonBits);
    }

    /** PG inet ordering: family, then address bytes, then prefix length. */
    @Override
    public int compareTo(InetValue other) {
        // Family first (IPv4 < IPv6)
        if (this.address.length != other.address.length) {
            return Integer.compare(this.address.length, other.address.length);
        }
        // Then compare addresses byte-by-byte (unsigned)
        for (int i = 0; i < address.length; i++) {
            int cmp = Integer.compare(address[i] & 0xFF, other.address[i] & 0xFF);
            if (cmp != 0) return cmp;
        }
        // Then prefix length
        return Integer.compare(this.prefixLength, other.prefixLength);
    }

    /** PG inet equality: same address AND same prefix. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InetValue)) return false;
        InetValue other = (InetValue) o;
        return prefixLength == other.prefixLength && Arrays.equals(address, other.address);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(address) + prefixLength;
    }

    /** PG display: address/prefix, but omits prefix if it equals max bits. */
    @Override
    public String toString() {
        return abbrev();
    }

    // --- Static helpers ---

    static void zeroHostBits(byte[] addr, int prefix) {
        for (int i = prefix; i < addr.length * 8; i++) {
            addr[i / 8] &= (byte) ~(0x80 >> (i % 8));
        }
    }

    static byte[] bigIntToBytes(BigInteger val, int length) {
        byte[] raw = val.toByteArray();
        byte[] result = new byte[length];
        // BigInteger.toByteArray may have leading zero byte for sign
        int srcStart = raw.length > length ? raw.length - length : 0;
        int dstStart = length > raw.length ? length - raw.length : 0;
        int copyLen = Math.min(raw.length - srcStart, length - dstStart);
        System.arraycopy(raw, srcStart, result, dstStart, copyLen);
        return result;
    }

    /** Format byte array as IPv4 dotted-decimal or IPv6 compressed notation. */
    public static String formatAddress(byte[] addr) {
        if (addr.length == 4) {
            return (addr[0] & 0xFF) + "." + (addr[1] & 0xFF) + "." + (addr[2] & 0xFF) + "." + (addr[3] & 0xFF);
        }
        // IPv6: compressed notation
        return formatIPv6(addr);
    }

    /** Format IPv6 address in compressed (RFC 5952) notation. */
    private static String formatIPv6(byte[] addr) {
        // Check for IPv4-mapped addresses: ::ffff:x.x.x.x
        // Bytes 0-9 = 0, bytes 10-11 = 0xFF, bytes 12-15 = IPv4 address
        boolean isV4Mapped = true;
        for (int i = 0; i < 10; i++) {
            if (addr[i] != 0) { isV4Mapped = false; break; }
        }
        if (isV4Mapped && (addr[10] & 0xFF) == 0xFF && (addr[11] & 0xFF) == 0xFF) {
            return "::ffff:" + (addr[12] & 0xFF) + "." + (addr[13] & 0xFF) + "."
                    + (addr[14] & 0xFF) + "." + (addr[15] & 0xFF);
        }

        int[] groups = new int[8];
        for (int i = 0; i < 8; i++) {
            groups[i] = ((addr[i * 2] & 0xFF) << 8) | (addr[i * 2 + 1] & 0xFF);
        }
        // Find longest run of zeros for :: compression
        int bestStart = -1, bestLen = 0;
        int curStart = -1, curLen = 0;
        for (int i = 0; i < 8; i++) {
            if (groups[i] == 0) {
                if (curStart == -1) curStart = i;
                curLen++;
                if (curLen > bestLen) {
                    bestStart = curStart;
                    bestLen = curLen;
                }
            } else {
                curStart = -1;
                curLen = 0;
            }
        }
        // Only compress runs of 2+ zeros (RFC 5952 says 1 zero should not be compressed)
        if (bestLen < 2) bestStart = -1;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            if (i == bestStart) {
                sb.append("::");
                i += bestLen - 1;
                continue;
            }
            if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ':') sb.append(':');
            sb.append(Integer.toHexString(groups[i]));
        }
        return sb.toString();
    }

    private void checkSameFamily(InetValue other) {
        if (this.address.length != other.address.length) {
            throw new MemgresException("cannot AND/OR inet values of different families", "42883");
        }
    }
}
