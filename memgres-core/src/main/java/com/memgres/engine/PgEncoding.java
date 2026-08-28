package com.memgres.engine;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The character encodings PostgreSQL names, and what reading or writing text in one of them
 * means.
 *
 * <p>An encoding argument used to be checked against a list of names and then dropped, so
 * {@code convert_from(b,'LATIN1')} read UTF-8 whatever it was told and {@code convert} could not
 * fail. The name is the whole of what those functions do: the same bytes are different text in
 * two encodings, and answering as though they were not is answering a question nobody asked.
 */
final class PgEncoding {

    private PgEncoding() {
    }

    /** PostgreSQL's encodings in its own numbering, so a code names the same encoding it does. */
    static final String[] BY_CODE = {
            "SQL_ASCII", "EUC_JP", "EUC_CN", "EUC_KR", "EUC_TW", "EUC_JIS_2004", "UTF8",
            "MULE_INTERNAL", "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5", "LATIN6",
            "LATIN7", "LATIN8", "LATIN9", "LATIN10", "WIN1256", "WIN1258", "WIN866", "WIN874",
            "KOI8R", "WIN1251", "WIN1252", "ISO_8859_5", "ISO_8859_6", "ISO_8859_7",
            "ISO_8859_8", "WIN1250", "WIN1253", "WIN1254", "WIN1255", "WIN1257", "KOI8U"};

    /** What each of those is called where the JDK knows it. */
    private static final Map<String, String> JAVA_CHARSET = new HashMap<String, String>();

    static {
        JAVA_CHARSET.put("UTF8", "UTF-8");
        JAVA_CHARSET.put("LATIN1", "ISO-8859-1");
        JAVA_CHARSET.put("LATIN2", "ISO-8859-2");
        JAVA_CHARSET.put("LATIN3", "ISO-8859-3");
        JAVA_CHARSET.put("LATIN4", "ISO-8859-4");
        JAVA_CHARSET.put("LATIN5", "ISO-8859-9");
        JAVA_CHARSET.put("LATIN6", "ISO-8859-10");
        JAVA_CHARSET.put("LATIN7", "ISO-8859-13");
        JAVA_CHARSET.put("LATIN8", "ISO-8859-14");
        JAVA_CHARSET.put("LATIN9", "ISO-8859-15");
        JAVA_CHARSET.put("LATIN10", "ISO-8859-16");
        JAVA_CHARSET.put("ISO_8859_5", "ISO-8859-5");
        JAVA_CHARSET.put("ISO_8859_6", "ISO-8859-6");
        JAVA_CHARSET.put("ISO_8859_7", "ISO-8859-7");
        JAVA_CHARSET.put("ISO_8859_8", "ISO-8859-8");
        JAVA_CHARSET.put("KOI8R", "KOI8-R");
        JAVA_CHARSET.put("KOI8U", "KOI8-U");
        JAVA_CHARSET.put("WIN866", "IBM866");
        JAVA_CHARSET.put("WIN874", "x-windows-874");
        JAVA_CHARSET.put("WIN1250", "windows-1250");
        JAVA_CHARSET.put("WIN1251", "windows-1251");
        JAVA_CHARSET.put("WIN1252", "windows-1252");
        JAVA_CHARSET.put("WIN1253", "windows-1253");
        JAVA_CHARSET.put("WIN1254", "windows-1254");
        JAVA_CHARSET.put("WIN1255", "windows-1255");
        JAVA_CHARSET.put("WIN1256", "windows-1256");
        JAVA_CHARSET.put("WIN1257", "windows-1257");
        JAVA_CHARSET.put("WIN1258", "windows-1258");
        JAVA_CHARSET.put("EUC_JP", "EUC-JP");
        JAVA_CHARSET.put("EUC_CN", "GB2312");
        JAVA_CHARSET.put("EUC_KR", "EUC-KR");
        JAVA_CHARSET.put("EUC_TW", "EUC-TW");
        JAVA_CHARSET.put("BIG5", "Big5");
        JAVA_CHARSET.put("GBK", "GBK");
        JAVA_CHARSET.put("GB18030", "GB18030");
        JAVA_CHARSET.put("SJIS", "Shift_JIS");
        JAVA_CHARSET.put("UHC", "x-windows-949");
    }

    /** The name PostgreSQL knows this encoding by, or {@code null} if it knows none. */
    static String canonical(String given) {
        if (given == null) return null;
        String name = given.trim();
        for (String known : BY_CODE) {
            if (known.equalsIgnoreCase(name)) return known;
        }
        for (String known : JAVA_CHARSET.keySet()) {
            if (known.equalsIgnoreCase(name)) return known;
        }
        return null;
    }

    /**
     * The encoding an argument names, whether written as a name or as PostgreSQL's number for it.
     *
     * <p>How a name it does not know is reported depends on where it was written. A conversion
     * has a side to name -- the encoding read from or the one written to -- and says which was
     * wrong, as an invalid parameter. A function taking one encoding is looking the name up in
     * the catalogue, and an unknown one is an object that does not exist.
     *
     * @param what {@code "source"} or {@code "destination"} where a conversion has two of them,
     *     and {@code null} where the name is simply being looked up
     */
    static String named(Object arg, String what) {
        if (arg instanceof Number) {
            int code = ((Number) arg).intValue();
            if (code < 0 || code >= BY_CODE.length) {
                throw new MemgresException(code + " is not a valid encoding code", "42704");
            }
            return BY_CODE[code];
        }
        String name = canonical(String.valueOf(arg));
        if (name == null) {
            if (what == null) {
                throw new MemgresException(arg + " is not a valid encoding name", "42704");
            }
            throw new MemgresException(
                    "invalid " + what + " encoding name \"" + arg + "\"", "22023");
        }
        return name;
    }

    /**
     * SQL_ASCII is not an encoding so much as the absence of one: the bytes are carried through
     * untouched and no character is looked up. Every other name has a JDK charset behind it.
     */
    static boolean isRawBytes(String canonicalName) {
        return "SQL_ASCII".equals(canonicalName) || "MULE_INTERNAL".equals(canonicalName);
    }

    static Charset charset(String canonicalName) {
        String javaName = JAVA_CHARSET.get(canonicalName);
        if (javaName == null) {
            throw new MemgresException("conversion between " + canonicalName
                    + " and UTF8 is not supported", "0A000");
        }
        try {
            return Charset.forName(javaName);
        } catch (RuntimeException e) {
            throw new MemgresException("conversion between " + canonicalName
                    + " and UTF8 is not supported", "0A000");
        }
    }

    /** Read bytes as text in the named encoding, refusing bytes that spell no character in it. */
    static String decode(byte[] bytes, String canonicalName) {
        if (isRawBytes(canonicalName)) {
            return new String(bytes, StandardCharsets.ISO_8859_1);
        }
        CharsetDecoder decoder = charset(canonicalName).newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new MemgresException(
                    "invalid byte sequence for encoding \"" + canonicalName + "\"", "22021");
        }
    }

    /** Write text as bytes in the named encoding, refusing characters it cannot spell. */
    static byte[] encode(String text, String canonicalName) {
        if (isRawBytes(canonicalName)) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        CharsetEncoder encoder = charset(canonicalName).newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            ByteBuffer buffer = encoder.encode(java.nio.CharBuffer.wrap(text));
            byte[] out = new byte[buffer.remaining()];
            buffer.get(out);
            return out;
        } catch (CharacterCodingException e) {
            throw new MemgresException("character with byte sequence in encoding \"UTF8\" has no "
                    + "equivalent in encoding \"" + canonicalName + "\"", "22P05");
        }
    }

    /**
     * The encodings {@code to_ascii} will read from. PostgreSQL carries a table per encoding
     * rather than a rule, and has only ever written three of them.
     */
    static boolean hasAsciiTable(String canonicalName) {
        return asciiTable(canonicalName) != null;
    }

    /**
     * How each byte from 160 upwards is spelled in ASCII. These are PostgreSQL's own tables, so
     * that {@code to_ascii} answers what it answers rather than what stripping accents would:
     * the two agree on the accented letters and disagree on everything else, and half of what
     * these encodings hold above 160 is not a letter at all.
     */
    private static String asciiTable(String canonicalName) {
        if ("LATIN1".equals(canonicalName)) {
            return " !c##Y|S\"Ca<--R-o+23'u]*,1o>///?"
                    + "AAAAAAACEEEEIIIIDNOOOOOxOUUUUYPBaaaaaaaceeeeiiiidnooooo/ouuuuypy";
        }
        if ("LATIN2".equals(canonicalName)) {
            return " A^L$A'S^SS\"S<S'T^Z-Z'Z^Z'a^l'l>l's<s's>t^z'z\"z^z'"
                    + "R'A'A^A(A\"L'C'C,C<E'E((E\"E<I'I^D<D-N'N<O'O^O\"O~xR<U0U'U\"U'T,ss"
                    + "r'a'a^a(a\"l'c'c,c<e'e((e\"e<i'i^d<d-n'n<o'o^o\"o~/r<u0u'u\"u't,'";
        }
        if ("LATIN9".equals(canonicalName)) {
            return " !c##Y|S\"Ca<--R-o+23Zu]z,1o>OoY?"
                    + "AAAAAAACEEEEIIIIDNOOOOOxOUUUUYPBaaaaaaaceeeeiiiidnooooo/ouuuuypy";
        }
        if ("WIN1250".equals(canonicalName)) {
            return " a^,l'\"..+-S<S'S,S<S'a^l'''\"\".--s>s's,s<s'"
                    + " v(L$A(\"CS<>--rZ-o+,l''u p,a(,l>L'z."
                    + "R'A'A^A(A\"L'C'C,C<E'E((E\"E<I'I^D<D-N'N<O'O^O\"O~xR<U0U'U\"U'T,ss"
                    + "r'a'a^a(a\"l'c'c,c<e'e((e\"e<i'i^d<d-n'n<o'o^o\"o~/r<u0u'u\"u't,.";
        }
        return null;
    }

    /**
     * The ASCII spelling of text whose bytes are in the named encoding.
     *
     * <p>The text arrives as characters already, so it is written back out in that encoding to
     * recover the bytes the caller meant, and each of those is then looked up. This is what makes
     * {@code to_ascii('Karél','LATIN1')} answer {@code KarACl}: the two bytes of the UTF-8
     * {@code é} are two separate LATIN1 characters, and each has its own ASCII spelling.
     */
    static String toAscii(String text, String canonicalName) {
        String table = asciiTable(canonicalName);
        if (table == null) {
            throw new MemgresException(
                    "encoding conversion from " + canonicalName + " to ASCII not supported",
                    "0A000");
        }
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        StringBuilder out = new StringBuilder(bytes.length);
        for (byte b : bytes) {
            int unsigned = b & 0xFF;
            if (unsigned < 128) {
                out.append((char) unsigned);
            } else if (unsigned - 160 < table.length() && unsigned >= 160) {
                out.append(table.charAt(unsigned - 160));
            } else {
                out.append(' ');
            }
        }
        return out.toString();
    }
}
