package com.memgres.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * What happens to the blanks a {@code character(n)} declaration adds.
 *
 * <p>PostgreSQL stores a bpchar padded out to its declared width and reads it back trimmed: the
 * conversion from bpchar to any other string type drops the trailing blanks, and every routine
 * declared over text takes its argument through that conversion. So {@code 'ab'::char(5)} is five
 * characters where it is a bpchar and two everywhere it is read as a text — which is why
 * {@code length} answers 2, {@code upper} answers AB, and {@code reverse} answers ba.
 *
 * <p>memgres stored the padding and never dropped it, so the blanks came back out of every one of
 * those: {@code upper} answered "AB   ", {@code reverse} answered "   ba", and {@code md5} hashed
 * a different string than PostgreSQL did.
 *
 * <p>The padding is not always dropped. It is part of the value, so it stays wherever the value is
 * handled as a bpchar rather than read as a text: in {@code octet_length}, which measures what is
 * stored; inside an array or a row, whose elements keep their own type; and in the value a client
 * is sent for a column of the type.
 */
final class BlankPadding {

    private BlankPadding() {
    }

    /**
     * The routines that handle a bpchar as one rather than reading it as a text, so the blanks it
     * was declared with are still there when they answer.
     */
    private static final Set<String> KEEPS_THE_PADDING = new HashSet<String>();

    static {
        // What is stored, measured in bytes.
        KEEPS_THE_PADDING.add("octet_length");
        // A value written into something that holds its type: an array, a row, a JSON string.
        KEEPS_THE_PADDING.add("array_agg");
        KEEPS_THE_PADDING.add("array_append");
        KEEPS_THE_PADDING.add("array_prepend");
        KEEPS_THE_PADDING.add("array_fill");
        KEEPS_THE_PADDING.add("to_json");
        KEEPS_THE_PADDING.add("to_jsonb");
        KEEPS_THE_PADDING.add("row_to_json");
        KEEPS_THE_PADDING.add("json_build_array");
        KEEPS_THE_PADDING.add("json_build_object");
        KEEPS_THE_PADDING.add("jsonb_build_array");
        KEEPS_THE_PADDING.add("jsonb_build_object");
        // The identity of a value, which is the value itself.
        KEEPS_THE_PADDING.add("pg_typeof");
        KEEPS_THE_PADDING.add("pg_column_size");
        // Declared over "any" rather than text, so each argument is written out as its own type
        // writes itself and a bpchar arrives with the blanks it was declared with. It is what
        // makes concat('ab'::char(5), '|') differ from 'ab'::char(5) || '|'.
        KEEPS_THE_PADDING.add("concat");
        KEEPS_THE_PADDING.add("concat_ws");
        KEEPS_THE_PADDING.add("format");
    }

    /** Whether a routine of this name reads a bpchar argument as a text. */
    static boolean readsItAsText(String name) {
        return name != null && !KEEPS_THE_PADDING.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    /** Whether a written type name is the blank-padded character type. */
    static boolean isBlankPadded(String typeName) {
        if (typeName == null) return false;
        String t = typeName.toLowerCase().trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        // Written with quotes it is PostgreSQL's own single byte, which pads nothing.
        if (t.startsWith("\"")) return false;
        return t.equals("char") || t.equals("character") || t.equals("bpchar");
    }

    /** Whether a written type name is one a bpchar is read as, dropping its blanks on the way. */
    private static boolean isAnotherStringType(String typeName) {
        if (typeName == null) return false;
        String t = typeName.toLowerCase().trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        return t.equals("text") || t.equals("varchar") || t.equals("character varying")
                || t.equals("name") || t.equals("citext");
    }

    /** The value with the blanks a bpchar declaration added removed. */
    static Object trimmed(Object value) {
        if (!(value instanceof String)) return value;
        String s = (String) value;
        int end = s.length();
        while (end > 0 && s.charAt(end - 1) == ' ') end--;
        return end == s.length() ? s : s.substring(0, end);
    }

    /**
     * A value being cast from {@code sourceType} to {@code targetType}, with the blanks dropped
     * where the conversion is from a bpchar to another string type.
     */
    static Object readAsOtherString(Object value, String targetType, String sourceType) {
        if (!isBlankPadded(sourceType) || !isAnotherStringType(targetType)) return value;
        return trimmed(value);
    }
}
