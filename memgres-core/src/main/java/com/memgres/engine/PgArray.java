package com.memgres.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An array value: its elements, the lower bound of each of its dimensions, and — when it is known —
 * the type of its elements.
 *
 * <p>memgres used to carry an array either as a bare {@link List} or as the text of its literal,
 * and the two said different things. A list had no lower bounds, so an array written
 * {@code [0:1]={1,2}} could only be kept as text; and text is opaque, so every function but the
 * four that knew the {@code [lb:ub]=} prefix treated such an array as a string. This class is a
 * {@code List} — every site that reads an array by index goes on working — that also remembers the
 * two things a list cannot.
 *
 * <p>An array with the ordinary bounds is still just a list, so nothing has to construct one of
 * these to be correct; it is what an array becomes as soon as it has something to remember.
 */
public final class PgArray extends java.util.AbstractList<Object> {

    private final List<Object> elements;
    private final int[] lowerBounds;
    private final String elementType;

    private PgArray(List<Object> elements, int[] lowerBounds, String elementType) {
        this.elements = elements;
        this.lowerBounds = lowerBounds;
        this.elementType = elementType;
    }

    /** An array with the given elements, its dimensions starting at 1 and its element type unknown. */
    public static PgArray of(List<?> elements) {
        return of(elements, null, null);
    }

    /** An array whose elements are of a known type. */
    public static PgArray ofType(List<?> elements, String elementType) {
        return of(elements, null, elementType);
    }

    /**
     * An array with explicit lower bounds. A bound array shorter than the value's dimensions, or
     * one that is all 1s, leaves the array with the ordinary bounds.
     */
    public static PgArray of(List<?> elements, int[] lowerBounds, String elementType) {
        List<Object> copy = elements instanceof PgArray
                ? ((PgArray) elements).elements
                : new ArrayList<Object>(elements);
        int ndims = dimensionsOf(copy);
        int[] bounds = new int[ndims];
        for (int i = 0; i < ndims; i++) {
            bounds[i] = lowerBounds != null && i < lowerBounds.length ? lowerBounds[i] : 1;
        }
        return new PgArray(copy, bounds, elementType);
    }

    /**
     * The same array with a different element type. Returns this array when the type is already
     * what it says, so a value that has been named once is not copied again.
     */
    public PgArray withElementType(String type) {
        if (type == null || type.equals(elementType)) return this;
        return new PgArray(elements, lowerBounds, type);
    }

    /** The same elements, carrying the bounds and element type of {@code source}. */
    public static List<Object> like(List<?> source, List<Object> elements) {
        if (!(source instanceof PgArray)) return elements;
        PgArray array = (PgArray) source;
        return of(elements, array.lowerBounds, array.elementType);
    }

    /** The elements' type, or null when nothing has said what it is. */
    public String elementType() {
        return elementType;
    }

    /** The lower bound of each dimension, one entry per dimension. */
    public int[] lowerBounds() {
        int[] copy = new int[lowerBounds.length];
        System.arraycopy(lowerBounds, 0, copy, 0, lowerBounds.length);
        return copy;
    }

    /** The lower bound of one dimension, counting from 1, or 1 when the array has no such one. */
    public int lowerBound(int dimension) {
        return dimension >= 1 && dimension <= lowerBounds.length ? lowerBounds[dimension - 1] : 1;
    }

    /** The size of each dimension. */
    public int[] dims() {
        return dimensionsOf(elements, lowerBounds.length);
    }

    /** True when any dimension starts somewhere other than 1, which the text form has to say. */
    public boolean hasCustomLowerBounds() {
        for (int i = 0; i < lowerBounds.length; i++) {
            if (lowerBounds[i] != 1) return true;
        }
        return false;
    }

    /** The {@code [lb:ub]=} prefix PostgreSQL writes in front of such an array. */
    public String boundsPrefix() {
        int[] dims = dims();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lowerBounds.length; i++) {
            sb.append('[').append(lowerBounds[i]).append(':')
                    .append(lowerBounds[i] + dims[i] - 1).append(']');
        }
        return sb.append('=').toString();
    }

    /** The bounds of an array whose elements have been added to or taken from the end. */
    public PgArray resized(List<Object> newElements) {
        return of(newElements, lowerBounds, elementType);
    }

    @Override
    public Object get(int index) {
        return elements.get(index);
    }

    @Override
    public int size() {
        return elements.size();
    }

    /** How many dimensions a nested list has, counted down its first element. */
    static int dimensionsOf(List<?> list) {
        int ndims = 0;
        Object probe = list;
        while (probe instanceof List<?>) {
            List<?> level = (List<?>) probe;
            ndims++;
            probe = level.isEmpty() ? null : level.get(0);
        }
        return list.isEmpty() ? 0 : ndims;
    }

    private static int[] dimensionsOf(List<?> list, int ndims) {
        int[] dims = new int[ndims];
        Object probe = list;
        for (int i = 0; i < ndims; i++) {
            if (!(probe instanceof List<?>)) {
                dims[i] = 0;
                continue;
            }
            List<?> level = (List<?>) probe;
            dims[i] = level.size();
            probe = level.isEmpty() ? null : level.get(0);
        }
        return dims;
    }

    /** The elements of an array in row-major order, with every dimension flattened away. */
    public static List<Object> flatten(List<?> list) {
        List<Object> flat = new ArrayList<Object>();
        flattenInto(list, flat);
        return flat;
    }

    private static void flattenInto(List<?> list, List<Object> into) {
        for (Object element : list) {
            if (element instanceof List<?>) flattenInto((List<?>) element, into);
            else into.add(element);
        }
    }

    /** An empty array of the given element type. */
    public static PgArray empty(String elementType) {
        return of(Collections.emptyList(), null, elementType);
    }

    /**
     * Read an array however it arrives — as one of these, as a plain list, or as the text of a
     * literal — or null when the value is not an array at all. Text is read by the one reader that
     * knows PostgreSQL's rules, so an array that has been through a column is the array it was.
     */
    public static PgArray from(Object value) {
        if (value instanceof PgArray) return (PgArray) value;
        if (value instanceof List<?>) return of((List<?>) value);
        if (value instanceof String && looksLikeArrayText((String) value)) {
            ArrayLiteral literal = ArrayLiteral.parse((String) value);
            return of(literal.elements(), literal.lowerBounds(), null);
        }
        return null;
    }

    /** True when the text could be an array literal: braces, or the bounds prefix in front of them. */
    public static boolean looksLikeArrayText(String text) {
        int i = 0;
        while (i < text.length() && Character.isWhitespace(text.charAt(i))) i++;
        if (i < text.length() && text.charAt(i) == '[') {
            int assign = text.indexOf("]=", i);
            if (assign < 0) return false;
            i = assign + 2;
            while (i < text.length() && Character.isWhitespace(text.charAt(i))) i++;
        }
        return i < text.length() && text.charAt(i) == '{';
    }
}
