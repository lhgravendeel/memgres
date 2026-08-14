package com.memgres.engine;

import java.time.OffsetDateTime;

/**
 * The text an error's DETAIL prints one stored value as.
 *
 * <p>PostgreSQL writes the row a constraint refused, and the key a duplicate collided on, with the
 * output function of each column's own type: the whole point of printing them is that the reader
 * can go looking for that row again by what it says. Java's rendering of the same value is a
 * different string for half of what memgres stores: an array reads [1, 2] where PostgreSQL writes
 * {1,2}, a boolean reads true where PostgreSQL writes t, a timestamp puts a T between its date and
 * its time, and a bytea reads as the identity of its byte array. A DETAIL built out of
 * {@code toString} therefore named a row no query could return.
 */
final class ErrorValueText {

    private ErrorValueText() {
    }

    /** One value, or the word PostgreSQL writes in place of a column holding nothing. */
    static String of(Object value) {
        if (value == null) return "null";
        String written = TypeCoercion.toString(value);
        // A timestamptz is the one value that writer still hands back in Java's shape, because the
        // callers it was written for quote it and never had to agree with PostgreSQL about what is
        // inside the quotes. PostgreSQL separates the date from the time with a space and writes
        // the zone as an offset from UTC.
        if (value instanceof OffsetDateTime && written.indexOf('T') >= 0) {
            return RangeOperations.formatTimestamptz((OffsetDateTime) value);
        }
        return written;
    }
}
