package com.memgres.pgwire;

import com.memgres.engine.DataType;
import com.memgres.engine.MemgresException;
import com.memgres.engine.Session;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * What a bound parameter's value has to be for Bind to accept it.
 *
 * <p>PostgreSQL reads every parameter while it processes the Bind message: a value sent as text
 * goes through its type's input function and one sent as binary through its receive function, and
 * a value that will not read is refused there — before BindComplete, and before anything runs.
 * memgres read a parameter only where the statement's own expressions reached it, so a value that
 * could never be an integer was answered for after the client had already been told the bindings
 * were good, and one the statement never evaluated was not answered for at all.
 *
 * <p>What is read here is read by the same reader a cast uses, so a value this refuses is one the
 * statement would have refused anyway; the answer moves to the message that carried the value
 * rather than changing.
 */
final class PgWireParamValues {

    private PgWireParamValues() {}

    /**
     * The refusal reading this text as the type raises, or null where it reads — and null too for
     * a type whose reader is not one of the ones held against PostgreSQL's, since refusing a value
     * PostgreSQL accepts would be a worse answer than the one memgres gives now.
     */
    static MemgresException unreadable(Session session, String text, int typeOid) {
        if (session == null || text == null) return null;
        DataType type = DataType.fromOid(typeOid);
        if (type == null || !READ_AT_BIND.contains(type)) return null;
        try {
            session.executor().castValue(text, castName(type));
            return null;
        } catch (MemgresException refused) {
            // Class 22 is what PostgreSQL says about a value that is wrong for its type, and it is
            // the only thing a parameter's own text can be wrong about: anything else the reader
            // raises is about the type or the session rather than about what was sent.
            String state = refused.getSqlState();
            return state != null && state.startsWith("22") ? refused : null;
        } catch (RuntimeException | StackOverflowError unread) {
            // A reader that could not make sense of the value at all leaves it as it was, which is
            // what the statement is going to make of it in a moment.
            return null;
        }
    }

    /**
     * What refusing a binary value of the wrong length says, or null where the length is one the
     * type can be read from.
     *
     * <p>A receive function reads a fixed number of bytes and then insists the message holds
     * nothing more, so a value too short runs the reader off the end of the message and one too
     * long leaves data behind. PostgreSQL words the two differently, and words running off the end
     * differently again for a type read one byte at a time, because that reader asks for a byte
     * where the others ask for a run of them.
     */
    static MemgresException wrongBinaryLength(byte[] value, int typeOid, int position) {
        if (value == null) return null;
        // A numeric says how wide it is in its own first two bytes: four fields and then one for
        // each group of digits. Anything shorter than that runs the reader off the end, and
        // anything longer leaves digits nobody asked for behind.
        if (typeOid == 1700) {
            if (value.length < 8) return tooShort(typeOid);
            int digits = ((value[0] & 0xFF) << 8) | (value[1] & 0xFF);
            int needed = 8 + 2 * digits;
            if (value.length < needed) return tooShort(typeOid);
            if (value.length > needed) {
                return new MemgresException(
                        "incorrect binary data format in bind parameter " + position, "22P03");
            }
            return null;
        }
        Integer expected = FIXED_WIDTH.get(Integer.valueOf(typeOid));
        if (expected == null || value.length == expected.intValue()) return null;
        if (value.length > expected.intValue()) {
            return new MemgresException(
                    "incorrect binary data format in bind parameter " + position, "22P03");
        }
        return tooShort(typeOid);
    }

    /** What running off the end of the message says, in the words the type's reader uses. */
    private static MemgresException tooShort(int typeOid) {
        return new MemgresException(READ_BYTE_BY_BYTE.contains(Integer.valueOf(typeOid))
                ? "no data left in message" : "insufficient data left in message", "08P01");
    }

    /** The type spec the cast reader is asked for, which for an array is its element's and [ ]. */
    private static String castName(DataType type) {
        DataType element = DataType.elementOf(type);
        return element == null ? type.toRegtypeDisplay() : element.toRegtypeDisplay() + "[]";
    }

    /**
     * The types whose written form is read here. Each one's reader has been held against
     * PostgreSQL's input function value by value: it may accept text PostgreSQL refuses, which
     * leaves the answer where it was, but it refuses nothing PostgreSQL reads. The types left out
     * are read where they always were — {@code time} and {@code timetz} among them, because their
     * reader will not have {@code 12:34 PM}, which PostgreSQL reads as an ordinary time of day.
     */
    private static final Set<DataType> READ_AT_BIND = new HashSet<DataType>(Arrays.asList(
            DataType.BOOLEAN, DataType.SMALLINT, DataType.INTEGER, DataType.BIGINT,
            DataType.REAL, DataType.DOUBLE_PRECISION, DataType.NUMERIC, DataType.OID,
            DataType.XID,
            DataType.DATE, DataType.TIMESTAMP, DataType.TIMESTAMPTZ, DataType.INTERVAL,
            DataType.JSON, DataType.JSONB, DataType.BYTEA, DataType.XML, DataType.UUID,
            DataType.INET, DataType.CIDR, DataType.MACADDR, DataType.MACADDR8, DataType.MONEY,
            DataType.PG_LSN, DataType.BIT, DataType.VARBIT,
            DataType.POINT, DataType.LINE, DataType.LSEG, DataType.BOX, DataType.PATH,
            DataType.POLYGON, DataType.CIRCLE,
            DataType.INT4RANGE, DataType.INT8RANGE, DataType.NUMRANGE, DataType.DATERANGE,
            DataType.TSRANGE, DataType.TSTZRANGE,
            DataType.BOOL_ARRAY, DataType.INT2_ARRAY, DataType.INT4_ARRAY, DataType.INT8_ARRAY,
            DataType.FLOAT4_ARRAY, DataType.FLOAT8_ARRAY, DataType.NUMERIC_ARRAY,
            DataType.DATE_ARRAY, DataType.UUID_ARRAY,
            DataType.TEXT_ARRAY, DataType.VARCHAR_ARRAY, DataType.CHAR_ARRAY));

    /** How many bytes a type sent as binary is read from. */
    private static final Map<Integer, Integer> FIXED_WIDTH = new HashMap<Integer, Integer>();

    /** The types whose receive function asks for one byte at a time. */
    private static final Set<Integer> READ_BYTE_BY_BYTE = new HashSet<Integer>(
            Arrays.asList(Integer.valueOf(16), Integer.valueOf(18), Integer.valueOf(829)));

    static {
        FIXED_WIDTH.put(Integer.valueOf(16), Integer.valueOf(1));      // boolean
        FIXED_WIDTH.put(Integer.valueOf(18), Integer.valueOf(1));      // "char"
        FIXED_WIDTH.put(Integer.valueOf(21), Integer.valueOf(2));      // smallint
        FIXED_WIDTH.put(Integer.valueOf(23), Integer.valueOf(4));      // integer
        FIXED_WIDTH.put(Integer.valueOf(26), Integer.valueOf(4));      // oid
        FIXED_WIDTH.put(Integer.valueOf(28), Integer.valueOf(4));      // xid
        FIXED_WIDTH.put(Integer.valueOf(700), Integer.valueOf(4));     // real
        FIXED_WIDTH.put(Integer.valueOf(1082), Integer.valueOf(4));    // date
        FIXED_WIDTH.put(Integer.valueOf(20), Integer.valueOf(8));      // bigint
        FIXED_WIDTH.put(Integer.valueOf(701), Integer.valueOf(8));     // double precision
        FIXED_WIDTH.put(Integer.valueOf(790), Integer.valueOf(8));     // money
        FIXED_WIDTH.put(Integer.valueOf(1083), Integer.valueOf(8));    // time
        FIXED_WIDTH.put(Integer.valueOf(1114), Integer.valueOf(8));    // timestamp
        FIXED_WIDTH.put(Integer.valueOf(1184), Integer.valueOf(8));    // timestamptz
        FIXED_WIDTH.put(Integer.valueOf(3220), Integer.valueOf(8));    // pg_lsn
        FIXED_WIDTH.put(Integer.valueOf(27), Integer.valueOf(6));      // tid
        FIXED_WIDTH.put(Integer.valueOf(829), Integer.valueOf(6));     // macaddr
        FIXED_WIDTH.put(Integer.valueOf(1186), Integer.valueOf(16));   // interval
        FIXED_WIDTH.put(Integer.valueOf(2950), Integer.valueOf(16));   // uuid
    }
}
