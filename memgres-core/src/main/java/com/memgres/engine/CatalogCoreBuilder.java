package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds core catalog tables that deal with relational metadata:
 * pg_class, pg_attribute, pg_type, pg_namespace, pg_enum, pg_proc.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogCoreBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogCoreBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    /**
     * The array types the catalog registers: {array OID, element OID, typname, typalign}.
     * The name is spelled out rather than derived, because an element type memgres does not
     * carry in {@link DataType} would otherwise produce a name like {@code _18} that exists in
     * no PostgreSQL and resolves nowhere else.
     */
    private static final Object[][] STD_ARRAYS = {
            {1009, 25, "_text", "i"},
            {1007, 23, "_int4", "i"},
            {1034, 1033, "_aclitem", "d"},
            {1000, 16, "_bool", "i"},
            {1005, 21, "_int2", "i"},
            {1016, 20, "_int8", "d"},
            {1021, 700, "_float4", "i"},
            {1022, 701, "_float8", "d"},
            {1231, 1700, "_numeric", "i"},
            {1015, 1043, "_varchar", "i"},
            {1014, 1042, "_bpchar", "i"},
            {1003, 19, "_name", "i"},
            {1182, 1082, "_date", "i"},
            {1115, 1114, "_timestamp", "d"},
            {1185, 1184, "_timestamptz", "d"},
            {1183, 1083, "_time", "d"},
            {1270, 1266, "_timetz", "d"},
            {2951, 2950, "_uuid", "i"},
            {1001, 17, "_bytea", "i"},
            {1187, 1186, "_interval", "d"},
            {199, 114, "_json", "i"},
            {3807, 3802, "_jsonb", "i"},
            {1041, 869, "_inet", "i"},
            {1028, 26, "_oid", "i"},
            {1008, 24, "_regproc", "i"},
            {2210, 2205, "_regclass", "i"},
            {2211, 2206, "_regtype", "i"},
            {1002, 18, "_char", "i"},
            {1011, 28, "_xid", "i"},
            {1006, 22, "_int2vector", "i"},
            {1013, 30, "_oidvector", "i"},
            {791, 790, "_money", "d"},
            {1561, 1560, "_bit", "i"},
            {1563, 1562, "_varbit", "i"},
            {651, 650, "_cidr", "i"},
            {1040, 829, "_macaddr", "i"},
            {775, 774, "_macaddr8", "i"},
            {3643, 3614, "_tsvector", "i"},
            {3645, 3615, "_tsquery", "i"},
            {143, 142, "_xml", "i"},
            {1017, 600, "_point", "d"},
            {1018, 601, "_lseg", "d"},
            {1019, 602, "_path", "d"},
            {1020, 603, "_box", "d"},
            {1027, 604, "_polygon", "d"},
            {719, 718, "_circle", "d"},
            {629, 628, "_line", "d"},
            {3905, 3904, "_int4range", "i"},
            {3907, 3906, "_numrange", "i"},
            {3909, 3908, "_tsrange", "d"},
            {3911, 3910, "_tstzrange", "d"},
            {3913, 3912, "_daterange", "i"},
            {3927, 3926, "_int8range", "d"},
            {6150, 4451, "_int4multirange", "i"},
            {6151, 4532, "_nummultirange", "i"},
            {6152, 4533, "_tsmultirange", "d"},
            {6153, 4534, "_tstzmultirange", "d"},
            {6155, 4535, "_datemultirange", "i"},
            {6157, 4536, "_int8multirange", "d"},
            // hstore is an extension type, so PostgreSQL's OID for it and for its array differ
            // from installation to installation; memgres gives both a fixed one of its own. What
            // has to be true either way is that following hstore's typarray reaches a type, and
            // that an array of hstore can be named at all.
            {90002, 90001, "_hstore", "i"},
    };

    /** element type OID -> its array type OID, for pg_type.typarray. */
    private static final Map<Integer, Integer> ARRAY_OF;

    /** Built-in names PostgreSQL declares a signature for; see {@link BuiltinFunctionSignatures}. */
    private static final Set<String> SIGNED_BUILTINS;

    /**
     * Types whose I/O functions are not spelled {@code <typname>in}. PG names them after the
     * implementation rather than the type, and a regproc column that reports the wrong name is
     * as useless to a tool as one that reports none.
     */
    private static final Map<String, String> IO_BASE;

    /** Types that carry a typmod modifier, keyed by typname -> typmod function prefix. */
    private static final Map<String, String> TYPMOD_IO;

    /** Types with a dedicated typanalyze function. */
    private static final Map<String, String> TYPANALYZE;

    /**
     * The physical attributes PostgreSQL records for each type, read off a PG 18 catalog rather
     * than inferred: {typname, typlen, typbyval, typtype, typcategory, typispreferred, typdelim,
     * typalign, typstorage, typcollation}. These describe how a value is laid out and how it is
     * read back, so a client that decodes a value by following them — or a server-side check that
     * two types are physically compatible — needs the recorded answer, not a plausible one. A box
     * array is delimited by semicolons, not commas; name sorts under the C collation; aclitem is
     * sixteen bytes and double-aligned; and record is a varlena however short its length looks.
     *
     * <p>This is the one place the answer is kept: pg_type reports these values and pg_attribute
     * reports the same ones for a column of the type, so the two cannot disagree about a type.
     */
    private static final String[][] TYPE_ATTRIBUTES = {
            {"_aclitem", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_bit", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_bool", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_box", "-1", "f", "b", "A", "f", ";", "d", "x", "0"},
            {"_bpchar", "-1", "f", "b", "A", "f", ",", "i", "x", "100"},
            {"_bytea", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_char", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_cidr", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_circle", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_date", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_datemultirange", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_daterange", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_float4", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_float8", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_inet", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int2", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int2vector", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int4", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int4multirange", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int4range", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_int8", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_int8multirange", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_int8range", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_interval", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_json", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_jsonb", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_line", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_lseg", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_macaddr", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_macaddr8", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_money", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_name", "-1", "f", "b", "A", "f", ",", "i", "x", "950"},
            {"_numeric", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_nummultirange", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_numrange", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_oid", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_oidvector", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_path", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_point", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_polygon", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_record", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"_regclass", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_regproc", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_regtype", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_text", "-1", "f", "b", "A", "f", ",", "i", "x", "100"},
            {"_time", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_timestamp", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_timestamptz", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_timetz", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_tsmultirange", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_tsquery", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_tsrange", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_tstzmultirange", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_tstzrange", "-1", "f", "b", "A", "f", ",", "d", "x", "0"},
            {"_tsvector", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_uuid", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_varbit", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_varchar", "-1", "f", "b", "A", "f", ",", "i", "x", "100"},
            {"_xid", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"_xml", "-1", "f", "b", "A", "f", ",", "i", "x", "0"},
            {"aclitem", "16", "f", "b", "U", "f", ",", "d", "p", "0"},
            {"any", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anyarray", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"anycompatible", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anycompatiblearray", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"anycompatiblemultirange", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"anycompatiblenonarray", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anycompatiblerange", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"anyelement", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anyenum", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anymultirange", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"anynonarray", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"anyrange", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"bit", "-1", "f", "b", "V", "f", ",", "i", "x", "0"},
            {"bool", "1", "t", "b", "B", "t", ",", "c", "p", "0"},
            {"box", "32", "f", "b", "G", "f", ";", "d", "p", "0"},
            {"bpchar", "-1", "f", "b", "S", "f", ",", "i", "x", "100"},
            {"bytea", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
            {"cidr", "-1", "f", "b", "I", "f", ",", "i", "m", "0"},
            {"circle", "24", "f", "b", "G", "f", ",", "d", "p", "0"},
            {"cstring", "-2", "f", "p", "P", "f", ",", "c", "p", "0"},
            {"date", "4", "t", "b", "D", "f", ",", "i", "p", "0"},
            {"datemultirange", "-1", "f", "m", "R", "f", ",", "i", "x", "0"},
            {"daterange", "-1", "f", "r", "R", "f", ",", "i", "x", "0"},
            // A memgres enum column is a PG enum type: four bytes of OID into pg_enum.
            {"enum", "4", "t", "e", "E", "f", ",", "i", "p", "0"},
            {"event_trigger", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"float4", "4", "t", "b", "N", "f", ",", "i", "p", "0"},
            {"float8", "8", "t", "b", "N", "t", ",", "d", "p", "0"},
            {"hstore", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
            {"inet", "-1", "f", "b", "I", "t", ",", "i", "m", "0"},
            {"int2", "2", "t", "b", "N", "f", ",", "s", "p", "0"},
            {"int2vector", "-1", "f", "b", "A", "f", ",", "i", "p", "0"},
            {"int4", "4", "t", "b", "N", "f", ",", "i", "p", "0"},
            {"int4multirange", "-1", "f", "m", "R", "f", ",", "i", "x", "0"},
            {"int4range", "-1", "f", "r", "R", "f", ",", "i", "x", "0"},
            {"int8", "8", "t", "b", "N", "f", ",", "d", "p", "0"},
            {"int8multirange", "-1", "f", "m", "R", "f", ",", "d", "x", "0"},
            {"int8range", "-1", "f", "r", "R", "f", ",", "d", "x", "0"},
            {"internal", "8", "t", "p", "P", "f", ",", "d", "p", "0"},
            {"interval", "16", "f", "b", "T", "t", ",", "d", "p", "0"},
            {"json", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
            {"jsonb", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
            {"line", "24", "f", "b", "G", "f", ",", "d", "p", "0"},
            {"lseg", "32", "f", "b", "G", "f", ",", "d", "p", "0"},
            {"macaddr", "6", "f", "b", "U", "f", ",", "i", "p", "0"},
            {"macaddr8", "8", "f", "b", "U", "f", ",", "i", "p", "0"},
            {"money", "8", "t", "b", "N", "f", ",", "d", "p", "0"},
            {"name", "64", "f", "b", "S", "f", ",", "c", "p", "950"},
            {"numeric", "-1", "f", "b", "N", "f", ",", "i", "m", "0"},
            {"nummultirange", "-1", "f", "m", "R", "f", ",", "i", "x", "0"},
            {"numrange", "-1", "f", "r", "R", "f", ",", "i", "x", "0"},
            {"oid", "4", "t", "b", "N", "t", ",", "i", "p", "0"},
            {"oidvector", "-1", "f", "b", "A", "f", ",", "i", "p", "0"},
            {"path", "-1", "f", "b", "G", "f", ",", "d", "x", "0"},
            {"point", "16", "f", "b", "G", "f", ",", "d", "p", "0"},
            {"polygon", "-1", "f", "b", "G", "f", ",", "d", "x", "0"},
            {"record", "-1", "f", "p", "P", "f", ",", "d", "x", "0"},
            {"regclass", "4", "t", "b", "N", "f", ",", "i", "p", "0"},
            {"regproc", "4", "t", "b", "N", "f", ",", "i", "p", "0"},
            {"regtype", "4", "t", "b", "N", "f", ",", "i", "p", "0"},
            {"text", "-1", "f", "b", "S", "t", ",", "i", "x", "100"},
            {"time", "8", "t", "b", "D", "f", ",", "d", "p", "0"},
            {"timestamp", "8", "t", "b", "D", "f", ",", "d", "p", "0"},
            {"timestamptz", "8", "t", "b", "D", "t", ",", "d", "p", "0"},
            {"timetz", "12", "f", "b", "D", "f", ",", "d", "p", "0"},
            {"trigger", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"tsmultirange", "-1", "f", "m", "R", "f", ",", "d", "x", "0"},
            {"tsquery", "-1", "f", "b", "U", "f", ",", "i", "p", "0"},
            {"tsrange", "-1", "f", "r", "R", "f", ",", "d", "x", "0"},
            {"tstzmultirange", "-1", "f", "m", "R", "f", ",", "d", "x", "0"},
            {"tstzrange", "-1", "f", "r", "R", "f", ",", "d", "x", "0"},
            {"tsvector", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
            {"uuid", "16", "f", "b", "U", "f", ",", "c", "p", "0"},
            {"varbit", "-1", "f", "b", "V", "t", ",", "i", "x", "0"},
            {"varchar", "-1", "f", "b", "S", "f", ",", "i", "x", "100"},
            {"void", "4", "t", "p", "P", "f", ",", "i", "p", "0"},
            {"xid", "4", "t", "b", "U", "f", ",", "i", "p", "0"},
            {"xml", "-1", "f", "b", "U", "f", ",", "i", "x", "0"},
    };

    /** typname -> its row in {@link #TYPE_ATTRIBUTES}. */
    private static final Map<String, String[]> TYPE_ATTRS_BY_NAME;

    /** One type's recorded physical attributes, or null when memgres carries no record of it. */
    private static String[] typeAttrs(String typname) {
        return TYPE_ATTRS_BY_NAME.get(typname);
    }

    /**
     * The pg_type name a column of this type is stored under. A serial column is an integer
     * column with a sequence behind it, so it is laid out exactly as the integer is.
     */
    private static String storedTypeName(DataType dt) {
        switch (dt) {
            case SERIAL: return "int4";
            case BIGSERIAL: return "int8";
            case SMALLSERIAL: return "int2";
            default: return dt.getPgName();
        }
    }

    /** PostgreSQL's typlen for this type: its width in bytes, or -1 for a varlena. */
    static short typeLength(DataType dt) {
        String[] attrs = typeAttrs(storedTypeName(dt));
        return attrs == null ? (short) -1 : Short.parseShort(attrs[1]);
    }

    /** Whether a value of this type is passed by value, as pg_type records it. */
    private static boolean byValue(DataType dt) {
        String[] attrs = typeAttrs(storedTypeName(dt));
        return attrs != null && "t".equals(attrs[2]);
    }

    /** PostgreSQL's typalign: what boundary a value of the type has to start on. */
    static String typeAlign(DataType dt) {
        String[] attrs = typeAttrs(storedTypeName(dt));
        return attrs == null ? "i" : attrs[7];
    }

    /** PostgreSQL's typstorage: p plain, m main, e external, x extended. */
    static String typeStorage(DataType dt) {
        String[] attrs = typeAttrs(storedTypeName(dt));
        return attrs == null ? "p" : attrs[8];
    }

    static {
        Map<Integer, Integer> arrayOf = new HashMap<>();
        for (Object[] a : STD_ARRAYS) arrayOf.put((Integer) a[1], (Integer) a[0]);
        ARRAY_OF = Collections.unmodifiableMap(arrayOf);

        Map<String, String[]> attrs = new HashMap<>();
        for (String[] row : TYPE_ATTRIBUTES) attrs.put(row[0], row);
        // The bootstrap types are recorded once, in PgInternalTypes, and answer here in the same
        // shape rather than being written out a second time.
        for (Object[] it : PgInternalTypes.TYPES) {
            String name = (String) it[1];
            if (attrs.containsKey(name)) continue;
            attrs.put(name, new String[]{name, String.valueOf(it[2]),
                    Boolean.TRUE.equals(it[3]) ? "t" : "f", (String) it[4], (String) it[5],
                    "f", ",", (String) it[6], (String) it[7], String.valueOf(it[12])});
        }
        for (Object[] a : PgInternalTypes.ARRAYS) {
            String name = (String) a[1];
            if (attrs.containsKey(name)) continue;
            attrs.put(name, new String[]{name, "-1", "f", "b", "A", "f", ",",
                    (String) a[3], "x", "0"});
        }
        TYPE_ATTRS_BY_NAME = Collections.unmodifiableMap(attrs);

        Set<String> signed = new HashSet<>();
        for (String[] sig : BuiltinFunctionSignatures.SIGNATURES) signed.add(sig[0]);
        SIGNED_BUILTINS = Collections.unmodifiableSet(signed);

        Map<String, String> io = new HashMap<>();
        for (String t : new String[]{"bit", "box", "cidr", "circle", "date", "inet", "interval",
                "json", "jsonb", "line", "lseg", "macaddr", "macaddr8", "numeric", "path", "point",
                "time", "timestamp", "timestamptz", "timetz", "uuid", "varbit", "xml", "hstore"}) {
            io.put(t, t + "_");
        }
        io.put("money", "cash_");
        io.put("polygon", "poly_");
        for (String t : new String[]{"int4range", "int8range", "numrange", "daterange",
                "tsrange", "tstzrange"}) {
            io.put(t, "range_");
        }
        for (String t : new String[]{"int4multirange", "int8multirange", "nummultirange",
                "datemultirange", "tsmultirange", "tstzmultirange"}) {
            io.put(t, "multirange_");
        }
        IO_BASE = Collections.unmodifiableMap(io);

        Map<String, String> typmod = new HashMap<>();
        for (String t : new String[]{"bit", "varbit", "bpchar", "varchar", "numeric", "interval",
                "time", "timestamp", "timestamptz", "timetz"}) {
            typmod.put(t, t + "typmod");
        }
        TYPMOD_IO = Collections.unmodifiableMap(typmod);

        Map<String, String> analyze = new HashMap<>();
        for (String t : new String[]{"int4range", "int8range", "numrange", "daterange",
                "tsrange", "tstzrange"}) {
            analyze.put(t, "range_typanalyze");
        }
        for (String t : new String[]{"int4multirange", "int8multirange", "nummultirange",
                "datemultirange", "tsmultirange", "tstzmultirange"}) {
            analyze.put(t, "multirange_typanalyze");
        }
        analyze.put("tsvector", "ts_typanalyze");
        TYPANALYZE = Collections.unmodifiableMap(analyze);
    }

    /**
     * The types PostgreSQL builds out of a fixed number of another type, and records that in
     * typelem even though they are not array types.
     *
     * <p>{@code name} is 64 chars, a point is two float8s, a segment and a box are two points, a
     * line is three float8s. PostgreSQL fills typelem for all five — its subscripting operator
     * reads it, which is why {@code ('(1,2)'::point)[0]} answers 1 — and memgres reported 0, so
     * anything asking the catalog what a point is made of was told nothing.
     */
    private static final Map<String, Integer> FIXED_ELEMENT_OF;

    static {
        Map<String, Integer> elems = new HashMap<>();
        elems.put("name", 18);    // "char"
        elems.put("point", 701);  // float8
        elems.put("lseg", 600);   // point
        elems.put("box", 600);    // point
        elems.put("line", 701);   // float8
        FIXED_ELEMENT_OF = Collections.unmodifiableMap(elems);
    }

    /**
     * Pseudo-types: trigger, event_trigger, void, record, the polymorphic family, and the two
     * types only an internal function deals in. Columns: name, oid, typarray, and whether
     * PostgreSQL gives it binary I/O — how each is laid out is recorded once, in
     * {@link #TYPE_ATTRIBUTES}, because a pseudo-type's width is not the one its name suggests:
     * cstring is a pointer of length -2 and record is a varlena, so neither is passed by value.
     *
     * <p>Two of them do have an array type. {@code record}'s is {@code _record} and
     * {@code cstring}'s is {@code _cstring}, and both were reported as 0 — so a client following
     * typarray to find out how an array of the type is described reached nothing, on a type
     * memgres does carry the array row for. Five of them have binary I/O: record, anyarray,
     * anycompatiblearray, void and cstring, and reporting '-' for those said no binary
     * representation exists.
     */
    private static final String[][] PSEUDO_TYPES = {
            {"trigger", "2279", "0", "f"},
            {"event_trigger", "3838", "0", "f"},
            {"void", "2278", "0", "t"},
            {"record", "2249", "2287", "t"},
            {"any", "2276", "0", "f"},
            {"anyelement", "2283", "0", "f"},
            {"anyarray", "2277", "0", "t"},
            {"anynonarray", "2776", "0", "f"},
            {"anyenum", "3500", "0", "f"},
            {"anyrange", "3831", "0", "f"},
            {"anymultirange", "4537", "0", "f"},
            {"anycompatible", "5077", "0", "f"},
            {"anycompatiblearray", "5078", "0", "t"},
            {"anycompatiblenonarray", "5079", "0", "f"},
            {"anycompatiblerange", "5080", "0", "f"},
            {"anycompatiblemultirange", "4538", "0", "f"},
            {"internal", "2281", "0", "f"},
            {"cstring", "2275", "1263", "t"},
    };

    /** The pg_type row for a relation's composite row type. */
    private Object[] rowType(String schemaName, String relName, int nsOid) {
        return new Object[]{
                rowTypeOid(schemaName, relName), relName, nsOid, 10,
                (short) -1, false, "c", "C", false, true, ",",
                oids.oid("rel:" + schemaName + "." + relName), regproc(null), 0,
                rowTypeArrayOid(schemaName, relName),
                regproc("record_in"), regproc("record_out"), regproc("record_recv"),
                regproc("record_send"),
                regproc(null), regproc(null), regproc(null), "d", "x",
                false, 0, -1, 0, 0, null, null, null, 1
        };
    }

    /**
     * The {@code _name} array type that goes with a composite. Every composite type in PostgreSQL
     * has one — {@code pg_type.typarray} is never zero for a {@code typtype = 'c'} row — and a
     * client following that link to describe an array of the row type found nothing at the far end.
     */
    private Object[] rowTypeArray(String schemaName, String relName, int nsOid) {
        return new Object[]{
                rowTypeArrayOid(schemaName, relName), "_" + relName, nsOid, 10,
                (short) -1, false, "b", "A", false, true, ",",
                0, regproc("array_subscript_handler"), rowTypeOid(schemaName, relName), 0,
                regproc("array_in"), regproc("array_out"), regproc("array_recv"),
                regproc("array_send"),
                regproc(null), regproc(null), regproc("array_typanalyze"), "d", "x",
                false, 0, -1, 0, 0, null, null, null, 1
        };
    }

    /** The OID of the user-defined type a written name denotes, or {@code fallback}. */
    private int userTypeOid(String written, int fallback) {
        String key = TypeNamespace.oidKeyFor(database, written);
        return key == null ? fallback : oids.oid(key);
    }

    /**
     * The OID a column declared with a user-defined type points at. An array of one is a type of
     * its own with an OID of its own, registered beside the element's, exactly as an array of any
     * other type is; a column of {@code e[]} that pointed at {@code e} told format_type the column
     * held one value where it holds a list of them.
     */
    private int userTypeColumnOid(Column c, String written, int fallback) {
        String key = TypeNamespace.oidKeyFor(database, written);
        if (key == null) return fallback;
        return oids.oid(c.getArrayElementType() != null ? key + "[]" : key);
    }

    /**
     * The OID a column declared with a domain points at. Brackets after the domain's name make an
     * array of the domain, which PostgreSQL registers as a type of its own; a domain that is
     * itself built over an array is one type, and the column points straight at it.
     */
    private int domainColumnOid(Column c, int fallback) {
        String key = TypeNamespace.oidKeyFor(database, c.getDomainTypeName());
        if (key == null) return fallback;
        DomainType domain = database.getDomain(c.getDomainTypeName());
        boolean arrayOfTheDomain = c.getArrayElementType() != null
                && (domain == null || domain.getArrayElementType() == null);
        return oids.oid(arrayOfTheDomain ? key + "[]" : key);
    }

    private int rowTypeArrayOid(String schemaName, String relName) {
        return oids.oid(TypeNamespace.oidKey(schemaName, relName) + "[]");
    }

    /** The OID of a relation's row type, which pg_class.reltype names. */
    private int rowTypeOid(String schemaName, String relName) {
        return oids.oid(TypeNamespace.oidKey(schemaName, relName));
    }

    /**
     * The name of a pseudo-type or of aclitem, or null. regtype renders a type OID by name, and
     * one it cannot name renders as the number the reader already had.
     */
    static String otherTypeName(int oid) {
        if (oid == 1033) return "aclitem";
        if (oid == 1034) return "aclitem[]";
        for (String[] pt : PSEUDO_TYPES) {
            if (Integer.parseInt(pt[1]) == oid) return pt[0];
        }
        return null;
    }

    /**
     * The element type OID of one of the standard array types, or 0 when the OID is not one.
     * Lets regtype print {@code cidr[]} for 651 rather than the number, without a second list
     * of array types to keep in step with this one.
     */
    static int arrayElementOid(int arrayOid) {
        for (Object[] a : STD_ARRAYS) {
            if (((Integer) a[0]).intValue() == arrayOid) return ((Integer) a[1]).intValue();
        }
        return 0;
    }

    /** The prefix PG's I/O functions for this type are named with. */
    private static String ioBaseName(String typname) {
        String base = IO_BASE.get(typname);
        return base != null ? base : typname;
    }

    /**
     * A regproc value: prints as the function's name and compares as its OID, so both
     * {@code typinput::text} and {@code typinput = 0} behave the way PG's regproc column does.
     * A null or "-" name is PG's InvalidOid.
     */
    private RegprocValue regproc(String name) {
        if (name == null || "-".equals(name)) return new RegprocValue(0, "-");
        return new RegprocValue(oids.oid("proc:" + name), name);
    }

    /**
     * An oidvector built from PG's space-separated spelling. Held as a vector rather than a
     * string so {@code array_length(proargtypes, 1)} and {@code 0 = ANY (proargtypes)} — the
     * shape catalog-reading tools use — resolve the way they do against PG.
     */
    private static PgVector oidvector(String spaceSeparated) {
        List<Object> elems = new ArrayList<>();
        if (spaceSeparated != null && !spaceSeparated.isEmpty()) {
            for (String part : spaceSeparated.trim().split("\\s+")) {
                if (!part.isEmpty()) elems.add(Integer.parseInt(part));
            }
        }
        return new PgVector(elems);
    }

    /** Resolve the schema that owns a given sequence via the schemaObjectRegistry. */
    Table buildPgClass() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("relname", DataType.NAME),
                colNN("relnamespace", DataType.OID),
                col("reltype", DataType.OID),
                col("reloftype", DataType.OID),
                colNN("relowner", DataType.OID),
                col("relam", DataType.OID),
                col("relfilenode", DataType.OID),
                col("reltablespace", DataType.OID),
                col("relpages", DataType.INTEGER),
                col("reltuples", DataType.REAL),
                col("relallvisible", DataType.INTEGER),
                col("relallfrozen", DataType.INTEGER),
                col("reltoastrelid", DataType.OID),
                col("relhasindex", DataType.BOOLEAN),
                col("relisshared", DataType.BOOLEAN),
                col("relpersistence", DataType.INTERNAL_CHAR),
                colNN("relkind", DataType.INTERNAL_CHAR),
                col("relnatts", DataType.SMALLINT),
                col("relchecks", DataType.SMALLINT),
                col("relhasrules", DataType.BOOLEAN),
                col("relhastriggers", DataType.BOOLEAN),
                col("relhassubclass", DataType.BOOLEAN),
                col("relrowsecurity", DataType.BOOLEAN),
                col("relforcerowsecurity", DataType.BOOLEAN),
                col("relispopulated", DataType.BOOLEAN),
                col("relreplident", DataType.INTERNAL_CHAR),
                col("relispartition", DataType.BOOLEAN),
                col("relrewrite", DataType.OID),
                col("relfrozenxid", DataType.XID),
                col("relminmxid", DataType.XID),
                col("relacl", DataType.ACLITEM_ARRAY),
                col("reloptions", DataType.TEXT_ARRAY),
                col("relpartbound", DataType.PG_NODE_TREE),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_class", cols);

        // System catalog relations (pg_catalog schema). The names, their kind and the columns
        // behind each come from one place, so pg_class never advertises a relation the server
        // cannot answer for and relnatts is counted rather than guessed.
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        Map<String, List<Column>> catalogShapes = new PgCatalogBuilder(database, oids).catalogShapes();
        for (Map.Entry<String, List<Column>> shape : catalogShapes.entrySet()) {
            String sysTable = shape.getKey();
            int sysOid = oids.oid("rel:pg_catalog." + sysTable);
            table.insertRow(new Object[]{
                    sysOid, sysTable, pgCatalogNs,
                    rowTypeOid("pg_catalog", sysTable), 0,   // reltype, reloftype
                    10,              // relowner
                    0,               // relam
                    sysOid,          // relfilenode (= oid for system tables)
                    0,               // reltablespace
                    0, 0.0, 0, 0, 0,   // relpages, reltuples, relallvisible, relallfrozen, reltoastrelid
                    false, false, "p", PgCatalogRelations.relkind(sysTable),
                    (short) userColumnCount(shape.getValue()), (short) 0,   // relnatts, relchecks
                    false, false, false, false, false, // relhasrules..relforcerowsecurity

                    true, "d", false,       // relispopulated, relreplident, relispartition
                    0, 0, 0,                // relrewrite, relfrozenxid, relminmxid
                    null, null, null, 1     // relacl, reloptions, relpartbound, xmin
            });
        }
        // An index is a relation, but not a table. Reporting one as 'r' puts it in the way of
        // every tool that lists user tables by relkind.
        for (String sysIndex : PgCatalogRelations.INDEXES) {
            int sysOid = oids.oid("rel:pg_catalog." + sysIndex);
            table.insertRow(new Object[]{
                    sysOid, sysIndex, pgCatalogNs,
                    0, 0, 10, 403, sysOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "i",
                    (short) 1, (short) 0,
                    false, false, false, false, false,
                    true, "n", false,
                    0, 0, 0,
                    null, null, null, 1
            });
        }

        // information_schema's views are relations too, and a tool that reads pg_class to find
        // out what it may query has to find them there.
        int infoSchemaNs = oids.oid("ns:information_schema");
        for (String isView : InfoSchemaBuilder.INFORMATION_SCHEMA_VIEWS) {
            int viewOid = oids.oid("rel:information_schema." + isView);
            table.insertRow(new Object[]{
                    viewOid, isView, infoSchemaNs,
                    0, 0, 10, 0, 0, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "v",
                    (short) 0, (short) 0,
                    false, false, false, false, false,
                    true, "d", false,
                    0, 0, 0,
                    null, null, null, 1
            });
        }

        // M22: tables on either side of a FK carry internal RI triggers in PG,
        // so pg_class.relhastriggers is true for them. Collect FK-referenced tables.
        java.util.Set<String> fkReferencedTables = new java.util.HashSet<>();
        for (Schema sch : database.getSchemas().values()) {
            for (Table tt : sch.getTables().values()) {
                for (StoredConstraint sc : tt.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY && sc.getReferencesTable() != null) {
                        fkReferencedTables.add(sc.getReferencesTable().toLowerCase());
                    }
                }
            }
        }

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            int nsOid = oids.oid("ns:" + schemaEntry.getKey());
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int ownerOid = resolveOwnerOid(database, oids, "table:" + schemaEntry.getKey() + "." + t.getName());
                int tblOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                // Count CHECK constraints
                short checkCount = 0;
                boolean hasTriggers = false;
                boolean hasForeignKey = false;
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.CHECK) checkCount++;
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) hasForeignKey = true;
                }
                if (database.getAllTriggers().containsKey(t.getName())) hasTriggers = true;
                // M22: FK endpoints have internal RI triggers
                if (hasForeignKey || fkReferencedTables.contains(t.getName().toLowerCase())) hasTriggers = true;
                boolean hasIdx = !t.getConstraints().isEmpty() || database.getIndexColumns().keySet().stream()
                        .anyMatch(idx -> { String ti = database.getIndexTable(idx); return ti != null && ti.equalsIgnoreCase(schemaEntry.getKey() + "." + t.getName()); });
                // Partition metadata for pg_class
                String relkind = t.getPartitionStrategy() != null ? "p" : "r";
                boolean relispartition = t.getPartitionParent() != null;
                String relpartbound = relispartition ? formatPartitionBound(t) : null;
                table.insertRow(new Object[]{
                        tblOid, t.getName(), nsOid,
                        rowTypeOid(schemaEntry.getKey(), t.getName()), 0,  // reltype, reloftype
                        ownerOid,
                        2,               // relam (heap=2)
                        tblOid,          // relfilenode
                        0,               // reltablespace
                        0, database.getAnalyzedTables().contains(schemaEntry.getKey() + "." + t.getName()) ? (double) t.getRows().size() : -1.0, 0, 0, 0, // relpages, reltuples (M22: -1 = never-analyzed), relallvisible, relallfrozen, reltoastrelid
                        hasIdx, false, relPersistence(schemaEntry.getKey(), t.isUnlogged()), relkind, // relhasindex, relisshared, relpersistence, relkind
                        (short) t.getAttributeCount(), checkCount, // relnatts, relchecks
                        // relhassubclass: a partitioned table or an inheritance parent has
                        // children, and a planner reads the flag to decide whether to look for
                        // any. PostgreSQL never clears it once set; reporting it from the links
                        // themselves is closer than reporting it never.
                        hasRules(schemaEntry.getKey(), t.getName()), hasTriggers,
                        !t.getPartitions().isEmpty() || !t.getChildren().isEmpty(),
                        t.isRlsEnabled(), t.isRlsForced(), // relhasrules..relforcerowsecurity

                        true, String.valueOf(t.getReplicaIdentity()), relispartition, // relispopulated, relreplident, relispartition
                        0, 0, 0,            // relrewrite, relfrozenxid, relminmxid
                        buildRelacl(AstExecutor.privilegeKey(schemaEntry.getKey(), t.getName())),
                        buildTableReloptions(t), relpartbound, 1 // relacl, reloptions, relpartbound, xmin
                });
            }
        }

        // Views
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int viewOwnerOid = resolveOwnerOid(database, oids, "view:" + vSchema + "." + vd.name());
            int vOid = oids.oid("rel:" + vSchema + "." + vd.name());
            // Build reloptions array string from view options
            String viewRelOptions = null;
            if (vd.reloptions() != null && !vd.reloptions().isEmpty()) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (Map.Entry<String, String> opt : vd.reloptions().entrySet()) {
                    if (!first) sb.append(",");
                    sb.append(opt.getKey()).append("=").append(opt.getValue());
                    first = false;
                }
                sb.append("}");
                viewRelOptions = sb.toString();
            }
            table.insertRow(new Object[]{
                    vOid, vd.name(), oids.oid("ns:" + vSchema),
                    rowTypeOid(vSchema, vd.name()), 0, viewOwnerOid, 0, vOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, relPersistence(vSchema, false), vd.materialized() ? "m" : "v",
                    (short) (vd.cachedColumns() != null ? vd.cachedColumns().size() : 0), (short) 0,
                    true, false, false, false, false,

                    !vd.materialized() || vd.populated(), "n", false,
                    0, 0, 0,
                    null, viewRelOptions, null, 1
            });
        }

        // Sequences - explicit sequences (each lives in the schema it was created in)
        for (Sequence explSeq : database.getSequences().values()) {
            String seqName = explSeq.getName();
            String explSeqSchema = explSeq.getSchemaName();
            int seqOwnerOid = resolveOwnerOid(database, oids, "sequence:" + seqName);
            int sOid = oids.oid("rel:" + explSeqSchema + "." + seqName);
            table.insertRow(new Object[]{
                    sOid, seqName, oids.oid("ns:" + explSeqSchema),
                    0, 0, seqOwnerOid, 0, sOid, 0,
                    1, 1.0, 0, 0, 0,
                    false, false, relPersistence(explSeqSchema, explSeq.isUnlogged()), "S",
                    (short) 3, (short) 0,   // sequences have 3 columns (last_value, log_cnt, is_called)
                    false, false, false, false, false,

                    true, "n", false, 0, 0, 0,
                    null, null, null, 1
            });
        }
        // Every serial and identity column's sequence is one of the sequences above, created with
        // the column. Composing <table>_<column>_seq for such columns as well emitted a row for a
        // relation nothing backs: after a table or column rename the composed name is not the
        // sequence's, and a partition that inherits an identity column has no sequence of its own.

        // Indexes (from explicit CREATE INDEX)
        Set<String> addedIndexNames = new HashSet<>();
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexKey = idx.getKey();
            String indexName = Database.idxName(indexKey);
            addedIndexNames.add(indexKey.toLowerCase());
            String storedTableQualified = database.getIndexTable(indexKey);
            String indexSchema = "public";
            if (storedTableQualified != null) {
                String[] parts = storedTableQualified.split("\\.", 2);
                if (parts.length == 2) {
                    indexSchema = parts[0];
                    String tableName = parts[1];
                    Schema schema = database.getSchema(indexSchema);
                    if (schema == null || schema.getTable(tableName) == null) continue;
                }
            } else {
                for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                    for (Map.Entry<String, Table> te : se.getValue().getTables().entrySet()) {
                        boolean allFound = true;
                        for (String colName : idx.getValue()) {
                            if (te.getValue().getColumnIndex(colName) < 0) { allFound = false; break; }
                        }
                        if (allFound) { indexSchema = se.getKey(); break; }
                    }
                }
            }
            int idxOid = oids.oid("rel:" + indexSchema + "." + indexName);
            short idxNatts = (short) idx.getValue().size();
            // Build reloptions array from index storage parameters
            Map<String, String> idxOpts = database.getIndexReloptions(indexKey);
            Object reloptionsVal = null;
            if (idxOpts != null && !idxOpts.isEmpty()) {
                List<String> optList = new ArrayList<>();
                for (Map.Entry<String, String> oe : idxOpts.entrySet()) {
                    optList.add(oe.getKey() + "=" + oe.getValue());
                }
                reloptionsVal = optList;
            }
            String idxMethod = database.getIndexMethod(indexKey);
            int relamOid = resolveAccessMethodOid(idxMethod);
            // Determine if this is a partitioned index (index on a partitioned table)
            String idxRelkind = "i";
            if (storedTableQualified != null) {
                String[] qParts = storedTableQualified.split("\\.", 2);
                if (qParts.length == 2) {
                    Schema idxSchema = database.getSchema(qParts[0]);
                    if (idxSchema != null) {
                        Table idxTable = idxSchema.getTable(qParts[1]);
                        if (idxTable != null && idxTable.getPartitionStrategy() != null) {
                            idxRelkind = "I";
                        }
                    }
                }
            }
            table.insertRow(new Object[]{
                    idxOid, indexName, oids.oid("ns:" + indexSchema),
                    0, 0, 10, relamOid, idxOid, 0,  // relam from access method
                    1, 0.0, 0, 0, 0,
                    false, false, "p", idxRelkind,
                    idxNatts, (short) 0,
                    false, false, false, false, false,

                    true, "n", false, 0, 0, 0,
                    null, reloptionsVal, null, 1
            });
        }

        // Indexes from PK/UNIQUE constraints (implicit indexes)
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                for (StoredConstraint sc : tableEntry.getValue().getConstraints()) {
                    if ((sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)
                            && !addedIndexNames.contains(
                                    Database.idxKey(schemaEntry.getKey(), sc.getName()).toLowerCase())) {
                        int ciOid = oids.oid("rel:" + schemaEntry.getKey() + "." + sc.getName());
                        short ciNatts = (short) (sc.getColumns() != null ? sc.getColumns().size() : 0);
                        String ciRelkind = tableEntry.getValue().getPartitionStrategy() != null ? "I" : "i";
                        table.insertRow(new Object[]{
                                ciOid, sc.getName(), oids.oid("ns:" + schemaEntry.getKey()),
                                0, 0, 10, 403, ciOid, 0,
                                1, 0.0, 0, 0, 0,
                                false, false, "p", ciRelkind,
                                ciNatts, (short) 0,
                                false, false, false, false, false,

                                true, "n", false, 0, 0, 0,
                                null, null, null, 1
                        });
                    }
                }
            }
        }

        // Composite types (relkind='c'). pg_type.typrelid and the composite's pg_attribute
        // rows already point at "rel:<schema>.<name>", so the same key links them up.
        for (Map.Entry<String, java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ctEntry
                : database.getCompositeTypes().entrySet()) {
            String ctKey = ctEntry.getKey();
            String ctName = TypeNamespace.nameOfKey(ctKey);
            int compositeNsOid = oids.oid("ns:" + TypeNamespace.schemaOfKey(ctKey));
            int ctOid = oids.oid("rel:" + TypeNamespace.schemaOfKey(ctKey) + "." + ctName);
            short ctNatts = (short) (ctEntry.getValue() != null ? ctEntry.getValue().size() : 0);
            table.insertRow(new Object[]{
                    ctOid, ctName, compositeNsOid,
                    oids.oid("type:" + ctKey), 0, resolveOwnerOid(database, oids, "type:" + ctKey), 0, 0, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "c",
                    ctNatts, (short) 0,
                    false, false, false, false, false,

                    true, "n", false, 0, 0, 0,
                    null, null, null, 1
            });
        }

        // Foreign tables (relkind='f'), each in the schema it was created in. One whose schema
        // has been dropped is not listed: DROP SCHEMA ... CASCADE takes its foreign tables with
        // it, and ForeignTables.live leaves out the ones that lost their schema.
        for (Object[] entry : ForeignTables.live(database)) {
            String ftSchema = (String) entry[0];
            Database.FdwForeignTable ft = (Database.FdwForeignTable) entry[1];
            // The oid key stays the bare-name one: pg_foreign_table pairs its ftrelid on the
            // same key, and a foreign table's identity has to be one number in both catalogs.
            int ftOid = oids.oid("rel:public." + ft.tableName);
            short ftNatts = (short) (ft.columns != null ? ft.columns.size() : 0);
            table.insertRow(new Object[]{
                    ftOid, ft.tableName, oids.oid("ns:" + ftSchema),
                    0, 0, 10, 0, ftOid, 0,
                    0, 0.0, 0, 0, 0,
                    false, false, "p", "f",
                    ftNatts, (short) 0,
                    false, false, false, false, false,
                    false, true, "d", false,
                    0, 0, 0,
                    null, null, null, 1
            });
        }

        return table;
    }

    /** Format a partition bound expression for relpartbound (PG-compatible syntax). */
    private static String formatPartitionBound(Table t) {
        if (t.isDefaultPartition()) return "DEFAULT";
        if (t.getPartitionLower() != null && t.getPartitionUpper() != null) {
            return "FOR VALUES FROM (" + formatBoundValue(t.getPartitionLower())
                    + ") TO (" + formatBoundValue(t.getPartitionUpper()) + ")";
        }
        if (t.getPartitionValues() != null) {
            StringBuilder sb = new StringBuilder("FOR VALUES IN (");
            for (int i = 0; i < t.getPartitionValues().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(formatBoundValue(t.getPartitionValues().get(i)));
            }
            sb.append(")");
            return sb.toString();
        }
        if (t.getPartitionModulus() != null && t.getPartitionRemainder() != null) {
            return "FOR VALUES WITH (modulus " + t.getPartitionModulus()
                    + ", remainder " + t.getPartitionRemainder() + ")";
        }
        return null;
    }

    private static String formatBoundValue(Object val) {
        if (val == null) return "NULL";
        if (val instanceof PartitionBound) return val.toString();
        if (val instanceof java.util.List) {
            java.util.List<?> vals = (java.util.List<?>) val;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vals.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(formatBoundValue(vals.get(i)));
            }
            return sb.toString();
        }
        // An enum bound is stored as the typed value so it orders by declaration position; the
        // deparse still has to read as the label PostgreSQL prints.
        if (val instanceof AstExecutor.PgEnum) return "'" + val + "'";
        if (val instanceof String) {
            String s = (String) val;
            if (s.equalsIgnoreCase("MINVALUE") || s.equalsIgnoreCase("MAXVALUE")) return s;
            return "'" + s + "'";
        }
        if (val instanceof java.time.LocalDate || val instanceof java.time.LocalDateTime
                || val instanceof java.time.LocalTime || val instanceof java.time.OffsetDateTime) {
            return "'" + val + "'";
        }
        return String.valueOf(val);
    }

    Table buildPgAttribute() {
        List<Column> cols = Cols.listOf(
                colNN("attrelid", DataType.OID),
                colNN("attname", DataType.NAME),
                colNN("atttypid", DataType.OID),
                colNN("attnum", DataType.SMALLINT),
                colNN("attnotnull", DataType.BOOLEAN),
                col("atttypmod", DataType.INTEGER),
                col("attlen", DataType.SMALLINT),
                colNN("attisdropped", DataType.BOOLEAN),
                colNN("atthasdef", DataType.BOOLEAN),
                col("attidentity", DataType.INTERNAL_CHAR),
                col("attgenerated", DataType.INTERNAL_CHAR),
                col("attcollation", DataType.OID),
                col("xmin", DataType.INTEGER),
                col("attislocal", DataType.BOOLEAN),
                col("attinhcount", DataType.SMALLINT),
                col("attfdwoptions", DataType.TEXT_ARRAY),
                col("attndims", DataType.SMALLINT),
                col("attacl", DataType.ACLITEM_ARRAY),
                col("attoptions", DataType.TEXT_ARRAY),
                col("attstattarget", DataType.SMALLINT),
                col("attstorage", DataType.INTERNAL_CHAR),
                col("attcompression", DataType.INTERNAL_CHAR),
                col("atthasmissing", DataType.BOOLEAN),
                col("attmissingval", DataType.ANYARRAY),
                col("attbyval", DataType.BOOLEAN),
                col("attalign", DataType.INTERNAL_CHAR)
        );
        Table table = new Table("pg_attribute", cols);
        // While the catalog shapes are being collected this table is only being asked what
        // columns it has, and filling it in would recurse back into the collection.
        if (PgCatalogBuilder.collectingShapes()) return table;

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                addUserRelationAttributes(table, relOid, t.getColumns(), false, t);
            }
        }

        // View columns (regular and materialized): resolved output columns stored on the ViewDef
        for (Database.ViewDef vd : database.getViews().values()) {
            if (vd.cachedColumns() == null || vd.cachedColumns().isEmpty()) continue;
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int vRelOid = oids.oid("rel:" + vSchema + "." + vd.name());
            // A view column is a query result, not storage: PG reports it as nullable even
            // when the base column it comes from is NOT NULL, and Hibernate's schema validator
            // reads that flag.
            addUserRelationAttributes(table, vRelOid, vd.cachedColumns(), true);
        }
        addPgAttributeExtras(table);
        return table;
    }

    /** Insert one pg_attribute row per column for a user relation (table or view). */
    private void addUserRelationAttributes(Table table, int relOid, List<Column> columns) {
        addUserRelationAttributes(table, relOid, columns, false, null);
    }

    private void addUserRelationAttributes(Table table, int relOid, List<Column> columns,
                                           boolean forceNullable) {
        addUserRelationAttributes(table, relOid, columns, forceNullable, null);
    }

    /**
     * @param forceNullable true for a view, whose columns carry no storage constraint of
     *                      their own regardless of what the underlying column declares
     * @param owner the relation these columns belong to when it is a table; the relations it
     *              inherits from are what attinhcount and attislocal are read from. Null for a
     *              view, which inherits from nothing.
     */
    private void addUserRelationAttributes(Table table, int relOid, List<Column> columns,
                                           boolean forceNullable, Table owner) {
        // The parents are the same list for every column of the relation, so the walk over the
        // inheritance links is done once rather than once per column.
        List<Table> directParents = owner == null ? null : owner.getDirectParents();
        // PostgreSQL updates a dropped column's row in place rather than deleting it, so the row
        // stays where it was among the relation's attributes and the columns after it keep their
        // numbers. Writing the rows in that order leaves a reader that does not sort them looking
        // at what PostgreSQL would have shown.
        List<Table.DroppedAttribute> dropped = owner == null
                ? java.util.Collections.<Table.DroppedAttribute>emptyList()
                : owner.getDroppedAttributes();
        int nextDropped = 0;
        for (int i = 0; i < columns.size(); i++) {
                    Column c = columns.get(i);
                    int attnum = owner == null ? i + 1 : owner.attnumAt(i);
                    while (nextDropped < dropped.size()
                            && dropped.get(nextDropped).getAttnum() < attnum) {
                        addDroppedRelationAttribute(table, relOid, dropped.get(nextDropped++));
                    }
                    // Determine identity type
                    // SERIAL/BIGSERIAL/SMALLSERIAL are NOT identity columns (attidentity stays empty)
                    // Only actual GENERATED AS IDENTITY columns get 'd' or 'a'
                    String identity = "";
                    if (c.getDefaultValue() != null) {
                        if (c.getDefaultValue().contains("__identity__:always")) {
                            identity = "a";
                        } else if (c.getDefaultValue().contains("__identity__")) {
                            identity = "d";
                        }
                    }
                    DataType colType = c.getType();
                    // M14: identity columns have atthasdef=f (no pg_attrdef row);
                    // their sequence is exposed via pg_depend, not a column default.
                    boolean isIdentityCol = !identity.isEmpty();
                    boolean hasDefault = c.isGenerated()
                            || (!isIdentityCol && (c.getDefaultValue() != null
                                || colType == DataType.SERIAL || colType == DataType.BIGSERIAL || colType == DataType.SMALLSERIAL));
                    // attlen, attbyval, attalign and attstorage are the type's, not the column's:
                    // a planner reading them decides how to lay the row out, and reporting -1 for
                    // a fixed-width type says the value is a varlena when it is not. They are read
                    // from the same record pg_type answers from, so the two cannot disagree.
                    // An array is a varlena whatever it holds: every array type in PG is -1 wide,
                    // stored extended and passed by reference. An array of a type the reader
                    // defined is recorded here as the element type with an element type beside it,
                    // so reading the layout off the type alone described a column of enums as the
                    // four-byte value passed by value that one enum is.
                    boolean holdsArray = DataType.isArrayType(colType)
                            || c.getArrayElementType() != null;
                    short attlen = holdsArray ? (short) -1 : typeLength(colType);
                    String storage = holdsArray ? "x" : typeStorage(colType);
                    // A composite value starts on a double boundary whatever it is made of, which
                    // is what pg_type records for every composite. The column carries the value as
                    // text, and text's alignment is the carrier's rather than the type's.
                    String align = c.getCompositeTypeName() != null ? "d" : typeAlign(colType);
                    // The declared width, precision or interval qualifier, packed the way
                    // format_type and every client that sizes a column read it back.
                    int typmod = CatalogHelper.attTypmod(c);
                    // Resolve atttypid: use custom type OID for enums/domains
                    int atttypid = c.getType().getOid();
                    // The column records the type it was declared with, schema and all, so a
                    // column of a.e points at a.e's OID and not at some other schema's e.
                    if (colType == DataType.ENUM && c.getEnumTypeName() != null) {
                        atttypid = userTypeColumnOid(c, c.getEnumTypeName(), atttypid);
                    } else if (c.getDomainTypeName() != null) {
                        atttypid = domainColumnOid(c, atttypid);
                    } else if (c.getRangeTypeName() != null) {
                        // A range is a type in its own right, and a column declared as one is a
                        // column of it. Carrying the value as the text it prints as left the
                        // column pointing at text's OID, so format_type named text where
                        // PostgreSQL names the range.
                        atttypid = userTypeColumnOid(c, c.getRangeTypeName(), atttypid);
                    } else if (c.getCompositeTypeName() != null) {
                        // A composite is a type the same way an enum or a domain is, and a column
                        // declared as one is a column of it. Carrying the value as text left the
                        // column pointing at text's OID, so format_type named text where
                        // PostgreSQL names the composite and a client had no way to learn what
                        // shape the value has.
                        atttypid = userTypeColumnOid(c, c.getCompositeTypeName(), atttypid);
                    }
                    // Use column-level overrides if set
                    String effectiveStorage = c.getAttStorageOverride() != null ? c.getAttStorageOverride() : storage;
                    // A column a parent also declares was inherited, and PostgreSQL counts how
                    // many parents declare it; a column no parent has is local, which is every
                    // column of an ordinary table.
                    short inhCount = 0;
                    if (directParents != null) {
                        for (Table parentRel : directParents) {
                            if (parentRel.getColumnIndex(c.getName()) >= 0) inhCount++;
                        }
                    }
                    // Whether the relation declared the column for itself is a separate question:
                    // a child that lists an inherited column in its own definition is told the
                    // column is local and inherited once over, while one that says nothing about
                    // it is told only that it inherited it. A partition declares nothing of its
                    // own, and a column nobody hands down any longer is the relation's own
                    // whatever it once was.
                    boolean attIsLocal = inhCount == 0
                            || (owner != null && owner.getPartitionParent() == null
                                && owner.isColumnLocal(c.getName()));
                    table.insertRow(new Object[]{
                            relOid,
                            c.getName(),
                            atttypid,
                            (short) attnum,
                            !forceNullable && !c.isNullable(),
                            typmod,
                            attlen,
                            false,
                            hasDefault,
                            identity,  // attidentity
                            c.isVirtual() ? "v" : c.isGenerated() ? "s" : "",  // attgenerated
                            // A column that sorts by a collation points at it; zero means none,
                            // which is what every column said however it was declared.
                            c.getCollation() == null ? 0
                                    : oids.oid("collation:" + c.getCollation().toLowerCase()),
                            // attndims: PG records 1 for a column declared as an array, and a
                            // client deciding whether to read the value as an array reads it.
                            1, attIsLocal, inhCount, null,
                            DataType.isArrayType(colType) || c.getArrayElementType() != null ? 1 : 0,
                            null,  // xmin, attislocal, attinhcount, attfdwoptions, attndims, attacl
                            null,      // attoptions
                            c.getAttStattarget(), // attstattarget
                            effectiveStorage,   // attstorage
                            c.getAttCompression(),        // attcompression
                            c.isAttHasMissing(), // atthasmissing
                            null,      // attmissingval
                            !holdsArray && byValue(colType), // attbyval
                            align  // attalign
                    });
        }
        while (nextDropped < dropped.size()) {
            addDroppedRelationAttribute(table, relOid, dropped.get(nextDropped++));
        }
    }

    /**
     * The pg_attribute row a DROP COLUMN leaves behind.
     *
     * <p>PostgreSQL does not delete the row. It marks the attribute dropped, clears its type link
     * -- the type may itself be gone by then -- and renames it to something no statement could
     * have written, so that the attributes after it keep the numbers a constraint, an index, a
     * default or a trigger is already holding. What the declared type settled about the row's
     * layout stays on the row, because the rows already stored are still laid out that way.
     */
    private void addDroppedRelationAttribute(Table table, int relOid, Table.DroppedAttribute gone) {
        Column c = gone.getColumn();
        DataType colType = c.getType();
        table.insertRow(new Object[]{
                relOid,
                Database.droppedAttributeName(gone.getAttnum()),
                0,                       // atttypid: the type link is cleared
                (short) gone.getAttnum(),
                false,                   // attnotnull: nothing is required of a column that is gone
                CatalogHelper.attTypmod(c),
                typeLength(colType),
                true,                    // attisdropped
                false,                   // atthasdef: the default went with the column
                "", "",                  // attidentity, attgenerated
                c.getCollation() == null ? 0
                        : oids.oid("collation:" + c.getCollation().toLowerCase()),
                1, gone.isLocal(), gone.getInheritCount(), null,
                DataType.isArrayType(colType) || c.getArrayElementType() != null ? 1 : 0,
                null,      // xmin, attislocal, attinhcount, attfdwoptions, attndims, attacl
                null,      // attoptions
                c.getAttStattarget(), // attstattarget
                c.getAttStorageOverride() != null
                        ? c.getAttStorageOverride() : typeStorage(colType), // attstorage
                c.getAttCompression(), // attcompression
                false,     // atthasmissing
                null,      // attmissingval
                byValue(colType), // attbyval
                c.getCompositeTypeName() != null ? "d" : typeAlign(colType) // attalign
        });
    }

    /**
     * Every sequence relation has the same three fixed columns in PG, and pg_class already
     * advertises relnatts=3 for them, so pg_attribute has to back that up.
     */
    private void addSequenceAttributes(Table table) {
        String[] names = {"last_value", "log_cnt", "is_called"};
        DataType[] types = {DataType.BIGINT, DataType.BIGINT, DataType.BOOLEAN};
        for (String qualified : CatalogHelper.getSequenceNames(database)) {
            String seqName = CatalogHelper.nameOf(qualified);
            int seqOid = oids.oid("rel:" + CatalogHelper.schemaOf(qualified) + "." + seqName);
            for (int i = 0; i < names.length; i++) {
                DataType dt = types[i];
                table.insertRow(new Object[]{
                        seqOid, names[i], dt.getOid(), (short) (i + 1),
                        true, -1, typeLength(dt), false, false,
                        "", "", 0, 1, true, 0, null, 0, null,
                        null, (short) -1, typeStorage(dt), "", false, null,
                        byValue(dt), typeAlign(dt)
                });
            }
        }
    }

    /**
     * The schema a user-defined type lives in, read from the schema object registry the same way
     * a sequence's is. A type registered nowhere counts as public, which is where the paths that
     * do not register put it.
     */
    private String typeSchema(String kind, String typeName) {
        for (Map.Entry<String, Schema> entry : database.getSchemas().entrySet()) {
            if (database.getSchemaObjects(entry.getKey())
                    .contains(kind + ":" + typeName.toLowerCase())) {
                return entry.getKey();
            }
        }
        return "public";
    }

    /** True when a composite type of this name lives in this very schema. */
    private boolean compositeLivesIn(String schemaName, String name) {
        return database.getCompositeTypes().containsKey(name.toLowerCase())
                && typeSchema("composite", name)
                        .equalsIgnoreCase(schemaName == null ? "public" : schemaName);
    }

    /** The pg_class key of a composite type's row-type relation, in the schema it lives in. */
    private String compositeRelKey(String ctName) {
        return "rel:" + typeSchema("composite", ctName) + "." + ctName;
    }

    private String sequenceSchema(String seqName) {
        for (Map.Entry<String, Schema> entry : database.getSchemas().entrySet()) {
            if (database.getSchemaObjects(entry.getKey()).contains("sequence:" + seqName.toLowerCase())) {
                return entry.getKey();
            }
        }
        return "public";
    }

    /**
     * Index relations have one pg_attribute row per key column, which is what pgjdbc's
     * getIndexInfo and psql's \d read. PG names an expression column after the top-level
     * function (lower), after the underlying column for a cast, and "expr" otherwise.
     */
    private void addIndexAttributes(Table table) {
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Table t : schemaEntry.getValue().getTables().values()) {
                java.util.Set<String> done = new java.util.HashSet<>();
                for (Map.Entry<String, java.util.List<String>> idx : database.getIndexColumns().entrySet()) {
                    String qualified = database.getIndexTable(idx.getKey());
                    if (qualified == null || !qualified.equalsIgnoreCase(schemaName + "." + t.getName())) continue;
                    String bare = Database.idxName(idx.getKey());
                    addIndexAttributeRows(table, schemaName, bare, t, idx.getValue());
                    done.add(bare.toLowerCase());
                }
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY
                            && sc.getType() != StoredConstraint.Type.UNIQUE) continue;
                    if (done.contains(sc.getName().toLowerCase())) continue;
                    addIndexAttributeRows(table, schemaName, sc.getName(), t, sc.getColumns());
                }
            }
        }
    }

    private void addIndexAttributeRows(Table table, String schemaName, String indexName,
                                       Table t, java.util.List<String> cols) {
        if (cols == null) return;
        int idxOid = oids.oid("rel:" + schemaName + "." + indexName);
        for (int i = 0; i < cols.size(); i++) {
            String col = cols.get(i);
            String attname;
            DataType type;
            int colIdx = t.getColumnIndex(col);
            if (colIdx >= 0) {
                attname = t.getColumns().get(colIdx).getName();
                type = t.getColumns().get(colIdx).getType();
            } else {
                attname = indexExprName(col);
                type = indexExprType(col, t);
            }
            table.insertRow(new Object[]{
                    idxOid, attname, type.getOid(), (short) (i + 1),
                    false, -1, typeLength(type), false, false,
                    "", "", 0, 1, true, 0, null, 0, null,
                    null, (short) -1, typeStorage(type), "", false, null,
                    byValue(type), typeAlign(type)
            });
        }
    }

    private static String indexExprName(String exprText) {
        try {
            com.memgres.engine.parser.ast.Expression e =
                    com.memgres.engine.parser.Parser.parseExpression(exprText);
            if (e instanceof com.memgres.engine.parser.ast.ColumnRef) {
                return ((com.memgres.engine.parser.ast.ColumnRef) e).column().toLowerCase();
            }
            if (e instanceof com.memgres.engine.parser.ast.CastExpr) {
                return indexExprName(SqlUnparser.exprToSql(
                        ((com.memgres.engine.parser.ast.CastExpr) e).expr()));
            }
            if (e instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
                return ((com.memgres.engine.parser.ast.FunctionCallExpr) e).name().toLowerCase();
            }
        } catch (Exception ignored) {
            // Unparseable expression: PG's generic name applies.
        }
        return "expr";
    }

    private DataType indexExprType(String exprText, Table t) {
        try {
            com.memgres.engine.parser.ast.Expression e =
                    com.memgres.engine.parser.Parser.parseExpression(exprText);
            return indexExprType(e, t);
        } catch (Exception ignored) {
            return DataType.TEXT;
        }
    }

    private DataType indexExprType(com.memgres.engine.parser.ast.Expression e, Table t) {
        if (e instanceof com.memgres.engine.parser.ast.ColumnRef) {
            int i = t.getColumnIndex(((com.memgres.engine.parser.ast.ColumnRef) e).column());
            return i >= 0 ? t.getColumns().get(i).getType() : DataType.TEXT;
        }
        if (e instanceof com.memgres.engine.parser.ast.CastExpr) {
            DataType dt = DataType.fromPgName(((com.memgres.engine.parser.ast.CastExpr) e).typeName());
            return dt != null ? dt : DataType.TEXT;
        }
        if (e instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
            com.memgres.engine.parser.ast.FunctionCallExpr fn =
                    (com.memgres.engine.parser.ast.FunctionCallExpr) e;
            String n = fn.name().toLowerCase();
            if (INDEX_TEXT_FUNCS.contains(n)) return DataType.TEXT;
            if (INDEX_INT_FUNCS.contains(n)) return DataType.INTEGER;
            if (!fn.args().isEmpty()) return indexExprType(fn.args().get(0), t);
            return DataType.TEXT;
        }
        if (e instanceof com.memgres.engine.parser.ast.BinaryExpr) {
            return indexExprType(((com.memgres.engine.parser.ast.BinaryExpr) e).left(), t);
        }
        return DataType.TEXT;
    }

    private static final java.util.Set<String> INDEX_TEXT_FUNCS = new java.util.HashSet<>(java.util.Arrays.asList(
            "lower", "upper", "initcap", "btrim", "ltrim", "rtrim", "md5", "replace",
            "substr", "substring", "concat", "concat_ws", "reverse", "translate"));

    private static final java.util.Set<String> INDEX_INT_FUNCS = new java.util.HashSet<>(java.util.Arrays.asList(
            "length", "char_length", "character_length", "octet_length", "strpos", "ascii"));

    /** pg_attribute rows for foreign tables, composite types, and system catalogs. */
    private void addPgAttributeExtras(Table table) {
        addSequenceAttributes(table);
        addIndexAttributes(table);
        // Foreign table columns. A foreign table whose schema was dropped is gone from pg_class,
        // so its columns go with it -- an attribute row pointing at no relation is worse than none.
        for (Object[] entry : ForeignTables.live(database)) {
            Database.FdwForeignTable ft = (Database.FdwForeignTable) entry[1];
            int ftRelOid = oids.oid("rel:public." + ft.tableName);
            if (ft.columns != null) {
                for (int i = 0; i < ft.columns.size(); i++) {
                    String[] colParts = ft.columns.get(i);
                    String colName = colParts[0];
                    String colTypeName = colParts.length > 1 ? colParts[1] : "text";
                    int typOid = resolveTypeOidByName(colTypeName);
                    table.insertRow(new Object[]{
                            ftRelOid, colName, typOid, (short) (i + 1),
                            false, -1, (short) -1, false, false,
                            "", "", 0, 1, true, 0, null, 0, null,
                            null, (short) -1, "p", "", false, null, false, "i"
                    });
                }
            }
        }

        // (sequence and index attributes are added by addSequenceAttributes/addIndexAttributes)

        // Composite type attributes
        for (Map.Entry<String, java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ctEntry
                : database.getCompositeTypes().entrySet()) {
            String ctKey = ctEntry.getKey();
            int ctRelOid = oids.oid("rel:" + TypeNamespace.schemaOfKey(ctKey) + "."
                    + TypeNamespace.nameOfKey(ctKey));
            java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = ctEntry.getValue();
            for (int i = 0; i < fields.size(); i++) {
                com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField f = fields.get(i);
                // An attribute a drop took away keeps its row and its number: PostgreSQL marks it
                // dropped and clears atttypid, so the attributes after it do not shift and a client
                // reading the shape of the type sees the gap rather than a type that renumbered
                // itself. The modifier it was declared with stays on the row, as PostgreSQL leaves
                // it there.
                boolean dropped = Database.isDroppedAttribute(f);
                int atttypid = dropped ? 0 : resolveTypeOidByName(f.typeName());
                // How wide a value of the attribute is, whether it is passed by value, what
                // boundary it starts on and how it is stored are the declared type's answers and
                // not the attribute's: a planner reading them lays the row out from them, and
                // saying -1 and "p" for every attribute described a type made only of varlenas.
                // A dropped attribute keeps them -- PostgreSQL clears its atttypid and leaves the
                // rest of the row as the type it was declared with wrote it.
                String[] layout = attributeLayout(f.typeName());
                table.insertRow(new Object[]{
                        ctRelOid, f.name(), atttypid, (short) (i + 1),
                        false, writtenTypmod(f.typeName()),
                        layout == null ? (short) -1 : Short.parseShort(layout[0]),
                        dropped, false,
                        "", "", 0, 1, true, 0, null, 0, null,
                        null, (short) -1, layout == null ? "p" : layout[3], "", false, null,
                        layout != null && "t".equals(layout[1]),
                        layout == null ? "i" : layout[2]
                });
            }
        }

        addCatalogRelationAttributes(table);
    }

    /**
     * One pg_attribute row per column of every relation memgres publishes in pg_catalog.
     *
     * <p>The columns come from the builders themselves rather than from a second list written out
     * by hand, so what the catalog says a catalog relation looks like is what a SELECT from it
     * actually returns. Without these rows nothing can introspect the catalog: psql's
     * {@code \d pg_class}, a schema browser expanding the system catalogs and any code that
     * derives a column list from pg_attribute all come back empty.
     */
    private void addCatalogRelationAttributes(Table table) {
        Map<String, List<Column>> shapes = new PgCatalogBuilder(database, oids).catalogShapes();
        for (Map.Entry<String, List<Column>> shape : shapes.entrySet()) {
            int relOid = oids.oid("rel:pg_catalog." + shape.getKey());
            int attnum = 0;
            for (Column c : shape.getValue()) {
                if (isSystemColumn(c)) continue;
                attnum++;
                DataType dt = catalogColumnType(c.getType());
                table.insertRow(new Object[]{
                        relOid, c.getName(), c.getType().getOid(), (short) attnum,
                        !c.isNullable(), -1, typeLength(dt), false, false,
                        "", "", 0, 1, true, 0, null, 0, null,
                        null, (short) -1, typeStorage(dt), "", false, null,
                        byValue(dt), typeAlign(dt)
                });
            }
        }
    }

    /**
     * PostgreSQL keeps its row-header columns out of a positive attnum, and a relation that
     * reported them as ordinary columns would answer {@code relnatts} and
     * {@code information_schema.columns} with more columns than it has.
     */
    private static final Set<String> SYSTEM_COLUMNS = new HashSet<>(Arrays.asList(
            "xmin", "xmax", "cmin", "cmax", "ctid", "tableoid"));

    /**
     * The row-header column memgres carries alongside every catalog table. The name alone is not
     * enough to tell: pg_replication_slots has a real xmin column, of type xid, which is an
     * ordinary column of that view and does belong in pg_attribute.
     */
    static boolean isSystemColumn(Column c) {
        return SYSTEM_COLUMNS.contains(c.getName().toLowerCase())
                && c.getType() == DataType.INTEGER;
    }

    /** How many of a catalog relation's columns count towards pg_class.relnatts. */
    private static int userColumnCount(List<Column> cols) {
        int n = 0;
        for (Column c : cols) {
            if (!isSystemColumn(c)) n++;
        }
        return n;
    }

    /**
     * The type a catalog column is physically laid out as. A flag column memgres declares CHAR is
     * PostgreSQL's single-byte {@code "char"}, not the bpchar that shares the spelling, and the
     * two are laid out nothing alike: one byte passed by value against a varlena.
     */
    private static DataType catalogColumnType(DataType dt) {
        return dt == DataType.CHAR ? DataType.INTERNAL_CHAR : dt;
    }

    Table buildPgType() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("typname", DataType.NAME),
                colNN("typnamespace", DataType.OID),
                col("typowner", DataType.OID),
                col("typlen", DataType.SMALLINT),
                col("typbyval", DataType.BOOLEAN),
                col("typtype", DataType.INTERNAL_CHAR),
                col("typcategory", DataType.INTERNAL_CHAR),
                col("typispreferred", DataType.BOOLEAN),
                col("typisdefined", DataType.BOOLEAN),
                col("typdelim", DataType.INTERNAL_CHAR),
                col("typrelid", DataType.OID),
                col("typsubscript", DataType.REGPROC),
                col("typelem", DataType.OID),
                col("typarray", DataType.OID),
                col("typinput", DataType.REGPROC),
                col("typoutput", DataType.REGPROC),
                col("typreceive", DataType.REGPROC),
                col("typsend", DataType.REGPROC),
                col("typmodin", DataType.REGPROC),
                col("typmodout", DataType.REGPROC),
                col("typanalyze", DataType.REGPROC),
                col("typalign", DataType.INTERNAL_CHAR),
                col("typstorage", DataType.INTERNAL_CHAR),
                col("typnotnull", DataType.BOOLEAN),
                col("typbasetype", DataType.OID),
                col("typtypmod", DataType.INTEGER),
                col("typndims", DataType.INTEGER),
                col("typcollation", DataType.OID),
                col("typdefaultbin", DataType.PG_NODE_TREE),
                col("typdefault", DataType.TEXT),
                col("typacl", DataType.ACLITEM_ARRAY),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_type", cols);
        int pgCatalogOid = oids.oid("ns:pg_catalog");

        for (DataType dt : DataType.values()) {
            // Array types are emitted separately (correct typcategory='A'/typelem) below; emitting
            // them here as "base types" would leave a duplicate row with typcategory='U'/typelem=0
            // that pgjdbc may resolve first, mis-classifying the array column as Types.OTHER.
            if (dt == DataType.ENUM || dt == DataType.SERIAL || dt == DataType.BIGSERIAL
                    || dt == DataType.SMALLSERIAL
                    || dt.getPgName().startsWith("_")
                    || dt == DataType.RECORD || dt == DataType.VOID
                    // Registered from PgInternalTypes instead, with PG's own metadata.
                    || dt == DataType.PG_NODE_TREE || dt == DataType.ANYARRAY
                    || dt == DataType.INTERNAL_CHAR
                    || dt == DataType.PG_LSN || dt == DataType.PG_NDISTINCT
                    || dt == DataType.PG_DEPENDENCIES || dt == DataType.PG_MCV_LIST) continue;
            String pgName = dt.getPgName();
            // One reading of the type's physical attributes, shared with the pg_attribute rows,
            // so what pg_type says a date is and what pg_attribute says a date column is agree.
            String[] attrs = typeAttrs(pgName);
            // A range and a multirange are their own kinds of type, not base types, and tools
            // that bucket types by typtype rely on the distinction.
            String typtype = attrs != null ? attrs[3]
                    : pgName.endsWith("multirange") ? "m" : pgName.endsWith("range") ? "r" : "b";
            String cat = attrs != null ? attrs[4] : pgName.endsWith("range") ? "R" : "U";
            short typlen = attrs != null ? Short.parseShort(attrs[1]) : (short) -1;
            boolean typbyval = attrs != null && "t".equals(attrs[2]);
            boolean isPreferred = attrs != null && "t".equals(attrs[5]);
            String typdelim = attrs != null ? attrs[6] : ",";
            String typalign = attrs != null ? attrs[7] : "i";
            String typstorage = attrs != null ? attrs[8] : "p";
            int collation = attrs != null ? Integer.parseInt(attrs[9]) : 0;
            // A driver that does not hardcode an array OID discovers array support by following
            // typarray from the element type (pgjdbc's TypeInfoCache does exactly this), so every
            // element whose array type is registered has to point back at it.
            int typarray = ARRAY_OF.containsKey(dt.getOid()) ? ARRAY_OF.get(dt.getOid()) : 0;
            // A vector holds elements, and PG records which: 0-based int2vector/oidvector carry
            // typelem the same way an array type does.
            int typelem = dt == DataType.INT2VECTOR ? DataType.SMALLINT.getOid()
                    : dt == DataType.OIDVECTOR ? DataType.OID.getOid()
                    : FIXED_ELEMENT_OF.containsKey(pgName) ? FIXED_ELEMENT_OF.get(pgName) : 0;
            Object subscript = (dt == DataType.INT2VECTOR || dt == DataType.OIDVECTOR)
                    ? regproc("array_subscript_handler") : regproc(null);
            String io = ioBaseName(pgName);
            table.insertRow(new Object[]{
                    dt.getOid(), pgName, pgCatalogOid,
                    10,          // typowner
                    typlen, typbyval, typtype, cat, isPreferred, true, typdelim,
                    0,           // typrelid
                    subscript,
                    typelem,
                    typarray,
                    regproc(io + "in"), regproc(io + "out"),       // typinput, typoutput
                    regproc(io + "recv"), regproc(io + "send"),    // typreceive, typsend
                    regproc(TYPMOD_IO.get(pgName) == null ? null : TYPMOD_IO.get(pgName) + "in"),
                    regproc(TYPMOD_IO.get(pgName) == null ? null : TYPMOD_IO.get(pgName) + "out"),
                    regproc(TYPANALYZE.get(pgName)),
                    typalign, typstorage,
                    false, 0, -1, 0, collation,          // typnotnull, typbasetype, typtypmod, typndims, typcollation
                    null, null, null, 1                   // typdefaultbin, typdefault, typacl, xmin
            });
        }

        // The bootstrap types the catalogs point at. A cast whose source is regprocedure or a
        // handler function whose return type is index_am_handler names a type here, and a join
        // from that column to pg_type drops the row entirely when it is missing.
        for (Object[] it : PgInternalTypes.TYPES) {
            String itName = (String) it[1];
            short itLen = (short) (int) (Integer) it[2];
            // The I/O prefix is recorded with the type, not built from its name: PostgreSQL
            // spells these charin and pg_lsn_in and brin_bloom_summary_in, and a name derived
            // from the type is a function nothing can call.
            String itIo = (String) it[10];
            boolean itBinary = Boolean.TRUE.equals(it[11]);
            table.insertRow(new Object[]{
                    it[0], itName, pgCatalogOid, 10,
                    itLen, it[3], it[4], it[5], false, true, ",",
                    0, regproc(null), it[8], it[9],
                    regproc(itIo + "in"), regproc(itIo + "out"),
                    itBinary ? regproc(itIo + "recv") : regproc(null),
                    itBinary ? regproc(itIo + "send") : regproc(null),
                    regproc(null), regproc(null), regproc(null), it[6], it[7],
                    false, 0, -1, 0, it[12], null, null, null, 1
            });
        }

        // ... and the array types those bootstrap types name through typarray, so following one
        // reaches a type rather than nothing.
        for (Object[] a : PgInternalTypes.ARRAYS) {
            table.insertRow(new Object[]{
                    a[0], a[1], pgCatalogOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), a[2], 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), a[3], "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // aclitem base type (OID 1033) — a 16-byte, double-aligned struct in PG 18.
        String[] aclAttrs = typeAttrs("aclitem");
        table.insertRow(new Object[]{
                1033, "aclitem", pgCatalogOid, 10,
                Short.parseShort(aclAttrs[1]), false, "b", aclAttrs[4], false, true, aclAttrs[6],
                0, regproc(null), 0, 1034,
                regproc("aclitemin"), regproc("aclitemout"), regproc(null), regproc(null),
                regproc(null), regproc(null), regproc(null), aclAttrs[7], aclAttrs[8],
                false, 0, -1, 0, 0, null, null, null, 1
        });

        // Standard array types (typcategory='A', typelem -> element OID). Without a pg_type row
        // pgjdbc can only classify an array column when it *hardcodes* the array OID; types it
        // doesn't hardcode (notably _jsonb) otherwise resolve to Types.OTHER and come back as a
        // single PGobject instead of a java.sql.Array. Rows here make the classification data-
        // driven so those columns decode as arrays. (Enum arrays are added per-enum below.)
        for (Object[] a : STD_ARRAYS) {
            int arrOid = (Integer) a[0];
            int elemOid = (Integer) a[1];
            String arrName = (String) a[2];
            // An array inherits its element's alignment, delimiter and collation, and PG records
            // all three per array type: _box is semicolon-delimited because a box is, and _name
            // sorts under C. Reading them off the record is what keeps them right.
            String[] arrAttrs = typeAttrs(arrName);
            String align = arrAttrs != null ? arrAttrs[7] : (String) a[3];
            String arrDelim = arrAttrs != null ? arrAttrs[6] : ",";
            int arrCollation = arrAttrs != null ? Integer.parseInt(arrAttrs[9])
                    : ((elemOid == 25 || elemOid == 1043 || elemOid == 1042 || elemOid == 19) ? 100 : 0);
            table.insertRow(new Object[]{
                    arrOid, arrName, pgCatalogOid, 10,
                    (short) -1, false, "b", "A", false, true, arrDelim,
                    0, regproc("array_subscript_handler"), elemOid, 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), align, "x",
                    false, 0, -1, 0, arrCollation, null, null, null, 1
            });
        }

        for (String[] pt : PSEUDO_TYPES) {
            String ptName = pt[0];
            int ptOid = Integer.parseInt(pt[1]);
            String[] ptAttrs = typeAttrs(ptName);
            boolean ptBinary = "t".equals(pt[3]);
            table.insertRow(new Object[]{
                    ptOid, ptName, pgCatalogOid, 10,
                    Short.parseShort(ptAttrs[1]), "t".equals(ptAttrs[2]), "p", "P", false, true, ",",
                    0, regproc(null), 0, Integer.parseInt(pt[2]),
                    regproc(ptName + "_in"), regproc(ptName + "_out"),
                    ptBinary ? regproc(ptName + "_recv") : regproc(null),
                    ptBinary ? regproc(ptName + "_send") : regproc(null),
                    regproc(null), regproc(null), regproc(null), ptAttrs[7], ptAttrs[8],
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // record[] is a real array type over the record pseudo-type, and a client asked to read
        // one — a recursive query's SEARCH or CYCLE column — looks its row up here to learn the
        // element type and delimiter before it will parse the value at all. An array over a
        // pseudo-type is itself a pseudo-type: PG records typtype 'p'.
        table.insertRow(new Object[]{
                2287, "_record", pgCatalogOid, 10,
                (short) -1, false, "p", "P", false, true, ",",
                0, regproc("array_subscript_handler"), 2249, 0,
                regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                regproc(null), regproc(null), regproc("array_typanalyze"), "d", "x",
                false, 0, -1, 0, 0, null, null, null, 1
        });

        // Add custom enum types. Each is in the schema it was created in, and two schemas each
        // holding an "e" are two rows with two OIDs rather than one row that answers for both.
        for (Map.Entry<String, CustomEnum> ceEntry : database.getCustomEnums().entrySet()) {
            CustomEnum ce = ceEntry.getValue();
            int enumNsOid = oids.oid("ns:" + TypeNamespace.schemaOfKey(ceEntry.getKey()));
            int enumOid = oids.oid("type:" + ceEntry.getKey());
            // Every PG enum type also gets an array-type pg_type row (typname "_<name>"); mint
            // its OID eagerly and link both rows (element.typarray -> array oid,
            // array.typelem -> element oid) so pgjdbc's TypeInfoCache queries for an
            // enum-ARRAY column (getArrayDelimiter, getPGArrayElement, ...) resolve instead of
            // finding zero rows. See PgWireValueFormatter.columnTypeOid, which advertises this
            // same "type:<name>[]" OID for "<name>[]"-typed columns.
            int enumArrayOid = oids.oid("type:" + ceEntry.getKey() + "[]");
            table.insertRow(new Object[]{
                    enumOid, ce.getName(), enumNsOid, 10,
                    (short) 4, true, "e", "E", false, true, ",",
                    0, regproc(null), 0, enumArrayOid,
                    regproc("enum_in"), regproc("enum_out"), regproc("enum_recv"), regproc("enum_send"),
                    regproc(null), regproc(null), regproc(null), "i", "p",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
            table.insertRow(new Object[]{
                    enumArrayOid, "_" + ce.getName(), enumNsOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), enumOid, 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), "i", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // Add composite types
        for (Map.Entry<String, java.util.List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField>> ctEntry
                : database.getCompositeTypes().entrySet()) {
            String ctKey = ctEntry.getKey();
            String ctName = TypeNamespace.nameOfKey(ctKey);
            String ctSchema = TypeNamespace.schemaOfKey(ctKey);
            int ctNsOid = oids.oid("ns:" + ctSchema);
            int ctRelOid = oids.oid("rel:" + ctSchema + "." + ctName);
            table.insertRow(new Object[]{
                    oids.oid("type:" + ctKey), ctName, ctNsOid, 10,
                    (short) -1, false, "c", "C", false, true, ",",
                    ctRelOid, regproc(null), 0, oids.oid("type:" + ctKey + "[]"),
                    regproc("record_in"), regproc("record_out"), regproc("record_recv"), regproc("record_send"),
                    regproc(null), regproc(null), regproc(null), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
            table.insertRow(new Object[]{
                    oids.oid("type:" + ctKey + "[]"), "_" + ctName, ctNsOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), oids.oid("type:" + ctKey), 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // Shell types: a name reserved but not yet defined, so typisdefined is false
        for (String shellKey : database.getShellTypes()) {
            table.insertRow(new Object[]{
                    oids.oid("type:" + shellKey), TypeNamespace.nameOfKey(shellKey),
                    oids.oid("ns:" + TypeNamespace.schemaOfKey(shellKey)), 10,
                    (short) 4, false, "p", "P", false, false, ",",
                    0, null, 0, 0,
                    "shell_in", "shell_out", "-", "-",
                    "-", "-", "-", "i", "p",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // Every table is also a row type, and PG registers that composite type alongside it.
        // DatabaseMetaData.getUDTs reads exactly these rows, so without them a tool asking what
        // user-defined types the database has is told there are none.
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            if ("pg_catalog".equalsIgnoreCase(schemaName)
                    || "information_schema".equalsIgnoreCase(schemaName)) continue;
            int relNsOid = oids.oid("ns:" + schemaName);
            for (Table rel : schemaEntry.getValue().getTables().values()) {
                String relName = rel.getName();
                if (database.getCompositeTypes().containsKey(
                        TypeNamespace.key(schemaName, relName))) continue;
                table.insertRow(rowType(schemaName, relName, relNsOid));
                table.insertRow(rowTypeArray(schemaName, relName, relNsOid));
            }
        }

        // A view has a row type too, and DatabaseMetaData.getUDTs reads it the same way.
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            if (database.getCompositeTypes().containsKey(
                    TypeNamespace.key(vSchema, vd.name()))) continue;
            table.insertRow(rowType(vSchema, vd.name(), oids.oid("ns:" + vSchema)));
            table.insertRow(rowTypeArray(vSchema, vd.name(), oids.oid("ns:" + vSchema)));
        }

        // ... and so does every catalog relation: in PostgreSQL pg_class.reltype names it, and a
        // join from pg_class to pg_type on reltype is how a tool asks what a relation's row
        // looks like as a value. Without these the newly-described catalog relations were the
        // one thing in the catalog whose reltype led nowhere.
        for (String sysRel : PgCatalogRelations.ALL) {
            table.insertRow(rowType("pg_catalog", sysRel, pgCatalogOid));
            table.insertRow(rowTypeArray("pg_catalog", sysRel, pgCatalogOid));
        }

        // Add domain types
        for (Map.Entry<String, DomainType> domEntry : database.getDomains().entrySet()) {
            DomainType dom = domEntry.getValue();
            int domNsOid = oids.oid("ns:" + TypeNamespace.schemaOfKey(domEntry.getKey()));
            // Resolve base type OID
            int baseTypeOid = 0;
            String baseTypeCat = "U";
            for (DataType dt : DataType.values()) {
                if (dt.getPgName().equalsIgnoreCase(dom.getBaseTypeName())
                        || dt.name().equalsIgnoreCase(dom.getBaseTypeName())) {
                    baseTypeOid = dt.getOid();
                    switch (dt) {
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case REAL:
                        case DOUBLE_PRECISION:
                        case NUMERIC:
                        case MONEY:
                            baseTypeCat = "N";
                            break;
                        case BOOLEAN:
                            baseTypeCat = "B";
                            break;
                        case VARCHAR:
                        case CHAR:
                        case TEXT:
                        case NAME:
                            baseTypeCat = "S";
                            break;
                        case DATE:
                        case TIMESTAMP:
                        case TIMESTAMPTZ:
                        case TIME:
                        case INTERVAL:
                            baseTypeCat = "D";
                            break;
                        default:
                            baseTypeCat = "U";
                            break;
                    }
                    break;
                }
            }
            // A domain over an array has the array type as its base, not the element: a tool
            // following typbasetype to size an input must not be told it is a scalar.
            if (dom.isArray() && dom.getBaseType() != null) {
                baseTypeOid = dom.getBaseType().getOid();
                baseTypeCat = "A";
            }
            // A domain is a type, and PostgreSQL gives every type an array type. The row was
            // written with typarray zero and no array row beside it, so following typarray from a
            // domain reached nothing where following it from an enum or a composite reached theirs.
            int domOid = oids.oid("type:" + domEntry.getKey());
            int domArrayOid = oids.oid("type:" + domEntry.getKey() + "[]");
            table.insertRow(new Object[]{
                    domOid, dom.getName(), domNsOid, 10,
                    (short) -1, false, "d", baseTypeCat, false, true, ",",
                    0, regproc(null), 0, domArrayOid,
                    regproc("domain_in"), regproc("domain_out"), regproc("domain_recv"), regproc("domain_send"),
                    regproc(null), regproc(null), regproc(null), "i", "x",
                    dom.isNotNull(), baseTypeOid, -1, 0, 0, null,
                    dom.getDefaultValue(), null, 1
            });
            table.insertRow(new Object[]{
                    domArrayOid, "_" + dom.getName(), domNsOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), domOid, 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), "i", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        // Add user-defined range types. PostgreSQL creates four types for every one of these: the
        // range, the multirange that holds ranges of it, and an array type over each. Writing only
        // the range left a reader following typarray at nothing, and a multirange type nobody
        // could name at all.
        for (Map.Entry<String, String> rangeEntry : database.getRangeTypes().entrySet()) {
            String rangeKey = rangeEntry.getKey();
            String rangeName = TypeNamespace.nameOfKey(rangeKey);
            String multiName = RangeOperations.multirangeTypeName(rangeName);
            String multiKey = TypeNamespace.key(TypeNamespace.schemaOfKey(rangeKey), multiName);
            int rangeNsOid = oids.oid("ns:" + TypeNamespace.schemaOfKey(rangeKey));
            int rangeOid = oids.oid("type:" + rangeKey);
            int rangeArrayOid = oids.oid("type:" + rangeKey + "[]");
            int multiOid = oids.oid("type:" + multiKey);
            int multiArrayOid = oids.oid("type:" + multiKey + "[]");
            table.insertRow(new Object[]{
                    rangeOid, rangeName, rangeNsOid, 10,
                    (short) -1, false, "r", "R", false, true, ",",
                    0, regproc(null), 0, rangeArrayOid,
                    regproc("range_in"), regproc("range_out"), regproc("range_recv"), regproc("range_send"),
                    regproc(null), regproc(null), regproc("range_typanalyze"), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
            table.insertRow(new Object[]{
                    rangeArrayOid, "_" + rangeName, rangeNsOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), rangeOid, 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
            table.insertRow(new Object[]{
                    multiOid, multiName, rangeNsOid, 10,
                    (short) -1, false, "m", "R", false, true, ",",
                    0, regproc(null), 0, multiArrayOid,
                    regproc("multirange_in"), regproc("multirange_out"),
                    regproc("multirange_recv"), regproc("multirange_send"),
                    regproc(null), regproc(null), regproc("multirange_typanalyze"), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
            table.insertRow(new Object[]{
                    multiArrayOid, "_" + multiName, rangeNsOid, 10,
                    (short) -1, false, "b", "A", false, true, ",",
                    0, regproc("array_subscript_handler"), multiOid, 0,
                    regproc("array_in"), regproc("array_out"), regproc("array_recv"), regproc("array_send"),
                    regproc(null), regproc(null), regproc("array_typanalyze"), "d", "x",
                    false, 0, -1, 0, 0, null, null, null, 1
            });
        }

        return table;
    }

    Table buildPgNamespace() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("nspname", DataType.NAME),
                colNN("nspowner", DataType.OID),
                col("xmin", DataType.INTEGER),
                col("nspacl", DataType.ACLITEM_ARRAY)
        );
        Table table = new Table("pg_namespace", cols);

        // Built-in namespaces; nspacl=NULL means "default ACL", which is what pg_dump expects
        table.insertRow(new Object[]{oids.oid("ns:pg_catalog"), "pg_catalog", 10, 1, null});
        table.insertRow(new Object[]{oids.oid("ns:information_schema"), "information_schema", 10, 1, null});
        table.insertRow(new Object[]{oids.oid("ns:pg_toast"), "pg_toast", 10, 1, null});

        for (String schemaName : database.getSchemas().keySet()) {
            int ownerOid = CatalogHelper.resolveOwnerOid(database, oids, "schema:" + schemaName);
            java.util.List<String> acl = database.getSchemaAcl(schemaName);
            String aclText = acl != null && !acl.isEmpty() ? "{" + String.join(",", acl) + "}" : null;
            table.insertRow(new Object[]{oids.oid("ns:" + schemaName), schemaName, ownerOid, 1, aclText});
        }
        return table;
    }

    Table buildPgEnum() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("enumtypid", DataType.OID),
                colNN("enumsortorder", DataType.REAL),
                colNN("enumlabel", DataType.NAME)
        );
        Table table = new Table("pg_enum", cols);
        for (Map.Entry<String, CustomEnum> entry : database.getCustomEnums().entrySet()) {
            CustomEnum ce = entry.getValue();
            int typid = oids.oid("type:" + entry.getKey());
            List<String> labels = ce.getLabels();
            List<Double> sortOrders = ce.getSortOrders();
            for (int i = 0; i < labels.size(); i++) {
                table.insertRow(new Object[]{
                        oids.oid("enum:" + entry.getKey() + ":" + labels.get(i)),
                        typid, sortOrders.get(i), labels.get(i)
                });
            }
        }
        return table;
    }

    /**
     * The key a routine's OID is filed under. A name can carry several routines and PostgreSQL
     * gives each a row of its own: the first of a name answers to the bare key -- which is what
     * regproc, pg_trigger and pg_description all reach for -- and every further one is told apart
     * by where it sits among the overloads of that name.
     */
    static String routineOidKey(String name, int overloadIndex) {
        return overloadIndex == 0 ? "proc:" + name : "proc:" + name + "#" + overloadIndex;
    }

    /**
     * The same key for a routine the database holds, counted the way the pg_proc rows count it, so
     * that a number minted when the routine was created is the number its row will carry.
     */
    static String routineOidKey(Database database, PgFunction routine) {
        int at = 0;
        for (PgFunction other : database.getFunctionOverloads(routine.getName())) {
            if (other == routine) break;
            if (other.getName().equals(routine.getName())) at++;
        }
        return routineOidKey(routine.getName(), at);
    }

    Table buildPgProc() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("proname", DataType.NAME),
                colNN("pronamespace", DataType.OID),
                col("proowner", DataType.OID),
                col("prolang", DataType.OID),
                col("procost", DataType.REAL),
                col("prorows", DataType.REAL),
                col("provariadic", DataType.OID),
                col("prosupport", DataType.REGPROC),
                col("prokind", DataType.INTERNAL_CHAR),
                col("prosecdef", DataType.BOOLEAN),
                col("proleakproof", DataType.BOOLEAN),
                col("proisstrict", DataType.BOOLEAN),
                col("proretset", DataType.BOOLEAN),
                col("provolatile", DataType.INTERNAL_CHAR),
                col("proparallel", DataType.INTERNAL_CHAR),
                col("pronargs", DataType.SMALLINT),
                col("pronargdefaults", DataType.SMALLINT),
                col("prorettype", DataType.OID),
                col("proargtypes", DataType.OIDVECTOR),
                col("proallargtypes", DataType.OID_ARRAY),
                col("proargmodes", DataType.INTERNAL_CHAR_ARRAY),
                col("proargnames", DataType.TEXT_ARRAY),
                col("proargdefaults", DataType.PG_NODE_TREE),
                col("protrftypes", DataType.OID_ARRAY),
                col("prosrc", DataType.TEXT),
                col("probin", DataType.TEXT),
                col("prosqlbody", DataType.PG_NODE_TREE),
                col("proconfig", DataType.TEXT_ARRAY),
                col("proacl", DataType.ACLITEM_ARRAY),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_proc", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        int cLangOid = oids.oid("lang:c");
        int internalLangOid = oids.oid("lang:internal");

        int amHandlerType = 325;   // index_am_handler
        int tableAmHandlerType = 269;
        // Built-in handler functions for pg_am (access methods). A table access method's handler
        // answers table_am_handler, not the index_am_handler the six index methods answer, and a
        // client reading the return type to tell the two kinds apart needs the difference.
        String[] amHandlers = {"heap_tableam_handler", "bthandler", "hashhandler",
                "gisthandler", "ginhandler", "spghandler", "brinhandler"};
        for (String h : amHandlers) {
            boolean isTableAm = h.equals("heap_tableam_handler");
            int hLang = isTableAm ? cLangOid : internalLangOid;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + h), h, pgCatalogNs, 10, hLang, 1.0, 0.0,
                    0, "-", "f", false, false, true, false, "v", "u",
                    (short) 1, (short) 0, isTableAm ? tableAmHandlerType : amHandlerType,
                    oidvector("2281"), null, null, null, null, null,
                    h, null, null, null, null, 1
            });
        }

        // Language handler/validator/inline functions (referenced by pg_language)
        int langHandlerType = 2280; // language_handler
        int voidType = 2278;        // void
        int sqlLangOid = oids.oid("lang:sql");
        // Validators: return void, take oid arg
        String[] validators = {"fmgr_internal_validator", "fmgr_c_validator", "fmgr_sql_validator"};
        for (String v : validators) {
            table.insertRow(new Object[]{
                    oids.oid("proc:" + v), v, pgCatalogNs, 10, internalLangOid, 1.0, 0.0,
                    0, "-", "f", false, false, true, false, "v", "u",
                    (short) 1, (short) 0, voidType,
                    oidvector("26"), null, null, null, null, null,
                    v, null, null, null, null, 1
            });
        }
        // PL/pgSQL call handler: returns language_handler
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_call_handler"), "plpgsql_call_handler", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 0, (short) 0, langHandlerType,
                oidvector(""), null, null, null, null, null,
                "plpgsql_call_handler", null, null, null, null, 1
        });
        // PL/pgSQL inline handler: returns void, takes internal arg
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_inline_handler"), "plpgsql_inline_handler", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 1, (short) 0, voidType,
                oidvector("2281"), null, null, null, null, null,
                "plpgsql_inline_handler", null, null, null, null, 1
        });
        // PL/pgSQL validator: returns void, takes oid arg
        table.insertRow(new Object[]{
                oids.oid("proc:plpgsql_validator"), "plpgsql_validator", pgCatalogNs, 10,
                cLangOid, 1.0, 0.0, 0, "-", "f", false, false, true, false, "v", "u",
                (short) 1, (short) 0, voidType,
                oidvector("26"), null, null, null, null, null,
                "plpgsql_validator", null, null, null, null, 1
        });

        // Built-in aggregates (prokind='a'), one row per overload with the type that overload
        // returns. Reporting anyelement for all of them says nothing a caller can use:
        // sum(int8) returns numeric, and a client deciding how to read the result needs that.
        // any_value, mode, the two percentile_* and the nine regr_* are evaluated correctly and
        // are listed nowhere, so a tool asking the catalog what this server aggregates over is
        // told less than the engine can do. They are not added here: every prokind='a' row needs
        // a pg_aggregate row behind it — PostgreSQL has no aggregate without one, and
        // pg-catalog-consistency.sql checks exactly that — and pg_aggregate is built from
        // BuiltinAggregateSignatures.AGGREGATES, which is where the thirteen have to go.
        Map<String, Integer> aggIndex = new HashMap<>();
        for (String[] agg : BuiltinAggregateSignatures.AGGREGATES) {
            String aggName = agg[0];
            int idx = aggIndex.merge(aggName, 0, (a, b) -> a + 1);
            String oidKey = idx == 0 ? "proc:" + aggName : "proc:" + aggName + "#agg" + idx;
            String[] args = agg[2].isEmpty() ? new String[0] : agg[2].split(" ");
            // Every one of PostgreSQL's 161 built-in aggregates is parallel safe. Reporting them
            // unsafe told a planner reading the column that count(), sum() and min() cannot be
            // computed in a parallel plan — a claim about the server that is simply not true of
            // the aggregates memgres implements.
            table.insertRow(new Object[]{
                    oids.oid(oidKey), aggName, pgCatalogNs, 10, internalLangOid, 1.0, 0.0,
                    0, "-", "a", false, false, false, false, "i", "s",
                    (short) args.length, (short) 0, Integer.parseInt(agg[1]),
                    oidvector(agg[2]), null, null, null, null, null,
                    aggName, null, null, null, null, 1
            });
        }

        // Window functions (prokind='w'). PG will not let one be called as an ordinary function,
        // and a catalog that reports it as 'f' invites exactly that call.
        Map<String, Integer> winIndex = new HashMap<>();
        for (String[] win : BuiltinFunctionSignatures.WINDOW_FUNCTIONS) {
            String winName = win[0];
            int idx = winIndex.merge(winName, 0, (a, b) -> a + 1);
            String oidKey = idx == 0 ? "proc:" + winName : "proc:" + winName + "#win" + idx;
            String[] args = win[2].isEmpty() ? new String[0] : win[2].split(" ");
            // Parallel safe, like the aggregates; and strict exactly when the window function
            // takes an argument — lag, lead, first_value, last_value, nth_value and ntile all
            // return NULL for a NULL input without their body running, while the six that take
            // none (row_number, rank, dense_rank, percent_rank, cume_dist) cannot be strict at
            // all. memgres reported every one of them not strict.
            table.insertRow(new Object[]{
                    oids.oid(oidKey), winName, pgCatalogNs, 10, internalLangOid, 1.0, 0.0,
                    0, "-", "w", false, false, args.length > 0, false, "i", "s",
                    (short) args.length, (short) 0, Integer.parseInt(win[1]),
                    oidvector(win[2]), null, null, null, null, null,
                    winName, null, null, null, null, 1
            });
        }

        int publicNs = oids.oid("ns:public");
        // Iterate all function overloads (not just last-added per name)
        List<PgFunction> allFuncs = new java.util.ArrayList<>();
        Map<String, Integer> overloadIndex = new java.util.HashMap<>();
        for (Map.Entry<String, PgFunction> entry : database.getFunctions().entrySet()) {
            List<PgFunction> overloads = database.getFunctionOverloads(entry.getKey());
            if (overloads != null && !overloads.isEmpty()) {
                for (PgFunction f : overloads) {
                    if (!allFuncs.contains(f)) allFuncs.add(f);
                }
            } else {
                if (!allFuncs.contains(entry.getValue())) allFuncs.add(entry.getValue());
            }
        }
        for (PgFunction fn : allFuncs) {
            String funcSchema = fn.getSchemaName() != null ? fn.getSchemaName() : "public";
            int funcNs = funcSchema.equals("pg_catalog") ? pgCatalogNs : oids.oid("ns:" + funcSchema);
            String lang = fn.getLanguage() != null ? fn.getLanguage().toLowerCase() : "plpgsql";
            int langOid;
            switch (lang) {
                case "sql":
                    langOid = oids.oid("lang:sql");
                    break;
                case "c":
                    langOid = oids.oid("lang:c");
                    break;
                case "internal":
                    langOid = internalLangOid;
                    break;
                default:
                    langOid = oids.oid("lang:plpgsql");
                    break;
            }
            String kind = fn.isProcedure() ? "p" : fn.isWindowFunction() ? "w" : "f";
            // The parameters, split the way PostgreSQL splits them. A RETURNS TABLE column is
            // recorded as mode 't' rather than 'o': it is a column of the result, not a parameter,
            // which is why it is neither counted nor printed among the arguments.
            List<PgFunction.Param> params = fn.getParams() != null ? fn.getParams() : Cols.listOf();
            boolean tableResult = "TABLE".equalsIgnoreCase(fn.getReturnType());
            List<Object> modes = new ArrayList<>();
            List<Object> allTypes = new ArrayList<>();
            List<Object> names = new ArrayList<>();
            StringBuilder argTypesBuilder = new StringBuilder();
            short nargs = 0;
            short ndefaults = 0;
            int variadicElem = 0;
            int outCount = 0;
            int lastOutType = 0;
            for (PgFunction.Param p : params) {
                String mode = paramModeLetter(p.mode());
                if (tableResult && "o".equals(mode)) mode = "t";
                int typeOid = resolveTypeOidByName(p.typeName());
                modes.add(mode);
                allTypes.add(typeOid);
                names.add(p.name() != null ? p.name() : "");
                // proargtypes names what a call passes: everything but the OUT and TABLE entries.
                if (!"o".equals(mode) && !"t".equals(mode)) {
                    if (argTypesBuilder.length() > 0) argTypesBuilder.append(' ');
                    argTypesBuilder.append(typeOid);
                    nargs++;
                }
                if ("o".equals(mode) || "b".equals(mode)) {
                    outCount++;
                    lastOutType = typeOid;
                }
                if ("v".equals(mode)) variadicElem = elementTypeOid(p.typeName(), typeOid);
                if (p.defaultExpr() != null && !p.defaultExpr().isEmpty()) ndefaults++;
            }
            // The three array columns are NULL where they would say nothing: no mode but IN, and
            // no name at all. proallargtypes goes with proargmodes, since it is what the modes
            // are modes of.
            boolean anyNonIn = modes.stream().anyMatch(m -> !"i".equals(m));
            boolean anyNamed = names.stream().anyMatch(n -> !n.toString().isEmpty());
            List<Object> proargmodes = anyNonIn ? modes : null;
            List<Object> proallargtypes = anyNonIn ? allTypes : null;
            List<Object> proargnames = anyNamed ? names : null;
            String fnOwner = fn.getOwner();
            int fnOwnerOid = (fnOwner != null && !fnOwner.isEmpty()) ? oids.oid("role:" + fnOwner) : 10;
            // Build proconfig from function-level SET clauses (e.g., "work_mem=256MB")
            String proconfig = null;
            if (fn.getSetClauses() != null && !fn.getSetClauses().isEmpty()) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (Map.Entry<String, String> sc : fn.getSetClauses().entrySet()) {
                    if (!first) sb.append(",");
                    sb.append(sc.getKey()).append("=").append(sc.getValue());
                    first = false;
                }
                sb.append("}");
                proconfig = sb.toString();
            }
            // proargdefaults is one expression per defaulted argument, and the reader splits it on
            // '|', the same as for a built-in. Joining them with a space and wrapping each in
            // node-tree punctuation meant the reader saw one default for the first of them and
            // printed the punctuation as though it were the expression.
            String proargdefaults = null;
            StringBuilder defs = new StringBuilder();
            for (PgFunction.Param p : params) {
                if (p.defaultExpr() != null && !p.defaultExpr().isEmpty()) {
                    if (defs.length() > 0) defs.append("|");
                    defs.append(p.defaultExpr());
                }
            }
            if (defs.length() > 0) proargdefaults = defs.toString();
            // prosqlbody: populated for BEGIN ATOMIC functions
            String prosqlbody = fn.isAtomicBody() ? fn.getBody() : null;
            // What comes back, and whether it comes back a row at a time. SETOF is not part of the
            // type — it is the proretset flag beside it — so recording "SETOF int" as the type
            // named no type at all, and a client reading prorettype got 0 for every set-returning
            // function it asked about.
            String declared = fn.getReturnType();
            boolean retset = false;
            int retType;
            if (fn.isProcedure()) {
                // A procedure returns nothing, unless it has an OUT or INOUT parameter to hand
                // back through, which makes its result a record however many there are.
                retType = outCount > 0 ? 2249 : 2278;
            } else if (tableResult) {
                retType = 2249;
                retset = true;
            } else if (declared != null && declared.length() > 6
                    && declared.regionMatches(true, 0, "SETOF ", 0, 6)) {
                retType = resolveTypeOidByName(declared.substring(6).trim());
                retset = true;
            } else if (declared != null && !declared.isEmpty()) {
                retType = resolveTypeOidByName(declared);
            } else {
                // No declared result: it is the OUT parameters, one of which is its own type and
                // several of which together are a record.
                retType = outCount == 1 ? lastOutType : (outCount > 1 ? 2249 : 2278);
            }
            // A set-returning function is estimated at a thousand rows unless it said otherwise;
            // one that returns a single value has no row estimate at all.
            double prorows = fn.getRows() > 0 ? fn.getRows() : (retset ? 1000 : 0);
            // Use unique OID key for overloaded functions (append param count)
            int idx = overloadIndex.merge(fn.getName(), 0, (a, b) -> a + 1);
            String oidKey = routineOidKey(fn.getName(), idx);
            table.insertRow(new Object[]{
                    oids.oid(oidKey), fn.getName(), funcNs, fnOwnerOid,
                    langOid, fn.getCost(), prorows, variadicElem, "-", kind,
                    fn.isSecurityDefiner(), fn.isLeakproof(), fn.isStrict(), retset,
                    fn.getVolatility() != null ? fn.getVolatility().substring(0, 1).toLowerCase() : "v",
                    fn.getParallel() != null ? fn.getParallel().substring(0, 1).toLowerCase() : "u",
                    nargs, ndefaults, retType,
                    oidvector(argTypesBuilder.toString()), proallargtypes, proargmodes, proargnames,
                    proargdefaults, null,
                    fn.getBody(), null, prosqlbody, proconfig, null, 1
            });
        }

        // The event trigger helpers are registered from their recorded signatures further down,
        // like every other built-in. A row written here first won every one of those four names,
        // and the row written here said void: pg_event_trigger_ddl_commands returns SETOF record
        // and pg_event_trigger_table_rewrite_oid returns an oid, so anything reading the catalog
        // to decide how to call them read a function that returns nothing.

        // Built-in extension functions (pgcrypto, fuzzystrmatch, unaccent, json). The uuid-ossp
        // and pg_trgm names are deliberately absent: PostgreSQL puts an extension's functions in
        // the schema the extension was installed into and nowhere until then, so those are
        // registered below only once CREATE EXTENSION has run.
        String[] extensionFunctions = {
                "digest", "hmac", "gen_salt", "gen_random_uuid",
                "levenshtein", "soundex",
                "unaccent",
                "json_strip_nulls", "jsonb_strip_nulls",
                "jsonb_object", "json_populate_record", "json_populate_recordset",
                "jsonb_populate_record", "jsonb_populate_recordset",
                "jsonb_path_match", "jsonb_path_match_tz",
                "row_to_json", "to_json", "to_jsonb",
                "uuidv4",
                "unicode_version", "unicode_assigned"
        };
        for (String extFn : extensionFunctions) {
            // A name PostgreSQL declares a signature for is registered from that signature below
            if (SIGNED_BUILTINS.contains(extFn)) continue;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + extFn), extFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "i", "u",
                    (short) 0, (short) 0, 0,
                    oidvector(""), null, null, null, null, null,
                    extFn, null, null, null, null, 1
            });
        }

        // Replication / WAL functions (stubs for pg_proc visibility)
        String[] replicationFunctions = {
                "pg_create_logical_replication_slot",
                "pg_create_physical_replication_slot",
                "pg_drop_replication_slot",
                "pg_logical_slot_get_changes",
                "pg_logical_slot_peek_changes",
                "pg_replication_slot_advance",
                "pg_switch_wal",
                "pg_walfile_name",
                "pg_last_wal_receive_lsn",
                "pg_last_wal_replay_lsn",
                "pg_backup_start",
                "pg_backup_stop",
                "pg_promote",
                "pg_create_restore_point",
                "pg_wal_replay_pause",
                "pg_wal_replay_resume"
        };
        for (String replFn : replicationFunctions) {
            if (SIGNED_BUILTINS.contains(replFn)) continue;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + replFn), replFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "v", "u",
                    (short) 0, (short) 0, 0,
                    oidvector(""), null, null, null, null, null,
                    replFn, null, null, null, null, 1
            });
        }

        // Large object functions
        // has_largeobject_privilege(user oid, loid oid, privilege text) → boolean
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 3, (short) 0, 16, // returns boolean (OID 16)
                oidvector("26 26 25"), null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // has_largeobject_privilege(user name, loid oid, privilege text) → boolean
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege_name"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 3, (short) 0, 16, // returns boolean (OID 16)
                oidvector("19 26 25"), null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // has_largeobject_privilege(loid oid, privilege text) → boolean (current user)
        table.insertRow(new Object[]{
                oids.oid("proc:has_largeobject_privilege_2"), "has_largeobject_privilege", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "s", "u",
                (short) 2, (short) 0, 16, // returns boolean (OID 16)
                oidvector("26 25"), null, null, null, null, null,
                "has_largeobject_privilege", null, null, null, null, 1
        });
        // lo_export(loid oid, filename text) → int4
        table.insertRow(new Object[]{
                oids.oid("proc:lo_export"), "lo_export", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 23, // returns int4 (OID 23)
                oidvector("26 25"), null, null, null, null, null,
                "lo_export", null, null, null, null, 1
        });
        // lo_import(filename text) → oid
        table.insertRow(new Object[]{
                oids.oid("proc:lo_import"), "lo_import", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 1, (short) 0, 26, // returns oid (OID 26)
                oidvector("25"), null, null, null, null, null,
                "lo_import", null, null, null, null, 1
        });
        // lo_import(filename text, loid oid) → oid (2-arg overload)
        table.insertRow(new Object[]{
                oids.oid("proc:lo_import_2"), "lo_import", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 26, // returns oid (OID 26)
                oidvector("25 26"), null, null, null, null, null,
                "lo_import", null, null, null, null, 1
        });
        // lo_truncate64(fd int4, len int8) → int4
        table.insertRow(new Object[]{
                oids.oid("proc:lo_truncate64"), "lo_truncate64", pgCatalogNs, 10,
                internalLangOid, 1.0, 0.0, 0, "-", "f",
                false, false, false, false, "v", "u",
                (short) 2, (short) 0, 23, // returns int4 (OID 23)
                oidvector("23 20"), null, null, null, null, null,
                "lo_truncate64", null, null, null, null, 1
        });

        // uuid_extract_timestamp and uuid_extract_version are registered from their recorded
        // signatures further down. The rows written here won both names and said
        // uuid_extract_version returns integer; PostgreSQL returns smallint.

        // pg_control_* functions (return record)
        String[] pgControlFunctions = {
                "pg_control_checkpoint", "pg_control_init",
                "pg_control_recovery", "pg_control_system"
        };
        for (String ctlFn : pgControlFunctions) {
            // One record, not a set of them: each of these reads the control file once and
            // answers a single row, and PG records proretset false and provolatile 'v' for all
            // four. Reporting a set makes a client expect to iterate what it cannot.
            table.insertRow(new Object[]{
                    oids.oid("proc:" + ctlFn), ctlFn, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, true, false, "v", "u",
                    (short) 0, (short) 0, 2249, // returns record (OID 2249)
                    oidvector(""), null, null, null, null, null,
                    ctlFn, null, null, null, null, 1
            });
        }

        // User-defined aggregates: emit with prokind='a'
        for (Map.Entry<String, PgAggregate> aggEntry : database.getUserAggregates().entrySet()) {
            PgAggregate agg = aggEntry.getValue();
            // Determine namespace from the aggregate's schema
            String aggSchema = agg.getSchemaName() != null ? agg.getSchemaName() : "public";
            int aggNs = aggSchema.equals("pg_catalog") ? pgCatalogNs : oids.oid("ns:" + aggSchema);
            // Determine arg count from aggregate's argTypes
            short aggNargs = agg.getArgTypes() != null ? (short) agg.getArgTypes().length : 0;
            // An aggregate with no final function returns its transition type; PG never leaves
            // prorettype unset, and a client reading the result type needs it.
            int aggRetType = resolveTypeOidByName(agg.getStype());
            StringBuilder aggArgs = new StringBuilder();
            if (agg.getArgTypes() != null) {
                for (String at : agg.getArgTypes()) {
                    if (aggArgs.length() > 0) aggArgs.append(' ');
                    aggArgs.append(resolveTypeOidByName(at));
                }
            }
            table.insertRow(new Object[]{
                    oids.oid("proc:" + agg.getName()), agg.getName(), aggNs, 10,
                    oids.oid("lang:internal"), 1.0, 0.0, 0, "-", "a",
                    false, false, false, false, "i", "u",
                    aggNargs, (short) 0, aggRetType,
                    oidvector(aggArgs.toString()), null, null, null, null, null,
                    null, null, null, null, null, 1
            });
        }

        // The functions behind the built-in operators. pg_operator.oprcode points at one of these
        // for every operator, and a reference to a function with no pg_proc row is what makes
        // psql's \do and every "what does this operator do" query come back empty.
        // Seeded from what is already in the table, not empty: an OID here is minted from the
        // name alone, so a name registered twice is two rows sharing one OID — which is a
        // duplicate key in a catalog every join reads by OID.
        Set<String> operatorFuncs = existingProcNames(table);
        for (Object[] op : PgOperatorTable.OPERATORS) {
            String fname = (String) op[5];
            // As with the cast functions: a name PostgreSQL declares signatures for is registered
            // from those, whole. Deriving a row from the one operator that names it recorded a
            // single overload of a name that has several — tsquery_phrase lost the form that
            // takes a distance, because <-> names only the two-argument one.
            if (SIGNED_BUILTINS.contains(fname)) continue;
            if (!operatorFuncs.add(fname.toLowerCase())) continue;
            int left = (Integer) op[2];
            int right = (Integer) op[3];
            String argTypes = left == 0 ? String.valueOf(right)
                    : right == 0 ? String.valueOf(left) : left + " " + right;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + fname), fname, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, true, false, "i", "s",
                    (short) (left == 0 || right == 0 ? 1 : 2), (short) 0, op[4],
                    oidvector(argTypes), null, null, null, null, null,
                    fname, null, null, null, null, 1
            });
        }

        // The I/O functions pg_type points at. Every type names an input, an output and often a
        // receive, send, typmod or analyze function, and a regproc column naming a function with
        // no pg_proc row is a reference a client cannot follow: "which function parses this
        // type" comes back empty. The names are read back from the pg_type rows rather than
        // derived a second time, so the two lists cannot drift apart.
        addTypeSupportFunctions(table, pgCatalogNs, internalLangOid);

        // The conversion functions pg_cast points at. A cast row naming a function with no
        // pg_proc row behind it drops out of every join that reads the cast catalogue, which is
        // how a tool works out what conversions the server will perform.
        // Keyed by name alone, and seeded from the table: one conversion function serving several
        // source types is one function, and giving it a row per source type made several rows
        // carry the one OID its name mints.
        Set<String> castFuncs = existingProcNames(table);
        for (Object[] c : PgCastTable.CASTS) {
            String fname = (String) c[2];
            // A name PostgreSQL declares a signature for is registered from that signature
            // below, whole. Minting a row here from the first cast that happens to name it
            // read the target type off that cast: oid(bigint) returns oid, and the first row
            // naming it is the int8 -> regproc cast, so pg_proc said it returned regproc.
            if (SIGNED_BUILTINS.contains(fname)) continue;
            if (fname.isEmpty() || !castFuncs.add(fname.toLowerCase())) continue;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + fname), fname, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, true, false, "i", "s",
                    (short) 1, (short) 0, c[1],
                    oidvector(String.valueOf(c[0])), null, null, null, null, null,
                    fname, null, null, null, null, 1
            });
        }

        // The rest of what memgres evaluates. Without a row here a function works when called but
        // is invisible to anything that asks the catalog first — the JDBC driver's getFunctions,
        // ::regproc, and psql's \df all read pg_proc. Runs last, and skips a name already
        // registered above with a fuller signature, so no function is listed twice.
        Set<String> alreadyListed = new HashSet<>();
        for (Object[] existing : table.getRows()) {
            if (existing.length > 1 && existing[1] != null) {
                alreadyListed.add(existing[1].toString().toLowerCase());
            }
        }
        // A row is a claim about what the server can do, and a name with no signature behind it
        // is a claim nothing can act on: overload resolution, pg_get_function_arguments and a
        // client deciding how to bind a call all read the argument and return types, so each
        // overload PostgreSQL declares is registered with the types it declares.
        Map<String, Integer> signatureIndex = new HashMap<>();
        Set<String> signed = new HashSet<>();
        for (String[] sig : BuiltinFunctionSignatures.SIGNATURES) {
            String name = sig[0];
            if (alreadyListed.contains(name.toLowerCase())) continue;
            // An extension's functions live in the schema the extension was installed into, and
            // nowhere at all before CREATE EXTENSION runs. Listing them in pg_catalog on a fresh
            // database advertised nine names PostgreSQL has nowhere, every one of them a call
            // memgres itself refuses until the extension exists.
            String owningExtension = BuiltinFunctionSignatures.owningExtension(name);
            int fnNs = pgCatalogNs;
            if (owningExtension != null) {
                if (!database.hasExtension(owningExtension)) continue;
                String extSchema = database.getExtensionSchema(owningExtension);
                fnNs = extSchema == null ? publicNs : oids.oid("ns:" + extSchema);
            }
            signed.add(name.toLowerCase());
            String[] args = sig[2].isEmpty() ? new String[0] : sig[2].split(" ");
            int idx = signatureIndex.merge(name, 0, (a, b) -> a + 1);
            String oidKey = idx == 0 ? "proc:" + name : "proc:" + name + "#sig" + idx;
            // PostgreSQL declares its built-ins strict — a NULL argument gives NULL without the
            // body running — and parallel safe unless the function is volatile, in which case
            // it is parallel restricted. Reporting false/'u' for every one of them left two
            // columns of a pg_proc row saying nothing a planner or a client could use.
            char volatility = sig[3].charAt(1);
            // The parameters this signature does not insist on are the ones carrying a default,
            // which is what pronargdefaults counts; and a variadic signature collects its tail
            // into the last declared type, which is what provariadic names. Both were reported
            // as zero for every row, so a client could not tell format(text, VARIADIC "any")
            // from a two-argument function or see that jsonb_set's fourth argument may be left
            // out — the arity table has known this all along and pg_proc did not say it.
            int fewest = BuiltinFunctionSignatures.fewestArguments(sig);
            short nargdefaults = (short) Math.max(0, args.length - fewest);
            // provariadic names the type the tail collects INTO, which for an array parameter is
            // its element type rather than the array itself.
            int variadic = BuiltinFunctionSignatures.isVariadic(sig) && args.length > 0
                    ? BuiltinFunctionSignatures.variadicElementType(
                            Integer.parseInt(args[args.length - 1]))
                    : 0;
            table.insertRow(new Object[]{
                    oids.oid(oidKey), name, fnNs, 10,
                    internalLangOid, 1.0, sig[3].charAt(0) == 't' ? 1000.0 : 0.0,
                    variadic, "-", "f",
                    false, false, BuiltinFunctionSignatures.isStrict(name, sig[2]),
                    sig[3].charAt(0) == 't',
                    String.valueOf(volatility), volatility == 'v' ? "r" : "s",
                    (short) args.length, nargdefaults, Integer.parseInt(sig[1]),
                    oidvector(sig[2]), null, null, null, null, null,
                    name, null, null, null, null, 1
            });
        }
        // Names memgres evaluates that PostgreSQL has no signature for keep a bare row: without
        // one the function works when called but is invisible to anything that asks first.
        for (String builtin : BuiltinFunctionNames.NAMES) {
            if (signed.contains(builtin.toLowerCase())) continue;
            if (!alreadyListed.add(builtin.toLowerCase())) continue;
            table.insertRow(new Object[]{
                    oids.oid("proc:" + builtin), builtin, pgCatalogNs, 10,
                    internalLangOid, 1.0, 0.0, 0, "-", "f",
                    false, false, false, false, "i", "u",
                    (short) 0, (short) 0, 0,
                    oidvector(""), null, null, null, null, null,
                    builtin, null, null, null, null, 1
            });
        }

        applyRecordedProcRows(table, pgCatalogNs);
        return table;
    }

    /**
     * Give every pg_catalog function the columns PostgreSQL records for it.
     *
     * <p>Run over the finished table rather than at each of the dozen places a row is written,
     * because the rows come from a dozen places: a signature, an operator's function, a type's I/O
     * function, a cast's conversion function, an aggregate, a window function, and — for
     * {@code pg_sleep} and its two relatives — a {@link PgFunction} the database registers at
     * startup. The last of those is why the sweep is over the table: those three rows carried
     * argument names PostgreSQL does not give them, and no amount of care at the signature loop
     * would have reached a row that loop never writes.
     *
     * <p>Only pg_catalog rows are touched. A user function named {@code concat(any)} is that
     * user's function and PostgreSQL's numbers say nothing about it.
     */
    private void applyRecordedProcRows(Table table, int pgCatalogNs) {
        int nameAt = table.getColumnIndex("proname");
        int nsAt = table.getColumnIndex("pronamespace");
        int argsAt = table.getColumnIndex("proargtypes");
        int costAt = table.getColumnIndex("procost");
        int rowsAt = table.getColumnIndex("prorows");
        int variadicAt = table.getColumnIndex("provariadic");
        int strictAt = table.getColumnIndex("proisstrict");
        int volatileAt = table.getColumnIndex("provolatile");
        int parallelAt = table.getColumnIndex("proparallel");
        int allArgsAt = table.getColumnIndex("proallargtypes");
        int modesAt = table.getColumnIndex("proargmodes");
        int namesAt = table.getColumnIndex("proargnames");
        int defaultsAt = table.getColumnIndex("proargdefaults");
        for (Object[] row : table.getRows()) {
            if (!(row[nsAt] instanceof Number)
                    || ((Number) row[nsAt]).intValue() != pgCatalogNs) continue;
            if (row[nameAt] == null) continue;
            String argTypes = row[argsAt] == null ? "" : row[argsAt].toString();
            String[] rec = BuiltinFunctionSignatures.recordedProcRow(
                    row[nameAt].toString(), argTypes);
            if (rec == null) continue;
            row[allArgsAt] = oidListOrNull(rec[2]);
            row[modesAt] = textListOrNull(rec[3]);
            row[namesAt] = textListOrNull(rec[4]);
            // proargdefaults is one expression per defaulted argument, and the reader splits it
            // on '|'; the table separates them with '~' because '|' separates its own fields.
            row[defaultsAt] = rec[5].isEmpty() ? null : rec[5].replace('~', '|');
            row[variadicAt] = Integer.valueOf(Integer.parseInt(rec[6]));
            row[costAt] = Double.valueOf(rec[7]);
            row[rowsAt] = Double.valueOf(rec[8]);
            row[volatileAt] = rec[9];
            row[parallelAt] = rec[10];
            row[strictAt] = Boolean.valueOf("t".equals(rec[11]));
        }
    }

    /** A comma-separated list of OIDs as the array column holds it, or null when it is empty. */
    private static List<Object> oidListOrNull(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.isEmpty()) return null;
        List<Object> out = new ArrayList<>();
        for (String part : commaSeparated.split(",")) out.add(Integer.valueOf(part.trim()));
        return out;
    }

    /** A comma-separated list of strings as the array column holds it, or null when it is empty. */
    private static List<Object> textListOrNull(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.isEmpty()) return null;
        List<Object> out = new ArrayList<>();
        for (String part : commaSeparated.split(",", -1)) out.add(part);
        return out;
    }

    /**
     * The I/O functions PostgreSQL declares over something other than the type that names them.
     *
     * <p>Two groups. The polymorphic ones — array, record, range, multirange, domain, enum — are
     * one function serving every type of their kind, so PostgreSQL declares them over anyarray,
     * record, anyrange, anymultirange, "any" and anyenum rather than over the type whose pg_type
     * row points at them. The rest are the types that carry a typmod: their input and receive
     * functions take the type OID and the typmod beside the value, three arguments where the
     * others take one.
     *
     * <p>Columns: proname, prorettype, proargtypes. Read from the reference server.
     */
    private static final String[][] DECLARED_IO = {
            {"array_in", "2277", "2275 26 23"},
            {"array_out", "2275", "2277"},
            {"array_recv", "2277", "2281 26 23"},
            {"array_send", "17", "2277"},
            {"record_in", "2249", "2275 26 23"},
            {"record_out", "2275", "2249"},
            {"record_recv", "2249", "2281 26 23"},
            {"record_send", "17", "2249"},
            {"range_in", "3831", "2275 26 23"},
            {"range_out", "2275", "3831"},
            {"range_recv", "3831", "2281 26 23"},
            {"range_send", "17", "3831"},
            {"multirange_in", "4537", "2275 26 23"},
            {"multirange_out", "2275", "4537"},
            {"multirange_recv", "4537", "2281 26 23"},
            {"multirange_send", "17", "4537"},
            {"domain_in", "2276", "2275 26 23"},
            {"domain_out", "2275", "2276"},
            {"domain_recv", "2276", "2281 26 23"},
            {"domain_send", "17", "2276"},
            {"enum_in", "3500", "2275 26"},
            {"enum_out", "2275", "3500"},
            {"enum_recv", "3500", "2281 26"},
            {"enum_send", "17", "3500"},
            {"bit_in", "1560", "2275 26 23"},
            {"bit_recv", "1560", "2281 26 23"},
            {"bpcharin", "1042", "2275 26 23"},
            {"bpcharrecv", "1042", "2281 26 23"},
            {"interval_in", "1186", "2275 26 23"},
            {"interval_recv", "1186", "2281 26 23"},
            {"numeric_in", "1700", "2275 26 23"},
            {"numeric_recv", "1700", "2281 26 23"},
            {"time_in", "1083", "2275 26 23"},
            {"time_recv", "1083", "2281 26 23"},
            {"timestamp_in", "1114", "2275 26 23"},
            {"timestamp_recv", "1114", "2281 26 23"},
            {"timestamptz_in", "1184", "2275 26 23"},
            {"timestamptz_recv", "1184", "2281 26 23"},
            {"timetz_in", "1266", "2275 26 23"},
            {"timetz_recv", "1266", "2281 26 23"},
            {"varbit_in", "1562", "2275 26 23"},
            {"varbit_recv", "1562", "2281 26 23"},
            {"varcharin", "1043", "2275 26 23"},
            {"varcharrecv", "1043", "2281 26 23"},
    };

    /**
     * I/O functions that take the type OID and the typmod beside the value but return the type
     * whose pg_type row named them, so only their argument list is recorded here.
     */
    private static final String[] IO_WITH_TYPMOD_ARGS = {
            "anyrange_in", "anymultirange_in", "anycompatiblerange_in", "anycompatiblemultirange_in",
    };

    /** Whether this input function reads a type OID and a typmod beside the value. */
    private static boolean takesTypmodArgs(String name) {
        for (String io : IO_WITH_TYPMOD_ARGS) {
            if (io.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /** The signature PostgreSQL declares for this I/O function, or null when it declares none. */
    private static String[] declaredIoSignature(String name) {
        for (String[] io : DECLARED_IO) {
            if (io[0].equalsIgnoreCase(name)) return new String[]{io[1], io[2]};
        }
        return null;
    }

    /** cstring, the type PostgreSQL's I/O functions read from and write to. */
    private static final int CSTRING = 2275;
    /** internal, what a receive or analyze function is handed. */
    private static final int INTERNAL = 2281;

    /**
     * A pg_proc row for every function a pg_type row names.
     *
     * <p>The columns are read off the built pg_type table, so a type registered later brings its
     * I/O functions with it without this having to be extended.
     */
    /** The function names pg_proc already carries, lower-cased, so a name is registered once. */
    private static Set<String> existingProcNames(Table table) {
        Set<String> names = new HashSet<String>();
        for (Object[] existing : table.getRows()) {
            if (existing.length > 1 && existing[1] != null) {
                names.add(existing[1].toString().toLowerCase());
            }
        }
        return names;
    }

    private void addTypeSupportFunctions(Table table, int pgCatalogNs, int internalLangOid) {
        Set<String> listed = existingProcNames(table);
        Table types = buildPgType();
        int oidAt = types.getColumnIndex("oid");
        // arg type, return type: an input function reads a cstring and answers the type, an
        // output function does the reverse, and so on down PostgreSQL's own conventions.
        String[] names = {"typinput", "typoutput", "typreceive", "typsend",
                "typmodin", "typmodout", "typanalyze", "typsubscript"};
        for (Object[] row : types.getRows()) {
            int typeOid = ((Number) row[oidAt]).intValue();
            for (String col : names) {
                int at = types.getColumnIndex(col);
                if (at < 0 || at >= row.length) continue;
                if (!(row[at] instanceof RegprocValue)) continue;
                RegprocValue fn = (RegprocValue) row[at];
                if (fn.oid() == 0 || fn.name() == null) continue;
                if (!listed.add(fn.name().toLowerCase())) continue;
                String argTypes;
                int retType;
                // A handful of I/O functions serve more than the one type that names them, and
                // PostgreSQL declares those polymorphically rather than over the type at hand:
                // array_in answers anyarray, not the particular array type whose pg_type row
                // reached it, and it reads a type OID and a typmod beside the cstring. Recorded
                // here from the reference server so proargtypes and prorettype are the ones a
                // caller resolves against.
                String[] declared = declaredIoSignature(fn.name());
                if (declared != null) {
                    retType = Integer.parseInt(declared[0]);
                    argTypes = declared[1];
                } else if (takesTypmodArgs(fn.name())) {
                    retType = typeOid;
                    argTypes = CSTRING + " 26 23";
                } else {
                    int argType;
                    if ("typinput".equals(col)) { argType = CSTRING; retType = typeOid; }
                    else if ("typoutput".equals(col)) { argType = typeOid; retType = CSTRING; }
                    else if ("typreceive".equals(col)) { argType = INTERNAL; retType = typeOid; }
                    else if ("typsend".equals(col)) { argType = typeOid; retType = 17; }
                    else if ("typmodin".equals(col)) { argType = 1263; retType = 23; }
                    else if ("typmodout".equals(col)) { argType = 23; retType = CSTRING; }
                    else if ("typanalyze".equals(col)) { argType = INTERNAL; retType = 16; }
                    else { argType = INTERNAL; retType = INTERNAL; }
                    argTypes = String.valueOf(argType);
                }
                int nargs = argTypes.trim().isEmpty() ? 0 : argTypes.trim().split("\\s+").length;
                table.insertRow(new Object[]{
                        oids.oid("proc:" + fn.name()), fn.name(), pgCatalogNs, 10,
                        internalLangOid, 1.0, 0.0, 0, "-", "f",
                        false, false, true, false, "i", "s",
                        (short) nargs, (short) 0, retType,
                        oidvector(argTypes), null, null, null, null, null,
                        fn.name(), null, null, null, null, 1
                });
            }
        }
    }

    /**
     * Build an aclitem[] string for a table based on granted privileges.
     * Returns null if no privileges have been granted, or a PG-style aclitem array string.
     */
    /** @param tableName the schema-qualified key privileges are recorded under */
    private String buildRelacl(String tableName) {
        Map<String, Set<String>> allPrivs = database.getAllRolePrivileges();
        // Collect grants: grantee -> set of privilege abbreviations
        Map<String, Set<String>> aclEntries = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> entry : allPrivs.entrySet()) {
            String role = entry.getKey();
            for (String priv : entry.getValue()) {
                // Format: "PRIVILEGE:OBJECTTYPE:OBJECTNAME"
                String[] parts = priv.split(":", 3);
                if (parts.length == 3 && parts[1].equalsIgnoreCase("TABLE")
                        && parts[2].equalsIgnoreCase(tableName)) {
                    String abbrev = privAbbrev(parts[0]);
                    if (abbrev != null) {
                        aclEntries.computeIfAbsent(role, k -> new java.util.LinkedHashSet<>()).add(abbrev);
                    }
                }
            }
        }
        if (aclEntries.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Set<String>> entry : aclEntries.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            String grantee = entry.getKey().equalsIgnoreCase("public") ? "" : entry.getKey();
            sb.append(grantee).append("=");
            for (String a : entry.getValue()) sb.append(a);
            sb.append("/").append("memgres"); // grantor
        }
        sb.append("}");
        return sb.toString();
    }

    /** Build a PG text-array string for table storage parameters (reloptions). */
    private String buildTableReloptions(Table t) {
        Map<String, String> opts = t.getReloptions();
        if (opts == null || opts.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : opts.entrySet()) {
            if (!first) sb.append(",");
            sb.append(entry.getKey()).append("=").append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    /** Map a privilege name to its PG aclitem abbreviation. */
    private static String privAbbrev(String priv) {
        switch (priv.toUpperCase()) {
            case "SELECT": return "r";
            case "INSERT": return "a";
            case "UPDATE": return "w";
            case "DELETE": return "d";
            case "TRUNCATE": return "D";
            case "REFERENCES": return "x";
            case "TRIGGER": return "t";
            case "ALL": return "arwdDxt";
            case "USAGE": return "U";
            default: return null;
        }
    }

    private int resolveAccessMethodOid(String method) {
        if (method == null) return 403; // default btree
        switch (method.toLowerCase()) {
            case "btree": return 403;
            case "hash": return 405;
            case "gist": return 783;
            case "gin": return 2742;
            case "spgist": return 4000;
            case "brin": return 3580;
            default: return oids.oid("am:" + method.toLowerCase());
        }
    }

    /** Resolve a type name (e.g., "int", "text", "integer") to its PG OID. */
    /** The single letter pg_proc records a parameter's mode as. */
    private static String paramModeLetter(String mode) {
        String m = mode == null ? "in" : mode.toLowerCase().trim();
        switch (m) {
            case "": case "in": return "i";
            case "out": return "o";
            case "inout": return "b";
            case "variadic": return "v";
            default: return m;   // already a letter
        }
    }

    /**
     * What a VARIADIC parameter's tail collects into, which is what provariadic names.
     *
     * <p>PostgreSQL records the ELEMENT type, not the array the parameter was declared as: a
     * {@code VARIADIC c int[]} has provariadic 23, not 1007. {@code "any"} is its own element type.
     */
    private int elementTypeOid(String declaredTypeName, int declaredOid) {
        if (declaredTypeName != null) {
            String t = declaredTypeName.trim();
            if (t.endsWith("[]")) return resolveTypeOidByName(t.substring(0, t.length() - 2));
        }
        DataType arr = DataType.fromOid(declaredOid);
        DataType elem = arr == null ? null : DataType.elementOf(arr);
        return elem != null ? elem.getOid() : declaredOid;
    }

    /**
     * The modifier a written type name carries, packed the way {@code atttypmod} carries it.
     *
     * <p>A composite type's attribute is stored as the type name the statement wrote, so the width
     * in {@code varchar(5)} and the precision in {@code numeric(8,2)} are in that text and nowhere
     * else. Answering -1 for every one of them said each attribute had been declared with no
     * modifier at all: {@code format_type} named an unbounded varchar where PostgreSQL names
     * varchar(5), and a client sizing an input from the catalog was told the wrong type.
     */
    private static int writtenTypmod(String typeName) {
        if (typeName == null) return -1;
        String lower = typeName.toLowerCase(Locale.ROOT).trim();
        // An array takes its modifier from its element, and that is where it is written.
        if (lower.endsWith("[]")) lower = lower.substring(0, lower.length() - 2).trim();
        // An interval writes the fields it keeps as words rather than in brackets -- "interval
        // hour to minute" -- and those fields are as much a modifier as a width is. Reading only
        // what stands inside brackets threw the whole qualifier away, so format_type named a
        // full-range interval where PostgreSQL names the narrowed one.
        if (lower.equals("interval") || lower.startsWith("interval ")
                || lower.startsWith("interval(")) {
            return intervalWrittenTypmod(lower);
        }
        int open = lower.indexOf('(');
        int close = lower.indexOf(')', open + 1);
        if (open <= 0 || close < 0) return -1;
        DataType dt = DataType.fromPgName(lower.substring(0, open).trim());
        if (dt == null) return -1;
        String[] parts = lower.substring(open + 1, close).split(",");
        Integer precision;
        Integer scale = null;
        try {
            precision = Integer.valueOf(parts[0].trim());
            if (parts.length > 1) scale = Integer.valueOf(parts[1].trim());
        } catch (NumberFormatException e) {
            return -1;
        }
        return CatalogHelper.attTypmod(dt, precision, scale, null);
    }

    /**
     * The modifier an interval declaration carries. PostgreSQL packs the fields the qualifier
     * keeps into the high half of the number and the fractional-seconds precision into the low
     * half, and {@code interval second(2)} writes both: the qualifier before the brackets and the
     * precision inside them.
     */
    private static int intervalWrittenTypmod(String written) {
        String qualifier = written.substring("interval".length()).trim();
        Integer precision = null;
        int open = qualifier.indexOf('(');
        if (open >= 0) {
            int close = qualifier.indexOf(')', open + 1);
            if (close < 0) return -1;
            try {
                precision = Integer.valueOf(qualifier.substring(open + 1, close).trim());
            } catch (NumberFormatException e) {
                return -1;
            }
            qualifier = qualifier.substring(0, open).trim();
        }
        return CatalogHelper.attTypmod(DataType.INTERVAL, precision, null,
                qualifier.isEmpty() ? null : qualifier);
    }

    /**
     * How a value of the type a composite attribute was declared with is laid out: its width,
     * whether it is passed by value, what boundary it starts on and how it is stored, in that
     * order. PostgreSQL keeps these on the type and copies them onto every attribute of it, which
     * is why an attribute a drop took away still reports the ones its declared type had even
     * though its atttypid has been cleared.
     *
     * <p>An array is a varlena of its own whatever it holds, so only the boundary comes from the
     * element. A type memgres keeps no record of leaves the caller its own answer.
     */
    private String[] attributeLayout(String typeName) {
        if (typeName == null) return null;
        String lower = typeName.toLowerCase(Locale.ROOT).trim();
        int paren = lower.indexOf('(');
        if (paren > 0) {
            int close = lower.lastIndexOf(')');
            String tail = close >= 0 && close + 1 < lower.length() ? lower.substring(close + 1) : "";
            lower = (lower.substring(0, paren) + tail).trim();
        }
        boolean array = lower.endsWith("[]");
        if (array) lower = lower.substring(0, lower.length() - 2).trim();
        // The fields an interval was narrowed to are a qualifier, not another type.
        if (lower.startsWith("interval ")) lower = "interval";
        String[] attrs = typeAttrs(layoutTypname(lower));
        if (attrs == null) return null;
        if (array) return new String[]{"-1", "f", attrs[7], "x"};
        return new String[]{attrs[1], attrs[2], attrs[7], attrs[8]};
    }

    /**
     * The pg_type name whose layout a written type name has. A type the reader defined is laid out
     * as what it is made of: an enum is the four bytes of its OID, a composite is a record whatever
     * its attributes are, and a domain is exactly the type it is built on.
     */
    private String layoutTypname(String written) {
        DataType dt = DataType.fromPgName(written);
        if (dt != null) return storedTypeName(dt);
        String key = TypeNamespace.find(database.typeKeys(), written);
        if (key == null) return written;
        if (database.getCustomEnums().containsKey(key)) return "int4";
        if (database.getCompositeTypes().containsKey(key)) return "record";
        DomainType domain = database.getDomains().get(key);
        if (domain != null && domain.getBaseType() != null) {
            return storedTypeName(domain.getBaseType());
        }
        return written;
    }

    private int resolveTypeOidByName(String typeName) {
        if (typeName == null) return 0;
        String lower = typeName.toLowerCase().trim();
        // pg_proc records a type OID and has nowhere to put a modifier, so numeric(10,2) is the
        // numeric type. Resolving the written name whole found nothing for any of them.
        int paren = lower.indexOf('(');
        if (paren > 0) {
            int close = lower.lastIndexOf(')');
            String tail = close >= 0 && close + 1 < lower.length() ? lower.substring(close + 1) : "";
            lower = (lower.substring(0, paren) + tail).trim();
        }
        // The fields an interval was narrowed to are a qualifier the same way a precision is:
        // "interval day to second" is the interval type, and pg_proc has nowhere to say more.
        if (lower.startsWith("interval ")) lower = "interval";
        // An array of a type is a type of its own, with its own OID. Resolving the whole written
        // name found nothing, so every array-typed parameter and result was recorded as OID 0 —
        // a type a client cannot look up and cannot name.
        if (lower.endsWith("[]")) {
            String element = lower.substring(0, lower.length() - 2);
            int elem = resolveTypeOidByName(element);
            DataType elemType = elem == 0 ? null : DataType.fromOid(elem);
            DataType arrayType = elemType == null ? null : DataType.arrayOf(elemType);
            if (arrayType != null) return arrayType.getOid();
            // An array of a type the reader defined is a type of its own with an OID of its own,
            // registered beside the element's. Answering zero for it left an attribute declared
            // "myenum[]" pointing at no type at all, which format_type prints as a dash.
            String userKey = TypeNamespace.oidKeyFor(database, element);
            return userKey == null ? 0 : oids.oid(userKey + "[]");
        }
        // Handle common aliases
        switch (lower) {
            case "int": case "integer": case "int4": return 23;
            case "bigint": case "int8": return 20;
            case "smallint": case "int2": return 21;
            case "text": return 25;
            case "varchar": case "character varying": return 1043;
            case "char": case "character": case "bpchar": return 1042;
            case "boolean": case "bool": return 16;
            case "float4": case "real": return 700;
            case "float8": case "double precision": return 701;
            case "numeric": case "decimal": return 1700;
            case "date": return 1082;
            case "timestamp": case "timestamp without time zone": return 1114;
            case "timestamptz": case "timestamp with time zone": return 1184;
            case "time": case "time without time zone": return 1083;
            case "interval": return 1186;
            case "uuid": return 2950;
            case "json": return 114;
            case "jsonb": return 3802;
            case "bytea": return 17;
            case "void": return 2278;
            case "record": return 2249;
            case "trigger": return 2279;
            default: break;
        }
        // Try matching against DataType enum
        for (DataType dt : DataType.values()) {
            if (dt.getPgName().equalsIgnoreCase(lower) || dt.name().equalsIgnoreCase(lower)) {
                return dt.getOid();
            }
        }
        // A user-defined type: a composite attribute's declared type, a function's argument or
        // return type. It answers under the schema that holds it.
        String userKey = TypeNamespace.oidKeyFor(database, lower);
        return userKey == null ? 0 : oids.oid(userKey);
    }

    /** PG marks anything in a pg_temp namespace as temporary, ahead of unlogged. */
    /**
     * What {@code relhasrules} reports. PostgreSQL documents it as "has (or once had) rules" and
     * only clears the flag at VACUUM, so a relation whose rules have all been dropped still
     * answers true.
     */
    private boolean hasRules(String schemaName, String relName) {
        return database.everHadRules(schemaName, relName);
    }

    private static String relPersistence(String schemaName, boolean unlogged) {
        if (schemaName != null && schemaName.toLowerCase().startsWith("pg_temp")) return "t";
        return unlogged ? "u" : "p";
    }

}
