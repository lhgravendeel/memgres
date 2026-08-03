package com.memgres.engine;

/**
 * The bootstrap types PostgreSQL's own catalogs are built out of.
 *
 * <p>These are not types a table column can usefully be declared as, so {@link DataType} does not
 * model most of them — but the catalogs point at them constantly: pg_cast names regprocedure as a
 * source, pg_proc gives a handler function a return type of index_am_handler, pg_index types an
 * expression column pg_node_tree. A reference to a type that has no pg_type row silently drops
 * every row of the join that reads it, so each one that is referenced is registered here.
 *
 * <p>Columns: oid, typname, typlen, typbyval, typtype, typcategory, typalign, typstorage,
 * typelem, typarray, the prefix its I/O functions are named with, and whether it has binary I/O
 * at all — the same values PostgreSQL 18 reports.
 *
 * <p>The I/O prefix is recorded rather than derived because PostgreSQL does not name these
 * functions to one rule. Most of the reg* family and the three bootstrap types with short names
 * run the suffix straight on — {@code charin}, {@code tidin}, {@code xid8in}, {@code regrolein} —
 * while the longer names take an underscore, {@code pg_lsn_in} and {@code jsonpath_in}; and the
 * two BRIN summary types drop the {@code pg_} their type name carries, so
 * {@code pg_brin_bloom_summary}'s input function is {@code brin_bloom_summary_in}. Appending
 * {@code "_in"} to the type name gave fifty-six names PostgreSQL has nowhere, every one of them a
 * function a client reading pg_proc would have found and could never have called.
 */
final class PgInternalTypes {

    private PgInternalTypes() {
    }

    static final Object[][] TYPES = {
            {18, "char", 1, true, "b", "Z", "c", "p", 0, 1002, "char", true},
            {27, "tid", 6, false, "b", "U", "s", "p", 0, 1010, "tid", true},
            {29, "cid", 4, true, "b", "U", "i", "p", 0, 1012, "cid", true},
            {32, "pg_ddl_command", 8, true, "p", "P", "d", "p", 0, 0, "pg_ddl_command_", true},
            {194, "pg_node_tree", -1, false, "b", "Z", "i", "x", 0, 0, "pg_node_tree_", true},
            {269, "table_am_handler", 4, true, "p", "P", "i", "p", 0, 0, "table_am_handler_", false},
            {325, "index_am_handler", 4, true, "p", "P", "i", "p", 0, 0, "index_am_handler_", false},
            {2202, "regprocedure", 4, true, "b", "N", "i", "p", 0, 2207, "regprocedure", true},
            {2203, "regoper", 4, true, "b", "N", "i", "p", 0, 2208, "regoper", true},
            {2204, "regoperator", 4, true, "b", "N", "i", "p", 0, 2209, "regoperator", true},
            {2280, "language_handler", 4, true, "p", "P", "i", "p", 0, 0, "language_handler_", false},
            {2970, "txid_snapshot", -1, false, "b", "U", "d", "x", 0, 2949, "txid_snapshot_", true},
            {3115, "fdw_handler", 4, true, "p", "P", "i", "p", 0, 0, "fdw_handler_", false},
            {3220, "pg_lsn", 8, true, "b", "U", "d", "p", 0, 3221, "pg_lsn_", true},
            {3310, "tsm_handler", 4, true, "p", "P", "i", "p", 0, 0, "tsm_handler_", false},
            {3361, "pg_ndistinct", -1, false, "b", "Z", "i", "x", 0, 0, "pg_ndistinct_", true},
            {3402, "pg_dependencies", -1, false, "b", "Z", "i", "x", 0, 0, "pg_dependencies_", true},
            {4072, "jsonpath", -1, false, "b", "U", "i", "x", 0, 4073, "jsonpath_", true},
            {3734, "regconfig", 4, true, "b", "N", "i", "p", 0, 3735, "regconfig", true},
            {3769, "regdictionary", 4, true, "b", "N", "i", "p", 0, 3770, "regdictionary", true},
            {4089, "regnamespace", 4, true, "b", "N", "i", "p", 0, 4090, "regnamespace", true},
            {4096, "regrole", 4, true, "b", "N", "i", "p", 0, 4097, "regrole", true},
            {4191, "regcollation", 4, true, "b", "N", "i", "p", 0, 4192, "regcollation", true},
            {4600, "pg_brin_bloom_summary", -1, false, "b", "Z", "i", "x", 0, 0,
                    "brin_bloom_summary_", true},
            {4601, "pg_brin_minmax_multi_summary", -1, false, "b", "Z", "i", "x", 0, 0,
                    "brin_minmax_multi_summary_", true},
            {5017, "pg_mcv_list", -1, false, "b", "Z", "i", "x", 0, 0, "pg_mcv_list_", true},
            {5038, "pg_snapshot", -1, false, "b", "U", "d", "x", 0, 5039, "pg_snapshot_", true},
            {5069, "xid8", 8, true, "b", "U", "d", "p", 0, 271, "xid8", true},
    };

    /**
     * The array types the bootstrap types point at through typarray.
     *
     * <p>A typarray naming a type with no pg_type row is the same dangling reference the array
     * types were registered to remove: a client following it — pgjdbc's TypeInfoCache does,
     * to decide whether a column can be read as a java.sql.Array — resolves it to nothing.
     *
     * <p>Columns: oid, typname, typelem, typalign.
     */
    static final Object[][] ARRAYS = {
            {271, "_xid8", 5069, "d"},
            {1010, "_tid", 27, "i"},
            {1012, "_cid", 29, "i"},
            {2207, "_regprocedure", 2202, "i"},
            {2208, "_regoper", 2203, "i"},
            {2209, "_regoperator", 2204, "i"},
            {2949, "_txid_snapshot", 2970, "d"},
            {3221, "_pg_lsn", 3220, "d"},
            {3735, "_regconfig", 3734, "i"},
            {3770, "_regdictionary", 3769, "i"},
            {4073, "_jsonpath", 4072, "i"},
            {4090, "_regnamespace", 4089, "i"},
            {4097, "_regrole", 4096, "i"},
            {4192, "_regcollation", 4191, "i"},
            {5039, "_pg_snapshot", 5038, "d"},
    };

    /** Whether one of these bootstrap types is named that. */
    static boolean holds(String typname) {
        if (typname == null) return false;
        for (Object[] t : TYPES) {
            if (((String) t[1]).equalsIgnoreCase(typname)) return true;
        }
        return false;
    }

    /**
     * The name PostgreSQL prints for one of these OIDs, or null when it is not one of them.
     * An array type prints as its element type followed by {@code []}, the way regtype does.
     */
    static String nameForOid(int oid) {
        for (Object[] t : TYPES) {
            if (((Integer) t[0]).intValue() == oid) return (String) t[1];
        }
        for (Object[] a : ARRAYS) {
            if (((Integer) a[0]).intValue() == oid) {
                String elem = nameForOid(((Integer) a[2]).intValue());
                return elem == null ? (String) a[1] : elem + "[]";
            }
        }
        return null;
    }
}
