package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds pg_catalog virtual tables related to the type system, casting,
 * operators, languages, and extensions.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogTypeSystemBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogTypeSystemBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    Table buildPgCollation() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("collname", DataType.NAME),
                colNN("collnamespace", DataType.OID),
                colNN("collowner", DataType.OID),
                colNN("collprovider", DataType.INTERNAL_CHAR),
                col("collisdeterministic", DataType.BOOLEAN),
                col("collencoding", DataType.INTEGER),
                col("colllocale", DataType.TEXT),
                col("collicurules", DataType.TEXT),
                col("collcollate", DataType.TEXT),
                col("collctype", DataType.TEXT),
                col("collversion", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_collation", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        // The collations every PostgreSQL installation has. A name listed here that the real
        // server does not have is a hazard rather than clutter: a tool that reads pg_collation and
        // offers "C.UTF-8" or "en_US.utf8" produces SQL that fails there, and memgres itself
        // rejects those names in COLLATE, so they are not claimed here either.
        table.insertRow(new Object[]{ oids.oid("collation:default"), "default", pgCatalogNs, 10, "d", true, -1, null, null, null, null, null, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:C"), "C", pgCatalogNs, 10, "c", true, -1, null, null, "C", "C", null, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:POSIX"), "POSIX", pgCatalogNs, 10, "c", true, -1, null, null, "POSIX", "POSIX", null, 1 });
        // The three builtin-provider collations PG 18 ships. A builtin collation states its locale
        // in colllocale and leaves collcollate and collctype null — those two are the libc
        // provider's columns, and reporting them filled in tells a client this is a libc
        // collation, which is what pg_dump and \dO decide on.
        table.insertRow(new Object[]{ oids.oid("collation:ucs_basic"), "ucs_basic", pgCatalogNs, 10, "b", true, 6, "C", null, null, null, "1", 1 });
        table.insertRow(new Object[]{ oids.oid("collation:pg_c_utf8"), "pg_c_utf8", pgCatalogNs, 10, "b", true, 6, "C.UTF-8", null, null, null, "1", 1 });
        // The ICU root collation PG registers under the plain name "unicode".
        table.insertRow(new Object[]{ oids.oid("collation:unicode"), "unicode", pgCatalogNs, 10, "i", true, -1, "und", null, null, null, null, 1 });
        String javaColl = "java-" + System.getProperty("java.version", "17");
        // ICU-provider collations. PG spells an ICU locale with a hyphen — en-US, not en_US —
        // and that spelling is what a client passes back to COLLATE.
        table.insertRow(new Object[]{ oids.oid("collation:und-x-icu"), "und-x-icu", pgCatalogNs, 10, "i", true, -1, "und", null, null, null, javaColl, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:en-US-x-icu"), "en-US-x-icu", pgCatalogNs, 10, "i", true, -1, "en-US", null, null, null, javaColl, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:en-x-icu"), "en-x-icu", pgCatalogNs, 10, "i", true, -1, "en", null, null, null, javaColl, 1 });
        // en_US is a name PostgreSQL does have; what it does not have is the ".UTF-8" spellings.
        // Removing the name along with them took away a collation COLLATE "en_US" accepts.
        table.insertRow(new Object[]{ oids.oid("collation:en_US"), "en_US", pgCatalogNs, 10, "i", true, 6, "en-US", null, "en-US", "en-US", javaColl, 1 });
        // User-defined collations (from CREATE COLLATION)
        for (java.util.Map.Entry<String, Database.CollationDef> entry : database.getUserCollations().entrySet()) {
            Database.CollationDef coll = entry.getValue();
            int publicNs = oids.oid("ns:public");
            table.insertRow(new Object[]{
                    oids.oid("collation:" + coll.name), coll.name, publicNs, 10, coll.provider,
                    coll.deterministic, 6, coll.locale, null,
                    coll.lcCollate != null ? coll.lcCollate : coll.locale,
                    coll.lcCtype != null ? coll.lcCtype : coll.locale,
                    javaColl, 1
            });
        }
        return table;
    }

    Table buildPgRange() {
        List<Column> cols = Cols.listOf(
                colNN("rngtypid", DataType.OID),
                col("rngsubtype", DataType.OID),
                col("rngmultitypid", DataType.OID),
                col("rngcollation", DataType.OID),
                col("rngsubopc", DataType.OID),
                col("rngcanonical", DataType.REGPROC),
                col("rngsubdiff", DataType.REGPROC)
        );
        Table table = new Table("pg_range", cols);
        // PG built-in range types: rngtypid, rngsubtype, rngmultitypid, rngcollation, rngsubopc, rngcanonical, rngsubdiff
        table.insertRow(new Object[]{3904, 23,   4451, 0, 0, 0, 0}); // int4range   → int4,      int4multirange
        table.insertRow(new Object[]{3906, 1700, 4532, 0, 0, 0, 0}); // numrange    → numeric,   nummultirange
        table.insertRow(new Object[]{3908, 1114, 4533, 0, 0, 0, 0}); // tsrange     → timestamp, tsmultirange
        table.insertRow(new Object[]{3910, 1184, 4534, 0, 0, 0, 0}); // tstzrange   → timestamptz, tstzmultirange
        table.insertRow(new Object[]{3912, 1082, 4535, 0, 0, 0, 0}); // daterange   → date,      datemultirange
        table.insertRow(new Object[]{3926, 20,   4536, 0, 0, 0, 0}); // int8range   → int8,      int8multirange

        // User-defined range types
        for (Map.Entry<String, String> entry : database.getRangeTypes().entrySet()) {
            String rangeName = entry.getKey();
            String subtypeName = entry.getValue();
            int rangeTypeOid = oids.oid("type:" + rangeName);
            int subtypeOid = resolveTypeOid(subtypeName);
            table.insertRow(new Object[]{rangeTypeOid, subtypeOid, 0, 0, 0, 0, 0});
        }
        return table;
    }

    Table buildPgExtension() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("extname", DataType.TEXT),
                col("extowner", DataType.INTEGER),
                col("extnamespace", DataType.INTEGER),
                col("extrelocatable", DataType.BOOLEAN),
                col("extversion", DataType.TEXT),
                col("extconfig", DataType.TEXT),
                col("extcondition", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        // Populate with plpgsql extension (always present in PG)
        Table table = new Table("pg_extension", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{oids.oid("ext:plpgsql"), "plpgsql", 10, pgCatalogNs, false, "1.0", null, null, 1});
        // Add user-installed extensions (skip plpgsql, already added above)
        for (java.util.Map.Entry<String, String> entry : database.getInstalledExtensions().entrySet()) {
            String extName = entry.getKey();
            if ("plpgsql".equalsIgnoreCase(extName)) continue;
            String extVersion = entry.getValue();
            String extSchema = database.getExtensionSchema(extName);
            int extNs = (extSchema != null) ? oids.oid("ns:" + extSchema) : pgCatalogNs;
            table.insertRow(new Object[]{oids.oid("ext:" + extName), extName, 10, extNs, false, extVersion, null, null, 1});
        }
        return table;
    }

    Table buildPgLanguage() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("lanname", DataType.TEXT),
                col("lanowner", DataType.INTEGER), col("lanispl", DataType.BOOLEAN),
                col("lanpltrusted", DataType.BOOLEAN), col("lanplcallfoid", DataType.INTEGER),
                col("laninline", DataType.INTEGER), col("lanvalidator", DataType.INTEGER),
                col("lanacl", DataType.ACLITEM_ARRAY), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_language", cols);
        // Language handler/validator/inline function OIDs (must match entries in pg_proc)
        int fmgrInternalValidator = oids.oid("proc:fmgr_internal_validator");
        int fmgrCValidator = oids.oid("proc:fmgr_c_validator");
        int fmgrSqlValidator = oids.oid("proc:fmgr_sql_validator");
        int plpgsqlCallHandler = oids.oid("proc:plpgsql_call_handler");
        int plpgsqlInlineHandler = oids.oid("proc:plpgsql_inline_handler");
        int plpgsqlValidator = oids.oid("proc:plpgsql_validator");
        table.insertRow(new Object[]{oids.oid("lang:internal"), "internal", 10, false, false, 0, 0, fmgrInternalValidator, null, 1});
        table.insertRow(new Object[]{oids.oid("lang:c"), "c", 10, false, false, 0, 0, fmgrCValidator, null, 1});
        table.insertRow(new Object[]{oids.oid("lang:sql"), "sql", 10, false, true, 0, 0, fmgrSqlValidator, null, 1});
        table.insertRow(new Object[]{oids.oid("lang:plpgsql"), "plpgsql", 10, true, true, plpgsqlCallHandler, plpgsqlInlineHandler, plpgsqlValidator, null, 1});
        return table;
    }

    Table buildPgCast() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("castsource", DataType.OID),
                colNN("casttarget", DataType.OID), col("castfunc", DataType.OID),
                col("castcontext", DataType.INTERNAL_CHAR),
                col("castmethod", DataType.INTERNAL_CHAR),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_cast", cols);
        // Exactly the conversions PostgreSQL registers between the types memgres models. A row
        // here is a claim the server will perform the conversion, and an implicit row changes
        // what the server accepts without being asked, so nothing is added on a guess. castfunc
        // is resolved through the same name pg_proc registers the function under, so the join
        // from pg_cast to pg_proc — the normal way to read the table — lands on a row.
        int castOid = 5000;
        for (Object[] c : PgCastTable.CASTS) {
            String fname = (String) c[2];
            int castfunc = fname.isEmpty() ? 0 : oids.oid("proc:" + fname);
            table.insertRow(new Object[]{castOid++, c[0], c[1], castfunc, c[3], c[4], 1});
        }

        // User-defined casts (CREATE CAST)
        for (Object[] uc : database.getUserDefinedCasts()) {
            table.insertRow(new Object[]{castOid++, uc[0], uc[1], uc[2], uc[3], uc[4], 1});
        }

        return table;
    }


    Table buildPgOperator() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("oprname", DataType.NAME),
                col("oprnamespace", DataType.OID), col("oprowner", DataType.OID),
                col("oprkind", DataType.INTERNAL_CHAR),
                col("oprcanmerge", DataType.BOOLEAN), col("oprcanhash", DataType.BOOLEAN),
                col("oprleft", DataType.OID), col("oprright", DataType.OID),
                col("oprresult", DataType.OID),
                col("oprcom", DataType.OID), col("oprnegate", DataType.OID),
                col("oprcode", DataType.REGPROC), col("oprrest", DataType.REGPROC),
                col("oprjoin", DataType.REGPROC), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_operator", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        int publicNs = oids.oid("ns:public");

        // Built-in operators, one row per operand-type combination PostgreSQL registers. A row
        // is keyed by its full signature rather than by its spelling, so the several operators
        // that share a name stay distinguishable and each carries a result type.
        for (Object[] op : PgOperatorTable.OPERATORS) {
            table.insertRow(new Object[]{
                    builtinOperatorOid(op), op[0], pgCatalogNs, 10,
                    op[1], op[6], op[7],
                    op[2], op[3], op[4],
                    signatureOid((String) op[8]), signatureOid((String) op[9]),
                    // A regproc value, so oprcode::text answers the function's name the way
                    // PostgreSQL does rather than handing the reader back an OID.
                    new RegprocValue(oids.oid("proc:" + op[5]), (String) op[5]),
                    new RegprocValue(0, "-"), new RegprocValue(0, "-"), 1});
        }

        // User-defined operators
        for (Map.Entry<String, PgOperator> entry : database.getUserOperators().entrySet()) {
            PgOperator op = entry.getValue();
            String schemaName = op.getSchemaName() != null ? op.getSchemaName() : "public";
            int ns = "pg_catalog".equals(schemaName) ? pgCatalogNs : oids.oid("ns:" + schemaName);
            int ownerOid = op.getOwner() != null ? oids.oid("role:" + op.getOwner()) : 10;
            int opOid = oids.oid("operator:" + schemaName + "." + op.getKey());
            int leftOid = resolveTypeOid(op.getLeftArg());
            int rightOid = resolveTypeOid(op.getRightArg());
            int comOid = 0;
            if (op.getCommutator() != null) {
                // Self-referencing commutator: use own OID
                if (op.getCommutator().equals(op.getName())) {
                    comOid = opOid;
                }
            }
            // Resolve oprcode (backing function OID) and oprresult (return type OID)
            int opcodeOid = 0;
            int resultOid = 0;
            if (op.getFunction() != null) {
                PgFunction func = database.getFunction(op.getFunction());
                if (func != null) {
                    opcodeOid = oids.oid("func:" + op.getFunction().toLowerCase());
                    if (func.getReturnType() != null) {
                        resultOid = resolveTypeOid(func.getReturnType());
                    }
                }
            }
            // Resolve oprnegate (negator operator OID)
            int negOid = 0;
            if (op.getNegator() != null) {
                if (op.getNegator().equals(op.getName())) {
                    negOid = opOid; // Self-referencing negator
                } else {
                    // Try to find the negator operator
                    for (Map.Entry<String, PgOperator> negEntry : database.getUserOperators().entrySet()) {
                        if (negEntry.getValue().getName().equals(op.getNegator())) {
                            String negSchema = negEntry.getValue().getSchemaName() != null ? negEntry.getValue().getSchemaName() : "public";
                            negOid = oids.oid("operator:" + negSchema + "." + negEntry.getValue().getKey());
                            break;
                        }
                    }
                }
            }
            table.insertRow(new Object[]{opOid, op.getName(), ns, ownerOid,
                    op.getKind(), op.isMerges(), op.isHashes(),
                    leftOid, rightOid, resultOid, comOid, negOid,
                    opcodeOid, 0, 0, 1});
        }

        return table;
    }

    /** The OID of a built-in operator, keyed by the signature that identifies it. */
    private int builtinOperatorOid(Object[] op) {
        return oids.oid("operator:pg_catalog." + op[0] + " " + op[2] + " " + op[3]);
    }

    /** Resolve an "name left right" reference to another built-in operator; 0 when there is none. */
    private int signatureOid(String signature) {
        if (signature == null || signature.isEmpty()) return 0;
        return oids.oid("operator:pg_catalog." + signature);
    }

    /** The OID of the built-in operator with this name and operand types, or 0. */
    int operatorOid(String name, int left, int right) {
        for (Object[] op : PgOperatorTable.OPERATORS) {
            if (op[0].equals(name) && ((Integer) op[2]) == left && ((Integer) op[3]) == right) {
                return builtinOperatorOid(op);
            }
        }
        return 0;
    }

    /**
     * Check if an operator name is invalid in PostgreSQL.
     * PG rule: multi-character operators ending with + or - must also contain
     * at least one character from ~!@#%^&amp;|`?\
     */
    private static boolean isInvalidPgOperatorName(String name) {
        char last = name.charAt(name.length() - 1);
        if (last != '+' && last != '-') return false;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if ("~!@#%^&|`?\\".indexOf(c) >= 0) return false;
        }
        return true;
    }

    private int resolveTypeOid(String typeName) {
        if (typeName == null) return 0;
        try {
            DataType dt = DataType.fromPgName(typeName);
            if (dt != null) return dt.getOid();
        } catch (Exception ignored) {}
        return oids.oid("type:" + typeName.toLowerCase());
    }

    private int resolveAccessMethodOid(String method) {
        if (method == null) return 0;
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

    /**
     * The core btree and hash operator classes PostgreSQL 18 ships that are not written out one by
     * one below. Read off a PG 18 pg_opclass, restricted to the pg_catalog namespace so nothing a
     * contrib extension supplies is claimed here.
     *
     * <p>Columns: opcname, access method, opcintype, opcdefault, opckeytype, opfname.
     */
    private static final Object[][] CORE_OPCLASSES = {
            // btree
            {"array_ops", "btree", 2277, true, 0, "array_ops"},
            {"bit_ops", "btree", 1560, true, 0, "bit_ops"},
            {"bpchar_ops", "btree", 1042, true, 0, "bpchar_ops"},
            {"bpchar_pattern_ops", "btree", 1042, false, 0, "bpchar_pattern_ops"},
            {"bytea_ops", "btree", 17, true, 0, "bytea_ops"},
            {"char_ops", "btree", 18, true, 0, "char_ops"},
            {"cidr_ops", "btree", 869, false, 0, "network_ops"},
            {"enum_ops", "btree", 3500, true, 0, "enum_ops"},
            {"inet_ops", "btree", 869, true, 0, "network_ops"},
            {"interval_ops", "btree", 1186, true, 0, "interval_ops"},
            {"jsonb_ops", "btree", 3802, true, 0, "jsonb_ops"},
            {"macaddr8_ops", "btree", 774, true, 0, "macaddr8_ops"},
            {"macaddr_ops", "btree", 829, true, 0, "macaddr_ops"},
            {"money_ops", "btree", 790, true, 0, "money_ops"},
            {"multirange_ops", "btree", 4537, true, 0, "multirange_ops"},
            // name_ops sorts as cstring does, which is what opckeytype records.
            {"name_ops", "btree", 19, true, 2275, "text_ops"},
            {"oid_ops", "btree", 26, true, 0, "oid_ops"},
            {"oidvector_ops", "btree", 30, true, 0, "oidvector_ops"},
            {"pg_lsn_ops", "btree", 3220, true, 0, "pg_lsn_ops"},
            {"range_ops", "btree", 3831, true, 0, "range_ops"},
            {"record_image_ops", "btree", 2249, false, 0, "record_image_ops"},
            {"record_ops", "btree", 2249, true, 0, "record_ops"},
            {"tid_ops", "btree", 27, true, 0, "tid_ops"},
            {"time_ops", "btree", 1083, true, 0, "time_ops"},
            {"timetz_ops", "btree", 1266, true, 0, "timetz_ops"},
            {"tsquery_ops", "btree", 3615, true, 0, "tsquery_ops"},
            {"tsvector_ops", "btree", 3614, true, 0, "tsvector_ops"},
            {"varbit_ops", "btree", 1562, true, 0, "varbit_ops"},
            {"xid8_ops", "btree", 5069, true, 0, "xid8_ops"},
            // hash
            {"aclitem_ops", "hash", 1033, true, 0, "aclitem_ops"},
            {"array_ops", "hash", 2277, true, 0, "array_ops"},
            {"bpchar_ops", "hash", 1042, true, 0, "bpchar_ops"},
            {"bpchar_pattern_ops", "hash", 1042, false, 0, "bpchar_pattern_ops"},
            {"bytea_ops", "hash", 17, true, 0, "bytea_ops"},
            {"char_ops", "hash", 18, true, 0, "char_ops"},
            {"cid_ops", "hash", 29, true, 0, "cid_ops"},
            {"cidr_ops", "hash", 869, false, 0, "network_ops"},
            {"date_ops", "hash", 1082, true, 0, "date_ops"},
            {"enum_ops", "hash", 3500, true, 0, "enum_ops"},
            {"float4_ops", "hash", 700, true, 0, "float_ops"},
            {"float8_ops", "hash", 701, true, 0, "float_ops"},
            {"inet_ops", "hash", 869, true, 0, "network_ops"},
            {"int2_ops", "hash", 21, true, 0, "integer_ops"},
            {"int8_ops", "hash", 20, true, 0, "integer_ops"},
            {"interval_ops", "hash", 1186, true, 0, "interval_ops"},
            {"jsonb_ops", "hash", 3802, true, 0, "jsonb_ops"},
            {"macaddr8_ops", "hash", 774, true, 0, "macaddr8_ops"},
            {"macaddr_ops", "hash", 829, true, 0, "macaddr_ops"},
            {"multirange_ops", "hash", 4537, true, 0, "multirange_ops"},
            {"name_ops", "hash", 19, true, 0, "text_ops"},
            {"numeric_ops", "hash", 1700, true, 0, "numeric_ops"},
            {"oid_ops", "hash", 26, true, 0, "oid_ops"},
            {"oidvector_ops", "hash", 30, true, 0, "oidvector_ops"},
            {"pg_lsn_ops", "hash", 3220, true, 0, "pg_lsn_ops"},
            {"range_ops", "hash", 3831, true, 0, "range_ops"},
            {"record_ops", "hash", 2249, true, 0, "record_ops"},
            {"text_pattern_ops", "hash", 25, false, 0, "text_pattern_ops"},
            {"tid_ops", "hash", 27, true, 0, "tid_ops"},
            {"time_ops", "hash", 1083, true, 0, "time_ops"},
            {"timestamp_ops", "hash", 1114, true, 0, "timestamp_ops"},
            {"timestamptz_ops", "hash", 1184, true, 0, "timestamptz_ops"},
            {"timetz_ops", "hash", 1266, true, 0, "timetz_ops"},
            {"uuid_ops", "hash", 2950, true, 0, "uuid_ops"},
            {"varchar_pattern_ops", "hash", 25, false, 0, "text_pattern_ops"},
            {"xid8_ops", "hash", 5069, true, 0, "xid8_ops"},
            {"xid_ops", "hash", 28, true, 0, "xid_ops"},
    };

    /** The families {@link #CORE_OPCLASSES} belongs to: opfname, access method. */
    private static final Object[][] CORE_OPFAMILIES = {
            {"array_ops", "btree"}, {"bit_ops", "btree"}, {"bpchar_ops", "btree"},
            {"bpchar_pattern_ops", "btree"}, {"bytea_ops", "btree"}, {"char_ops", "btree"},
            {"enum_ops", "btree"}, {"interval_ops", "btree"}, {"jsonb_ops", "btree"},
            {"macaddr8_ops", "btree"}, {"macaddr_ops", "btree"}, {"money_ops", "btree"},
            {"multirange_ops", "btree"}, {"network_ops", "btree"}, {"oid_ops", "btree"},
            {"oidvector_ops", "btree"}, {"pg_lsn_ops", "btree"}, {"range_ops", "btree"},
            {"record_image_ops", "btree"}, {"record_ops", "btree"}, {"tid_ops", "btree"},
            {"time_ops", "btree"}, {"timetz_ops", "btree"}, {"tsquery_ops", "btree"},
            {"tsvector_ops", "btree"}, {"varbit_ops", "btree"}, {"xid8_ops", "btree"},
            {"aclitem_ops", "hash"}, {"array_ops", "hash"}, {"bpchar_ops", "hash"},
            {"bpchar_pattern_ops", "hash"}, {"bytea_ops", "hash"}, {"char_ops", "hash"},
            {"cid_ops", "hash"}, {"date_ops", "hash"}, {"enum_ops", "hash"},
            {"float_ops", "hash"}, {"interval_ops", "hash"}, {"jsonb_ops", "hash"},
            {"macaddr8_ops", "hash"}, {"macaddr_ops", "hash"}, {"multirange_ops", "hash"},
            {"network_ops", "hash"}, {"numeric_ops", "hash"}, {"oid_ops", "hash"},
            {"oidvector_ops", "hash"}, {"pg_lsn_ops", "hash"}, {"range_ops", "hash"},
            {"record_ops", "hash"}, {"text_pattern_ops", "hash"}, {"tid_ops", "hash"},
            {"time_ops", "hash"}, {"timestamp_ops", "hash"}, {"timestamptz_ops", "hash"},
            {"timetz_ops", "hash"}, {"uuid_ops", "hash"}, {"xid8_ops", "hash"},
            {"xid_ops", "hash"},
    };

    Table buildPgOpclass() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("opcname", DataType.NAME),
                col("opcnamespace", DataType.OID), col("opcowner", DataType.OID),
                col("opcfamily", DataType.OID), col("opcintype", DataType.OID),
                col("opckeytype", DataType.OID), col("opcdefault", DataType.BOOLEAN),
                col("opcmethod", DataType.OID), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_opclass", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        // Standard btree operator classes (one per data type family)
        table.insertRow(new Object[]{403, "int4_ops", pgCatalogNs, 10,
                oids.oid("opfamily:integer_ops"), DataType.INTEGER.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:text_ops"), "text_ops", pgCatalogNs, 10,
                oids.oid("opfamily:text_ops"), DataType.TEXT.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:bool_ops"), "bool_ops", pgCatalogNs, 10,
                oids.oid("opfamily:bool_ops"), DataType.BOOLEAN.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:int8_ops"), "int8_ops", pgCatalogNs, 10,
                oids.oid("opfamily:integer_ops"), DataType.BIGINT.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:int2_ops"), "int2_ops", pgCatalogNs, 10,
                oids.oid("opfamily:integer_ops"), DataType.SMALLINT.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:float4_ops"), "float4_ops", pgCatalogNs, 10,
                oids.oid("opfamily:float_ops"), DataType.REAL.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:float8_ops"), "float8_ops", pgCatalogNs, 10,
                oids.oid("opfamily:float_ops"), DataType.DOUBLE_PRECISION.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:numeric_ops"), "numeric_ops", pgCatalogNs, 10,
                oids.oid("opfamily:numeric_ops"), DataType.NUMERIC.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:date_ops"), "date_ops", pgCatalogNs, 10,
                oids.oid("opfamily:datetime_ops"), DataType.DATE.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:timestamp_ops"), "timestamp_ops", pgCatalogNs, 10,
                oids.oid("opfamily:datetime_ops"), DataType.TIMESTAMP.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:timestamptz_ops"), "timestamptz_ops", pgCatalogNs, 10,
                oids.oid("opfamily:datetime_ops"), DataType.TIMESTAMPTZ.getOid(), 0, true, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:uuid_ops"), "uuid_ops", pgCatalogNs, 10,
                oids.oid("opfamily:uuid_ops"), DataType.UUID.getOid(), 0, true, 403, 1});
        // varchar_ops is an alias for text_ops, and PG does *not* mark it the default: the
        // default btree class for varchar is text_ops itself. PG keys it on text rather than
        // on varchar — a client resolving "which class handles this type" reads opcintype.
        table.insertRow(new Object[]{oids.oid("opclass:varchar_ops"), "varchar_ops", pgCatalogNs, 10,
                oids.oid("opfamily:text_ops"), DataType.TEXT.getOid(), 0, false, 403, 1});
        table.insertRow(new Object[]{oids.oid("opclass:hash_varchar_ops"), "varchar_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_text_ops"), DataType.TEXT.getOid(), 0, false, 405, 1});
        // Hash operator classes (same names as btree; this is correct PG behavior)
        table.insertRow(new Object[]{oids.oid("opclass:hash_int4_ops"), "int4_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_integer_ops"), DataType.INTEGER.getOid(), 0, true, 405, 1});
        table.insertRow(new Object[]{oids.oid("opclass:hash_text_ops"), "text_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_text_ops"), DataType.TEXT.getOid(), 0, true, 405, 1});
        table.insertRow(new Object[]{oids.oid("opclass:hash_bool_ops"), "bool_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_bool_ops"), DataType.BOOLEAN.getOid(), 0, true, 405, 1});

        // text_pattern_ops (btree, non-default for text)
        table.insertRow(new Object[]{oids.oid("opclass:text_pattern_ops"), "text_pattern_ops", pgCatalogNs, 10,
                oids.oid("opfamily:text_pattern_ops"), DataType.TEXT.getOid(), 0, false, 403, 1});
        // varchar_pattern_ops (btree, non-default). Keyed on text rather than varchar, the same way
        // varchar_ops is: PG has one set of comparison operators and both classes name text.
        table.insertRow(new Object[]{oids.oid("opclass:varchar_pattern_ops"), "varchar_pattern_ops", pgCatalogNs, 10,
                oids.oid("opfamily:text_pattern_ops"), DataType.TEXT.getOid(), 0, false, 403, 1});

        // The rest of the operator classes PostgreSQL itself ships for btree and hash. Every one
        // of these is a core class, not something a contrib extension supplies, and code that asks
        // "which class indexes this type" — CREATE INDEX validation, pg_dump, an ORM choosing an
        // index — reads pg_opclass to find out. Without a row the answer is that the type cannot
        // be indexed at all.
        for (Object[] c : CORE_OPCLASSES) {
            String amName = (String) c[1];
            boolean btree = "btree".equals(amName);
            String keyPrefix = btree ? "opclass:" : "opclass:" + amName + "_";
            String famPrefix = btree ? "opfamily:" : "opfamily:" + amName + "_";
            table.insertRow(new Object[]{
                    oids.oid(keyPrefix + c[0]), c[0], pgCatalogNs, 10,
                    oids.oid(famPrefix + c[5]), c[2], c[4], c[3],
                    resolveAccessMethodOid(amName), 1});
        }

        // GIN operator classes
        table.insertRow(new Object[]{oids.oid("opclass:gin_tsvector_ops"), "tsvector_ops", pgCatalogNs, 10,
                oids.oid("opfamily:gin_tsvector_ops"), DataType.TSVECTOR.getOid(), 0, true, 2742, 1});
        table.insertRow(new Object[]{oids.oid("opclass:gin_jsonb_ops"), "jsonb_ops", pgCatalogNs, 10,
                oids.oid("opfamily:gin_jsonb_ops"), DataType.JSONB.getOid(), 0, true, 2742, 1});
        // GIN array_ops is polymorphic: PG types it anyarray, not "no type at all".
        table.insertRow(new Object[]{oids.oid("opclass:gin_array_ops"), "array_ops", pgCatalogNs, 10,
                oids.oid("opfamily:gin_array_ops"), DataType.ANYARRAY.getOid(), 0, true, 2742, 1});

        // GIST operator classes
        table.insertRow(new Object[]{oids.oid("opclass:gist_point_ops"), "point_ops", pgCatalogNs, 10,
                oids.oid("opfamily:gist_point_ops"), DataType.POINT.getOid(), 0, true, 783, 1});
        table.insertRow(new Object[]{oids.oid("opclass:gist_box_ops"), "box_ops", pgCatalogNs, 10,
                oids.oid("opfamily:gist_box_ops"), DataType.BOX.getOid(), 0, true, 783, 1});

        // User-defined operator classes
        int publicNsOpc = oids.oid("ns:public");
        for (Map.Entry<String, PgOperatorClass> entry : database.getUserOperatorClasses().entrySet()) {
            PgOperatorClass cls = entry.getValue();
            String schemaName = cls.getSchemaName() != null ? cls.getSchemaName() : "public";
            int ns = "pg_catalog".equals(schemaName) ? pgCatalogNs : oids.oid("ns:" + schemaName);
            int ownerOid = cls.getOwner() != null ? oids.oid("role:" + cls.getOwner()) : 10;
            int clsOid = oids.oid("opclass:" + cls.getKey());
            int typeOid = resolveTypeOid(cls.getForType());
            int methodOid = resolveAccessMethodOid(cls.getMethod());
            int familyOid = 0;
            if (cls.getFamilyName() != null) {
                String famKey = cls.getFamilyName().toLowerCase() + ":" + cls.getMethod().toLowerCase();
                familyOid = oids.oid("opfamily:" + famKey);
            }
            table.insertRow(new Object[]{clsOid, cls.getName(), ns, ownerOid,
                    familyOid, typeOid, 0, cls.isDefault(), methodOid, 1});
        }

        return table;
    }

    Table buildPgOpfamily() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("opfname", DataType.NAME),
                col("opfnamespace", DataType.OID), col("opfowner", DataType.OID),
                col("opfmethod", DataType.OID), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_opfamily", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        // Btree operator families
        table.insertRow(new Object[]{oids.oid("opfamily:integer_ops"), "integer_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:text_ops"), "text_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:bool_ops"), "bool_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:float_ops"), "float_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:numeric_ops"), "numeric_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:datetime_ops"), "datetime_ops", pgCatalogNs, 10, 403, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:uuid_ops"), "uuid_ops", pgCatalogNs, 10, 403, 1});
        // Hash operator families
        table.insertRow(new Object[]{oids.oid("opfamily:hash_integer_ops"), "integer_ops", pgCatalogNs, 10, 405, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:hash_text_ops"), "text_ops", pgCatalogNs, 10, 405, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:hash_bool_ops"), "bool_ops", pgCatalogNs, 10, 405, 1});
        // Pattern ops families
        table.insertRow(new Object[]{oids.oid("opfamily:text_pattern_ops"), "text_pattern_ops", pgCatalogNs, 10, 403, 1});
        // The families the classes above belong to. A class names its family, and every operator
        // strategy is recorded against the family rather than the class, so a family with no row
        // leaves each of its classes pointing at nothing.
        for (Object[] f : CORE_OPFAMILIES) {
            String amName = (String) f[1];
            String keyPrefix = "btree".equals(amName) ? "opfamily:" : "opfamily:" + amName + "_";
            table.insertRow(new Object[]{oids.oid(keyPrefix + f[0]), f[0], pgCatalogNs, 10,
                    resolveAccessMethodOid(amName), 1});
        }

        // GIN operator families
        table.insertRow(new Object[]{oids.oid("opfamily:gin_tsvector_ops"), "tsvector_ops", pgCatalogNs, 10, 2742, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:gin_jsonb_ops"), "jsonb_ops", pgCatalogNs, 10, 2742, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:gin_array_ops"), "array_ops", pgCatalogNs, 10, 2742, 1});
        // GIST operator families
        table.insertRow(new Object[]{oids.oid("opfamily:gist_point_ops"), "point_ops", pgCatalogNs, 10, 783, 1});
        table.insertRow(new Object[]{oids.oid("opfamily:gist_box_ops"), "box_ops", pgCatalogNs, 10, 783, 1});

        // User-defined operator families
        int publicNsOpf = oids.oid("ns:public");
        for (Map.Entry<String, PgOperatorFamily> entry : database.getUserOperatorFamilies().entrySet()) {
            PgOperatorFamily fam = entry.getValue();
            String schemaName = fam.getSchemaName() != null ? fam.getSchemaName() : "public";
            int ns = "pg_catalog".equals(schemaName) ? pgCatalogNs : oids.oid("ns:" + schemaName);
            int ownerOid = fam.getOwner() != null ? oids.oid("role:" + fam.getOwner()) : 10;
            int famOid = oids.oid("opfamily:" + fam.getKey());
            int methodOid = resolveAccessMethodOid(fam.getMethod());
            table.insertRow(new Object[]{famOid, fam.getName(), ns, ownerOid, methodOid, 1});
        }

        return table;
    }

    /**
     * pg_aggregate, in PostgreSQL's own column order and with one row per aggregate overload.
     *
     * <p>The row is what says an aggregate is an aggregate: every tool that asks what aggregates
     * a server has joins pg_proc to here on aggfnoid, and an overload with no row of its own
     * drops out of that join. Listing one row per name where pg_proc lists one per overload left
     * a hundred and fifteen of them unreachable.
     */
    Table buildPgAggregate() {
        List<Column> cols = Cols.listOf(
                colNN("aggfnoid", DataType.REGPROC), col("aggkind", DataType.INTERNAL_CHAR),
                col("aggnumdirectargs", DataType.SMALLINT),
                col("aggtransfn", DataType.REGPROC), col("aggfinalfn", DataType.REGPROC),
                col("aggcombinefn", DataType.REGPROC), col("aggserialfn", DataType.REGPROC),
                col("aggdeserialfn", DataType.REGPROC), col("aggmtransfn", DataType.REGPROC),
                col("aggminvtransfn", DataType.REGPROC), col("aggmfinalfn", DataType.REGPROC),
                col("aggfinalextra", DataType.BOOLEAN), col("aggmfinalextra", DataType.BOOLEAN),
                col("aggfinalmodify", DataType.INTERNAL_CHAR),
                col("aggmfinalmodify", DataType.INTERNAL_CHAR),
                col("aggsortop", DataType.OID), col("aggtranstype", DataType.OID),
                col("aggtransspace", DataType.INTEGER), col("aggmtranstype", DataType.OID),
                col("aggmtransspace", DataType.INTEGER),
                col("agginitval", DataType.TEXT), col("aggminitval", DataType.TEXT));
        Table table = new Table("pg_aggregate", cols);

        // One row per built-in overload, keyed the way pg_proc keys the same overload, so the
        // join on aggfnoid reaches every aggregate row pg_proc reports.
        Map<String, Integer> aggIndex = new java.util.HashMap<>();
        for (String[] agg : BuiltinAggregateSignatures.AGGREGATES) {
            String aggName = agg[0];
            int idx = aggIndex.merge(aggName, 0, (a, b) -> a + 1);
            String oidKey = idx == 0 ? "proc:" + aggName : "proc:" + aggName + "#agg" + idx;
            table.insertRow(builtinAggregateRow(
                    new RegprocValue(oids.oid(oidKey), aggName), Integer.parseInt(agg[1]), null));
        }

        // Populate with user-defined aggregates
        for (Map.Entry<String, PgAggregate> entry : database.getUserAggregates().entrySet()) {
            PgAggregate agg = entry.getValue();
            Object aggFn = new RegprocValue(oids.oid("proc:" + agg.getName()), agg.getName());
            Object sfuncVal = new RegprocValue(oids.oid("proc:" + agg.getSfunc()), agg.getSfunc());
            Object finalfuncVal = agg.getFinalfunc() != null
                    ? new RegprocValue(oids.oid("proc:" + agg.getFinalfunc()), agg.getFinalfunc())
                    : new RegprocValue(0, "-");
            Object combinefuncVal = agg.getCombinefunc() != null
                    ? new RegprocValue(oids.oid("proc:" + agg.getCombinefunc()), agg.getCombinefunc())
                    : new RegprocValue(0, "-");
            Object[] row = builtinAggregateRow(aggFn, resolveTypeOid(agg.getStype()), agg.getInitcond());
            row[3] = sfuncVal;      // aggtransfn
            row[4] = finalfuncVal;  // aggfinalfn
            row[5] = combinefuncVal; // aggcombinefn
            table.insertRow(row);
        }
        return table;
    }

    /** A pg_aggregate row with the support functions left unclaimed rather than invented. */
    private Object[] builtinAggregateRow(Object aggfnoid, int transType, String initval) {
        RegprocValue none = new RegprocValue(0, "-");
        return new Object[]{
                aggfnoid, "n", (short) 0,
                none, none, none, none, none, none, none, none,
                false, false, "r", "r",
                0, transType, 0, 0, 0,
                initval, null
        };
    }

    Table buildPgAmop() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), col("amopfamily", DataType.INTEGER),
                col("amoplefttype", DataType.INTEGER), col("amoprighttype", DataType.INTEGER),
                col("amopstrategy", DataType.SMALLINT), col("amoppurpose", DataType.CHAR),
                col("amopopr", DataType.INTEGER),
                col("amopmethod", DataType.INTEGER), col("amopsortfamily", DataType.INTEGER),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_amop", cols);
        int btreeAm = 403;
        // btree strategies 1-5, in PG's order: less, leq, eq, geq, gt. Each names the operator it
        // stands for, so a join from pg_amop to pg_operator -- the normal way to read this table
        // -- resolves instead of dropping every row.
        String[] btreeOps = {"<", "<=", "=", ">=", ">"};
        Object[][] families = {
                {"integer_ops", DataType.INTEGER.getOid()},
                {"text_ops", DataType.TEXT.getOid()},
        };
        for (Object[] fam : families) {
            String famName = (String) fam[0];
            int typeOid = (Integer) fam[1];
            int famOid = oids.oid("opfamily:" + famName);
            for (short strat = 1; strat <= 5; strat++) {
                table.insertRow(new Object[]{
                        oids.oid("amop:" + famName + ":" + strat), famOid,
                        typeOid, typeOid, strat, "s",
                        operatorOid(btreeOps[strat - 1], typeOid, typeOid),
                        btreeAm, 0, 1});
            }
        }
        return table;
    }

    Table buildPgAmproc() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), col("amprocfamily", DataType.INTEGER),
                col("amproclefttype", DataType.INTEGER), col("amprocrighttype", DataType.INTEGER),
                col("amprocnum", DataType.SMALLINT), col("amproc", DataType.INTEGER),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_amproc", cols);
        // PG 18 has no btree support procedures in pg_amproc (btree uses pg_amop instead).
        // Table is intentionally empty for now.
        return table;
    }
}
