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
        // The seven collations PG 18 pins into the catalogue, with the OIDs it pins them at:
        // these are compiled into the server rather than imported from the host's locales, so
        // they are the same seven numbers everywhere and a client may hold on to one.
        table.insertRow(new Object[]{ 100, "default", pgCatalogNs, 10, "d", true, -1, null, null, null, null, null, 1 });
        table.insertRow(new Object[]{ 950, "C", pgCatalogNs, 10, "c", true, -1, null, null, "C", "C", null, 1 });
        table.insertRow(new Object[]{ 951, "POSIX", pgCatalogNs, 10, "c", true, -1, null, null, "POSIX", "POSIX", null, 1 });
        // The three builtin-provider collations PG 18 ships. A builtin collation states its locale
        // in colllocale and leaves collcollate and collctype null — those two are the libc
        // provider's columns, and reporting them filled in tells a client this is a libc
        // collation, which is what pg_dump and \dO decide on.
        table.insertRow(new Object[]{ 962, "ucs_basic", pgCatalogNs, 10, "b", true, 6, "C", null, null, null, "1", 1 });
        table.insertRow(new Object[]{ 811, "pg_c_utf8", pgCatalogNs, 10, "b", true, 6, "C.UTF-8", null, null, null, "1", 1 });
        table.insertRow(new Object[]{ 6411, "pg_unicode_fast", pgCatalogNs, 10, "b", true, 6, "PG_UNICODE_FAST", null, null, null, "1", 1 });
        // The ICU root collation PG registers under the plain name "unicode".
        table.insertRow(new Object[]{ 963, "unicode", pgCatalogNs, 10, "i", true, -1, "und", null, null, null, null, 1 });
        String javaColl = "java-" + System.getProperty("java.version", "17");
        // ICU-provider collations. PG spells an ICU locale with a hyphen — en-US, not en_US —
        // and that spelling is what a client passes back to COLLATE. These are imported from the
        // host at initdb rather than pinned, so their OIDs are not fixed and are minted here.
        table.insertRow(new Object[]{ oids.oid("collation:und-x-icu"), "und-x-icu", pgCatalogNs, 10, "i", true, -1, "und", null, null, null, javaColl, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:en-US-x-icu"), "en-US-x-icu", pgCatalogNs, 10, "i", true, -1, "en-US", null, null, null, javaColl, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:en-x-icu"), "en-x-icu", pgCatalogNs, 10, "i", true, -1, "en", null, null, null, javaColl, 1 });
        // There is deliberately no en_US row. PostgreSQL imports one on a host whose locale list
        // has it, but as a libc collation at that locale's own encoding — never as an ICU
        // collation at UTF8, which is what memgres used to claim. A row nothing on the real
        // server matches is a name a client will offer and then find missing there.
        // User-defined collations (from CREATE COLLATION)
        for (java.util.Map.Entry<String, Database.CollationDef> entry : database.getUserCollations().entrySet()) {
            Database.CollationDef coll = entry.getValue();
            int publicNs = oids.oid("ns:public");
            // An ICU collation is encoding-independent, which PostgreSQL records as collencoding
            // -1; only a libc collation is tied to one encoding. Writing 6 for both made
            // PostgreSQL's own lookup condition, "collencoding = -1 OR collencoding = the
            // database encoding", find the row under the wrong arm.
            int encoding = "i".equals(coll.provider) ? -1 : 6;
            table.insertRow(new Object[]{
                    oids.oid("collation:" + coll.name), coll.name, publicNs, 10, coll.provider,
                    coll.deterministic, encoding, coll.locale, null,
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
            String subtypeName = entry.getValue();
            int rangeTypeOid = oids.oid("type:" + entry.getKey());
            int subtypeOid = resolveTypeOid(subtypeName);
            table.insertRow(new Object[]{rangeTypeOid, subtypeOid, 0, 0, 0, 0, 0});
        }
        return table;
    }

    Table buildPgExtension() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("extname", DataType.NAME),
                col("extowner", DataType.OID),
                col("extnamespace", DataType.OID),
                col("extrelocatable", DataType.BOOLEAN),
                col("extversion", DataType.TEXT),
                col("extconfig", DataType.OID_ARRAY),
                col("extcondition", DataType.TEXT_ARRAY),
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
                colNN("oid", DataType.OID), colNN("lanname", DataType.NAME),
                col("lanowner", DataType.OID), col("lanispl", DataType.BOOLEAN),
                col("lanpltrusted", DataType.BOOLEAN), col("lanplcallfoid", DataType.OID),
                col("laninline", DataType.OID), col("lanvalidator", DataType.OID),
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
        for (Object[] c : PgCastTable.CASTS) {
            String fname = (String) c[2];
            int castfunc = fname.isEmpty() ? 0 : oids.oid("proc:" + fname);
            table.insertRow(new Object[]{c[5], c[0], c[1], castfunc, c[3], c[4], 1});
        }

        // User-defined casts (CREATE CAST). Numbered above FirstNormalObjectId, where every
        // object somebody created belongs — the built-ins above keep PostgreSQL's own numbers,
        // which are all below it.
        int userCastOid = 16384;
        for (Object[] uc : database.getUserDefinedCasts()) {
            table.insertRow(new Object[]{userCastOid++, uc[0], uc[1], uc[2], uc[3], uc[4], 1});
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

    /**
     * PostgreSQL's OID for each built-in operator, keyed by the "name left right" signature that
     * identifies it.
     *
     * <p>Built-in operators carry the numbers PostgreSQL gives them rather than numbers minted
     * from the user-object counter, so a reference to one is the same reference on both servers
     * and the {@code oid &lt; 16384} test every client uses to tell a shipped object from a
     * created one answers the way it does there.
     */
    private static final Map<String, Integer> BUILTIN_OPERATOR_OIDS = builtinOperatorOids();

    private static Map<String, Integer> builtinOperatorOids() {
        Map<String, Integer> bySignature = new HashMap<String, Integer>();
        for (Object[] op : PgOperatorTable.OPERATORS) {
            bySignature.put(op[0] + " " + op[2] + " " + op[3], (Integer) op[10]);
        }
        return bySignature;
    }

    /** The OID of a built-in operator row. */
    private static int builtinOperatorOid(Object[] op) {
        return (Integer) op[10];
    }

    /** Resolve an "name left right" reference to another built-in operator; 0 when there is none. */
    private static int signatureOid(String signature) {
        if (signature == null || signature.isEmpty()) return 0;
        Integer oid = BUILTIN_OPERATOR_OIDS.get(signature);
        return oid == null ? 0 : oid.intValue();
    }

    /** The OID of the built-in operator with this name and operand types, or 0. */
    static int builtinOperatorOid(String name, int left, int right) {
        Integer oid = BUILTIN_OPERATOR_OIDS.get(name + " " + left + " " + right);
        return oid == null ? 0 : oid.intValue();
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
        String key = TypeNamespace.oidKeyFor(database, typeName);
        return oids.oid(key != null ? key : TypeNamespace.oidKey(null, typeName));
    }

    private int resolveAccessMethodOid(String method) {
        if (method == null) return 0;
        int builtin = accessMethodOid(method);
        return builtin != 0 ? builtin : oids.oid("am:" + method.toLowerCase());
    }

    /** PostgreSQL's OID for one of the access methods it ships, or 0 for anything else. */
    private static int accessMethodOid(String method) {
        switch (method.toLowerCase()) {
            case "btree": return 403;
            case "hash": return 405;
            case "gist": return 783;
            case "gin": return 2742;
            case "spgist": return 4000;
            case "brin": return 3580;
            default: return 0;
        }
    }

    /**
     * The operator classes PostgreSQL 18 ships, read off pg_opclass with the extension-owned rows
     * and everything outside pg_catalog removed, so nothing a contrib package supplies is claimed
     * here.
     *
     * <p>A class is what says a type can be indexed at all. CREATE INDEX validation, pg_dump, an
     * ORM picking an index and psql's {@code \dAc} all ask pg_opclass which class handles a type,
     * and a type with no row is one they report as unindexable. Carrying the btree and hash
     * classes alone answered "this cannot be indexed" for every gist, spgist and brin class
     * PostgreSQL has, and left a gin index on jsonb_path_ops pointing pg_index at no row.
     *
     * <p>Columns: opcname, access method, opcintype, opcdefault, opckeytype, opfname.
     */
    private static final Object[][] OPCLASSES = {
            // btree
            {"array_ops", "btree", 2277, true, 0, "array_ops"},
            {"bit_ops", "btree", 1560, true, 0, "bit_ops"},
            {"bool_ops", "btree", 16, true, 0, "bool_ops"},
            {"bpchar_ops", "btree", 1042, true, 0, "bpchar_ops"},
            {"bpchar_pattern_ops", "btree", 1042, false, 0, "bpchar_pattern_ops"},
            {"bytea_ops", "btree", 17, true, 0, "bytea_ops"},
            {"char_ops", "btree", 18, true, 0, "char_ops"},
            {"cidr_ops", "btree", 869, false, 0, "network_ops"},
            {"date_ops", "btree", 1082, true, 0, "datetime_ops"},
            {"enum_ops", "btree", 3500, true, 0, "enum_ops"},
            {"float4_ops", "btree", 700, true, 0, "float_ops"},
            {"float8_ops", "btree", 701, true, 0, "float_ops"},
            {"inet_ops", "btree", 869, true, 0, "network_ops"},
            {"int2_ops", "btree", 21, true, 0, "integer_ops"},
            {"int4_ops", "btree", 23, true, 0, "integer_ops"},
            {"int8_ops", "btree", 20, true, 0, "integer_ops"},
            {"interval_ops", "btree", 1186, true, 0, "interval_ops"},
            {"jsonb_ops", "btree", 3802, true, 0, "jsonb_ops"},
            {"macaddr8_ops", "btree", 774, true, 0, "macaddr8_ops"},
            {"macaddr_ops", "btree", 829, true, 0, "macaddr_ops"},
            {"money_ops", "btree", 790, true, 0, "money_ops"},
            {"multirange_ops", "btree", 4537, true, 0, "multirange_ops"},
            {"name_ops", "btree", 19, true, 2275, "text_ops"},
            {"numeric_ops", "btree", 1700, true, 0, "numeric_ops"},
            {"oid_ops", "btree", 26, true, 0, "oid_ops"},
            {"oidvector_ops", "btree", 30, true, 0, "oidvector_ops"},
            {"pg_lsn_ops", "btree", 3220, true, 0, "pg_lsn_ops"},
            {"range_ops", "btree", 3831, true, 0, "range_ops"},
            {"record_image_ops", "btree", 2249, false, 0, "record_image_ops"},
            {"record_ops", "btree", 2249, true, 0, "record_ops"},
            {"text_ops", "btree", 25, true, 0, "text_ops"},
            {"text_pattern_ops", "btree", 25, false, 0, "text_pattern_ops"},
            {"tid_ops", "btree", 27, true, 0, "tid_ops"},
            {"time_ops", "btree", 1083, true, 0, "time_ops"},
            {"timestamp_ops", "btree", 1114, true, 0, "datetime_ops"},
            {"timestamptz_ops", "btree", 1184, true, 0, "datetime_ops"},
            {"timetz_ops", "btree", 1266, true, 0, "timetz_ops"},
            {"tsquery_ops", "btree", 3615, true, 0, "tsquery_ops"},
            {"tsvector_ops", "btree", 3614, true, 0, "tsvector_ops"},
            {"uuid_ops", "btree", 2950, true, 0, "uuid_ops"},
            {"varbit_ops", "btree", 1562, true, 0, "varbit_ops"},
            {"varchar_ops", "btree", 25, false, 0, "text_ops"},
            {"varchar_pattern_ops", "btree", 25, false, 0, "text_pattern_ops"},
            {"xid8_ops", "btree", 5069, true, 0, "xid8_ops"},
            // hash
            {"aclitem_ops", "hash", 1033, true, 0, "aclitem_ops"},
            {"array_ops", "hash", 2277, true, 0, "array_ops"},
            {"bool_ops", "hash", 16, true, 0, "bool_ops"},
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
            {"int4_ops", "hash", 23, true, 0, "integer_ops"},
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
            {"text_ops", "hash", 25, true, 0, "text_ops"},
            {"text_pattern_ops", "hash", 25, false, 0, "text_pattern_ops"},
            {"tid_ops", "hash", 27, true, 0, "tid_ops"},
            {"time_ops", "hash", 1083, true, 0, "time_ops"},
            {"timestamp_ops", "hash", 1114, true, 0, "timestamp_ops"},
            {"timestamptz_ops", "hash", 1184, true, 0, "timestamptz_ops"},
            {"timetz_ops", "hash", 1266, true, 0, "timetz_ops"},
            {"uuid_ops", "hash", 2950, true, 0, "uuid_ops"},
            {"varchar_ops", "hash", 25, false, 0, "text_ops"},
            {"varchar_pattern_ops", "hash", 25, false, 0, "text_pattern_ops"},
            {"xid8_ops", "hash", 5069, true, 0, "xid8_ops"},
            {"xid_ops", "hash", 28, true, 0, "xid_ops"},
            // gist
            {"box_ops", "gist", 603, true, 0, "box_ops"},
            {"circle_ops", "gist", 718, true, 603, "circle_ops"},
            {"inet_ops", "gist", 869, false, 0, "network_ops"},
            {"multirange_ops", "gist", 4537, true, 3831, "multirange_ops"},
            {"point_ops", "gist", 600, true, 603, "point_ops"},
            {"poly_ops", "gist", 604, true, 603, "poly_ops"},
            {"range_ops", "gist", 3831, true, 0, "range_ops"},
            {"tsquery_ops", "gist", 3615, true, 20, "tsquery_ops"},
            // opckeytype is gtsvector (3642) in PostgreSQL; memgres has no such pg_type row and an
            // opckeytype naming a type that is not there is a reference a reader cannot follow.
            {"tsvector_ops", "gist", 3614, true, 0, "tsvector_ops"},
            // spgist
            {"box_ops", "spgist", 603, true, 0, "box_ops"},
            {"inet_ops", "spgist", 869, true, 0, "network_ops"},
            {"kd_point_ops", "spgist", 600, false, 0, "kd_point_ops"},
            {"poly_ops", "spgist", 604, true, 603, "poly_ops"},
            {"quad_point_ops", "spgist", 600, true, 0, "quad_point_ops"},
            {"range_ops", "spgist", 3831, true, 0, "range_ops"},
            {"text_ops", "spgist", 25, true, 0, "text_ops"},
            // gin
            {"array_ops", "gin", 2277, true, 2283, "array_ops"},
            {"jsonb_ops", "gin", 3802, true, 25, "jsonb_ops"},
            {"jsonb_path_ops", "gin", 3802, false, 23, "jsonb_path_ops"},
            {"tsvector_ops", "gin", 3614, true, 25, "tsvector_ops"},
            // brin
            {"bit_minmax_ops", "brin", 1560, true, 1560, "bit_minmax_ops"},
            {"box_inclusion_ops", "brin", 603, true, 603, "box_inclusion_ops"},
            {"bpchar_bloom_ops", "brin", 1042, false, 1042, "bpchar_bloom_ops"},
            {"bpchar_minmax_ops", "brin", 1042, true, 1042, "bpchar_minmax_ops"},
            {"bytea_bloom_ops", "brin", 17, false, 17, "bytea_bloom_ops"},
            {"bytea_minmax_ops", "brin", 17, true, 17, "bytea_minmax_ops"},
            {"char_bloom_ops", "brin", 18, false, 18, "char_bloom_ops"},
            {"char_minmax_ops", "brin", 18, true, 18, "char_minmax_ops"},
            {"date_bloom_ops", "brin", 1082, false, 1082, "datetime_bloom_ops"},
            {"date_minmax_multi_ops", "brin", 1082, false, 1082, "datetime_minmax_multi_ops"},
            {"date_minmax_ops", "brin", 1082, true, 1082, "datetime_minmax_ops"},
            {"float4_bloom_ops", "brin", 700, false, 700, "float_bloom_ops"},
            {"float4_minmax_multi_ops", "brin", 700, false, 700, "float_minmax_multi_ops"},
            {"float4_minmax_ops", "brin", 700, true, 700, "float_minmax_ops"},
            {"float8_bloom_ops", "brin", 701, false, 701, "float_bloom_ops"},
            {"float8_minmax_multi_ops", "brin", 701, false, 701, "float_minmax_multi_ops"},
            {"float8_minmax_ops", "brin", 701, true, 701, "float_minmax_ops"},
            {"inet_bloom_ops", "brin", 869, false, 869, "network_bloom_ops"},
            {"inet_inclusion_ops", "brin", 869, true, 869, "network_inclusion_ops"},
            {"inet_minmax_multi_ops", "brin", 869, false, 869, "network_minmax_multi_ops"},
            {"inet_minmax_ops", "brin", 869, false, 869, "network_minmax_ops"},
            {"int2_bloom_ops", "brin", 21, false, 21, "integer_bloom_ops"},
            {"int2_minmax_multi_ops", "brin", 21, false, 21, "integer_minmax_multi_ops"},
            {"int2_minmax_ops", "brin", 21, true, 21, "integer_minmax_ops"},
            {"int4_bloom_ops", "brin", 23, false, 23, "integer_bloom_ops"},
            {"int4_minmax_multi_ops", "brin", 23, false, 23, "integer_minmax_multi_ops"},
            {"int4_minmax_ops", "brin", 23, true, 23, "integer_minmax_ops"},
            {"int8_bloom_ops", "brin", 20, false, 20, "integer_bloom_ops"},
            {"int8_minmax_multi_ops", "brin", 20, false, 20, "integer_minmax_multi_ops"},
            {"int8_minmax_ops", "brin", 20, true, 20, "integer_minmax_ops"},
            {"interval_bloom_ops", "brin", 1186, false, 1186, "interval_bloom_ops"},
            {"interval_minmax_multi_ops", "brin", 1186, false, 1186, "interval_minmax_multi_ops"},
            {"interval_minmax_ops", "brin", 1186, true, 1186, "interval_minmax_ops"},
            {"macaddr8_bloom_ops", "brin", 774, false, 774, "macaddr8_bloom_ops"},
            {"macaddr8_minmax_multi_ops", "brin", 774, false, 774, "macaddr8_minmax_multi_ops"},
            {"macaddr8_minmax_ops", "brin", 774, true, 774, "macaddr8_minmax_ops"},
            {"macaddr_bloom_ops", "brin", 829, false, 829, "macaddr_bloom_ops"},
            {"macaddr_minmax_multi_ops", "brin", 829, false, 829, "macaddr_minmax_multi_ops"},
            {"macaddr_minmax_ops", "brin", 829, true, 829, "macaddr_minmax_ops"},
            {"name_bloom_ops", "brin", 19, false, 19, "name_bloom_ops"},
            {"name_minmax_ops", "brin", 19, true, 19, "name_minmax_ops"},
            {"numeric_bloom_ops", "brin", 1700, false, 1700, "numeric_bloom_ops"},
            {"numeric_minmax_multi_ops", "brin", 1700, false, 1700, "numeric_minmax_multi_ops"},
            {"numeric_minmax_ops", "brin", 1700, true, 1700, "numeric_minmax_ops"},
            {"oid_bloom_ops", "brin", 26, false, 26, "oid_bloom_ops"},
            {"oid_minmax_multi_ops", "brin", 26, false, 26, "oid_minmax_multi_ops"},
            {"oid_minmax_ops", "brin", 26, true, 26, "oid_minmax_ops"},
            {"pg_lsn_bloom_ops", "brin", 3220, false, 3220, "pg_lsn_bloom_ops"},
            {"pg_lsn_minmax_multi_ops", "brin", 3220, false, 3220, "pg_lsn_minmax_multi_ops"},
            {"pg_lsn_minmax_ops", "brin", 3220, true, 3220, "pg_lsn_minmax_ops"},
            {"range_inclusion_ops", "brin", 3831, true, 3831, "range_inclusion_ops"},
            {"text_bloom_ops", "brin", 25, false, 25, "text_bloom_ops"},
            {"text_minmax_ops", "brin", 25, true, 25, "text_minmax_ops"},
            {"tid_bloom_ops", "brin", 27, false, 27, "tid_bloom_ops"},
            {"tid_minmax_multi_ops", "brin", 27, false, 27, "tid_minmax_multi_ops"},
            {"tid_minmax_ops", "brin", 27, true, 27, "tid_minmax_ops"},
            {"time_bloom_ops", "brin", 1083, false, 1083, "time_bloom_ops"},
            {"time_minmax_multi_ops", "brin", 1083, false, 1083, "time_minmax_multi_ops"},
            {"time_minmax_ops", "brin", 1083, true, 1083, "time_minmax_ops"},
            {"timestamp_bloom_ops", "brin", 1114, false, 1114, "datetime_bloom_ops"},
            {"timestamp_minmax_multi_ops", "brin", 1114, false, 1114, "datetime_minmax_multi_ops"},
            {"timestamp_minmax_ops", "brin", 1114, true, 1114, "datetime_minmax_ops"},
            {"timestamptz_bloom_ops", "brin", 1184, false, 1184, "datetime_bloom_ops"},
            {"timestamptz_minmax_multi_ops", "brin", 1184, false, 1184, "datetime_minmax_multi_ops"},
            {"timestamptz_minmax_ops", "brin", 1184, true, 1184, "datetime_minmax_ops"},
            {"timetz_bloom_ops", "brin", 1266, false, 1266, "timetz_bloom_ops"},
            {"timetz_minmax_multi_ops", "brin", 1266, false, 1266, "timetz_minmax_multi_ops"},
            {"timetz_minmax_ops", "brin", 1266, true, 1266, "timetz_minmax_ops"},
            {"uuid_bloom_ops", "brin", 2950, false, 2950, "uuid_bloom_ops"},
            {"uuid_minmax_multi_ops", "brin", 2950, false, 2950, "uuid_minmax_multi_ops"},
            {"uuid_minmax_ops", "brin", 2950, true, 2950, "uuid_minmax_ops"},
            {"varbit_minmax_ops", "brin", 1562, true, 1562, "varbit_minmax_ops"},
    };

    /**
     * The families {@link #OPCLASSES} belong to: opfname, access method.
     *
     * <p>Every operator strategy and support procedure is recorded against the family rather than
     * the class, so a family with no row leaves each of its classes pointing at nothing.
     */
    private static final Object[][] OPFAMILIES = {
            // btree
            {"array_ops", "btree"},
            {"bit_ops", "btree"},
            {"bool_ops", "btree"},
            {"bpchar_ops", "btree"},
            {"bpchar_pattern_ops", "btree"},
            {"bytea_ops", "btree"},
            {"char_ops", "btree"},
            {"datetime_ops", "btree"},
            {"enum_ops", "btree"},
            {"float_ops", "btree"},
            {"integer_ops", "btree"},
            {"interval_ops", "btree"},
            {"jsonb_ops", "btree"},
            {"macaddr8_ops", "btree"},
            {"macaddr_ops", "btree"},
            {"money_ops", "btree"},
            {"multirange_ops", "btree"},
            {"network_ops", "btree"},
            {"numeric_ops", "btree"},
            {"oid_ops", "btree"},
            {"oidvector_ops", "btree"},
            {"pg_lsn_ops", "btree"},
            {"range_ops", "btree"},
            {"record_image_ops", "btree"},
            {"record_ops", "btree"},
            {"text_ops", "btree"},
            {"text_pattern_ops", "btree"},
            {"tid_ops", "btree"},
            {"time_ops", "btree"},
            {"timetz_ops", "btree"},
            {"tsquery_ops", "btree"},
            {"tsvector_ops", "btree"},
            {"uuid_ops", "btree"},
            {"varbit_ops", "btree"},
            {"xid8_ops", "btree"},
            // hash
            {"aclitem_ops", "hash"},
            {"array_ops", "hash"},
            {"bool_ops", "hash"},
            {"bpchar_ops", "hash"},
            {"bpchar_pattern_ops", "hash"},
            {"bytea_ops", "hash"},
            {"char_ops", "hash"},
            {"cid_ops", "hash"},
            {"date_ops", "hash"},
            {"enum_ops", "hash"},
            {"float_ops", "hash"},
            {"integer_ops", "hash"},
            {"interval_ops", "hash"},
            {"jsonb_ops", "hash"},
            {"macaddr8_ops", "hash"},
            {"macaddr_ops", "hash"},
            {"multirange_ops", "hash"},
            {"network_ops", "hash"},
            {"numeric_ops", "hash"},
            {"oid_ops", "hash"},
            {"oidvector_ops", "hash"},
            {"pg_lsn_ops", "hash"},
            {"range_ops", "hash"},
            {"record_ops", "hash"},
            {"text_ops", "hash"},
            {"text_pattern_ops", "hash"},
            {"tid_ops", "hash"},
            {"time_ops", "hash"},
            {"timestamp_ops", "hash"},
            {"timestamptz_ops", "hash"},
            {"timetz_ops", "hash"},
            {"uuid_ops", "hash"},
            {"xid8_ops", "hash"},
            {"xid_ops", "hash"},
            // gist
            {"box_ops", "gist"},
            {"circle_ops", "gist"},
            {"multirange_ops", "gist"},
            {"network_ops", "gist"},
            {"point_ops", "gist"},
            {"poly_ops", "gist"},
            {"range_ops", "gist"},
            {"tsquery_ops", "gist"},
            {"tsvector_ops", "gist"},
            // spgist
            {"box_ops", "spgist"},
            {"kd_point_ops", "spgist"},
            {"network_ops", "spgist"},
            {"poly_ops", "spgist"},
            {"quad_point_ops", "spgist"},
            {"range_ops", "spgist"},
            {"text_ops", "spgist"},
            // gin
            {"array_ops", "gin"},
            {"jsonb_ops", "gin"},
            {"jsonb_path_ops", "gin"},
            {"tsvector_ops", "gin"},
            // brin
            {"bit_minmax_ops", "brin"},
            {"box_inclusion_ops", "brin"},
            {"bpchar_bloom_ops", "brin"},
            {"bpchar_minmax_ops", "brin"},
            {"bytea_bloom_ops", "brin"},
            {"bytea_minmax_ops", "brin"},
            {"char_bloom_ops", "brin"},
            {"char_minmax_ops", "brin"},
            {"datetime_bloom_ops", "brin"},
            {"datetime_minmax_multi_ops", "brin"},
            {"datetime_minmax_ops", "brin"},
            {"float_bloom_ops", "brin"},
            {"float_minmax_multi_ops", "brin"},
            {"float_minmax_ops", "brin"},
            {"integer_bloom_ops", "brin"},
            {"integer_minmax_multi_ops", "brin"},
            {"integer_minmax_ops", "brin"},
            {"interval_bloom_ops", "brin"},
            {"interval_minmax_multi_ops", "brin"},
            {"interval_minmax_ops", "brin"},
            {"macaddr8_bloom_ops", "brin"},
            {"macaddr8_minmax_multi_ops", "brin"},
            {"macaddr8_minmax_ops", "brin"},
            {"macaddr_bloom_ops", "brin"},
            {"macaddr_minmax_multi_ops", "brin"},
            {"macaddr_minmax_ops", "brin"},
            {"name_bloom_ops", "brin"},
            {"name_minmax_ops", "brin"},
            {"network_bloom_ops", "brin"},
            {"network_inclusion_ops", "brin"},
            {"network_minmax_multi_ops", "brin"},
            {"network_minmax_ops", "brin"},
            {"numeric_bloom_ops", "brin"},
            {"numeric_minmax_multi_ops", "brin"},
            {"numeric_minmax_ops", "brin"},
            {"oid_bloom_ops", "brin"},
            {"oid_minmax_multi_ops", "brin"},
            {"oid_minmax_ops", "brin"},
            {"pg_lsn_bloom_ops", "brin"},
            {"pg_lsn_minmax_multi_ops", "brin"},
            {"pg_lsn_minmax_ops", "brin"},
            {"range_inclusion_ops", "brin"},
            {"text_bloom_ops", "brin"},
            {"text_minmax_ops", "brin"},
            {"tid_bloom_ops", "brin"},
            {"tid_minmax_multi_ops", "brin"},
            {"tid_minmax_ops", "brin"},
            {"time_bloom_ops", "brin"},
            {"time_minmax_multi_ops", "brin"},
            {"time_minmax_ops", "brin"},
            {"timetz_bloom_ops", "brin"},
            {"timetz_minmax_multi_ops", "brin"},
            {"timetz_minmax_ops", "brin"},
            {"uuid_bloom_ops", "brin"},
            {"uuid_minmax_multi_ops", "brin"},
            {"uuid_minmax_ops", "brin"},
            {"varbit_minmax_ops", "brin"},
    };

    /**
     * Operator class and family names repeat across access methods -- int4_ops is a btree class
     * and a hash class -- so a key has to say which one it means. The first access method in this
     * order keeps the bare name, and the bare name is also what CREATE INDEX resolves an explicit
     * operator class by, so a class only one access method has stays reachable that way.
     */
    private static final List<String> AM_KEY_ORDER =
            Arrays.asList("btree", "hash", "gist", "spgist", "gin", "brin");

    private static final Map<String, String> OPCLASS_KEYS = objectKeys(OPCLASSES, "opclass:");
    private static final Map<String, String> OPFAMILY_KEYS = objectKeys(OPFAMILIES, "opfamily:");

    /** The OID key for each (name, access method) pair in a table of catalog objects. */
    private static Map<String, String> objectKeys(Object[][] rows, String prefix) {
        Map<String, String> owner = new HashMap<String, String>();
        for (Object[] r : rows) {
            String name = (String) r[0];
            String held = owner.get(name);
            if (held == null || AM_KEY_ORDER.indexOf(r[1]) < AM_KEY_ORDER.indexOf(held)) {
                owner.put(name, (String) r[1]);
            }
        }
        Map<String, String> keys = new HashMap<String, String>();
        for (Object[] r : rows) {
            String name = (String) r[0];
            String am = (String) r[1];
            keys.put(name + "/" + am,
                    am.equals(owner.get(name)) ? prefix + name : prefix + am + "_" + name);
        }
        return keys;
    }

    /**
     * PostgreSQL's own OID for the default btree class over integer.
     *
     * <p>Index creation writes this number into pg_index.indclass for an integer column, and for
     * any type it has no class for, so the class has to be findable at it. Numbering int4_ops
     * anything else -- it used to carry 403, which is btree's own pg_am OID -- left every integer
     * index, and so every integer primary key, naming a pg_opclass row that does not exist.
     */
    private static final int INT4_OPS_OID = 1978;

    /** The OID of a built-in operator class. */
    private int opclassOid(String name, String amName) {
        if ("int4_ops".equals(name) && "btree".equals(amName)) return INT4_OPS_OID;
        return oids.oid(OPCLASS_KEYS.get(name + "/" + amName));
    }

    /** The OID of a built-in operator family. */
    private int opfamilyOid(String name, String amName) {
        return oids.oid(OPFAMILY_KEYS.get(name + "/" + amName));
    }

    Table buildPgOpclass() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), colNN("opcname", DataType.NAME),
                col("opcnamespace", DataType.OID), col("opcowner", DataType.OID),
                col("opcfamily", DataType.OID), col("opcintype", DataType.OID),
                col("opckeytype", DataType.OID), col("opcdefault", DataType.BOOLEAN),
                col("opcmethod", DataType.OID), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_opclass", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        for (Object[] c : OPCLASSES) {
            String name = (String) c[0];
            String amName = (String) c[1];
            table.insertRow(new Object[]{
                    opclassOid(name, amName), name, pgCatalogNs, 10,
                    opfamilyOid((String) c[5], amName), c[2], c[4], c[3],
                    resolveAccessMethodOid(amName), 1});
        }

        // User-defined operator classes
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
        for (Object[] f : OPFAMILIES) {
            String name = (String) f[0];
            String amName = (String) f[1];
            table.insertRow(new Object[]{opfamilyOid(name, amName), name, pgCatalogNs, 10,
                    resolveAccessMethodOid(amName), 1});
        }

        // User-defined operator families
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

    /**
     * pg_amop: which operator an operator family answers each index strategy with, as PostgreSQL
     * 18 records it, with the extension-owned rows removed.
     *
     * <p>This is the table that says what an index can be searched by. psql's {@code \dAo}, a
     * planner deciding whether a WHERE clause is indexable, and anything asking "is there a
     * btree class that understands {@code ~<~}" read it. Ten rows for two families left
     * seventy-two of memgres's own families claiming no operators at all.
     *
     * <p>Fields, space separated: opfname, access method, amoplefttype, amoprighttype,
     * amopstrategy, amoppurpose, the operator's name, and the sort family of an ordering
     * operator ("-" when there is none).
     */
    private static final String[] AMOPS = {
            // aclitem_ops / hash
            "aclitem_ops hash 1033 1033 1 s = -",
            // array_ops / btree
            "array_ops btree 2277 2277 1 s < -",
            "array_ops btree 2277 2277 2 s <= -",
            "array_ops btree 2277 2277 3 s = -",
            "array_ops btree 2277 2277 4 s >= -",
            "array_ops btree 2277 2277 5 s > -",
            // array_ops / gin
            "array_ops gin 2277 2277 1 s && -",
            "array_ops gin 2277 2277 2 s @> -",
            "array_ops gin 2277 2277 3 s <@ -",
            "array_ops gin 2277 2277 4 s = -",
            // array_ops / hash
            "array_ops hash 2277 2277 1 s = -",
            // bit_minmax_ops / brin
            "bit_minmax_ops brin 1560 1560 1 s < -",
            "bit_minmax_ops brin 1560 1560 2 s <= -",
            "bit_minmax_ops brin 1560 1560 3 s = -",
            "bit_minmax_ops brin 1560 1560 4 s >= -",
            "bit_minmax_ops brin 1560 1560 5 s > -",
            // bit_ops / btree
            "bit_ops btree 1560 1560 1 s < -",
            "bit_ops btree 1560 1560 2 s <= -",
            "bit_ops btree 1560 1560 3 s = -",
            "bit_ops btree 1560 1560 4 s >= -",
            "bit_ops btree 1560 1560 5 s > -",
            // bool_ops / btree
            "bool_ops btree 16 16 1 s < -",
            "bool_ops btree 16 16 2 s <= -",
            "bool_ops btree 16 16 3 s = -",
            "bool_ops btree 16 16 4 s >= -",
            "bool_ops btree 16 16 5 s > -",
            // bool_ops / hash
            "bool_ops hash 16 16 1 s = -",
            // box_inclusion_ops / brin
            "box_inclusion_ops brin 603 600 7 s @> -",
            "box_inclusion_ops brin 603 603 1 s << -",
            "box_inclusion_ops brin 603 603 2 s &< -",
            "box_inclusion_ops brin 603 603 3 s && -",
            "box_inclusion_ops brin 603 603 4 s &> -",
            "box_inclusion_ops brin 603 603 5 s >> -",
            "box_inclusion_ops brin 603 603 6 s ~= -",
            "box_inclusion_ops brin 603 603 7 s @> -",
            "box_inclusion_ops brin 603 603 8 s <@ -",
            "box_inclusion_ops brin 603 603 9 s &<| -",
            "box_inclusion_ops brin 603 603 10 s <<| -",
            "box_inclusion_ops brin 603 603 11 s |>> -",
            "box_inclusion_ops brin 603 603 12 s |&> -",
            // box_ops / gist
            "box_ops gist 603 600 15 o <-> float_ops",
            "box_ops gist 603 603 1 s << -",
            "box_ops gist 603 603 2 s &< -",
            "box_ops gist 603 603 3 s && -",
            "box_ops gist 603 603 4 s &> -",
            "box_ops gist 603 603 5 s >> -",
            "box_ops gist 603 603 6 s ~= -",
            "box_ops gist 603 603 7 s @> -",
            "box_ops gist 603 603 8 s <@ -",
            "box_ops gist 603 603 9 s &<| -",
            "box_ops gist 603 603 10 s <<| -",
            "box_ops gist 603 603 11 s |>> -",
            "box_ops gist 603 603 12 s |&> -",
            // box_ops / spgist
            "box_ops spgist 603 600 15 o <-> float_ops",
            "box_ops spgist 603 603 1 s << -",
            "box_ops spgist 603 603 2 s &< -",
            "box_ops spgist 603 603 3 s && -",
            "box_ops spgist 603 603 4 s &> -",
            "box_ops spgist 603 603 5 s >> -",
            "box_ops spgist 603 603 6 s ~= -",
            "box_ops spgist 603 603 7 s @> -",
            "box_ops spgist 603 603 8 s <@ -",
            "box_ops spgist 603 603 9 s &<| -",
            "box_ops spgist 603 603 10 s <<| -",
            "box_ops spgist 603 603 11 s |>> -",
            "box_ops spgist 603 603 12 s |&> -",
            // bpchar_bloom_ops / brin
            "bpchar_bloom_ops brin 1042 1042 1 s = -",
            // bpchar_minmax_ops / brin
            "bpchar_minmax_ops brin 1042 1042 1 s < -",
            "bpchar_minmax_ops brin 1042 1042 2 s <= -",
            "bpchar_minmax_ops brin 1042 1042 3 s = -",
            "bpchar_minmax_ops brin 1042 1042 4 s >= -",
            "bpchar_minmax_ops brin 1042 1042 5 s > -",
            // bpchar_ops / btree
            "bpchar_ops btree 1042 1042 1 s < -",
            "bpchar_ops btree 1042 1042 2 s <= -",
            "bpchar_ops btree 1042 1042 3 s = -",
            "bpchar_ops btree 1042 1042 4 s >= -",
            "bpchar_ops btree 1042 1042 5 s > -",
            // bpchar_ops / hash
            "bpchar_ops hash 1042 1042 1 s = -",
            // bpchar_pattern_ops / btree
            "bpchar_pattern_ops btree 1042 1042 1 s ~<~ -",
            "bpchar_pattern_ops btree 1042 1042 2 s ~<=~ -",
            "bpchar_pattern_ops btree 1042 1042 3 s = -",
            "bpchar_pattern_ops btree 1042 1042 4 s ~>=~ -",
            "bpchar_pattern_ops btree 1042 1042 5 s ~>~ -",
            // bpchar_pattern_ops / hash
            "bpchar_pattern_ops hash 1042 1042 1 s = -",
            // bytea_bloom_ops / brin
            "bytea_bloom_ops brin 17 17 1 s = -",
            // bytea_minmax_ops / brin
            "bytea_minmax_ops brin 17 17 1 s < -",
            "bytea_minmax_ops brin 17 17 2 s <= -",
            "bytea_minmax_ops brin 17 17 3 s = -",
            "bytea_minmax_ops brin 17 17 4 s >= -",
            "bytea_minmax_ops brin 17 17 5 s > -",
            // bytea_ops / btree
            "bytea_ops btree 17 17 1 s < -",
            "bytea_ops btree 17 17 2 s <= -",
            "bytea_ops btree 17 17 3 s = -",
            "bytea_ops btree 17 17 4 s >= -",
            "bytea_ops btree 17 17 5 s > -",
            // bytea_ops / hash
            "bytea_ops hash 17 17 1 s = -",
            // char_bloom_ops / brin
            "char_bloom_ops brin 18 18 1 s = -",
            // char_minmax_ops / brin
            "char_minmax_ops brin 18 18 1 s < -",
            "char_minmax_ops brin 18 18 2 s <= -",
            "char_minmax_ops brin 18 18 3 s = -",
            "char_minmax_ops brin 18 18 4 s >= -",
            "char_minmax_ops brin 18 18 5 s > -",
            // char_ops / btree
            "char_ops btree 18 18 1 s < -",
            "char_ops btree 18 18 2 s <= -",
            "char_ops btree 18 18 3 s = -",
            "char_ops btree 18 18 4 s >= -",
            "char_ops btree 18 18 5 s > -",
            // char_ops / hash
            "char_ops hash 18 18 1 s = -",
            // cid_ops / hash
            "cid_ops hash 29 29 1 s = -",
            // circle_ops / gist
            "circle_ops gist 718 600 15 o <-> float_ops",
            "circle_ops gist 718 718 1 s << -",
            "circle_ops gist 718 718 2 s &< -",
            "circle_ops gist 718 718 3 s && -",
            "circle_ops gist 718 718 4 s &> -",
            "circle_ops gist 718 718 5 s >> -",
            "circle_ops gist 718 718 6 s ~= -",
            "circle_ops gist 718 718 7 s @> -",
            "circle_ops gist 718 718 8 s <@ -",
            "circle_ops gist 718 718 9 s &<| -",
            "circle_ops gist 718 718 10 s <<| -",
            "circle_ops gist 718 718 11 s |>> -",
            "circle_ops gist 718 718 12 s |&> -",
            // date_ops / hash
            "date_ops hash 1082 1082 1 s = -",
            // datetime_bloom_ops / brin
            "datetime_bloom_ops brin 1082 1082 1 s = -",
            "datetime_bloom_ops brin 1114 1114 1 s = -",
            "datetime_bloom_ops brin 1184 1184 1 s = -",
            // datetime_minmax_multi_ops / brin
            "datetime_minmax_multi_ops brin 1082 1082 1 s < -",
            "datetime_minmax_multi_ops brin 1082 1082 2 s <= -",
            "datetime_minmax_multi_ops brin 1082 1082 3 s = -",
            "datetime_minmax_multi_ops brin 1082 1082 4 s >= -",
            "datetime_minmax_multi_ops brin 1082 1082 5 s > -",
            "datetime_minmax_multi_ops brin 1082 1114 1 s < -",
            "datetime_minmax_multi_ops brin 1082 1114 2 s <= -",
            "datetime_minmax_multi_ops brin 1082 1114 3 s = -",
            "datetime_minmax_multi_ops brin 1082 1114 4 s >= -",
            "datetime_minmax_multi_ops brin 1082 1114 5 s > -",
            "datetime_minmax_multi_ops brin 1082 1184 1 s < -",
            "datetime_minmax_multi_ops brin 1082 1184 2 s <= -",
            "datetime_minmax_multi_ops brin 1082 1184 3 s = -",
            "datetime_minmax_multi_ops brin 1082 1184 4 s >= -",
            "datetime_minmax_multi_ops brin 1082 1184 5 s > -",
            "datetime_minmax_multi_ops brin 1114 1082 1 s < -",
            "datetime_minmax_multi_ops brin 1114 1082 2 s <= -",
            "datetime_minmax_multi_ops brin 1114 1082 3 s = -",
            "datetime_minmax_multi_ops brin 1114 1082 4 s >= -",
            "datetime_minmax_multi_ops brin 1114 1082 5 s > -",
            "datetime_minmax_multi_ops brin 1114 1114 1 s < -",
            "datetime_minmax_multi_ops brin 1114 1114 2 s <= -",
            "datetime_minmax_multi_ops brin 1114 1114 3 s = -",
            "datetime_minmax_multi_ops brin 1114 1114 4 s >= -",
            "datetime_minmax_multi_ops brin 1114 1114 5 s > -",
            "datetime_minmax_multi_ops brin 1114 1184 1 s < -",
            "datetime_minmax_multi_ops brin 1114 1184 2 s <= -",
            "datetime_minmax_multi_ops brin 1114 1184 3 s = -",
            "datetime_minmax_multi_ops brin 1114 1184 4 s >= -",
            "datetime_minmax_multi_ops brin 1114 1184 5 s > -",
            "datetime_minmax_multi_ops brin 1184 1082 1 s < -",
            "datetime_minmax_multi_ops brin 1184 1082 2 s <= -",
            "datetime_minmax_multi_ops brin 1184 1082 3 s = -",
            "datetime_minmax_multi_ops brin 1184 1082 4 s >= -",
            "datetime_minmax_multi_ops brin 1184 1082 5 s > -",
            "datetime_minmax_multi_ops brin 1184 1114 1 s < -",
            "datetime_minmax_multi_ops brin 1184 1114 2 s <= -",
            "datetime_minmax_multi_ops brin 1184 1114 3 s = -",
            "datetime_minmax_multi_ops brin 1184 1114 4 s >= -",
            "datetime_minmax_multi_ops brin 1184 1114 5 s > -",
            "datetime_minmax_multi_ops brin 1184 1184 1 s < -",
            "datetime_minmax_multi_ops brin 1184 1184 2 s <= -",
            "datetime_minmax_multi_ops brin 1184 1184 3 s = -",
            "datetime_minmax_multi_ops brin 1184 1184 4 s >= -",
            "datetime_minmax_multi_ops brin 1184 1184 5 s > -",
            // datetime_minmax_ops / brin
            "datetime_minmax_ops brin 1082 1082 1 s < -",
            "datetime_minmax_ops brin 1082 1082 2 s <= -",
            "datetime_minmax_ops brin 1082 1082 3 s = -",
            "datetime_minmax_ops brin 1082 1082 4 s >= -",
            "datetime_minmax_ops brin 1082 1082 5 s > -",
            "datetime_minmax_ops brin 1082 1114 1 s < -",
            "datetime_minmax_ops brin 1082 1114 2 s <= -",
            "datetime_minmax_ops brin 1082 1114 3 s = -",
            "datetime_minmax_ops brin 1082 1114 4 s >= -",
            "datetime_minmax_ops brin 1082 1114 5 s > -",
            "datetime_minmax_ops brin 1082 1184 1 s < -",
            "datetime_minmax_ops brin 1082 1184 2 s <= -",
            "datetime_minmax_ops brin 1082 1184 3 s = -",
            "datetime_minmax_ops brin 1082 1184 4 s >= -",
            "datetime_minmax_ops brin 1082 1184 5 s > -",
            "datetime_minmax_ops brin 1114 1082 1 s < -",
            "datetime_minmax_ops brin 1114 1082 2 s <= -",
            "datetime_minmax_ops brin 1114 1082 3 s = -",
            "datetime_minmax_ops brin 1114 1082 4 s >= -",
            "datetime_minmax_ops brin 1114 1082 5 s > -",
            "datetime_minmax_ops brin 1114 1114 1 s < -",
            "datetime_minmax_ops brin 1114 1114 2 s <= -",
            "datetime_minmax_ops brin 1114 1114 3 s = -",
            "datetime_minmax_ops brin 1114 1114 4 s >= -",
            "datetime_minmax_ops brin 1114 1114 5 s > -",
            "datetime_minmax_ops brin 1114 1184 1 s < -",
            "datetime_minmax_ops brin 1114 1184 2 s <= -",
            "datetime_minmax_ops brin 1114 1184 3 s = -",
            "datetime_minmax_ops brin 1114 1184 4 s >= -",
            "datetime_minmax_ops brin 1114 1184 5 s > -",
            "datetime_minmax_ops brin 1184 1082 1 s < -",
            "datetime_minmax_ops brin 1184 1082 2 s <= -",
            "datetime_minmax_ops brin 1184 1082 3 s = -",
            "datetime_minmax_ops brin 1184 1082 4 s >= -",
            "datetime_minmax_ops brin 1184 1082 5 s > -",
            "datetime_minmax_ops brin 1184 1114 1 s < -",
            "datetime_minmax_ops brin 1184 1114 2 s <= -",
            "datetime_minmax_ops brin 1184 1114 3 s = -",
            "datetime_minmax_ops brin 1184 1114 4 s >= -",
            "datetime_minmax_ops brin 1184 1114 5 s > -",
            "datetime_minmax_ops brin 1184 1184 1 s < -",
            "datetime_minmax_ops brin 1184 1184 2 s <= -",
            "datetime_minmax_ops brin 1184 1184 3 s = -",
            "datetime_minmax_ops brin 1184 1184 4 s >= -",
            "datetime_minmax_ops brin 1184 1184 5 s > -",
            // datetime_ops / btree
            "datetime_ops btree 1082 1082 1 s < -",
            "datetime_ops btree 1082 1082 2 s <= -",
            "datetime_ops btree 1082 1082 3 s = -",
            "datetime_ops btree 1082 1082 4 s >= -",
            "datetime_ops btree 1082 1082 5 s > -",
            "datetime_ops btree 1082 1114 1 s < -",
            "datetime_ops btree 1082 1114 2 s <= -",
            "datetime_ops btree 1082 1114 3 s = -",
            "datetime_ops btree 1082 1114 4 s >= -",
            "datetime_ops btree 1082 1114 5 s > -",
            "datetime_ops btree 1082 1184 1 s < -",
            "datetime_ops btree 1082 1184 2 s <= -",
            "datetime_ops btree 1082 1184 3 s = -",
            "datetime_ops btree 1082 1184 4 s >= -",
            "datetime_ops btree 1082 1184 5 s > -",
            "datetime_ops btree 1114 1082 1 s < -",
            "datetime_ops btree 1114 1082 2 s <= -",
            "datetime_ops btree 1114 1082 3 s = -",
            "datetime_ops btree 1114 1082 4 s >= -",
            "datetime_ops btree 1114 1082 5 s > -",
            "datetime_ops btree 1114 1114 1 s < -",
            "datetime_ops btree 1114 1114 2 s <= -",
            "datetime_ops btree 1114 1114 3 s = -",
            "datetime_ops btree 1114 1114 4 s >= -",
            "datetime_ops btree 1114 1114 5 s > -",
            "datetime_ops btree 1114 1184 1 s < -",
            "datetime_ops btree 1114 1184 2 s <= -",
            "datetime_ops btree 1114 1184 3 s = -",
            "datetime_ops btree 1114 1184 4 s >= -",
            "datetime_ops btree 1114 1184 5 s > -",
            "datetime_ops btree 1184 1082 1 s < -",
            "datetime_ops btree 1184 1082 2 s <= -",
            "datetime_ops btree 1184 1082 3 s = -",
            "datetime_ops btree 1184 1082 4 s >= -",
            "datetime_ops btree 1184 1082 5 s > -",
            "datetime_ops btree 1184 1114 1 s < -",
            "datetime_ops btree 1184 1114 2 s <= -",
            "datetime_ops btree 1184 1114 3 s = -",
            "datetime_ops btree 1184 1114 4 s >= -",
            "datetime_ops btree 1184 1114 5 s > -",
            "datetime_ops btree 1184 1184 1 s < -",
            "datetime_ops btree 1184 1184 2 s <= -",
            "datetime_ops btree 1184 1184 3 s = -",
            "datetime_ops btree 1184 1184 4 s >= -",
            "datetime_ops btree 1184 1184 5 s > -",
            // enum_ops / btree
            "enum_ops btree 3500 3500 1 s < -",
            "enum_ops btree 3500 3500 2 s <= -",
            "enum_ops btree 3500 3500 3 s = -",
            "enum_ops btree 3500 3500 4 s >= -",
            "enum_ops btree 3500 3500 5 s > -",
            // enum_ops / hash
            "enum_ops hash 3500 3500 1 s = -",
            // float_bloom_ops / brin
            "float_bloom_ops brin 700 700 1 s = -",
            "float_bloom_ops brin 701 701 1 s = -",
            // float_minmax_multi_ops / brin
            "float_minmax_multi_ops brin 700 700 1 s < -",
            "float_minmax_multi_ops brin 700 700 2 s <= -",
            "float_minmax_multi_ops brin 700 700 3 s = -",
            "float_minmax_multi_ops brin 700 700 4 s >= -",
            "float_minmax_multi_ops brin 700 700 5 s > -",
            "float_minmax_multi_ops brin 700 701 1 s < -",
            "float_minmax_multi_ops brin 700 701 2 s <= -",
            "float_minmax_multi_ops brin 700 701 3 s = -",
            "float_minmax_multi_ops brin 700 701 4 s >= -",
            "float_minmax_multi_ops brin 700 701 5 s > -",
            "float_minmax_multi_ops brin 701 700 1 s < -",
            "float_minmax_multi_ops brin 701 700 2 s <= -",
            "float_minmax_multi_ops brin 701 700 3 s = -",
            "float_minmax_multi_ops brin 701 700 4 s >= -",
            "float_minmax_multi_ops brin 701 700 5 s > -",
            "float_minmax_multi_ops brin 701 701 1 s < -",
            "float_minmax_multi_ops brin 701 701 2 s <= -",
            "float_minmax_multi_ops brin 701 701 3 s = -",
            "float_minmax_multi_ops brin 701 701 4 s >= -",
            "float_minmax_multi_ops brin 701 701 5 s > -",
            // float_minmax_ops / brin
            "float_minmax_ops brin 700 700 1 s < -",
            "float_minmax_ops brin 700 700 2 s <= -",
            "float_minmax_ops brin 700 700 3 s = -",
            "float_minmax_ops brin 700 700 4 s >= -",
            "float_minmax_ops brin 700 700 5 s > -",
            "float_minmax_ops brin 700 701 1 s < -",
            "float_minmax_ops brin 700 701 2 s <= -",
            "float_minmax_ops brin 700 701 3 s = -",
            "float_minmax_ops brin 700 701 4 s >= -",
            "float_minmax_ops brin 700 701 5 s > -",
            "float_minmax_ops brin 701 700 1 s < -",
            "float_minmax_ops brin 701 700 2 s <= -",
            "float_minmax_ops brin 701 700 3 s = -",
            "float_minmax_ops brin 701 700 4 s >= -",
            "float_minmax_ops brin 701 700 5 s > -",
            "float_minmax_ops brin 701 701 1 s < -",
            "float_minmax_ops brin 701 701 2 s <= -",
            "float_minmax_ops brin 701 701 3 s = -",
            "float_minmax_ops brin 701 701 4 s >= -",
            "float_minmax_ops brin 701 701 5 s > -",
            // float_ops / btree
            "float_ops btree 700 700 1 s < -",
            "float_ops btree 700 700 2 s <= -",
            "float_ops btree 700 700 3 s = -",
            "float_ops btree 700 700 4 s >= -",
            "float_ops btree 700 700 5 s > -",
            "float_ops btree 700 701 1 s < -",
            "float_ops btree 700 701 2 s <= -",
            "float_ops btree 700 701 3 s = -",
            "float_ops btree 700 701 4 s >= -",
            "float_ops btree 700 701 5 s > -",
            "float_ops btree 701 700 1 s < -",
            "float_ops btree 701 700 2 s <= -",
            "float_ops btree 701 700 3 s = -",
            "float_ops btree 701 700 4 s >= -",
            "float_ops btree 701 700 5 s > -",
            "float_ops btree 701 701 1 s < -",
            "float_ops btree 701 701 2 s <= -",
            "float_ops btree 701 701 3 s = -",
            "float_ops btree 701 701 4 s >= -",
            "float_ops btree 701 701 5 s > -",
            // float_ops / hash
            "float_ops hash 700 700 1 s = -",
            "float_ops hash 700 701 1 s = -",
            "float_ops hash 701 700 1 s = -",
            "float_ops hash 701 701 1 s = -",
            // integer_bloom_ops / brin
            "integer_bloom_ops brin 20 20 1 s = -",
            "integer_bloom_ops brin 21 21 1 s = -",
            "integer_bloom_ops brin 23 23 1 s = -",
            // integer_minmax_multi_ops / brin
            "integer_minmax_multi_ops brin 20 20 1 s < -",
            "integer_minmax_multi_ops brin 20 20 2 s <= -",
            "integer_minmax_multi_ops brin 20 20 3 s = -",
            "integer_minmax_multi_ops brin 20 20 4 s >= -",
            "integer_minmax_multi_ops brin 20 20 5 s > -",
            "integer_minmax_multi_ops brin 20 21 1 s < -",
            "integer_minmax_multi_ops brin 20 21 2 s <= -",
            "integer_minmax_multi_ops brin 20 21 3 s = -",
            "integer_minmax_multi_ops brin 20 21 4 s >= -",
            "integer_minmax_multi_ops brin 20 21 5 s > -",
            "integer_minmax_multi_ops brin 20 23 1 s < -",
            "integer_minmax_multi_ops brin 20 23 2 s <= -",
            "integer_minmax_multi_ops brin 20 23 3 s = -",
            "integer_minmax_multi_ops brin 20 23 4 s >= -",
            "integer_minmax_multi_ops brin 20 23 5 s > -",
            "integer_minmax_multi_ops brin 21 20 1 s < -",
            "integer_minmax_multi_ops brin 21 20 2 s <= -",
            "integer_minmax_multi_ops brin 21 20 3 s = -",
            "integer_minmax_multi_ops brin 21 20 4 s >= -",
            "integer_minmax_multi_ops brin 21 20 5 s > -",
            "integer_minmax_multi_ops brin 21 21 1 s < -",
            "integer_minmax_multi_ops brin 21 21 2 s <= -",
            "integer_minmax_multi_ops brin 21 21 3 s = -",
            "integer_minmax_multi_ops brin 21 21 4 s >= -",
            "integer_minmax_multi_ops brin 21 21 5 s > -",
            "integer_minmax_multi_ops brin 21 23 1 s < -",
            "integer_minmax_multi_ops brin 21 23 2 s <= -",
            "integer_minmax_multi_ops brin 21 23 3 s = -",
            "integer_minmax_multi_ops brin 21 23 4 s >= -",
            "integer_minmax_multi_ops brin 21 23 5 s > -",
            "integer_minmax_multi_ops brin 23 20 1 s < -",
            "integer_minmax_multi_ops brin 23 20 2 s <= -",
            "integer_minmax_multi_ops brin 23 20 3 s = -",
            "integer_minmax_multi_ops brin 23 20 4 s >= -",
            "integer_minmax_multi_ops brin 23 20 5 s > -",
            "integer_minmax_multi_ops brin 23 21 1 s < -",
            "integer_minmax_multi_ops brin 23 21 2 s <= -",
            "integer_minmax_multi_ops brin 23 21 3 s = -",
            "integer_minmax_multi_ops brin 23 21 4 s >= -",
            "integer_minmax_multi_ops brin 23 21 5 s > -",
            "integer_minmax_multi_ops brin 23 23 1 s < -",
            "integer_minmax_multi_ops brin 23 23 2 s <= -",
            "integer_minmax_multi_ops brin 23 23 3 s = -",
            "integer_minmax_multi_ops brin 23 23 4 s >= -",
            "integer_minmax_multi_ops brin 23 23 5 s > -",
            // integer_minmax_ops / brin
            "integer_minmax_ops brin 20 20 1 s < -",
            "integer_minmax_ops brin 20 20 2 s <= -",
            "integer_minmax_ops brin 20 20 3 s = -",
            "integer_minmax_ops brin 20 20 4 s >= -",
            "integer_minmax_ops brin 20 20 5 s > -",
            "integer_minmax_ops brin 20 21 1 s < -",
            "integer_minmax_ops brin 20 21 2 s <= -",
            "integer_minmax_ops brin 20 21 3 s = -",
            "integer_minmax_ops brin 20 21 4 s >= -",
            "integer_minmax_ops brin 20 21 5 s > -",
            "integer_minmax_ops brin 20 23 1 s < -",
            "integer_minmax_ops brin 20 23 2 s <= -",
            "integer_minmax_ops brin 20 23 3 s = -",
            "integer_minmax_ops brin 20 23 4 s >= -",
            "integer_minmax_ops brin 20 23 5 s > -",
            "integer_minmax_ops brin 21 20 1 s < -",
            "integer_minmax_ops brin 21 20 2 s <= -",
            "integer_minmax_ops brin 21 20 3 s = -",
            "integer_minmax_ops brin 21 20 4 s >= -",
            "integer_minmax_ops brin 21 20 5 s > -",
            "integer_minmax_ops brin 21 21 1 s < -",
            "integer_minmax_ops brin 21 21 2 s <= -",
            "integer_minmax_ops brin 21 21 3 s = -",
            "integer_minmax_ops brin 21 21 4 s >= -",
            "integer_minmax_ops brin 21 21 5 s > -",
            "integer_minmax_ops brin 21 23 1 s < -",
            "integer_minmax_ops brin 21 23 2 s <= -",
            "integer_minmax_ops brin 21 23 3 s = -",
            "integer_minmax_ops brin 21 23 4 s >= -",
            "integer_minmax_ops brin 21 23 5 s > -",
            "integer_minmax_ops brin 23 20 1 s < -",
            "integer_minmax_ops brin 23 20 2 s <= -",
            "integer_minmax_ops brin 23 20 3 s = -",
            "integer_minmax_ops brin 23 20 4 s >= -",
            "integer_minmax_ops brin 23 20 5 s > -",
            "integer_minmax_ops brin 23 21 1 s < -",
            "integer_minmax_ops brin 23 21 2 s <= -",
            "integer_minmax_ops brin 23 21 3 s = -",
            "integer_minmax_ops brin 23 21 4 s >= -",
            "integer_minmax_ops brin 23 21 5 s > -",
            "integer_minmax_ops brin 23 23 1 s < -",
            "integer_minmax_ops brin 23 23 2 s <= -",
            "integer_minmax_ops brin 23 23 3 s = -",
            "integer_minmax_ops brin 23 23 4 s >= -",
            "integer_minmax_ops brin 23 23 5 s > -",
            // integer_ops / btree
            "integer_ops btree 20 20 1 s < -",
            "integer_ops btree 20 20 2 s <= -",
            "integer_ops btree 20 20 3 s = -",
            "integer_ops btree 20 20 4 s >= -",
            "integer_ops btree 20 20 5 s > -",
            "integer_ops btree 20 21 1 s < -",
            "integer_ops btree 20 21 2 s <= -",
            "integer_ops btree 20 21 3 s = -",
            "integer_ops btree 20 21 4 s >= -",
            "integer_ops btree 20 21 5 s > -",
            "integer_ops btree 20 23 1 s < -",
            "integer_ops btree 20 23 2 s <= -",
            "integer_ops btree 20 23 3 s = -",
            "integer_ops btree 20 23 4 s >= -",
            "integer_ops btree 20 23 5 s > -",
            "integer_ops btree 21 20 1 s < -",
            "integer_ops btree 21 20 2 s <= -",
            "integer_ops btree 21 20 3 s = -",
            "integer_ops btree 21 20 4 s >= -",
            "integer_ops btree 21 20 5 s > -",
            "integer_ops btree 21 21 1 s < -",
            "integer_ops btree 21 21 2 s <= -",
            "integer_ops btree 21 21 3 s = -",
            "integer_ops btree 21 21 4 s >= -",
            "integer_ops btree 21 21 5 s > -",
            "integer_ops btree 21 23 1 s < -",
            "integer_ops btree 21 23 2 s <= -",
            "integer_ops btree 21 23 3 s = -",
            "integer_ops btree 21 23 4 s >= -",
            "integer_ops btree 21 23 5 s > -",
            "integer_ops btree 23 20 1 s < -",
            "integer_ops btree 23 20 2 s <= -",
            "integer_ops btree 23 20 3 s = -",
            "integer_ops btree 23 20 4 s >= -",
            "integer_ops btree 23 20 5 s > -",
            "integer_ops btree 23 21 1 s < -",
            "integer_ops btree 23 21 2 s <= -",
            "integer_ops btree 23 21 3 s = -",
            "integer_ops btree 23 21 4 s >= -",
            "integer_ops btree 23 21 5 s > -",
            "integer_ops btree 23 23 1 s < -",
            "integer_ops btree 23 23 2 s <= -",
            "integer_ops btree 23 23 3 s = -",
            "integer_ops btree 23 23 4 s >= -",
            "integer_ops btree 23 23 5 s > -",
            // integer_ops / hash
            "integer_ops hash 20 20 1 s = -",
            "integer_ops hash 20 21 1 s = -",
            "integer_ops hash 20 23 1 s = -",
            "integer_ops hash 21 20 1 s = -",
            "integer_ops hash 21 21 1 s = -",
            "integer_ops hash 21 23 1 s = -",
            "integer_ops hash 23 20 1 s = -",
            "integer_ops hash 23 21 1 s = -",
            "integer_ops hash 23 23 1 s = -",
            // interval_bloom_ops / brin
            "interval_bloom_ops brin 1186 1186 1 s = -",
            // interval_minmax_multi_ops / brin
            "interval_minmax_multi_ops brin 1186 1186 1 s < -",
            "interval_minmax_multi_ops brin 1186 1186 2 s <= -",
            "interval_minmax_multi_ops brin 1186 1186 3 s = -",
            "interval_minmax_multi_ops brin 1186 1186 4 s >= -",
            "interval_minmax_multi_ops brin 1186 1186 5 s > -",
            // interval_minmax_ops / brin
            "interval_minmax_ops brin 1186 1186 1 s < -",
            "interval_minmax_ops brin 1186 1186 2 s <= -",
            "interval_minmax_ops brin 1186 1186 3 s = -",
            "interval_minmax_ops brin 1186 1186 4 s >= -",
            "interval_minmax_ops brin 1186 1186 5 s > -",
            // interval_ops / btree
            "interval_ops btree 1186 1186 1 s < -",
            "interval_ops btree 1186 1186 2 s <= -",
            "interval_ops btree 1186 1186 3 s = -",
            "interval_ops btree 1186 1186 4 s >= -",
            "interval_ops btree 1186 1186 5 s > -",
            // interval_ops / hash
            "interval_ops hash 1186 1186 1 s = -",
            // jsonb_ops / btree
            "jsonb_ops btree 3802 3802 1 s < -",
            "jsonb_ops btree 3802 3802 2 s <= -",
            "jsonb_ops btree 3802 3802 3 s = -",
            "jsonb_ops btree 3802 3802 4 s >= -",
            "jsonb_ops btree 3802 3802 5 s > -",
            // jsonb_ops / gin
            "jsonb_ops gin 3802 25 9 s ? -",
            "jsonb_ops gin 3802 1009 10 s ?| -",
            "jsonb_ops gin 3802 1009 11 s ?& -",
            "jsonb_ops gin 3802 3802 7 s @> -",
            "jsonb_ops gin 3802 4072 15 s @? -",
            "jsonb_ops gin 3802 4072 16 s @@ -",
            // jsonb_ops / hash
            "jsonb_ops hash 3802 3802 1 s = -",
            // jsonb_path_ops / gin
            "jsonb_path_ops gin 3802 3802 7 s @> -",
            "jsonb_path_ops gin 3802 4072 15 s @? -",
            "jsonb_path_ops gin 3802 4072 16 s @@ -",
            // kd_point_ops / spgist
            "kd_point_ops spgist 600 600 1 s << -",
            "kd_point_ops spgist 600 600 5 s >> -",
            "kd_point_ops spgist 600 600 6 s ~= -",
            "kd_point_ops spgist 600 600 10 s <<| -",
            "kd_point_ops spgist 600 600 11 s |>> -",
            "kd_point_ops spgist 600 600 15 o <-> float_ops",
            "kd_point_ops spgist 600 600 29 s <^ -",
            "kd_point_ops spgist 600 600 30 s >^ -",
            "kd_point_ops spgist 600 603 8 s <@ -",
            // macaddr8_bloom_ops / brin
            "macaddr8_bloom_ops brin 774 774 1 s = -",
            // macaddr8_minmax_multi_ops / brin
            "macaddr8_minmax_multi_ops brin 774 774 1 s < -",
            "macaddr8_minmax_multi_ops brin 774 774 2 s <= -",
            "macaddr8_minmax_multi_ops brin 774 774 3 s = -",
            "macaddr8_minmax_multi_ops brin 774 774 4 s >= -",
            "macaddr8_minmax_multi_ops brin 774 774 5 s > -",
            // macaddr8_minmax_ops / brin
            "macaddr8_minmax_ops brin 774 774 1 s < -",
            "macaddr8_minmax_ops brin 774 774 2 s <= -",
            "macaddr8_minmax_ops brin 774 774 3 s = -",
            "macaddr8_minmax_ops brin 774 774 4 s >= -",
            "macaddr8_minmax_ops brin 774 774 5 s > -",
            // macaddr8_ops / btree
            "macaddr8_ops btree 774 774 1 s < -",
            "macaddr8_ops btree 774 774 2 s <= -",
            "macaddr8_ops btree 774 774 3 s = -",
            "macaddr8_ops btree 774 774 4 s >= -",
            "macaddr8_ops btree 774 774 5 s > -",
            // macaddr8_ops / hash
            "macaddr8_ops hash 774 774 1 s = -",
            // macaddr_bloom_ops / brin
            "macaddr_bloom_ops brin 829 829 1 s = -",
            // macaddr_minmax_multi_ops / brin
            "macaddr_minmax_multi_ops brin 829 829 1 s < -",
            "macaddr_minmax_multi_ops brin 829 829 2 s <= -",
            "macaddr_minmax_multi_ops brin 829 829 3 s = -",
            "macaddr_minmax_multi_ops brin 829 829 4 s >= -",
            "macaddr_minmax_multi_ops brin 829 829 5 s > -",
            // macaddr_minmax_ops / brin
            "macaddr_minmax_ops brin 829 829 1 s < -",
            "macaddr_minmax_ops brin 829 829 2 s <= -",
            "macaddr_minmax_ops brin 829 829 3 s = -",
            "macaddr_minmax_ops brin 829 829 4 s >= -",
            "macaddr_minmax_ops brin 829 829 5 s > -",
            // macaddr_ops / btree
            "macaddr_ops btree 829 829 1 s < -",
            "macaddr_ops btree 829 829 2 s <= -",
            "macaddr_ops btree 829 829 3 s = -",
            "macaddr_ops btree 829 829 4 s >= -",
            "macaddr_ops btree 829 829 5 s > -",
            // macaddr_ops / hash
            "macaddr_ops hash 829 829 1 s = -",
            // money_ops / btree
            "money_ops btree 790 790 1 s < -",
            "money_ops btree 790 790 2 s <= -",
            "money_ops btree 790 790 3 s = -",
            "money_ops btree 790 790 4 s >= -",
            "money_ops btree 790 790 5 s > -",
            // multirange_ops / btree
            "multirange_ops btree 4537 4537 1 s < -",
            "multirange_ops btree 4537 4537 2 s <= -",
            "multirange_ops btree 4537 4537 3 s = -",
            "multirange_ops btree 4537 4537 4 s >= -",
            "multirange_ops btree 4537 4537 5 s > -",
            // multirange_ops / gist
            "multirange_ops gist 4537 2283 16 s @> -",
            "multirange_ops gist 4537 3831 1 s << -",
            "multirange_ops gist 4537 3831 2 s &< -",
            "multirange_ops gist 4537 3831 3 s && -",
            "multirange_ops gist 4537 3831 4 s &> -",
            "multirange_ops gist 4537 3831 5 s >> -",
            "multirange_ops gist 4537 3831 6 s -|- -",
            "multirange_ops gist 4537 3831 7 s @> -",
            "multirange_ops gist 4537 3831 8 s <@ -",
            "multirange_ops gist 4537 4537 1 s << -",
            "multirange_ops gist 4537 4537 2 s &< -",
            "multirange_ops gist 4537 4537 3 s && -",
            "multirange_ops gist 4537 4537 4 s &> -",
            "multirange_ops gist 4537 4537 5 s >> -",
            "multirange_ops gist 4537 4537 6 s -|- -",
            "multirange_ops gist 4537 4537 7 s @> -",
            "multirange_ops gist 4537 4537 8 s <@ -",
            "multirange_ops gist 4537 4537 18 s = -",
            // multirange_ops / hash
            "multirange_ops hash 4537 4537 1 s = -",
            // name_bloom_ops / brin
            "name_bloom_ops brin 19 19 1 s = -",
            // name_minmax_ops / brin
            "name_minmax_ops brin 19 19 1 s < -",
            "name_minmax_ops brin 19 19 2 s <= -",
            "name_minmax_ops brin 19 19 3 s = -",
            "name_minmax_ops brin 19 19 4 s >= -",
            "name_minmax_ops brin 19 19 5 s > -",
            // network_bloom_ops / brin
            "network_bloom_ops brin 869 869 1 s = -",
            // network_inclusion_ops / brin
            "network_inclusion_ops brin 869 869 3 s && -",
            "network_inclusion_ops brin 869 869 7 s >>= -",
            "network_inclusion_ops brin 869 869 8 s <<= -",
            "network_inclusion_ops brin 869 869 18 s = -",
            "network_inclusion_ops brin 869 869 24 s >> -",
            "network_inclusion_ops brin 869 869 26 s << -",
            // network_minmax_multi_ops / brin
            "network_minmax_multi_ops brin 869 869 1 s < -",
            "network_minmax_multi_ops brin 869 869 2 s <= -",
            "network_minmax_multi_ops brin 869 869 3 s = -",
            "network_minmax_multi_ops brin 869 869 4 s >= -",
            "network_minmax_multi_ops brin 869 869 5 s > -",
            // network_minmax_ops / brin
            "network_minmax_ops brin 869 869 1 s < -",
            "network_minmax_ops brin 869 869 2 s <= -",
            "network_minmax_ops brin 869 869 3 s = -",
            "network_minmax_ops brin 869 869 4 s >= -",
            "network_minmax_ops brin 869 869 5 s > -",
            // network_ops / btree
            "network_ops btree 869 869 1 s < -",
            "network_ops btree 869 869 2 s <= -",
            "network_ops btree 869 869 3 s = -",
            "network_ops btree 869 869 4 s >= -",
            "network_ops btree 869 869 5 s > -",
            // network_ops / gist
            "network_ops gist 869 869 3 s && -",
            "network_ops gist 869 869 18 s = -",
            "network_ops gist 869 869 19 s <> -",
            "network_ops gist 869 869 20 s < -",
            "network_ops gist 869 869 21 s <= -",
            "network_ops gist 869 869 22 s > -",
            "network_ops gist 869 869 23 s >= -",
            "network_ops gist 869 869 24 s << -",
            "network_ops gist 869 869 25 s <<= -",
            "network_ops gist 869 869 26 s >> -",
            "network_ops gist 869 869 27 s >>= -",
            // network_ops / hash
            "network_ops hash 869 869 1 s = -",
            // network_ops / spgist
            "network_ops spgist 869 869 3 s && -",
            "network_ops spgist 869 869 18 s = -",
            "network_ops spgist 869 869 19 s <> -",
            "network_ops spgist 869 869 20 s < -",
            "network_ops spgist 869 869 21 s <= -",
            "network_ops spgist 869 869 22 s > -",
            "network_ops spgist 869 869 23 s >= -",
            "network_ops spgist 869 869 24 s << -",
            "network_ops spgist 869 869 25 s <<= -",
            "network_ops spgist 869 869 26 s >> -",
            "network_ops spgist 869 869 27 s >>= -",
            // numeric_bloom_ops / brin
            "numeric_bloom_ops brin 1700 1700 1 s = -",
            // numeric_minmax_multi_ops / brin
            "numeric_minmax_multi_ops brin 1700 1700 1 s < -",
            "numeric_minmax_multi_ops brin 1700 1700 2 s <= -",
            "numeric_minmax_multi_ops brin 1700 1700 3 s = -",
            "numeric_minmax_multi_ops brin 1700 1700 4 s >= -",
            "numeric_minmax_multi_ops brin 1700 1700 5 s > -",
            // numeric_minmax_ops / brin
            "numeric_minmax_ops brin 1700 1700 1 s < -",
            "numeric_minmax_ops brin 1700 1700 2 s <= -",
            "numeric_minmax_ops brin 1700 1700 3 s = -",
            "numeric_minmax_ops brin 1700 1700 4 s >= -",
            "numeric_minmax_ops brin 1700 1700 5 s > -",
            // numeric_ops / btree
            "numeric_ops btree 1700 1700 1 s < -",
            "numeric_ops btree 1700 1700 2 s <= -",
            "numeric_ops btree 1700 1700 3 s = -",
            "numeric_ops btree 1700 1700 4 s >= -",
            "numeric_ops btree 1700 1700 5 s > -",
            // numeric_ops / hash
            "numeric_ops hash 1700 1700 1 s = -",
            // oid_bloom_ops / brin
            "oid_bloom_ops brin 26 26 1 s = -",
            // oid_minmax_multi_ops / brin
            "oid_minmax_multi_ops brin 26 26 1 s < -",
            "oid_minmax_multi_ops brin 26 26 2 s <= -",
            "oid_minmax_multi_ops brin 26 26 3 s = -",
            "oid_minmax_multi_ops brin 26 26 4 s >= -",
            "oid_minmax_multi_ops brin 26 26 5 s > -",
            // oid_minmax_ops / brin
            "oid_minmax_ops brin 26 26 1 s < -",
            "oid_minmax_ops brin 26 26 2 s <= -",
            "oid_minmax_ops brin 26 26 3 s = -",
            "oid_minmax_ops brin 26 26 4 s >= -",
            "oid_minmax_ops brin 26 26 5 s > -",
            // oid_ops / btree
            "oid_ops btree 26 26 1 s < -",
            "oid_ops btree 26 26 2 s <= -",
            "oid_ops btree 26 26 3 s = -",
            "oid_ops btree 26 26 4 s >= -",
            "oid_ops btree 26 26 5 s > -",
            // oid_ops / hash
            "oid_ops hash 26 26 1 s = -",
            // oidvector_ops / btree
            "oidvector_ops btree 30 30 1 s < -",
            "oidvector_ops btree 30 30 2 s <= -",
            "oidvector_ops btree 30 30 3 s = -",
            "oidvector_ops btree 30 30 4 s >= -",
            "oidvector_ops btree 30 30 5 s > -",
            // oidvector_ops / hash
            "oidvector_ops hash 30 30 1 s = -",
            // pg_lsn_bloom_ops / brin
            "pg_lsn_bloom_ops brin 3220 3220 1 s = -",
            // pg_lsn_minmax_multi_ops / brin
            "pg_lsn_minmax_multi_ops brin 3220 3220 1 s < -",
            "pg_lsn_minmax_multi_ops brin 3220 3220 2 s <= -",
            "pg_lsn_minmax_multi_ops brin 3220 3220 3 s = -",
            "pg_lsn_minmax_multi_ops brin 3220 3220 4 s >= -",
            "pg_lsn_minmax_multi_ops brin 3220 3220 5 s > -",
            // pg_lsn_minmax_ops / brin
            "pg_lsn_minmax_ops brin 3220 3220 1 s < -",
            "pg_lsn_minmax_ops brin 3220 3220 2 s <= -",
            "pg_lsn_minmax_ops brin 3220 3220 3 s = -",
            "pg_lsn_minmax_ops brin 3220 3220 4 s >= -",
            "pg_lsn_minmax_ops brin 3220 3220 5 s > -",
            // pg_lsn_ops / btree
            "pg_lsn_ops btree 3220 3220 1 s < -",
            "pg_lsn_ops btree 3220 3220 2 s <= -",
            "pg_lsn_ops btree 3220 3220 3 s = -",
            "pg_lsn_ops btree 3220 3220 4 s >= -",
            "pg_lsn_ops btree 3220 3220 5 s > -",
            // pg_lsn_ops / hash
            "pg_lsn_ops hash 3220 3220 1 s = -",
            // point_ops / gist
            "point_ops gist 600 600 1 s << -",
            "point_ops gist 600 600 5 s >> -",
            "point_ops gist 600 600 6 s ~= -",
            "point_ops gist 600 600 10 s <<| -",
            "point_ops gist 600 600 11 s |>> -",
            "point_ops gist 600 600 15 o <-> float_ops",
            "point_ops gist 600 600 29 s <^ -",
            "point_ops gist 600 600 30 s >^ -",
            "point_ops gist 600 603 28 s <@ -",
            "point_ops gist 600 604 48 s <@ -",
            "point_ops gist 600 718 68 s <@ -",
            // poly_ops / gist
            "poly_ops gist 604 600 15 o <-> float_ops",
            "poly_ops gist 604 604 1 s << -",
            "poly_ops gist 604 604 2 s &< -",
            "poly_ops gist 604 604 3 s && -",
            "poly_ops gist 604 604 4 s &> -",
            "poly_ops gist 604 604 5 s >> -",
            "poly_ops gist 604 604 6 s ~= -",
            "poly_ops gist 604 604 7 s @> -",
            "poly_ops gist 604 604 8 s <@ -",
            "poly_ops gist 604 604 9 s &<| -",
            "poly_ops gist 604 604 10 s <<| -",
            "poly_ops gist 604 604 11 s |>> -",
            "poly_ops gist 604 604 12 s |&> -",
            // poly_ops / spgist
            "poly_ops spgist 604 600 15 o <-> float_ops",
            "poly_ops spgist 604 604 1 s << -",
            "poly_ops spgist 604 604 2 s &< -",
            "poly_ops spgist 604 604 3 s && -",
            "poly_ops spgist 604 604 4 s &> -",
            "poly_ops spgist 604 604 5 s >> -",
            "poly_ops spgist 604 604 6 s ~= -",
            "poly_ops spgist 604 604 7 s @> -",
            "poly_ops spgist 604 604 8 s <@ -",
            "poly_ops spgist 604 604 9 s &<| -",
            "poly_ops spgist 604 604 10 s <<| -",
            "poly_ops spgist 604 604 11 s |>> -",
            "poly_ops spgist 604 604 12 s |&> -",
            // quad_point_ops / spgist
            "quad_point_ops spgist 600 600 1 s << -",
            "quad_point_ops spgist 600 600 5 s >> -",
            "quad_point_ops spgist 600 600 6 s ~= -",
            "quad_point_ops spgist 600 600 10 s <<| -",
            "quad_point_ops spgist 600 600 11 s |>> -",
            "quad_point_ops spgist 600 600 15 o <-> float_ops",
            "quad_point_ops spgist 600 600 29 s <^ -",
            "quad_point_ops spgist 600 600 30 s >^ -",
            "quad_point_ops spgist 600 603 8 s <@ -",
            // range_inclusion_ops / brin
            "range_inclusion_ops brin 3831 2283 16 s @> -",
            "range_inclusion_ops brin 3831 3831 1 s << -",
            "range_inclusion_ops brin 3831 3831 2 s &< -",
            "range_inclusion_ops brin 3831 3831 3 s && -",
            "range_inclusion_ops brin 3831 3831 4 s &> -",
            "range_inclusion_ops brin 3831 3831 5 s >> -",
            "range_inclusion_ops brin 3831 3831 7 s @> -",
            "range_inclusion_ops brin 3831 3831 8 s <@ -",
            "range_inclusion_ops brin 3831 3831 17 s -|- -",
            "range_inclusion_ops brin 3831 3831 18 s = -",
            "range_inclusion_ops brin 3831 3831 20 s < -",
            "range_inclusion_ops brin 3831 3831 21 s <= -",
            "range_inclusion_ops brin 3831 3831 22 s > -",
            "range_inclusion_ops brin 3831 3831 23 s >= -",
            // range_ops / btree
            "range_ops btree 3831 3831 1 s < -",
            "range_ops btree 3831 3831 2 s <= -",
            "range_ops btree 3831 3831 3 s = -",
            "range_ops btree 3831 3831 4 s >= -",
            "range_ops btree 3831 3831 5 s > -",
            // range_ops / gist
            "range_ops gist 3831 2283 16 s @> -",
            "range_ops gist 3831 3831 1 s << -",
            "range_ops gist 3831 3831 2 s &< -",
            "range_ops gist 3831 3831 3 s && -",
            "range_ops gist 3831 3831 4 s &> -",
            "range_ops gist 3831 3831 5 s >> -",
            "range_ops gist 3831 3831 6 s -|- -",
            "range_ops gist 3831 3831 7 s @> -",
            "range_ops gist 3831 3831 8 s <@ -",
            "range_ops gist 3831 3831 18 s = -",
            "range_ops gist 3831 4537 1 s << -",
            "range_ops gist 3831 4537 2 s &< -",
            "range_ops gist 3831 4537 3 s && -",
            "range_ops gist 3831 4537 4 s &> -",
            "range_ops gist 3831 4537 5 s >> -",
            "range_ops gist 3831 4537 6 s -|- -",
            "range_ops gist 3831 4537 7 s @> -",
            "range_ops gist 3831 4537 8 s <@ -",
            // range_ops / hash
            "range_ops hash 3831 3831 1 s = -",
            // range_ops / spgist
            "range_ops spgist 3831 2283 16 s @> -",
            "range_ops spgist 3831 3831 1 s << -",
            "range_ops spgist 3831 3831 2 s &< -",
            "range_ops spgist 3831 3831 3 s && -",
            "range_ops spgist 3831 3831 4 s &> -",
            "range_ops spgist 3831 3831 5 s >> -",
            "range_ops spgist 3831 3831 6 s -|- -",
            "range_ops spgist 3831 3831 7 s @> -",
            "range_ops spgist 3831 3831 8 s <@ -",
            "range_ops spgist 3831 3831 18 s = -",
            // record_image_ops / btree
            "record_image_ops btree 2249 2249 1 s *< -",
            "record_image_ops btree 2249 2249 2 s *<= -",
            "record_image_ops btree 2249 2249 3 s *= -",
            "record_image_ops btree 2249 2249 4 s *>= -",
            "record_image_ops btree 2249 2249 5 s *> -",
            // record_ops / btree
            "record_ops btree 2249 2249 1 s < -",
            "record_ops btree 2249 2249 2 s <= -",
            "record_ops btree 2249 2249 3 s = -",
            "record_ops btree 2249 2249 4 s >= -",
            "record_ops btree 2249 2249 5 s > -",
            // record_ops / hash
            "record_ops hash 2249 2249 1 s = -",
            // text_bloom_ops / brin
            "text_bloom_ops brin 25 25 1 s = -",
            // text_minmax_ops / brin
            "text_minmax_ops brin 25 25 1 s < -",
            "text_minmax_ops brin 25 25 2 s <= -",
            "text_minmax_ops brin 25 25 3 s = -",
            "text_minmax_ops brin 25 25 4 s >= -",
            "text_minmax_ops brin 25 25 5 s > -",
            // text_ops / btree
            "text_ops btree 19 19 1 s < -",
            "text_ops btree 19 19 2 s <= -",
            "text_ops btree 19 19 3 s = -",
            "text_ops btree 19 19 4 s >= -",
            "text_ops btree 19 19 5 s > -",
            "text_ops btree 19 25 1 s < -",
            "text_ops btree 19 25 2 s <= -",
            "text_ops btree 19 25 3 s = -",
            "text_ops btree 19 25 4 s >= -",
            "text_ops btree 19 25 5 s > -",
            "text_ops btree 25 19 1 s < -",
            "text_ops btree 25 19 2 s <= -",
            "text_ops btree 25 19 3 s = -",
            "text_ops btree 25 19 4 s >= -",
            "text_ops btree 25 19 5 s > -",
            "text_ops btree 25 25 1 s < -",
            "text_ops btree 25 25 2 s <= -",
            "text_ops btree 25 25 3 s = -",
            "text_ops btree 25 25 4 s >= -",
            "text_ops btree 25 25 5 s > -",
            // text_ops / hash
            "text_ops hash 19 19 1 s = -",
            "text_ops hash 19 25 1 s = -",
            "text_ops hash 25 19 1 s = -",
            "text_ops hash 25 25 1 s = -",
            // text_ops / spgist
            "text_ops spgist 25 25 1 s ~<~ -",
            "text_ops spgist 25 25 2 s ~<=~ -",
            "text_ops spgist 25 25 3 s = -",
            "text_ops spgist 25 25 4 s ~>=~ -",
            "text_ops spgist 25 25 5 s ~>~ -",
            "text_ops spgist 25 25 11 s < -",
            "text_ops spgist 25 25 12 s <= -",
            "text_ops spgist 25 25 14 s >= -",
            "text_ops spgist 25 25 15 s > -",
            "text_ops spgist 25 25 28 s ^@ -",
            // text_pattern_ops / btree
            "text_pattern_ops btree 25 25 1 s ~<~ -",
            "text_pattern_ops btree 25 25 2 s ~<=~ -",
            "text_pattern_ops btree 25 25 3 s = -",
            "text_pattern_ops btree 25 25 4 s ~>=~ -",
            "text_pattern_ops btree 25 25 5 s ~>~ -",
            // text_pattern_ops / hash
            "text_pattern_ops hash 25 25 1 s = -",
            // tid_bloom_ops / brin
            "tid_bloom_ops brin 27 27 1 s = -",
            // tid_minmax_multi_ops / brin
            "tid_minmax_multi_ops brin 27 27 1 s < -",
            "tid_minmax_multi_ops brin 27 27 2 s <= -",
            "tid_minmax_multi_ops brin 27 27 3 s = -",
            "tid_minmax_multi_ops brin 27 27 4 s >= -",
            "tid_minmax_multi_ops brin 27 27 5 s > -",
            // tid_minmax_ops / brin
            "tid_minmax_ops brin 27 27 1 s < -",
            "tid_minmax_ops brin 27 27 2 s <= -",
            "tid_minmax_ops brin 27 27 3 s = -",
            "tid_minmax_ops brin 27 27 4 s >= -",
            "tid_minmax_ops brin 27 27 5 s > -",
            // tid_ops / btree
            "tid_ops btree 27 27 1 s < -",
            "tid_ops btree 27 27 2 s <= -",
            "tid_ops btree 27 27 3 s = -",
            "tid_ops btree 27 27 4 s >= -",
            "tid_ops btree 27 27 5 s > -",
            // tid_ops / hash
            "tid_ops hash 27 27 1 s = -",
            // time_bloom_ops / brin
            "time_bloom_ops brin 1083 1083 1 s = -",
            // time_minmax_multi_ops / brin
            "time_minmax_multi_ops brin 1083 1083 1 s < -",
            "time_minmax_multi_ops brin 1083 1083 2 s <= -",
            "time_minmax_multi_ops brin 1083 1083 3 s = -",
            "time_minmax_multi_ops brin 1083 1083 4 s >= -",
            "time_minmax_multi_ops brin 1083 1083 5 s > -",
            // time_minmax_ops / brin
            "time_minmax_ops brin 1083 1083 1 s < -",
            "time_minmax_ops brin 1083 1083 2 s <= -",
            "time_minmax_ops brin 1083 1083 3 s = -",
            "time_minmax_ops brin 1083 1083 4 s >= -",
            "time_minmax_ops brin 1083 1083 5 s > -",
            // time_ops / btree
            "time_ops btree 1083 1083 1 s < -",
            "time_ops btree 1083 1083 2 s <= -",
            "time_ops btree 1083 1083 3 s = -",
            "time_ops btree 1083 1083 4 s >= -",
            "time_ops btree 1083 1083 5 s > -",
            // time_ops / hash
            "time_ops hash 1083 1083 1 s = -",
            // timestamp_ops / hash
            "timestamp_ops hash 1114 1114 1 s = -",
            // timestamptz_ops / hash
            "timestamptz_ops hash 1184 1184 1 s = -",
            // timetz_bloom_ops / brin
            "timetz_bloom_ops brin 1266 1266 1 s = -",
            // timetz_minmax_multi_ops / brin
            "timetz_minmax_multi_ops brin 1266 1266 1 s < -",
            "timetz_minmax_multi_ops brin 1266 1266 2 s <= -",
            "timetz_minmax_multi_ops brin 1266 1266 3 s = -",
            "timetz_minmax_multi_ops brin 1266 1266 4 s >= -",
            "timetz_minmax_multi_ops brin 1266 1266 5 s > -",
            // timetz_minmax_ops / brin
            "timetz_minmax_ops brin 1266 1266 1 s < -",
            "timetz_minmax_ops brin 1266 1266 2 s <= -",
            "timetz_minmax_ops brin 1266 1266 3 s = -",
            "timetz_minmax_ops brin 1266 1266 4 s >= -",
            "timetz_minmax_ops brin 1266 1266 5 s > -",
            // timetz_ops / btree
            "timetz_ops btree 1266 1266 1 s < -",
            "timetz_ops btree 1266 1266 2 s <= -",
            "timetz_ops btree 1266 1266 3 s = -",
            "timetz_ops btree 1266 1266 4 s >= -",
            "timetz_ops btree 1266 1266 5 s > -",
            // timetz_ops / hash
            "timetz_ops hash 1266 1266 1 s = -",
            // tsquery_ops / btree
            "tsquery_ops btree 3615 3615 1 s < -",
            "tsquery_ops btree 3615 3615 2 s <= -",
            "tsquery_ops btree 3615 3615 3 s = -",
            "tsquery_ops btree 3615 3615 4 s >= -",
            "tsquery_ops btree 3615 3615 5 s > -",
            // tsquery_ops / gist
            "tsquery_ops gist 3615 3615 7 s @> -",
            "tsquery_ops gist 3615 3615 8 s <@ -",
            // tsvector_ops / btree
            "tsvector_ops btree 3614 3614 1 s < -",
            "tsvector_ops btree 3614 3614 2 s <= -",
            "tsvector_ops btree 3614 3614 3 s = -",
            "tsvector_ops btree 3614 3614 4 s >= -",
            "tsvector_ops btree 3614 3614 5 s > -",
            // tsvector_ops / gin
            "tsvector_ops gin 3614 3615 1 s @@ -",
            "tsvector_ops gin 3614 3615 2 s @@@ -",
            // tsvector_ops / gist
            "tsvector_ops gist 3614 3615 1 s @@ -",
            // uuid_bloom_ops / brin
            "uuid_bloom_ops brin 2950 2950 1 s = -",
            // uuid_minmax_multi_ops / brin
            "uuid_minmax_multi_ops brin 2950 2950 1 s < -",
            "uuid_minmax_multi_ops brin 2950 2950 2 s <= -",
            "uuid_minmax_multi_ops brin 2950 2950 3 s = -",
            "uuid_minmax_multi_ops brin 2950 2950 4 s >= -",
            "uuid_minmax_multi_ops brin 2950 2950 5 s > -",
            // uuid_minmax_ops / brin
            "uuid_minmax_ops brin 2950 2950 1 s < -",
            "uuid_minmax_ops brin 2950 2950 2 s <= -",
            "uuid_minmax_ops brin 2950 2950 3 s = -",
            "uuid_minmax_ops brin 2950 2950 4 s >= -",
            "uuid_minmax_ops brin 2950 2950 5 s > -",
            // uuid_ops / btree
            "uuid_ops btree 2950 2950 1 s < -",
            "uuid_ops btree 2950 2950 2 s <= -",
            "uuid_ops btree 2950 2950 3 s = -",
            "uuid_ops btree 2950 2950 4 s >= -",
            "uuid_ops btree 2950 2950 5 s > -",
            // uuid_ops / hash
            "uuid_ops hash 2950 2950 1 s = -",
            // varbit_minmax_ops / brin
            "varbit_minmax_ops brin 1562 1562 1 s < -",
            "varbit_minmax_ops brin 1562 1562 2 s <= -",
            "varbit_minmax_ops brin 1562 1562 3 s = -",
            "varbit_minmax_ops brin 1562 1562 4 s >= -",
            "varbit_minmax_ops brin 1562 1562 5 s > -",
            // varbit_ops / btree
            "varbit_ops btree 1562 1562 1 s < -",
            "varbit_ops btree 1562 1562 2 s <= -",
            "varbit_ops btree 1562 1562 3 s = -",
            "varbit_ops btree 1562 1562 4 s >= -",
            "varbit_ops btree 1562 1562 5 s > -",
            // xid8_ops / btree
            "xid8_ops btree 5069 5069 1 s < -",
            "xid8_ops btree 5069 5069 2 s <= -",
            "xid8_ops btree 5069 5069 3 s = -",
            "xid8_ops btree 5069 5069 4 s >= -",
            "xid8_ops btree 5069 5069 5 s > -",
            // xid8_ops / hash
            "xid8_ops hash 5069 5069 1 s = -",
            // xid_ops / hash
            "xid_ops hash 28 28 1 s = -",
    };

    /**
     * {@link #AMOPS} with everything that does not depend on the running database resolved once:
     * the OID key, the family key, the operand types, the strategy, and the OID of the operator
     * itself, which is fixed because a built-in operator carries PostgreSQL's own OID.
     *
     * <p>Columns: oid key, family key, amoplefttype, amoprighttype, amopstrategy, amoppurpose,
     * amopopr, sort family key (null when none), amopmethod.
     */
    private static final Object[][] AMOP_ROWS = amopRows();

    private static Object[][] amopRows() {
        List<Object[]> rows = new ArrayList<Object[]>(AMOPS.length);
        for (String entry : AMOPS) {
            String[] f = entry.split(" ");
            int left = Integer.parseInt(f[2]);
            int right = Integer.parseInt(f[3]);
            // A strategy names an operator. Writing the row when memgres does not carry that
            // operator would send every reader of pg_amop to a pg_operator row that is not
            // there, so the row is left out rather than left dangling.
            int opr = builtinOperatorOid(f[6], left, right);
            if (opr == 0) continue;
            String amName = f[1];
            rows.add(new Object[]{
                    "amop:" + f[0] + "/" + amName + "/" + left + "/" + right + "/" + f[4] + "/" + f[5],
                    OPFAMILY_KEYS.get(f[0] + "/" + amName),
                    left, right, Short.valueOf(f[4]), f[5], opr,
                    "-".equals(f[7]) ? null : OPFAMILY_KEYS.get(f[7] + "/btree"),
                    accessMethodOid(amName)});
        }
        return rows.toArray(new Object[rows.size()][]);
    }

    Table buildPgAmop() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID), col("amopfamily", DataType.OID),
                col("amoplefttype", DataType.INTEGER), col("amoprighttype", DataType.INTEGER),
                col("amopstrategy", DataType.SMALLINT), col("amoppurpose", DataType.INTERNAL_CHAR),
                col("amopopr", DataType.INTEGER),
                col("amopmethod", DataType.INTEGER), col("amopsortfamily", DataType.INTEGER),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_amop", cols);
        for (Object[] r : AMOP_ROWS) {
            table.insertRow(new Object[]{
                    oids.oid((String) r[0]), oids.oid((String) r[1]),
                    r[2], r[3], r[4], r[5], r[6], r[8],
                    r[7] == null ? 0 : oids.oid((String) r[7]), 1});
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
        // Empty, and not because PostgreSQL's is: PG 18 has 714 rows here, twenty-five of them
        // for integer_ops/btree alone. Every one names a support function -- btint4cmp,
        // hashint4, gin_extract_jsonb -- and memgres registers no pg_proc row for any of the 299
        // such functions, so a row written here would name a function that is not there. A
        // reference a reader cannot follow is worse than a row that is not offered, so these
        // wait until pg_proc carries the support functions.
        return table;
    }
}
