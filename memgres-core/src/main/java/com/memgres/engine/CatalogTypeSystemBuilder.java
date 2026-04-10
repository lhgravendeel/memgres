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
                colNN("oid", DataType.INTEGER),
                colNN("collname", DataType.TEXT),
                colNN("collnamespace", DataType.INTEGER),
                colNN("collowner", DataType.INTEGER),
                colNN("collprovider", DataType.CHAR),
                col("collisdeterministic", DataType.BOOLEAN),
                col("collencoding", DataType.INTEGER),
                col("colllocale", DataType.TEXT),
                col("collcollate", DataType.TEXT),
                col("collctype", DataType.TEXT),
                col("collversion", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_collation", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        table.insertRow(new Object[]{ oids.oid("collation:default"), "default", pgCatalogNs, 10, "d", true, -1, null, null, null, null, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:C"), "C", pgCatalogNs, 10, "c", true, -1, null, "C", "C", null, 1 });
        table.insertRow(new Object[]{ oids.oid("collation:POSIX"), "POSIX", pgCatalogNs, 10, "c", true, -1, null, "POSIX", "POSIX", null, 1 });
        return table;
    }

    Table buildPgRange() {
        List<Column> cols = Cols.listOf(
                colNN("rngtypid", DataType.INTEGER),
                col("rngsubtype", DataType.INTEGER),
                col("rngmultitypid", DataType.INTEGER),
                col("rngcollation", DataType.INTEGER),
                col("rngsubopc", DataType.INTEGER),
                col("rngcanonical", DataType.INTEGER),
                col("rngsubdiff", DataType.INTEGER)
        );
        return new Table("pg_range", cols);
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
                colNN("oid", DataType.INTEGER), colNN("castsource", DataType.INTEGER),
                colNN("casttarget", DataType.INTEGER), col("castfunc", DataType.INTEGER),
                col("castcontext", DataType.CHAR), col("castmethod", DataType.CHAR),
                col("xmin", DataType.INTEGER));
        Table table = new Table("pg_cast", cols);
        // Populate with common PG casts. castcontext: 'e'=explicit, 'a'=assignment, 'i'=implicit
        // castmethod: 'f'=function, 'b'=binary-coercible, 'i'=I/O conversion
        int castOid = 5000; // starting OID for casts
        // Numeric casts
        int INT2 = 21, INT4 = 23, INT8 = 20, FLOAT4 = 700, FLOAT8 = 701, NUMERIC = 1700;
        int TEXT = 25, VARCHAR = 1043, CHAR = 18, BPCHAR = 1042, NAME = 19;
        int BOOL = 16, OID = 26, DATE = 1082, TIME = 1083, TIMESTAMP = 1114, TIMESTAMPTZ = 1184;
        int INTERVAL = 1186, UUID = 2950, JSON = 114, JSONB = 3802, BYTEA = 17, INET = 869, CIDR = 650;
        int XML = 142, REGCLASS = 2205, REGTYPE = 2206, REGPROC = 24, REGOPER = 2203;
        int REGPROCEDURE = 2202, REGOPERATOR = 2204;
        // Integer promotions (implicit)
        int[][] implicitCasts = {
            {INT2, INT4}, {INT2, INT8}, {INT2, FLOAT4}, {INT2, FLOAT8}, {INT2, NUMERIC},
            {INT4, INT8}, {INT4, FLOAT8}, {INT4, NUMERIC},
            {INT8, NUMERIC},
            {FLOAT4, FLOAT8},
            {INT4, FLOAT4}, // actually assignment in PG but common
        };
        for (int[] c : implicitCasts) {
            table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "i", "b", 1});
        }
        // Assignment casts
        int[][] assignCasts = {
            {INT4, INT2}, {INT8, INT4}, {INT8, INT2},
            {FLOAT4, INT4}, {FLOAT4, INT2}, {FLOAT8, INT4}, {FLOAT8, INT2}, {FLOAT8, INT8}, {FLOAT8, FLOAT4},
            {NUMERIC, INT2}, {NUMERIC, INT4}, {NUMERIC, INT8}, {NUMERIC, FLOAT4}, {NUMERIC, FLOAT8},
            {TEXT, VARCHAR}, {TEXT, BPCHAR}, {TEXT, CHAR}, {TEXT, NAME},
            {VARCHAR, TEXT}, {BPCHAR, TEXT}, {CHAR, TEXT}, {NAME, TEXT},
            {VARCHAR, BPCHAR}, {VARCHAR, CHAR}, {VARCHAR, NAME},
            {BPCHAR, VARCHAR}, {BPCHAR, CHAR}, {BPCHAR, NAME},
        };
        for (int[] c : assignCasts) {
            table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "a", "b", 1});
        }
        // Explicit casts
        int[][] explicitCasts = {
            {BOOL, INT4}, {INT4, BOOL},
            {TEXT, INT4}, {TEXT, INT8}, {TEXT, INT2}, {TEXT, FLOAT4}, {TEXT, FLOAT8}, {TEXT, NUMERIC},
            {TEXT, BOOL}, {TEXT, DATE}, {TEXT, TIMESTAMP}, {TEXT, TIMESTAMPTZ}, {TEXT, INTERVAL},
            {TEXT, UUID}, {TEXT, JSON}, {TEXT, JSONB}, {TEXT, INET}, {TEXT, CIDR}, {TEXT, XML},
            {INT4, TEXT}, {INT8, TEXT}, {INT2, TEXT}, {FLOAT4, TEXT}, {FLOAT8, TEXT}, {NUMERIC, TEXT},
            {BOOL, TEXT}, {DATE, TEXT}, {TIMESTAMP, TEXT}, {TIMESTAMPTZ, TEXT}, {INTERVAL, TEXT},
            {UUID, TEXT}, {JSON, TEXT}, {JSONB, TEXT}, {INET, TEXT}, {CIDR, TEXT}, {XML, TEXT},
            {TIMESTAMP, DATE}, {TIMESTAMP, TIME},
            {TIMESTAMPTZ, DATE}, {TIMESTAMPTZ, TIMESTAMP}, {TIMESTAMPTZ, TIME},
            {INTERVAL, TIME},
            {JSON, JSONB}, {JSONB, JSON},
            {INET, CIDR}, {CIDR, INET},
            {BYTEA, TEXT}, {TEXT, BYTEA},
            // reg* types
            {INT4, REGCLASS}, {INT4, REGTYPE}, {INT4, REGPROC}, {INT4, REGOPER},
            {INT4, REGPROCEDURE}, {INT4, REGOPERATOR},
            {REGCLASS, INT4}, {REGTYPE, INT4}, {REGPROC, INT4}, {REGOPER, INT4},
            {REGPROCEDURE, INT4}, {REGOPERATOR, INT4},
            {REGCLASS, OID}, {REGTYPE, OID}, {REGPROC, OID}, {REGOPER, OID},
            {OID, REGCLASS}, {OID, REGTYPE}, {OID, REGPROC}, {OID, REGOPER},
            {OID, INT4}, {INT4, OID}, {INT8, OID}, {OID, INT8},
            {TEXT, REGCLASS}, {REGCLASS, TEXT},
        };
        for (int[] c : explicitCasts) {
            table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "e", "i", 1});
        }

        // Additional type OIDs for PG18 completeness
        int TIMETZ = 1266, MONEY = 790;
        int BIT_T = 1560, VARBIT_T = 1562;
        int POINT = 600, LINE_T = 628, LSEG_T = 601, PG_BOX = 603, PG_PATH = 602, POLYGON_T = 604, CIRCLE_T = 718;
        int TSVECTOR = 3614, TSQUERY = 3615;
        int INT4RANGE = 3904, INT8RANGE = 3926, NUMRANGE = 3906, DATERANGE = 3912, TSRANGE = 3908, TSTZRANGE = 3910;
        int REGCONFIG = 3734, REGDICTIONARY = 3769, REGNAMESPACE = 4089, REGROLE = 4096;
        int XID = 28, CID = 29, TID = 27, MACADDR = 829, MACADDR8 = 774;
        int INT2VECTOR = 22, OIDVECTOR = 30, REFCURSOR = 1790;

        // ---- Implicit casts (missing promotions) ----
        int[][] moreImplicit = {
            {INT8, FLOAT4}, {INT8, FLOAT8},
            {FLOAT4, NUMERIC}, {FLOAT8, NUMERIC},
            {TIME, TIMETZ},
            {TIMESTAMP, TIMESTAMPTZ},
            {DATE, TIMESTAMPTZ},
            {DATE, TIMESTAMP},
            {BIT_T, VARBIT_T},
        };
        for (int[] c : moreImplicit) table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "i", "b", 1});

        // ---- Assignment casts (missing) ----
        int[][] moreAssign = {
            {NUMERIC, MONEY}, {INT4, MONEY}, {INT8, MONEY},
            {MONEY, NUMERIC}, {MONEY, INT4}, {MONEY, INT8},
            {BIT_T, INT4}, {BIT_T, INT8}, {INT4, BIT_T}, {INT8, BIT_T},
            {TIMETZ, TIME},
            {INTERVAL, INTERVAL},
            {TIMESTAMP, TIMESTAMP},
            {TIMESTAMPTZ, TIMESTAMPTZ},
            {TIME, TIME},
            {TIMETZ, TIMETZ},
            {BIT_T, BIT_T},
            {VARBIT_T, VARBIT_T},
            {VARCHAR, VARCHAR},
            {BPCHAR, BPCHAR},
            {NUMERIC, NUMERIC},
        };
        for (int[] c : moreAssign) table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "a", "f", 1});

        // ---- Explicit / I/O casts (large batch) ----
        int[][] moreExplicit = {
            {TIME, INTERVAL},
            {TEXT, TIME}, {TIME, TEXT}, {TEXT, TIMETZ}, {TIMETZ, TEXT},
            {TIMETZ, TIMESTAMPTZ},

            // Money
            {TEXT, MONEY}, {MONEY, TEXT},

            // Bit string
            {TEXT, BIT_T}, {BIT_T, TEXT}, {TEXT, VARBIT_T}, {VARBIT_T, TEXT},

            // Geometric
            {TEXT, POINT}, {POINT, TEXT},
            {TEXT, LINE_T}, {LINE_T, TEXT},
            {TEXT, LSEG_T}, {LSEG_T, TEXT},
            {TEXT, PG_BOX}, {PG_BOX, TEXT},
            {TEXT, PG_PATH}, {PG_PATH, TEXT},
            {TEXT, POLYGON_T}, {POLYGON_T, TEXT},
            {TEXT, CIRCLE_T}, {CIRCLE_T, TEXT},
            {PG_BOX, POINT}, {PG_BOX, LSEG_T}, {PG_BOX, CIRCLE_T}, {PG_BOX, POLYGON_T},
            {POINT, PG_BOX},
            {LSEG_T, POINT},
            {POLYGON_T, POINT}, {POLYGON_T, PG_BOX}, {POLYGON_T, CIRCLE_T}, {POLYGON_T, PG_PATH},
            {CIRCLE_T, POINT}, {CIRCLE_T, PG_BOX}, {CIRCLE_T, POLYGON_T},
            {PG_PATH, POLYGON_T}, {PG_PATH, POINT},

            // Full-text search
            {TEXT, TSVECTOR}, {TSVECTOR, TEXT},
            {TEXT, TSQUERY}, {TSQUERY, TEXT},
            {TEXT, REGCONFIG}, {REGCONFIG, TEXT},
            {TEXT, REGDICTIONARY}, {REGDICTIONARY, TEXT},

            // Range types
            {TEXT, INT4RANGE}, {INT4RANGE, TEXT},
            {TEXT, INT8RANGE}, {INT8RANGE, TEXT},
            {TEXT, NUMRANGE}, {NUMRANGE, TEXT},
            {TEXT, DATERANGE}, {DATERANGE, TEXT},
            {TEXT, TSRANGE}, {TSRANGE, TEXT},
            {TEXT, TSTZRANGE}, {TSTZRANGE, TEXT},

            // Additional reg* types
            {INT4, REGCONFIG}, {REGCONFIG, INT4}, {REGCONFIG, OID}, {OID, REGCONFIG},
            {INT4, REGDICTIONARY}, {REGDICTIONARY, INT4}, {REGDICTIONARY, OID}, {OID, REGDICTIONARY},
            {INT4, REGNAMESPACE}, {REGNAMESPACE, INT4}, {REGNAMESPACE, OID}, {OID, REGNAMESPACE},
            {TEXT, REGNAMESPACE}, {REGNAMESPACE, TEXT},
            {INT4, REGROLE}, {REGROLE, INT4}, {REGROLE, OID}, {OID, REGROLE},
            {TEXT, REGROLE}, {REGROLE, TEXT},
            {TEXT, REGTYPE}, {REGTYPE, TEXT},
            {TEXT, REGPROC}, {REGPROC, TEXT},
            {TEXT, REGOPER}, {REGOPER, TEXT},
            {TEXT, REGPROCEDURE}, {REGPROCEDURE, TEXT},
            {TEXT, REGOPERATOR}, {REGOPERATOR, TEXT},

            // OID related
            {OID, REGPROCEDURE}, {REGPROCEDURE, OID},
            {OID, REGOPERATOR}, {REGOPERATOR, OID},
            {INT2, OID},

            // XID/CID/TID
            {XID, INT4}, {XID, INT8}, {INT4, XID}, {TEXT, XID}, {XID, TEXT},
            {CID, INT4}, {INT4, CID}, {TEXT, CID}, {CID, TEXT},
            {TID, TEXT}, {TEXT, TID},

            // MACADDR
            {TEXT, MACADDR}, {MACADDR, TEXT},
            {MACADDR, MACADDR8}, {MACADDR8, MACADDR},
            {TEXT, MACADDR8}, {MACADDR8, TEXT},

            // Vectors
            {INT2VECTOR, TEXT}, {TEXT, INT2VECTOR},
            {OIDVECTOR, TEXT}, {TEXT, OIDVECTOR},

            // Boolean extras
            {BOOL, INT2}, {INT2, BOOL},

            // NAME conversions
            {NAME, INT4}, {NAME, INT8},

            // REFCURSOR
            {TEXT, REFCURSOR}, {REFCURSOR, TEXT},

            // MONEY from float
            {FLOAT4, MONEY}, {FLOAT8, MONEY},

            // JSONB from other types
            {INT2, JSONB}, {INT4, JSONB}, {INT8, JSONB}, {FLOAT4, JSONB}, {FLOAT8, JSONB},
            {NUMERIC, JSONB}, {BOOL, JSONB},

            // Bytea
            {BYTEA, INT4}, {INT4, BYTEA},
        };
        for (int[] c : moreExplicit) table.insertRow(new Object[]{castOid++, c[0], c[1], 0, "e", "i", 1});

        return table;
    }

    Table buildPgOperator() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("oprname", DataType.TEXT),
                col("oprnamespace", DataType.INTEGER), col("oprowner", DataType.INTEGER),
                col("oprkind", DataType.CHAR), col("oprleft", DataType.INTEGER),
                col("oprright", DataType.INTEGER), col("oprresult", DataType.INTEGER),
                col("oprcode", DataType.INTEGER), col("oprrest", DataType.INTEGER),
                col("oprjoin", DataType.INTEGER), col("oprcom", DataType.INTEGER),
                col("oprnegate", DataType.INTEGER), col("oprcanmerge", DataType.BOOLEAN),
                col("oprcanhash", DataType.BOOLEAN), col("xmin", DataType.INTEGER));
        Table table = new Table("pg_operator", cols);
        int pgCatalogNs = oids.oid("ns:pg_catalog");
        int publicNs = oids.oid("ns:public");

        // Bootstrap built-in binary operators
        String[][] builtinBinary = {
                {"+", "b"}, {"-", "b"}, {"*", "b"}, {"/", "b"}, {"%", "b"},
                {"=", "b"}, {"<>", "b"}, {"!=", "b"}, {"<", "b"}, {">", "b"},
                {"<=", "b"}, {">=", "b"}, {"||", "b"}, {"&&", "b"},
                {"~~", "b"}, {"!~~", "b"}, {"~~*", "b"}, {"!~~*", "b"},
                {"~", "b"}, {"!~", "b"}, {"~*", "b"}, {"!~*", "b"},
                {"@>", "b"}, {"<@", "b"}, {"?", "b"}, {"?|", "b"}, {"?&", "b"},
                {"->", "b"}, {"->>", "b"}, {"#>", "b"}, {"#>>", "b"},
                {"IS", "b"}, {"^", "b"}, {"&", "b"}, {"|", "b"},
                {"<<", "b"}, {">>", "b"},
        };
        for (String[] op : builtinBinary) {
            int opOid = oids.oid("operator:pg_catalog." + op[0]);
            table.insertRow(new Object[]{opOid, op[0], pgCatalogNs, 10,
                    op[1], 0, 0, 0, 0, 0, 0, 0, 0, false, false, 1});
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
                    op.getKind(), leftOid, rightOid, resultOid, opcodeOid, 0, 0, comOid, negOid,
                    op.isMerges(), op.isHashes(), 1});
        }

        return table;
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

    Table buildPgOpclass() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), colNN("opcname", DataType.TEXT),
                col("opcnamespace", DataType.INTEGER), col("opcowner", DataType.INTEGER),
                col("opcfamily", DataType.INTEGER), col("opcintype", DataType.INTEGER),
                col("opckeytype", DataType.INTEGER), col("opcdefault", DataType.BOOLEAN),
                col("opcmethod", DataType.INTEGER), col("xmin", DataType.INTEGER));
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
        table.insertRow(new Object[]{oids.oid("opclass:varchar_ops"), "varchar_ops", pgCatalogNs, 10,
                oids.oid("opfamily:text_ops"), DataType.VARCHAR.getOid(), 0, true, 403, 1});
        // Hash operator classes (same names as btree; this is correct PG behavior)
        table.insertRow(new Object[]{oids.oid("opclass:hash_int4_ops"), "int4_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_integer_ops"), DataType.INTEGER.getOid(), 0, true, 405, 1});
        table.insertRow(new Object[]{oids.oid("opclass:hash_text_ops"), "text_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_text_ops"), DataType.TEXT.getOid(), 0, true, 405, 1});
        table.insertRow(new Object[]{oids.oid("opclass:hash_bool_ops"), "bool_ops", pgCatalogNs, 10,
                oids.oid("opfamily:hash_bool_ops"), DataType.BOOLEAN.getOid(), 0, true, 405, 1});

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
                colNN("oid", DataType.INTEGER), colNN("opfname", DataType.TEXT),
                col("opfnamespace", DataType.INTEGER), col("opfowner", DataType.INTEGER),
                col("opfmethod", DataType.INTEGER), col("xmin", DataType.INTEGER));
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

    Table buildPgAggregate() {
        List<Column> cols = Cols.listOf(
                colNN("aggfnoid", DataType.INTEGER), col("aggtransfn", DataType.INTEGER),
                col("aggtranstype", DataType.INTEGER), col("aggfinalfn", DataType.INTEGER),
                col("agginitval", DataType.TEXT), col("aggsortop", DataType.INTEGER),
                col("aggfinalextra", DataType.BOOLEAN), col("aggtransspace", DataType.INTEGER),
                col("aggmtransfn", DataType.INTEGER), col("aggminvtransfn", DataType.INTEGER),
                col("aggmtranstype", DataType.INTEGER), col("aggmtransspace", DataType.INTEGER),
                col("aggmfinalfn", DataType.INTEGER), col("aggmfinalextra", DataType.BOOLEAN),
                col("aggminitval", DataType.TEXT), col("aggkind", DataType.CHAR),
                col("aggnumdirectargs", DataType.SMALLINT),
                col("aggcombinefn", DataType.INTEGER), col("aggserialfn", DataType.INTEGER),
                col("aggdeserialfn", DataType.INTEGER), col("xmin", DataType.INTEGER));
        return new Table("pg_aggregate", cols);
    }

    Table buildPgAmop() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), col("amopfamily", DataType.INTEGER),
                col("amoplefttype", DataType.INTEGER), col("amoprighttype", DataType.INTEGER),
                col("amopstrategy", DataType.SMALLINT), col("amopopr", DataType.INTEGER),
                col("amopsortfamily", DataType.INTEGER), col("xmin", DataType.INTEGER));
        return new Table("pg_amop", cols);
    }

    Table buildPgAmproc() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER), col("amprocfamily", DataType.INTEGER),
                col("amproclefttype", DataType.INTEGER), col("amprocrighttype", DataType.INTEGER),
                col("amprocnum", DataType.SMALLINT), col("amproc", DataType.INTEGER),
                col("xmin", DataType.INTEGER));
        return new Table("pg_amproc", cols);
    }
}
