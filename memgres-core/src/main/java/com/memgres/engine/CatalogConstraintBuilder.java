package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds constraint, index, dependency, and related metadata pg_catalog tables.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogConstraintBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogConstraintBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    /**
     * The columns a CHECK constraint reads, as attnums in column order — what PostgreSQL keeps
     * in {@code pg_constraint.conkey} for a CHECK. information_schema.constraint_column_usage is
     * built from it, so a constraint with no conkey is a constraint no tool can attribute to a
     * column.
     */
    private List<Object> checkedColumns(Table t, StoredConstraint sc) {
        if (sc.getCheckExpr() == null) return null;
        Set<String> named = new LinkedHashSet<>();
        for (String name : DdlExecutor.referencedColumnNames(sc.getCheckExpr())) {
            named.add(name.toLowerCase());
        }
        List<Object> attnums = new ArrayList<>();
        for (int i = 0; i < t.getColumns().size(); i++) {
            if (named.contains(t.getColumns().get(i).getName().toLowerCase())) {
                attnums.add(Integer.valueOf(t.attnumAt(i)));
            }
        }
        return attnums.isEmpty() ? null : attnums;
    }

    /**
     * The schema the table a foreign key references actually lives in, or null when no relation
     * of that name can be found.
     *
     * <p>Every catalog column a foreign key fills in — {@code confrelid}, {@code confkey},
     * {@code conindid} and the relation its action triggers are filed against — names the
     * <em>referenced</em> relation, so all of them have to agree on which relation that is.
     * Taking the child's schema as the answer names a relation that need not exist: a table in
     * one schema referencing a table in another through the search path produced rows keyed
     * {@code rel:<child schema>.<parent name>}, which either matched nothing at all (a dangling
     * {@code tgrelid}) or, once a table of that name was created in the child's schema, latched
     * onto a table the constraint has nothing to do with.
     *
     * <p>A qualified reference is taken at its word as long as the relation is really there. An
     * unqualified one was resolved through the search path when the constraint was written and
     * memgres keeps no record of which schema won, so it is resolved the only two ways that
     * cannot name the wrong relation: the child's own schema, or the one schema in the database
     * that holds a table of that name. When neither settles it the foreign key contributes no
     * rows rather than rows about somebody else's table.
     */
    private String referencedSchema(String childSchema, StoredConstraint sc) {
        String refName = sc.getReferencesTable();
        if (refName == null) return null;
        String declared = sc.getReferencesSchema();
        if (declared != null) {
            Schema declaredSchema = database.getSchemas().get(declared);
            if (declaredSchema != null && declaredSchema.getTable(refName) != null) return declared;
        }
        Schema own = childSchema == null ? null : database.getSchemas().get(childSchema);
        if (own != null && own.getTable(refName) != null) return childSchema;
        String only = null;
        for (Map.Entry<String, Schema> entry : database.getSchemas().entrySet()) {
            if (entry.getValue().getTable(refName) == null) continue;
            if (only != null) return null;
            only = entry.getKey();
        }
        return only;
    }

    /**
     * The key a constraint's OID is minted from. A constraint name is unique only within its
     * table, and a table name only within its schema, so both halves belong in the key: two
     * schemas each holding a {@code chi} table with an {@code fk_z} constraint are two
     * constraints, and keying them alike gave them one OID between them, which doubled every
     * row of a join from pg_trigger or pg_depend to pg_constraint.
     */
    static String constraintKey(String schemaName, String tableName, String constraintName) {
        return "con:" + schemaName + "." + tableName + "." + constraintName;
    }

    Table buildPgConstraint() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("conname", DataType.NAME),
                colNN("connamespace", DataType.OID),
                colNN("contype", DataType.INTERNAL_CHAR),
                colNN("conrelid", DataType.OID),
                colNN("confrelid", DataType.OID),
                col("conkey", DataType.INT2_ARRAY),
                col("confkey", DataType.INT2_ARRAY),
                col("condeferrable", DataType.BOOLEAN),
                col("condeferred", DataType.BOOLEAN),
                col("convalidated", DataType.BOOLEAN),
                col("conislocal", DataType.BOOLEAN),
                col("conindid", DataType.OID),
                col("confupdtype", DataType.INTERNAL_CHAR),
                col("confdeltype", DataType.INTERNAL_CHAR),
                col("confmatchtype", DataType.INTERNAL_CHAR),
                col("conpfeqop", DataType.OID_ARRAY),
                col("conppeqop", DataType.OID_ARRAY),
                col("conffeqop", DataType.OID_ARRAY),
                col("confdelsetcols", DataType.INT2_ARRAY),
                col("coninhcount", DataType.SMALLINT),
                col("connoinherit", DataType.BOOLEAN),
                col("conenforced", DataType.BOOLEAN),
                col("conbin", DataType.PG_NODE_TREE),
                col("conexclop", DataType.OID_ARRAY),
                col("conperiod", DataType.BOOLEAN),
                col("conparentid", DataType.OID),
                col("contypid", DataType.OID),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_constraint", cols);

        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            int nsOid = oids.oid("ns:" + schemaEntry.getKey());
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (StoredConstraint sc : t.getConstraints()) {
                    // UNIQUE constraints from CREATE UNIQUE INDEX (not ADD CONSTRAINT) are not in pg_constraint
                    if (sc.getType() == StoredConstraint.Type.UNIQUE && sc.isFromIndex()) continue;
                    String contype;
                    switch (sc.getType()) {
                        case PRIMARY_KEY:
                            contype = "p";
                            break;
                        case UNIQUE:
                            contype = "u";
                            break;
                        case CHECK:
                            contype = "c";
                            break;
                        case FOREIGN_KEY:
                            contype = "f";
                            break;
                        case EXCLUDE:
                            contype = "x";
                            break;
                        default:
                            throw new IllegalStateException("Unknown constraint type: " + sc.getType());
                    }
                    String refSchema = sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                            ? referencedSchema(schemaEntry.getKey(), sc) : null;
                    int confrelid = 0;
                    if (refSchema != null) {
                        confrelid = oids.oid("rel:" + refSchema + "." + sc.getReferencesTable());
                    }
                    // Convert column names to attnum array string
                    List<Object> conkey = columnNamesToAttnums(t, sc.getColumns());
                    if (sc.getType() == StoredConstraint.Type.CHECK) {
                        // A CHECK names no columns of its own: the ones it constrains are the
                        // ones its expression reads, and that is what conkey lists.
                        conkey = checkedColumns(t, sc);
                    }
                    Table refTable = refSchema == null ? null
                            : database.getSchemas().get(refSchema).getTable(sc.getReferencesTable());
                    List<Object> confkey = null;
                    if (refTable != null) {
                        confkey = columnNamesToAttnums(refTable, sc.getReferencesColumns());
                    }
                    int conindid = 0;
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                            || sc.getType() == StoredConstraint.Type.UNIQUE
                            || sc.getType() == StoredConstraint.Type.EXCLUDE) {
                        conindid = oids.oid("rel:" + schemaEntry.getKey() + "." + sc.getName());
                    } else if (refTable != null) {
                        // conindid names the index behind the referenced table's primary key, and
                        // that index lives in the referenced table's schema, not the child's.
                        for (StoredConstraint refCon : refTable.getConstraints()) {
                            if (refCon.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                                conindid = oids.oid("rel:" + refSchema + "." + refCon.getName());
                                break;
                            }
                        }
                    }
                    // In PG, confupdtype/confdeltype are always a single char (' ' for non-FK)
                    String confupdtype = " ";
                    String confdeltype = " ";
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        confupdtype = fkActionCode(sc.getOnUpdate());
                        confdeltype = fkActionCode(sc.getOnDelete());
                    }
                    // NO INHERIT is a clause only a CHECK carries, but PostgreSQL marks every
                    // index-backed constraint non-inheritable by construction: an index does not
                    // span an inheritance hierarchy, so neither can the constraint behind it.
                    boolean connoinherit = sc.isNoInherit()
                            || sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                            || sc.getType() == StoredConstraint.Type.UNIQUE
                            || sc.getType() == StoredConstraint.Type.FOREIGN_KEY
                            || sc.getType() == StoredConstraint.Type.EXCLUDE;
                    // FK-only fields: confmatchtype defaults to 's' (SIMPLE) for standard PG foreign keys
                    String confmatchtype = " ";
                    List<Object> conpfeqop = null;
                    List<Object> conppeqop = null;
                    List<Object> conffeqop = null;
                    if (sc.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                        if ("FULL".equals(sc.getMatchType())) {
                            confmatchtype = "f";
                        } else if ("PARTIAL".equals(sc.getMatchType())) {
                            confmatchtype = "p";
                        } else {
                            confmatchtype = "s"; // SIMPLE (MATCH SIMPLE is PG default)
                        }
                        // One OID per referenced column; we don't track the real operator OID,
                        // but PG guarantees these arrays are non-empty for FKs.
                        int n = conkey == null ? 0 : conkey.size();
                        conpfeqop = new java.util.ArrayList<>();
                        conppeqop = new java.util.ArrayList<>();
                        conffeqop = new java.util.ArrayList<>();
                        for (int i = 0; i < n; i++) {
                            // 96 = OID for integer equality operator int4eq — acts as a sentinel non-zero OID
                            conpfeqop.add(96);
                            conppeqop.add(96);
                            conffeqop.add(96);
                        }
                    }
                    // A constraint a relation holds because a parent has it is not the relation's
                    // own: PostgreSQL reports conislocal false for it and counts in coninhcount the
                    // parents that handed it down. The pair is what separates a rule the relation
                    // declared from one it only enforces, and it is what says whether the relation
                    // may drop it -- reporting every row as local claimed a partition's key and an
                    // inherited CHECK were declared where they are merely obeyed.
                    int coninhcount = inheritedParentCount(t, sc);
                    // Whether the relation declared the rule for itself is a separate question
                    // from how many parents also carry it: a child that restates its parent's
                    // CHECK under the same name is told it declared it and inherits it once over.
                    // A partition declares nothing of its own, and a rule nobody hands down any
                    // longer is the relation's own whatever it once was.
                    boolean conislocal = coninhcount == 0
                            || (t.getPartitionParent() == null && sc.getInheritedFrom() == null);
                    table.insertRow(new Object[]{
                            oids.oid(constraintKey(schemaEntry.getKey(), t.getName(), sc.getName())),
                            sc.getName(),
                            nsOid,
                            contype,
                            relOid,
                            confrelid,
                            conkey,
                            confkey,
                            // A constraint nobody enforces cannot have been validated, so
                            // PostgreSQL reports convalidated false for it whatever was recorded.
                            sc.isDeferrable(), sc.isInitiallyDeferred(),
                            sc.isConvalidated() && !sc.isNotEnforced(), // condeferrable, condeferred, convalidated
                            conislocal, conindid,
                            confupdtype,
                            confdeltype,
                            confmatchtype, conpfeqop, conppeqop, conffeqop, null /*confdelsetcols*/, coninhcount,
                            connoinherit,
                            !sc.isNotEnforced(), // conenforced: true = enforced (default), false = not enforced
                            sc.getType() == StoredConstraint.Type.CHECK && sc.getCheckExpr() != null
                                    ? "{OPEXPR " + sc.getCheckExpr().toString() + "}" : null, // conbin
                            excludeOperators(t, sc), sc.isPeriod(), 0, 0, 1 // conexclop, conperiod, conparentid, contypid, xmin
                    });
                }
                // PG 18: NOT NULL constraints are tracked in pg_constraint with contype='n'
                // Collect columns covered by UNIQUE constraints promoted from index (USING INDEX)
                java.util.Set<String> promotedUniqueColumns = new java.util.HashSet<>();
                for (StoredConstraint usc : t.getConstraints()) {
                    if (usc.getType() == StoredConstraint.Type.UNIQUE && usc.isPromotedFromIndex()) {
                        for (String c : usc.getColumns()) promotedUniqueColumns.add(c.toLowerCase());
                    }
                }
                for (Column c : t.getColumns()) {
                    boolean isPromotedUnique = promotedUniqueColumns.contains(c.getName().toLowerCase());
                    // Emit NOT NULL for all NOT NULL columns (including PK columns),
                    // but skip columns covered by UNIQUE constraints promoted from index
                    if (!c.isNullable() && !isPromotedUnique) {
                        List<Object> nnConkey = columnNamesToAttnums(t, Cols.listOf(c.getName()));
                        // A NOT NULL a parent declares is enforced by everything below it, and a
                        // descendant that never declared it holds the parent's constraint, under
                        // the parent's name -- the name PostgreSQL uses when it refuses that
                        // descendant permission to drop it -- with one count per direct parent
                        // that passed the rule down. But a relation that restated NOT NULL in its
                        // own definition declared it however many parents also do: PostgreSQL
                        // gives that one the relation's own name and lets it be dropped there. So
                        // the walk for the name stops at the first relation that declared it,
                        // rather than running all the way to the top of the chain.
                        int coninhcount = t.retainedNotNullInheritCount(c.getName());
                        for (Table direct : t.getDirectParents()) {
                            if (declaresNotNull(direct, c.getName())) coninhcount++;
                        }
                        // A partition takes its columns from the partitioned table and holds
                        // that table's rule on them, but one that declared NOT NULL for itself
                        // holds its own: PostgreSQL leaves that constraint the partition's even
                        // after the partitioned table declares the rule too.
                        boolean notNullIsLocal = coninhcount == 0 || t.isNotNullLocal(c.getName());
                        String conname = notNullConstraintName(t, c.getName());
                        table.insertRow(new Object[]{
                                oids.oid(constraintKey(schemaEntry.getKey(), t.getName(), conname)),
                                conname,
                                nsOid,
                                "n",
                                relOid,
                                0,
                                nnConkey,
                                null,
                                // A NOT NULL declared NOT VALID is a constraint whose rows have
                                // not been read, and PostgreSQL records that on the relation that
                                // declared it and on everything holding it from there.
                                false, false, t.isNotNullValidated(c.getName()),
                                notNullIsLocal, 0,
                                " ", " ",
                                " " /*confmatchtype*/, null, null, null, null, coninhcount,
                                t.isNotNullNoInherit(c.getName()),
                                true, // conenforced
                                null, null, false, 0, 0, 1
                        });
                    }
                }
            }
        }

        // Domain constraints (contypid points to domain type OID)
        for (Map.Entry<String, DomainType> domEntry : database.getDomains().entrySet()) {
            DomainType dom = domEntry.getValue();
            int domTypeOid = oids.oid("type:" + domEntry.getKey());
            // A domain's constraints belong to the schema the domain was created in. Filing every
            // one of them under public hid them from a query naming the domain's own schema.
            int publicNsOid = oids.oid("ns:"
                    + (dom.getSchemaName() != null ? dom.getSchemaName() : "public"));
            // Inline CHECK (from CREATE DOMAIN ... CHECK(...))
            if (dom.getCheckExpression() != null) {
                String conname = dom.getName() + "_check";
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + conname),
                        conname,
                        publicNsOid,
                        "c",        // contype = CHECK
                        0,          // conrelid (0 for domain constraints)
                        0,          // confrelid
                        null, null, // conkey, confkey
                        false, false, true,
                        true, 0,
                        " ", " ",
                        " " /*confmatchtype*/, null, null, null, null, 0 /*conpfeqop,conppeqop,conffeqop,confdelsetcols,coninhcount*/,
                        // A domain constraint inherits nowhere and PostgreSQL reports it false.
                        false, // connoinherit
                        true, // conenforced
                        dom.getCheckExpression(), null, false, 0, domTypeOid, 1
                });
            }
            // Named constraints (from ALTER DOMAIN ADD CONSTRAINT)
            for (DomainType.NamedConstraint nc : dom.getNamedConstraints()) {
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + nc.name()),
                        nc.name(),
                        publicNsOid,
                        "c",
                        0, 0,
                        null, null,
                        false, false, nc.isValidated(),
                        true, 0,
                        " ", " ",
                        " " /*confmatchtype*/, null, null, null, null, 0 /*conpfeqop,conppeqop,conffeqop,confdelsetcols,coninhcount*/,
                        false, // connoinherit
                        true, // conenforced
                        nc.rawCheckExpr(), null, false, 0, domTypeOid, 1
                });
            }
            // NOT NULL constraint on domain
            if (dom.isNotNull()) {
                String conname = dom.getName() + "_not_null";
                table.insertRow(new Object[]{
                        oids.oid("con:domain:" + dom.getName() + "." + conname),
                        conname,
                        publicNsOid,
                        "n",
                        0, 0,
                        null, null,
                        false, false, true,
                        true, 0,
                        " ", " ",
                        " " /*confmatchtype*/, null, null, null, null, 0 /*conpfeqop,conppeqop,conffeqop,confdelsetcols,coninhcount*/,
                        false, // connoinherit
                        true, // conenforced
                        null, null, false, 0, domTypeOid, 1
                });
            }
        }

        // A constraint trigger is a constraint as well as a trigger, and PostgreSQL gives it a
        // pg_constraint row of contype 't'. Without one, a client asking what constraints a table
        // carries, or joining pg_trigger.tgconstraint back to pg_constraint, is told the trigger
        // constrains nothing. One row per name: a trigger written for several events is held here
        // as one PgTrigger per event, and they are all the same constraint.
        Set<String> seenTriggerConstraints = new java.util.LinkedHashSet<>();
        for (Map.Entry<String, List<PgTrigger>> trigEntry : database.getAllTriggers().entrySet()) {
            for (PgTrigger trig : trigEntry.getValue()) {
                if (!trig.isConstraintTrigger() || trig.getTableName() == null) continue;
                String trigSchema = trig.getSchemaName() != null ? trig.getSchemaName() : "public";
                String key = constraintKey(trigSchema, trig.getTableName(), trig.getName());
                if (!seenTriggerConstraints.add(key.toLowerCase())) continue;
                table.insertRow(new Object[]{
                        oids.oid(key),
                        trig.getName(),
                        oids.oid("ns:" + trigSchema),
                        "t",
                        oids.oid("rel:" + trigSchema + "." + trig.getTableName()),
                        0,
                        null, null,
                        trig.isDeferrable(), trig.isInitiallyDeferred(), true,
                        true, 0,
                        " ", " ",
                        " " /*confmatchtype*/, null, null, null, null, 0,
                        true, // connoinherit: a trigger constraint does not travel to a child
                        true, // conenforced
                        null, null, false, 0, 0, 1
                });
            }
        }

        return table;
    }

    /**
     * The operator OIDs behind an EXCLUDE constraint, which is what {@code conexclop} holds -- one
     * per element, in the order they were written. Every other constraint kind has none, and
     * PostgreSQL reports null for it. A reader handed null for an EXCLUDE cannot tell what the
     * constraint excludes at all.
     */
    private List<Object> excludeOperators(Table t, StoredConstraint sc) {
        if (sc.getType() != StoredConstraint.Type.EXCLUDE || sc.getExcludeElements() == null) {
            return null;
        }
        List<Object> ops = new java.util.ArrayList<>();
        for (StoredConstraint.ExcludeElement e : sc.getExcludeElements()) {
            int idx = t.getColumnIndex(e.column());
            int typeOid = idx >= 0 ? t.getColumns().get(idx).getType().getOid() : 0;
            int opOid = CatalogTypeSystemBuilder.builtinOperatorOid(e.operator(), typeOid, typeOid);
            // A constraint always excludes by some operator, so PostgreSQL never stores a zero
            // here; int4eq stands in for one this engine does not model, as the foreign-key
            // equality arrays already do.
            ops.add(opOid != 0 ? Integer.valueOf(opOid) : Integer.valueOf(96));
        }
        return ops;
    }

    /**
     * How many parents hand this constraint down to the relation that carries it.
     *
     * <p>It is a count, not a flag: a rule two parents both declare reaches the child twice, and
     * that number is what decides whether letting one parent go leaves the child still holding
     * somebody else's rule. So it is answered from the parents the relation has now rather than
     * from a note of which one the copy came from, which could only ever remember one of them.
     *
     * <p>A CHECK keeps the name it was declared with, and it is the only kind ordinary inheritance
     * passes down -- a child's keys and foreign keys are its own. A partition takes every rule the
     * partitioned table declares, and its key constraint is renamed after the partition, so that
     * one is recognised by what it constrains instead of by the name it now answers to.
     */
    static int inheritedParentCount(Table t, StoredConstraint sc) {
        // A parent that dropped its own copy along with the column it tests told this relation
        // nothing, so what it handed down goes on being counted; the links no longer say so.
        int count = sc.getRetainedInheritCount();
        for (Table parent : t.getDirectParents()) {
            if (parent == null) continue;
            if (parent == t.getPartitionParent()) {
                if (isKeyConstraint(sc) ? hasMatchingKey(parent, sc)
                        : declaresConstraint(parent, sc.getName())) {
                    count++;
                }
            } else if (sc.getType() == StoredConstraint.Type.CHECK
                    && declaresInheritableCheck(parent, sc.getName())) {
                count++;
            }
        }
        return count;
    }

    /** True when this relation carries a constraint of the given name. */
    private static boolean declaresConstraint(Table parent, String name) {
        return name != null && parent.getConstraint(name) != null;
    }

    /**
     * True when the parent declares a CHECK of this name that travels. NO INHERIT says the rule
     * was never going to reach a child at all, so a child carrying that name declared it itself.
     */
    private static boolean declaresInheritableCheck(Table parent, String name) {
        if (name == null) return false;
        StoredConstraint parentCon = parent.getConstraint(name);
        return parentCon != null && parentCon.getType() == StoredConstraint.Type.CHECK
                && !parentCon.isNoInherit();
    }

    /** A constraint an index stands behind, which a partition takes from the partitioned table. */
    private static boolean isKeyConstraint(StoredConstraint sc) {
        return sc.getType() == StoredConstraint.Type.PRIMARY_KEY
                || sc.getType() == StoredConstraint.Type.UNIQUE;
    }

    /** True when the parent declares a key of the same kind over the same columns. */
    private static boolean hasMatchingKey(Table parent, StoredConstraint sc) {
        for (StoredConstraint parentCon : parent.getConstraints()) {
            if (parentCon.getType() != sc.getType()) continue;
            List<String> left = parentCon.getColumns();
            List<String> right = sc.getColumns();
            if (left == null || right == null || left.size() != right.size()) continue;
            boolean same = true;
            for (int i = 0; i < left.size(); i++) {
                if (left.get(i) == null || !left.get(i).equalsIgnoreCase(right.get(i))) {
                    same = false;
                    break;
                }
            }
            if (same) return true;
        }
        return false;
    }

    /**
     * True when this relation declares the column NOT NULL for everything below it as well. A rule
     * written NO INHERIT is about the declaring relation's own rows: PostgreSQL hands the column
     * down without it, so nothing underneath holds it and nothing underneath counts it.
     */
    static boolean declaresNotNull(Table table, String column) {
        if (table == null || column == null) return false;
        int idx = table.getColumnIndex(column);
        return idx >= 0 && !table.getColumns().get(idx).isNullable()
                && !table.isNotNullNoInherit(column);
    }

    /**
     * The name this column's NOT NULL constraint answers to.
     *
     * <p>PostgreSQL gives a constraint its name once, when it is created, and never works it out
     * again: a relation that never declared the rule holds the constraint of the relation that
     * did, under that relation's name -- which is the name a drop has to quote and the name a
     * refusal names it by. So the search climbs to the first relation whose own definition said
     * NOT NULL and stops there, even where a relation further up says it too, and a relation that
     * has been told the rule is its own answers with the name it holds.
     */
    static String notNullConstraintName(Table t, String column) {
        Table owner = t;
        while (!owner.isNotNullLocal(column)) {
            // A relation whose parent has let the column go, or which has left the parent that
            // declared the rule while another goes on handing it down, holds a name the links no
            // longer lead to. That name was written down when it stopped being derivable, and it
            // is the name the constraint has always answered to.
            String written = owner.inheritedNotNullName(column);
            if (written != null) return written;
            Table above = null;
            for (Table candidate : owner.getDirectParents()) {
                if (declaresNotNull(candidate, column)) { above = candidate; break; }
            }
            if (above == null) break;
            owner = above;
        }
        // The writer may have named the constraint; only fall back to the default spelling when
        // nobody did.
        String conname = owner.notNullConstraintName(column);
        return conname != null ? conname : owner.getName() + "_" + column + "_not_null";
    }

    Table buildPgIndex() {
        List<Column> cols = Cols.listOf(
                colNN("indexrelid", DataType.OID),
                colNN("indrelid", DataType.OID),
                colNN("indisunique", DataType.BOOLEAN),
                colNN("indisprimary", DataType.BOOLEAN),
                colNN("indisexclusion", DataType.BOOLEAN),
                col("indimmediate", DataType.BOOLEAN),
                col("indkey", DataType.INT2VECTOR),
                col("indnkeyatts", DataType.SMALLINT),
                col("indnatts", DataType.SMALLINT),
                col("indisvalid", DataType.BOOLEAN),
                col("indisready", DataType.BOOLEAN),
                col("indislive", DataType.BOOLEAN),
                col("indcheckxmin", DataType.BOOLEAN),
                // An expression column is an internal parse tree, not text a client may read:
                // pg_node_tree is what tells it so.
                col("indexprs", DataType.PG_NODE_TREE),
                col("indpred", DataType.PG_NODE_TREE),
                col("indisclustered", DataType.BOOLEAN),
                col("indisreplident", DataType.BOOLEAN),
                col("indoption", DataType.INT2VECTOR),
                col("indnullsnotdistinct", DataType.BOOLEAN),
                col("indclass", DataType.OIDVECTOR),
                col("indcollation", DataType.OIDVECTOR)
        );
        Table table = new Table("pg_index", cols);
        // Populate from stored index metadata
        for (Map.Entry<String, List<String>> idx : database.getIndexColumns().entrySet()) {
            String indexKey = idx.getKey();
            String indexName = Database.idxName(indexKey);
            List<String> indexCols = idx.getValue();
            // Use stored table name to find the exact table for this index
            String storedTableQualified = database.getIndexTable(indexKey);
            // Find the table that this index belongs to
            for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
                for (Map.Entry<String, com.memgres.engine.Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                    com.memgres.engine.Table t = tableEntry.getValue();
                    // Only match this index to its actual table (using stored metadata)
                    if (storedTableQualified != null) {
                        String qualifiedName = schemaEntry.getKey() + "." + t.getName();
                        if (!qualifiedName.equalsIgnoreCase(storedTableQualified)) continue;
                    }
                    boolean hasExprCols = false;
                    StringBuilder indkey = new StringBuilder();
                    StringBuilder exprParts = new StringBuilder();
                    boolean allResolved = true;
                    for (String colName : indexCols) {
                        int colIdx = t.getColumnIndex(colName);
                        if (colIdx < 0) {
                            // Expression column (e.g., lower(email)); use 0 per PostgreSQL convention
                            hasExprCols = true;
                            if (indkey.length() > 0) indkey.append(" ");
                            indkey.append(0);
                            if (exprParts.length() > 0) exprParts.append(", ");
                            exprParts.append(colName);
                        } else {
                            if (indkey.length() > 0) indkey.append(" ");
                            indkey.append(t.attnumAt(colIdx)); // the relation's attribute number
                        }
                    }
                    if (indkey.length() > 0) {
                        int indexOid = oids.oid("rel:" + schemaEntry.getKey() + "." + indexName);
                        int tableOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                        // Check uniqueness from both constraint metadata and explicit index flags
                        boolean isUnique = database.isUniqueIndex(indexKey) ||
                                t.getConstraints().stream()
                                .anyMatch(sc -> sc.getName().equalsIgnoreCase(indexName) &&
                                        (sc.getType() == StoredConstraint.Type.UNIQUE || sc.getType() == StoredConstraint.Type.PRIMARY_KEY));
                        boolean isPrimary = t.getConstraints().stream()
                                .anyMatch(sc -> sc.getName().equalsIgnoreCase(indexName) && sc.getType() == StoredConstraint.Type.PRIMARY_KEY);
                        // Store indkey as a PgVector (0-based int2vector)
                        List<Object> indkeyElems = new java.util.ArrayList<>();
                        for (String num : indkey.toString().split(" ")) {
                            indkeyElems.add(Integer.parseInt(num));
                        }
                        PgVector indkeyVec = new PgVector(indkeyElems);
                        // Get WHERE predicate for partial indexes. pg_get_expr prints the
                        // stored tree, so indpred holds the deparsed predicate -- with the
                        // parentheses the unpretty form always puts round an operator expression
                        // -- rather than the text CREATE INDEX happened to write.
                        String whereClause = CatalogHelper.deparseIndexPredicate(database,
                                schemaEntry.getKey() + "." + t.getName(),
                                database.getIndexWhereClause(indexKey));
                        String indexprs = hasExprCols ? exprParts.toString() : null;
                        // Build indoption, indclass, indcollation as PgVectors
                        List<String> columnOptions = database.getIndexColumnOptions(indexKey);
                        List<Object> optionElems = new java.util.ArrayList<>();
                        List<Object> classElems = new java.util.ArrayList<>();
                        List<Object> collElems = new java.util.ArrayList<>();
                        for (int ic = 0; ic < indkeyElems.size(); ic++) {
                            int optBits = 0;
                            if (columnOptions != null && ic < columnOptions.size()) {
                                String opts = columnOptions.get(ic);
                                if (opts != null) {
                                    if (opts.contains("DESC")) optBits |= 1;        // INDOPTION_DESC
                                    if (opts.contains("NULLS FIRST")) optBits |= 2; // INDOPTION_NULLS_FIRST
                                    if (opts.contains("NULLS LAST") && opts.contains("DESC")) {
                                        // DESC NULLS LAST is non-default for DESC; no extra bit needed
                                        // but NULLS FIRST bit should NOT be set
                                    }
                                }
                            }
                            optionElems.add(optBits);
                            // Resolve opclass OID based on column type
                            int opclassOid = resolveColumnOpclass(t, indexCols, ic, columnOptions, database);
                            classElems.add(opclassOid);
                            collElems.add(0);      // default collation
                        }
                        // INCLUDE columns: add to indkey but not to indoption/indclass
                        List<String> includeColumns = database.getIndexIncludeColumns(indexKey);
                        int nKeyAtts = indkeyElems.size();
                        if (includeColumns != null && !includeColumns.isEmpty()) {
                            for (String incCol : includeColumns) {
                                int colIdx = t.getColumnIndex(incCol);
                                if (colIdx >= 0) {
                                    indkeyElems.add(t.attnumAt(colIdx));
                                } else {
                                    indkeyElems.add(0);
                                }
                            }
                            indkeyVec = new PgVector(indkeyElems);
                        }
                        int totalAtts = indkeyElems.size();
                        boolean nullsNotDistinct = database.isIndexNullsNotDistinct(indexKey);
                        boolean isClustered = database.isClusteredIndex(indexKey);
                        boolean isExclusion = t.getConstraints().stream()
                                .anyMatch(sc -> sc.getName().equalsIgnoreCase(indexName)
                                        && sc.getType() == StoredConstraint.Type.EXCLUDE);
                        table.insertRow(new Object[]{indexOid, tableOid, isUnique, isPrimary,
                                isExclusion,
                                true, // indimmediate
                                indkeyVec,
                                (short) nKeyAtts, (short) totalAtts, true, true, true, false, indexprs, whereClause, isClustered,
                                false, // indisreplident
                                new PgVector(optionElems),
                                nullsNotDistinct, new PgVector(classElems), new PgVector(collElems)});
                        break;
                    }
                }
            }
        }
        // Also add indexes from PK and UNIQUE constraints (implicit indexes)
        Set<String> existingIndexNames = new HashSet<>();
        for (String key : database.getIndexColumns().keySet()) {
            existingIndexNames.add(key.toLowerCase());
        }
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, com.memgres.engine.Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                com.memgres.engine.Table t = tableEntry.getValue();
                int tableOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                        String indexName = sc.getName();
                        if (existingIndexNames.contains(
                                Database.idxKey(schemaEntry.getKey(), indexName).toLowerCase())) continue;
                        List<Object> indkeyList = new java.util.ArrayList<>();
                        for (String colName : sc.getColumns()) {
                            int colIdx = t.getColumnIndex(colName);
                            if (colIdx >= 0) {
                                indkeyList.add(t.attnumAt(colIdx));
                            }
                        }
                        if (!indkeyList.isEmpty()) {
                            int indexOid = oids.oid("rel:" + schemaEntry.getKey() + "." + indexName);
                            PgVector indkeyVec = new PgVector(indkeyList);
                            List<Object> optElems = new java.util.ArrayList<>();
                            List<Object> clsElems = new java.util.ArrayList<>();
                            List<Object> colElems = new java.util.ArrayList<>();
                            for (int ic = 0; ic < indkeyList.size(); ic++) {
                                optElems.add(0);
                                clsElems.add(1978);
                                colElems.add(0);
                            }
                            // Resolve opclass OIDs based on column types
                            List<Object> resolvedClsElems = new java.util.ArrayList<>();
                            List<String> scColumns = sc.getColumns();
                            for (int ic = 0; ic < scColumns.size(); ic++) {
                                int opclassOid = resolveColumnOpclass(t, scColumns, ic, null, database);
                                resolvedClsElems.add(opclassOid);
                            }
                            boolean constraintClustered = database.isClusteredIndex(indexName);
                            table.insertRow(new Object[]{
                                    indexOid, tableOid,
                                    true, // isUnique
                                    sc.getType() == StoredConstraint.Type.PRIMARY_KEY, // isPrimary
                                    false, // indisexclusion
                                    true, // indimmediate
                                    indkeyVec,
                                    (short) indkeyList.size(), (short) indkeyList.size(),
                                    true, true, true, false, null, null, constraintClustered,
                                    false, // indisreplident
                                    new PgVector(optElems),
                                    // The index a UNIQUE constraint is backed by keeps the
                                    // constraint's own NULLS NOT DISTINCT.
                                    sc.isNullsNotDistinct(),
                                    new PgVector(resolvedClsElems), new PgVector(colElems)
                            });
                        }
                    }
                }
            }
        }

        return table;
    }

    Table buildPgAttrdef() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("adrelid", DataType.OID),
                colNN("adnum", DataType.SMALLINT),
                col("adbin", DataType.PG_NODE_TREE)
        );
        Table table = new Table("pg_attrdef", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaEntry.getKey() + "." + t.getName());
                for (int i = 0; i < t.getColumns().size(); i++) {
                    Column c = t.getColumns().get(i);
                    // M14: GENERATED ... AS IDENTITY columns have no pg_attrdef row
                    // (atthasdef=f). The backing sequence is exposed via pg_depend /
                    // pg_get_serial_sequence, not as a column default.
                    if (c.getDefaultValue() != null && c.getDefaultValue().startsWith("__identity__")) {
                        continue;
                    }
                    if (c.isGenerated()) {
                        // Generated columns: store the generation expression in pg_attrdef
                        // pg_dump reads this to emit GENERATED ALWAYS AS (...) STORED/VIRTUAL
                        // pg_dump reads adbin back through pg_get_expr, so it has to be the
                        // deparsed form -- the same text information_schema reports.
                        String genExpr = CatalogHelper.renderGeneratedExpr(t, c);
                        table.insertRow(new Object[]{
                                oids.oid("attrdef:" + t.getName() + "." + c.getName()),
                                relOid, (short) t.attnumAt(i), genExpr
                        });
                    } else if (c.getDefaultValue() != null || c.getType() == DataType.SERIAL
                            || c.getType() == DataType.BIGSERIAL || c.getType() == DataType.SMALLSERIAL) {
                        String formatted = formatColumnDefault(c);
                        String defaultExpr = formatted != null ? formatted
                                : "nextval('" + t.getName() + "_" + c.getName() + "_seq'::regclass)";
                        table.insertRow(new Object[]{
                                oids.oid("attrdef:" + t.getName() + "." + c.getName()),
                                relOid, (short) t.attnumAt(i), defaultExpr
                        });
                    }
                }
            }
        }
        // A default written on a view column belongs to the view: PostgreSQL files it against the
        // view's own relation, which is why ALTER VIEW ... SET DEFAULT leaves the relation
        // underneath with no default at all. Filed nowhere here, the clause was readable through
        // information_schema and absent from the catalogue information_schema is derived from --
        // so pg_dump, which reads this relation, dumped a view whose default had gone.
        for (Database.ViewDef vd : database.getViews().values()) {
            List<Column> viewColumns = vd.cachedColumns();
            if (viewColumns == null) continue;
            String viewSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int viewOid = oids.oid("rel:" + viewSchema + "." + vd.name());
            for (int i = 0; i < viewColumns.size(); i++) {
                Column c = viewColumns.get(i);
                if (c.getDefaultValue() == null) continue;
                String rendered = formatColumnDefault(c);
                if (rendered == null) continue;
                table.insertRow(new Object[]{
                        oids.oid("attrdef:view:" + viewSchema + "." + vd.name() + "." + c.getName()),
                        viewOid, (short) (i + 1), rendered
                });
            }
        }
        return table;
    }

    /** True for a GENERATED ... AS IDENTITY column, which memgres records in its default. */
    private static boolean isIdentityColumn(Column c) {
        return c != null && c.getDefaultValue() != null
                && c.getDefaultValue().contains("__identity__");
    }

    Table buildPgDepend() {
        List<Column> cols = Cols.listOf(
                colNN("classid", DataType.OID),
                colNN("objid", DataType.OID),
                colNN("objsubid", DataType.INTEGER),
                colNN("refclassid", DataType.OID),
                colNN("refobjid", DataType.OID),
                colNN("refobjsubid", DataType.INTEGER),
                colNN("deptype", DataType.INTERNAL_CHAR)
        );
        Table table = new Table("pg_depend", cols);
        int pgClassOid = oids.oid("rel:pg_catalog.pg_class");

        // A sequence a column owns depends on that column. Every serial and identity column's
        // sequence is a sequence like any other, recorded with OWNED BY, so this one walk over
        // the sequences covers all three; composing the name from the column as well produced a
        // second row for the same pair, and pg_dump reads one row per sequence -- a sequence
        // named twice here was written into the dump twice, and the restore refused the second.
        for (java.util.Map.Entry<String, Sequence> seqEntry : database.getSequences().entrySet()) {
            Sequence seq = seqEntry.getValue();
            if (seq.getOwnedByTable() == null) continue;
            // Find the owning table and column index
            for (java.util.Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
                Table ownerTbl = schemaEntry.getValue().getTable(seq.getOwnedByTable());
                if (ownerTbl == null) continue;
                int colIdx = -1;
                Column ownerCol = null;
                for (int ci = 0; ci < ownerTbl.getColumns().size(); ci++) {
                    if (ownerTbl.getColumns().get(ci).getName().equalsIgnoreCase(seq.getOwnedByColumn())) {
                        colIdx = ci + 1;
                        ownerCol = ownerTbl.getColumns().get(ci);
                        break;
                    }
                }
                if (colIdx > 0) {
                    int seqOid = oids.oid("rel:" + seq.getSchemaName() + "." + seq.getName());
                    int ownerTblOid = oids.oid("rel:" + schemaEntry.getKey() + "." + ownerTbl.getName());
                    // An identity column's sequence is part of the column -- 'i', internal -- and
                    // cannot be dropped on its own; a serial's or an OWNED BY sequence's is 'a',
                    // auto, and goes when the column goes but can also be dropped by name.
                    String deptype = isIdentityColumn(ownerCol) ? "i" : "a";
                    table.insertRow(new Object[]{pgClassOid, seqOid, 0, pgClassOid, ownerTblOid,
                            colIdx, deptype});
                }
                break;
            }
        }

        // View dependencies: rewrite rule -> referenced table (via pg_rewrite)
        int pgRewriteClassOid = oids.oid("rel:pg_catalog.pg_rewrite");
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int ruleOid = oids.oid("rule:_RETURN_" + vd.name());
            // Extract referenced tables from the view's SELECT statement
            if (vd.query() instanceof com.memgres.engine.parser.ast.SelectStmt && ((com.memgres.engine.parser.ast.SelectStmt) vd.query()).from() != null) {
                com.memgres.engine.parser.ast.SelectStmt sel = (com.memgres.engine.parser.ast.SelectStmt) vd.query();
                for (com.memgres.engine.parser.ast.SelectStmt.FromItem fromItem : sel.from()) {
                    collectViewDependencies(fromItem, vSchema, pgRewriteClassOid, ruleOid, pgClassOid, table);
                }
            }
        }

        // plpgsql extension dependencies (deptype='e' = extension member)
        int pgExtensionClassOid = oids.oid("rel:pg_catalog.pg_extension");
        int pgLanguageClassOid = oids.oid("rel:pg_catalog.pg_language");
        int pgProcClassOid = oids.oid("rel:pg_catalog.pg_proc");
        int plpgsqlExtOid = oids.oid("ext:plpgsql");
        // plpgsql language depends on plpgsql extension
        table.insertRow(new Object[]{pgLanguageClassOid, oids.oid("lang:plpgsql"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        // plpgsql handler procs depend on plpgsql extension
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_call_handler"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_inline_handler"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});
        table.insertRow(new Object[]{pgProcClassOid, oids.oid("proc:plpgsql_validator"), 0, pgExtensionClassOid, plpgsqlExtOid, 0, "e"});

        return table;
    }

    Table buildPgRewrite() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("rulename", DataType.NAME),
                colNN("ev_class", DataType.OID),
                col("ev_type", DataType.INTERNAL_CHAR),
                col("ev_enabled", DataType.INTERNAL_CHAR),
                col("is_instead", DataType.BOOLEAN),
                // A rule's qualification and action are parse trees, not text a client can read
                col("ev_qual", DataType.PG_NODE_TREE),
                col("ev_action", DataType.PG_NODE_TREE),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_rewrite", cols);
        // Views have implicit _RETURN rules in PG
        for (Database.ViewDef vd : database.getViews().values()) {
            String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
            int relOid = oids.oid("rel:" + vSchema + "." + vd.name());
            table.insertRow(new Object[]{
                    oids.oid("rule:_RETURN_" + vd.name()), "_RETURN", relOid,
                    "1", "O", true, null, null, 1
            });
        }
        // A rule written with CREATE RULE is a row here too, and it is the only place a client can
        // learn that a relation carries one. Two schemas may each hold a relation of the same
        // name, and each carries its own rules, so both the relation this points at and the rule's
        // own identity are settled by the schema as well as by the name.
        for (Database.StoredRule rule : database.getRuleEntries()) {
            String ruleSchema = rule.getSchema() == null ? "public" : rule.getSchema();
            int relOid = oids.oid("rel:" + ruleSchema + "." + rule.getTable());
            table.insertRow(new Object[]{
                    oids.oid("rule:" + ruleSchema + "." + rule.getTable() + ":" + rule.getName()),
                    rule.getName(), relOid,
                    ruleEventType(rule.getEvent()),
                    rule.getEnabledState(),
                    rule.isInstead(),
                    null, null, 1
            });
        }
        return table;
    }

    /** The code {@code pg_rewrite.ev_type} gives an event, as PostgreSQL numbers them. */
    static String ruleEventType(String event) {
        if (event == null) return "1";
        if ("SELECT".equalsIgnoreCase(event)) return "1";
        if ("UPDATE".equalsIgnoreCase(event)) return "2";
        if ("INSERT".equalsIgnoreCase(event)) return "3";
        if ("DELETE".equalsIgnoreCase(event)) return "4";
        return "1";
    }

    Table buildPgDescription() {
        List<Column> cols = Cols.listOf(
                colNN("objoid", DataType.OID),
                colNN("classoid", DataType.OID),
                colNN("objsubid", DataType.INTEGER),
                col("description", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_description", cols);
        // classoid = OID of the catalog table in pg_class (must match ::regclass resolution)
        int pgAmClassOid = oids.oid("rel:pg_catalog.pg_am");
        int pgLanguageClassOid = oids.oid("rel:pg_catalog.pg_language");
        int pgExtensionClassOid = oids.oid("rel:pg_catalog.pg_extension");

        // Access method descriptions
        table.insertRow(new Object[]{2, pgAmClassOid, 0, "heap table access method", 1});
        table.insertRow(new Object[]{403, pgAmClassOid, 0, "b-tree index access method", 1});
        table.insertRow(new Object[]{405, pgAmClassOid, 0, "hash index access method", 1});
        table.insertRow(new Object[]{783, pgAmClassOid, 0, "GiST index access method", 1});
        table.insertRow(new Object[]{2742, pgAmClassOid, 0, "GIN index access method", 1});
        table.insertRow(new Object[]{4000, pgAmClassOid, 0, "SP-GiST index access method", 1});
        table.insertRow(new Object[]{3580, pgAmClassOid, 0, "block range index (BRIN) access method", 1});

        // Language descriptions
        table.insertRow(new Object[]{oids.oid("lang:internal"), pgLanguageClassOid, 0, "built-in functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:c"), pgLanguageClassOid, 0, "dynamically-loaded C functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:sql"), pgLanguageClassOid, 0, "SQL-language functions", 1});
        table.insertRow(new Object[]{oids.oid("lang:plpgsql"), pgLanguageClassOid, 0, "PL/pgSQL procedural language", 1});

        // Extension descriptions
        table.insertRow(new Object[]{oids.oid("ext:plpgsql"), pgExtensionClassOid, 0, "PL/pgSQL procedural language", 1});

        // Proc descriptions for handler functions
        int pgProcClassOid = oids.oid("rel:pg_catalog.pg_proc");
        table.insertRow(new Object[]{oids.oid("proc:heap_tableam_handler"), pgProcClassOid, 0, "heap table access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:bthandler"), pgProcClassOid, 0, "b-tree index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:hashhandler"), pgProcClassOid, 0, "hash index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:gisthandler"), pgProcClassOid, 0, "GiST index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:ginhandler"), pgProcClassOid, 0, "GIN index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:spghandler"), pgProcClassOid, 0, "SP-GiST index access method handler", 1});
        table.insertRow(new Object[]{oids.oid("proc:brinhandler"), pgProcClassOid, 0, "BRIN index access method handler", 1});

        // User-defined comments (from COMMENT ON statements). Every kind that can carry one is
        // covered, and each row takes the OID of the object the comment is actually on: a comment
        // key names one schema, so a.t's comment does not describe b.t.
        for (Map.Entry<String, String> entry : database.getComments().entrySet()) {
            String key = entry.getKey(); // "<kind>:<schema>.<name>"
            String desc = entry.getValue();
            int colonIdx = key.indexOf(':');
            if (colonIdx < 0) continue;
            Object[] row = describeComment(key.substring(0, colonIdx),
                    key.substring(colonIdx + 1), desc);
            if (row != null) table.insertRow(row);
        }

        return table;
    }

    /**
     * The pg_description row a stored comment makes, or null when nothing of that name is there
     * to describe. {@code objName} is {@code schema.name} for everything a schema holds, so the
     * OID comes from that schema and not from whichever one was reached first.
     */
    private Object[] describeComment(String objType, String objName, String desc) {
        int pgClassClassOid = oids.oid("rel:pg_catalog.pg_class");
        int pgNamespaceClassOid = oids.oid("rel:pg_catalog.pg_namespace");
        int pgTypeClassOid = oids.oid("rel:pg_catalog.pg_type");
        int pgProcClassOid = oids.oid("rel:pg_catalog.pg_proc");
        int pgConstraintClassOid = oids.oid("rel:pg_catalog.pg_constraint");
        int pgTriggerClassOid = oids.oid("rel:pg_catalog.pg_trigger");
        int pgRewriteClassOid = oids.oid("rel:pg_catalog.pg_rewrite");
        int pgPolicyClassOid = oids.oid("rel:pg_catalog.pg_policy");
        String schema = TypeNamespace.schemaOfKey(objName);
        String bare = TypeNamespace.nameOfKey(objName);

        switch (objType) {
            case "table":
            case "relation":
            case "view":
            case "materialized view":
            case "index":
            case "sequence":
            case "foreign table": {
                // All six are relations, and a relation's OID is minted under its own schema.
                if (RelationNamespace.kindOf(database, schema, bare) == null) return null;
                return new Object[]{oids.oid("rel:" + schema + "." + bare),
                        pgClassClassOid, 0, desc, 1};
            }
            case "column": {
                // objName is "<schema>.<relation>.<column>". A view carries column comments too,
                // and its columns are not in the schema's table map.
                int dotIdx = bare.lastIndexOf('.');
                if (dotIdx <= 0) return null;
                String relName = bare.substring(0, dotIdx);
                String colName = bare.substring(dotIdx + 1);
                Schema s = database.getSchema(schema);
                Table t = s == null ? null : s.getTable(relName);
                int colIdx = -1;
                // A comment on a column is filed against the column's attribute number, and a
                // column dropped before it leaves the ones after holding the numbers they had.
                int colAttnum = -1;
                if (t != null) {
                    colIdx = t.getColumnIndex(colName);
                    if (colIdx >= 0) colAttnum = t.attnumAt(colIdx);
                } else {
                    Database.ViewDef v = database.getView(schema, relName);
                    List<Column> vcols = v == null ? null : v.cachedColumns();
                    if (vcols != null) {
                        for (int i = 0; i < vcols.size(); i++) {
                            if (vcols.get(i).getName().equalsIgnoreCase(colName)) { colIdx = i; break; }
                        }
                    }
                }
                if (colIdx < 0) {
                    // A composite type's attributes are columns too, held by the relation the
                    // type carries rather than by a table of that name.
                    List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> fields = database.getCompositeType(relName);
                    if (fields != null) {
                        for (int i = 0; i < fields.size(); i++) {
                            if (fields.get(i).name().equalsIgnoreCase(colName)) { colIdx = i; break; }
                        }
                    }
                }
                if (colIdx < 0) return null;
                return new Object[]{oids.oid("rel:" + schema + "." + relName),
                        pgClassClassOid, colAttnum > 0 ? colAttnum : colIdx + 1, desc, 1};
            }
            case "type":
            case "domain": {
                String typeKey = TypeNamespace.key(schema, bare);
                if (!database.typeKeys().contains(typeKey)) return null;
                return new Object[]{oids.oid("type:" + typeKey), pgTypeClassOid, 0, desc, 1};
            }
            case "function":
            case "procedure":
            case "routine":
            case "aggregate": {
                PgFunction fn = database.getFunction(bare);
                if (fn == null) return null;
                return new Object[]{oids.oid("proc:" + bare), pgProcClassOid, 0, desc, 1};
            }
            case "schema": {
                if (database.getSchema(objName) == null) return null;
                return new Object[]{oids.oid("ns:" + objName), pgNamespaceClassOid, 0, desc, 1};
            }
            // The three relation-scoped kinds are keyed "<schema>.<relation>.<object>"; each
            // catalog mints its OIDs under its own spelling of that, so each is rebuilt here.
            case "constraint": {
                int dotIdx = bare.lastIndexOf('.');
                if (dotIdx <= 0) return null;
                return new Object[]{oids.oid("con:" + schema + "." + bare),
                        pgConstraintClassOid, 0, desc, 1};
            }
            case "trigger": {
                int dotIdx = bare.lastIndexOf('.');
                if (dotIdx <= 0) return null;
                return new Object[]{oids.oid("trig:" + schema + "." + bare),
                        pgTriggerClassOid, 0, desc, 1};
            }
            case "rule": {
                int dotIdx = bare.lastIndexOf('.');
                if (dotIdx <= 0) return null;
                return new Object[]{oids.oid("rule:" + schema + "." + bare.substring(0, dotIdx)
                        + ":" + bare.substring(dotIdx + 1)), pgRewriteClassOid, 0, desc, 1};
            }
            case "policy": {
                int dotIdx = bare.lastIndexOf('.');
                if (dotIdx <= 0) return null;
                return new Object[]{oids.oid("pol:" + schema + "." + bare),
                        pgPolicyClassOid, 0, desc, 1};
            }
            default:
                return null;
        }
    }

    Table buildPgTrigger() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("tgrelid", DataType.OID),
                colNN("tgname", DataType.NAME),
                col("tgfoid", DataType.OID),
                col("tgtype", DataType.SMALLINT),
                col("tgenabled", DataType.INTERNAL_CHAR),
                col("tgisinternal", DataType.BOOLEAN),
                col("tgconstrrelid", DataType.OID),
                col("tgconstrindid", DataType.OID),
                col("tgdeferrable", DataType.BOOLEAN),
                col("tginitdeferred", DataType.BOOLEAN),
                col("tgnargs", DataType.SMALLINT),
                col("tgargs", DataType.BYTEA),
                col("tgattr", DataType.INT2VECTOR),
                col("tgqual", DataType.PG_NODE_TREE),
                col("tgconstraint", DataType.OID),
                col("tgoldtable", DataType.NAME),
                col("tgnewtable", DataType.NAME),
                col("tgparentid", DataType.OID),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_trigger", cols);
        // Group triggers by relation and name to combine multiple events into one pg_trigger
        // row. A trigger name is only unique within its relation, so grouping by name alone
        // merged same-named triggers on different tables and lost all but one of them.
        Map<String, List<PgTrigger>> byName = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, List<PgTrigger>> entry : database.getAllTriggers().entrySet()) {
            for (PgTrigger trigger : entry.getValue()) {
                String key = (trigger.getTableName() == null ? "" : trigger.getTableName().toLowerCase())
                        + "." + trigger.getName().toLowerCase();
                byName.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(trigger);
            }
        }
        for (Map.Entry<String, List<PgTrigger>> entry : byName.entrySet()) {
            PgTrigger first = entry.getValue().get(0);
            String trigSchema = first.getSchemaName() != null ? first.getSchemaName() : "public";
            Table t = null;
            Schema schema = database.getSchemas().get(trigSchema);
            if (schema != null) t = schema.getTable(first.getTableName());
            if (t == null) {
                for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                    Table candidate = se.getValue().getTable(first.getTableName());
                    if (candidate != null) { t = candidate; trigSchema = se.getKey(); break; }
                }
            }
            // An INSTEAD OF trigger is defined on a view, which is not among a schema's tables and
            // so is not found above. PostgreSQL still records the view's OID here, and every join
            // from pg_trigger to pg_class goes through it; leaving 0 lost the trigger from \d, from
            // JDBC metadata and from every schema-diff tool. The key is the one pg_class builds the
            // view's own row with. Tables are searched first, so a table still wins the name.
            String trigRelName = first.getTableName();
            boolean relationFound = t != null;
            if (t == null) {
                for (Database.ViewDef vd : database.getViews().values()) {
                    if (vd.name().equalsIgnoreCase(first.getTableName())) {
                        trigSchema = vd.schemaName() != null ? vd.schemaName() : "public";
                        trigRelName = vd.name();
                        relationFound = true;
                        break;
                    }
                }
            }
            int relOid = relationFound ? oids.oid("rel:" + trigSchema + "." + trigRelName) : 0;

            // Build tgtype bitmask (PG convention):
            // bit 0 = ROW (1), bit 1 = BEFORE (2), bit 2 = INSERT (4),
            // bit 3 = DELETE (8), bit 4 = UPDATE (16), bit 5 = TRUNCATE (32),
            // bit 6 = INSTEAD (64)
            int tgtype = first.isForEachStatement() ? 0 : 1; // bit 0 = FOR EACH ROW
            if (first.getTiming() == PgTrigger.Timing.BEFORE) tgtype |= (1 << 1);
            else if (first.getTiming() == PgTrigger.Timing.INSTEAD_OF) tgtype |= (1 << 6);
            // AFTER has no dedicated bit in PG (absence of BEFORE = AFTER)
            for (PgTrigger trig : entry.getValue()) {
                switch (trig.getEvent()) {
                    case INSERT:
                        tgtype |= (1 << 2);
                        break;
                    case DELETE:
                        tgtype |= (1 << 3);
                        break;
                    case UPDATE:
                        tgtype |= (1 << 4);
                        break;
                    case TRUNCATE:
                        tgtype |= (1 << 5);
                        break;
                }
            }

            // Resolve trigger function OID from pg_proc
            int tgfoid = oids.oid("proc:" + first.getFunctionName());

            String tgenabled = first.getEnabledState();
            String whenCondition = first.getWhenClause();

            // The arguments written after the function name are part of what the trigger is:
            // TG_NARGS and TG_ARGV read them at run time, and pg_dump reads them back out of
            // tgnargs/tgargs. PG stores them as one bytea of NUL-terminated strings.
            List<String> trigArgs = null;
            for (PgTrigger trig : entry.getValue()) {
                if (trig.getArgs() != null && !trig.getArgs().isEmpty()) { trigArgs = trig.getArgs(); break; }
            }
            int tgnargs = trigArgs == null ? 0 : trigArgs.size();
            byte[] tgargs = encodeTriggerArgs(trigArgs);

            // tgattr holds the attnums of an UPDATE OF column list. A trigger with no column
            // list has an empty vector rather than a null one, which is what lets a client write
            // array_length(tgattr, 1) without a null check.
            List<Object> attrNums = new java.util.ArrayList<>();
            for (PgTrigger trig : entry.getValue()) {
                if (trig.getUpdateColumns() == null || t == null) continue;
                for (String colName : trig.getUpdateColumns()) {
                    int attnum = t.attnumOf(colName);
                    if (attnum > 0 && !attrNums.contains(attnum)) attrNums.add(attnum);
                }
            }
            // A CONSTRAINT TRIGGER's deferrability is part of when it runs, and a client asking
            // whether SET CONSTRAINTS can move it reads these two columns to find out.
            boolean trigDeferrable = false;
            boolean trigDeferred = false;
            for (PgTrigger trig : entry.getValue()) {
                if (trig.isDeferrable()) trigDeferrable = true;
                if (trig.isInitiallyDeferred()) trigDeferred = true;
            }
            // A constraint trigger has a pg_constraint row of its own; tgconstraint is what points
            // a reader at it, and reporting zero said the trigger was not a constraint at all.
            // The relation the FROM clause named is filed under tgconstrrelid.
            int tgconstraint = first.isConstraintTrigger()
                    ? oids.oid(constraintKey(trigSchema, first.getTableName(), first.getName())) : 0;
            // A partition's copy of its parent's trigger points back at the trigger it was cloned
            // from; that is how a reader tells a copy from a trigger written on the partition, and
            // reporting zero for it said every partition had declared one of its own.
            int tgparentid = first.getClonedFromTable() == null ? 0
                    : oids.oid("trig:" + trigSchema + "." + first.getClonedFromTable()
                            + "." + first.getName());
            int tgconstrrelid = 0;
            String fromRelation = first.getConstraintRelation();
            if (fromRelation != null) {
                int dot = fromRelation.indexOf('.');
                String fromSchema = dot > 0 ? fromRelation.substring(0, dot) : trigSchema;
                String fromName = dot > 0 ? fromRelation.substring(dot + 1) : fromRelation;
                tgconstrrelid = oids.oid("rel:" + fromSchema + "." + fromName);
            }
            table.insertRow(new Object[]{
                    oids.oid("trig:" + trigSchema + "." + first.getTableName() + "." + first.getName()),
                    relOid, first.getName(),
                    tgfoid, (short) tgtype, tgenabled, false, tgconstrrelid, 0, trigDeferrable, trigDeferred,
                    (short) tgnargs, tgargs, new PgVector(attrNums), whenCondition, tgconstraint,
                    first.getOldTransitionTable(), first.getNewTransitionTable(), tgparentid, 1
            });
        }
        addForeignKeyTriggers(table);
        return table;
    }

    /**
     * The rows a foreign key puts in pg_trigger. PostgreSQL enforces a foreign key with four
     * internal row triggers — an INSERT and an UPDATE check on the referencing table, a DELETE
     * and an UPDATE action on the referenced one — and they are as much a part of the catalog as
     * the constraint is: a client counting the triggers on a table, or looking for what a
     * constraint installed, sees nothing at all without them. They are marked tgisinternal, which
     * is how a tool that wants only the triggers a user wrote leaves them out.
     */
    private void addForeignKeyTriggers(Table table) {
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (com.memgres.engine.Table t : schemaEntry.getValue().getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    if (sc.getReferencesTable() == null) continue;
                    // The action triggers are filed against the referenced relation, so unless
                    // that relation can be named there is nothing to file them against and the
                    // foreign key contributes no rows at all.
                    String refSchema = referencedSchema(schemaName, sc);
                    if (refSchema == null) continue;
                    com.memgres.engine.Table refTable =
                            database.getSchemas().get(refSchema).getTable(sc.getReferencesTable());
                    int childOid = oids.oid("rel:" + schemaName + "." + t.getName());
                    int parentOid = oids.oid("rel:" + refSchema + "." + sc.getReferencesTable());
                    int conOid = oids.oid(constraintKey(schemaName, t.getName(), sc.getName()));
                    int indOid = 0;
                    for (StoredConstraint refCon : refTable.getConstraints()) {
                        if (refCon.getType() == StoredConstraint.Type.PRIMARY_KEY
                                || refCon.getType() == StoredConstraint.Type.UNIQUE) {
                            indOid = oids.oid("rel:" + refSchema + "." + refCon.getName());
                            break;
                        }
                    }
                    String key = schemaName + "." + t.getName() + "." + sc.getName();
                    // The check triggers sit on the referencing table and carry the constraint's
                    // own deferrability.
                    riTrigger(table, key, "c", 5, childOid, parentOid, conOid, indOid,
                            "RI_FKey_check_ins", sc.isDeferrable(), sc.isInitiallyDeferred());
                    riTrigger(table, key, "c", 17, childOid, parentOid, conOid, indOid,
                            "RI_FKey_check_upd", sc.isDeferrable(), sc.isInitiallyDeferred());
                    // An action trigger can only wait until the end of the transaction when
                    // there is nothing for it to do but complain: NO ACTION defers with the
                    // constraint, while a cascade or a set-null has to run as the row changes.
                    boolean delDefer = isNoAction(sc.getOnDelete()) && sc.isDeferrable();
                    boolean updDefer = isNoAction(sc.getOnUpdate()) && sc.isDeferrable();
                    riTrigger(table, key, "a", 9, parentOid, childOid, conOid, indOid,
                            riFunction(sc.getOnDelete(), "del"),
                            delDefer, delDefer && sc.isInitiallyDeferred());
                    riTrigger(table, key, "a", 17, parentOid, childOid, conOid, indOid,
                            riFunction(sc.getOnUpdate(), "upd"),
                            updDefer, updDefer && sc.isInitiallyDeferred());
                }
            }
        }
    }

    /** True for the action that only checks, which is also the one a foreign key defaults to. */
    private static boolean isNoAction(StoredConstraint.FkAction action) {
        return action == null || action == StoredConstraint.FkAction.NO_ACTION;
    }

    /** The referential-integrity function a referential action is carried out by. */
    private static String riFunction(StoredConstraint.FkAction action, String event) {
        if (action == StoredConstraint.FkAction.CASCADE) return "RI_FKey_cascade_" + event;
        if (action == StoredConstraint.FkAction.SET_NULL) return "RI_FKey_setnull_" + event;
        if (action == StoredConstraint.FkAction.SET_DEFAULT) return "RI_FKey_setdefault_" + event;
        if (action == StoredConstraint.FkAction.RESTRICT) return "RI_FKey_restrict_" + event;
        return "RI_FKey_noaction_" + event;
    }

    /** One internal referential-integrity trigger row. */
    private void riTrigger(Table table, String constraintKey, String side, int tgtype,
                           int relOid, int otherOid, int conOid, int indOid,
                           String function, boolean deferrable, boolean deferred) {
        int trigOid = oids.oid("trig:ri:" + side + ":" + tgtype + ":" + constraintKey);
        table.insertRow(new Object[]{
                trigOid, relOid, "RI_ConstraintTrigger_" + side + "_" + trigOid,
                oids.oid("proc:" + function), (short) tgtype, "O", true,
                otherOid, indOid, deferrable, deferred,
                (short) 0, new byte[0], new PgVector(new java.util.ArrayList<Object>()), null, conOid,
                null, null, 0, 1
        });
    }

    /**
     * The trigger arguments as PostgreSQL stores them in {@code pg_trigger.tgargs}: every
     * argument in order, each followed by a NUL byte. An argument list of {@code ('a b', '')}
     * is nine bytes, not two strings.
     */
    private static byte[] encodeTriggerArgs(List<String> args) {
        if (args == null || args.isEmpty()) return new byte[0];
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        for (String arg : args) {
            byte[] bytes = (arg == null ? "" : arg).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            out.write(bytes, 0, bytes.length);
            out.write(0);
        }
        return out.toByteArray();
    }

    /** Recursively collect table references from FROM items for view dependency tracking. */
    void collectViewDependencies(com.memgres.engine.parser.ast.SelectStmt.FromItem fromItem,
                                  String defaultSchema,
                                  int rewriteClassOid, int ruleOid,
                                  int pgClassOid, Table depTable) {
        if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.TableRef) {
            com.memgres.engine.parser.ast.SelectStmt.TableRef tr = (com.memgres.engine.parser.ast.SelectStmt.TableRef) fromItem;
            String tSchema = tr.schema() != null ? tr.schema() : defaultSchema;
            String tName = tr.table();
            // Verify the table exists in some schema
            boolean found = false;
            for (Map.Entry<String, Schema> se : database.getSchemas().entrySet()) {
                if (se.getKey().equalsIgnoreCase(tSchema) && se.getValue().getTable(tName) != null) {
                    found = true;
                    break;
                }
            }
            if (found) {
                int tableOid = oids.oid("rel:" + tSchema + "." + tName);
                // deptype 'n' = normal dependency
                depTable.insertRow(new Object[]{rewriteClassOid, ruleOid, 0, pgClassOid, tableOid, 0, "n"});
            }
        } else if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.JoinFrom) {
            com.memgres.engine.parser.ast.SelectStmt.JoinFrom jf = (com.memgres.engine.parser.ast.SelectStmt.JoinFrom) fromItem;
            collectViewDependencies(jf.left(), defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
            collectViewDependencies(jf.right(), defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
        } else if (fromItem instanceof com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) {
            com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom sq = (com.memgres.engine.parser.ast.SelectStmt.SubqueryFrom) fromItem;
            if (sq.subquery() instanceof com.memgres.engine.parser.ast.SelectStmt && ((com.memgres.engine.parser.ast.SelectStmt) sq.subquery()).from() != null) {
                com.memgres.engine.parser.ast.SelectStmt sub = (com.memgres.engine.parser.ast.SelectStmt) sq.subquery();
                for (com.memgres.engine.parser.ast.SelectStmt.FromItem fi : sub.from()) {
                    collectViewDependencies(fi, defaultSchema, rewriteClassOid, ruleOid, pgClassOid, depTable);
                }
            }
        }
    }

    /**
     * Resolve the btree opclass OID for a given index column based on its data type.
     * Maps common types to their default btree opclass OIDs (matching pg_opclass).
     */
    private int resolveColumnOpclass(Table t, List<String> indexCols, int colIndex,
                                     List<String> columnOptions, Database db) {
        // If an explicit opclass is specified in options, resolve it by name
        if (columnOptions != null && colIndex < columnOptions.size()) {
            String opts = columnOptions.get(colIndex);
            if (opts != null && opts.contains("opclass:")) {
                String opclassName = null;
                for (String part : opts.split(" ")) {
                    if (part.startsWith("opclass:")) {
                        opclassName = part.substring(8);
                        break;
                    }
                }
                if (opclassName != null) {
                    return oids.oid("opclass:" + opclassName);
                }
            }
        }
        // Resolve by column data type
        String colName = colIndex < indexCols.size() ? indexCols.get(colIndex) : null;
        if (colName != null) {
            int ci = t.getColumnIndex(colName);
            if (ci >= 0) {
                DataType dt = t.getColumns().get(ci).getType();
                switch (dt) {
                    case INTEGER:
                    case SERIAL:
                        return 1978; // int4_ops
                    case TEXT:
                    case VARCHAR:
                    case CHAR:
                        return oids.oid("opclass:text_ops");
                    case BIGINT:
                    case BIGSERIAL:
                        return oids.oid("opclass:int8_ops");
                    case SMALLINT:
                    case SMALLSERIAL:
                        return oids.oid("opclass:int2_ops");
                    case BOOLEAN:
                        return oids.oid("opclass:bool_ops");
                    case REAL:
                        return oids.oid("opclass:float4_ops");
                    case DOUBLE_PRECISION:
                        return oids.oid("opclass:float8_ops");
                    case NUMERIC:
                        return oids.oid("opclass:numeric_ops");
                    case DATE:
                        return oids.oid("opclass:date_ops");
                    case TIMESTAMP:
                        return oids.oid("opclass:timestamp_ops");
                    case TIMESTAMPTZ:
                        return oids.oid("opclass:timestamptz_ops");
                    case UUID:
                        return oids.oid("opclass:uuid_ops");
                    default:
                        return 1978; // fallback to int4_ops
                }
            }
        }
        return 1978; // default fallback
    }
}
