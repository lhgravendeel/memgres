package com.memgres.engine.parser.ast;

/**
 * Column definition within CREATE TABLE.
 */
public final class ColumnDef {
    public final String name;
    public final String typeName;
    public final Integer precision;
    public final Integer scale;
    public final boolean notNull;
    public final boolean primaryKey;
    public final boolean unique;
    public final Expression defaultExpr;
    public final String referencesTable;
    public final String referencesColumn;
    public final String generatedExpr;
    public final String identity;                // stored as string, null otherwise
    public final String refOnDelete;
    public final String refOnUpdate;
    public final Long identityStart;             // START WITH value for identity columns
    public final Long identityIncrement;         // INCREMENT BY value for identity columns
    public final boolean deferrable;             // DEFERRABLE on column-level FK
    public final boolean initiallyDeferred;      // INITIALLY DEFERRED on column-level FK
    public final Expression checkConstraintExpr; // or null

    public ColumnDef(
            String name,
            String typeName,
            Integer precision,
            Integer scale,
            boolean notNull,
            boolean primaryKey,
            boolean unique,
            Expression defaultExpr,
            String referencesTable,
            String referencesColumn,
            String generatedExpr,
            String identity,
            String refOnDelete,
            String refOnUpdate,
            Long identityStart,
            Long identityIncrement,
            boolean deferrable,
            boolean initiallyDeferred,
            Expression checkConstraintExpr
    ) {
        this.name = name;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
        this.notNull = notNull;
        this.primaryKey = primaryKey;
        this.unique = unique;
        this.defaultExpr = defaultExpr;
        this.referencesTable = referencesTable;
        this.referencesColumn = referencesColumn;
        this.generatedExpr = generatedExpr;
        this.identity = identity;
        this.refOnDelete = refOnDelete;
        this.refOnUpdate = refOnUpdate;
        this.identityStart = identityStart;
        this.identityIncrement = identityIncrement;
        this.deferrable = deferrable;
        this.initiallyDeferred = initiallyDeferred;
        this.checkConstraintExpr = checkConstraintExpr;
    }

    /** Full constructor without checkConstraintExpr (backwards compatible). */
    public ColumnDef(String name, String typeName, Integer precision, Integer scale,
                     boolean notNull, boolean primaryKey, boolean unique,
                     Expression defaultExpr, String referencesTable, String referencesColumn,
                     String generatedExpr, String identity, String refOnDelete, String refOnUpdate,
                     Long identityStart, Long identityIncrement, boolean deferrable, boolean initiallyDeferred) {
        this(name, typeName, precision, scale, notNull, primaryKey, unique,
                defaultExpr, referencesTable, referencesColumn, generatedExpr, identity,
                refOnDelete, refOnUpdate, identityStart, identityIncrement, deferrable, initiallyDeferred, null);
    }

    /** Constructor with identity options and deferrable. */
    public ColumnDef(String name, String typeName, Integer precision, Integer scale,
                     boolean notNull, boolean primaryKey, boolean unique,
                     Expression defaultExpr, String referencesTable, String referencesColumn,
                     String generatedExpr, String identity, String refOnDelete, String refOnUpdate) {
        this(name, typeName, precision, scale, notNull, primaryKey, unique,
                defaultExpr, referencesTable, referencesColumn, generatedExpr, identity,
                refOnDelete, refOnUpdate, null, null, false, false, null);
    }

    /** Backward-compatible constructor without FK actions. */
    public ColumnDef(String name, String typeName, Integer precision, Integer scale,
                     boolean notNull, boolean primaryKey, boolean unique,
                     Expression defaultExpr, String referencesTable, String referencesColumn,
                     String generatedExpr, String identity) {
        this(name, typeName, precision, scale, notNull, primaryKey, unique,
                defaultExpr, referencesTable, referencesColumn, generatedExpr, identity,
                null, null, null, null, false, false, null);
    }

    /** Backward-compatible constructor without generatedExpr. */
    public ColumnDef(String name, String typeName, Integer precision, Integer scale,
                     boolean notNull, boolean primaryKey, boolean unique,
                     Expression defaultExpr, String referencesTable, String referencesColumn) {
        this(name, typeName, precision, scale, notNull, primaryKey, unique,
                defaultExpr, referencesTable, referencesColumn, null, null,
                null, null, null, null, false, false, null);
    }

    /** Backward-compatible constructor without identity. */
    public ColumnDef(String name, String typeName, Integer precision, Integer scale,
                     boolean notNull, boolean primaryKey, boolean unique,
                     Expression defaultExpr, String referencesTable, String referencesColumn,
                     String generatedExpr) {
        this(name, typeName, precision, scale, notNull, primaryKey, unique,
                defaultExpr, referencesTable, referencesColumn, generatedExpr, null,
                null, null, null, null, false, false, null);
    }

    public String name() { return name; }
    public String typeName() { return typeName; }
    public Integer precision() { return precision; }
    public Integer scale() { return scale; }
    public boolean notNull() { return notNull; }
    public boolean primaryKey() { return primaryKey; }
    public boolean unique() { return unique; }
    public Expression defaultExpr() { return defaultExpr; }
    public String referencesTable() { return referencesTable; }
    public String referencesColumn() { return referencesColumn; }
    public String generatedExpr() { return generatedExpr; }
    public String identity() { return identity; }
    public String refOnDelete() { return refOnDelete; }
    public String refOnUpdate() { return refOnUpdate; }
    public Long identityStart() { return identityStart; }
    public Long identityIncrement() { return identityIncrement; }
    public boolean deferrable() { return deferrable; }
    public boolean initiallyDeferred() { return initiallyDeferred; }
    public Expression checkConstraintExpr() { return checkConstraintExpr; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDef that = (ColumnDef) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(typeName, that.typeName)
            && java.util.Objects.equals(precision, that.precision)
            && java.util.Objects.equals(scale, that.scale)
            && notNull == that.notNull
            && primaryKey == that.primaryKey
            && unique == that.unique
            && java.util.Objects.equals(defaultExpr, that.defaultExpr)
            && java.util.Objects.equals(referencesTable, that.referencesTable)
            && java.util.Objects.equals(referencesColumn, that.referencesColumn)
            && java.util.Objects.equals(generatedExpr, that.generatedExpr)
            && java.util.Objects.equals(identity, that.identity)
            && java.util.Objects.equals(refOnDelete, that.refOnDelete)
            && java.util.Objects.equals(refOnUpdate, that.refOnUpdate)
            && java.util.Objects.equals(identityStart, that.identityStart)
            && java.util.Objects.equals(identityIncrement, that.identityIncrement)
            && deferrable == that.deferrable
            && initiallyDeferred == that.initiallyDeferred
            && java.util.Objects.equals(checkConstraintExpr, that.checkConstraintExpr);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, typeName, precision, scale, notNull, primaryKey, unique, defaultExpr, referencesTable, referencesColumn, generatedExpr, identity, refOnDelete, refOnUpdate, identityStart, identityIncrement, deferrable, initiallyDeferred, checkConstraintExpr);
    }

    @Override
    public String toString() {
        return "ColumnDef[name=" + name + ", " + "typeName=" + typeName + ", " + "precision=" + precision + ", " + "scale=" + scale + ", " + "notNull=" + notNull + ", " + "primaryKey=" + primaryKey + ", " + "unique=" + unique + ", " + "defaultExpr=" + defaultExpr + ", " + "referencesTable=" + referencesTable + ", " + "referencesColumn=" + referencesColumn + ", " + "generatedExpr=" + generatedExpr + ", " + "identity=" + identity + ", " + "refOnDelete=" + refOnDelete + ", " + "refOnUpdate=" + refOnUpdate + ", " + "identityStart=" + identityStart + ", " + "identityIncrement=" + identityIncrement + ", " + "deferrable=" + deferrable + ", " + "initiallyDeferred=" + initiallyDeferred + ", " + "checkConstraintExpr=" + checkConstraintExpr + "]";
    }
}
