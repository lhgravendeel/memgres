package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE [TEMP] TABLE [IF NOT EXISTS] [schema.]name (columns..., constraints...)
 * [INHERITS (parent, ...)]
 * [PARTITION BY {RANGE|LIST|HASH} (column)]
 * [ON COMMIT {DROP|DELETE ROWS|PRESERVE ROWS}]
 *
 * CREATE TABLE name PARTITION OF parent FOR VALUES ...
 */
public final class CreateTableStmt implements Statement {
    public final String schema;
    public final String name;
    public final boolean ifNotExists;
    public final boolean temporary;
    public final boolean unlogged;
    public final List<ColumnDef> columns;
    public final List<TableConstraint> constraints;
    public final List<String> inherits;
    public final String partitionBy;
    public final String partitionColumn;
    public final String partitionOfParent;
    public final List<String> partitionBounds;
    public final List<String> likeTables;
    public final String onCommitAction;
    public final java.util.Map<String, String> withOptions;
    /**
     * The composite type a typed table takes its columns from: CREATE TABLE t OF ct. The columns
     * are the type's attributes, so the definition list holds no column definitions at all and
     * whoever executes the statement reads them off the type.
     */
    private String ofType;

    /**
     * The schema written on the PARTITION OF parent, and the schema written on each INHERITS
     * parent -- one entry per parent, null where that name was written bare. A qualifier is a
     * name of its own rather than part of the relation's: joining the two back together with a
     * dot would read a quoted identifier that holds one as a qualified name.
     */
    private String partitionOfParentSchema;
    private List<String> inheritsSchemas;

    public String ofType() { return ofType; }
    public void setOfType(String ofType) { this.ofType = ofType; }

    public String partitionOfParentSchema() { return partitionOfParentSchema; }
    public void setPartitionOfParentSchema(String schema) { this.partitionOfParentSchema = schema; }

    /** The schema written on the {@link #inherits()} entry at that position, or null for none. */
    public String inheritsSchema(int index) {
        if (inheritsSchemas == null || index < 0 || index >= inheritsSchemas.size()) return null;
        return inheritsSchemas.get(index);
    }

    public void setInheritsSchemas(List<String> schemas) { this.inheritsSchemas = schemas; }

    public CreateTableStmt(
            String schema,
            String name,
            boolean ifNotExists,
            boolean temporary,
            boolean unlogged,
            List<ColumnDef> columns,
            List<TableConstraint> constraints,
            List<String> inherits,
            String partitionBy,
            String partitionColumn,
            String partitionOfParent,
            List<String> partitionBounds,
            List<String> likeTables,
            String onCommitAction
    ) {
        this(schema, name, ifNotExists, temporary, unlogged, columns, constraints, inherits,
                partitionBy, partitionColumn, partitionOfParent, partitionBounds, likeTables, onCommitAction, null);
    }

    public CreateTableStmt(
            String schema,
            String name,
            boolean ifNotExists,
            boolean temporary,
            boolean unlogged,
            List<ColumnDef> columns,
            List<TableConstraint> constraints,
            List<String> inherits,
            String partitionBy,
            String partitionColumn,
            String partitionOfParent,
            List<String> partitionBounds,
            List<String> likeTables,
            String onCommitAction,
            java.util.Map<String, String> withOptions
    ) {
        this.schema = schema;
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.temporary = temporary;
        this.unlogged = unlogged;
        this.columns = columns;
        this.constraints = constraints;
        this.inherits = inherits;
        this.partitionBy = partitionBy;
        this.partitionColumn = partitionColumn;
        this.partitionOfParent = partitionOfParent;
        this.partitionBounds = partitionBounds;
        this.likeTables = likeTables;
        this.onCommitAction = onCommitAction;
        this.withOptions = withOptions;
    }

    /** Backward-compatible constructor without inheritance/partitioning. */
    public CreateTableStmt(String schema, String name, boolean ifNotExists, boolean temporary,
                           List<ColumnDef> columns, List<TableConstraint> constraints) {
        this(schema, name, ifNotExists, temporary, false, columns, constraints, null, null, null, null, null, null, null);
    }

    /** Constructor without LIKE or onCommitAction. */
    public CreateTableStmt(String schema, String name, boolean ifNotExists, boolean temporary,
                           List<ColumnDef> columns, List<TableConstraint> constraints,
                           List<String> inherits, String partitionBy, String partitionColumn,
                           String partitionOfParent, List<String> partitionBounds) {
        this(schema, name, ifNotExists, temporary, false, columns, constraints, inherits,
                partitionBy, partitionColumn, partitionOfParent, partitionBounds, null, null);
    }

    /** Constructor without onCommitAction. */
    public CreateTableStmt(String schema, String name, boolean ifNotExists, boolean temporary,
                           List<ColumnDef> columns, List<TableConstraint> constraints,
                           List<String> inherits, String partitionBy, String partitionColumn,
                           String partitionOfParent, List<String> partitionBounds,
                           List<String> likeTables) {
        this(schema, name, ifNotExists, temporary, false, columns, constraints, inherits,
                partitionBy, partitionColumn, partitionOfParent, partitionBounds, likeTables, null);
    }

    public String schema() { return schema; }
    public String name() { return name; }
    public boolean ifNotExists() { return ifNotExists; }
    public boolean temporary() { return temporary; }
    public boolean unlogged() { return unlogged; }
    public List<ColumnDef> columns() { return columns; }
    public List<TableConstraint> constraints() { return constraints; }
    public List<String> inherits() { return inherits; }
    public String partitionBy() { return partitionBy; }
    public String partitionColumn() { return partitionColumn; }
    public String partitionOfParent() { return partitionOfParent; }
    public List<String> partitionBounds() { return partitionBounds; }
    public List<String> likeTables() { return likeTables; }
    public String onCommitAction() { return onCommitAction; }
    public java.util.Map<String, String> withOptions() { return withOptions; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateTableStmt that = (CreateTableStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(name, that.name)
            && ifNotExists == that.ifNotExists
            && temporary == that.temporary
            && java.util.Objects.equals(columns, that.columns)
            && java.util.Objects.equals(constraints, that.constraints)
            && java.util.Objects.equals(inherits, that.inherits)
            && java.util.Objects.equals(partitionBy, that.partitionBy)
            && java.util.Objects.equals(partitionColumn, that.partitionColumn)
            && java.util.Objects.equals(partitionOfParent, that.partitionOfParent)
            && java.util.Objects.equals(partitionBounds, that.partitionBounds)
            && java.util.Objects.equals(likeTables, that.likeTables)
            && java.util.Objects.equals(onCommitAction, that.onCommitAction);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, name, ifNotExists, temporary, columns, constraints, inherits, partitionBy, partitionColumn, partitionOfParent, partitionBounds, likeTables, onCommitAction);
    }

    @Override
    public String toString() {
        return "CreateTableStmt[schema=" + schema + ", " + "name=" + name + ", " + "ifNotExists=" + ifNotExists + ", " + "temporary=" + temporary + ", " + "columns=" + columns + ", " + "constraints=" + constraints + ", " + "inherits=" + inherits + ", " + "partitionBy=" + partitionBy + ", " + "partitionColumn=" + partitionColumn + ", " + "partitionOfParent=" + partitionOfParent + ", " + "partitionBounds=" + partitionBounds + ", " + "likeTables=" + likeTables + ", " + "onCommitAction=" + onCommitAction + "]";
    }
}
