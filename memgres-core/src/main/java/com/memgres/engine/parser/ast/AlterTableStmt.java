package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * ALTER TABLE [schema.]name action [, action ...]
 */
public final class AlterTableStmt implements Statement {
    public final String schema;
    public final String table;
    public final List<AlterAction> actions;
    public final boolean ifExists;

    public AlterTableStmt(String schema, String table, List<AlterAction> actions, boolean ifExists) {
        this.schema = schema;
        this.table = table;
        this.actions = actions;
        this.ifExists = ifExists;
    }

    public AlterTableStmt(String schema, String table, List<AlterAction> actions) {
        this(schema, table, actions, false);
    }

    public interface AlterAction {}

        public static final class AddColumn implements AlterAction {
        public final ColumnDef column;
        public final boolean ifNotExists;

        public AddColumn(ColumnDef column, boolean ifNotExists) {
            this.column = column;
            this.ifNotExists = ifNotExists;
        }

        public AddColumn(ColumnDef column) { this(column, false); }

        public ColumnDef column() { return column; }
        public boolean ifNotExists() { return ifNotExists; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddColumn that = (AddColumn) o;
            return java.util.Objects.equals(column, that.column)
                && ifNotExists == that.ifNotExists;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(column, ifNotExists);
        }

        @Override
        public String toString() {
            return "AddColumn[column=" + column + ", " + "ifNotExists=" + ifNotExists + "]";
        }
    }
        public static final class DropColumn implements AlterAction {
        public final String column;
        public final boolean ifExists;
        public final boolean cascade;

        public DropColumn(String column, boolean ifExists, boolean cascade) {
            this.column = column;
            this.ifExists = ifExists;
            this.cascade = cascade;
        }

        public String column() { return column; }
        public boolean ifExists() { return ifExists; }
        public boolean cascade() { return cascade; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropColumn that = (DropColumn) o;
            return java.util.Objects.equals(column, that.column)
                && ifExists == that.ifExists
                && cascade == that.cascade;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(column, ifExists, cascade);
        }

        @Override
        public String toString() {
            return "DropColumn[column=" + column + ", " + "ifExists=" + ifExists + ", " + "cascade=" + cascade + "]";
        }
    }
        public static final class AlterColumn implements AlterAction {
        public final String column;
        public final AlterColumnAction action;

        public AlterColumn(String column, AlterColumnAction action) {
            this.column = column;
            this.action = action;
        }

        public String column() { return column; }
        public AlterColumnAction action() { return action; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AlterColumn that = (AlterColumn) o;
            return java.util.Objects.equals(column, that.column)
                && java.util.Objects.equals(action, that.action);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(column, action);
        }

        @Override
        public String toString() {
            return "AlterColumn[column=" + column + ", " + "action=" + action + "]";
        }
    }
        public static final class AddConstraint implements AlterAction {
        public final TableConstraint constraint;

        public AddConstraint(TableConstraint constraint) {
            this.constraint = constraint;
        }

        public TableConstraint constraint() { return constraint; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddConstraint that = (AddConstraint) o;
            return java.util.Objects.equals(constraint, that.constraint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(constraint);
        }

        @Override
        public String toString() {
            return "AddConstraint[constraint=" + constraint + "]";
        }
    }
        public static final class DropConstraint implements AlterAction {
        public final String name;
        public final boolean ifExists;
        public final boolean cascade;

        public DropConstraint(String name, boolean ifExists, boolean cascade) {
            this.name = name;
            this.ifExists = ifExists;
            this.cascade = cascade;
        }

        public String name() { return name; }
        public boolean ifExists() { return ifExists; }
        public boolean cascade() { return cascade; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropConstraint that = (DropConstraint) o;
            return java.util.Objects.equals(name, that.name)
                && ifExists == that.ifExists
                && cascade == that.cascade;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, ifExists, cascade);
        }

        @Override
        public String toString() {
            return "DropConstraint[name=" + name + ", " + "ifExists=" + ifExists + ", " + "cascade=" + cascade + "]";
        }
    }
        public static final class RenameColumn implements AlterAction {
        public final String oldName;
        public final String newName;

        public RenameColumn(String oldName, String newName) {
            this.oldName = oldName;
            this.newName = newName;
        }

        public String oldName() { return oldName; }
        public String newName() { return newName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RenameColumn that = (RenameColumn) o;
            return java.util.Objects.equals(oldName, that.oldName)
                && java.util.Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(oldName, newName);
        }

        @Override
        public String toString() {
            return "RenameColumn[oldName=" + oldName + ", " + "newName=" + newName + "]";
        }
    }
        public static final class RenameTable implements AlterAction {
        public final String newName;

        public RenameTable(String newName) {
            this.newName = newName;
        }

        public String newName() { return newName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RenameTable that = (RenameTable) o;
            return java.util.Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(newName);
        }

        @Override
        public String toString() {
            return "RenameTable[newName=" + newName + "]";
        }
    }
        public static final class EnableRls implements AlterAction {
        public EnableRls() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "EnableRls[]";
        }
    }
        public static final class DisableRls implements AlterAction {
        public DisableRls() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "DisableRls[]";
        }
    }
        public static final class ForceRls implements AlterAction {
        public ForceRls() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "ForceRls[]";
        }
    }
        public static final class NoForceRls implements AlterAction {
        public NoForceRls() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "NoForceRls[]";
        }
    }
        public static final class AttachPartition implements AlterAction {
        public final String partitionSchema;
        public final String partitionName;
        public final List<String> bounds;

        public AttachPartition(String partitionSchema, String partitionName, List<String> bounds) {
            this.partitionSchema = partitionSchema;
            this.partitionName = partitionName;
            this.bounds = bounds;
        }

        public AttachPartition(String partitionName, List<String> bounds) { this(null, partitionName, bounds); }

        public String partitionSchema() { return partitionSchema; }
        public String partitionName() { return partitionName; }
        public List<String> bounds() { return bounds; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AttachPartition that = (AttachPartition) o;
            return java.util.Objects.equals(partitionSchema, that.partitionSchema)
                && java.util.Objects.equals(partitionName, that.partitionName)
                && java.util.Objects.equals(bounds, that.bounds);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(partitionSchema, partitionName, bounds);
        }

        @Override
        public String toString() {
            return "AttachPartition[partitionSchema=" + partitionSchema + ", " + "partitionName=" + partitionName + ", " + "bounds=" + bounds + "]";
        }
    }
        public static final class DetachPartition implements AlterAction {
        public final String partitionSchema;
        public final String partitionName;

        public DetachPartition(String partitionSchema, String partitionName) {
            this.partitionSchema = partitionSchema;
            this.partitionName = partitionName;
        }

        public DetachPartition(String partitionName) { this(null, partitionName); }

        public String partitionSchema() { return partitionSchema; }
        public String partitionName() { return partitionName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DetachPartition that = (DetachPartition) o;
            return java.util.Objects.equals(partitionSchema, that.partitionSchema)
                && java.util.Objects.equals(partitionName, that.partitionName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(partitionSchema, partitionName);
        }

        @Override
        public String toString() {
            return "DetachPartition[partitionSchema=" + partitionSchema + ", " + "partitionName=" + partitionName + "]";
        }
    }
        public static final class RenameConstraint implements AlterAction {
        public final String oldName;
        public final String newName;

        public RenameConstraint(String oldName, String newName) {
            this.oldName = oldName;
            this.newName = newName;
        }

        public String oldName() { return oldName; }
        public String newName() { return newName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RenameConstraint that = (RenameConstraint) o;
            return java.util.Objects.equals(oldName, that.oldName)
                && java.util.Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(oldName, newName);
        }

        @Override
        public String toString() {
            return "RenameConstraint[oldName=" + oldName + ", " + "newName=" + newName + "]";
        }
    }
        public static final class SetSchema implements AlterAction {
        public final String newSchema;

        public SetSchema(String newSchema) {
            this.newSchema = newSchema;
        }

        public String newSchema() { return newSchema; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetSchema that = (SetSchema) o;
            return java.util.Objects.equals(newSchema, that.newSchema);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(newSchema);
        }

        @Override
        public String toString() {
            return "SetSchema[newSchema=" + newSchema + "]";
        }
    }
        public static final class Inherit implements AlterAction {
        public final String parentTable;

        public Inherit(String parentTable) {
            this.parentTable = parentTable;
        }

        public String parentTable() { return parentTable; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Inherit that = (Inherit) o;
            return java.util.Objects.equals(parentTable, that.parentTable);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(parentTable);
        }

        @Override
        public String toString() {
            return "Inherit[parentTable=" + parentTable + "]";
        }
    }
        public static final class NoInherit implements AlterAction {
        public final String parentTable;

        public NoInherit(String parentTable) {
            this.parentTable = parentTable;
        }

        public String parentTable() { return parentTable; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NoInherit that = (NoInherit) o;
            return java.util.Objects.equals(parentTable, that.parentTable);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(parentTable);
        }

        @Override
        public String toString() {
            return "NoInherit[parentTable=" + parentTable + "]";
        }
    }
        public static final class SetStorageParams implements AlterAction {
        public SetStorageParams() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "SetStorageParams[]";
        }
    }
        public static final class OwnerTo implements AlterAction {
        public final String newOwner;

        public OwnerTo(String newOwner) {
            this.newOwner = newOwner;
        }

        public String newOwner() { return newOwner; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OwnerTo that = (OwnerTo) o;
            return java.util.Objects.equals(newOwner, that.newOwner);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(newOwner);
        }

        @Override
        public String toString() {
            return "OwnerTo[newOwner=" + newOwner + "]";
        }
    }

    public interface AlterColumnAction {}

        public static final class SetType implements AlterColumnAction {
        public final String typeName;
        public final Expression usingExpr;

        public SetType(String typeName, Expression usingExpr) {
            this.typeName = typeName;
            this.usingExpr = usingExpr;
        }

        public SetType(String typeName) { this(typeName, null); }

        public String typeName() { return typeName; }
        public Expression usingExpr() { return usingExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetType that = (SetType) o;
            return java.util.Objects.equals(typeName, that.typeName)
                && java.util.Objects.equals(usingExpr, that.usingExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(typeName, usingExpr);
        }

        @Override
        public String toString() {
            return "SetType[typeName=" + typeName + ", " + "usingExpr=" + usingExpr + "]";
        }
    }
        public static final class SetDefault implements AlterColumnAction {
        public final Expression expr;

        public SetDefault(Expression expr) {
            this.expr = expr;
        }

        public Expression expr() { return expr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SetDefault that = (SetDefault) o;
            return java.util.Objects.equals(expr, that.expr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(expr);
        }

        @Override
        public String toString() {
            return "SetDefault[expr=" + expr + "]";
        }
    }
        public static final class DropDefault implements AlterColumnAction {
        public DropDefault() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "DropDefault[]";
        }
    }
        public static final class SetNotNull implements AlterColumnAction {
        public SetNotNull() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "SetNotNull[]";
        }
    }
        public static final class DropNotNull implements AlterColumnAction {
        public DropNotNull() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "DropNotNull[]";
        }
    }
        public static final class ColumnNoOp implements AlterColumnAction {
        public ColumnNoOp() {}

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public String toString() {
            return "ColumnNoOp[]";
        }
    }

    public String schema() { return schema; }
    public String table() { return table; }
    public List<AlterAction> actions() { return actions; }
    public boolean ifExists() { return ifExists; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterTableStmt that = (AlterTableStmt) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(table, that.table)
            && java.util.Objects.equals(actions, that.actions)
            && ifExists == that.ifExists;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, table, actions, ifExists);
    }

    @Override
    public String toString() {
        return "AlterTableStmt[schema=" + schema + ", " + "table=" + table + ", " + "actions=" + actions + ", " + "ifExists=" + ifExists + "]";
    }
}
