package com.memgres.engine;

/**
 * Undo what a statement made of one column's NOT NULL on one relation.
 *
 * <p>DDL is transactional in PostgreSQL, so a transaction that declared the rule -- or withdrew
 * it -- and then rolled back leaves the column refusing exactly the nulls it refused before, under
 * the constraint it answered to before. Four things settle that, and all four are the relation's
 * own: whether the column takes a null at all, the name the constraint was created with, whether
 * the relation declared the rule itself or holds it for a parent, and the name written down for a
 * rule it holds for a parent it can no longer read the name from.
 *
 * <p>One entry describes one relation, so a statement that reached a whole hierarchy records one
 * per relation it could reach, in the state each of them was in before it ran.
 */
public final class NotNullUndo implements Session.UndoEntry {
    private final String schema;
    private final String tableName;
    private final String column;
    private final boolean wasNullable;
    private final String constraintName;
    private final boolean wasLocal;
    private final String pinnedName;

    public NotNullUndo(String schema, String tableName, String column, boolean wasNullable,
                       String constraintName, boolean wasLocal, String pinnedName) {
        this.schema = schema;
        this.tableName = tableName;
        this.column = column;
        this.wasNullable = wasNullable;
        this.constraintName = constraintName;
        this.wasLocal = wasLocal;
        this.pinnedName = pinnedName;
    }

    @Override
    public void undo(Database db) {
        Schema s = db.getSchema(schema);
        if (s == null) return;
        Table table = s.getTable(tableName);
        if (table == null || table.getColumnIndex(column) < 0) return;
        table.alterColumnNullable(column, wasNullable);
        if (wasNullable) {
            // A column that took a null carried no constraint, so there is no name to put back
            // and nothing was being held on anybody's behalf.
            table.setNotNullConstraintName(column, null);
            table.pinInheritedNotNullName(column, null);
            return;
        }
        // Marking the column NOT NULL says nothing about whose rule it is, so that is restored
        // in as many words rather than left at whatever the statement made of it.
        if (wasLocal) table.markNotNullLocal(column);
        else table.markNotNullInherited(column);
        table.setNotNullConstraintName(column, constraintName);
        table.pinInheritedNotNullName(column, pinnedName);
    }

    @Override
    public String toString() {
        return "NotNullUndo[schema=" + schema + ", tableName=" + tableName
                + ", column=" + column + ", nullable=" + wasNullable + "]";
    }
}
