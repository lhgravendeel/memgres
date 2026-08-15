package com.memgres.engine;

/**
 * Puts a constraint back to not-validated when the statement that read its rows is rolled back.
 *
 * <p>VALIDATE CONSTRAINT is a statement like any other in PostgreSQL: the scan it runs is part of
 * the transaction, so a rollback leaves the constraint exactly as unvalidated as it was and the
 * rows have to be read again. Left standing, a validation that was rolled back said the rows had
 * been held to a rule they had never been read against, and the next VALIDATE found nothing to do.
 *
 * <p>A NOT NULL is kept on the column rather than in the relation's constraint list, so the column
 * is named for that one and the constraint's own name for every other kind.
 */
public final class ConstraintValidationUndo implements Session.UndoEntry {
    private final String schema;
    private final String tableName;
    private final String constraintName;
    private final String column;

    public ConstraintValidationUndo(String schema, String tableName, String constraintName,
                                    String column) {
        this.schema = schema;
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.column = column;
    }

    @Override
    public void undo(Database db) {
        Schema s = db.getSchema(schema);
        if (s == null) return;
        Table table = s.getTable(tableName);
        if (table == null) return;
        if (column != null) {
            if (table.getColumnIndex(column) < 0) return;
            table.markNotNullNotValidated(column);
            return;
        }
        StoredConstraint sc = table.getConstraint(constraintName);
        if (sc != null) sc.setConvalidated(false);
    }

    @Override
    public String toString() {
        return "ConstraintValidationUndo[schema=" + schema + ", tableName=" + tableName
                + ", constraintName=" + constraintName + ", column=" + column + "]";
    }
}
