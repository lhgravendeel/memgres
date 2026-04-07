package com.memgres.engine.parser.ast;

/**
 * BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE SAVEPOINT, START TRANSACTION,
 * PREPARE TRANSACTION, COMMIT PREPARED, ROLLBACK PREPARED
 */
public final class TransactionStmt implements Statement {
    public final TransactionAction action;
    public final String savepointName;
    public final String isolationLevel;
    public final Boolean readOnly;

    public TransactionStmt(TransactionAction action, String savepointName, String isolationLevel, Boolean readOnly) {
        this.action = action;
        this.savepointName = savepointName;
        this.isolationLevel = isolationLevel;
        this.readOnly = readOnly;
    }

    public enum TransactionAction {
        BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE_SAVEPOINT, ROLLBACK_TO_SAVEPOINT,
        PREPARE_TRANSACTION, COMMIT_PREPARED, ROLLBACK_PREPARED
    }

    /** Convenience constructor for simple actions (no isolation or read-only). */
    public TransactionStmt(TransactionAction action, String savepointName) {
        this(action, savepointName, null, null);
    }

    public TransactionAction action() { return action; }
    public String savepointName() { return savepointName; }
    public String isolationLevel() { return isolationLevel; }
    public Boolean readOnly() { return readOnly; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionStmt that = (TransactionStmt) o;
        return java.util.Objects.equals(action, that.action)
            && java.util.Objects.equals(savepointName, that.savepointName)
            && java.util.Objects.equals(isolationLevel, that.isolationLevel)
            && java.util.Objects.equals(readOnly, that.readOnly);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(action, savepointName, isolationLevel, readOnly);
    }

    @Override
    public String toString() {
        return "TransactionStmt[action=" + action + ", " + "savepointName=" + savepointName + ", " + "isolationLevel=" + isolationLevel + ", " + "readOnly=" + readOnly + "]";
    }
}
