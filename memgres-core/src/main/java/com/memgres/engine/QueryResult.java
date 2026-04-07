package com.memgres.engine;

import com.memgres.engine.parser.ast.CopyStmt;

import java.util.Collections;
import java.util.List;

/**
 * Result of executing a SQL statement.
 */
public class QueryResult {

    public enum Type {
        SELECT, INSERT, UPDATE, DELETE, MERGE, CREATE_TABLE, DROP_TABLE, CREATE_TYPE, CREATE_FUNCTION, CREATE_TRIGGER, EMPTY, SET, BEGIN, COMMIT, ROLLBACK, CALL, SELECT_INTO, ALTER_TYPE,
        COPY_OUT, COPY_IN
    }

    private final Type type;
    private final List<Column> columns;
    private final List<Object[]> rows;
    private final int affectedRows;
    private final String message;
    private final CopyStmt copyStmt;  // context for COPY_OUT / COPY_IN

    private QueryResult(Type type, List<Column> columns, List<Object[]> rows, int affectedRows, String message) {
        this(type, columns, rows, affectedRows, message, null);
    }

    private QueryResult(Type type, List<Column> columns, List<Object[]> rows, int affectedRows, String message, CopyStmt copyStmt) {
        this.type = type;
        this.columns = columns;
        this.rows = rows;
        this.affectedRows = affectedRows;
        this.message = message;
        this.copyStmt = copyStmt;
    }

    public static QueryResult select(List<Column> columns, List<Object[]> rows) {
        return new QueryResult(Type.SELECT, columns, rows, rows.size(), null);
    }

    public static QueryResult command(Type type, int affectedRows) {
        return new QueryResult(type, Collections.emptyList(), Collections.emptyList(), affectedRows, null);
    }

    /** DML with RETURNING clause; carries both affected row count and result rows. */
    public static QueryResult returning(Type type, List<Column> columns, List<Object[]> rows, int affectedRows) {
        return new QueryResult(type, columns, rows, affectedRows, null);
    }

    public static QueryResult empty() {
        return new QueryResult(Type.EMPTY, Collections.emptyList(), Collections.emptyList(), 0, null);
    }

    public static QueryResult message(Type type, String message) {
        return new QueryResult(type, Collections.emptyList(), Collections.emptyList(), 0, message);
    }

    /** COPY TO STDOUT: carries the rows to send and the CopyStmt for format info. */
    public static QueryResult copyOut(List<Column> columns, List<Object[]> rows, CopyStmt copyStmt) {
        return new QueryResult(Type.COPY_OUT, columns, rows, rows.size(), null, copyStmt);
    }

    /** COPY FROM STDIN: carries CopyStmt context; PgWireHandler will collect data. */
    public static QueryResult copyIn(CopyStmt copyStmt) {
        return new QueryResult(Type.COPY_IN, Collections.emptyList(), Collections.emptyList(), 0, null, copyStmt);
    }

    public Type getType() {
        return type;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public String getMessage() {
        return message;
    }

    public CopyStmt getCopyStmt() {
        return copyStmt;
    }
}
