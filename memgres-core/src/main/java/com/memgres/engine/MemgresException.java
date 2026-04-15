package com.memgres.engine;

/**
 * Exception thrown by the Memgres engine for SQL errors.
 * Supports SQLSTATE error codes per the PostgreSQL specification.
 */
public class MemgresException extends RuntimeException {

    private final String sqlState;
    private String detail;
    private String hint;
    private String column;
    private String constraint;
    private String datatype;
    private String table;
    private String schema;

    public MemgresException(String message) {
        super(message);
        this.sqlState = inferSqlState(message);
    }

    public MemgresException(String message, String sqlState) {
        super(message);
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }

    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public String getHint() { return hint; }
    public void setHint(String hint) { this.hint = hint; }
    public String getColumn() { return column; }
    public void setColumn(String column) { this.column = column; }
    public String getConstraint() { return constraint; }
    public void setConstraint(String constraint) { this.constraint = constraint; }
    public String getDatatype() { return datatype; }
    public void setDatatype(String datatype) { this.datatype = datatype; }
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }

    /**
     * Infer a SQLSTATE code from common error message patterns.
     * This provides reasonable defaults when callers don't specify an explicit code.
     */
    private static String inferSqlState(String message) {
        if (message == null) return "42000";
        String lower = message.toLowerCase();

        // 42P01: undefined table/relation
        if (lower.contains("table not found") || (lower.contains("relation") && lower.contains("does not exist"))
                || lower.contains("table reference not found"))
            return "42P01";

        // 42703: undefined column
        if (lower.contains("column not found") || (lower.contains("column") && lower.contains("does not exist")))
            return "42703";

        // 42704: undefined type/object
        if ((lower.contains("type") && lower.contains("does not exist"))
                || (lower.contains("role") && lower.contains("does not exist")))
            return "42704";

        // 22012: division by zero
        if (lower.contains("division by zero"))
            return "22012";

        // 22003: numeric value out of range
        if (lower.contains("out of range") || lower.contains("overflow"))
            return "22003";

        // 42P02: undefined parameter
        if (lower.contains("parameter") && lower.contains("does not exist"))
            return "42P02";

        // 42883: undefined function/operator
        if (lower.contains("unknown function") || (lower.contains("function") && lower.contains("does not exist"))
                || lower.contains("operator does not exist"))
            return "42883";

        // 42804: datatype mismatch
        if (lower.contains("type mismatch") || lower.contains("datatype mismatch")
                || lower.contains("array subscript must have type"))
            return "42804";

        // 42723: duplicate function
        if (lower.contains("function") && lower.contains("already exists"))
            return "42723";

        // 42P06: duplicate schema
        if (lower.contains("schema") && lower.contains("already exists"))
            return "42P06";

        // 42P07: duplicate table
        if ((lower.contains("table") && lower.contains("already exists"))
                || (lower.contains("relation") && lower.contains("already exists")))
            return "42P07";

        // 42710: duplicate object
        if (lower.contains("already exists"))
            return "42710";

        // 25P02: in failed transaction
        if (lower.contains("current transaction is aborted"))
            return "25P02";

        // 22P02: invalid text representation
        if (lower.contains("invalid input syntax"))
            return "22P02";

        // Default
        return "42000";
    }
}
