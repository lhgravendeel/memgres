package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CREATE [OR REPLACE] [MATERIALIZED] VIEW name [(col1, col2, ...)] AS SELECT ...
 * [WITH [CASCADED|LOCAL] CHECK OPTION]
 * [WITH DATA | WITH NO DATA]  (materialized views)
 */
public final class CreateViewStmt implements Statement {
    public final String name;
    public final Statement query;
    public final boolean orReplace;
    public final boolean materialized;
    public final List<String> columnNames;
    public final boolean withData;
    public final String checkOption;
    public final Map<String, String> withOptions;

    public CreateViewStmt(String name, Statement query, boolean orReplace, boolean materialized,
                          List<String> columnNames, boolean withData, String checkOption) {
        this(name, query, orReplace, materialized, columnNames, withData, checkOption, null);
    }

    public CreateViewStmt(String name, Statement query, boolean orReplace, boolean materialized,
                          List<String> columnNames, boolean withData, String checkOption,
                          Map<String, String> withOptions) {
        this.name = name;
        this.query = query;
        this.orReplace = orReplace;
        this.materialized = materialized;
        this.columnNames = columnNames;
        this.withData = withData;
        this.checkOption = checkOption;
        this.withOptions = withOptions;
    }

    /** Backward-compatible constructor (no check option) */
    public CreateViewStmt(String name, Statement query, boolean orReplace, boolean materialized,
                          List<String> columnNames, boolean withData) {
        this(name, query, orReplace, materialized, columnNames, withData, null);
    }
    /** Backward-compatible constructor */
    public CreateViewStmt(String name, Statement query, boolean orReplace, boolean materialized) {
        this(name, query, orReplace, materialized, null, true, null);
    }

    public String name() { return name; }
    public Statement query() { return query; }
    public boolean orReplace() { return orReplace; }
    public boolean materialized() { return materialized; }
    public List<String> columnNames() { return columnNames; }
    public boolean withData() { return withData; }
    public String checkOption() { return checkOption; }
    public Map<String, String> withOptions() { return withOptions; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateViewStmt that = (CreateViewStmt) o;
        return Objects.equals(name, that.name)
            && Objects.equals(query, that.query)
            && orReplace == that.orReplace
            && materialized == that.materialized
            && Objects.equals(columnNames, that.columnNames)
            && withData == that.withData
            && Objects.equals(checkOption, that.checkOption);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, orReplace, materialized, columnNames, withData, checkOption);
    }

    @Override
    public String toString() {
        return "CreateViewStmt[name=" + name + ", query=" + query + ", orReplace=" + orReplace
            + ", materialized=" + materialized + ", columnNames=" + columnNames + ", withData=" + withData
            + ", checkOption=" + checkOption + "]";
    }
}
