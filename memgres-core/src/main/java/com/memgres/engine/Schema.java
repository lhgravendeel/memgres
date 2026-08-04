package com.memgres.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Represents a database schema (namespace for tables).
 */
public class Schema {

    private final String name;
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    public Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public void addTable(Table table) {
        table.setSchemaName(name);
        tables.put(table.getName(), table);
    }

    public void removeTable(String name) {
        tables.remove(name);
    }

    /**
     * The tables in this schema, as the session running the current statement may see them.
     *
     * <p>A relation created by a transaction that has not committed does not exist yet for anyone
     * else, so it is left out — which is what keeps it out of pg_class, information_schema and
     * every other listing built by walking the schemas. The map itself is returned unchanged
     * whenever there is nothing to hide, which is every ordinary statement.
     */
    public Map<String, Table> getTables() {
        return Database.visibleTables(tables);
    }
}
