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
        tables.put(table.getName(), table);
    }

    public void removeTable(String name) {
        tables.remove(name);
    }

    public Map<String, Table> getTables() {
        return tables;
    }
}
