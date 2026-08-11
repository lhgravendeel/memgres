package com.memgres.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Represents a database schema (namespace for tables).
 */
public class Schema {

    private final String name;
    private final Map<String, Table> tables = new ConcurrentHashMap<>();

    /**
     * Relations a still-open transaction has dropped from this schema, and who dropped them.
     *
     * <p>The dropping session sees them gone at once, which is what its own statements expect and
     * what its undo log restores. Every other session goes on seeing them: the transaction may
     * roll back, and a relation that is still there is not one another session may be told is
     * missing. Held here rather than globally so two databases cannot see each other's drops.
     */
    private final Map<String, Object[]> droppedUncommitted = new ConcurrentHashMap<>();

    public Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Table getTable(String name) {
        Table table = tables.get(name);
        // A relation another session has dropped inside a transaction that is still open has not
        // gone anywhere yet: that transaction may roll back, and until it ends the relation is
        // still there for everyone else.
        return table != null ? table : droppedButUncommitted(name);
    }

    public void addTable(Table table) {
        table.setSchemaName(name);
        tables.put(table.getName(), table);
    }

    public void removeTable(String name) {
        Table removed = tables.remove(name);
        if (removed != null) {
            Session dropper = Database.currentViewer();
            if (dropper != null && dropper.isInTransaction()) {
                droppedUncommitted.put(name, new Object[]{removed, dropper});
            }
        }
    }

    /**
     * The tables in this schema, as the session running the current statement may see them.
     *
     * <p>A relation created by a transaction that has not committed does not exist yet for anyone
     * else, so it is left out — which is what keeps it out of pg_class, information_schema and
     * every other listing built by walking the schemas. The map itself is returned unchanged
     * whenever there is nothing to hide, which is every ordinary statement.
     */
    /** The relation of this name a still-open transaction of another session dropped, or null. */
    private Table droppedButUncommitted(String tableName) {
        Object[] entry = droppedUncommitted.get(tableName);
        if (entry == null) return null;
        Session dropper = (Session) entry[1];
        if (!dropper.isInTransaction()) return null;
        Session viewer = Database.currentViewer();
        return viewer == null || viewer == dropper ? null : (Table) entry[0];
    }

    /** {@code tables} with any relation another session's open transaction dropped added back. */
    private Map<String, Table> withUncommittedDrops(Map<String, Table> visible) {
        if (droppedUncommitted.isEmpty()) return visible;
        Map<String, Table> shown = null;
        for (String dropped : droppedUncommitted.keySet()) {
            if (visible.containsKey(dropped)) continue;
            Table back = droppedButUncommitted(dropped);
            if (back == null) continue;
            if (shown == null) shown = new java.util.LinkedHashMap<>(visible);
            shown.put(dropped, back);
        }
        return shown == null ? visible : shown;
    }

    /** The transaction that dropped these relations has ended; stop holding them for anyone. */
    void forgetDroppedBy(Session dropper) {
        droppedUncommitted.values().removeIf(v -> v[1] == dropper);
        // A relation that transaction renamed goes on answering to its old name for everyone
        // else, and that too ends when the transaction does.
        for (Table t : tables.values()) t.forgetUncommittedRename(dropper);
    }

    public Map<String, Table> getTables() {
        return withUncommittedDrops(Database.visibleTables(tables));
    }
}
