package com.memgres.engine;

import com.memgres.engine.parser.ast.MaintenanceStmt;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

/**
 * VACUUM, ANALYZE, REINDEX, CLUSTER and CHECKPOINT, once the grammar has read them.
 *
 * <p>What is left after parsing is finding what the statement names and asking whether it is the
 * kind of thing the statement may be given. PostgreSQL's maintenance statements each take a
 * particular kind of relation: an index is reindexed and a table is not, a table or a materialized
 * view is vacuumed and a view is not, and a relation that is not there at all is reported as
 * missing rather than as the wrong kind. Resolving the target as an ordinary table and nothing
 * else meant a materialized view was refused as a view, a system catalogue was refused as absent,
 * and an index handed to REINDEX TABLE was accepted.
 */
final class MaintenanceExecutor {

    private final AstExecutor executor;

    MaintenanceExecutor(AstExecutor executor) {
        this.executor = executor;
    }

    QueryResult execute(MaintenanceStmt stmt) {
        switch (stmt.verb()) {
            case CHECKPOINT:
                return QueryResult.message(QueryResult.Type.SET, "CHECKPOINT");
            case REINDEX:
                return reindex(stmt);
            case CLUSTER:
                return cluster(stmt);
            case ANALYZE:
                return vacuumOrAnalyze(stmt, false);
            case VACUUM:
            default:
                return vacuumOrAnalyze(stmt, true);
        }
    }

    // ---- what a name stands for -----------------------------------------------------------

    /** The kinds of relation the maintenance statements tell apart. */
    private enum Kind { TABLE, MATVIEW, VIEW, INDEX, SEQUENCE, CATALOG, ABSENT }

    /**
     * What kind of relation a name stands for. The catalogue relations are relations the
     * maintenance statements accept, and they live nowhere the ordinary table resolver looks.
     */
    private Kind kindOf(String schema, String name) {
        String where = schema != null ? schema : executor.defaultSchema();
        Database.ViewDef view = executor.database.getView(where, name);
        if (view != null) return view.materialized ? Kind.MATVIEW : Kind.VIEW;
        Schema s = executor.database.getSchema(where);
        if (s != null && s.getTables().containsKey(name.toLowerCase(java.util.Locale.ROOT))) return Kind.TABLE;
        if (executor.database.hasIndex(name) || namesAConstraintIndex(name)) return Kind.INDEX;
        if (executor.database.getSequence(where, name) != null) return Kind.SEQUENCE;
        if (isCatalogRelation(schema, name)) return Kind.CATALOG;
        return Kind.ABSENT;
    }

    /** An index PostgreSQL builds for a constraint is an index, and is named like one. */
    private boolean namesAConstraintIndex(String name) {
        for (Schema s : executor.database.getSchemas().values()) {
            for (Table t : s.getTables().values()) {
                for (StoredConstraint sc : t.getConstraints()) {
                    if (sc.getName() != null && sc.getName().equalsIgnoreCase(name)) return true;
                }
            }
        }
        return false;
    }

    /** Whether a name reaches one of the catalogue relations, qualified or by the search path. */
    private boolean isCatalogRelation(String schema, String name) {
        if (schema != null && !schema.equalsIgnoreCase("pg_catalog")
                && !schema.equalsIgnoreCase("information_schema")) {
            return false;
        }
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        return lower.startsWith("pg_") || schema != null;
    }

    private MemgresException notARelation(MaintenanceStmt stmt) {
        return new MemgresException(
                "relation \"" + stmt.writtenName() + "\" does not exist", "42P01");
    }

    private MemgresException wrongKind(String name, String wanted) {
        return new MemgresException("\"" + name + "\" is not " + wanted, "42809");
    }

    // ---- REINDEX ---------------------------------------------------------------------------

    private QueryResult reindex(MaintenanceStmt stmt) {
        if (stmt.concurrently() && executor.session != null
                && executor.session.isInTransaction()) {
            throw new MemgresException(
                    "REINDEX CONCURRENTLY cannot run inside a transaction block", "25001");
        }
        switch (stmt.target()) {
            case SCHEMA: {
                if (executor.database.getSchema(stmt.name()) == null) {
                    throw new MemgresException(
                            "schema \"" + stmt.name() + "\" does not exist", "3F000");
                }
                break;
            }
            case DATABASE: {
                String open = executor.session != null
                        ? executor.session.getDatabaseName() : null;
                if (open != null && !open.equalsIgnoreCase(stmt.name())) {
                    throw new MemgresException(
                            "can only reindex the currently open database", "0A000");
                }
                break;
            }
            case SYSTEM:
                break;
            case INDEX: {
                Kind kind = kindOf(stmt.schema(), stmt.name());
                if (kind == Kind.ABSENT) throw notARelation(stmt);
                if (kind != Kind.INDEX) throw wrongKind(stmt.name(), "an index");
                break;
            }
            case TABLE:
            default: {
                Kind kind = kindOf(stmt.schema(), stmt.name());
                if (kind == Kind.ABSENT) throw notARelation(stmt);
                if (kind != Kind.TABLE && kind != Kind.MATVIEW && kind != Kind.CATALOG) {
                    throw wrongKind(stmt.name(), "a table or materialized view");
                }
                break;
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "REINDEX");
    }

    // ---- CLUSTER ---------------------------------------------------------------------------

    private QueryResult cluster(MaintenanceStmt stmt) {
        if (stmt.name() == null) return QueryResult.message(QueryResult.Type.SET, "CLUSTER");
        Kind kind = kindOf(stmt.schema(), stmt.name());
        if (kind == Kind.ABSENT) throw notARelation(stmt);
        if (kind != Kind.TABLE && kind != Kind.MATVIEW && kind != Kind.CATALOG) {
            throw wrongKind(stmt.name(), "a table or materialized view");
        }
        if (stmt.indexName() == null) {
            if (!hasClusteredIndex(stmt.name())) {
                throw new MemgresException("there is no previously clustered index for table \""
                        + stmt.name() + "\"", "42704");
            }
        } else {
            executor.database.setClusteredIndex(stmt.indexName());
        }
        return QueryResult.message(QueryResult.Type.SET, "CLUSTER");
    }

    private boolean hasClusteredIndex(String tableName) {
        for (Map.Entry<String, String> e : executor.database.getIndexTableNames().entrySet()) {
            String on = e.getValue();
            if (on == null) continue;
            if (!on.equalsIgnoreCase(tableName) && !on.endsWith("." + tableName)) continue;
            if (executor.database.isClusteredIndex(e.getKey())) return true;
        }
        return false;
    }

    // ---- VACUUM and ANALYZE ----------------------------------------------------------------

    private QueryResult vacuumOrAnalyze(MaintenanceStmt stmt, boolean vacuuming) {
        String label = vacuuming ? "VACUUM" : "ANALYZE";
        if (vacuuming && executor.session != null && executor.session.isInRoutine()) {
            throw new MemgresException("VACUUM cannot be executed from a function", "25001");
        }
        if (vacuuming && executor.session != null && executor.session.isInTransaction()) {
            throw new MemgresException("VACUUM cannot run inside a transaction block", "25001");
        }
        boolean analysing = !vacuuming || stmt.isOn("ANALYZE") || stmt.isOn("ANALYSE");
        OffsetDateTime now = OffsetDateTime.now();
        if (stmt.name() != null) {
            Kind kind = kindOf(stmt.schema(), stmt.name());
            if (kind == Kind.ABSENT) throw notARelation(stmt);
            if (kind == Kind.VIEW || kind == Kind.INDEX || kind == Kind.SEQUENCE) {
                throw wrongKind(stmt.name(), "a table or materialized view");
            }
            if (kind == Kind.CATALOG) {
                // A catalogue relation is answered from the state it describes; there is nothing
                // stored to gather statistics over, and PostgreSQL accepts the statement anyway.
                return QueryResult.message(QueryResult.Type.SET, label);
            }
            String where = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
            Table table = kind == Kind.MATVIEW ? null : executor.resolveTable(where, stmt.name());
            if (stmt.columns() != null && table != null) {
                requireColumns(table, stmt.name(), stmt.columns());
            }
            if (table != null) {
                if (vacuuming) table.setLastVacuum(now);
                if (analysing) {
                    executor.database.recordAnalyzedTable(where + "." + stmt.name());
                    table.setLastAnalyze(now);
                    executor.database.recordAnalyzedColumns(
                            where + "." + stmt.name(), stmt.columns());
                    gatherStatistics(where + "." + stmt.name(), table, stmt.columns());
                }
            }
            if (analysing) markStatisticsAnalysed(stmt.name());
        } else {
            for (Map.Entry<String, Schema> schemaEntry
                    : executor.database.getSchemas().entrySet()) {
                for (Map.Entry<String, Table> tableEntry
                        : schemaEntry.getValue().getTables().entrySet()) {
                    if (vacuuming) tableEntry.getValue().setLastVacuum(now);
                    if (analysing) {
                        String relation = schemaEntry.getKey() + "." + tableEntry.getKey();
                        executor.database.recordAnalyzedTable(relation);
                        tableEntry.getValue().setLastAnalyze(now);
                        executor.database.recordAnalyzedColumns(relation, null);
                        gatherStatistics(relation, tableEntry.getValue(), null);
                    }
                }
            }
            if (analysing) markStatisticsAnalysed(null);
        }
        if (stmt.isOn("VERBOSE") && executor.session != null) {
            executor.session.addNotice("NOTICE", "00000", "vacuuming \"public."
                    + (stmt.name() == null ? "all tables" : stmt.name()) + "\"", null);
        }
        return QueryResult.message(QueryResult.Type.SET, label);
    }

    /**
     * What this ANALYZE learned, kept against the relation. A statement that names columns
     * gathers statistics for those and leaves the rest of the relation's as it found them.
     */
    private void gatherStatistics(String relation, Table table, List<String> columns) {
        Map<String, ColumnStatistics> gathered = ColumnStatistics.gather(table);
        if (columns != null) {
            Map<String, ColumnStatistics> named = new java.util.LinkedHashMap<>();
            for (String column : columns) {
                ColumnStatistics stats = gathered.get(column.toLowerCase(java.util.Locale.ROOT));
                if (stats != null) named.put(column.toLowerCase(java.util.Locale.ROOT), stats);
            }
            gathered = named;
        }
        // ANALYZE writes a catalogue row for each column it learned something about, and a
        // transaction that writes one gets an id of its own. Over a relation with no rows it
        // writes nothing and stays read-only, which is why the id turns on the statistics
        // gathered rather than on the statement having run.
        if (!gathered.isEmpty() && executor.session != null) {
            executor.session.getTransactionId();
        }
        executor.database.recordColumnStatistics(relation, gathered);
    }

    /** The columns an ANALYZE names are columns of the relation it names. */
    private void requireColumns(Table table, String relation, List<String> columns) {
        for (String column : columns) {
            if (table.getColumnIndex(column) < 0) {
                throw new MemgresException("column \"" + column + "\" of relation \""
                        + relation + "\" does not exist", "42703");
            }
        }
    }

    private void markStatisticsAnalysed(String tableName) {
        for (ExtendedStatistic es : executor.database.getAllExtendedStatistics().values()) {
            if (tableName == null || es.getTableName().equalsIgnoreCase(tableName)) {
                es.setAnalyzed(true);
            }
        }
    }
}
