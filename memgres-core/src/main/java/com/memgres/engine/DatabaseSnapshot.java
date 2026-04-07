package com.memgres.engine;

import java.util.*;

/**
 * Captures and restores the data state of a {@link Database}.
 * Only captures row data and sequence state, not schema/DDL.
 * This makes snapshot + restore very fast (array copies, no SQL parsing).
 */
public class DatabaseSnapshot {

    private final Map<String, Map<String, TableData>> schemaSnapshots;
    private final Map<String, SequenceData> sequenceSnapshots;

    private DatabaseSnapshot(Map<String, Map<String, TableData>> schemaSnapshots,
                             Map<String, SequenceData> sequenceSnapshots) {
        this.schemaSnapshots = schemaSnapshots;
        this.sequenceSnapshots = sequenceSnapshots;
    }

    /**
     * Takes a snapshot of all row data and sequence values in the database.
     */
    public static DatabaseSnapshot capture(Database database) {
        // Snapshot table rows
        Map<String, Map<String, TableData>> schemas = new LinkedHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            Schema schema = schemaEntry.getValue();
            Map<String, TableData> tables = new LinkedHashMap<>();
            for (Map.Entry<String, Table> tableEntry : schema.getTables().entrySet()) {
                Table table = tableEntry.getValue();
                tables.put(tableEntry.getKey(), TableData.capture(table));
            }
            schemas.put(schemaName, tables);
        }

        // Snapshot sequences
        Map<String, SequenceData> sequences = new LinkedHashMap<>();
        for (Map.Entry<String, Sequence> entry : database.getSequences().entrySet()) {
            Sequence seq = entry.getValue();
            sequences.put(entry.getKey(), new SequenceData(seq.currValRaw(), seq.isCalled()));
        }

        return new DatabaseSnapshot(schemas, sequences);
    }

    /**
     * Restores the database to this snapshot's state.
     * Replaces all table rows and resets sequence counters.
     */
    public void restore(Database database) {
        // Restore table rows
        for (Map.Entry<String, Map<String, TableData>> schemaEntry : schemaSnapshots.entrySet()) {
            Schema schema = database.getSchema(schemaEntry.getKey());
            if (schema == null) continue;
            for (Map.Entry<String, TableData> tableEntry : schemaEntry.getValue().entrySet()) {
                Table table = schema.getTable(tableEntry.getKey());
                if (table == null) continue;
                tableEntry.getValue().restore(table);
            }
        }

        // Restore sequences
        for (Map.Entry<String, SequenceData> entry : sequenceSnapshots.entrySet()) {
            Sequence seq = database.getSequence(entry.getKey());
            if (seq != null) {
                SequenceData data = entry.getValue();
                seq.setVal(data.value, data.called);
            }
        }
    }

        private static final class SequenceData {
        public final long value;
        public final boolean called;

        public SequenceData(long value, boolean called) {
            this.value = value;
            this.called = called;
        }

        public long value() { return value; }
        public boolean called() { return called; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SequenceData that = (SequenceData) o;
            return value == that.value
                && called == that.called;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(value, called);
        }

        @Override
        public String toString() {
            return "SequenceData[value=" + value + ", " + "called=" + called + "]";
        }
    }

        private static final class TableData {
        public final List<Object[]> rows;
        public final long serialCounter;

        public TableData(List<Object[]> rows, long serialCounter) {
            this.rows = rows;
            this.serialCounter = serialCounter;
        }

        static TableData capture(Table table) {
            List<Object[]> copy = new ArrayList<>(table.getRows().size());
            for (Object[] row : table.getRows()) {
                copy.add(row.clone());
            }
            return new TableData(copy, table.getSerialCounter());
        }

        void restore(Table table) {
            table.getRows().clear();
            for (Object[] row : rows) {
                table.getRows().add(row.clone());
            }
            table.rebuildAllIndexes();
            table.resetSerialCounter(serialCounter);
        }

        public List<Object[]> rows() { return rows; }
        public long serialCounter() { return serialCounter; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableData that = (TableData) o;
            return java.util.Objects.equals(rows, that.rows)
                && serialCounter == that.serialCounter;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(rows, serialCounter);
        }

        @Override
        public String toString() {
            return "TableData[rows=" + rows + ", " + "serialCounter=" + serialCounter + "]";
        }
    }
}
