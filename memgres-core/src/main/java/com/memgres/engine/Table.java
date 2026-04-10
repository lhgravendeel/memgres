package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An in-memory table storing rows as Object arrays.
 */
public class Table {

    private final String name;
    private final List<Column> columns;
    private final List<Object[]> rows = new CopyOnWriteArrayList<>();
    private final AtomicLong serialCounter = new AtomicLong(1);
    private final List<StoredConstraint> constraints = new ArrayList<>();
    private final ReentrantLock writeLock = new ReentrantLock();

    // Hash indexes keyed by constraint name (for PK, UNIQUE constraints)
    private final Map<String, TableIndex> indexes = new ConcurrentHashMap<>();

    // Inheritance
    private Table parentTable;
    private final List<Table> children = new ArrayList<>();

    // Partitioning
    private String partitionStrategy; // RANGE, LIST, HASH (null if not partitioned)
    private String partitionColumn;
    private final List<Table> partitions = new ArrayList<>();
    private Table partitionParent;
    private Object partitionLower;     // for RANGE
    private Object partitionUpper;     // for RANGE
    private List<Object> partitionValues; // for LIST
    private Integer partitionModulus;   // for HASH
    private Integer partitionRemainder; // for HASH
    private boolean defaultPartition;  // DEFAULT partition

    // Row-level security
    private boolean rlsEnabled;
    private boolean rlsForced;
    private final List<RlsPolicy> rlsPolicies = new ArrayList<>();

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
    }

    public String getName() {
        return name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public void insertRow(Object[] row) {
        rows.add(row);
        for (TableIndex idx : indexes.values()) {
            idx.put(row);
        }
    }

    public ReentrantLock getWriteLock() {
        return writeLock;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    /**
     * Get all rows including inherited children (for table inheritance).
     * Returns own rows + all children's rows (recursively).
     */
    public List<Object[]> getAllRows() {
        if (children.isEmpty() && partitions.isEmpty()) return rows;
        List<Object[]> allRows = new ArrayList<>(rows);
        for (Table child : children) {
            // Map child rows to parent column layout
            for (Object[] childRow : child.getAllRows()) {
                Object[] parentRow = new Object[columns.size()];
                for (int i = 0; i < columns.size() && i < childRow.length; i++) {
                    parentRow[i] = childRow[i];
                }
                allRows.add(parentRow);
            }
        }
        for (Table partition : partitions) {
            allRows.addAll(partition.getRows());
        }
        return allRows;
    }

    /**
     * Record pairing a row with the table it physically belongs to.
     * For partitioned tables, source is the partition; for regular tables, source is this table.
     */
        public static final class RowWithSource {
        public final Table source;
        public final Object[] row;

        public RowWithSource(Table source, Object[] row) {
            this.source = source;
            this.row = row;
        }

        public Table source() { return source; }
        public Object[] row() { return row; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RowWithSource that = (RowWithSource) o;
            return java.util.Objects.equals(source, that.source)
                && java.util.Arrays.equals(row, that.row);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(source, java.util.Arrays.hashCode(row));
        }

        @Override
        public String toString() {
            return "RowWithSource[source=" + source + ", " + "row=" + java.util.Arrays.toString(row) + "]";
        }
    }

    /**
     * Get all rows with their source table (the table that physically stores the row).
     * For partitioned tables, the source is the partition table.
     * For inherited tables, the source is the child table.
     * For regular tables, the source is this table itself.
     */
    public List<RowWithSource> getAllRowsWithSource() {
        if (children.isEmpty() && partitions.isEmpty()) {
            List<RowWithSource> result = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                result.add(new RowWithSource(this, row));
            }
            return result;
        }
        List<RowWithSource> allRows = new ArrayList<>();
        for (Object[] row : rows) {
            allRows.add(new RowWithSource(this, row));
        }
        for (Table child : children) {
            for (RowWithSource childRws : child.getAllRowsWithSource()) {
                Object[] parentRow = new Object[columns.size()];
                for (int i = 0; i < columns.size() && i < childRws.row().length; i++) {
                    parentRow[i] = childRws.row()[i];
                }
                allRows.add(new RowWithSource(childRws.source(), parentRow));
            }
        }
        for (Table partition : partitions) {
            for (Object[] row : partition.getRows()) {
                allRows.add(new RowWithSource(partition, row));
            }
        }
        return allRows;
    }

    public long nextSerial() {
        return serialCounter.getAndIncrement();
    }

    public int deleteAll() {
        writeLock.lock();
        try {
            int count = rows.size();
            rows.clear();
            for (TableIndex idx : indexes.values()) {
                idx.clear();
            }
            return count;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Atomically remove specific rows from the table using identity comparison.
     * This is thread-safe for concurrent deletes from different sessions.
     */
    public int deleteRows(java.util.Set<Object[]> toDelete) {
        writeLock.lock();
        try {
            int deleted = 0;
            List<Object[]> snapshot = new ArrayList<>(rows);
            rows.clear();
            for (Object[] row : snapshot) {
                if (toDelete.contains(row)) {
                    for (TableIndex idx : indexes.values()) {
                        idx.remove(row);
                    }
                    deleted++;
                } else {
                    rows.add(row);
                }
            }
            return deleted;
        } finally {
            writeLock.unlock();
        }
    }

    public void addColumn(Column column) {
        addColumn(column, null);
    }

    public void addColumn(Column column, Object defaultValue) {
        columns.add(column);
        int newColIdx = columns.size() - 1;
        for (int i = 0; i < rows.size(); i++) {
            Object[] oldRow = rows.get(i);
            Object[] newRow = new Object[columns.size()];
            System.arraycopy(oldRow, 0, newRow, 0, oldRow.length);
            if (defaultValue != null) {
                newRow[newColIdx] = defaultValue;
            }
            rows.set(i, newRow);
        }
        // Column indices haven't changed for existing columns, but row references changed
        rebuildAllIndexes();
    }

    public void removeColumn(String columnName) {
        int idx = getColumnIndex(columnName);
        if (idx < 0) throw new MemgresException("Column not found: " + columnName);
        columns.remove(idx);
        for (int i = 0; i < rows.size(); i++) {
            Object[] oldRow = rows.get(i);
            Object[] newRow = new Object[columns.size()];
            for (int j = 0, k = 0; j < oldRow.length; j++) {
                if (j != idx) newRow[k++] = oldRow[j];
            }
            rows.set(i, newRow);
        }
        // Column indices changed, so rebuild index column mappings
        // Remove indexes referencing the dropped column, rebuild the rest
        List<String> toRemove = new ArrayList<>();
        for (Map.Entry<String, TableIndex> entry : indexes.entrySet()) {
            for (int ci : entry.getValue().getColumnIndices()) {
                if (ci == idx) {
                    toRemove.add(entry.getKey());
                    break;
                }
            }
        }
        toRemove.forEach(indexes::remove);
        // Remaining indexes need column index remapping; simplest to rebuild from constraints
        rebuildIndexesFromConstraints();
    }

    /** Rebuild all indexes from current constraints (after column layout changes). */
    void rebuildIndexesFromConstraints() {
        indexes.clear();
        for (StoredConstraint sc : constraints) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                // Skip expression-based indexes because they can't use simple column lookups
                if (sc.getExpressionColumns() != null && !sc.getExpressionColumns().isEmpty()) continue;
                // Skip partial indexes since they need WHERE evaluation
                if (sc.getWhereExpr() != null) continue;
                int[] colIndices = resolveColumnIndices(sc.getColumns());
                if (colIndices != null) {
                    TableIndex idx = new TableIndex(sc.getName(), colIndices, true);
                    buildIndex(idx);
                }
            }
        }
    }

    /** Resolve column names to indices, returns null if any column not found. */
    int[] resolveColumnIndices(List<String> columnNames) {
        int[] indices = new int[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            indices[i] = getColumnIndex(columnNames.get(i));
            if (indices[i] < 0) return null;
        }
        return indices;
    }

    public void addColumnAt(Column column, int position, List<Object> values) {
        columns.add(position, column);
        for (int i = 0; i < rows.size(); i++) {
            Object[] oldRow = rows.get(i);
            Object[] newRow = new Object[oldRow.length + 1];
            System.arraycopy(oldRow, 0, newRow, 0, position);
            newRow[position] = values != null && i < values.size() ? values.get(i) : null;
            System.arraycopy(oldRow, position, newRow, position + 1, oldRow.length - position);
            rows.set(i, newRow);
        }
        rebuildIndexesFromConstraints();
    }

    public void renameColumn(String oldName, String newName) {
        int idx = getColumnIndex(oldName);
        if (idx < 0) throw new MemgresException("Column not found: " + oldName);
        Column old = columns.get(idx);
        columns.set(idx, new Column(newName, old.getType(), old.isNullable(), old.isPrimaryKey(),
                old.getDefaultValue(), old.getEnumTypeName(), old.getPrecision(), old.getScale(), old.getGeneratedExpr()));
    }

    public void alterColumnType(String columnName, DataType newType) {
        int idx = getColumnIndex(columnName);
        if (idx < 0) throw new MemgresException("Column not found: " + columnName);
        Column old = columns.get(idx);
        columns.set(idx, new Column(old.getName(), newType, old.isNullable(), old.isPrimaryKey(),
                old.getDefaultValue(), old.getEnumTypeName(), old.getPrecision(), old.getScale(), old.getGeneratedExpr()));
    }

    public void alterColumnDefault(String columnName, String defaultValue) {
        int idx = getColumnIndex(columnName);
        if (idx < 0) throw new MemgresException("Column not found: " + columnName);
        Column old = columns.get(idx);
        columns.set(idx, new Column(old.getName(), old.getType(), old.isNullable(), old.isPrimaryKey(),
                defaultValue, old.getEnumTypeName(), old.getPrecision(), old.getScale(), old.getGeneratedExpr()));
    }

    public void alterColumnNullable(String columnName, boolean nullable) {
        int idx = getColumnIndex(columnName);
        if (idx < 0) throw new MemgresException("Column not found: " + columnName);
        Column old = columns.get(idx);
        columns.set(idx, new Column(old.getName(), old.getType(), nullable, old.isPrimaryKey(),
                old.getDefaultValue(), old.getEnumTypeName(), old.getPrecision(), old.getScale(), old.getGeneratedExpr()));
    }

    public long getSerialCounter() {
        return serialCounter.get();
    }

    public void resetSerialCounter(long value) {
        serialCounter.set(value);
    }

    /**
     * Notify indexes that a row's values are about to change (UPDATE).
     * Must be called BEFORE the in-place arraycopy with the old values.
     */
    public void beforeRowUpdate(Object[] row, Object[] oldValues) {
        for (TableIndex idx : indexes.values()) {
            idx.removeByOldValues(oldValues, row);
        }
    }

    /**
     * Notify indexes that a row's values have changed (UPDATE).
     * Must be called AFTER the in-place arraycopy with new values.
     */
    public void afterRowUpdate(Object[] row) {
        for (TableIndex idx : indexes.values()) {
            idx.put(row);
        }
    }

    /** Remove a single row from the table and its indexes. */
    public void removeRow(Object[] row) {
        for (TableIndex idx : indexes.values()) {
            idx.remove(row);
        }
        rows.remove(row);
    }

    // Index management
    public Map<String, TableIndex> getIndexes() { return indexes; }

    public void addIndex(TableIndex index) {
        indexes.put(index.getConstraintName(), index);
    }

    public void removeIndex(String constraintName) {
        indexes.remove(constraintName);
    }

    public TableIndex getIndex(String constraintName) {
        return indexes.get(constraintName);
    }

    /**
     * Build an index from existing rows (used when adding a constraint to a populated table).
     */
    public void buildIndex(TableIndex index) {
        for (Object[] row : rows) {
            index.put(row);
        }
        indexes.put(index.getConstraintName(), index);
    }

    /**
     * Rebuild all indexes (used after column add/remove which changes row layout).
     */
    void rebuildAllIndexes() {
        for (TableIndex idx : indexes.values()) {
            idx.clear();
            for (Object[] row : rows) {
                idx.put(row);
            }
        }
    }

    // Constraint management
    public List<StoredConstraint> getConstraints() { return constraints; }
    public void addConstraint(StoredConstraint constraint) {
        constraints.add(constraint);
        // Automatically build a hash index for PK/UNIQUE constraints on simple columns
        if ((constraint.getType() == StoredConstraint.Type.PRIMARY_KEY
                || constraint.getType() == StoredConstraint.Type.UNIQUE)
                && constraint.getName() != null
                && (constraint.getExpressionColumns() == null || constraint.getExpressionColumns().isEmpty())
                && constraint.getWhereExpr() == null) {
            int[] colIndices = resolveColumnIndices(constraint.getColumns());
            if (colIndices != null) {
                // Skip building index on virtual columns (computed on read, not stored in row)
                boolean hasVirtualCol = false;
                for (int ci : colIndices) {
                    if (ci < columns.size() && columns.get(ci).isVirtual()) {
                        hasVirtualCol = true;
                        break;
                    }
                }
                if (!hasVirtualCol) {
                    TableIndex idx = new TableIndex(constraint.getName(), colIndices, true);
                    buildIndex(idx);
                }
            }
        }
    }
    public void removeConstraint(String name) {
        constraints.removeIf(c -> c.getName() != null && c.getName().equalsIgnoreCase(name));
        indexes.remove(name);
    }
    public StoredConstraint getConstraint(String name) {
        for (StoredConstraint c : constraints) {
            if (c.getName() != null && c.getName().equalsIgnoreCase(name)) return c;
        }
        return null;
    }

    // Inheritance
    public Table getParentTable() { return parentTable; }
    public void setParentTable(Table parent) { this.parentTable = parent; }
    public List<Table> getChildren() { return children; }
    public void addChild(Table child) { children.add(child); }
    public void removeChild(Table child) { children.remove(child); }

    // Partitioning
    public String getPartitionStrategy() { return partitionStrategy; }
    public void setPartitionStrategy(String strategy) { this.partitionStrategy = strategy; }
    public String getPartitionColumn() { return partitionColumn; }
    public void setPartitionColumn(String column) { this.partitionColumn = column; }
    public List<Table> getPartitions() { return partitions; }
    public void addPartition(Table partition) { partitions.add(partition); }
    public Table getPartitionParent() { return partitionParent; }
    public void setPartitionParent(Table parent) { this.partitionParent = parent; }
    public Object getPartitionLower() { return partitionLower; }
    public void setPartitionBounds(Object lower, Object upper) { this.partitionLower = lower; this.partitionUpper = upper; }
    public Object getPartitionUpper() { return partitionUpper; }
    public List<Object> getPartitionValues() { return partitionValues; }
    public void setPartitionValues(List<Object> values) { this.partitionValues = values; }
    public Integer getPartitionModulus() { return partitionModulus; }
    public Integer getPartitionRemainder() { return partitionRemainder; }
    public void setPartitionHash(int modulus, int remainder) { this.partitionModulus = modulus; this.partitionRemainder = remainder; }
    public boolean isDefaultPartition() { return defaultPartition; }
    public void setDefaultPartition(boolean defaultPartition) { this.defaultPartition = defaultPartition; }
    public void removePartition(Table partition) { partitions.remove(partition); }

    // Row-level security
    public boolean isRlsEnabled() { return rlsEnabled; }
    public void setRlsEnabled(boolean enabled) { this.rlsEnabled = enabled; }
    public boolean isRlsForced() { return rlsForced; }
    public void setRlsForced(boolean forced) { this.rlsForced = forced; }
    public List<RlsPolicy> getRlsPolicies() { return rlsPolicies; }
    public void addRlsPolicy(RlsPolicy policy) { rlsPolicies.add(policy); }
}
