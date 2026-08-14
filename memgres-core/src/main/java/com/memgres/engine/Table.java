package com.memgres.engine;

import java.util.ArrayList;
import java.util.Arrays;
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

    /**
     * A rename is the same relation under another name, so the name is changed on the table
     * rather than copied onto a replacement. Every partition bound, inheritance link, policy,
     * storage parameter and constraint name a copy would have had to remember stays where it is.
     */
    private volatile String name;
    // CopyOnWriteArrayList: safe for concurrent reads (getColumnIndex from DML threads)
    // while DDL methods (renameColumn, alterColumnType, etc.) modify via set().
    private final List<Column> columns;
    /**
     * The attribute numbers this relation has handed out that no column answers to any more, in
     * ascending order, each beside the column that held it. PostgreSQL does not renumber a
     * relation when a column goes: the dropped column's row stays behind under a name nothing
     * could have written, so every column after it keeps the number a constraint, an index, a
     * default or a trigger may already be holding.
     */
    private final List<DroppedAttribute> droppedAttributes = new CopyOnWriteArrayList<>();
    private volatile List<Object[]> rows = new ArrayList<>();
    private final AtomicLong serialCounter = new AtomicLong(1);
    /**
     * The line pointers this relation has handed out. Where a tuple sits is a property of the
     * relation's own file rather than of the name it stands under, so a rename or a move to
     * another schema carries the numbering with it, while a relation created after another was
     * dropped begins a file of its own and starts again at one.
     */
    private final AtomicLong tupleIdCounter = new AtomicLong(0);
    /**
     * What each row's system columns answer: the transaction that wrote it, the one that removed
     * it, the command each of those was, and the line pointer it sits at, kept against the row
     * itself.
     *
     * <p>None of that belongs to the name the relation stands under. PostgreSQL renames a relation,
     * and moves one between schemas, without rewriting a tuple, so afterwards a row still names
     * the transaction that inserted it and still sits where it always sat. Held under the written
     * name instead, the whole book became unreachable the moment the name changed, and the book a
     * dropped relation left behind stayed for as long as the server ran.
     */
    private final Map<Object[], long[]> rowMeta = new java.util.IdentityHashMap<>();
    private final List<StoredConstraint> constraints = new CopyOnWriteArrayList<>();
    /**
     * The name a column's NOT NULL constraint answers to, keyed by column. Every NOT NULL column
     * has such a constraint in PostgreSQL; it is named {@code <table>_<column>_not_null} unless
     * the writer named it, and that name is what DROP CONSTRAINT, RENAME CONSTRAINT and ALTER
     * CONSTRAINT ask for. Only names that differ from the default are stored here.
     */
    private final Map<String, String> notNullConstraintNames = new java.util.concurrent.ConcurrentHashMap<>();
    /** Per column: the name an inherited NOT NULL answers to, once it can no longer be derived. */
    private final Map<String, String> inheritedNotNullNames = new java.util.concurrent.ConcurrentHashMap<>();
    /** Per column: parents whose NOT NULL this one still counts although they no longer declare it. */
    private final Map<String, Integer> retainedNotNullInheritCounts = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * The columns this relation holds only because a parent declares them. PostgreSQL calls a
     * column local when the relation's own definition named it, and that is a different question
     * from how many parents also have it: a child that restates an inherited column in its own
     * column list is reported local and inherited once over, both at the same time.
     */
    private final java.util.Set<String> inheritedColumns =
            java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    /**
     * The columns whose NOT NULL arrived from a parent rather than from this relation's own
     * definition. Such a constraint answers to the name the relation that declared it gave it, and
     * nobody below may drop it; a child that restates NOT NULL for itself carries a constraint of
     * its own name that it may withdraw.
     */
    private final java.util.Set<String> inheritedNotNullColumns =
            java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    /**
     * The columns whose NOT NULL was declared NO INHERIT. Such a rule is about this relation's own
     * rows and nobody else's: PostgreSQL hands the column down without it, and a descendant that
     * declares NOT NULL for itself counts no parent for it.
     */
    private final java.util.Set<String> noInheritNotNullColumns =
            java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    /**
     * The columns whose NOT NULL was declared NOT VALID and has not been validated since. Every
     * constraint PostgreSQL keeps carries that flag, and a NOT NULL is one constraint per column,
     * so this is where the column's own copy of it lives. The rule itself is in force either way --
     * every row written from now on is held to it -- and the flag says only whether the rows
     * already stored have been read, which is what VALIDATE CONSTRAINT goes back to do.
     */
    private final java.util.Set<String> notValidatedNotNullColumns =
            java.util.concurrent.ConcurrentHashMap.<String>newKeySet();
    private final ReentrantLock writeLock = new ReentrantLock();

    // Hash indexes keyed by constraint name (for PK, UNIQUE constraints)
    private final Map<String, TableIndex> indexes = new ConcurrentHashMap<>();

    // DML statistics counters for pg_stat_user_tables
    private final AtomicLong tupInserted = new AtomicLong(0);
    private final AtomicLong tupUpdated = new AtomicLong(0);
    private final AtomicLong tupDeleted = new AtomicLong(0);
    private final AtomicLong idxScanCount = new AtomicLong(0);

    // Maintenance timestamps for pg_stat_user_tables
    private volatile java.time.OffsetDateTime lastVacuum;
    private volatile java.time.OffsetDateTime lastAnalyze;

    // Inheritance
    /**
     * Every table this one directly inherits from, in the order they were named. PostgreSQL keeps
     * one pg_inherits row per parent with inhseqno counting from 1, so a table declared under two
     * parents has two of them; a single field kept only whichever came last.
     */
    private final List<Table> inheritParents = new CopyOnWriteArrayList<>();
    private final List<Table> children = new CopyOnWriteArrayList<>();

    // Partitioning
    private String partitionStrategy; // RANGE, LIST, HASH (null if not partitioned)
    private String partitionColumn;
    private final List<Table> partitions = new CopyOnWriteArrayList<>();
    private Table partitionParent;
    private Object partitionLower;     // for RANGE
    private Object partitionUpper;     // for RANGE
    private List<Object> partitionValues; // for LIST
    private Integer partitionModulus;   // for HASH
    private Integer partitionRemainder; // for HASH
    private boolean defaultPartition;  // DEFAULT partition

    // Storage parameters (WITH options, e.g. fillfactor=80)
    private Map<String, String> reloptions;

    // Unlogged table
    private boolean unlogged;

    // Provenance marker: true for transient virtual tables built by FromFunctionResolver for
    // set-returning functions in FROM (generate_series, unnest, ...). Never set on stored tables,
    // subquery/VALUES/CTE result tables. Gates the attribute-notation fallback in ExprEvaluator.
    private boolean functionResult;

    // Provenance marker: true for the transient virtual table built for a non-auto-updatable
    // view that carries INSTEAD OF triggers. Its rows are a snapshot of the view's projection,
    // so UPDATE/DELETE dispatch INSTEAD OF triggers per matching row rather than touching storage.
    private boolean viewProjection;

    // Replica identity for logical replication (DEFAULT, FULL, NOTHING, or index name)
    // 'd' = DEFAULT (PK), 'f' = FULL, 'n' = NOTHING, 'i' = USING INDEX
    private volatile char replicaIdentity = 'd';

    // Row-level security
    private boolean rlsEnabled;
    private boolean rlsForced;
    private final List<RlsPolicy> rlsPolicies = new CopyOnWriteArrayList<>();

    public Table(String name, List<Column> columns) {
        this.name = name;
        this.columns = new CopyOnWriteArrayList<>(columns);
    }

    /**
     * The name this relation answers to for the session asking. A rename inside an open
     * transaction has happened for nobody else yet, and since the rename changes the one
     * relation object rather than making a second one, everyone else would otherwise read the
     * new name off it — which is the new name leaking out of an uncommitted transaction.
     */
    public String getName() {
        String previous = renamedFrom;
        if (previous == null) return name;
        Session renamer = renamedBy;
        if (renamer == null || !renamer.isInTransaction()) return name;
        Session viewer = Database.currentViewer();
        return viewer == null || viewer == renamer ? name : previous;
    }

    /**
     * Rename this table. Only a schema that has already removed it under the old name may call
     * this: the schema keys its tables by name, so the two have to move together.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * The name this relation is still known by outside the transaction that renamed it, and the
     * session that did. DDL is transactional, so until that transaction ends the old name is the
     * relation's name for every other session.
     */
    private volatile String renamedFrom;
    private volatile Session renamedBy;

    /** Record that {@code renamer}, mid-transaction, renamed this relation away from {@code from}. */
    void markUncommittedRename(String from, Session renamer) {
        if (renamer == null || !renamer.isInTransaction()) return;
        // Two renames in one transaction: the name everyone else knows is the one it started with.
        if (renamedFrom == null || renamedBy != renamer) renamedFrom = from;
        renamedBy = renamer;
    }

    /** That transaction has ended, one way or the other; the name it left is the name. */
    void forgetUncommittedRename(Session renamer) {
        if (renamedBy == renamer) {
            renamedBy = null;
            renamedFrom = null;
        }
    }

    /**
     * The schema holding this table, set when it is added to one. A column default names its
     * sequence without a qualifier, so the answer to "which sequence does {@code t_id_seq} mean"
     * is only decidable once the table knows which schema it is in.
     */
    private String schemaName = "public";

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName == null ? "public" : schemaName;
    }

    public List<Column> getColumns() {
        // A column an open transaction added is not part of the relation for anyone else yet, so
        // it is left out of their view — which keeps it out of pg_attribute, information_schema
        // and every listing built from the table's own columns.
        return Database.visibleColumns(columns);
    }

    /**
     * Where each column name sits, built on demand and dropped whenever the column list changes.
     * Null when it has to be built again.
     */
    private volatile Map<String, Integer> columnPositions;

    public int getColumnIndex(String columnName) {
        if (columnName == null) return -1;
        Map<String, Integer> positions = columnPositions;
        if (positions == null) {
            // A name may appear twice -- an ALTER that renames one column on to another's name,
            // a virtual relation built from a join -- and the answer has always been the first
            // of them, so a later entry does not replace an earlier one.
            positions = new java.util.HashMap<String, Integer>();
            for (int i = 0; i < columns.size(); i++) {
                String n = columns.get(i).getName();
                if (n == null) continue;
                if (!positions.containsKey(n)) positions.put(n, Integer.valueOf(i));
            }
            columnPositions = positions;
        }
        // A name matches as it is written. The lexer has already folded every unquoted identifier
        // to lower case, so what arrives here still carrying capitals was written in quotes and
        // means that exact name: "mixed" is not the column "MiXeD", and matching the two let a
        // query read a column it did not name and an ALTER drop one it did not name.
        Integer at = positions.get(columnName);
        return at == null ? -1 : at.intValue();
    }

    /** The column list changed, so where the names sit has to be worked out again. */
    private void columnsChanged() {
        columnPositions = null;
    }

    public void insertRow(Object[] row) {
        writeLock.lock();
        try {
            append(row);
            for (TableIndex idx : indexes.values()) {
                idx.put(row);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Room past the end of {@link #rows} for the next appends, or null when the rows in front
     * of readers are a list this table does not own. Written only under {@link #writeLock}.
     */
    private Object[][] spare;

    /**
     * Add one row without copying the ones already there.
     *
     * <p>Readers walk {@link #rows} without holding the lock, so a row list may never be added
     * to once it has been handed out -- which made loading a relation cost a copy of everything
     * already in it, and loading n rows cost n squared. Reading a CTE or a system catalogue of
     * any size spent most of its time there.
     *
     * <p>What is handed out instead is a window on to an array with room to spare: the next row
     * is written past the end of every window already given away, and only then is a window one
     * longer published. A reader holding the shorter window cannot reach the new entry, and the
     * array it is reading is never moved out from under it -- when the room runs out the rows
     * are copied into a larger array and the old one is left to the readers still on it.
     *
     * <p>Caller holds {@link #writeLock}.
     */
    private void append(Object[] row) {
        int n = rows.size();
        if (spare == null || n >= spare.length) {
            Object[][] grown = new Object[Math.max(8, n + (n >> 1) + 1)][];
            for (int i = 0; i < n; i++) grown[i] = rows.get(i);
            spare = grown;
        }
        spare[n] = row;
        rows = Arrays.asList(spare).subList(0, n + 1);
    }

    /**
     * Put a row list in front of readers. Anything but an append replaces the list wholesale,
     * which leaves the room {@link #append} was keeping behind an array nothing points at.
     */
    private void publish(List<Object[]> replacement) {
        spare = null;
        rows = replacement;
    }

    public ReentrantLock getWriteLock() {
        return writeLock;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    /**
     * Hand this table rows it does not own a copy of.
     *
     * <p>{@link #replaceAllRows} copies what it is given, which is right for a table whose rows
     * are values someone may go on to change. The rows of a virtual relation built for one FROM
     * item are not: they are worked out from the item itself and read as the query walks them,
     * and copying them would build the very list the item was written not to build.
     */
    public void publishGeneratedRows(List<Object[]> generated) {
        writeLock.lock();
        try {
            publish(generated);
        } finally {
            writeLock.unlock();
        }
    }

    /** Atomically replace all rows (used by snapshot restore and temp table truncation). */
    public void replaceAllRows(List<Object[]> newRows) {
        writeLock.lock();
        try {
            publish(new ArrayList<>(newRows));
        } finally {
            writeLock.unlock();
        }
    }

    /** Atomically replace all rows AND rebuild all indexes under a single lock acquisition.
     *  Used by snapshot restore to prevent a concurrent DML from slipping in between
     *  the row swap and the index rebuild. */
    public void replaceAllRowsAndRebuildIndexes(List<Object[]> newRows) {
        writeLock.lock();
        try {
            publish(new ArrayList<>(newRows));
            for (TableIndex idx : indexes.values()) {
                idx.clear();
                for (Object[] row : rows) {
                    idx.put(row);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Atomically clear all rows without touching indexes (used by ON COMMIT DELETE ROWS). */
    public void clearRows() {
        writeLock.lock();
        try {
            publish(new ArrayList<Object[]>());
        } finally {
            writeLock.unlock();
        }
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
            // Recurse so multi-level partitioning (partition of a partition) is included
            allRows.addAll(partition.getAllRows());
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
        /**
         * The row as the relation that stores it holds it. Reading a row through the relation
         * above it rearranges it to that relation's columns, and the rearranged copy is not the
         * tuple PostgreSQL answers ctid, xmin and the rest from: those belong to the row where it
         * lives, which is this one.
         */
        public final Object[] stored;

        public RowWithSource(Table source, Object[] row) {
            this(source, row, row);
        }

        public RowWithSource(Table source, Object[] row, Object[] stored) {
            this.source = source;
            this.row = row;
            this.stored = stored;
        }

        public Table source() { return source; }
        public Object[] row() { return row; }
        public Object[] stored() { return stored; }

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
                allRows.add(new RowWithSource(childRws.source(), parentRow, childRws.stored()));
            }
        }
        for (Table partition : partitions) {
            // Recurse so multi-level partitioning (partition of a partition) is included
            for (RowWithSource rws : partition.getAllRowsWithSource()) {
                Object[] asParent = partition.rowToParent(rws.row());
                allRows.add(asParent == rws.row() ? rws
                        : new RowWithSource(rws.source(), asParent, rws.stored()));
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
            publish(new ArrayList<Object[]>());
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
     * Builds a new list and swaps the volatile reference so concurrent readers
     * never observe a partially-rebuilt intermediate state.
     */
    public int deleteRows(java.util.Set<Object[]> toDelete) {
        writeLock.lock();
        try {
            List<Object[]> current = rows;
            List<Object[]> surviving = new ArrayList<>(current.size());
            int deleted = 0;
            for (Object[] row : current) {
                if (toDelete.contains(row)) {
                    for (TableIndex idx : indexes.values()) {
                        idx.remove(row);
                    }
                    deleted++;
                } else {
                    surviving.add(row);
                }
            }
            publish(surviving);
            return deleted;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Delete a single row from the table using identity comparison.
     */
    public void deleteRow(Object[] row) {
        java.util.Set<Object[]> s = java.util.Collections.singleton(row);
        deleteRows(s);
    }

    public void addColumn(Column column) {
        addColumn(column, null);
    }

    public void addColumn(Column column, Object defaultValue) {
        writeLock.lock();
        try {
            columns.add(column);
            columnsChanged();
            int newColIdx = columns.size() - 1;
            List<Object[]> current = rows;
            List<Object[]> newRows = new ArrayList<>(current.size());
            for (Object[] oldRow : current) {
                Object[] newRow = new Object[columns.size()];
                System.arraycopy(oldRow, 0, newRow, 0, oldRow.length);
                if (defaultValue != null) {
                    newRow[newColIdx] = defaultValue;
                }
                newRows.add(newRow);
            }
            publish(newRows);
            rebuildAllIndexes();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * The index of a column the caller has to be able to find, in PostgreSQL's words when it
     * cannot: the complaint names the relation the name was looked for in, and quotes the name.
     * The SQLSTATE is stated here rather than left to be inferred from the message text, which
     * is what "Column not found" was relying on.
     */
    private int requireColumnIndex(String columnName) {
        int idx = getColumnIndex(columnName);
        if (idx < 0) {
            throw new MemgresException("column \"" + columnName + "\" of relation \""
                    + name + "\" does not exist", "42703");
        }
        return idx;
    }

    /**
     * One attribute number a dropped column left taken, and the column that held it.
     *
     * <p>PostgreSQL forgets what type a dropped attribute had -- the type itself may be gone by
     * now -- but keeps the width, alignment and storage that type gave it, because the rows
     * already stored are laid out that way. The column it was declared as is what answers all of
     * those, so the column is what is kept.
     */
    public static final class DroppedAttribute {
        private final int attnum;
        private final Column column;
        private final boolean local;
        private final short inheritCount;

        DroppedAttribute(int attnum, Column column, boolean local, short inheritCount) {
            this.attnum = attnum;
            this.column = column;
            this.local = local;
            this.inheritCount = inheritCount;
        }

        public int getAttnum() { return attnum; }

        public Column getColumn() { return column; }

        /** Whether the relation's own definition named the column, as attislocal reports it. */
        public boolean isLocal() { return local; }

        /** How many relations above this one declared the column, as attinhcount reports it. */
        public short getInheritCount() { return inheritCount; }
    }

    /** Whether a column of this relation has been dropped, leaving its number taken. */
    public boolean hasDroppedAttributes() {
        return !droppedAttributes.isEmpty();
    }

    /** The numbers dropped columns left behind, ascending, beside the columns that held them. */
    public List<DroppedAttribute> getDroppedAttributes() {
        return droppedAttributes;
    }

    /**
     * The attribute number PostgreSQL reports for the column sitting at {@code index}: its
     * position plus one for every column dropped at or before it, since a dropped column never
     * gives its number back. An index naming no column answers 0, the number pg_index and
     * pg_trigger use for something that is not a column of the relation.
     */
    public int attnumAt(int index) {
        if (index < 0) return 0;
        int attnum = index + 1;
        for (DroppedAttribute gone : droppedAttributes) {
            if (gone.getAttnum() <= attnum) attnum++;
        }
        return attnum;
    }

    /** The same for a column named rather than counted; 0 when this relation has no such column. */
    public int attnumOf(String columnName) {
        return attnumAt(getColumnIndex(columnName));
    }

    /**
     * Where the column carrying {@code attnum} sits, or -1 when the number belongs to a dropped
     * column or to no attribute of this relation at all. An attnum is not a position: past a
     * dropped column the numbers run ahead of the list they are read out of.
     */
    public int columnIndexOfAttnum(int attnum) {
        if (attnum < 1) return -1;
        int index = attnum - 1;
        for (DroppedAttribute gone : droppedAttributes) {
            if (gone.getAttnum() == attnum) return -1;
            if (gone.getAttnum() < attnum) index--;
        }
        return index >= 0 && index < columns.size() ? index : -1;
    }

    /**
     * Every attribute this relation has ever numbered, the dropped ones included. PostgreSQL
     * counts them all in pg_class.relnatts, which is why relnatts and the number of columns a
     * relation still has are two different questions.
     */
    public int getAttributeCount() {
        return getColumns().size() + droppedAttributes.size();
    }

    public void removeColumn(String columnName) {
        removeColumn(columnName, true);
    }

    /**
     * @param leaveDroppedAttribute whether the column's number stays taken. A DROP COLUMN leaves
     *                              it taken, which is why PostgreSQL never renumbers what follows;
     *                              a column that was added and then taken back was never part of
     *                              the relation, so its number goes back with it and the next
     *                              ADD COLUMN is handed the same one.
     */
    public void removeColumn(String columnName, boolean leaveDroppedAttribute) {
        writeLock.lock();
        try {
            int idx = requireColumnIndex(columnName);
            int attnum = attnumAt(idx);
            Column removed = columns.get(idx);
            // PG drops constraints that depend on the column automatically: single-column
            // constraints, multi-column PK/UNIQUE/FK containing it, and CHECK/EXCLUDE/partial
            // constraints whose expressions reference it. Without this they linger in
            // pg_constraint referencing a nonexistent column.
            List<StoredConstraint> dependent = new ArrayList<>();
            for (StoredConstraint sc : constraints) {
                if (sc.dependsOnColumn(columnName)) dependent.add(sc);
            }
            for (StoredConstraint sc : dependent) {
                constraints.remove(sc);
                if (sc.getName() != null) indexes.remove(sc.getName());
            }
            columns.remove(idx);
            if (leaveDroppedAttribute) {
                // Whether the column was the relation's own travels with the row it leaves
                // behind: PostgreSQL clears a dropped attribute's type and its name and leaves
                // the rest of what the attribute said standing. A partition declares nothing of
                // its own, and by the time a child's copy of an inherited column goes the parents
                // have already dropped theirs, so what is left to count is the relations this one
                // takes its columns from.
                boolean local = partitionParent == null && isColumnLocal(columnName);
                recordDroppedAttribute(attnum, removed, local,
                        local ? (short) 0 : (short) getDirectParents().size());
            }
            inheritedColumns.remove(columnName.toLowerCase());
            inheritedNotNullColumns.remove(columnName.toLowerCase());
            noInheritNotNullColumns.remove(columnName.toLowerCase());
            noInheritNotNullColumns.remove(columnName.toLowerCase());
            inheritedNotNullNames.remove(columnName.toLowerCase());
            retainedNotNullInheritCounts.remove(columnName.toLowerCase());
            notValidatedNotNullColumns.remove(columnName.toLowerCase());
            columnsChanged();
            List<Object[]> current = rows;
            List<Object[]> newRows = new ArrayList<>(current.size());
            for (Object[] oldRow : current) {
                Object[] newRow = new Object[columns.size()];
                for (int j = 0, k = 0; j < oldRow.length; j++) {
                    if (j != idx) newRow[k++] = oldRow[j];
                }
                newRows.add(newRow);
            }
            publish(newRows);
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
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remember that {@code attnum} is taken and by what, keeping the list ascending so that
     * {@link #attnumAt} can count the numbers below a position in a single pass. A column dropped
     * from the end of the relation is recorded after one dropped from the middle, so the order
     * they are recorded in is not the order they are held in.
     */
    private void recordDroppedAttribute(int attnum, Column column, boolean local, short inhCount) {
        int at = 0;
        while (at < droppedAttributes.size()
                && droppedAttributes.get(at).getAttnum() < attnum) {
            at++;
        }
        droppedAttributes.add(at, new DroppedAttribute(attnum, column, local, inhCount));
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
        writeLock.lock();
        try {
            // Putting a column back is undoing the drop that took it away, and a drop that never
            // happened left no number taken: the relation is the one it was before, so nothing may
            // be holding a number aside for a column it still has.
            for (DroppedAttribute gone : droppedAttributes) {
                if (gone.getColumn() == column) {
                    droppedAttributes.remove(gone);
                    break;
                }
            }
            columns.add(position, column);
            columnsChanged();
            List<Object[]> current = rows;
            List<Object[]> newRows = new ArrayList<>(current.size());
            for (int i = 0; i < current.size(); i++) {
                Object[] oldRow = current.get(i);
                Object[] newRow = new Object[oldRow.length + 1];
                System.arraycopy(oldRow, 0, newRow, 0, position);
                newRow[position] = values != null && i < values.size() ? values.get(i) : null;
                System.arraycopy(oldRow, position, newRow, position + 1, oldRow.length - position);
                newRows.add(newRow);
            }
            publish(newRows);
            rebuildIndexesFromConstraints();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Renames a column, preserving ALL column attributes (virtual/domain/composite/array
     * element type, catalog attributes) and rewriting every reference this table holds to
     * the old name: PK/UNIQUE/FK/CHECK/EXCLUDE constraint definitions and generated-column
     * expressions of other columns. Backing {@link TableIndex}es are positional, so they
     * stay valid without a rebuild. References from OTHER tables (incoming foreign keys)
     * and database-level index metadata are rewritten by the ALTER TABLE executor.
     */
    public void renameColumn(String oldName, String newName) {
        writeLock.lock();
        try {
            int idx = getColumnIndex(oldName);
            // A rename names only the column: PostgreSQL leaves the relation out of this one,
            // where DROP COLUMN and ALTER COLUMN both name it.
            if (idx < 0) {
                throw new MemgresException("column \"" + oldName + "\" does not exist", "42703");
            }
            Column old = columns.get(idx);
            columns.set(idx, old.withName(newName));
            // It is the same column under another name, so whether the relation declared it, and
            // whether it declared the NOT NULL on it, travel with it.
            if (inheritedColumns.remove(oldName.toLowerCase())) {
                inheritedColumns.add(newName.toLowerCase());
            }
            if (inheritedNotNullColumns.remove(oldName.toLowerCase())) {
                inheritedNotNullColumns.add(newName.toLowerCase());
            }
            if (notValidatedNotNullColumns.remove(oldName.toLowerCase())) {
                notValidatedNotNullColumns.add(newName.toLowerCase());
            }
            columnsChanged();
            // Keep constraints attached to the column under its new name
            for (StoredConstraint sc : constraints) {
                sc.renameColumn(oldName, newName);
            }
            // Rewrite generated-column expressions on other columns that reference the old name
            for (int i = 0; i < columns.size(); i++) {
                Column c = columns.get(i);
                if (i == idx || c.getGeneratedExpr() == null) continue;
                String updated = StoredConstraint.renameIdentifier(c.getGeneratedExpr(), oldName, newName);
                if (!updated.equals(c.getGeneratedExpr())) {
                    columns.set(i, c.withGeneratedExpr(updated));
                    columnsChanged();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public void alterColumnType(String columnName, DataType newType) {
        alterColumnType(columnName, newType, null, null);
    }

    /**
     * Changes a column's type, replacing the full type spec including its typmod: the new
     * precision/scale come from the new type declaration (null when it has none), never carried
     * over from the old column. Mirrors PostgreSQL, where {@code ALTER COLUMN x TYPE numeric(10,2)}
     * sets scale 2 and {@code ALTER COLUMN x TYPE numeric} (no typmod) removes any previous
     * precision/scale constraint.
     */
    public void alterColumnType(String columnName, DataType newType, Integer precision, Integer scale) {
        int idx = requireColumnIndex(columnName);
        // Historical behavior for callers that don't resolve type metadata themselves
        // (e.g. serial->int conversion): carry over the old column's enum type name.
        alterColumnType(columnName, newType, precision, scale, columns.get(idx).getEnumTypeName(), null);
    }

    /**
     * Full-replacement variant: the new type's enum identity and array element type come from the
     * new type declaration (both null when it has none), never carried over from the old column —
     * PG semantics, where ALTER COLUMN TYPE replaces the type spec entirely. Needed so
     * {@code ALTER COLUMN x TYPE some_enum[]} yields a column distinguishable from a scalar
     * {@code some_enum} column (see PgWireValueFormatter.columnTypeOid).
     */
    public void alterColumnType(String columnName, DataType newType, Integer precision, Integer scale,
                                String enumTypeName, DataType arrayElementType) {
        int idx = requireColumnIndex(columnName);
        Column old = columns.get(idx);
        columns.set(idx, old.withType(newType, precision, scale, enumTypeName, arrayElementType));
        columnsChanged();
    }

    public void alterColumnDefault(String columnName, String defaultValue) {
        int idx = requireColumnIndex(columnName);
        columns.set(idx, columns.get(idx).withDefault(defaultValue));
        columnsChanged();
    }

    public void alterColumnNullable(String columnName, boolean nullable) {
        int idx = requireColumnIndex(columnName);
        columns.set(idx, columns.get(idx).withNullable(nullable));
        // The constraint goes with the flag, so a NOT NULL declared here afterwards is this
        // relation's own rather than one it was still holding on a parent's behalf.
        if (nullable) {
            inheritedNotNullColumns.remove(columnName.toLowerCase());
            notValidatedNotNullColumns.remove(columnName.toLowerCase());
        }
        columnsChanged();
    }

    /** The line pointer the next tuple written into this relation takes. */
    public long nextTupleId() {
        return tupleIdCounter.incrementAndGet();
    }

    /** How many line pointers this relation has handed out. */
    public long getTupleIdCounter() {
        return tupleIdCounter.get();
    }

    /** What this relation's rows answer for xmin, xmax, cmin, cmax and ctid. */
    public Map<Object[], long[]> getRowMeta() {
        return rowMeta;
    }

    /**
     * Number the relation's tuples from {@code value} again. TRUNCATE gives the relation a new
     * file, so the row written after it lives at the first line pointer once more; a TRUNCATE
     * that is rolled back leaves the old file in place, and with it the numbering it had reached.
     */
    public void resetTupleIdCounter(long value) {
        tupleIdCounter.set(value);
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
     * @deprecated Use {@link #updateRowInPlace(Object[], Object[], Object[])} instead for atomic index+data update.
     */
    public void beforeRowUpdate(Object[] row, Object[] oldValues) {
        for (TableIndex idx : indexes.values()) {
            idx.removeByOldValues(oldValues, row);
        }
    }

    /**
     * Notify indexes that a row's values have changed (UPDATE).
     * Must be called AFTER the in-place arraycopy with new values.
     * @deprecated Use {@link #updateRowInPlace(Object[], Object[], Object[])} instead for atomic index+data update.
     */
    public void afterRowUpdate(Object[] row) {
        for (TableIndex idx : indexes.values()) {
            idx.put(row);
        }
    }

    /**
     * Atomically update a row's data in-place under writeLock: remove old index entries,
     * copy new values into the row, then add new index entries.
     * This ensures concurrent readers never see partially-updated row data and
     * all index mutations are serialized.
     */
    public void updateRowInPlace(Object[] row, Object[] oldValues, Object[] newValues) {
        writeLock.lock();
        try {
            System.arraycopy(newValues, 0, row, 0, row.length);
            for (TableIndex idx : indexes.values()) {
                idx.moveEntry(row, oldValues);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** Remove a single row from the table and its indexes. */
    public void removeRow(Object[] row) {
        writeLock.lock();
        try {
            for (TableIndex idx : indexes.values()) {
                idx.remove(row);
            }
            List<Object[]> current = rows;
            List<Object[]> newRows = new ArrayList<>(current.size());
            for (Object[] r : current) {
                if (r != row) newRows.add(r);
            }
            publish(newRows);
        } finally {
            writeLock.unlock();
        }
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
     * Acquires writeLock to ensure the index captures a consistent snapshot of rows
     * and is registered atomically — preventing concurrent INSERTs from being missed.
     */
    public void buildIndex(TableIndex index) {
        writeLock.lock();
        try {
            for (Object[] row : rows) {
                index.put(row);
            }
            indexes.put(index.getConstraintName(), index);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Rebuild all indexes (used after column add/remove or snapshot restore).
     * Acquires writeLock to serialize with concurrent DML index mutations.
     * ReentrantLock handles re-entrance when called from addColumn/addColumnAt
     * which already hold the lock.
     */
    void rebuildAllIndexes() {
        writeLock.lock();
        try {
            for (TableIndex idx : indexes.values()) {
                idx.clear();
                for (Object[] row : rows) {
                    idx.put(row);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Validates existing rows against a PK/UNIQUE constraint that is about to be added
     * (ALTER TABLE ADD CONSTRAINT). PostgreSQL validates the data while building the
     * backing unique index; memgres' {@link TableIndex#put} silently overwrites duplicate
     * keys, so without this check duplicates would persist under the new constraint.
     * <p>
     * Rows of partitions/children are included (row storage of a partitioned table lives on
     * the leaves). Expression-based and partial (WHERE) constraints are skipped — their keys
     * cannot be computed here without an evaluator.
     *
     * @throws MemgresException 23502 if a PRIMARY KEY column contains NULLs,
     *                          23505 if the key columns contain duplicate values
     */
    public void validateNewUniqueConstraint(StoredConstraint sc) {
        if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) return;
        if (sc.getExpressionColumns() != null && !sc.getExpressionColumns().isEmpty()) return;
        if (sc.getWhereExpr() != null) return;
        if (sc.getColumns().isEmpty()) return;
        for (String col : sc.getColumns()) {
            if (getColumnIndex(col) < 0) {
                throw new MemgresException("column \"" + col + "\" named in key does not exist", "42703");
            }
        }
        int[] colIndices = resolveColumnIndices(sc.getColumns());
        if (colIndices == null) return;
        List<Object[]> allRows = getAllRows();
        if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) {
            // PG: ADD PRIMARY KEY implies NOT NULL; existing NULLs abort with 23502
            for (Object[] row : allRows) {
                for (int ci = 0; ci < colIndices.length; ci++) {
                    if (row[colIndices[ci]] == null) {
                        throw PgErrors.columnContainsNulls(sc.getColumns().get(ci), "relation", name);
                    }
                }
            }
        }
        TableIndex probe = new TableIndex(sc.getName(), colIndices, true);
        java.util.Set<TableIndex.IndexKey> seen = new java.util.HashSet<>();
        for (Object[] row : allRows) {
            TableIndex.IndexKey key = probe.extractKey(row);
            if (!sc.isNullsNotDistinct()) {
                boolean hasNull = false;
                for (Object v : key.values) {
                    if (v == null) { hasNull = true; break; }
                }
                if (hasNull) continue; // NULLs are distinct: never conflict
            }
            if (!seen.add(key)) {
                Object[] keyValues = new Object[colIndices.length];
                for (int ci = 0; ci < colIndices.length; ci++) {
                    keyValues[ci] = row[colIndices[ci]];
                }
                MemgresException dup = new MemgresException("could not create unique index \""
                        + sc.getName() + "\"", "23505");
                dup.setConstraint(sc.getName());
                dup.setDetail(IndexKeyDescription.duplicated(this, sc.getColumns(), keyValues));
                throw dup;
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

    /** Give this column's NOT NULL constraint a name other than the default one. */
    public void setNotNullConstraintName(String column, String name) {
        if (column == null) return;
        if (name == null) notNullConstraintNames.remove(column.toLowerCase());
        else notNullConstraintNames.put(column.toLowerCase(), name);
    }

    /**
     * The name this column's NOT NULL constraint answers to. Returns the default
     * {@code <table>_<column>_not_null} when the writer never named it, and null when the column
     * has no NOT NULL constraint at all.
     */
    public String notNullConstraintName(String column) {
        if (column == null) return null;
        int idx = getColumnIndex(column);
        if (idx < 0 || columns.get(idx).isNullable()) return null;
        String explicit = notNullConstraintNames.get(column.toLowerCase());
        return explicit != null ? explicit : defaultNotNullConstraintName(column);
    }

    /** The name PostgreSQL gives a NOT NULL constraint nobody named. */
    public String defaultNotNullConstraintName(String column) {
        return name + "_" + column + "_not_null";
    }

    /** The column a NOT NULL constraint of this name covers, or null when there is none. */
    public String notNullConstraintColumn(String constraintName) {
        if (constraintName == null) return null;
        for (Column c : columns) {
            if (c.isNullable()) continue;
            String own = notNullConstraintName(c.getName());
            if (own != null && own.equalsIgnoreCase(constraintName)) return c.getName();
        }
        return null;
    }

    /**
     * A column rename leaves the constraint's name alone in PostgreSQL, so a default name has to
     * be written down before the column it was derived from changes underneath it.
     */
    public void pinNotNullConstraintName(String column) {
        String current = notNullConstraintName(column);
        if (current != null) notNullConstraintNames.put(column.toLowerCase(), current);
    }

    /** Carry a column's NOT NULL constraint name across a rename of that column. */
    public void moveNotNullConstraintName(String oldColumn, String newColumn) {
        if (oldColumn == null || newColumn == null) return;
        String pinned = notNullConstraintNames.remove(oldColumn.toLowerCase());
        if (pinned != null) notNullConstraintNames.put(newColumn.toLowerCase(), pinned);
    }
    public StoredConstraint getConstraint(String name) {
        for (StoredConstraint c : constraints) {
            if (c.getName() != null && c.getName().equalsIgnoreCase(name)) return c;
        }
        return null;
    }

    // Unlogged
    public boolean isUnlogged() { return unlogged; }
    public void setUnlogged(boolean unlogged) { this.unlogged = unlogged; }

    // FROM-function (SRF) result provenance
    /**
     * Whether this relation was built from a query rather than declared.
     *
     * <p>A derived table, a CTE, a view's projection, a VALUES list and a FROM-function are all
     * built here as a Table whose columns carry whatever type the builder could read off the
     * values it produced, which is not always the type the expression behind them has. A check
     * that refuses a statement on the strength of a column's type may not trust one of those, so
     * it has to be able to tell the two apart — and where the definition <em>does</em> settle a
     * type, {@link #definedColumnType} is the one to read instead of the column's own.
     */
    public boolean hasDefinedColumnTypes() { return definedColumnTypes != null; }

    /**
     * The type this column certainly has, worked out from the definition the relation was built
     * from, as PostgreSQL spells the type; null where the definition does not settle it.
     */
    public String definedColumnType(int columnIndex) {
        if (definedColumnTypes == null || columnIndex < 0
                || columnIndex >= definedColumnTypes.length) {
            return null;
        }
        return definedColumnTypes[columnIndex];
    }

    /**
     * Records this as a relation built from a query, along with the types its definition settles.
     * One entry per column, null where the definition settles nothing.
     */
    public void setDefinedColumnTypes(String[] types) { this.definedColumnTypes = types; }

    private String[] definedColumnTypes;

    /**
     * The relation this one stands in front of, when a FROM item's alias list renamed its columns;
     * null for every other relation.
     *
     * <p>PostgreSQL renames the references to a relation's columns and not the expressions stored
     * with them, so a VIRTUAL generated column reached through such a list still carries a
     * generation expression written in the names the relation underneath answers to. The rows are
     * that relation's own and its columns sit in the same places, so the expression is worked out
     * against it while the query above writes the new names.
     */
    public Table getColumnsRenamedFrom() { return columnsRenamedFrom; }

    public void setColumnsRenamedFrom(Table renamed) { this.columnsRenamedFrom = renamed; }

    private Table columnsRenamedFrom;

    public boolean isFunctionResult() { return functionResult; }
    public void setFunctionResult(boolean functionResult) { this.functionResult = functionResult; }

    public boolean isViewProjection() { return viewProjection; }
    public void setViewProjection(boolean viewProjection) { this.viewProjection = viewProjection; }

    /**
     * The composite type this table was declared OF, schema-qualified, or null.
     *
     * <p>A typed table's shape is the type's, not its own: PostgreSQL refuses to add a column to
     * one, and refuses to drop the type while the table stands. Both need the table to remember
     * which type it came from, because the columns alone no longer say.
     */
    public String getOfTypeName() { return ofTypeName; }
    public void setOfTypeName(String ofTypeName) { this.ofTypeName = ofTypeName; }

    private String ofTypeName;
    public Map<String, String> getReloptions() { return reloptions; }
    public void setReloptions(Map<String, String> reloptions) { this.reloptions = reloptions; }

    // Inheritance
    /** The first parent, which is the only one for all but a multiply-inheriting table. */
    public Table getParentTable() { return inheritParents.isEmpty() ? null : inheritParents.get(0); }

    /** Records another inheritance parent; one already linked is not linked a second time. */
    public void setParentTable(Table parent) {
        if (parent == null) return;
        for (Table existing : inheritParents) {
            if (existing == parent) return;
        }
        inheritParents.add(parent);
    }

    /**
     * Breaks one inheritance link and leaves the rest standing. NO INHERIT names a single parent,
     * and a child declared under two of them goes on inheriting from the other -- emptying the
     * whole list took the remaining parent's columns and constraints away from the child as well,
     * and left pg_inherits saying it inherits from nobody.
     */
    public void removeParentTable(Table parent) {
        if (parent == null) return;
        for (int i = 0; i < inheritParents.size(); i++) {
            if (inheritParents.get(i) == parent) {
                inheritParents.remove(i);
                return;
            }
        }
    }

    /** Every table this one inherits from, in the order the declaration named them. */
    public List<Table> getInheritParents() { return inheritParents; }

    /**
     * The relations this one takes its columns from: the partitioned table it is a partition of,
     * or the tables it was declared to inherit. PostgreSQL counts them per column in
     * pg_attribute.attinhcount and calls a column local only when none of them declares it.
     */
    public List<Table> getDirectParents() {
        if (partitionParent == null) return inheritParents;
        List<Table> out = new ArrayList<>(inheritParents.size() + 1);
        out.add(partitionParent);
        out.addAll(inheritParents);
        return out;
    }

    /**
     * Records that this relation holds the column only because a parent declares it, which is what
     * PostgreSQL reports as attislocal false. A column arrives that way from INHERITS and from a
     * column added to a parent afterwards, never from the relation's own column list.
     */
    public void markColumnInherited(String column) {
        if (column != null) inheritedColumns.add(column.toLowerCase());
    }

    /** True when this relation's own definition named the column. */
    public boolean isColumnLocal(String column) {
        return column != null && !inheritedColumns.contains(column.toLowerCase());
    }

    /**
     * Records that the column is this relation's own from now on. A parent that drops a column
     * from itself alone leaves every child holding it, and PostgreSQL then records it as a column
     * each of them declared: nobody hands it down any longer, so there is nobody left to hold it
     * on behalf of.
     */
    public void markColumnLocal(String column) {
        if (column != null) inheritedColumns.remove(column.toLowerCase());
    }

    /**
     * Records that the column's NOT NULL is a parent's rule rather than this relation's own. The
     * constraint then answers to the name the parent gave it, and only the parent may withdraw it.
     */
    public void markNotNullInherited(String column) {
        if (column != null) inheritedNotNullColumns.add(column.toLowerCase());
    }

    /** True when this relation's own definition declared the column NOT NULL. */
    public boolean isNotNullLocal(String column) {
        return column != null && !inheritedNotNullColumns.contains(column.toLowerCase());
    }

    /**
     * Records that the column's NOT NULL was declared NO INHERIT, which is a rule about this
     * relation's rows alone. PostgreSQL hands nothing of it down: a child takes the column with no
     * NOT NULL on it at all, and one that declares NOT NULL for itself holds a rule it owns
     * outright rather than one it shares with the relation above.
     */
    public void markNotNullNoInherit(String column) {
        if (column != null) noInheritNotNullColumns.add(column.toLowerCase());
    }

    /** True when this relation's NOT NULL on the column stops here rather than being handed down. */
    public boolean isNotNullNoInherit(String column) {
        return column != null && noInheritNotNullColumns.contains(column.toLowerCase());
    }

    /**
     * Records that the column's NOT NULL was declared NOT VALID: the rows already stored were not
     * read, so PostgreSQL leaves the constraint standing over every write from now on and waits
     * for VALIDATE CONSTRAINT to go back over what is there.
     */
    public void markNotNullNotValidated(String column) {
        if (column != null) notValidatedNotNullColumns.add(column.toLowerCase());
    }

    /** Records that the rows already stored have been read and none of them holds a null. */
    public void markNotNullValidated(String column) {
        if (column != null) notValidatedNotNullColumns.remove(column.toLowerCase());
    }

    /** Whether the rows already stored have been held to the column's NOT NULL. */
    public boolean isNotNullValidated(String column) {
        return column == null || !notValidatedNotNullColumns.contains(column.toLowerCase());
    }

    /**
     * Records that the column's NOT NULL is this relation's own from now on. A relation that
     * leaves the parent whose rule it was holds it on nobody's behalf, which is what lets it
     * withdraw the rule; PostgreSQL leaves it the relation's own even if that parent takes the
     * relation back afterwards.
     */
    public void markNotNullLocal(String column) {
        if (column != null) inheritedNotNullColumns.remove(column.toLowerCase());
    }

    /**
     * Writes down the name this relation's inherited NOT NULL answers to.
     *
     * <p>PostgreSQL names a constraint once, when it is created, and the name of a rule a relation
     * took from a parent is the name that parent gave it -- which stops being derivable the moment
     * the links change underneath it. A relation declared under two parents keeps the first one's
     * name after it leaves that parent, and one whose parent has dropped the column keeps it with
     * nothing above it to read it from. Only that name is kept here; a rule the relation declared
     * for itself answers to {@link #notNullConstraintName} as it always did.
     */
    public void pinInheritedNotNullName(String column, String name) {
        if (column == null) return;
        if (name == null) inheritedNotNullNames.remove(column.toLowerCase());
        else inheritedNotNullNames.put(column.toLowerCase(), name);
    }

    /** The name written down for an inherited NOT NULL on this column, or null when none was. */
    public String inheritedNotNullName(String column) {
        return column == null ? null : inheritedNotNullNames.get(column.toLowerCase());
    }

    /**
     * How many parents handed this column's NOT NULL down and have since let the column go without
     * withdrawing the rule.
     *
     * <p>PostgreSQL counts the parents a constraint was taken from once and never counts them
     * again: a parent's copy dropped along with the column it tests tells the descendants nothing,
     * so the count stands where it stood. The links say nothing about those parents any more, so
     * what they contributed is kept here and added to what the links still say.
     */
    public int retainedNotNullInheritCount(String column) {
        if (column == null) return 0;
        Integer held = retainedNotNullInheritCounts.get(column.toLowerCase());
        return held == null ? 0 : held.intValue();
    }

    public void setRetainedNotNullInheritCount(String column, int count) {
        if (column == null) return;
        if (count <= 0) retainedNotNullInheritCounts.remove(column.toLowerCase());
        else retainedNotNullInheritCounts.put(column.toLowerCase(), Integer.valueOf(count));
    }

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
    /**
     * Maps this partition's column positions to the parent's, when ATTACH bound a table
     * whose columns are in a different order. PostgreSQL matches partition columns by
     * name and keeps each table's own attribute order, so rows have to be permuted as
     * they cross the boundary in either direction. Null when the orders already agree.
     */
    private int[] parentColumnRemap;

    public int[] getParentColumnRemap() { return parentColumnRemap; }

    public void setParentColumnRemap(int[] remap) { this.parentColumnRemap = remap; }

    /** Re-orders a row written through the parent into this partition's own column order. */
    public Object[] rowFromParent(Object[] parentRow) {
        if (parentColumnRemap == null) {
            // The orders agree, so the row passes straight through — but a partition that ended
            // up with a different number of columns from its parent would otherwise hand a short
            // row to code indexing by its own columns, which reads past the end of it.
            if (parentRow != null && parentRow.length != columns.size()) {
                return java.util.Arrays.copyOf(parentRow, columns.size());
            }
            return parentRow;
        }
        Object[] out = new Object[columns.size()];
        for (int i = 0; i < parentColumnRemap.length && i < parentRow.length; i++) {
            int target = parentColumnRemap[i];
            if (target >= 0 && target < out.length) out[target] = parentRow[i];
        }
        return out;
    }

    /** Re-orders one of this partition's rows into the parent's column order. */
    public Object[] rowToParent(Object[] childRow) {
        if (parentColumnRemap == null) return childRow;
        Object[] out = new Object[parentColumnRemap.length];
        for (int i = 0; i < parentColumnRemap.length; i++) {
            int source = parentColumnRemap[i];
            if (source >= 0 && source < childRow.length) out[i] = childRow[source];
        }
        return out;
    }

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

    /** Clear all partition bound metadata (used when a partition is detached from its parent). */
    public void clearPartitionBounds() {
        this.partitionLower = null;
        this.partitionUpper = null;
        this.partitionValues = null;
        this.partitionModulus = null;
        this.partitionRemainder = null;
        this.defaultPartition = false;
    }

    // Row-level security
    public boolean isRlsEnabled() { return rlsEnabled; }
    public void setRlsEnabled(boolean enabled) { this.rlsEnabled = enabled; }
    public boolean isRlsForced() { return rlsForced; }
    public void setRlsForced(boolean forced) { this.rlsForced = forced; }
    public List<RlsPolicy> getRlsPolicies() { return rlsPolicies; }
    public void addRlsPolicy(RlsPolicy policy) { rlsPolicies.add(policy); }

    // Replica identity
    public char getReplicaIdentity() { return replicaIdentity; }
    public void setReplicaIdentity(char identity) { this.replicaIdentity = identity; }

    /**
     * Whether this table has a usable replica identity for UPDATE/DELETE in
     * logical replication.  PG considers DEFAULT ('d') usable only when the
     * table actually has a primary key; FULL ('f') is always usable; NOTHING
     * ('n') is never usable; INDEX ('i') is usable.
     */
    public boolean hasUsableReplicaIdentity() {
        switch (replicaIdentity) {
            case 'f': // FULL — always usable
            case 'i': // USING INDEX — usable
                return true;
            case 'd': // DEFAULT — usable only if PK exists
                for (StoredConstraint c : constraints) {
                    if (c.getType() == StoredConstraint.Type.PRIMARY_KEY) return true;
                }
                return false;
            default:  // 'n' (NOTHING) or unknown
                return false;
        }
    }

    // DML statistics
    public long getTupInserted() { return tupInserted.get(); }
    public long getTupUpdated() { return tupUpdated.get(); }
    public long getTupDeleted() { return tupDeleted.get(); }
    public void incrementTupInserted(long count) { tupInserted.addAndGet(count); }
    public void incrementTupUpdated(long count) { tupUpdated.addAndGet(count); }
    public void incrementTupDeleted(long count) { tupDeleted.addAndGet(count); }
    public long getIdxScanCount() { return idxScanCount.get(); }
    public void incrementIdxScanCount() { idxScanCount.incrementAndGet(); }

    // Maintenance timestamps
    public java.time.OffsetDateTime getLastVacuum() { return lastVacuum; }
    public void setLastVacuum(java.time.OffsetDateTime ts) { this.lastVacuum = ts; }
    public java.time.OffsetDateTime getLastAnalyze() { return lastAnalyze; }
    public void setLastAnalyze(java.time.OffsetDateTime ts) { this.lastAnalyze = ts; }
}
