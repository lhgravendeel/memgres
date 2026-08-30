package com.memgres.engine;

import com.memgres.engine.parser.ast.CreateTableStmt;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * How many command identifiers a statement spends on the catalogue.
 *
 * <p>PostgreSQL hands the rest of a statement a fresh command identifier whenever it has to make
 * what it has just written visible to the work that follows: the relation before the routine
 * filling it opens it, the column defaults before the constraints checked against them, each
 * index before the next one is built. A stretch of writes between two of those points costs one
 * identifier however many catalogue rows it wrote — which is why three CHECK constraints on one
 * definition cost what one costs — and a stretch that wrote nothing costs none, which is why a
 * statement that only reads leaves the counter where it stands.
 *
 * <p>Retiring a relation is the other way about: its catalogue rows are found and deleted one at a
 * time, so there each row is an identifier of its own.
 *
 * <p>cmin reports the counter as it stood when a row version was written, so a transaction that
 * spends fewer of them than PostgreSQL does reads a lower number from that point on for every row
 * it goes on to write.
 */
final class CommandIdCost {

    private CommandIdCost() {
    }

    /** How wide a row may be before PostgreSQL starts moving parts of it out of line. */
    private static final int TOAST_TUPLE_THRESHOLD = 2032;

    /** A heap tuple's header, before the bitmap saying which of its columns are null. */
    private static final int TUPLE_HEADER_SIZE = 23;

    /** The most bytes one character of the server's encoding takes. */
    private static final int BYTES_PER_CHARACTER = 4;

    /**
     * Whether PostgreSQL builds a TOAST table for a relation of these columns.
     *
     * <p>It builds one when the relation holds at least one column whose value it is allowed to
     * move out of line and the widest row those columns could make would not fit in a quarter of a
     * page. A type nothing bounds — text, or a numeric written without a precision — settles it on
     * its own; a bounded one contributes the widest value it could hold, which is why varchar(10)
     * leaves a row that always fits and varchar(502) does not.
     */
    static boolean needsToastTable(List<Column> columns) {
        if (columns == null || columns.isEmpty()) return false;
        int dataLength = 0;
        boolean widthUnbounded = false;
        boolean anyMovable = false;
        for (Column column : columns) {
            // A virtual generated column is worked out on the way out and never stored, so it
            // takes no room in the row it belongs to.
            if (column.isVirtual()) continue;
            if (column.getArrayElementType() != null) {
                // Every array is a value of no fixed width that PostgreSQL is willing to move.
                dataLength = alignTo(dataLength, 4);
                widthUnbounded = true;
                anyMovable = true;
                continue;
            }
            dataLength = alignTo(dataLength, alignmentOf(column.getType()));
            int width = CatalogCoreBuilder.typeLength(column.getType());
            if (width > 0) {
                dataLength += width;
                continue;
            }
            int widest = widestValueOf(column);
            if (widest < 0) widthUnbounded = true;
            else dataLength += widest;
            if (!"p".equals(storageOf(column))) anyMovable = true;
        }
        if (!anyMovable) return false;
        if (widthUnbounded) return true;
        int rowLength = maxAlign(TUPLE_HEADER_SIZE + (columns.size() + 7) / 8)
                + maxAlign(dataLength);
        return rowLength > TOAST_TUPLE_THRESHOLD;
    }

    /**
     * The identifiers CREATE TABLE spends on the relation it has just defined.
     *
     * @param temporarySchemaMadeWithIt whether this is the temporary object that brought the
     *                                  session's own schema into being, which PostgreSQL writes
     *                                  as it writes any other schema
     */
    static int forCreatedTable(Database database, CreateTableStmt stmt, Table table,
                               String schemaName, boolean temporarySchemaMadeWithIt) {
        int cost = 1;
        if (temporarySchemaMadeWithIt) cost += 2;
        // A partitioned table holds no rows of its own, so there is never anything of it to move
        // out of line and PostgreSQL gives it no TOAST table.
        if (stmt.partitionBy() == null && needsToastTable(table.getColumns())) cost += 4;
        boolean anyDefault = false;
        boolean anyNotNullOrCheck = false;
        for (Column column : table.getColumns()) {
            // The sequence behind a serial or an identity column is a relation of its own, and
            // saying which column owns it is written after it.
            if (sequenceBehind(column)) cost += 3;
            if (writesDefault(column)) anyDefault = true;
            if (!column.isNullable()) anyNotNullOrCheck = true;
        }
        int foreignKeys = 0;
        Set<String> referenced = new HashSet<String>();
        for (StoredConstraint constraint : table.getConstraints()) {
            if (constraint.getType() == StoredConstraint.Type.CHECK) {
                anyNotNullOrCheck = true;
            } else if (constraint.getType() == StoredConstraint.Type.FOREIGN_KEY) {
                foreignKeys++;
                String other = referencedRelation(constraint, schemaName);
                if (other != null && !other.equals(relationKey(schemaName, table.getName()))) {
                    referenced.add(other);
                }
            } else if (constraint.getType() == StoredConstraint.Type.PRIMARY_KEY
                    || constraint.getType() == StoredConstraint.Type.UNIQUE
                    || constraint.getType() == StoredConstraint.Type.EXCLUDE) {
                // Each of these builds an index and then files the constraint that owns it.
                cost += 2;
            }
        }
        // What the table takes from its parents is written where its own constraints are.
        if (stmt.inherits() != null && !stmt.inherits().isEmpty()) anyNotNullOrCheck = true;
        // The defaults are written together and the constraints checked against them together,
        // so a definition carrying several of either spends what one of them spends.
        if (anyDefault) cost++;
        if (anyNotNullOrCheck) cost++;
        if (foreignKeys > 0) {
            // Each key writes its row and the triggers that enforce it, and a relation being
            // pointed at for the first time is marked as carrying triggers -- which is a write of
            // its own, and one that a relation already at the far end of a key, or already
            // carrying a trigger, does not need.
            cost += 1 + 4 * foreignKeys;
            for (String other : referenced) {
                if (!alreadyBearsTriggers(database, table, other)) cost++;
            }
        }
        if (stmt.partitionBy() != null) cost++;
        return cost;
    }

    /**
     * Whether PostgreSQL already records this relation as carrying triggers, which is what a
     * foreign key newly pointing at it would otherwise have to write.
     */
    private static boolean alreadyBearsTriggers(Database database, Table creating,
                                                String relation) {
        String bare = relation.substring(relation.indexOf('.') + 1);
        if (database.getAllTriggers().containsKey(bare)) return true;
        for (Map.Entry<String, Schema> holder : database.getSchemas().entrySet()) {
            for (Table other : holder.getValue().getTables().values()) {
                if (other == creating) continue;
                for (StoredConstraint constraint : other.getConstraints()) {
                    if (constraint.getType() != StoredConstraint.Type.FOREIGN_KEY) continue;
                    String points = referencedRelation(constraint, holder.getKey());
                    if (relation.equals(points)) return true;
                }
            }
        }
        return false;
    }

    /**
     * The identifiers CREATE TABLE ... PARTITION OF spends.
     *
     * <p>A partition declares nothing of its own: its columns, its checks and its indexes are the
     * partitioned table's, written again against the relation that holds the rows. The index a
     * partition receives belongs to a constraint of the partitioned table rather than to one of
     * its own, so it costs what a bare index costs and not what a key constraint costs.
     */
    static int forCreatedPartition(Database database, String schemaName, Table partition) {
        int cost = 3;
        if (partition.getPartitionStrategy() == null
                && needsToastTable(partition.getColumns())) {
            cost += 4;
        }
        boolean anyDefault = false;
        boolean anyNotNullOrCheck = false;
        for (Column column : partition.getColumns()) {
            if (writesDefault(column)) anyDefault = true;
            if (!column.isNullable()) anyNotNullOrCheck = true;
        }
        for (StoredConstraint constraint : partition.getConstraints()) {
            if (constraint.getType() == StoredConstraint.Type.CHECK) anyNotNullOrCheck = true;
        }
        if (anyDefault) cost++;
        if (anyNotNullOrCheck) cost++;
        return cost + indexesOf(database, schemaName, partition);
    }

    /**
     * The identifiers CREATE TABLE ... AS spends: the relation, the TOAST table its columns may
     * need, and one more for the rows when the query's answer is kept.
     */
    static int forRelationBuiltByQuery(List<Column> columns, boolean withData) {
        int cost = 1 + toastTableCost(columns);
        return withData ? cost + 1 : cost;
    }

    /**
     * How far past the relation's own catalogue rows the counter has moved by the time a query
     * that builds a relation writes the rows into it.
     */
    static int rowsOfRelationBuiltByQuery(List<Column> columns) {
        return 1 + toastTableCost(columns);
    }

    /** What building a TOAST table costs a statement that defines a relation. */
    private static int toastTableCost(List<Column> columns) {
        return needsToastTable(columns) ? 4 : 0;
    }

    /**
     * The identifiers retiring one relation spends.
     *
     * <p>Three for the relation itself — it, the composite type of its name and that type's array
     * type — and then one for every catalogue row that described it: each index, each constraint,
     * each column default and the sequence a serial or identity column owns. A foreign key costs
     * more than the others because the triggers that enforced it go with it.
     */
    static int forDroppedRelation(Database database, String schemaName, Table table) {
        int cost = 3;
        if (table.getPartitionStrategy() == null && needsToastTable(table.getColumns())) cost += 2;
        for (Column column : table.getColumns()) {
            if (!column.isNullable()) cost++;
            if (writesDefault(column)) cost++;
            if (sequenceBehind(column)) cost++;
        }
        for (StoredConstraint constraint : table.getConstraints()) {
            cost += constraint.getType() == StoredConstraint.Type.FOREIGN_KEY ? 5 : 1;
        }
        return cost + indexesOf(database, schemaName, table);
    }

    /**
     * The identifiers emptying one relation spends. TRUNCATE gives the relation storage it has
     * never held a row in, and the TOAST table behind it and every index on it are built again
     * beside it.
     */
    static int forTruncatedRelation(Database database, String schemaName, Table table) {
        int cost = 1;
        if (table.getPartitionStrategy() == null && needsToastTable(table.getColumns())) cost += 3;
        return cost + 2 * indexesOf(database, schemaName, table);
    }

    /** How many sequences this relation's columns draw from, which RESTART IDENTITY rewrites. */
    static int sequencesBehind(Table table) {
        int found = 0;
        for (Column column : table.getColumns()) {
            if (sequenceBehind(column)) found++;
        }
        return found;
    }

    /**
     * The identifiers CREATE MATERIALIZED VIEW spends: the relation and the rule that remembers
     * its query, the TOAST table its columns may need, and, when it is created with its data, the
     * refresh that fills it.
     */
    static int forCreatedMaterializedView(List<Column> columns, boolean withData) {
        int cost = 2 + toastTableCost(columns);
        return withData ? cost + forRefreshedMaterializedView(columns) : cost;
    }

    /**
     * The identifiers REFRESH MATERIALIZED VIEW spends. PostgreSQL fills a relation of its own and
     * then swaps it into place, so everything behind the view is built a second time.
     */
    static int forRefreshedMaterializedView(List<Column> columns) {
        return needsToastTable(columns) ? 14 : 6;
    }

    /**
     * The identifiers retiring a view spends: the relation, the composite type of its name, that
     * type's array type and the rule that answered from it. A materialized view has storage as
     * well, so what was moved out of line goes with it.
     */
    static int forDroppedView(List<Column> columns, boolean materialized) {
        int cost = 4;
        if (materialized && needsToastTable(columns)) cost += 2;
        return cost;
    }

    /** Whether this column is fed by a sequence of its own: a serial, or an identity. */
    private static boolean sequenceBehind(Column column) {
        if (isIdentity(column)) return true;
        DataType type = column.getType();
        return type == DataType.SERIAL || type == DataType.BIGSERIAL
                || type == DataType.SMALLSERIAL;
    }

    /** An identity column draws from its sequence without a stored default standing for it. */
    private static boolean isIdentity(Column column) {
        String value = column.getDefaultValue();
        return value != null && value.startsWith("__identity__:");
    }

    /** Whether the column has a stored expression behind it, a catalogue row of its own. */
    private static boolean writesDefault(Column column) {
        if (isIdentity(column)) return false;
        return column.getDefaultValue() != null || column.isGenerated();
    }

    /** How many indexes stand on this relation: those a key constraint owns and those declared. */
    private static int indexesOf(Database database, String schemaName, Table table) {
        int indexes = 0;
        for (StoredConstraint constraint : table.getConstraints()) {
            if (constraint.getType() == StoredConstraint.Type.PRIMARY_KEY
                    || constraint.getType() == StoredConstraint.Type.UNIQUE) {
                indexes++;
            }
        }
        return indexes + declaredIndexesOf(database, schemaName, table);
    }

    /**
     * The indexes registered against this relation. The index behind an EXCLUDE constraint is one
     * of them, which is why that constraint does not count another.
     */
    private static int declaredIndexesOf(Database database, String schemaName, Table table) {
        String owner = relationKey(schemaName, table.getName());
        int found = 0;
        for (Map.Entry<String, String> entry : database.getIndexTableNames().entrySet()) {
            String on = entry.getValue();
            if (on == null) continue;
            int dot = on.indexOf('.');
            String key = dot > 0 ? relationKey(on.substring(0, dot), on.substring(dot + 1))
                    : relationKey(null, on);
            if (owner.equals(key)) found++;
        }
        return found;
    }

    /** How one relation is named when two of them are compared. */
    private static String relationKey(String schemaName, String name) {
        String schema = schemaName == null || schemaName.isEmpty() ? "public" : schemaName;
        return schema.toLowerCase(java.util.Locale.ROOT) + "." + (name == null ? "" : name.toLowerCase(java.util.Locale.ROOT));
    }

    /** The relation a foreign key points at, as the same kind of name. */
    private static String referencedRelation(StoredConstraint constraint, String schemaName) {
        String table = constraint.getReferencesTable();
        if (table == null) return null;
        String schema = constraint.getReferencesSchema() != null
                ? constraint.getReferencesSchema() : schemaName;
        return relationKey(schema, table);
    }

    /**
     * The widest a stored value of this column can be, or -1 when nothing bounds it.
     *
     * <p>A character length is counted in characters, so the widest value is that many of the
     * widest character the encoding has plus the header every stored value of unbounded width
     * carries. A numeric's precision says how many base-ten-thousand digits it takes, and a bit
     * string's length how many bytes.
     */
    private static int widestValueOf(Column column) {
        Integer written = column.getPrecision();
        switch (column.getType()) {
            case VARCHAR:
                return written == null ? -1 : written * BYTES_PER_CHARACTER + 4;
            case CHAR:
                return (written == null ? 1 : written) * BYTES_PER_CHARACTER + 4;
            case NUMERIC:
                return written == null ? -1 : 8 + ((written + 6) / 4) * 2;
            case BIT:
                return ((written == null ? 1 : written) + 7) / 8 + 8;
            case VARBIT:
                return written == null ? -1 : (written + 7) / 8 + 8;
            default:
                return -1;
        }
    }

    /** Where a value of this column may be kept: p in the row itself, anything else out of line. */
    private static String storageOf(Column column) {
        String written = column.getAttStorageOverride();
        if (written != null && !written.isEmpty()) {
            return written.substring(0, 1).toLowerCase(java.util.Locale.ROOT);
        }
        return CatalogCoreBuilder.typeStorage(column.getType());
    }

    /** What boundary a value of this type has to start on. */
    private static int alignmentOf(DataType type) {
        String align = CatalogCoreBuilder.typeAlign(type);
        if ("d".equals(align)) return 8;
        if ("i".equals(align)) return 4;
        if ("s".equals(align)) return 2;
        return 1;
    }

    private static int alignTo(int offset, int boundary) {
        return (offset + boundary - 1) & ~(boundary - 1);
    }

    private static int maxAlign(int length) {
        return alignTo(length, 8);
    }
}
