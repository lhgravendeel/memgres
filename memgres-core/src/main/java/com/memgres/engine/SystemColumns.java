package com.memgres.engine;

import com.memgres.engine.parser.ast.ColumnRef;

import java.util.List;
import java.util.Locale;

/**
 * The types the six system columns have.
 *
 * <p>A system column is not among the columns a relation declares, so every place that reads a
 * type off a relation found nothing for one and fell back on text or on nothing at all. They are
 * as declared as any other column -- more so, since nobody chose them -- and this says what they
 * are: a tuple identifier is a tid, the two transaction stamps are xids, the two command stamps
 * are cids, and the relation stamp is an oid.
 */
final class SystemColumns {

    private SystemColumns() {
    }

    /** The type of a system column by that name, or null where the name is not one of them. */
    static DataType typeOf(String column) {
        if (column == null) return null;
        switch (column.toLowerCase(Locale.ROOT)) {
            case "ctid":
                return DataType.TID;
            case "xmin":
            case "xmax":
                return DataType.XID;
            case "cmin":
            case "cmax":
                return DataType.CID;
            case "tableoid":
                return DataType.OID;
            default:
                return null;
        }
    }

    /**
     * The type this reference has as a system column, or null where it is not one here.
     *
     * <p>A relation that composes its rows has no system columns, which {@link Table#storesRows}
     * is what says; and a relation that happens to declare a column with one of these names has
     * an ordinary column, which its own declaration types. Only what is left is a system column.
     */
    static DataType resolve(ColumnRef ref, List<RowContext.TableBinding> bindings) {
        if (ref == null || bindings == null) return null;
        DataType type = typeOf(ref.column());
        if (type == null) return null;
        boolean overStoredRows = false;
        for (RowContext.TableBinding b : bindings) {
            if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                    && (b.table() == null
                        || !ref.table().equalsIgnoreCase(b.table().getName()))) continue;
            if (b.table() != null && b.table().getColumnIndex(ref.column()) >= 0) return null;
            if (b.sourceTable() != null && b.sourceTable().storesRows()) overStoredRows = true;
        }
        return overStoredRows ? type : null;
    }
}
