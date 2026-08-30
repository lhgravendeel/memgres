package com.memgres.engine;

import com.memgres.engine.parser.ast.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Composite type operations, extracted from AstExecutor to reduce class size.
 */
class CompositeTypeHandler {
    private final AstExecutor executor;

    CompositeTypeHandler(AstExecutor executor) {
        this.executor = executor;
    }

    String resolveCompositeTypeName(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            String tn = cast.typeName().toLowerCase(java.util.Locale.ROOT).trim();
            if (executor.database.isCompositeType(tn)) return tn;
            // Tables also serve as composite types in PG
            if (executor.database.getTable(tn) != null) return tn;
            return null;
        }
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr innerFa = (FieldAccessExpr) expr;
            // Chained access: ((expr)::type).field1, get the type of field1
            String parentType = resolveCompositeTypeName(innerFa.expr(), ctx);
            if (parentType != null) {
                List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(parentType);
                if (fields != null) {
                    for (CreateTypeStmt.CompositeField f : fields) {
                        if (f.name().equalsIgnoreCase(innerFa.field())) {
                            String ft = f.typeName().toLowerCase(java.util.Locale.ROOT).trim();
                            if (executor.database.isCompositeType(ft)) return ft;
                            return null;
                        }
                    }
                }
            }
            return null;
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            Column col = ctx.resolveColumnDef(ref.table(), ref.column());
            if (col != null && col.getCompositeTypeName() != null) {
                return col.getCompositeTypeName().toLowerCase(java.util.Locale.ROOT);
            }
            return null;
        }
        if (expr instanceof SubscriptExpr) {
            // One element of an array of a composite is a value of that composite: PostgreSQL
            // types a subscript as the array's element type, so (cs[1]).a names a field of the
            // composite rather than a field of something that has none.
            return arrayElementCompositeType(((SubscriptExpr) expr).base(), ctx);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            // populate_record / json_populate_record return the type of their first argument
            String fname = fn.name().toLowerCase(java.util.Locale.ROOT);
            // Both families fill the same record from the same base argument, so both take their
            // shape from it -- reading only the json one left (jsonb_populate_record(...)).*
            // with no columns to expand to and so no row at all.
            if ((fname.equals("populate_record") || fname.equals("json_populate_record")
                    || fname.equals("jsonb_populate_record"))
                    && fn.args() != null && !fn.args().isEmpty()) {
                return resolveCompositeTypeName(fn.args().get(0), ctx);
            }
            PgFunction pgFunc = executor.database.getFunction(fn.name());
            if (pgFunc != null) {
                String rt = pgFunc.getReturnType().toLowerCase(java.util.Locale.ROOT).trim();
                if (executor.database.isCompositeType(rt)) return rt;
            }
            return null;
        }
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            // For (SELECT col FROM table).field, infer composite type from the inner select's column
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null && ((SelectStmt) sq.subquery()).targets().size() == 1) {
                SelectStmt sel = (SelectStmt) sq.subquery();
                Expression targetExpr = sel.targets().get(0).expr();
                // First try with the outer context
                String result = resolveCompositeTypeName(targetExpr, ctx);
                if (result != null) return result;
                // Try resolving column from the subquery's FROM clause tables
                if (targetExpr instanceof ColumnRef && sel.from() != null) {
                    ColumnRef ref = (ColumnRef) targetExpr;
                    for (SelectStmt.FromItem fromItem : sel.from()) {
                        String tableName = fromItem instanceof SelectStmt.TableRef ? ((SelectStmt.TableRef) fromItem).table() : null;
                        if (tableName != null) {
                            Table table = executor.resolveTableSafe(tableName);
                            if (table != null) {
                                int idx = table.getColumnIndex(ref.column());
                                if (idx >= 0) {
                                    Column col = table.getColumns().get(idx);
                                    if (col.getCompositeTypeName() != null) {
                                        return col.getCompositeTypeName().toLowerCase(java.util.Locale.ROOT);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return null;
        }
        return null;
    }

    /**
     * The composite type an expression's array holds elements of, or null when it holds anything
     * else.
     *
     * <p>A column declared as an array of a composite is carried as the text its array prints as
     * and typed text, so the type it answers with says neither that it is an array nor what its
     * elements are -- only the column's own declaration does. PostgreSQL types both {@code cs[1]}
     * and {@code unnest(cs)} as the composite, which is what makes a field of one reachable.
     */
    String arrayElementCompositeType(Expression expr, RowContext ctx) {
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            Column col = ctx.resolveColumnDef(ref.table(), ref.column());
            if (col != null && col.getArrayElementType() != null
                    && col.getCompositeTypeName() != null) {
                return col.getCompositeTypeName().toLowerCase(java.util.Locale.ROOT);
            }
            return null;
        }
        if (expr instanceof CastExpr) {
            String written = ((CastExpr) expr).typeName().toLowerCase(java.util.Locale.ROOT).trim();
            if (!written.endsWith("[]")) return null;
            String element = written.substring(0, written.length() - 2).trim();
            return executor.database.isCompositeType(element) ? element : null;
        }
        return null;
    }

    Object extractCompositeField(Object val, String fieldName, String typeName) {
        if (val == null) return null;
        if (val instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) val;
            List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
            if (fields != null) {
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                        return i < row.values().size() ? row.values().get(i) : null;
                    }
                }
            }
            return null;
        }
        if (val instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) val;
            Object fieldVal = map.get(fieldName.toLowerCase(java.util.Locale.ROOT));
            if (fieldVal == null) fieldVal = map.get(fieldName);
            return fieldVal;
        }
        if (val instanceof String && RecordLiteral.looksLikeRecord((String) val)) {
            List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
            if (fields != null) {
                List<RecordLiteral.Field> parts = RecordLiteral.parse((String) val);
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                        if (i < parts.size()) {
                            RecordLiteral.Field part = parts.get(i);
                            return coerceFieldValue(part.text, fields.get(i).typeName(), part.quoted);
                        }
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Refuse a value that is not shaped like the composite it is being made into.
     *
     * <p>PostgreSQL answers a badly shaped value two different ways, and which one it gives
     * depends on what the value already was. A row constructor is a record being converted, so a
     * record holding the wrong number of fields is a cast that cannot be made and the type is
     * named. Text is input for the type's own reader, so text of the wrong shape is bad input and
     * the literal that could not be read is quoted back. Stored, the two are the same text, which
     * is why the question is asked while the value still knows which it was.
     *
     * <p>A field whose own type is a composite is read by that type's reader in turn, and it is
     * read before the fields after it are counted -- which is why a badly shaped nested value is
     * blamed on the inner type and quotes the inner text rather than the whole literal.
     */
    void requireCompositeShape(Object value, String typeName) {
        if (value == null || typeName == null) return;
        List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
        if (fields == null) return;
        if (value instanceof AstExecutor.PgRow) {
            List<Object> values = ((AstExecutor.PgRow) value).values();
            if (values.size() != fields.size()) {
                // The type is named the way this session would write it rather than by the key it
                // is stored under, and PostgreSQL adds which way the record was the wrong shape --
                // that is what tells the writer whether to add a field or take one away.
                MemgresException wrongShape = new MemgresException("cannot cast type record to "
                        + TypeNamespace.display(executor.database, executor.session, typeName),
                        "42846");
                wrongShape.setDetail(values.size() > fields.size()
                        ? "Input has too many columns." : "Input has too few columns.");
                throw wrongShape;
            }
            for (int i = 0; i < values.size(); i++) {
                requireCompositeShape(values.get(i), fields.get(i).typeName());
            }
            return;
        }
        if (!(value instanceof String)) return;
        String text = ((String) value).trim();
        if (text.isEmpty() || text.charAt(0) != '(') {
            throw malformedRecord(text, "Missing left parenthesis.");
        }
        if (text.length() < 2 || text.charAt(text.length() - 1) != ')') {
            throw malformedRecord(text, "Unexpected end of input.");
        }
        List<RecordLiteral.Field> parts = RecordLiteral.parse(text);
        for (int i = 0; i < parts.size() && i < fields.size(); i++) {
            RecordLiteral.Field part = parts.get(i);
            // An unquoted field with nothing in it is the SQL null, which every type accepts.
            if (!part.quoted && part.text.isEmpty()) continue;
            requireCompositeShape(part.text, fields.get(i).typeName());
        }
        if (parts.size() != fields.size()) {
            throw malformedRecord(text, parts.size() > fields.size()
                    ? "Too many columns." : "Too few columns.");
        }
    }

    /** PostgreSQL quotes the literal its record reader could not read and says what stopped it. */
    private MemgresException malformedRecord(String text, String detail) {
        MemgresException ex = new MemgresException(
                "malformed record literal: \"" + text + "\"", "22P02");
        ex.setDetail(detail);
        return ex;
    }

    /** The fields of a composite literal, as the composite's own reader reads them. */
    String[] splitCompositeString(String inner) {
        List<RecordLiteral.Field> fields = RecordLiteral.parse(inner);
        String[] parts = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) parts[i] = fields.get(i).text;
        return parts;
    }

    AstExecutor.PgRow parseCompositeToRow(String s, String typeName) {
        List<RecordLiteral.Field> parts = RecordLiteral.parse(s);
        List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            RecordLiteral.Field part = parts.get(i);
            if (fields != null && i < fields.size()) {
                values.add(coerceFieldValue(part.text, fields.get(i).typeName(), part.quoted));
            } else {
                values.add(part.quoted || !part.text.isEmpty() ? part.text : null);
            }
        }
        return new AstExecutor.PgRow(values);
    }

    /**
     * Resolve composite type fields by name, with table-type fallback.
     */
    java.util.List<CreateTypeStmt.CompositeField> resolveFieldsForType(String typeName) {
        if (typeName == null) return null;
        java.util.List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
        if (fields != null) return fields;
        // Fallback: PG treats tables as composite types
        Table tbl = executor.database.getTable(typeName);
        if (tbl != null) {
            fields = new ArrayList<>();
            for (Column c : tbl.getColumns()) {
                fields.add(new CreateTypeStmt.CompositeField(c.getName(), c.getType().getPgName()));
            }
            return fields;
        }
        return null;
    }

    /**
     * Populate a record from hstore: start with base values, overlay matching hstore keys with type coercion.
     */
    java.util.Map<String, Object> populateFromHstore(Object baseVal, HstoreValue hs,
            java.util.List<CreateTypeStmt.CompositeField> fields) {
        java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
        // Initialize from base argument
        if (baseVal instanceof java.util.Map) {
            for (java.util.Map.Entry<?, ?> e : ((java.util.Map<?, ?>) baseVal).entrySet()) {
                result.put(e.getKey().toString(), e.getValue());
            }
        } else if (baseVal instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) baseVal;
            for (int i = 0; i < fields.size() && i < row.values().size(); i++) {
                result.put(fields.get(i).name(), row.values().get(i));
            }
        } else {
            // Base is NULL — initialize all fields to null
            for (CreateTypeStmt.CompositeField f : fields) {
                result.put(f.name(), null);
            }
        }
        // Overlay hstore values with type coercion
        for (CreateTypeStmt.CompositeField f : fields) {
            if (hs.getData().containsKey(f.name())) {
                String val = hs.get(f.name());
                // hstore values are already unquoted; an empty string is a real empty
                // string (hstore NULL is represented as Java null), so pass quoted=true
                result.put(f.name(), val == null ? null : coerceFieldValue(val, f.typeName(), true));
            }
        }
        return result;
    }

    Object coerceFieldValue(String val, String typeName) {
        return coerceFieldValue(val, typeName, false);
    }

    /**
     * Coerce a composite field's text value to its Java type. In a composite literal
     * only an UNQUOTED empty field means NULL; a quoted empty string ("") is the
     * empty string, so callers must pass {@code quoted} accordingly.
     */
    Object coerceFieldValue(String val, String typeName, boolean quoted) {
        if (val == null) return null;
        if (val.isEmpty() && !quoted) return null;
        String lt = typeName.toLowerCase(java.util.Locale.ROOT).trim();
        try {
            switch (lt) {
                case "int":
                case "int4":
                case "integer":
                    return Integer.parseInt(val);
                case "int8":
                case "bigint":
                    return Long.parseLong(val);
                case "int2":
                case "smallint":
                    return (short) Integer.parseInt(val);
                case "float4":
                case "real":
                    return Float.parseFloat(val);
                case "float8":
                case "double precision":
                    return Double.parseDouble(val);
                case "numeric":
                case "decimal":
                    return new java.math.BigDecimal(val);
                case "boolean":
                case "bool":
                    // The field was written by boolean's output function, which writes one letter;
                    // Java's parser reads only the word, so every stored true read back false.
                    return TypeCoercion.toBoolean(val);
                case "text":
                case "varchar":
                case "character varying":
                case "name":
                case "unknown":
                    return val;
                default:
                    // Every other type reads its own field back with its own input function, so a
                    // field holding an array comes back as an array rather than as its text.
                    return executor.castEvaluator.applyCast(val, lt);
            }
        } catch (NumberFormatException e) {
            return val;
        } catch (MemgresException e) {
            return val;
        }
    }
}
