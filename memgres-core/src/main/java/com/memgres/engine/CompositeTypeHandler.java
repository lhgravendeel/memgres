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
            String tn = cast.typeName().toLowerCase().trim();
            if (executor.database.isCompositeType(tn)) return tn;
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
                            String ft = f.typeName().toLowerCase().trim();
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
                return col.getCompositeTypeName().toLowerCase();
            }
            return null;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            PgFunction pgFunc = executor.database.getFunction(fn.name());
            if (pgFunc != null) {
                String rt = pgFunc.getReturnType().toLowerCase().trim();
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
                                        return col.getCompositeTypeName().toLowerCase();
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

    Object extractCompositeField(Object val, String fieldName, String typeName) {
        if (val == null) return null;
        if (val instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow row = (AstExecutor.PgRow) val;
            List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
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
            Object fieldVal = map.get(fieldName.toLowerCase());
            if (fieldVal == null) fieldVal = map.get(fieldName);
            return fieldVal;
        }
        if (val instanceof String && ((String) val).startsWith("(") && ((String) val).endsWith(")")) {
            String s = (String) val;
            List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
            if (fields != null) {
                String[] parts = splitCompositeString(s.substring(1, s.length() - 1));
                for (int i = 0; i < fields.size(); i++) {
                    if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                        if (i < parts.length) {
                            String part = parts[i];
                            if (part.startsWith("\"") && part.endsWith("\"")) {
                                part = part.substring(1, part.length() - 1);
                            }
                            return coerceFieldValue(part, fields.get(i).typeName());
                        }
                        return null;
                    }
                }
            }
        }
        return null;
    }

    String[] splitCompositeString(String inner) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inQuote = false;
        StringBuilder current = new StringBuilder();
        for (int i = 0; i < inner.length(); i++) {
            char ch = inner.charAt(i);
            if (inQuote) {
                current.append(ch);
                if (ch == '"') inQuote = false;
            } else if (ch == '"') {
                current.append(ch);
                inQuote = true;
            } else if (ch == '(') {
                depth++;
                current.append(ch);
            } else if (ch == ')') {
                depth--;
                current.append(ch);
            } else if (ch == ',' && depth == 0) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }
        parts.add(current.toString());
        return parts.toArray(new String[0]);
    }

    AstExecutor.PgRow parseCompositeToRow(String s, String typeName) {
        String inner;
        if (s.startsWith("(") && s.endsWith(")")) {
            inner = s.substring(1, s.length() - 1);
        } else {
            inner = s;
        }
        String[] parts = splitCompositeString(inner);
        List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.startsWith("\"") && part.endsWith("\"")) {
                part = part.substring(1, part.length() - 1);
            }
            if (fields != null && i < fields.size()) {
                values.add(coerceFieldValue(part, fields.get(i).typeName()));
            } else {
                values.add(part);
            }
        }
        return new AstExecutor.PgRow(values);
    }

    Object coerceFieldValue(String val, String typeName) {
        if (val == null || val.isEmpty()) return null;
        String lt = typeName.toLowerCase().trim();
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
                    return Boolean.parseBoolean(val);
                default:
                    return val;
            }
        } catch (NumberFormatException e) {
            return val;
        }
    }
}
