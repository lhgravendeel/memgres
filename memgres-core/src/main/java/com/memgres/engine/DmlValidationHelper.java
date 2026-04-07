package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.SelectStmt;

import java.util.*;

/**
 * Row validation helpers: citext folding, enum validation, domain checks,
 * view WITH CHECK OPTION enforcement.
 * Extracted from DmlExecutor to separate validation concerns.
 */
class DmlValidationHelper {

    private final AstExecutor executor;

    DmlValidationHelper(AstExecutor executor) {
        this.executor = executor;
    }

    void applyCitextFolding(Table table, Object[] row) {
        for (int i = 0; i < table.getColumns().size() && i < row.length; i++) {
            if (row[i] instanceof String) {
                String s = (String) row[i];
                Column col = table.getColumns().get(i);
                String domainName = col.getDomainTypeName();
                if (domainName != null) {
                    DomainType domain = executor.database.getDomain(domainName);
                    if (domain != null && domain.getBaseTypeName() != null
                            && domain.getBaseTypeName().equalsIgnoreCase("citext")) {
                        row[i] = s.toLowerCase();
                    }
                }
            }
        }
    }

    void validateEnumValues(Object[] row, Table table) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            if (col.getType() == DataType.ENUM && row[i] != null) {
                // Skip validation for enum array columns
                if (col.getArrayElementType() != null || (row[i] instanceof String && ((String) row[i]).startsWith("{"))) {
                    String s = (String) row[i];
                    continue;
                }
                String enumTypeName = col.getEnumTypeName();
                if (enumTypeName != null) {
                    CustomEnum customEnum = executor.database.getCustomEnum(enumTypeName);
                    if (customEnum != null && !customEnum.isValidLabel(row[i].toString())) {
                        throw new MemgresException(
                                "invalid input value for enum " + enumTypeName + ": \"" + row[i] + "\"",
                                "22P02");
                    }
                }
            }
        }
    }

    void validateDomainChecks(Object[] row, Table table) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column col = table.getColumns().get(i);
            String domainName = col.getDomainTypeName();
            if (domainName != null) {
                DomainType domain = executor.database.getDomain(domainName);
                if (domain != null) {
                    Table tempTable = new Table("_domain_check",
                            Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                    RowContext tempCtx = new RowContext(tempTable, null, new Object[]{row[i]});
                    // Check the original (unnamed) CHECK constraint
                    // In PG, a CHECK that returns NULL does NOT violate; only explicit false violates
                    if (domain.getParsedCheck() != null) {
                        Object result = executor.evalExpr(domain.getParsedCheck(), tempCtx);
                        if (result != null && !executor.isTruthy(result)) {
                            throw new MemgresException(
                                    "value for domain " + domainName + " violates check constraint \"" + domainName + "_check\"",
                                    "23514");
                        }
                    }
                    // Check named constraints added via ALTER DOMAIN ADD CONSTRAINT
                    for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                        if (nc.parsedCheck() != null) {
                            Object result = executor.evalExpr(nc.parsedCheck(), tempCtx);
                            if (result != null && !executor.isTruthy(result)) {
                                throw new MemgresException(
                                        "value for domain " + domainName + " violates check constraint \"" + nc.name() + "\"",
                                        "23514");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Collect all WITH CHECK OPTION expressions for a view and its base views (CASCADED).
     * Returns empty list if the target is a table or has no check option.
     */
    List<Expression> collectViewCheckExprs(String targetName) {
        List<Expression> exprs = new ArrayList<>();
        collectViewCheckExprsRecursive(targetName, true, exprs, new HashSet<>());
        return exprs;
    }

    private void collectViewCheckExprsRecursive(String viewName, boolean first,
                                                  List<Expression> exprs,
                                                  Set<String> visited) {
        if (!visited.add(viewName.toLowerCase())) return;
        Database.ViewDef view = executor.database.getView(viewName);
        if (view == null) return;
        // Add this view's WHERE clause if it has a CHECK OPTION (or if cascaded from parent)
        if (view.checkOption() != null && view.query() instanceof SelectStmt && ((SelectStmt) view.query()).where() != null) {
            SelectStmt sel = (SelectStmt) view.query();
            exprs.add(sel.where());
        }
        // If CASCADED, also check base views
        if ("CASCADED".equals(view.checkOption()) || first) {
            // Find base views this view reads from
            if (view.query() instanceof SelectStmt && ((SelectStmt) view.query()).from() != null) {
                SelectStmt sel = (SelectStmt) view.query();
                for (SelectStmt.FromItem fromItem : sel.from()) {
                    if (fromItem instanceof SelectStmt.TableRef) {
                        SelectStmt.TableRef ref = (SelectStmt.TableRef) fromItem;
                        Database.ViewDef baseView = executor.database.getView(ref.table());
                        if (baseView != null && baseView.checkOption() != null) {
                            collectViewCheckExprsRecursive(ref.table(), false, exprs, visited);
                        }
                    }
                }
            }
        }
    }

    /** Validate a row against collected WITH CHECK OPTION expressions. Throws 44000 if violated. */
    void enforceViewCheckOption(List<Expression> checkExprs, Table table, Object[] row) {
        if (checkExprs.isEmpty()) return;
        RowContext ctx = new RowContext(table, table.getName(), row);
        for (Expression checkExpr : checkExprs) {
            try {
                Object result = executor.evalExpr(checkExpr, ctx);
                if (!executor.isTruthy(result)) {
                    throw new MemgresException("new row violates check option for view", "44000");
                }
            } catch (MemgresException me) {
                if ("44000".equals(me.getSqlState())) throw me;
                // Other eval errors, treat as violation
                throw new MemgresException("new row violates check option for view", "44000");
            }
        }
    }
}
