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

    /** A column's recorded domain named the way PostgreSQL names it in an error about it. */
    private String domainDisplay(String stored) {
        return TypeNamespace.display(executor.database, executor.session, stored);
    }

    /**
     * A value the column's domain refuses. PostgreSQL names both the domain and the constraint in
     * the error's own fields, and it names the domain there bare: the field is already about one
     * type, so the qualifier the sentence needs to stay unambiguous has no work to do in it.
     */
    private MemgresException domainCheckViolation(String domainName, String constraintName) {
        MemgresException ex = new MemgresException("value for domain " + domainDisplay(domainName)
                + " violates check constraint \"" + constraintName + "\"", "23514");
        ex.setConstraint(constraintName);
        ex.setDatatype(TypeNamespace.bare(domainName));
        return ex;
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
                        throw new MemgresException("invalid input value for enum "
                                + TypeNamespace.display(executor.database, executor.session, enumTypeName)
                                + ": \"" + row[i] + "\"", "22P02");
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
                // Walk from the base domain outwards: a domain over a domain inherits its
                // constraints, and PG reports the innermost one a value violates
                java.util.List<DomainType> chain = new java.util.ArrayList<>();
                DomainType d = executor.database.getDomain(domainName);
                for (int guard = 0; d != null && guard < 64; guard++) {
                    chain.add(0, d);
                    String base = d.getBaseTypeName();
                    d = base == null ? null : executor.database.getDomain(base);
                }
                for (DomainType domain : chain) {
                    if (row[i] == null && domain.isNotNull()) {
                        MemgresException ex = new MemgresException("domain "
                                + domainDisplay(domain.getName())
                                + " does not allow null values", "23502");
                        ex.setDatatype(domain.getName());
                        throw ex;
                    }
                    // A domain CHECK still runs for NULL: CHECK (VALUE IS NOT NULL) rejects it
                    Table tempTable = new Table("_domain_check",
                            Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                    RowContext tempCtx = new RowContext(tempTable, null, new Object[]{row[i]});
                    // Check the original (unnamed) CHECK constraint
                    // In PG, a CHECK that returns NULL does NOT violate; only explicit false violates
                    if (domain.getParsedCheck() != null) {
                        Object result = executor.evalExpr(domain.getParsedCheck(), tempCtx);
                        if (result != null && !executor.isTruthy(result)) {
                            throw domainCheckViolation(domainName, domain.getName() + "_check");
                        }
                    }
                    // Check named constraints added via ALTER DOMAIN ADD CONSTRAINT
                    for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                        if (nc.parsedCheck() != null) {
                            Object result = executor.evalExpr(nc.parsedCheck(), tempCtx);
                            if (result != null && !executor.isTruthy(result)) {
                                throw domainCheckViolation(domainName, nc.name());
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
    List<ViewCheck> collectViewCheckExprs(String targetName) {
        List<ViewCheck> exprs = new ArrayList<>();
        collectViewCheckExprsRecursive(targetName, false, exprs, new HashSet<>());
        return exprs;
    }

    /**
     * One view's WITH CHECK OPTION condition, kept with the name of the view that declared it.
     * A write through a chain of views can fail any of their conditions, and PostgreSQL names
     * the one that failed — without it a caller only learns that some view rejected the row.
     */
    static final class ViewCheck {
        final String viewName;
        final Expression expr;

        ViewCheck(String viewName, Expression expr) {
            this.viewName = viewName;
            this.expr = expr;
        }
    }

    private void collectViewCheckExprsRecursive(String viewName, boolean cascading,
                                                  List<ViewCheck> exprs,
                                                  Set<String> visited) {
        if (!visited.add(viewName.toLowerCase())) return;
        Database.ViewDef view = executor.database.getView(viewName);
        if (view == null) return;
        // Add this view's WHERE clause if it has a CHECK OPTION OR if a parent is cascading
        boolean hasCheck = view.checkOption() != null;
        if ((hasCheck || cascading) && view.query() instanceof SelectStmt && ((SelectStmt) view.query()).where() != null) {
            SelectStmt sel = (SelectStmt) view.query();
            exprs.add(new ViewCheck(view.name(), sel.where()));
        }
        // Always descend: a base view's own WITH CHECK OPTION applies to rows written
        // through an outer view, whatever the outer view declares. The cascading flag only
        // decides whether a base view WITHOUT its own check option contributes its WHERE.
        boolean shouldCascade = "CASCADED".equals(view.checkOption()) || cascading;
        if (view.query() instanceof SelectStmt && ((SelectStmt) view.query()).from() != null) {
            SelectStmt sel = (SelectStmt) view.query();
            for (SelectStmt.FromItem fromItem : sel.from()) {
                if (fromItem instanceof SelectStmt.TableRef) {
                    SelectStmt.TableRef ref = (SelectStmt.TableRef) fromItem;
                    Database.ViewDef baseView = executor.database.getView(ref.table());
                    if (baseView != null) {
                        collectViewCheckExprsRecursive(ref.table(), shouldCascade, exprs, visited);
                    }
                }
            }
        }
    }

    /** Validate a row against collected WITH CHECK OPTION expressions. Throws 44000 if violated. */
    void enforceViewCheckOption(List<ViewCheck> checkExprs, Table table, Object[] row) {
        if (checkExprs.isEmpty()) return;
        RowContext ctx = new RowContext(table, table.getName(), row);
        for (ViewCheck check : checkExprs) {
            try {
                Object result = executor.evalExpr(check.expr, ctx);
                if (!executor.isTruthy(result)) {
                    throw checkOptionViolation(check.viewName, row);
                }
            } catch (MemgresException me) {
                if ("44000".equals(me.getSqlState())) throw me;
                // Other eval errors, treat as violation
                throw checkOptionViolation(check.viewName, row);
            }
        }
    }

    /**
     * PostgreSQL names the view whose check option the row failed and prints the row itself, so
     * a caller writing many rows at once can tell which one was rejected and why.
     */
    private MemgresException checkOptionViolation(String viewName, Object[] row) {
        StringBuilder failing = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            if (i > 0) failing.append(", ");
            failing.append(row[i] == null ? "null" : row[i].toString());
        }
        MemgresException ex = new MemgresException(
                "new row violates check option for view \"" + viewName + "\"", "44000");
        ex.setDetail("Failing row contains (" + failing + ").");
        return ex;
    }
}
