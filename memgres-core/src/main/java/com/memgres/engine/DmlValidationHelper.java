package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.CreateTypeStmt;
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

    /**
     * A value on its way into a column, held to the column's type before it is stored.
     *
     * <p>A composite is judged here rather than once it is stored, because stored it is the same
     * text whichever way it was written and PostgreSQL answers the two ways differently: a record
     * of the wrong shape is a cast it cannot make, text of the wrong shape is input its reader
     * cannot read.
     */
    Object storedValue(Object value, Column column) {
        if (column.getCompositeTypeName() != null && column.getArrayElementType() == null) {
            executor.compositeTypeHandler.requireCompositeShape(value,
                    column.getCompositeTypeName());
        }
        return TypeCoercion.coerceForStorage(value, column);
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
            // A composite's fields are values of the types the composite declares for them, so a
            // field typed by a domain is judged by that domain wherever the composite is built --
            // including from a plain string written into the column, and from an assignment to one
            // field, neither of which passes through a cast to the composite. An array of a
            // composite is judged element by element by the cast that builds it, so only a value
            // of the composite itself is judged here.
            if (col.getCompositeTypeName() != null && col.getArrayElementType() == null) {
                row[i] = valueOfComposite(row[i], col.getCompositeTypeName());
                validateCompositeFieldDomains(row[i], col.getCompositeTypeName());
            }
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
                // An array of a domain holds one of the domain's values in every element, and
                // PostgreSQL builds each of them as a value of the domain -- so the domain's rules
                // are every element's rules, down through the dimensions. A null array holds no
                // value of the domain at all, and a domain built over an array is itself one value.
                for (Object held : domainValuesIn(row[i], col, domainName)) {
                    for (DomainType domain : chain) {
                        if (held == null && domain.isNotNull()) {
                            MemgresException ex = new MemgresException("domain "
                                    + domainDisplay(domain.getName())
                                    + " does not allow null values", "23502");
                            ex.setDatatype(domain.getName());
                            throw ex;
                        }
                        // A domain CHECK still runs for NULL: CHECK (VALUE IS NOT NULL) rejects it
                        Table tempTable = new Table("_domain_check",
                                Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                        RowContext tempCtx = new RowContext(tempTable, null, new Object[]{held});
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
    }

    /**
     * The values a column's domain has to be satisfied by. One for an ordinary column, and for a
     * column whose domain is itself built over an array; every element, however many dimensions
     * deep, for a column declared as an array of the domain; and none at all where such a column
     * holds no array, since a null array is not a value of the domain.
     */
    private List<Object> domainValuesIn(Object value, Column col, String domainName) {
        DomainType domain = executor.database.getDomain(domainName);
        boolean arrayOfTheDomain = col.getArrayElementType() != null
                && (domain == null || domain.getArrayElementType() == null);
        if (!arrayOfTheDomain) return Collections.singletonList(value);
        List<Object> held = new ArrayList<Object>();
        PgArray array = value == null ? null : PgArray.from(value);
        if (array != null) collectDomainValues(array, held);
        return held;
    }

    /** Every element of an array, walking into the arrays a multidimensional one is written as. */
    private void collectDomainValues(List<?> elements, List<Object> out) {
        for (Object element : elements) {
            if (element instanceof List<?>) collectDomainValues((List<?>) element, out);
            else out.add(element);
        }
    }

    /**
     * Every domain a composite value's fields are declared with, run against those fields.
     *
     * <p>PostgreSQL builds a value of the field's own type for each field of a composite, whichever
     * way the composite was written, and a domain's constraints run wherever a value of it is
     * built. Only the value already held is judged: nothing here changes what is stored.
     */
    private void validateCompositeFieldDomains(Object value, String typeName) {
        if (value == null) return;
        List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
        if (fields == null) return;
        if (value instanceof AstExecutor.PgRow) {
            List<Object> values = ((AstExecutor.PgRow) value).values();
            for (int i = 0; i < values.size() && i < fields.size(); i++) {
                Object field = values.get(i);
                // A value already held as itself is written the way its own output function writes
                // it, which is the form its input function reads back.
                checkFieldAgainstItsType(field == null ? null : field.toString(), true,
                        fields.get(i).typeName());
            }
            return;
        }
        if (value instanceof String && RecordLiteral.looksLikeRecord((String) value)) {
            // The literal is read by the composite's own reader, which is the only thing that
            // knows which quotes were structure and which were content.
            List<RecordLiteral.Field> parts = RecordLiteral.parse((String) value);
            for (int i = 0; i < parts.size() && i < fields.size(); i++) {
                checkFieldAgainstItsType(parts.get(i).text, parts.get(i).quoted,
                        fields.get(i).typeName());
            }
        }
    }

    /**
     * One field read as the type its composite declares for it, for the constraints that reading
     * runs. A domain carries them itself and a composite carries its own fields' ones, so those
     * are the only two types a field is re-read as; anything else has nothing to answer for.
     */
    private void checkFieldAgainstItsType(String text, boolean quoted, String fieldType) {
        if (text == null || fieldType == null) return;
        // An unquoted field with nothing in it is the SQL null, which no CHECK is run for here.
        if (text.isEmpty() && !quoted) return;
        if (executor.database.isCompositeType(fieldType)) {
            validateCompositeFieldDomains(text, fieldType);
            return;
        }
        if (!executor.database.isDomain(fieldType)) return;
        try {
            executor.castEvaluator.applyCast(text, fieldType.toLowerCase().trim());
        } catch (MemgresException e) {
            // A type whose input function cannot read the text is not what is being judged: only
            // the constraints the type carries are, and PostgreSQL refuses the whole write when
            // one of those fails.
            if (e.getSqlState() != null && e.getSqlState().startsWith("23")) throw e;
        }
    }

    /**
     * A value being written into a composite-typed column, built as a value of that composite.
     *
     * <p>PostgreSQL makes a value of the column's own type before it stores a row, and for a
     * composite that means one field of each attribute's declared type. A record with the wrong
     * number of fields is not a value of the type at all, and an attribute declared varchar(3) has
     * no room for a fourth character wherever the value is built -- the same refusal a column of
     * that width gives. Storing whatever text the writer happened to write let a column of the
     * composite hold anything shaped like a record.
     */
    private Object valueOfComposite(Object value, String typeName) {
        executor.compositeTypeHandler.requireCompositeShape(value, typeName);
        if (!(value instanceof String)) return value;
        String text = ((String) value).trim();
        if (!RecordLiteral.looksLikeRecord(text)) return value;
        List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
        if (fields == null) return value;
        List<RecordLiteral.Field> parts = RecordLiteral.parse(text);
        List<Object> held = new ArrayList<>();
        boolean changed = false;
        for (int i = 0; i < parts.size(); i++) {
            RecordLiteral.Field part = parts.get(i);
            // An unquoted field with nothing in it is the SQL null, which no width bounds.
            Object was = part.quoted || !part.text.isEmpty() ? part.text : null;
            Object now = fieldHeldToItsType(was, fields.get(i).typeName());
            if (now != was) changed = true;
            held.add(now);
        }
        // Only a value the attribute types actually changed is rewritten, so a record that was
        // already what its type says stays exactly as it was written.
        return changed ? new AstExecutor.PgRow(held).toPgText() : value;
    }

    /**
     * One field held to the width, length or scale its composite declares for it. A declaration
     * with no modifier bounds nothing, which is the ordinary case and leaves the field alone.
     */
    private Object fieldHeldToItsType(Object value, String fieldType) {
        return TypeCoercion.heldToItsType(value, fieldType);
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
            failing.append(ErrorValueText.of(row[i]));
        }
        MemgresException ex = new MemgresException(
                "new row violates check option for view \"" + viewName + "\"", "44000");
        ex.setDetail("Failing row contains (" + failing + ").");
        return ex;
    }
}
