package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL DOMAIN type: a named type with optional constraints.
 * CREATE DOMAIN name AS base_type [NOT NULL] [CHECK (expr)]
 */
public class DomainType {

    private final String name;
    private final DataType baseType;
    private final String baseTypeName; // original type name (e.g., "citext")
    private final boolean notNull;
    private String checkExpression; // raw SQL text for the CHECK constraint
    private Expression parsedCheck; // parsed CHECK expression (VALUE replaced with the actual value)
    private String defaultValue;

    /** Named constraints added via ALTER DOMAIN ADD CONSTRAINT. */
    private final List<NamedConstraint> namedConstraints = new ArrayList<>();

    public DomainType(String name, DataType baseType, boolean notNull, String checkExpression, Expression parsedCheck, String defaultValue) {
        this(name, baseType, null, notNull, checkExpression, parsedCheck, defaultValue);
    }

    public DomainType(String name, DataType baseType, String baseTypeName, boolean notNull, String checkExpression, Expression parsedCheck, String defaultValue) {
        this.name = name;
        this.baseType = baseType;
        this.baseTypeName = baseTypeName;
        this.notNull = notNull;
        this.checkExpression = checkExpression;
        this.parsedCheck = parsedCheck;
        this.defaultValue = defaultValue;
    }

    public String getName() { return name; }
    public DataType getBaseType() { return baseType; }
    public String getBaseTypeName() { return baseTypeName; }
    public boolean isNotNull() { return notNull; }
    public String getCheckExpression() { return checkExpression; }
    public Expression getParsedCheck() { return parsedCheck; }
    public String getDefaultValue() { return defaultValue; }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void addConstraint(String constraintName, String rawCheckExpr, Expression parsedCheckExpr) {
        namedConstraints.add(new NamedConstraint(constraintName, rawCheckExpr, parsedCheckExpr, true));
    }

    public void addConstraint(String constraintName, String rawCheckExpr, Expression parsedCheckExpr, boolean validated) {
        namedConstraints.add(new NamedConstraint(constraintName, rawCheckExpr, parsedCheckExpr, validated));
    }

    public void removeConstraint(String constraintName) {
        namedConstraints.removeIf(c -> c.name().equalsIgnoreCase(constraintName));
    }

    public List<NamedConstraint> getNamedConstraints() {
        return namedConstraints;
    }

    /** A named CHECK constraint on a domain. */
        public static final class NamedConstraint {
        private String name;
        public final String rawCheckExpr;
        public final Expression parsedCheck;
        private boolean validated;

        public NamedConstraint(String name, String rawCheckExpr, Expression parsedCheck) {
            this(name, rawCheckExpr, parsedCheck, true);
        }

        public NamedConstraint(String name, String rawCheckExpr, Expression parsedCheck, boolean validated) {
            this.name = name;
            this.rawCheckExpr = rawCheckExpr;
            this.parsedCheck = parsedCheck;
            this.validated = validated;
        }

        public String name() { return name; }
        public void setName(String name) { this.name = name; }
        public String rawCheckExpr() { return rawCheckExpr; }
        public Expression parsedCheck() { return parsedCheck; }
        public boolean isValidated() { return validated; }
        public void setValidated(boolean validated) { this.validated = validated; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NamedConstraint that = (NamedConstraint) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(rawCheckExpr, that.rawCheckExpr)
                && java.util.Objects.equals(parsedCheck, that.parsedCheck);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, rawCheckExpr, parsedCheck);
        }

        @Override
        public String toString() {
            return "NamedConstraint[name=" + name + ", " + "rawCheckExpr=" + rawCheckExpr + ", " + "parsedCheck=" + parsedCheck + "]";
        }
    }
}
