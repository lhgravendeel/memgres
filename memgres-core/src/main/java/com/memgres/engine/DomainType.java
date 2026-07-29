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
    private boolean notNull;
    private String checkExpression; // raw SQL text for the CHECK constraint
    private Expression parsedCheck; // parsed CHECK expression (VALUE replaced with the actual value)
    private String defaultValue;
    /** The base type's declared modifier, e.g. 12 and null for varchar(12), 10 and 2 for numeric(10,2). */
    private Integer precision;
    private Integer scale;
    /** The interval field qualifier of the base type, lower case: "day to second", "year to month". */
    private String intervalQualifier;
    /** The element type when the domain is over an array, e.g. INTEGER for {@code integer[]}. */
    private DataType arrayElementType;
    /** The schema the domain was created in. */
    private String schemaName = "public";

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
    /** ALTER DOMAIN ... SET/DROP NOT NULL. */
    public void setNotNull(boolean notNull) { this.notNull = notNull; }
    public String getCheckExpression() { return checkExpression; }
    public Expression getParsedCheck() { return parsedCheck; }
    public String getDefaultValue() { return defaultValue; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }

    public String getIntervalQualifier() { return intervalQualifier; }
    public DataType getArrayElementType() { return arrayElementType; }
    public boolean isArray() { return arrayElementType != null; }
    public String getSchemaName() { return schemaName; }

    /** Record the base type's declared modifier, so information_schema can describe the domain. */
    public void setTypmod(Integer precision, Integer scale) {
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Record the rest of the base type's declaration — its interval field qualifier, whether it
     * is an array, and the schema the domain lives in. A column declared with the domain is a
     * column of that base type, so everything the declaration said has to travel with it.
     */
    public void setBaseTypeFacts(String intervalQualifier, DataType arrayElementType) {
        this.intervalQualifier = intervalQualifier;
        this.arrayElementType = arrayElementType;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /** Forget the CHECK that CREATE DOMAIN wrote, once ALTER DOMAIN DROP CONSTRAINT names it. */
    public void clearInlineCheck() {
        this.checkExpression = null;
        this.parsedCheck = null;
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
