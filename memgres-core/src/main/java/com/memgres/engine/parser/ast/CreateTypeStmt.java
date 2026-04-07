package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE TYPE name AS ENUM ('label1', 'label2', ...)
 * CREATE TYPE name AS (field1 type1, field2 type2, ...)
 */
public final class CreateTypeStmt implements Statement {
    public final String name;
    public final List<String> enumLabels;
    public final List<CompositeField> compositeFields;

    public CreateTypeStmt(String name, List<String> enumLabels, List<CompositeField> compositeFields) {
        this.name = name;
        this.enumLabels = enumLabels;
        this.compositeFields = compositeFields;
    }

        public static final class CompositeField {
        public final String name;
        public final String typeName;

        public CompositeField(String name, String typeName) {
            this.name = name;
            this.typeName = typeName;
        }

        public String name() { return name; }
        public String typeName() { return typeName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeField that = (CompositeField) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(typeName, that.typeName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, typeName);
        }

        @Override
        public String toString() {
            return "CompositeField[name=" + name + ", " + "typeName=" + typeName + "]";
        }
    }

    /** Backward-compatible: enum-only constructor */
    public CreateTypeStmt(String name, List<String> enumLabels) {
        this(name, enumLabels, null);
    }

    public String name() { return name; }
    public List<String> enumLabels() { return enumLabels; }
    public List<CompositeField> compositeFields() { return compositeFields; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateTypeStmt that = (CreateTypeStmt) o;
        return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(enumLabels, that.enumLabels)
            && java.util.Objects.equals(compositeFields, that.compositeFields);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(name, enumLabels, compositeFields);
    }

    @Override
    public String toString() {
        return "CreateTypeStmt[name=" + name + ", " + "enumLabels=" + enumLabels + ", " + "compositeFields=" + compositeFields + "]";
    }
}
