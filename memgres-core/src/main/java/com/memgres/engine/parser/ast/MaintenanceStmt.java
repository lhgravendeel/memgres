package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * VACUUM, ANALYZE, REINDEX, CLUSTER and CHECKPOINT.
 *
 * <p>These have a grammar of their own — an option list, a target of a named kind, a column list,
 * an index — and PostgreSQL says what is wrong with each part where that part stands. Carried as a
 * pair of strings and taken apart again with split() there was nowhere to say any of it: the
 * option list was consumed and dropped, the target's kind was never asked about, and a column list
 * was read and then ignored.
 */
public final class MaintenanceStmt implements Statement {

    /** Which statement this is. */
    public enum Verb { VACUUM, ANALYZE, REINDEX, CLUSTER, CHECKPOINT }

    /** What a REINDEX names. */
    public enum Target { INDEX, TABLE, SCHEMA, DATABASE, SYSTEM }

    /** One option of the parenthesised list, with the value it was written with. */
    public static final class Option {
        public final String name;
        public final String value;

        public Option(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String name() { return name; }

        public String value() { return value; }

        @Override
        public String toString() {
            return value == null ? name : name + " " + value;
        }
    }

    private final Verb verb;
    private final List<Option> options;
    private final Target target;
    private final String schema;
    private final String name;
    private final List<String> columns;
    private final String indexName;
    private final boolean concurrently;

    public MaintenanceStmt(Verb verb, List<Option> options, Target target, String schema,
                           String name, List<String> columns, String indexName,
                           boolean concurrently) {
        this.verb = verb;
        this.options = options;
        this.target = target;
        this.schema = schema;
        this.name = name;
        this.columns = columns;
        this.indexName = indexName;
        this.concurrently = concurrently;
    }

    public Verb verb() { return verb; }

    public List<Option> options() { return options; }

    /** What a REINDEX names, or null for the statements that name a relation outright. */
    public Target target() { return target; }

    public String schema() { return schema; }

    /** The relation, schema or database named, or null when the statement names none. */
    public String name() { return name; }

    /** The columns an ANALYZE was told to gather statistics for, or null when it names none. */
    public List<String> columns() { return columns; }

    /** The index a CLUSTER was told to cluster on, or null. */
    public String indexName() { return indexName; }

    public boolean concurrently() { return concurrently; }

    /** Whether an option of this name was written, whatever value it carries. */
    public boolean has(String option) {
        for (Option o : options) {
            if (o.name.equalsIgnoreCase(option)) return true;
        }
        return false;
    }

    /**
     * Whether a boolean option is on. An option written with no value is on, which is what makes
     * {@code VACUUM (ANALYZE)} analyse and {@code VACUUM (ANALYZE FALSE)} not.
     */
    public boolean isOn(String option) {
        for (Option o : options) {
            if (!o.name.equalsIgnoreCase(option)) continue;
            if (o.value == null) return true;
            return !("false".equalsIgnoreCase(o.value) || "off".equalsIgnoreCase(o.value)
                    || "0".equals(o.value) || "f".equalsIgnoreCase(o.value)
                    || "n".equalsIgnoreCase(o.value) || "no".equalsIgnoreCase(o.value));
        }
        return false;
    }

    /** The value written for an option, or null when the option was not written at all. */
    public String valueOf(String option) {
        for (Option o : options) {
            if (o.name.equalsIgnoreCase(option)) return o.value;
        }
        return null;
    }

    /** The relation as it was written, for an error that has to name it back. */
    public String writtenName() {
        return schema == null ? name : schema + "." + name;
    }

    @Override
    public String toString() {
        return "MaintenanceStmt[" + verb + " " + (target == null ? "" : target + " ")
                + writtenName() + "]";
    }
}
