package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * COMMENT ON, and SECURITY LABEL, which is written the same way.
 *
 * <p>What either one names is a kind and then a name written the way that kind is written: a cast
 * by the two types it converts between, an operator by its spelling and its operands, a trigger by
 * its relation, an operator class by its access method. Read instead as "every word before IS, the
 * last of them the name", a kind nobody defined was a kind, a list of targets was one target, and
 * a cast's parentheses were thrown away along with the types inside them.
 */
public final class CommentStmt implements Statement {

    private final String kind;
    private final String schema;
    private final String relation;
    private final String name;
    private final List<String> args;
    private final String using;
    private final String comment;
    private final boolean securityLabel;
    private final String provider;

    public CommentStmt(String kind, String schema, String relation, String name, List<String> args,
                       String using, String comment, boolean securityLabel, String provider) {
        this.kind = kind;
        this.schema = schema;
        this.relation = relation;
        this.name = name;
        this.args = args;
        this.using = using;
        this.comment = comment;
        this.securityLabel = securityLabel;
        this.provider = provider;
    }

    /** The object kind, upper-cased and with its words separated by one space. */
    public String kind() { return kind; }

    /** The schema written before the object's name, or null when none was. */
    public String schema() { return schema; }

    /**
     * The relation a relation-scoped object was named against — the {@code t} of
     * {@code COMMENT ON TRIGGER g ON t} — or the table of a {@code COLUMN}. Null otherwise.
     */
    public String relation() { return relation; }

    /** The object's own name: the column, the trigger, the table, the type. */
    public String name() { return name; }

    /**
     * The argument types written in parentheses: a routine's or an operator's operands, or the two
     * types a cast converts between. Null when the kind takes none and none was written.
     */
    public List<String> args() { return args; }

    /** The access method of an operator class or family, or the language of a transform. */
    public String using() { return using; }

    /** The comment itself, or null for {@code IS NULL}, which takes the comment away. */
    public String comment() { return comment; }

    public boolean isSecurityLabel() { return securityLabel; }

    /** The provider a SECURITY LABEL was written FOR, or null. */
    public String provider() { return provider; }

    /** The object's name as it was written, for an error that has to name it back. */
    public String writtenName() {
        StringBuilder sb = new StringBuilder();
        if (schema != null) sb.append(schema).append('.');
        if (relation != null) sb.append(relation).append('.');
        sb.append(name);
        return sb.toString();
    }

    /** The relation as it was written, qualified if a schema was. */
    public String writtenRelation() {
        return schema == null ? relation : schema + "." + relation;
    }

    @Override
    public String toString() {
        return (securityLabel ? "SecurityLabelStmt[" : "CommentStmt[")
                + kind + " " + writtenName() + "]";
    }
}
