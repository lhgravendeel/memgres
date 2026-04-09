package com.memgres.engine.parser.ast;

/**
 * ALTER OPERATOR name (left_type, right_type) OWNER TO | SET SCHEMA | SET (...)
 * ALTER OPERATOR FAMILY name USING method OWNER TO | SET SCHEMA | RENAME TO | ADD | DROP
 * ALTER OPERATOR CLASS name USING method OWNER TO | SET SCHEMA | RENAME TO
 */
public final class AlterOperatorStmt implements Statement {

    public enum ObjectKind { OPERATOR, OPERATOR_FAMILY, OPERATOR_CLASS }
    public enum AlterAction { OWNER_TO, SET_SCHEMA, RENAME_TO, SET_PROPERTIES, ADD_MEMBER, DROP_MEMBER }

    public final ObjectKind objectKind;
    public final String name;
    public final String leftArg;       // for OPERATOR: left arg type (may be null/NONE)
    public final String rightArg;      // for OPERATOR: right arg type
    public final String method;        // for FAMILY/CLASS: USING method
    public final AlterAction action;
    public final String value;         // new owner, new schema, or new name depending on action

    public AlterOperatorStmt(ObjectKind objectKind, String name, String leftArg, String rightArg,
                             String method, AlterAction action, String value) {
        this.objectKind = objectKind;
        this.name = name;
        this.leftArg = leftArg;
        this.rightArg = rightArg;
        this.method = method;
        this.action = action;
        this.value = value;
    }

    public ObjectKind objectKind() { return objectKind; }
    public String name() { return name; }
    public String leftArg() { return leftArg; }
    public String rightArg() { return rightArg; }
    public String method() { return method; }
    public AlterAction action() { return action; }
    public String value() { return value; }
}
