package com.memgres.engine.parser.ast;

/**
 * ALTER OPERATOR name (left_type, right_type) OWNER TO | SET SCHEMA | SET (...)
 * ALTER OPERATOR FAMILY name USING method OWNER TO | SET SCHEMA | RENAME TO | ADD | DROP
 * ALTER OPERATOR CLASS name USING method OWNER TO | SET SCHEMA | RENAME TO
 */
public final class AlterOperatorStmt implements Statement {

    public enum ObjectKind { OPERATOR, OPERATOR_FAMILY, OPERATOR_CLASS }
    public enum AlterAction { OWNER_TO, SET_SCHEMA, RENAME_TO, SET_PROPERTIES, ADD_MEMBER, DROP_MEMBER }

    /**
     * One OPERATOR or FUNCTION item of an ADD or DROP list.
     *
     * <p>A family holds its members by what they are for rather than by their own names: the
     * number says which comparison or which support routine the member stands for, and the two
     * types say which pair of operands it covers. The operator or function named is what fills
     * that place, and a DROP names only the place.
     */
    public static final class Member {
        public final boolean function;
        public final int number;
        /** The operator or function named, or null on a DROP, which names only the place. */
        public final String named;
        public final java.util.List<String> argTypes;

        public Member(boolean function, int number, String named,
                      java.util.List<String> argTypes) {
            this.function = function;
            this.number = number;
            this.named = named;
            this.argTypes = argTypes;
        }

        /** How PostgreSQL writes the member in a complaint: the number, then the operand types. */
        public String written(java.util.List<String> canonicalTypes) {
            StringBuilder sb = new StringBuilder().append(number).append('(');
            for (int i = 0; i < canonicalTypes.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(canonicalTypes.get(i));
            }
            return sb.append(')').toString();
        }
    }

    /** The items an ADD or DROP listed, empty for every other action. */
    public final java.util.List<Member> members;

    public final ObjectKind objectKind;
    public final String name;
    public final String leftArg;       // for OPERATOR: left arg type (may be null/NONE)
    public final String rightArg;      // for OPERATOR: right arg type
    public final String method;        // for FAMILY/CLASS: USING method
    public final AlterAction action;
    public final String value;         // new owner, new schema, or new name depending on action

    public AlterOperatorStmt(ObjectKind objectKind, String name, String leftArg, String rightArg,
                             String method, AlterAction action, String value) {
        this(objectKind, name, leftArg, rightArg, method, action, value,
                java.util.Collections.<Member>emptyList());
    }

    public AlterOperatorStmt(ObjectKind objectKind, String name, String leftArg, String rightArg,
                             String method, AlterAction action, String value,
                             java.util.List<Member> members) {
        this.members = members;
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
    public java.util.List<Member> members() { return members; }
}
