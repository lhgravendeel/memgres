package com.memgres.engine;

/**
 * Represents a user-defined operator family created via CREATE OPERATOR FAMILY.
 */
public class PgOperatorFamily {
    private String name;
    private final String method;       // index access method: btree, hash, gist, gin, etc.
    private String schemaName;
    private String owner;

    /**
     * The places this family has filled. A family knows its members by the place each one holds
     * rather than by its own name, which is why a member is dropped by naming the number and the
     * types and nothing else — and the catalogue reads the same list to say what the family has.
     */
    private final java.util.Map<String, Member> members = new java.util.LinkedHashMap<>();

    /** One filled place: which kind, which number, over which pair of operand types, by what. */
    public static final class Member {
        public final boolean function;
        public final int number;
        public final String leftType;
        public final String rightType;
        /** The operator or function that fills the place, or null where none was named. */
        public final String named;

        public Member(boolean function, int number, String leftType, String rightType,
                      String named) {
            this.function = function;
            this.number = number;
            this.leftType = leftType;
            this.rightType = rightType;
            this.named = named;
        }
    }

    public PgOperatorFamily(String name, String method) {
        this.name = name;
        this.method = method;
        this.schemaName = "public";
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getMethod() { return method; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    /** True when the family did not already hold this place. */
    public boolean addMember(String place, Member member) {
        return members.put(place, member) == null;
    }

    /** True when the family held this place and no longer does. */
    public boolean removeMember(String place) { return members.remove(place) != null; }

    public boolean hasMember(String place) { return members.containsKey(place); }

    /** Every place this family has filled, in the order they were filled. */
    public java.util.Collection<Member> members() { return members.values(); }

    /**
     * Key for storage: name + method (same name can exist for different methods).
     */
    public String getKey() {
        return name.toLowerCase(java.util.Locale.ROOT) + ":" + method.toLowerCase(java.util.Locale.ROOT);
    }
}
