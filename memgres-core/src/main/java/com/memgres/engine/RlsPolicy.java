package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;

import java.util.List;

/**
 * Row-level security policy stored on a table.
 */
public class RlsPolicy {

    private final String name;
    private final String command; // SELECT, INSERT, UPDATE, DELETE, ALL
    private final Expression usingExpr;
    private final Expression withCheckExpr;
    private final List<String> roles; // target roles (null or empty = PUBLIC/all roles)

    public RlsPolicy(String name, String command, Expression usingExpr, Expression withCheckExpr) {
        this(name, command, usingExpr, withCheckExpr, null);
    }

    public RlsPolicy(String name, String command, Expression usingExpr, Expression withCheckExpr, List<String> roles) {
        this.name = name;
        this.command = command;
        this.usingExpr = usingExpr;
        this.withCheckExpr = withCheckExpr;
        this.roles = roles;
    }

    public String getName() { return name; }
    public String getCommand() { return command; }
    public Expression getUsingExpr() { return usingExpr; }
    public Expression getWithCheckExpr() { return withCheckExpr; }
    public List<String> getRoles() { return roles; }

    public boolean appliesTo(String operation) {
        if (command.equalsIgnoreCase("ALL")) return true;
        return command.equalsIgnoreCase(operation);
    }

    /**
     * Check if this policy applies to the given role.
     * If no roles are specified (null or empty), the policy applies to PUBLIC (all roles).
     */
    public boolean appliesToRole(String roleName) {
        if (roles == null || roles.isEmpty()) return true;
        for (String r : roles) {
            if (r.equalsIgnoreCase(roleName) || r.equalsIgnoreCase("PUBLIC")) {
                return true;
            }
        }
        return false;
    }
}
