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
    private final String policyType; // PERMISSIVE (default) or RESTRICTIVE

    public RlsPolicy(String name, String command, Expression usingExpr, Expression withCheckExpr) {
        this(name, command, usingExpr, withCheckExpr, null, "PERMISSIVE");
    }

    public RlsPolicy(String name, String command, Expression usingExpr, Expression withCheckExpr, List<String> roles) {
        this(name, command, usingExpr, withCheckExpr, roles, "PERMISSIVE");
    }

    public RlsPolicy(String name, String command, Expression usingExpr, Expression withCheckExpr, List<String> roles, String policyType) {
        this.name = name;
        this.command = command;
        this.usingExpr = usingExpr;
        this.withCheckExpr = withCheckExpr;
        this.roles = roles;
        this.policyType = policyType != null ? policyType : "PERMISSIVE";
    }

    public String getName() { return name; }
    public String getCommand() { return command; }
    public Expression getUsingExpr() { return usingExpr; }
    public Expression getWithCheckExpr() { return withCheckExpr; }
    public List<String> getRoles() { return roles; }
    public String getPolicyType() { return policyType; }
    public boolean isPermissive() { return "PERMISSIVE".equalsIgnoreCase(policyType); }
    public boolean isRestrictive() { return "RESTRICTIVE".equalsIgnoreCase(policyType); }

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
