package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * CREATE COLLATION [IF NOT EXISTS] name (option = value, ...)
 * or CREATE COLLATION [IF NOT EXISTS] name FROM existing_collation
 */
public final class CreateCollationStmt implements Statement {
    public final String name;
    public final boolean ifNotExists;
    public final String fromCollation;          // non-null if FROM clause
    public final Map<String, String> options;   // provider, locale, lc_collate, lc_ctype, deterministic

    /** The schema the definition named, or null when it named none. */
    public final String schemaName;

    public CreateCollationStmt(String name, boolean ifNotExists, String fromCollation, Map<String, String> options) {
        this(name, ifNotExists, fromCollation, options, null);
    }

    public CreateCollationStmt(String name, boolean ifNotExists, String fromCollation,
                               Map<String, String> options, String schemaName) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.fromCollation = fromCollation;
        this.options = options;
        this.schemaName = schemaName;
    }
}
