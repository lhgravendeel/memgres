package com.memgres.engine.parser.ast;

import java.util.List;

/**
 * CREATE CAST (source_type AS target_type) WITH FUNCTION func(args) [AS ASSIGNMENT|AS IMPLICIT]
 * CREATE CAST (source_type AS target_type) WITHOUT FUNCTION [AS ASSIGNMENT|AS IMPLICIT]
 */
public final class CreateCastStmt implements Statement {
    public final String sourceType;
    public final String targetType;
    public final String functionName;       // null for WITHOUT FUNCTION / WITH INOUT
    public final List<String> funcArgTypes; // declared argument types, null if not spelled out
    public final boolean withInout;         // WITH INOUT (I/O conversion cast)
    public final String castContext;        // "e" (explicit), "a" (assignment), "i" (implicit)

    public CreateCastStmt(String sourceType, String targetType, String functionName, String castContext) {
        this(sourceType, targetType, functionName, null, false, castContext);
    }

    public CreateCastStmt(String sourceType, String targetType, String functionName,
                          List<String> funcArgTypes, boolean withInout, String castContext) {
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.functionName = functionName;
        this.funcArgTypes = funcArgTypes;
        this.withInout = withInout;
        this.castContext = castContext;
    }
}
