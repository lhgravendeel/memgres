package com.memgres.engine.parser.ast;

/**
 * CREATE CAST (source_type AS target_type) WITH FUNCTION func(args) [AS ASSIGNMENT|AS IMPLICIT]
 * CREATE CAST (source_type AS target_type) WITHOUT FUNCTION [AS ASSIGNMENT|AS IMPLICIT]
 */
public final class CreateCastStmt implements Statement {
    public final String sourceType;
    public final String targetType;
    public final String functionName;   // null for WITHOUT FUNCTION
    public final String castContext;    // "e" (explicit), "a" (assignment), "i" (implicit)

    public CreateCastStmt(String sourceType, String targetType, String functionName, String castContext) {
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.functionName = functionName;
        this.castContext = castContext;
    }
}
