package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles CREATE/ALTER/DROP for types, functions, triggers, sequences, domains, and generic DROP dispatch.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlObjectExecutor {
    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlObjectExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- CREATE TYPE ----

    QueryResult executeCreateType(CreateTypeStmt stmt) {
        ddl.checkPgCatalogWriteProtection();
        if (stmt.enumLabels() != null) {
            if (executor.database.getCustomEnum(stmt.name()) != null) {
                throw new MemgresException("type \"" + stmt.name() + "\" already exists", "42710");
            }
            executor.database.addCustomEnum(new CustomEnum(stmt.name(), stmt.enumLabels()));
            executor.database.registerSchemaObject(executor.defaultSchema(), "enum", stmt.name());
        }
        if (stmt.compositeFields() != null) {
            if (executor.database.isCompositeType(stmt.name())) {
                throw new MemgresException("type \"" + stmt.name() + "\" already exists", "42710");
            }
            executor.database.addCompositeType(stmt.name(), stmt.compositeFields());
            executor.database.registerSchemaObject(executor.defaultSchema(), "composite", stmt.name());
        }
        return QueryResult.command(QueryResult.Type.CREATE_TYPE, 0);
    }

    // ---- ALTER TYPE ----

    QueryResult executeAlterType(AlterTypeStmt stmt) {
        CustomEnum existing = executor.database.getCustomEnum(stmt.typeName());
        if (existing == null) throw new MemgresException("type \"" + stmt.typeName() + "\" does not exist", "42704");

        switch (stmt.action()) {
            case ADD_VALUE: {
                if (stmt.ifNotExists() && existing.isValidLabel(stmt.value())) break;
                if (!stmt.ifNotExists() && existing.isValidLabel(stmt.value())) {
                    throw new MemgresException("enum label \"" + stmt.value() + "\" already exists", "42710");
                }
                List<String> labels = new ArrayList<>(existing.getLabels());
                if ("BEFORE".equals(stmt.position())) {
                    int idx = labels.indexOf(stmt.neighbor());
                    if (idx < 0) throw new MemgresException("\"" + stmt.neighbor() + "\" is not an existing enum label", "22023");
                    labels.add(idx, stmt.value());
                } else if ("AFTER".equals(stmt.position())) {
                    int idx = labels.indexOf(stmt.neighbor());
                    if (idx < 0) throw new MemgresException("\"" + stmt.neighbor() + "\" is not an existing enum label", "22023");
                    labels.add(idx + 1, stmt.value());
                } else {
                    labels.add(stmt.value());
                }
                executor.database.replaceCustomEnum(new CustomEnum(stmt.typeName(), labels));
                break;
            }
            case RENAME_VALUE: {
                List<String> labels = new ArrayList<>(existing.getLabels());
                int idx = labels.indexOf(stmt.value());
                if (idx >= 0) labels.set(idx, stmt.newValue());
                executor.database.replaceCustomEnum(new CustomEnum(stmt.typeName(), labels));
                break;
            }
            case RENAME_TO: {
                executor.database.removeCustomEnum(stmt.typeName());
                executor.database.addCustomEnum(new CustomEnum(stmt.value(), existing.getLabels()));
                break;
            }
            case SET_SCHEMA: {
                break;
            }
            case OWNER_TO: {
                break;
            }
        }
        return QueryResult.command(QueryResult.Type.ALTER_TYPE, 0);
    }

    // ---- CREATE FUNCTION ----

    QueryResult executeCreateAggregate(CreateAggregateStmt stmt) {
        // Validate that the state transition function exists (skip known built-in PG functions)
        if (stmt.sfunc() != null) {
            PgFunction func = executor.database.getFunction(stmt.sfunc());
            if (func == null && !isKnownBuiltinFunction(stmt.sfunc())) {
                throw new MemgresException("function " + stmt.sfunc() + " does not exist", "42883");
            }
        }
        PgAggregate agg = new PgAggregate(
                stmt.name(),
                stmt.sfunc(),
                stmt.stype(),
                stmt.initcond(),
                stmt.finalfunc(),
                stmt.combinefunc(),
                stmt.sortop(),
                stmt.argTypes() != null ? stmt.argTypes().toArray(new String[0]) : new String[0]
        );
        agg.setSchemaName(executor.defaultSchema());
        executor.database.addAggregate(agg);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    // ---- CREATE/ALTER/DROP OPERATOR ----

    QueryResult executeCreateOperator(CreateOperatorStmt stmt) {
        // Validate that at least one of LEFTARG/RIGHTARG is specified
        if (stmt.leftArg() == null && stmt.rightArg() == null) {
            throw new MemgresException(
                "operator argument types must be specified", "42P13");
        }

        // Validate that the backing function exists (skip for well-known built-in PG functions)
        if (stmt.function() != null) {
            String funcName = stmt.function();
            PgFunction func;
            if (funcName.contains(".")) {
                String[] parts = funcName.split("\\.", 2);
                func = executor.database.getFunction(parts[0], parts[1]);
            } else {
                func = executor.database.getFunction(funcName);
            }
            if (func == null && !isKnownBuiltinFunction(funcName)) {
                StringBuilder sig = new StringBuilder(funcName).append("(");
                boolean first = true;
                if (stmt.leftArg() != null) { sig.append(stmt.leftArg()); first = false; }
                if (stmt.rightArg() != null) { if (!first) sig.append(", "); sig.append(stmt.rightArg()); }
                sig.append(")");
                throw new MemgresException("function " + sig + " does not exist", "42883");
            }
        }

        PgOperator op = new PgOperator(stmt.name(), stmt.leftArg(), stmt.rightArg(), stmt.function());
        op.setCommutator(stmt.commutator());
        op.setNegator(stmt.negator());
        op.setRestrict(stmt.restrict());
        op.setJoin(stmt.join());
        op.setHashes(stmt.hashes());
        op.setMerges(stmt.merges());
        if (stmt.schema() != null) {
            op.setSchemaName(stmt.schema());
        } else {
            // Use current default schema from search_path (PG creates in first writable schema)
            op.setSchemaName(executor.defaultSchema());
        }
        // Set owner to current user
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            op.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperator(op.getKey())) {
            throw new MemgresException("operator " + stmt.name() + " already exists", "42710");
        }

        executor.database.addOperator(op);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateOperatorFamily(CreateOperatorFamilyStmt stmt) {
        PgOperatorFamily fam = new PgOperatorFamily(stmt.name(), stmt.method());
        if (stmt.schema() != null) fam.setSchemaName(stmt.schema());
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            fam.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperatorFamily(fam.getKey())) {
            throw new MemgresException("operator family \"" + stmt.name() + "\" for access method \""
                    + stmt.method() + "\" already exists", "42710");
        }

        executor.database.addOperatorFamily(fam);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateOperatorClass(CreateOperatorClassStmt stmt) {
        PgOperatorClass cls = new PgOperatorClass(stmt.name(), stmt.forType(), stmt.method(), stmt.isDefault());
        if (stmt.schema() != null) cls.setSchemaName(stmt.schema());
        cls.setFamilyName(stmt.familyName());
        if (executor.session != null) {
            String role = executor.session.getGucSettings().get("role");
            cls.setOwner(role != null ? role : "memgres");
        }

        // Check for duplicate
        if (executor.database.hasOperatorClass(cls.getKey())) {
            throw new MemgresException("operator class \"" + stmt.name() + "\" for access method \""
                    + stmt.method() + "\" already exists", "42710");
        }

        executor.database.addOperatorClass(cls);
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeAlterOperator(AlterOperatorStmt stmt) {
        switch (stmt.objectKind()) {
            case OPERATOR:
                return executeAlterOperatorObj(stmt);
            case OPERATOR_FAMILY:
                return executeAlterOperatorFamilyObj(stmt);
            case OPERATOR_CLASS:
                return executeAlterOperatorClassObj(stmt);
            default:
                return QueryResult.command(QueryResult.Type.SET, 0);
        }
    }

    private QueryResult executeAlterOperatorObj(AlterOperatorStmt stmt) {
        // Build key from schema + name + arg types
        String l = stmt.leftArg() != null ? stmt.leftArg().toLowerCase() : "NONE";
        String r = stmt.rightArg() != null ? stmt.rightArg().toLowerCase() : "NONE";
        String schema = "public";
        String opName = stmt.name();
        int dotIdx = opName.indexOf('.');
        if (dotIdx > 0) { schema = opName.substring(0, dotIdx); opName = opName.substring(dotIdx + 1); }
        String key = schema.toLowerCase() + "." + opName + "(" + l + "," + r + ")";

        PgOperator op = executor.database.getOperator(key);
        if (op == null) {
            throw new MemgresException("operator does not exist: " + stmt.name(), "42704");
        }

        switch (stmt.action()) {
            case OWNER_TO:
                op.setOwner(stmt.value());
                break;
            case SET_SCHEMA:
                op.setSchemaName(stmt.value());
                break;
            case SET_PROPERTIES:
                // Properties already consumed by parser; no further action needed
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    private QueryResult executeAlterOperatorFamilyObj(AlterOperatorStmt stmt) {
        String key = stmt.name().toLowerCase() + ":" + stmt.method().toLowerCase();
        PgOperatorFamily fam = executor.database.getOperatorFamily(key);
        if (fam == null && stmt.action() != AlterOperatorStmt.AlterAction.ADD_MEMBER
                && stmt.action() != AlterOperatorStmt.AlterAction.DROP_MEMBER) {
            throw new MemgresException("operator family \"" + stmt.name()
                    + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
        }
        if (fam == null) {
            // ADD/DROP MEMBER on non-existent family — just accept
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        switch (stmt.action()) {
            case OWNER_TO:
                fam.setOwner(stmt.value());
                break;
            case RENAME_TO:
                executor.database.removeOperatorFamily(key);
                fam.setName(stmt.value());
                executor.database.addOperatorFamily(fam);
                break;
            case SET_SCHEMA:
                fam.setSchemaName(stmt.value());
                break;
            case ADD_MEMBER:
            case DROP_MEMBER:
                // Members are tracked conceptually but we don't maintain a member list
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    private QueryResult executeAlterOperatorClassObj(AlterOperatorStmt stmt) {
        String key = stmt.name().toLowerCase() + ":" + stmt.method().toLowerCase();
        PgOperatorClass cls = executor.database.getOperatorClass(key);
        if (cls == null) {
            throw new MemgresException("operator class \"" + stmt.name()
                    + "\" does not exist for access method \"" + stmt.method() + "\"", "42704");
        }

        switch (stmt.action()) {
            case OWNER_TO:
                cls.setOwner(stmt.value());
                break;
            case RENAME_TO:
                executor.database.removeOperatorClass(key);
                cls.setName(stmt.value());
                executor.database.addOperatorClass(cls);
                break;
            case SET_SCHEMA:
                cls.setSchemaName(stmt.value());
                break;
            default:
                break;
        }
        return QueryResult.command(QueryResult.Type.SET, 0);
    }

    QueryResult executeCreateFunction(CreateFunctionStmt stmt) {
        if ("pg_catalog".equalsIgnoreCase(stmt.schema())) {
            throw new MemgresException("permission denied to create function in schema pg_catalog", "42501");
        }
        if (stmt.schema() == null) {
            String targetSchema = executor.defaultSchema();
            if ("pg_catalog".equals(targetSchema)) {
                throw new MemgresException("permission denied to create function in schema pg_catalog", "42501");
            }
            if ("information_schema".equals(targetSchema)) {
                throw new MemgresException("no schema has been selected to create in", "3F000");
            }
            if (executor.database.getSchema(targetSchema) == null) {
                throw new MemgresException("no schema has been selected to create in", "3F000");
            }
        }

        // Validate return type exists (PG validates at CREATE time)
        if (stmt.returnType() != null && !stmt.returnType().isEmpty()) {
            String retType = stmt.returnType();
            String baseRetType = retType;
            if (baseRetType.toUpperCase().startsWith("SETOF ")) {
                baseRetType = baseRetType.substring(6).trim();
            }
            validateTypeExists(baseRetType);
        }

        List<PgFunction.Param> params = new ArrayList<>();
        if (stmt.parsedParams() != null) {
            for (CreateFunctionStmt.FuncParam fp : stmt.parsedParams()) {
                // Validate parameter types exist (PG validates at CREATE time)
                if (fp.typeName() != null) {
                    validateTypeExists(fp.typeName());
                }
                // Validate default expression function references
                if (fp.defaultExpr() != null) {
                    validateDefaultExpr(fp.defaultExpr());
                }
                params.add(new PgFunction.Param(fp.name(), fp.typeName(), fp.mode(), fp.defaultExpr()));
            }
        }

        // Validate PL/pgSQL declared variable types (PG validates at CREATE time when check_function_bodies=on)
        boolean checkBodies = executor.session == null || !"off".equalsIgnoreCase(
                executor.session.getGucSettings().get("check_function_bodies"));
        // Check for unsupported transaction commands (SAVEPOINT, ROLLBACK TO) that prevent procedure registration
        boolean hasUnsupportedTxnCmd = false;
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            hasUnsupportedTxnCmd = containsUnsupportedTransactionCommand(stmt.body());
        }
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            validatePlpgsqlDeclarations(stmt.body());
        }

        // Validate SQL language function bodies (only when check_function_bodies=on)
        if (checkBodies && "sql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            validateSqlFunctionBody(stmt, params);
        }

        // Validate plpgsql function bodies (only when check_function_bodies=on)
        if (checkBodies && "plpgsql".equalsIgnoreCase(stmt.language()) && stmt.body() != null) {
            String retType = stmt.returnType();
            boolean needsReturnValue = retType != null && !retType.isEmpty()
                    && !"void".equalsIgnoreCase(retType) && !"trigger".equalsIgnoreCase(retType)
                    && !retType.toUpperCase().startsWith("SETOF") && !"TABLE".equalsIgnoreCase(retType);
            if (needsReturnValue) {
                java.util.regex.Matcher rm = java.util.regex.Pattern.compile("\\breturn\\s*;", java.util.regex.Pattern.CASE_INSENSITIVE).matcher(stmt.body());
                if (rm.find()) {
                    throw new MemgresException("RETURN must have a return value for function returning " + retType, "42601");
                }
            }
            // Try to parse SQL expressions inside the PL/pgSQL body (PG validates at creation time).
            // Specifically parse RETURN <expr> statements to catch syntax errors.
            // Skip RETURN QUERY ..., RETURN NEXT ..., and bare RETURN; statements.
            try {
                java.util.regex.Matcher retMatcher = java.util.regex.Pattern.compile(
                    "\\bRETURN\\s+(.+?)\\s*;", java.util.regex.Pattern.CASE_INSENSITIVE
                ).matcher(stmt.body());
                while (retMatcher.find()) {
                    String retExpr = retMatcher.group(1).trim();
                    if (!retExpr.isEmpty() && !retExpr.equalsIgnoreCase("NEXT")
                            && !retExpr.equalsIgnoreCase("QUERY")
                            && !retExpr.toUpperCase().startsWith("QUERY ")
                            && !retExpr.toUpperCase().startsWith("NEXT ")) {
                        // Try parsing as a SELECT expression
                        com.memgres.engine.parser.Parser.parse("SELECT " + retExpr);
                    }
                }
            } catch (MemgresException e) {
                if ("42601".equals(e.getSqlState())) throw e;
                // Ignore non-syntax errors
            }
        }

        // Check for duplicate function
        List<String> newParamTypes = params.stream()
                .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                .map(PgFunction.Param::typeName)
                .collect(Collectors.toList());
        List<PgFunction> existingOverloads = executor.database.getFunctionOverloads(stmt.name());
        for (PgFunction existing : existingOverloads) {
            List<String> existingTypes = existing.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.toList());
            if (existingTypes.size() == newParamTypes.size()) {
                boolean sameTypes = true;
                for (int i = 0; i < existingTypes.size(); i++) {
                    String et = existingTypes.get(i) != null ? existingTypes.get(i).toLowerCase() : "";
                    String nt = newParamTypes.get(i) != null ? newParamTypes.get(i).toLowerCase() : "";
                    if (!et.equals(nt)) { sameTypes = false; break; }
                }
                if (sameTypes) {
                    if (stmt.orReplace()) {
                        executor.database.removeFunction(stmt.name(), existingTypes);
                        break;
                    }
                    throw new MemgresException("function \"" + stmt.name() + "\" already exists with same argument types", "42723");
                }
            }
        }

        // If the body contains unsupported transaction commands (SAVEPOINT, ROLLBACK TO SAVEPOINT),
        // PG rejects the function at creation time. We silently skip registration so CALL gets 42883.
        if (hasUnsupportedTxnCmd) {
            return QueryResult.command(QueryResult.Type.CREATE_FUNCTION, 0);
        }

        PgFunction pgFunc = new PgFunction(stmt.name(), stmt.returnType(), stmt.body(),
                stmt.language(), params, stmt.isProcedure());
        String funcSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        pgFunc.setSchemaName(funcSchema);
        pgFunc.setSecurityDefiner(stmt.securityDefiner());
        pgFunc.setStrict(stmt.strict());
        pgFunc.setLeakproof(stmt.leakproof());
        pgFunc.setVolatility(stmt.volatility());
        pgFunc.setSetClauses(stmt.setClauses());
        pgFunc.setOwner(executor.sessionUser());
        executor.database.addFunction(pgFunc);
        executor.database.registerSchemaObject(funcSchema, "function", stmt.name());
        executor.database.setObjectOwner("function:" + stmt.name(), executor.sessionUser());
        executor.recordUndo(new Session.CreateFunctionUndo(stmt.name()));
        return QueryResult.command(QueryResult.Type.CREATE_FUNCTION, 0);
    }

    private void validateSqlFunctionBody(CreateFunctionStmt stmt, List<PgFunction.Param> params) {
        try {
            // Validate type casts, function calls, and sequences in SQL body text
            validateSqlBodyReferences(stmt.body());
            List<String> bodyStmts = splitSqlStatements(stmt.body());
            for (String bodyStr : bodyStmts) {
                Statement parsed = com.memgres.engine.parser.Parser.parse(bodyStr);
                validateSqlFunctionStatement(parsed, stmt, params);
            }
        } catch (MemgresException e) {
            if ("42601".equals(e.getSqlState()) && stmt.body() != null) {
                String bodyTrimmed = stmt.body().trim().replaceAll(";\\s*$", "").trim();
                if (bodyTrimmed.equalsIgnoreCase("SELECT")) {
                    throw new MemgresException(
                            "return type mismatch in function declared to return " + (stmt.returnType() != null ? stmt.returnType() : "unknown")
                            + "\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                            "42P13");
                }
            }
            throw e;
        } catch (Exception e) {
            String msg = e.getMessage();
            throw new MemgresException(msg != null ? msg : "syntax error in function body", "42601");
        }
    }

    /**
     * Validate references in SQL function body text: type casts, function calls, sequences.
     * SQL-language functions in PG eagerly validate all object references at CREATE time.
     */
    private void validateSqlBodyReferences(String body) {
        // Check type casts: ::type_name
        java.util.regex.Matcher castMatcher = java.util.regex.Pattern.compile(
                "::\\s*([a-zA-Z_][a-zA-Z0-9_]*)").matcher(body);
        while (castMatcher.find()) {
            String typeName = castMatcher.group(1);
            if (!isKnownType(typeName)) {
                throw new MemgresException("type \"" + typeName + "\" does not exist", "42704");
            }
        }
        // Check function calls: name(...) that aren't table refs like INSERT INTO table(col)
        java.util.regex.Matcher fnMatcher = java.util.regex.Pattern.compile(
                "(?:^|\\s)([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(").matcher(body);
        while (fnMatcher.find()) {
            String fnName = fnMatcher.group(1).toLowerCase();
            if (isKnownSqlKeyword(fnName)) continue;
            if (isBuiltinFunction(fnName)) continue;
            // Skip if preceded by INTO, FROM, TABLE, UPDATE, JOIN (table reference, not function call)
            // Also skip if preceded by ')' which indicates a table alias like ') t(col)'
            int start = fnMatcher.start(1);
            String before = body.substring(0, start).trim().toLowerCase();
            if (before.endsWith("into") || before.endsWith("from") || before.endsWith("table")
                    || before.endsWith("update") || before.endsWith("join")
                    || before.endsWith("on") || before.endsWith(")")) continue;
            if (executor.database.getFunction(fnName) == null) {
                throw new MemgresException("function " + fnName + "() does not exist", "42883");
            }
        }
        // Check sequence references: nextval('seq_name'), currval('seq_name'), setval('seq_name', ...)
        java.util.regex.Matcher seqMatcher = java.util.regex.Pattern.compile(
                "(?:nextval|currval|setval)\\s*\\(\\s*'([^']+)'").matcher(body);
        while (seqMatcher.find()) {
            String seqName = seqMatcher.group(1);
            if (executor.database.getSequence(seqName) == null) {
                throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
            }
        }
    }

    private boolean isKnownType(String typeName) {
        if (BUILTIN_TYPES.contains(typeName.toLowerCase())) return true;
        if (DataType.fromPgName(typeName) != null) return true;
        if (executor.database.isCustomEnum(typeName)) return true;
        if (executor.database.isDomain(typeName)) return true;
        if (executor.database.isCompositeType(typeName)) return true;
        return false;
    }

    private static boolean isKnownSqlKeyword(String name) {
        return Cols.setOf("select", "from", "where", "insert", "update", "delete",
                "values", "into", "set", "create", "alter", "drop", "table",
                "index", "view", "function", "procedure", "trigger",
                "if", "case", "when", "then", "else", "end", "and", "or", "not",
                "in", "exists", "between", "like", "is", "as", "on", "join",
                "left", "right", "inner", "outer", "cross", "full",
                "group", "order", "having", "limit", "offset", "union",
                "except", "intersect", "with", "returning", "row",
                "over", "partition", "rows", "range", "groups", "filter",
                "within", "window", "lateral", "distinct", "all", "any",
                "some", "array", "default", "null", "true", "false",
                "asc", "desc", "nulls", "first", "last", "fetch", "next",
                "coalesce", "greatest", "least", "cast",
                "count", "sum", "avg", "min", "max",
                "begin", "do", "call", "perform", "return").contains(name);
    }

    private void validateSqlFunctionStatement(Statement parsed, CreateFunctionStmt stmt,
                                               List<PgFunction.Param> params) {
        String retType = stmt.returnType();

        if (parsed instanceof SelectStmt
                && (((SelectStmt) parsed).targets() == null || ((SelectStmt) parsed).targets().isEmpty())
                && (((SelectStmt) parsed).from() == null || ((SelectStmt) parsed).from().isEmpty())) {
            SelectStmt sel = (SelectStmt) parsed;
            throw new MemgresException(
                    "return type mismatch in function declared to return " + (retType != null ? retType : "integer")
                    + "\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                    "42P13");
        }

        if (!stmt.isProcedure() && "void".equalsIgnoreCase(retType) && parsed instanceof SelectStmt) {
            throw new MemgresException(
                    "return type mismatch in function declared to return void\n  Detail: Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
                    "42P13");
        }

        if (parsed instanceof SelectStmt
                && ((SelectStmt) parsed).targets() != null && !((SelectStmt) parsed).targets().isEmpty()
                && retType != null && !retType.isEmpty()
                && !"void".equalsIgnoreCase(retType)
                && !retType.toUpperCase().startsWith("SETOF")) {
            SelectStmt sel = (SelectStmt) parsed;
            Expression firstExpr = sel.targets().get(0).expr();
            if (firstExpr instanceof Literal
                    && ((Literal) firstExpr).literalType() == Literal.LiteralType.STRING) {
                Literal lit = (Literal) firstExpr;
                if (isNumericType(retType)) {
                    throw new MemgresException(
                            "return type mismatch in function declared to return " + retType
                                    + "\n  Detail: Actual return type is text.", "42P13");
                }
            }
            if (firstExpr instanceof CastExpr) {
                CastExpr castExpr = (CastExpr) firstExpr;
                checkCastReturnTypeMismatch(castExpr.typeName(), retType);
            }
        }

        // Recurse into UNION/INTERSECT/EXCEPT sub-statements
        if (parsed instanceof com.memgres.engine.parser.ast.SetOpStmt) {
            com.memgres.engine.parser.ast.SetOpStmt setOp = (com.memgres.engine.parser.ast.SetOpStmt) parsed;
            validateSqlFunctionStatement(setOp.left(), stmt, params);
            validateSqlFunctionStatement(setOp.right(), stmt, params);
            return;
        }

        validateTableRefsInStatement(parsed);

        if (parsed instanceof SelectStmt && ((SelectStmt) parsed).from() != null) {
            SelectStmt sel = (SelectStmt) parsed;
            for (SelectStmt.FromItem fromItem : sel.from()) {
                if (fromItem instanceof SelectStmt.TableRef) {
                    SelectStmt.TableRef tr = (SelectStmt.TableRef) fromItem;
                    try {
                        String trSchema = tr.schema() != null ? tr.schema() : "public";
                        Table t = executor.resolveTable(trSchema, tr.table());
                        for (SelectStmt.SelectTarget target : sel.targets()) {
                            if (target.expr() instanceof ColumnRef
                                    && ((ColumnRef) target.expr()).table() == null && !"*".equals(((ColumnRef) target.expr()).column())) {
                                ColumnRef cr = (ColumnRef) target.expr();
                                boolean isParam = params.stream().anyMatch(p -> p.name() != null && p.name().equalsIgnoreCase(cr.column()));
                                if (!isParam && t.getColumnIndex(cr.column()) < 0) {
                                    throw new MemgresException("column \"" + cr.column() + "\" does not exist", "42703");
                                }
                            }
                        }
                    } catch (MemgresException me) {
                        if ("42703".equals(me.getSqlState()) || "42P01".equals(me.getSqlState())) throw me;
                    }
                }
            }
        }
    }

    private void validateTableRefsInStatement(Statement parsed) {
        if (parsed instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) parsed;
            resolveTableIfPresent(ins.schema(), ins.table());
            // Validate subquery in INSERT ... SELECT
            if (ins.selectStmt() != null) {
                validateTableRefsInStatement(ins.selectStmt());
            }
        } else if (parsed instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) parsed;
            resolveTableIfPresent(upd.schema(), upd.table());
            if (upd.from() != null) {
                for (SelectStmt.FromItem fi : upd.from()) {
                    validateFromItem(fi);
                }
            }
        } else if (parsed instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) parsed;
            resolveTableIfPresent(del.schema(), del.table());
            if (del.using() != null) {
                for (SelectStmt.FromItem fi : del.using()) {
                    validateFromItem(fi);
                }
            }
        } else if (parsed instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) parsed;
            if (sel.from() != null) {
                for (SelectStmt.FromItem fi : sel.from()) {
                    validateFromItem(fi);
                }
            }
        }
    }

    private void resolveTableIfPresent(String schema, String tableName) {
        if (tableName != null) {
            String s = schema != null ? schema : executor.defaultSchema();
            try {
                executor.resolveTable(s, tableName);
            } catch (MemgresException e) {
                if ("42P01".equals(e.getSqlState())) throw e;
            }
        }
    }

    private void validateFromItem(SelectStmt.FromItem fi) {
        if (fi instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
            resolveTableIfPresent(tr.schema(), tr.table());
        } else if (fi instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) fi;
            validateFromItem(join.left());
            validateFromItem(join.right());
        } else if (fi instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) fi;
            validateTableRefsInStatement(sub.subquery());
        }
    }

    private static final Set<String> BUILTIN_TYPES = Cols.setOf(
            "void", "trigger", "record", "anyelement", "anyarray", "anynonarray", "anyenum",
            "anyrange", "anymultirange", "anycompatible", "anycompatiblearray",
            "anycompatiblenonarray", "anycompatiblerange", "cstring", "internal",
            "opaque", "refcursor", "unknown", "event_trigger",
            "int", "int2", "int4", "int8", "integer", "bigint", "smallint",
            "serial", "bigserial", "smallserial",
            "numeric", "decimal", "real", "float", "float4", "float8",
            "double precision", "money",
            "text", "varchar", "character varying", "char", "character", "bpchar", "name",
            "boolean", "bool",
            "bytea", "uuid", "json", "jsonb", "xml",
            "date", "time", "timestamp", "timestamptz",
            "timestamp with time zone", "timestamp without time zone",
            "time with time zone", "time without time zone",
            "interval",
            "inet", "cidr", "macaddr", "macaddr8",
            "bit", "varbit", "bit varying",
            "tsvector", "tsquery",
            "point", "line", "lseg", "box", "path", "polygon", "circle",
            "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
            "int4multirange", "int8multirange", "nummultirange", "datemultirange",
            "tsmultirange", "tstzmultirange",
            "oid", "regproc", "regtype", "regclass", "regoper", "regprocedure",
            "regoperator", "regconfig", "regdictionary", "regnamespace", "regrole",
            "pg_lsn", "pg_snapshot", "txid_snapshot", "xid", "xid8", "cid", "tid",
            "aclitem");

    /**
     * Validate that a type name exists (built-in, enum, domain, composite, or table-as-type).
     * Used for validating return types and parameter types at CREATE FUNCTION time.
     */
    private void validateTypeExists(String typeName) {
        if (typeName == null || typeName.isEmpty()) return;
        String base = typeName.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
        if (base.isEmpty()) return;
        // TABLE return type with column list is validated separately
        if (base.equalsIgnoreCase("TABLE")) return;
        if (BUILTIN_TYPES.contains(base.toLowerCase())) return;
        if (DataType.fromPgName(base) != null) return;
        if (executor.database.isCustomEnum(base)) return;
        if (executor.database.isDomain(base)) return;
        if (executor.database.isCompositeType(base)) return;
        // Check if it's a table used as a composite type
        String schema = executor.defaultSchema();
        if (schema != null) {
            Schema s = executor.database.getSchema(schema);
            if (s != null && s.getTable(base) != null) return;
        }
        // Also check public schema
        Schema pub = executor.database.getSchema("public");
        if (pub != null && pub.getTable(base) != null) return;
        throw new MemgresException("type \"" + base + "\" does not exist", "42704");
    }

    /**
     * Validate default expression for function parameters.
     * Checks that function calls in defaults reference existing functions.
     */
    private void validateDefaultExpr(String defaultExpr) {
        if (defaultExpr == null) return;
        // Check for function call pattern: name()
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(").matcher(defaultExpr);
        while (m.find()) {
            String fnName = m.group(1).toLowerCase();
            // Skip built-in functions
            if (isBuiltinFunction(fnName)) continue;
            if (executor.database.getFunction(fnName) == null) {
                throw new MemgresException("function " + fnName + "() does not exist", "42883");
            }
        }
    }

    /** Check if function name looks like a known PG internal C function (used in CREATE OPERATOR/AGGREGATE). */
    private static boolean isKnownBuiltinFunction(String rawName) {
        String name = rawName.contains(".") ? rawName.substring(rawName.lastIndexOf('.') + 1) : rawName;
        // PG internal C functions often follow patterns: int4pl, int4eq, float8lt, texteq, etc.
        if (name.matches("(int[248]|float[48]|numeric|text|bool|date|timestamp|interval|oid|name|char|varchar|bytea|uuid|json|jsonb|box|point|circle|path|line|lseg|polygon|inet|macaddr|bit|varbit|cash|bpchar)\\w+")) return true;
        // Common aggregate transition/combine/final functions
        if (name.matches("(hash|btree|gin|gist|brin|spg|pg_)\\w+")) return true;
        if (name.matches("\\w+(recv|send|in|out|typmod|analyze|cmp|hash|eq|ne|lt|le|gt|ge|_agg|_accum|_combine|_final|_serialize|_deserialize|_transition)")) return true;
        return false;
    }

    private static boolean isBuiltinFunction(String name) {
        // Common built-in functions that don't need validation
        return Cols.setOf("now", "current_timestamp", "current_date", "current_time",
                "current_user", "session_user", "localtime", "localtimestamp",
                "clock_timestamp", "statement_timestamp", "transaction_timestamp",
                "gen_random_uuid", "random", "nextval", "currval", "setval",
                "coalesce", "nullif", "greatest", "least", "cast",
                "array_agg", "string_agg", "count", "sum", "avg", "min", "max",
                "row_number", "rank", "dense_rank", "lag", "lead",
                "upper", "lower", "trim", "btrim", "ltrim", "rtrim",
                "length", "char_length", "octet_length", "bit_length",
                "substring", "position", "overlay", "replace", "translate",
                "concat", "concat_ws", "format", "quote_ident", "quote_literal",
                "quote_nullable", "regexp_match", "regexp_matches", "regexp_replace",
                "to_char", "to_number", "to_date", "to_timestamp",
                "abs", "ceil", "ceiling", "floor", "round", "trunc", "sign", "sqrt",
                "power", "exp", "ln", "log", "mod", "div",
                "array_length", "array_upper", "array_lower", "unnest",
                "array_append", "array_prepend", "array_cat", "array_remove",
                "array_to_string", "string_to_array", "array_position", "array_positions",
                "array_ndims", "array_dims", "array_fill", "array_replace",
                "current_setting", "set_config",
                "split_part", "left", "right", "repeat", "reverse", "lpad", "rpad",
                "starts_with", "encode", "decode", "md5",
                "date_part", "date_trunc", "extract", "age", "make_interval",
                "row_to_json", "json_build_object", "json_build_array",
                "jsonb_build_object", "jsonb_build_array",
                "generate_series", "pg_typeof", "pg_sleep").contains(name);
    }

    /**
     * Validate PL/pgSQL DECLARE variable types at CREATE FUNCTION time.
     * PG 18 validates that declared variable types exist immediately.
     */
    private void validatePlpgsqlDeclarations(String body) {
        try {
            com.memgres.engine.plpgsql.PlpgsqlStatement.Block block =
                    com.memgres.engine.plpgsql.PlpgsqlParser.parse(body);
            for (com.memgres.engine.plpgsql.PlpgsqlStatement.VarDeclaration decl : block.declarations()) {
                String typeName = decl.typeName();
                if (typeName == null || typeName.isEmpty()) continue;
                if ("REFCURSOR".equalsIgnoreCase(typeName) || "refcursor".equals(typeName)) continue;
                if ("record".equalsIgnoreCase(typeName)) continue;

                // Handle %ROWTYPE
                if (typeName.toUpperCase().endsWith("%ROWTYPE")) {
                    String tableName = typeName.substring(0, typeName.length() - 8); // remove %ROWTYPE
                    if (tableName.endsWith(".")) tableName = tableName.substring(0, tableName.length() - 1);
                    // Validate table exists
                    try {
                        executor.resolveTable(executor.defaultSchema(), tableName);
                    } catch (MemgresException e) {
                        if ("42P01".equals(e.getSqlState())) throw e;
                    }
                    continue;
                }

                // Handle %TYPE
                if (typeName.toUpperCase().endsWith("%TYPE")) {
                    String ref = typeName.substring(0, typeName.length() - 5); // remove %TYPE
                    int dotIdx = ref.lastIndexOf('.');
                    if (dotIdx > 0) {
                        String tableName = ref.substring(0, dotIdx);
                        String colName = ref.substring(dotIdx + 1);
                        try {
                            Table table = executor.resolveTable(executor.defaultSchema(), tableName);
                            if (table.getColumnIndex(colName) < 0) {
                                throw new MemgresException("column \"" + colName + "\" does not exist", "42703");
                            }
                        } catch (MemgresException e) {
                            if ("42P01".equals(e.getSqlState()) || "42703".equals(e.getSqlState())) throw e;
                        }
                    }
                    continue;
                }

                // Regular type - validate existence
                validateTypeExists(typeName);
            }
        } catch (MemgresException e) {
            throw e;
        } catch (Exception ignored) {
            // Parse errors in the body are not validation errors at CREATE time
        }
    }

    /**
     * Checks if a PL/pgSQL body contains unsupported transaction commands
     * (SAVEPOINT, ROLLBACK TO SAVEPOINT) that PG rejects at creation time.
     */
    private boolean containsUnsupportedTransactionCommand(String body) {
        try {
            com.memgres.engine.plpgsql.PlpgsqlStatement.Block block =
                    com.memgres.engine.plpgsql.PlpgsqlParser.parse(body);
            return containsAbortStmt(block.body());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean containsAbortStmt(java.util.List<com.memgres.engine.plpgsql.PlpgsqlStatement> stmts) {
        for (com.memgres.engine.plpgsql.PlpgsqlStatement stmt : stmts) {
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.SavepointStmt) {
                return true;
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.Block) {
                com.memgres.engine.plpgsql.PlpgsqlStatement.Block b =
                        (com.memgres.engine.plpgsql.PlpgsqlStatement.Block) stmt;
                if (containsAbortStmt(b.body())) return true;
                for (com.memgres.engine.plpgsql.PlpgsqlStatement.ExceptionHandler h : b.exceptionHandlers()) {
                    if (containsAbortStmt(h.body())) return true;
                }
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt) {
                com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt ifStmt =
                        (com.memgres.engine.plpgsql.PlpgsqlStatement.IfStmt) stmt;
                if (containsAbortStmt(ifStmt.thenBody())) return true;
                if (containsAbortStmt(ifStmt.elseBody())) return true;
                for (com.memgres.engine.plpgsql.PlpgsqlStatement.ElsifClause c : ifStmt.elsifClauses()) {
                    if (containsAbortStmt(c.body())) return true;
                }
            }
            if (stmt instanceof com.memgres.engine.plpgsql.PlpgsqlStatement.LoopStmt) {
                if (containsAbortStmt(((com.memgres.engine.plpgsql.PlpgsqlStatement.LoopStmt) stmt).body())) return true;
            }
        }
        return false;
    }

    private static final Set<String> NUMERIC_TYPES = Cols.setOf(
            "int", "integer", "bigint", "smallint", "int2", "int4", "int8",
            "numeric", "decimal", "real", "float", "float4", "float8",
            "double precision", "money");
    private static final Set<String> STRING_TYPES = Cols.setOf(
            "text", "varchar", "character varying", "char", "character", "name");

    static boolean isNumericType(String type) {
        return NUMERIC_TYPES.contains(type.toLowerCase().trim());
    }

    static void checkCastReturnTypeMismatch(String castTo, String retType) {
        String ct = castTo.toLowerCase().trim();
        String rt = retType.toLowerCase().trim();
        boolean castIsString = STRING_TYPES.contains(ct) || ct.startsWith("character varying") || ct.startsWith("character(");
        boolean retIsNumeric = NUMERIC_TYPES.contains(rt);
        boolean retIsString = STRING_TYPES.contains(rt) || rt.startsWith("character varying");
        boolean castIsNumeric = NUMERIC_TYPES.contains(ct);
        if (castIsString && retIsNumeric) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + retType
                            + "\n  Detail: Actual return type is text.", "42P13");
        }
        if (castIsNumeric && retIsString) {
            throw new MemgresException(
                    "return type mismatch in function declared to return " + retType
                            + "\n  Detail: Actual return type is integer.", "42P13");
        }
    }

    /** Split SQL body into individual statements separated by semicolons. */
    private List<String> splitSqlStatements(String body) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c == '\'') {
                current.append(c);
                i++;
                while (i < body.length()) {
                    current.append(body.charAt(i));
                    if (body.charAt(i) == '\'' && (i + 1 >= body.length() || body.charAt(i + 1) != '\'')) {
                        i++;
                        break;
                    }
                    if (body.charAt(i) == '\'' && i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                        current.append(body.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    i++;
                }
            } else if (c == ';') {
                String stmt2 = current.toString().trim();
                if (!stmt2.isEmpty()) result.add(stmt2);
                current.setLength(0);
                i++;
            } else {
                current.append(c);
                i++;
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result.isEmpty() ? Cols.listOf(body.trim()) : result;
    }

    // ---- CALL ----

    QueryResult executeCall(CallStmt stmt) {
        PgFunction function;
        String callName = stmt.name();
        if (callName.contains(".")) {
            String[] parts = callName.split("\\.", 2);
            function = executor.database.getFunction(parts[0], parts[1]);
        } else {
            function = executor.database.getFunction(callName);
        }
        if (function == null) {
            String argTypes = stmt.args().isEmpty() ? "" : stmt.args().stream().map(a -> {
                try { Object v = executor.evalExpr(a, null);
                    return v instanceof Integer ? "integer" : v instanceof Long ? "bigint" : "unknown";
                } catch (Exception e) { return "unknown"; }
            }).collect(Collectors.joining(", "));
            throw new MemgresException("procedure " + stmt.name() + "(" + argTypes + ") does not exist", "42883");
        }
        if (!function.isProcedure()) {
            String argTypes = function.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.joining(", "));
            throw new MemgresException(stmt.name() + "(" + argTypes + ") is not a procedure", "42809");
        }
        // Count required IN params (minimum) and total params (maximum, including OUT/INOUT)
        int requiredInParams = 0, inParamCount = 0, totalParams = function.getParams().size();
        List<PgFunction.Param> outParams = new ArrayList<>();
        for (PgFunction.Param p : function.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode)) {
                outParams.add(p);
            } else if ("INOUT".equals(mode)) {
                inParamCount++;
                if (p.defaultExpr() == null) requiredInParams++;
                outParams.add(p);
            } else {
                inParamCount++;
                if (p.defaultExpr() == null) requiredInParams++;
            }
        }
        // PG allows CALL with either just IN args or all args (including OUT placeholders)
        int argCount = stmt.args().size();
        if (argCount != totalParams && (argCount < requiredInParams || argCount > inParamCount)) {
            String argTypes = stmt.args().isEmpty() ? "" : "integer";
            throw new MemgresException("procedure " + stmt.name() + "(" + argTypes + ") does not exist", "42883");
        }
        // Build args list: only pass IN/INOUT values to executeFunction
        List<Object> args = new ArrayList<>();
        int argIdx = 0;
        for (int i = 0; i < function.getParams().size(); i++) {
            PgFunction.Param p = function.getParams().get(i);
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode)) {
                if (argCount == totalParams) argIdx++; // skip the OUT placeholder arg
            } else {
                // IN or INOUT
                if (argIdx < stmt.args().size()) {
                    args.add(executor.evalExpr(stmt.args().get(argIdx++), null));
                }
            }
        }
        // Start implicit transaction for procedure (PG behavior: CALL in autocommit starts a txn)
        boolean implicitTxn = false;
        if (executor.session != null && executor.session.getStatus() == Session.TransactionStatus.IDLE) {
            executor.session.begin();
            implicitTxn = true;
        }
        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
        Object returnVal;
        boolean procedureFailed = false;
        try {
            returnVal = plExec.executeFunction(function, args);
        } catch (RuntimeException e) {
            procedureFailed = true;
            // Rollback the transaction on procedure error
            if (executor.session != null) {
                Session.TransactionStatus st = executor.session.getStatus();
                if (st == Session.TransactionStatus.IN_TRANSACTION || st == Session.TransactionStatus.FAILED) {
                    executor.session.rollback();
                }
            }
            throw e;
        } finally {
            // After procedure returns successfully, commit any trailing implicit transaction
            if (!procedureFailed && executor.session != null) {
                Session.TransactionStatus st = executor.session.getStatus();
                if (st == Session.TransactionStatus.IN_TRANSACTION) {
                    executor.session.commit();
                } else if (st == Session.TransactionStatus.FAILED) {
                    executor.session.rollback();
                }
            }
        }
        // If there are OUT/INOUT params, return a result set
        if (!outParams.isEmpty()) {
            List<Column> columns = new ArrayList<>();
            for (PgFunction.Param p : outParams) {
                String colName = p.name() != null ? p.name() : "column" + (columns.size() + 1);
                columns.add(new Column(colName, DataType.TEXT, true, false, null));
            }
            Object[] row;
            if (returnVal instanceof Object[]) {
                row = (Object[]) returnVal;
            } else {
                row = new Object[] { returnVal };
            }
            List<Object[]> rows = new ArrayList<>();
            rows.add(row);
            return QueryResult.select(columns, rows);
        }
        return QueryResult.command(QueryResult.Type.CALL, 0);
    }

    // ---- CREATE TRIGGER ----

    QueryResult executeCreateTrigger(CreateTriggerStmt stmt) {
        String triggerTableSchema = stmt.schema() != null ? stmt.schema() : executor.defaultSchema();
        if (stmt.table() != null) {
            executor.resolveTable(triggerTableSchema, stmt.table());
        }
        PgTrigger.Timing timing;
        switch (stmt.timing()) {
            case "BEFORE":
                timing = PgTrigger.Timing.BEFORE;
                break;
            case "AFTER":
                timing = PgTrigger.Timing.AFTER;
                break;
            case "INSTEAD OF":
                timing = PgTrigger.Timing.INSTEAD_OF;
                break;
            default:
                timing = PgTrigger.Timing.BEFORE;
                break;
        }
        boolean isView = stmt.table() != null && executor.database.hasView(stmt.table());
        if (timing == PgTrigger.Timing.INSTEAD_OF && !isView) {
            throw new MemgresException("INSTEAD OF triggers are only for views", "42P17");
        }
        if ((timing == PgTrigger.Timing.BEFORE || timing == PgTrigger.Timing.AFTER) && isView) {
            throw new MemgresException("\"" + stmt.table() + "\" is a view\n  Detail: Views cannot have BEFORE or AFTER row-level triggers.", "42809");
        }
        List<PgTrigger.Event> trigEvents = new ArrayList<>();
        for (String event : stmt.events()) {
            try {
                trigEvents.add(PgTrigger.Event.valueOf(event));
            } catch (IllegalArgumentException e) {
                throw new MemgresException("syntax error at or near \"" + event.toLowerCase() + "\"", "42601");
            }
        }
        if (stmt.functionName() != null) {
            PgFunction trigFunc = executor.database.getFunction(stmt.functionName());
            if (trigFunc == null) {
                throw new MemgresException("function " + stmt.functionName() + "() does not exist", "42883");
            }
            String trigRetType = trigFunc.getReturnType();
            if (trigRetType != null && !trigRetType.isEmpty()
                    && !"trigger".equalsIgnoreCase(trigRetType) && !"void".equalsIgnoreCase(trigRetType)) {
                throw new MemgresException("function " + stmt.functionName() + " must return type trigger", "42P17");
            }
        }
        if (stmt.whenClause() != null && stmt.table() != null) {
            try {
                Table trigTable = executor.resolveTable(triggerTableSchema, stmt.table());
                Expression whenExpr = com.memgres.engine.parser.Parser.parseExpression(stmt.whenClause());
                ddl.validateExprColumnRefs(whenExpr, trigTable, null);
            } catch (MemgresException me) {
                if ("42703".equals(me.getSqlState())) throw me;
            } catch (Exception ignored) {}
        }
        for (PgTrigger.Event trigEvent : trigEvents) {
            PgTrigger trigger = new PgTrigger(
                    stmt.name(), timing, trigEvent, stmt.table(), stmt.functionName(),
                    trigEvent == PgTrigger.Event.UPDATE ? stmt.updateOfColumns() : null,
                    stmt.newTransitionTable(), stmt.oldTransitionTable(), stmt.forEachStatement());
            trigger.setSchemaName(triggerTableSchema);
            executor.database.addTrigger(trigger);
        }
        return QueryResult.command(QueryResult.Type.CREATE_TRIGGER, 0);
    }

    // ---- DROP (generic) ----

    QueryResult executeDropStmt(DropStmt stmt) {
        switch (stmt.objectType()) {
            case VIEW:
                dropView(stmt);
                break;
            case SEQUENCE:
                dropSequence(stmt);
                break;
            case INDEX:
                dropIndex(stmt);
                break;
            case FUNCTION:
                dropFunction(stmt);
                break;
            case TRIGGER:
                dropTrigger(stmt);
                break;
            case TYPE:
                dropType(stmt);
                break;
            case SCHEMA:
                dropSchema(stmt);
                break;
            case DOMAIN: {
                if (!stmt.ifExists() && !executor.database.isDomain(stmt.name())) {
                    throw new MemgresException("type \"" + stmt.name() + "\" does not exist", "42704");
                }
                executor.database.removeDomain(stmt.name());
                break;
            }
            case POLICY:
                dropPolicy(stmt);
                break;
            case RULE:
                dropRule(stmt);
                break;
            case AGGREGATE: {
                executor.database.removeAggregate(stmt.name());
                break;
            }
            case EXTENSION:
            case COLLATION:
            case CAST:
            case CONVERSION: {
                break; // no-op
            }
            case OPERATOR: {
                // name is encoded as "opname(leftarg,rightarg)" by the parser
                String opKey = stmt.name();
                if (!executor.database.hasOperator(opKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator does not exist: " + stmt.name(), "42704");
                    }
                }
                executor.database.removeOperator(opKey);
                break;
            }
            case OPERATOR_FAMILY: {
                String famMethod = stmt.onTable() != null ? stmt.onTable() : "btree";
                String famKey = stmt.name().toLowerCase() + ":" + famMethod.toLowerCase();
                if (!executor.database.hasOperatorFamily(famKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator family \"" + stmt.name()
                                + "\" does not exist for access method \"" + famMethod + "\"", "42704");
                    }
                } else {
                    // CASCADE: also drop operator classes in this family
                    if (stmt.cascade()) {
                        executor.database.removeOperatorClassesByFamily(stmt.name());
                    }
                    executor.database.removeOperatorFamily(famKey);
                }
                break;
            }
            case OPERATOR_CLASS: {
                String clsMethod = stmt.onTable() != null ? stmt.onTable() : "btree";
                String clsKey = stmt.name().toLowerCase() + ":" + clsMethod.toLowerCase();
                if (!executor.database.hasOperatorClass(clsKey)) {
                    if (!stmt.ifExists()) {
                        throw new MemgresException("operator class \"" + stmt.name()
                                + "\" does not exist for access method \"" + clsMethod + "\"", "42704");
                    }
                }
                executor.database.removeOperatorClass(clsKey);
                break;
            }
        }
        return QueryResult.command(QueryResult.Type.DROP_TABLE, 0);
    }

    private void dropView(DropStmt stmt) {
        if (!stmt.ifExists()) {
            if (!executor.database.hasView(stmt.name())) {
                if (ddl.resolveTableOrNull(stmt.name()) != null) {
                    throw new MemgresException("\"" + stmt.name() + "\" is not a view", "42809");
                }
                throw new MemgresException("view \"" + stmt.name() + "\" does not exist", "42P01");
            }
        }
        Database.ViewDef oldView = executor.database.getView(stmt.name());
        if (oldView != null) {
            executor.recordUndo(new Session.DropViewUndo(stmt.name(), oldView));
        }
        String dropViewSchema = (oldView != null && oldView.schemaName() != null)
                ? oldView.schemaName() : executor.defaultSchema();
        executor.database.removeObjectOwner("view:" + dropViewSchema + "." + stmt.name());
        executor.database.removeView(stmt.name());
    }

    private void dropSequence(DropStmt stmt) {
        if (!stmt.ifExists()) {
            if (!executor.database.hasSequence(stmt.name())) {
                if (ddl.resolveTableOrNull(stmt.name()) != null || executor.database.hasView(stmt.name())) {
                    throw new MemgresException("\"" + stmt.name() + "\" is not a sequence", "42809");
                }
                throw new MemgresException("sequence \"" + stmt.name() + "\" does not exist", "42P01");
            }
        }
        Sequence oldSeq = executor.database.getSequence(stmt.name());
        if (oldSeq != null) {
            executor.recordUndo(new Session.DropSequenceUndo(stmt.name(), oldSeq));
        }
        executor.database.removeSequence(stmt.name());
        executor.database.removeObjectOwner("sequence:" + stmt.name());
    }

    private void dropIndex(DropStmt stmt) {
        if (!stmt.ifExists() && !executor.database.hasIndex(stmt.name())) {
            throw new MemgresException("index \"" + stmt.name() + "\" does not exist", "42704");
        }
        if (executor.database.hasIndex(stmt.name())) {
            String storedTable = executor.database.getIndexTable(stmt.name());
            if (storedTable != null) {
                try {
                    int dotIdx = storedTable.indexOf('.');
                    String schema = dotIdx >= 0 ? storedTable.substring(0, dotIdx) : "public";
                    String tableName = dotIdx >= 0 ? storedTable.substring(dotIdx + 1) : storedTable;
                    Table t = executor.resolveTable(schema, tableName);
                    t.getConstraints().removeIf(sc -> sc.getName().equalsIgnoreCase(stmt.name()));
                } catch (MemgresException ignored) {}
            }
            executor.database.removeIndex(stmt.name());
        }
    }

    private void dropFunction(DropStmt stmt) {
        if (executor.database.getFunction(stmt.name()) == null && !stmt.ifExists()) {
            throw new MemgresException("function " + stmt.name() + "() does not exist", "42883");
        }
        if (stmt.paramTypes() != null) {
            executor.database.removeFunction(stmt.name(), stmt.paramTypes());
        } else {
            executor.database.removeFunction(stmt.name());
        }
        executor.database.removeObjectOwner("function:" + stmt.name());
    }

    private void dropTrigger(DropStmt stmt) {
        if (stmt.onTable() != null) {
            if (!stmt.ifExists()) {
                List<PgTrigger> tableTriggers = executor.database.getTriggersForTable(stmt.onTable());
                boolean found = tableTriggers.stream()
                        .anyMatch(t -> t.getName().equalsIgnoreCase(stmt.name()));
                if (!found) {
                    throw new MemgresException("trigger \"" + stmt.name() + "\" for table \"" + stmt.onTable() + "\" does not exist", "42704");
                }
            }
            executor.database.removeTrigger(stmt.name(), stmt.onTable());
        }
    }

    private void dropType(DropStmt stmt) {
        CustomEnum existing = executor.database.getCustomEnum(stmt.name());
        boolean isComposite = executor.database.isCompositeType(stmt.name());
        if (existing == null && !isComposite && !stmt.ifExists()) {
            throw new MemgresException("type \"" + stmt.name() + "\" does not exist", "42704");
        }
        if (existing != null) executor.database.removeCustomEnum(stmt.name());
        if (isComposite) executor.database.removeCompositeType(stmt.name());
    }

    private void dropSchema(DropStmt stmt) {
        Schema schema = executor.database.getSchema(stmt.name());
        if (schema == null && !stmt.ifExists()) {
            throw new MemgresException("schema \"" + stmt.name() + "\" does not exist", "3F000");
        }
        if (schema != null) {
            if (stmt.cascade()) {
                List<String> tableNames = new ArrayList<>(schema.getTables().keySet());
                for (String tName : tableNames) {
                    executor.database.getAllTriggers().remove(tName.toLowerCase());
                }
                for (String tName : tableNames) {
                    executor.database.removePrivilegesOnObject("TABLE", tName);
                    executor.database.removePrivilegesOnObject("TABLE", stmt.name() + "." + tName);
                    executor.database.removeObjectOwner("table:" + stmt.name() + "." + tName);
                }
                executor.database.removePrivilegesOnObject("SCHEMA", stmt.name());
                tableNames.forEach(schema::removeTable);

                String schemaName = stmt.name().toLowerCase();
                Set<String> registeredObjects = new HashSet<>(executor.database.getSchemaObjects(schemaName));
                for (String entry : registeredObjects) {
                    int colonIdx = entry.indexOf(':');
                    if (colonIdx < 0) continue;
                    String objType = entry.substring(0, colonIdx);
                    String objName = entry.substring(colonIdx + 1);
                    switch (objType) {
                        case "enum":
                            executor.database.removeCustomEnum(objName);
                            break;
                        case "composite":
                            executor.database.removeCompositeType(objName);
                            break;
                        case "sequence":
                            executor.database.removeSequence(objName);
                            break;
                        case "domain":
                            executor.database.removeDomain(objName);
                            break;
                        case "index":
                            executor.database.removeIndex(objName);
                            break;
                        case "function":
                            executor.database.removeFunction(objName);
                            break;
                        case "view":
                            executor.database.removeView(objName);
                            break;
                    }
                }
                executor.database.removeSchemaObjects(schemaName);
            } else if (!schema.getTables().isEmpty()) {
                throw new MemgresException("cannot drop schema " + stmt.name() + " because other objects depend on it");
            }
            executor.database.removeSchema(stmt.name());
            executor.database.removeObjectOwner("schema:" + stmt.name());
        }
    }

    private void dropPolicy(DropStmt stmt) {
        if (stmt.onTable() != null) {
            Table table = executor.resolveTable("public", stmt.onTable());
            if (!stmt.ifExists()) {
                boolean found = table.getRlsPolicies().stream()
                        .anyMatch(p -> p.getName().equalsIgnoreCase(stmt.name()));
                if (!found) {
                    throw new MemgresException("policy \"" + stmt.name() + "\" for table \"" + stmt.onTable() + "\" does not exist", "42704");
                }
            }
            table.getRlsPolicies().removeIf(p -> p.getName().equalsIgnoreCase(stmt.name()));
        } else if (!stmt.ifExists()) {
            throw new MemgresException("must specify table for DROP POLICY");
        }
    }

    private void dropRule(DropStmt stmt) {
        if (stmt.onTable() != null && !stmt.ifExists()) {
            executor.resolveTable(executor.defaultSchema(), stmt.onTable());
        }
        String onTable = stmt.onTable() != null ? stmt.onTable() : "";
        if (executor.database.hasRule(stmt.name(), onTable)) {
            executor.database.removeRule(stmt.name(), onTable);
        } else if (!stmt.ifExists()) {
            throw new MemgresException("rule \"" + stmt.name() + "\" for relation \"" + onTable + "\" does not exist", "42704");
        }
    }

    // ---- CREATE SEQUENCE ----

    QueryResult executeCreateSequence(CreateSequenceStmt stmt) {
        String seqName = stmt.name();
        if (stmt.temporary() && executor.session != null) {
            seqName = executor.session.getTempSchemaName() + "." + seqName;
        }
        if (executor.database.hasSequence(seqName)) {
            if (stmt.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE SEQUENCE");
            throw new MemgresException("relation \"" + stmt.name() + "\" already exists", "42P07");
        }
        long incr = stmt.incrementBy() != null ? stmt.incrementBy() : 1L;
        if (incr == 0) {
            throw new MemgresException("INCREMENT must not be zero", "22023");
        }
        long minVal = stmt.minValue() != null ? stmt.minValue() : (incr > 0 ? 1L : Long.MIN_VALUE);
        long maxVal = stmt.maxValue() != null ? stmt.maxValue() : (incr > 0 ? Long.MAX_VALUE : -1L);
        if (minVal > maxVal) {
            throw new MemgresException("MINVALUE (" + minVal + ") must be less than MAXVALUE (" + maxVal + ")", "22023");
        }
        Sequence seq = new Sequence(seqName, stmt.startWith(), incr, minVal, maxVal);
        if (stmt.cycle() != null) seq.setCycle(stmt.cycle());
        executor.database.addSequence(seq);
        executor.database.registerSchemaObject(executor.defaultSchema(), "sequence", seqName);
        executor.recordUndo(new Session.CreateSequenceUndo(seqName));
        executor.database.setObjectOwner("sequence:" + seqName, executor.sessionUser());
        return QueryResult.message(QueryResult.Type.SET, "CREATE SEQUENCE");
    }

    // ---- ALTER SEQUENCE ----

    QueryResult executeAlterSequence(AlterSequenceStmt stmt) {
        Sequence seq = executor.database.getSequence(stmt.name());
        if (seq == null) throw new MemgresException("relation \"" + stmt.name() + "\" does not exist", "42P01");
        if (stmt.incrementBy() != null) seq.setIncrementBy(stmt.incrementBy());
        if (stmt.minValue() != null) seq.setMinValue(stmt.minValue());
        if (stmt.maxValue() != null) seq.setMaxValue(stmt.maxValue());
        if (stmt.startWith() != null) seq.setStartWith(stmt.startWith());
        if (stmt.cycle() != null) seq.setCycle(stmt.cycle());
        if (stmt.restart()) {
            if (stmt.restartWith() != null) seq.restart(stmt.restartWith());
            else seq.restart();
        }
        if (stmt.ownerTo() != null) {
            String newOwner = ddl.resolveOwnerName(stmt.ownerTo());
            if (!executor.database.hasRole(newOwner)) {
                throw new MemgresException("role \"" + newOwner + "\" does not exist", "42704");
            }
            executor.database.setObjectOwner("sequence:" + stmt.name(), newOwner);
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER SEQUENCE");
    }

    // ---- CREATE DOMAIN ----

    QueryResult executeCreateDomain(CreateDomainStmt stmt) {
        if (executor.database.getDomain(stmt.name()) != null) {
            throw new MemgresException("type \"" + stmt.name() + "\" already exists", "42710");
        }
        String baseTypeName = stmt.baseType().replaceAll("\\(.*\\)", "").trim().replace("[]", "").trim();
        DataType baseType = DataType.fromPgName(baseTypeName);
        if (baseType == null) {
            DomainType parent = executor.database.getDomain(baseTypeName);
            if (parent != null) baseType = parent.getBaseType();
            else throw new MemgresException("type \"" + baseTypeName + "\" does not exist");
        }

        executor.database.addDomain(new DomainType(
                stmt.name(), baseType, baseTypeName, stmt.notNull(),
                stmt.checkExpr() != null ? stmt.checkExpr().toString() : null,
                stmt.checkExpr(),
                stmt.defaultExpr() != null ? DdlExecutor.exprToDefaultString(stmt.defaultExpr()) : null
        ));
        executor.database.registerSchemaObject(executor.defaultSchema(), "domain", stmt.name());
        return QueryResult.message(QueryResult.Type.SET, "CREATE DOMAIN");
    }

    // ---- ALTER DOMAIN ----

    QueryResult executeAlterDomain(AlterDomainStmt stmt) {
        DomainType domain = executor.database.getDomain(stmt.domainName());
        if (domain == null) {
            throw new MemgresException("type \"" + stmt.domainName() + "\" does not exist", "42704");
        }
        switch (stmt.action()) {
            case "SET_DEFAULT":
                domain.setDefaultValue(stmt.defaultValue());
                break;
            case "DROP_DEFAULT":
                domain.setDefaultValue(null);
                break;
            case "ADD_CONSTRAINT": {
                if (stmt.checkExpr() != null) {
                    try {
                        Table valueTable = new Table("_domain_check",
                                Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                        RowContext checkCtx = new RowContext(valueTable, null, new Object[]{null});
                        executor.evalExpr(stmt.checkExpr(), checkCtx);
                    } catch (MemgresException e) {
                        if ("42883".equals(e.getSqlState())) throw e;
                    }
                }
                for (Schema schema : executor.database.getSchemas().values()) {
                    for (Table table : schema.getTables().values()) {
                        for (int ci = 0; ci < table.getColumns().size(); ci++) {
                            Column col = table.getColumns().get(ci);
                            if (stmt.domainName().equals(col.getDomainTypeName())) {
                                Table valTable = new Table("_domain_check",
                                        Cols.listOf(new Column("value", domain.getBaseType(), true, false, null)));
                                for (Object[] row : table.getRows()) {
                                    Object val = row[ci];
                                    if (val != null && stmt.checkExpr() != null) {
                                        RowContext valCtx = new RowContext(valTable, null, new Object[]{val});
                                        Object result = executor.evalExpr(stmt.checkExpr(), valCtx);
                                        if (!executor.isTruthy(result)) {
                                            throw new MemgresException(
                                                    "value for domain " + stmt.domainName() + " violates check constraint \"" + stmt.constraintName() + "\"",
                                                    "23514");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                domain.addConstraint(stmt.constraintName(), stmt.rawCheckExpr(), stmt.checkExpr());
                break;
            }
            case "DROP_CONSTRAINT":
                domain.removeConstraint(stmt.constraintName());
                break;
            case "VALIDATE": {
                break;
            }
            case "NO_OP": {
                break;
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER DOMAIN");
    }

    // ---- CREATE INDEX ----

    QueryResult executeCreateIndex(CreateIndexStmt s) {
        if (s.name() != null && executor.database.hasIndex(s.name())) {
            if (s.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE INDEX");
            throw new MemgresException("relation \"" + s.name() + "\" already exists", "42P07");
        }
        // Validate index columns exist on the target table
        if (s.table() != null && s.columns() != null) {
            try {
                String idxSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table idxTable = executor.resolveTable(idxSchema, s.table());
                for (String col : s.columns()) {
                    // Skip expression-based index columns (contain parens, operators, or spaces)
                    if (col.contains("(") || col.contains(")") || col.contains(" ")
                            || col.contains("+") || col.contains("*") || col.contains("/") || col.contains("||")) {
                        // Expression-based index column; try to evaluate against a dummy row to catch type errors
                        String exprStr = col.trim();
                        // Reject volatile/stable functions and operators in indexes
                        DdlExecutor.checkExpressionImmutability(exprStr, executor.database,
                                "functions in index expression must be marked IMMUTABLE");
                        // Try to evaluate the expression against a dummy row to catch type errors
                        try {
                            // Strip outer wrapper parens like ((a + b)) → a + b, but NOT function call parens
                            String exprToParse = exprStr;
                            while (exprToParse.startsWith("(") && exprToParse.endsWith(")")) {
                                // Check that the outer parens are matched (not part of a function call)
                                int depth = 0;
                                boolean outerMatched = false;
                                for (int ci = 0; ci < exprToParse.length(); ci++) {
                                    if (exprToParse.charAt(ci) == '(') depth++;
                                    else if (exprToParse.charAt(ci) == ')') depth--;
                                    if (depth == 0 && ci == exprToParse.length() - 1) outerMatched = true;
                                    if (depth == 0 && ci < exprToParse.length() - 1) break;
                                }
                                if (outerMatched) {
                                    exprToParse = exprToParse.substring(1, exprToParse.length() - 1).trim();
                                } else {
                                    break;
                                }
                            }
                            Expression idxExpr =
                                com.memgres.engine.parser.Parser.parseExpression(exprToParse);
                            // Create a dummy row context with default non-null values
                            Object[] dummyRow = new Object[idxTable.getColumns().size()];
                            for (int di = 0; di < idxTable.getColumns().size(); di++) {
                                Column dc = idxTable.getColumns().get(di);
                                switch (dc.getType()) {
                                    case INTEGER:
                                    case BIGINT:
                                    case SMALLINT:
                                        dummyRow[di] = 0L;
                                        break;
                                    case NUMERIC:
                                    case DOUBLE_PRECISION:
                                    case REAL:
                                        dummyRow[di] = 0.0;
                                        break;
                                    case BOOLEAN:
                                        dummyRow[di] = false;
                                        break;
                                    default:
                                        dummyRow[di] = "dummy";
                                        break;
                                }
                            }
                            // Compute virtual columns on the dummy row so expression
                            // indexes referencing virtual columns evaluate correctly
                            if (executor.dmlExecutor.hasVirtualColumns(idxTable)) {
                                dummyRow = executor.dmlExecutor.computeVirtualColumns(idxTable, dummyRow);
                            }
                            RowContext dummyCtx = new RowContext(idxTable, idxTable.getName(), dummyRow);
                            executor.evalExpr(idxExpr, dummyCtx);
                        } catch (MemgresException me) {
                            if ("42883".equals(me.getSqlState()) || "42804".equals(me.getSqlState())
                                    || "42702".equals(me.getSqlState()) || "42P18".equals(me.getSqlState())
                                    || me.getMessage() != null && me.getMessage().contains("operator does not exist")) {
                                throw me;
                            }
                            // Other eval errors (e.g., null arithmetic), try string extraction fallback
                            String stripped = exprStr;
                            while (stripped.startsWith("(")) stripped = stripped.substring(1).trim();
                            int parenIdx = stripped.indexOf('(');
                            if (parenIdx > 0) {
                                String funcName = stripped.substring(0, parenIdx).trim().toLowerCase();
                                if (!funcName.isEmpty()) {
                                    if (executor.database.getFunction(funcName) != null) continue;
                                    try {
                                        executor.functionEvaluator.evalFunction(
                                            new FunctionCallExpr(funcName,
                                                Cols.listOf(Literal.ofNull())), null);
                                    } catch (MemgresException me2) {
                                        if ("42883".equals(me2.getSqlState())) {
                                            throw new MemgresException("function " + funcName + "(text) does not exist", "42883");
                                        }
                                    } catch (Exception ignored2) {}
                                }
                            }
                        } catch (Exception ignored) {}
                        continue;
                    }
                    if (idxTable.getColumnIndex(col) < 0) {
                        throw new MemgresException("column \"" + col + "\" does not exist", "42703");
                    }
                }
                // Validate WHERE predicate (partial index condition) references existing columns
                if (s.whereClause() != null) {
                    try {
                        Expression predExpr =
                            com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        // Walk the expression to find column references
                        ddl.validateExprColumnRefs(predExpr, idxTable, null);
                    } catch (MemgresException me) {
                        if ("42703".equals(me.getSqlState())) throw me;
                        // Other errors ignored
                    } catch (Exception ignored) {}
                }
            } catch (MemgresException e) {
                if ("42703".equals(e.getSqlState()) || "42883".equals(e.getSqlState())
                        || "0A000".equals(e.getSqlState()) || "42804".equals(e.getSqlState())
                        || "42P17".equals(e.getSqlState())) throw e;
                // Re-throw table-not-found only if it's also not a view (materialized views are valid index targets)
                if ("42P01".equals(e.getSqlState()) && !executor.database.hasView(s.table())) throw e;
                // Other errors (e.g., schema issues, table is a view); skip column validation
            }
        }
        // For UNIQUE indexes, validate existing data for uniqueness before creating the index
        if (s.unique() && s.table() != null && s.columns() != null) {
            try {
                String valSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table valTable = executor.resolveTable(valSchema, s.table());
                List<Object[]> existingRows = valTable.getRows();
                if (existingRows != null && existingRows.size() > 1) {
                    // Parse WHERE predicate if present (partial unique index)
                    Expression wherePred = null;
                    if (s.whereClause() != null) {
                        try {
                            wherePred = com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        } catch (Exception ignored) {}
                    }
                    // Parse expression columns if any
                    boolean hasExprCols = s.columns().stream().anyMatch(c ->
                            c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                            || c.contains("*") || c.contains("/") || c.contains("||"));
                    List<Expression> parsedExprs = null;
                    if (hasExprCols) {
                        parsedExprs = new ArrayList<>();
                        for (String col : s.columns()) {
                            try {
                                parsedExprs.add(com.memgres.engine.parser.Parser.parseExpression(col));
                            } catch (Exception e) {
                                parsedExprs = null;
                                break;
                            }
                        }
                    }
                    // Collect key values for rows that pass the WHERE predicate
                    Set<String> seenKeys = new HashSet<>();
                    boolean idxHasVirtual = executor.dmlExecutor.hasVirtualColumns(valTable);
                    for (Object[] row : existingRows) {
                        Object[] evalRow = idxHasVirtual ? executor.dmlExecutor.computeVirtualColumns(valTable, row) : row;
                        RowContext rowCtx = new RowContext(valTable, valTable.getName(), evalRow);
                        // Check WHERE predicate and skip rows that don't match
                        if (wherePred != null) {
                            try {
                                Object predResult = executor.evalExpr(wherePred, rowCtx);
                                if (!Boolean.TRUE.equals(predResult)) continue;
                            } catch (Exception e) {
                                continue;
                            }
                        }
                        // Compute key values
                        StringBuilder keyBuilder = new StringBuilder();
                        if (parsedExprs != null) {
                            for (Expression expr : parsedExprs) {
                                try {
                                    Object val = executor.evalExpr(expr, rowCtx);
                                    keyBuilder.append(val == null ? "\0NULL\0" : val.toString()).append('\1');
                                } catch (Exception e) {
                                    keyBuilder.append("\0ERR\0").append('\1');
                                }
                            }
                        } else {
                            for (String col : s.columns()) {
                                int ci = valTable.getColumnIndex(col);
                                if (ci >= 0) {
                                    Object val = evalRow[ci];
                                    keyBuilder.append(val == null ? "\0NULL\0" : val.toString()).append('\1');
                                }
                            }
                        }
                        String key = keyBuilder.toString();
                        if (!key.contains("\0NULL\0") && !seenKeys.add(key)) {
                            String idxName = s.name() != null ? s.name() : s.table() + "_unique";
                            throw new MemgresException(
                                "could not create unique index \"" + idxName + "\"\n  "
                                + "Detail: Key already exists.", "23505");
                        }
                    }
                }
            } catch (MemgresException e) {
                if ("23505".equals(e.getSqlState())) throw e;
                // Other errors (table not found, etc.); skip validation
            }
        }
        if (s.name() != null && s.columns() != null) {
            executor.database.addIndex(s.name(), s.columns());
            // Store index metadata (table name, uniqueness, method, WHERE clause)
            String idxSchemaForMeta = s.schema() != null ? s.schema() : executor.defaultSchema();
            executor.database.addIndexMeta(s.name(), idxSchemaForMeta + "." + s.table(), s.unique(),
                    s.method(), s.whereClause());
            executor.database.registerSchemaObject(idxSchemaForMeta, "index", s.name());
            executor.recordUndo(new Session.CreateIndexUndo(s.name()));
            // Build a real TableIndex for simple column indexes (non-expression, non-partial)
            // so they can be used for index scans in SELECT queries
            if (s.table() != null && s.whereClause() == null) {
                boolean hasExprCols = s.columns().stream().anyMatch(c ->
                        c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                        || c.contains("*") || c.contains("/") || c.contains("||"));
                if (!hasExprCols) {
                    try {
                        Table idxTable2 = executor.resolveTable(idxSchemaForMeta, s.table());
                        int[] colIndices = new int[s.columns().size()];
                        boolean allFound = true;
                        for (int ci = 0; ci < s.columns().size(); ci++) {
                            int idx = idxTable2.getColumnIndex(s.columns().get(ci));
                            if (idx < 0) { allFound = false; break; }
                            colIndices[ci] = idx;
                        }
                        // Skip building index on virtual columns (computed on read, not stored)
                        boolean hasVirtualCol = false;
                        if (allFound) {
                            for (int ci : colIndices) {
                                if (idxTable2.getColumns().get(ci).isVirtual()) {
                                    hasVirtualCol = true;
                                    break;
                                }
                            }
                        }
                        if (allFound && !hasVirtualCol && idxTable2.getIndex(s.name()) == null) {
                            TableIndex tableIdx = new TableIndex(s.name(), colIndices, s.unique());
                            idxTable2.buildIndex(tableIdx);
                        }
                    } catch (MemgresException ignored) {}
                }
            }
        }
        // For UNIQUE indexes, also add a UNIQUE constraint to enforce uniqueness
        if (s.unique() && s.table() != null && s.columns() != null) {
            try {
                String uIdxSchema = s.schema() != null ? s.schema() : executor.defaultSchema();
                Table idxTable = executor.resolveTable(uIdxSchema, s.table());
                String constraintName = s.name() != null ? s.name() : s.table() + "_unique";
                StoredConstraint sc = StoredConstraint.unique(constraintName, s.columns());
                // For partial unique indexes, parse and store the WHERE predicate
                if (s.whereClause() != null) {
                    try {
                        Expression predExpr = com.memgres.engine.parser.Parser.parseExpression(s.whereClause());
                        sc.setWhereExpr(predExpr);
                    } catch (Exception ignored) {}
                }
                // For expression-based indexes (e.g., lower(email), (a + b)), parse and store the expressions
                // Detect expressions: contains parens, operators, or spaces (not a simple column name)
                boolean hasExprCols = s.columns().stream().anyMatch(c ->
                        c.contains("(") || c.contains(" ") || c.contains("+") || c.contains("-")
                        || c.contains("*") || c.contains("/") || c.contains("||"));
                if (hasExprCols) {
                    List<Expression> exprCols = new ArrayList<>();
                    for (String col : s.columns()) {
                        try {
                            exprCols.add(com.memgres.engine.parser.Parser.parseExpression(col));
                        } catch (Exception e) {
                            exprCols = null;
                            break;
                        }
                    }
                    if (exprCols != null) {
                        sc.setExpressionColumns(exprCols);
                    }
                }
                sc.setFromIndex(true);
                idxTable.addConstraint(sc);
            } catch (MemgresException ignored) {
                // Table might not exist yet (e.g., on materialized views)
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "CREATE INDEX");
    }
}
