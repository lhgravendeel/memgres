package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DDL statement parsing (CREATE, DROP, ALTER, TRUNCATE), extracted from Parser to reduce class size.
 * Orchestrates delegation to specialized parsers: DdlTableParser, DdlFunctionParser, DdlIndexParser,
 * DdlRoleParser, DdlPolicyParser, DdlAlterActionParser.
 */
class DdlParser {
    final Parser parser;
    private final DdlTableParser tableParser;
    private final DdlFunctionParser functionParser;
    private final DdlIndexParser indexParser;
    private final DdlRoleParser roleParser;
    private final DdlPolicyParser policyParser;
    private final DdlAlterActionParser alterActionParser;

    DdlParser(Parser parser) {
        this.parser = parser;
        this.tableParser = new DdlTableParser(parser);
        this.functionParser = new DdlFunctionParser(parser);
        this.indexParser = new DdlIndexParser(parser);
        this.roleParser = new DdlRoleParser(parser);
        this.policyParser = new DdlPolicyParser(parser);
        this.alterActionParser = new DdlAlterActionParser(parser, tableParser);
    }

    // ---- Delegate entry points (called from Parser.java) ----

    void consumeUntilParen() {
        DdlTableParser.consumeUntilParen(parser);
    }

    CallStmt parseCall() {
        return functionParser.parseCall();
    }

    // ---- CREATE dispatcher ----

    Statement parseCreate() {
        parser.expectKeyword("CREATE");

        boolean orReplace = parser.matchKeywords("OR", "REPLACE");
        boolean temporary = parser.matchKeyword("TEMPORARY") || parser.matchKeyword("TEMP");
        boolean unlogged = parser.matchKeyword("UNLOGGED");
        boolean unique = parser.matchKeyword("UNIQUE");
        parser.matchKeyword("TRUSTED");
        parser.matchKeyword("PROCEDURAL");

        if (parser.matchKeyword("TABLE")) return tableParser.parseCreateTable(temporary);
        if (parser.matchKeyword("TYPE")) return parseCreateType();
        if (parser.matchKeyword("FUNCTION")) return functionParser.parseCreateFunction(orReplace, false);
        if (parser.matchKeyword("PROCEDURE")) return functionParser.parseCreateFunction(orReplace, true);
        if (parser.matchKeyword("TRIGGER")) return parseCreateTrigger(orReplace);
        if (parser.matchKeywords("CONSTRAINT", "TRIGGER")) return parseCreateTrigger(false);
        if (parser.matchKeyword("EXTENSION")) return parseCreateExtension();
        if (parser.matchKeyword("INDEX")) return indexParser.parseCreateIndex(unique, false);
        if (parser.matchKeyword("VIEW")) return parseCreateView(orReplace, false);
        if (parser.matchKeyword("MATERIALIZED")) {
            parser.expectKeyword("VIEW");
            return parseCreateView(orReplace, true);
        }
        if (parser.matchKeyword("SEQUENCE")) return parseCreateSequence(temporary);
        if (parser.matchKeyword("DOMAIN")) return parseCreateDomain();
        if (parser.matchKeyword("SCHEMA")) return parseCreateSchema();
        if (parser.matchKeyword("POLICY")) return policyParser.parseCreatePolicy();
        if (parser.matchKeyword("ROLE")) return roleParser.parseCreateRole(false);
        if (parser.matchKeywords("USER", "MAPPING")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_noop", "ok");
        }
        if (parser.matchKeyword("USER")) return roleParser.parseCreateRole(true);
        if (unique) {
            parser.expectKeyword("INDEX");
            return indexParser.parseCreateIndex(true, false);
        }

        if (orReplace && parser.matchKeyword("VIEW")) return parseCreateView(true, false);
        if (orReplace && parser.matchKeyword("TRIGGER")) return parseCreateTrigger(true);
        if (parser.matchKeyword("GROUP")) return roleParser.parseCreateRole(false);
        if (parser.matchKeyword("RULE")) return parseCreateRule();

        if (parser.matchKeyword("AGGREGATE")) return parseCreateAggregate();

        if (parser.matchKeywords("OPERATOR", "CLASS")) return parseCreateOperatorClass();
        if (parser.matchKeywords("OPERATOR", "FAMILY")) return parseCreateOperatorFamily();
        if (parser.matchKeyword("OPERATOR")) return parseCreateOperator();

        // CREATE LANGUAGE — no-op, but PG 18 does not support IF NOT EXISTS
        if (parser.matchKeyword("LANGUAGE")) {
            if (parser.checkKeyword("IF")) {
                throw new MemgresException(
                        "syntax error at or near \"NOT\"", "42601");
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_noop", "ok");
        }

        // No-op CREATE targets (accepted but not functionally implemented)
        if (parser.matchKeyword("COLLATION") || parser.matchKeyword("CAST")
                || parser.matchKeyword("CONVERSION")
                || parser.matchKeywords("DEFAULT", "CONVERSION")
                || parser.matchKeywords("TEXT", "SEARCH")
                || parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")
                || parser.matchKeyword("SERVER")
                || parser.matchKeywords("FOREIGN", "TABLE")
                || parser.matchKeyword("PUBLICATION")
                || parser.matchKeyword("SUBSCRIPTION")
                || parser.matchKeyword("TABLESPACE")
                || parser.matchKeywords("EVENT", "TRIGGER")
                || parser.matchKeyword("TRANSFORM")
                || parser.matchKeywords("ACCESS", "METHOD")
                || parser.matchKeyword("STATISTICS")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_noop", "ok");
        }

        // CREATE DATABASE dbname [options...]
        if (parser.matchKeyword("DATABASE")) {
            String dbName = parser.readIdentifier();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_database", dbName);
        }

        throw new ParseException("Unsupported CREATE statement", parser.peek());
    }

    // ---- DROP dispatcher ----

    Statement parseDrop() {
        parser.expectKeyword("DROP");

        if (parser.matchKeywords("USER", "MAPPING")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        if (parser.matchKeyword("ROLE") || parser.matchKeyword("USER") || parser.matchKeyword("GROUP")) {
            return roleParser.parseDropRole();
        }
        if (parser.matchKeyword("POLICY")) {
            return policyParser.parseDropPolicy();
        }
        if (parser.matchKeyword("OWNED")) {
            parser.expectKeyword("BY");
            String role = parser.readIdentifier();
            boolean cascade = parser.matchKeyword("CASCADE");
            if (!cascade) parser.matchKeyword("RESTRICT");
            return new DropOwnedStmt(role, cascade);
        }

        if (parser.matchKeyword("TABLE")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String schema = null;
            String name = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) { schema = name; name = parser.readIdentifier(); }
            List<String> additionalTables = new ArrayList<>();
            while (parser.match(TokenType.COMMA)) {
                String extra = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) extra = parser.readIdentifier();
                additionalTables.add(extra);
            }
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new DropTableStmt(schema, name, ifExists, cascade, additionalTables);
        }

        DropStmt.ObjectType objectType;
        if (parser.matchKeyword("FUNCTION")) objectType = DropStmt.ObjectType.FUNCTION;
        else if (parser.matchKeyword("PROCEDURE")) objectType = DropStmt.ObjectType.FUNCTION;
        else if (parser.matchKeyword("ROUTINE")) objectType = DropStmt.ObjectType.FUNCTION;
        else if (parser.matchKeyword("TRIGGER")) objectType = DropStmt.ObjectType.TRIGGER;
        else if (parser.matchKeyword("TYPE")) objectType = DropStmt.ObjectType.TYPE;
        else if (parser.matchKeyword("INDEX")) objectType = DropStmt.ObjectType.INDEX;
        else if (parser.matchKeyword("VIEW")) objectType = DropStmt.ObjectType.VIEW;
        else if (parser.matchKeyword("SEQUENCE")) objectType = DropStmt.ObjectType.SEQUENCE;
        else if (parser.matchKeyword("SCHEMA")) objectType = DropStmt.ObjectType.SCHEMA;
        else if (parser.matchKeyword("DOMAIN")) objectType = DropStmt.ObjectType.DOMAIN;
        else if (parser.matchKeyword("MATERIALIZED")) { parser.expectKeyword("VIEW"); objectType = DropStmt.ObjectType.VIEW; }
        else if (parser.matchKeyword("EXTENSION")) objectType = DropStmt.ObjectType.EXTENSION;
        else if (parser.matchKeyword("RULE")) objectType = DropStmt.ObjectType.RULE;
        else if (parser.matchKeyword("COLLATION")) objectType = DropStmt.ObjectType.COLLATION;
        else if (parser.matchKeyword("CAST")) objectType = DropStmt.ObjectType.CAST;
        else if (parser.matchKeyword("CONVERSION")) objectType = DropStmt.ObjectType.CONVERSION;
        else if (parser.matchKeyword("AGGREGATE")) objectType = DropStmt.ObjectType.AGGREGATE;
        else if (parser.matchKeywords("OPERATOR", "CLASS")) objectType = DropStmt.ObjectType.OPERATOR_CLASS;
        else if (parser.matchKeywords("OPERATOR", "FAMILY")) objectType = DropStmt.ObjectType.OPERATOR_FAMILY;
        else if (parser.matchKeyword("OPERATOR")) objectType = DropStmt.ObjectType.OPERATOR;
        else if (parser.matchKeywords("TEXT", "SEARCH")) {
            parser.matchKeyword("CONFIGURATION");
            parser.matchKeyword("DICTIONARY");
            parser.matchKeyword("PARSER");
            parser.matchKeyword("TEMPLATE");
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")
                || parser.matchKeyword("SERVER")
                || parser.matchKeywords("FOREIGN", "TABLE")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeyword("PUBLICATION") || parser.matchKeyword("SUBSCRIPTION")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeyword("DATABASE")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String dbName = parser.readIdentifier();
            boolean force = false;
            // WITH (FORCE) or (FORCE)
            if (parser.matchKeyword("WITH")) {
                // WITH (FORCE)
            }
            if (parser.match(TokenType.LEFT_PAREN)) {
                if (parser.matchKeyword("FORCE")) {
                    force = true;
                }
                parser.match(TokenType.RIGHT_PAREN);
            } else if (parser.matchKeyword("FORCE")) {
                force = true;
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            String action = "drop_database";
            if (ifExists) action += "_if_exists";
            if (force) action += "_force";
            return new SetStmt(action, dbName);
        }
        else if (parser.matchKeyword("TABLESPACE")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeyword("LANGUAGE")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeywords("EVENT", "TRIGGER")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeyword("TRANSFORM")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeywords("ACCESS", "METHOD")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else if (parser.matchKeyword("STATISTICS")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_noop", "ok");
        }
        else throw new ParseException("Unsupported DROP target", parser.peek());

        if (objectType == DropStmt.ObjectType.INDEX) parser.matchKeyword("CONCURRENTLY");
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        if (objectType == DropStmt.ObjectType.INDEX) parser.matchKeyword("CONCURRENTLY");

        if (objectType == DropStmt.ObjectType.CAST) {
            if (parser.check(TokenType.LEFT_PAREN)) DdlTableParser.consumeUntilParen(parser);
            boolean cascade2 = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new DropStmt(objectType, "cast", null, ifExists, cascade2);
        }

        String name;
        if (objectType == DropStmt.ObjectType.OPERATOR) {
            StringBuilder sb = new StringBuilder();
            while (!parser.isAtEnd() && !parser.check(TokenType.LEFT_PAREN) && !parser.check(TokenType.SEMICOLON)
                    && !parser.checkKeyword("CASCADE") && !parser.checkKeyword("RESTRICT")) {
                sb.append(parser.advance().value());
            }
            name = sb.toString().trim();
        } else {
            name = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) name = parser.readIdentifier();
        }

        java.util.List<String> funcParamTypes = null;
        if ((objectType == DropStmt.ObjectType.FUNCTION || objectType == DropStmt.ObjectType.AGGREGATE)
                && parser.check(TokenType.LEFT_PAREN)) {
            funcParamTypes = parseFunctionDropParamTypes();
        }
        if (objectType == DropStmt.ObjectType.OPERATOR && parser.check(TokenType.LEFT_PAREN)) {
            // Capture arg types for operator overload resolution
            parser.expect(TokenType.LEFT_PAREN);
            StringBuilder argKey = new StringBuilder();
            int depth = 1;
            while (!parser.isAtEnd() && depth > 0) {
                Token t = parser.peek();
                if (t.type() == TokenType.LEFT_PAREN) depth++;
                else if (t.type() == TokenType.RIGHT_PAREN) { depth--; if (depth == 0) break; }
                argKey.append(parser.advance().value());
            }
            parser.expect(TokenType.RIGHT_PAREN);
            // Parse arg types from "integer, integer" or "NONE, integer"
            String[] parts = argKey.toString().split(",");
            String leftArg = parts.length > 0 ? parts[0].trim() : "NONE";
            String rightArg = parts.length > 1 ? parts[1].trim() : "NONE";
            if ("NONE".equalsIgnoreCase(leftArg)) leftArg = "NONE";
            if ("NONE".equalsIgnoreCase(rightArg)) rightArg = "NONE";
            // Encode as operator key: schema.name(leftarg,rightarg)
            // Parse schema from operator name if present (e.g., "s1.+++" -> schema=s1, op=+++)
            String opSchema = "public";
            String opName = name;
            int dotIdx = name.indexOf('.');
            if (dotIdx > 0) {
                opSchema = name.substring(0, dotIdx);
                opName = name.substring(dotIdx + 1);
            }
            name = opSchema.toLowerCase() + "." + opName + "(" + leftArg.toLowerCase() + "," + rightArg.toLowerCase() + ")";
        }
        if ((objectType == DropStmt.ObjectType.OPERATOR_CLASS
                || objectType == DropStmt.ObjectType.OPERATOR_FAMILY) && parser.check(TokenType.LEFT_PAREN)) {
            DdlTableParser.consumeUntilParen(parser);
        }
        String opMethod = null;
        if ((objectType == DropStmt.ObjectType.OPERATOR_CLASS || objectType == DropStmt.ObjectType.OPERATOR_FAMILY)
                && parser.matchKeyword("USING")) {
            opMethod = parser.readIdentifier();
        }

        String onTable = null;
        if ((objectType == DropStmt.ObjectType.TRIGGER || objectType == DropStmt.ObjectType.RULE)
                && parser.matchKeyword("ON")) {
            onTable = parser.readIdentifier();
        }
        // For OPERATOR CLASS/FAMILY, store the USING method in onTable
        if (opMethod != null) {
            onTable = opMethod;
        }

        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");

        return new DropStmt(objectType, name, onTable, ifExists, cascade, funcParamTypes);
    }

    /**
     * Parse parameter types from DROP FUNCTION name(type1, type2, ...).
     * Supports optional parameter modes (IN, OUT, INOUT, VARIADIC) and parameter names.
     * Returns a list of type names (excluding OUT parameters).
     */
    private java.util.List<String> parseFunctionDropParamTypes() {
        parser.expect(TokenType.LEFT_PAREN);
        java.util.List<String> types = new java.util.ArrayList<>();
        if (parser.check(TokenType.RIGHT_PAREN)) {
            parser.advance();
            return types;
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            // Skip optional mode keywords
            String mode = "IN";
            if (parser.checkKeyword("IN") || parser.checkKeyword("OUT")
                    || parser.checkKeyword("INOUT") || parser.checkKeyword("VARIADIC")) {
                mode = parser.advance().value().toUpperCase();
            }
            // Collect type tokens until comma or right paren
            StringBuilder typeBuf = new StringBuilder();
            int depth = 0;
            while (!parser.isAtEnd()) {
                if (depth == 0 && (parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN))) break;
                if (parser.check(TokenType.LEFT_PAREN)) depth++;
                if (parser.check(TokenType.RIGHT_PAREN)) depth--;
                if (typeBuf.length() > 0) typeBuf.append(" ");
                typeBuf.append(parser.advance().value());
            }
            if (!"OUT".equals(mode)) {
                types.add(typeBuf.toString().trim());
            }
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return types;
    }

    // ---- ALTER dispatcher ----

    Statement parseAlter() {
        parser.expectKeyword("ALTER");
        if (parser.matchKeyword("TYPE")) return parseAlterType();
        if (parser.matchKeyword("SEQUENCE")) return parseAlterSequence();
        if (parser.matchKeywords("USER", "MAPPING")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }
        if (parser.matchKeyword("ROLE") || parser.matchKeyword("USER")) return roleParser.parseAlterRole();
        if (parser.matchKeyword("POLICY")) return policyParser.parseAlterPolicy();
        if (parser.matchKeywords("DEFAULT", "PRIVILEGES")) return parseAlterDefaultPrivileges();
        if (parser.matchKeywords("TEXT", "SEARCH")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }
        if (parser.matchKeyword("GROUP")) return roleParser.parseAlterRole();

        if (parser.matchKeyword("VIEW")) {
            boolean viewIfExists = parser.matchKeywords("IF", "EXISTS");
            String viewName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) viewName = parser.readIdentifier();
            if (parser.matchKeywords("RENAME", "TO")) {
                return new AlterViewStmt(viewName, parser.readIdentifier(), viewIfExists, AlterViewStmt.Action.RENAME_TO);
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterViewStmt(viewName, parser.readIdentifier(), viewIfExists, AlterViewStmt.Action.OWNER_TO);
            }
            if (parser.matchKeyword("SET") && parser.check(TokenType.LEFT_PAREN)) {
                Map<String, String> opts = parseViewWithOptions();
                return new AlterViewStmt(viewName, null, viewIfExists, AlterViewStmt.Action.SET_OPTIONS, opts);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterViewStmt(viewName, null, viewIfExists, AlterViewStmt.Action.NO_OP);
        }

        if (parser.matchKeyword("DOMAIN")) return parseAlterDomain();

        if (parser.matchKeyword("FUNCTION")) {
            return parseAlterFunctionOrProcedure(false);
        }
        if (parser.matchKeyword("PROCEDURE")) {
            return parseAlterFunctionOrProcedure(true);
        }
        // ALTER ROUTINE is PG's alias that works for both functions and procedures
        if (parser.matchKeyword("ROUTINE")) {
            return parseAlterFunctionOrProcedure(false);
        }

        if (parser.matchKeyword("DATABASE")) {
            String dbName = parser.readIdentifier();
            if (parser.matchKeywords("RENAME", "TO")) {
                String newName = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return new SetStmt("alter_database_rename", dbName + "\0" + newName);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }

        if (parser.matchKeyword("SCHEMA")) {
            String schemaName = parser.readIdentifier();
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterSchemaOwnerStmt(schemaName, parser.readIdentifier());
            }
            if (parser.matchKeywords("RENAME", "TO")) {
                parser.readIdentifier();
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }

        // Operator ALTER targets — parse into AlterOperatorStmt
        if (parser.matchKeywords("OPERATOR", "CLASS")) return parseAlterOperatorClass();
        if (parser.matchKeywords("OPERATOR", "FAMILY")) return parseAlterOperatorFamily();
        if (parser.matchKeyword("OPERATOR")) return parseAlterOperator();

        if (parser.matchKeyword("INDEX")) {
            return parseAlterIndex();
        }

        // No-op ALTER targets
        if (parser.matchKeyword("EXTENSION")
                || parser.matchKeyword("AGGREGATE") || parser.matchKeyword("COLLATION")
                || parser.matchKeyword("RULE") || parser.matchKeyword("CONVERSION")
                || parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")
                || parser.matchKeyword("SERVER")
                || parser.matchKeywords("FOREIGN", "TABLE")
                || parser.matchKeyword("PUBLICATION")
                || parser.matchKeyword("SUBSCRIPTION")
                || parser.matchKeyword("TABLESPACE")
                || parser.matchKeyword("LANGUAGE")
                || parser.matchKeywords("EVENT", "TRIGGER")
                || parser.matchKeywords("LARGE", "OBJECT")
                || parser.matchKeyword("TRANSFORM")
                || parser.matchKeyword("STATISTICS")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }
        parser.expectKeyword("TABLE");

        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        parser.matchKeyword("ONLY");
        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { schema = table; table = parser.readIdentifier(); }

        List<AlterTableStmt.AlterAction> actions = new ArrayList<>();
        do {
            actions.add(alterActionParser.parseAlterAction());
        } while (parser.match(TokenType.COMMA));

        return new AlterTableStmt(schema, table, actions, ifExists);
    }

    // ---- CREATE TRIGGER ----

    CreateTriggerStmt parseCreateTrigger(boolean orReplace) {
        String name = parser.readIdentifier();

        String timing;
        if (parser.matchKeyword("BEFORE")) timing = "BEFORE";
        else if (parser.matchKeyword("AFTER")) timing = "AFTER";
        else if (parser.matchKeywords("INSTEAD", "OF")) timing = "INSTEAD OF";
        else throw new ParseException("Expected BEFORE, AFTER, or INSTEAD OF", parser.peek());

        List<String> events = new ArrayList<>();
        List<String> updateOfColumns = new ArrayList<>();
        String event = parser.readIdentifier().toUpperCase();
        events.add(event);
        if (event.equals("UPDATE") && parser.matchKeyword("OF")) {
            do { updateOfColumns.add(parser.readIdentifier().toLowerCase()); } while (parser.match(TokenType.COMMA));
        }
        while (parser.matchKeyword("OR")) {
            event = parser.readIdentifier().toUpperCase();
            events.add(event);
            if (event.equals("UPDATE") && parser.matchKeyword("OF")) {
                do { updateOfColumns.add(parser.readIdentifier().toLowerCase()); } while (parser.match(TokenType.COMMA));
            }
        }

        parser.expectKeyword("ON");
        String tableSchema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { tableSchema = table; table = parser.readIdentifier(); }

        String newTransitionTable = null;
        String oldTransitionTable = null;
        if (parser.matchKeyword("REFERENCING")) {
            while (parser.checkKeyword("OLD") || parser.checkKeyword("NEW")) {
                boolean isNew = parser.matchKeyword("NEW");
                if (!isNew) parser.matchKeyword("OLD");
                parser.matchKeyword("TABLE");
                parser.matchKeyword("AS");
                String transName = parser.readIdentifier();
                if (isNew) newTransitionTable = transName;
                else oldTransitionTable = transName;
            }
        }

        parser.matchKeyword("DEFERRABLE");
        if (parser.matchKeyword("INITIALLY")) {
            if (!parser.matchKeyword("DEFERRED")) parser.matchKeyword("IMMEDIATE");
        }

        parser.expectKeyword("FOR");
        parser.matchKeyword("EACH");
        boolean forEachRow = true;
        if (parser.matchKeyword("STATEMENT")) {
            forEachRow = false;
        } else {
            parser.expectKeyword("ROW");
        }

        String whenClause = null;
        if (parser.matchKeyword("WHEN")) {
            parser.expect(TokenType.LEFT_PAREN);
            whenClause = tableParser.buildRawSqlUntilCloseParen();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        parser.expectKeyword("EXECUTE");
        if (!parser.matchKeyword("FUNCTION")) {
            parser.expectKeyword("PROCEDURE");
        }

        String funcName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) funcName = parser.readIdentifier();
        parser.expect(TokenType.LEFT_PAREN);
        while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
            parser.advance();
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);

        return new CreateTriggerStmt(name, timing, events, table, tableSchema, funcName, orReplace, whenClause,
                updateOfColumns.isEmpty() ? null : updateOfColumns, newTransitionTable, oldTransitionTable, !forEachRow);
    }

    // ---- CREATE VIEW ----

    CreateViewStmt parseCreateView(boolean orReplace, boolean materialized) {
        if (parser.matchKeywords("IF", "NOT", "EXISTS")) orReplace = true;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifier();

        List<String> columnNames = null;
        if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("AS")) {
            parser.expect(TokenType.LEFT_PAREN);
            columnNames = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        if (materialized) {
            parser.expectKeyword("AS");
            boolean parenWrapped = parser.match(TokenType.LEFT_PAREN);
            Statement query = parser.tryParseSetOp(parser.parseSelect());
            if (parenWrapped) parser.expect(TokenType.RIGHT_PAREN);
            boolean withData = true;
            if (parser.matchKeyword("WITH")) {
                if (parser.matchKeyword("NO")) { parser.expectKeyword("DATA"); withData = false; }
                else parser.expectKeyword("DATA");
            }
            return new CreateViewStmt(name, query, orReplace, true, columnNames, withData);
        }

        // Parse WITH (options) before AS, e.g. WITH (security_invoker = true)
        Map<String, String> withOptions = null;
        if (parser.matchKeyword("WITH")) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                withOptions = parseViewWithOptions();
            } else {
                // This is actually WITH CHECK OPTION without options, push back
                // But since we already consumed WITH, handle inline
                if (parser.matchKeyword("CASCADED") || parser.matchKeyword("LOCAL") || parser.checkKeyword("CHECK")) {
                    // Oops - this was WITH [CASCADED|LOCAL] CHECK OPTION after query
                    // But we haven't parsed AS/query yet, so this must be WITH (...) options
                    // This path shouldn't be reached for well-formed SQL
                }
            }
        }

        parser.expectKeyword("AS");
        boolean parenWrapped2 = parser.match(TokenType.LEFT_PAREN);
        Statement query = parser.tryParseSetOp(parser.parseSelect());
        if (parenWrapped2) parser.expect(TokenType.RIGHT_PAREN);

        String checkOption = null;
        if (parser.matchKeyword("WITH")) {
            if (parser.matchKeyword("CASCADED")) checkOption = "CASCADED";
            else if (parser.matchKeyword("LOCAL")) checkOption = "LOCAL";
            parser.expectKeyword("CHECK");
            parser.expectKeyword("OPTION");
            if (checkOption == null) checkOption = "CASCADED";
        }

        return new CreateViewStmt(name, query, orReplace, false, columnNames, true, checkOption, withOptions);
    }

    private Map<String, String> parseViewWithOptions() {
        Map<String, String> opts = new LinkedHashMap<>();
        parser.expect(TokenType.LEFT_PAREN);
        do {
            String key = parser.readIdentifier();
            parser.expect(TokenType.EQUALS);
            // value can be identifier (true/false) or string literal or number
            String value;
            if (parser.check(TokenType.STRING_LITERAL)) {
                value = parser.advance().value();
            } else if (parser.check(TokenType.INTEGER_LITERAL)) {
                value = parser.advance().value();
            } else {
                value = parser.readIdentifier();
            }
            opts.put(key.toLowerCase(), value.toLowerCase());
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return opts;
    }

    RefreshMaterializedViewStmt parseRefreshMaterializedView() {
        parser.expectKeyword("REFRESH");
        parser.expectKeyword("MATERIALIZED");
        parser.expectKeyword("VIEW");
        parser.matchKeyword("CONCURRENTLY");
        String name = parser.readIdentifier();
        return new RefreshMaterializedViewStmt(name);
    }

    // ---- Misc small CREATE statements ----

    CreateTypeStmt parseCreateType() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifier();
        parser.expectKeyword("AS");
        if (parser.matchKeyword("ENUM")) {
            parser.expect(TokenType.LEFT_PAREN);
            List<String> labels = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                do { labels.add(parser.expect(TokenType.STRING_LITERAL).value()); } while (parser.match(TokenType.COMMA));
            }
            parser.expect(TokenType.RIGHT_PAREN);
            return new CreateTypeStmt(name, labels);
        }
        if (parser.matchKeyword("RANGE")) {
            parser.expect(TokenType.LEFT_PAREN);
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) parser.advance();
            parser.expect(TokenType.RIGHT_PAREN);
            return new CreateTypeStmt(name, Cols.listOf());
        }
        parser.expect(TokenType.LEFT_PAREN);
        List<CreateTypeStmt.CompositeField> fields = new ArrayList<>();
        if (!parser.check(TokenType.RIGHT_PAREN)) {
            do {
                String fieldName = parser.readIdentifier();
                String fieldType = parser.parseTypeName();
                fields.add(new CreateTypeStmt.CompositeField(fieldName, fieldType));
            } while (parser.match(TokenType.COMMA));
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return new CreateTypeStmt(name, null, fields);
    }

    CreateExtensionStmt parseCreateExtension() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifierOrString();
        return new CreateExtensionStmt(name, ifNotExists);
    }

    Statement parseCreateAggregate() {
        String name = parser.readIdentifier();
        // Parse argument types: CREATE AGGREGATE name ( argtype [, ...] ) ( ... )
        // or CREATE AGGREGATE name ( ORDER BY argtype ) for ordered-set aggregates
        List<String> argTypes = new ArrayList<>();
        if (parser.match(TokenType.LEFT_PAREN)) {
            // Check for ORDER BY (ordered-set aggregate) or * or type list
            if (parser.matchKeyword("ORDER")) {
                parser.matchKeyword("BY");
            }
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                // Handle ORDER BY appearing mid-list (ordered-set aggregates)
                if (parser.checkKeyword("ORDER")) {
                    parser.advance(); // consume ORDER
                    parser.matchKeyword("BY"); // consume BY
                    continue;
                }
                if (parser.check(TokenType.STAR)) {
                    parser.advance();
                    argTypes.add("*");
                } else {
                    argTypes.add(parser.parseTypeName());
                }
                parser.match(TokenType.COMMA);
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }

        // Parse aggregate definition body: ( SFUNC = ..., STYPE = ..., ... )
        String sfunc = null, stype = null, initcond = null, finalfunc = null, combinefunc = null, sortop = null;
        if (parser.match(TokenType.LEFT_PAREN)) {
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                String key = parser.readIdentifier().toUpperCase();
                parser.match(TokenType.EQUALS);
                switch (key) {
                    case "SFUNC":
                    case "BASETYPE":
                        if (key.equals("SFUNC")) sfunc = parser.readIdentifier();
                        else parser.readIdentifier(); // BASETYPE consumed but not used (old syntax)
                        break;
                    case "STYPE":
                        stype = parser.parseTypeName();
                        break;
                    case "FINALFUNC":
                        finalfunc = parser.readIdentifier();
                        break;
                    case "INITCOND":
                        // INITCOND can be a string literal, number literal, or identifier
                        if (parser.check(TokenType.STRING_LITERAL)) {
                            initcond = parser.advance().value();
                        } else if (parser.check(TokenType.INTEGER_LITERAL) || parser.check(TokenType.FLOAT_LITERAL)) {
                            initcond = parser.advance().value();
                        } else {
                            initcond = parser.readIdentifier();
                        }
                        break;
                    case "COMBINEFUNC":
                        combinefunc = parser.readIdentifier();
                        break;
                    case "SORTOP":
                        // Sort operator can be = , < , > etc.
                        sortop = parser.advance().value();
                        break;
                    case "FINALFUNC_EXTRA":
                    case "FINALFUNC_MODIFY":
                    case "PARALLEL":
                    case "HYPOTHETICAL":
                    case "MSFUNC":
                    case "MINVFUNC":
                    case "MSTYPE":
                    case "MFINALFUNC":
                    case "MFINALFUNC_EXTRA":
                    case "MFINALFUNC_MODIFY":
                    case "MINITCOND":
                    case "MSSPACE":
                    case "SERIALFUNC":
                    case "DESERIALFUNC":
                        // Consume value(s) — parsed but not stored
                        if (parser.check(TokenType.STRING_LITERAL)) {
                            parser.advance();
                        } else if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                            parser.advance();
                        }
                        break;
                    default:
                        // Unknown key — skip value
                        if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                            parser.advance();
                        }
                        break;
                }
                parser.match(TokenType.COMMA);
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }

        if (sfunc == null || stype == null) {
            // Incomplete aggregate — treat as no-op (some dumps have partial aggregates)
            return new SetStmt("create_noop", "ok");
        }
        return new CreateAggregateStmt(name, argTypes, sfunc, stype, initcond, finalfunc, combinefunc, sortop);
    }

    CreateRuleStmt parseCreateRule() {
        String name = parser.readIdentifier();
        parser.expectKeyword("AS");
        parser.expectKeyword("ON");
        String event = parser.readIdentifier().toUpperCase();
        parser.expectKeyword("TO");
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) table = parser.readIdentifier();
        if (parser.matchKeyword("WHERE")) {
            int depth = 0;
            while (!parser.isAtEnd() && !(parser.checkKeyword("DO") && depth == 0)) {
                if (parser.check(TokenType.LEFT_PAREN)) depth++;
                if (parser.check(TokenType.RIGHT_PAREN)) depth--;
                parser.advance();
            }
        }
        parser.expectKeyword("DO");
        String action;
        if (parser.matchKeyword("INSTEAD")) action = "INSTEAD";
        else if (parser.matchKeyword("ALSO")) action = "ALSO";
        else action = "ALSO";
        String command;
        if (parser.matchKeyword("NOTHING")) {
            command = "NOTHING";
        } else {
            StringBuilder sb = new StringBuilder();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                sb.append(parser.advance().value()).append(' ');
            }
            command = sb.toString().trim();
        }
        return new CreateRuleStmt(name, event, table, action, command);
    }

    Statement parseCreateSchema() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        String authorization = null;
        if (parser.matchKeyword("AUTHORIZATION")) authorization = parser.readIdentifier();
        return new CreateSchemaStmt(name, ifNotExists, authorization);
    }

    CreateDomainStmt parseCreateDomain() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifier();
        parser.expectKeyword("AS");
        String baseType = parser.parseTypeName();
        Expression defaultExpr = null;
        boolean notNull = false;
        Expression checkExpr = null;
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("DEFAULT")) { defaultExpr = parser.parseExpression(); }
            else if (parser.matchKeywords("NOT", "NULL")) { notNull = true; }
            else if (parser.matchKeyword("NULL")) { notNull = false; }
            else if (parser.matchKeyword("CHECK")) {
                parser.expect(TokenType.LEFT_PAREN);
                checkExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
            } else if (parser.matchKeyword("CONSTRAINT")) { parser.readIdentifier(); }
            else { break; }
        }
        return new CreateDomainStmt(name, baseType, defaultExpr, notNull, checkExpr);
    }

    // ---- SEQUENCE ----

    long readSeqLong() {
        boolean neg = false;
        if (parser.check(TokenType.MINUS)) { parser.advance(); neg = true; }
        long val = Long.parseLong(parser.advance().value());
        return neg ? -val : val;
    }

    private void parseSequenceOptions(Long[] startWith, Long[] incrementBy, Long[] minValue,
                                      Long[] maxValue, Boolean[] cycle, Integer[] cache) {
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeywords("START", "WITH")) { startWith[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("START")) { startWith[0] = readSeqLong(); continue; }
            if (parser.matchKeywords("INCREMENT", "BY")) { incrementBy[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("INCREMENT")) { incrementBy[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("MINVALUE")) { minValue[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("MAXVALUE")) { maxValue[0] = readSeqLong(); continue; }
            if (parser.matchKeywords("NO", "MINVALUE")) { continue; }
            if (parser.matchKeywords("NO", "MAXVALUE")) { continue; }
            if (parser.matchKeyword("CACHE")) { cache[0] = (int) readSeqLong(); continue; }
            if (parser.matchKeyword("CYCLE")) { cycle[0] = true; continue; }
            if (parser.matchKeywords("NO", "CYCLE")) { cycle[0] = false; continue; }
            if (parser.matchKeywords("OWNED", "BY")) {
                if (parser.matchKeyword("NONE")) { continue; }
                parser.readIdentifier(); if (parser.match(TokenType.DOT)) { parser.readIdentifier(); if (parser.match(TokenType.DOT)) parser.readIdentifier(); }
                continue;
            }
            if (parser.matchKeyword("AS")) { parser.readIdentifier(); continue; }
            break;
        }
    }

    CreateSequenceStmt parseCreateSequence(boolean temporary) {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifier();
        Long[] startWith = {null}, incrementBy = {null}, minValue = {null}, maxValue = {null};
        Boolean[] cycle = {null};
        Integer[] cache = {null};
        parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle, cache);
        CreateSequenceStmt stmt = new CreateSequenceStmt(name, ifNotExists, startWith[0], incrementBy[0], minValue[0], maxValue[0], cycle[0], temporary);
        stmt.setCache(cache[0]);
        return stmt;
    }

    AlterSequenceStmt parseAlterSequence() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifier();
        boolean restart = false;
        Long restartWith = null;
        Long[] startWith = {null}, incrementBy = {null}, minValue = {null}, maxValue = {null};
        Boolean[] cycle = {null};
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("RESTART")) {
                restart = true;
                if (parser.matchKeyword("WITH")) { restartWith = readSeqLong(); }
                else if (parser.check(TokenType.INTEGER_LITERAL) || parser.check(TokenType.MINUS)) { restartWith = readSeqLong(); }
                continue;
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterSequenceStmt(name, parser.readIdentifier());
            }
            int saved = parser.pos;
            Integer[] cache = {null};
            parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle, cache);
            if (parser.pos == saved) break;
        }
        return new AlterSequenceStmt(name, restart, restartWith, incrementBy[0], minValue[0], maxValue[0], startWith[0], cycle[0]);
    }

    // ---- TRUNCATE ----

    TruncateStmt parseTruncate() {
        parser.expectKeyword("TRUNCATE");
        parser.matchKeyword("TABLE");
        parser.matchKeyword("ONLY");
        List<String> tables = new ArrayList<>();
        String tbl = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) tbl = parser.readIdentifier();
        tables.add(tbl);
        while (parser.match(TokenType.COMMA)) {
            parser.matchKeyword("ONLY");
            String next = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) next = parser.readIdentifier();
            tables.add(next);
        }
        boolean restartIdentity = false;
        if (parser.matchKeyword("RESTART")) { parser.expectKeyword("IDENTITY"); restartIdentity = true; }
        else if (parser.matchKeyword("CONTINUE")) { parser.expectKeyword("IDENTITY"); }
        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");
        return new TruncateStmt(tables, cascade, restartIdentity);
    }

    // ---- ALTER specific types ----

    AlterDomainStmt parseAlterDomain() {
        String domainName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) domainName = parser.readIdentifier();
        if (parser.matchKeywords("SET", "DEFAULT")) {
            int startPos = parser.pos;
            Expression expr = parser.parseExpression();
            return new AlterDomainStmt(domainName, "SET_DEFAULT", buildRawSqlFromTokens(startPos, parser.pos), null, null, null);
        }
        if (parser.matchKeywords("DROP", "DEFAULT"))
            return new AlterDomainStmt(domainName, "DROP_DEFAULT", null, null, null, null);
        if (parser.matchKeywords("ADD", "CONSTRAINT")) {
            String constraintName = parser.readIdentifier();
            parser.expectKeyword("CHECK");
            parser.expect(TokenType.LEFT_PAREN);
            int startPos = parser.pos;
            Expression checkExpr = parser.parseExpression();
            String raw = buildRawSqlFromTokens(startPos, parser.pos);
            parser.expect(TokenType.RIGHT_PAREN);
            return new AlterDomainStmt(domainName, "ADD_CONSTRAINT", null, constraintName, checkExpr, raw);
        }
        if (parser.matchKeywords("DROP", "CONSTRAINT")) {
            return new AlterDomainStmt(domainName, "DROP_CONSTRAINT", null, parser.readIdentifier(), null, null);
        }
        if (parser.matchKeywords("VALIDATE", "CONSTRAINT")) {
            return new AlterDomainStmt(domainName, "VALIDATE", null, parser.readIdentifier(), null, null);
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterDomainStmt(domainName, "NO_OP", null, null, null, null);
    }

    AlterTypeStmt parseAlterType() {
        String typeName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) typeName = parser.readIdentifier();
        if (parser.matchKeywords("ADD", "VALUE")) {
            boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
            Token val = parser.expect(TokenType.STRING_LITERAL);
            String position = null; String neighbor = null;
            if (parser.matchKeyword("BEFORE")) { position = "BEFORE"; neighbor = parser.expect(TokenType.STRING_LITERAL).value(); }
            else if (parser.matchKeyword("AFTER")) { position = "AFTER"; neighbor = parser.expect(TokenType.STRING_LITERAL).value(); }
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.ADD_VALUE, val.value(), null, ifNotExists, position, neighbor);
        }
        if (parser.matchKeywords("RENAME", "VALUE")) {
            Token oldVal = parser.expect(TokenType.STRING_LITERAL);
            parser.expectKeyword("TO");
            Token newVal = parser.expect(TokenType.STRING_LITERAL);
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.RENAME_VALUE, oldVal.value(), newVal.value(), false, null, null);
        }
        if (parser.matchKeywords("RENAME", "TO"))
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.RENAME_TO, parser.readIdentifier(), null, false, null, null);
        if (parser.matchKeywords("SET", "SCHEMA"))
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.SET_SCHEMA, parser.readIdentifier(), null, false, null, null);
        if (parser.matchKeywords("OWNER", "TO"))
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.OWNER_TO, parser.readIdentifier(), null, false, null, null);
        throw new ParseException("Unsupported ALTER TYPE action", parser.peek());
    }

    AlterDefaultPrivilegesStmt parseAlterDefaultPrivileges() {
        String forRole = null; String inSchema = null;
        if (parser.matchKeyword("FOR")) { parser.matchKeyword("ROLE"); parser.matchKeyword("USER"); forRole = parser.readIdentifier(); }
        if (parser.matchKeyword("IN")) { parser.expectKeyword("SCHEMA"); inSchema = parser.readIdentifier(); }
        boolean isGrant;
        List<String> privileges = new ArrayList<>();
        if (parser.matchKeyword("GRANT")) { isGrant = true; }
        else { parser.expectKeyword("REVOKE"); isGrant = false; if (parser.matchKeywords("GRANT", "OPTION")) parser.expectKeyword("FOR"); }
        do {
            String priv = parser.readIdentifier().toUpperCase();
            if (priv.equals("ALL")) { parser.matchKeyword("PRIVILEGES"); privileges.add("ALL"); }
            else { privileges.add(priv); }
        } while (parser.match(TokenType.COMMA));
        parser.expectKeyword("ON");
        String objectType = parser.readIdentifier().toUpperCase();
        List<String> grantees = new ArrayList<>();
        if (parser.matchKeyword("TO") || parser.matchKeyword("FROM")) {
            do { grantees.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterDefaultPrivilegesStmt(forRole, inSchema, isGrant, privileges, objectType, grantees);
    }

    // ---- Shared utilities ----

    static String readValueOrMinMax(Parser parser) {
        if (parser.checkKeyword("MINVALUE")) { parser.advance(); return "MINVALUE"; }
        if (parser.checkKeyword("MAXVALUE")) { parser.advance(); return "MAXVALUE"; }
        return parser.advance().value();
    }

    // ---- CREATE OPERATOR ----

    private Statement parseCreateOperator() {
        // Read operator name (may be schema-qualified)
        String schema = null;
        StringBuilder opName = new StringBuilder();
        // Operator names can be symbol tokens
        while (!parser.isAtEnd() && !parser.check(TokenType.LEFT_PAREN)) {
            String tok = parser.advance().value();
            if (tok.equals(".") && opName.length() > 0 && schema == null) {
                schema = opName.toString();
                opName.setLength(0);
            } else {
                opName.append(tok);
            }
        }
        String name = opName.toString().trim();

        parser.expect(TokenType.LEFT_PAREN);

        String leftArg = null, rightArg = null, function = null;
        String commutator = null, negator = null, restrict = null, join = null;
        boolean hashes = false, merges = false;

        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (parser.match(TokenType.COMMA)) continue;
            String kw = parser.advance().value().toUpperCase();
            switch (kw) {
                case "LEFTARG":
                    parser.match(TokenType.EQUALS);
                    leftArg = readTypeName();
                    break;
                case "RIGHTARG":
                    parser.match(TokenType.EQUALS);
                    rightArg = readTypeName();
                    break;
                case "FUNCTION":
                case "PROCEDURE":
                    parser.match(TokenType.EQUALS);
                    function = readDottedIdentifier();
                    break;
                case "COMMUTATOR":
                    parser.match(TokenType.EQUALS);
                    commutator = readOperatorName();
                    break;
                case "NEGATOR":
                    parser.match(TokenType.EQUALS);
                    negator = readOperatorName();
                    break;
                case "RESTRICT":
                    parser.match(TokenType.EQUALS);
                    restrict = readDottedIdentifier();
                    break;
                case "JOIN":
                    parser.match(TokenType.EQUALS);
                    join = readDottedIdentifier();
                    break;
                case "HASHES":
                    hashes = true;
                    break;
                case "MERGES":
                    merges = true;
                    break;
                default:
                    // skip unknown attributes
                    if (parser.check(TokenType.EQUALS)) {
                        parser.advance();
                        parser.advance();
                    }
                    break;
            }
        }
        parser.expect(TokenType.RIGHT_PAREN);

        return new CreateOperatorStmt(schema, name, leftArg, rightArg, function,
                commutator, negator, restrict, join, hashes, merges);
    }

    /** Read an operator name (may be multi-symbol, e.g. ===, !==). */
    private String readOperatorName() {
        StringBuilder sb = new StringBuilder();
        // Handle OPERATOR(schema.op) syntax
        if (parser.checkKeyword("OPERATOR")) {
            parser.advance();
            parser.expect(TokenType.LEFT_PAREN);
            while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
                sb.append(parser.advance().value());
            }
            parser.expect(TokenType.RIGHT_PAREN);
            return sb.toString();
        }
        // Simple operator name: consume symbol tokens
        while (!parser.isAtEnd() && !parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)
                && !parser.check(TokenType.SEMICOLON)) {
            Token t = parser.peek();
            // Stop if we hit a keyword that's a new attribute
            if (t.type() == TokenType.IDENTIFIER && isOperatorAttribute(t.value().toUpperCase())) break;
            sb.append(parser.advance().value());
        }
        return sb.toString().trim();
    }

    private boolean isOperatorAttribute(String val) {
        return "LEFTARG".equals(val) || "RIGHTARG".equals(val) || "FUNCTION".equals(val)
                || "PROCEDURE".equals(val) || "COMMUTATOR".equals(val) || "NEGATOR".equals(val)
                || "RESTRICT".equals(val) || "JOIN".equals(val) || "HASHES".equals(val) || "MERGES".equals(val);
    }

    private String readDottedIdentifier() {
        StringBuilder sb = new StringBuilder();
        sb.append(parser.readIdentifier());
        while (parser.check(TokenType.DOT)) {
            sb.append(parser.advance().value());
            sb.append(parser.readIdentifier());
        }
        return sb.toString();
    }

    private String readTypeName() {
        StringBuilder sb = new StringBuilder();
        sb.append(parser.readIdentifier());
        // Handle multi-word types like "double precision", "character varying"
        if (parser.checkKeyword("PRECISION") || parser.checkKeyword("VARYING")
                || parser.checkKeyword("WITHOUT") || parser.checkKeyword("WITH")
                || parser.checkKeyword("ZONE")) {
            sb.append(" ").append(parser.advance().value());
            if (parser.checkKeyword("TIME") || parser.checkKeyword("ZONE")) {
                sb.append(" ").append(parser.advance().value());
            }
        }
        // Handle array type
        if (parser.check(TokenType.LEFT_BRACKET)) {
            sb.append(parser.advance().value());
            if (parser.check(TokenType.RIGHT_BRACKET)) sb.append(parser.advance().value());
        }
        return sb.toString();
    }

    // ---- CREATE OPERATOR FAMILY ----

    private Statement parseCreateOperatorFamily() {
        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        parser.expectKeyword("USING");
        String method = parser.readIdentifier();
        return new CreateOperatorFamilyStmt(schema, name, method);
    }

    // ---- CREATE OPERATOR CLASS ----

    private Statement parseCreateOperatorClass() {
        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        boolean isDefault = parser.matchKeyword("DEFAULT");
        parser.expectKeyword("FOR");
        parser.expectKeyword("TYPE");
        String forType = readTypeName();
        parser.expectKeyword("USING");
        String method = parser.readIdentifier();
        String familyName = null;
        if (parser.matchKeyword("FAMILY")) {
            familyName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) {
                // schema.family — just use the family name
                familyName = parser.readIdentifier();
            }
        }
        // Consume AS clause (operator/function definitions) — stored in the opclass itself
        if (parser.matchKeyword("AS")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                parser.advance();
            }
        }
        return new CreateOperatorClassStmt(schema, name, isDefault, forType, method, familyName);
    }

    // ---- ALTER FUNCTION / ALTER PROCEDURE ----

    /** Check if a token is the start of a multi-word SQL type name. */
    private static boolean isMultiWordTypeStart(String token) {
        switch (token.toLowerCase()) {
            case "double":        // double precision
            case "character":     // character varying
            case "timestamp":     // timestamp with/without time zone
            case "time":          // time with/without time zone
            case "bit":           // bit varying
                return true;
            default:
                return false;
        }
    }

    private Statement parseAlterFunctionOrProcedure(boolean isProcedure) {
        // PG 18: ALTER FUNCTION/PROCEDURE does not support IF EXISTS
        if (parser.checkKeyword("IF")) {
            throw new MemgresException(
                    "syntax error at or near \"" + parser.peek().value() + "\"", "42601");
        }
        boolean ifExists = false;
        // Read function/procedure name (possibly schema-qualified)
        String schema = null;
        String funcName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { schema = funcName; funcName = parser.readIdentifier(); }
        // Parse parameter type list if present (e.g., "(integer, text)")
        java.util.List<String> paramTypes = null;
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume '('
            paramTypes = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                // Parse parameter types, skipping optional IN/OUT/INOUT/VARIADIC mode and optional param name
                while (true) {
                    // Skip parameter mode keywords if present
                    if (parser.checkKeyword("IN") || parser.checkKeyword("OUT")
                            || parser.checkKeyword("INOUT") || parser.checkKeyword("VARIADIC")) {
                        parser.advance();
                    }
                    // Read the type name; could be multi-word (e.g., "double precision", "character varying")
                    // But first, check if next token is a name followed by a type (e.g., "x integer")
                    StringBuilder typeName = new StringBuilder();
                    String first = parser.advance().value();
                    // Peek ahead: if next token is not comma/right-paren and first looks like a param name,
                    // the first token was the parameter name, and we need to read the actual type
                    if (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)
                            && !parser.isAtEnd()) {
                        // First was a param name or start of multi-word type
                        // Heuristic: if first is a known type-start keyword, include it in the type
                        String second = parser.advance().value();
                        boolean firstIsTypeStart = isMultiWordTypeStart(first);
                        if (firstIsTypeStart) {
                            typeName.append(first).append(" ");
                        }
                        typeName.append(second);
                        while (!parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)
                                && !parser.isAtEnd()) {
                            typeName.append(" ").append(parser.advance().value());
                        }
                    } else {
                        // first IS the type (no param name)
                        typeName.append(first);
                    }
                    paramTypes.add(typeName.toString().trim());
                    if (parser.check(TokenType.COMMA)) {
                        parser.advance(); // consume ','
                    } else {
                        break;
                    }
                }
            }
            if (parser.check(TokenType.RIGHT_PAREN)) parser.advance(); // consume ')'
        }

        // RENAME TO
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return AlterFunctionStmt.renameTo(funcName, schema, isProcedure, ifExists, newName, paramTypes);
        }
        // SET SCHEMA
        if (parser.matchKeywords("SET", "SCHEMA")) {
            String newSchema = parser.readIdentifier();
            return AlterFunctionStmt.setSchema(funcName, schema, isProcedure, ifExists, newSchema, paramTypes);
        }
        // OWNER TO
        if (parser.matchKeywords("OWNER", "TO")) {
            String newOwner = parser.readIdentifier();
            return AlterFunctionStmt.ownerTo(funcName, schema, isProcedure, ifExists, newOwner, paramTypes);
        }

        // Attribute changes: VOLATILE, STABLE, IMMUTABLE, STRICT, CALLED ON NULL INPUT,
        // RETURNS NULL ON NULL INPUT, SECURITY DEFINER/INVOKER, LEAKPROOF, NOT LEAKPROOF,
        // COST n, ROWS n, PARALLEL {SAFE|RESTRICTED|UNSAFE}, SET/RESET config
        String volatility = null;
        Boolean strict = null;
        Boolean securityDefiner = null;
        Boolean leakproof = null;
        Double cost = null;
        Double rows = null;
        String parallel = null;
        java.util.Map<String, String> setClauses = null;
        java.util.List<String> resetParams = null;

        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("VOLATILE")) {
                volatility = "VOLATILE";
            } else if (parser.matchKeyword("STABLE")) {
                volatility = "STABLE";
            } else if (parser.matchKeyword("IMMUTABLE")) {
                volatility = "IMMUTABLE";
            } else if (parser.matchKeyword("STRICT")) {
                strict = true;
            } else if (parser.matchKeywords("CALLED", "ON", "NULL", "INPUT")) {
                strict = false;
            } else if (parser.matchKeywords("RETURNS", "NULL", "ON", "NULL", "INPUT")) {
                strict = true;
            } else if (parser.matchKeywords("SECURITY", "DEFINER")) {
                securityDefiner = true;
            } else if (parser.matchKeywords("SECURITY", "INVOKER")) {
                securityDefiner = false;
            } else if (parser.matchKeywords("NOT", "LEAKPROOF")) {
                leakproof = false;
            } else if (parser.matchKeyword("LEAKPROOF")) {
                leakproof = true;
            } else if (parser.matchKeyword("COST")) {
                cost = Double.parseDouble(parser.advance().value());
            } else if (parser.matchKeyword("ROWS")) {
                rows = Double.parseDouble(parser.advance().value());
            } else if (parser.matchKeywords("PARALLEL", "SAFE")) {
                parallel = "SAFE";
            } else if (parser.matchKeywords("PARALLEL", "RESTRICTED")) {
                parallel = "RESTRICTED";
            } else if (parser.matchKeywords("PARALLEL", "UNSAFE")) {
                parallel = "UNSAFE";
            } else if (parser.matchKeyword("RESET")) {
                if (parser.matchKeyword("ALL")) {
                    if (resetParams == null) resetParams = new ArrayList<>();
                    resetParams.add("ALL");
                } else {
                    String param = parser.readIdentifier();
                    if (resetParams == null) resetParams = new ArrayList<>();
                    resetParams.add(param);
                }
            } else if (parser.matchKeyword("SET")) {
                String param = parser.readIdentifier();
                if (parser.matchKeyword("TO") || parser.match(TokenType.EQUALS)) {
                    StringBuilder val = new StringBuilder();
                    while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)
                            && !parser.checkKeyword("SET") && !parser.checkKeyword("RESET")
                            && !parser.checkKeyword("VOLATILE") && !parser.checkKeyword("STABLE")
                            && !parser.checkKeyword("IMMUTABLE") && !parser.checkKeyword("STRICT")
                            && !parser.checkKeyword("COST") && !parser.checkKeyword("ROWS")
                            && !parser.checkKeyword("SECURITY") && !parser.checkKeyword("PARALLEL")
                            && !parser.checkKeyword("CALLED") && !parser.checkKeyword("RETURNS")
                            && !parser.checkKeyword("LEAKPROOF")) {
                        if (val.length() > 0) val.append(" ");
                        val.append(parser.advance().value());
                    }
                    if (setClauses == null) setClauses = new java.util.LinkedHashMap<>();
                    setClauses.put(param, val.toString());
                } else if (parser.matchKeywords("FROM", "CURRENT")) {
                    if (setClauses == null) setClauses = new java.util.LinkedHashMap<>();
                    setClauses.put(param, "FROM CURRENT");
                }
            } else {
                // Unknown attribute — skip it
                parser.advance();
            }
        }

        return new AlterFunctionStmt(funcName, schema, isProcedure, ifExists, AlterFunctionStmt.Action.SET_ATTRIBUTES,
                null, volatility, strict, securityDefiner, leakproof, cost, rows, parallel, setClauses, resetParams, paramTypes);
    }

    // ---- ALTER INDEX ----

    private Statement parseAlterIndex() {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");

        // Handle ALL IN TABLESPACE (no-op)
        if (parser.matchKeyword("ALL")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }

        String indexName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) indexName = parser.readIdentifier();

        // RENAME TO
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.RENAME_TO, newName);
        }
        // SET TABLESPACE (no-op)
        if (parser.matchKeywords("SET", "TABLESPACE")) {
            String tablespace = parser.readIdentifier();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.SET_TABLESPACE, tablespace);
        }
        // ATTACH PARTITION (no-op)
        if (parser.matchKeywords("ATTACH", "PARTITION")) {
            String partIdx = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) partIdx = parser.readIdentifier();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.ATTACH_PARTITION, partIdx);
        }
        // ALTER COLUMN n SET STATISTICS n (no-op)
        if (parser.matchKeywords("ALTER", "COLUMN")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.SET_STATISTICS, null);
        }
        // SET ( key = value, ... ) — parse storage parameters
        // PG 18: ALTER INDEX does not support SET SCHEMA — reject it
        if (parser.checkKeyword("SET") && parser.checkKeywordAt(1, "SCHEMA")) {
            throw new MemgresException(
                    "syntax error at or near \"SCHEMA\"", "42601");
        }
        if (parser.matchKeyword("SET")) {
            java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
            if (parser.match(TokenType.LEFT_PAREN)) {
                while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
                    String key = parser.readIdentifier().toLowerCase();
                    parser.match(TokenType.EQUALS);
                    StringBuilder val = new StringBuilder();
                    while (!parser.isAtEnd() && !parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                        val.append(parser.advance().value());
                    }
                    params.put(key, val.toString().trim());
                    parser.match(TokenType.COMMA);
                }
                parser.match(TokenType.RIGHT_PAREN);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.SET_PARAMS, null, params);
        }
        // RESET ( param, ... ) — remove storage parameters
        if (parser.matchKeyword("RESET")) {
            java.util.Map<String, String> params = new java.util.LinkedHashMap<>();
            if (parser.match(TokenType.LEFT_PAREN)) {
                while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
                    String key = parser.readIdentifier().toLowerCase();
                    params.put(key, null); // null value signals removal
                    parser.match(TokenType.COMMA);
                }
                parser.match(TokenType.RIGHT_PAREN);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.RESET_PARAMS, null, params);
        }

        // Fallback: consume and no-op
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterIndexStmt(indexName, ifExists, AlterIndexStmt.Action.NO_OP, null);
    }

    // ---- ALTER OPERATOR ----

    private Statement parseAlterOperator() {
        // Read operator name
        StringBuilder opName = new StringBuilder();
        while (!parser.isAtEnd() && !parser.check(TokenType.LEFT_PAREN)) {
            opName.append(parser.advance().value());
        }
        String name = opName.toString().trim();

        // Read (left_type, right_type)
        parser.expect(TokenType.LEFT_PAREN);
        String leftArg = readTypeOrNone();
        parser.expect(TokenType.COMMA);
        String rightArg = readTypeOrNone();
        parser.expect(TokenType.RIGHT_PAREN);

        // Parse action
        if (parser.matchKeywords("OWNER", "TO")) {
            String owner = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR,
                    name, leftArg, rightArg, null, AlterOperatorStmt.AlterAction.OWNER_TO, owner);
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            String schema = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR,
                    name, leftArg, rightArg, null, AlterOperatorStmt.AlterAction.SET_SCHEMA, schema);
        }
        if (parser.matchKeyword("SET")) {
            // SET (RESTRICT = ..., JOIN = ...)
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR,
                    name, leftArg, rightArg, null, AlterOperatorStmt.AlterAction.SET_PROPERTIES, null);
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR,
                name, leftArg, rightArg, null, AlterOperatorStmt.AlterAction.SET_PROPERTIES, null);
    }

    private String readTypeOrNone() {
        if (parser.checkKeyword("NONE")) {
            parser.advance();
            return null;
        }
        return readTypeName();
    }

    // ---- ALTER OPERATOR FAMILY ----

    private Statement parseAlterOperatorFamily() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            name = parser.readIdentifier(); // skip schema for now
        }
        parser.expectKeyword("USING");
        String method = parser.readIdentifier();

        if (parser.matchKeywords("OWNER", "TO")) {
            String owner = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                    name, null, null, method, AlterOperatorStmt.AlterAction.OWNER_TO, owner);
        }
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                    name, null, null, method, AlterOperatorStmt.AlterAction.RENAME_TO, newName);
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            String schema = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                    name, null, null, method, AlterOperatorStmt.AlterAction.SET_SCHEMA, schema);
        }
        if (parser.matchKeyword("ADD")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                    name, null, null, method, AlterOperatorStmt.AlterAction.ADD_MEMBER, null);
        }
        if (parser.matchKeyword("DROP")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                    name, null, null, method, AlterOperatorStmt.AlterAction.DROP_MEMBER, null);
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_FAMILY,
                name, null, null, method, AlterOperatorStmt.AlterAction.SET_PROPERTIES, null);
    }

    // ---- ALTER OPERATOR CLASS ----

    private Statement parseAlterOperatorClass() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            name = parser.readIdentifier(); // skip schema for now
        }
        parser.expectKeyword("USING");
        String method = parser.readIdentifier();

        if (parser.matchKeywords("OWNER", "TO")) {
            String owner = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_CLASS,
                    name, null, null, method, AlterOperatorStmt.AlterAction.OWNER_TO, owner);
        }
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_CLASS,
                    name, null, null, method, AlterOperatorStmt.AlterAction.RENAME_TO, newName);
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            String schema = parser.readIdentifier();
            return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_CLASS,
                    name, null, null, method, AlterOperatorStmt.AlterAction.SET_SCHEMA, schema);
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterOperatorStmt(AlterOperatorStmt.ObjectKind.OPERATOR_CLASS,
                name, null, null, method, AlterOperatorStmt.AlterAction.SET_PROPERTIES, null);
    }

    private String buildRawSqlFromTokens(int startPos, int endPos) {
        StringBuilder raw = new StringBuilder();
        for (int i = startPos; i < endPos; i++) {
            if (i > startPos) raw.append(' ');
            Token t = parser.tokens.get(i);
            if (t.type() == TokenType.STRING_LITERAL) {
                raw.append("'").append(t.value().replace("'", "''")).append("'");
            } else {
                raw.append(t.value());
            }
        }
        return raw.toString();
    }
}
