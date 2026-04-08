package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

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
        if (parser.matchKeyword("USER")) return roleParser.parseCreateRole(true);
        if (unique) {
            parser.expectKeyword("INDEX");
            return indexParser.parseCreateIndex(true, false);
        }

        if (orReplace && parser.matchKeyword("VIEW")) return parseCreateView(true, false);
        if (orReplace && parser.matchKeyword("TRIGGER")) return parseCreateTrigger(true);
        if (parser.matchKeyword("GROUP")) return roleParser.parseCreateRole(false);
        if (parser.matchKeyword("RULE")) return parseCreateRule();

        // No-op CREATE targets
        if (parser.matchKeyword("COLLATION") || parser.matchKeyword("CAST")
                || parser.matchKeyword("CONVERSION") || parser.matchKeyword("AGGREGATE")
                || parser.matchKeywords("OPERATOR", "CLASS") || parser.matchKeywords("OPERATOR", "FAMILY")
                || parser.matchKeyword("OPERATOR")
                || parser.matchKeywords("DEFAULT", "CONVERSION")
                || parser.matchKeywords("TEXT", "SEARCH")
                || parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")
                || parser.matchKeyword("SERVER")
                || parser.matchKeywords("USER", "MAPPING")
                || parser.matchKeywords("FOREIGN", "TABLE")
                || parser.matchKeyword("PUBLICATION")
                || parser.matchKeyword("SUBSCRIPTION")
                || parser.matchKeyword("TABLESPACE")
                || parser.matchKeyword("LANGUAGE")
                || parser.matchKeywords("EVENT", "TRIGGER")
                || parser.matchKeyword("TRANSFORM")
                || parser.matchKeywords("ACCESS", "METHOD")
                || parser.matchKeyword("STATISTICS")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_noop", "ok");
        }

        // CREATE DATABASE [IF NOT EXISTS] dbname [options...]
        if (parser.matchKeyword("DATABASE")) {
            boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
            String dbName = parser.readIdentifier();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt(ifNotExists ? "create_database_if_not_exists" : "create_database", dbName);
        }

        throw new ParseException("Unsupported CREATE statement", parser.peek());
    }

    // ---- DROP dispatcher ----

    Statement parseDrop() {
        parser.expectKeyword("DROP");

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
                || parser.matchKeywords("USER", "MAPPING")
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
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt(ifExists ? "drop_database_if_exists" : "drop_database", dbName);
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

        if ((objectType == DropStmt.ObjectType.FUNCTION || objectType == DropStmt.ObjectType.AGGREGATE)
                && parser.check(TokenType.LEFT_PAREN)) {
            DdlTableParser.consumeUntilParen(parser);
        }
        if ((objectType == DropStmt.ObjectType.OPERATOR || objectType == DropStmt.ObjectType.OPERATOR_CLASS
                || objectType == DropStmt.ObjectType.OPERATOR_FAMILY) && parser.check(TokenType.LEFT_PAREN)) {
            DdlTableParser.consumeUntilParen(parser);
        }
        if ((objectType == DropStmt.ObjectType.OPERATOR_CLASS || objectType == DropStmt.ObjectType.OPERATOR_FAMILY)
                && parser.matchKeyword("USING")) {
            parser.readIdentifier();
        }

        String onTable = null;
        if ((objectType == DropStmt.ObjectType.TRIGGER || objectType == DropStmt.ObjectType.RULE)
                && parser.matchKeyword("ON")) {
            onTable = parser.readIdentifier();
        }

        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");

        return new DropStmt(objectType, name, onTable, ifExists, cascade);
    }

    // ---- ALTER dispatcher ----

    Statement parseAlter() {
        parser.expectKeyword("ALTER");
        if (parser.matchKeyword("TYPE")) return parseAlterType();
        if (parser.matchKeyword("SEQUENCE")) return parseAlterSequence();
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
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterViewStmt(viewName, null, viewIfExists, AlterViewStmt.Action.NO_OP);
        }

        if (parser.matchKeyword("DOMAIN")) return parseAlterDomain();

        if (parser.matchKeyword("FUNCTION")) {
            String funcName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) funcName = parser.readIdentifier();
            if (parser.check(TokenType.LEFT_PAREN)) DdlTableParser.consumeUntilParen(parser);
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterFunctionOwnerStmt(funcName, parser.readIdentifier());
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

        // No-op ALTER targets
        if (parser.matchKeyword("PROCEDURE") || parser.matchKeyword("EXTENSION")
                || parser.matchKeyword("AGGREGATE") || parser.matchKeyword("OPERATOR") || parser.matchKeyword("COLLATION")
                || parser.matchKeyword("RULE") || parser.matchKeyword("CONVERSION")
                || parser.matchKeyword("INDEX")
                || parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")
                || parser.matchKeyword("SERVER")
                || parser.matchKeywords("USER", "MAPPING")
                || parser.matchKeywords("FOREIGN", "TABLE")
                || parser.matchKeyword("PUBLICATION")
                || parser.matchKeyword("SUBSCRIPTION")
                || parser.matchKeyword("DATABASE")
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

        return new CreateViewStmt(name, query, orReplace, false, columnNames, true, checkOption);
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
                                      Long[] maxValue, Boolean[] cycle) {
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeywords("START", "WITH")) { startWith[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("START")) { startWith[0] = readSeqLong(); continue; }
            if (parser.matchKeywords("INCREMENT", "BY")) { incrementBy[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("INCREMENT")) { incrementBy[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("MINVALUE")) { minValue[0] = readSeqLong(); continue; }
            if (parser.matchKeyword("MAXVALUE")) { maxValue[0] = readSeqLong(); continue; }
            if (parser.matchKeywords("NO", "MINVALUE")) { continue; }
            if (parser.matchKeywords("NO", "MAXVALUE")) { continue; }
            if (parser.matchKeyword("CACHE")) { parser.advance(); continue; }
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
        parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle);
        return new CreateSequenceStmt(name, ifNotExists, startWith[0], incrementBy[0], minValue[0], maxValue[0], cycle[0], temporary);
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
            parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle);
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
