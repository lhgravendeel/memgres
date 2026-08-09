package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.PgErrors;
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

        if (parser.matchKeyword("TABLE")) return tableParser.parseCreateTable(temporary, unlogged);
        if (parser.matchKeyword("TYPE")) return parseCreateType();
        if (parser.matchKeyword("FUNCTION")) return functionParser.parseCreateFunction(orReplace, false);
        if (parser.matchKeyword("PROCEDURE")) return functionParser.parseCreateFunction(orReplace, true);
        if (parser.matchKeyword("TRIGGER")) return parseCreateTrigger(orReplace);
        if (parser.matchKeywords("CONSTRAINT", "TRIGGER")) return parseCreateTrigger(false, true);
        if (parser.matchKeyword("EXTENSION")) return parseCreateExtension();
        if (parser.matchKeyword("INDEX")) return indexParser.parseCreateIndex(unique, false);
        if (parser.matchKeyword("VIEW")) return parseCreateView(orReplace, false);
        // CREATE RECURSIVE VIEW v (cols) AS q is shorthand for a view whose body is
        // WITH RECURSIVE v (cols) AS (q) SELECT cols FROM v.
        if (parser.matchKeywords("RECURSIVE", "VIEW")) return parseCreateRecursiveView(orReplace);
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
            return parseCreateUserMapping();
        }
        if (parser.matchKeyword("USER")) return roleParser.parseCreateRole(true);
        if (unique) {
            parser.expectKeyword("INDEX");
            return indexParser.parseCreateIndex(true, false);
        }

        if (orReplace && parser.matchKeyword("VIEW")) return parseCreateView(true, false);
        if (orReplace && parser.matchKeyword("TRIGGER")) return parseCreateTrigger(true);
        if (parser.matchKeyword("GROUP")) return roleParser.parseCreateRole(false);
        if (parser.matchKeyword("RULE")) return parseCreateRule(orReplace);

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
            String langName = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_stub", "language" + STUB_SEP + langName);
        }

        // CREATE EVENT TRIGGER
        if (parser.matchKeywords("EVENT", "TRIGGER")) return parseCreateEventTrigger();

        if (parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")) {
            return parseCreateForeignDataWrapper();
        }
        if (parser.matchKeyword("SERVER")) {
            return parseCreateServer();
        }
        if (parser.matchKeywords("FOREIGN", "TABLE")) {
            return parseCreateForeignTable();
        }
        if (parser.matchKeyword("PUBLICATION")) {
            return parseCreatePublication();
        }
        if (parser.matchKeyword("SUBSCRIPTION")) {
            return parseCreateSubscription();
        }

        // Text Search DDL
        if (parser.matchKeywords("TEXT", "SEARCH")) {
            return parseCreateTextSearch();
        }

        // CREATE COLLATION
        if (parser.matchKeyword("COLLATION")) {
            return parseCreateCollation();
        }

        // CREATE CAST
        if (parser.matchKeyword("CAST")) {
            return parseCreateCast();
        }

        // No-op CREATE targets (accepted but not functionally implemented)
        // A conversion and a tablespace have no behaviour here, but their names are remembered
        // so a later ALTER on a name that was never created is refused rather than ignored.
        if (parser.matchKeyword("CONVERSION") || parser.matchKeywords("DEFAULT", "CONVERSION")) {
            String convName = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_stub", "conversion" + STUB_SEP + convName);
        }
        if (parser.matchKeyword("TABLESPACE")) {
            String tsName = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_stub", "tablespace" + STUB_SEP + tsName);
        }
        if (parser.matchKeyword("TRANSFORM")
                || parser.matchKeywords("ACCESS", "METHOD")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("create_noop", "ok");
        }

        if (parser.matchKeyword("STATISTICS")) {
            return parseCreateStatistics();
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

    /** The characters PostgreSQL builds an operator name from; a schema may qualify it. */
    private static final String OPERATOR_NAME_CHARS = "+-*/<>=~!@#%^&|`?";

    private static boolean isOperatorSymbolName(String name) {
        if (name == null || name.isEmpty()) return false;
        String bare = name.substring(name.lastIndexOf('.') + 1);
        if (bare.isEmpty()) return false;
        for (int i = 0; i < bare.length(); i++) {
            if (OPERATOR_NAME_CHARS.indexOf(bare.charAt(i)) < 0) return false;
        }
        return true;
    }

    Statement parseDrop() {
        parser.expectKeyword("DROP");

        if (parser.matchKeywords("USER", "MAPPING")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_user_mapping", "ok");
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
                if (parser.match(TokenType.DOT)) extra = extra + "." + parser.readIdentifier();
                additionalTables.add(extra);
            }
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new DropTableStmt(schema, name, ifExists, cascade, additionalTables);
        }

        DropStmt.ObjectType objectType;
        if (parser.matchKeyword("FUNCTION")) objectType = DropStmt.ObjectType.FUNCTION;
        else if (parser.matchKeyword("PROCEDURE")) objectType = DropStmt.ObjectType.PROCEDURE;
        else if (parser.matchKeyword("ROUTINE")) objectType = DropStmt.ObjectType.FUNCTION;
        else if (parser.matchKeyword("TRIGGER")) objectType = DropStmt.ObjectType.TRIGGER;
        else if (parser.matchKeyword("TYPE")) objectType = DropStmt.ObjectType.TYPE;
        else if (parser.matchKeyword("INDEX")) objectType = DropStmt.ObjectType.INDEX;
        else if (parser.matchKeyword("VIEW")) objectType = DropStmt.ObjectType.VIEW;
        else if (parser.matchKeyword("SEQUENCE")) objectType = DropStmt.ObjectType.SEQUENCE;
        else if (parser.matchKeyword("SCHEMA")) objectType = DropStmt.ObjectType.SCHEMA;
        else if (parser.matchKeyword("DOMAIN")) objectType = DropStmt.ObjectType.DOMAIN;
        else if (parser.matchKeyword("MATERIALIZED")) { parser.expectKeyword("VIEW"); objectType = DropStmt.ObjectType.MATERIALIZED_VIEW; }
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
            return parseDropTextSearch();
        }
        else if (parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")) {
            return parseDropObject("foreign-data wrapper");
        }
        else if (parser.matchKeyword("SERVER")) {
            return parseDropObject("server");
        }
        else if (parser.matchKeywords("FOREIGN", "TABLE")) {
            boolean ftIfExists = parser.matchKeywords("IF", "EXISTS");
            String ftSchema = null;
            String ftName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) { ftSchema = ftName; ftName = parser.readIdentifier(); }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            // The schema and IF EXISTS both decide what the drop does: a foreign table in
            // another schema is not this one, and only IF EXISTS makes a missing one silent.
            return new SetStmt("drop_foreign_table",
                    ftName + "\0" + (ftSchema != null ? ftSchema : "") + "\0" + ftIfExists);
        }
        else if (parser.matchKeyword("PUBLICATION")) {
            return parseDropObject("publication");
        }
        else if (parser.matchKeyword("SUBSCRIPTION")) {
            return parseDropObject("subscription");
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
            parser.matchKeywords("IF", "EXISTS");
            String tsName = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_stub", "tablespace" + STUB_SEP + tsName);
        }
        else if (parser.matchKeyword("LANGUAGE")) {
            parser.matchKeyword("PROCEDURAL");
            parser.matchKeywords("IF", "EXISTS");
            String langName = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_stub", "language" + STUB_SEP + langName);
        }
        else if (parser.matchKeywords("EVENT", "TRIGGER")) {
            return parseDropEventTrigger();
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
            boolean dropIfExists = parser.matchKeywords("IF", "EXISTS");
            String statName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) statName = statName + "." + parser.readIdentifier();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("drop_statistics", statName + "\0" + (dropIfExists ? "1" : "0"));
        }
        else throw new ParseException("Unsupported DROP target", parser.peek());

        if (objectType == DropStmt.ObjectType.INDEX) parser.matchKeyword("CONCURRENTLY");
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        if (objectType == DropStmt.ObjectType.INDEX) parser.matchKeyword("CONCURRENTLY");

        if (objectType == DropStmt.ObjectType.CAST) {
            String castName = "cast";
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.advance(); // consume '('
                String sourceType = parser.parseTypeName();
                parser.expectKeyword("AS");
                String targetType = parser.parseTypeName();
                parser.expect(TokenType.RIGHT_PAREN);
                castName = sourceType.toLowerCase() + "->" + targetType.toLowerCase();
            }
            boolean cascade2 = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new DropStmt(objectType, castName, null, ifExists, cascade2);
        }

        String name;
        String qualifier = null;
        if (objectType == DropStmt.ObjectType.OPERATOR) {
            StringBuilder sb = new StringBuilder();
            while (!parser.isAtEnd() && !parser.check(TokenType.LEFT_PAREN) && !parser.check(TokenType.SEMICOLON)
                    && !parser.checkKeyword("CASCADE") && !parser.checkKeyword("RESTRICT")) {
                sb.append(parser.advance().value());
            }
            name = sb.toString().trim();
            // An operator is named in symbols, so a word here is not a name this statement can
            // have. PostgreSQL cannot parse it either and reports the token that follows.
            if (!isOperatorSymbolName(name)) {
                throw new ParseException("operator name must be symbolic", parser.peek());
            }
        } else {
            name = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) {
                qualifier = name;
                name = parser.readIdentifier();
            }
        }
        // Every kind carries the qualifier onward so the executor can tell a missing schema from
        // a missing object; routines and indexes are also looked up by it, because an unqualified
        // name of either only reaches what the search path makes visible, and the rest still key
        // by bare name. An operator writes its own schema into its name above.
        String dropSchema = objectType == DropStmt.ObjectType.OPERATOR ? null : qualifier;

        java.util.List<String> funcParamTypes = null;
        if ((objectType == DropStmt.ObjectType.FUNCTION
                || objectType == DropStmt.ObjectType.PROCEDURE
                || objectType == DropStmt.ObjectType.AGGREGATE)
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

        return new DropStmt(objectType, name, onTable, ifExists, cascade, funcParamTypes, dropSchema);
    }

    /**
     * The words of one signature parameter with its name removed, if it had one.
     *
     * <p>A signature may name its parameters — the identity arguments memgres itself deparses do,
     * which is what a dump writes — and the name is no part of the type. It is the first word,
     * unless that word begins a type whose own name is two words or is about to take a modifier
     * or a pair of brackets.
     */
    private static java.util.List<String> withoutParameterName(java.util.List<String> words) {
        if (words.size() < 2) return words;
        String first = words.get(0);
        String second = words.get(1);
        if (isMultiWordTypeStart(first) || "(".equals(second) || "[".equals(second)) return words;
        return words.subList(1, words.size());
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
            if (parser.checkKeyword("IN") || parser.checkKeyword("INOUT")
                    || parser.checkKeyword("VARIADIC") || parser.checkIdentifier("OUT")
                    || parser.checkIdentifier("INOUT") || parser.checkIdentifier("VARIADIC")) {
                mode = parser.advance().value().toUpperCase();
                if ("IN".equals(mode) && (parser.checkIdentifier("OUT") || parser.checkKeyword("OUT"))) {
                    parser.advance();
                    mode = "INOUT";
                }
            }
            // Collect the parameter's words until comma or right paren
            java.util.List<String> words = new java.util.ArrayList<>();
            int depth = 0;
            while (!parser.isAtEnd()) {
                if (depth == 0 && (parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN))) break;
                if (parser.check(TokenType.LEFT_PAREN)) depth++;
                if (parser.check(TokenType.RIGHT_PAREN)) depth--;
                words.add(parser.advance().value());
            }
            if (!"OUT".equals(mode)) {
                types.add(String.join(" ", withoutParameterName(words)));
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
            // The mapping is named by its server, so a server that was never created is what
            // PostgreSQL reports missing.
            String mappingServer = "";
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                if (parser.matchKeyword("SERVER")) { mappingServer = parser.readIdentifier(); continue; }
                parser.advance();
            }
            if (mappingServer.isEmpty()) return new SetStmt("alter_noop", "ok");
            return new SetStmt("alter_object", "server" + STUB_SEP + mappingServer + STUB_SEP + "");
        }
        if (parser.matchKeyword("ROLE") || parser.matchKeyword("USER")) return roleParser.parseAlterRole();
        if (parser.matchKeyword("POLICY")) return policyParser.parseAlterPolicy();
        if (parser.matchKeywords("DEFAULT", "PRIVILEGES")) return parseAlterDefaultPrivileges();
        if (parser.matchKeywords("TEXT", "SEARCH")) {
            return parseAlterTextSearch();
        }
        if (parser.matchKeyword("GROUP")) return roleParser.parseAlterRole();

        if (parser.matchKeyword("MATERIALIZED")) {
            parser.expectKeyword("VIEW");
            boolean viewIfExists = parser.matchKeywords("IF", "EXISTS");
            String viewName = parser.readIdentifier();
            // Keep the qualifier: ALTER MATERIALIZED VIEW a.v names a's relation and no other
            // schema's, and dropping it aimed the complaint at a name in the wrong schema.
            String viewSchema = null;
            if (parser.match(TokenType.DOT)) {
                viewSchema = viewName;
                viewName = parser.readIdentifier();
            }
            String writtenView = viewSchema == null ? viewName : viewSchema + "." + viewName;
            if (parser.matchKeywords("RENAME", "COLUMN")) {
                String oldCol = parser.readIdentifier();
                parser.expectKeyword("TO");
                String newCol = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return renameRelationColumn(viewName, oldCol, newCol, viewIfExists);
            }
            if (parser.matchKeywords("RENAME", "TO")) {
                return new AlterViewStmt(writtenView, parser.readIdentifier(), viewIfExists,
                        AlterViewStmt.Action.RENAME_TO, null, true);
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterViewStmt(writtenView, parser.readIdentifier(), viewIfExists,
                        AlterViewStmt.Action.OWNER_TO, null, true);
            }
            if (parser.matchKeywords("SET", "SCHEMA")) {
                String newSchema = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return new AlterTableStmt(viewSchema, viewName,
                        Cols.listOf((AlterTableStmt.AlterAction)
                                new AlterTableStmt.SetSchema(newSchema)), viewIfExists);
            }
            if (parser.matchKeyword("SET") && parser.check(TokenType.LEFT_PAREN)) {
                Map<String, String> opts = parseViewWithOptions();
                return new AlterViewStmt(writtenView, null, viewIfExists,
                        AlterViewStmt.Action.SET_OPTIONS, opts, true);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterViewStmt(writtenView, null, viewIfExists,
                    AlterViewStmt.Action.NO_OP, null, true);
        }

        if (parser.matchKeyword("VIEW")) {
            boolean viewIfExists = parser.matchKeywords("IF", "EXISTS");
            String viewName = parser.readIdentifier();
            // Keep the qualifier: ALTER VIEW a.v names a's relation and no other schema's.
            String viewSchema = null;
            if (parser.match(TokenType.DOT)) {
                viewSchema = viewName;
                viewName = parser.readIdentifier();
            }
            String writtenView = viewSchema == null ? viewName : viewSchema + "." + viewName;
            if (parser.matchKeywords("RENAME", "COLUMN")) {
                String oldCol = parser.readIdentifier();
                parser.expectKeyword("TO");
                String newCol = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return renameRelationColumn(viewName, oldCol, newCol, viewIfExists);
            }
            if (parser.matchKeywords("SET", "SCHEMA")) {
                String newSchema = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return new AlterTableStmt(viewSchema, viewName,
                        Cols.listOf((AlterTableStmt.AlterAction)
                                new AlterTableStmt.SetSchema(newSchema)), viewIfExists);
            }
            if (parser.matchKeywords("RENAME", "TO")) {
                return new AlterViewStmt(writtenView, parser.readIdentifier(), viewIfExists, AlterViewStmt.Action.RENAME_TO);
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterViewStmt(writtenView, parser.readIdentifier(), viewIfExists, AlterViewStmt.Action.OWNER_TO);
            }
            if (parser.matchKeyword("SET") && parser.check(TokenType.LEFT_PAREN)) {
                Map<String, String> opts = parseViewWithOptions();
                return new AlterViewStmt(writtenView, null, viewIfExists, AlterViewStmt.Action.SET_OPTIONS, opts);
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterViewStmt(writtenView, null, viewIfExists, AlterViewStmt.Action.NO_OP);
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
            // ALTER SCHEMA has no IF EXISTS form. IF is an ordinary word, so PostgreSQL takes it
            // for the schema name and stops at the EXISTS behind it.
            if (parser.checkKeyword("EXISTS")) {
                throw ParseException.saying("syntax error at or near \"" + parser.peek().value()
                        + "\"", parser.peek(), "42601");
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                return new AlterSchemaOwnerStmt(schemaName, parser.readIdentifier());
            }
            if (parser.matchKeywords("RENAME", "TO")) {
                String newName = parser.readIdentifier();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return new AlterSchemaRenameStmt(schemaName, newName);
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

        // ALTER EVENT TRIGGER
        if (parser.matchKeywords("EVENT", "TRIGGER")) return parseAlterEventTrigger();

        if (parser.matchKeywords("FOREIGN", "DATA", "WRAPPER")) {
            return parseAlterObject("foreign-data wrapper");
        }
        if (parser.matchKeyword("SERVER")) {
            return parseAlterServer();
        }
        if (parser.matchKeywords("FOREIGN", "TABLE")) {
            return parseAlterObject("foreign table");
        }
        if (parser.matchKeyword("PUBLICATION")) {
            return parseAlterPublication();
        }
        if (parser.matchKeyword("SUBSCRIPTION")) {
            return parseAlterObject("subscription");
        }

        // ALTER EXTENSION — parse SET SCHEMA and UPDATE
        if (parser.matchKeyword("EXTENSION")) {
            String extName = parser.readIdentifierOrString();
            if (parser.matchKeywords("SET", "SCHEMA")) {
                String newSchema = parser.readIdentifierOrString();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
                return new SetStmt("alter_extension_set_schema", extName + ":" + newSchema);
            }
            // UPDATE, ADD/DROP member and the rest change nothing memgres records, but PostgreSQL
            // still refuses one on an extension that was never installed. A bare UPDATE also says
            // so when the installed version is already the newest one.
            boolean bareUpdate = parser.matchKeyword("UPDATE") && !parser.checkKeyword("TO");
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_object", "extension" + STUB_SEP + extName + STUB_SEP + ""
                    + STUB_SEP + (bareUpdate ? "update" : ""));
        }
        if (parser.matchKeyword("AGGREGATE")) return parseAlterAggregate();
        // ALTER on a name that was never created is an error in PostgreSQL, so each of these
        // carries the name it was written with and is checked before it is ignored.
        if (parser.matchKeyword("COLLATION")) return parseAlterStub("collation");
        if (parser.matchKeyword("CONVERSION")) return parseAlterStub("conversion");
        if (parser.matchKeyword("TABLESPACE")) return parseAlterStub("tablespace");
        if (parser.matchKeyword("LANGUAGE") || parser.matchKeywords("PROCEDURAL", "LANGUAGE")) {
            return parseAlterStub("language");
        }
        if (parser.matchKeyword("RULE")) return parseAlterRuleStub();
        if (parser.matchKeyword("TRIGGER")) return parseAlterTriggerStub();
        if (parser.matchKeywords("LARGE", "OBJECT")) {
            String loid = parser.advance().value();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_object", "large object" + STUB_SEP + loid + STUB_SEP + "");
        }
        if (parser.matchKeyword("TRANSFORM")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_noop", "ok");
        }
        if (parser.matchKeyword("STATISTICS")) {
            return parseAlterStatistics();
        }
        parser.expectKeyword("TABLE");

        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        boolean only = parser.matchKeyword("ONLY");
        String schema = null;
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { schema = table; table = parser.readIdentifier(); }

        List<AlterTableStmt.AlterAction> actions = new ArrayList<>();
        do {
            actions.add(alterActionParser.parseAlterAction());
        } while (parser.match(TokenType.COMMA));

        return new AlterTableStmt(schema, table, actions, ifExists, only);
    }

    // ---- CREATE TRIGGER ----

    CreateTriggerStmt parseCreateTrigger(boolean orReplace) {
        return parseCreateTrigger(orReplace, false);
    }

    /**
     * A constraint trigger is a deferred check on a completed row change, so PG's grammar admits
     * only {@code AFTER ... FOR EACH ROW} — the other spellings are not a definition it rejects
     * later but a sentence the grammar never had, which is why they come back as syntax errors.
     */
    CreateTriggerStmt parseCreateTrigger(boolean orReplace, boolean constraint) {
        String name = parser.readIdentifier();

        Token timingToken = parser.peek();
        String timing;
        if (parser.matchKeyword("BEFORE")) timing = "BEFORE";
        else if (parser.matchKeyword("AFTER")) timing = "AFTER";
        else if (parser.matchKeywords("INSTEAD", "OF")) timing = "INSTEAD OF";
        else throw new ParseException("Expected BEFORE, AFTER, or INSTEAD OF", parser.peek());
        if (constraint && !"AFTER".equals(timing)) {
            throw new ParseException("syntax error at or near \"" + timingToken.value() + "\"", timingToken);
        }

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

        boolean trigDeferrable = parser.matchKeyword("DEFERRABLE");
        boolean trigInitiallyDeferred = false;
        if (parser.matchKeyword("INITIALLY")) {
            if (parser.matchKeyword("DEFERRED")) {
                trigInitiallyDeferred = true;
            } else {
                parser.matchKeyword("IMMEDIATE");
            }
        }

        parser.expectKeyword("FOR");
        parser.matchKeyword("EACH");
        boolean forEachRow = true;
        Token levelToken = parser.peek();
        if (parser.matchKeyword("STATEMENT")) {
            forEachRow = false;
        } else {
            parser.expectKeyword("ROW");
        }
        if (constraint && !forEachRow) {
            throw new ParseException("syntax error at or near \"" + levelToken.value() + "\"", levelToken);
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
        if (parser.match(TokenType.DOT)) funcName = funcName + "." + parser.readIdentifier();
        parser.expect(TokenType.LEFT_PAREN);
        List<String> funcArgs = new ArrayList<>();
        while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
            String argToken = parser.advance().value;
            // Strip surrounding quotes from string literal args
            if (argToken.length() >= 2 && argToken.startsWith("'") && argToken.endsWith("'")) {
                argToken = argToken.substring(1, argToken.length() - 1);
            }
            funcArgs.add(argToken);
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);

        return new CreateTriggerStmt(name, timing, events, table, tableSchema, funcName, orReplace, whenClause,
                updateOfColumns.isEmpty() ? null : updateOfColumns, newTransitionTable, oldTransitionTable, !forEachRow,
                trigDeferrable, trigInitiallyDeferred, funcArgs.isEmpty() ? null : funcArgs);
    }

    // ---- CREATE VIEW ----

    /**
     * Point a recursive view's self-references at the CTE that stands in for it. PG allows the
     * self-reference only in a FROM clause of the recursive term, so the FROM lists of the body
     * and of each half of a set operation are where it can appear.
     */
    private void retargetSelfReference(Statement stmt, String viewName, String cteName) {
        if (stmt instanceof SetOpStmt) {
            SetOpStmt setOp = (SetOpStmt) stmt;
            retargetSelfReference(setOp.left(), viewName, cteName);
            retargetSelfReference(setOp.right(), viewName, cteName);
            return;
        }
        if (!(stmt instanceof SelectStmt)) return;
        SelectStmt sel = (SelectStmt) stmt;
        if (sel.from() == null) return;
        for (int i = 0; i < sel.from().size(); i++) {
            SelectStmt.FromItem item = sel.from().get(i);
            if (!(item instanceof SelectStmt.TableRef)) continue;
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            if (ref.schema() != null || !viewName.equalsIgnoreCase(ref.table())) continue;
            String alias = ref.alias() != null ? ref.alias() : viewName;
            sel.from().set(i, new SelectStmt.TableRef(null, cteName, alias));
        }
    }

    /**
     * A recursive view names itself in its own body. PG defines it as exactly the view whose
     * query is a WITH RECURSIVE CTE of the same name selected from, so that is what we build.
     */
    private CreateViewStmt parseCreateRecursiveView(boolean orReplace) {
        if (parser.matchKeywords("IF", "NOT", "EXISTS")) orReplace = true;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();

        parser.expect(TokenType.LEFT_PAREN);
        List<String> columnNames = parser.parseIdentifierList();
        parser.expect(TokenType.RIGHT_PAREN);

        parser.expectKeyword("AS");
        int viewParens = Math.max(0, parser.countLeadingParensBeforeQuery());
        parser.consumeLeadingParens(viewParens);
        Statement body = parser.tryParseSetOp(parser.parseSelect());
        parser.consumeTrailingParens(viewParens);

        // The body names the view to refer to itself. Point those references at the CTE instead,
        // keeping the view's name as the alias so qualified column references still read the same
        // — otherwise resolving the name would expand the view again, without end.
        String cteName = "__recursive_view_" + name.toLowerCase() + "__";
        retargetSelfReference(body, name, cteName);

        SelectStmt.CommonTableExpr cte = new SelectStmt.CommonTableExpr(
                cteName, columnNames, body, true, null, false, null, null, null, null);
        List<SelectStmt.SelectTarget> targets = new ArrayList<>();
        for (String col : columnNames) {
            targets.add(new SelectStmt.SelectTarget(new ColumnRef(null, col), null));
        }
        List<SelectStmt.FromItem> from = new ArrayList<>();
        from.add(new SelectStmt.TableRef(null, cteName, name));
        SelectStmt query = new SelectStmt(false, null, targets, from, null, null, null, null,
                null, null, null, Cols.listOf(cte), null, null, false);
        return new CreateViewStmt(name, query, orReplace, false, columnNames, true);
    }

    CreateViewStmt parseCreateView(boolean orReplace, boolean materialized) {
        if (parser.matchKeywords("IF", "NOT", "EXISTS")) orReplace = true;
        String name = parser.readIdentifier();
        String viewSchema = null;
        if (parser.match(TokenType.DOT)) { viewSchema = name; name = parser.readIdentifier(); }

        List<String> columnNames = null;
        if (parser.check(TokenType.LEFT_PAREN) && !parser.checkKeyword("AS")) {
            parser.expect(TokenType.LEFT_PAREN);
            columnNames = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        if (materialized) {
            parser.expectKeyword("AS");
            int viewParens = Math.max(0, parser.countLeadingParensBeforeQuery());
            parser.consumeLeadingParens(viewParens);
            Statement query = parser.tryParseSetOp(parseViewBody());
            parser.consumeTrailingParens(viewParens);
            boolean withData = true;
            if (parser.matchKeyword("WITH")) {
                if (parser.matchKeyword("NO")) { parser.expectKeyword("DATA"); withData = false; }
                else parser.expectKeyword("DATA");
            }
            return new CreateViewStmt(name, query, orReplace, true, columnNames, withData).withSchema(viewSchema);
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
        int viewParens2 = Math.max(0, parser.countLeadingParensBeforeQuery());
        parser.consumeLeadingParens(viewParens2);
        Statement query = parser.tryParseSetOp(parseViewBody());
        parser.consumeTrailingParens(viewParens2);

        String checkOption = null;
        if (parser.matchKeyword("WITH")) {
            if (parser.matchKeyword("CASCADED")) checkOption = "CASCADED";
            else if (parser.matchKeyword("LOCAL")) checkOption = "LOCAL";
            parser.expectKeyword("CHECK");
            parser.expectKeyword("OPTION");
            if (checkOption == null) checkOption = "CASCADED";
        }

        return new CreateViewStmt(name, query, orReplace, false, columnNames, true, checkOption, withOptions)
                .withSchema(viewSchema);
    }

    /**
     * A view's body. It is usually a SELECT, but a bare VALUES list is a query in its own right
     * and PostgreSQL accepts one — {@code CREATE VIEW v AS VALUES (1), (2)} defines a two-row
     * view — so the same body parser the standalone statement uses is used here.
     */
    private Statement parseViewBody() {
        if (parser.checkKeyword("VALUES")) return parser.parseValuesBody();
        return parser.parseSelect();
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
        boolean concurrently = parser.matchKeyword("CONCURRENTLY");
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        boolean withData = true;
        if (parser.matchKeyword("WITH")) {
            if (parser.matchKeyword("NO")) { parser.expectKeyword("DATA"); withData = false; }
            else parser.expectKeyword("DATA");
        }
        return new RefreshMaterializedViewStmt(name, withData, concurrently);
    }

    // ---- Misc small CREATE statements ----

    CreateTypeStmt parseCreateType() {
        String name = parser.readIdentifier();
        // The schema is kept, not discarded: a.e and b.e are two types, and which one this is
        // decides where it lands and which of them a later name resolves to.
        String schema = null;
        if (parser.match(TokenType.DOT)) { schema = name; name = parser.readIdentifier(); }
        // CREATE TYPE name; with no definition is a shell type — the documented first step of
        // defining a base type, and the name a base type's I/O functions refer to.
        if (!parser.checkKeyword("AS")) {
            CreateTypeStmt shell = new CreateTypeStmt(name, null, null);
            shell.setShell(true);
            shell.setSchemaName(schema);
            return shell;
        }
        parser.expectKeyword("AS");
        if (parser.matchKeyword("ENUM")) {
            parser.expect(TokenType.LEFT_PAREN);
            List<String> labels = new ArrayList<>();
            if (!parser.check(TokenType.RIGHT_PAREN)) {
                do { labels.add(parser.expect(TokenType.STRING_LITERAL).value()); } while (parser.match(TokenType.COMMA));
            }
            parser.expect(TokenType.RIGHT_PAREN);
            CreateTypeStmt e = new CreateTypeStmt(name, labels);
            e.setSchemaName(schema);
            return e;
        }
        if (parser.matchKeyword("RANGE")) {
            parser.expect(TokenType.LEFT_PAREN);
            String rangeSubtype = null;
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                if (parser.checkIdentCI("SUBTYPE")) {
                    parser.advance();
                    parser.expect(TokenType.EQUALS);
                    rangeSubtype = parser.parseTypeName();
                } else {
                    parser.advance();
                }
            }
            parser.expect(TokenType.RIGHT_PAREN);
            if (rangeSubtype == null) {
                throw PgErrors.syntax("type attribute \"subtype\" is required");
            }
            CreateTypeStmt r = new CreateTypeStmt(name, null, null, rangeSubtype);
            r.setSchemaName(schema);
            return r;
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
        CreateTypeStmt c = new CreateTypeStmt(name, null, fields);
        c.setSchemaName(schema);
        return c;
    }

    CreateExtensionStmt parseCreateExtension() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifierOrString();
        // Parse optional WITH, SCHEMA, VERSION, CASCADE clauses
        parser.matchKeyword("WITH"); // optional WITH keyword
        String schema = null;
        String version = null;
        boolean cascade = false;
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("SCHEMA")) {
                schema = parser.readIdentifierOrString();
            } else if (parser.matchKeyword("VERSION")) {
                version = parser.readIdentifierOrString();
            } else if (parser.matchKeyword("CASCADE")) {
                cascade = true;
            } else if (parser.matchKeyword("FROM")) {
                // FROM old_version — skip
                parser.readIdentifierOrString();
            } else {
                break;
            }
        }
        return new CreateExtensionStmt(name, ifNotExists, schema, version, cascade);
    }

    Statement parseCreateAggregate() {
        String name = parser.readIdentifier();
        // Parse argument types: CREATE AGGREGATE name ( argtype [, ...] ) ( ... )
        // or CREATE AGGREGATE name ( ORDER BY argtype ) for ordered-set aggregates
        List<String> argTypes = new ArrayList<>();
        // Arguments before ORDER BY are the direct arguments of an ordered-set aggregate; they
        // are not passed to the transition function, so the two groups have to stay apart.
        int directArgCount = -1;
        if (parser.match(TokenType.LEFT_PAREN)) {
            // Check for ORDER BY (ordered-set aggregate) or * or type list
            if (parser.matchKeyword("ORDER")) {
                parser.matchKeyword("BY");
                directArgCount = 0;
            }
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                // Handle ORDER BY appearing mid-list (ordered-set aggregates)
                if (parser.checkKeyword("ORDER")) {
                    parser.advance(); // consume ORDER
                    parser.matchKeyword("BY"); // consume BY
                    directArgCount = argTypes.size();
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

        return new CreateAggregateStmt(name, argTypes, directArgCount, sfunc, stype, initcond,
                finalfunc, combinefunc, sortop);
    }

    /**
     * ALTER VIEW ... RENAME COLUMN says the same thing as the ALTER TABLE spelling, so it is
     * turned into that and handled in one place.
     */
    private AlterTableStmt renameRelationColumn(String relation, String oldCol, String newCol,
                                                 boolean ifExists) {
        return new AlterTableStmt(null, relation,
                Cols.listOf((AlterTableStmt.AlterAction)
                        new AlterTableStmt.RenameColumn(oldCol, newCol)), ifExists);
    }

    CreateRuleStmt parseCreateRule() {
        return parseCreateRule(false);
    }

    CreateRuleStmt parseCreateRule(boolean orReplace) {
        String name = parser.readIdentifier();
        parser.expectKeyword("AS");
        parser.expectKeyword("ON");
        Token eventToken = parser.peek();
        String event = parser.readIdentifier().toUpperCase();
        if (!"SELECT".equals(event) && !"INSERT".equals(event)
                && !"UPDATE".equals(event) && !"DELETE".equals(event)) {
            throw new ParseException("unrecognized rule event", eventToken);
        }
        parser.expectKeyword("TO");
        String table = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) table = table + "." + parser.readIdentifier();
        StringBuilder where = new StringBuilder();
        if (parser.matchKeyword("WHERE")) {
            int depth = 0;
            while (!parser.isAtEnd() && !(parser.checkKeyword("DO") && depth == 0)) {
                if (parser.check(TokenType.LEFT_PAREN)) depth++;
                if (parser.check(TokenType.RIGHT_PAREN)) depth--;
                where.append(ruleCommandToken(parser.advance())).append(' ');
            }
        }
        parser.expectKeyword("DO");
        String action;
        if (parser.matchKeyword("INSTEAD")) action = "INSTEAD";
        else if (parser.matchKeyword("ALSO")) action = "ALSO";
        else action = "ALSO";
        List<String> commands = new ArrayList<>();
        if (parser.matchKeyword("NOTHING")) {
            commands.add("NOTHING");
        } else if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            parseRuleActionList(commands);
        } else {
            StringBuilder sb = new StringBuilder();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                sb.append(ruleCommandToken(parser.advance())).append(' ');
            }
            if (sb.length() > 0) commands.add(sb.toString().trim());
        }
        return new CreateRuleStmt(name, event, table, action, commands,
                where.length() == 0 ? null : where.toString().trim(), orReplace);
    }

    /**
     * The parenthesised form of a rule action holds several statements separated by semicolons,
     * so the closing parenthesis — not the first semicolon — ends the rule. Running off the end
     * of the input means the parenthesis was never closed.
     */
    private void parseRuleActionList(List<String> commands) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (true) {
            if (parser.isAtEnd()) throw PgErrors.syntax("syntax error at end of input");
            if (depth == 0 && parser.check(TokenType.RIGHT_PAREN)) {
                parser.advance();
                break;
            }
            if (parser.check(TokenType.SEMICOLON)) {
                parser.advance();
                if (sb.length() > 0) {
                    commands.add(sb.toString().trim());
                    sb.setLength(0);
                }
                continue;
            }
            if (sb.length() == 0 && parser.checkKeyword("NOTHING")) {
                throw new ParseException("NOTHING is not a rule action here", parser.peek());
            }
            if (parser.check(TokenType.LEFT_PAREN)) depth++;
            else if (parser.check(TokenType.RIGHT_PAREN)) depth--;
            sb.append(ruleCommandToken(parser.advance())).append(' ');
        }
        if (sb.length() > 0) commands.add(sb.toString().trim());
    }

    /**
     * A rule's action is kept as text and re-parsed when the rule fires, so each token has to go
     * back in the form it was written. A literal's token value carries only its content, and
     * putting that back bare would turn it into an identifier.
     */
    private static String ruleCommandToken(Token token) {
        switch (token.type()) {
            case STRING_LITERAL:
                return "'" + token.value().replace("'", "''") + "'";
            case DOLLAR_STRING_LITERAL:
                return "$$" + token.value() + "$$";
            case BIT_STRING_LITERAL:
                return "B'" + token.value() + "'";
            case QUOTED_IDENTIFIER:
                return "\"" + token.value().replace("\"", "\"\"") + "\"";
            default:
                return token.value();
        }
    }

    Statement parseCreateSchema() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        // The pg_ prefix is reserved for system schemas, so a schema cannot claim it.
        if (name != null && name.toLowerCase().startsWith("pg_")) {
            throw new MemgresException("unacceptable schema name \"" + name + "\"", "42939");
        }
        String authorization = null;
        if (parser.matchKeyword("AUTHORIZATION")) authorization = parser.readIdentifier();
        return new CreateSchemaStmt(name, ifNotExists, authorization);
    }

    CreateDomainStmt parseCreateDomain() {
        String name = parser.readIdentifier();
        String schema = null;
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        parser.expectKeyword("AS");
        String baseType = parser.parseTypeName();
        Expression defaultExpr = null;
        boolean notNull = false;
        boolean sawNotNull = false, sawNull = false, sawDefault = false;
        Expression checkExpr = null;
        String constraintName = null;
        String collation = null;
        if (parser.matchKeyword("COLLATE")) {
            collation = parser.readIdentifierOrString();
            ExpressionParser.validateCollationStatic(collation, parser.peek());
            if (parser.checkKeyword("COLLATE")) {
                throw PgErrors.syntax("multiple COLLATE clauses not allowed");
            }
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("DEFAULT")) {
                if (sawDefault) throw PgErrors.syntax("multiple default expressions");
                sawDefault = true;
                defaultExpr = parser.parseExpression();
            }
            else if (parser.matchKeywords("NOT", "NULL")) {
                // Writing it twice is not merely redundant to PostgreSQL: it refuses the domain.
                if (sawNotNull) {
                    throw new MemgresException("redundant NOT NULL constraint definition", "42P17");
                }
                notNull = true;
                sawNotNull = true;
            }
            else if (parser.matchKeyword("NULL")) { notNull = false; sawNull = true; }
            else if (parser.matchKeyword("CHECK")) {
                parser.expect(TokenType.LEFT_PAREN);
                checkExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
            } else if (parser.matchKeyword("CONSTRAINT")) { constraintName = parser.readIdentifier(); }
            // A domain has no rows, so the table-level constraint forms are meaningless on one.
            else if (parser.checkKeyword("UNIQUE")) {
                throw PgErrors.syntax("unique constraints not possible for domains");
            }
            else if (parser.checkKeyword("PRIMARY") && parser.checkKeywordAt(1, "KEY")) {
                throw PgErrors.syntax("primary key constraints not possible for domains");
            }
            else if (parser.checkKeyword("REFERENCES")) {
                throw PgErrors.syntax("foreign key constraints not possible for domains");
            }
            else { break; }
            if (sawNotNull && sawNull) {
                throw PgErrors.syntax("conflicting NULL/NOT NULL constraints");
            }
        }
        CreateDomainStmt domainStmt =
                new CreateDomainStmt(name, baseType, defaultExpr, notNull, checkExpr, constraintName);
        domainStmt.setCollation(collation);
        domainStmt.setSchemaName(schema);
        return domainStmt;
    }

    // ---- SEQUENCE ----

    long readSeqLong() {
        boolean neg = false;
        if (parser.check(TokenType.MINUS)) { parser.advance(); neg = true; }
        long val = Long.parseLong(parser.advance().value());
        return neg ? -val : val;
    }

    private void parseSequenceOptions(Long[] startWith, Long[] incrementBy, Long[] minValue,
                                      Long[] maxValue, Boolean[] cycle, Integer[] cache, String[] asType,
                                      String[] ownedByTable, String[] ownedByColumn) {
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
                if (parser.matchKeyword("NONE")) {
                    if (ownedByTable != null) { ownedByTable[0] = "NONE"; ownedByColumn[0] = "NONE"; }
                    continue;
                }
                String part1 = parser.readIdentifier();
                String part2 = null, part3 = null;
                if (parser.match(TokenType.DOT)) { part2 = parser.readIdentifier(); if (parser.match(TokenType.DOT)) part3 = parser.readIdentifier(); }
                if (ownedByTable != null) {
                    if (part3 != null) { ownedByTable[0] = part2; ownedByColumn[0] = part3; }
                    else if (part2 != null) { ownedByTable[0] = part1; ownedByColumn[0] = part2; }
                    else { ownedByTable[0] = part1; }
                }
                continue;
            }
            if (parser.matchKeyword("AS")) { if (asType != null) asType[0] = parser.readIdentifier(); else parser.readIdentifier(); continue; }
            break;
        }
    }

    CreateSequenceStmt parseCreateSequence(boolean temporary) {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        String seqSchema = null;
        if (parser.match(TokenType.DOT)) { seqSchema = name; name = parser.readIdentifier(); }
        Long[] startWith = {null}, incrementBy = {null}, minValue = {null}, maxValue = {null};
        Boolean[] cycle = {null};
        Integer[] cache = {null};
        String[] asType = {null};
        String[] ownedByTable = {null}, ownedByColumn = {null};
        parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle, cache, asType, ownedByTable, ownedByColumn);
        CreateSequenceStmt stmt = new CreateSequenceStmt(name, ifNotExists, startWith[0], incrementBy[0], minValue[0], maxValue[0], cycle[0], temporary);
        stmt.setCache(cache[0]);
        stmt.setAsType(asType[0]);
        stmt.setOwnedBy(ownedByTable[0], ownedByColumn[0]);
        stmt.setSchema(seqSchema);
        return stmt;
    }

    AlterSequenceStmt parseAlterSequence() {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        String name = parser.readIdentifier();
        // Keep the qualifier: ALTER SEQUENCE a.s names a's sequence and no other schema's, and
        // dropping the schema here made the statement alter — and relocate — whichever one it found.
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        if (parser.matchKeywords("RENAME", "TO")) {
            AlterSequenceStmt renamed = AlterSequenceStmt.renameTo(name, parser.readIdentifier());
            renamed.setIfExists(ifExists);
            return renamed;
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            AlterSequenceStmt moved = new AlterSequenceStmt(name, false, null, null, null,
                    null, null, null, null, null, null, null);
            moved.setIfExists(ifExists);
            moved.setSetSchema(parser.readIdentifier());
            return moved;
        }
        boolean restart = false;
        Long restartWith = null;
        Long[] startWith = {null}, incrementBy = {null}, minValue = {null}, maxValue = {null};
        Boolean[] cycle = {null};
        String[] ownedByTable = {null}, ownedByColumn = {null};
        String[] asType = {null};
        Integer[] cache = {null};
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.matchKeyword("RESTART")) {
                restart = true;
                if (parser.matchKeyword("WITH")) { restartWith = readSeqLong(); }
                else if (parser.check(TokenType.INTEGER_LITERAL) || parser.check(TokenType.MINUS)) { restartWith = readSeqLong(); }
                continue;
            }
            if (parser.matchKeywords("OWNER", "TO")) {
                AlterSequenceStmt owned = new AlterSequenceStmt(name, parser.readIdentifier());
                owned.setIfExists(ifExists);
                return owned;
            }
            int saved = parser.pos;
            parseSequenceOptions(startWith, incrementBy, minValue, maxValue, cycle, cache, asType, ownedByTable, ownedByColumn);
            if (parser.pos == saved) break;
        }
        AlterSequenceStmt stmt = new AlterSequenceStmt(name, restart, restartWith, incrementBy[0], minValue[0],
                maxValue[0], startWith[0], cycle[0], null, null, ownedByTable[0], ownedByColumn[0]);
        stmt.setIfExists(ifExists);
        stmt.setCache(cache[0]);
        stmt.setAsType(asType[0]);
        return stmt;
    }

    // ---- TRUNCATE ----

    TruncateStmt parseTruncate() {
        parser.expectKeyword("TRUNCATE");
        parser.matchKeyword("TABLE");
        List<String> tables = new ArrayList<>();
        List<Boolean> onlyFlags = new ArrayList<>();
        onlyFlags.add(parser.matchKeyword("ONLY"));
        String tbl = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) tbl = tbl + "." + parser.readIdentifier();
        tables.add(tbl);
        while (parser.match(TokenType.COMMA)) {
            onlyFlags.add(parser.matchKeyword("ONLY"));
            String next = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) next = next + "." + parser.readIdentifier();
            tables.add(next);
        }
        boolean restartIdentity = false;
        if (parser.matchKeyword("RESTART")) { parser.expectKeyword("IDENTITY"); restartIdentity = true; }
        else if (parser.matchKeyword("CONTINUE")) { parser.expectKeyword("IDENTITY"); }
        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");
        return new TruncateStmt(tables, cascade, restartIdentity, onlyFlags);
    }

    // ---- ALTER specific types ----

    /**
     * {@code ALTER <kind> name [RENAME TO new | anything else]} for the object kinds memgres
     * accepts without implementing. The name travels with the statement so the executor can
     * refuse one that was never created, and a RENAME takes effect on the remembered name.
     */
    private Statement parseAlterStub(String kind) {
        String name = parser.readIdentifierOrString();
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        // SET SCHEMA and OWNER TO change nothing memgres records for these kinds, but PostgreSQL
        // still refuses a schema or a role that is not there, so both names are carried through
        // to be checked rather than discarded with the rest of the statement.
        String schema = "";
        String owner = "";
        if (parser.matchKeywords("SET", "SCHEMA")) schema = parser.readIdentifierOrString();
        else if (parser.matchKeywords("OWNER", "TO")) owner = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_stub", kind + STUB_SEP + name + STUB_SEP + newName
                + STUB_SEP + schema + STUB_SEP + owner);
    }

    /** {@code ALTER RULE name ON relation ...}: the rule is looked up on its relation. */
    private Statement parseAlterRuleStub() {
        String name = parser.readIdentifierOrString();
        String relation = "";
        if (parser.matchKeyword("ON")) {
            relation = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) relation = relation + "." + parser.readIdentifier();
        }
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_rule", name + STUB_SEP + relation + STUB_SEP + newName);
    }

    /** {@code ALTER TRIGGER name ON relation RENAME TO new}. */
    private Statement parseAlterTriggerStub() {
        String name = parser.readIdentifierOrString();
        String relation = "";
        if (parser.matchKeyword("ON")) {
            relation = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) relation = relation + "." + parser.readIdentifier();
        }
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_trigger", name + STUB_SEP + relation + STUB_SEP + newName);
    }

    /** Separator inside the encoded payload of a stub statement; SQL text cannot contain it. */
    private static final String STUB_SEP = "\u0001";

    /**
     * {@code ALTER <kind> name ...} for the object kinds memgres keeps in a registry of its own
     * rather than among the stubs. The kind and the name travel with the statement so the executor
     * can refuse one that was never created, and a RENAME takes effect on the registered name.
     */
    private Statement parseAlterObject(String kind) {
        String name = parser.readIdentifierOrString();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifierOrString();
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        return new SetStmt("alter_object",
                kind + STUB_SEP + name + STUB_SEP + newName + STUB_SEP + "" + parseMoveOrReown());
    }

    /**
     * The tail of an ALTER that memgres records nothing for: a {@code SET SCHEMA} or an
     * {@code OWNER TO}. PostgreSQL still refuses a schema or a role that does not exist, so both
     * names are carried through to be checked rather than discarded with the rest of the
     * statement. Returns the two trailing payload fields, empty when neither clause was written.
     */
    private String parseMoveOrReown() {
        String schema = "";
        String owner = "";
        if (parser.matchKeywords("SET", "SCHEMA")) schema = parser.readIdentifierOrString();
        else if (parser.matchKeywords("OWNER", "TO")) owner = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return STUB_SEP + schema + STUB_SEP + owner;
    }

    AlterDomainStmt parseAlterDomain() {
        String domainName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) domainName = domainName + "." + parser.readIdentifier();
        if (parser.matchKeywords("SET", "DEFAULT")) {
            int startPos = parser.pos;
            Expression expr = parser.parseExpression();
            return new AlterDomainStmt(domainName, "SET_DEFAULT", buildRawSqlFromTokens(startPos, parser.pos), null, null, null);
        }
        if (parser.matchKeywords("DROP", "DEFAULT"))
            return new AlterDomainStmt(domainName, "DROP_DEFAULT", null, null, null, null);
        if (parser.matchKeywords("SET", "NOT", "NULL"))
            return new AlterDomainStmt(domainName, "SET_NOT_NULL", null, null, null, null);
        if (parser.matchKeywords("DROP", "NOT", "NULL"))
            return new AlterDomainStmt(domainName, "DROP_NOT_NULL", null, null, null, null);
        // The constraint name is optional: ALTER DOMAIN d ADD CHECK (...) is as valid as the
        // named form, and PostgreSQL then names the constraint itself.
        if (parser.matchKeyword("ADD")) {
            String constraintName = parser.matchKeyword("CONSTRAINT") ? parser.readIdentifier() : null;
            parser.expectKeyword("CHECK");
            parser.expect(TokenType.LEFT_PAREN);
            int startPos = parser.pos;
            Expression checkExpr = parser.parseExpression();
            String raw = buildRawSqlFromTokens(startPos, parser.pos);
            parser.expect(TokenType.RIGHT_PAREN);
            boolean notValid = parser.matchKeywords("NOT", "VALID");
            return new AlterDomainStmt(domainName, "ADD_CONSTRAINT", null, constraintName, checkExpr, raw, notValid, null);
        }
        if (parser.matchKeywords("DROP", "CONSTRAINT")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String name = parser.readIdentifier();
            return new AlterDomainStmt(domainName,
                    ifExists ? "DROP_CONSTRAINT_IF_EXISTS" : "DROP_CONSTRAINT", null, name, null, null);
        }
        if (parser.matchKeywords("VALIDATE", "CONSTRAINT")) {
            return new AlterDomainStmt(domainName, "VALIDATE", null, parser.readIdentifier(), null, null);
        }
        if (parser.matchKeywords("RENAME", "CONSTRAINT")) {
            String oldName = parser.readIdentifier();
            parser.expectKeyword("TO");
            String newName = parser.readIdentifier();
            return new AlterDomainStmt(domainName, "RENAME_CONSTRAINT", null, oldName, null, null, false, newName);
        }
        // RENAME TO and SET SCHEMA really move the domain; reading them as no-ops reported a
        // rename that never happened and left the old name answering.
        if (parser.matchKeywords("RENAME", "TO")) {
            return new AlterDomainStmt(domainName, "RENAME_TO", null, null, null, null, false,
                    parser.readIdentifier());
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            return new AlterDomainStmt(domainName, "SET_SCHEMA", null, null, null, null, false,
                    parser.readIdentifier());
        }
        // Nothing is recorded for a change of owner, but the role still has to be one that exists.
        if (parser.matchKeywords("OWNER", "TO")) {
            return new AlterDomainStmt(domainName, "OWNER_TO", null, null, null, null, false,
                    parser.readIdentifierOrString());
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new AlterDomainStmt(domainName, "NO_OP", null, null, null, null);
    }

    AlterTypeStmt parseAlterType() {
        String typeName = parser.readIdentifier();
        // Carried, not discarded: ALTER TYPE a.e names a.e however many other schemas hold an e,
        // and PostgreSQL reports the name it could not find as it was written.
        if (parser.match(TokenType.DOT)) typeName = typeName.toLowerCase() + "." + parser.readIdentifier();
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
        if (parser.matchKeywords("RENAME", "ATTRIBUTE")) {
            String attrName = parser.readIdentifier();
            parser.expectKeyword("TO");
            String newName = parser.readIdentifier();
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.RENAME_ATTRIBUTE, attrName, newName, false, null, null);
        }
        if (parser.matchKeywords("RENAME", "TO"))
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.RENAME_TO, parser.readIdentifier(), null, false, null, null);
        if (parser.matchKeywords("ADD", "ATTRIBUTE")) {
            // ADD ATTRIBUTE has no IF NOT EXISTS — only DROP ATTRIBUTE has IF EXISTS — so the
            // words are a syntax error rather than an attribute called "not".
            if (parser.matchKeyword("IF")) {
                throw new ParseException("syntax error", parser.peek());
            }
            String attrName = parser.readIdentifier();
            String attrType = parser.readIdentifier();
            // Handle multi-word types like "double precision"
            if (attrType.equalsIgnoreCase("double") && parser.matchKeyword("PRECISION")) {
                attrType = "double precision";
            } else if (attrType.equalsIgnoreCase("character") && parser.matchKeyword("VARYING")) {
                attrType = "character varying";
            }
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.ADD_ATTRIBUTE, attrName, attrType, false, null, null);
        }
        if (parser.matchKeywords("DROP", "ATTRIBUTE")) {
            boolean attrIfExists = parser.matchKeywords("IF", "EXISTS");
            String attrName = parser.readIdentifier();
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.DROP_ATTRIBUTE, attrName, null,
                    false, null, null, attrIfExists);
        }
        if (parser.matchKeywords("ALTER", "ATTRIBUTE")) {
            String attrName = parser.readIdentifier();
            parser.matchKeyword("SET"); // optional SET before DATA
            parser.matchKeyword("DATA"); // optional DATA
            parser.expectKeyword("TYPE");
            String newType = parser.readIdentifier();
            if (newType.equalsIgnoreCase("double") && parser.matchKeyword("PRECISION")) {
                newType = "double precision";
            } else if (newType.equalsIgnoreCase("character") && parser.matchKeyword("VARYING")) {
                newType = "character varying";
            }
            return new AlterTypeStmt(typeName, AlterTypeStmt.Action.ALTER_ATTRIBUTE_TYPE, attrName, newType, false, null, null);
        }
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

        // Signed numeric literal: -100, +5, -1.5
        if ((parser.check(TokenType.MINUS) || parser.check(TokenType.PLUS))
                && parser.pos + 1 < parser.tokens.size()
                && (parser.tokens.get(parser.pos + 1).type() == TokenType.INTEGER_LITERAL
                    || parser.tokens.get(parser.pos + 1).type() == TokenType.FLOAT_LITERAL)) {
            String sign = parser.advance().value();
            String num = parser.advance().value();
            skipBoundCast(parser);
            return sign + num;
        }

        // Typed literal: DATE '2026-04-01', TIMESTAMP '...'
        Token cur = parser.peek();
        Token next = parser.pos + 1 < parser.tokens.size() ? parser.tokens.get(parser.pos + 1) : null;
        if ((cur.type() == TokenType.KEYWORD || cur.type() == TokenType.IDENTIFIER)
                && next != null && next.type() == TokenType.STRING_LITERAL) {
            parser.advance(); // skip the type keyword
            String value = parser.advance().value();
            skipBoundCast(parser);
            // The lexer strips quotes; re-add them so string values stay distinguishable
            // from the bare keywords NULL / MINVALUE / MAXVALUE / TRUE / FALSE
            return "'" + value + "'";
        }

        // Anything else is an expression. PostgreSQL evaluates a partition bound when the
        // partition is created, so FOR VALUES FROM (abs(-1)) and FROM (1 + 1) are ordinary bounds,
        // and a bound that has no value then is reported for what is wrong with it rather than as
        // a syntax error. The source text is carried under a marker; the executor evaluates it.
        if (isBoundExpression(parser)) {
            int exprStart = parser.pos;
            parser.parseExpression();
            StringBuilder sb = new StringBuilder(Parser.BOUND_EXPRESSION_MARKER);
            for (int i = exprStart; i < parser.pos; i++) {
                Token t = parser.tokens.get(i);
                if (i > exprStart) sb.append(' ');
                sb.append(t.type() == TokenType.STRING_LITERAL
                        ? "'" + t.value().replace("'", "''") + "'" : t.value());
            }
            return sb.toString();
        }

        // Plain literal with optional cast
        Token valueTok = parser.advance();
        rejectColumnReferenceBound(valueTok);
        String value = valueTok.value();
        skipBoundCast(parser);
        if (valueTok.type() == TokenType.STRING_LITERAL
                || valueTok.type() == TokenType.DOLLAR_STRING_LITERAL) {
            return "'" + value + "'";
        }
        return value;
    }

    /**
     * A bound is a value the partition is given once, not something read off a row, so a name in
     * one is a column reference and PostgreSQL says so — {@code FOR VALUES FROM (id) TO (10)} and
     * {@code FOR VALUES IN (sad)} alike, the second being why an enum label has to be quoted here.
     *
     * <p>Only an identifier is refused. A keyword that stands for a value of its own —
     * MINVALUE, MAXVALUE, NULL, TRUE, FALSE, {@code current_date} — is a value, not a name.
     */
    static void rejectColumnReferenceBound(Token tok) {
        if (tok.type() == TokenType.IDENTIFIER || tok.type() == TokenType.QUOTED_IDENTIFIER) {
            throw new com.memgres.engine.MemgresException(
                    "cannot use column reference in partition bound expression", "0A000");
        }
    }

    /**
     * MODULUS and REMAINDER take an integer literal and nothing else: PostgreSQL's grammar has
     * {@code Iconst} there, so anything else is a syntax error rather than a bad bound.
     */
    static String readHashBoundInteger(Parser parser) {
        Token tok = parser.peek();
        if (tok.type() != TokenType.INTEGER_LITERAL) {
            throw new com.memgres.engine.MemgresException(
                    "syntax error at or near \"" + tok.value() + "\"", "42601");
        }
        return parser.advance().value();
    }

    /**
     * Whether the bound at this position is more than one literal token: a call, or a literal
     * followed by an operator. A bound that ends at a comma, the closing paren or a cast is the
     * plain literal the caller reads directly.
     */
    private static boolean isBoundExpression(Parser parser) {
        Token cur = parser.peek();
        Token next = parser.pos + 1 < parser.tokens.size() ? parser.tokens.get(parser.pos + 1) : null;
        if (next == null) return false;
        if ((cur.type() == TokenType.KEYWORD || cur.type() == TokenType.IDENTIFIER)
                && next.type() == TokenType.LEFT_PAREN) {
            return true;
        }
        return next.type() != TokenType.COMMA && next.type() != TokenType.RIGHT_PAREN
                && next.type() != TokenType.CAST;
    }

    private static void skipBoundCast(Parser parser) {
        if (!parser.match(TokenType.CAST)) return;
        parser.advance(); // type name
        while (parser.match(TokenType.DOT)) parser.advance(); // schema.type
        if (parser.match(TokenType.LEFT_PAREN)) { // varchar(100)
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) parser.advance();
            parser.match(TokenType.RIGHT_PAREN);
        }
        while (parser.match(TokenType.LEFT_BRACKET)) { // int[]
            while (!parser.check(TokenType.RIGHT_BRACKET) && !parser.isAtEnd()) parser.advance();
            parser.match(TokenType.RIGHT_BRACKET);
        }
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
        // The grammar only accepts a symbolic operator name here, so a word is a syntax error
        // at the parenthesis that follows it, exactly as PG reports it.
        for (int i = 0; i < name.length(); i++) {
            if ("+-*/<>=~!@#%^&|`?".indexOf(name.charAt(i)) < 0) {
                throw new com.memgres.engine.MemgresException("syntax error at or near \"(\"", "42601");
            }
        }

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
            case "interval":      // interval day to second
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
                    // A routine is identified by what a call passes it, so an OUT parameter may be
                    // written in the signature and is not part of what the signature matches.
                    // Matching on it too meant a signature memgres itself deparses — which names
                    // the OUT parameters, as PostgreSQL's does — named no routine memgres could
                    // find, and a dump of a function with OUT parameters would not restore.
                    // OUT is no keyword to the lexer, so it has to be recognised as the
                    // identifier it arrives as; checking only for a keyword never matched it and
                    // the word was read as the first half of the parameter's type.
                    String paramMode = "IN";
                    if (parser.checkKeyword("IN") || parser.checkKeyword("INOUT")
                            || parser.checkKeyword("VARIADIC") || parser.checkIdentifier("OUT")
                            || parser.checkIdentifier("INOUT") || parser.checkIdentifier("VARIADIC")) {
                        paramMode = parser.advance().value().toUpperCase();
                        if ("IN".equals(paramMode) && (parser.checkIdentifier("OUT")
                                || parser.checkKeyword("OUT"))) {
                            parser.advance();
                            paramMode = "INOUT";
                        }
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
                    if (!"OUT".equals(paramMode)) paramTypes.add(typeName.toString().trim());
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
        // DEPENDS ON EXTENSION / NO DEPENDS ON EXTENSION: memgres records no such dependency, but
        // PostgreSQL still refuses an extension that was never installed.
        int beforeDepends = parser.pos;
        boolean noDepends = matchIdentCI("NO");
        if (matchIdentCI("DEPENDS")) {
            parser.matchKeyword("ON");
            parser.matchKeyword("EXTENSION");
            String ext = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return AlterFunctionStmt.dependsOnExtension(funcName, schema, isProcedure, ext, paramTypes);
        }
        // Not this form after all; a bare NO belongs to whatever attribute follows.
        if (noDepends) parser.pos = beforeDepends;

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

        // The same option list CREATE reads, so the same two refusals apply to it.
        RoutineOptions opts = new RoutineOptions(isProcedure);

        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token at = parser.peek();
            if (parser.checkKeyword("VOLATILE") || parser.checkKeyword("STABLE")
                    || parser.checkKeyword("IMMUTABLE")) {
                opts.take(RoutineOptions.VOLATILITY, at);
                volatility = parser.advance().value();
            } else if (parser.matchKeyword("STRICT")) {
                opts.take(RoutineOptions.STRICT, at);
                strict = true;
            } else if (parser.matchKeywords("CALLED", "ON", "NULL", "INPUT")) {
                opts.take(RoutineOptions.STRICT, at);
                strict = false;
            } else if (parser.matchKeywords("RETURNS", "NULL", "ON", "NULL", "INPUT")) {
                opts.take(RoutineOptions.STRICT, at);
                strict = true;
            } else if (parser.matchKeywords("SECURITY", "DEFINER")) {
                opts.take(RoutineOptions.SECURITY, at);
                securityDefiner = true;
            } else if (parser.matchKeywords("SECURITY", "INVOKER")) {
                opts.take(RoutineOptions.SECURITY, at);
                securityDefiner = false;
            } else if (parser.matchKeywords("NOT", "LEAKPROOF")) {
                opts.take(RoutineOptions.LEAKPROOF, at);
                leakproof = false;
            } else if (parser.matchKeyword("LEAKPROOF")) {
                opts.take(RoutineOptions.LEAKPROOF, at);
                leakproof = true;
            } else if (parser.matchKeyword("COST")) {
                opts.take(RoutineOptions.COST, at);
                cost = routineCost(parser, "COST");
            } else if (parser.matchKeyword("ROWS")) {
                opts.take(RoutineOptions.ROWS, at);
                rows = routineCost(parser, "ROWS");
            } else if (parser.matchKeywords("PARALLEL", "SAFE")) {
                opts.take(RoutineOptions.PARALLEL, at);
                parallel = "SAFE";
            } else if (parser.matchKeywords("PARALLEL", "RESTRICTED")) {
                opts.take(RoutineOptions.PARALLEL, at);
                parallel = "RESTRICTED";
            } else if (parser.matchKeywords("PARALLEL", "UNSAFE")) {
                opts.take(RoutineOptions.PARALLEL, at);
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
        // Keep the qualifier: ALTER INDEX a.i names a's index and no other schema's.
        if (parser.match(TokenType.DOT)) indexName = indexName + "." + parser.readIdentifier();

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
            if (parser.match(TokenType.DOT)) partIdx = partIdx + "." + parser.readIdentifier();
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

    // ---- CREATE EVENT TRIGGER ----

    private static final java.util.Set<String> VALID_EVENT_TRIGGER_EVENTS = Cols.setOf(
            "ddl_command_start", "ddl_command_end", "sql_drop", "table_rewrite");

    Statement parseCreateEventTrigger() {
        String name = parser.readIdentifier();
        parser.expectKeyword("ON");
        String event = parser.readIdentifier();
        if (!VALID_EVENT_TRIGGER_EVENTS.contains(event)) {
            throw new com.memgres.engine.MemgresException(
                    "unrecognized event name \"" + event + "\"", "42601");
        }
        List<String> tags = null;
        if (parser.matchKeyword("WHEN")) {
            // WHEN TAG IN ('tag1', 'tag2', ...); TAG is the only filter variable PG defines
            String filterVar = parser.readIdentifier();
            if (!"tag".equalsIgnoreCase(filterVar)) {
                throw new com.memgres.engine.MemgresException(
                        "unrecognized filter variable \"" + filterVar.toLowerCase() + "\"", "42601");
            }
            parser.expectKeyword("IN");
            parser.match(TokenType.LEFT_PAREN);
            tags = new ArrayList<>();
            do {
                Token t = parser.peek();
                if (t.type() == TokenType.STRING_LITERAL) {
                    tags.add(t.value());
                    parser.advance();
                } else {
                    tags.add(parser.readIdentifier());
                }
            } while (parser.match(TokenType.COMMA));
            parser.match(TokenType.RIGHT_PAREN);
        }
        parser.expectKeyword("EXECUTE");
        if (!parser.matchKeyword("FUNCTION")) {
            parser.expectKeyword("PROCEDURE");
        }
        String funcName = parser.readIdentifier();
        parser.match(TokenType.LEFT_PAREN);
        parser.match(TokenType.RIGHT_PAREN);
        return new CreateEventTriggerStmt(name, event, tags, funcName);
    }

    // ---- DROP EVENT TRIGGER ----

    Statement parseDropEventTrigger() {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        String name = parser.readIdentifier();
        // optional CASCADE / RESTRICT
        parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");
        return new DropEventTriggerStmt(name, ifExists);
    }

    // ---- ALTER EVENT TRIGGER ----

    Statement parseAlterEventTrigger() {
        String name = parser.readIdentifier();
        if (parser.matchKeyword("DISABLE")) {
            return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.DISABLE, null);
        }
        if (parser.matchKeyword("ENABLE")) {
            if (parser.matchKeyword("REPLICA")) {
                return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.ENABLE_REPLICA, null);
            }
            if (parser.matchKeyword("ALWAYS")) {
                return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.ENABLE_ALWAYS, null);
            }
            return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.ENABLE, null);
        }
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.RENAME, newName);
        }
        if (parser.matchKeywords("OWNER", "TO")) {
            String newOwner = parser.readIdentifier();
            return new AlterEventTriggerStmt(name, AlterEventTriggerStmt.Action.OWNER, newOwner);
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_noop", "ok");
    }

    // ---- CREATE STATISTICS ----

    Statement parseCreateStatistics() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();

        // Optional (kinds): (ndistinct, dependencies, mcv)
        List<String> kinds = new ArrayList<>();
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // (
            do {
                kinds.add(parser.readIdentifier().toLowerCase());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        parser.expectKeyword("ON");

        // Parse columns/expressions: col1, col2, (expr1), ...
        List<String> columns = new ArrayList<>();
        do {
            if (parser.check(TokenType.LEFT_PAREN)) {
                // Expression in parens
                parser.advance();
                StringBuilder expr = new StringBuilder();
                int depth = 1;
                while (!parser.isAtEnd() && depth > 0) {
                    Token t = parser.peek();
                    if (t.type() == TokenType.LEFT_PAREN) depth++;
                    if (t.type() == TokenType.RIGHT_PAREN) {
                        depth--;
                        if (depth == 0) break;
                    }
                    if (expr.length() > 0) expr.append(" ");
                    expr.append(parser.advance().value());
                }
                parser.expect(TokenType.RIGHT_PAREN);
                columns.add("(" + expr + ")");
            } else {
                columns.add(parser.readIdentifier());
            }
        } while (parser.match(TokenType.COMMA));

        parser.expectKeyword("FROM");
        String tableName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) tableName = tableName + "." + parser.readIdentifier();

        // Encode: "create_statistics:name:table:col1,col2:kind1,kind2:ifNotExists"
        String kindsStr = kinds.isEmpty() ? "" : String.join(",", kinds);
        String colsStr = String.join(",", columns);
        return new SetStmt("create_statistics", name + "\0" + tableName + "\0" + colsStr + "\0"
                + kindsStr + "\0" + (ifNotExists ? "1" : "0"));
    }

    // ---- ALTER AGGREGATE ----

    Statement parseAlterAggregate() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        List<String> argTypes = new ArrayList<>();
        if (parser.check(TokenType.LEFT_PAREN)) {
            argTypes = parseFunctionDropParamTypes();
        }
        String action = "noop";
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) {
            action = "rename";
            newName = parser.readIdentifier();
        }
        // SET SCHEMA and OWNER TO record nothing, but the schema and the role still have to exist.
        String schema = "";
        String owner = "";
        if (parser.matchKeywords("SET", "SCHEMA")) schema = parser.readIdentifierOrString();
        else if (parser.matchKeywords("OWNER", "TO")) owner = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_aggregate",
                name + "\0" + String.join("\1", argTypes) + "\0" + action + "\0" + newName
                        + "\0" + schema + "\0" + owner);
    }

    // ---- ALTER STATISTICS ----

    Statement parseAlterStatistics() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();

        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new SetStmt("alter_statistics_rename", name + "\0" + newName);
        }
        if (parser.matchKeywords("SET", "STATISTICS")) {
            String target = parser.advance().value();
            return new SetStmt("alter_statistics_target", name + "\0" + target);
        }
        // Neither records anything, but the statistics object, the role and the schema all still
        // have to exist for PostgreSQL to accept the statement.
        if (parser.matchKeywords("OWNER", "TO")) {
            return new SetStmt("alter_statistics_move", name + "\0" + "" + "\0"
                    + parser.readIdentifierOrString());
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            return new SetStmt("alter_statistics_move", name + "\0"
                    + parser.readIdentifierOrString() + "\0" + "");
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_statistics_move", name + "\0" + "" + "\0" + "");
    }

    // ---- FDW DDL parsing ----

    /** Parse OPTIONS (key 'value', ...) clause, returns PG array format {key=value,...} or null. */
    private String parseOptionsClause() {
        if (!parser.matchKeyword("OPTIONS")) return null;
        parser.expect(TokenType.LEFT_PAREN);
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (!first) sb.append(",");
            first = false;
            // Optional SET/ADD/DROP prefix for ALTER
            parser.matchKeyword("SET");
            parser.matchKeyword("ADD");
            parser.matchKeyword("DROP");
            String key = parser.readIdentifier();
            String value = "";
            if (parser.check(TokenType.STRING_LITERAL)) {
                value = parser.advance().value();
            }
            sb.append(key).append("=").append(value);
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);
        sb.append("}");
        return sb.toString();
    }

    private Statement parseCreateForeignDataWrapper() {
        parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        String options = null;
        // Consume HANDLER, VALIDATOR, OPTIONS, NO HANDLER, NO VALIDATOR
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            if (parser.checkKeyword("OPTIONS")) {
                options = parseOptionsClause();
            } else if (parser.matchKeyword("HANDLER") || parser.matchKeyword("VALIDATOR")) {
                parser.readIdentifier(); // handler/validator function name
            } else if (parser.matchKeyword("NO")) {
                parser.matchKeyword("HANDLER");
                parser.matchKeyword("VALIDATOR");
            } else {
                break;
            }
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_fdw", name + "\0" + (options != null ? options : ""));
    }

    private Statement parseCreateServer() {
        parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();
        // Optional TYPE 'type', VERSION 'version'
        if (parser.matchKeyword("TYPE")) {
            if (parser.check(TokenType.STRING_LITERAL)) parser.advance();
        }
        if (parser.matchKeyword("VERSION")) {
            if (parser.check(TokenType.STRING_LITERAL)) parser.advance();
        }
        parser.expectKeyword("FOREIGN");
        parser.expectKeyword("DATA");
        parser.expectKeyword("WRAPPER");
        String fdwName = parser.readIdentifier();
        String options = parseOptionsClause();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_server", name + "\0" + fdwName + "\0" + (options != null ? options : ""));
    }

    private Statement parseCreateUserMapping() {
        parser.expectKeyword("FOR");
        String userName;
        if (parser.matchKeyword("PUBLIC")) {
            userName = "PUBLIC";
        } else if (parser.matchKeyword("CURRENT_USER") || parser.matchKeyword("CURRENT_ROLE")) {
            userName = "memgres";
        } else if (parser.matchKeyword("SESSION_USER")) {
            userName = "memgres";
        } else {
            userName = parser.readIdentifier();
        }
        parser.expectKeyword("SERVER");
        String serverName = parser.readIdentifier();
        String options = parseOptionsClause();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_user_mapping", serverName + "\0" + userName + "\0" + (options != null ? options : ""));
    }

    private Statement parseCreateForeignTable() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { schema = name; name = parser.readIdentifier(); }
        // Parse columns
        List<String> colDefs = new ArrayList<>(); // stored as "name\ttype"
        if (parser.match(TokenType.LEFT_PAREN)) {
            while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
                String colName = parser.readIdentifier();
                String colType = parser.parseTypeName();
                colDefs.add(colName + "\t" + colType);
                // Skip optional NOT NULL, DEFAULT, etc.
                while (!parser.isAtEnd() && !parser.check(TokenType.COMMA) && !parser.check(TokenType.RIGHT_PAREN)) {
                    parser.advance();
                }
                parser.match(TokenType.COMMA);
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }
        String serverName = "";
        if (parser.matchKeyword("SERVER")) {
            serverName = parser.readIdentifier();
        }
        String options = parseOptionsClause();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        // Encode columns as col1\ttype1\ncol2\ttype2. The schema the statement wrote is carried
        // along with them: a foreign table is created in the schema it names, like any relation.
        String colStr = String.join("\n", colDefs);
        return new SetStmt("create_foreign_table", name + "\0" + serverName + "\0"
                + (options != null ? options : "") + "\0" + colStr + "\0"
                + (schema != null ? schema : "") + "\0" + ifNotExists);
    }

    private Statement parseAlterServer() {
        String name = parser.readIdentifier();
        // Check for OPTIONS clause
        if (parser.checkKeyword("OPTIONS")) {
            String options = parseOptionsClause();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_server_options", name + "\0" + (options != null ? options : ""));
        }
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_object", "server" + STUB_SEP + name + STUB_SEP + newName);
    }

    // ---- Publication / Subscription DDL parsing ----

    private Statement parseCreatePublication() {
        String name = parser.readIdentifier();
        boolean allTables = false;
        List<String> tables = new ArrayList<>();
        String schemaName = null;
        if (parser.matchKeywords("FOR", "ALL", "TABLES")) {
            allTables = true;
        } else if (parser.matchKeyword("FOR")) {
            if (parser.matchKeywords("TABLES", "IN", "SCHEMA")) {
                schemaName = parser.readIdentifier();
            } else {
                parser.matchKeyword("TABLE");
                // Parse table list
                do {
                    parser.matchKeyword("ONLY");
                    String tbl = parser.readIdentifier();
                    if (parser.match(TokenType.DOT)) tbl = tbl + "." + parser.readIdentifier();
                    tables.add(tbl);
                } while (parser.match(TokenType.COMMA));
            }
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        // Encode: name\0allTables\0table1,table2\0schemaName
        return new SetStmt("create_publication", name + "\0" + allTables + "\0" + String.join(",", tables)
                + "\0" + (schemaName != null ? schemaName : ""));
    }

    private Statement parseAlterPublication() {
        String name = parser.readIdentifier();
        if (parser.matchKeywords("ADD", "TABLE")) {
            List<String> tables = new ArrayList<>();
            do {
                parser.matchKeyword("ONLY");
                String tbl = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) tbl = tbl + "." + parser.readIdentifier();
                tables.add(tbl);
            } while (parser.match(TokenType.COMMA));
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_add_table", name + "\0" + String.join(",", tables));
        }
        if (parser.matchKeywords("SET", "TABLE")) {
            List<String> tables = new ArrayList<>();
            do {
                parser.matchKeyword("ONLY");
                String tbl = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) tbl = tbl + "." + parser.readIdentifier();
                tables.add(tbl);
            } while (parser.match(TokenType.COMMA));
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_set_table", name + "\0" + String.join(",", tables));
        }
        if (parser.matchKeywords("DROP", "TABLE")) {
            List<String> tables = new ArrayList<>();
            do {
                parser.matchKeyword("ONLY");
                String tbl = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) tbl = tbl + "." + parser.readIdentifier();
                tables.add(tbl);
            } while (parser.match(TokenType.COMMA));
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_drop_table", name + "\0" + String.join(",", tables));
        }
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_rename", name + "\0" + newName);
        }
        if (parser.matchKeywords("OWNER", "TO")) {
            String owner = parser.readIdentifierOrString();
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_owner", name + "\0" + owner);
        }
        // SET (option = value): nothing here is recorded, but PostgreSQL names an option it does
        // not know rather than accepting it, so the keys are carried through to be checked.
        if (parser.matchKeyword("SET") && parser.check(TokenType.LEFT_PAREN)) {
            List<String> keys = new ArrayList<>();
            parser.expect(TokenType.LEFT_PAREN);
            while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
                keys.add(parser.readIdentifierOrString());
                while (!parser.isAtEnd() && !parser.check(TokenType.COMMA)
                        && !parser.check(TokenType.RIGHT_PAREN)) {
                    parser.advance();
                }
                if (!parser.match(TokenType.COMMA)) break;
            }
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new SetStmt("alter_publication_options", name + "\0" + String.join(",", keys));
        }
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_publication_noop", name);
    }

    private Statement parseCreateSubscription() {
        String name = parser.readIdentifier();
        parser.expectKeyword("CONNECTION");
        String conninfo = "";
        if (parser.check(TokenType.STRING_LITERAL)) {
            conninfo = parser.advance().value();
        }
        parser.expectKeyword("PUBLICATION");
        String pubName = parser.readIdentifier();
        // Consume optional WITH (...) clause
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_subscription", name + "\0" + conninfo + "\0" + pubName);
    }

    // ---------------------------------------------------------------
    //  Text Search DDL
    // ---------------------------------------------------------------

    /** Match an identifier (non-keyword) case-insensitively. */
    private boolean matchIdentCI(String val) {
        if (parser.checkIdentCI(val)) {
            parser.advance();
            return true;
        }
        // Also try matchKeyword in case the token happens to be a keyword
        return parser.matchKeyword(val);
    }

    private Statement parseCreateTextSearch() {
        if (matchIdentCI("CONFIGURATION")) {
            return parseCreateTextSearchConfiguration();
        }
        if (matchIdentCI("DICTIONARY")) {
            return parseCreateTextSearchDictionary();
        }
        // PARSER / TEMPLATE – accept but no-op
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("create_noop", "ok");
    }

    private Statement parseCreateTextSearchConfiguration() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        parser.expect(TokenType.LEFT_PAREN);
        String copyFrom = null;
        String parserName = null;
        while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
            if (parser.matchKeyword("COPY") || matchIdentCI("COPY")) {
                parser.expect(TokenType.EQUALS);
                copyFrom = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) copyFrom = copyFrom + "." + parser.readIdentifier();
            } else if (matchIdentCI("PARSER")) {
                parser.expect(TokenType.EQUALS);
                parserName = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) parserName = parserName + "." + parser.readIdentifier();
            } else {
                parser.advance();
            }
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);
        // Encode: name \0 copyFrom \0 parserName
        String val = name + "\0" + (copyFrom != null ? copyFrom : "") + "\0" + (parserName != null ? parserName : "");
        return new SetStmt("create_ts_config", val);
    }

    private Statement parseCreateTextSearchDictionary() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        parser.expect(TokenType.LEFT_PAREN);
        String template = null;
        StringBuilder opts = new StringBuilder();
        while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
            if (matchIdentCI("TEMPLATE")) {
                parser.expect(TokenType.EQUALS);
                template = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) template = template + "." + parser.readIdentifier();
            } else {
                // Collect remaining options (e.g. STOPWORDS = english)
                String key = parser.advance().value();
                if (parser.match(TokenType.EQUALS)) {
                    String val = parser.advance().value();
                    if (opts.length() > 0) opts.append(", ");
                    opts.append(key).append(" = ").append(val);
                }
            }
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);
        String val = name + "\0" + (template != null ? template : "") + "\0" + opts;
        return new SetStmt("create_ts_dict", val);
    }

    private Statement parseDropTextSearch() {
        String kind = "text search configuration";
        if (matchIdentCI("CONFIGURATION")) kind = "text search configuration";
        else if (matchIdentCI("DICTIONARY")) kind = "text search dictionary";
        else if (matchIdentCI("PARSER")) kind = "text search parser";
        else if (matchIdentCI("TEMPLATE")) kind = "text search template";
        return parseDropObject(kind);
    }

    /**
     * {@code DROP <kind> [IF EXISTS] name} for the kinds memgres keeps in a registry of its own.
     * The kind and the name travel with the statement so the executor can refuse a name that was
     * never created and say which one IF EXISTS skipped.
     */
    private Statement parseDropObject(String kind) {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        String name = parser.readIdentifierOrString();
        if (parser.match(TokenType.DOT)) name = parser.readIdentifierOrString();
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("drop_object", kind + STUB_SEP + name + STUB_SEP + (ifExists ? "1" : ""));
    }

    private Statement parseAlterTextSearch() {
        if (matchIdentCI("CONFIGURATION")) {
            return parseAlterTextSearchConfiguration();
        }
        // A dictionary, a parser and a template each change nothing memgres records, but the name
        // still has to be one that exists.
        if (matchIdentCI("DICTIONARY")) return parseAlterObject("text search dictionary");
        if (matchIdentCI("PARSER")) return parseAlterObject("text search parser");
        if (matchIdentCI("TEMPLATE")) return parseAlterObject("text search template");
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
        return new SetStmt("alter_noop", "ok");
    }

    private Statement parseAlterTextSearchConfiguration() {
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) name = name + "." + parser.readIdentifier();
        // Look for ALTER MAPPING FOR ... WITH ...
        // or ADD MAPPING FOR ... WITH ...
        // or DROP MAPPING [IF EXISTS] FOR ...
        if (parser.matchKeyword("ALTER") || parser.matchKeyword("ADD")) {
            if (parser.matchKeyword("MAPPING") || matchIdentCI("MAPPING")) {
                if (parser.matchKeyword("FOR")) {
                    List<String> tokenTypes = new java.util.ArrayList<>();
                    tokenTypes.add(parser.readIdentifier());
                    while (parser.match(TokenType.COMMA)) {
                        tokenTypes.add(parser.readIdentifier());
                    }
                    if (parser.matchKeyword("WITH")) {
                        List<String> dicts = new java.util.ArrayList<>();
                        dicts.add(parser.readIdentifier());
                        while (parser.match(TokenType.COMMA)) {
                            dicts.add(parser.readIdentifier());
                        }
                        // Encode: configName \0 tokenType1,tokenType2 \0 dict1,dict2
                        String val = name + "\0" + String.join(",", tokenTypes) + "\0" + String.join(",", dicts);
                        return new SetStmt("alter_ts_config_mapping", val);
                    }
                }
            }
        }
        // Every other variant changes nothing memgres records, but the configuration still has
        // to be one that exists — and a RENAME has to move the name it is registered under.
        String newName = "";
        if (parser.matchKeywords("RENAME", "TO")) newName = parser.readIdentifierOrString();
        return new SetStmt("alter_object", "text search configuration" + STUB_SEP + name
                + STUB_SEP + newName + STUB_SEP + "" + parseMoveOrReown());
    }

    // ---- CREATE COLLATION ----

    private Statement parseCreateCollation() {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
        String name = parser.readIdentifier();

        // CREATE COLLATION name FROM existing_collation
        if (parser.matchKeyword("FROM")) {
            String from = parser.readIdentifier();
            return new CreateCollationStmt(name, ifNotExists, from, new LinkedHashMap<>());
        }

        // CREATE COLLATION name (option = value, ...)
        Map<String, String> options = new LinkedHashMap<>();
        if (parser.match(TokenType.LEFT_PAREN)) {
            do {
                String optName = parser.readIdentifier().toLowerCase();
                parser.expect(TokenType.EQUALS);
                String optVal;
                if (parser.check(TokenType.STRING_LITERAL)) {
                    optVal = parser.advance().value();
                } else if (parser.checkKeyword("TRUE") || parser.checkKeyword("FALSE")) {
                    optVal = parser.advance().value().toLowerCase();
                } else {
                    optVal = parser.advance().value();
                }
                options.put(optName, optVal);
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        return new CreateCollationStmt(name, ifNotExists, null, options);
    }

    // ---- CREATE CAST ----

    private Statement parseCreateCast() {
        parser.expect(TokenType.LEFT_PAREN);
        String sourceType = parser.parseTypeName();
        parser.expectKeyword("AS");
        String targetType = parser.parseTypeName();
        parser.expect(TokenType.RIGHT_PAREN);

        String functionName = null;
        List<String> funcArgTypes = null;
        boolean withInout = false;
        if (parser.matchKeyword("WITH")) {
            if (parser.matchKeyword("FUNCTION")) {
                functionName = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) functionName += "." + parser.readIdentifier();
                if (parser.check(TokenType.LEFT_PAREN)) {
                    funcArgTypes = parseFunctionDropParamTypes();
                }
            } else if (parser.matchKeyword("INOUT")) {
                // WITH INOUT: I/O conversion cast
                withInout = true;
            }
        } else if (parser.matchKeywords("WITHOUT", "FUNCTION")) {
            functionName = null;
        }

        String castContext = "e"; // explicit by default
        if (parser.matchKeyword("AS")) {
            if (parser.matchKeyword("ASSIGNMENT")) {
                castContext = "a";
            } else if (parser.matchKeyword("IMPLICIT")) {
                castContext = "i";
            }
        }

        return new CreateCastStmt(sourceType, targetType, functionName, funcArgTypes, withInout, castContext);
    }
    /**
     * The number written after COST or ROWS. Both must be positive, and a negative sign or a word
     * is a syntax error where it stands — reading the token with a bare parseDouble let the
     * NumberFormatException out as an internal error.
     */
    private static double routineCost(Parser parser, String option) {
        Token token = parser.peek();
        boolean negative = false;
        if (token.type() == TokenType.MINUS) {
            negative = true;
            parser.advance();
            token = parser.peek();
        }
        String text = token.value();
        double value;
        try {
            value = Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw ParseException.saying("syntax error at or near \"" + text + "\"", token, "42601");
        }
        parser.advance();
        if (negative) value = -value;
        if (value <= 0) {
            throw new com.memgres.engine.MemgresException(option + " must be positive", "22023");
        }
        return value;
    }

}
