package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Role management parsing (CREATE/ALTER/DROP ROLE), extracted from DdlParser.
 */
class DdlRoleParser {
    private final Parser parser;

    DdlRoleParser(Parser parser) {
        this.parser = parser;
    }

    CreateRoleStmt parseCreateRole(boolean isUser) {
        // CREATE ROLE IF NOT EXISTS is not valid PostgreSQL syntax
        if (parser.checkKeyword("IF") && parser.checkKeywordAt(1, "NOT") && parser.checkKeywordAt(2, "EXISTS")) {
            throw new ParseException("syntax error at or near \"IF\"", parser.peek());
        }
        String name = parser.readIdentifier();
        Map<String, String> options = new LinkedHashMap<>();

        parser.matchKeyword("WITH"); // optional WITH

        parseRoleOptions(options);

        // Extra CREATE-only clauses
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token t = parser.peek();
            if (t.type() != TokenType.KEYWORD && t.type() != TokenType.IDENTIFIER) break;
            String kw = t.value().toUpperCase();
            parser.advance();
            switch (kw) {
                case "IN": {
                    // IN ROLE role / IN GROUP role
                    if (parser.matchKeyword("ROLE") || parser.matchKeyword("GROUP")) {
                        do { parser.readIdentifier(); } while (parser.match(TokenType.COMMA));
                    }
                    break;
                }
                case "ROLE": {
                    // ROLE name[, ...] (members)
                    do { parser.readIdentifier(); } while (parser.match(TokenType.COMMA));
                    break;
                }
                case "ADMIN": {
                    // ADMIN name[, ...] (admin members)
                    do { parser.readIdentifier(); } while (parser.match(TokenType.COMMA));
                    break;
                }
                case "SYSID":
                    parser.advance();
                    break;
                default: {
                    /* unknown option, skip */ 
                    break;
                }
            }
        }

        return new CreateRoleStmt(name, isUser, options);
    }

    AlterRoleStmt parseAlterRole() {
        String name = parser.readIdentifier();

        // ALTER ROLE name RENAME TO newname
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterRoleStmt(name, newName, Cols.mapOf());
        }

        // ALTER ROLE name SET param = value / TO value
        if (parser.matchKeyword("SET")) {
            String param = parser.readIdentifier();
            Map<String, String> options = new LinkedHashMap<>();
            if (parser.match(TokenType.EQUALS) || parser.matchKeyword("TO")) {
                StringBuilder valBuf = new StringBuilder();
                while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
                    Token vt = parser.advance();
                    if (valBuf.length() > 0) valBuf.append(" ");
                    if (vt.type() == TokenType.STRING_LITERAL) {
                        valBuf.append(vt.value());
                    } else {
                        valBuf.append(vt.value());
                    }
                }
                options.put("SET_CONFIG", param + "=" + valBuf.toString().trim());
            }
            return new AlterRoleStmt(name, null, options);
        }
        // ALTER ROLE name RESET param, no-op
        if (parser.matchKeyword("RESET")) {
            while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) parser.advance();
            return new AlterRoleStmt(name, null, Cols.mapOf());
        }

        Map<String, String> options = new LinkedHashMap<>();
        parser.matchKeyword("WITH"); // optional WITH
        parseRoleOptions(options);

        return new AlterRoleStmt(name, null, options);
    }

    DropRoleStmt parseDropRole() {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        String name = parser.readIdentifier();
        // Skip any additional names (DROP ROLE name, name2, ...)
        while (parser.match(TokenType.COMMA)) parser.readIdentifier();
        return new DropRoleStmt(name, ifExists);
    }

    /** Shared role option parsing for CREATE ROLE and ALTER ROLE. */
    private void parseRoleOptions(Map<String, String> options) {
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token t = parser.peek();
            if (t.type() != TokenType.KEYWORD && t.type() != TokenType.IDENTIFIER) break;
            String kw = t.value().toUpperCase();
            switch (kw) {
                case "SUPERUSER": {
                    parser.advance(); options.put("SUPERUSER", "true"); 
                    break;
                }
                case "NOSUPERUSER": {
                    parser.advance(); options.put("SUPERUSER", "false"); 
                    break;
                }
                case "CREATEDB": {
                    parser.advance(); options.put("CREATEDB", "true"); 
                    break;
                }
                case "NOCREATEDB": {
                    parser.advance(); options.put("CREATEDB", "false"); 
                    break;
                }
                case "CREATEROLE": {
                    parser.advance(); options.put("CREATEROLE", "true"); 
                    break;
                }
                case "NOCREATEROLE": {
                    parser.advance(); options.put("CREATEROLE", "false"); 
                    break;
                }
                case "LOGIN": {
                    parser.advance(); options.put("LOGIN", "true"); 
                    break;
                }
                case "NOLOGIN": {
                    parser.advance(); options.put("LOGIN", "false"); 
                    break;
                }
                case "INHERIT": {
                    parser.advance(); options.put("INHERIT", "true"); 
                    break;
                }
                case "NOINHERIT": {
                    parser.advance(); options.put("INHERIT", "false"); 
                    break;
                }
                case "REPLICATION": {
                    parser.advance(); options.put("REPLICATION", "true"); 
                    break;
                }
                case "NOREPLICATION": {
                    parser.advance(); options.put("REPLICATION", "false"); 
                    break;
                }
                case "BYPASSRLS": {
                    parser.advance(); options.put("BYPASSRLS", "true"); 
                    break;
                }
                case "NOBYPASSRLS": {
                    parser.advance(); options.put("BYPASSRLS", "false"); 
                    break;
                }
                case "PASSWORD": {
                    parser.advance();
                    if (parser.matchKeyword("NULL")) {
                        options.put("PASSWORD", null);
                    } else {
                        options.put("PASSWORD", parser.advance().value());
                    }
                    break;
                }
                case "ENCRYPTED": {
                    parser.advance();
                    parser.expectKeyword("PASSWORD");
                    options.put("PASSWORD", parser.advance().value());
                    break;
                }
                case "CONNECTION": {
                    parser.advance();
                    parser.expectKeyword("LIMIT");
                    options.put("CONNECTION_LIMIT", parser.advance().value());
                    break;
                }
                case "VALID": {
                    parser.advance();
                    parser.expectKeyword("UNTIL");
                    options.put("VALID_UNTIL", parser.advance().value());
                    break;
                }
                default: {
                    return; 
                }
            }
        }
    }
}
