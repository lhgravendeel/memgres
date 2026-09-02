package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.LinkedHashMap;
import java.util.List;
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
        List<String> inRoles = new java.util.ArrayList<>();

        parser.matchKeyword("WITH"); // optional WITH

        parseRoleOptions(options);
        // CREATE USER is CREATE ROLE with LOGIN: the two words differ in exactly that, and a
        // user made without it could not connect.
        if (isUser && !options.containsKey("LOGIN")) options.put("LOGIN", "true");

        // Extra CREATE-only clauses
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token t = parser.peek();
            if (t.type() != TokenType.KEYWORD && t.type() != TokenType.IDENTIFIER) break;
            String kw = t.value().toUpperCase(java.util.Locale.ROOT);
            parser.advance();
            switch (kw) {
                case "IN": {
                    // M12: IN ROLE role / IN GROUP role — capture role names
                    if (parser.matchKeyword("ROLE") || parser.matchKeyword("GROUP")) {
                        do { inRoles.add(parser.readIdentifier()); } while (parser.match(TokenType.COMMA));
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

        return new CreateRoleStmt(name, isUser, options, inRoles);
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
        List<String> names = new java.util.ArrayList<>();
        names.add(parser.readIdentifier());
        while (parser.match(TokenType.COMMA)) names.add(parser.readIdentifier());
        return new DropRoleStmt(names, ifExists);
    }

    /** The flag options, each written with or without NO, and the attribute each sets. */
    private static final Map<String, String> FLAG_OPTIONS = flagOptions();

    private static Map<String, String> flagOptions() {
        Map<String, String> m = new LinkedHashMap<>();
        for (String name : new String[]{"SUPERUSER", "CREATEDB", "CREATEROLE", "LOGIN",
                "INHERIT", "REPLICATION", "BYPASSRLS"}) {
            m.put(name, name);
            m.put("NO" + name, name);
        }
        return m;
    }

    /** The words that open a clause of the statement rather than naming an option. */
    private static final java.util.Set<String> TAIL_WORDS = new java.util.HashSet<>(
            java.util.Arrays.asList("IN", "ROLE", "USER", "ADMIN", "SET", "RESET", "RENAME",
                    "WITH", "TO", "GRANTED", "SYSID", "ADD", "DROP", "GROUP"));

    /**
     * Shared role option parsing for CREATE ROLE and ALTER ROLE.
     *
     * <p>Every word here is an option or it is nothing. Left as a bare {@code return}, a word
     * nobody defined ended the parse and the statement reported success with that word and
     * everything after it discarded — so a typed option was silently not applied. And two
     * options that contradict each other are refused rather than resolved last-one-wins.
     */
    /** Refuse an option the statement has already given, whatever it gave it as. */
    private static void requireNotAlreadyGiven(Map<String, String> options, String key, Token at) {
        if (options.containsKey(key)) {
            throw ParseException.saying("conflicting or redundant options", at, "42601");
        }
    }

    private void parseRoleOptions(Map<String, String> options) {
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON)) {
            Token t = parser.peek();
            if (t.type() != TokenType.KEYWORD && t.type() != TokenType.IDENTIFIER) break;
            String kw = t.value().toUpperCase(java.util.Locale.ROOT);
            // The tail of the statement is not an option list: these words open clauses of
            // their own, and reading them here would take them away from the parser that wants
            // them.
            if (TAIL_WORDS.contains(kw)) break;
            String flag = FLAG_OPTIONS.get(kw);
            if (flag != null) {
                // An option may be given once. PostgreSQL does not resolve a repeat, whether the
                // second one agrees with the first or contradicts it: LOGIN LOGIN is refused for
                // the same reason LOGIN NOLOGIN is.
                if (options.containsKey(flag)) {
                    throw ParseException.saying("conflicting or redundant options", t, "42601");
                }
                parser.advance();
                options.put(flag, kw.startsWith("NO") ? "false" : "true");
                continue;
            }
            switch (kw) {
                case "PASSWORD": {
                    requireNotAlreadyGiven(options, "PASSWORD", t);
                    parser.advance();
                    if (parser.matchKeyword("NULL")) {
                        options.put("PASSWORD", null);
                    } else {
                        options.put("PASSWORD", parser.advance().value());
                    }
                    break;
                }
                case "ENCRYPTED": {
                    requireNotAlreadyGiven(options, "PASSWORD", t);
                    parser.advance();
                    parser.expectKeyword("PASSWORD");
                    options.put("PASSWORD", parser.advance().value());
                    break;
                }
                case "CONNECTION": {
                    requireNotAlreadyGiven(options, "CONNECTION_LIMIT", t);
                    parser.advance();
                    parser.expectKeyword("LIMIT");
                    options.put("CONNECTION_LIMIT", parser.advance().value());
                    break;
                }
                case "VALID": {
                    requireNotAlreadyGiven(options, "VALID_UNTIL", t);
                    parser.advance();
                    parser.expectKeyword("UNTIL");
                    options.put("VALID_UNTIL", parser.advance().value());
                    break;
                }
                default: {
                    throw ParseException.saying("unrecognized role option \""
                            + t.raw().toLowerCase(java.util.Locale.ROOT) + "\"", t, "42601");
                }
            }
        }
    }
}
