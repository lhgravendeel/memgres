package com.memgres.engine.parser;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Policy management parsing (CREATE/ALTER/DROP POLICY), extracted from DdlParser.
 */
class DdlPolicyParser {
    private final Parser parser;

    DdlPolicyParser(Parser parser) {
        this.parser = parser;
    }

    CreatePolicyStmt parseCreatePolicy() {
        String name = parser.readIdentifier();
        parser.expectKeyword("ON");
        String table = parser.readIdentifier();

        // AS {PERMISSIVE|RESTRICTIVE}
        String policyType = "PERMISSIVE";
        if (parser.matchKeyword("AS")) {
            policyType = parser.advance().value().toUpperCase();
        }

        String command = "ALL";
        if (parser.matchKeyword("FOR")) {
            Token cmdToken = parser.advance();
            command = cmdToken.value().toUpperCase();
            if (!command.equals("ALL") && !command.equals("SELECT") && !command.equals("INSERT")
                    && !command.equals("UPDATE") && !command.equals("DELETE")) {
                throw new ParseException("unrecognized row security command type \"" + cmdToken.value() + "\"", cmdToken);
            }
        }

        // TO role [, ...] | PUBLIC | CURRENT_USER | SESSION_USER
        List<String> roles = null;
        if (parser.matchKeyword("TO")) {
            roles = new ArrayList<>();
            do {
                roles.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
        }

        Expression[] exprs = parseUsingWithCheck();
        return new CreatePolicyStmt(name, table, command, exprs[0], exprs[1], policyType, roles);
    }

    DropStmt parseDropPolicy() {
        boolean ifExists = parser.matchKeywords("IF", "EXISTS");
        String name = parser.readIdentifier();
        String onTable = null;
        if (parser.matchKeyword("ON")) {
            onTable = parser.readIdentifier();
        }
        boolean cascade = parser.matchKeyword("CASCADE");
        parser.matchKeyword("RESTRICT");
        return new DropStmt(DropStmt.ObjectType.POLICY, name, onTable, ifExists, cascade);
    }

    AlterPolicyStmt parseAlterPolicy() {
        String name = parser.readIdentifier();
        parser.expectKeyword("ON");
        String table = parser.readIdentifier();

        // ALTER POLICY name ON table RENAME TO newname
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            // PG does not allow changing the command type via ALTER POLICY ... RENAME
            if (parser.matchKeyword("FOR")) {
                Token cmdToken = parser.advance();
                throw new ParseException("syntax error at or near \"" + cmdToken.value() + "\"", cmdToken);
            }
            return new AlterPolicyStmt(name, table, newName, null, null);
        }

        // Skip TO role clause
        if (parser.matchKeyword("TO")) {
            do { parser.readIdentifier(); } while (parser.match(TokenType.COMMA));
        }

        Expression[] exprs = parseUsingWithCheck();
        return new AlterPolicyStmt(name, table, null, exprs[0], exprs[1]);
    }

    /** Shared parsing for USING (expr) and WITH CHECK (expr) clauses. */
    private Expression[] parseUsingWithCheck() {
        Expression usingExpr = null;
        Expression withCheckExpr = null;

        if (parser.matchKeyword("USING")) {
            parser.expect(TokenType.LEFT_PAREN);
            usingExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
        }
        if (parser.matchKeywords("WITH", "CHECK")) {
            parser.expect(TokenType.LEFT_PAREN);
            withCheckExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        return new Expression[]{usingExpr, withCheckExpr};
    }
}
