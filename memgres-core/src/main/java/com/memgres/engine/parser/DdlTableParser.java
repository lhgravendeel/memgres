package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Table creation parsing (CREATE TABLE, column defs, table constraints),
 * extracted from DdlParser.
 */
class DdlTableParser {
    private final Parser parser;
    private final List<TableConstraint> pendingColumnChecks = new ArrayList<>();

    DdlTableParser(Parser parser) {
        this.parser = parser;
    }

    Statement parseCreateTable(boolean temporary) {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");

        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }

        // CREATE TABLE ... AS query
        if (parser.matchKeyword("AS")) {
            Statement query = parser.parseStatement();
            boolean withData = true;
            if (parser.matchKeyword("WITH")) {
                if (parser.matchKeyword("NO")) {
                    parser.expectKeyword("DATA");
                    withData = false;
                } else {
                    parser.expectKeyword("DATA");
                    withData = true;
                }
            }
            return new CreateTableAsStmt(schema, name, ifNotExists, temporary, query, withData);
        }

        // PARTITION OF parent FOR VALUES ...
        if (parser.matchKeywords("PARTITION", "OF")) {
            String parentName = parser.readIdentifier();
            List<String> bounds = new ArrayList<>();
            if (parser.matchKeyword("DEFAULT")) {
                bounds.add("DEFAULT");
            } else if (parser.matchKeyword("FOR")) {
                parser.expectKeyword("VALUES");
                if (parser.matchKeyword("FROM")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("FROM");
                    bounds.add(DdlParser.readValueOrMinMax(parser));
                    parser.expect(TokenType.RIGHT_PAREN);
                    parser.expectKeyword("TO");
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("TO");
                    bounds.add(DdlParser.readValueOrMinMax(parser));
                    parser.expect(TokenType.RIGHT_PAREN);
                } else if (parser.matchKeyword("IN")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("IN");
                    do {
                        bounds.add(parser.advance().value());
                    } while (parser.match(TokenType.COMMA));
                    parser.expect(TokenType.RIGHT_PAREN);
                } else if (parser.matchKeyword("WITH")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("HASH");
                    parser.expectKeyword("MODULUS");
                    bounds.add(parser.advance().value());
                    parser.expect(TokenType.COMMA);
                    parser.expectKeyword("REMAINDER");
                    bounds.add(parser.advance().value());
                    parser.expect(TokenType.RIGHT_PAREN);
                }
            }
            String subPartBy = null;
            String subPartCol = null;
            if (parser.matchKeywords("PARTITION", "BY")) {
                if (parser.matchKeyword("RANGE")) subPartBy = "RANGE";
                else if (parser.matchKeyword("LIST")) subPartBy = "LIST";
                else { parser.expectKeyword("HASH"); subPartBy = "HASH"; }
                parser.expect(TokenType.LEFT_PAREN);
                subPartCol = parser.readIdentifier();
                parser.expect(TokenType.RIGHT_PAREN);
            }
            return new CreateTableStmt(schema, name, ifNotExists, temporary,
                    Cols.listOf(), Cols.listOf(), null, subPartBy, subPartCol, parentName, bounds);
        }

        parser.expect(TokenType.LEFT_PAREN);

        List<ColumnDef> columns = new ArrayList<>();
        List<TableConstraint> constraints = new ArrayList<>();

        List<String> likeTables = new ArrayList<>();
        if (parser.check(TokenType.RIGHT_PAREN)) {
            // Empty, no columns
        } else do {
            if (parser.matchKeyword("LIKE")) {
                likeTables.add(parser.readIdentifier());
                while (parser.matchKeyword("INCLUDING") || parser.matchKeyword("EXCLUDING")) {
                    parser.readIdentifier();
                }
            } else if (isTableConstraintStart()) {
                constraints.add(parseTableConstraint());
            } else {
                ColumnDef colDef = parseColumnDef();
                columns.add(colDef);
                while (!pendingColumnChecks.isEmpty()) {
                    constraints.add(pendingColumnChecks.remove(0));
                }
            }
        } while (parser.match(TokenType.COMMA));

        parser.expect(TokenType.RIGHT_PAREN);

        List<String> inherits = null;
        if (parser.matchKeyword("INHERITS")) {
            parser.expect(TokenType.LEFT_PAREN);
            inherits = new ArrayList<>();
            do {
                inherits.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        String partitionBy = null;
        String partitionCol = null;
        if (parser.matchKeywords("PARTITION", "BY")) {
            if (parser.matchKeyword("RANGE")) partitionBy = "RANGE";
            else if (parser.matchKeyword("LIST")) partitionBy = "LIST";
            else { parser.expectKeyword("HASH"); partitionBy = "HASH"; }
            parser.expect(TokenType.LEFT_PAREN);
            partitionCol = parser.readIdentifier();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        String onCommitAction = null;
        if (parser.matchKeywords("ON", "COMMIT")) {
            if (parser.matchKeyword("DROP")) {
                onCommitAction = "DROP";
            } else if (parser.matchKeywords("DELETE", "ROWS")) {
                onCommitAction = "DELETE ROWS";
            } else if (parser.matchKeywords("PRESERVE", "ROWS")) {
                onCommitAction = "PRESERVE ROWS";
            }
        }

        return new CreateTableStmt(schema, name, ifNotExists, temporary, columns, constraints,
                inherits, partitionBy, partitionCol, null, null,
                likeTables.isEmpty() ? null : likeTables, onCommitAction);
    }

    boolean isTableConstraintStart() {
        Token t = parser.peek();
        if (t.type() != TokenType.KEYWORD) return false;
        switch (t.value()) {
            case "PRIMARY":
            case "UNIQUE":
            case "CHECK":
            case "FOREIGN":
            case "CONSTRAINT":
            case "EXCLUDE":
                return true;
            case "NOT": {
                if (parser.pos + 1 < parser.tokens.size()
                        && parser.tokens.get(parser.pos + 1).type() == TokenType.KEYWORD
                        && parser.tokens.get(parser.pos + 1).value().equals("NULL")) {
                    if (parser.pos + 2 < parser.tokens.size()) {
                        Token afterNull = parser.tokens.get(parser.pos + 2);
                        return afterNull.type() == TokenType.IDENTIFIER;
                    }
                }
                return false;
            }
            default:
                return false;
        }
    }

    ColumnDef parseColumnDef() {
        String colName = parser.readIdentifier();
        String typeName = parser.parseTypeName();

        boolean notNull = false;
        boolean pk = false;
        boolean unique = false;
        Expression defaultExpr = null;
        String refTable = null;
        String refColumn = null;
        String refOnDelete = null;
        String refOnUpdate = null;
        String generatedExpr = null;
        String identity = null;
        Long identityStart = null;
        Long identityIncrement = null;
        boolean deferrable = false;
        boolean initiallyDeferred = false;
        Expression columnCheckExpr = null;

        while (true) {
            if (parser.matchKeywords("NOT", "NULL")) { notNull = true; continue; }
            if (parser.matchKeyword("NULL")) { notNull = false; continue; }
            if (parser.matchKeywords("PRIMARY", "KEY")) { pk = true; notNull = true; continue; }
            if (parser.matchKeyword("UNIQUE")) {
                unique = true;
                if (parser.checkKeyword("NULLS")) {
                    parser.advance();
                    if (parser.matchKeywords("NOT", "DISTINCT")) {
                        pendingColumnChecks.add(new TableConstraint(null,
                                TableConstraint.ConstraintType.UNIQUE,
                                Cols.listOf(colName), null, null, null, null, null, true));
                        unique = false;
                    } else {
                        parser.matchKeyword("DISTINCT");
                    }
                }
                if (parser.matchKeyword("DEFERRABLE")) {
                    if (parser.matchKeyword("INITIALLY")) {
                        parser.matchKeyword("DEFERRED");
                        parser.matchKeyword("IMMEDIATE");
                    }
                } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                    parser.advance(); parser.advance();
                }
                continue;
            }
            if (parser.matchKeyword("DEFAULT")) { defaultExpr = parser.parseExpression(); continue; }
            if (parser.matchKeyword("REFERENCES")) {
                refTable = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) {
                    refTable = parser.readIdentifier();
                }
                if (parser.check(TokenType.LEFT_PAREN)) {
                    parser.expect(TokenType.LEFT_PAREN);
                    refColumn = parser.readIdentifier();
                    parser.expect(TokenType.RIGHT_PAREN);
                }
                while (parser.matchKeyword("ON")) {
                    if (parser.matchKeyword("DELETE")) {
                        refOnDelete = parseReferentialAction();
                    } else if (parser.matchKeyword("UPDATE")) {
                        refOnUpdate = parseReferentialAction();
                    }
                }
                if (parser.matchKeyword("DEFERRABLE")) {
                    deferrable = true;
                    if (parser.matchKeyword("INITIALLY")) {
                        if (parser.matchKeyword("DEFERRED")) initiallyDeferred = true;
                        else parser.matchKeyword("IMMEDIATE");
                    }
                } else if (parser.matchKeyword("NOT")) {
                    parser.matchKeyword("DEFERRABLE");
                }
                continue;
            }
            if (parser.matchKeyword("CHECK")) {
                parser.expect(TokenType.LEFT_PAREN);
                Expression checkExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
                columnCheckExpr = checkExpr;
                pendingColumnChecks.add(new TableConstraint(null, TableConstraint.ConstraintType.CHECK,
                        Cols.listOf(), checkExpr, null, null, null, null));
                continue;
            }
            if (parser.matchKeyword("CONSTRAINT")) {
                String constraintName = parser.readIdentifier();
                if (parser.matchKeyword("CHECK")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    Expression checkExpr = parser.parseExpression();
                    parser.expect(TokenType.RIGHT_PAREN);
                    columnCheckExpr = checkExpr;
                    pendingColumnChecks.add(new TableConstraint(constraintName, TableConstraint.ConstraintType.CHECK,
                            Cols.listOf(), checkExpr, null, null, null, null));
                } else if (parser.matchKeywords("NOT", "NULL")) {
                    notNull = true;
                } else if (parser.matchKeywords("PRIMARY", "KEY")) {
                    pk = true; notNull = true;
                } else if (parser.matchKeyword("UNIQUE")) {
                    unique = true;
                }
                continue;
            }
            if (parser.matchKeyword("GENERATED")) {
                if (parser.matchKeyword("ALWAYS")) {
                    parser.expectKeyword("AS");
                    if (parser.checkKeyword("IDENTITY")) {
                        parser.advance();
                        identity = "ALWAYS";
                        if (parser.check(TokenType.LEFT_PAREN)) {
                            parser.advance();
                            long[] opts = parseSequenceOptionsInParens();
                            identityStart = opts[0] != Long.MIN_VALUE ? opts[0] : null;
                            identityIncrement = opts[1] != Long.MIN_VALUE ? opts[1] : null;
                        }
                    } else {
                        parser.expect(TokenType.LEFT_PAREN);
                        generatedExpr = buildRawSqlUntilCloseParen();
                        parser.expect(TokenType.RIGHT_PAREN);
                        parser.matchKeyword("STORED");
                    }
                } else {
                    parser.matchKeyword("BY");
                    parser.matchKeyword("DEFAULT");
                    parser.expectKeyword("AS");
                    parser.expectKeyword("IDENTITY");
                    identity = "BY DEFAULT";
                    if (parser.check(TokenType.LEFT_PAREN)) {
                        parser.advance();
                        long[] opts = parseSequenceOptionsInParens();
                        identityStart = opts[0] != Long.MIN_VALUE ? opts[0] : null;
                        identityIncrement = opts[1] != Long.MIN_VALUE ? opts[1] : null;
                    }
                }
                continue;
            }
            if (parser.matchKeyword("COLLATE")) {
                if (!parser.isClauseKeyword()) {
                    parser.readIdentifier();
                }
                continue;
            }
            break;
        }

        Integer precision = null;
        Integer scale = null;
        int parenStart = typeName.indexOf('(');
        if (parenStart >= 0) {
            int parenEnd = typeName.indexOf(')');
            if (parenEnd > parenStart) {
                String inner = typeName.substring(parenStart + 1, parenEnd);
                String[] parts = inner.split(",");
                try {
                    precision = Integer.parseInt(parts[0].trim());
                    if (parts.length > 1) scale = Integer.parseInt(parts[1].trim());
                } catch (NumberFormatException e) { /* ignore */ }
            }
        }

        return new ColumnDef(colName, typeName, precision, scale, notNull, pk, unique,
                defaultExpr, refTable, refColumn, generatedExpr, identity, refOnDelete, refOnUpdate,
                identityStart, identityIncrement, deferrable, initiallyDeferred, columnCheckExpr);
    }

    long[] parseSequenceOptionsInParens() {
        long startWith = Long.MIN_VALUE;
        long incrementBy = Long.MIN_VALUE;
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (parser.matchKeyword("START")) {
                parser.matchKeyword("WITH");
                startWith = Long.parseLong(parser.advance().value());
            } else if (parser.matchKeyword("INCREMENT")) {
                parser.matchKeyword("BY");
                incrementBy = Long.parseLong(parser.advance().value());
            } else if (parser.matchKeyword("MINVALUE") || parser.matchKeyword("MAXVALUE") || parser.matchKeyword("CACHE")) {
                if (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN) && parser.peek().type() == TokenType.INTEGER_LITERAL) {
                    parser.advance();
                }
            } else if (parser.matchKeywords("NO", "MINVALUE") || parser.matchKeywords("NO", "MAXVALUE") || parser.matchKeyword("CYCLE")) {
                // consumed
            } else {
                parser.advance();
            }
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return new long[]{startWith, incrementBy};
    }

    TableConstraint parseTableConstraint() {
        String constraintName = null;
        if (parser.matchKeyword("CONSTRAINT")) {
            constraintName = parser.readIdentifier();
        }

        if (parser.matchKeywords("NOT", "NULL")) {
            String col = parser.readIdentifier();
            parser.matchKeywords("NO", "INHERIT");
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.NOT_NULL,
                    Cols.listOf(col), null, null, null, null, null);
        }

        if (parser.matchKeywords("PRIMARY", "KEY")) {
            if (parser.matchKeywords("USING", "INDEX")) {
                String indexName = parser.readIdentifier();
                return new TableConstraint(constraintName, TableConstraint.ConstraintType.PRIMARY_KEY,
                        Cols.listOf("__using_index__:" + indexName), null, null, null, null, null);
            }
            parser.expect(TokenType.LEFT_PAREN);
            List<String> cols = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.PRIMARY_KEY,
                    cols, null, null, null, null, null);
        }

        if (parser.matchKeyword("UNIQUE")) {
            if (parser.matchKeywords("USING", "INDEX")) {
                String indexName = parser.readIdentifier();
                return new TableConstraint(constraintName, TableConstraint.ConstraintType.UNIQUE,
                        Cols.listOf("__using_index__:" + indexName), null, null, null, null, null);
            }
            boolean nullsNotDistinct = false;
            if (parser.matchKeyword("NULLS")) {
                if (parser.matchKeywords("NOT", "DISTINCT")) {
                    nullsNotDistinct = true;
                } else {
                    parser.matchKeyword("DISTINCT");
                }
            }
            parser.expect(TokenType.LEFT_PAREN);
            List<String> cols = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.UNIQUE,
                    cols, null, null, null, null, null, nullsNotDistinct);
        }

        if (parser.matchKeyword("CHECK")) {
            parser.expect(TokenType.LEFT_PAREN);
            Expression checkExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
            parser.matchKeywords("NO", "INHERIT");
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.CHECK,
                    null, checkExpr, null, null, null, null);
        }

        if (parser.matchKeywords("FOREIGN", "KEY")) {
            parser.expect(TokenType.LEFT_PAREN);
            List<String> cols = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
            parser.expectKeyword("REFERENCES");
            String refTable = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) refTable = parser.readIdentifier();
            List<String> refCols = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.expect(TokenType.LEFT_PAREN);
                refCols = parser.parseIdentifierList();
                parser.expect(TokenType.RIGHT_PAREN);
            }
            String onDelete = null, onUpdate = null;
            while (parser.matchKeyword("ON")) {
                if (parser.matchKeyword("DELETE")) onDelete = parseReferentialAction();
                else if (parser.matchKeyword("UPDATE")) onUpdate = parseReferentialAction();
            }
            boolean fkDeferrable = false;
            boolean fkInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                fkDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) fkInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.matchKeyword("NOT")) {
                parser.matchKeyword("DEFERRABLE");
            }
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.FOREIGN_KEY,
                    cols, null, refTable, refCols, onDelete, onUpdate, false, fkDeferrable, fkInitiallyDeferred);
        }

        if (parser.matchKeyword("EXCLUDE")) {
            parser.matchKeyword("USING");
            if (!parser.check(TokenType.LEFT_PAREN)) {
                parser.readIdentifier();
            }
            List<TableConstraint.ExcludeElement> excludeElements = new ArrayList<>();
            List<String> excludeCols = new ArrayList<>();
            parser.expect(TokenType.LEFT_PAREN);
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                StringBuilder colExpr = new StringBuilder();
                int depth = 0;
                while (!parser.isAtEnd()) {
                    if (depth == 0 && parser.checkKeyword("WITH")) break;
                    Token et = parser.advance();
                    if (et.type() == TokenType.LEFT_PAREN) depth++;
                    else if (et.type() == TokenType.RIGHT_PAREN) depth--;
                    if (colExpr.length() > 0) colExpr.append(" ");
                    colExpr.append(et.value());
                }
                String col = colExpr.toString().trim();
                parser.expectKeyword("WITH");
                String op;
                if (parser.check(TokenType.OVERLAP)) {
                    parser.advance();
                    op = "&&";
                } else {
                    op = parser.advance().value();
                }
                excludeElements.add(new TableConstraint.ExcludeElement(col, op));
                excludeCols.add(col);
                if (!parser.match(TokenType.COMMA)) break;
            }
            parser.expect(TokenType.RIGHT_PAREN);
            if (parser.checkKeyword("WHERE")) {
                parser.advance();
                consumeUntilParen(parser);
            }
            if (parser.matchKeyword("DEFERRABLE")) {
                if (parser.matchKeyword("INITIALLY")) {
                    if (!parser.matchKeyword("DEFERRED")) parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.matchKeyword("NOT")) {
                parser.matchKeyword("DEFERRABLE");
            }
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.EXCLUDE,
                    excludeCols, null, null, null, null, null, false, false, false, excludeElements);
        }

        throw new ParseException("Expected constraint type", parser.peek());
    }

    String parseReferentialAction() {
        if (parser.matchKeyword("CASCADE")) return "CASCADE";
        if (parser.matchKeywords("SET", "NULL")) {
            if (parser.check(TokenType.LEFT_PAREN)) { consumeUntilParen(parser); }
            return "SET NULL";
        }
        if (parser.matchKeywords("SET", "DEFAULT")) {
            if (parser.check(TokenType.LEFT_PAREN)) { consumeUntilParen(parser); }
            return "SET DEFAULT";
        }
        if (parser.matchKeyword("RESTRICT")) return "RESTRICT";
        if (parser.matchKeywords("NO", "ACTION")) return "NO ACTION";
        throw new ParseException("Expected referential action", parser.peek());
    }

    // ---- Static utilities shared across parsers ----

    static void consumeUntilCommaOrParen(Parser parser) {
        int depth = 0;
        while (!parser.isAtEnd()) {
            if (depth == 0 && (parser.check(TokenType.COMMA) || parser.check(TokenType.RIGHT_PAREN))) break;
            if (parser.check(TokenType.LEFT_PAREN)) depth++;
            if (parser.check(TokenType.RIGHT_PAREN)) depth--;
            parser.advance();
        }
    }

    static void consumeUntilParen(Parser parser) {
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance();
            int depth = 1;
            while (!parser.isAtEnd() && depth > 0) {
                if (parser.check(TokenType.LEFT_PAREN)) depth++;
                if (parser.check(TokenType.RIGHT_PAREN)) depth--;
                parser.advance();
            }
        }
    }

    /**
     * Capture raw SQL text from the current position until a closing RIGHT_PAREN at depth 0.
     * Does NOT consume the closing RIGHT_PAREN.
     */
    String buildRawSqlUntilCloseParen() {
        StringBuilder text = new StringBuilder();
        int depth = 1;
        while (!parser.isAtEnd() && depth > 0) {
            Token t = parser.peek();
            if (t.type() == TokenType.LEFT_PAREN) depth++;
            if (t.type() == TokenType.RIGHT_PAREN) {
                depth--;
                if (depth == 0) break;
            }
            if (text.length() > 0) text.append(" ");
            if (t.type() == TokenType.STRING_LITERAL) {
                text.append("'").append(t.value().replace("'", "''")).append("'");
            } else {
                text.append(t.value());
            }
            parser.advance();
        }
        return text.toString();
    }
}
