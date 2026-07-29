package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Table creation parsing (CREATE TABLE, column defs, table constraints),
 * extracted from DdlParser.
 */
class DdlTableParser {
    /** The properties LIKE can copy. Anything else is a syntax error, not a silently ignored word. */
    private static final java.util.Set<String> LIKE_OPTIONS = Cols.setOf(
            "ALL", "COMMENTS", "COMPRESSION", "CONSTRAINTS", "DEFAULTS", "GENERATED",
            "IDENTITY", "INDEXES", "STATISTICS", "STORAGE");

    private final Parser parser;
    private final List<TableConstraint> pendingColumnChecks = new ArrayList<>();

    DdlTableParser(Parser parser) {
        this.parser = parser;
    }

    Statement parseCreateTable(boolean temporary, boolean unlogged) {
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
                    while (parser.match(TokenType.COMMA)) {
                        bounds.add(DdlParser.readValueOrMinMax(parser));
                    }
                    parser.expect(TokenType.RIGHT_PAREN);
                    parser.expectKeyword("TO");
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("TO");
                    bounds.add(DdlParser.readValueOrMinMax(parser));
                    while (parser.match(TokenType.COMMA)) {
                        bounds.add(DdlParser.readValueOrMinMax(parser));
                    }
                    parser.expect(TokenType.RIGHT_PAREN);
                } else if (parser.matchKeyword("IN")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("IN");
                    do {
                        bounds.add(DdlParser.readValueOrMinMax(parser));
                    } while (parser.match(TokenType.COMMA));
                    parser.expect(TokenType.RIGHT_PAREN);
                } else if (parser.matchKeyword("WITH")) {
                    parser.expect(TokenType.LEFT_PAREN);
                    bounds.add("HASH");
                    parser.expectKeyword("MODULUS");
                    bounds.add(DdlParser.readHashBoundInteger(parser));
                    parser.expect(TokenType.COMMA);
                    parser.expectKeyword("REMAINDER");
                    bounds.add(DdlParser.readHashBoundInteger(parser));
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
                StringBuilder subPartColBuf = new StringBuilder(readPartitionElement());
                while (parser.match(TokenType.COMMA)) {
                    subPartColBuf.append(", ").append(readPartitionElement());
                }
                subPartCol = subPartColBuf.toString();
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
                String likeTableName = parser.readIdentifier();
                StringBuilder likeOpts = new StringBuilder();
                while (parser.matchKeyword("INCLUDING") || parser.matchKeyword("EXCLUDING")) {
                    boolean including = parser.tokens.get(parser.pos - 1).value().equals("INCLUDING");
                    Token optToken = parser.peek();
                    String what = parser.readIdentifier().toUpperCase();
                    if (!LIKE_OPTIONS.contains(what)) {
                        throw new ParseException("syntax error", optToken);
                    }
                    if (including) {
                        if (likeOpts.length() > 0) likeOpts.append(",");
                        likeOpts.append(what);
                    }
                }
                if (likeOpts.length() > 0) {
                    likeTables.add(likeTableName + ":" + likeOpts);
                } else {
                    likeTables.add(likeTableName);
                }
            } else if (isTableConstraintStart()) {
                TableConstraint tc = parseTableConstraint();
                // A table constraint written in CREATE TABLE may carry NOT VALID. There is
                // nothing already stored for it to skip over, so PostgreSQL takes the
                // constraint as valid and only the word is spare.
                if (parser.matchKeywords("NOT", "VALID")) {
                    if (tc.type() == TableConstraint.ConstraintType.UNIQUE
                            || tc.type() == TableConstraint.ConstraintType.PRIMARY_KEY) {
                        throw new MemgresException(
                                (tc.type() == TableConstraint.ConstraintType.UNIQUE
                                        ? "UNIQUE" : "PRIMARY KEY")
                                + " constraints cannot be marked NOT VALID", "0A000");
                    }
                }
                constraints.add(tc);
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
            StringBuilder partColBuf = new StringBuilder(readPartitionElement());
            while (parser.match(TokenType.COMMA)) {
                partColBuf.append(", ").append(readPartitionElement());
            }
            partitionCol = partColBuf.toString();
            parser.expect(TokenType.RIGHT_PAREN);
        }

        // WITH (storage_parameter = value, ...)
        java.util.Map<String, String> withOptions = null;
        if (parser.matchKeyword("WITH")) {
            if (parser.match(TokenType.LEFT_PAREN)) {
                withOptions = new java.util.LinkedHashMap<>();
                do {
                    String key = parser.readIdentifier();
                    parser.expect(TokenType.EQUALS);
                    String val = parser.advance().value();
                    withOptions.put(key.toLowerCase(), val);
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            }
        }

        String onCommitAction = null;
        if (parser.matchKeywords("ON", "COMMIT")) {
            if (parser.matchKeyword("DROP")) {
                onCommitAction = "DROP";
            } else if (parser.matchKeywords("DELETE", "ROWS")) {
                onCommitAction = "DELETE ROWS";
            } else {
                // PRESERVE is not reserved, so it arrives as a plain identifier.
                parser.readIdentifier();
                parser.matchKeyword("ROWS");
                onCommitAction = "PRESERVE ROWS";
            }
        }

        return new CreateTableStmt(schema, name, ifNotExists, temporary, unlogged, columns, constraints,
                inherits, partitionBy, partitionCol, null, null,
                likeTables.isEmpty() ? null : likeTables, onCommitAction, withOptions);
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
        boolean generatedVirtual = false;
        String identity = null;
        Long identityStart = null;
        Long identityIncrement = null;
        boolean deferrable = false;
        boolean initiallyDeferred = false;
        boolean colNotEnforced = false;
        String colRefMatchType = null;
        Expression columnCheckExpr = null;
        // A CONSTRAINT clause names whatever constraint follows it. PG stores that name, and it
        // is the one SET CONSTRAINTS, ALTER TABLE DROP CONSTRAINT and pg_constraint use.
        String pendingName = null;
        String pkName = null;
        String uqName = null;
        String fkName = null;

        while (true) {
            if (parser.matchKeywords("NOT", "NULL")) { notNull = true; pendingName = null; continue; }
            if (parser.matchKeyword("NULL")) { notNull = false; continue; }
            if (parser.matchKeywords("PRIMARY", "KEY")) {
                pk = true;
                notNull = true;
                pkName = pendingName;
                pendingName = null;
                // A column-level key may say when it is checked, just as a table-level one can.
                if (parser.matchKeyword("DEFERRABLE")) {
                    deferrable = true;
                    if (parser.matchKeyword("INITIALLY")) {
                        if (parser.matchKeyword("DEFERRED")) initiallyDeferred = true;
                        else parser.matchKeyword("IMMEDIATE");
                    }
                } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                    parser.advance(); parser.advance();
                }
                continue;
            }
            if (parser.matchKeyword("UNIQUE")) {
                unique = true;
                uqName = pendingName;
                pendingName = null;
                if (parser.checkKeyword("NULLS")) {
                    parser.advance();
                    if (parser.matchKeywords("NOT", "DISTINCT")) {
                        pendingColumnChecks.add(new TableConstraint(uqName,
                                TableConstraint.ConstraintType.UNIQUE,
                                Cols.listOf(colName), null, null, null, null, null, true));
                        unique = false;
                        uqName = null;
                    } else {
                        parser.matchKeyword("DISTINCT");
                    }
                }
                if (parser.matchKeyword("DEFERRABLE")) {
                    deferrable = true;
                    if (parser.matchKeyword("INITIALLY")) {
                        if (parser.matchKeyword("DEFERRED")) initiallyDeferred = true;
                        else parser.matchKeyword("IMMEDIATE");
                    }
                } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                    parser.advance(); parser.advance();
                }
                continue;
            }
            if (parser.matchKeyword("DEFAULT")) { defaultExpr = parser.parseExpression(); continue; }
            if (parser.matchKeyword("REFERENCES")) {
                fkName = pendingName;
                pendingName = null;
                refTable = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) {
                    refTable = refTable + "." + parser.readIdentifier();
                }
                if (parser.check(TokenType.LEFT_PAREN)) {
                    parser.expect(TokenType.LEFT_PAREN);
                    refColumn = parser.readIdentifier();
                    parser.expect(TokenType.RIGHT_PAREN);
                }
                // MATCH FULL | MATCH PARTIAL | MATCH SIMPLE
                if (parser.matchKeyword("MATCH")) {
                    if (parser.matchKeyword("FULL")) colRefMatchType = "FULL";
                    else if (parser.matchKeyword("PARTIAL")) colRefMatchType = matchPartial();
                    else if (parser.matchKeyword("SIMPLE")) colRefMatchType = "SIMPLE";
                }
                while (parser.matchKeyword("ON")) {
                    if (parser.matchKeyword("DELETE")) {
                        refOnDelete = parseReferentialAction(true);
                    } else if (parser.matchKeyword("UPDATE")) {
                        refOnUpdate = parseReferentialAction(false);
                    }
                }
                if (parser.matchKeyword("DEFERRABLE")) {
                    deferrable = true;
                    if (parser.matchKeyword("INITIALLY")) {
                        if (parser.matchKeyword("DEFERRED")) initiallyDeferred = true;
                        else parser.matchKeyword("IMMEDIATE");
                    }
                } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                    parser.advance(); parser.advance();
                }
                if (parseNotEnforced()) colNotEnforced = true;
                continue;
            }
            if (parser.matchKeyword("CHECK")) {
                parser.expect(TokenType.LEFT_PAREN);
                Expression checkExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
                boolean colChkNoInherit = parser.matchKeywords("NO", "INHERIT");
                boolean checkNotEnforced = parseNotEnforced();
                columnCheckExpr = checkExpr;
                // A check the statement named is stored on the table rather than on the column,
                // which is how it was stored before this clause could carry a name.
                List<String> checkCols = new ArrayList<String>();
                if (pendingName == null) checkCols.add(colName);
                pendingColumnChecks.add(new TableConstraint(pendingName, TableConstraint.ConstraintType.CHECK,
                        checkCols, checkExpr, null, null, null, null,
                        false, false, false, checkNotEnforced, colChkNoInherit, null, null));
                pendingName = null;
                continue;
            }
            // CONSTRAINT only names what comes next; the clause itself is one of the ones above.
            if (parser.matchKeyword("CONSTRAINT")) {
                pendingName = parser.readIdentifier();
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
                        // PG 18: VIRTUAL is default if neither STORED nor VIRTUAL specified
                        if (parser.matchKeyword("STORED")) {
                            generatedVirtual = false;
                        } else if (parser.matchKeyword("VIRTUAL")) {
                            generatedVirtual = true;
                        } else {
                            generatedVirtual = true; // PG 18 default
                        }
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
                    com.memgres.engine.DdlDefinitionChecks.rejectUncollatableType(typeName);
                    String collation = parser.readIdentifier();
                    if (parser.match(TokenType.DOT)) collation = collation + "." + parser.readIdentifier();
                    ExpressionParser.validateCollationStatic(collation, parser.peek());
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

        ColumnDef def = new ColumnDef(colName, typeName, precision, scale, notNull, pk, unique,
                defaultExpr, refTable, refColumn, generatedExpr, generatedVirtual, identity, refOnDelete, refOnUpdate,
                identityStart, identityIncrement, deferrable, initiallyDeferred, colNotEnforced, colRefMatchType, columnCheckExpr);
        def.setPrimaryKeyName(pkName);
        def.setUniqueName(uqName);
        def.setForeignKeyName(fkName);
        return def;
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

    /**
     * Match {@code WITHOUT OVERLAPS}. OVERLAPS is not a reserved word, so it arrives as a plain
     * identifier and has to be matched on the word rather than on the token's kind.
     */
    private boolean matchWithoutOverlaps() {
        if (!parser.checkKeyword("WITHOUT")) return false;
        int idx = parser.pos + 1;
        if (idx >= parser.tokens.size()) return false;
        if (!"OVERLAPS".equalsIgnoreCase(parser.tokens.get(idx).value())) return false;
        parser.advance();
        parser.advance();
        return true;
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
            List<String> cols = new ArrayList<String>();
            String withoutOverlapsCol = null;
            do {
                String c = parser.readIdentifier();
                // PRIMARY KEY (id, valid WITHOUT OVERLAPS): the key is unique per period rather
                // than outright, which is an exclusion constraint by another name.
                if (matchWithoutOverlaps()) {
                    withoutOverlapsCol = c;
                } else {
                    cols.add(c);
                }
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
            if (withoutOverlapsCol != null) {
                List<TableConstraint.ExcludeElement> elems = new ArrayList<TableConstraint.ExcludeElement>();
                for (String c : cols) {
                    elems.add(new TableConstraint.ExcludeElement(c, "="));
                }
                elems.add(new TableConstraint.ExcludeElement(withoutOverlapsCol, "&&"));
                List<String> allCols = new ArrayList<String>(cols);
                allCols.add(withoutOverlapsCol);
                TableConstraint temporal = new TableConstraint(
                        constraintName != null ? constraintName : null,
                        TableConstraint.ConstraintType.EXCLUDE,
                        allCols, null, null, null, null, null, false, false, false, false, elems);
                temporal.setExcludeMethod("gist");
                return temporal;
            }
            boolean pkDeferrable = false, pkInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                pkDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) pkInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance(); parser.advance();
            }
            if (parseNotEnforced()) {
                throw new MemgresException("PRIMARY KEY constraints cannot be marked NOT ENFORCED", "0A000");
            }
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.PRIMARY_KEY,
                    cols, null, null, null, null, null, false, pkDeferrable, pkInitiallyDeferred, false, null);
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
            List<String> cols = parser.parseColumnOrExpressionList();
            parser.expect(TokenType.RIGHT_PAREN);
            boolean uqDeferrable = false, uqInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                uqDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) uqInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance(); parser.advance();
            }
            if (parseNotEnforced()) {
                throw new MemgresException("UNIQUE constraints cannot be marked NOT ENFORCED", "0A000");
            }
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.UNIQUE,
                    cols, null, null, null, null, null, nullsNotDistinct, uqDeferrable, uqInitiallyDeferred, false, null);
        }

        if (parser.matchKeyword("CHECK")) {
            parser.expect(TokenType.LEFT_PAREN);
            Expression checkExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
            boolean chkNoInherit = parser.matchKeywords("NO", "INHERIT");
            boolean chkDeferrable = false, chkInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                chkDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) chkInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance(); parser.advance();
            }
            boolean checkNotEnforced = parseNotEnforced();
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.CHECK,
                    null, checkExpr, null, null, null, null, false, chkDeferrable, chkInitiallyDeferred, checkNotEnforced, chkNoInherit, null, null);
        }

        if (parser.matchKeywords("FOREIGN", "KEY")) {
            parser.expect(TokenType.LEFT_PAREN);
            List<String> cols = parser.parseIdentifierList();
            parser.expect(TokenType.RIGHT_PAREN);
            parser.expectKeyword("REFERENCES");
            String refTable = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) refTable = refTable + "." + parser.readIdentifier();
            List<String> refCols = null;
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.expect(TokenType.LEFT_PAREN);
                refCols = parser.parseIdentifierList();
                parser.expect(TokenType.RIGHT_PAREN);
            }
            // MATCH FULL | MATCH PARTIAL | MATCH SIMPLE
            String fkMatchType = null;
            if (parser.matchKeyword("MATCH")) {
                if (parser.matchKeyword("FULL")) fkMatchType = "FULL";
                else if (parser.matchKeyword("PARTIAL")) fkMatchType = matchPartial();
                else if (parser.matchKeyword("SIMPLE")) fkMatchType = "SIMPLE";
            }
            String onDelete = null, onUpdate = null;
            while (parser.matchKeyword("ON")) {
                if (parser.matchKeyword("DELETE")) onDelete = parseReferentialAction(true);
                else if (parser.matchKeyword("UPDATE")) onUpdate = parseReferentialAction(false);
            }
            boolean fkDeferrable = false;
            boolean fkInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                fkDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) fkInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance(); parser.advance();
            }
            boolean fkNotEnforced = parseNotEnforced();
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.FOREIGN_KEY,
                    cols, null, refTable, refCols, onDelete, onUpdate, false, fkDeferrable, fkInitiallyDeferred, fkNotEnforced, fkMatchType, null);
        }

        if (parser.matchKeyword("EXCLUDE")) {
            parser.matchKeyword("USING");
            String excludeMethod = "btree";  // PG's default index access method
            if (!parser.check(TokenType.LEFT_PAREN)) {
                excludeMethod = parser.readIdentifier();
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
            boolean exDeferrable = false, exInitiallyDeferred = false;
            if (parser.matchKeyword("DEFERRABLE")) {
                exDeferrable = true;
                if (parser.matchKeyword("INITIALLY")) {
                    if (parser.matchKeyword("DEFERRED")) exInitiallyDeferred = true;
                    else parser.matchKeyword("IMMEDIATE");
                }
            } else if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance(); parser.advance();
            }
            boolean exNotEnforced = parseNotEnforced();
            if (exNotEnforced) {
                throw new com.memgres.engine.MemgresException(
                        "EXCLUDE constraints cannot be marked NOT ENFORCED", "0A000");
            }
            TableConstraint excl = new TableConstraint(constraintName, TableConstraint.ConstraintType.EXCLUDE,
                    excludeCols, null, null, null, null, null, false, exDeferrable, exInitiallyDeferred, false, excludeElements);
            excl.setExcludeMethod(excludeMethod);
            return excl;
        }

        throw new ParseException("Expected constraint type", parser.peek());
    }

    /**
     * PostgreSQL has never implemented MATCH PARTIAL and rejects it in the grammar. Accepting it
     * would leave the constraint's semantics undefined — there would be no reference behaviour to
     * match — so it is rejected here too, with PostgreSQL's own wording.
     */
    private String matchPartial() {
        throw new MemgresException("MATCH PARTIAL not yet implemented", "0A000");
    }

    /**
     * @param onDelete true for ON DELETE, false for ON UPDATE; a column list is only accepted
     *                 on ON DELETE, exactly as PostgreSQL's grammar has it since PG 15
     */
    String parseReferentialAction(boolean onDelete) {
        if (parser.matchKeyword("CASCADE")) return "CASCADE";
        if (parser.matchKeywords("SET", "NULL")) {
            String cols = parseReferentialActionColumns("SET NULL", onDelete);
            return cols == null ? "SET NULL" : "SET NULL:" + cols;
        }
        if (parser.matchKeywords("SET", "DEFAULT")) {
            String cols = parseReferentialActionColumns("SET DEFAULT", onDelete);
            return cols == null ? "SET DEFAULT" : "SET DEFAULT:" + cols;
        }
        if (parser.matchKeyword("RESTRICT")) return "RESTRICT";
        if (parser.matchKeywords("NO", "ACTION")) return "NO ACTION";
        throw new ParseException("Expected referential action", parser.peek());
    }

    /** Parse the optional column list of SET NULL / SET DEFAULT; returns null when absent. */
    private String parseReferentialActionColumns(String action, boolean onDelete) {
        if (!parser.check(TokenType.LEFT_PAREN)) return null;
        if (!onDelete) {
            throw new MemgresException("a column list with " + action
                    + " is only supported for ON DELETE actions", "0A000");
        }
        parser.advance(); // consume '('
        StringBuilder colList = new StringBuilder();
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (colList.length() > 0) colList.append(",");
            colList.append(parser.readIdentifier());
            parser.match(TokenType.COMMA);
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return colList.toString();
    }

    /** Parse optional [NOT] ENFORCED clause (PG 18). Returns true if NOT ENFORCED. */
    private boolean parseNotEnforced() {
        if (parser.matchKeyword("ENFORCED")) return false;
        if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "ENFORCED")) {
            parser.advance(); parser.advance();
            return true;
        }
        return false;
    }

    /**
     * Read a partition key element. This can be a simple column name or an expression
     * like date_trunc('month', col). We capture raw SQL text, handling nested parens.
     */
    private String readPartitionElement() {
        StringBuilder sb = new StringBuilder();
        // Handle expression wrapped in parens, e.g., (lower(s))
        if (parser.check(TokenType.LEFT_PAREN)) {
            sb.append("(");
            parser.advance(); // consume (
            int depth = 1;
            while (!parser.isAtEnd() && depth > 0) {
                Token t = parser.peek();
                if (t.type() == TokenType.LEFT_PAREN) depth++;
                if (t.type() == TokenType.RIGHT_PAREN) {
                    depth--;
                    if (depth == 0) { parser.advance(); break; }
                }
                if (t.type() == TokenType.STRING_LITERAL) {
                    sb.append("'").append(t.value().replace("'", "''")).append("'");
                } else {
                    sb.append(t.value());
                }
                parser.advance();
                if (depth > 0) {
                    Token next = parser.peek();
                    if (next.type() != TokenType.RIGHT_PAREN && next.type() != TokenType.COMMA
                            && t.type() != TokenType.LEFT_PAREN && t.type() != TokenType.COMMA) {
                        sb.append(" ");
                    }
                }
            }
            sb.append(")");
            // Optional COLLATE or opclass after the element
            if (parser.matchKeyword("COLLATE")) {
                sb.append(" COLLATE ").append(parser.readIdentifier());
            }
            if (parser.peek().type() == TokenType.IDENTIFIER && !parser.check(TokenType.COMMA)
                    && !parser.check(TokenType.RIGHT_PAREN)) {
                sb.append(" ").append(parser.readIdentifier());
            }
            return sb.toString();
        }
        // Check if this looks like a function call or expression (identifier followed by LEFT_PAREN)
        // or just a simple identifier
        String firstName = parser.readIdentifier();
        sb.append(firstName);
        if (parser.check(TokenType.LEFT_PAREN)) {
            // This is a function call or expression with parens - capture it all
            sb.append("(");
            parser.advance(); // consume (
            int depth = 1;
            while (!parser.isAtEnd() && depth > 0) {
                Token t = parser.peek();
                if (t.type() == TokenType.LEFT_PAREN) depth++;
                if (t.type() == TokenType.RIGHT_PAREN) {
                    depth--;
                    if (depth == 0) { parser.advance(); break; }
                }
                if (t.type() == TokenType.STRING_LITERAL) {
                    sb.append("'").append(t.value().replace("'", "''")).append("'");
                } else {
                    sb.append(t.value());
                }
                parser.advance();
                // Add separator space unless next is a comma, paren, or we just appended a paren
                if (depth > 0) {
                    Token next = parser.peek();
                    if (next.type() != TokenType.RIGHT_PAREN && next.type() != TokenType.COMMA
                            && t.type() != TokenType.LEFT_PAREN && t.type() != TokenType.COMMA) {
                        sb.append(" ");
                    }
                }
            }
            sb.append(")");
        }
        // Optional COLLATE or opclass after the element
        if (parser.matchKeyword("COLLATE")) {
            sb.append(" COLLATE ").append(parser.readIdentifier());
        }
        // Optional operator class name
        if (parser.peek().type() == TokenType.IDENTIFIER && !parser.check(TokenType.COMMA)
                && !parser.check(TokenType.RIGHT_PAREN)) {
            sb.append(" ").append(parser.readIdentifier());
        }
        return sb.toString();
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
