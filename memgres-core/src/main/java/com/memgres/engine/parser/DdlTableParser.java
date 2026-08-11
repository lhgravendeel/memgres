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

    /** Whether this expression is the null constant, written plainly. */
    private static boolean isNullLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.NULL;
    }

    /** The properties LIKE can copy. Anything else is a syntax error, not a silently ignored word. */
    private static final java.util.Set<String> LIKE_OPTIONS = Cols.setOf(
            "ALL", "COMMENTS", "COMPRESSION", "CONSTRAINTS", "DEFAULTS", "GENERATED",
            "IDENTITY", "INDEXES", "STATISTICS", "STORAGE");

    private final Parser parser;
    private final List<TableConstraint> pendingColumnChecks = new ArrayList<>();

    /**
     * The relation whose column definitions are being read. PostgreSQL names it in every complaint
     * about a contradictory definition -- {@code multiple default values specified for column "a"
     * of table "t"} -- and the name is not otherwise in scope in the clause loop, because the same
     * loop serves CREATE TABLE and ALTER TABLE ADD COLUMN.
     */
    private String relationName;

    DdlTableParser(Parser parser) {
        this.parser = parser;
    }

    /** Tell the column-definition reader which relation the definitions belong to. */
    void setRelationName(String name) {
        this.relationName = name;
    }

    Statement parseCreateTable(boolean temporary, boolean unlogged) {
        boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");

        String schema = null;
        String name = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) {
            schema = name;
            name = parser.readIdentifier();
        }
        setRelationName(name);

        // CREATE TABLE name OF composite_type [ (constraint, ...) ]: a typed table takes its
        // columns from the type, so what may be written here are constraints rather than columns.
        if (parser.matchKeyword("OF")) {
            // PostgreSQL's grammar reads a plain, possibly qualified name here rather than a type
            // with a modifier, because the parenthesis that may follow opens the constraint list.
            // Reading it as a modifier took "(PRIMARY" for a width and stopped on the KEY after it.
            String ofType = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) {
                ofType = ofType.toLowerCase() + "." + parser.readIdentifier().toLowerCase();
            }
            List<TableConstraint> ofConstraints = new ArrayList<TableConstraint>();
            List<ColumnDef> ofColumnOptions = new ArrayList<ColumnDef>();
            if (parser.match(TokenType.LEFT_PAREN)) {
                if (!parser.check(TokenType.RIGHT_PAREN)) {
                    do {
                        // The element list of a typed table holds table constraints and, for a
                        // column the type already declares, the options written for it here:
                        // PostgreSQL's own alternative is ColId WITH OPTIONS ColQualList.
                        if (isTableConstraintStart()) {
                            ofConstraints.add(parseTableConstraint());
                            continue;
                        }
                        String optionColumn = parser.readColumnName();
                        parser.expectKeyword("WITH");
                        parser.expectKeyword("OPTIONS");
                        ofColumnOptions.add(parseColumnQualifiers(optionColumn, null));
                        while (!pendingColumnChecks.isEmpty()) {
                            ofConstraints.add(pendingColumnChecks.remove(0));
                        }
                    } while (parser.match(TokenType.COMMA));
                }
                parser.expect(TokenType.RIGHT_PAREN);
            }
            CreateTableStmt typed = new CreateTableStmt(schema, name, ifNotExists, temporary,
                    unlogged, ofColumnOptions, ofConstraints, null, null, null, null,
                    null, null, null, null);
            typed.setOfType(ofType);
            return typed;
        }

        // CREATE TABLE ... (a, b) AS query -- a bare name list renaming the query's columns,
        // told from an ordinary column definition list by what follows the closing paren.
        List<String> ctasColumns = null;
        if (parser.check(TokenType.LEFT_PAREN) && bareNameListFollowedByAs()) {
            parser.expect(TokenType.LEFT_PAREN);
            ctasColumns = new ArrayList<String>();
            do {
                ctasColumns.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
        }

        // CREATE TABLE ... AS query
        if (parser.matchKeyword("AS")) {
            // What follows AS is a query. EXPLAIN carries one but is not one, and taking it
            // anyway built a table out of the plan text.
            if (parser.checkKeyword("EXPLAIN")) {
                Token t = parser.peek();
                throw ParseException.saying("syntax error at or near \"" + t.raw() + "\"", t, "42601");
            }
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
            return new CreateTableAsStmt(schema, name, ifNotExists, temporary, query, withData,
                    ctasColumns);
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
                    // EXCLUDING is written down too: the options are applied left to right, so
                    // INCLUDING ALL EXCLUDING DEFAULTS has to be able to take the defaults back
                    // out again. A dropped EXCLUDING left ALL standing on its own.
                    if (likeOpts.length() > 0) likeOpts.append(",");
                    likeOpts.append(including ? "" : "-").append(what);
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
            int partElements = 1;
            while (parser.match(TokenType.COMMA)) {
                partColBuf.append(", ").append(readPartitionElement());
                partElements++;
            }
            // LIST matches one value against a written list, so there is nothing for a second
            // column to be matched against. RANGE compares a tuple of keys and HASH hashes one,
            // and PostgreSQL takes several columns for both.
            if (partElements > 1 && "LIST".equals(partitionBy)) {
                throw new MemgresException("cannot use \"list\" partition strategy with more than"
                        + " one column", "42P17");
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
        String colName = parser.readColumnName();
        String typeName = parser.parseTypeName();
        return parseColumnQualifiers(colName, serialSpelling(typeName));
    }

    /**
     * The qualifiers of a column definition, over a name and a type already read.
     *
     * <p>A typed table writes them on their own — {@code col WITH OPTIONS NOT NULL} — because the
     * type comes from the composite the table is OF and there is none to read here. The list of
     * qualifiers PostgreSQL takes is the same one either way, so both are read by this.
     */
    ColumnDef parseColumnQualifiers(String colName, String typeName) {
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
        String columnCollation = null;
        String pkName = null;
        String uqName = null;
        String fkName = null;
        String nnName = null;
        // The clauses of a column definition are a set, not a sequence: PostgreSQL refuses one
        // that says the same thing twice or two contradictory things, rather than letting the last
        // clause written decide what the column is.
        boolean sawNotNull = false, sawNull = false, sawDefault = false;
        boolean sawIdentity = false, sawGenerated = false;
        boolean notNullNoInherit = false;
        IdentityOptions identityOptions = null;
        // SERIAL stands for a column whose default draws from a sequence, so a DEFAULT written
        // beside it is a second default even though the first one is made by the executor.
        if (isSerialTypeName(typeName)) sawDefault = true;

        while (true) {
            if (parser.matchKeywords("NOT", "NULL")) {
                if (sawNull) throw conflictingNullDeclarations(colName);
                notNull = true;
                sawNotNull = true;
                // A NOT NULL may say it is not to travel to inheritance children, exactly as a
                // table-level CHECK may; reading nothing after the two words made NO INHERIT a
                // syntax error on a definition PostgreSQL accepts.
                notNullNoInherit = parser.matchKeywords("NO", "INHERIT");
                // A CONSTRAINT clause names this NOT NULL just as it names a key, and PG stores
                // that name: it is what pg_constraint lists and what DROP CONSTRAINT asks for.
                nnName = pendingName;
                pendingName = null;
                continue;
            }
            if (parser.matchKeyword("NULL")) {
                if (sawNotNull) throw conflictingNullDeclarations(colName);
                notNull = false;
                sawNull = true;
                continue;
            }
            if (parser.matchKeywords("PRIMARY", "KEY")) {
                pk = true;
                notNull = true;
                pkName = pendingName;
                pendingName = null;
                // A column-level key may say when it is checked, just as a table-level one can.
                Deferrability pkDef = parseDeferrability();
                if (pkDef.firstClause != null) {
                    deferrable = pkDef.deferrable;
                    initiallyDeferred = pkDef.initiallyDeferred;
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
                Deferrability uqDef = parseDeferrability();
                if (uqDef.firstClause != null) {
                    deferrable = uqDef.deferrable;
                    initiallyDeferred = uqDef.initiallyDeferred;
                }
                continue;
            }
            if (parser.matchKeyword("DEFAULT")) {
                // A column has one default; a second clause contradicts the first rather than
                // replacing it, and PostgreSQL names the column and its table when it says so.
                if (sawDefault) {
                    throw com.memgres.engine.PgErrors.syntax(
                            "multiple default values specified for column \"" + colName
                            + "\" of table \"" + relationOrEmpty() + "\"");
                }
                sawDefault = true;
                Expression written = parser.parseExpression();
                // A column with no default already answers NULL, so DEFAULT NULL adds nothing:
                // PostgreSQL records no default for it at all, and pg_attribute says the column
                // has none. Storing one made the catalogue describe a table nobody wrote.
                defaultExpr = isNullLiteral(written) ? null : written;
                continue;
            }
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
                Deferrability fkDef = parseDeferrability();
                if (fkDef.firstClause != null) {
                    deferrable = fkDef.deferrable;
                    initiallyDeferred = fkDef.initiallyDeferred;
                }
                if (parseNotEnforced()) colNotEnforced = true;
                continue;
            }
            if (parser.matchKeyword("CHECK")) {
                parser.expect(TokenType.LEFT_PAREN);
                Expression checkExpr = parser.parseExpression();
                parser.expect(TokenType.RIGHT_PAREN);
                boolean colChkNoInherit = parser.matchKeywords("NO", "INHERIT");
                rejectColumnCheckDeferrability(parseDeferrability());
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
                        requireOneGeneration(colName, sawIdentity || sawGenerated);
                        sawIdentity = true;
                        identity = "ALWAYS";
                        if (parser.check(TokenType.LEFT_PAREN)) {
                            parser.advance();
                            identityOptions = parseSequenceOptionsInParens();
                            identityStart = identityOptions.start;
                            identityIncrement = identityOptions.increment;
                        }
                    } else {
                        requireOneGeneration(colName, sawIdentity || sawGenerated);
                        sawGenerated = true;
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
                    // Only identity may be written BY DEFAULT: a generation expression is
                    // recomputed for every row, so there is nothing for a written value to
                    // override. PostgreSQL names that rule rather than reporting the paren.
                    if (parser.check(TokenType.LEFT_PAREN)) {
                        throw com.memgres.engine.PgErrors.syntax(
                                "for a generated column, GENERATED ALWAYS must be specified");
                    }
                    parser.expectKeyword("IDENTITY");
                    requireOneGeneration(colName, sawIdentity || sawGenerated);
                    sawIdentity = true;
                    identity = "BY DEFAULT";
                    if (parser.check(TokenType.LEFT_PAREN)) {
                        parser.advance();
                        identityOptions = parseSequenceOptionsInParens();
                        identityStart = identityOptions.start;
                        identityIncrement = identityOptions.increment;
                    }
                }
                continue;
            }
            if (parser.matchKeyword("COLLATE")) {
                if (!parser.isClauseKeyword()) {
                    // Whether the type carries a collation at all is the executor's to say, and it
                    // says it last: PostgreSQL settles the type name and then the collation name
                    // before it asks whether the two go together.
                    String collation = parser.readIdentifier();
                    if (parser.match(TokenType.DOT)) collation = collation + "." + parser.readIdentifier();
                    ExpressionParser.validateCollationStatic(collation, parser.peek());
                    columnCollation = collation;
                    // A column carries one collation, so a second clause is a syntax error
                    // rather than an override.
                    if (parser.checkKeyword("COLLATE")) {
                        throw new MemgresException("multiple COLLATE clauses not allowed", "42601");
                    }
                }
                continue;
            }
            break;
        }

        Integer precision = null;
        Integer scale = null;
        // A typed table's column options carry no type at all, so there is no modifier to read
        // off one: the composite the table is OF settles both.
        int parenStart = typeName == null ? -1 : typeName.indexOf('(');
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
        def.collation = columnCollation;
        def.setPrimaryKeyName(pkName);
        def.setUniqueName(uqName);
        def.setForeignKeyName(fkName);
        def.setNotNullName(nnName);
        def.setNotNullNoInherit(notNullNoInherit);
        if (identityOptions != null) {
            def.setIdentityOptions(identityOptions.minValue, identityOptions.maxValue,
                    identityOptions.cache, identityOptions.cycle, identityOptions.sequenceName,
                    identityOptions.any);
        }
        return def;
    }

    /**
     * The CHECK and UNIQUE NULLS NOT DISTINCT clauses a column definition carried, taken away from
     * the parser. CREATE TABLE collects them with the table's own constraints; ALTER TABLE ADD
     * COLUMN has to take them from here or they are simply dropped.
     */
    List<TableConstraint> drainPendingColumnChecks() {
        List<TableConstraint> drained = new ArrayList<TableConstraint>(pendingColumnChecks);
        pendingColumnChecks.clear();
        return drained;
    }

    /**
     * The options of the sequence behind an identity column, exactly as CREATE SEQUENCE writes
     * them. Every one of them is PostgreSQL's to check before the column is defined, so keeping
     * only START and INCREMENT meant no option check ever ran.
     */
    static final class IdentityOptions {
        Long start;
        Long increment;
        Long minValue;
        Long maxValue;
        Integer cache;
        Boolean cycle;
        String sequenceName;
        /** Whether the list said anything at all, which is what tells a described sequence from a default one. */
        boolean any;
    }

    IdentityOptions parseSequenceOptionsInParens() {
        IdentityOptions opts = new IdentityOptions();
        while (!parser.isAtEnd() && !parser.check(TokenType.RIGHT_PAREN)) {
            if (parser.matchKeywords("SEQUENCE", "NAME")) {
                String seqName = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) seqName = seqName + "." + parser.readIdentifier();
                opts.sequenceName = seqName;
                opts.any = true;
            } else if (parser.matchKeyword("START")) {
                parser.matchKeyword("WITH");
                // The lexer hands a minus over as a token of its own, so reading one raw token
                // saw "-" and failed with an internal error where PostgreSQL takes the value.
                opts.start = Long.valueOf(DdlParser.readSeqLong(parser));
                opts.any = true;
            } else if (parser.matchKeyword("INCREMENT")) {
                parser.matchKeyword("BY");
                opts.increment = Long.valueOf(DdlParser.readSeqLong(parser));
                opts.any = true;
            } else if (parser.matchKeywords("NO", "MINVALUE")) {
                opts.minValue = null;
                opts.any = true;
            } else if (parser.matchKeywords("NO", "MAXVALUE")) {
                opts.maxValue = null;
                opts.any = true;
            } else if (parser.matchKeywords("NO", "CYCLE")) {
                opts.cycle = Boolean.FALSE;
                opts.any = true;
            } else if (parser.matchKeyword("MINVALUE")) {
                opts.minValue = Long.valueOf(DdlParser.readSeqLong(parser));
                opts.any = true;
            } else if (parser.matchKeyword("MAXVALUE")) {
                opts.maxValue = Long.valueOf(DdlParser.readSeqLong(parser));
                opts.any = true;
            } else if (parser.matchKeyword("CACHE")) {
                opts.cache = Integer.valueOf((int) DdlParser.readSeqLong(parser));
                opts.any = true;
            } else if (parser.matchKeyword("CYCLE")) {
                opts.cycle = Boolean.TRUE;
                opts.any = true;
            } else {
                parser.advance();
            }
        }
        parser.expect(TokenType.RIGHT_PAREN);
        return opts;
    }

    /** The relation the definition belongs to, as PostgreSQL names it in these complaints. */
    private String relationOrEmpty() {
        return relationName == null ? "" : relationName;
    }

    /** {@code 42601} -- the definition says both that the column may be null and that it may not. */
    private MemgresException conflictingNullDeclarations(String colName) {
        return com.memgres.engine.PgErrors.syntax(
                "conflicting NULL/NOT NULL declarations for column \"" + colName
                + "\" of table \"" + relationOrEmpty() + "\"");
    }

    /**
     * A column's value comes either from an identity sequence or from a generation expression, and
     * PostgreSQL refuses a definition that asks for both.
     */
    private void requireOneGeneration(String colName, boolean already) {
        if (already) {
            throw com.memgres.engine.PgErrors.syntax(
                    "both identity and generation expression specified for column \"" + colName
                    + "\" of table \"" + relationOrEmpty() + "\"");
        }
    }

    /** True for the pseudo-types that stand for an integer column fed by a sequence. */
    private static boolean isSerialTypeName(String typeName) {
        if (typeName == null) return false;
        String bare = typeName.replaceAll("\\(.*\\)", "").trim();
        return bare.equalsIgnoreCase("serial") || bare.equalsIgnoreCase("bigserial")
                || bare.equalsIgnoreCase("smallserial") || bare.equalsIgnoreCase("serial2")
                || bare.equalsIgnoreCase("serial4") || bare.equalsIgnoreCase("serial8");
    }

    /**
     * The type a column declared with the serial shorthand really has.
     *
     * <p>serial2, serial4 and serial8 are the same shorthand as smallserial, serial and bigserial:
     * PostgreSQL matches all six spellings where a column's type is written and rewrites the column
     * to an integer column with a sequence behind it. The shorthand means that only in a column
     * definition -- a cast, a domain, a composite field or a retype naming one of these words finds
     * no type of the name at all -- so the numbered spellings are folded onto the words they stand
     * for here, where a column definition is read, rather than made types of their own. A name
     * written under a schema is a real type name and is left alone: pg_catalog holds no serial2.
     *
     * <p>Neither an array of the shorthand nor a modifier written after it means anything. One
     * sequence stands behind one serial column, so there is nothing for an array of them to draw
     * from; and the integer type the shorthand stands for takes no modifier -- which is the type
     * PostgreSQL names when it turns one down, because that is the type the column would have had.
     */
    private static String serialSpelling(String typeName) {
        if (typeName == null) return null;
        String written = typeName.replaceAll("\\(.*\\)", "").trim();
        boolean isArray = written.endsWith("[]");
        String scalar = isArray ? written.substring(0, written.length() - 2).trim() : written;
        if (scalar.indexOf('.') >= 0 || !isSerialTypeName(scalar)) return typeName;
        if (isArray) {
            throw com.memgres.engine.PgErrors.notImplemented("array of serial is not implemented");
        }
        String folded;
        String integerType;
        if (scalar.equalsIgnoreCase("smallserial") || scalar.equalsIgnoreCase("serial2")) {
            folded = "smallserial";
            integerType = "smallint";
        } else if (scalar.equalsIgnoreCase("bigserial") || scalar.equalsIgnoreCase("serial8")) {
            folded = "bigserial";
            integerType = "bigint";
        } else {
            folded = "serial";
            integerType = "integer";
        }
        if (!written.equals(typeName.trim())) {
            throw com.memgres.engine.PgErrors.syntax(
                    "type modifier is not allowed for type \"" + integerType + "\"");
        }
        return folded;
    }

    /**
     * {@code INCLUDE (col, ...)} after a key's column list, standard since PG 11. The named
     * columns are payload carried in the index rather than part of the key -- PostgreSQL compares
     * only the key columns for uniqueness -- so they are kept apart from the key list from here on.
     */
    private List<String> parseIncludeColumns() {
        if (!parser.matchKeyword("INCLUDE")) return null;
        parser.expect(TokenType.LEFT_PAREN);
        List<String> cols = new ArrayList<String>();
        do {
            cols.add(parser.readIdentifier());
        } while (parser.match(TokenType.COMMA));
        parser.expect(TokenType.RIGHT_PAREN);
        return cols;
    }

    /**
     * A foreign key's column list, where the last of them may be written {@code PERIOD}.
     *
     * <p>{@code FOREIGN KEY (id, PERIOD v) REFERENCES p (id, PERIOD v)} names the same columns an
     * ordinary key would; what {@code PERIOD} says is that the last one is a span rather than a
     * value, and is to be covered rather than matched. Reading the list without it stopped at the
     * word and reported a syntax error on the column after it.
     */
    private List<String> parseKeyColumnsWithPeriod(boolean[] sawPeriod) {
        List<String> cols = new ArrayList<String>();
        do {
            if (periodPrefixAhead()) {
                parser.advance();
                sawPeriod[0] = true;
            }
            cols.add(parser.readIdentifier());
        } while (parser.match(TokenType.COMMA));
        return cols;
    }

    /**
     * Whether the word here is {@code PERIOD} introducing a column rather than naming one.
     *
     * <p>PERIOD is not a reserved word, so a column may be called it, and {@code FOREIGN KEY
     * (period)} names that column. What tells them apart is what follows: the marker is written
     * in front of a column name, and a column name is followed by a comma or the closing paren.
     */
    private boolean periodPrefixAhead() {
        if (!"PERIOD".equalsIgnoreCase(parser.peek().value())) return false;
        int next = parser.pos + 1;
        if (next >= parser.tokens.size()) return false;
        TokenType type = parser.tokens.get(next).type();
        return type == TokenType.IDENTIFIER || type == TokenType.KEYWORD;
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

    /**
     * Whether the key list starting at the open paren carries a {@code WITHOUT OVERLAPS} element.
     * Looked at before parsing so an ordinary key list — which may hold expressions a temporal key
     * never does — keeps going down the path that already reads it.
     */
    private boolean withoutOverlapsAheadInParens() {
        int depth = 0;
        for (int i = parser.pos; i < parser.tokens.size(); i++) {
            TokenType t = parser.tokens.get(i).type();
            if (t == TokenType.LEFT_PAREN) depth++;
            else if (t == TokenType.RIGHT_PAREN) {
                if (depth == 0) return false;
                depth--;
            } else if (depth == 0 && "WITHOUT".equalsIgnoreCase(parser.tokens.get(i).value())
                    && i + 1 < parser.tokens.size()
                    && "OVERLAPS".equalsIgnoreCase(parser.tokens.get(i + 1).value())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Read {@code (a, b, period WITHOUT OVERLAPS)} — without the closing paren — as the exclusion
     * constraint it means: the scalar columns compared for equality, the period for overlap.
     */
    private TableConstraint parseWithoutOverlapsKey(String constraintName) {
        List<String> cols = new ArrayList<String>();
        String withoutOverlapsCol = null;
        do {
            String c = parser.readIdentifier();
            if (matchWithoutOverlaps()) {
                withoutOverlapsCol = c;
            } else {
                cols.add(c);
            }
        } while (parser.match(TokenType.COMMA));
        List<TableConstraint.ExcludeElement> elems = new ArrayList<TableConstraint.ExcludeElement>();
        for (String c : cols) {
            elems.add(new TableConstraint.ExcludeElement(c, "="));
        }
        elems.add(new TableConstraint.ExcludeElement(withoutOverlapsCol, "&&"));
        List<String> allCols = new ArrayList<String>(cols);
        allCols.add(withoutOverlapsCol);
        TableConstraint temporal = new TableConstraint(constraintName,
                TableConstraint.ConstraintType.EXCLUDE,
                allCols, null, null, null, null, null, false, false, false, false, elems);
        temporal.setExcludeMethod("gist");
        return temporal;
    }

    /**
     * Whether the parenthesised list starting here is a bare list of names with {@code AS} after
     * it, which is the CREATE TABLE t (a, b) AS query form rather than a column definition list.
     * Every item has to be a single identifier: anything carrying a type, a constraint or an
     * expression is an ordinary definition and is left to the path that reads those.
     */
    private boolean bareNameListFollowedByAs() {
        int i = parser.pos;
        if (parser.tokens.get(i).type() != TokenType.LEFT_PAREN) return false;
        i++;
        boolean wantName = true;
        while (i < parser.tokens.size()) {
            TokenType t = parser.tokens.get(i).type();
            if (t == TokenType.RIGHT_PAREN) {
                if (wantName) return false;
                i++;
                break;
            }
            if (wantName) {
                if (t == TokenType.COMMA || t == TokenType.LEFT_PAREN) return false;
                wantName = false;
            } else {
                if (t != TokenType.COMMA) return false;
                wantName = true;
            }
            i++;
        }
        return i < parser.tokens.size()
                && "AS".equalsIgnoreCase(parser.tokens.get(i).value());
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
            // PRIMARY KEY (id, valid WITHOUT OVERLAPS): the key is unique per period rather
            // than outright, which is an exclusion constraint by another name.
            if (withoutOverlapsAheadInParens()) {
                TableConstraint temporal = parseWithoutOverlapsKey(constraintName);
                parser.expect(TokenType.RIGHT_PAREN);
                return temporal;
            }
            List<String> cols = new ArrayList<String>();
            do {
                cols.add(parser.readIdentifier());
            } while (parser.match(TokenType.COMMA));
            parser.expect(TokenType.RIGHT_PAREN);
            List<String> pkInclude = parseIncludeColumns();
            Deferrability pkDef = parseDeferrability();
            boolean pkDeferrable = pkDef.deferrable, pkInitiallyDeferred = pkDef.initiallyDeferred;
            if (parseNotEnforced()) {
                throw new MemgresException("PRIMARY KEY constraints cannot be marked NOT ENFORCED", "0A000");
            }
            TableConstraint pkConstraint = new TableConstraint(constraintName,
                    TableConstraint.ConstraintType.PRIMARY_KEY,
                    cols, null, null, null, null, null, false, pkDeferrable, pkInitiallyDeferred, false, null);
            pkConstraint.setIncludedColumns(pkInclude);
            return pkConstraint;
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
            // UNIQUE (id, valid_at WITHOUT OVERLAPS) is the same temporal key as the PRIMARY KEY
            // spelling: unique per period rather than outright, which is an exclusion constraint
            // by another name.
            if (withoutOverlapsAheadInParens()) {
                TableConstraint temporal = parseWithoutOverlapsKey(constraintName);
                parser.expect(TokenType.RIGHT_PAREN);
                // The constraint this becomes carries no deferrability of its own, but the
                // clauses are still ordinary SQL after the key and have to be read off.
                parseDeferrability();
                if (parseNotEnforced()) {
                    throw new MemgresException("UNIQUE constraints cannot be marked NOT ENFORCED", "0A000");
                }
                return temporal;
            }
            List<String> cols = parser.parseColumnOrExpressionList();
            parser.expect(TokenType.RIGHT_PAREN);
            List<String> uqInclude = parseIncludeColumns();
            Deferrability uqDef = parseDeferrability();
            boolean uqDeferrable = uqDef.deferrable, uqInitiallyDeferred = uqDef.initiallyDeferred;
            if (parseNotEnforced()) {
                throw new MemgresException("UNIQUE constraints cannot be marked NOT ENFORCED", "0A000");
            }
            TableConstraint uqConstraint = new TableConstraint(constraintName,
                    TableConstraint.ConstraintType.UNIQUE,
                    cols, null, null, null, null, null, nullsNotDistinct, uqDeferrable, uqInitiallyDeferred, false, null);
            uqConstraint.setIncludedColumns(uqInclude);
            return uqConstraint;
        }

        if (parser.matchKeyword("CHECK")) {
            parser.expect(TokenType.LEFT_PAREN);
            Expression checkExpr = parser.parseExpression();
            parser.expect(TokenType.RIGHT_PAREN);
            boolean chkNoInherit = parser.matchKeywords("NO", "INHERIT");
            rejectDeferrableCheck(parseDeferrability());
            boolean checkNotEnforced = parseNotEnforced();
            return new TableConstraint(constraintName, TableConstraint.ConstraintType.CHECK,
                    null, checkExpr, null, null, null, null, false, false, false, checkNotEnforced, chkNoInherit, null, null);
        }

        if (parser.matchKeywords("FOREIGN", "KEY")) {
            parser.expect(TokenType.LEFT_PAREN);
            boolean[] referencingPeriod = new boolean[1];
            List<String> cols = parseKeyColumnsWithPeriod(referencingPeriod);
            parser.expect(TokenType.RIGHT_PAREN);
            parser.expectKeyword("REFERENCES");
            String refTable = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) refTable = refTable + "." + parser.readIdentifier();
            List<String> refCols = null;
            boolean[] referencedPeriod = new boolean[1];
            if (parser.check(TokenType.LEFT_PAREN)) {
                parser.expect(TokenType.LEFT_PAREN);
                refCols = parseKeyColumnsWithPeriod(referencedPeriod);
                parser.expect(TokenType.RIGHT_PAREN);
            }
            if (referencingPeriod[0] != referencedPeriod[0]) {
                throw new MemgresException(referencingPeriod[0]
                        ? "foreign key uses PERIOD on the referencing table but not the referenced table"
                        : "foreign key uses PERIOD on the referenced table but not the referencing table",
                        "42830");
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
            Deferrability fkDef = parseDeferrability();
            boolean fkDeferrable = fkDef.deferrable;
            boolean fkInitiallyDeferred = fkDef.initiallyDeferred;
            boolean fkNotEnforced = parseNotEnforced();
            TableConstraint fk = new TableConstraint(constraintName, TableConstraint.ConstraintType.FOREIGN_KEY,
                    cols, null, refTable, refCols, onDelete, onUpdate, false, fkDeferrable, fkInitiallyDeferred, fkNotEnforced, fkMatchType, null);
            fk.setPeriod(referencingPeriod[0]);
            return fk;
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
            Deferrability exDef = parseDeferrability();
            boolean exDeferrable = exDef.deferrable, exInitiallyDeferred = exDef.initiallyDeferred;
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

    /** When a constraint is checked: DEFERRABLE and INITIALLY, resolved together. */
    static final class Deferrability {
        boolean deferrable;
        boolean initiallyDeferred;
        /** Whether NOT DEFERRABLE was written, which INITIALLY DEFERRED cannot then undo. */
        boolean sawNotDeferrable;
        /** The first clause as it was written, for the error a misplaced one raises. */
        String firstClause;

        void saw(String clause) {
            if (firstClause == null) firstClause = clause;
        }
    }

    /**
     * Read the constraint attributes that say when a constraint is checked. PostgreSQL takes
     * {@code DEFERRABLE}, {@code NOT DEFERRABLE}, {@code INITIALLY DEFERRED} and
     * {@code INITIALLY IMMEDIATE} as independent clauses in any order and any combination, so
     * {@code NOT DEFERRABLE INITIALLY IMMEDIATE} and a bare {@code INITIALLY DEFERRED} are both
     * ordinary SQL; the latter carries DEFERRABLE with it. Reads nothing when none is present.
     */
    private Deferrability parseDeferrability() {
        Deferrability d = new Deferrability();
        while (true) {
            if (parser.checkKeyword("NOT") && parser.checkKeywordAt(1, "DEFERRABLE")) {
                parser.advance();
                parser.advance();
                d.deferrable = false;
                d.initiallyDeferred = false;
                d.sawNotDeferrable = true;
                d.saw("NOT DEFERRABLE");
                continue;
            }
            if (parser.matchKeyword("DEFERRABLE")) {
                d.deferrable = true;
                d.saw("DEFERRABLE");
                continue;
            }
            if (parser.checkKeyword("INITIALLY")
                    && (parser.checkKeywordAt(1, "DEFERRED") || parser.checkKeywordAt(1, "IMMEDIATE"))) {
                parser.advance();
                if (parser.matchKeyword("DEFERRED")) {
                    // A bare INITIALLY DEFERRED carries DEFERRABLE with it, but it cannot carry it
                    // over a NOT DEFERRABLE the same clause list already wrote down: the two say
                    // opposite things about when the constraint is checked.
                    if (d.sawNotDeferrable) {
                        throw com.memgres.engine.PgErrors.syntax(
                                "constraint declared INITIALLY DEFERRED must be DEFERRABLE");
                    }
                    d.initiallyDeferred = true;
                    d.deferrable = true;
                    d.saw("INITIALLY DEFERRED");
                } else {
                    parser.advance();
                    d.initiallyDeferred = false;
                    d.saw("INITIALLY IMMEDIATE");
                }
                continue;
            }
            return d;
        }
    }

    /**
     * A CHECK constraint is evaluated by the row that writes it and nothing later can make it
     * true, so PostgreSQL refuses to mark one DEFERRABLE rather than accepting an attribute it
     * would then ignore. {@code NOT DEFERRABLE} and {@code INITIALLY IMMEDIATE} say what a CHECK
     * already is and are accepted.
     */
    private void rejectDeferrableCheck(Deferrability d) {
        if (d.deferrable) {
            throw new MemgresException("CHECK constraints cannot be marked DEFERRABLE", "0A000");
        }
    }

    /**
     * On a column, the deferrability clauses belong to a key or a reference; written after a
     * CHECK there is nothing for them to attach to, and PostgreSQL says so by name — including
     * for {@code NOT DEFERRABLE}, which a table-level CHECK accepts.
     */
    private void rejectColumnCheckDeferrability(Deferrability d) {
        if (d.firstClause != null) {
            throw new MemgresException("misplaced " + d.firstClause + " clause", "42601");
        }
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
