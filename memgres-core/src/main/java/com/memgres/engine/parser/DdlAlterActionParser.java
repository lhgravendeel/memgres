package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * ALTER TABLE action parsing (ADD/DROP/ALTER COLUMN, constraints, partitions, etc.),
 * extracted from DdlParser.
 */
class DdlAlterActionParser {
    private final Parser parser;
    private final DdlTableParser tableParser;

    DdlAlterActionParser(Parser parser, DdlTableParser tableParser) {
        this.parser = parser;
        this.tableParser = tableParser;
    }

    AlterTableStmt.AlterAction parseAlterAction() {
        if (parser.matchKeywords("ADD", "COLUMN")) {
            boolean ifNotExists = parser.matchKeywords("IF", "NOT", "EXISTS");
            return new AlterTableStmt.AddColumn(tableParser.parseColumnDef(), ifNotExists);
        }
        if (parser.matchKeyword("ADD")) {
            if (tableParser.isTableConstraintStart()) {
                TableConstraint tc = tableParser.parseTableConstraint();
                boolean notValid = parser.matchKeywords("NOT", "VALID");
                return new AlterTableStmt.AddConstraint(tc, notValid);
            }
            // ADD COLUMN without the COLUMN keyword
            return new AlterTableStmt.AddColumn(tableParser.parseColumnDef());
        }
        if (parser.matchKeywords("DROP", "COLUMN")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String col = parser.readIdentifier();
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new AlterTableStmt.DropColumn(col, ifExists, cascade);
        }
        if (parser.matchKeywords("DROP", "CONSTRAINT")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String name = parser.readIdentifier();
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new AlterTableStmt.DropConstraint(name, ifExists, cascade);
        }
        // DROP col: shorthand for DROP COLUMN col (without COLUMN keyword)
        if (parser.matchKeyword("DROP")) {
            boolean ifExists = parser.matchKeywords("IF", "EXISTS");
            String col = parser.readIdentifier();
            boolean cascade = parser.matchKeyword("CASCADE");
            parser.matchKeyword("RESTRICT");
            return new AlterTableStmt.DropColumn(col, ifExists, cascade);
        }
        if (parser.matchKeywords("ALTER", "COLUMN")) {
            String col = parser.readIdentifier();
            return new AlterTableStmt.AlterColumn(col, parseAlterColumnAction());
        }
        if (parser.matchKeywords("ALTER", "CONSTRAINT")) {
            return parseAlterConstraint();
        }
        // ALTER colname (without COLUMN keyword): shorthand for ALTER COLUMN colname
        if (parser.matchKeyword("ALTER")) {
            String col = parser.readIdentifier();
            return new AlterTableStmt.AlterColumn(col, parseAlterColumnAction());
        }
        if (parser.matchKeywords("RENAME", "CONSTRAINT")) {
            String oldName = parser.readIdentifier();
            parser.expectKeyword("TO");
            String newName = parser.readIdentifier();
            return new AlterTableStmt.RenameConstraint(oldName, newName);
        }
        if (parser.matchKeywords("RENAME", "COLUMN")) {
            String oldName = parser.readIdentifier();
            parser.expectKeyword("TO");
            String newName = parser.readIdentifier();
            return new AlterTableStmt.RenameColumn(oldName, newName);
        }
        if (parser.matchKeywords("RENAME", "TO")) {
            String newName = parser.readIdentifier();
            return new AlterTableStmt.RenameTable(newName);
        }
        // RENAME colname TO newname: shorthand for RENAME COLUMN colname TO newname
        if (parser.matchKeyword("RENAME")) {
            String oldName = parser.readIdentifier();
            parser.expectKeyword("TO");
            String newName = parser.readIdentifier();
            return new AlterTableStmt.RenameColumn(oldName, newName);
        }
        if (parser.matchKeywords("SET", "SCHEMA")) {
            String newSchema = parser.readIdentifier();
            return new AlterTableStmt.SetSchema(newSchema);
        }
        // SET (storage_parameter = value, ...): no-op for in-memory database
        if (parser.matchKeyword("SET")) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                DdlTableParser.consumeUntilParen(parser);
                return new AlterTableStmt.SetStorageParams();
            }
            // SET TABLESPACE tsname: no-op for in-memory database
            if (parser.matchKeyword("TABLESPACE")) {
                parser.readIdentifier(); // tablespace name
                return new AlterTableStmt.SetStorageParams();
            }
            // SET LOGGED: change relpersistence to 'p' (permanent)
            if (parser.matchKeyword("LOGGED") || parser.matchIdentifier("LOGGED")) {
                return new AlterTableStmt.SetLogged(true);
            }
            // SET UNLOGGED: change relpersistence to 'u' (unlogged)
            if (parser.matchKeyword("UNLOGGED")) {
                return new AlterTableStmt.SetLogged(false);
            }
            // SET ACCESS METHOD amname: no-op for in-memory database
            if (parser.matchKeywords("ACCESS", "METHOD")) {
                parser.readIdentifier(); // access method name
                return new AlterTableStmt.SetStorageParams();
            }
            // SET WITHOUT CLUSTER / SET WITHOUT OIDS: both concern on-disk layout, so there is
            // nothing to change here, but they are valid SQL and must not be a syntax error.
            if (parser.matchKeyword("WITHOUT")) {
                Token what = parser.peek();
                String word = parser.readIdentifier();
                if (!"cluster".equalsIgnoreCase(word) && !"oids".equalsIgnoreCase(word)) {
                    throw new ParseException("Expected CLUSTER or OIDS", what);
                }
                return new AlterTableStmt.SetStorageParams();
            }
            // Fall through; could be other SET variants, but for now error
            throw new ParseException("Unsupported ALTER TABLE SET action", parser.peek());
        }
        // RESET (storage_parameter, ...): no-op for in-memory database
        if (parser.matchKeyword("RESET")) {
            if (parser.check(TokenType.LEFT_PAREN)) {
                DdlTableParser.consumeUntilParen(parser);
                return new AlterTableStmt.SetStorageParams();
            }
        }
        if (parser.matchKeyword("OWNER")) {
            parser.expectKeyword("TO");
            String newOwner = parser.readIdentifier();
            return new AlterTableStmt.OwnerTo(newOwner);
        }
        if (parser.matchKeyword("FORCE")) {
            parser.expectKeyword("ROW");
            parser.expectKeyword("LEVEL");
            parser.expectKeyword("SECURITY");
            return new AlterTableStmt.ForceRls();
        }
        if (parser.matchKeywords("NO", "FORCE")) {
            parser.expectKeyword("ROW");
            parser.expectKeyword("LEVEL");
            parser.expectKeyword("SECURITY");
            return new AlterTableStmt.NoForceRls();
        }
        if (parser.matchKeyword("ENABLE")) {
            if (parser.matchKeyword("ROW")) {
                parser.expectKeyword("LEVEL");
                parser.expectKeyword("SECURITY");
                return new AlterTableStmt.EnableRls();
            }
            // ENABLE TRIGGER / ENABLE REPLICA TRIGGER / ENABLE ALWAYS TRIGGER
            parser.matchKeyword("REPLICA");
            parser.matchKeyword("ALWAYS");
            parser.expectKeyword("TRIGGER");
            String trigName = parser.readIdentifier(); // trigger name or ALL
            return new AlterTableStmt.EnableTrigger(trigName);
        }
        if (parser.matchKeyword("DISABLE")) {
            if (parser.matchKeyword("ROW")) {
                parser.expectKeyword("LEVEL");
                parser.expectKeyword("SECURITY");
                return new AlterTableStmt.DisableRls();
            }
            // DISABLE TRIGGER
            parser.expectKeyword("TRIGGER");
            String trigName = parser.readIdentifier(); // trigger name or ALL
            return new AlterTableStmt.DisableTrigger(trigName);
        }
        if (parser.matchKeywords("ATTACH", "PARTITION")) {
            return parseAttachPartition();
        }
        if (parser.matchKeywords("DETACH", "PARTITION")) {
            String detachSchema = null;
            String partName = parser.readIdentifier();
            if (parser.match(TokenType.DOT)) { detachSchema = partName; partName = parser.readIdentifier(); }
            parser.matchKeyword("CONCURRENTLY"); // optional
            return new AlterTableStmt.DetachPartition(detachSchema, partName);
        }
        if (parser.matchKeyword("INHERIT")) {
            String parentName = parser.readIdentifier();
            return new AlterTableStmt.Inherit(parentName);
        }
        if (parser.matchKeywords("NO", "INHERIT")) {
            String parentName = parser.readIdentifier();
            return new AlterTableStmt.NoInherit(parentName);
        }
        if (parser.matchKeyword("VALIDATE")) {
            parser.expectKeyword("CONSTRAINT");
            String constraintName = parser.readIdentifier();
            return new AlterTableStmt.ValidateConstraint(constraintName);
        }
        // REPLICA IDENTITY { DEFAULT | USING INDEX indexname | FULL | NOTHING }
        if (parser.peek().value().equalsIgnoreCase("REPLICA")
                && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).value().equalsIgnoreCase("IDENTITY")) {
            parser.advance(); // consume REPLICA
            parser.advance(); // consume IDENTITY
            char identity;
            if (parser.matchKeyword("USING")) {
                parser.expectKeyword("INDEX");
                parser.readIdentifier(); // index name
                identity = 'i';
            } else if (parser.matchKeyword("FULL")) {
                identity = 'f';
            } else if (parser.matchKeyword("NOTHING")) {
                identity = 'n';
            } else if (parser.matchKeyword("DEFAULT")) {
                identity = 'd';
            } else {
                identity = 'd';
            }
            return new AlterTableStmt.SetReplicaIdentity(identity);
        }
        if (parser.matchKeywords("CLUSTER", "ON")) {
            parser.readIdentifier(); // index name, no-op for in-memory db
            return new AlterTableStmt.RenameTable(null);
        }

        throw new ParseException("Unsupported ALTER TABLE action", parser.peek());
    }

    /**
     * ALTER CONSTRAINT name followed by a constraint attribute list. The attributes are a set,
     * not a sequence: PostgreSQL rejects a list that names the same property twice with
     * different values, and rejects INITIALLY DEFERRED alongside NOT DEFERRABLE because the two
     * contradict each other.
     */
    private AlterTableStmt.AlterAction parseAlterConstraint() {
        String constraintName = parser.readIdentifier();
        Boolean deferrable = null;
        Boolean initiallyDeferred = null;
        Boolean enforced = null;
        boolean inheritability = false;
        while (true) {
            if (parser.matchKeyword("DEFERRABLE")) {
                deferrable = conflictFree(deferrable, Boolean.TRUE);
            } else if (parser.matchKeywords("NOT", "DEFERRABLE")) {
                deferrable = conflictFree(deferrable, Boolean.FALSE);
            } else if (parser.matchKeyword("INITIALLY")) {
                Boolean next = parser.matchKeyword("DEFERRED") ? Boolean.TRUE : Boolean.FALSE;
                if (!next) parser.matchKeyword("IMMEDIATE");
                initiallyDeferred = conflictFree(initiallyDeferred, next);
            } else if (parser.matchKeyword("ENFORCED")) {
                enforced = conflictFree(enforced, Boolean.TRUE);
            } else if (parser.matchKeywords("NOT", "ENFORCED")) {
                enforced = conflictFree(enforced, Boolean.FALSE);
            } else if (parser.matchKeywords("NO", "INHERIT") || parser.matchKeyword("INHERIT")) {
                inheritability = true;
            } else if (parser.matchKeywords("NOT", "VALID")) {
                throw new MemgresException("constraints cannot be altered to be NOT VALID", "0A000");
            } else {
                break;
            }
        }
        if (Boolean.TRUE.equals(initiallyDeferred)) {
            if (Boolean.FALSE.equals(deferrable)) {
                throw new MemgresException("constraint declared INITIALLY DEFERRED must be DEFERRABLE", "42601");
            }
            deferrable = Boolean.TRUE; // INITIALLY DEFERRED implies DEFERRABLE, as in PG's grammar
        }
        if (Boolean.FALSE.equals(deferrable)) {
            initiallyDeferred = Boolean.FALSE;
        }
        return new AlterTableStmt.AlterConstraintAttrs(constraintName, deferrable,
                initiallyDeferred, enforced, inheritability);
    }

    private static Boolean conflictFree(Boolean current, Boolean next) {
        if (current != null && !current.equals(next)) {
            throw new MemgresException("conflicting constraint properties", "42601");
        }
        return next;
    }

    AlterTableStmt.AlterColumnAction parseAlterColumnAction() {
        if (parser.matchKeywords("SET", "NOT", "NULL")) return new AlterTableStmt.SetNotNull();
        if (parser.matchKeywords("DROP", "NOT", "NULL")) return new AlterTableStmt.DropNotNull();
        if (parser.matchKeywords("SET", "DEFAULT")) return new AlterTableStmt.SetDefault(parser.parseExpression());
        if (parser.matchKeywords("DROP", "DEFAULT")) return new AlterTableStmt.DropDefault();
        if (parser.matchKeyword("TYPE") || parser.matchKeywords("SET", "DATA", "TYPE")) {
            String typeName = parser.parseTypeName();
            // A collation only exists for the collatable types, so naming one for any other type
            // is a contradiction PG refuses rather than silently ignoring.
            if (parser.matchKeyword("COLLATE")) {
                com.memgres.engine.DdlDefinitionChecks.rejectUncollatableType(typeName);
                String collation = parser.readIdentifier();
                if (parser.match(TokenType.DOT)) collation = collation + "." + parser.readIdentifier();
                ExpressionParser.validateCollationStatic(collation, parser.peek());
            }
            // Capture optional USING clause for data conversion
            Expression usingExpr = null;
            if (parser.matchKeyword("USING")) usingExpr = parser.parseExpression();
            return new AlterTableStmt.SetType(typeName, usingExpr);
        }
        // SET STATISTICS n / SET STORAGE type: planner hints, no-op
        if (parser.checkKeyword("SET") && parser.pos + 1 < parser.tokens.size()) {
            String nextVal = parser.tokens.get(parser.pos + 1).value().toUpperCase();
            if (nextVal.equals("STATISTICS")) { parser.advance(); parser.advance(); int target = Integer.parseInt(parser.advance().value()); return new AlterTableStmt.SetStatistics(target); }
            if (nextVal.equals("STORAGE")) { parser.advance(); parser.advance(); String storageType = parser.readIdentifier(); return new AlterTableStmt.SetStorage(storageType); }
            // SET GENERATED ALWAYS / SET GENERATED BY DEFAULT: change identity mode
            if (nextVal.equals("GENERATED")) {
                parser.advance(); // consume SET
                parser.advance(); // consume GENERATED
                boolean byDefault = parser.matchKeyword("BY");
                if (byDefault) parser.matchKeyword("DEFAULT");
                else parser.matchKeyword("ALWAYS");
                String marker = byDefault ? "__identity__:bydefault" : "__identity__:always";
                return new AlterTableStmt.SetDefault(new FunctionCallExpr("nextval",
                        Cols.listOf(Literal.ofString(marker))));
            }
            // SET INCREMENT BY n: modify identity sequence increment
            if (nextVal.equals("INCREMENT")) {
                parser.advance(); // consume SET
                parser.advance(); // consume INCREMENT
                parser.matchKeyword("BY"); // optional BY
                long incVal = Long.parseLong(parser.advance().value());
                return new AlterTableStmt.SetDefault(new FunctionCallExpr("nextval",
                        Cols.listOf(Literal.ofString("__set_increment__:" + incVal))));
            }
            // SET START WITH / SET MINVALUE / SET MAXVALUE / SET CYCLE / SET CACHE, no-op
            if (nextVal.equals("START") || nextVal.equals("MINVALUE") || nextVal.equals("MAXVALUE")
                    || nextVal.equals("CYCLE") || nextVal.equals("CACHE")) {
                parser.advance(); consumeUntilEndOfAction(); return new AlterTableStmt.ColumnNoOp();
            }
        }
        // ADD GENERATED [ALWAYS|BY DEFAULT] AS IDENTITY [(sequence_options)]
        if (parser.matchKeywords("ADD", "GENERATED")) {
            return parseAddGenerated();
        }
        if (parser.matchKeyword("ADD")) {
            consumeUntilEndOfAction();
            return new AlterTableStmt.ColumnNoOp();
        }
        // DROP IDENTITY [IF EXISTS]: remove identity
        if (parser.matchKeywords("DROP", "IDENTITY")) {
            return new AlterTableStmt.DropIdentity(parser.matchKeywords("IF", "EXISTS"));
        }
        // DROP EXPRESSION [IF EXISTS]: turn a stored generated column into an ordinary one.
        // EXPRESSION is not reserved, so it arrives as a plain identifier.
        if (parser.checkKeyword("DROP") && parser.pos + 1 < parser.tokens.size()
                && "EXPRESSION".equalsIgnoreCase(parser.tokens.get(parser.pos + 1).value())) {
            parser.advance();
            parser.advance();
            return new AlterTableStmt.DropExpression(parser.matchKeywords("IF", "EXISTS"));
        }
        // RESTART [WITH n]: identity restart
        if (parser.matchKeyword("RESTART")) {
            if (parser.matchKeyword("WITH")) {
                long restartVal = Long.parseLong(parser.advance().value());
                return new AlterTableStmt.SetDefault(new FunctionCallExpr("nextval",
                        Cols.listOf(Literal.ofString("__restart__:" + restartVal))));
            }
            return new AlterTableStmt.SetDefault(new FunctionCallExpr("nextval",
                    Cols.listOf(Literal.ofString("__restart__"))));
        }
        // SET COMPRESSION method: no-op for in-memory database, just consume method name
        if (parser.checkKeyword("SET") && parser.pos + 1 < parser.tokens.size()
                && parser.tokens.get(parser.pos + 1).value().toUpperCase().equals("COMPRESSION")) {
            parser.advance(); // SET
            parser.advance(); // COMPRESSION
            String method = parser.readIdentifier(); // pglz, lz4, default
            return new AlterTableStmt.SetCompression(method);
        }
        throw new ParseException("Unsupported ALTER COLUMN action", parser.peek());
    }

    private AlterTableStmt.AlterColumnAction parseAddGenerated() {
        boolean addAlways = parser.matchKeyword("ALWAYS");
        if (!addAlways) { parser.matchKeyword("BY"); parser.matchKeyword("DEFAULT"); }
        parser.matchKeyword("AS"); parser.matchKeyword("IDENTITY");
        // Parse optional sequence options in parens
        Long startWith = null;
        Long incrementBy = null;
        String seqName = null;
        if (parser.check(TokenType.LEFT_PAREN)) {
            parser.advance(); // consume (
            while (!parser.check(TokenType.RIGHT_PAREN) && !parser.isAtEnd()) {
                if (parser.matchKeywords("SEQUENCE", "NAME")) {
                    seqName = parser.readIdentifier();
                    if (parser.match(TokenType.DOT)) seqName = parser.readIdentifier(); // skip schema
                } else if (parser.matchKeywords("START", "WITH")) {
                    startWith = Long.parseLong(parser.advance().value());
                } else if (parser.matchKeywords("INCREMENT", "BY")) {
                    incrementBy = Long.parseLong(parser.advance().value());
                } else if (parser.matchKeyword("START")) {
                    startWith = Long.parseLong(parser.advance().value());
                } else if (parser.matchKeyword("INCREMENT")) {
                    incrementBy = Long.parseLong(parser.advance().value());
                } else {
                    parser.advance(); // skip unrecognized tokens
                }
            }
            parser.expect(TokenType.RIGHT_PAREN);
        }
        String identityInfo = addAlways ? "__identity__:add:always" : "__identity__:add:bydefault";
        if (startWith != null) identityInfo += ":start=" + startWith;
        if (incrementBy != null) identityInfo += ":increment=" + incrementBy;
        if (seqName != null) identityInfo += ":seqname=" + seqName;
        return new AlterTableStmt.SetDefault(new FunctionCallExpr("nextval",
                Cols.listOf(Literal.ofString(identityInfo))));
    }

    private AlterTableStmt.AlterAction parseAttachPartition() {
        String partSchema = null;
        String partName = parser.readIdentifier();
        if (parser.match(TokenType.DOT)) { partSchema = partName; partName = parser.readIdentifier(); }
        List<String> bounds = new ArrayList<>();
        if (parser.matchKeyword("DEFAULT")) {
            bounds.add("DEFAULT");
        } else if (parser.matchKeyword("FOR")) {
            parser.expectKeyword("VALUES");
            if (parser.matchKeyword("IN")) {
                parser.expect(TokenType.LEFT_PAREN);
                bounds.add("IN");
                do {
                    bounds.add(DdlParser.readValueOrMinMax(parser));
                } while (parser.match(TokenType.COMMA));
                parser.expect(TokenType.RIGHT_PAREN);
            } else if (parser.matchKeyword("FROM")) {
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
            } else if (parser.matchKeyword("WITH")) {
                // HASH partition: WITH (MODULUS m, REMAINDER r)
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
        if (bounds.isEmpty()) {
            // PG's grammar requires a bound spec here; without one there is nothing to route on.
            if (parser.isAtEnd() || parser.check(TokenType.SEMICOLON)) {
                throw com.memgres.engine.PgErrors.syntax("syntax error at end of input");
            }
            throw new ParseException("Expected partition bound specification", parser.peek());
        }
        return new AlterTableStmt.AttachPartition(partSchema, partName, bounds);
    }

    /** Consume tokens until the next comma (another ALTER action), semicolon, or EOF. Handles nested parens. */
    void consumeUntilEndOfAction() {
        int depth = 0;
        while (!parser.isAtEnd() && !parser.check(TokenType.SEMICOLON) && !parser.check(TokenType.EOF)) {
            if (parser.check(TokenType.LEFT_PAREN)) { depth++; parser.advance(); continue; }
            if (parser.check(TokenType.RIGHT_PAREN)) {
                if (depth == 0) break;
                depth--; parser.advance(); continue;
            }
            if (parser.check(TokenType.COMMA) && depth == 0) break;
            parser.advance();
        }
    }
}
