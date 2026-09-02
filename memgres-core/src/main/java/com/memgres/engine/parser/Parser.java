package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;
import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Recursive-descent SQL parser. Consumes tokens from the Lexer and produces AST nodes.
 * Extends ExpressionParser which provides token navigation utilities and expression parsing.
 * Delegates statement parsing to specialized parsers: SelectParser, DmlParser, DdlParser, UtilityParser.
 */
public class Parser extends ExpressionParser {

    /**
     * Marks a partition bound that was written as an expression rather than a literal. The text
     * after the marker is the expression's source; whoever creates the partition evaluates it,
     * because only the engine can say what {@code abs(-1)} is worth or that {@code count(1)} is
     * an aggregate.
     */
    public static final String BOUND_EXPRESSION_MARKER = "__bound_expr__:";

    private final SelectParser selectParser;
    private final DmlParser dmlParser;
    private final DdlParser ddlParser;
    private final UtilityParser utilityParser;

    public Parser(List<Token> tokens) {
        super(tokens);
        this.selectParser = new SelectParser(this);
        this.dmlParser = new DmlParser(this);
        this.ddlParser = new DdlParser(this);
        this.utilityParser = new UtilityParser(this);
    }

    /** Parse a standalone expression (e.g. for generated column or CHECK constraint validation). */
    public static Expression parseExpression(String expr) {
        return parseExpression(expr, null);
    }

    /**
     * @param typeSchemas filled, when not null, with the schema every qualified type name in the
     *        expression was written under. A definition kept as text and read back later goes
     *        through here rather than through {@link #parse}, so this is where its qualifiers are.
     */
    public static Expression parseExpression(String expr, List<String> typeSchemas) {
        Lexer lexer = new Lexer(expr);
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        Expression parsed = parser.parseExpression();
        if (typeSchemas != null) typeSchemas.addAll(parser.typeSchemaQualifiers());
        return parsed;
    }

    public static Statement parse(String sql) {
        return parse(sql, null);
    }

    /**
     * @param typeSchemas filled, when not null, with the schema every qualified type name in the
     *        statement was written under, leftmost first. A qualifier is the one part of a type
     *        name the parser does not keep, and it still has to name a schema that exists.
     */
    public static Statement parse(String sql, List<String> typeSchemas) {
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        Statement stmt = parser.parseStatement();
        if (typeSchemas != null) typeSchemas.addAll(parser.typeSchemaQualifiers());
        // Check for unexpected trailing tokens (after skipping semicolons)
        parser.skipSemicolons();
        if (!parser.isAtEnd()) {
            Token t = parser.peek();
            // Always reject unmatched closing parens
            if (t.type() == TokenType.RIGHT_PAREN) {
                throw new ParseException("syntax error at or near \")\"", t);
            }
            // For SELECT and INSERT, reject trailing identifiers/literals. These indicate
            // typos or garbage after the statement. We limit this to SELECT/INSERT because
            // other statements (CREATE FUNCTION, BEGIN, ALTER TABLE) may have trailing tokens
            // that the parser doesn't fully consume (LEAKPROOF, READ WRITE, NOT VALID, etc.)
            if (stmt instanceof SelectStmt || stmt instanceof InsertStmt || stmt instanceof SetOpStmt
                    || stmt instanceof UpdateStmt || stmt instanceof DeleteStmt
                    || stmt instanceof MergeStmt
                    || stmt instanceof UnlistenStmt || stmt instanceof NotifyStmt
                    || stmt instanceof ListenStmt || stmt instanceof ExplainStmt) {
                if (t.type() == TokenType.ERROR) {
                    // Bare $ is a syntax error (unterminated parameter reference)
                    if ("$".equals(t.value())) {
                        throw new ParseException("syntax error at or near \"" + t.value() + "\"", t);
                    }
                    // Single ! is a syntax error (42601)
                    if ("!".equals(t.value())) {
                        throw new ParseException("syntax error at or near \"!\"", t);
                    }
                    // !! can be the tsquery NOT prefix operator; skip explicit error here
                    // so the expression parser can handle it
                    throw new ParseException("operator does not exist: " + t.value(), t, "42883");
                }
                if (t.type() == TokenType.IDENTIFIER
                        || t.type() == TokenType.INTEGER_LITERAL
                        || t.type() == TokenType.FLOAT_LITERAL
                        || t.type() == TokenType.STRING_LITERAL
                        || t.type() == TokenType.KEYWORD
                        || t.type() == TokenType.DOT
                        || t.type() == TokenType.DOT_DOT
                        // A parameter left standing after the statement is as much garbage as a
                        // leftover identifier. Dropping it silently accepted "WHERE jb ? ?",
                        // where a driver's second placeholder turns the jsonb existence operator
                        // into a parameter and leaves the first one with nothing to do.
                        || t.type() == TokenType.PARAM
                        || t.type() == TokenType.STAR) {
                    throw new ParseException("syntax error at or near \"" + t.value() + "\"", t);
                }
            }
        }
        return stmt;
    }

    public static List<Statement> parseAll(String sql) {
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();
        Parser parser = new Parser(tokens);
        List<Statement> stmts = new ArrayList<>();
        while (!parser.isAtEnd()) {
            parser.skipSemicolons();
            if (parser.isAtEnd()) break;
            Statement s = parser.parseStatement();
            if (s != null) stmts.add(s);
            parser.skipSemicolons();
        }
        return stmts;
    }

    // ---- Statement parsing ----

    public Statement parseStatement() {
        Token t = peek();
        // Handle empty input (only comments/whitespace)
        if (t.type() == TokenType.EOF) {
            return null;
        }
        if (t.type() == TokenType.KEYWORD) {
            Statement stmt;
            switch (t.value()) {
                case "SELECT":
                    stmt = selectParser.parseSelectFull();
                    break;
                case "WITH":
                    stmt = selectParser.parseWithStatement();
                    break;
                case "VALUES":
                    stmt = selectParser.parseValues();
                    break;
                case "TABLE":
                    // Read as a query rather than as a bare relation name, so the ORDER BY and
                    // LIMIT PostgreSQL allows after one are read here too.
                    stmt = selectParser.parseSelectFull();
                    break;
                case "INSERT":
                    stmt = dmlParser.parseInsert();
                    break;
                case "UPDATE":
                    stmt = dmlParser.parseUpdate();
                    break;
                case "DELETE":
                    stmt = dmlParser.parseDelete();
                    break;
                case "MERGE":
                    stmt = dmlParser.parseMerge();
                    break;
                case "CREATE":
                    stmt = ddlParser.parseCreate();
                    break;
                case "DROP":
                    stmt = ddlParser.parseDrop();
                    break;
                case "ALTER":
                    stmt = ddlParser.parseAlter();
                    break;
                case "TRUNCATE":
                    stmt = ddlParser.parseTruncate();
                    break;
                case "CALL":
                    stmt = ddlParser.parseCall();
                    break;
                case "REFRESH":
                    stmt = ddlParser.parseRefreshMaterializedView();
                    break;
                case "SET":
                    stmt = utilityParser.parseSet();
                    break;
                case "DISCARD":
                    stmt = utilityParser.parseDiscard();
                    break;
                case "BEGIN":
                case "START":
                    stmt = utilityParser.parseTransactionBegin();
                    break;
                case "COMMIT":
                case "END":
                    stmt = utilityParser.parseTransactionCommit();
                    break;
                case "ROLLBACK":
                case "ABORT":
                    stmt = utilityParser.parseTransactionRollback();
                    break;
                case "SAVEPOINT":
                    stmt = utilityParser.parseSavepoint();
                    break;
                case "RELEASE":
                    stmt = utilityParser.parseReleaseSavepoint();
                    break;
                case "EXPLAIN":
                    stmt = utilityParser.parseExplain();
                    break;
                case "COPY":
                    stmt = utilityParser.parseCopy();
                    break;
                case "LISTEN":
                    stmt = utilityParser.parseListen();
                    break;
                case "NOTIFY":
                    stmt = utilityParser.parseNotify();
                    break;
                case "UNLISTEN":
                    stmt = utilityParser.parseUnlisten();
                    break;
                case "PREPARE":
                    stmt = utilityParser.parsePrepare();
                    break;
                case "EXECUTE":
                    stmt = utilityParser.parseExecuteStmt();
                    break;
                case "DEALLOCATE":
                    stmt = utilityParser.parseDeallocate();
                    break;
                case "DECLARE":
                    stmt = utilityParser.parseDeclareCursor();
                    break;
                case "FETCH":
                    stmt = utilityParser.parseFetchOrMove(false);
                    break;
                case "MOVE":
                    stmt = utilityParser.parseFetchOrMove(true);
                    break;
                case "CLOSE":
                    stmt = utilityParser.parseClose();
                    break;
                case "LOCK":
                    stmt = utilityParser.parseLock();
                    break;
                case "GRANT":
                    stmt = utilityParser.parseGrant();
                    break;
                case "REVOKE":
                    stmt = utilityParser.parseRevoke();
                    break;
                case "REASSIGN":
                    stmt = utilityParser.parseReassign();
                    break;
                case "DO":
                    stmt = utilityParser.parseDo();
                    break;
                case "RESET":
                    stmt = utilityParser.parseReset();
                    break;
                case "SHOW":
                    stmt = utilityParser.parseShow();
                    break;
                case "COMMENT":
                    stmt = utilityParser.parseComment();
                    break;
                case "SECURITY":
                    stmt = utilityParser.parseSecurityLabel();
                    break;
                case "ANALYZE":
                case "ANALYSE":
                    stmt = utilityParser.parseAnalyze();
                    break;
                case "VACUUM":
                    stmt = utilityParser.parseVacuum();
                    break;
                case "REINDEX":
                    stmt = utilityParser.parseReindex();
                    break;
                case "CLUSTER":
                    stmt = utilityParser.parseCluster();
                    break;
                case "CHECKPOINT":
                    stmt = utilityParser.parseCheckpoint();
                    break;
                case "LOAD":
                    stmt = utilityParser.parseLoad();
                    break;
                case "IMPORT":
                    stmt = utilityParser.parseImport();
                    break;
                default:
                    throw new ParseException("Unsupported statement", t);
            }
            // Check for set operations (UNION, INTERSECT, EXCEPT)
            return selectParser.tryParseSetOp(stmt);
        }
        // Parenthesized statement: (SELECT ...) UNION/EXCEPT/INTERSECT ...
        if (t.type() == TokenType.LEFT_PAREN) {
            advance();
            Statement inner = parseStatement();
            expect(TokenType.RIGHT_PAREN);
            return selectParser.tryParseSetOp(inner);
        }
        throw new ParseException("Expected SQL statement", t);
    }

    private static final Set<String> STATEMENT_STARTERS = Cols.setOf(
            "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "TRUNCATE", "SET", "DISCARD", "BEGIN", "START", "COMMIT", "END", "ROLLBACK",
            "ABORT", "SAVEPOINT", "RELEASE", "EXPLAIN", "COPY", "CALL", "LISTEN", "NOTIFY",
            "UNLISTEN", "REFRESH", "VALUES", "MERGE", "PREPARE", "EXECUTE", "DEALLOCATE",
            "DECLARE", "FETCH", "MOVE", "CLOSE", "LOCK", "GRANT", "REVOKE", "REASSIGN",
            "DO", "RESET", "SHOW", "COMMENT", "SECURITY", "ANALYZE", "ANALYSE", "VACUUM",
            "REINDEX", "CLUSTER", "CHECKPOINT", "LOAD", "IMPORT", "TABLE"
    );

    // ---- Overrides from ExpressionParser ----

    @Override
    protected SelectStmt parseSelect() {
        Statement result = selectParser.parseSelectFull();
        if (result instanceof SelectStmt) return ((SelectStmt) result);
        // If SELECT INTO was parsed, this shouldn't happen in subquery context
        throw new ParseException("SELECT INTO not allowed in subquery context", peek());
    }

    @Override
    protected Statement parseSubqueryWithSetOps() {
        Statement result;
        if (checkKeyword("VALUES")) {
            result = selectParser.parseValuesBody();
            // A row lock is taken on a row of a table, and a VALUES list has none, so there is
            // nothing here for FOR UPDATE to lock.
            if (checkKeyword("FOR")) {
                throw new MemgresException("FOR UPDATE cannot be applied to VALUES", "0A000");
            }
        } else {
            result = parseSelect();
        }
        return selectParser.tryParseSetOp(result);
    }

    /** Parse any query form -- SELECT, WITH or VALUES -- including set operations. */
    Statement parseSubqueryWithSetOpsPublic() {
        return parseSubqueryWithSetOps();
    }

    // ---- Forwarding methods for cross-delegate calls ----

    Statement tryParseSetOp(Statement left) {
        return selectParser.tryParseSetOp(left);
    }

    /** Parse one operand of a set operation, taking off any parentheses around it. */
    Statement parseSetOpOperandPublic() {
        return selectParser.parseSetOpOperandPublic();
    }

    List<SelectStmt.FromItem> parseFromList() {
        return selectParser.parseFromList();
    }

    SelectStmt.FromItem parseFromItem() {
        return selectParser.parseFromItem();
    }

    List<SelectStmt.SelectTarget> parseSelectTargets() {
        return selectParser.parseSelectTargets();
    }

    boolean isKeywordValidAsBareAlias() {
        return selectParser.isKeywordValidAsBareAlias();
    }

    Statement parseValuesBody() {
        return selectParser.parseValuesBody();
    }

    Expression parseLimitOffsetExpr() {
        return selectParser.parseLimitOffsetExpr();
    }

    InsertStmt parseInsert() {
        return dmlParser.parseInsert();
    }

    InsertStmt parseInsert(List<SelectStmt.CommonTableExpr> withClauses) {
        return dmlParser.parseInsert(withClauses);
    }

    UpdateStmt parseUpdate() {
        return dmlParser.parseUpdate();
    }

    UpdateStmt parseUpdate(List<SelectStmt.CommonTableExpr> withClauses) {
        return dmlParser.parseUpdate(withClauses);
    }

    DeleteStmt parseDelete() {
        return dmlParser.parseDelete();
    }

    DeleteStmt parseDelete(List<SelectStmt.CommonTableExpr> withClauses) {
        return dmlParser.parseDelete(withClauses);
    }

    MergeStmt parseMerge(List<SelectStmt.CommonTableExpr> withClauses) {
        return dmlParser.parseMerge(withClauses);
    }

    List<InsertStmt.SetClause> parseSetClauses() {
        return dmlParser.parseSetClauses();
    }

    void consumeUntilParen() {
        ddlParser.consumeUntilParen();
    }
}
