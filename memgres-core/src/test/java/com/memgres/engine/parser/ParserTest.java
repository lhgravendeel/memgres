package com.memgres.engine.parser;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.parser.ast.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the SQL parser. Validates AST structure for various SQL patterns.
 */
class ParserTest {

    // ---- SELECT ----

    @Test
    void shouldParseSimpleSelect() {
        Statement stmt = Parser.parse("SELECT 1");
        assertInstanceOf(SelectStmt.class, stmt);
        SelectStmt sel = (SelectStmt) stmt;
        assertNull(sel.from());
        assertEquals(1, sel.targets().size());
    }

    @Test
    void shouldParseSelectWithFrom() {
        Statement stmt = Parser.parse("SELECT name, age FROM users");
        SelectStmt sel = (SelectStmt) stmt;
        assertEquals(2, sel.targets().size());
        assertEquals(1, sel.from().size());
        assertInstanceOf(SelectStmt.TableRef.class, sel.from().get(0));
    }

    @Test
    void shouldParseSelectStar() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM users");
        assertInstanceOf(WildcardExpr.class, sel.targets().get(0).expr());
    }

    @Test
    void shouldParseSelectWithWhere() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM users WHERE id = 1");
        assertNotNull(sel.where());
        assertInstanceOf(BinaryExpr.class, sel.where());
        assertEquals(BinaryExpr.BinOp.EQUAL, ((BinaryExpr) sel.where()).op());
    }

    @Test
    void shouldParseCompoundWhere() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE a = 1 AND b > 2 OR c IS NULL");
        assertNotNull(sel.where());
        // OR is lowest precedence, so top-level should be OR
        assertInstanceOf(BinaryExpr.class, sel.where());
        assertEquals(BinaryExpr.BinOp.OR, ((BinaryExpr) sel.where()).op());
    }

    @Test
    void shouldParseOrderByLimitOffset() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t ORDER BY name DESC, id ASC LIMIT 10 OFFSET 5");
        assertEquals(2, sel.orderBy().size());
        assertTrue(sel.orderBy().get(0).descending());
        assertFalse(sel.orderBy().get(1).descending());
        assertNotNull(sel.limit());
        assertNotNull(sel.offset());
    }

    @Test
    void shouldParseSelectDistinct() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT DISTINCT name FROM users");
        assertTrue(sel.distinct());
    }

    @Test
    void shouldParseTableAlias() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT u.name FROM users u");
        assertInstanceOf(SelectStmt.TableRef.class, sel.from().get(0));
        assertEquals("u", ((SelectStmt.TableRef) sel.from().get(0)).alias());
    }

    @Test
    void shouldParseColumnAlias() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT name AS full_name FROM users");
        assertEquals("full_name", sel.targets().get(0).alias());
    }

    // ---- Expression parsing ----

    @Test
    void shouldParseArithmetic() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT 1 + 2 * 3");
        Expression expr = sel.targets().get(0).expr();
        // Should be 1 + (2 * 3) due to precedence
        assertInstanceOf(BinaryExpr.class, expr);
        BinaryExpr add = (BinaryExpr) expr;
        assertEquals(BinaryExpr.BinOp.ADD, add.op());
        assertInstanceOf(BinaryExpr.class, add.right());
        assertEquals(BinaryExpr.BinOp.MULTIPLY, ((BinaryExpr) add.right()).op());
    }

    @Test
    void shouldParseTypeCast() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT '42'::integer");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(CastExpr.class, expr);
        assertEquals("integer", ((CastExpr) expr).typeName());
    }

    @Test
    void shouldParseJsonbCast() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT '{\"key\": \"value\"}'::jsonb");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(CastExpr.class, expr);
        assertEquals("jsonb", ((CastExpr) expr).typeName());
    }

    @Test
    void shouldParseFunctionCall() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT uuid_generate_v4()");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(FunctionCallExpr.class, expr);
        assertEquals("uuid_generate_v4", ((FunctionCallExpr) expr).name());
    }

    @Test
    void shouldParseFunctionWithArgs() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT coalesce(a, b, 'default')");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(FunctionCallExpr.class, expr);
        assertEquals(3, ((FunctionCallExpr) expr).args().size());
    }

    @Test
    void shouldParseCountStar() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT count(*) FROM t");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(FunctionCallExpr.class, expr);
        assertTrue(((FunctionCallExpr) expr).star());
    }

    @Test
    void shouldParseCaseExpression() {
        SelectStmt sel = (SelectStmt) Parser.parse(
                "SELECT CASE WHEN x > 0 THEN 'pos' WHEN x = 0 THEN 'zero' ELSE 'neg' END FROM t");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(CaseExpr.class, expr);
        CaseExpr c = (CaseExpr) expr;
        assertEquals(2, c.whenClauses().size());
        assertNotNull(c.elseExpr());
    }

    @Test
    void shouldParseInExpression() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE id IN (1, 2, 3)");
        assertInstanceOf(InExpr.class, sel.where());
        InExpr in = (InExpr) sel.where();
        assertEquals(3, in.values().size());
        assertFalse(in.negated());
    }

    @Test
    void shouldParseNotIn() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE id NOT IN (1, 2)");
        assertInstanceOf(InExpr.class, sel.where());
        assertTrue(((InExpr) sel.where()).negated());
    }

    @Test
    void shouldParseBetween() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE x BETWEEN 1 AND 10");
        assertInstanceOf(BetweenExpr.class, sel.where());
    }

    @Test
    void shouldParseIsNull() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE x IS NULL");
        assertInstanceOf(IsNullExpr.class, sel.where());
        assertFalse(((IsNullExpr) sel.where()).negated());
    }

    @Test
    void shouldParseIsNotNull() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE x IS NOT NULL");
        assertInstanceOf(IsNullExpr.class, sel.where());
        assertTrue(((IsNullExpr) sel.where()).negated());
    }

    @Test
    void shouldParseLike() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM t WHERE name LIKE '%test%'");
        assertInstanceOf(BinaryExpr.class, sel.where());
        assertEquals(BinaryExpr.BinOp.LIKE, ((BinaryExpr) sel.where()).op());
    }

    @Test
    void shouldParseNegation() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT -5");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(UnaryExpr.class, expr);
        assertEquals(UnaryExpr.UnaryOp.NEGATE, ((UnaryExpr) expr).op());
    }

    @Test
    void shouldParseBooleansAndNull() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT true, false, null");
        assertEquals(3, sel.targets().size());
    }

    @Test
    void shouldParseCastFunction() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT CAST(x AS integer) FROM t");
        assertInstanceOf(CastExpr.class, sel.targets().get(0).expr());
    }

    @Test
    void shouldParseStringConcatenation() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT 'hello' || ' ' || 'world'");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(BinaryExpr.class, expr);
        assertEquals(BinaryExpr.BinOp.CONCAT, ((BinaryExpr) expr).op());
    }

    // ---- INSERT ----

    @Test
    void shouldParseInsert() {
        Statement stmt = Parser.parse("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')");
        assertInstanceOf(InsertStmt.class, stmt);
        InsertStmt ins = (InsertStmt) stmt;
        assertEquals("users", ins.table());
        assertEquals(2, ins.columns().size());
        assertEquals(1, ins.values().size());
        assertEquals(2, ins.values().get(0).size());
    }

    @Test
    void shouldParseMultiRowInsert() {
        Statement stmt = Parser.parse("INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)");
        InsertStmt ins = (InsertStmt) stmt;
        assertEquals(3, ins.values().size());
    }

    @Test
    void shouldParseInsertWithCast() {
        Statement stmt = Parser.parse("INSERT INTO t (data) VALUES ('{\"a\":1}'::jsonb)");
        InsertStmt ins = (InsertStmt) stmt;
        assertInstanceOf(CastExpr.class, ins.values().get(0).get(0));
    }

    // ---- UPDATE ----

    @Test
    void shouldParseUpdate() {
        Statement stmt = Parser.parse("UPDATE users SET name = 'Bob' WHERE id = 1");
        assertInstanceOf(UpdateStmt.class, stmt);
        UpdateStmt upd = (UpdateStmt) stmt;
        assertEquals("users", upd.table());
        assertEquals(1, upd.setClauses().size());
        assertNotNull(upd.where());
    }

    // ---- DELETE ----

    @Test
    void shouldParseDelete() {
        Statement stmt = Parser.parse("DELETE FROM users WHERE id = 1");
        assertInstanceOf(DeleteStmt.class, stmt);
        DeleteStmt del = (DeleteStmt) stmt;
        assertEquals("users", del.table());
        assertNotNull(del.where());
    }

    // ---- CREATE TABLE ----

    @Test
    void shouldParseCreateTable() {
        Statement stmt = Parser.parse(
                "CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, email varchar(255))");
        assertInstanceOf(CreateTableStmt.class, stmt);
        CreateTableStmt ct = (CreateTableStmt) stmt;
        assertEquals("users", ct.name());
        assertEquals(3, ct.columns().size());
        assertEquals("id", ct.columns().get(0).name());
        assertTrue(ct.columns().get(0).primaryKey());
        assertTrue(ct.columns().get(1).notNull());
    }

    @Test
    void shouldParseCreateTableIfNotExists() {
        CreateTableStmt ct = (CreateTableStmt) Parser.parse(
                "CREATE TABLE IF NOT EXISTS t (id integer)");
        assertTrue(ct.ifNotExists());
    }

    @Test
    void shouldParseCreateTableWithDefault() {
        CreateTableStmt ct = (CreateTableStmt) Parser.parse(
                "CREATE TABLE t (id uuid DEFAULT gen_random_uuid(), name text)");
        assertEquals(2, ct.columns().size());
        assertNotNull(ct.columns().get(0).defaultExpr());
    }

    // ---- CREATE TYPE ----

    @Test
    void shouldParseCreateTypeEnum() {
        Statement stmt = Parser.parse("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
        assertInstanceOf(CreateTypeStmt.class, stmt);
        CreateTypeStmt ct = (CreateTypeStmt) stmt;
        assertEquals("mood", ct.name());
        assertEquals(Cols.listOf("sad", "ok", "happy"), ct.enumLabels());
    }

    // ---- CREATE FUNCTION ----

    @Test
    void shouldParseCreateFunction() {
        Statement stmt = Parser.parse(
                "CREATE FUNCTION my_func() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
        assertInstanceOf(CreateFunctionStmt.class, stmt);
        CreateFunctionStmt cf = (CreateFunctionStmt) stmt;
        assertEquals("my_func", cf.name());
        assertEquals("trigger", cf.returnType());
        assertEquals("plpgsql", cf.language());
    }

    // ---- CREATE TRIGGER ----

    @Test
    void shouldParseCreateTrigger() {
        Statement stmt = Parser.parse(
                "CREATE TRIGGER trg BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION my_func()");
        assertInstanceOf(CreateTriggerStmt.class, stmt);
        CreateTriggerStmt ct = (CreateTriggerStmt) stmt;
        assertEquals("trg", ct.name());
        assertEquals("BEFORE", ct.timing());
        assertEquals(Cols.listOf("INSERT"), ct.events());
        assertEquals("users", ct.table());
        assertEquals("my_func", ct.functionName());
    }

    // ---- Transaction ----

    @Test
    void shouldParseTransactions() {
        assertInstanceOf(TransactionStmt.class, Parser.parse("BEGIN"));
        assertInstanceOf(TransactionStmt.class, Parser.parse("COMMIT"));
        assertInstanceOf(TransactionStmt.class, Parser.parse("ROLLBACK"));
        assertInstanceOf(TransactionStmt.class, Parser.parse("START TRANSACTION"));
    }

    // ---- SET / DISCARD ----

    @Test
    void shouldParseSet() {
        assertInstanceOf(SetStmt.class, Parser.parse("SET search_path = public"));
        assertInstanceOf(SetStmt.class, Parser.parse("SET timezone TO 'UTC'"));
    }

    @Test
    void shouldParseDiscard() {
        assertInstanceOf(DiscardStmt.class, Parser.parse("DISCARD ALL"));
    }

    // ---- DROP ----

    @Test
    void shouldParseDropTable() {
        Statement stmt = Parser.parse("DROP TABLE IF EXISTS users CASCADE");
        assertInstanceOf(DropTableStmt.class, stmt);
        DropTableStmt dt = (DropTableStmt) stmt;
        assertTrue(dt.ifExists());
        assertTrue(dt.cascade());
    }

    @Test
    void shouldParseDropFunction() {
        assertInstanceOf(DropStmt.class, Parser.parse("DROP FUNCTION IF EXISTS my_func"));
    }

    @Test
    void shouldParseDropTrigger() {
        Statement stmt = Parser.parse("DROP TRIGGER my_trigger ON my_table");
        assertInstanceOf(DropStmt.class, stmt);
        assertEquals("my_table", ((DropStmt) stmt).onTable());
    }

    // ---- CREATE EXTENSION ----

    @Test
    void shouldParseCreateExtension() {
        Statement stmt = Parser.parse("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
        assertInstanceOf(CreateExtensionStmt.class, stmt);
        assertTrue(((CreateExtensionStmt) stmt).ifNotExists());
    }

    // ---- Multi-statement ----

    @Test
    void shouldParseMultipleStatements() {
        List<Statement> stmts = Parser.parseAll("SELECT 1; SELECT 2; SELECT 3");
        assertEquals(3, stmts.size());
    }

    // ---- Complex real-world queries ----

    @Test
    void shouldParseQualifiedColumnRef() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT u.name FROM users u WHERE u.id = 1");
        Expression target = sel.targets().get(0).expr();
        assertInstanceOf(ColumnRef.class, target);
        ColumnRef ref = (ColumnRef) target;
        assertEquals("u", ref.table());
        assertEquals("name", ref.column());
    }

    @Test
    void shouldParseSchemaQualifiedTable() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT * FROM public.users");
        SelectStmt.TableRef ref = (SelectStmt.TableRef) sel.from().get(0);
        assertEquals("public", ref.schema());
        assertEquals("users", ref.table());
    }

    @Test
    void shouldParseJsonArrow() {
        SelectStmt sel = (SelectStmt) Parser.parse("SELECT data->>'name' FROM t");
        Expression expr = sel.targets().get(0).expr();
        assertInstanceOf(BinaryExpr.class, expr);
        assertEquals(BinaryExpr.BinOp.JSON_ARROW_TEXT, ((BinaryExpr) expr).op());
    }

    @Test
    void shouldParseSQLComment() {
        Statement stmt = Parser.parse("-- this is a comment\nSELECT 1");
        assertInstanceOf(SelectStmt.class, stmt);
    }

    @Test
    void shouldParseBlockComment() {
        Statement stmt = Parser.parse("/* block comment */ SELECT /* inner */ 1");
        assertInstanceOf(SelectStmt.class, stmt);
    }

    @Test
    void shouldParseAlterTable() {
        Statement stmt = Parser.parse("ALTER TABLE users ADD COLUMN age integer");
        assertInstanceOf(AlterTableStmt.class, stmt);
        AlterTableStmt alt = (AlterTableStmt) stmt;
        assertEquals(1, alt.actions().size());
        assertInstanceOf(AlterTableStmt.AddColumn.class, alt.actions().get(0));
    }

    @Test
    void shouldParseTruncate() {
        Statement stmt = Parser.parse("TRUNCATE TABLE users");
        assertInstanceOf(TruncateStmt.class, stmt);
    }
}
