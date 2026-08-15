package com.memgres.engine;

import com.memgres.engine.parser.ast.AnyAllArrayExpr;
import com.memgres.engine.parser.ast.AnyAllExpr;
import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.ArraySliceExpr;
import com.memgres.engine.parser.ast.ArraySubqueryExpr;
import com.memgres.engine.parser.ast.AtTimeZoneExpr;
import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.CollateExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.CustomOperatorExpr;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FieldAccessExpr;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.IsBooleanExpr;
import com.memgres.engine.parser.ast.IsJsonExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.LikeExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.NamedArgExpr;
import com.memgres.engine.parser.ast.OrderedSetAggExpr;
import com.memgres.engine.parser.ast.QualifiedOperatorExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.SubscriptExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

/**
 * Reading a stored expression the way PostgreSQL reads one, and refusing it for the first thing
 * that is wrong with it.
 *
 * <p>PostgreSQL transforms a CHECK, a generation expression, a DEFAULT or an index predicate as it
 * walks it, left to right, settling every name and every call at the node where it stands. So
 * which fault a definition is refused for follows from where the faults were <em>written</em>:
 * {@code CHECK (nosuchcol > 0 AND (SELECT true))} is a column that does not exist and the same two
 * the other way round are a sub-query in a check constraint. Running the checks one after another
 * over the whole expression instead put whichever check happened to run first in front, and the
 * writer was told about the second fault.
 *
 * <p>The reading hands the expression over the moment it reaches something a later check words for
 * itself — a sub-query, an aggregate, a window call, a set-returning call — so that check goes on
 * saying exactly what it said before. Only a name or a call written <em>before</em> one of those is
 * reported here.
 *
 * <p>It reads every shape that holds an expression inside it, each part in the order it was
 * written, because that is the order the faults in it are reported in. Where the expression holds
 * something it cannot see through it reads nothing at all and leaves the whole decision to the
 * checks that follow, because a fault it could not see may well have been written first.
 */
final class StoredExprNames {

    private final DdlExecutor ddl;
    private final Table table;
    private final String newColumnName;
    private final boolean systemColumnsResolve;
    private final boolean qualifiersNameRelation;

    private StoredExprNames(DdlExecutor ddl, Table table, String newColumnName,
                            boolean systemColumnsResolve, boolean qualifiersNameRelation) {
        this.ddl = ddl;
        this.table = table;
        this.newColumnName = newColumnName;
        this.systemColumnsResolve = systemColumnsResolve;
        this.qualifiersNameRelation = qualifiersNameRelation;
    }

    /**
     * Read {@code expr} against the relation the definition is being stored on, raising the first
     * name or call PostgreSQL would refuse. A null relation is a definition with no row in scope —
     * a DEFAULT — where only the calls are judged.
     */
    static void read(DdlExecutor ddl, Expression expr, Table table, String newColumnName,
                     boolean systemColumnsResolve, boolean qualifiersNameRelation) {
        if (expr == null) return;
        StoredExprNames reader = new StoredExprNames(ddl, table, newColumnName,
                systemColumnsResolve, qualifiersNameRelation);
        if (reader.firstHandOver(expr) != reader.walk(expr, false)) return;
        reader.walk(expr, true);
    }

    /**
     * Where the reading gets to along the shapes it walks, and what it judges on the way when
     * {@code raise} is set.
     *
     * @return the node it handed the expression over at, or null where it read the whole of it
     */
    private Object walk(Expression expr, boolean raise) {
        if (expr == null) return null;
        if (handsOver(expr)) return expr;
        if (expr instanceof ColumnRef) {
            if (raise && table != null) {
                ddl.validateExprColumnRefs(expr, table, newColumnName, systemColumnsResolve,
                        qualifiersNameRelation);
            }
            return null;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            Object stopped = walk(bin.left(), raise);
            return stopped != null ? stopped : walk(bin.right(), raise);
        }
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr op = (CustomOperatorExpr) expr;
            Object stopped = walk(op.left(), raise);
            return stopped != null ? stopped : walk(op.right(), raise);
        }
        if (expr instanceof UnaryExpr) return walk(((UnaryExpr) expr).operand(), raise);
        if (expr instanceof CastExpr) return walk(((CastExpr) expr).expr(), raise);
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) expr;
            // The arguments are transformed before the function is looked for: their types are
            // what the name is resolved with, so a column that is not there is reported first.
            if (call.args() != null) {
                for (Expression arg : call.args()) {
                    Object stopped = walk(arg, raise);
                    if (stopped != null) return stopped;
                }
            }
            if (raise) requireFunctionExists(call);
            return null;
        }
        // The shapes below hold their operands the way the ones above do, and PostgreSQL settles
        // a name written in one of them where it stands: a column named inside a CASE arm, an IN
        // list, a BETWEEN bound or an array constructor is refused for not being there exactly as
        // one written beside them is. Each is walked in the order it was written, because that is
        // the order the faults in it are reported in.
        if (expr instanceof CaseExpr) {
            CaseExpr branches = (CaseExpr) expr;
            Object stopped = walk(branches.operand(), raise);
            if (branches.whenClauses() != null) {
                for (CaseExpr.WhenClause when : branches.whenClauses()) {
                    if (stopped == null) stopped = walk(when.condition(), raise);
                    if (stopped == null) stopped = walk(when.result(), raise);
                }
            }
            return stopped != null ? stopped : walk(branches.elseExpr(), raise);
        }
        if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            Object stopped = walk(in.expr(), raise);
            if (in.values() != null) {
                for (Expression value : in.values()) {
                    if (stopped == null) stopped = walk(value, raise);
                }
            }
            return stopped;
        }
        if (expr instanceof BetweenExpr) {
            BetweenExpr range = (BetweenExpr) expr;
            Object stopped = walk(range.expr(), raise);
            if (stopped == null) stopped = walk(range.low(), raise);
            return stopped != null ? stopped : walk(range.high(), raise);
        }
        if (expr instanceof ArrayExpr) {
            Object stopped = null;
            List<Expression> elements = ((ArrayExpr) expr).elements();
            if (elements != null) {
                for (Expression element : elements) {
                    if (stopped == null) stopped = walk(element, raise);
                }
            }
            return stopped;
        }
        if (expr instanceof SubscriptExpr) {
            SubscriptExpr subscripted = (SubscriptExpr) expr;
            Object stopped = walk(subscripted.base(), raise);
            if (subscripted.subscripts() != null) {
                for (SubscriptExpr.Subscript one : subscripted.subscripts()) {
                    if (stopped == null) stopped = walk(one.lower(), raise);
                    if (stopped == null) stopped = walk(one.upper(), raise);
                }
            }
            return stopped;
        }
        if (expr instanceof ArraySliceExpr) {
            ArraySliceExpr slice = (ArraySliceExpr) expr;
            Object stopped = walk(slice.array(), raise);
            if (stopped == null) stopped = walk(slice.lower(), raise);
            return stopped != null ? stopped : walk(slice.upper(), raise);
        }
        if (expr instanceof LikeExpr) {
            LikeExpr like = (LikeExpr) expr;
            Object stopped = walk(like.left(), raise);
            return stopped != null ? stopped : walk(like.pattern(), raise);
        }
        if (expr instanceof AtTimeZoneExpr) {
            AtTimeZoneExpr zoned = (AtTimeZoneExpr) expr;
            Object stopped = walk(zoned.expr(), raise);
            return stopped != null ? stopped : walk(zoned.zone(), raise);
        }
        if (expr instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr anyAll = (AnyAllArrayExpr) expr;
            Object stopped = walk(anyAll.left(), raise);
            return stopped != null ? stopped : walk(anyAll.array(), raise);
        }
        if (expr instanceof IsNullExpr) return walk(((IsNullExpr) expr).expr(), raise);
        if (expr instanceof IsBooleanExpr) return walk(((IsBooleanExpr) expr).expr(), raise);
        if (expr instanceof IsJsonExpr) return walk(((IsJsonExpr) expr).expr(), raise);
        if (expr instanceof CollateExpr) return walk(((CollateExpr) expr).expr(), raise);
        if (expr instanceof FieldAccessExpr) return walk(((FieldAccessExpr) expr).expr(), raise);
        if (expr instanceof NamedArgExpr) return walk(((NamedArgExpr) expr).value(), raise);
        if (expr instanceof QualifiedOperatorExpr) {
            return walk(((QualifiedOperatorExpr) expr).inner(), raise);
        }
        return null;
    }

    /** The first node anywhere in the expression that a later check words for itself. */
    private Object firstHandOver(Object node) {
        if (node == null) return null;
        if (handsOver(node)) return node;
        final Object[] found = new Object[1];
        AstWalk.forEachChild(node, new Consumer<Object>() {
            @Override
            public void accept(Object child) {
                if (found[0] == null) found[0] = firstHandOver(child);
            }
        });
        return found[0];
    }

    /**
     * Whether some other check already has words of its own for this node. A sub-query, an
     * aggregate, a window call, a call carrying a clause only an aggregate may have and a
     * set-returning call are each refused elsewhere and named there in PostgreSQL's own terms.
     */
    private boolean handsOver(Object node) {
        if (node instanceof SelectStmt || node instanceof SetOpStmt
                || node instanceof SubqueryExpr || node instanceof ExistsExpr
                || node instanceof ArraySubqueryExpr || node instanceof AnyAllExpr
                || node instanceof WindowFuncExpr || node instanceof OrderedSetAggExpr) {
            return true;
        }
        if (!(node instanceof FunctionCallExpr)) return false;
        FunctionCallExpr call = (FunctionCallExpr) node;
        if (call.name() == null) return false;
        return ddl.executor.selectExecutor.isAggregateFunction(call.name())
                || call.filter() != null || call.orderBy() != null || call.distinct()
                || "grouping".equalsIgnoreCase(call.name())
                || ddl.executor.selectExecutor.containsSrf(call);
    }

    /**
     * {@code 42883} for a call naming no routine these arguments reach, and {@code 3F000} for one
     * written under a schema nothing answers to. PostgreSQL resolves a name together with the
     * types it was written over, and it does so where the definition is written rather than at the
     * first row that evaluates it, so nothing is ever stored calling something that could never be
     * called.
     */
    private void requireFunctionExists(FunctionCallExpr call) {
        String written = call.name();
        if (written == null) return;
        requireQualifierSchemaExists(written);
        String bare = FunctionEvaluator.stripSchemaPrefix(written.toLowerCase(Locale.ROOT));
        // A call of one argument whose name is a type is a cast written the other way round, and
        // PostgreSQL reads it as one when it finds no function of that name. It is the one
        // argument that makes it a cast: the same name over two arguments, or none, is a missing
        // function there as anywhere else.
        boolean namesType = call.args() != null && call.args().size() == 1
                && ddl.executor.functionEvaluator.coercibleTypeName(bare) != null;
        boolean readAsCast = namesType && castExists(call, bare);
        // A name still carrying a schema of its own names that schema's routines and no other's:
        // the built-ins are pg_catalog's, so only what a user declared in that schema answers to
        // it, and a type of that schema answers only as the cast the one-argument form is.
        if (bare.indexOf('.') >= 0) {
            if (readAsCast || namesDeclaredRoutine(bare)) return;
            MemgresException elsewhere = noSuchRoutine(call, written);
            if (elsewhere != null) throw elsewhere;
            return;
        }
        if (ddl.executor.database.getFunction(bare) != null) return;
        if (namesType) {
            // The cast is what such a call is, so where PostgreSQL has no such cast only a routine
            // of that name can answer for it — and a type name it declares no routine under has
            // nothing left to be. That is the whole of why date(a) over an integer column is
            // refused where date(a) over a timestamp column is stored.
            if (readAsCast) return;
            if (BuiltinCallTypes.records(bare)) {
                requireArgumentTypesTake(call, written, bare);
                return;
            }
            MemgresException uncast = noSuchRoutine(call, written);
            if (uncast != null) throw uncast;
            return;
        }
        if (BuiltinFunctionNames.isCallable(bare)) {
            requireArgumentTypesReach(call, written, bare);
            return;
        }
        if (call.args() != null && call.args().size() == 1
                && TypeNamespace.find(ddl.executor.database.typeKeys(), written) != null
                // A composite is the one such name a value cannot be read into: it has no input
                // function of its own, so PostgreSQL finds no cast to read the one-argument form
                // as and reports the routine of that name it does not have.
                && ddl.executor.database.getCompositeType(written) == null) {
            return;
        }
        MemgresException missing = noSuchRoutine(call, written);
        if (missing != null) throw missing;
    }

    /**
     * Whether the conversion a one-argument call on a type name asks for is one PostgreSQL has.
     *
     * <p>Such a call is a cast written the other way round, and a cast PostgreSQL does not declare
     * is no more a call than a routine it does not have: an integer reaches a date by no
     * conversion at all, while it reaches a bigint, a boolean and an oid by one each. The reading
     * is the engine's own reading of pg_cast, which answers only where it can say positively that
     * there is no path — so an argument this cannot type, a type carried as something else and a
     * type name it cannot resolve all leave the call as the cast it looks like.
     */
    private boolean castExists(FunctionCallExpr call, String bare) {
        DataType target = ddl.executor.functionEvaluator.typeNameCoercionResultType(bare, call);
        if (target == null) return true;
        String argType = BooleanContext.typeOf(call.args().get(0), BooleanContext.Types.of(table));
        DataType source = argType == null ? null : DataType.fromPgName(argType);
        // An argument nothing has typed is read by the target's input function, whatever it says.
        if (source == null) return true;
        return CastLegality.refusalFor(source, target) == null;
    }

    /**
     * Whether a user has declared a routine of this name in the schema the call writes it under.
     * A qualified call is looked for in that one schema, and the built-ins are pg_catalog's, so
     * nothing else answers to a name written under a schema of the user's.
     */
    private boolean namesDeclaredRoutine(String qualified) {
        int dot = qualified.lastIndexOf('.');
        String schema = qualified.substring(0, dot);
        String name = qualified.substring(dot + 1);
        if (ddl.executor.database.getFunction(schema, name) != null) return true;
        // A routine declared without naming a schema lives in public, which the database records
        // as no schema at all rather than as that name.
        for (PgFunction declared : ddl.executor.database.getFunctionOverloads(name)) {
            if (Database.schemaOf(declared).equalsIgnoreCase(schema)) return true;
        }
        PgFunction only = ddl.executor.database.getFunction(name);
        return only != null && Database.schemaOf(only).equalsIgnoreCase(schema);
    }

    /**
     * The complaint a call that resolves to nothing is refused with, or null where this engine
     * cannot name what the call was written over.
     *
     * <p>The complaint names the argument types the name was looked up with, which is how a reader
     * tells "no function of that name" from "none of that name taking these". A literal written
     * without a type of its own is still of type unknown and PostgreSQL says so; an argument this
     * engine cannot type at all leaves the call unjudged rather than named with a guess.
     */
    private MemgresException noSuchRoutine(FunctionCallExpr call, String written) {
        StringBuilder types = new StringBuilder();
        List<Expression> args = call.args();
        if (args != null) {
            for (Expression arg : args) {
                String type = BooleanContext.typeOf(arg, BooleanContext.Types.of(table));
                if (type == null) {
                    if (!(arg instanceof Literal)) return null;
                    type = "unknown";
                }
                if (types.length() > 0) types.append(", ");
                types.append(type);
            }
        }
        MemgresException e = new MemgresException(
                "function " + written + "(" + types + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types."
                + " You might need to add explicit type casts.");
        return e;
    }

    /**
     * {@code 3F000} for a call written under a schema nothing answers to.
     *
     * <p>A qualified call is looked for in that one schema and nowhere else, so PostgreSQL settles
     * the qualifier before it goes looking for anything of that name — and a definition naming a
     * schema that is not there is refused for the schema rather than stored to fail later. Only a
     * single qualifier is judged, and only where nothing at all answers to it: a schema of the
     * user's, one of the two the catalog supplies, or the session's own temporary one.
     */
    private void requireQualifierSchemaExists(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0 || name.indexOf('.', dot + 1) >= 0) return;
        String qualifier = name.substring(0, dot);
        String folded = qualifier.toLowerCase(Locale.ROOT);
        if ("pg_catalog".equals(folded) || "information_schema".equals(folded)) return;
        // The lexer folds an unquoted name and leaves a quoted one as it was written, so a schema
        // created under a spelling that still carries a capital answers to that spelling alone.
        if (ddl.executor.database.getSchema(qualifier) != null
                || ddl.executor.database.getSchema(folded) != null) {
            return;
        }
        String temporary = ddl.executor.session == null ? null
                : ddl.executor.session.getTempSchemaName();
        if (temporary != null && (temporary.equals(qualifier) || temporary.equals(folded))) return;
        throw new MemgresException("schema \"" + qualifier + "\" does not exist", "3F000");
    }

    /**
     * {@code 42883} for a name PostgreSQL declares something under, but nothing that takes these
     * argument types.
     *
     * <p>A name is resolved together with the types it was written over, so {@code lower(a)} over
     * an integer column is a function that does not exist rather than a call that will fail at the
     * first row it reaches — and PostgreSQL settles that where the definition is written. It is
     * judged against the same tables the call is judged against when it runs, so the same call
     * written into a definition and written into a query is answered the same way.
     *
     * <p>An argument whose type the statement does not settle leaves the whole call alone, as does
     * a name whose written argument list says nothing about which signature was meant — a call the
     * grammar spelled out, or one of the names that take any number of arguments. Refusing a call
     * on a guess is the worse of the two mistakes.
     */
    private void requireArgumentTypesReach(FunctionCallExpr call, String written, String bare) {
        if (FunctionEvaluator.acceptsAnyArity(bare)) return;
        requireArgumentTypesTake(call, written, bare);
    }

    /**
     * The same judgement for a call whose own shape says which signature was meant, whatever the
     * name would say on its own.
     *
     * <p>A type name written over one argument is such a call. point, box and the multirange
     * constructors take any number of arguments, so the count settles nothing for them — but the
     * one-argument form of each is the conversion the type does, and PostgreSQL refuses
     * {@code point(a)} over an integer column exactly as it refuses {@code date(a)} there.
     */
    private void requireArgumentTypesTake(FunctionCallExpr call, String written, String bare) {
        List<Expression> args = call.args();
        if (args == null || args.isEmpty()) return;
        if (call.star() || call.spelledInGrammar) return;
        if (!BuiltinCallTypes.records(bare)) return;
        BooleanContext.Types types = BooleanContext.Types.of(table);
        int[] oids = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            Expression arg = args.get(i);
            if (arg instanceof NamedArgExpr) return;
            String argType = BooleanContext.typeOf(arg, types);
            if (argType == null) {
                if (!isUntypedLiteral(arg)) return;
                oids[i] = BuiltinCallTypes.UNKNOWN;
                continue;
            }
            DataType declared = DataType.fromPgName(argType);
            if (declared == null) return;
            oids[i] = declared.getOid();
        }
        BuiltinCallTypes.requireCallable(bare, written, oids);
        BuiltinCallTypes.requireReachable(bare, written, oids);
        if (!StoredCallSignature.mayTake(bare, oids)) {
            MemgresException missing = noSuchRoutine(call, written);
            if (missing != null) throw missing;
        }
    }

    /** A literal PostgreSQL leaves of type unknown until the call it stands in says what it is. */
    private static boolean isUntypedLiteral(Expression arg) {
        if (!(arg instanceof Literal)) return false;
        Literal.LiteralType kind = ((Literal) arg).literalType();
        return kind == Literal.LiteralType.STRING || kind == Literal.LiteralType.NULL;
    }

    /**
     * The same judgement of one call, for an expression whose names another reader settles. The
     * predicate written beside a conflict target is such an expression: PostgreSQL analyses it as
     * an index predicate, so a call naming no function is refused there exactly as it is refused
     * where the index itself is written, whether or not any index would have arbitrated.
     */
    static void readCall(DdlExecutor ddl, FunctionCallExpr call, Table table) {
        StoredExprNames reader = new StoredExprNames(ddl, table, null, false, false);
        if (reader.handsOver(call)) return;
        reader.requireFunctionExists(call);
    }
}
