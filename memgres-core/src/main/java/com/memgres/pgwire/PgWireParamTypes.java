package com.memgres.pgwire;

import com.memgres.engine.BuiltinCallTypes;
import com.memgres.engine.Column;
import com.memgres.engine.DataType;
import com.memgres.engine.Database;
import com.memgres.engine.DomainType;
import com.memgres.engine.MemgresException;
import com.memgres.engine.Schema;
import com.memgres.engine.Session;
import com.memgres.engine.Table;
import com.memgres.engine.parser.Parser;
import com.memgres.engine.parser.ast.AnyAllArrayExpr;
import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.ArraySliceExpr;
import com.memgres.engine.parser.ast.SubscriptExpr;
import com.memgres.engine.parser.ast.AtTimeZoneExpr;
import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.CollateExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.DeleteStmt;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.InsertStmt;
import com.memgres.engine.parser.ast.IsBooleanExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.LikeExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.NamedArgExpr;
import com.memgres.engine.parser.ast.ParamRef;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.parser.ast.UpdateStmt;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * What a client is told the parameters of a prepared statement are.
 *
 * <p>PostgreSQL answers Describe with the types it resolved the parameters to while analysing the
 * statement, so a driver's {@code ParameterMetaData} names the column's type rather than a
 * placeholder. A parameter with nothing to go on is text either way; the cases worth resolving are
 * the ones where the statement itself says what the value has to be — a comparison against a
 * column, a value written into a column, an operand beside a literal, or an argument of a
 * function whose signature settles it.
 *
 * <p>An argument position can also settle on nothing <em>because</em> the name means too many
 * things: {@code sum} is declared over two type categories, so {@code sum(?)} names none of them
 * and PostgreSQL refuses the statement rather than describing a parameter it did not resolve.
 * {@link BuiltinCallTypes} decides both, from the same signature tables the catalog is built from,
 * so a call resolves here exactly as it resolves when it runs.
 *
 * <p>Where the answer is still not plain the parameter keeps its unresolved type, because naming a
 * type the statement does not imply would tell the driver to send something the server then has to
 * refuse.
 */
final class PgWireParamTypes {

    private PgWireParamTypes() {}

    /**
     * The OIDs to describe {@code numParams} parameters of {@code sql} with, 0 where the statement
     * does not say.
     *
     * <p>A statement this class cannot read still has to be describable, so a parse failure
     * resolves nothing rather than failing. A call the statement cannot mean is different: that is
     * the answer, and it is passed on.
     */
    static int[] infer(String sql, int numParams, Database database, Session session) {
        int[] oids = new int[numParams];
        if (numParams == 0 || sql == null || database == null) return oids;
        try {
            Statement stmt = Parser.parse(sql);
            new Resolver(database, session, oids, Collections.<String, Table>emptyMap()).statement(stmt);
        } catch (MemgresException e) {
            throw e;                 // the statement names a call PostgreSQL cannot choose
        } catch (RuntimeException e) {
            // Describe is metadata, not execution: a statement this class cannot read still has to
            // be describable, with the types it would have had before.
        }
        return oids;
    }

    /** Walks one query level, recording a type for every parameter whose context fixes one. */
    private static final class Resolver {
        private final Database database;
        private final Session session;
        private final int[] oids;
        /** Relation names and aliases in scope, mapped to the relation they stand for. */
        private final Map<String, Table> scope;

        Resolver(Database database, Session session, int[] oids, Map<String, Table> outerScope) {
            this.database = database;
            this.session = session;
            this.oids = oids;
            this.scope = new HashMap<>(outerScope);
        }

        void statement(Statement stmt) {
            if (stmt instanceof SelectStmt) {
                SelectStmt s = (SelectStmt) stmt;
                subqueries(s.withClauses());
                addFromItems(s.from());
                for (SelectStmt.SelectTarget t : nullSafe(s.targets())) condition(t.expr());
                condition(s.where());
                condition(s.having());
                for (Expression g : nullSafe(s.groupBy())) condition(g);
                for (SelectStmt.OrderByItem o : nullSafe(s.orderBy())) condition(o.expr());
                // A row count is a bigint, whatever the query it limits looks like.
                assignOid(s.limit(), DataType.BIGINT.getOid());
                assignOid(s.offset(), DataType.BIGINT.getOid());
                condition(s.limit());
                condition(s.offset());
            } else if (stmt instanceof SetOpStmt) {
                SetOpStmt so = (SetOpStmt) stmt;
                nested(so.left());
                nested(so.right());
            } else if (stmt instanceof UpdateStmt) {
                UpdateStmt u = (UpdateStmt) stmt;
                subqueries(u.withClauses());
                Table target = findTable(u.schema(), u.table());
                addRelation(u.alias() != null ? u.alias() : u.table(), target);
                addFromItems(u.from());
                for (InsertStmt.SetClause set : nullSafe(u.setClauses())) {
                    assignOid(set.value(), columnOid(target, set.column()));
                    condition(set.value());
                }
                condition(u.where());
            } else if (stmt instanceof DeleteStmt) {
                DeleteStmt d = (DeleteStmt) stmt;
                subqueries(d.withClauses());
                addRelation(d.alias() != null ? d.alias() : d.table(), findTable(d.schema(), d.table()));
                addFromItems(d.using());
                condition(d.where());
            } else if (stmt instanceof InsertStmt) {
                InsertStmt ins = (InsertStmt) stmt;
                subqueries(ins.withClauses());
                Table target = findTable(ins.schema(), ins.table());
                addRelation(ins.table(), target);
                List<String> cols = ins.columns();
                for (List<Expression> row : nullSafe(ins.values())) {
                    for (int i = 0; i < row.size(); i++) {
                        assignOid(row.get(i), insertColumnOid(target, cols, i));
                        condition(row.get(i));
                    }
                }
                if (ins.selectStmt() != null) insertSelect(ins, target, cols);
            }
        }

        /** The column an INSERT writes its {@code i}th value into, whether or not it was named. */
        private int insertColumnOid(Table target, List<String> cols, int i) {
            if (cols != null) return i < cols.size() ? columnOid(target, cols.get(i)) : 0;
            if (target == null || i >= target.getColumns().size()) return 0;
            return oidOf(target.getColumns().get(i));
        }

        /**
         * {@code INSERT INTO t (c) SELECT ?} writes the select's own output into {@code c}, so the
         * column settles the target the same way a VALUES row does.
         */
        private void insertSelect(InsertStmt ins, Table target, List<String> cols) {
            Statement query = ins.selectStmt();
            if (query instanceof SelectStmt) {
                SelectStmt sel = (SelectStmt) query;
                List<SelectStmt.SelectTarget> targets = nullSafe(sel.targets());
                for (int i = 0; i < targets.size(); i++) {
                    assignOid(targets.get(i).expr(), insertColumnOid(target, cols, i));
                }
            }
            nested(query);
        }

        /** Walk a nested query with this level's relations still visible, as a correlated one is. */
        private void nested(Statement stmt) {
            if (stmt == null) return;
            new Resolver(database, session, oids, scope).statement(stmt);
        }

        private void subqueries(List<SelectStmt.CommonTableExpr> ctes) {
            for (SelectStmt.CommonTableExpr cte : nullSafe(ctes)) nested(cte.query());
        }

        // ---- FROM scope ----

        private void addFromItems(List<SelectStmt.FromItem> items) {
            for (SelectStmt.FromItem item : nullSafe(items)) addFromItem(item);
        }

        private void addFromItem(SelectStmt.FromItem item) {
            if (item instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
                addRelation(ref.alias() != null ? ref.alias() : ref.table(), findTable(ref.schema(), ref.table()));
            } else if (item instanceof SelectStmt.JoinFrom) {
                SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
                addFromItem(join.left());
                addFromItem(join.right());
                condition(join.on());
            } else if (item instanceof SelectStmt.SubqueryFrom) {
                nested(((SelectStmt.SubqueryFrom) item).subquery());
            } else if (item instanceof SelectStmt.FunctionFrom) {
                SelectStmt.FunctionFrom fn = (SelectStmt.FunctionFrom) item;
                call(fn.functionName(), fn.args());
            }
        }

        private void addRelation(String name, Table table) {
            if (name == null || table == null) return;
            scope.put(name.toLowerCase(Locale.ROOT), table);
        }

        // ---- Expression walking ----

        /** Read an expression, resolving parameters against whatever they sit beside. */
        private void condition(Expression expr) {
            if (expr == null) return;
            if (expr instanceof BinaryExpr) {
                binary((BinaryExpr) expr);
            } else if (expr instanceof UnaryExpr) {
                condition(((UnaryExpr) expr).operand());
            } else if (expr instanceof InExpr) {
                InExpr in = (InExpr) expr;
                int want = typeOid(in.expr());
                // = ANY (x) is one operand against a whole array, where IN (a, b) is a list of
                // operands: the parameter behind the first stands for every value, not for one.
                if (in.fromAny() && nullSafe(in.values()).size() == 1) {
                    DataType array = DataType.arrayOf(DataType.fromOid(want));
                    if (array != null) want = array.getOid();
                }
                for (Expression v : nullSafe(in.values())) { assignOid(v, want); condition(v); }
                condition(in.expr());
            } else if (expr instanceof BetweenExpr) {
                BetweenExpr bt = (BetweenExpr) expr;
                int want = typeOid(bt.expr());
                assignOid(bt.low(), want);
                assignOid(bt.high(), want);
                condition(bt.expr());
                condition(bt.low());
                condition(bt.high());
            } else if (expr instanceof CastExpr) {
                CastExpr cast = (CastExpr) expr;
                // A parameter written with a cast is of the type it was cast to, whatever the
                // rest of the statement would otherwise have settled on for it.
                assignOid(cast.expr(), namedTypeOid(cast.typeName()));
                condition(cast.expr());
            } else if (expr instanceof SubqueryExpr) {
                nested(((SubqueryExpr) expr).subquery());
            } else if (expr instanceof ExistsExpr) {
                nested(((ExistsExpr) expr).subquery());
            } else if (expr instanceof CaseExpr) {
                caseExpr((CaseExpr) expr);
            } else if (expr instanceof AnyAllArrayExpr) {
                anyAllArray((AnyAllArrayExpr) expr);
            } else if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                call(fn.name(), fn.args());
                condition(fn.filter());
                for (SelectStmt.OrderByItem o : nullSafe(fn.orderBy())) condition(o.expr());
            } else if (expr instanceof WindowFuncExpr) {
                WindowFuncExpr w = (WindowFuncExpr) expr;
                call(w.name(), w.args());
                condition(w.filter());
                for (Expression p : nullSafe(w.partitionBy())) condition(p);
                for (SelectStmt.OrderByItem o : nullSafe(w.orderBy())) condition(o.expr());
            } else if (expr instanceof LikeExpr) {
                LikeExpr like = (LikeExpr) expr;
                assignOid(like.pattern(), DataType.TEXT.getOid());
                assignOid(like.left(), DataType.TEXT.getOid());
                condition(like.left());
                condition(like.pattern());
            } else if (expr instanceof IsNullExpr) {
                condition(((IsNullExpr) expr).expr());
            } else if (expr instanceof IsBooleanExpr) {
                condition(((IsBooleanExpr) expr).expr());
            } else if (expr instanceof CollateExpr) {
                condition(((CollateExpr) expr).expr());
            } else if (expr instanceof AtTimeZoneExpr) {
                condition(((AtTimeZoneExpr) expr).expr());
            } else if (expr instanceof ArraySliceExpr) {
                ArraySliceExpr slice = (ArraySliceExpr) expr;
                assignOid(slice.lower(), DataType.INTEGER.getOid());
                assignOid(slice.upper(), DataType.INTEGER.getOid());
                condition(slice.array());
            } else if (expr instanceof SubscriptExpr) {
                SubscriptExpr sub = (SubscriptExpr) expr;
                // What a subscript is depends on what is being subscripted: a json container is
                // reached by a text key, everything else by an integer.
                int subscriptType = subscriptOid(typeOid(sub.base()));
                for (SubscriptExpr.Subscript one : sub.subscripts()) {
                    if (subscriptType != 0) {
                        assignOid(one.lower(), subscriptType);
                        assignOid(one.upper(), subscriptType);
                    }
                }
                condition(sub.base());
            } else if (expr instanceof NamedArgExpr) {
                condition(((NamedArgExpr) expr).value());
            } else if (expr instanceof ArrayExpr) {
                for (Expression e : nullSafe(((ArrayExpr) expr).elements())) condition(e);
            }
        }

        private void binary(BinaryExpr b) {
            if (b.op() == BinaryExpr.BinOp.JSON_SUBSCRIPT) {
                // A subscript is not compared with what it indexes: an array is indexed by an
                // integer and a JSON container by a key, whatever the container's own type is.
                assignOid(b.right(), subscriptOid(typeOid(b.left())));
                condition(b.left());
                condition(b.right());
                return;
            }
            if (rowConstructors(b)) return;
            int leftType = typeOid(b.left());
            int rightType = typeOid(b.right());
            assignOid(b.left(), otherSideOid(b.op(), rightType));
            assignOid(b.right(), otherSideOid(b.op(), leftType));
            condition(b.left());
            condition(b.right());
            requireComparable(b);
        }

        /**
         * Refuses a comparison between two kinds of value there is no operator for.
         *
         * <p>Only judged where a parameter settled one of the two sides, which is the case the
         * statement could not have been written any other way: {@code i = (SELECT ?)} compares an
         * integer with the text an unsettled parameter becomes at that subquery's output, and
         * {@code ts > now() - ?} compares an instant with the length of time the subtraction
         * produced. Both are decided before anything runs, and PostgreSQL decides them the same
         * way. A pair this class records no category for is left alone.
         */
        private void requireComparable(BinaryExpr b) {
            if (!isComparison(b.op())) return;
            if (!containsParam(b.left()) && !containsParam(b.right())) return;
            char left = BuiltinCallTypes.categoryOf(typeOid(b.left()));
            char right = BuiltinCallTypes.categoryOf(typeOid(b.right()));
            if (left == 0 || right == 0 || left == right) return;
            throw new MemgresException("operator does not exist: "
                    + typeName(typeOid(b.left())) + " " + symbolOf(b.op()) + " "
                    + typeName(typeOid(b.right()))
                    + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }

        private boolean isComparison(BinaryExpr.BinOp op) {
            return op == BinaryExpr.BinOp.EQUAL || op == BinaryExpr.BinOp.NOT_EQUAL
                    || op == BinaryExpr.BinOp.LESS_THAN || op == BinaryExpr.BinOp.GREATER_THAN
                    || op == BinaryExpr.BinOp.LESS_EQUAL || op == BinaryExpr.BinOp.GREATER_EQUAL;
        }

        private String symbolOf(BinaryExpr.BinOp op) {
            switch (op) {
                case EQUAL: return "=";
                case NOT_EQUAL: return "<>";
                case LESS_THAN: return "<";
                case GREATER_THAN: return ">";
                case LESS_EQUAL: return "<=";
                default: return ">=";
            }
        }

        private String typeName(int oid) {
            return BuiltinCallTypes.typeName(oid);
        }

        /** Whether a parameter stands anywhere under this expression. */
        private boolean containsParam(Expression expr) {
            if (expr == null) return false;
            if (expr instanceof ParamRef) return true;
            if (expr instanceof BinaryExpr) {
                return containsParam(((BinaryExpr) expr).left())
                        || containsParam(((BinaryExpr) expr).right());
            }
            if (expr instanceof UnaryExpr) return containsParam(((UnaryExpr) expr).operand());
            if (expr instanceof CastExpr) return containsParam(((CastExpr) expr).expr());
            if (expr instanceof CollateExpr) return containsParam(((CollateExpr) expr).expr());
            if (expr instanceof SubqueryExpr) {
                Statement inner = ((SubqueryExpr) expr).subquery();
                if (!(inner instanceof SelectStmt)) return false;
                for (SelectStmt.SelectTarget t : nullSafe(((SelectStmt) inner).targets())) {
                    if (containsParam(t.expr())) return true;
                }
                return false;
            }
            if (expr instanceof FunctionCallExpr) {
                for (Expression a : nullSafe(((FunctionCallExpr) expr).args())) {
                    if (containsParam(a)) return true;
                }
            }
            return false;
        }

        /** {@code (a, b) = (?, ?)} settles each parameter against the element it stands beside. */
        private boolean rowConstructors(BinaryExpr b) {
            List<Expression> left = rowElements(b.left());
            List<Expression> right = rowElements(b.right());
            if (left == null || right == null || left.size() != right.size()) return false;
            for (int i = 0; i < left.size(); i++) {
                assignOid(left.get(i), typeOid(right.get(i)));
                assignOid(right.get(i), typeOid(left.get(i)));
                condition(left.get(i));
                condition(right.get(i));
            }
            return true;
        }

        private List<Expression> rowElements(Expression expr) {
            if (!(expr instanceof ArrayExpr)) return null;
            ArrayExpr row = (ArrayExpr) expr;
            return row.isRow() ? row.elements() : null;
        }

        /**
         * The type an operand of {@code op} takes when the other side is {@code otherType}.
         *
         * <p>Most operators compare like with like, so the other side's type is the answer.
         * Arithmetic on an instant is the exception, and the sign decides it: what is added to a
         * moment is a length of time, and what is subtracted from one is either a length of time
         * or another moment — PostgreSQL reads the unsettled operand as the same type as the one
         * beside it whenever that pair has an operator, which for subtraction it always has.
         * {@code date + unknown} fits a number of days and an interval equally well, so it names
         * no operator at all and PostgreSQL says so. This is the rule the evaluator applies to an
         * untyped literal, applied to a parameter, which is the same thing at this point.
         */
        private int otherSideOid(BinaryExpr.BinOp op, int otherType) {
            boolean add = op == BinaryExpr.BinOp.ADD;
            boolean subtract = op == BinaryExpr.BinOp.SUBTRACT;
            if (!add && !subtract) return otherType;
            if (otherType == DataType.INTERVAL.getOid()) return DataType.INTERVAL.getOid();
            if (otherType == DataType.DATE.getOid()) {
                if (add) throw notUnique("date", "+");
                return otherType;                       // date - date is the only pair there is
            }
            if (otherType == DataType.TIMETZ.getOid()) {
                throw notUnique("time with time zone", add ? "+" : "-");
            }
            if (otherType == DataType.TIMESTAMP.getOid() || otherType == DataType.TIMESTAMPTZ.getOid()
                    || otherType == DataType.TIME.getOid()) {
                return add ? DataType.INTERVAL.getOid() : otherType;
            }
            return otherType;
        }

        private MemgresException notUnique(String typeName, String symbol) {
            return new MemgresException("operator is not unique: " + typeName + " " + symbol
                    + " unknown\n  Hint: Could not choose a best candidate operator."
                    + " You might need to add explicit type casts.", "42725");
        }

        /** What indexes a container: a position in an array, a key in a JSON object. */
        private int subscriptOid(int containerType) {
            if (containerType == DataType.JSONB.getOid() || containerType == DataType.JSON.getOid()) {
                return DataType.TEXT.getOid();
            }
            return containerType == 0 ? 0 : DataType.INTEGER.getOid();
        }

        private void caseExpr(CaseExpr c) {
            int operandType = typeOid(c.operand());
            condition(c.operand());
            int resultType = typeOid(c.elseExpr());
            for (CaseExpr.WhenClause w : nullSafe(c.whenClauses())) {
                if (resultType == 0) resultType = typeOid(w.result());
            }
            for (CaseExpr.WhenClause w : nullSafe(c.whenClauses())) {
                // With an operand, each WHEN is compared with it; without one, each is a predicate.
                assignOid(w.condition(), operandType);
                condition(w.condition());
                assignOid(w.result(), resultType);
                condition(w.result());
            }
            assignOid(c.elseExpr(), resultType);
            condition(c.elseExpr());
        }

        private void anyAllArray(AnyAllArrayExpr a) {
            int leftType = typeOid(a.left());
            int arrayType = typeOid(a.array());
            // The right-hand side of ANY is the array over the left-hand side's type, not that
            // type itself: a driver told int4 would send one value where a list is wanted.
            DataType array = DataType.arrayOf(DataType.fromOid(leftType));
            if (array != null) assignOid(a.array(), array.getOid());
            DataType element = DataType.elementOf(DataType.fromOid(arrayType));
            if (element != null) assignOid(a.left(), element.getOid());
            condition(a.left());
            condition(a.array());
        }

        /**
         * The arguments of a built-in, resolved from its declared signatures. Where the name is
         * one PostgreSQL cannot choose between, the call is refused rather than described.
         */
        private void call(String name, List<Expression> args) {
            List<Expression> actual = flattenRows(nullSafe(args));
            for (Expression arg : actual) condition(arg);
            if (name == null || actual.isEmpty()) return;

            if (unifiesItsArguments(name)) {
                int shared = 0;
                for (Expression arg : actual) {
                    if (shared == 0) shared = typeOid(arg);
                }
                for (Expression arg : actual) assignOid(arg, shared);
                return;
            }
            if (!BuiltinCallTypes.records(name)) return;

            int[] written = new int[actual.size()];
            boolean anyUnsettled = false;
            for (int i = 0; i < actual.size(); i++) {
                written[i] = typeOid(actual.get(i));
                if (written[i] != 0) continue;
                // An argument of no type and an argument whose type this class cannot read are
                // not the same thing, and only the first is what PostgreSQL calls unknown.
                // Reading the second as unknown made a call look unchoosable that the statement
                // had said quite enough about.
                if (!saysNothingAboutItsType(actual.get(i))) return;
                anyUnsettled = true;
            }
            BuiltinCallTypes.requireCallable(name, written);
            if (!anyUnsettled) return;
            int[] resolved = BuiltinCallTypes.argumentTypes(name, written);
            if (resolved == null) return;
            for (int i = 0; i < actual.size(); i++) {
                if (!BuiltinCallTypes.isPolymorphic(resolved[i])) {
                    assignOid(actual.get(i), resolved[i]);
                } else if (actual.get(i) instanceof ParamRef) {
                    // The signature says "whatever was passed", and a parameter passes nothing
                    // for it to read: there is no type to describe, and PostgreSQL says so
                    // rather than choosing one.
                    throw new MemgresException("could not determine data type of parameter $"
                            + ((ParamRef) actual.get(i)).index(), "42P18");
                }
            }
        }

        /**
         * The arguments a call really passes. {@code (a, b) OVERLAPS (c, d)} is written as two
         * pairs and declared as four arguments, so the pairs are read apart before the signature
         * is looked up — otherwise the call is judged against an arity nothing declares.
         */
        private List<Expression> flattenRows(List<Expression> args) {
            boolean anyRow = false;
            for (Expression arg : args) {
                if (rowElements(arg) != null) { anyRow = true; break; }
            }
            if (!anyRow) return args;
            List<Expression> flat = new ArrayList<>();
            for (Expression arg : args) {
                List<Expression> row = rowElements(arg);
                if (row == null) flat.add(arg);
                else flat.addAll(row);
            }
            return flat;
        }

        /** An argument that really has no type yet: an untyped literal, or a parameter. */
        private boolean saysNothingAboutItsType(Expression expr) {
            if (expr instanceof ParamRef) return true;
            if (!(expr instanceof Literal)) return false;
            Literal.LiteralType type = ((Literal) expr).literalType();
            return type == Literal.LiteralType.STRING || type == Literal.LiteralType.NULL;
        }

        /** The forms whose arguments are all of one type, whichever of them says what it is. */
        private boolean unifiesItsArguments(String name) {
            String lower = name.toLowerCase(Locale.ROOT);
            return lower.equals("coalesce") || lower.equals("greatest") || lower.equals("least")
                    || lower.equals("nullif");
        }

        /** Record {@code oid} for {@code expr} when it is a parameter with nothing yet. */
        private void assignOid(Expression expr, int oid) {
            if (oid == 0 || !(expr instanceof ParamRef)) return;
            int idx = ((ParamRef) expr).index() - 1;
            if (idx < 0 || idx >= oids.length) return;
            if (oids[idx] == 0) oids[idx] = oid;
        }

        /**
         * The type an expression has without knowing anything about the parameters in it. A
         * parameter has whatever an earlier part of the statement already settled on it, and
         * nothing this class does not read has one at all.
         */
        private int typeOid(Expression expr) {
            if (expr instanceof ParamRef) {
                int idx = ((ParamRef) expr).index() - 1;
                return idx >= 0 && idx < oids.length ? oids[idx] : 0;
            }
            if (expr instanceof ColumnRef) {
                ColumnRef ref = (ColumnRef) expr;
                if (ref.table() != null) {
                    return columnOid(scope.get(ref.table().toLowerCase(Locale.ROOT)), ref.column());
                }
                int found = 0;
                for (Table t : scope.values()) {
                    int c = columnOid(t, ref.column());
                    if (c == 0) continue;
                    if (found != 0 && found != c) return 0;   // ambiguous, say nothing
                    found = c;
                }
                return found;
            }
            if (expr instanceof CastExpr) {
                return namedTypeOid(((CastExpr) expr).typeName());
            }
            if (expr instanceof CollateExpr) {
                return typeOid(((CollateExpr) expr).expr());
            }
            if (expr instanceof AtTimeZoneExpr) {
                // Reading an instant in a zone gives a moment without one, and giving a zone to a
                // moment without one gives an instant.
                int inner = typeOid(((AtTimeZoneExpr) expr).expr());
                if (inner == DataType.TIMESTAMPTZ.getOid()) return DataType.TIMESTAMP.getOid();
                if (inner == DataType.TIMESTAMP.getOid()) return DataType.TIMESTAMPTZ.getOid();
                return 0;
            }
            if (expr instanceof Literal) {
                Literal lit = (Literal) expr;
                switch (lit.literalType()) {
                    case INTEGER: return integerLiteralType(lit.value()).getOid();
                    case FLOAT: return DataType.NUMERIC.getOid();
                    case BOOLEAN: return DataType.BOOLEAN.getOid();
                    default: return 0;   // a string literal is of unknown type until it is used
                }
            }
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                return callResultOid(fn.name(), fn.args(), fn.star());
            }
            if (expr instanceof WindowFuncExpr) {
                WindowFuncExpr w = (WindowFuncExpr) expr;
                return callResultOid(w.name(), w.args(), w.star());
            }
            if (expr instanceof CaseExpr) {
                CaseExpr c = (CaseExpr) expr;
                int result = typeOid(c.elseExpr());
                for (CaseExpr.WhenClause w : nullSafe(c.whenClauses())) {
                    if (result == 0) result = typeOid(w.result());
                }
                return result;
            }
            if (expr instanceof ArrayExpr) {
                ArrayExpr array = (ArrayExpr) expr;
                if (array.isRow()) return 0;
                for (Expression e : nullSafe(array.elements())) {
                    DataType over = DataType.arrayOf(DataType.fromOid(typeOid(e)));
                    if (over != null) return over.getOid();
                }
                return 0;
            }
            if (expr instanceof SubqueryExpr) {
                return subqueryOutputOid(((SubqueryExpr) expr).subquery());
            }
            if (expr instanceof SubscriptExpr) {
                SubscriptExpr sub = (SubscriptExpr) expr;
                DataType base = DataType.fromOid(typeOid(sub.base()));
                if (base == null) return 0;
                if (!DataType.isArrayType(base)) return base.getOid();
                if (sub.isSlice()) return base.getOid();
                DataType element = DataType.elementOf(base);
                return element == null ? 0 : element.getOid();
            }
            if (expr instanceof BinaryExpr) {
                BinaryExpr b = (BinaryExpr) expr;
                if (b.op() == BinaryExpr.BinOp.JSON_SUBSCRIPT) {
                    DataType element = DataType.elementOf(DataType.fromOid(typeOid(b.left())));
                    return element == null ? 0 : element.getOid();
                }
                int left = typeOid(b.left());
                int right = typeOid(b.right());
                if (left != right) return 0;
                // One moment taken from another is the time between them, not a third moment.
                if (b.op() == BinaryExpr.BinOp.SUBTRACT && isInstant(left)) {
                    return left == DataType.DATE.getOid()
                            ? DataType.INTEGER.getOid() : DataType.INTERVAL.getOid();
                }
                return left;
            }
            return 0;
        }

        private boolean isInstant(int oid) {
            return oid == DataType.DATE.getOid() || oid == DataType.TIMESTAMP.getOid()
                    || oid == DataType.TIMESTAMPTZ.getOid() || oid == DataType.TIME.getOid()
                    || oid == DataType.TIMETZ.getOid();
        }

        /**
         * The type a scalar subquery answers with. A parameter left unsettled by the query it sits
         * in is text by the time that query has an output — a subquery is analysed on its own, so
         * nothing outside it can still settle its type.
         */
        private int subqueryOutputOid(Statement stmt) {
            if (!(stmt instanceof SelectStmt)) return 0;
            List<SelectStmt.SelectTarget> targets = nullSafe(((SelectStmt) stmt).targets());
            if (targets.size() != 1) return 0;
            Expression only = targets.get(0).expr();
            int settled = typeOid(only);
            if (settled != 0) return settled;
            return only instanceof ParamRef ? DataType.TEXT.getOid() : 0;
        }

        /** The type a call answers with, {@code count(*)} included — its argument is written as one. */
        private int callResultOid(String name, List<Expression> args, boolean star) {
            if (name == null) return 0;
            List<Expression> actual = nullSafe(args);
            int[] written = new int[star && actual.isEmpty() ? 1 : actual.size()];
            for (int i = 0; i < actual.size(); i++) written[i] = typeOid(actual.get(i));
            if (star && actual.isEmpty()) written[0] = ANY_OID;
            return BuiltinCallTypes.resultType(name, written);
        }

        /** An integer literal is int4 while it fits, and int8 beyond that — as PostgreSQL reads it. */
        private DataType integerLiteralType(String value) {
            try {
                long v = Long.parseLong(value);
                return (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? DataType.INTEGER : DataType.BIGINT;
            } catch (RuntimeException e) {
                return DataType.NUMERIC;
            }
        }

        private int columnOid(Table table, String column) {
            if (table == null || column == null) return 0;
            for (Column c : table.getColumns()) {
                if (c.getName().equalsIgnoreCase(column)) return oidOf(c);
            }
            return 0;
        }

        /**
         * A domain has no operators of its own — the ones over the type it is built on are used
         * for it — so a parameter set beside a domain column resolves to that base type, which is
         * what the operator PostgreSQL chose actually takes.
         */
        private int oidOf(Column c) {
            if (c.getDomainTypeName() != null) return domainBaseOid(c.getDomainTypeName());
            if (c.getCompositeTypeName() != null) return 0;
            if (c.getArrayElementType() == null && c.getEnumTypeName() == null) {
                return unknownParamOid(c.getType());
            }
            return PgWireValueFormatter.columnTypeOid(c.getType(), c, session);
        }

        private int domainBaseOid(String domainName) {
            DomainType domain = database.getDomain(domainName);
            if (domain == null) return 0;
            if (domain.getArrayElementType() != null) {
                DataType over = DataType.arrayOf(domain.getArrayElementType());
                return over == null ? 0 : over.getOid();
            }
            return unknownParamOid(domain.getBaseType());
        }

        /** The OID of a type written in the statement, whether it is built in or declared. */
        private int namedTypeOid(String name) {
            if (name == null) return 0;
            String base = baseTypeName(name);
            DataType dt = DataType.fromPgName(base);
            if (dt != null && dt != DataType.ENUM) return unknownParamOid(dt);
            DomainType domain = database.getDomain(base);
            if (domain != null) return domainBaseOid(base);
            if (session == null) return 0;
            int declared = session.resolveOid(session.typeOidKey(base));
            return declared > 0 ? declared : 0;
        }

        /**
         * A varchar column carries no comparison operator of its own — text's are used for it — so
         * a parameter set beside one resolves to text, not to varchar.
         */
        private int unknownParamOid(DataType type) {
            if (type == null) return 0;
            return type == DataType.VARCHAR ? DataType.TEXT.getOid() : type.getOid();
        }

        private Table findTable(String schemaName, String tableName) {
            if (tableName == null) return null;
            String lower = tableName.toLowerCase(Locale.ROOT);
            if (schemaName != null) {
                Schema s = database.getSchemas().get(schemaName.toLowerCase(Locale.ROOT));
                return s == null ? null : s.getTable(lower);
            }
            for (Schema s : database.getSchemas().values()) {
                Table t = s.getTable(lower);
                if (t != null) return t;
            }
            return null;
        }
    }

    /** {@code "any"}, the type {@code count(*)} is declared to take. */
    private static final int ANY_OID = 2276;

    /** A type name as written, without its precision or its array marker. */
    private static String baseTypeName(String name) {
        String base = name.trim();
        int paren = base.indexOf('(');
        if (paren > 0) base = base.substring(0, paren).trim();
        while (base.endsWith("[]")) base = base.substring(0, base.length() - 2).trim();
        int dot = base.lastIndexOf('.');
        if (dot >= 0) base = base.substring(dot + 1);
        return base;
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? Collections.<T>emptyList() : list;
    }
}
