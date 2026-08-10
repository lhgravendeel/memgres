package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

/**
 * Geometric function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class GeometricFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    GeometricFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * Reject a shape PG has no such function for. Geometric values are stored as text, so the
     * shape is read back from the literal: a box is two points, a circle carries its radius.
     * A three-point {@code ((..))} value is a polygon or a closed path, and PG defines neither
     * center() nor a comparison for those.
     */
    private void requireBoxOrCircle(String text, String funcName) {
        String shape = shapeName(text);
        if ("circle".equals(shape) || "box".equals(shape)) return;
        String t = text.trim();
        throw new MemgresException("function " + funcName + "(" + shapeName(t) + ") does not exist"
                + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /** PG has area() for box, circle and path; a polygon, lseg, line or point has none. */
    private void requireAreaShape(String declaredType, String text) {
        String shape = declaredType != null ? declaredType : shapeName(text);
        if (shape.equals("box") || shape.equals("circle") || shape.equals("path")) return;
        throw new MemgresException("function area(" + shape + ") does not exist"
                + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /**
     * The geometric type an argument expression is declared to have, or null when it carries no
     * declaration. A type-annotated literal and a cast both name their type outright; a column
     * reference takes it from the column. Anything else is an untyped literal, which PG itself
     * cannot resolve to an overload without a cast.
     */
    private String declaredGeometricType(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name == null ? null : geometricTypeName(name.toLowerCase().trim());
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : ctx.getBindings()) {
                if (b.table() == null) continue;
                if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                        && !ref.table().equalsIgnoreCase(b.table().getName())) {
                    continue;
                }
                int idx = b.table().getColumnIndex(ref.column());
                if (idx < 0) continue;
                DataType type = b.table().getColumns().get(idx).getType();
                return type == null ? null : geometricTypeName(type.getPgName());
            }
        }
        return null;
    }

    /** The given type name when it names a geometric type, else null. */
    /**
     * Refuses a call whose argument count no signature of the name takes.
     *
     * <p>A name PostgreSQL does not have carries no row in the signature table, so the arity rule
     * that judges every other call says nothing about it. These are memgres's own, and they still
     * take what they take.
     */
    private static void requireArity(FunctionCallExpr fn, int arity) {
        if (fn.args().size() == arity) return;
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < fn.args().size(); i++) {
            types.append(i > 0 ? ", " : "").append("point");
        }
        MemgresException e = new MemgresException(
                "function " + fn.name() + "(" + types + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types. You might need to add explicit type casts.");
        throw e;
    }

    private static String geometricTypeName(String name) {
        switch (name) {
            case "point": case "line": case "lseg":
            case "box": case "path": case "polygon": case "circle":
                return name;
            default:
                return null;
        }
    }

    /** How many {@code (x,y)} pairs the literal holds. */
    static int countPoints(String text) {
        int n = 0;
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '(') n++;
        }
        return n;
    }

    /**
     * The PG type a geometric value denotes, read back from its output form. PG's own rendering
     * is the discriminator: a box prints its two points bare, {@code (1,1),(0,0)}, while a
     * polygon or closed path wraps them, {@code ((0,0),(1,1))}.
     */
    static String shapeName(String text) {
        String t = text.trim();
        if (t.startsWith("<")) return "circle";
        if (t.startsWith("{")) return "line";
        if (t.startsWith("[")) return countPoints(t) == 2 ? "lseg" : "path";
        if (t.startsWith("((")) return "polygon";
        if (t.startsWith("(")) {
            int pts = countPoints(t);
            return pts <= 1 ? "point" : pts == 2 ? "box" : "polygon";
        }
        return "unknown";
    }

    private void requireGeometric(Object arg, String funcName) {
        if (arg instanceof Number) {
            throw new MemgresException(
                "function " + funcName + "(integer) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        String s = arg.toString();
        if (!s.isEmpty() && !GeometricOperations.isGeometricString(s)) {
            throw new MemgresException(
                "function " + funcName + "(unknown) is not unique\n  Hint: Could not choose a best candidate function.", "42725");
        }
    }

    /** The names PostgreSQL declares as a constructor of a shape. */
    private static final java.util.Set<String> SHAPE_CONSTRUCTORS =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "point", "box", "circle", "lseg", "line", "path", "polygon"));

    /**
     * Refuses a constructor call PostgreSQL has no signature for.
     *
     * <p>Every one of these needs an argument: {@code point()} resolves to nothing, and memgres
     * read the first argument of an empty list and reported the resulting internal error to the
     * client. A single argument has to be something a shape can be read out of — a number is not,
     * which is why {@code point(42)} is a function that does not exist rather than a point at
     * (NaN,NaN), and a piece of text that is not a shape is bad input for the type.
     */
    private void requireConstructorArgs(String name, FunctionCallExpr fn, RowContext ctx) {
        if (!SHAPE_CONSTRUCTORS.contains(name)) return;
        if (fn.args().isEmpty()) {
            throw new MemgresException("function " + name + "() does not exist"
                    + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (fn.args().size() != 1) return;
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        if (arg == null) return;
        if (arg instanceof Number || arg instanceof Boolean) {
            throw new MemgresException("function " + name + "(" + AstExecutor.pgTypeNameOf(arg)
                    + ") does not exist"
                    + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (arg instanceof String && !GeometricOperations.isGeometricString(((String) arg).trim())) {
            // Not written as a shape, so it is either the plain list of numbers PostgreSQL also
            // reads -- "1,2" for a point, "0,0,5" for a circle -- or it is not input for this type
            // at all. Let the cast decide, so the constructor and the cast beside it agree: a test
            // of its own for what a shape looks like refused text the very same value could be
            // cast to. A value that IS written as a shape is left alone, because these constructors
            // also build one shape from another and that is not text this type can read.
            executor.castEvaluator.applyCast(arg, name, true);
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        requireConstructorArgs(name, fn, ctx);
        switch (name) {
            case "area": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "area");
                // PG defines area() for box, circle and path only. A closed path and a polygon
                // print identically, so the value's text cannot tell them apart -- the declared
                // type of the argument expression is what decides.
                requireAreaShape(declaredGeometricType(fn.args().get(0), ctx), arg.toString());
                return GeometricOperations.area(arg.toString());
            }
            case "center": {
                if (fn.args().size() != 1) {
                    throw new MemgresException("function center() does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "center");
                // PG only defines center() for box and circle; a polygon, path or lseg has none
                requireBoxOrCircle(arg.toString(), "center");
                return GeometricOperations.format(GeometricOperations.center(arg.toString()));
            }
            case "diameter": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "diameter");
                return GeometricOperations.diameter(arg.toString());
            }
            case "radius": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "radius");
                String rs = arg.toString().trim();
                if (!rs.startsWith("<") || !rs.endsWith(">")) {
                    throw new MemgresException(
                        "function radius(box) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                return GeometricOperations.radius(rs);
            }
            case "height": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "height");
                return GeometricOperations.height(arg.toString());
            }
            case "width": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "width");
                return GeometricOperations.width(arg.toString());
            }
            case "npoints": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.npoints(arg.toString());
            }
            case "isclosed": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.isclosed(arg.toString());
            }
            case "isopen": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.isopen(arg.toString());
            }
            case "pclose": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.pclose(arg.toString());
            }
            case "popen": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.popen(arg.toString());
            }
            case "diagonal": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.diagonal(arg.toString());
            }
            case "slope": {
                if (fn.args().size() == 2) {
                    Object p1 = executor.evalExpr(fn.args().get(0), ctx);
                    Object p2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (p1 == null || p2 == null) return null;
                    return GeometricOperations.slope(p1.toString(), p2.toString());
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.slope(arg.toString(), null);
            }
            case "bound_box": {
                if (fn.args().size() == 2) {
                    Object a = executor.evalExpr(fn.args().get(0), ctx);
                    Object b = executor.evalExpr(fn.args().get(1), ctx);
                    if (a == null || b == null) return null;
                    return GeometricOperations.boundBox(a.toString(), b.toString());
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.boundBox(arg.toString(), null);
            }
            case "point": {
                if (fn.args().size() == 2) {
                    Object x = executor.evalExpr(fn.args().get(0), ctx);
                    Object y = executor.evalExpr(fn.args().get(1), ctx);
                    if (x == null || y == null) return null;
                    return GeometricOperations.format(
                            new GeometricOperations.PgPoint(executor.toDouble(x), executor.toDouble(y)));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.format(GeometricOperations.toPoint(arg.toString()));
            }
            case "box": {
                if (fn.args().size() > 2) {
                    throw new MemgresException(
                        "function box(point, point, point) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                if (fn.args().size() == 2) {
                    Object p1 = executor.evalExpr(fn.args().get(0), ctx);
                    Object p2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (p1 == null || p2 == null) return null;
                    if (p1 instanceof String && !GeometricOperations.isGeometricString(((String) p1))) {
                        String s1 = (String) p1;
                        throw new MemgresException("invalid input syntax for type point: \"" + s1 + "\"", "22P02");
                    }
                    if (p2 instanceof String && !GeometricOperations.isGeometricString(((String) p2))) {
                        String s2 = (String) p2;
                        throw new MemgresException("invalid input syntax for type point: \"" + s2 + "\"", "22P02");
                    }
                    requireGeometric(p1, "box");
                    requireGeometric(p2, "box");
                    return GeometricOperations.format(GeometricOperations.toBox(p1.toString(), p2.toString()));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                // box(point) — zero-area box at that point (PG behavior)
                String argTypeName = getArgCastType(fn.args().get(0));
                if ("point".equals(argTypeName)) {
                    GeometricOperations.PgPoint pt = GeometricOperations.parsePoint(arg.toString());
                    return GeometricOperations.format(new GeometricOperations.PgBox(pt, pt));
                }
                requireGeometric(arg, "box");
                return GeometricOperations.format(GeometricOperations.toBox(arg.toString(), null));
            }
            case "circle": {
                if (fn.args().size() == 2) {
                    Object p = executor.evalExpr(fn.args().get(0), ctx);
                    Object r = executor.evalExpr(fn.args().get(1), ctx);
                    if (p == null || r == null) return null;
                    GeometricOperations.PgPoint center = GeometricOperations.parsePoint(p.toString());
                    return GeometricOperations.format(
                            new GeometricOperations.PgCircle(center, executor.toDouble(r)));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.format(GeometricOperations.toCircle(arg.toString()));
            }
            case "lseg": {
                if (fn.args().size() == 2) {
                    Object p1 = executor.evalExpr(fn.args().get(0), ctx);
                    Object p2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (p1 == null || p2 == null) return null;
                    return GeometricOperations.format(new GeometricOperations.PgLseg(
                            GeometricOperations.parsePoint(p1.toString()),
                            GeometricOperations.parsePoint(p2.toString())));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.format(GeometricOperations.parseLseg(arg.toString()));
            }
            case "line": {
                if (fn.args().size() == 2) {
                    Object p1 = executor.evalExpr(fn.args().get(0), ctx);
                    Object p2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (p1 == null || p2 == null) return null;
                    return GeometricOperations.format(GeometricOperations.lineFromPoints(
                            GeometricOperations.parsePoint(p1.toString()),
                            GeometricOperations.parsePoint(p2.toString())));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.format(GeometricOperations.parseLine(arg.toString()));
            }
            case "polygon": {
                if (fn.args().size() == 2) {
                    // polygon(npts, circle) - create polygon with npts vertices from circle
                    Object nptsObj = executor.evalExpr(fn.args().get(0), ctx);
                    Object circleObj = executor.evalExpr(fn.args().get(1), ctx);
                    if (nptsObj == null || circleObj == null) return null;
                    int npts = ((Number) nptsObj).intValue();
                    GeometricOperations.PgCircle circle = GeometricOperations.toCircle(circleObj.toString());
                    return GeometricOperations.format(GeometricOperations.toPolygon(circle, npts));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof java.util.List<?>) {
                    throw new MemgresException(
                        "function polygon(text[]) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                return GeometricOperations.toPolygon(arg.toString());
            }
            case "path": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.toPath(arg.toString());
            }
            case "intersects": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                return GeometricOperations.intersects(a.toString(), b.toString());
            }
            case "closest_point": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                GeometricOperations.PgPoint pt = GeometricOperations.closestPoint(a.toString(), b.toString());
                return pt != null ? GeometricOperations.format(pt) : null;
            }
            case "is_horizontal": {
                // One segment, and nothing else. PostgreSQL has no function of this name at all --
                // its own spellings are lseg_horizontal and ishorizontal -- so the one-argument
                // form is memgres's own, and the two-argument form was an invention on top of an
                // invention. It answered only because the catalog carried a row for the name.
                requireArity(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.isHorizontal(
                        GeometricOperations.parseLseg(arg.toString()));
            }
            case "is_vertical": {
                // One segment, and nothing else. PostgreSQL has no function of this name at all --
                // its own spellings are lseg_horizontal and ishorizontal -- so the one-argument
                // form is memgres's own, and the two-argument form was an invention on top of an
                // invention. It answered only because the catalog carried a row for the name.
                requireArity(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.isVertical(
                        GeometricOperations.parseLseg(arg.toString()));
            }
            case "is_parallel": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                return GeometricOperations.isParallel(
                        GeometricOperations.parseLseg(a.toString()),
                        GeometricOperations.parseLseg(b.toString()));
            }
            case "is_perpendicular": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                return GeometricOperations.isPerpendicular(
                        GeometricOperations.parseLseg(a.toString()),
                        GeometricOperations.parseLseg(b.toString()));
            }
            default:
                return NOT_HANDLED;
        }
    }

    /**
     * Extract the type name from a CastExpr argument (e.g., polygon '(...)' -> "polygon").
     * Returns null if the argument is not a type cast.
     */
    private String getArgCastType(Expression expr) {
        if (expr instanceof CastExpr) {
            return ((CastExpr) expr).typeName().toLowerCase();
        }
        return null;
    }
}
