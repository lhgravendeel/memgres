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

    private void requireGeometric(Object arg, String funcName) {
        if (arg instanceof Number) {
            throw new MemgresException(
                "function " + funcName + "(integer) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
        }
        String s = arg.toString();
        if (!s.isEmpty() && !GeometricOperations.isGeometricString(s)) {
            throw new MemgresException(
                "function " + funcName + "(unknown) is not unique\n  Hint: Could not choose a best candidate function.", "42725");
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "area": {
                // PG: area() only works on box, circle, path. NOT polygon.
                String argTypeName = getArgCastType(fn.args().get(0));
                if ("polygon".equals(argTypeName)) {
                    throw new MemgresException(
                        "function area(polygon) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "area");
                return GeometricOperations.area(arg.toString());
            }
            case "center": {
                if (fn.args().size() != 1) {
                    throw new MemgresException("function center() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                // PG: center() only works on box, circle. NOT lseg, polygon, path.
                String argTypeName = getArgCastType(fn.args().get(0));
                if ("lseg".equals(argTypeName)) {
                    throw new MemgresException(
                        "function center(lseg) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                if ("polygon".equals(argTypeName)) {
                    throw new MemgresException(
                        "function center(polygon) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                requireGeometric(arg, "center");
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
                        "function radius(box) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
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
                        "function polygon(text[]) does not exist\n  Hint: No function matches the given name and argument types.", "42883");
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
                if (fn.args().size() == 2) {
                    Object a = executor.evalExpr(fn.args().get(0), ctx);
                    Object b = executor.evalExpr(fn.args().get(1), ctx);
                    if (a == null || b == null) return null;
                    return GeometricOperations.isHorizontal(
                            GeometricOperations.parsePoint(a.toString()),
                            GeometricOperations.parsePoint(b.toString()));
                }
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : GeometricOperations.isHorizontal(
                        GeometricOperations.parseLseg(arg.toString()));
            }
            case "is_vertical": {
                if (fn.args().size() == 2) {
                    Object a = executor.evalExpr(fn.args().get(0), ctx);
                    Object b = executor.evalExpr(fn.args().get(1), ctx);
                    if (a == null || b == null) return null;
                    return GeometricOperations.isVertical(
                            GeometricOperations.parsePoint(a.toString()),
                            GeometricOperations.parsePoint(b.toString()));
                }
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
