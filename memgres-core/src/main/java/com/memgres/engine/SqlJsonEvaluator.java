package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.JsonExistsExpr;
import com.memgres.engine.parser.ast.JsonQueryExpr;
import com.memgres.engine.parser.ast.JsonTableExpr;
import com.memgres.engine.parser.ast.JsonValueExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The SQL/JSON query expressions -- JSON_EXISTS, JSON_VALUE, JSON_QUERY -- and the columns of
 * JSON_TABLE, which are those same expressions written once per row.
 *
 * <p>All four are one operation with three answers to give. A path is applied to a document and
 * selects a list of items; what happens next is decided by three questions asked in a fixed order.
 *
 * <p>Did the walk fail? Then the ON ERROR clause answers, and nothing else runs. Did it select
 * nothing? Then the ON EMPTY clause answers. Otherwise the items are read, and how they are read
 * is the difference between the expressions: JSON_EXISTS only asks whether there were any,
 * JSON_VALUE wants exactly one scalar and hands back the value inside it, and JSON_QUERY wants a
 * document and has a wrapper clause to say what to do when the path selected more than one. The
 * reading can fail in turn -- more items than the clause can hold, a scalar where a document was
 * wanted, a value the RETURNING type will not take -- and then the ON ERROR clause answers after
 * all.
 *
 * <p>Two things happen before any of that and so are outside every ON clause. The path is parsed,
 * because a path that is not a path is a mistake in the statement rather than a fact about the
 * document; and the document is read, because the argument is json and text that is not json
 * never became one.
 */
final class SqlJsonEvaluator {

    private final AstExecutor executor;

    SqlJsonEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    // ------------------------------------------------------------------ the expressions

    Object exists(JsonExistsExpr je, RowContext ctx) {
        Object inputVal = executor.evalExpr(je.input(), ctx);
        Object pathVal = executor.evalExpr(je.path(), ctx);
        if (pathVal == null) return null;
        JsonPath path = JsonFunctions.parsePath(pathVal.toString());
        if (inputVal == null) return null;
        JsonValue document = readDocument(inputVal);
        try {
            JsonValue vars = executor.functionEvaluator.jsonFunctions.passingVars(je.passing(), ctx);
            return !JsonPathEvaluator.query(document, path, vars, false).isEmpty();
        } catch (MemgresException e) {
            if (je.onError() == null || je.onError() == JsonExistsExpr.OnBehavior.FALSE_VAL) {
                return Boolean.FALSE;
            }
            switch (je.onError()) {
                case TRUE_VAL:
                    return Boolean.TRUE;
                case UNKNOWN_VAL:
                    return null;
                default:
                    throw e;
            }
        }
    }

    Object value(JsonValueExpr jv, RowContext ctx) {
        return evaluate(jv.input(), jv.path(), jv.passing, readingOf(jv), ctx);
    }

    Object query(JsonQueryExpr jq, RowContext ctx) {
        return evaluate(jq.input(), jq.path(), jq.passing, readingOf(jq), ctx);
    }

    /** What JSON_VALUE asks for: one scalar, read as the SQL value inside it. */
    private static Reading readingOf(JsonValueExpr jv) {
        Reading r = new Reading();
        r.document = false;
        r.returningType = jv.returningType;
        r.onEmpty = jv.onEmpty;
        r.defaultOnEmpty = jv.defaultOnEmpty;
        r.onError = jv.onError;
        r.defaultOnError = jv.defaultOnError;
        return r;
    }

    /** What JSON_QUERY asks for: a document, with a wrapper to say how many items may make it. */
    private static Reading readingOf(JsonQueryExpr jq) {
        Reading r = new Reading();
        r.document = true;
        r.wrapper = jq.wrapper;
        r.omitQuotes = jq.quotes == JsonQueryExpr.QuotesBehavior.OMIT;
        r.returningType = jq.returningType;
        r.onEmpty = jq.onEmpty;
        r.defaultOnEmpty = jq.defaultOnEmpty;
        r.onError = jq.onError;
        r.defaultOnError = jq.defaultOnError;
        return r;
    }

    private Object evaluate(Expression input, Expression pathExpr, Map<String, Expression> passing,
                            Reading reading, RowContext ctx) {
        Object inputVal = executor.evalExpr(input, ctx);
        Object pathVal = executor.evalExpr(pathExpr, ctx);
        if (pathVal == null) return null;
        JsonPath path = JsonFunctions.parsePath(pathVal.toString());
        if (inputVal == null) return null;
        JsonValue document = readDocument(inputVal);
        JsonValue vars = executor.functionEvaluator.jsonFunctions.passingVars(passing, ctx);
        return read(document, path, vars, reading, ctx);
    }

    /**
     * The document argument, read as jsonb. The argument's type is json, so text that is not json
     * never became one and the failure belongs to the cast rather than to the expression: no ON
     * ERROR clause answers for it.
     */
    private static JsonValue readDocument(Object inputVal) {
        String text = inputVal.toString();
        ExprEvaluator.requireJson(text);
        return JsonParser.parseJsonb(text.trim());
    }

    // ------------------------------------------------------------------ the shared reading

    /**
     * What a JSON_VALUE, a JSON_QUERY or a JSON_TABLE column asks of the items its path selected.
     * A JSON_TABLE column is one of the two expressions written in the COLUMNS list, so it fills
     * this in the same way and is read by the same code.
     */
    static final class Reading {
        /** True for JSON_QUERY's reading, where an item is a document; false for JSON_VALUE's. */
        boolean document;
        JsonQueryExpr.WrapperBehavior wrapper = JsonQueryExpr.WrapperBehavior.NONE;
        boolean omitQuotes;
        /** The RETURNING type as it was written, or null for the reading's own default. */
        String returningType;
        JsonExistsExpr.OnBehavior onEmpty;
        Expression defaultOnEmpty;
        JsonExistsExpr.OnBehavior onError;
        Expression defaultOnError;
        /**
         * The JSON_TABLE column this reading belongs to, or null where it is an expression
         * written on its own. A column's complaints name it, because a COLUMNS list may hold
         * many of them and "JSON_VALUE" is not what anything in the statement is called.
         */
        String columnName;
    }

    /** What a JSON_TABLE column asks for: the same reading, named after the column. */
    static Reading readingOf(JsonTableExpr.JsonTableColumn col) {
        Reading r = new Reading();
        r.document = JsonTableExpr.JsonTableColumn.readsDocument(
                col.typeName, col.formatJson, col.wrapper, col.quotes);
        r.wrapper = col.wrapper;
        r.omitQuotes = col.quotes == JsonQueryExpr.QuotesBehavior.OMIT;
        r.returningType = col.typeName;
        r.onEmpty = col.onEmpty;
        r.defaultOnEmpty = col.defaultOnEmpty;
        r.onError = col.onError;
        r.defaultOnError = col.defaultOnError;
        r.columnName = col.name;
        return r;
    }

    private static String stripModifier(String typeSpec) {
        if (typeSpec == null) return null;
        int open = typeSpec.indexOf('(');
        return open < 0 ? typeSpec.trim() : typeSpec.substring(0, open).trim();
    }

    /** How a complaint names the reading that raised it. */
    private static String whose(Reading reading) {
        return reading.columnName == null
                ? (reading.document ? "in JSON_QUERY" : "in JSON_VALUE")
                : "for column \"" + reading.columnName + "\"";
    }

    /** Whether the path selects anything, which is the whole of JSON_EXISTS and of an EXISTS column. */
    Boolean readExists(JsonValue document, JsonPath path, JsonValue vars,
                       JsonExistsExpr.OnBehavior onError) {
        try {
            return !JsonPathEvaluator.query(document, path, vars, false).isEmpty();
        } catch (MemgresException e) {
            if (onError == JsonExistsExpr.OnBehavior.ERROR) throw e;
            if (onError == JsonExistsExpr.OnBehavior.TRUE_VAL) return Boolean.TRUE;
            return onError == JsonExistsExpr.OnBehavior.UNKNOWN_VAL ? null : Boolean.FALSE;
        }
    }

    Object read(JsonValue document, JsonPath path, JsonValue vars, Reading reading, RowContext ctx) {
        List<JsonValue> items;
        try {
            items = JsonPathEvaluator.query(document, path, vars, false);
        } catch (MemgresException e) {
            return onError(reading, e, ctx);
        }
        if (items.isEmpty()) return onEmpty(reading, ctx);
        try {
            return held(coerce(shaped(items, reading), reading), reading);
        } catch (MemgresException e) {
            return onError(reading, e, ctx);
        }
    }

    /**
     * The one item the clause hands on, out of everything the path selected.
     *
     * <p>JSON_VALUE wants a single scalar and has no way of saying otherwise. JSON_QUERY wants a
     * single document unless a wrapper was asked for: the unconditional one always makes an array,
     * and the conditional one makes one only where there is more than one item to hold -- a lone
     * item, of any kind, is handed on as it stands.
     */
    private static JsonValue shaped(List<JsonValue> items, Reading reading) {
        if (!reading.document) {
            if (items.size() > 1) {
                throw new MemgresException("JSON path expression " + whose(reading)
                        + " must return single scalar item", "22034");
            }
            JsonValue item = items.get(0);
            if (!item.isScalar()) {
                throw new MemgresException("JSON path expression " + whose(reading)
                        + " must return single scalar item", "2203F");
            }
            return item;
        }
        boolean wrap = reading.wrapper == JsonQueryExpr.WrapperBehavior.WITH_WRAPPER
                || (reading.wrapper == JsonQueryExpr.WrapperBehavior.WITH_CONDITIONAL_WRAPPER
                        && items.size() > 1);
        if (!wrap && items.size() > 1) {
            MemgresException e = new MemgresException("JSON path expression " + whose(reading)
                    + " must return single item when no wrapper is requested", "22034");
            e.setHint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.");
            throw e;
        }
        return wrap ? JsonValue.array(items) : items.get(0);
    }

    /**
     * The item as a SQL value of the RETURNING type.
     *
     * <p>Except where the type is itself a json type, the conversion goes through text and through
     * that type's own reader -- the same one a client's parameter or a COPY field goes through --
     * rather than through a cast. The difference shows on the character types: a cast to
     * varchar(2) shortens a longer string, while reading one refuses it.
     */
    private Object coerce(JsonValue item, Reading reading) {
        DataType target = typeNamed(reading.returningType);
        if (!reading.document) {
            // JSON_VALUE reads the value inside the item, so the JSON null -- the document holding
            // no value -- is SQL's null whatever type was asked for.
            if (item.isNull()) return null;
            if (target == DataType.JSON || target == DataType.JSONB) return written(item, target);
            return convert(scalarText(item), reading.returningType);
        }
        // OMIT QUOTES takes the characters out of a string item; every other kind of item, and
        // every string once it has been wrapped, is written as the document it is.
        String text = reading.omitQuotes && item.kind() == JsonValue.STRING
                ? item.asString() : written(item, target);
        if (reading.returningType == null || target == DataType.JSON || target == DataType.JSONB) {
            // Back through the reader, because OMIT QUOTES may have left something that is no
            // longer a document -- which is why JSON_QUERY of a string with the quotes taken off
            // is null rather than the characters.
            ExprEvaluator.requireJson(text);
            return target == DataType.JSON ? text : JsonWriter.jsonb(JsonParser.parseJsonb(text));
        }
        // An array type is filled from the array's items, one at a time -- unless OMIT QUOTES was
        // asked for, which says to hand on characters, and characters are read as an array
        // literal is. A document is not one, so JSON_QUERY of an array with the quotes taken off
        // is a malformed literal rather than the array.
        if (DataType.isArrayType(target) && !reading.omitQuotes) {
            return arrayOf(item, reading.returningType);
        }
        return convert(text, reading.returningType);
    }

    /** The item as its document text, spaced the way the type that will hold it spaces one. */
    private static String written(JsonValue item, DataType target) {
        return target == DataType.JSON ? JsonWriter.json(item) : JsonWriter.jsonb(item);
    }

    /**
     * A scalar item as the SQL text inside it, which is not the text the document wrote: a string
     * is its characters with the escapes decoded, a number is a numeric and so has no exponent,
     * and a boolean prints as booleans do.
     */
    private static String scalarText(JsonValue item) {
        switch (item.kind()) {
            case JsonValue.STRING:
                return item.asString();
            case JsonValue.NUMBER:
                return item.asNumber().toPlainString();
            case JsonValue.BOOLEAN:
                return item.asBoolean() ? "t" : "f";
            default:
                return item.asString();
        }
    }

    /**
     * An array item as a SQL array of the type asked for. The elements are read one at a time, so
     * an array of strings fills a text[] with the characters rather than with the quoted spellings
     * -- which is the one place the document reading and the value reading meet.
     */
    private Object arrayOf(JsonValue item, String typeSpec) {
        if (!item.isArray()) throw new MemgresException("expected JSON array", "22P02");
        List<Object> elements = new ArrayList<>(item.size());
        for (int i = 0; i < item.size(); i++) {
            JsonValue element = item.at(i);
            if (element.isNull()) elements.add(null);
            else if (element.isScalar()) elements.add(scalarText(element));
            else elements.add(JsonWriter.jsonb(element));
        }
        return convert(TypeCoercion.formatPgArray(elements), typeSpec);
    }

    /** Text read as the type named, or the text itself where nothing was named. */
    private Object convert(String text, String typeSpec) {
        if (typeSpec == null) return text;
        // The width is checked against the characters rather than against what the cast made of
        // them: a cast to varchar(2) shortens a longer string, while reading one refuses it.
        TypeCoercion.heldToItsType(text, typeSpec);
        return executor.castEvaluator.applyCast(text, typeSpec);
    }

    /**
     * Whether the path selected anything, as a value of the type an EXISTS column declares. The
     * answer is a boolean, so the numeric types take the one and the zero it counts as and every
     * other type takes the words it prints as.
     */
    Object existsAs(Boolean found, String typeSpec) {
        if (found == null) return null;
        DataType target = typeNamed(typeSpec);
        if (target == null || target == DataType.BOOLEAN) return found;
        if (NUMERIC_EXISTS_TYPES.contains(target)) return convert(found ? "1" : "0", typeSpec);
        return convert(found ? "true" : "false", typeSpec);
    }

    private static final java.util.Set<DataType> NUMERIC_EXISTS_TYPES =
            java.util.EnumSet.of(DataType.SMALLINT, DataType.INTEGER, DataType.BIGINT,
                    DataType.NUMERIC, DataType.REAL, DataType.DOUBLE_PRECISION);

    /** The converted value held to the width or scale the RETURNING type declares. */
    private static Object held(Object value, Reading reading) {
        return TypeCoercion.heldToItsType(value, reading.returningType);
    }

    /**
     * The type a RETURNING clause names, and the reading's own default where it named none:
     * JSON_QUERY answers with a document and JSON_VALUE with text.
     */
    private DataType typeNamed(String typeSpec) {
        if (typeSpec == null) return null;
        DataType declared = DataType.fromPgName(typeSpec.trim());
        if (declared != null) return declared;
        return DataType.fromPgName(typeSpec.replaceAll("\\(.*\\)", "").trim());
    }

    // ------------------------------------------------------------------ the ON clauses

    private Object onEmpty(Reading reading, RowContext ctx) {
        if (reading.defaultOnEmpty != null) return defaulted(reading.defaultOnEmpty, reading, ctx);
        if (reading.onEmpty == JsonExistsExpr.OnBehavior.ERROR) {
            throw new MemgresException("no SQL/JSON item found for specified path"
                    + (reading.columnName == null ? "" : " of column \"" + reading.columnName + "\""),
                    "22035");
        }
        return emptyOf(reading, reading.onEmpty, "ON EMPTY");
    }

    private Object onError(Reading reading, MemgresException raised, RowContext ctx) {
        if (reading.onError == JsonExistsExpr.OnBehavior.ERROR) throw raised;
        if (reading.defaultOnError != null) return defaulted(reading.defaultOnError, reading, ctx);
        return emptyOf(reading, reading.onError, "ON ERROR");
    }

    /**
     * A DEFAULT clause's value, as a value of the type the reading answers in.
     *
     * <p>The clause stands in for the reading, so it answers in the same type -- and it is cast
     * to it rather than read as it: {@code RETURNING int DEFAULT 5.6} answers 6. A value the
     * type will not take is a mistake in the statement and raises where it stands; the ON ERROR
     * clause has already had its say by the time this is reached.
     */
    private Object defaulted(Expression defaultValue, Reading reading, RowContext ctx) {
        Object value = executor.evalExpr(defaultValue, ctx);
        if (value == null || reading.returningType == null) return value;
        return executor.castEvaluator.applyCast(value, reading.returningType);
    }

    /**
     * The empty array or object an ON clause may ask for, and null for every other answer.
     *
     * <p>The document it names stands in for the item that was not read, so it is converted the
     * way an item of the same shape would be. A type that will not take it is a mistake in the
     * statement rather than something to fall back from -- the fallback is what this already is
     * -- so the refusal is reported against the clause and not handed to ON ERROR.
     */
    private Object emptyOf(Reading reading, JsonExistsExpr.OnBehavior behavior, String clause) {
        boolean array = behavior == JsonExistsExpr.OnBehavior.EMPTY_ARRAY;
        if (!array && behavior != JsonExistsExpr.OnBehavior.EMPTY_OBJECT) return null;
        Reading asDocument = new Reading();
        asDocument.document = true;
        asDocument.returningType = reading.returningType;
        try {
            return held(coerce(JsonParser.parseJsonb(array ? "[]" : "{}"), asDocument), reading);
        } catch (MemgresException e) {
            MemgresException refused = new MemgresException("could not coerce " + clause
                    + " expression (" + (array ? "EMPTY ARRAY" : "EMPTY OBJECT")
                    + ") to the RETURNING type", "42804");
            refused.setDetail(e.getMessage());
            throw refused;
        }
    }
}
