package com.memgres.engine.parser;

import com.memgres.engine.parser.ast.JsonExistsExpr;

import java.util.EnumSet;
import java.util.Set;

/**
 * What an {@code ON EMPTY} or {@code ON ERROR} clause is allowed to answer.
 *
 * <p>The clauses are written the same way wherever they appear, so the grammar reads them all
 * alike. What differs is the reading behind them: a scalar reading has nowhere to put an empty
 * array, and a truth reading has nothing to make of NULL. So the answer is read first and
 * refused afterwards, naming the set that would have been taken -- which is why these are not
 * syntax errors even though they are raised while parsing.
 */
final class JsonOnClauses {

    private JsonOnClauses() {
    }

    /** The answers one shape of reading takes, and how a complaint lists them. */
    enum Answers {
        SCALAR(EnumSet.of(JsonExistsExpr.OnBehavior.ERROR, JsonExistsExpr.OnBehavior.NULL_VAL),
                true, "Only ERROR, NULL, or DEFAULT expression"),
        DOCUMENT(EnumSet.of(JsonExistsExpr.OnBehavior.ERROR, JsonExistsExpr.OnBehavior.NULL_VAL,
                JsonExistsExpr.OnBehavior.EMPTY_ARRAY, JsonExistsExpr.OnBehavior.EMPTY_OBJECT),
                true, "Only ERROR, NULL, EMPTY ARRAY, EMPTY OBJECT, or DEFAULT expression"),
        TRUTH(EnumSet.of(JsonExistsExpr.OnBehavior.ERROR, JsonExistsExpr.OnBehavior.TRUE_VAL,
                JsonExistsExpr.OnBehavior.FALSE_VAL, JsonExistsExpr.OnBehavior.UNKNOWN_VAL),
                false, "Only ERROR, TRUE, FALSE, or UNKNOWN");

        private final Set<JsonExistsExpr.OnBehavior> allowed;
        private final boolean allowsDefault;
        private final String listed;

        Answers(Set<JsonExistsExpr.OnBehavior> allowed, boolean allowsDefault, String listed) {
            this.allowed = allowed;
            this.allowsDefault = allowsDefault;
            this.listed = listed;
        }

        /** Whether a truth reading, which has an ON ERROR clause and no ON EMPTY one. */
        boolean truth() {
            return this == TRUTH;
        }
    }

    /**
     * Refuses an answer the reading cannot give.
     *
     * @param answers what this reading takes
     * @param whom    how the complaint's detail names the reading, as PostgreSQL names it
     * @param column  the JSON_TABLE column the clause belongs to, or null for an expression
     * @param onEmpty whether the clause read was the ON EMPTY one
     * @param start   the word the answer began at, which is where the complaint points
     */
    static void require(Answers answers, String whom, String column, boolean onEmpty,
                        JsonExistsExpr.OnBehavior behavior, boolean isDefault, Token start) {
        if (isDefault ? answers.allowsDefault : answers.allowed.contains(behavior)) return;
        String clause = onEmpty ? "ON EMPTY" : "ON ERROR";
        ParseException refusal = ParseException.saying("invalid " + clause + " behavior"
                + (column == null ? "" : " for column \"" + column + "\""), start, "42601");
        refusal.setDetail(answers.listed + " is allowed in " + clause + " for " + whom + ".");
        throw refusal;
    }
}
