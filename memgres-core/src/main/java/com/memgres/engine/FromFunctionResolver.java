package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolves set-returning functions in FROM clauses (e.g., generate_series, unnest, jsonb_each).
 * Extracted from FromResolver to separate concerns.
 */
class FromFunctionResolver {

    /**
     * How many rows a temporal generate_series will build. Stopping short of the requested range
     * would return a shorter answer than was asked for with nothing to say so; past this point the
     * request is refused instead.
     */
    private static final long MAX_SERIES_ROWS = 10_000_000L;

    /**
     * Poll for cancellation once per 1024 generated rows. A series builds rows without evaluating
     * any expression, so it would otherwise run past statement_timeout unnoticed.
     */
    private static final long CANCEL_POLL_MASK = 1023L;

    private static MemgresException seriesTooLarge() {
        return new MemgresException(
                "generate_series would produce more than " + MAX_SERIES_ROWS + " rows", "54000");
    }
    private final AstExecutor executor;

    FromFunctionResolver(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * Resolve a set-returning function in FROM clause.
     * Marks the resulting virtual tables with SRF provenance ({@link Table#setFunctionResult})
     * so ExprEvaluator's attribute-notation fallback (alias.func ≡ func(alias)) only ever applies
     * to FROM-function aliases, never to ordinary table/subquery/VALUES/CTE aliases.
     */
    List<RowContext> resolveFunctionFrom(SelectStmt.FunctionFrom funcFrom) {
        // A FROM item is what the query reads, so its arguments are settled before there is any
        // row to group or to number. PostgreSQL says so before it even looks the function up,
        // and a TABLESAMPLE percentage — carried here as a function item — is the same clause.
        boolean spelledConstruct = funcFrom.functionName().startsWith("__");
        for (Expression arg : funcFrom.args()) {
            executor.selectExecutor.placementCheck.reject(arg, "functions in FROM");
            // A FROM item's own call produces the rows; one nested in its arguments would have to
            // produce them for it, and PostgreSQL says where a set-returning call may stand
            // instead. The spelled-out constructs -- ROWS FROM, JSON_TABLE, XMLTABLE, TABLESAMPLE
            // -- carry their items as arguments, and those items are at top level.
            if (!spelledConstruct && executor.selectExecutor.containsSrf(arg)) {
                throw new MemgresException(
                        "set-returning functions must appear at top level of FROM", "0A000");
            }
        }
        rejectOrdinalityWithColumnDefinitionList(funcFrom);
        // A set-returning call is resolved from its written arguments like any other, and
        // generate_series is declared over three numeric types: two untyped literals name none
        // of them.
        FunctionEvaluator.rejectAmbiguousBuiltin(executor, funcFrom.functionName().toLowerCase(),
                funcFrom.args(), null);
        List<RowContext> contexts = appendOrdinality(funcFrom, doResolveFunctionFrom(funcFrom));
        checkColumnAliasCount(funcFrom, contexts);
        // TABLESAMPLE binds the real stored table; never flag persistent tables.
        if (!funcFrom.functionName().toLowerCase().startsWith("__tablesample__:")) {
            // The columns carry whatever type the resolver read off the values it built, so what
            // the call itself settles is recorded beside them. See DefinedTypes. The call settles
            // the same thing for every row it produced, so it is worked out once.
            String[] settled = null;
            if (!contexts.isEmpty()) {
                // Every row of one FROM item is bound to the same relations, so the first row
                // names all of them; walking the rest set the same two properties again, once per
                // row, which for a long series is the only reason the rows had to exist.
                for (RowContext.TableBinding b : contexts.get(0).getBindings()) {
                    b.table().setFunctionResult(true);
                    int width = b.table().getColumns().size();
                    if (settled == null || settled.length != width) {
                        settled = executor.definedTypes.ofFunction(funcFrom, width);
                    }
                    b.table().setDefinedColumnTypes(settled);
                }
            }
        }
        return contexts;
    }

    /**
     * The ordinality column, added once for every function rather than by each function for
     * itself.
     *
     * <p>WITH ORDINALITY numbers the rows a FROM item produced, whatever produced them, so it is
     * the same column in every case: one bigint counting from 1, named by the alias after the
     * last of the function's own columns or {@code ordinality} when there is none. Each resolver
     * used to add it on its own account, which meant the ones that had never been asked about it
     * — {@code string_to_table}, {@code regexp_split_to_table}, {@code json_object_keys},
     * {@code generate_subscripts}, {@code regexp_matches}, a function returning TABLE — silently
     * dropped the column, and the alias list that named it then had one name too many for the
     * columns that were left.
     */
    private List<RowContext> appendOrdinality(SelectStmt.FunctionFrom funcFrom,
                                              List<RowContext> contexts) {
        if (!funcFrom.withOrdinality() || contexts.isEmpty()) return contexts;
        List<RowContext.TableBinding> first = contexts.get(0).getBindings();
        if (first.isEmpty()) return contexts;
        Table source = first.get(0).table();
        String bindAlias = first.get(0).alias();
        List<Column> base = source.getColumns();
        List<Column> cols = new ArrayList<>(base);
        List<String> aliases = funcFrom.columnAliases();
        String ordName = aliases != null && aliases.size() > base.size()
                ? stripColType(aliases.get(base.size())) : "ordinality";
        cols.add(new Column(ordName, DataType.BIGINT, true, false, null));
        Table numbered = new Table(source.getName(), cols);
        List<Object[]> rows = new ArrayList<>();
        List<RowContext> out = new ArrayList<>();
        long ord = 1;
        for (RowContext ctx : contexts) {
            List<RowContext.TableBinding> bindings = ctx.getBindings();
            Object[] src = bindings.isEmpty() ? new Object[0] : bindings.get(0).row();
            Object[] row = new Object[cols.size()];
            for (int i = 0; i < base.size(); i++) row[i] = i < src.length ? src[i] : null;
            row[base.size()] = ord++;
            rows.add(row);
            out.add(new RowContext(numbered, bindAlias, row));
        }
        numbered.replaceAllRows(rows);
        return out;
    }

    /**
     * WITH ORDINALITY and a column definition list describe the same alias list two ways — the
     * definition list says what the columns are and the ordinality column is one more than that —
     * so PostgreSQL refuses the pair and points at the spelling that can express both.
     */
    private static void rejectOrdinalityWithColumnDefinitionList(SelectStmt.FunctionFrom funcFrom) {
        if (!funcFrom.withOrdinality()) return;
        if (funcFrom.functionName().startsWith("__")) return;
        if (!hasColumnDefinitionList(funcFrom.columnAliases())) return;
        MemgresException e = PgErrors.syntax(
                "WITH ORDINALITY cannot be used with a column definition list");
        e.setHint("Put the column definition list inside ROWS FROM().");
        throw e;
    }

    /**
     * A column alias list cannot name more columns than the function produces. Extra aliases
     * are silently ignorable only in the sense that PG refuses them outright, which matters
     * for WITH ORDINALITY, where the ordinality column is easy to miscount.
     */
    private void checkColumnAliasCount(SelectStmt.FunctionFrom funcFrom, List<RowContext> contexts) {
        List<String> aliases = funcFrom.columnAliases();
        if (aliases == null || aliases.isEmpty() || contexts.isEmpty()) return;
        List<RowContext.TableBinding> bindings = contexts.get(0).getBindings();
        if (bindings.isEmpty()) return;
        int available = bindings.get(0).table().getColumns().size();
        if (aliases.size() > available) {
            String rel = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
            throw new MemgresException("table \"" + rel + "\" has " + available
                    + " columns available but " + aliases.size() + " columns specified", "42P10").suppressPosition();
        }
    }

    private List<RowContext> doResolveFunctionFrom(SelectStmt.FunctionFrom funcFrom) {
        String rawFname = funcFrom.functionName().toLowerCase();
        // A dot in a function's name separates the schema it was written under from the function
        // itself, and only the function is looked up. The TABLESAMPLE marker is not a function name
        // at all -- it carries the relation being sampled, and that relation may itself be written
        // schema.table -- so it keeps every part of its name and is still recognised as the marker.
        String fname = rawFname.contains(".") && !rawFname.startsWith("__tablesample__:")
                ? rawFname.substring(rawFname.lastIndexOf('.') + 1) : rawFname;
        String alias = funcFrom.alias() != null ? funcFrom.alias() : fname;
        List<String> rawColAliases = funcFrom.columnAliases();
        // Strip type info from column aliases for most functions (jsonb_to_record/json_to_record use raw aliases)
        List<String> colAliases = stripColTypes(rawColAliases);
        if (fname.equals("__json_table__")) return resolveJsonTable(funcFrom, alias);
        if (fname.equals("__xmltable__")) return resolveXmlTable(funcFrom, alias);
        if (fname.equals("__rows_from__")) return resolveRowsFrom(funcFrom, alias);
        if (SINGLE_VALUE_SRFS.contains(fname) && hasColumnDefinitionList(rawColAliases)) {
            throw onlyForRecord();
        }
        checkBuiltinColumnDefinitionList(fname, rawColAliases);
        List<Object> evalArgs = new ArrayList<>();
        for (Expression arg : funcFrom.args()) {
            evalArgs.add(executor.evalExpr(arg, null));
        }
        if (fname.equals("generate_series")) return resolveGenerateSeries(alias, colAliases, evalArgs);
        if (fname.equals("generate_subscripts")) {
            // The array is declared anyarray, so an argument with no type of its own -- a bare
            // literal, or a bare NULL -- leaves the declaration nothing to be resolved against,
            // and PostgreSQL will not guess one for it.
            Expression arrayArg = funcFrom.args().isEmpty() ? null : funcFrom.args().get(0);
            if (arrayArg instanceof Literal
                    && (((Literal) arrayArg).literalType() == Literal.LiteralType.STRING
                        || ((Literal) arrayArg).literalType() == Literal.LiteralType.NULL)) {
                throw new MemgresException(
                        "could not determine polymorphic type because input has type unknown", "42804");
            }
            return resolveGenerateSubscripts(alias, colAliases, evalArgs);
        }
        if (fname.equals("pg_indexam_has_property")) return resolvePgIndexamHasProperty(alias, evalArgs);
        if (fname.equals("pg_available_extension_versions")) return resolvePgAvailableExtensionVersions(alias);
        if (fname.equals("pg_show_all_settings")) return resolvePgShowAllSettings(alias);
        if (fname.equals("pg_get_loaded_modules")) return resolvePgGetLoadedModules(alias);
        if (fname.equals("pg_stat_get_backend_io")) return resolvePgStatGetBackendIo(alias, evalArgs);
        if (fname.equals("unnest")) return resolveUnnest(alias, colAliases, evalArgs, funcFrom.args());
        if (fname.equals("_pg_expandarray")) return resolveExpandArray(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_each") || fname.equals("jsonb_each_text") || fname.equals("json_each") || fname.equals("json_each_text"))
            return resolveJsonEach(fname, alias, colAliases, evalArgs);
        if (fname.equals("jsonb_to_recordset") || fname.equals("json_to_recordset") || fname.equals("jsonb_to_record") || fname.equals("json_to_record"))
            return resolveJsonToRecordset(alias, rawColAliases, evalArgs);
        if (fname.equals("json_populate_recordset") || fname.equals("jsonb_populate_recordset")
                || fname.equals("json_populate_record") || fname.equals("jsonb_populate_record"))
            return resolveJsonPopulateRecordset(funcFrom, alias, colAliases, evalArgs);
        if (fname.equals("populate_record"))
            return resolveHstorePopulateRecord(funcFrom, alias, evalArgs);
        if (fname.equals("regexp_matches")) return resolveRegexpMatches(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_path_query")) return resolveJsonbPathQuery(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_array_elements") || fname.equals("json_array_elements") ||
            fname.equals("jsonb_array_elements_text") || fname.equals("json_array_elements_text"))
            return resolveJsonArrayElements(fname, alias, colAliases, evalArgs);
        if (fname.equals("jsonb_object_keys") || fname.equals("json_object_keys"))
            return resolveJsonObjectKeys(fname, alias, colAliases, evalArgs);
        if (fname.equals("pg_options_to_table") || fname.equals("pg_catalog.pg_options_to_table"))
            return resolvePgOptionsToTable(alias, colAliases, evalArgs);
        if (fname.equals("pg_get_sequence_data") || fname.equals("pg_catalog.pg_get_sequence_data"))
            return resolvePgGetSequenceData(alias, colAliases, evalArgs);
        if (fname.equals("string_to_table")) return resolveStringToTable(alias, colAliases, evalArgs);
        if (fname.equals("regexp_split_to_table")) return resolveRegexpSplitToTable(alias, colAliases, evalArgs);
        if (fname.startsWith("__tablesample__:")) return resolveTablesample(fname, alias, evalArgs);
        if (fname.equals("pg_create_logical_replication_slot")) return resolveCreateLogicalReplicationSlot(alias, colAliases, evalArgs);
        if (fname.equals("pg_create_physical_replication_slot")) return resolveCreatePhysicalReplicationSlot(alias, colAliases, evalArgs);
        if (fname.equals("pg_ls_dir")) return resolvePgLsDir(alias, colAliases);
        if (fname.equals("pg_ls_logdir") || fname.equals("pg_ls_waldir") ||
            fname.equals("pg_ls_tmpdir") || fname.equals("pg_ls_archive_statusdir"))
            return resolvePgLsDirRecord(alias, colAliases);
        if (fname.equals("pg_partition_tree")) return resolvePgPartitionTree(alias, colAliases, evalArgs);
        if (fname.equals("pg_partition_ancestors")) return resolvePgPartitionAncestors(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_path_query_tz")) return resolveJsonbPathQuery(alias, colAliases, evalArgs);
        if (fname.equals("ts_stat")) return resolveTsStat(alias, colAliases, evalArgs);
        if (fname.equals("ts_debug")) return resolveTsDebug(alias, colAliases, evalArgs);
        if (fname.equals("ts_parse")) return resolveTsParse(alias, colAliases, evalArgs);
        if (fname.equals("ts_token_type")) return resolveTsTokenType(alias, colAliases, evalArgs);
        if (fname.equals("pg_listening_channels")) return resolvePgListeningChannels(alias);
        if (fname.equals("skeys")) return resolveHstoreSkeys(alias, colAliases, evalArgs);
        if (fname.equals("svals")) return resolveHstoreSvals(alias, colAliases, evalArgs);
        if (fname.equals("each")) return resolveHstoreEach(alias, colAliases, evalArgs);

        // Try user-defined function
        PgFunction userFunc = executor.database.getFunction(fname);
        if (userFunc != null) {
            // A procedure produces no rows to read from, so it cannot stand in a FROM list.
            // Only calls written as expressions were checked, so reading one here answered with
            // an empty relation rather than saying what it is.
            if (userFunc.isProcedure()) {
                StringBuilder types = new StringBuilder();
                for (PgFunction.Param p : userFunc.getParams()) {
                    if ("OUT".equalsIgnoreCase(p.mode())) continue;
                    if (types.length() > 0) types.append(", ");
                    types.append(p.typeName() == null ? "any"
                            : CatalogSystemFunctions.readableTypeName(p.typeName()));
                }
                throw new MemgresException(fname + "(" + types + ") is a procedure"
                        + "\n  Hint: To call a procedure, use CALL.", "42809");
            }
            checkRecordColumnDefinitionList(userFunc, funcFrom);
            // Raw, not stripped: a column definition list gives the columns their types too.
            return resolveUserFunction(userFunc, alias, rawColAliases, evalArgs);
        }

        return resolveScalarFunctionInFrom(funcFrom, fname, alias, colAliases);
    }

    /**
     * A function returning bare {@code record} has no column names or types of its own, so the
     * caller has to supply them; one that declares OUT or TABLE parameters already has them, and
     * PG rejects a second, possibly contradicting, description of the same row; and one that
     * returns a named type has nothing a description could add.
     *
     * <p>What counts as a description is a column definition list -- names with types. A bare
     * alias list only renames the columns that are there, so it is allowed wherever a table
     * alias is, and it does not satisfy the requirement a {@code record} result places on the
     * caller: {@code SELECT * FROM f() AS t(p, q)} is still missing the types.
     */
    private void checkRecordColumnDefinitionList(PgFunction userFunc, SelectStmt.FunctionFrom funcFrom) {
        boolean definitionList = hasColumnDefinitionList(funcFrom.columnAliases());
        if (userFunc.hasOutParams()) {
            if (definitionList) {
                throw PgErrors.syntax(
                        "a column definition list is redundant for a function with OUT parameters");
            }
            return;
        }
        if (userFunc.declaresRecordResult()) {
            if (!definitionList) {
                throw PgErrors.syntax(
                        "a column definition list is required for functions returning \"record\"");
            }
            return;
        }
        if (definitionList) throw onlyForRecord();
    }

    /**
     * The same question {@link #checkRecordColumnDefinitionList} asks of a declared function, asked
     * of a built-in one.
     *
     * <p>Two named sets rather than a rule, because what a built-in's signature says is not
     * something this engine records: {@code json_each} declares its {@code key} and {@code value}
     * as OUT parameters and a list describing them again is redundant, while {@code json_to_record}
     * returns bare {@code record} and cannot answer without one. Every name here was measured
     * against PostgreSQL; a built-in on neither list keeps whatever it accepted before.
     */
    private static void checkBuiltinColumnDefinitionList(String fname, List<String> rawColAliases) {
        boolean definitionList = hasColumnDefinitionList(rawColAliases);
        if (RECORD_RESULT_SRFS.contains(fname)) {
            if (!definitionList) {
                throw PgErrors.syntax(
                        "a column definition list is required for functions returning \"record\"");
            }
            return;
        }
        if (definitionList && OUT_PARAM_SRFS.contains(fname)) {
            throw PgErrors.syntax(
                    "a column definition list is redundant for a function with OUT parameters");
        }
    }

    /** Built-ins that return bare {@code record} and so have to be told what their columns are. */
    private static final Set<String> RECORD_RESULT_SRFS = new HashSet<>(Arrays.asList(
            "json_to_record", "jsonb_to_record", "json_to_recordset", "jsonb_to_recordset"));

    /** Built-ins whose own signature already names their columns. */
    private static final Set<String> OUT_PARAM_SRFS = new HashSet<>(Arrays.asList(
            "json_each", "json_each_text", "jsonb_each", "jsonb_each_text", "each"));

    private static MemgresException onlyForRecord() {
        return PgErrors.syntax(
                "a column definition list is only allowed for functions returning \"record\"");
    }

    /**
     * The type the single column of a set-returning FROM item carries, or null when unknown.
     *
     * <p>Needed where the item produced no row and there is nothing to read the type off: an
     * outer join still has to describe the columns it padded with NULLs, and describing them as
     * text made {@code LEFT JOIN generate_series(1,0) AS a(g)} answer a text column where
     * PostgreSQL answers an integer one. Only the two functions whose result type is decided by
     * their arguments are answered for; anything else keeps the type its caller already assumed.
     */
    DataType singleColumnType(SelectStmt.FunctionFrom funcFrom) {
        String fname = FunctionEvaluator.stripSchemaPrefix(funcFrom.functionName().toLowerCase());
        if (funcFrom.args().isEmpty()) return null;
        if (fname.equals("unnest") && funcFrom.args().size() == 1) {
            return DataType.elementOf(inferInOuterScope(funcFrom.args().get(0)));
        }
        if (fname.equals("generate_series")) {
            DataType start = inferInOuterScope(funcFrom.args().get(0));
            // The numeric overload answers int4 unless a bound is wider; the date and timestamp
            // ones answer their own type, which is what the bound already is.
            if (start == DataType.BIGINT || start == DataType.NUMERIC || start == DataType.DATE
                    || start == DataType.TIMESTAMP || start == DataType.TIMESTAMPTZ) {
                return start == DataType.DATE ? DataType.TIMESTAMPTZ : start;
            }
            return DataType.INTEGER;
        }
        return null;
    }

    /** The type of an expression, read against whatever row context encloses this FROM item. */
    private DataType inferInOuterScope(Expression expr) {
        List<RowContext.TableBinding> bindings = executor.outerContextStack.isEmpty()
                ? new ArrayList<RowContext.TableBinding>()
                : executor.outerContextStack.peek().getBindings();
        try {
            return executor.exprEvaluator.inferTypeFromContext(expr, bindings);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** A column definition list names types; an alias list only renames what is already there. */
    static boolean hasColumnDefinitionList(List<String> aliases) {
        if (aliases == null) return false;
        for (String alias : aliases) {
            if (alias != null && alias.indexOf(' ') > 0) return true;
        }
        return false;
    }

    /**
     * The built-in set-returning functions whose rows are one value, not a record. None of them
     * can be given a column definition list, for the same reason a table cannot: the columns are
     * already decided. Named one by one rather than by exclusion so that a function whose shape
     * this file does not know stays as permissive as it was.
     */
    static final Set<String> SINGLE_VALUE_SRFS = new HashSet<>(Arrays.asList(
            "generate_series", "generate_subscripts", "unnest", "string_to_table",
            "regexp_split_to_table", "json_object_keys", "jsonb_object_keys",
            "json_array_elements", "jsonb_array_elements",
            "json_array_elements_text", "jsonb_array_elements_text",
            "jsonb_path_query", "jsonb_path_query_tz", "skeys", "svals"));

    /**
     * PG puts no set-returning requirement on a function in FROM: a plain scalar call is simply
     * a one-row, one-column relation named after the function.
     */
    private List<RowContext> resolveScalarFunctionInFrom(SelectStmt.FunctionFrom funcFrom, String fname,
                                                        String alias, List<String> colAliases) {
        Object value = executor.evalExpr(new FunctionCallExpr(funcFrom.functionName(), funcFrom.args()), null);
        DataType type = value == null ? DataType.TEXT : TypeCoercion.inferType(value);
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(firstColAlias(colAliases, alias), type, true, false, null));
        Object[] row = new Object[]{value};
        Table virtualTable = new Table(alias, cols);
        virtualTable.insertRow(row);
        List<RowContext> contexts = new ArrayList<>();
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- generate_series ----

    private List<RowContext> resolveGenerateSeries(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object stepObj = evalArgs.size() > 2 ? evalArgs.get(2) : null;
        // Every generate_series overload is strict: a NULL bound or step produces no rows at all.
        // The overloads read their arguments straight, so a NULL one reached a conversion that
        // dereferenced it and the NullPointerException escaped as an internal error.
        for (Object arg : evalArgs) {
            if (arg == null) return emptySeries(alias, colAliases, evalArgs);
        }
        // A zero step can never reach the stop value, so PG rejects it rather than looping
        if (stepObj instanceof Number && ((Number) stepObj).doubleValue() == 0.0) {
            throw new MemgresException("step size cannot equal zero", "22023");
        }
        if (stepObj instanceof PgInterval && ((PgInterval) stepObj).isZero()) {
            throw new MemgresException("step size cannot equal zero", "22023");
        }
        // An infinite step is no more a distance between two values than a zero one is: it steps
        // straight past the stop value, which made the series the one row it started on.
        if (stepObj instanceof PgInterval && ((PgInterval) stepObj).isInfinite()) {
            throw new MemgresException("step size cannot be infinite", "22023");
        }
        // OffsetDateTime (timestamptz) overload
        if (evalArgs.get(0) instanceof java.time.OffsetDateTime) {
            java.time.OffsetDateTime tzStart = (java.time.OffsetDateTime) evalArgs.get(0);
            java.time.OffsetDateTime tzStop = evalArgs.get(1) instanceof java.time.OffsetDateTime
                    ? (java.time.OffsetDateTime) evalArgs.get(1) : TypeCoercion.toOffsetDateTime(evalArgs.get(1));
            PgInterval ivStep = stepObj != null ? TypeCoercion.toInterval(stepObj) : new PgInterval(0, 1, 0);
            boolean ascending = !ivStep.isNegative();
            String colName = firstColAlias(colAliases, alias);
            List<Column> cols = new ArrayList<>();
            cols.add(new Column(colName, DataType.TIMESTAMPTZ, true, false, null));
            Table virtualTable = new Table(alias, cols);
            long tzStepMicros = SeriesRows.fixedStepMicros(ivStep);
            if (tzStepMicros != 0) {
                return publishSeries(virtualTable, alias,
                        SeriesRows.ofTimestampTzs(tzStart, tzStop, tzStepMicros));
            }
            List<Object[]> rows = new ArrayList<>();
            List<RowContext> contexts = new ArrayList<>();
            java.time.OffsetDateTime cur = tzStart;
            for (long guard = 0; guard < MAX_SERIES_ROWS; guard++) {
                if ((guard & CANCEL_POLL_MASK) == 0) StatementCancel.check();
                if (ascending ? cur.isAfter(tzStop) : cur.isBefore(tzStop)) break;
                Object[] row = new Object[]{cur};
                rows.add(row);
                contexts.add(new RowContext(virtualTable, alias, row));
                java.time.OffsetDateTime next = ivStep.addTo(cur);
                if (next.isEqual(cur)) break;
                cur = next;
                if (guard == MAX_SERIES_ROWS - 1) throw seriesTooLarge();
            }
            virtualTable.replaceAllRows(rows);
            return contexts;
        }
        // Date/timestamp overload
        if (evalArgs.get(0) instanceof java.time.LocalDate || evalArgs.get(0) instanceof java.time.LocalDateTime
                || (stepObj instanceof PgInterval)) {
            boolean dateInput = evalArgs.get(0) instanceof java.time.LocalDate;
            java.time.LocalDateTime dtStart = dateInput ? ((java.time.LocalDate) evalArgs.get(0)).atStartOfDay() : TypeCoercion.toLocalDateTime(evalArgs.get(0));
            java.time.LocalDateTime dtStop = evalArgs.get(1) instanceof java.time.LocalDate ? ((java.time.LocalDate) evalArgs.get(1)).atStartOfDay() : TypeCoercion.toLocalDateTime(evalArgs.get(1));
            // PostgreSQL walks the series a step at a time and never tests for a bound it cannot
            // reach, so an infinite stop carries the walk off the end of the timestamp range and
            // is reported as that -- not as a series too long to hold, which is memgres's own
            // limit and not one PostgreSQL has.
            if (dtStop.equals(TypeCoercion.TIMESTAMP_INFINITY)
                    || dtStop.equals(TypeCoercion.TIMESTAMP_NEG_INFINITY)) {
                throw new MemgresException("timestamp out of range", "22008");
            }
            PgInterval ivStep = stepObj != null ? TypeCoercion.toInterval(stepObj) : new PgInterval(0, 1, 0);
            boolean ascending = !ivStep.isNegative();
            String colName = firstColAlias(colAliases, alias);
            // DATE input → timestamptz (PG promotes), TIMESTAMP input → timestamp
            List<Column> cols = new ArrayList<>();
            cols.add(new Column(colName, dateInput ? DataType.TIMESTAMPTZ : DataType.TIMESTAMP, true, false, null));
            Table virtualTable = new Table(alias, cols);
            long stepMicros = SeriesRows.fixedStepMicros(ivStep);
            if (stepMicros != 0) {
                return publishSeries(virtualTable, alias,
                        SeriesRows.ofTimestamps(dtStart, dtStop, stepMicros, dateInput));
            }
            List<Object[]> rows = new ArrayList<>();
            List<RowContext> contexts = new ArrayList<>();
            java.time.LocalDateTime cur = dtStart;
            for (long guard = 0; guard < MAX_SERIES_ROWS; guard++) {
                if ((guard & CANCEL_POLL_MASK) == 0) StatementCancel.check();
                if (ascending ? cur.isAfter(dtStop) : cur.isBefore(dtStop)) break;
                Object val = dateInput ? cur.atZone(java.time.ZoneOffset.UTC).toOffsetDateTime() : cur;
                Object[] row = new Object[]{val};
                rows.add(row);
                contexts.add(new RowContext(virtualTable, alias, row));
                java.time.LocalDateTime next = ivStep.addTo(cur);
                if (next.isEqual(cur)) break;
                cur = next;
                if (guard == MAX_SERIES_ROWS - 1) throw seriesTooLarge();
            }
            virtualTable.replaceAllRows(rows);
            return contexts;
        }
        // Numeric overload: a fractional bound or step must not be truncated to bigint
        if (evalArgs.get(0) instanceof java.math.BigDecimal
                || evalArgs.get(1) instanceof java.math.BigDecimal
                || stepObj instanceof java.math.BigDecimal) {
            java.math.BigDecimal nStart = TypeCoercion.toBigDecimal(evalArgs.get(0));
            java.math.BigDecimal nStop = TypeCoercion.toBigDecimal(evalArgs.get(1));
            java.math.BigDecimal nStep = stepObj != null
                    ? TypeCoercion.toBigDecimal(stepObj) : java.math.BigDecimal.ONE;
            String numColName = firstColAlias(colAliases, alias);
            List<Column> numCols = new ArrayList<>();
            numCols.add(new Column(numColName, DataType.NUMERIC, true, false, null));
            Table numTable = new Table(alias, numCols);
            // Each value is the start plus so many steps, which carries the scale that adding the
            // step that many times carries: 1.0, 1.25, 1.50, ... as PostgreSQL prints them.
            return publishSeries(numTable, alias, SeriesRows.ofNumerics(nStart, nStop, nStep));
        }
        try {
            executor.toLong(evalArgs.get(0));
            executor.toLong(evalArgs.get(1));
        } catch (MemgresException e) {
            if (!"22P02".equals(e.getSqlState())) throw e;
            throw new MemgresException("function generate_series(unknown, unknown) is not unique", "42725");
        }
        long start = executor.toLong(evalArgs.get(0));
        long stop = executor.toLong(evalArgs.get(1));
        long step = evalArgs.size() > 2 ? executor.toLong(evalArgs.get(2)) : 1;

        // PostgreSQL declares generate_series over int4 and over int8, and picks the int8 one as
        // soon as an argument is already a bigint -- so the rows it answers are bigints too, and
        // calling them int4 made the driver read a column PostgreSQL describes as int8.
        DataType seriesType = DataType.INTEGER;
        for (Object seriesArg : evalArgs) {
            if (seriesArg instanceof Long || seriesArg instanceof java.math.BigInteger) {
                seriesType = DataType.BIGINT;
            }
        }
        String colName = firstColAlias(colAliases, alias);
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(colName, seriesType, true, false, null));
        Table virtualTable = new Table(alias, cols);
        if (step == 0) return publishSeries(virtualTable, alias, new ArrayList<Object[]>());
        return publishSeries(virtualTable, alias,
                SeriesRows.ofIntegers(start, stop, step, seriesType == DataType.BIGINT));
    }

    /**
     * The rows of a series, handed on as the FROM item's contexts and left on the virtual table
     * for anything that reads it there.
     *
     * <p>Neither list holds the rows: they are worked out as they are read, so a series costs what
     * the query reads of it rather than what it spans. That is what lets a series be longer than
     * memgres would have held -- PostgreSQL has no size past which it refuses one -- and what
     * makes a LIMIT over a long series cost the rows it asked for.
     */
    private List<RowContext> publishSeries(Table virtualTable, String alias, List<Object[]> rows) {
        virtualTable.publishGeneratedRows(rows);
        return SeriesRows.contextsOver(virtualTable, alias, rows);
    }

    /**
     * No rows at all, which is what a strict set-returning function answers when an argument is
     * NULL. The column still has to be named and typed, because a caller may describe the result
     * without ever reading a row from it.
     */
    private List<RowContext> emptySeries(String alias, List<String> colAliases, List<Object> evalArgs) {
        DataType type = DataType.INTEGER;
        for (Object arg : evalArgs) {
            if (arg != null) {
                type = TypeCoercion.inferType(arg);
                break;
            }
        }
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(firstColAlias(colAliases, alias), type, true, false, null));
        return publishSeries(new Table(alias, cols), alias, new ArrayList<Object[]>());
    }

    // ---- generate_subscripts ----

    private List<RowContext> resolveStringToTable(String alias, List<String> colAliases, List<Object> evalArgs) {
        return oneColumnRows(alias, colAliases, stringToTableValues(evalArgs));
    }

    /**
     * The values {@code string_to_table} produces, one per row.
     *
     * <p>Shared with the select-list path: a set-returning function is the same function wherever
     * it is written, and PostgreSQL answers {@code SELECT string_to_table('a,b,c', ',')} with the
     * three rows that {@code FROM string_to_table('a,b,c', ',')} answers with.
     */
    static List<Object> stringToTableValues(List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function string_to_table() requires at least 2 arguments", "42883");
        Object strObj = evalArgs.get(0);
        Object delimObj = evalArgs.size() > 1 ? evalArgs.get(1) : null;
        List<Object> values = new ArrayList<>();
        if (strObj == null) return values;
        String str = strObj.toString();
        // PG: string_to_table('', delim) returns 0 rows when the input string is empty
        if (str.isEmpty() && delimObj != null && delimObj.toString().length() > 0) return values;
        if (delimObj == null) {
            // NULL delimiter: each character as separate row
            for (int i = 0; i < str.length(); i++) values.add(String.valueOf(str.charAt(i)));
            return values;
        }
        String delim = delimObj.toString();
        String nullStr = evalArgs.size() > 2 && evalArgs.get(2) != null ? evalArgs.get(2).toString() : null;
        String[] parts = delim.isEmpty() ? new String[]{ str } : str.split(java.util.regex.Pattern.quote(delim), -1);
        for (String part : parts) {
            values.add(nullStr != null && part.equals(nullStr) ? null : part);
        }
        return values;
    }

    // ---- regexp_split_to_table ----

    private List<RowContext> resolveRegexpSplitToTable(String alias, List<String> colAliases, List<Object> evalArgs) {
        return oneColumnRows(alias, colAliases, regexpSplitToTableValues(evalArgs));
    }

    /** The values {@code regexp_split_to_table} produces, one per row. See above. */
    static List<Object> regexpSplitToTableValues(List<Object> evalArgs) {
        if (evalArgs.size() < 2) throw new MemgresException("function regexp_split_to_table() requires at least 2 arguments", "42883");
        Object strObj = evalArgs.get(0);
        Object patternObj = evalArgs.get(1);
        List<Object> values = new ArrayList<>();
        if (strObj == null || patternObj == null) return values;
        if (evalArgs.size() > 2 && evalArgs.get(2) == null) return values;
        String flags = evalArgs.size() > 2 ? evalArgs.get(2).toString() : "";
        for (String part : PgRegex.split(strObj.toString(), PgRegex.compile(patternObj.toString(),
                PgRegex.parseFlags(flags, false, "regexp_split_to_table")))) {
            values.add(part);
        }
        return values;
    }

    /** One row per value, in a single text column named for the alias or the column alias. */
    private List<RowContext> oneColumnRows(String alias, List<String> colAliases, List<Object> values) {
        Column col = new Column(firstColAlias(colAliases, alias), DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        for (Object value : values) {
            Object[] row = new Object[]{ value };
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    /**
     * The elements along one dimension of an array, or null when the array has no such
     * dimension — which is what {@code generate_subscripts} needs to know to stay empty.
     */
    private static List<Object> dimensionElements(Object arr, int dim) {
        if (dim < 1) return null;
        List<Object> elements = toElementList(arr);
        if (dim == 1) return elements;
        if (elements.isEmpty()) return null;
        Object first = elements.get(0);
        boolean nested = first instanceof List
                || (first instanceof String && ((String) first).startsWith("{") && ((String) first).endsWith("}"));
        if (!nested) return null;
        return dimensionElements(first, dim - 1);
    }

    private List<RowContext> resolveGenerateSubscripts(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function generate_subscripts() does not exist", "42883");
        Object arrObj = evalArgs.get(0);
        if (evalArgs.size() > 1 && evalArgs.get(1) == null) return new ArrayList<>();
        int dim = evalArgs.size() > 1 ? executor.toInt(evalArgs.get(1)) : 1;
        boolean reverse = evalArgs.size() > 2 && executor.isTruthy(evalArgs.get(2));

        List<Object> elements;
        int lowerBound = 1;
        if (arrObj instanceof String && ((String) arrObj).contains("=") && ((String) arrObj).startsWith("[")) {
            String s = (String) arrObj;
            int eqIdx = s.indexOf('=');
            String bounds = s.substring(0, eqIdx);
            String content = s.substring(eqIdx + 1);
            String[] parts = bounds.substring(1, bounds.length() - 1).split(":");
            if (parts.length == 2) {
                lowerBound = Integer.parseInt(parts[0].trim());
            }
            elements = dimensionElements(content, dim);
        } else {
            elements = dimensionElements(arrObj, dim);
        }
        // A dimension the array does not have yields no subscripts at all
        if (elements == null) return new ArrayList<>();

        int lo = lowerBound;
        int hi = lo + elements.size() - 1;

        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.INTEGER, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        if (lo <= hi) {
            if (reverse) {
                for (int i = hi; i >= lo; i--) {
                    Object[] row = new Object[]{i};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            } else {
                for (int i = lo; i <= hi; i++) {
                    Object[] row = new Object[]{i};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- pg_indexam_has_property ----

    private List<RowContext> resolvePgIndexamHasProperty(String alias, List<Object> evalArgs) {
        boolean result = true;
        if (evalArgs.size() >= 2) {
            String prop = String.valueOf(evalArgs.get(1)).toLowerCase();
            switch (prop) {
                case "can_order":
                case "can_unique":
                case "can_multi_col":
                    result = true;
                    break;
                default:
                    result = false;
                    break;
            }
        }
        Column col = new Column(alias, DataType.BOOLEAN, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        Object[] row = new Object[]{result};
        virtualTable.insertRow(row);
        return Cols.listOf(new RowContext(virtualTable, alias, row));
    }

    // ---- pg_available_extension_versions ----

    /**
     * {@code pg_get_loaded_modules()}, PostgreSQL 18's list of the shared libraries a backend has
     * loaded.
     *
     * <p>memgres loads none — everything it implements is its own — so the answer is no rows.
     * PostgreSQL's own answer is no rows too until something loads a module, which is why what
     * matters here is that the call resolves rather than what it counts.
     */
    private List<RowContext> resolvePgGetLoadedModules(String alias) {
        List<Column> cols = Cols.listOf(
                new Column("module_name", DataType.TEXT, true, false, null),
                new Column("version", DataType.TEXT, true, false, null),
                new Column("file_name", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        return SeriesRows.contextsOver(virtualTable, alias, new ArrayList<Object[]>());
    }

    /**
     * {@code pg_stat_get_backend_io(pid)}, the I/O one backend has done, as {@code pg_stat_io}
     * reports it for all of them together.
     *
     * <p>A pid that is nobody's answers nothing, which is how PostgreSQL says it does not know the
     * backend. A pid that is a backend's answers the rows that backend could have done I/O in,
     * with the counters memgres has for them, which is none.
     */
    private List<RowContext> resolvePgStatGetBackendIo(String alias, List<Object> evalArgs) {
        Table source = executor.getSystemCatalog().resolve("pg_catalog", "pg_stat_io");
        List<Column> cols = new ArrayList<>();
        for (Column c : source.getColumns()) {
            if (!c.getName().equals("backend_type")) cols.add(c);
        }
        Table virtualTable = new Table(alias, cols);
        List<Object[]> rows = new ArrayList<>();
        Object pidArg = evalArgs.isEmpty() ? null : evalArgs.get(0);
        boolean known = false;
        if (pidArg != null) {
            int pid = TypeCoercion.toInteger(pidArg).intValue();
            for (Session other : executor.database.getActiveSessions()) {
                if (other.getPid() == pid) { known = true; break; }
            }
        }
        if (known) {
            List<Column> sourceCols = source.getColumns();
            for (Object[] ioRow : source.getRows()) {
                if (!"client backend".equals(ioRow[0])) continue;
                Object[] row = new Object[cols.size()];
                for (int i = 1; i < sourceCols.size(); i++) row[i - 1] = ioRow[i];
                rows.add(row);
            }
        }
        virtualTable.replaceAllRows(rows);
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] row : rows) contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    private List<RowContext> resolvePgAvailableExtensionVersions(String alias) {
        List<Column> cols = Cols.listOf(
                new Column("name", DataType.TEXT, true, false, null),
                new Column("version", DataType.TEXT, true, false, null),
                new Column("superuser", DataType.BOOLEAN, true, false, null),
                new Column("trusted", DataType.BOOLEAN, true, false, null),
                new Column("relocatable", DataType.BOOLEAN, true, false, null),
                new Column("schema", DataType.TEXT, true, false, null),
                new Column("requires", DataType.TEXT, true, false, null),
                new Column("comment", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        Object[] row = new Object[]{"plpgsql", "1.0", true, true, false, "pg_catalog", null, "PL/pgSQL procedural language"};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- pg_show_all_settings ----

    private List<RowContext> resolvePgShowAllSettings(String alias) {
        List<Column> cols = Cols.listOf(
                new Column("name", DataType.TEXT, true, false, null),
                new Column("setting", DataType.TEXT, true, false, null),
                new Column("unit", DataType.TEXT, true, false, null),
                new Column("category", DataType.TEXT, true, false, null),
                new Column("short_desc", DataType.TEXT, true, false, null),
                new Column("extra_desc", DataType.TEXT, true, false, null),
                new Column("context", DataType.TEXT, true, false, null),
                new Column("vartype", DataType.TEXT, true, false, null),
                new Column("source", DataType.TEXT, true, false, null),
                new Column("min_val", DataType.TEXT, true, false, null),
                new Column("max_val", DataType.TEXT, true, false, null),
                new Column("enumvals", DataType.TEXT_ARRAY, true, false, null),
                new Column("boot_val", DataType.TEXT, true, false, null),
                new Column("reset_val", DataType.TEXT, true, false, null),
                new Column("sourcefile", DataType.TEXT, true, false, null),
                new Column("sourceline", DataType.INTEGER, true, false, null),
                new Column("pending_restart", DataType.BOOLEAN, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        GucSettings guc = executor.session != null ? executor.session.getGucSettings() : new GucSettings();
        for (Map.Entry<String, String> e : guc.getAll().entrySet()) {
            String name = e.getKey();
            GucSettings.Def def = GucSettings.definition(name);
            String value = guc.get(name);
            if (value == null) value = e.getValue();
            Object[] row = new Object[cols.size()];
            row[0] = guc.getCanonicalName(name);
            row[1] = value;
            row[2] = def != null ? def.unit : null;
            row[3] = def != null ? def.category : "Customized Options";
            row[4] = def != null ? def.shortDesc : null;
            row[5] = def != null ? def.extraDesc : null;
            row[6] = def != null ? def.context : "user";
            row[7] = def != null ? def.vartype : "string";
            // The same three answers pg_settings gives, from the same facts: a transaction's own
            // settings are imposed by the transaction rather than chosen by the session.
            row[8] = guc.hasSessionOverride(name) ? "session"
                    : (SessionExecutor.isTransactionScopedGuc(name) ? "override" : "default");
            row[9] = def != null ? def.minVal : null;
            row[10] = def != null ? def.maxVal : null;
            row[11] = def != null ? def.enumVals : null;
            row[12] = def != null ? def.bootVal : value;
            row[13] = def != null ? guc.getResetValue(name) : value;
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    /**
     * information_schema._pg_expandarray(anyarray) pairs each element with its 1-based index.
     * ORM-generated metadata SQL uses it to walk pg_index.indkey.
     */
    private List<RowContext> resolveExpandArray(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) {
            throw new MemgresException("function _pg_expandarray() does not exist"
                    + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        String valueCol = (colAliases != null && !colAliases.isEmpty()) ? colAliases.get(0) : "x";
        String indexCol = (colAliases != null && colAliases.size() >= 2) ? colAliases.get(1) : "n";
        Table virtualTable = new Table(alias, Cols.listOf(
                new Column(valueCol, DataType.TEXT, true, false, null),
                new Column(indexCol, DataType.INTEGER, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        List<Object> elements = FunctionEvaluator.flattenArray(toElementList(evalArgs.get(0)));
        for (int i = 0; i < elements.size(); i++) {
            Object[] row = new Object[]{elements.get(i), i + 1};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    // ---- unnest ----

    private List<RowContext> resolveUnnest(String alias, List<String> colAliases, List<Object> evalArgs,
                                           List<Expression> argExprs) {
        // unnest is declared three times over -- anyarray, anymultirange and tsvector -- so an
        // argument with no type of its own fits all three equally and PostgreSQL will not choose.
        if (argExprs != null && argExprs.size() == 1 && evalArgs.size() == 1
                && argExprs.get(0) instanceof Literal
                && ((Literal) argExprs.get(0)).literalType() == Literal.LiteralType.STRING) {
            throw new MemgresException("function unnest(unknown) is not unique"
                    + "\n  Hint: Could not choose a best candidate function."
                    + " You might need to add explicit type casts.", "42725");
        }
        if (evalArgs.isEmpty()) {
            throw new MemgresException("function unnest() does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (evalArgs.size() > 1) {
            return resolveMultiUnnest(alias, colAliases, evalArgs, argExprs);
        }
        // Single array/multirange unnest
        Object arr = evalArgs.get(0);
        // Multirange unnest: convert to list of range strings
        if (arr instanceof String) {
            String s = ((String) arr).trim();
            if (RangeOperations.isMultirangeOrEmpty(s)) {
                java.util.List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(s);
                List<Object> mrElements = new ArrayList<>();
                for (RangeOperations.PgRange r : ranges) {
                    mrElements.add(r.toString());
                }
                arr = mrElements;
            }
        }
        // The tsvector spelling of unnest does not produce elements but records: a lexeme, the
        // positions it was found at and the weight each of those carries. It builds its own
        // relation for that reason, rather than one column of whatever the argument holds.
        if (arr instanceof TsVector) {
            return unnestTsVector(alias, colAliases, (TsVector) arr);
        }
        // PG unnest fully flattens multidimensional arrays into scalar elements
        List<Object> elements = FunctionEvaluator.flattenArray(toElementList(arr));

        String colName = firstColAlias(colAliases, alias);
        List<Column> cols = new ArrayList<>();
        // The rows unnest produces are the array's elements, so the column carries the element
        // type: an int4[] unnests to int4, not to the text the driver could not decode.
        // Read against the enclosing row: a lateral unnest(ARRAY[a]) names a column of the item
        // to its left, and inferring with no bindings answered text for an integer array.
        DataType elementType = argExprs == null || argExprs.isEmpty() ? null
                : DataType.elementOf(inferInOuterScope(argExprs.get(0)));
        cols.add(new Column(colName, elementType != null ? elementType : DataType.TEXT, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Object elem : elements) {
            Object[] row = new Object[]{elem};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    /**
     * The rows unnest(tsvector) produces, one per lexeme, in the order the vector holds them.
     *
     * <p>A lexeme stored without positions has neither positions nor weights, and PostgreSQL
     * reports that as two nulls rather than as two arrays holding nothing.
     */
    private List<RowContext> unnestTsVector(String alias, List<String> colAliases, TsVector vec) {
        String lexemeCol = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "lexeme";
        String posCol = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "positions";
        String weightCol = (colAliases != null && colAliases.size() > 2) ? colAliases.get(2) : "weights";
        List<Column> cols = Cols.listOf(
                new Column(lexemeCol, DataType.TEXT, true, false, null),
                new Column(posCol, DataType.INT2_ARRAY, true, false, null),
                new Column(weightCol, DataType.TEXT_ARRAY, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Map.Entry<String, List<TsVector.PosEntry>> e : vec.getLexemeMap().entrySet()) {
            List<Object> positions = new ArrayList<>();
            List<Object> weights = new ArrayList<>();
            for (TsVector.PosEntry pe : e.getValue()) {
                positions.add(Short.valueOf((short) pe.position()));
                weights.add(String.valueOf(pe.weight()));
            }
            Object[] row = new Object[]{e.getKey(),
                    positions.isEmpty() ? null : PgArray.of(positions),
                    weights.isEmpty() ? null : PgArray.of(weights)};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }


    private List<RowContext> resolveMultiUnnest(String alias, List<String> colAliases, List<Object> evalArgs,
                                                List<Expression> argExprs) {
        List<List<Object>> allElements = new ArrayList<>();
        int maxLen = 0;
        for (Object arr : evalArgs) {
            // PG unnest fully flattens multidimensional arrays into scalar elements
            List<Object> elems = FunctionEvaluator.flattenArray(toElementList(arr));
            allElements.add(elems);
            maxLen = Math.max(maxLen, elems.size());
        }
        List<Column> cols = new ArrayList<>();
        for (int i = 0; i < evalArgs.size(); i++) {
            // Every column of a multi-argument unnest is named for the function, as each column
            // of the equivalent ROWS FROM would be -- not col1..colN, which is a name PostgreSQL
            // never gives a column. Each carries its own array's element type.
            String cname = (colAliases != null && i < colAliases.size()) ? colAliases.get(i) : "unnest";
            DataType elementType = argExprs != null && i < argExprs.size()
                    ? DataType.elementOf(executor.exprEvaluator.inferExprType(argExprs.get(i))) : null;
            cols.add(new Column(cname, elementType != null ? elementType : DataType.TEXT, true, false, null));
        }
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (int row = 0; row < maxLen; row++) {
            Object[] rowData = new Object[cols.size()];
            for (int c = 0; c < allElements.size(); c++) {
                rowData[c] = row < allElements.get(c).size() ? allElements.get(c).get(row) : null;
            }
            virtualTable.insertRow(rowData);
            contexts.add(new RowContext(virtualTable, alias, rowData));
        }
        return contexts;
    }

    // ---- jsonb_each / json_each ----

    private List<RowContext> resolveJsonEach(String fname, String alias, List<String> colAliases,
                                             List<Object> evalArgs) {
        Object jsonVal = evalArgs.get(0);
        boolean isText = fname.contains("_text");
        String keyCol = (colAliases != null && colAliases.size() >= 1) ? colAliases.get(0) : "key";
        String valCol = (colAliases != null && colAliases.size() >= 2) ? colAliases.get(1) : "value";
        // The _text spellings render the value as text; the others hand back the JSON itself,
        // which is a json or jsonb column and not a string that happens to look like one.
        DataType valType = isText ? DataType.TEXT
                : fname.startsWith("jsonb") ? DataType.JSONB : DataType.JSON;
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(keyCol, DataType.TEXT, true, false, null));
        cols.add(new Column(valCol, valType, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null) {
            String json = jsonVal.toString().trim();
            JsonFunctions.requireJsonEachObject(fname, json);
            try {
                Map<String, String> pairs = JsonFunctions.eachMembers(fname, json);
                for (Map.Entry<String, String> entry : pairs.entrySet()) {
                    String value = entry.getValue();
                    if (isText && value != null && value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    Object[] row = new Object[]{entry.getKey(), value};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            } catch (Exception e) { /* skip */ }
        }
        return contexts;
    }

    // ---- jsonb_to_recordset / json_to_recordset ----

    private List<RowContext> resolveJsonToRecordset(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object jsonVal = evalArgs.get(0);
        List<Column> cols = new ArrayList<>();
        if (colAliases != null) {
            for (String ca : colAliases) {
                // Column alias may contain type info: "name type" e.g. "a int"
                int spaceIdx = ca.indexOf(' ');
                if (spaceIdx > 0) {
                    String colName = ca.substring(0, spaceIdx);
                    String typeName = ca.substring(spaceIdx + 1).trim();
                    DataType dt = DataType.fromPgName(typeName);
                    if (dt == null) dt = DataType.TEXT;
                    cols.add(new Column(colName, dt, true, false, null));
                } else {
                    cols.add(new Column(ca, DataType.TEXT, true, false, null));
                }
            }
        }
        if (cols.isEmpty()) cols.add(new Column("value", DataType.TEXT, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null) {
            String json = jsonVal.toString().trim();
            boolean isArray = json.startsWith("[");
            List<String> jsonObjects = new ArrayList<>();
            if (isArray) {
                json = json.substring(1, json.length() - 1).trim();
                int depth = 0;
                StringBuilder current = new StringBuilder();
                for (char c : json.toCharArray()) {
                    if (c == '{') depth++;
                    if (c == '}') depth--;
                    current.append(c);
                    if (depth == 0 && current.length() > 0) {
                        jsonObjects.add(current.toString().trim());
                        current = new StringBuilder();
                    }
                    if (c == ',' && depth == 0) current = new StringBuilder();
                }
                if (current.length() > 0) jsonObjects.add(current.toString().trim());
            } else {
                jsonObjects.add(json);
            }
            for (String obj : jsonObjects) {
                if (obj.isEmpty() || obj.equals(",")) continue;
                Object[] row = new Object[cols.size()];
                for (int ci = 0; ci < cols.size(); ci++) {
                    String key = cols.get(ci).getName();
                    String extracted = JsonOperations.extractKey(obj, key);
                    if (extracted != null) {
                        extracted = extracted.trim();
                        if (extracted.startsWith("\"") && extracted.endsWith("\"")) {
                            extracted = extracted.substring(1, extracted.length() - 1);
                        }
                        if (extracted.equals("true")) extracted = "t";
                        else if (extracted.equals("false")) extracted = "f";
                        else if (extracted.equals("null")) extracted = null;
                    }
                    if (extracted != null) {
                        DataType colDt = cols.get(ci).getType();
                        if (colDt == DataType.INTEGER || colDt == DataType.BIGINT || colDt == DataType.SMALLINT) {
                            try { row[ci] = Long.parseLong(extracted); } catch (NumberFormatException e) { row[ci] = extracted; }
                        } else if (colDt == DataType.NUMERIC || colDt == DataType.DOUBLE_PRECISION || colDt == DataType.REAL) {
                            try { row[ci] = new java.math.BigDecimal(extracted); } catch (NumberFormatException e) { row[ci] = extracted; }
                        } else if (colDt == DataType.BOOLEAN) {
                            row[ci] = "t".equals(extracted) || "true".equalsIgnoreCase(extracted);
                        } else {
                            row[ci] = extracted;
                        }
                    } else {
                        row[ci] = null;
                    }
                }
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- json_populate_recordset / jsonb_populate_recordset ----

    private List<RowContext> resolveJsonPopulateRecordset(SelectStmt.FunctionFrom funcFrom,
            String alias, List<String> colAliases, List<Object> evalArgs) {
        // First arg defines the composite type (e.g. NULL::my_type)
        // Extract composite type name from the CastExpr in the first argument
        List<Column> cols = new ArrayList<>();
        if (funcFrom.args().size() >= 1 && funcFrom.args().get(0) instanceof CastExpr) {
            String typeName = ((CastExpr) funcFrom.args().get(0)).typeName().toLowerCase();
            List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
            if (fields != null) {
                for (CreateTypeStmt.CompositeField field : fields) {
                    DataType dt = DataType.fromPgName(field.typeName());
                    cols.add(new Column(field.name(), dt != null ? dt : DataType.TEXT, true, false, null));
                }
            } else {
                // Fall back to table columns (PG treats table types as composite types)
                Table tbl = executor.database.getTable(typeName);
                if (tbl != null) {
                    for (Column c : tbl.getColumns()) {
                        cols.add(new Column(c.getName(), c.getType(), true, false, null));
                    }
                }
            }
        }
        if (cols.isEmpty() && colAliases != null) {
            for (String ca : colAliases) {
                cols.add(new Column(ca, DataType.TEXT, true, false, null));
            }
        }
        if (cols.isEmpty()) cols.add(new Column("value", DataType.TEXT, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        Object jsonVal = evalArgs.size() > 1 ? evalArgs.get(1) : null;
        if (jsonVal != null) {
            String json = jsonVal.toString().trim();
            List<String> jsonObjects = new ArrayList<>();
            if (json.startsWith("[")) {
                // Array of objects
                json = json.substring(1, json.length() - 1).trim();
                int depth = 0;
                StringBuilder current = new StringBuilder();
                for (char c : json.toCharArray()) {
                    if (c == '{') depth++;
                    if (c == '}') depth--;
                    current.append(c);
                    if (depth == 0 && current.length() > 0) {
                        String s = current.toString().trim();
                        if (!s.isEmpty() && !s.equals(",")) jsonObjects.add(s);
                        current = new StringBuilder();
                    }
                    if (c == ',' && depth == 0) current = new StringBuilder();
                }
                if (current.length() > 0) {
                    String s = current.toString().trim();
                    if (!s.isEmpty() && !s.equals(",")) jsonObjects.add(s);
                }
            } else if (json.startsWith("{")) {
                jsonObjects.add(json);
            }
            for (String obj : jsonObjects) {
                if (obj.isEmpty() || obj.equals(",")) continue;
                Object[] row = new Object[cols.size()];
                for (int ci = 0; ci < cols.size(); ci++) {
                    String key = cols.get(ci).getName();
                    String extracted = JsonOperations.extractKey(obj, key);
                    if (extracted != null) {
                        extracted = extracted.trim();
                        if (extracted.startsWith("\"") && extracted.endsWith("\"")) {
                            extracted = extracted.substring(1, extracted.length() - 1);
                        }
                        if (extracted.equals("null")) extracted = null;
                        else if (extracted.equals("true")) extracted = "t";
                        else if (extracted.equals("false")) extracted = "f";
                    }
                    row[ci] = extracted;
                }
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- hstore populate_record ----

    private List<RowContext> resolveHstorePopulateRecord(SelectStmt.FunctionFrom funcFrom,
            String alias, List<Object> evalArgs) {
        // Extract composite type from CastExpr first argument
        List<Column> cols = new ArrayList<>();
        String typeName = null;
        if (funcFrom.args().size() >= 1 && funcFrom.args().get(0) instanceof CastExpr) {
            typeName = ((CastExpr) funcFrom.args().get(0)).typeName().toLowerCase();
            java.util.List<CreateTypeStmt.CompositeField> fields =
                    executor.compositeTypeHandler.resolveFieldsForType(typeName);
            if (fields != null) {
                for (CreateTypeStmt.CompositeField field : fields) {
                    DataType dt = DataType.fromPgName(field.typeName());
                    cols.add(new Column(field.name(), dt != null ? dt : DataType.TEXT, true, false, null));
                }
            }
        }
        if (cols.isEmpty()) cols.add(new Column("value", DataType.TEXT, true, false, null));

        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();

        // Evaluate: populate_record(base, hstore)
        Object hstoreVal = evalArgs.size() > 1 ? evalArgs.get(1) : null;
        if (hstoreVal != null) {
            HstoreValue hs = (hstoreVal instanceof HstoreValue)
                    ? (HstoreValue) hstoreVal : HstoreValue.parse(hstoreVal.toString());
            java.util.List<CreateTypeStmt.CompositeField> fields =
                    executor.compositeTypeHandler.resolveFieldsForType(typeName);
            if (fields != null) {
                Object baseVal = evalArgs.get(0);
                java.util.Map<String, Object> populated =
                        executor.compositeTypeHandler.populateFromHstore(baseVal, hs, fields);
                Object[] row = new Object[cols.size()];
                for (int ci = 0; ci < cols.size(); ci++) {
                    row[ci] = populated.get(cols.get(ci).getName());
                }
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- regexp_matches ----

    private List<RowContext> resolveRegexpMatches(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object str = evalArgs.get(0);
        Object pattern = evalArgs.get(1);
        String flags = evalArgs.size() > 2 ? String.valueOf(evalArgs.get(2)) : "";
        if (str == null || pattern == null) return Cols.listOf();
        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(pattern.toString(), jflags).matcher(str.toString());
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int g = 1; g <= matcher.groupCount(); g++) groups.add(matcher.group(g));
            if (groups.isEmpty()) groups.add(matcher.group(0));
            Object[] row = new Object[]{groups};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
            if (!flags.contains("g")) break;
        }
        return contexts;
    }

    // ---- jsonb_path_query ----

    private List<RowContext> resolveJsonbPathQuery(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.size() < 2) throw new MemgresException("function jsonb_path_query requires at least 2 arguments", "42883");
        Object jsonVal = evalArgs.get(0);
        Object pathVal = evalArgs.get(1);
        String colName = firstColAlias(colAliases, alias);
        Table virtualTable = new Table(alias, Cols.listOf(new Column(colName, DataType.JSONB, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null && pathVal != null) {
            String json = jsonVal.toString().trim();
            String path = pathVal.toString().trim();
            // The optional third argument binds $name references in the path
            if (evalArgs.size() > 2 && evalArgs.get(2) != null) {
                path = JsonFunctions.bindJsonPathVars(path, evalArgs.get(2).toString());
            }
            List<String> stringResults = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            for (String s : stringResults) {
                String trimmed = s.trim();
                Object val;
                if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
                    val = trimmed.substring(1, trimmed.length() - 1);
                } else {
                    val = s;
                }
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- jsonb_array_elements / json_array_elements ----

    private List<RowContext> resolveJsonArrayElements(String fname, String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function " + fname + "() requires 1 argument", "42883");
        Object json = evalArgs.get(0);
        boolean textMode = fname.endsWith("_text");
        String colName = firstColAlias(colAliases, "value");
        // json_array_elements answers json and jsonb_array_elements answers jsonb; calling both
        // jsonb described a json column as one the driver would have read differently.
        DataType dt = textMode ? DataType.TEXT
                : fname.startsWith("jsonb") ? DataType.JSONB : DataType.JSON;
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(colName, dt, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        if (json != null) {
            String s = json.toString().trim();
            JsonFunctions.requireJsonArray(fname, s);
            List<String> elements = JsonOperations.parseArrayElements(s);
            for (String elem : elements) {
                String val = elem.trim();
                if (textMode) val = JsonOperations.jsonValueToText(val);
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- jsonb_object_keys / json_object_keys ----

    private List<RowContext> resolveJsonObjectKeys(String fname, String alias, List<String> colAliases,
                                                   List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function " + fname + "() requires 1 argument", "42883");
        // A call whose rows are one value answers under the name the FROM clause gave it, and
        // only under the function's own name where the clause gave it none.
        Table virtualTable = new Table(alias, Cols.listOf(
                new Column(firstColAlias(colAliases, alias), DataType.TEXT, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        Object json = evalArgs.get(0);
        if (json != null) {
            String s = json.toString().trim();
            JsonFunctions.requireJsonObject(fname, s);
            for (String key : JsonFunctions.eachMembers(fname, s).keySet()) {
                Object[] row = new Object[]{key};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- pg_options_to_table ----

    private List<RowContext> resolvePgOptionsToTable(String alias, List<String> colAliases, List<Object> evalArgs) {
        String col1 = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "option_name";
        String col2 = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "option_value";
        Table virtualTable = new Table(alias != null ? alias : "pg_options_to_table",
                Cols.listOf(new Column(col1, DataType.TEXT, true, false, null),
                        new Column(col2, DataType.TEXT, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        if (!evalArgs.isEmpty() && evalArgs.get(0) != null) {
            String input = evalArgs.get(0).toString();
            if (input.startsWith("{") && input.endsWith("}")) {
                input = input.substring(1, input.length() - 1);
            }
            if (!input.isEmpty()) {
                for (String opt : input.split(",")) {
                    String[] kv = opt.split("=", 2);
                    Object[] row = new Object[]{kv[0].trim(), kv.length > 1 ? kv[1].trim() : ""};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- pg_get_sequence_data ----

    private List<RowContext> resolvePgGetSequenceData(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object seqOidArg = evalArgs.isEmpty() ? null : evalArgs.get(0);
        int seqOid = seqOidArg != null ? ((Number) seqOidArg).intValue() : 0;
        String col1 = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "last_value";
        String col2 = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "is_called";
        String tblAlias = alias != null ? alias : "pg_get_sequence_data";
        Table virtualTable = new Table(tblAlias,
                Cols.listOf(new Column(col1, DataType.BIGINT, true, false, null),
                        new Column(col2, DataType.BOOLEAN, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        Database db = executor.database;
        Map<String, Integer> oidMap = executor.systemCatalog.getOidMap();
        for (String qualified : CatalogHelper.getSequenceNames(db)) {
            for (Map.Entry<String, Integer> entry : oidMap.entrySet()) {
                if (entry.getValue() == seqOid && entry.getKey().equals("rel:" + qualified)) {
                    Sequence seq = db.getSequence(qualified);
                    long lastVal;
                    boolean isCalled;
                    if (seq != null) {
                        lastVal = seq.currValRaw();
                        isCalled = seq.isCalled();
                    } else {
                        long[] resolved = resolveImplicitSerial(db, qualified);
                        lastVal = resolved[0];
                        isCalled = resolved[1] != 0;
                    }
                    Object[] row = new Object[]{lastVal, isCalled};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, tblAlias, row));
                    return contexts;
                }
            }
        }
        Object[] row = new Object[]{1L, false};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, tblAlias, row));
        return contexts;
    }

    // ---- TABLESAMPLE ----

    private List<RowContext> resolveTablesample(String fname, String alias, List<Object> evalArgs) {
        String tableName = fname.substring("__tablesample__:".length());
        String method = evalArgs.get(0).toString();
        // Both parameters are read before anything is sampled, and PostgreSQL says so rather than
        // sampling with nothing in hand: the percentage is 2202H and the seed 2202G.
        if (evalArgs.get(1) == null) {
            throw new MemgresException("TABLESAMPLE parameter cannot be null", "2202H");
        }
        double pct = executor.toDouble(evalArgs.get(1));
        // REPEATABLE is declared over double precision, so a fraction is a seed and not a fault in
        // the statement. What the sampler wants of it is a repeatable starting point: a whole
        // number is used as itself, so a seed that always picked these rows still picks them.
        Long seed = null;
        if (evalArgs.size() > 2) {
            if (evalArgs.get(2) == null) {
                throw new MemgresException("TABLESAMPLE REPEATABLE parameter cannot be null", "2202G");
            }
            double written = executor.toDouble(evalArgs.get(2));
            seed = Long.valueOf(written == Math.rint(written) && !Double.isInfinite(written)
                    ? (long) written : Double.doubleToLongBits(written));
        }

        // The sampler reads the percentage as a fraction of the whole, so anything outside 0..100
        // names no fraction at all. PostgreSQL says the same words either side of the range.
        if (pct < 0 || pct > 100) {
            throw new MemgresException("sample percentage must be between 0 and 100", "2202H");
        }

        // Sampling reads a fraction of a stored relation's pages, so there has to be one. A WITH
        // item is computed, not stored, and PG says so rather than that the name is unknown.
        if (executor.selectExecutor.namesWithItem(tableName)) {
            throw new MemgresException(
                    "TABLESAMPLE clause can only be applied to tables and materialized views",
                    "0A000");
        }
        // A qualified relation names the schema to look in; the qualifier is not part of the
        // relation's own name, so the two halves are told apart before the relation is opened.
        int qualifier = tableName.indexOf('.');
        String sampleSchema = qualifier > 0 ? tableName.substring(0, qualifier) : null;
        String sampleName = qualifier > 0 ? tableName.substring(qualifier + 1) : tableName;
        // And it has to be a relation whose pages are its own. A view has none -- reading it runs
        // its query -- and neither has a sequence, so PostgreSQL refuses both by kind before it
        // samples anything. Resolving first rewrote a view to the table underneath it and sampled
        // that, which is a relation the statement never named. A materialized view does hold its
        // rows, and PostgreSQL samples it.
        List<String> samplePath = sampleSchema != null
                ? java.util.Collections.singletonList(sampleSchema)
                : executor.relationSearchPath();
        Database.ViewDef sampledMatview = null;
        for (String schema : samplePath) {
            String kind = RelationNamespace.kindOf(executor.database, schema, sampleName);
            if (kind == null) continue;
            if (RelationNamespace.VIEW.equals(kind) || RelationNamespace.SEQUENCE.equals(kind)) {
                throw new MemgresException(
                        "TABLESAMPLE clause can only be applied to tables and materialized views",
                        "0A000");
            }
            // A materialized view's rows are not in the schema's tables, so the lookup below would
            // not find them and the statement was refused as naming nothing -- a relation the same
            // session can read without TABLESAMPLE. The rows it holds are what PostgreSQL samples.
            if (RelationNamespace.MATVIEW.equals(kind)) {
                sampledMatview = executor.database.getView(schema, sampleName);
            }
            break;
        }
        Table table;
        if (sampledMatview != null) {
            table = matviewRelation(sampledMatview, sampleName);
        } else {
            try {
                table = executor.resolveTable(sampleSchema, sampleName, sampleSchema != null);
            } catch (MemgresException e) {
                throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
            }
        }
        // The rows of an inheritance child or a partition belong to the relation named, so a
        // sampled scan draws from the same population a plain scan reads. Sampling the relation's
        // own list alone made TABLESAMPLE see a fraction of a table it could not report.
        List<Object[]> allRows = new ArrayList<>(table.getAllRows());
        List<Object[]> sampledRows;

        if (pct == 100.0) {
            sampledRows = allRows;
        } else if (pct == 0.0) {
            sampledRows = new ArrayList<>();
        } else {
            java.util.Random rng = seed != null ? new java.util.Random(seed) : new java.util.Random();
            sampledRows = new ArrayList<>();
            double prob = pct / 100.0;
            for (Object[] row : allRows) {
                if (rng.nextDouble() < prob) {
                    sampledRows.add(row);
                }
            }
        }

        String tableAlias = alias != null ? alias : tableName;
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] row : sampledRows) {
            contexts.add(new RowContext(table, tableAlias, row));
        }
        return contexts;
    }

    /**
     * A materialized view's stored rows, as a relation a sampled scan can read.
     *
     * <p>PostgreSQL keeps the rows of the last refresh, which is why it samples a materialized
     * view as it samples a table. One that has never been populated holds nothing to sample, and
     * PostgreSQL refuses every scan of it -- sampled or plain -- until a REFRESH has run.
     */
    private Table matviewRelation(Database.ViewDef view, String name) {
        if (!view.populated()) {
            MemgresException e = new MemgresException(
                    "materialized view \"" + view.name() + "\" has not been populated", "55000");
            e.setHint("Use the REFRESH MATERIALIZED VIEW command.");
            throw e;
        }
        List<Column> cols = view.cachedColumns() != null
                ? view.cachedColumns() : new ArrayList<Column>();
        Table held = new Table(name, cols);
        if (view.cachedRows() != null) {
            for (Object[] row : view.cachedRows()) held.insertRow(row);
        }
        return held;
    }

    // ---- ROWS FROM ----

    /**
     * {@code ROWS FROM (f(...), g(...))} is the functions side by side rather than one after the
     * other: each keeps all of its own columns, in its own order, and the item has as many rows
     * as the longest of them, the shorter ones reading NULL past their end.
     *
     * <p>Each function is resolved exactly as it would be on its own, so a function returning a
     * record contributes that record's columns under their own names and types --
     * {@code ROWS FROM (generate_series(1,2), json_each('{"a":1}'))} is three columns, not two.
     */
    private List<RowContext> resolveRowsFrom(SelectStmt.FunctionFrom funcFrom, String alias) {
        List<Column> cols = new ArrayList<>();
        List<List<Object[]>> perFunctionRows = new ArrayList<>();
        List<Integer> widths = new ArrayList<>();
        int maxLen = 0;
        for (Expression arg : funcFrom.args()) {
            FunctionCallExpr call;
            List<String> columnDefs = null;
            if (arg instanceof RowsFromItem) {
                call = ((RowsFromItem) arg).call();
                columnDefs = ((RowsFromItem) arg).columnDefs();
            } else if (arg instanceof FunctionCallExpr) {
                call = (FunctionCallExpr) arg;
            } else {
                continue;
            }
            SelectStmt.FunctionFrom sub =
                    new SelectStmt.FunctionFrom(call.name(), call.args(), null, columnDefs);
            List<RowContext> rows = resolveFunctionFrom(sub);
            List<Column> subCols = null;
            List<Object[]> subRows = new ArrayList<>();
            for (RowContext rc : rows) {
                List<RowContext.TableBinding> bindings = rc.getBindings();
                if (bindings.isEmpty()) continue;
                if (subCols == null) subCols = bindings.get(0).table().getColumns();
                subRows.add(bindings.get(0).row());
            }
            if (subCols == null) {
                // Nothing was produced, so nothing described the shape; PG still names the
                // column after the function.
                subCols = Cols.listOf(new Column(
                        FunctionEvaluator.stripSchemaPrefix(call.name().toLowerCase()),
                        DataType.TEXT, true, false, null));
            }
            cols.addAll(subCols);
            widths.add(subCols.size());
            perFunctionRows.add(subRows);
            maxLen = Math.max(maxLen, subRows.size());
        }

        List<String> ca = funcFrom.columnAliases();
        if (ca != null) {
            for (int i = 0; i < ca.size() && i < cols.size(); i++) {
                Column c = cols.get(i);
                cols.set(i, new Column(stripColType(ca.get(i)), c.getType(), true, false, null));
            }
        }
        String tblAlias = alias != null ? alias : "rows_from";
        Table virtualTable = new Table(tblAlias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (int row = 0; row < maxLen; row++) {
            Object[] rowData = new Object[cols.size()];
            int at = 0;
            for (int f = 0; f < perFunctionRows.size(); f++) {
                List<Object[]> rows = perFunctionRows.get(f);
                Object[] src = row < rows.size() ? rows.get(row) : null;
                for (int c = 0; c < widths.get(f); c++) {
                    rowData[at + c] = src != null && c < src.length ? src[c] : null;
                }
                at += widths.get(f);
            }
            virtualTable.insertRow(rowData);
            contexts.add(new RowContext(virtualTable, tblAlias, rowData));
        }
        return contexts;
    }

    // ---- User-defined functions ----

    private List<RowContext> resolveUserFunction(PgFunction userFunc, String alias, List<String> colAliases, List<Object> evalArgs) {
        // STRICT: return empty set if any argument is NULL (PG returns empty set for strict SRFs)
        if (userFunc.isStrict()) {
            for (Object arg : evalArgs) {
                if (arg == null) {
                    return Collections.emptyList();
                }
            }
        }
        com.memgres.engine.plpgsql.PlpgsqlExecutor plExec = new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session);
        Object result = plExec.executeFunction(userFunc, evalArgs);
        // A function that does not return a set answers with one value however that value is
        // shaped, and an array is one value. Reading every java list as the rows of a set turned
        // a function returning int[] into a column of its elements: SELECT * FROM f() gave two
        // rows of text where PostgreSQL gives one row holding the array.
        if (result instanceof List<?> && !userFunc.isSetReturning()) {
            String arrayColName = firstColAlias(colAliases, alias);
            DataType arrayType = DataType.fromPgName(userFunc.getReturnType());
            Table arrayTable = new Table(alias, Cols.listOf(new Column(arrayColName,
                    arrayType != null ? arrayType : DataType.TEXT, true, false, null)));
            Object[] arrayRow = new Object[]{result};
            arrayTable.insertRow(arrayRow);
            return Cols.listOf(new RowContext(arrayTable, alias, arrayRow));
        }
        if (result instanceof List<?>) {
            List<?> resultList = (List<?>) result;
            List<PgFunction.Param> params = userFunc.getParams();
            String returnType = userFunc.getReturnType();

            List<Column> cols = new ArrayList<>();
            // The type a SETOF of a scalar returns. RETURNS SETOF int answers int4 columns, and
            // reporting them as text left the driver decoding integers as strings.
            DataType scalarSetofType = null;
            if (returnType != null && returnType.toUpperCase().startsWith("SETOF ")) {
                String refTable = returnType.substring(6).trim();
                scalarSetofType = DataType.fromPgName(refTable);
                if (!"record".equalsIgnoreCase(refTable)) {
                    // Try composite type first
                    List<com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField> ctFields =
                            executor.database.getCompositeType(refTable);
                    if (ctFields != null) {
                        for (com.memgres.engine.parser.ast.CreateTypeStmt.CompositeField f : ctFields) {
                            DataType dt = DataType.fromPgName(f.typeName());
                            if (dt == null) dt = DataType.TEXT;
                            cols.add(new Column(f.name(), dt, true, false, null));
                        }
                    } else {
                        try {
                            Table sourceTable = executor.resolveTable("public", refTable);
                            if (sourceTable != null) {
                                cols.addAll(sourceTable.getColumns());
                            }
                        } catch (Exception e) {
                            // Not a table, use single column
                        }
                    }
                }
            }

            Table virtualTable;
            List<RowContext> contexts = new ArrayList<>();
            if (!resultList.isEmpty() && resultList.get(0) instanceof Map) {
                // Composite record results (Map from PL/pgSQL composite type assignments)
                if (cols.isEmpty()) {
                    // Infer columns from the first Map's keys
                    @SuppressWarnings("unchecked")
                    Map<String, Object> firstMap = (Map<String, Object>) resultList.get(0);
                    for (String key : firstMap.keySet()) {
                        cols.add(new Column(key, DataType.TEXT, true, false, null));
                    }
                }
                virtualTable = new Table(alias, cols);
                for (Object item : resultList) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) item;
                    Object[] rowArr = new Object[cols.size()];
                    for (int i = 0; i < cols.size(); i++) {
                        rowArr[i] = map.get(cols.get(i).getName().toLowerCase());
                    }
                    virtualTable.insertRow(rowArr);
                    contexts.add(new RowContext(virtualTable, alias, rowArr));
                }
            } else if (!resultList.isEmpty() && resultList.get(0) instanceof Object[]) {
                Object[] firstRow = (Object[]) resultList.get(0);
                // A signature that names its own result columns is the description; an alias
                // list written alongside only renames them, and never says how many there are.
                boolean namedBySignature = userFunc.hasOutParams();
                if (cols.isEmpty() && !namedBySignature && colAliases != null && !colAliases.isEmpty()) {
                    // A column whose type does not fit is what PostgreSQL reports first, even
                    // when the list is the wrong length as well: it walks the columns it has
                    // before it counts them.
                    checkRecordColumnTypes(userFunc, colAliases, firstRow);
                    checkRecordShape(userFunc, colAliases.size(), firstRow.length);
                    for (int i = 0; i < colAliases.size(); i++) {
                        cols.add(columnFromDef(colAliases.get(i), i + 1));
                    }
                } else if (cols.isEmpty()) {
                    for (int i = 0; i < firstRow.length; i++) {
                        cols.add(new Column("column" + (i + 1), DataType.TEXT, true, false, null));
                    }
                }
                int colIdx = 0;
                for (PgFunction.Param p : params) {
                    if ("OUT".equalsIgnoreCase(p.mode()) && colIdx < cols.size()) {
                        DataType dt = DataType.fromPgName(p.typeName());
                        cols.set(colIdx, new Column(p.name() != null ? p.name() : "column" + (colIdx + 1),
                                dt != null ? dt : DataType.TEXT, true, false, null));
                        colIdx++;
                    }
                }
                if (namedBySignature && colAliases != null) {
                    for (int i = 0; i < colAliases.size() && i < cols.size(); i++) {
                        Column c = cols.get(i);
                        cols.set(i, new Column(stripColType(colAliases.get(i)), c.getType(),
                                true, false, null));
                    }
                }
                virtualTable = new Table(alias, cols);
                for (Object row : resultList) {
                    Object[] rowArr = (Object[]) row;
                    virtualTable.insertRow(rowArr);
                    contexts.add(new RowContext(virtualTable, alias, rowArr));
                }
            } else {
                // For RETURNS TABLE with single-column results, use OUT param name
                String colName = alias;
                if ("TABLE".equalsIgnoreCase(returnType)) {
                    for (PgFunction.Param p : params) {
                        if ("OUT".equalsIgnoreCase(p.mode()) && p.name() != null) {
                            colName = p.name();
                            break;
                        }
                    }
                }
                if (colAliases != null && !colAliases.isEmpty()) colName = stripColType(colAliases.get(0));
                cols.add(new Column(colName,
                        scalarSetofType != null ? scalarSetofType : DataType.TEXT, true, false, null));
                virtualTable = new Table(alias, cols);
                for (Object val : resultList) {
                    Object[] row = new Object[]{val};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
            return contexts;
        }
        // Non-list result: OUT-param record function or scalar-returning function in FROM
        List<PgFunction.Param> outParams = new ArrayList<>();
        for (PgFunction.Param p : userFunc.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode) || "INOUT".equals(mode)) outParams.add(p);
        }
        if (!outParams.isEmpty()) {
            List<Column> cols = new ArrayList<>();
            for (int i = 0; i < outParams.size(); i++) {
                PgFunction.Param op = outParams.get(i);
                String cname = (colAliases != null && i < colAliases.size())
                        ? stripColType(colAliases.get(i))
                        : (op.name() != null ? op.name() : ("column" + (i + 1)));
                DataType dt = DataType.fromPgName(op.typeName());
                cols.add(new Column(cname, dt != null ? dt : DataType.TEXT, true, false, null));
            }
            Table virtualTable = new Table(alias, cols);
            List<RowContext> contexts = new ArrayList<>();
            Object[] row;
            if (result instanceof Object[]) {
                row = (Object[]) result;
            } else {
                row = new Object[]{result};
            }
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
            return contexts;
        }
        // For RETURNS record with caller-provided column aliases, expand the record
        if ("record".equalsIgnoreCase(userFunc.getReturnType()) && colAliases != null && !colAliases.isEmpty()) {
            Object[] rowArr;
            if (result instanceof Object[]) {
                rowArr = (Object[]) result;
            } else if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) result;
                rowArr = map.values().toArray();
            } else {
                // A single value carries no shape of its own to check against the alias list
                rowArr = new Object[colAliases.size()];
                rowArr[0] = result;
            }
            List<Column> cols = new ArrayList<>();
            for (int i = 0; i < colAliases.size(); i++) {
                cols.add(columnFromDef(colAliases.get(i), i + 1));
            }
            Table virtualTable = new Table(alias, cols);
            List<RowContext> contexts = new ArrayList<>();
            virtualTable.insertRow(rowArr);
            contexts.add(new RowContext(virtualTable, alias, rowArr));
            return contexts;
        }
        // Scalar function in FROM: one column of the type the function declares.
        String colName = firstColAlias(colAliases, alias);
        DataType declared = DataType.fromPgName(userFunc.getReturnType());
        Column col = new Column(colName, declared != null ? declared : DataType.TEXT,
                true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        Object[] row = new Object[]{result};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- Shared helpers ----

    /** The complaint PostgreSQL makes when a column definition list does not fit the record. */
    private static MemgresException recordShapeError(PgFunction userFunc) {
        String lang = userFunc.getLanguage();
        if (lang != null && lang.equalsIgnoreCase("plpgsql")) {
            return new MemgresException(
                    "structure of query does not match function result type", "42804");
        }
        MemgresException mismatch = new MemgresException(
                "return type mismatch in function declared to return record", "42P13");
        // A SQL function's body is the one statement PostgreSQL names by its number, and that is
        // the whole of what it reports for this. PL/pgSQL's is a stack of frames of its own, so a
        // routine written in it is left with nothing rather than with a frame it never had.
        mismatch.setPgContext("SQL function \"" + userFunc.getName() + "\" statement 1");
        return mismatch;
    }

    /**
     * The caller's column definition list is the only description of a {@code record} result,
     * so it has to agree with what the function body actually produces.
     */
    private static void checkRecordShape(PgFunction userFunc, int declared, int produced) {
        if (declared == produced) return;
        // Which complaint PostgreSQL makes depends on the language. A SQL function's body is
        // checked against the column definition list as part of resolving the call, so a list
        // that does not fit is an invalid function definition; PL/pgSQL's RETURN QUERY compares
        // the query it just ran against the record type it resolved, which is a datatype
        // mismatch. Same fault, two codes, and a client that branches on SQLSTATE sees both.
        MemgresException e = recordShapeError(userFunc);
        // Both counts are the detail: which one is larger is visible from them, and the writer of
        // the definition list needs to know what the body actually produced.
        e.setDetail("Number of returned columns (" + produced
                + ") does not match expected column count (" + declared + ").");
        throw e;
    }

    /**
     * A column definition list does not convert what the body produced; it declares what the
     * caller says it is. A value of a kind the declared type could never hold means the two
     * descriptions are of different results, which is the same complaint as the wrong width.
     */
    private static void checkRecordColumnTypes(PgFunction userFunc, List<String> colAliases, Object[] firstRow) {
        for (int i = 0; i < colAliases.size() && i < firstRow.length; i++) {
            String def = colAliases.get(i);
            int sp = def == null ? -1 : def.indexOf(' ');
            if (sp <= 0) continue;
            String declared = def.substring(sp + 1).trim();
            String want = valueClass(declared);
            String got = valueClass(firstRow[i]);
            if (want == null || got == null || want.equals(got)) continue;
            MemgresException e = recordShapeError(userFunc);
            // PostgreSQL names both types and the column they disagree at, by name and by
            // position, so the writer of the definition list can see which entry to change.
            e.setDetail("Returned type " + producedTypeName(firstRow[i])
                    + " does not match expected type " + DataType.canonicalName(declared)
                    + " in column \"" + def.substring(0, sp).trim()
                    + "\" (position " + (i + 1) + ").");
            throw e;
        }
    }

    /** The type name PostgreSQL would print for a value the body produced. */
    private static String producedTypeName(Object value) {
        if (value instanceof String) return "text";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Integer) return "integer";
        if (value instanceof Long) return "bigint";
        if (value instanceof Short) return "smallint";
        if (value instanceof Float || value instanceof Double) return "double precision";
        return "numeric";
    }

    /**
     * The broad kind of value a declared type holds, or null for a type whose values this check
     * cannot tell apart from another's. Only kinds that can never stand in for each other are
     * named: a narrower comparison would refuse results PostgreSQL accepts.
     */
    private static String valueClass(String typeName) {
        if (typeName == null) return null;
        String t = typeName.trim().toLowerCase();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        if (t.endsWith("[]")) return null;
        switch (t) {
            case "smallint": case "int2": case "integer": case "int": case "int4":
            case "bigint": case "int8": case "numeric": case "decimal":
            case "real": case "float4": case "double precision": case "float8":
                return "number";
            case "text": case "varchar": case "character varying": case "char":
            case "character": case "bpchar": case "name":
                return "text";
            case "boolean": case "bool":
                return "boolean";
            default:
                return null;
        }
    }

    /** The same broad kind, read off a value, or null when the value does not say. */
    private static String valueClass(Object value) {
        if (value instanceof Number) return "number";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof String) return "text";
        return null;
    }

    private static String firstColAlias(List<String> colAliases, String fallback) {
        return (colAliases != null && !colAliases.isEmpty()) ? stripColType(colAliases.get(0)) : fallback;
    }

    /**
     * The column one column-definition entry describes. An entry that names a type -- {@code "x
     * integer"} -- gives the column that type; a bare name leaves it unknown, and text is what
     * the rest of this file falls back to.
     */
    private static Column columnFromDef(String def, int ordinal) {
        String name = stripColType(def);
        DataType type = null;
        int sp = def == null ? -1 : def.indexOf(' ');
        if (sp > 0) type = DataType.fromPgName(def.substring(sp + 1).trim());
        return new Column(name != null ? name : "column" + ordinal,
                type != null ? type : DataType.TEXT, true, false, null);
    }

    /** Strip type information from a column alias like "id integer" -> "id". */
    static String stripColType(String alias) {
        if (alias == null) return null;
        int sp = alias.indexOf(' ');
        return sp > 0 ? alias.substring(0, sp) : alias;
    }

    /** Strip type info from all column aliases. */
    static List<String> stripColTypes(List<String> colAliases) {
        if (colAliases == null) return null;
        List<String> result = new ArrayList<>();
        for (String ca : colAliases) {
            result.add(stripColType(ca));
        }
        return result;
    }

    /**
     * Resolve an implicit SERIAL sequence's current value from the table's serial counter.
     */
    private long[] resolveImplicitSerial(Database db, String qualifiedSeqName) {
        String seqName = CatalogHelper.nameOf(qualifiedSeqName);
        if (seqName.endsWith("_seq")) {
            String prefix = seqName.substring(0, seqName.length() - 4);
            int lastUnderscore = prefix.lastIndexOf('_');
            if (lastUnderscore > 0) {
                String tblName = prefix.substring(0, lastUnderscore);
                Schema schema = db.getSchema(CatalogHelper.schemaOf(qualifiedSeqName));
                if (schema != null) {
                    Table tbl = schema.getTable(tblName);
                    if (tbl != null) {
                        long counter = tbl.getSerialCounter();
                        if (counter > 1) {
                            return new long[]{counter - 1, 1};
                        } else {
                            return new long[]{1, 0};
                        }
                    }
                }
            }
        }
        return new long[]{1, 0};
    }

    static List<Object> toElementList(Object arr) {
        if (arr instanceof List<?>) {
            List<?> l = (List<?>) arr;
            List<Object> result = new ArrayList<>();
            for (Object e : l) result.add(e);
            return result;
        }
        if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
            // Quote- and nesting-aware parse (commas inside quoted elements are not separators)
            List<Object> parsed = FunctionEvaluator.parseSimplePgArray((String) arr);
            List<Object> result = new ArrayList<>();
            for (Object e : parsed) {
                result.add(e instanceof String ? parseNumericIfPossible((String) e) : e);
            }
            return result;
        }
        if (arr == null) return new ArrayList<>();
        return Cols.listOf(arr);
    }

    private static Object parseNumericIfPossible(String s) {
        if (s == null || s.isEmpty()) return s;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { }
        try { return Long.parseLong(s); } catch (NumberFormatException e) { }
        try { return new java.math.BigDecimal(s); } catch (NumberFormatException e) { }
        return s;
    }

    /**
     * Apply column aliases to a list of columns, modifying in place.
     * Shared by subquery and LATERAL handling code.
     */
    static List<Column> applyColumnAliases(List<Column> columns, List<String> aliases) {
        return applyColumnAliases(columns, aliases, null);
    }

    /**
     * As above, refusing an alias list longer than the relation is when the relation is named.
     *
     * <p>An alias list renames the columns a FROM item exposes, so naming more of them than exist
     * names nothing — PostgreSQL says so rather than dropping the extras, and dropping them left
     * {@code (SELECT 1, 2) AS t(a, b, c)} looking like it had worked. Fewer aliases than columns
     * is ordinary: the ones past the list keep their own names.
     */
    static List<Column> applyColumnAliases(List<Column> columns, List<String> aliases, String relation) {
        return applyColumnAliases(columns, aliases, relation, "table");
    }

    /**
     * The same, for a FROM item PostgreSQL names by something other than a relation -- a
     * parenthesised join, which it calls a join expression.
     */
    static List<Column> applyColumnAliases(List<Column> columns, List<String> aliases,
                                           String relation, String noun) {
        if (aliases == null) return columns;
        if (relation != null && aliases.size() > columns.size()) {
            throw new MemgresException(noun + " \"" + relation + "\" has " + columns.size()
                    + " columns available but " + aliases.size() + " columns specified", "42P10").suppressPosition();
        }
        List<Column> result = new ArrayList<>(columns);
        for (int i = 0; i < aliases.size() && i < result.size(); i++) {
            // A rename is a rename: the column keeps the type it had, down to the parts a bare
            // DataType does not carry — an element type, an enum's or a domain's name, a
            // precision. Rebuilt from the DataType alone, an integer[] came back as _int4.
            result.set(i, result.get(i).withName(aliases.get(i)));
        }
        return result;
    }

    // ---- JSON_TABLE ----

    private List<RowContext> resolveJsonTable(SelectStmt.FunctionFrom funcFrom, String alias) {
        if (funcFrom.args().isEmpty() || !(funcFrom.args().get(0) instanceof JsonTableExpr)) {
            throw new MemgresException("Invalid JSON_TABLE expression");
        }
        JsonTableExpr jt = (JsonTableExpr) funcFrom.args().get(0);

        // Evaluate input and path
        Object inputVal = executor.evalExpr(jt.input, null);
        if (inputVal == null) return new ArrayList<>();
        Object pathVal = executor.evalExpr(jt.path, null);
        if (pathVal == null) return new ArrayList<>();
        String json = inputVal.toString();
        String path = pathVal.toString();

        // Build column definitions
        List<Column> cols = new ArrayList<>();
        collectColumnDefs(jt.columns, cols);

        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();

        // Validate JSON input
        if (!ExprEvaluator.isValidJson(json)) {
            if (jt.onError == JsonExistsExpr.OnBehavior.ERROR) {
                ExprEvaluator.requireJson(json);
            }
            return contexts; // EMPTY ON ERROR (default)
        }

        // Extract rows using the root path
        try {
            List<String> rowJsons = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            for (int rowIdx = 0; rowIdx < rowJsons.size(); rowIdx++) {
                String rowJson = rowJsons.get(rowIdx);
                // Build rows — nested paths cause row multiplication
                List<List<Object>> expandedRows = buildJsonTableRows(jt.columns, rowJson, rowIdx);
                for (List<Object> rowValues : expandedRows) {
                    Object[] row = rowValues.toArray();
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        } catch (MemgresException e) {
            if (jt.onError == JsonExistsExpr.OnBehavior.ERROR) {
                throw e; // Preserve original SQLSTATE (42601 for jsonpath errors, etc.)
            }
            // Default: EMPTY ON ERROR — return empty result
        } catch (Exception e) {
            if (jt.onError == JsonExistsExpr.OnBehavior.ERROR) {
                throw new MemgresException("invalid input syntax for type json", "22P02");
            }
            // Default: EMPTY ON ERROR — return empty result
        }

        return contexts;
    }

    private void collectColumnDefs(List<JsonTableExpr.JsonTableColumn> columns, List<Column> cols) {
        for (JsonTableExpr.JsonTableColumn col : columns) {
            if (col.nestedColumns != null) {
                collectColumnDefs(col.nestedColumns, cols);
            } else {
                cols.add(new Column(col.name, col.forOrdinality ? DataType.INTEGER : DataType.TEXT, true, false, null));
            }
        }
    }

    private List<List<Object>> buildJsonTableRows(List<JsonTableExpr.JsonTableColumn> columns,
                                                    String rowJson, int rowIdx) {
        // Check if there's a nested column — if so, we need row multiplication
        int nestedIdx = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).nestedColumns != null) {
                nestedIdx = i;
                break;
            }
        }

        if (nestedIdx < 0) {
            // No nested columns — produce a single row
            List<Object> row = new ArrayList<>();
            for (JsonTableExpr.JsonTableColumn col : columns) {
                row.add(extractColumnValue(col, rowJson, rowIdx));
            }
            return Cols.listOf(row);
        }

        // Has nested column — extract non-nested values first, then multiply by nested rows
        List<Object> parentValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (i == nestedIdx) continue;
            parentValues.add(extractColumnValue(columns.get(i), rowJson, rowIdx));
        }

        // Evaluate nested path and expand
        JsonTableExpr.JsonTableColumn nestedCol = columns.get(nestedIdx);
        String nestedPath = nestedCol.nestedPath != null ? executor.evalExpr(nestedCol.nestedPath, null).toString() : "$";
        List<String> nestedJsons = executor.functionEvaluator.evaluateJsonPathAll(rowJson, nestedPath);

        List<List<Object>> result = new ArrayList<>();
        for (int ni = 0; ni < nestedJsons.size(); ni++) {
            String nestedJson = nestedJsons.get(ni);
            // Recursively build rows for nested columns (supports multi-level nesting)
            List<List<Object>> nestedRows = buildJsonTableRows(nestedCol.nestedColumns, nestedJson, ni);
            for (List<Object> nestedRow : nestedRows) {
                List<Object> row = new ArrayList<>();
                int parentIdx = 0;
                int nestedValIdx = 0;
                for (int i = 0; i < columns.size(); i++) {
                    if (i == nestedIdx) {
                        // Add all values from the nested row
                        row.addAll(nestedRow);
                    } else {
                        row.add(parentValues.get(parentIdx++));
                    }
                }
                result.add(row);
            }
        }

        if (result.isEmpty()) {
            // No nested results — produce one row with nulls for nested columns
            List<Object> row = new ArrayList<>();
            int parentIdx = 0;
            int nestedColCount = countLeafColumns(nestedCol.nestedColumns);
            for (int i = 0; i < columns.size(); i++) {
                if (i == nestedIdx) {
                    for (int nc = 0; nc < nestedColCount; nc++) {
                        row.add(null);
                    }
                } else {
                    row.add(parentValues.get(parentIdx++));
                }
            }
            result.add(row);
        }

        return result;
    }

    /** Count the total number of leaf columns (recursing into nested columns). */
    private int countLeafColumns(List<JsonTableExpr.JsonTableColumn> columns) {
        int count = 0;
        for (JsonTableExpr.JsonTableColumn col : columns) {
            if (col.nestedColumns != null) {
                count += countLeafColumns(col.nestedColumns);
            } else {
                count++;
            }
        }
        return count;
    }

    private Object extractColumnValue(JsonTableExpr.JsonTableColumn col, String rowJson, int rowIdx) {
        if (col.forOrdinality) {
            return rowIdx + 1;
        }
        if (col.existsPath) {
            String ep = col.pathExpr != null ? executor.evalExpr(col.pathExpr, null).toString() : "$";
            List<String> vals = executor.functionEvaluator.evaluateJsonPathAll(rowJson, ep);
            return !vals.isEmpty();
        }
        // Regular column: extract value via path
        String colPath = col.pathExpr != null ? executor.evalExpr(col.pathExpr, null).toString() : ("$." + col.name);
        try {
            List<String> vals = executor.functionEvaluator.evaluateJsonPathAll(rowJson, colPath);
            if (vals.isEmpty()) {
                if (col.defaultOnEmpty != null) {
                    return executor.evalExpr(col.defaultOnEmpty, null);
                }
                return null;
            }
            String raw = vals.get(0);
            // For jsonb/json columns, normalize with PG jsonb spacing
            if (col.typeName != null && (col.typeName.equalsIgnoreCase("jsonb") || col.typeName.equalsIgnoreCase("json"))) {
                return JsonOperations.normalizeJsonb(raw.trim());
            }
            return unquoteJsonString(raw);
        } catch (Exception e) {
            if (col.defaultOnError != null) {
                return executor.evalExpr(col.defaultOnError, null);
            }
            return null;
        }
    }

    private String unquoteJsonString(String val) {
        if (val == null) return null;
        val = val.trim();
        if (val.startsWith("\"") && val.endsWith("\"") && val.length() >= 2) {
            return val.substring(1, val.length() - 1);
        }
        if ("null".equals(val)) return null;
        return val;
    }

    // ---- pg_create_logical_replication_slot ----

    private List<RowContext> resolveCreateLogicalReplicationSlot(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.size() < 2) throw new MemgresException("function pg_create_logical_replication_slot requires at least 2 arguments", "42883");
        String slotName = String.valueOf(evalArgs.get(0));
        String plugin = String.valueOf(evalArgs.get(1));
        // Create the slot in the database
        executor.database.addReplicationSlot(new Database.ReplicationSlot(slotName, plugin, "logical"));
        // Return a single row with (slot_name text, lsn pg_lsn)
        String c1 = colAliases != null && colAliases.size() > 0 ? colAliases.get(0) : "slot_name";
        String c2 = colAliases != null && colAliases.size() > 1 ? colAliases.get(1) : "lsn";
        Column col1 = new Column(c1, DataType.TEXT, true, false, null);
        Column col2 = new Column(c2, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col1, col2));
        Object[] row = new Object[]{slotName, "0/0"};
        virtualTable.insertRow(row);
        List<RowContext> contexts = new ArrayList<>();
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    private List<RowContext> resolveCreatePhysicalReplicationSlot(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function pg_create_physical_replication_slot requires at least 1 argument", "42883");
        String slotName = String.valueOf(evalArgs.get(0));
        executor.database.addReplicationSlot(new Database.ReplicationSlot(slotName, null, "physical"));
        String c1 = colAliases != null && colAliases.size() > 0 ? colAliases.get(0) : "slot_name";
        String c2 = colAliases != null && colAliases.size() > 1 ? colAliases.get(1) : "lsn";
        Column col1 = new Column(c1, DataType.TEXT, true, false, null);
        Column col2 = new Column(c2, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col1, col2));
        Object[] row = new Object[]{slotName, null};
        virtualTable.insertRow(row);
        List<RowContext> contexts = new ArrayList<>();
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- pg_ls_dir (set of text, stub returns empty) ----

    private List<RowContext> resolvePgLsDir(String alias, List<String> colAliases) {
        String colName = colAliases != null && !colAliases.isEmpty() ? colAliases.get(0) : "pg_ls_dir";
        Column col = new Column(colName, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        return new ArrayList<>();
    }

    // ---- pg_ls_logdir / pg_ls_waldir / pg_ls_tmpdir / pg_ls_archive_statusdir ----
    // Returns set of (name text, size bigint, modification timestamptz) — empty stub

    private List<RowContext> resolvePgLsDirRecord(String alias, List<String> colAliases) {
        String c1 = colAliases != null && colAliases.size() > 0 ? colAliases.get(0) : "name";
        String c2 = colAliases != null && colAliases.size() > 1 ? colAliases.get(1) : "size";
        String c3 = colAliases != null && colAliases.size() > 2 ? colAliases.get(2) : "modification";
        List<Column> cols = Cols.listOf(
                new Column(c1, DataType.TEXT, true, false, null),
                new Column(c2, DataType.BIGINT, true, false, null),
                new Column(c3, DataType.TIMESTAMPTZ, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        return new ArrayList<>();
    }

    // ---- pg_partition_tree(regclass) ----
    // Returns set of (relid regclass, parentrelid regclass, isleaf boolean, level int)

    private List<RowContext> resolvePgPartitionTree(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) return new ArrayList<>();
        String tableName = evalArgs.get(0).toString();
        Table rootTable = executor.resolveTableAnySchema(tableName);
        if (rootTable == null) {
            throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
        }

        String c1 = colAliases != null && colAliases.size() > 0 ? colAliases.get(0) : "relid";
        String c2 = colAliases != null && colAliases.size() > 1 ? colAliases.get(1) : "parentrelid";
        String c3 = colAliases != null && colAliases.size() > 2 ? colAliases.get(2) : "isleaf";
        String c4 = colAliases != null && colAliases.size() > 3 ? colAliases.get(3) : "level";
        List<Column> cols = Cols.listOf(
                new Column(c1, DataType.TEXT, true, false, null),
                new Column(c2, DataType.TEXT, true, false, null),
                new Column(c3, DataType.BOOLEAN, true, false, null),
                new Column(c4, DataType.INTEGER, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();

        // Recursively collect partition tree
        collectPartitionTree(rootTable, null, 0, virtualTable, alias, contexts);
        return contexts;
    }

    private void collectPartitionTree(Table table, String parentName, int level,
                                       Table virtualTable, String alias, List<RowContext> contexts) {
        String name = table.getName();
        List<Table> partitions = table.getPartitions();
        boolean isLeaf = partitions == null || partitions.isEmpty();
        Object[] row = new Object[]{name, parentName, isLeaf, level};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, alias, row));
        if (!isLeaf) {
            for (Table child : partitions) {
                collectPartitionTree(child, name, level + 1, virtualTable, alias, contexts);
            }
        }
    }

    // ---- pg_partition_ancestors(regclass) ----
    // Returns set of regclass (the table itself and all its ancestors up to the root)

    private List<RowContext> resolvePgPartitionAncestors(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) return new ArrayList<>();
        String tableName = evalArgs.get(0).toString();
        Table table = executor.resolveTableAnySchema(tableName);
        if (table == null) {
            throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
        }

        // PostgreSQL declares the output parameter as relid, so an unaliased call is queried by
        // that name; naming the column after the function made every such query fail.
        String colName = colAliases != null && !colAliases.isEmpty() ? colAliases.get(0) : "relid";
        Column col = new Column(colName, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();

        // Walk up the partition hierarchy
        Table current = table;
        while (current != null) {
            Object[] row = new Object[]{current.getName()};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
            current = current.getPartitionParent();
        }
        return contexts;
    }

    // ---- pg_listening_channels ----

    private List<RowContext> resolvePgListeningChannels(String alias) {
        Column col = new Column(alias, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        if (executor.session != null) {
            List<?> channels = executor.database.getNotificationManager()
                    .getListeningChannels(executor.session);
            if (channels != null) {
                for (Object ch : channels) {
                    Object[] row = new Object[]{ch != null ? ch.toString() : null};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- ts_debug ----

    // ---- ts_stat ----

    private List<RowContext> resolveTsStat(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) {
            throw new MemgresException("function ts_stat(text) requires a text query argument", "42883");
        }
        String sql = evalArgs.get(0).toString();
        // Optional weights filter (2nd arg): only count entries with these weights.
        String weightFilter = evalArgs.size() > 1 && evalArgs.get(1) != null
                ? evalArgs.get(1).toString().toUpperCase() : null;
        QueryResult qr = executor.execute(sql);
        // word -> [ndoc, nentry]
        Map<String, long[]> stats = new TreeMap<>();
        if (qr != null && qr.getRows() != null) {
            for (Object[] row : qr.getRows()) {
                if (row.length == 0 || row[0] == null) continue;
                TsVector vec = row[0] instanceof TsVector
                        ? (TsVector) row[0] : TsVector.parseLiteral(row[0].toString());
                if (vec == null) continue;
                for (Map.Entry<String, List<TsVector.PosEntry>> e : vec.getLexemeMap().entrySet()) {
                    int entries = 0;
                    for (TsVector.PosEntry pe : e.getValue()) {
                        if (weightFilter == null || weightFilter.indexOf(pe.weight()) >= 0) entries++;
                    }
                    // A position-less lexeme counts as a single occurrence.
                    if (e.getValue().isEmpty() && weightFilter == null) entries = 1;
                    if (entries == 0) continue;
                    long[] s = stats.computeIfAbsent(e.getKey(), k -> new long[2]);
                    s[0] += 1;          // ndoc
                    s[1] += entries;    // nentry
                }
            }
        }
        String wordCol = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "word";
        String ndocCol = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "ndoc";
        String nentryCol = (colAliases != null && colAliases.size() > 2) ? colAliases.get(2) : "nentry";
        List<Column> cols = Cols.listOf(
                new Column(wordCol, DataType.TEXT, true, false, null),
                new Column(ndocCol, DataType.INTEGER, true, false, null),
                new Column(nentryCol, DataType.INTEGER, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Map.Entry<String, long[]> e : stats.entrySet()) {
            Object[] row = new Object[]{e.getKey(), (int) e.getValue()[0], (int) e.getValue()[1]};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    private List<RowContext> resolveTsDebug(String alias, List<String> colAliases, List<Object> evalArgs) {
        String config = "english";
        String input;
        if (evalArgs.size() >= 2) {
            config = String.valueOf(evalArgs.get(0));
            input = String.valueOf(evalArgs.get(1));
        } else {
            input = String.valueOf(evalArgs.get(0));
        }
        List<Object[]> debugRows = TextSearchOperations.tsDebug(config, input);
        List<Column> cols = Cols.listOf(
                new Column("alias", DataType.TEXT, true, false, null),
                new Column("description", DataType.TEXT, true, false, null),
                new Column("token", DataType.TEXT, true, false, null),
                new Column("dictionaries", DataType.TEXT, true, false, null),
                new Column("dictionary", DataType.TEXT, true, false, null),
                new Column("lexemes", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] dr : debugRows) {
            // The row already holds the six values ts_debug reports, with the two lists written
            // as arrays: wrapping them a second time nested each inside an array of one, and the
            // dictionary that handled the token is a name of its own, not that list.
            Object[] row = new Object[]{dr[0], dr[1], dr[2], dr[3], dr[4], dr[5]};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    // ---- ts_parse ----

    private List<RowContext> resolveTsParse(String alias, List<String> colAliases, List<Object> evalArgs) {
        String parserName = String.valueOf(evalArgs.get(0));
        String text = String.valueOf(evalArgs.get(1));
        List<Object[]> tokens = TextSearchOperations.tsParse(parserName, text);
        List<Column> cols = Cols.listOf(
                new Column("tokid", DataType.INTEGER, true, false, null),
                new Column("token", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] t : tokens) {
            Object[] row = new Object[]{t[0], t[1]};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    // ---- ts_token_type ----

    private List<RowContext> resolveTsTokenType(String alias, List<String> colAliases, List<Object> evalArgs) {
        String parserName = evalArgs.isEmpty() ? "default" : String.valueOf(evalArgs.get(0));
        List<Object[]> types = TextSearchOperations.tsTokenType(parserName);
        List<Column> cols = Cols.listOf(
                new Column("tokid", DataType.INTEGER, true, false, null),
                new Column("alias", DataType.TEXT, true, false, null),
                new Column("description", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] t : types) {
            Object[] row = new Object[]{t[0], t[1], t[2]};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    // ---- XMLTABLE ----

    private List<RowContext> resolveXmlTable(SelectStmt.FunctionFrom funcFrom, String alias) {
        List<Expression> args = funcFrom.args();
        if (args.size() < 2) return new ArrayList<>();

        // args[0] = xpath expression, args[1] = xml document, args[2..] = column definitions
        String xpath = executor.evalExpr(args.get(0), null).toString();
        Object xmlObj = executor.evalExpr(args.get(1), null);
        String xmlStr = xmlObj != null ? xmlObj.toString() : "";

        // Parse column definitions from args[2..]
        List<String> colNames = new ArrayList<>();
        List<String> colTypes = new ArrayList<>();
        List<String> colPaths = new ArrayList<>();
        for (int i = 2; i < args.size(); i++) {
            String def = args.get(i) instanceof com.memgres.engine.parser.ast.Literal
                    ? ((com.memgres.engine.parser.ast.Literal) args.get(i)).value()
                    : executor.evalExpr(args.get(i), null).toString();
            String[] parts = def.split(":", 3);
            colNames.add(parts[0]);
            colTypes.add(parts.length > 1 ? parts[1] : "text");
            colPaths.add(parts.length > 2 ? parts[2] : parts[0]);
        }

        // Use Java XPath to evaluate
        List<RowContext> contexts = new ArrayList<>();
        try {
            javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
            org.w3c.dom.Document doc = builder.parse(new org.xml.sax.InputSource(new java.io.StringReader(xmlStr)));
            javax.xml.xpath.XPathFactory xpathFactory = javax.xml.xpath.XPathFactory.newInstance();
            javax.xml.xpath.XPath xp = xpathFactory.newXPath();
            org.w3c.dom.NodeList rows = (org.w3c.dom.NodeList) xp.evaluate(xpath, doc, javax.xml.xpath.XPathConstants.NODESET);

            List<Column> cols = new ArrayList<>();
            for (int i = 0; i < colNames.size(); i++) {
                DataType dt = DataType.fromPgName(colTypes.get(i));
                cols.add(new Column(colNames.get(i), dt != null ? dt : DataType.TEXT, true, false, null));
            }
            Table virtualTable = new Table("__xmltable__", cols);

            for (int r = 0; r < rows.getLength(); r++) {
                org.w3c.dom.Node rowNode = rows.item(r);
                Object[] rowVals = new Object[colNames.size()];
                for (int c = 0; c < colNames.size(); c++) {
                    String colPath = colPaths.get(c);
                    try {
                        String val = xp.evaluate(colPath, rowNode);
                        if (val != null && !val.isEmpty()) {
                            DataType dt = DataType.fromPgName(colTypes.get(c));
                            // Each column is read in the type it was declared. Reading every
                            // integer column as an int threw for anything past 2147483647, and
                            // the catch below turned a bigint the document did carry into NULL.
                            if (dt == DataType.BIGINT) {
                                rowVals[c] = Long.parseLong(val.trim());
                            } else if (dt == DataType.SMALLINT) {
                                rowVals[c] = Short.parseShort(val.trim());
                            } else if (dt == DataType.INTEGER) {
                                rowVals[c] = Integer.parseInt(val.trim());
                            } else {
                                rowVals[c] = val;
                            }
                        }
                    } catch (Exception e) {
                        rowVals[c] = null;
                    }
                }
                virtualTable.insertRow(rowVals);
                contexts.add(new RowContext(virtualTable, alias, rowVals));
            }
        } catch (Exception e) {
            throw new MemgresException("XMLTABLE evaluation error: " + e.getMessage(), "42000");
        }
        return contexts;
    }

    // ---- hstore SRFs: skeys, svals, each ----

    private List<RowContext> resolveHstoreSkeys(String alias, List<String> colAliases,
                                                List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) return java.util.Collections.emptyList();
        HstoreValue h = evalArgs.get(0) instanceof HstoreValue
                ? (HstoreValue) evalArgs.get(0) : HstoreValue.parse(evalArgs.get(0).toString());
        String effectiveAlias = alias != null ? alias : "skeys";
        Column col = new Column(firstColAlias(colAliases, effectiveAlias),
                DataType.TEXT, true, false, null);
        Table vt = new Table(effectiveAlias, Cols.listOf(col));
        List<RowContext> rows = new ArrayList<>();
        for (String k : h.keys()) {
            rows.add(new RowContext(vt, effectiveAlias, new Object[]{k}));
        }
        return rows;
    }

    private List<RowContext> resolveHstoreSvals(String alias, List<String> colAliases,
                                                List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) return java.util.Collections.emptyList();
        HstoreValue h = evalArgs.get(0) instanceof HstoreValue
                ? (HstoreValue) evalArgs.get(0) : HstoreValue.parse(evalArgs.get(0).toString());
        String effectiveAlias = alias != null ? alias : "svals";
        Column col = new Column(firstColAlias(colAliases, effectiveAlias),
                DataType.TEXT, true, false, null);
        Table vt = new Table(effectiveAlias, Cols.listOf(col));
        List<RowContext> rows = new ArrayList<>();
        for (String v : h.values()) {
            rows.add(new RowContext(vt, effectiveAlias, new Object[]{v}));
        }
        return rows;
    }

    private List<RowContext> resolveHstoreEach(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty() || evalArgs.get(0) == null) return java.util.Collections.emptyList();
        HstoreValue h = evalArgs.get(0) instanceof HstoreValue
                ? (HstoreValue) evalArgs.get(0) : HstoreValue.parse(evalArgs.get(0).toString());
        String col1 = colAliases != null && colAliases.size() > 0 ? colAliases.get(0) : "key";
        String col2 = colAliases != null && colAliases.size() > 1 ? colAliases.get(1) : "value";
        String effectiveAlias = alias != null ? alias : "each";
        List<Column> cols = Cols.listOf(
                new Column(col1, DataType.TEXT, true, false, null),
                new Column(col2, DataType.TEXT, true, false, null));
        Table vt = new Table(effectiveAlias, cols);
        List<RowContext> rows = new ArrayList<>();
        for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
            rows.add(new RowContext(vt, effectiveAlias, new Object[]{e.getKey(), e.getValue()}));
        }
        return rows;
    }
}
