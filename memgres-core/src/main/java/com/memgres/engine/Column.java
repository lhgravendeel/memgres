package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;

/**
 * Represents a column definition in a table.
 */
public class Column {

    private final String name;
    private final DataType type;
    private final boolean nullable;
    private boolean primaryKey;
    private String defaultValue;       // String representation (for display/catalog)
    private Expression parsedDefaultExpr;     // Parsed AST (for evaluation; avoids re-parsing)
    private String enumTypeName;
    private final Integer precision;
    private final Integer scale;
    private final String generatedExpr;
    /**
     * The generation expression as a parse tree, worked out the first time a row needs it and
     * kept from then on. See {@link #getGeneratedExprAst()}.
     */
    private volatile Expression generatedExprAst;
    private final boolean virtual;          // PG 18: VIRTUAL generated column (computed on read)
    private String domainTypeName;
    /** The collation this column sorts by, when one was written for it. */
    private String collation;

    private String compositeTypeName;  // For composite type columns (e.g., "pair")

    /**
     * The range type this column was declared with, e.g. the {@code r} of {@code CREATE TYPE r AS
     * RANGE (subtype = int4)}. A range's values are carried as the text they print as, so without
     * this the column was indistinguishable from a text one and the catalogue named text where
     * PostgreSQL names the range.
     */
    private String rangeTypeName;


    private final DataType arrayElementType; // For array columns, the element type (e.g., INTEGER for integer[])
    private int tableOid;    // PgWire RowDescription: source table OID (0 if not from a real table)
    private short attNum;    // PgWire RowDescription: column attribute number (0 if not from a real table)
    // pg_attribute.attstattarget: null means the column has no target of its own and the system
    // default applies, which is what PostgreSQL 17 and later report there. A short could not hold
    // the 10000 ceiling either, so a larger target wrapped negative on the way in.
    private Integer attStattarget;
    private String attStorageOverride;  // pg_attribute.attstorage override (null = use type default)
    private String attCompression = "";  // pg_attribute.attcompression (empty = default, "p" = pglz, "l" = lz4)
    private boolean attHasMissing;       // pg_attribute.atthasmissing (true when added via ALTER TABLE ADD COLUMN with DEFAULT)
    /**
     * The field qualifier of an interval column, lower case and without its precision:
     * "year to month", "day to second". Null for every other column, and for a plain interval.
     * The qualifier is part of the type modifier rather than of the type, so it travels here
     * alongside the precision instead of turning "interval day" into a type of its own.
     */
    private String intervalQualifier;

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue) {
        this(name, type, nullable, primaryKey, defaultValue, null, null, null, null, null, null, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue, String enumTypeName) {
        this(name, type, nullable, primaryKey, defaultValue, enumTypeName, null, null, null, null, null, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue,
                  String enumTypeName, Integer precision, Integer scale) {
        this(name, type, nullable, primaryKey, defaultValue, enumTypeName, precision, scale, null, null, null, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue,
                  String enumTypeName, Integer precision, Integer scale, String generatedExpr) {
        this(name, type, nullable, primaryKey, defaultValue, enumTypeName, precision, scale, generatedExpr, null, null, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue,
                  String enumTypeName, Integer precision, Integer scale, String generatedExpr, String domainTypeName) {
        this(name, type, nullable, primaryKey, defaultValue, enumTypeName, precision, scale, generatedExpr, domainTypeName, null, null);
    }

    /** A result column holding one value of a composite type, named by that type. */
    public static Column ofCompositeType(String name, String compositeTypeName) {
        return new Column(name, DataType.TEXT, true, false, null, null, null, null, null,
                null, compositeTypeName, null);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue,
                  String enumTypeName, Integer precision, Integer scale, String generatedExpr, String domainTypeName,
                  String compositeTypeName, DataType arrayElementType) {
        this(name, type, nullable, primaryKey, defaultValue, enumTypeName, precision, scale, generatedExpr, false, domainTypeName, compositeTypeName, arrayElementType);
    }

    public Column(String name, DataType type, boolean nullable, boolean primaryKey, String defaultValue,
                  String enumTypeName, Integer precision, Integer scale, String generatedExpr, boolean virtual,
                  String domainTypeName, String compositeTypeName, DataType arrayElementType) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.primaryKey = primaryKey;
        this.defaultValue = defaultValue;
        this.enumTypeName = enumTypeName;
        this.precision = precision;
        this.scale = scale;
        this.generatedExpr = generatedExpr;
        this.virtual = virtual;
        this.domainTypeName = domainTypeName;
        this.compositeTypeName = compositeTypeName;
        this.arrayElementType = arrayElementType;
        // Pre-parse the default expression if present
        if (defaultValue != null) {
            try {
                this.parsedDefaultExpr = new com.memgres.engine.parser.Parser(
                        new com.memgres.engine.parser.Lexer(defaultValue).tokenize()
                ).parseExpression();
            } catch (Exception e) {
                // If parsing fails, leave null; evaluateDefault will fall back to string
                this.parsedDefaultExpr = null;
            }
        }
    }

    /**
     * Copies the mutable runtime attributes (catalog/wire metadata that lives outside the
     * constructor) onto another column. Used by the {@code with*} copy methods so ALTER
     * TABLE operations that rebuild a column never silently lose these attributes.
     */
    private void copyRuntimeAttrsTo(Column c) {
        c.tableOid = tableOid;
        c.attNum = attNum;
        c.attStattarget = attStattarget;
        c.attStorageOverride = attStorageOverride;
        c.attCompression = attCompression;
        c.attHasMissing = attHasMissing;
        c.intervalQualifier = intervalQualifier;
        c.rangeTypeName = rangeTypeName;
    }

    /** Copy of this column with a new name; every other attribute is preserved. */
    public Column withName(String newName) {
        Column c = new Column(newName, type, nullable, primaryKey, defaultValue, enumTypeName,
                precision, scale, generatedExpr, virtual, domainTypeName, compositeTypeName, arrayElementType);
        copyRuntimeAttrsTo(c);
        c.parsedDefaultExpr = parsedDefaultExpr;
        return c;
    }

    /** Copy of this column with a new nullability; every other attribute is preserved. */
    public Column withNullable(boolean newNullable) {
        Column c = new Column(name, type, newNullable, primaryKey, defaultValue, enumTypeName,
                precision, scale, generatedExpr, virtual, domainTypeName, compositeTypeName, arrayElementType);
        copyRuntimeAttrsTo(c);
        c.parsedDefaultExpr = parsedDefaultExpr;
        return c;
    }

    /** Copy of this column with a new default expression; every other attribute is preserved. */
    public Column withDefault(String newDefault) {
        Column c = new Column(name, type, nullable, primaryKey, newDefault, enumTypeName,
                precision, scale, generatedExpr, virtual, domainTypeName, compositeTypeName, arrayElementType);
        copyRuntimeAttrsTo(c);
        // parsedDefaultExpr intentionally NOT copied: the constructor re-parses the new default
        return c;
    }

    /** Copy of this column with a new generation expression; every other attribute is preserved. */
    public Column withGeneratedExpr(String newGeneratedExpr) {
        Column c = new Column(name, type, nullable, primaryKey, defaultValue, enumTypeName,
                precision, scale, newGeneratedExpr, virtual, domainTypeName, compositeTypeName, arrayElementType);
        copyRuntimeAttrsTo(c);
        c.parsedDefaultExpr = parsedDefaultExpr;
        return c;
    }

    /**
     * Copy of this column with a replaced type spec (type, precision/scale, enum identity,
     * array element type — all coming from the new declaration). Name, nullability, PK flag,
     * default, generation expression/virtual flag, and runtime attributes are preserved.
     * Domain/composite identity is dropped: ALTER COLUMN TYPE replaces the type spec entirely.
     */
    public Column withType(DataType newType, Integer newPrecision, Integer newScale,
                           String newEnumTypeName, DataType newArrayElementType) {
        Column c = new Column(name, newType, nullable, primaryKey, defaultValue, newEnumTypeName,
                newPrecision, newScale, generatedExpr, virtual, null, null, newArrayElementType);
        copyRuntimeAttrsTo(c);
        c.intervalQualifier = null;  // the new declaration carries its own qualifier, if any
        c.rangeTypeName = null;      // and its own type, which the range identity belonged to
        c.parsedDefaultExpr = parsedDefaultExpr;
        return c;
    }

    public String getCollation() { return collation; }

    public void setCollation(String collation) { this.collation = collation; }

    public String getName() { return name; }
    public DataType getType() { return type; }
    public boolean isNullable() { return nullable; }
    public boolean isPrimaryKey() { return primaryKey; }
    /** Clear or set the flag when the table's PRIMARY KEY constraint is dropped or added. */
    public void setPrimaryKey(boolean primaryKey) { this.primaryKey = primaryKey; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public Expression getParsedDefaultExpr() { return parsedDefaultExpr; }
    public void setParsedDefaultExpr(Expression expr) { this.parsedDefaultExpr = expr; }
    public String getEnumTypeName() { return enumTypeName; }
    /** ALTER TYPE ... RENAME TO: the column keeps its enum, which now answers to another name. */
    public void setEnumTypeName(String enumTypeName) { this.enumTypeName = enumTypeName; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }
    public String getGeneratedExpr() { return generatedExpr; }

    /**
     * The generation expression's parse tree, or null when the column has no generation
     * expression.
     *
     * <p>PostgreSQL computes a generated column from the stored expression against the row's own
     * values, so the tree is what the value comes from. Writing the row's values into the
     * expression's text and reading that text back as SQL makes a value that spells a column name
     * into that column and a value that spells SQL into SQL. Parsed once, since the text never
     * changes: ALTER TABLE ... SET EXPRESSION builds a new column.
     */
    public Expression getGeneratedExprAst() {
        Expression ast = generatedExprAst;
        if (ast == null && generatedExpr != null) {
            ast = com.memgres.engine.parser.Parser.parseExpression(generatedExpr);
            generatedExprAst = ast;
        }
        return ast;
    }

    public boolean isGenerated() { return generatedExpr != null; }
    public boolean isVirtual() { return virtual; }
    public String getDomainTypeName() { return domainTypeName; }
    /** ALTER DOMAIN ... RENAME TO: the column keeps its domain, which now answers to another name. */
    public void setDomainTypeName(String domainTypeName) { this.domainTypeName = domainTypeName; }
    public String getCompositeTypeName() { return compositeTypeName; }
    /** ALTER TYPE ... RENAME TO: the column keeps its composite under its new name. */
    public void setCompositeTypeName(String compositeTypeName) { this.compositeTypeName = compositeTypeName; }
    public String getRangeTypeName() { return rangeTypeName; }
    /** ALTER TYPE ... RENAME TO: the column keeps its range under its new name. */
    public void setRangeTypeName(String rangeTypeName) { this.rangeTypeName = rangeTypeName; }
    public DataType getArrayElementType() { return arrayElementType; }
    public int getTableOid() { return tableOid; }
    public void setTableOid(int tableOid) { this.tableOid = tableOid; }
    public short getAttNum() { return attNum; }
    public void setAttNum(short attNum) { this.attNum = attNum; }
    public Integer getAttStattarget() { return attStattarget; }
    public void setAttStattarget(Integer attStattarget) { this.attStattarget = attStattarget; }
    public String getAttStorageOverride() { return attStorageOverride; }
    public void setAttStorageOverride(String attStorageOverride) { this.attStorageOverride = attStorageOverride; }
    public String getAttCompression() { return attCompression; }
    public void setAttCompression(String attCompression) { this.attCompression = attCompression; }
    public String getIntervalQualifier() { return intervalQualifier; }
    public void setIntervalQualifier(String intervalQualifier) { this.intervalQualifier = intervalQualifier; }
    public boolean isAttHasMissing() { return attHasMissing; }
    public void setAttHasMissing(boolean attHasMissing) { this.attHasMissing = attHasMissing; }
}
