package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;

/**
 * Represents a column definition in a table.
 */
public class Column {

    private final String name;
    private final DataType type;
    private final boolean nullable;
    private final boolean primaryKey;
    private String defaultValue;       // String representation (for display/catalog)
    private Expression parsedDefaultExpr;     // Parsed AST (for evaluation; avoids re-parsing)
    private final String enumTypeName;
    private final Integer precision;
    private final Integer scale;
    private final String generatedExpr;
    private final boolean virtual;          // PG 18: VIRTUAL generated column (computed on read)
    private final String domainTypeName;
    private final String compositeTypeName;  // For composite type columns (e.g., "pair")
    private final DataType arrayElementType; // For array columns, the element type (e.g., INTEGER for integer[])
    private int tableOid;    // PgWire RowDescription: source table OID (0 if not from a real table)
    private short attNum;    // PgWire RowDescription: column attribute number (0 if not from a real table)
    private short attStattarget = -1;  // pg_attribute.attstattarget (-1 = use system default)
    private String attStorageOverride;  // pg_attribute.attstorage override (null = use type default)
    private String attCompression = "";  // pg_attribute.attcompression (empty = default, "p" = pglz, "l" = lz4)
    private boolean attHasMissing;       // pg_attribute.atthasmissing (true when added via ALTER TABLE ADD COLUMN with DEFAULT)

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
        c.parsedDefaultExpr = parsedDefaultExpr;
        return c;
    }

    public String getName() { return name; }
    public DataType getType() { return type; }
    public boolean isNullable() { return nullable; }
    public boolean isPrimaryKey() { return primaryKey; }
    public String getDefaultValue() { return defaultValue; }
    public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }
    public Expression getParsedDefaultExpr() { return parsedDefaultExpr; }
    public void setParsedDefaultExpr(Expression expr) { this.parsedDefaultExpr = expr; }
    public String getEnumTypeName() { return enumTypeName; }
    public Integer getPrecision() { return precision; }
    public Integer getScale() { return scale; }
    public String getGeneratedExpr() { return generatedExpr; }
    public boolean isGenerated() { return generatedExpr != null; }
    public boolean isVirtual() { return virtual; }
    public String getDomainTypeName() { return domainTypeName; }
    public String getCompositeTypeName() { return compositeTypeName; }
    public DataType getArrayElementType() { return arrayElementType; }
    public int getTableOid() { return tableOid; }
    public void setTableOid(int tableOid) { this.tableOid = tableOid; }
    public short getAttNum() { return attNum; }
    public void setAttNum(short attNum) { this.attNum = attNum; }
    public short getAttStattarget() { return attStattarget; }
    public void setAttStattarget(short attStattarget) { this.attStattarget = attStattarget; }
    public String getAttStorageOverride() { return attStorageOverride; }
    public void setAttStorageOverride(String attStorageOverride) { this.attStorageOverride = attStorageOverride; }
    public String getAttCompression() { return attCompression; }
    public void setAttCompression(String attCompression) { this.attCompression = attCompression; }
    public boolean isAttHasMissing() { return attHasMissing; }
    public void setAttHasMissing(boolean attHasMissing) { this.attHasMissing = attHasMissing; }
}
