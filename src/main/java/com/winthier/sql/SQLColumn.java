package com.winthier.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Id;
import lombok.Getter;

public final class SQLColumn {
    private final SQLTable table;
    private final Field field;
    @Getter private final Class<?> fieldType;
    @Getter private String columnName;
    @Getter private final String fieldName;
    private String defaultValueString;
    @Getter private String columnDefinition;
    private boolean notNull;
    private boolean autoIncrement;
    private int length;
    private final int precision;
    @Getter private final SQLType type;
    @Getter private boolean id = false;
    @Getter private boolean unique;
    private Method getterMethod;
    private Method setterMethod;
    @Getter private String keyName;
    @Getter private String uniqueKeyName;

    protected SQLColumn(final SQLTable table, final Field field) {
        this.table = table;
        this.field = field;
        this.fieldType = field.getType();
        this.notNull = table.isNotNull();
        Column columnAnnotation = field.getAnnotation(Column.class);
        if (field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(SQLRow.Id.class)) {
            id = true;
            autoIncrement = true;
            notNull = true;
        }
        if (columnAnnotation != null) {
            this.columnName = columnAnnotation.name();
            this.columnDefinition = columnAnnotation.columnDefinition();
            this.notNull = !columnAnnotation.nullable();
            this.length = columnAnnotation.length();
            this.precision = columnAnnotation.precision();
            this.unique = columnAnnotation.unique();
        } else {
            this.length = 255;
            this.precision = 0;
            this.unique = this.id;
        }
        type = SQLType.of(field);
        String typeDefinition = null;
        for (Annotation annotation : field.getDeclaredAnnotations()) {
            if (annotation instanceof SQLRow.LongText) {
                typeDefinition = "longtext";
            } else if (annotation instanceof SQLRow.MediumText) {
                typeDefinition = "mediumtext";
            } else if (annotation instanceof SQLRow.Text) {
                typeDefinition = "text";
            } else if (annotation instanceof SQLRow.Char chr) {
                length = chr.value();
                typeDefinition = "char(" + length + ")";
            } else if (annotation instanceof SQLRow.VarChar varChar) {
                length = varChar.value();
                typeDefinition = "varchar(" + length + ")";
            } else if (annotation instanceof SQLRow.Binary binary) {
                length = binary.value();
                typeDefinition = "binary(" + length + ")";
            } else if (annotation instanceof SQLRow.VarBinary varBinary) {
                length = varBinary.value();
                typeDefinition = "varbinary(" + length + ")";
            } else if (annotation instanceof SQLRow.TinyInt) {
                typeDefinition = "tinyint";
            } else if (annotation instanceof SQLRow.SmallInt) {
                typeDefinition = "smallint";
            } else if (annotation instanceof SQLRow.MediumInt) {
                typeDefinition = "mediumint";
            } else if (annotation instanceof SQLRow.Int) {
                typeDefinition = "int";
            } else if (annotation instanceof SQLRow.BigInt) {
                typeDefinition = "bigint";
            } else if (annotation instanceof SQLRow.Default dfl) {
                defaultValueString = dfl.value();
            } else if (annotation instanceof SQLRow.NotNull) {
                notNull = true;
            } else if (annotation instanceof SQLRow.Nullable) {
                notNull = false;
            } else if (annotation instanceof SQLRow.Name name) {
                this.columnName = name.value();
            } else if (annotation instanceof SQLRow.Keyed keyed) {
                this.keyName = keyed.value();
            } else if (annotation instanceof SQLRow.Unique uq) {
                this.uniqueKeyName = uq.value();
            }
        }
        if (columnDefinition == null || columnDefinition.isEmpty()) {
            if (typeDefinition == null) {
                typeDefinition = computeTypeDefinition();
            }
            columnDefinition = typeDefinition
                + (!notNull && !id
                   ? ""
                   : " NOT NULL")
                + (!autoIncrement
                   ? ""
                   : " AUTO_INCREMENT")
                + (defaultValueString == null || defaultValueString.isEmpty()
                   ? (!notNull && !id ? " DEFAULT NULL" : "")
                   : " DEFAULT " + defaultValueString);
        }
        this.fieldName = field.getName();
        if (columnName == null || columnName.isEmpty()) {
            if (type == SQLType.REFERENCE) {
                columnName = SQLUtil.camelToLowerCase(fieldName) + "_id";
            } else {
                columnName = SQLUtil.camelToLowerCase(fieldName);
            }
        }
        String getterName;
        String fieldCamel = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        if (fieldType == boolean.class) {
            getterName = fieldCamel.startsWith("Is")
                ? "is" + fieldCamel.substring(2)
                : "is" + fieldCamel;
        } else {
            getterName = "get" + fieldCamel;
        }
        String setterName = fieldCamel.startsWith("Is")
            ? "set" + fieldCamel.substring(2)
            : "set" + fieldCamel;
        try {
            getterMethod = field.getDeclaringClass().getMethod(getterName);
            setterMethod = field.getDeclaringClass().getMethod(setterName, fieldType);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalStateException(nsme);
        }
    }

    private String computeTypeDefinition() {
        return switch (type) {
        case INT -> "int" + computePrecisionDefinition();
        case LONG -> "bigint" + computePrecisionDefinition();
        case STRING -> computeStringTypeDefinition();
        case UUID -> "varchar(40)";
        case FLOAT -> "float";
        case DOUBLE -> "double";
        case DATE -> "datetime";
        case BOOLEAN -> "tinyint";
        case ENUM -> "int";
        case REFERENCE -> "int";
        case BYTE_ARRAY -> "binary(" + length + ")";
        };
    }

    private String computeStringTypeDefinition() {
        if (length > 16777215) return "longtext";
        if (length > 65535) return "mediumtext";
        if (length > 1024) return "text";
        return "varchar(" + length + ")";
    }

    private String computePrecisionDefinition() {
        return precision > 0
            ? "(" + precision + ")"
            : "";
    }

    protected String getCreateTableFragment() {
        return "`" + getColumnName() + "` " + columnDefinition;
    }

    /**
     * Load (set) a value from the ResultSet into the instance.
     */
    protected void load(Connection connection, SQLRow instance, ResultSet result) {
        try {
            Object value;
            switch (type) {
            case UUID:
                String str = result.getObject(getColumnName(), String.class);
                if (str == null) {
                    value = null;
                } else {
                    value = UUID.fromString(str);
                }
                break;
            case DATE:
                value = result.getObject(getColumnName(), java.sql.Timestamp.class);
                break;
            case ENUM:
                int num = result.getInt(getColumnName());
                if (num == 0 && result.wasNull()) {
                    value = null;
                } else {
                    try {
                        Method method = fieldType.getMethod("values");
                        Object arr = method.invoke(null);
                        value = Array.get(arr, num);
                    } catch (Exception e) {
                        table.getDatabase().getPlugin().getLogger()
                            .warning("Error loading enum from " + table.getTableName() + "." + columnName);
                        e.printStackTrace();
                        value = null;
                    }
                }
                break;
            case REFERENCE:
                num = result.getInt(getColumnName());
                if (num == 0 && result.wasNull()) {
                    value = null;
                } else {
                    SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(fieldType);
                    value = refTable.find(connection, num);
                }
                break;
            default:
                value = result.getObject(getColumnName(), fieldType);
            }
            setterMethod.invoke(instance, value);
        } catch (SQLException sqle) {
            throw new IllegalStateException(sqle);
        } catch (IllegalAccessException iae) {
            throw new IllegalStateException(iae);
        } catch (InvocationTargetException ite) {
            throw new IllegalStateException(ite);
        }
    }

    public Object getObject(Connection connection, ResultSet result) {
        try {
            switch (type) {
            case UUID:
                String str = result.getObject(getColumnName(), String.class);
                if (str == null) {
                    return null;
                } else {
                    return UUID.fromString(str);
                }
            case DATE:
                return result.getObject(getColumnName(), java.sql.Timestamp.class); // ???
            case ENUM:
                int num = result.getInt(getColumnName());
                if (num == 0 && result.wasNull()) {
                    return null;
                } else {
                    try {
                        Method method = fieldType.getMethod("values");
                        Object arr = method.invoke(null);
                        return Array.get(arr, num);
                    } catch (Exception e) {
                        table.getDatabase().getPlugin().getLogger()
                            .warning("Error loading enum from " + table.getTableName() + "." + columnName);
                        e.printStackTrace();
                        return null;
                    }
                }
            case REFERENCE:
                num = result.getInt(getColumnName());
                if (num == 0 && result.wasNull()) {
                    return null;
                } else {
                    SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(fieldType);
                    return refTable.find(connection, num);
                }
            default:
                return result.getObject(getColumnName(), fieldType);
            }
        } catch (SQLException sqle) {
            throw new IllegalStateException(sqle);
        }
    }

    /**
     * Create a VALUE assignment for
     * ```
     * INSERT INTO `tblName` (columnNames) VALUES (...), (...)
     * ```
     * kind of statements.  The statement will be compiled later, with
     * question mark placeholders.
     * @param instance The SQLRow instance
     * @param fragments The list of Strings to add to, with placeholder
     * @param values The list of object values.
     */
    protected void createSaveFragment(SQLRow instance, StringBuilder sb, List<Object> values) {
        Object value = getValue(instance);
        if (value == null) {
            sb.append("NULL");
        } else {
            sb.append("?");
            if (type == SQLType.REFERENCE) {
                SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(fieldType);
                if (refTable.getIdColumn() == null) {
                    throw new NullPointerException("Referenced table has no id column: " + value.getClass().getName());
                }
                if (!(value instanceof SQLRow row)) {
                    throw new IllegalArgumentException("Required type " + fieldType.getName() + " (SQLRow)"
                                                       + ", got " + value.getClass().getName());
                }
                Object refId = refTable.getIdColumn().getValue(row);
                if (refId == null) throw new NullPointerException("Referenced table has no id: " + value.getClass().getName() + ": " + value);
                values.add(refId);
            } else if (type == SQLType.ENUM) {
                values.add(((Enum) value).ordinal());
            } else {
                values.add(value);
            }
        }
    }

    /**
     * Note that this wants the value instead of the instance, unlike the above!
     */
    protected String createSetFragment(Object value, List<Object> values) {
        if (value == null) {
            return "`" + columnName + "` = NULL";
        } else {
            if (type == SQLType.REFERENCE) {
                SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(fieldType);
                if (refTable.getIdColumn() == null) {
                    throw new NullPointerException("Referenced table has no id column: " + value.getClass().getName());
                }
                if (!(value instanceof SQLRow row)) {
                    throw new IllegalArgumentException("Required type " + fieldType.getName() + " (SQLRow)"
                                                       + ", got " + value.getClass().getName());
                }
                Object refId = refTable.getIdColumn().getValue(row);
                if (refId == null) throw new NullPointerException("Referenced table has no id: " + value.getClass().getName() + ": " + value);
                values.add(refId);
            } else if (type == SQLType.ENUM) {
                values.add(((Enum) value).ordinal());
            } else {
                values.add(value);
            }
            return "`" + columnName + "` = ?";
        }
    }

    protected Object getValue(SQLRow instance) {
        try {
            return getterMethod.invoke(instance);
        } catch (IllegalAccessException iae) {
            throw new IllegalStateException(iae);
        } catch (InvocationTargetException ite) {
            throw new IllegalStateException(ite);
        }
    }

    protected void setValue(SQLRow instance, Object value) {
        try {
            setterMethod.invoke(instance, value);
        } catch (IllegalAccessException iae) {
            throw new IllegalStateException(iae);
        } catch (InvocationTargetException ite) {
            throw new IllegalStateException(ite);
        }
    }

    public boolean hasDefaultValue() {
        return defaultValueString != null && !defaultValueString.isEmpty();
    }
}
