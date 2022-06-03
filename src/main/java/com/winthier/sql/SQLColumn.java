package com.winthier.sql;

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
import javax.persistence.PersistenceException;
import lombok.Getter;
import lombok.Setter;

@Getter
public final class SQLColumn {
    private final SQLTable table;
    private final Field field;
    private String columnName;
    private String columnDefinition;
    private final boolean nullable;
    private final int length;
    private final int precision;
    private final SQLType type;
    private final boolean id;
    @Setter private boolean unique;
    private Method getterMethod;
    private Method setterMethod;

    protected SQLColumn(final SQLTable table, final Field field) {
        this.table = table;
        this.field = field;
        Column columnAnnotation = field.getAnnotation(Column.class);
        this.id = field.getAnnotation(Id.class) != null;
        if (columnAnnotation != null) {
            this.columnName = columnAnnotation.name();
            this.columnDefinition = columnAnnotation.columnDefinition();
            this.nullable = columnAnnotation.nullable() && !id;
            this.length = columnAnnotation.length();
            this.precision = columnAnnotation.precision();
            this.unique = columnAnnotation.unique();
        } else {
            this.nullable = !id;
            this.length = 255;
            this.precision = 0;
            this.unique = this.id;
        }
        type = SQLType.of(field);
        if (columnName == null || columnName.isEmpty()) {
            if (type == SQLType.REFERENCE) {
                columnName = SQLUtil.camelToLowerCase(field.getName()) + "_id";
            } else {
                columnName = SQLUtil.camelToLowerCase(field.getName());
            }
        }
        String getterName;
        String fieldCamel = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
        if (field.getType() == boolean.class) {
            getterName = "is" + fieldCamel;
        } else {
            getterName = "get" + fieldCamel;
        }
        String setterName = "set" + fieldCamel;
        try {
            getterMethod = field.getDeclaringClass().getMethod(getterName);
            setterMethod = field.getDeclaringClass().getMethod(setterName, field.getType());
        } catch (NoSuchMethodException nsme) {
            throw new PersistenceException(nsme);
        }
    }

    public String getColumnName() {
        return columnName;
    }

    private String getTypeDefinition() {
        switch (type) {
        case INT:
            return precision > 0
                ? "int(" + precision + ")"
                : "int";
        case LONG:
            return precision > 0
                ? "bigint(" + precision + ")"
                : "bigint";
        case STRING:
            if (length > 16777215) {
                return "longtext";
            } else if (length > 65535) {
                return "mediumtext";
            } else if (length > 1024) {
                return "text";
            } else {
                return "varchar(" + length + ")";
            }
        case UUID:
            return "varchar(40)";
        case FLOAT:
            return "float";
        case DOUBLE:
            return "double";
        case DATE:
            return "datetime";
        case BOOLEAN:
            return "tinyint";
        case ENUM:
            return "int";
        case REFERENCE:
            return "int";
        default:
            throw new IllegalArgumentException("Type definition not implemented: " + type);
        }
    }

    protected String getColumnDefinition() {
        if (columnDefinition == null || columnDefinition.isEmpty()) {
            if (id) {
                columnDefinition = getTypeDefinition() + " AUTO_INCREMENT";
            } else if (!nullable) {
                columnDefinition = getTypeDefinition() + " NOT NULL";
            } else {
                columnDefinition = getTypeDefinition();
            }
        }
        return columnDefinition;
    }

    protected String getCreateTableFragment() {
        return "`" + getColumnName() + "` " + getColumnDefinition();
    }

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
                        Method method = field.getType().getMethod("values");
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
                    SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(field.getType());
                    value = refTable.find(connection, num);
                }
                break;
            default:
                value = result.getObject(getColumnName(), field.getType());
            }
            setterMethod.invoke(instance, value);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
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
                SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(field.getType());
                if (refTable.getIdColumn() == null) {
                    throw new NullPointerException("Referenced table has no id column: " + value.getClass().getName());
                }
                if (!(value instanceof SQLRow row)) {
                    throw new IllegalArgumentException("Required type " + field.getType().getName() + " (SQLRow)"
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
                SQLTable<? extends SQLRow> refTable = table.getDatabase().findTable(field.getType());
                if (refTable.getIdColumn() == null) {
                    throw new NullPointerException("Referenced table has no id column: " + value.getClass().getName());
                }
                if (!(value instanceof SQLRow row)) {
                    throw new IllegalArgumentException("Required type " + field.getType().getName() + " (SQLRow)"
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
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
        }
    }

    protected void setValue(SQLRow instance, Object value) {
        try {
            setterMethod.invoke(instance, value);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
        }
    }
}
