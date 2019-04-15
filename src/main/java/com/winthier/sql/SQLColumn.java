package com.winthier.sql;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.EntityNotFoundException;
import javax.persistence.Id;
import javax.persistence.PersistenceException;
import javax.persistence.Version;
import lombok.Getter;
import lombok.Setter;

@Getter
final class SQLColumn {
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
    private final boolean version;
    private Method getterMethod;
    private Method setterMethod;

    SQLColumn(SQLTable table, Field field) {
        this.table = table;
        this.field = field;
        Column columnAnnotation = field.getAnnotation(Column.class);
        this.id = field.getAnnotation(Id.class) != null;
        this.version = field.getAnnotation(Version.class) != null;
        if (columnAnnotation != null) {
            this.columnName = columnAnnotation.name();
            this.columnDefinition = columnAnnotation.columnDefinition();
            this.nullable = columnAnnotation.nullable() && !id && !version;
            this.length = columnAnnotation.length();
            this.precision = columnAnnotation.precision() > 0 ? columnAnnotation.precision() : 11;
            this.unique = columnAnnotation.unique();
        } else {
            this.nullable = !id && !version;
            this.length = 255;
            this.precision = 11;
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
            return "int(" + precision + ")";
        case STRING:
            if (length > 1024) {
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
        case BOOL:
            return "tinyint(1)";
        case ENUM:
            return "int(11)";
        case REFERENCE:
            return "int(11)";
        default:
            throw new IllegalArgumentException("Type definition not implemented: " + type);
        }
    }

    String getColumnDefinition() {
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

    String getCreateTableFragment() {
        return "`" + getColumnName() + "` " + getColumnDefinition();
    }

    void load(Connection connection, Object inst, ResultSet result) {
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
                        // TODO better error handling!
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
                    SQLTable refTable = table.getDatabase().getTable(field.getType());
                    if (refTable == null) {
                        throw new EntityNotFoundException("Table not registered: " + field.getType());
                    }
                    value = refTable.find(connection, num);
                }
                break;
            default:
                value = result.getObject(getColumnName(), field.getType());
            }
            setterMethod.invoke(inst, value);
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
     * @param inst The column Object instance
     * @param fragments The list of Strings to add to, with placeholder
     * @param values The list of object values.
     */
    void createSaveFragment(Object inst, StringBuilder sb, List<Object> values) {
        Object value = getValue(inst);
        if (value == null) {
            sb.append("NULL");
        } else {
            sb.append("?");
            if (type == SQLType.REFERENCE) {
                SQLTable refTable = table.getDatabase().getTable(field.getType());
                if (refTable == null) {
                    throw new EntityNotFoundException("Table not registered: " + field.getType());
                }
                if (refTable.getIdColumn() == null) {
                    throw new NullPointerException("Referenced table has no id column: " + value.getClass().getName());
                }
                Object refId = refTable.getIdColumn().getValue(value);
                if (refId == null) throw new NullPointerException("Referenced table has no id: " + value.getClass().getName() + ": " + value);
                values.add(refId);
            } else if (type == SQLType.ENUM) {
                values.add(((Enum)value).ordinal());
            } else {
                values.add(value);
            }
        }
    }

    void updateVersionValue(Object inst) {
        Object newValue;
        if (type == SQLType.INT) {
            Object oldValue = getValue(inst);
            if (oldValue == null) {
                newValue = 1;
            } else {
                newValue = (Integer)oldValue + 1;
            }
        } else if (type == SQLType.DATE) {
            newValue = new Timestamp((System.currentTimeMillis() / 1000L) * 1000L);
        } else {
            throw new PersistenceException("Unsupported version type: " + field.getType());
        }
        setValue(inst, newValue);
    }

    Object getValue(Object inst) {
        try {
            return getterMethod.invoke(inst);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
        }
    }

    void setValue(Object inst, Object value) {
        try {
            setterMethod.invoke(inst, value);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
        }
    }
}
