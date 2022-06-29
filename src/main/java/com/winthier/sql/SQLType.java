package com.winthier.sql;

import java.lang.reflect.Field;
import java.sql.Blob;
import java.util.Date;
import java.util.UUID;

/**
 * Java types representable in SQL.  Changes here may need to be reflected in other places:
 * - SQLColumn#computeTypeDefinition
 * - SQLUtil#formatStatement (optional)
 * - SQLColumn#load (optional)
 */
enum SQLType {
    INT(Integer.class, int.class),
    LONG(Long.class, int.class),
    FLOAT(Float.class, float.class),
    DOUBLE(Double.class, double.class),
    BOOLEAN(Boolean.class, boolean.class),
    STRING(String.class),
    UUID(UUID.class),
    DATE(Date.class),
    ENUM(Enum.class),
    BYTE_ARRAY(byte[].class),
    REFERENCE(SQLRow.class),
    BLOB(Blob.class);

    private final Class<?>[] classes;

    SQLType(final Class<?>... classes) {
        this.classes = classes;
    }

    public boolean canYield(Class<?> clazz) {
        for (Class<?> it : classes) {
            if (it == clazz) return true;
        }
        return false;
    }

    static SQLType of(Field field) {
        Class<?> fieldType = field.getType();
        if (fieldType == Integer.class || fieldType == int.class) {
            return INT;
        } else if (fieldType == Long.class || fieldType == long.class) {
            return LONG;
        } else if (fieldType == String.class) {
            return STRING;
        } else if (fieldType == UUID.class) {
            return UUID;
        } else if (fieldType == Float.class || fieldType == float.class) {
            return FLOAT;
        } else if (fieldType == Double.class || fieldType == double.class) {
            return DOUBLE;
        } else if (Date.class.isAssignableFrom(fieldType)) {
            return DATE;
        } else if (fieldType == Boolean.class || fieldType == boolean.class) {
            return BOOLEAN;
        } else if (Enum.class.isAssignableFrom(fieldType)) {
            return ENUM;
        } else if (fieldType == byte[].class) {
            return BYTE_ARRAY;
        } else if (SQLRow.class.isAssignableFrom(fieldType)) {
            return REFERENCE;
        } else if (Blob.class.isAssignableFrom(fieldType)) {
            return BLOB;
        } else {
            throw new IllegalArgumentException("No SQL type found for " + fieldType.getName());
        }
    }
}
