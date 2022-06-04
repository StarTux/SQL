package com.winthier.sql;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.UUID;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;

/**
 * Java types representable in SQL.  Changes here may need to be reflected in other places:
 * - SQLColumn#getTypeDefinition
 * - SQLUtil#formatStatement (maybe)
 */
enum SQLType {
    INT,
    LONG,
    STRING,
    UUID,
    FLOAT,
    DOUBLE,
    DATE,
    BOOLEAN,
    ENUM,
    BYTE_ARRAY,
    REFERENCE;

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
        } else if (field.getAnnotation(ManyToOne.class) != null || field.getAnnotation(OneToOne.class) != null) {
            return REFERENCE;
        } else if (fieldType == byte[].class) {
            return BYTE_ARRAY;
        } else {
            throw new IllegalArgumentException("No SQL type found for " + fieldType.getName());
        }
    }
}
