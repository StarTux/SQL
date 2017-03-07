package com.winthier.sql;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.UUID;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;

enum SQLType {
    INT,
    STRING,
    UUID,
    FLOAT,
    DOUBLE,
    DATE,
    BOOL,
    ENUM,
    REFERENCE;

    static SQLType of(Field field) {
        Class<?> fieldType = field.getType();
        if (fieldType == Integer.class || fieldType == int.class) {
            return SQLType.INT;
        } else if (fieldType == String.class) {
            return SQLType.STRING;
        } else if (fieldType == UUID.class) {
            return SQLType.UUID;
        } else if (fieldType == Float.class || fieldType == float.class) {
            return SQLType.FLOAT;
        } else if (fieldType == Double.class || fieldType == double.class) {
            return SQLType.DOUBLE;
        } else if (Date.class.isAssignableFrom(fieldType)) {
            return SQLType.DATE;
        } else if (fieldType == Boolean.class || fieldType == boolean.class) {
            return SQLType.BOOL;
        } else if (Enum.class.isAssignableFrom(fieldType)) {
            return SQLType.ENUM;
        } else if (field.getAnnotation(ManyToOne.class) != null || field.getAnnotation(OneToOne.class) != null) {
            return SQLType.REFERENCE;
        } else {
            throw new IllegalArgumentException("No SQL type found for " + fieldType.getName());
        }
    }
}
