package com.winthier.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public final class SQLUtil {
    private SQLUtil() { }

    public static List<String> splitCamelCase(String src) {
        List<String> tokens = new ArrayList<>();
        int wordStart = 0;
        char c = src.charAt(0);
        int capsCount = Character.isUpperCase(c) ? 1 : 0;
        for (int i = 1; i < src.length(); ++i) {
            c = src.charAt(i);
            if (Character.isUpperCase(c)) {
                switch (capsCount) {
                case 0:
                    tokens.add(src.substring(wordStart, i));
                    wordStart = i;
                    break;
                default:
                    break;
                }
                capsCount += 1;
            } else {
                switch (capsCount) {
                case 0:
                case 1:
                    break;
                default:
                    tokens.add(src.substring(wordStart, i - 1));
                    wordStart = i - 1;
                }
                capsCount = 0;
            }
        }
        tokens.add(src.substring(wordStart, src.length()));
        return tokens;
    }

    public static String glue(List<String> tokens, String glue) {
        if (tokens.isEmpty()) return "";
        StringBuilder builder = new StringBuilder(tokens.get(0));
        for (int i = 1; i < tokens.size(); ++i) builder.append(glue).append(tokens.get(i));
        return builder.toString();
    }

    public static String camelToLowerCase(String src) {
        List<String> tokens = splitCamelCase(src);
        return glue(tokens, "_").toLowerCase();
    }

    public static void formatStatement(PreparedStatement statement, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); ++i) {
            Object value = values.get(i);
            int index = i + 1;
            if (value instanceof String) {
                statement.setString(index, (String)value);
            } else if (value instanceof Date) {
                statement.setTimestamp(index, new Timestamp(((Date)value).getTime()));
            } else if (value instanceof Boolean) {
                statement.setBoolean(index, (Boolean)value);
            } else if (value instanceof Integer) {
                statement.setInt(index, (Integer)value);
            } else if (value instanceof Float) {
                statement.setFloat(index, (Float)value);
            } else if (value instanceof Double) {
                statement.setDouble(index, (Double)value);
            } else if (value instanceof UUID) {
                statement.setString(index, value.toString());
            } else if (value instanceof Enum) {
                statement.setString(index, ((Enum)value).name());
            } else {
                throw new IllegalArgumentException("Unexpected type: " + value.getClass().getName());
            }
        }
    }
}
