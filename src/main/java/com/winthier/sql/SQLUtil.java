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
        for (int index = 1; index <= values.size(); index += 1) {
            Object value = values.get(index - 1);
            if (value instanceof String s) {
                statement.setString(index, s);
            } else if (value instanceof Date date) {
                statement.setTimestamp(index, new Timestamp(date.getTime()));
            } else if (value instanceof Boolean b) {
                statement.setBoolean(index, b);
            } else if (value instanceof Integer i) {
                statement.setInt(index, i);
            } else if (value instanceof Long l) {
                statement.setLong(index, l);
            } else if (value instanceof Float f) {
                statement.setFloat(index, f);
            } else if (value instanceof Double d) {
                statement.setDouble(index, d);
            } else if (value instanceof UUID uuid) {
                statement.setString(index, uuid.toString());
            } else if (value instanceof Enum en) {
                statement.setInt(index, en.ordinal());
            } else if (value instanceof byte[] byteArray) {
                statement.setBytes(index, byteArray);
            } else {
                String name = value == null ? "null" : value.getClass().getName();
                throw new IllegalArgumentException("Unexpected type in '" + statement + "': " + name + ", " + values + ", index=" + index);
            }
        }
    }
}
