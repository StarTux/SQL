package com.winthier.sql;

import com.winthier.sql.condition.SQLComparison;
import com.winthier.sql.condition.SQLConditionList;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.persistence.PersistenceException;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class SQLUpdater<E> {
    private final SQLDatabase database;
    private final SQLTable<E> table;
    private E instance;
    private List<NewValue> valueList = new ArrayList<>();
    private SQLConditionList conditionList;
    private String sql;

    protected enum Operation {
        SET("="),
        SET_ATOMIC("="),
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        UPDATE("!");

        protected final String symbol;

        Operation(final String symbol) {
            this.symbol = symbol;
        }
    }

    @Data
    protected static final class NewValue {
        protected final SQLColumn column;
        protected final Operation operation;
        protected final Object value;
    }

    private void prepareComparisons() {
        if (instance != null) {
            Object id = table.getIdColumn().getValue(instance);
            if (id == null) throw new IllegalStateException("id is null!");
            if (conditionList == null) conditionList = SQLConditionList.and(table);
            conditionList.add(SQLComparison.eq(table.getIdColumn(), id));
        }
        for (NewValue newValue : valueList) {
            if (newValue.operation == Operation.SET_ATOMIC) {
                Object value = newValue.column.getValue(instance);
                if (conditionList == null) conditionList = SQLConditionList.and(table);
                conditionList.add(SQLComparison.eq(newValue.column, value));
            }
        }
    }

    public boolean sync() {
        if (instance != null && table.getIdColumn() == null) {
            throw new IllegalStateException("No id column: " + table.getTableName());
        }
        if (valueList.isEmpty()) {
            throw new IllegalStateException("Empty value list!");
        }
        prepareComparisons();
        List<Object> values = new ArrayList<>(1 + 2 * valueList.size());
        List<String> setters = new ArrayList<>(valueList.size());
        for (NewValue newValue : valueList) {
            String name = "`" + newValue.column.getColumnName() + "`";
            switch (newValue.operation) {
            case SET:
            case SET_ATOMIC:
                if (newValue.value == null) {
                    setters.add(name + " = NULL");
                } else {
                    setters.add(name + " = ?");
                    values.add(newValue.value);
                }
                break;
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
                setters.add(name + " = " + name + " " + newValue.operation.symbol + " ?");
                values.add(newValue.value);
                break;
            case UPDATE:
                if (instance == null) throw new NullPointerException("UPDATE: instance cannot be null");
                Object value = newValue.column.getValue(instance);
                if (value == null) {
                    setters.add(name + " = NULL");
                } else {
                    setters.add(name + " = ?");
                    values.add(value);
                }
                break;
            default:
                throw new IllegalStateException(newValue.operation + ": operation not implemented");
            }
        }
        sql = "UPDATE `" + table.getTableName() + "`"
            + " SET " + String.join(", ", setters)
            + (conditionList == null || conditionList.isEmpty() ? "" : " WHERE " + conditionList.compile(values));
        int ret;
        try (PreparedStatement statement = database.getConnection().prepareStatement(sql)) {
            SQLUtil.formatStatement(statement, values);
            database.debugLog(statement);
            ret = statement.executeUpdate();
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
        if (ret <= 0) return false;
        if (instance != null) {
            for (NewValue newValue : valueList) {
                switch (newValue.operation) {
                case UPDATE:
                    break;
                case SET:
                case SET_ATOMIC:
                    newValue.column.setValue(instance, newValue.value);
                    break;
                case ADD:
                case SUBTRACT:
                case MULTIPLY:
                case DIVIDE:
                default:
                    // We don't attempt to re-create mysql
                    // computations.  Clients must know that these
                    // operations don't update on their end.
                    break;
                }
            }
        }
        return true;
    }

    public void async(Consumer<Boolean> callback) {
        database.scheduleAsyncTask(() -> {
                boolean res = sync();
                if (callback != null) {
                    callback.accept(res);
                }
            });
    }

    public SQLUpdater<E> row(final E theInstance) {
        this.instance = theInstance;
        return this;
    }

    public SQLUpdater<E> addValue(final String key, final Object value, final Operation operation) {
        SQLColumn column = table.getColumn(key);
        if (column == null) throw new IllegalStateException("Column not found: " + key);
        valueList.add(new NewValue(column, operation, value));
        return this;
    }

    public SQLUpdater<E> set(final String key, final Object value) {
        return addValue(key, value, Operation.SET);
    }

    public SQLUpdater<E> atomic(final String key, final Object value) {
        return addValue(key, value, Operation.SET_ATOMIC);
    }

    public SQLUpdater<E> add(final String key, final Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("add: number expected!");
        return addValue(key, value, Operation.ADD);
    }

    public SQLUpdater<E> subtract(final String key, final Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("subtract: number expected!");
        return addValue(key, value, Operation.SUBTRACT);
    }

    public SQLUpdater<E> multiply(final String key, final Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("multiply: number expected!");
        return addValue(key, value, Operation.MULTIPLY);
    }

    public SQLUpdater<E> divide(final String key, final Object value) {
        if (!(value instanceof Number)) throw new IllegalArgumentException("divide: number expected!");
        return addValue(key, value, Operation.DIVIDE);
    }

    public SQLUpdater<E> update(final String... keys) {
        for (String key : keys) {
            addValue(key, null, Operation.UPDATE);
        }
        return this;
    }

    public SQLConditionList where() {
        if (conditionList == null) conditionList = SQLConditionList.and(table);
        return conditionList;
    }

    public SQLUpdater<E> where(Consumer<SQLConditionList> consumer) {
        consumer.accept(where());
        return this;
    }
}
