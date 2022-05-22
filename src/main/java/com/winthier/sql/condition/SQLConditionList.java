package com.winthier.sql.condition;

import com.winthier.sql.SQLColumn;
import com.winthier.sql.SQLTable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class SQLConditionList implements SQLCondition {
    private final SQLTable table;
    private final Conjunction conjunction;
    private List<SQLCondition> list = new ArrayList<>();

    public enum Conjunction {
        AND(" AND "),
        OR(" OR ");

        protected final String delim;

        Conjunction(final String delim) {
            this.delim = delim;
        }
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public SQLConditionList add(SQLCondition condition) {
        list.add(condition);
        return this;
    }

    public static SQLConditionList and(SQLTable table) {
        return new SQLConditionList(table, Conjunction.AND);
    }

    public static SQLConditionList or(SQLTable table) {
        return new SQLConditionList(table, Conjunction.OR);
    }

    @Override
    public String compile(List<Object> values) {
        if (list.size() == 1) {
            return list.get(0).compile(values);
        }
        List<String> components = new ArrayList<>();
        for (SQLCondition condition : list) {
            components.add(condition.compile(values));
        }
        return "(" + String.join(conjunction.delim, components) + ")";
    }

    public boolean isAnd() {
        return conjunction == Conjunction.AND;
    }

    public boolean isOr() {
        return conjunction == Conjunction.OR;
    }

    public SQLConditionList and() {
        if (isAnd()) return this;
        SQLConditionList sublist = SQLConditionList.and(table);
        list.add(sublist);
        return sublist;
    }

    public SQLConditionList and(Consumer<SQLConditionList> consumer) {
        consumer.accept(and());
        return this;
    }

    public SQLConditionList or() {
        if (isOr()) return this;
        SQLConditionList sublist = SQLConditionList.or(table);
        list.add(sublist);
        return sublist;
    }

    public SQLConditionList or(Consumer<SQLConditionList> consumer) {
        consumer.accept(or());
        return this;
    }

    private SQLConditionList compare(final String label, SQLComparison.Comparator comparator, Object value, Object rvalue) {
        SQLColumn column = table.getColumn(label);
        if (column == null) throw new IllegalStateException("Column not found: " + label);
        SQLComparison comparison = new SQLComparison(column, comparator, value, rvalue);
        list.add(comparison);
        return this;
    }

    public SQLConditionList eq(String label, Object value) {
        compare(label, SQLComparison.Comparator.EQ, value, null);
        return this;
    }

    public SQLConditionList isNull(String label) {
        compare(label, SQLComparison.Comparator.EQ, null, null);
        return this;
    }

    public SQLConditionList neq(String label, Object value) {
        compare(label, SQLComparison.Comparator.NEQ, value, null);
        return this;
    }

    public SQLConditionList isNotNull(String label) {
        compare(label, SQLComparison.Comparator.NEQ, null, null);
        return this;
    }

    public SQLConditionList lt(String label, Object value) {
        compare(label, SQLComparison.Comparator.LT, value, null);
        return this;
    }

    public SQLConditionList gt(String label, Object value) {
        compare(label, SQLComparison.Comparator.GT, value, null);
        return this;
    }

    public SQLConditionList lte(String label, Object value) {
        compare(label, SQLComparison.Comparator.LTE, value, null);
        return this;
    }

    public SQLConditionList gte(String label, Object value) {
        compare(label, SQLComparison.Comparator.GTE, value, null);
        return this;
    }

    public SQLConditionList like(String label, Object value) {
        compare(label, SQLComparison.Comparator.LIKE, value, null);
        return this;
    }

    public SQLConditionList between(String label, Object value, Object rvalue) {
        compare(label, SQLComparison.Comparator.BETWEEN, value, rvalue);
        return this;
    }

    @Override
    public String toString() {
        List<String> strings = new ArrayList<>(list.size());
        for (SQLCondition it : list) {
            strings.add(it.toString());
        }
        return conjunction + "(" + String.join(", ", strings) + ")";
    }
}
