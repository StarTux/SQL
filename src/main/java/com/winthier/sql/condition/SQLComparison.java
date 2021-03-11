package com.winthier.sql.condition;

import com.winthier.sql.SQLColumn;
import java.util.List;

/**
 * A comparrison within a where clause.
 * Instances are supposed to be created via SQLConditionList.
 */
public final class SQLComparison implements SQLCondition {
    public final SQLColumn column;
    public final Comparator comparator;
    public final Object value;
    public final Object rvalue;

    public SQLComparison(final SQLColumn column, final Comparator comparator, final Object value, final Object rvalue) {
        switch (comparator) {
        case EQ:
        case NEQ:
            if (rvalue != null) throw new IllegalArgumentException(comparator + ": rvalue must be null");
            break;
        case LT:
        case GT:
        case LTE:
        case GTE:
            if (rvalue != null) throw new IllegalArgumentException(comparator + ": rvalue must be null");
            if (value == null) throw new IllegalArgumentException(comparator + ": value cannot be null");
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException(comparator + ": number required!");
            }
            break;
        case LIKE:
            if (rvalue != null) throw new IllegalArgumentException(comparator + ": rvalue must be null");
            if (!(value instanceof String)) {
                throw new IllegalArgumentException(comparator + ": string required!");
            }
            break;
        case BETWEEN:
            if (value == null) throw new IllegalArgumentException(comparator + ": value cannot be null");
            if (rvalue == null) throw new IllegalArgumentException(comparator + ": rvalue cannot be null");
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException(comparator + ": value: number required!");
            }
            if (!(rvalue instanceof Number)) {
                throw new IllegalArgumentException(comparator + ": rvalue: number required!");
            }
            break;
        default:
            throw new IllegalArgumentException(comparator + ": comparator not implemented");
        }
        this.column = column;
        this.comparator = comparator;
        this.value = value;
        this.rvalue = rvalue;
    }

    public SQLComparison(final SQLColumn column, final Comparator comparator, final Object value) {
        this(column, comparator, value, null);
    }

    public enum Comparator {
        EQ("="),
        NEQ("!="),
        LT("<"),
        GT(">"),
        LTE("<="),
        GTE(">="),
        LIKE("LIKE"),
        BETWEEN("BETWEEN");

        public final String symbol;

        Comparator(final String symbol) {
            this.symbol = symbol;
        }
    }

    @Override
    public String compile(List<Object> values) {
        String name = "`" + column.getColumnName() + "`";
        switch (comparator) {
        case EQ:
            if (value == null) {
                return name + " IS NULL";
            } else {
                values.add(value);
                return name + " = ?";
            }
        case NEQ:
            if (value == null) {
                return name + " IS NOT NULL";
            } else {
                values.add(value);
                return name + " != ?";
            }
        case BETWEEN:
            values.add(value);
            values.add(rvalue);
            return name + " BETWEEN ? AND ?";
        default:
            values.add(value);
            return name + " " + comparator.symbol + " ?";
        }
    }

    public static SQLComparison eq(SQLColumn column, Object value) {
        return new SQLComparison(column, Comparator.EQ, value, null);
    }
}
