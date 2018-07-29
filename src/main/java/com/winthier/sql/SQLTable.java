package com.winthier.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.persistence.OneToMany;
import javax.persistence.OptimisticLockException;
import javax.persistence.PersistenceException;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Value;

@Getter
public final class SQLTable<E> {
    private final Class<E> clazz;
    private final SQLDatabase database;
    private String tableName;
    private List<SQLColumn> columns;
    private SQLColumn idColumn;
    private SQLColumn versionColumn;
    private List<Key> keys;

    @Value
    static class Key {
        private String name;
        private List<SQLColumn> columns;

        static Key of(SQLColumn column) {
            return new Key(column.getColumnName(), Arrays.asList(column));
        }
    }

    SQLTable(Class<E> clazz, SQLDatabase database) {
        this.clazz = clazz;
        this.database = database;
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        String tablePrefix = database == null ? "" : database.getConfig().getPrefix();
        if (tableAnnotation != null) {
            tableName = tablePrefix + tableAnnotation.name();
        }
        if (tableName == null || tableName.isEmpty()) {
            tableName = tablePrefix + SQLUtil.camelToLowerCase(clazz.getSimpleName());
        }
    }

    List<SQLColumn> getColumns() {
        if (columns == null) {
            columns = new ArrayList<>();
            for (Field field: clazz.getDeclaredFields()) {
                if (Modifier.isTransient(field.getModifiers())
                    || Modifier.isStatic(field.getModifiers())
                    || Modifier.isFinal(field.getModifiers())
                    || field.getAnnotation(OneToMany.class) != null
                    || Collection.class.isAssignableFrom(field.getType())
                    || Map.class.isAssignableFrom(field.getType())) continue;
                SQLColumn column = new SQLColumn(this, field);
                columns.add(column);
                if (column.isId()) idColumn = column;
                if (column.isVersion()) versionColumn = column;
            }
        }
        return columns;
    }

    List<Key> getKeys() {
        if (keys == null) {
            keys = new ArrayList<>();
            Table tableAnnotation = clazz.getAnnotation(Table.class);
            if (tableAnnotation != null) {
                UniqueConstraint[] constraints = tableAnnotation.uniqueConstraints();
                if (constraints != null) {
                    int counter = 0;
                    for (UniqueConstraint constraint: constraints) {
                        counter += 1;
                        String name = "uq_" + getTableName() + "_" + counter;
                        List<SQLColumn> constraintColumns = new ArrayList<>();
                        for (String columnName: constraint.columnNames()) {
                            SQLColumn column = getColumn(columnName);
                            if (column == null) {
                                throw new IllegalArgumentException(clazz.getName() + ": Column for unique constraint not found: " + columnName);
                            } else {
                                constraintColumns.add(column);
                            }
                        }
                        keys.add(new Key(name, constraintColumns));
                    }
                }
            }
            for (SQLColumn column: getColumns()) {
                if (column.isUnique()) keys.add(Key.of(column));
            }
        }
        return keys;
    }

    SQLColumn getColumn(String label) {
        for (SQLColumn column: getColumns()) {
            if (column.getColumnName().equals(label)) return column;
            if (column.getField().getName().equals(label)) return column;
        }
        return null;
    }

    public String getCreateTableStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(getTableName()).append("` (\n  ");
        sb.append(getColumns().get(0).getCreateTableFragment());
        for (int i = 1; i < getColumns().size(); ++i) {
            sb.append(",\n  ");
            sb.append(getColumns().get(i).getCreateTableFragment());
        }
        if (idColumn != null) sb.append(",\n  PRIMARY KEY (`").append(idColumn.getColumnName()).append("`)");
        for (Key key: getKeys()) {
            sb.append(",\n  UNIQUE KEY `").append(key.getName()).append("` (`");
            List<SQLColumn> keyColumns = key.getColumns();
            sb.append(keyColumns.get(0).getColumnName());
            for (int i = 1; i < keyColumns.size(); ++i) {
                sb.append("`, `").append(keyColumns.get(i).getColumnName());
            }
            sb.append("`)");
        }
        sb.append("\n)");
        return sb.toString();
    }

    public int getRowCount() {
        try (Statement statement = database.getConnection().createStatement()) {
            String sql = "SELECT COUNT(*) `count` FROM `" + getTableName() + "`";
            database.debugLog(sql);
            ResultSet result = statement.executeQuery(sql);
            result.next();
            return result.getInt("count");
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public E createInstance(ResultSet result) {
        try {
            E row = clazz.newInstance();
            for (SQLColumn column: getColumns()) {
                column.load(row, result);
            }
            if (row instanceof SQLInterface) {
                ((SQLInterface)row).onLoad(result);
            }
            return row;
        } catch (InstantiationException ie) {
            throw new PersistenceException(ie);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        }
    }

    public int saveIgnore(E inst) {
        return save(inst, true);
    }

    public int save(E inst) {
        return save(inst, false);
    }

    private int save(E inst, boolean doIgnore) {
        StringBuilder sb = new StringBuilder();
        final String idCheck, versionCheck;
        Integer idValue = idColumn == null ? null : (Integer)idColumn.getValue(inst);
        List<Object> values = new ArrayList<>();
        if (idValue == null) {
            if (doIgnore) {
                sb.append("INSERT IGNORE INTO `" + getTableName() + "` SET ");
            } else {
                sb.append("INSERT INTO `" + getTableName() + "` SET ");
            }
            idCheck = null;
        } else {
            sb.append("UPDATE `" + getTableName() + "` SET ");
            idCheck = " WHERE `" + idColumn.getColumnName() + "` = " + idValue;
        }
        Object versionValue = versionColumn == null ? null : versionColumn.getValue(inst);
        if (versionValue != null) {
            versionCheck = " AND `" + versionColumn.getColumnName() + "` = ?";
        } else {
            versionCheck = null;
        }
        if (versionColumn != null) versionColumn.updateVersionValue(inst);
        List<String> fragments = new ArrayList<>();
        for (SQLColumn column: getColumns()) {
            if (column.isId()) continue;
            column.createSaveFragment(inst, fragments, values);
        }
        sb.append(fragments.get(0));
        for (int i = 1; i < fragments.size(); ++i) sb.append(", ").append(fragments.get(i));
        if (idCheck != null) {
            sb.append(idCheck);
        }
        if (versionCheck != null) {
            sb.append(versionCheck);
            values.add(versionValue);
        }
        try (PreparedStatement statement = database.getConnection().prepareStatement(sb.toString(), Statement.RETURN_GENERATED_KEYS)) {
            SQLUtil.formatStatement(statement, values);
            database.debugLog(statement);
            int ret = statement.executeUpdate();
            if (ret != 1) throw new OptimisticLockException("Failed to save row " + getTableName() + ": " + inst + ": " + statement);
            if (idColumn != null && idValue == null) {
                ResultSet result = statement.getGeneratedKeys();
                if (result.next()) {
                    int newId = result.getInt(1);
                    if (idColumn != null) {
                        idColumn.setValue(inst, newId);
                    }
                }
            }
            return ret;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public int delete(Object inst) {
        if (inst instanceof Collection) {
            Collection<?> col = (Collection<?>)inst;
            if (col.isEmpty()) return -1;
            if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
            Iterator<?> iter = col.iterator();
            StringBuilder sb = new StringBuilder();
            sb.append((Integer)idColumn.getValue(iter.next()));
            while (iter.hasNext()) {
                sb.append(", ").append((Integer)idColumn.getValue(iter.next()));
            }
            return database.executeUpdate("DELETE FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " IN (" + sb.toString() + ")");
        } else {
            if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
            Integer id = (Integer)idColumn.getValue(inst);
            if (id == null) throw new PersistenceException("Id not set: " + inst);
            return database.executeUpdate("DELETE FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " = " + id);
        }
    }

    public E find(int id) {
        if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
        try (Statement statement = database.getConnection().createStatement()) {
            String sql = "SELECT * FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " = " + id;
            database.debugLog(sql);
            ResultSet result = statement.executeQuery(sql);
            E row;
            if (result.next()) {
                row = createInstance(result);
            } else {
                row = null;
            }
            result.close();
            return row;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public Finder find() {
        return new Finder();
    }

    enum Comparison {
        EQ("="),
        NEQ("!="),
        LT("<"),
        GT(">"),
        LTE("<="),
        GTE(">="),
        LIKE("LIKE");
        public final String symbol;
        Comparison(String symbol) {
            this.symbol = symbol;
        }
    }

    public final class Finder {
        private final StringBuilder sb = new StringBuilder();
        private final List<Object> values = new ArrayList<>();
        private String conj = " WHERE ";
        private int offset = -1, limit = -1;
        private final List<String> order = new ArrayList<>();
        private static final String DEFAULT_CONJ = " AND ";

        Finder() { }

        private Finder compare(String label, Comparison comp, Object value) {
            if (value == null) throw new IllegalArgumentException("Value cannot be null!");
            SQLColumn column = getColumn(label);
            if (column == null) throw new IllegalArgumentException("Column not found in " + clazz.getName() + ": " + label);
            String columnName = column.getColumnName();
            sb.append(conj).append("`").append(columnName).append("`").append(" " + comp.symbol + " ?");
            if (column.getType() == SQLType.REFERENCE) {
                SQLTable refTable = database.getTable(column.getField().getType());
                if (refTable == null) throw new PersistenceException("Table not registered: " + column.getField().getType().getName());
                if (refTable.idColumn == null) throw new PersistenceException("Referenced table lacks id column: " + refTable.getTableName());
                values.add(refTable.idColumn.getValue(value));
            } else {
                values.add(value);
            }
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder idEq(int id) {
            if (idColumn == null) throw new IllegalArgumentException("idEq() requires id column!");
            sb.append(conj).append("`").append(idColumn.getColumnName()).append("` = ").append(id);
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder eq(String label, Object value) {
            return compare(label, Comparison.EQ, value);
        }

        public Finder ieq(String label, String value) {
            return compare(label, Comparison.EQ, value);
        }

        public Finder neq(String label, Object value) {
            return compare(label, Comparison.NEQ, value);
        }

        public Finder gt(String label, Object value) {
            return compare(label, Comparison.GT, value);
        }

        public Finder gte(String label, Object value) {
            return compare(label, Comparison.GTE, value);
        }

        public Finder lt(String label, Object value) {
            return compare(label, Comparison.LT, value);
        }

        public Finder lte(String label, Object value) {
            return compare(label, Comparison.LTE, value);
        }

        public Finder like(String label, String value) {
            return compare(label, Comparison.LIKE, value);
        }

        public Finder in(String label, Collection<?> col) {
            SQLColumn column = getColumn(label);
            if (column == null) throw new IllegalArgumentException("Column not found in " + clazz.getName() + ": " + label);
            String columnName = column.getColumnName();
            Iterator<?> iter = col.iterator();
            if (!iter.hasNext()) {
                sb.append(conj).append("`").append(columnName).append("`").append(" != `").append(columnName).append("`");
                conj = DEFAULT_CONJ;
                return this;
            }
            sb.append(conj).append("`").append(columnName).append("`").append(" IN (?");
            if (column.getType() == SQLType.REFERENCE) {
                values.add(database.getTable(column.getField().getType()).idColumn.getValue(iter.next()));
            } else {
                values.add(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(", ?");
                if (column.getType() == SQLType.REFERENCE) {
                    values.add(database.getTable(column.getField().getType()).idColumn.getValue(iter.next()));
                } else {
                    values.add(iter.next());
                }
            }
            sb.append(")");
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder isNull(String label) {
            SQLColumn column = getColumn(label);
            if (column == null) throw new IllegalArgumentException("Column not found in " + clazz.getName() + ": " + label);
            sb.append(conj).append("`").append(column.getColumnName()).append("` IS NULL");
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder isNotNull(String label) {
            SQLColumn column = getColumn(label);
            if (column == null) throw new IllegalArgumentException("Column not found in " + clazz.getName() + ": " + label);
            sb.append(conj).append("`").append(column.getColumnName()).append("` IS NOT NULL");
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder or() {
            conj = " OR ";
            return this;
        }

        private Finder orderBy(String label, String direction) {
            SQLColumn column = getColumn(label);
            if (column == null) throw new IllegalArgumentException("Column not found in " + clazz.getName() + ": " + label);
            order.add("`" + column.getColumnName() + "` " + direction);
            return this;
        }

        public Finder limit(int newLimit) {
            limit = newLimit;
            return this;
        }

        public Finder offset(int newOffset) {
            offset = newOffset;
            return this;
        }

        public Finder orderByAscending(String label) {
            orderBy(label, "ASC");
            return this;
        }

        public Finder orderByDescending(String label) {
            orderBy(label, "DESC");
            return this;
        }

        public Finder where() {
            return this;
        }

        public E findUnique() {
            limit(1);
            try (PreparedStatement statement = getSelectStatement()) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                if (result.next()) {
                    return createInstance(result);
                } else {
                    return null;
                }
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        public List<E> findList() {
            List<E> list = new ArrayList<>();
            try (PreparedStatement statement = getSelectStatement()) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                while (result.next()) {
                    list.add(createInstance(result));
                }
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
            return list;
        }

        public int delete() {
            try (PreparedStatement statement = getDeleteStatement()) {
                database.debugLog(statement);
                return statement.executeUpdate();
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        public int findRowCount() {
            List<E> list = new ArrayList<>();
            try (PreparedStatement statement = getRowCountStatement()) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                result.next();
                return result.getInt("row_count");
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        PreparedStatement getSelectStatement() throws SQLException {
            if (!order.isEmpty()) {
                sb.append(" ORDER BY ").append(order.get(0));
                for (int i = 1; i < order.size(); ++i) {
                    sb.append(", ").append(order.get(i));
                }
            }
            if (limit > 0) {
                sb.append(" LIMIT " + limit);
                if (offset > -1) sb.append(" OFFSET " + offset);
            }
            String sql = "SELECT * FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = database.getConnection().prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        PreparedStatement getDeleteStatement() throws SQLException {
            String sql = "DELETE FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = database.getConnection().prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        PreparedStatement getRowCountStatement() throws SQLException {
            String sql = "SELECT count(*) row_count FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = database.getConnection().prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }
    }
}
