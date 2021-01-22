package com.winthier.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.persistence.Index;
import javax.persistence.OneToMany;
import javax.persistence.PersistenceException;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.Value;
import org.bukkit.Bukkit;

@Getter
public final class SQLTable<E> {
    private final Class<E> clazz;
    private final SQLDatabase database;
    private String tableName;
    private SQLColumn idColumn;
    private final List<Key> keys = new ArrayList<>();
    private final List<SQLColumn> columns = new ArrayList<>();

    @Value
    static class Key {
        private boolean unique;
        private String name;
        private List<SQLColumn> columns;

        static Key of(SQLColumn column) {
            return new Key(true, column.getColumnName(), Arrays.asList(column));
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
        // Columns
        for (Field field: clazz.getDeclaredFields()) {
            if (Modifier.isTransient(field.getModifiers())
                || Modifier.isStatic(field.getModifiers())
                || Modifier.isFinal(field.getModifiers())
                || field.getAnnotation(OneToMany.class) != null
                || Collection.class.isAssignableFrom(field.getType())
                || Map.class.isAssignableFrom(field.getType())) continue;
            SQLColumn column = new SQLColumn(this, field);
            columns.add(column);
            if (column.isId()) {
                idColumn = column;
            } else if (column.isUnique()) {
                keys.add(Key.of(column));
            }
        }
        // Keys
        if (tableAnnotation != null) {
            // Unique constraints
            UniqueConstraint[] constraints = tableAnnotation.uniqueConstraints();
            if (constraints != null) {
                int counter = 0;
                for (UniqueConstraint constraint: constraints) {
                    counter += 1;
                    String name = "uq_" + getTableName() + "_" + counter;
                    List<SQLColumn> constraintColumns = new ArrayList<>();
                    for (String columnName: constraint.columnNames()) {
                        SQLColumn column = getColumn(columnName);
                        if (column == null) throw new IllegalArgumentException(clazz.getName() + ": Column for unique constraint not found: " + columnName);
                        constraintColumns.add(column);
                        column.setUnique(true);
                    }
                    keys.add(new Key(true, name, constraintColumns));
                }
            }
            // Index annotation
            Index[] indexes = tableAnnotation.indexes();
            if (indexes != null) {
                int counter = 0;
                for (Index index: indexes) {
                    counter += 1;
                    String name = "key_" + getTableName() + "_" + counter;
                    if (index.name() != null && !index.name().isEmpty()) name = index.name();
                    List<SQLColumn> indexColumns = new ArrayList<>();
                    for (String columnName: index.columnList().split(", ?")) {
                        SQLColumn column = getColumn(columnName);
                        if (column == null) throw new IllegalArgumentException(clazz.getName() + ": Column for index not found: " + columnName);
                        indexColumns.add(column);
                        if (index.unique()) column.setUnique(true);
                    }
                    keys.add(new Key(index.unique(), name, indexColumns));
                }
            }
        }
    }

    SQLColumn getColumn(String label) {
        for (SQLColumn column: columns) {
            if (column.getColumnName().equals(label)) return column;
            if (column.getField().getName().equals(label)) return column;
        }
        return null;
    }

    String getCreateTableStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(getTableName()).append("` (\n  ");
        sb.append(columns.get(0).getCreateTableFragment());
        for (int i = 1; i < columns.size(); ++i) {
            sb.append(",\n  ");
            sb.append(columns.get(i).getCreateTableFragment());
        }
        if (idColumn != null) sb.append(",\n  PRIMARY KEY (`").append(idColumn.getColumnName()).append("`)");
        for (Key key: keys) {
            if (key.isUnique()) {
                sb.append(",\n  UNIQUE KEY `").append(key.getName()).append("` (`");
            } else {
                sb.append(",\n  KEY `").append(key.getName()).append("` (`");
            }
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

    int getRowCount(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            String sql = "SELECT COUNT(*) `count` FROM `" + getTableName() + "`";
            database.debugLog(sql);
            ResultSet result = statement.executeQuery(sql);
            result.next();
            return result.getInt("count");
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    E createInstance(Connection connection, ResultSet result) {
        try {
            E row = clazz.newInstance();
            for (SQLColumn column: columns) {
                column.load(connection, row, result);
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

    int save(Connection connection, Collection<E> instances, boolean doIgnore, boolean doUpdate, Set<String> columnNames) {
        if (instances.isEmpty()) throw new PersistenceException("Instances cannot be empty");
        // Collect all columns used in the statement
        Set<SQLColumn> columnSet = new LinkedHashSet<>(columns.size());
        // An empty updateColumns means that no columns were specified
        // to be saved.  Anything else means we update all the
        // specified columns, and only these ones.
        Set<SQLColumn> updateColumns = new LinkedHashSet<>(columns.size());
        if (columnNames == null || columnNames.isEmpty()) {
            // If no column names are specified, add all columns
            columnSet.addAll(columns);
            // We never need the primary ID if this is just an insert.
            if (!doUpdate && idColumn != null) columnSet.remove(idColumn);
        } else {
            // We never need the primary ID if this is just an insert.
            if (doUpdate && idColumn != null) columnSet.add(idColumn);
            for (Key key: keys) {
                if (key.unique) {
                    for (SQLColumn uqColumn: key.columns) columnSet.add(uqColumn);
                }
            }
            for (SQLColumn column: columns) {
                if (!column.isNullable()) {
                    columnSet.add(column);
                }
            }
            for (String columnName: columnNames) {
                SQLColumn column = getColumn(columnName);
                if (column == null) throw new PersistenceException("Field not found: " + tableName + "." + columnName);
                columnSet.add(column);
                updateColumns.add(column);
            }
        }
        // Build the statement
        StringBuilder sb = new StringBuilder();
        // Insert statement
        if (doIgnore) {
            sb.append("INSERT IGNORE INTO `" + getTableName() + "`");
        } else {
            sb.append("INSERT INTO `" + getTableName() + "`");
        }
        // Write the column names
        if (columnSet.isEmpty()) throw new PersistenceException("Empty save statement: " + tableName);
        sb.append(" (`");
        Iterator<SQLColumn> columnIter = columnSet.iterator();
        sb.append(columnIter.next().getColumnName());
        while (columnIter.hasNext()) {
            sb.append("`, `").append(columnIter.next().getColumnName());
        }
        sb.append("`) VALUES");
        List<Object> values = new ArrayList<>(instances.size() * columnSet.size());
        boolean first = true;
        for (Object inst: instances) {
            if (!first) sb.append(",");
            first = false;
            sb.append(" (");
            columnIter = columnSet.iterator();
            columnIter.next().createSaveFragment(inst, sb, values);
            while (columnIter.hasNext()) {
                sb.append(", ");
                columnIter.next().createSaveFragment(inst, sb, values);
            }
            sb.append(")");
        }
        // Write the ON DUPLICATE UPDATE statement.
        if (doUpdate) {
            sb.append(" ON DUPLICATE KEY UPDATE");
            columnIter = updateColumns.isEmpty() ? columnSet.iterator() : updateColumns.iterator();
            SQLColumn column = columnIter.next();
            sb.append(" `").append(column.getColumnName()).append("`=VALUES(").append(column.getColumnName()).append(")");
            while (columnIter.hasNext()) {
                column = columnIter.next();
                if (updateColumns.isEmpty() && column.isId()) continue;
                sb.append(", `").append(column.getColumnName()).append("`=VALUES(`").append(column.getColumnName()).append("`)");
            }
        }
        // Build the statement
        try (PreparedStatement statement = connection.prepareStatement(sb.toString(), Statement.RETURN_GENERATED_KEYS)) {
            SQLUtil.formatStatement(statement, values);
            database.debugLog(statement);
            int ret = statement.executeUpdate();
            if (idColumn != null) {
                ResultSet keySet = statement.getGeneratedKeys();
                for (Object inst: instances) {
                    if (idColumn.getValue(inst) == null) {
                        if (keySet.next()) {
                            int newId = keySet.getInt(1);
                            idColumn.setValue(inst, newId);
                        } else {
                            throw new PersistenceException("Missing generated ID for instance: " + inst);
                        }
                    }
                }
            }
            return ret;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    int update(Connection connection, E instance, Set<String> columnNames) {
        if (idColumn == null) throw new IllegalStateException("No id column: " + tableName);
        List<SQLColumn> columnList = new ArrayList<>();
        if (columnNames == null || columnNames.isEmpty()) {
            columnList.addAll(columns);
            columnList.remove(idColumn);
        } else {
            for (String columnName : columnNames) {
                SQLColumn column = getColumn(columnName);
                if (column == null) throw new PersistenceException("Field not found: " + tableName + "." + columnName);
                columnList.add(column);
            }
        }
        // Build the statement
        List<Object> values = new ArrayList<>(1 + columnList.size());
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE `").append(getTableName()).append("` SET");
        Iterator<SQLColumn> iter = columnList.iterator();
        SQLColumn column = iter.next();
        sb.append(" `").append(column.getColumnName()).append("` = ?");
        values.add(column.getValue(instance));
        while (iter.hasNext()) {
            column = iter.next();
            sb.append(", `").append(column.getColumnName()).append("` = ?");
            values.add(column.getValue(instance));
        }
        sb.append(" WHERE `").append(idColumn.getColumnName()).append("` = ?");
        values.add(idColumn.getValue(instance));
        // Build the statement
        try (PreparedStatement statement = connection.prepareStatement(sb.toString(), Statement.RETURN_GENERATED_KEYS)) {
            SQLUtil.formatStatement(statement, values);
            database.debugLog(statement);
            int ret = statement.executeUpdate();
            return ret;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    int delete(Connection connection, Collection<Object> collection) {
        if (collection.isEmpty()) return -1;
        if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
        Iterator<?> iter = collection.iterator();
        StringBuilder sb = new StringBuilder();
        sb.append((Integer)idColumn.getValue(iter.next()));
        while (iter.hasNext()) {
            sb.append(", ").append((Integer)idColumn.getValue(iter.next()));
        }
        try (Statement statement = connection.createStatement()) {
            String sql = "DELETE FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " IN (" + sb.toString() + ")";
            database.debugLog(sql);
            return statement.executeUpdate(sql);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    E find(Connection connection, int id) {
        if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
        try (Statement statement = connection.createStatement()) {
            String sql = "SELECT * FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " = " + id;
            database.debugLog(sql);
            ResultSet result = statement.executeQuery(sql);
            E row;
            if (result.next()) {
                row = createInstance(connection, result);
            } else {
                row = null;
            }
            result.close();
            return row;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    Finder find() {
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

        // --- Finder result methods

        private E findUnique(Connection connection) {
            limit(1);
            try (PreparedStatement statement = getSelectStatement(connection)) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                if (result.next()) {
                    return createInstance(connection, result);
                } else {
                    return null;
                }
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        public E findUnique() {
            return findUnique(database.getConnection());
        }

        public void findUniqueAsync(Consumer<E> callback) {
            database.scheduleAsyncTask(() -> {
                    E result = findUnique(database.getAsyncConnection());
                    if (callback != null) Bukkit.getScheduler().runTask(database.getPlugin(), () -> callback.accept(result));
                });
        }

        private List<E> findList(Connection connection) {
            List<E> list = new ArrayList<>();
            try (PreparedStatement statement = getSelectStatement(connection)) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                while (result.next()) {
                    list.add(createInstance(connection, result));
                }
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
            return list;
        }

        public List<E> findList() {
            return findList(database.getConnection());
        }

        public void findListAsync(Consumer<List<E>> callback) {
            database.scheduleAsyncTask(() -> {
                    List<E> result = findList(database.getAsyncConnection());
                    Bukkit.getScheduler().runTask(database.getPlugin(), () -> callback.accept(result));
                });
        }

        private int delete(Connection connection) {
            try (PreparedStatement statement = getDeleteStatement(connection)) {
                database.debugLog(statement);
                return statement.executeUpdate();
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        public int delete() {
            return delete(database.getConnection());
        }

        public void deleteAsync(Consumer<Integer> callback) {
            database.scheduleAsyncTask(() -> {
                    int result = delete(database.getAsyncConnection());
                    if (callback != null) Bukkit.getScheduler().runTask(database.getPlugin(), () -> callback.accept(result));
                });
        }

        private int findRowCount(Connection connection) {
            List<E> list = new ArrayList<>();
            try (PreparedStatement statement = getRowCountStatement(connection)) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                result.next();
                return result.getInt("row_count");
            } catch (SQLException sqle) {
                throw new PersistenceException(sqle);
            }
        }

        public int findRowCount() {
            return findRowCount(database.getConnection());
        }

        public void findRowCountAsync(Consumer<Integer> callback) {
            database.scheduleAsyncTask(() -> {
                    int result = findRowCount(database.getAsyncConnection());
                    Bukkit.getScheduler().runTask(database.getPlugin(), () -> callback.accept(result));
                });
        }

        // --- Finder: create statements

        PreparedStatement getSelectStatement(Connection connection) throws SQLException {
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
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        PreparedStatement getDeleteStatement(Connection connection) throws SQLException {
            String sql = "DELETE FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        PreparedStatement getRowCountStatement(Connection connection) throws SQLException {
            String sql = "SELECT count(*) row_count FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }
    }
}
