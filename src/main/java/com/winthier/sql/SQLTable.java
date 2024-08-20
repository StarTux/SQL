package com.winthier.sql;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.bukkit.Bukkit;

@Getter
public final class SQLTable<E extends SQLRow> {
    private final Class<E> clazz;
    private final SQLDatabase database;
    private String tableName;
    private SQLColumn idColumn;
    private final Map<String, Key> keys = new LinkedHashMap<>();
    private final List<SQLColumn> columns = new ArrayList<>();
    private final Constructor<E> ctor;
    private final Map<String, SQLColumn> columnNameMap = new HashMap<>();
    private boolean notNull; // default value

    @Value @AllArgsConstructor
    protected static class Key {
        private final boolean unique;
        private final String name;
        private final List<SQLColumn> columns;

        static Key of(SQLColumn column) {
            return new Key(true, column.getColumnName(), Arrays.asList(column));
        }
    }

    protected SQLTable(final Class<E> clazz, final SQLDatabase database) {
        this.clazz = clazz;
        this.database = database;
        try {
            this.ctor = clazz.getConstructor();
        } catch (NoSuchMethodException nsme) {
            throw new PersistenceException(nsme);
        }
        String tablePrefix = database == null ? "" : database.getConfig().getPrefix();
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        if (tableAnnotation != null) {
            tableName = tablePrefix + tableAnnotation.name();
        }
        SQLRow.Name nameAnnotation = clazz.getAnnotation(SQLRow.Name.class);
        if (nameAnnotation != null) {
            tableName = tablePrefix + nameAnnotation.value();
        }
        if (tableName == null || tableName.isEmpty()) {
            tableName = tablePrefix + SQLUtil.camelToLowerCase(clazz.getSimpleName());
        }
        if (clazz.isAnnotationPresent(SQLRow.NotNull.class)) {
            notNull = true;
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
            columnNameMap.put(column.getColumnName(), column);
            columnNameMap.put(column.getFieldName(), column);
            if (column.isId()) {
                idColumn = column;
            } else if (column.isUnique()) {
                Key key = Key.of(column);
                keys.put(key.name, key);
            }
        }
        // Keys
        if (tableAnnotation != null) {
            // Unique constraints
            UniqueConstraint[] constraints = tableAnnotation.uniqueConstraints();
            if (constraints != null) {
                int counter = 0;
                for (UniqueConstraint constraint : constraints) {
                    counter += 1;
                    String name = constraint.name();
                    if (name == null || name.isEmpty()) {
                        name = "uq_" + getTableName() + "_" + counter;
                    }
                    List<SQLColumn> constraintColumns = new ArrayList<>();
                    for (String columnName : constraint.columnNames()) {
                        SQLColumn column = getColumn(columnName);
                        constraintColumns.add(column);
                    }
                    keys.put(name, new Key(true, name, constraintColumns));
                }
            }
            // Index annotation
            Index[] indexes = tableAnnotation.indexes();
            if (indexes != null) {
                int counter = 0;
                for (Index index : indexes) {
                    counter += 1;
                    String name = index.name();
                    if (name == null || name.isEmpty()) {
                        name = "key_" + getTableName() + "_" + counter;
                    }
                    if (index.name() != null && !index.name().isEmpty()) name = index.name();
                    List<SQLColumn> indexColumns = new ArrayList<>();
                    for (String columnName : index.columnList().split(", ?")) {
                        SQLColumn column = getColumn(columnName);
                        indexColumns.add(column);
                    }
                    keys.put(name, new Key(index.unique(), name, indexColumns));
                }
            }
        }
        for (Annotation annotation : clazz.getDeclaredAnnotations()) {
            if (annotation instanceof SQLRow.UniqueKey uniqueKey) {
                handleUniqueKey(uniqueKey);
            } else if (annotation instanceof SQLRow.Key key) {
                handleKey(key);
            } else if (annotation instanceof SQLRow.Keys keysAnn) {
                for (SQLRow.Key key : keysAnn.value()) {
                    handleKey(key);
                }
            } else if (annotation instanceof SQLRow.UniqueKeys uniqueKeys) {
                for (SQLRow.UniqueKey uniqueKey : uniqueKeys.value()) {
                    handleUniqueKey(uniqueKey);
                }
            }
        }
        for (SQLColumn col : columns) {
            if (col.getUniqueKeyName() != null) {
                String name = !col.getUniqueKeyName().isEmpty()
                    ? col.getUniqueKeyName()
                    : col.getColumnName();
                keys.put(name, new Key(true, name, List.of(col)));
            }
            if (col.getKeyName() != null) {
                String name = !col.getKeyName().isEmpty()
                    ? col.getKeyName()
                    : col.getColumnName();
                keys.put(name, new Key(false, name, List.of(col)));
            }
        }
    }

    private void handleUniqueKey(SQLRow.UniqueKey uniqueKey) {
        List<SQLColumn> columnList = new ArrayList<>();
        for (String columnName : uniqueKey.value()) {
            columnList.add(getColumn(columnName));
        }
        if (columnList.isEmpty()) {
            throw new IllegalStateException("Column list empty: "
                                            + clazz.getName() + "/" + uniqueKey.name());
        }
        String name = !uniqueKey.name().isEmpty()
            ? uniqueKey.name()
            : String.join("_", uniqueKey.value()).toLowerCase();
        keys.put(name, new Key(true, name, columnList));
    }

    private void handleKey(SQLRow.Key key) {
        List<SQLColumn> columnList = new ArrayList<>();
        for (String columnName : key.value()) {
            columnList.add(getColumn(columnName));
        }
        if (columnList.isEmpty()) {
            throw new IllegalStateException("Column list empty: "
                                            + clazz.getName() + "/" + key.name());
        }
        String name = !key.name().isEmpty()
            ? key.name()
            : String.join("_", key.value()).toLowerCase();
        keys.put(name, new Key(false, name, columnList));
    }

    public SQLColumn getColumn(String label) {
        SQLColumn result = columnNameMap.get(label);
        if (result == null) throw new IllegalStateException("Column not found: " + clazz.getName() + "." + label);
        return result;
    }

    protected String getCreateTableStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS `").append(getTableName()).append("` (\n  ");
        sb.append(columns.get(0).getCreateTableFragment());
        for (int i = 1; i < columns.size(); ++i) {
            sb.append(",\n  ");
            sb.append(columns.get(i).getCreateTableFragment());
        }
        if (idColumn != null) sb.append(",\n  PRIMARY KEY (`").append(idColumn.getColumnName()).append("`)");
        for (Key key : keys.values()) {
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

    protected int getRowCount(Connection connection) {
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

    protected E createInstance(Connection connection, ResultSet result, List<SQLColumn> columnList) {
        E row;
        try {
            row = ctor.newInstance();
        } catch (InstantiationException ie) {
            throw new PersistenceException(ie);
        } catch (IllegalAccessException iae) {
            throw new PersistenceException(iae);
        } catch (InvocationTargetException ite) {
            throw new PersistenceException(ite);
        }
        for (SQLColumn column : columnList) {
            column.load(connection, row, result);
        }
        if (row instanceof SQLInterface sqlInterface) {
            sqlInterface.onLoad(result);
        }
        return row;
    }

    protected int save(Connection connection, Collection<E> instances, boolean doIgnore, boolean doUpdate, Set<String> columnNames) {
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
            for (Key key : keys.values()) {
                if (key.unique) {
                    for (SQLColumn uqColumn : key.columns) columnSet.add(uqColumn);
                }
            }
            for (SQLColumn column : columns) {
                if (!column.hasDefaultValue()) {
                    columnSet.add(column);
                }
            }
            for (String columnName : columnNames) {
                SQLColumn column = getColumn(columnName);
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
        for (SQLRow inst : instances) {
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
            int ret;
            try {
                ret = statement.executeUpdate();
            } catch (SQLException sqle) {
                database.getPlugin().getLogger().warning("Error saving " + tableName + ": " + statement);
                throw new PersistenceException(sqle);
            }
            if (idColumn != null) {
                ResultSet keySet = statement.getGeneratedKeys();
                for (SQLRow inst : instances) {
                    if (idColumn.getValue(inst) == null) {
                        if (keySet.next()) {
                            Object newId = keySet.getObject(1);
                            idColumn.setValue(inst, newId);
                        } else {
                            if (!doIgnore) {
                                throw new PersistenceException("Missing generated ID for instance: " + inst);
                            }
                        }
                    }
                }
            }
            return ret;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    protected int update(Connection connection, E instance, Set<String> columnNames) {
        if (idColumn == null) throw new IllegalStateException("No id column: " + tableName);
        List<SQLColumn> columnList = new ArrayList<>();
        if (columnNames == null || columnNames.isEmpty()) {
            columnList.addAll(columns);
            columnList.remove(idColumn);
        } else {
            for (String columnName : columnNames) {
                SQLColumn column = getColumn(columnName);
                columnList.add(column);
            }
        }
        // Build the statement
        List<Object> values = new ArrayList<>(1 + columnList.size());
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE `").append(getTableName()).append("` SET");
        Iterator<SQLColumn> iter = columnList.iterator();
        sb.append(" ");
        SQLColumn column = iter.next();
        sb.append(column.createSetFragment(column.getValue(instance), values));
        while (iter.hasNext()) {
            column = iter.next();
            sb.append(", ");
            sb.append(column.createSetFragment(column.getValue(instance), values));
        }
        sb.append(" WHERE `").append(idColumn.getColumnName()).append("` = ?");
        values.add(idColumn.getValue(instance));
        // Build the statement
        try (PreparedStatement statement = connection.prepareStatement(sb.toString())) {
            SQLUtil.formatStatement(statement, values);
            database.debugLog(statement);
            int ret = statement.executeUpdate();
            return ret;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    protected int delete(Connection connection, Collection<E> collection) {
        if (collection.isEmpty()) return -1;
        if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
        Iterator<E> iter = collection.iterator();
        StringBuilder sb = new StringBuilder();
        sb.append((Integer) idColumn.getValue(iter.next()));
        while (iter.hasNext()) {
            sb.append(", ").append((Integer) idColumn.getValue(iter.next()));
        }
        try (Statement statement = connection.createStatement()) {
            String sql = "DELETE FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " IN (" + sb.toString() + ")";
            database.debugLog(sql);
            return statement.executeUpdate(sql);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    protected E find(Connection connection, int id) {
        if (idColumn == null) throw new PersistenceException("No id column defined: " + clazz.getName());
        try (Statement statement = connection.createStatement()) {
            String sql = "SELECT * FROM " + getTableName() + " WHERE " + idColumn.getColumnName() + " = " + id;
            database.debugLog(sql);
            ResultSet result = statement.executeQuery(sql);
            E row;
            if (result.next()) {
                row = createInstance(connection, result, columns);
            } else {
                row = null;
            }
            result.close();
            return row;
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    protected Finder find() {
        return new Finder();
    }

    protected enum Comparison {
        EQ("="),
        NEQ("!="),
        LT("<"),
        GT(">"),
        LTE("<="),
        GTE(">="),
        LIKE("LIKE");

        public final String symbol;

        Comparison(final String symbol) {
            this.symbol = symbol;
        }
    }

    public final class Finder {
        private final StringBuilder sb = new StringBuilder();
        private final List<Object> values = new ArrayList<>();
        private String conj = " WHERE ";
        private int offset = -1;
        private int limit = -1;
        private final List<String> order = new ArrayList<>();
        private static final String DEFAULT_CONJ = " AND ";
        private List<SQLColumn> columnList = null;

        Finder() { }

        private Finder compare(String label, Comparison comp, Object value) {
            if (value == null) throw new IllegalArgumentException("Value cannot be null!");
            SQLColumn column = getColumn(label);
            String columnName = column.getColumnName();
            sb.append(conj).append("`").append(columnName).append("`").append(" " + comp.symbol + " ?");
            if (column.getType() == SQLType.REFERENCE) {
                Class<?> type = column.getFieldType();
                SQLTable refTable = database.findTable(type);
                if (refTable.idColumn == null) {
                    throw new IllegalStateException("Referenced table lacks id column: " + refTable.getTableName());
                }
                if (!(value instanceof SQLRow instance)) {
                    throw new IllegalArgumentException("Required type " + type.getName() + " (SQLRow)"
                                                       + ", got " + value.getClass().getName());
                }
                values.add(refTable.idColumn.getValue(instance));
            } else {
                values.add(value);
            }
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder select(String... columnNames) {
            this.columnList = new ArrayList<>();
            for (String name : columnNames) {
                columnList.add(getColumn(name));
            }
            if (columnList.isEmpty()) {
                throw new IllegalStateException("Column list empty");
            }
            return this;
        }

        public Finder select(Collection<String> columnNames) {
            this.columnList = new ArrayList<>();
            for (String name : columnNames) {
                columnList.add(getColumn(name));
            }
            if (columnList.isEmpty()) {
                throw new IllegalStateException("Column list empty");
            }
            return this;
        }

        public Finder openParen() {
            sb.append(conj);
            conj = "";
            sb.append("(");
            return this;
        }

        public Finder closeParen() {
            sb.append(")");
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

        public Finder between(String label, Object v1, Object v2) {
            if (v1 == null) throw new IllegalArgumentException("v1 cannot be null!");
            if (v2 == null) throw new IllegalArgumentException("v2 cannot be null!");
            SQLColumn column = getColumn(label);
            String columnName = column.getColumnName();
            sb.append(conj).append("`").append(columnName).append("`").append(" BETWEEN ? AND ?");
            conj = DEFAULT_CONJ;
            values.add(v1);
            values.add(v2);
            return this;
        }

        public Finder in(String label, Collection<?> col) {
            SQLColumn column = getColumn(label);
            String columnName = column.getColumnName();
            Iterator<?> iter = col.iterator();
            if (!iter.hasNext()) {
                sb.append(conj).append("`").append(columnName).append("`").append(" != `").append(columnName).append("`");
                conj = DEFAULT_CONJ;
                return this;
            }
            sb.append(conj).append("`").append(columnName).append("`").append(" IN (?");
            if (column.getType() == SQLType.REFERENCE) {
                Class<?> type = column.getFieldType();
                Object value = iter.next();
                if (!(value instanceof SQLRow instance)) {
                    throw new IllegalArgumentException("Required type " + type.getName() + " (SQLRow)"
                                                       + ", got " + value.getClass().getName());
                }
                values.add(database.findTable(type).idColumn.getValue(instance));
            } else {
                values.add(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(", ?");
                if (column.getType() == SQLType.REFERENCE) {
                    Class<?> type = column.getFieldType();
                    Object value = iter.next();
                    if (!(value instanceof SQLRow instance)) {
                        throw new IllegalArgumentException("Required type " + type.getName() + " (SQLRow)"
                                                           + ", got " + value.getClass().getName());
                    }
                    values.add(database.findTable(type).idColumn.getValue(instance));
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
            sb.append(conj).append("`").append(column.getColumnName()).append("` IS NULL");
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder isNotNull(String label) {
            SQLColumn column = getColumn(label);
            sb.append(conj).append("`").append(column.getColumnName()).append("` IS NOT NULL");
            conj = DEFAULT_CONJ;
            return this;
        }

        public Finder or() {
            conj = " OR ";
            return this;
        }

        public Finder and() {
            conj = " AND ";
            return this;
        }

        private Finder orderBy(String label, String direction) {
            SQLColumn column = getColumn(label);
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
                    return createInstance(connection, result, columnList != null ? columnList : columns);
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
                    list.add(createInstance(connection, result, columnList != null ? columnList : columns));
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

        public <E> List<E> findValues(String columnName, Class<E> ofType) {
            SQLColumn column = getColumn(columnName);
            if (!column.getType().canYield(ofType)) {
                throw new IllegalStateException(ofType.getName() + "/" + column.getType());
            }
            columnList = List.of(column);
            List<E> list = new ArrayList<>();
            Connection connection = database.getConnection();
            try (PreparedStatement statement = getSelectStatement(connection)) {
                database.debugLog(statement);
                ResultSet result = statement.executeQuery();
                while (result.next()) {
                    Object obj = column.getObject(connection, result);
                    if (ofType.isInstance(obj)) {
                        list.add(ofType.cast(obj));
                    }
                }
            } catch (SQLException sqle) {
                throw new IllegalStateException(sqle);
            }
            return list;
        }

        public <E> void findValuesAsync(String columnName, Class<E> ofType, Consumer<List<E>> callback) {
            database.scheduleAsyncTask(() -> {
                    List<E> result = findValues(columnName, ofType);
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

        protected PreparedStatement getSelectStatement(Connection connection) throws SQLException {
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
            final String columnNameList;
            if (columnList == null) {
                columnNameList = "*";
            } else {
                List<String> columnNames = new ArrayList<>(columnList.size());
                for (SQLColumn col : columnList) {
                    columnNames.add("`" + col.getColumnName() + "`");
                }
                columnNameList = String.join(", ", columnNames);
            }
            String sql = "SELECT " + columnNameList + " FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        protected PreparedStatement getDeleteStatement(Connection connection) throws SQLException {
            if (limit > 0) {
                sb.append(" LIMIT " + limit);
                if (offset > -1) sb.append(" OFFSET " + offset);
            }
            String sql = "DELETE FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }

        protected PreparedStatement getRowCountStatement(Connection connection) throws SQLException {
            String sql = "SELECT count(*) row_count FROM `" + getTableName() + "`" + sb.toString();
            PreparedStatement statement = connection.prepareStatement(sql);
            SQLUtil.formatStatement(statement, values);
            return statement;
        }
    }

    public void createColumnIfMissing(String columnName) {
        final SQLColumn column = getColumn(columnName);
        final String query = "SELECT `" + column.getColumnName() + "` FROM `" + getTableName() + "` LIMIT 1";
        database.debugLog(query);
        try (Statement statement = database.getConnection().createStatement();
             ResultSet result = statement.executeQuery(query)) {
            database.getPlugin().getLogger().info("[" + tableName + "] Column `" + column.getColumnName() + "` exists. No action necessary");
            return;
        } catch (SQLException sqle) { }
        final int columnIndex = columns.indexOf(column);
        final String update = "ALTER TABLE `" + getTableName() + "` ADD COLUMN " + column.getCreateTableFragment()
            + (columnIndex == 0
               ? " FIRST"
               : " AFTER `" + columns.get(columnIndex - 1).getColumnName() + "`");
        database.getPlugin().getLogger().info("[" + tableName + "] Creating missing column " + column.getColumnName() + ": " + update);
        try (Statement statement = database.getConnection().createStatement()) {
            final int result = statement.executeUpdate(update);
            database.getPlugin().getLogger().info("[" + tableName + "] Creating missing column " + column.getColumnName() + " => " + result);
        } catch (SQLException sqle) {
            throw new IllegalStateException(update, sqle);
        }
    }
}
