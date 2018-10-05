package com.winthier.sql;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import javax.persistence.PersistenceException;
import lombok.Data;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

@Getter
public final class SQLDatabase {
    private final JavaPlugin plugin;
    private final Map<Class<?>, SQLTable<?>> tables = new HashMap<>();
    private static final String SQL_CONFIG_FILE = "sql.yml";
    private final boolean debug;
    private final boolean optimisticLocking;
    private final Config config;
    private Connection cachedConnection;

    // --- Constructors

    public SQLDatabase(JavaPlugin plugin) {
        this.plugin = plugin;
        config = new Config();
        config.setHost("127.0.0.1");
        config.setPort("3306");
        config.setDatabase(plugin.getName());
        config.setUser("user");
        config.setPassword("password");
        Plugin sqlPlugin = Bukkit.getPluginManager().getPlugin("SQL");
        if (sqlPlugin != null) {
            config.load(sqlPlugin.getConfig().getConfigurationSection("database"));
        }
        config.load(getPluginDatabaseConfig());
        this.debug = config.isDebug();
        this.optimisticLocking = config.optimisticLocking;
        debugLog(config);
    }

    private SQLDatabase(SQLDatabase other) {
        this.plugin = other.plugin;
        this.config = other.config;
        this.debug = other.debug;
        this.optimisticLocking = other.optimisticLocking;
    }

    public SQLDatabase async() {
        SQLDatabase cpy = new SQLDatabase(this);
        for (Class<?> clz: tables.keySet()) cpy.registerTable(clz);
        return cpy;
    }

    @Data
    final class Config {
        private String host = "", port = "", database = "", prefix = "", user = "", password = "";
        private boolean debug;
        private boolean optimisticLocking;

        void load(ConfigurationSection c) {
            final String name = plugin.getName();
            final String lowerName = SQLUtil.camelToLowerCase(name);
            String cHost = c.getString("host");
            String cPort = c.getString("port");
            String cDatabase = c.getString("database");
            String cPrefix = c.getString("prefix");
            String cUser = c.getString("user");
            String cPassword = c.getString("password");
            if (cHost != null && !cHost.isEmpty()) this.host = cHost;
            if (cPort != null && !cPort.isEmpty()) this.port = cPort;
            if (cDatabase != null && !cDatabase.isEmpty()) this.database = cDatabase.replace("{NAME}", name);
            if (cPrefix != null) this.prefix = cPrefix.replace("{NAME}", lowerName);
            if (cUser != null && !cUser.isEmpty()) this.user = cUser;
            if (cPassword != null && !cPassword.isEmpty()) this.password = cPassword;
            if (c.isSet("debug")) this.debug = c.getBoolean("debug");
            if (c.isSet("optimisticLocking")) this.optimisticLocking = c.getBoolean("optimisticLocking");
        }

        String getUrl() {
            return "jdbc:mysql://" + host + ":" + port + "/" + database;
        }

        @Override
        public String toString() {
            return String.format("SQLDatabase.Config(host=%s port=%s database=%s prefix=%s user=%s password=%s)", host, port, database, prefix, user, password);
        }
    }

    // --- API: Tables

    public <E> SQLTable registerTable(Class<E> clazz) {
        SQLTable<E> table = new SQLTable<>(clazz, this);
        tables.put(clazz, table);
        return table;
    }

    public void registerTables(Class<?>... clazzes) {
        for (Class<?> clazz: clazzes) {
            registerTable(clazz);
        }
    }

    public <E> SQLTable<E> getTable(Class<E> clazz) {
        @SuppressWarnings("unchecked")
        SQLTable<E> result = (SQLTable<E>)tables.get(clazz);
        return result;
    }

    public boolean createAllTables() {
        try {
            for (SQLTable table: tables.values()) {
                String sql = table.getCreateTableStatement();
                executeUpdate(sql);
            }
        } catch (PersistenceException pe) {
            pe.printStackTrace();
            return false;
        }
        return true;
    }

    // --- API: Find and update

    public <E> SQLTable<E>.Finder find(Class<E> clazz) {
        return getTable(clazz).find();
    }

    public <E> E find(Class<E> clazz, int id) {
        return getTable(clazz).find(getConnection(), id);
    }

    private int saveIgnore(Connection connection, Object inst) {
        if (inst instanceof Collection) {
            Collection<?> col = (Collection<?>)inst;
            if (col.isEmpty()) return 0;
            @SuppressWarnings("unchecked")
            SQLTable<Object> table = (SQLTable<Object>)tables.get(col.iterator().next().getClass());
            int result = 0;
            for (Object o: col) {
                result += table.saveIgnore(connection, o);
            }
            return result;
        } else {
            @SuppressWarnings("unchecked")
            SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
            return table.saveIgnore(connection, inst);
        }
    }

    public int saveIgnore(Object inst) {
        return saveIgnore(getConnection(), inst);
    }

    public void saveIgnoreAsync(Object inst, Consumer<Integer> callback) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                Connection connection = createNewConnection();
                int result = saveIgnore(connection, inst);
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    sqle.printStackTrace();
                }
                if (callback != null) Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
            });
    }

    private int save(Connection connection, Object inst) {
        if (inst instanceof Collection) {
            Collection<?> col = (Collection<?>)inst;
            if (col.isEmpty()) return 0;
            @SuppressWarnings("unchecked")
            SQLTable<Object> table = (SQLTable<Object>)tables.get(col.iterator().next().getClass());
            int result = 0;
            for (Object o: col) {
                result += table.save(connection, o);
            }
            return result;
        } else {
            @SuppressWarnings("unchecked")
            SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
            return table.save(connection, inst);
        }
    }

    public int save(Object inst) {
        return save(getConnection(), inst);
    }

    public void saveAsync(Object inst, Consumer<Integer> callback) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                Connection connection = createNewConnection();
                int result = save(connection, inst);
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    sqle.printStackTrace();
                }
                if (callback != null) Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
            });
    }

    public int save(Object inst, String... fields) {
        @SuppressWarnings("unchecked")
        SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
        return table.save(getConnection(), inst, fields);
    }

    public void saveAsync(Object inst, Consumer<Integer> callback, String... fields) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
                int result = table.save(getConnection(), inst, fields);
                if (callback != null) Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
            });
    }

    public int delete(Object inst) {
        @SuppressWarnings("unchecked")
        SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
        return table.delete(getConnection(), inst);
    }

    public void deleteAsync(Object inst, Consumer<Integer> callback) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
                Connection connection = createNewConnection();
                int result = table.delete(connection, inst);
                try {
                    connection.close();
                } catch (SQLException sqle) {
                    sqle.printStackTrace();
                }
                if (callback != null) Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
            });
    }

    public int executeUpdate(String sql) {
        try {
            Statement statement = getConnection().createStatement();
            debugLog(sql);
            return statement.executeUpdate(sql);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public void executeUpdateAsync(String sql, Consumer<Integer> callback) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    Statement statement = createNewConnection().createStatement();
                    debugLog(sql);
                    int result = statement.executeUpdate(sql);
                    if (callback != null) Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                } catch (SQLException sqle) {
                    throw new PersistenceException(sqle);
                }
            });
    }

    public ResultSet executeQuery(String sql) {
        try {
            Statement statement = getConnection().createStatement();
            debugLog(sql);
            return statement.executeQuery(sql);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public void executeQueryAsync(String sql, Consumer<ResultSet> callback) {
        Bukkit.getScheduler().runTaskAsynchronously(plugin, () -> {
                try {
                    Statement statement = getConnection().createStatement();
                    debugLog(sql);
                    ResultSet result = statement.executeQuery(sql);
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                } catch (SQLException sqle) {
                    throw new PersistenceException(sqle);
                }
            });
    }

    // --- Utility: Configuration

    ConfigurationSection getPluginDatabaseConfig() {
        File file = new File(plugin.getDataFolder(), SQL_CONFIG_FILE);
        YamlConfiguration pluginConfig = YamlConfiguration.loadConfiguration(file);
        InputStream inp = plugin.getResource(SQL_CONFIG_FILE);
        if (inp != null) {
            YamlConfiguration defaultConfig = YamlConfiguration.loadConfiguration(new InputStreamReader(inp));
            pluginConfig.setDefaults(defaultConfig);
        }
        return pluginConfig;
    }

    // --- Utility: Connection

    public synchronized Connection getConnection() {
        try {
            if (cachedConnection == null || !cachedConnection.isValid(1)) {
                Class.forName("com.mysql.jdbc.Driver");
                cachedConnection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
            }
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        } catch (ClassNotFoundException cnfe) {
            throw new PersistenceException(cnfe);
        }
        return cachedConnection;
    }

    public synchronized Connection createNewConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        } catch (ClassNotFoundException cnfe) {
            throw new PersistenceException(cnfe);
        }
    }

    void debugLog(Object o) {
        if (!debug) return;
        plugin.getLogger().info("[SQL] " + o);
    }
}
