package com.winthier.sql;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.persistence.PersistenceException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import lombok.Data;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

@Getter
public final class SQLDatabase {
    private final JavaPlugin plugin;
    private final Map<Class<?>, SQLTable<?>> tables = new HashMap<>();
    private static final String SQL_CONFIG_FILE = "sql.yml";
    private final boolean debug;
    private final Config config;
    private Connection primaryConnection;
    private Connection asyncConnection;
    private LinkedBlockingQueue<Runnable> asyncQueue;
    private BukkitTask asyncWorker = null;
    private Thread asyncThread = null;

    // --- Constructors

    public SQLDatabase(final JavaPlugin plugin) {
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
        debug = config.isDebug();
        debugLog(config);
    }

    private SQLDatabase(final SQLDatabase other) {
        plugin = other.plugin;
        config = other.config;
        debug = other.debug;
    }

    public SQLDatabase async() {
        SQLDatabase cpy = new SQLDatabase(this);
        for (Class<?> clz: tables.keySet()) cpy.registerTable(clz);
        return cpy;
    }

    // --- Utility: Configuration

    @Data
    final class Config {
        private String host = "";
        private String port = "";
        private String database = "";
        private String prefix = "";
        private String user = "";
        private String password = "";
        private boolean debug;
        int backlogThreshold = 1000;

        void load(ConfigurationSection c) {
            final String name = plugin.getName();
            final String lowerName = SQLUtil.camelToLowerCase(name);
            String cHost = c.getString("host");
            String cPort = c.getString("port");
            String cDatabase = c.getString("database");
            String cPrefix = c.getString("prefix");
            String cUser = c.getString("user");
            String cPassword = c.getString("password");
            if (cHost != null && !cHost.isEmpty()) host = cHost;
            if (cPort != null && !cPort.isEmpty()) port = cPort;
            if (cDatabase != null && !cDatabase.isEmpty()) {
                database = cDatabase.replace("{NAME}", name);
            }
            if (cPrefix != null) prefix = cPrefix.replace("{NAME}", lowerName);
            if (cUser != null && !cUser.isEmpty()) user = cUser;
            if (cPassword != null && !cPassword.isEmpty()) password = cPassword;
            if (c.isSet("debug")) debug = c.getBoolean("debug");
            backlogThreshold = c.getInt("backlogThreshold", backlogThreshold);
        }

        String getUrl() {
            return "jdbc:mysql://" + host + ":" + port + "/" + database;
        }

        @Override
        public String toString() {
            return String
                .format("Config(host=%s port=%s database=%s prefix=%s user=%s password=%s)",
                        host, port, database, prefix, user, password);
        }
    }

    ConfigurationSection getPluginDatabaseConfig() {
        File file = new File(plugin.getDataFolder(), SQL_CONFIG_FILE);
        YamlConfiguration pluginConfig = YamlConfiguration.loadConfiguration(file);
        InputStream inp = plugin.getResource(SQL_CONFIG_FILE);
        if (inp != null) {
            YamlConfiguration defaultConfig = YamlConfiguration
                .loadConfiguration(new InputStreamReader(inp));
            pluginConfig.setDefaults(defaultConfig);
        }
        return pluginConfig;
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
            SQLTable<E> result = (SQLTable<E>) tables.get(clazz);
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

    // --- API: Save

    /**
     * Internal save helper.
     */
    private int save(Connection connection, Object inst,
                     boolean doIgnore, boolean doUpdate,
                     Set<String> columnNames) {
        if (inst instanceof Collection) {
            @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) inst;
            if (collection.isEmpty()) return 0;
            Object any = collection.iterator().next().getClass();
            @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>) tables.get(any);
            if (table == null) {
                throw new PersistenceException("Table not found for object of class "
                                               + any.getClass().getName());
            }
            return table.save(connection, collection, doIgnore, doUpdate, columnNames);
        } else {
            @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>) tables.get(inst.getClass());
            if (table == null) {
                throw new PersistenceException("Table not found for object of class "
                                               + inst.getClass().getName());
            }
            return table.save(connection, Arrays.asList(inst), doIgnore, doUpdate, columnNames);
        }
    }

    private int update(Connection connection, Object inst, Set<String> columnNames) {
        @SuppressWarnings("unchecked")
        SQLTable<Object> table = (SQLTable<Object>) tables.get(inst.getClass());
        if (table == null) {
            throw new PersistenceException("Table not found for object of class " + inst.getClass().getName());
        }
        return table.update(connection, inst, columnNames);
    }

    public int saveIgnore(Object inst) {
        return save(getConnection(), inst, true, true, null);
    }

    public void saveIgnoreAsync(Object inst, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, true, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public int save(Object inst) {
        return save(getConnection(), inst, false, true, null);
    }

    public void saveAsync(Object inst, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, false, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public int save(Object inst, String... fields) {
        return save(getConnection(), inst, false, true,
                    new LinkedHashSet<>(Arrays.asList(fields)));
    }

    public void saveAsync(Object inst, Consumer<Integer> callback, String... fields) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, false, true,
                                  new LinkedHashSet<>(Arrays.asList(fields)));
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public int save(Object inst, Set<String> fields) {
        return save(getConnection(), inst, false, true, fields);
    }

    public void saveAsync(Object inst, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, false, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public void saveIgnoreAsync(Object inst, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, true, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public int update(Object inst, String... fields) {
        return update(getConnection(), inst, new LinkedHashSet<>(Arrays.asList(fields)));
    }

    public void updateAsync(Object inst, Consumer<Integer> callback, String... fields) {
        scheduleAsyncTask(() -> {
                int result = update(getAsyncConnection(), inst, new LinkedHashSet<>(Arrays.asList(fields)));
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public int insert(Object inst) {
        return save(getConnection(), inst, false, false, null);
    }

    public int insertIgnore(Object inst) {
        return save(getConnection(), inst, true, false, null);
    }

    public void insertAsync(Object inst, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, false, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public void insertIgnoreAsync(Object inst, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), inst, true, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    // --- API: Delete

    private int delete(Connection connection, Object inst) {
        if (inst instanceof Collection) {
            @SuppressWarnings("unchecked")
                Collection<Object> collection = (Collection<Object>) inst;
            if (collection.isEmpty()) return 0;
            Object o = collection.iterator().next();
            @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>) tables.get(o.getClass());
            if (table == null) {
                throw new PersistenceException("Table not found found for class "
                                               + o.getClass().getName());
            }
            return table.delete(connection, collection);
        } else {
            @SuppressWarnings("unchecked")
                SQLTable<Object> table = (SQLTable<Object>) tables.get(inst.getClass());
            if (table == null) {
                throw new PersistenceException("Table not found found for class "
                                               + inst.getClass().getName());
            }
            return table.delete(connection, Arrays.asList(inst));
        }
    }

    public int delete(Object inst) {
        return delete(getConnection(), inst);
    }

    public void deleteAsync(Object inst, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = delete(getAsyncConnection(), inst);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    // --- API: Raw statements

    public int executeUpdate(String sql) {
        try (Statement statement = getConnection().createStatement()) {
            debugLog(sql);
            return statement.executeUpdate(sql);
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public void executeUpdateAsync(String sql, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                try (Statement statement = getAsyncConnection().createStatement()) {
                    debugLog(sql);
                    int result = statement.executeUpdate(sql);
                    if (callback != null) {
                        Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                    }
                } catch (SQLException sqle) {
                    throw new PersistenceException(sqle);
                }
            });
    }

    public ResultSet executeQuery(String sql) {
        try (Statement statement = getConnection().createStatement()) {
            debugLog(sql);
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                CachedRowSet cached = RowSetProvider.newFactory().createCachedRowSet();
                cached.populate(resultSet);
                return cached;
            }
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        }
    }

    public void executeQueryAsync(String sql, Consumer<ResultSet> callback) {
        scheduleAsyncTask(() -> {
                try (Statement statement = getAsyncConnection().createStatement()) {
                    debugLog(sql);
                    try (ResultSet resultSet = statement.executeQuery(sql)) {
                        CachedRowSet cached = RowSetProvider.newFactory().createCachedRowSet();
                        cached.populate(resultSet);
                        Bukkit.getScheduler().runTask(plugin, () -> callback.accept(cached));
                    }
                } catch (SQLException sqle) {
                    throw new PersistenceException(sqle);
                }
            });
    }

    // --- Utility: Connection

    public Connection getConnection() {
        if (Bukkit.isPrimaryThread()) {
            return getPrimaryConnection();
        }
        if (Thread.currentThread().equals(asyncThread)) {
            return getAsyncConnection();
        }
        plugin.getLogger().warning("SQLDatabase.getConnection() called from neither primary nor async worker thread!");
        new Exception().printStackTrace();
        return getAsyncConnection();
    }

    public Connection getPrimaryConnection() {
        try {
            if (primaryConnection == null || !primaryConnection.isValid(1)) {
                Class.forName("com.mysql.jdbc.Driver");
                primaryConnection = DriverManager
                    .getConnection(config.getUrl(), config.getUser(), config.getPassword());
            }
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        } catch (ClassNotFoundException cnfe) {
            throw new PersistenceException(cnfe);
        }
        return primaryConnection;
    }

    public Connection getAsyncConnection() {
        try {
            if (asyncConnection == null || !asyncConnection.isValid(1)) {
                Class.forName("com.mysql.jdbc.Driver");
                asyncConnection = DriverManager
                    .getConnection(config.getUrl(), config.getUser(), config.getPassword());
            }
        } catch (SQLException sqle) {
            throw new PersistenceException(sqle);
        } catch (ClassNotFoundException cnfe) {
            throw new PersistenceException(cnfe);
        }
        return asyncConnection;
    }

    void debugLog(Object o) {
        if (!debug) return;
        plugin.getLogger().info("[SQL] " + o);
    }

    // --- Utility: Async

    public void scheduleAsyncTask(Runnable task) {
        if (!plugin.isEnabled()) {
            plugin.getLogger().warning("[SQL] Attempt to schedule async tasks"
                                       + " while plugin is disabled!");
        }
        if (asyncWorker == null) {
            synchronized (this) {
                if (asyncWorker == null) {
                    asyncQueue = new LinkedBlockingQueue<>();
                    asyncWorker = Bukkit.getScheduler().runTaskAsynchronously(plugin, this::asyncWorkerTask);
                }
            }
        }
        try {
            asyncQueue.put(task);
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    private void asyncWorkerTask() {
        asyncThread = Thread.currentThread();
        List<Runnable> tasks = new ArrayList<>();
        while (plugin.isEnabled() || !asyncQueue.isEmpty()) {
            try {
                // Empty the queue
                Runnable task = asyncQueue.poll(50, TimeUnit.MILLISECONDS);
                if (task == null) continue;
                tasks.clear();
                tasks.add(task);
                asyncQueue.drainTo(tasks);
                int backlog = tasks.size();
                if (backlog > config.backlogThreshold) {
                    plugin.getLogger()
                        .warning("[SQL] Backlog exceeds threshold: "
                                 + backlog + " > " + config.backlogThreshold);
                }
                for (Runnable run : tasks) {
                    try {
                        run.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                continue;
            }
        }
    }

    public void waitForAsyncTask() {
        if (asyncQueue == null) return;
        int pending = asyncQueue.size();
        if (pending == 0) return;
        plugin.getLogger().info("[SQL] " + pending + " tasks pending");
        while (true) {
            if (asyncQueue.isEmpty()) return;
            try {
                Thread.sleep(50);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

    public void close() {
        if (primaryConnection != null) {
            try {
                primaryConnection.close();
            } catch (SQLException pe) {
                pe.printStackTrace();
            }
        }
        if (asyncConnection != null) {
            try {
                asyncConnection.close();
            } catch (SQLException pe) {
                pe.printStackTrace();
            }
        }
    }

    public int getBacklogSize() {
        if (asyncQueue == null) return 0;
        return asyncQueue.size();
    }

    public <E> SQLUpdater<E> update(Class<E> clazz) {
        SQLTable table = getTable(clazz);
        if (table == null) throw new IllegalStateException("Table not found: " + clazz);
        return new SQLUpdater(this, table);
    }
}
