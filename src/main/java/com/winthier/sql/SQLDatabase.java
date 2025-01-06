package com.winthier.sql;

import com.cavetale.core.connect.ServerGroup;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.persistence.PersistenceException;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import lombok.Getter;
import org.bukkit.Bukkit;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

@Getter
public final class SQLDatabase {
    private final JavaPlugin plugin;
    private final Map<Class<? extends SQLRow>, SQLTable<? extends SQLRow>> tables = new HashMap<>();
    private static final String SQL_CONFIG_FILE = "sql.yml";
    private boolean debug;
    private Config config;
    private Connection primaryConnection;
    private Connection asyncConnection;
    private LinkedBlockingQueue<Runnable> asyncQueue;
    private BukkitTask asyncWorker = null;
    private Thread asyncThread = null;
    private Semaphore asyncSemaphore = new Semaphore(1);
    private boolean doStop = false;

    // --- Constructors

    public SQLDatabase(final JavaPlugin plugin) {
        this.plugin = plugin;
    }

    public SQLDatabase(final JavaPlugin plugin, final Config config) {
        this.plugin = plugin;
        this.config = config;
    }

    public Config getConfig() {
        if (config != null) return config;
        this.config = new Config();
        config.setHost("127.0.0.1");
        config.setPort("3306");
        config.setDatabase(plugin.getName());
        config.setUser("user");
        config.setPassword("password");
        ConfigurationSection globalConfig = findGlobalConfiguration();
        if (globalConfig != null) {
            config.load(plugin.getName(), globalConfig.getConfigurationSection("database"));
        } else {
            plugin.getLogger().severe("Global config NOT found");
        }
        config.load(plugin.getName(), getPluginDatabaseConfig());
        this.debug = config.isDebug();
        debugLog(config);
        SQLPlugin sqlPlugin = getSQLPlugin();
        if (sqlPlugin != null) {
            sqlPlugin.register(this);
        }
        return config;
    }

    private SQLPlugin getSQLPlugin() {
        return Bukkit.getPluginManager().getPlugin("SQL") instanceof SQLPlugin sqlPlugin ? sqlPlugin : null;
    }

    private ConfigurationSection findGlobalConfiguration() {
        SQLPlugin sqlPlugin = getSQLPlugin();
        if (sqlPlugin != null) {
            // Prefer the local file if it exists
            File localSQLPluginConfig = new File(sqlPlugin.getDataFolder(), "config.yml");
            if (localSQLPluginConfig.exists()) return sqlPlugin.getConfig();
        }
        String configFilename = determineGlobalConfigFilename();
        if (configFilename != null) {
            File file = new File("/home/mc/public/config/SQL/" + configFilename);
            if (file.exists()) {
                return YamlConfiguration.loadConfiguration(file);
            }
        }
        return sqlPlugin != null
            ? sqlPlugin.getConfig()
            : null;
    }

    protected String determineGlobalConfigFilename() {
        if (Set.of("Bans", "Chat", "Mail", "Perm", "PlayerCache", "PlayerInfo", "Ticket").contains(plugin.getName())) {
            // These plugins are always global
            return "main.yml";
        }
        if (Set.of("Fam", "MemberList", "Photos", "Title", "Wardrobe", "Warp", "Watchman").contains(plugin.getName())) {
            // Testing is separate; the rest is with main
            return ServerGroup.current() == ServerGroup.TESTING
                ? "testing.yml"
                : "main.yml";
        }
        switch (ServerGroup.current()) {
        case MUSEUM: return "museum.yml";
        case TESTING: return "testing.yml";
        case MAIN: return "main.yml";
        default:
            return null;
        }
    }

    private SQLDatabase(final SQLDatabase other) {
        plugin = other.plugin;
        config = other.getConfig();
        debug = other.debug;
    }

    public SQLDatabase async() {
        SQLDatabase cpy = new SQLDatabase(this);
        for (Class<? extends SQLRow> clz : tables.keySet()) {
            cpy.registerTable(clz);
        }
        return cpy;
    }

    // --- Utility: Configuration

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

    public <E extends SQLRow> SQLTable registerTable(Class<E> clazz) {
        SQLTable<E> table = new SQLTable<>(clazz, this);
        tables.put(clazz, table);
        return table;
    }

    public void registerTables(Collection<Class<? extends SQLRow>> classes) {
        for (Class<? extends SQLRow> clazz : classes) {
            registerTable(clazz);
        }
    }

    @Deprecated
    public void registerTables(Class<?>... classes) {
        for (Class<?> it : classes) {
            if (!SQLRow.class.isAssignableFrom(it)) {
                throw new RuntimeException(plugin.getName() + ": " + it.getName() + " does not implement SQLRow!");
            }
            @SuppressWarnings("unchecked") Class<? extends SQLRow> clazz = (Class<? extends SQLRow>) it;
            registerTable(clazz);
        }
    }

    public SQLTable<? extends SQLRow> findTable(Class<?> clazz) {
        SQLTable<? extends SQLRow> result = tables.get(clazz);
        if (result == null) throw new IllegalStateException(plugin.getName() + ": " + "Table not found: " + clazz.getName());
        return result;
    }

    public <E extends SQLRow> SQLTable<E> getTable(Class<E> clazz) {
        @SuppressWarnings("unchecked") SQLTable<E> result = (SQLTable<E>) tables.get(clazz);
        if (result == null) throw new IllegalStateException(plugin.getName() + ": " + "Table not found: " + clazz.getName());
        return result;
    }

    public <E extends SQLRow> SQLTable<E> getTable(E instance) {
        @SuppressWarnings("unchecked") Class<E> clazz = (Class<E>) instance.getClass();
        @SuppressWarnings("unchecked") SQLTable<E> result = (SQLTable<E>) tables.get(clazz);
        if (result == null) throw new IllegalStateException(plugin.getName() + ": " + "Table not found: " + clazz.getName());
        return result;
    }

    public boolean createAllTables() {
        try {
            for (SQLTable<? extends SQLRow> table : tables.values()) {
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

    public <E extends SQLRow> SQLTable<E>.Finder find(Class<E> clazz) {
        return getTable(clazz).find();
    }

    public <E extends SQLRow> E find(Class<E> clazz, int id) {
        return getTable(clazz).find(getConnection(), id);
    }

    // --- API: Save

    /**
     * Internal save helper.
     */
    private <E extends SQLRow> int save(Connection connection, E instance, boolean doIgnore, boolean doUpdate, Set<String> columnNames) {
        SQLTable<E> table = getTable(instance);
        return table.save(connection, List.of(instance), doIgnore, doUpdate, columnNames);
    }

    private <E extends SQLRow> int save(Connection connection, Collection<E> collection, boolean doIgnore, boolean doUpdate, Set<String> columnNames) {
        if (collection.isEmpty()) return 0;
        E any = collection.iterator().next();
        SQLTable<E> table = getTable(any);
        return table.save(connection, collection, doIgnore, doUpdate, columnNames);
    }

    private <E extends SQLRow> int update(Connection connection, E instance, Set<String> columnNames) {
        SQLTable<E> table = getTable(instance);
        return table.update(connection, instance, columnNames);
    }

    public <E extends SQLRow> int saveIgnore(E instance) {
        return save(getConnection(), instance, true, true, null);
    }

    public <E extends SQLRow> int saveIgnore(Collection<E> instances) {
        return save(getConnection(), instances, true, true, null);
    }

    public <E extends SQLRow> void saveIgnoreAsync(E instance, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, true, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveIgnoreAsync(Collection<E> instances, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, true, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> int save(E instance) {
        return save(getConnection(), instance, false, true, null);
    }

    public <E extends SQLRow> int save(Collection<E> instances) {
        return save(getConnection(), instances, false, true, null);
    }

    public <E extends SQLRow> void saveAsync(E instance, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, false, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveAsync(Collection<E> instances, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, false, true, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> int save(E instance, String... fields) {
        return save(getConnection(), instance, false, true, Set.of(fields));
    }

    public <E extends SQLRow> int save(Collection<E> instances, String... fields) {
        return save(getConnection(), instances, false, true, Set.of(fields));
    }

    public <E extends SQLRow> int save(Collection<E> instances, Set<String> fields) {
        return save(getConnection(), instances, false, true, fields);
    }

    public <E extends SQLRow> void saveAsync(E instance, Consumer<Integer> callback, String... fields) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, false, true, Set.of(fields));
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveAsync(Collection<E> instances, Consumer<Integer> callback, String... fields) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, false, true, Set.of(fields));
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveAsync(E instance, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, false, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveAsync(Collection<E> instances, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, false, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveIgnoreAsync(E instance, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, true, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void saveIgnoreAsync(Collection<E> instances, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, true, true, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> int update(E instance, String... fields) {
        return update(getConnection(), instance, Set.of(fields));
    }

    public <E extends SQLRow> void updateAsync(E instance, Consumer<Integer> callback, String... fields) {
        scheduleAsyncTask(() -> {
                int result = update(getAsyncConnection(), instance, Set.of(fields));
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void updateAsync(E instance, Set<String> fields, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = update(getAsyncConnection(), instance, fields);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> int insert(E instance) {
        return save(getConnection(), instance, false, false, null);
    }

    public <E extends SQLRow> int insert(Collection<E> instances) {
        return save(getConnection(), instances, false, false, null);
    }

    public <E extends SQLRow> int insertIgnore(E instance) {
        return save(getConnection(), instance, true, false, null);
    }

    public <E extends SQLRow> int insertIgnore(Collection<E> instances) {
        return save(getConnection(), instances, true, false, null);
    }

    public <E extends SQLRow> void insertAsync(E instance, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, false, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void insertAsync(Collection<E> instances, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, false, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void insertIgnoreAsync(E instance, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instance, true, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void insertIgnoreAsync(Collection<E> instances, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = save(getAsyncConnection(), instances, true, false, null);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    // --- API: Delete

    private <E extends SQLRow> int delete(Connection connection, E instance) {
        SQLTable<E> table = getTable(instance);
        return table.delete(connection, List.of(instance));
    }

    private <E extends SQLRow> int delete(Connection connection, Collection<E> collection) {
        if (collection.isEmpty()) return 0;
        E any = collection.iterator().next();
        SQLTable<E> table = getTable(any);
        return table.delete(connection, collection);
    }

    public <E extends SQLRow> int delete(E instance) {
        return delete(getConnection(), instance);
    }

    public <E extends SQLRow> int delete(Collection<E> instances) {
        return delete(getConnection(), instances);
    }

    public <E extends SQLRow> void deleteAsync(E instance, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = delete(getAsyncConnection(), instance);
                if (callback != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> callback.accept(result));
                }
            });
    }

    public <E extends SQLRow> void deleteAsync(Collection<E> instances, Consumer<Integer> callback) {
        scheduleAsyncTask(() -> {
                int result = delete(getAsyncConnection(), instances);
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
            throw new RuntimeException("plugin: " + plugin.getName(), sqle);
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
                    throw new RuntimeException("plugin: " + plugin.getName(), sqle);
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
            throw new RuntimeException("plugin: " + plugin.getName(), sqle);
        }
    }

    public List<Map<String, Object>> executeSafeQuery(String sql) {
        debugLog(sql);
        try (Statement statement = getConnection().createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            final List<Map<String, Object>> result = new ArrayList<>();
            final ResultSetMetaData meta = resultSet.getMetaData();
            final int columnCount = meta.getColumnCount();
            final List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i += 1) {
                columnNames.add(meta.getColumnName(i + 1));
            }
            while (resultSet.next()) {
                final Map<String, Object> entry = new LinkedHashMap<>();
                result.add(entry);
                for (String columnName : columnNames) {
                    entry.put(columnName, resultSet.getObject(columnName));
                }
            }
            return result;
        } catch (SQLException sqle) {
            throw new IllegalStateException("plugin: " + plugin.getName(), sqle);
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
                    throw new RuntimeException("plugin: " + plugin.getName(), sqle);
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
                primaryConnection = DriverManager
                    .getConnection(getConfig().getUrl(), getConfig().getUser(), getConfig().getPassword());
            }
        } catch (SQLException sqle) {
            throw new RuntimeException("plugin: " + plugin.getName(), sqle);
        }
        return primaryConnection;
    }

    public Connection getAsyncConnection() {
        try {
            if (asyncConnection == null || !asyncConnection.isValid(1)) {
                asyncConnection = DriverManager
                    .getConnection(getConfig().getUrl(), getConfig().getUser(), getConfig().getPassword());
            }
        } catch (SQLException sqle) {
            throw new RuntimeException("plugin: " + plugin.getName(), sqle);
        }
        return asyncConnection;
    }

    protected void debugLog(Object o) {
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
        if (!asyncSemaphore.tryAcquire()) {
            // Should only happen with extremely poor timing
            plugin.getLogger().warning("Async worker task creation failed!");
            return;
        }
        asyncThread = Thread.currentThread();
        List<Runnable> tasks = new ArrayList<>();
        while (!doStop && (plugin.isEnabled() || !asyncQueue.isEmpty())) {
            try {
                // Empty the queue
                Runnable task = asyncQueue.poll(50, TimeUnit.MILLISECONDS);
                if (task == null) continue;
                tasks.clear();
                tasks.add(task);
                asyncQueue.drainTo(tasks);
                int backlog = tasks.size();
                if (backlog > getConfig().backlogThreshold) {
                    plugin.getLogger()
                        .warning("[SQL] Backlog exceeds threshold: "
                                 + backlog + " > " + getConfig().backlogThreshold);
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
        asyncSemaphore.release();
    }

    public void waitForAsyncTask() {
        if (asyncQueue == null) return;
        int pending = asyncQueue.size();
        if (pending == 0) return;
        plugin.getLogger().info("[SQL] " + pending + " tasks pending");
        asyncSemaphore.acquireUninterruptibly();
        while (!asyncQueue.isEmpty()) {
            List<Runnable> list = new ArrayList<>(asyncQueue.size());
            asyncQueue.drainTo(list);
            for (Runnable run : list) {
                try {
                    run.run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
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
        SQLPlugin sqlPlugin = getSQLPlugin();
        if (sqlPlugin != null) {
            sqlPlugin.unregister(this);
        }
    }

    public int getBacklogSize() {
        if (asyncQueue == null) return 0;
        return asyncQueue.size();
    }

    public <E extends SQLRow> SQLUpdater<E> update(Class<E> clazz) {
        SQLTable<E> table = getTable(clazz);
        return new SQLUpdater<E>(this, table);
    }

    public static <E extends SQLRow> String testTableCreation(Class<E> clazz) {
        return new SQLTable<E>(clazz, null).getCreateTableStatement();
    }
}
