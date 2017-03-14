package com.winthier.sql;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

@RequiredArgsConstructor
public final class SQLDatabase {
    @Getter private final JavaPlugin plugin;
    private final Map<Class<?>, SQLTable<?>> tables = new HashMap<>();
    private static final String SQL_CONFIG_FILE = "sql.yml";
    private String tablePrefix;

    @Data
    final class Config {
        private String host = "", port = "", database = "", prefix = "", user = "", password = "";

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
            if (cPrefix != null && !cPrefix.isEmpty()) this.prefix = cPrefix.replace("{NAME}", lowerName);
            if (cUser != null && !cUser.isEmpty()) this.user = cUser;
            if (cPassword != null && !cPassword.isEmpty()) this.password = cPassword;
        }

        String getUrl() {
            return "jdbc:mysql://" + host + ":" + port + "/" + database;
        }

        @Override
        public String toString() {
            return String.format("SQLDatabase.Config(host=%s port=%s database=%s prefix=%s user=%s password=%s)", host, port, database, prefix, user, password);
        }
    }
    private Config config;
    private Connection connection;

    Config getConfig() {
        if (config == null) {
            config = new Config();
            config.setHost("127.0.0.1");
            config.setPort("3306");
            config.setDatabase(plugin.getName());
            config.setUser("user");
            config.setPassword("password");
            config.load(SQLPlugin.getInstance().getConfig().getConfigurationSection("database"));
            config.load(getPluginDatabaseConfig());
        }
        return config;
    }

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

    public <E> SQLTable<E>.Finder find(Class<E> clazz) {
        return getTable(clazz).find();
    }

    public <E> E find(Class<E> clazz, int id) {
        return getTable(clazz).find(id);
    }

    public int save(Object inst) {
        @SuppressWarnings("unchecked")
        SQLTable<Object> table = (SQLTable<Object>)tables.get(inst.getClass());
        return table.save(inst);
    }

    public ConfigurationSection getPluginDatabaseConfig() {
        File file = new File(plugin.getDataFolder(), SQL_CONFIG_FILE);
        YamlConfiguration pluginConfig = YamlConfiguration.loadConfiguration(file);
        InputStream inp = plugin.getResource(SQL_CONFIG_FILE);
        if (inp != null) {
            YamlConfiguration defaultConfig = YamlConfiguration.loadConfiguration(new InputStreamReader(inp));
            pluginConfig.setDefaults(defaultConfig);
        }
        return pluginConfig;
    }

    public Connection getConnection() throws SQLException {
        try {
            if (connection == null || !connection.isValid(1)) {
                Class.forName("com.mysql.jdbc.Driver");
                Config c = getConfig();
                connection = DriverManager.getConnection(c.getUrl(), c.getUser(), c.getPassword());
            }
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException(cnfe);
        }
        return connection;
    }

    public int executeUpdate(String sql) throws SQLException {
        try (Statement statement = getConnection().createStatement()) {
            return statement.executeUpdate(sql);
        } catch (SQLException sqle) {
            throw new SQLException(sqle);
        }
    }

    public boolean createAllTables() {
        try {
            for (SQLTable table: tables.values()) {
                String sql = table.getCreateTableStatement();
                executeUpdate(sql);
            }
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            return false;
        }
        return true;
    }
}
