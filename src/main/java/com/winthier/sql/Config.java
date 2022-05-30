package com.winthier.sql;

import lombok.Data;
import org.bukkit.configuration.ConfigurationSection;

@Data
final class Config {
    protected String host = "";
    protected String port = "";
    protected String database = "";
    protected String prefix = "";
    protected String user = "";
    protected String password = "";
    protected boolean debug;
    protected int backlogThreshold = 1000;

    protected void load(final String name, ConfigurationSection config) {
        final String lowerName = SQLUtil.camelToLowerCase(name);
        String cHost = config.getString("host");
        String cPort = config.getString("port");
        String cDatabase = config.getString("database");
        String cPrefix = config.getString("prefix");
        String cUser = config.getString("user");
        String cPassword = config.getString("password");
        if (cHost != null && !cHost.isEmpty()) host = cHost;
        if (cPort != null && !cPort.isEmpty()) port = cPort;
        if (cDatabase != null && !cDatabase.isEmpty()) {
            database = cDatabase.replace("{NAME}", name);
        }
        if (cPrefix != null) prefix = cPrefix.replace("{NAME}", lowerName);
        if (cUser != null && !cUser.isEmpty()) user = cUser;
        if (cPassword != null && !cPassword.isEmpty()) password = cPassword;
        if (config.isSet("debug")) debug = config.getBoolean("debug");
        backlogThreshold = config.getInt("backlogThreshold", backlogThreshold);
    }

    public String getUrl() {
        return "jdbc:mysql://" + host + ":" + port + "/" + database;
    }

    @Override
    public String toString() {
        return String.format("Config(host=%s port=%s database=%s prefix=%s user=%s password=%s)",
                             host, port, database, prefix, user, password);
    }
}
