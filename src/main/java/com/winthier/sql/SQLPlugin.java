package com.winthier.sql;

import java.util.ArrayList;
import org.bukkit.Bukkit;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.server.PluginDisableEvent;
import org.bukkit.plugin.java.JavaPlugin;

public final class SQLPlugin extends JavaPlugin implements Listener {
    protected final ArrayList<SQLDatabase> databases = new ArrayList<>();
    protected final SQLCommand sqlCommand = new SQLCommand(this);

    @Override
    public void onEnable() {
        Bukkit.getPluginManager().registerEvents(this, this);
        sqlCommand.enable();
    }

    @Override
    public void onDisable() {
        databases.clear();
    }

    protected void register(SQLDatabase database) {
        databases.add(database);
    }

    protected void unregister(SQLDatabase database) {
        databases.remove(database);
    }

    @EventHandler
    private void onPluginDisable(PluginDisableEvent event) {
        databases.removeIf(d -> d.getPlugin() == event.getPlugin());
    }

    public SQLDatabase findDatabase(String name) {
        for (SQLDatabase database : databases) {
            if (database.getPlugin().getName().equals(name)) return database;
        }
        return null;
    }
}
