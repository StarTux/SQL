package com.winthier.sql;

import org.bukkit.plugin.java.JavaPlugin;

public final class SQLPlugin extends JavaPlugin {
    @Override
    public void onEnable() {
        reloadConfig();
        saveDefaultConfig();
    }
}
