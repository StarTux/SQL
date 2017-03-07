package com.winthier.sql;

import lombok.Getter;
import org.bukkit.plugin.java.JavaPlugin;

public final class SQLPlugin extends JavaPlugin {
    @Getter private static SQLPlugin instance;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        instance = this;
    }

    @Override
    public void onDisable() {
        instance = null;
    }
}
