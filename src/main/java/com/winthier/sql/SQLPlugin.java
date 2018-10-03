package com.winthier.sql;

import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.plugin.java.annotation.plugin.ApiVersion;
import org.bukkit.plugin.java.annotation.plugin.Description;
import org.bukkit.plugin.java.annotation.plugin.Plugin;
import org.bukkit.plugin.java.annotation.plugin.Website;
import org.bukkit.plugin.java.annotation.plugin.author.Author;

@Plugin(name = "SQL", version = "0.1")
@Description("SQL library for Spigot. Replacement for Ebean.")
@ApiVersion(ApiVersion.Target.v1_13)
@Author("StarTux")
@Website("https://cavetale.com")
public final class SQLPlugin extends JavaPlugin {
    @Override
    public void onEnable() {
        reloadConfig();
        saveDefaultConfig();
    }
}
