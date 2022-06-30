package com.winthier.sql;

import com.cavetale.core.command.AbstractCommand;
import com.cavetale.core.command.CommandWarn;
import java.io.File;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.configuration.file.YamlConfiguration;
import static net.kyori.adventure.text.Component.join;
import static net.kyori.adventure.text.Component.space;
import static net.kyori.adventure.text.Component.text;
import static net.kyori.adventure.text.JoinConfiguration.noSeparators;
import static net.kyori.adventure.text.format.NamedTextColor.*;

public final class SQLCommand extends AbstractCommand<SQLPlugin> {
    protected SQLCommand(final SQLPlugin plugin) {
        super(plugin, "sql");
    }

    @Override
    protected void onEnable() {
        rootNode.addChild("save").denyTabCompletion()
            .description("Save the default config")
            .senderCaller(this::save);
        rootNode.addChild("list").denyTabCompletion()
            .description("List databases")
            .senderCaller(this::list);
        rootNode.addChild("move").arguments("<source> <dest>")
            .description("Move databases")
            .senderCaller(this::move);
    }

    private void save(CommandSender sender) {
        plugin.saveDefaultConfig();
        sender.sendMessage(text("Default config saved to disk", YELLOW));
    }

    private void list(CommandSender sender) {
        if (plugin.databases.isEmpty()) throw new CommandWarn("No databases to show");
        var eq = text(":", DARK_GRAY);
        for (SQLDatabase database : plugin.databases) {
            sender.sendMessage(join(noSeparators(),
                                    text("- ", DARK_GRAY),
                                    text(database.getPlugin().getName(), YELLOW),
                                    space(),
                                    text("db", GRAY), eq, text(database.getConfig().getDatabase()),
                                    space(),
                                    text("prefix", GRAY), eq, text(database.getConfig().getPrefix()),
                                    space(),
                                    text("global", GRAY), eq, text(database.determineGlobalConfigFilename()),
                                    space(),
                                    text("tables", GRAY), eq, text(database.getTables().size())));
        }
    }

    private boolean move(CommandSender sender, String[] args) {
        if (args.length != 2) return false;
        String src = args[0];
        String dst = args[1];
        SQLDatabase sourceDatabase = plugin.findDatabase(src);
        if (sourceDatabase == null) throw new CommandWarn("Database not found: " + src);
        File file = new File("/home/mc/public/config/SQL/" + dst + ".yml");
        if (!file.exists()) throw new CommandWarn("Config not found: " + file);
        Config config = new Config();
        ConfigurationSection section = YamlConfiguration.loadConfiguration(file);
        config.load(sourceDatabase.getPlugin().getName(), section.getConfigurationSection("database"));
        sender.sendMessage("Config: " + config.toString());
        SQLDatabase destDatabase = new SQLDatabase(sourceDatabase.getPlugin(), config);
        for (Class<? extends SQLRow> row : sourceDatabase.getTables().keySet()) {
            destDatabase.registerTable(row);
        }
        for (Class<? extends SQLRow> row : sourceDatabase.getTables().keySet()) {
            String sql = "alter table "
                + sourceDatabase.getConfig().getDatabase() + "." + sourceDatabase.getTable(row).getTableName()
                + " rename "
                + destDatabase.getConfig().getDatabase() + "." + destDatabase.getTable(row).getTableName();
            sender.sendMessage(sql);
            int result;
            try {
                result = sourceDatabase.executeUpdate(sql);
            } catch (Exception e) {
                e.printStackTrace();
                sender.sendMessage(text("Error! See console", RED));
                break;
            }
            sender.sendMessage(text(result + ": " + sql, AQUA));
        }
        destDatabase.close();
        return true;
    }
}
