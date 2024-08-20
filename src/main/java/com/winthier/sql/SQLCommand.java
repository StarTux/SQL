package com.winthier.sql;

import com.cavetale.core.command.AbstractCommand;
import com.cavetale.core.command.CommandWarn;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
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
            .description("Move database")
            .senderCaller(this::move);
        rootNode.addChild("copy").arguments("<source> <dest>")
            .description("Copy database")
            .senderCaller(this::copy);
        rootNode.addChild("query").arguments("<database> <query...>")
            .description("Run safe SQL query")
            .senderCaller(this::query);
        rootNode.addChild("update").arguments("<database> <update...>")
            .description("Run SQL update")
            .senderCaller(this::update);
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

    /**
     * Copy a database. Usually you would copy a plugin database to beta.
     * NOTE: This requires the source database to belong to a loaded
     * plugin and be connected to the source database.
     *
     * e.g.: /sql copy Perm testing
     *
     * To flush Permissions from main to testing.  Of course this
     * would have to be done from a server that is connected to the
     * global Perm database.
     */
    private boolean copy(CommandSender sender, String[] args) {
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
            String srcTable = sourceDatabase.getConfig().getDatabase() + "." + sourceDatabase.getTable(row).getTableName();
            String dstTable = destDatabase.getConfig().getDatabase() + "." + destDatabase.getTable(row).getTableName();
            List<String> sqls = List.of("drop table " + dstTable,
                                        "create table " + dstTable + " like " + srcTable,
                                        "insert into " + dstTable + " select * from " + srcTable);
            for (String sql : sqls) {
                sender.sendMessage(sql);
                int result;
                try {
                    result = sourceDatabase.executeUpdate(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                    sender.sendMessage(text("Error! See console: " + e.getMessage(), RED));
                    break;
                }
                sender.sendMessage(text(result + ": " + sql, AQUA));
            }
        }
        destDatabase.close();
        return true;
    }

    private boolean query(CommandSender sender, String[] args) {
        if (args.length < 2) return false;
        final String databaseName = args[0];
        final String query = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
        final SQLDatabase database = plugin.findDatabase(databaseName);
        if (database == null) throw new CommandWarn("Database not found: " + databaseName);
        final List<Map<String, Object>> result;
        try {
            result = database.executeSafeQuery(query);
        } catch (Exception e) {
            plugin.getLogger().log(Level.SEVERE, "Query: " + query, e);
            throw new CommandWarn("An error occured, see console: " + e.getMessage());
        }
        for (Map<String, Object> row : result) {
            sender.sendMessage(text(row.toString(), GRAY));
        }
        sender.sendMessage(text("" + result.size() + " result(s): " + query, YELLOW));
        return true;
    }

    private boolean update(CommandSender sender, String[] args) {
        if (args.length < 2) return false;
        final String databaseName = args[0];
        final String update = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
        final SQLDatabase database = plugin.findDatabase(databaseName);
        if (database == null) throw new CommandWarn("Database not found: " + databaseName);
        final int result;
        try {
            result = database.executeUpdate(update);
        } catch (Exception e) {
            plugin.getLogger().log(Level.SEVERE, "Update: " + update, e);
            throw new CommandWarn("An error occured, see console: " + e.getMessage());
        }
        sender.sendMessage(text("Update result " + result + ": " + update, YELLOW));
        return true;
    }
}
