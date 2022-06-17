package com.winthier.sql;

import com.winthier.sql.SQLRow.Key;
import com.winthier.sql.SQLRow.Name;
import com.winthier.sql.SQLRow.NotNull;
import com.winthier.sql.SQLRow.UniqueKey;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import lombok.Data;
import org.bukkit.Material;

@Data @Name("logs") @NotNull
@Key({"time", "playerUuid"})
@Key({"time", "playerName"})
@UniqueKey(value = {"playerUuid", "playerName"}, name = "xyz")
@UniqueKey({"playerUuid", "material"})
public final class SQLLog implements SQLRow {
    @Id
    private Long id;

    @Default("NOW()")
    private Date time;

    @Unique
    private UUID playerUuid;

    @VarChar(16)
    private String playerName;

    @Keyed("mat") @VarChar(40) @Name("mat")
    private Material material;

    private byte[] bytes;

    public static SQLLog mktest() {
        Random random = new Random(System.currentTimeMillis());
        SQLLog log = new SQLLog();
        log.setTime(new Date());
        log.setMaterial(Material.values()[random.nextInt(Material.values().length)]);
        return log;
    }
}
