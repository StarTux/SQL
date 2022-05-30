package com.winthier.sql;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bukkit.Material;

@Table(name = "log",
       uniqueConstraints = {@UniqueConstraint(columnNames = {"playerUuid", "playerName"})})
@Getter @Setter @NoArgsConstructor
public final class SQLLog implements SQLRow {
    @Id private Integer id;
    @Column(nullable = false)
    private Date time;
    private UUID playerUuid;
    private String playerName;
    @Column(unique = true)
    private Material material;

    @Override
    public String toString() {
        return String.format("SQLLog(id=%d time=%s material=%s)", id, time, material);
    }

    public static SQLLog mktest() {
        Random random = new Random(System.currentTimeMillis());
        SQLLog log = new SQLLog();
        log.setTime(new Date());
        log.setMaterial(Material.values()[random.nextInt(Material.values().length)]);
        return log;
    }
}
