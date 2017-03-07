package com.winthier.sql;

import java.util.Date;
import java.util.Random;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bukkit.Material;

@Entity
@Table(name = "log")
@Getter
@Setter
@NoArgsConstructor
public class SQLLog {
    @Id private Integer id;
    @Column(nullable = false)
    private Date time;
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
