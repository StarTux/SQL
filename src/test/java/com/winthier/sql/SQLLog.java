package com.winthier.sql;

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import javax.persistence.Version;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "log",
       uniqueConstraints = {@UniqueConstraint(columnNames = {"playerUuid", "playerName"})})
@Getter
@Setter
@NoArgsConstructor
public class SQLLog {
    @Id private Integer id;
    @Column(nullable = false)
    private Date time;
    private UUID playerUuid;
    private String playerName;
    @Column(unique = true)
    @Version private Date version;

    @Override
    public String toString() {
        return String.format("SQLLog(id=%d time=%s)", id, time);
    }

    public static SQLLog mktest() {
        Random random = new Random(System.currentTimeMillis());
        SQLLog log = new SQLLog();
        log.setTime(new Date());
        return log;
    }
}
