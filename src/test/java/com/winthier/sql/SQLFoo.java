package com.winthier.sql;

import java.util.Date;
import java.util.Random;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Table(name = "foo", indexes = @Index(columnList = "time,bar"))
@Getter @Setter @NoArgsConstructor
public final class SQLFoo implements SQLRow {
    @Id private Integer id;
    @Column(nullable = false)
    private Date time;
    @OneToOne
    private SQLLog bar;

    @Override
    public String toString() {
        return String.format("SQLFoo(id=%d time=%s bar=%s)", id, time, bar);
    }

    public static SQLFoo mktest() {
        Random random = new Random(System.currentTimeMillis());
        SQLFoo foo = new SQLFoo();
        foo.setTime(new Date());
        return foo;
    }
}
