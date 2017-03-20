package com.winthier.sql;

import java.util.Date;
import java.util.Random;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Version;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "foo")
@Getter
@Setter
@NoArgsConstructor
public class SQLFoo {
    @Id private Integer id;
    @Column(nullable = false)
    private Date time;
    @OneToOne
    private SQLLog bar;
    @Version private Integer version;

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
