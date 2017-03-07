package com.winthier.sql;

import org.junit.Test;

public class SQLTest {
    @Test
    public void main() {
        SQLTable<SQLLog> logTable = new SQLTable<>(SQLLog.class, null);
        SQLTable<SQLFoo> fooTable = new SQLTable<>(SQLFoo.class, null);
        System.out.println(logTable.getCreateTableStatement());
        System.out.println(fooTable.getCreateTableStatement());
    }
}
