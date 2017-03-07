package com.winthier.sql;

import java.sql.ResultSet;

public interface SQLInterface {
    default void onLoad(ResultSet result) {
    }
}
