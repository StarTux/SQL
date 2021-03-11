package com.winthier.sql.condition;

import java.util.List;

public interface SQLCondition {
    String compile(List<Object> values);
}
