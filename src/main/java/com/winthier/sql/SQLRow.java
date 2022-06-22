package com.winthier.sql;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public interface SQLRow {
    @Retention(RUNTIME) @Target({FIELD, TYPE}) public @interface Name {
        String value();
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface Id { }

    @Retention(RUNTIME) @Target(FIELD) public @interface Text { }

    @Retention(RUNTIME) @Target(FIELD) public @interface MediumText { }

    @Retention(RUNTIME) @Target(FIELD) public @interface LongText { }

    @Retention(RUNTIME) @Target(FIELD) public @interface Default {
        String value() default "";
    }

    @Retention(RUNTIME) @Target({FIELD, TYPE}) public @interface NotNull { }

    @Retention(RUNTIME) @Target(FIELD) public @interface Nullable { }

    @Retention(RUNTIME) @Target(FIELD) public @interface Char {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface VarChar {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface Binary {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface VarBinary {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface TinyInt { }
    @Retention(RUNTIME) @Target(FIELD) public @interface SmallInt { }
    @Retention(RUNTIME) @Target(FIELD) public @interface MediumInt { }
    @Retention(RUNTIME) @Target(FIELD) public @interface Int { }
    @Retention(RUNTIME) @Target(FIELD) public @interface BigInt { }

    @Retention(RUNTIME) @Target(FIELD) public @interface Unique {
        String value() default "";
    }

    @Retention(RUNTIME) @Target(FIELD) public @interface Keyed {
        String value() default "";
    }

    @Retention(RUNTIME) @Target(TYPE) @Repeatable(Keys.class)
    public @interface Key {
        String[] value();
        String name() default "";
    }

    @Retention(RUNTIME) @Target(TYPE)
    public @interface Keys {
        Key[] value();
    }

    @Retention(RUNTIME) @Target(TYPE) @Repeatable(UniqueKeys.class)
    public @interface UniqueKey {
        String[] value();
        String name() default "";
    }

    @Retention(RUNTIME) @Target(TYPE)
    public @interface UniqueKeys {
        UniqueKey[] value();
    }
}
