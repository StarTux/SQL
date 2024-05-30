package com.winthier.sql;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public interface SQLRow {
    @Retention(RUNTIME) @Target({FIELD, TYPE}) @interface Name {
        String value();
    }

    @Retention(RUNTIME) @Target(FIELD) @interface Id { }

    @Retention(RUNTIME) @Target(FIELD) @interface Text { }
    @Retention(RUNTIME) @Target(FIELD) @interface MediumText { }
    @Retention(RUNTIME) @Target(FIELD) @interface LongText { }

    @Retention(RUNTIME) @Target(FIELD) @interface MediumBlob { }
    @Retention(RUNTIME) @Target(FIELD) @interface LongBlob { }

    @Retention(RUNTIME) @Target(FIELD) @interface Default {
        String value() default "";
    }

    @Retention(RUNTIME) @Target({FIELD, TYPE}) @interface NotNull { }

    @Retention(RUNTIME) @Target(FIELD) @interface Nullable { }

    @Retention(RUNTIME) @Target(FIELD) @interface Char {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) @interface VarChar {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) @interface Binary {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) @interface VarBinary {
        int value();
    }

    @Retention(RUNTIME) @Target(FIELD) @interface TinyInt { }
    @Retention(RUNTIME) @Target(FIELD) @interface SmallInt { }
    @Retention(RUNTIME) @Target(FIELD) @interface MediumInt { }
    @Retention(RUNTIME) @Target(FIELD) @interface Int { }
    @Retention(RUNTIME) @Target(FIELD) @interface BigInt { }

    @Retention(RUNTIME) @Target(FIELD) @interface Unique {
        String value() default "";
    }

    @Retention(RUNTIME) @Target(FIELD) @interface Keyed {
        String value() default "";
    }

    @Retention(RUNTIME) @Target(TYPE) @Repeatable(Keys.class)
    @interface Key {
        String[] value();
        String name() default "";
    }

    @Retention(RUNTIME) @Target(TYPE)
    @interface Keys {
        Key[] value();
    }

    @Retention(RUNTIME) @Target(TYPE) @Repeatable(UniqueKeys.class)
    @interface UniqueKey {
        String[] value();
        String name() default "";
    }

    @Retention(RUNTIME) @Target(TYPE)
    @interface UniqueKeys {
        UniqueKey[] value();
    }
}
