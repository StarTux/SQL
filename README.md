# SQL
*MySQL library for Spigot.  Replacement for Ebean.*

## Usage
Client plugins need SQL as a dependency.  They must then create an instance of `SQLDatabase`, register table classes, and at least once call `generateAllTables()`.
```java
@Override
public void onEnable() {
    database = new SQLDatabase(this);
    database.registerTables(UserTable.class, ScoreTable.class);
    database.createAllTables();
}
```

## Configuration
There is a global configuration in this plugin's `database` section in the `config.yml` file, but individual plugin may override any setting in the root section of the `sql.yml` file in their respective plugin folders.

```yaml
host: '127.0.0.1'
port: '3306'
user: 'user'
password: 'password'
database: ''
prefix: ''
optimisticLocking: false
```

## Annotations
Table classes can and should be described with annotations.  This plugin respects the following annotations of the `javax.persistence` package.  All of them are shaded in to the plugin jar.
- Column
- Id
- Index
- OneToMany
- Table
- UniqueConstraint
- Version
