# Migrate Options

There are several options to control the migration of tables and views.

## ACID Tables

ACID tables require special handling.  By default, ACID tables are NOT migrated unless the following options are set.

<tabs>
<tab title="CLI">

`-ma|--migrate-acid` will set the migration to include ACID tables.
`-mao|--migrate-acid-only` will set the migration to include ONLY ACID tables.

</tab>
<tab title="Web UI">

![migrate-options.png](migrate-options.png)

</tab>
<tab title="Config File">

```yaml
migrateACID:
  "on": true
  only: false
```

</tab>
</tabs>


## Non-Native Tables

By default, non-native tables are NOT migrated.  Usually because there are requirements that the underlying technology be in place before the migration can happen.

If those prerequisites are met, you can set the `migrateNonNative` option to `true` in the configuration file.

This is
typical of tables in Hive that rely on other technologies to store the data. EG: HBase, Kafka, JDBC Federation, etc.

<tabs>
<tab title="CLI">

`-mnn|--migrate-non-native` will set the migration to include non-native tables.
`-mnno|--migrate-non-native-only` will set the migration to include ONLY non-native tables.

</tab>
<tab title="Web UI">

![migrate-options.png](migrate-options.png)

</tab>
<tab title="Config File">

```yaml
  migrateNonNative: true
```

</tab>
</tabs>


## Views

Views are NOT migrated by default.  Views require the underlying tables to be in place before they can be created. 
If you have views, migrate the tables first and then rerun the migration for the views.

<warning>
Do not rename databases or tables that view depend on.  The migration will fail.
</warning>

<tabs>
<tab title="CLI">

`-v|--views-only` will set the migration to include ONLY views.

</tab>
<tab title="Web UI">

![migrate-options.png](migrate-options.png)

</tab>
<tab title="Config File">

```yaml
migrateVIEW:
  "on": false
```

</tab>
</tabs>
