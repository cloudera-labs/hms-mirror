# Filter

## Databases

There are several ways to specify which databases to process.  The most simple method is to provide a list of 
database(s) to process.

The first is to set the database(s) to process explicitly.

<tabs>
<tab title="CLI">

`-db <databases>|--database <databases>` 'databases' is a comma separated list of target databases to process.

</tab>
<tab title="Web UI">

![filter_databases.png](filter_databases.png)

</tab>
<tab title="Config File">

```yaml
databases:
- db_name
- db_name2
```

</tab>
</tabs>

The second method is to use a Regular Expression to match the database(s) to process.

<tabs>
<tab title="CLI">

`-dbRegEx <regex>|--database-regex <regex>` 'regex' is a valid "Regular Expression" used to filter which databases 
to process from the target cluster.

</tab>
<tab title="Web UI">

![filter_db_regex.png](filter_db_regex.png)

</tab>
<tab title="Config File">

Using the `-dbRegEx` on the CLI will populate the `databases` section of the config file with the regular expression 
matches.

```yaml
databases:
- db_name
- db_name2
```

</tab>
</tabs>

The third method is the most flexible and complete option.  Use [Warehouse Plans](Warehouse-Plans.md) to define the 
which databases to process.  This method also allows you to define individual database location values.

<tabs>
<tab title="CLI">

`-wps,--warehouse-plans <db=ext-dir:mngd-dir[,db=ext-dir:mngd-dir]...>`

</tab>
<tab title="Web UI">

![filter_wp.png](filter_wp.png)

</tab>
<tab title="Config File">

```yaml
translator:
  warehousePlans:
    db1:
      source: "PLAN"
      externalDirectory: "/finance/external"
      managedDirectory: "/finance/managed"
    db2:
        source: "PLAN"
        externalDirectory: "/marketing/external"
        managedDirectory: "/marketing/managed"
```

</tab>
</tabs>

## Database Skip Properties (via RegEx)

A user managed list of properties that will be filter OUT from the migration.  For example: If you don't want to migrate a DBPROPERTY like `repl.incl.test=hello_world`, then add `repl\.incl.*` to this list.

<note>This is a RegEx, so ensure you follow that syntax.</note>

<tabs>
<tab title="CLI">

`-dbsp,--database-skip-properties <properties>` Comma separated list of database properties (regex) to skip during the migration process.  This will prevent the property from being set on the target cluster.

</tab>
<tab title="Web UI">

![dbSkipProps.png](dbSkipProps.png)

</tab>
<tab title="Config File">

```yaml
filter:
  dbPropertySkipList:
    - "repl\\.inc.*"
```

</tab>
</tabs>


## Tables

When nothing is specified, all tables in the processed databases are included.  To limit the tables processed, you 
can use the following options.

### By Regular Expression

<tabs>
<tab title="CLI">

`-tf <regex>` 'regex' used to match tables in the database to process. 
process.

`-tef <regex>` 'regex' used to match tables that would be 'EXCLUDED' in the database to process.
process.

</tab>
<tab title="Web UI">

![filter_tbl_regex.png](filter_tbl_regex.png)

</tab>
<tab title="Config File">

```yaml
filter:
  tblRegEx: "test.*"
```

```yaml
filter:
  tblExcludeRegEx: "tmp.*"
```

</tab>
</tabs>

### By Limits

<tabs>
<tab title="CLI">

`-tfp <partition-count>` 'partition-count' would be a limit of the number of partitions in a table that would be 
processed.  Tables with more partitions than the limit would be excluded. A value of `-1` would include all tables.

`-tfs <size MB>` 'size MB' would be a limit in size for tables that would be processed. Tables greater in size 
would be excluded. A value of `-1` would include all tables.

</tab>
<tab title="Web UI">

A value of `-1` would include all tables.

![filter_limits.png](filter_limits.png)

</tab>
<tab title="Config File">

A value of `-1` would include all tables.

```yaml
filter:
  tblSizeLimit: -1
  tblPartitionLimit: -1
```

</tab>
</tabs>