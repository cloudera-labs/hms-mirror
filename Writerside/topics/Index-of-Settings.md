# Index of Settings

## Copy Avro Schema Urls

<tabs>
<tab title="CLI">

`-asm,--avro-schema-migration`  Migrate AVRO Schema Files referenced in TBLPROPERTIES by 'avro.schema.url'. Without 
migration it is expected that the file will exist on the other cluster and match the 'url' defined in the schema DDL.
If it's not present, schema creation will FAIL. Specifying this option REQUIRES the LEFT and RIGHT cluster to be 
LINKED. See docs: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers

</tab>
<tab title="Web UI">

![attrib_misc.png](attrib_misc.png)

</tab>
<tab title="Config File">

`copyAvroSchemaUrls` is a boolean value that determines if the Avro schema URLs should be copied from the source to the target. This is useful if you're using Avro schemas in your source and target environments and want to maintain the same schema URLs in both environments. The default value is
`false`.

```yaml
copyAvroSchemaUrls: true|false
```

</tab>
</tabs>


## Data Strategy

Data Strategy controls how schemas and data are migrated.  The following values are supported:
- SCHEMA_ONLY
- SQL
- EXPORT_IMPORT
- HYBRID
- DUMP
- STORAGE_MIGRATION
- COMMON
- LINKED

<tabs>
<tab title="CLI">

`-d|--data-strategy <strategy>` 'strategy' is one of the strategies listed above.

</tab>
<tab title="Web UI">

The Web UI approaches this setting a little differently.  When you 'initialized' a migration session, you'll be 
asked to select a 'Data Strategy' from a drop-down list.  Saved sessions will have the 'data-strategy' embedded in 
them and is NOT editable.  You 'can' create a new session based on an existing session and maintain most of the 
properties of the original session.

![init_data-strategy.png](init_data-strategy.png)

</tab>
<tab title="Config File">

```yaml
dataStrategy: "SCHEMA_ONLY|SQL|EXPORT_IMPORT|HYBRID|DUMP|STORAGE_MIGRATION|COMMON|LINKED"
```

</tab>
</tabs>

## Database Only

When set we'll only migrate the database objects and not the tables.

<tabs>
<tab title="CLI">

`-dbo|--database-only` will only migrate the database objects and not the tables.

</tab>
<tab title="Web UI">

![attrib_misc.png](attrib_misc.png)

</tab>
<tab title="Config File">

```yaml
  databaseOnly: true|false
```

</tab>
</tabs>


## Dump Test Data

Dumping test data uses the running configuration to create a processable artifact that can be used for testing offline.

<tabs>
<tab title="CLI">

`dtd|--dump-test-data` will dump an artifact that can be used for testing offline.

</tab>
<tab title="Web UI">

Not an option in the Web UI.

</tab>
<tab title="Config File">

```yaml
  dumpTestData: true|false
```

</tab>
</tabs>

## Load Test Data File

Run the process with a test data file that was created with the `--dump-test-data` option.

<tabs>
<tab title="CLI">

`-ltd|--load-test-data` will load the test data file that was created with the `--dump-test-data` option.

</tab>
<tab title="Web UI">

Not an option in the Web UI.

</tab>
<tab title="Config File">

```yaml
  loadTestDataFile: "<path_to_test_data_file>"
```

</tab>
</tabs>


## Skip Link Check

When there are issues connecting to the source or target cluster storage systems, you can skip the link check.  
Doing so may mean that certain features will not work as expected.  EG: Limits used to determine processing that 
need to review the storage layer or the ability to copy Avro schema files or optimizations made based on file 

<tabs>
<tab title="CLI">

`-slc|--skip-link-check` will skip the link check.

</tab>
<tab title="Web UI">

![attrib_misc.png](attrib_misc.png)

</tab>
<tab title="Config File">

Default is `false`.

```yaml
  skipLinkCheck: true|false
```

</tab>
</tabs>

## Database Prefix

`dbPrefix` is a value to pre-pend to the database name when creating the database in the target cluster. This is
way to avoid conflicts with existing databases in the target cluster. The default value is an empty string.

```yaml
  dbPrefix: "<prefix>"
```

## Database Rename

`dbRename` is a string value that identifies the new name of the database in the target cluster. This is useful for
testing a single database migration to an alternate database in the target cluster. This is only valid for a single
database migration.

```yaml
  dbRename: "<new_db_name>"
```

## Execute

`execute` is a boolean value that determines if the migration should be executed. The default value is `false`,
which is the dry-run mode. In the 'dry-run' mode, all the reports are generated and none of the actual migration is
done.

<tip>
You should ALWAYS run a 'dry-run' before executing a migration.  This will give you a good idea of what will be done 
and provide you with the reports to review.
</tip>

```yaml
  execute: true|false
```

## Output Directory

Output directory is a string value that identifies the directory where the reports will be written. The default is
`$HOME/.hms-mirror/reports`. When this value is defined, the reports will be written to the specified output
directory with the timestamp as the 'name' of the report.

<tabs>
<tab title="CLI">

`-o|--output-dir <path_to_output_directory>` will set the output directory.

</tab>
<tab title="Web UI">

Not available in the Web UI.

</tab>
<tab title="Config File">

```yaml
  outputDirectory: "<path_to_output_directory>"
```

</tab>
</tabs>


## Encrypted Passwords

`encryptedPasswords` is a boolean value that determines if the passwords in the configuration file are encrypted.

```yaml
  encryptedPasswords: true|false
```

## Read-Only

Read-Only is a boolean value that determines if the migration should be read-only. The default value is `false`.  
When this value is set to `true`, table schema's created will not have a 'purge' flag set to ensure they can't drop
data. This is useful for testing migrations and for DR scenarios where you want to limit the exposure of potential
changes on the target cluster.

<warning>
This does NOT prevent data from being manipulated on the target cluster after the migration.
</warning>

<tabs>
<tab title="CLI">

`-ro|--read-only` will set the migration to read-only.

</tab>
<tab title="Web UI">


</tab>
<tab title="Config File">

```yaml
  readOnly: true|false
```

</tab>
</tabs>


## Skip Features

Skip Features is a boolean value that is `false` by default so feature check will be made. Features are a framework of checks that examine a table definition and make corrections to it to ensure it's compatible with the target cluster. We've found several circumstances where definitions extracted from the source cluster can NOT be replayed on the target cluster
for some reason. These features attempt to correct those issues during the migration.

<tabs>
<tab title="CLI">

`-sf|--skip-features` will skip the feature checks.

</tab>
<tab title="Web UI">


</tab>
<tab title="Config File">

```yaml
  skipFeatures: true|false
```

</tab>
</tabs>

## Transfer Ownership

You can choose to transfer ownership of objects from the source to target clusters.  Ownership is defined as the `owner` of the database or table in the metastore. This is useful if you are using Ranger policies that rely on the owner of the object.

<tabs>
<tab title="CLI">

`-to|--transfer-ownership` will transfer ownership of databases and tables from the source to the target cluster.

`-todb|--transfer-ownership-database` will transfer ownership of databases from the source to the target cluster.

`-totbl|--transfer-ownership-table` will transfer ownership of tables from the source to the target cluster.

</tab> 
<tab title="Web UI">

![attrib_misc.png](attrib_misc.png)

</tab>
<tab title="Config File">

```yaml
  ownershipTransfer:
    database: true|false
    table: true|false
```
</tab>

</tabs>


