# Index of Settings

## Copy Avro Schema Urls

`copyAvroSchemaUrls` is a boolean value that determines if the Avro schema URLs should be copied from the source to the target. This is useful if you're using Avro schemas in your source and target environments and want to maintain the same schema URLs in both environments. The default value is
`false`.

```yaml
  copyAvroSchemaUrls: true|false
```

## Data Strategy

`dataStrategy` identifies how/what will be migrated between clusters. The following values are supported:

- SCHEMA_ONLY
- SQL
- EXPORT_IMPORT
- HYBRID
- DUMP
- STORAGE_MIGRATION
- COMMON
- LINKED

```yaml
  dataStrategy: "SCHEMA_ONLY|SQL|EXPORT_IMPORT|HYBRID|DUMP|STORAGE_MIGRATION|COMMON|LINKED"
```

## Database Only

`databaseOnly` is a boolean value that determines if only the database objects should be migrated. The default value is
`false`.

```yaml
  databaseOnly: true|false
```

## Dump Test Data

`dumpTestData` is a boolean value that determines if test data should be dumped to the target. The default value is
`false`.

```yaml
  dumpTestData: true|false
```

## Load Test Data File

`loadTestDataFile` is a string value that identifies the file containing the test data to be loaded.

```yaml
  loadTestDataFile: "<path_to_test_data_file>"
```

## Skip Link Check

`skipLinkCheck` is a boolean value that determines if the link check should be skipped. The default value is
`false`. Each cluster identifies an HCFS namespace and the link check will verify that the namespace is accessible
from the `hms-mirror` host. In addition, if the `targetNamespace` is defined, the link check will check that as well.

```yaml
  skipLinkCheck: true|false
```

## Databases

`databases` is a list of databases to be migrated. It works in concert with 'Warehouse Plans' to provide a list of
databases to be migrated.

```yaml
  databases:
    - db_name
    - db_name2
```

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

## Migrate Non-Native Tables

`migrateNonNative` is a boolean value that determines if non-native tables should be migrated. The default is
`false`. A non-native table is a table that doesn't have a LOCATION element in the table definition. This is
typical of tables in Hive that rely on other technologies to store the data. EG: HBase, Kafka, JDBC Federation, etc.

```yaml
  migrateNonNative: true|false
```

## Output Directory

`outputDirectory` is a string value that identifies the directory where the reports will be written. The default is
`$HOME/.hms-mirror/reports`. When this value is defined, the reports will be written to the specified output
directory with the timestamp as the 'name' of the report.

```yaml
  outputDirectory: "<path_to_output_directory>"
```

## Encrypted Passwords

`encryptedPasswords` is a boolean value that determines if the passwords in the configuration file are encrypted.

```yaml
  encryptedPasswords: true|false
```

## Read-Only

`readOnly` is a boolean value that determines if the migration should be read-only. The default value is `false`.  
When this value is set to `true`, table schema's created will not have a 'purge' flag set to ensure they can't drop
data. This is useful for testing migrations and for DR scenarios where you want to limit the exposure of potential
changes on the target cluster.

```yaml
  readOnly: true|false
```

## Skip Features

`skipFeatures` is a boolean value that is `false` by default so feature check will be made. Features are a
framework of checks that examine a table definition and make corrections to it to ensure it's compatible with the
target cluster. We've found several circumstances where definitions extracted from the source cluster can NOT be replayed on the target cluster
for some reason. These features attempt to correct those issues during the migration.

```yaml
  skipFeatures: true|false
```