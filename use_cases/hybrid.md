# On-prem to Cloud

## Environments

- CDP -> CDP
- CDH 5/6 -> CDP
- HDP 2/3 -> CDP

## Assumptions

- LEFT HS2 is NOT Kerberized.  Since this is a Legacy cluster (which is kerberized) we need to establish a 'non' kerberized HS2 endpoint.  Use KNOX or setup an additional HS2 in the management console that isn't Kerberized.  hms-mirror is built (default) with Hadoop 3, of which the libraries are NOT backwardly compatible.
- Standalone JDBC jar files for the LEGACY and NON-LEGACY clusters are available to the running host as specified in the 'configuration'.
- `hms-mirror` is run from an EdgeNode on the LOWER (On-Prem) environment
    - The edgenode has network access to the Cloud HS2 endpoint
    - The LOWER environment also has access to the Storage systems used in `intermediate` or `common` storage.  So you'll need to ensure the proper configurations have been added to the lower environment and the libraries to communicate with the cloud storage environments is available.
- No Non-Native tables (Hive tables backed by HBase, JDBC, Kafka)
- The HiveServer2's on each cluster have enough concurrency to support the configured connections `transfer->concurrency`.  If not specified, the default is 4.

[Configuration Example](./cfg/hybrid_legacy_non-legacy.yaml)

## NOTES

`hms-mirror` runs in DRY-RUN mode by default.  Add `-e|--execute` to your command to actually run the process on the clusters.  Use `--accept` to avoid the verification questions (but don't deny their meaning).

All actions performed by `hms-mirror` are recorded in the *_execute.sql files.  Review them to understand the orchestration and process.

Review the report markdown files (html version also available) for details about the job.  Explanations regarding steps, issues, and failure reasons can be found there.

## Scenario Index

<!-- toc -->

- [Table Migration via SQL using `-is` or `--intermediate-storage`](#table-migration-via-sql-using--is-or---intermediate-storage)
- [Table Migration via SQL using `-cs` or `--common-storage`](#table-migration-via-sql-using--cs-or---common-storage)

<!-- tocstop -->

## Table Migration via SQL using `-is` or `--intermediate-storage`

There are two scenarios to account for here:
- Legacy to Non-Legacy
- Non-Legacy to Non-Legacy

Make sure you've correctly set your clusters `legacyHive` value correctly to ensure proper translations for non-transactional managed tables.

### Command

__Non ACID tables__

`hms-mirror -d SQL -o temp -db <target_db> -is <cloud_storage_bucket>`

__ACID tables__

`hms-mirror -d SQL -o temp -db <target_db> -is <cloud_storage_bucket> -ma`

### Notes

Using the `cloud_storage_bucket` as the intermediate location, `hms-mirror` will migrate data via SQL from the source table to a 'transfer' table that uses the `cloud_storage_bucket` as its location.  On the RIGHT cluster, a 'shadow' table will be created, using the same `cloud_storage_bucket` location as the transfer table.  Then the target table on the RIGHT cluster will be created using the RIGHT clusters `hcfsNamespace` as the anchor location.

SQL will be used to move data from the LEFT's source table to the transfer table, which will write the data to the `intermediate-storage` location.  The RIGHT cluster will read this data via the shadow table and again use SQL to lift that data from the `intermediate-storage` location to the base location of the RIGHT clusters hcfsNamespace.

This option moves the data twice.

## Table Migration via SQL using `-cs` or `--common-storage`

The `common-storage` option provides an opportunity to streamline the number of times data is copied to satisfy a migration.

For non-acid tables, the data migrates from the 'source' to the `common-storage` location, where it is referenced by the target cluster.  The `common-storage` location is actually the final resting location for non-acid tables.

### Commands

__Non ACID tables__

`hms-mirror -d SQL -o temp -db <target_db> -cs <cloud_storage_bucket>`

__ACID tables__

`hms-mirror -d SQL -o temp -db <target_db> -cs <cloud_storage_bucket> -ma`

### Notes

ACID tables use `common-storage` very much like `intermediate-storage` and require the data to be moved/converted twice.  ACID(source)->External(Common)->ACID(target).  The final resting place for ACID data will be the clusters default managed table location as defined in hive's `hive.metastore.warehouse.dir` property OR the databases `MANAGEDLOCATION` property.

If you wish to downgrade the ACID table using `-da` or `--downgrade-acid`, the `common-storage` location will be used as the final 'downgraded' resting location for the downgraded ACID table.  Downgraded ACID tables will also use the defaults defined in hive's `hive.metastore.warehouse.external.dir` OR the databases `LOCATION` property.

