# On Prem Legacy Hive to Non-Legacy Hive

## Environments

HDP 2.6 -> CDP
CDH 5.x -> CDP
CDH 6.x -> CDP

LEFT clusters are the LEGACY clusters.
RIGHT clusters are the NON-LEGACY clusters (hive3).

## Assumptions

- LEFT HS2 is NOT Kerberized.  Since this is a Legacy cluster (which is kerberized) we need to establish a 'non' kerberized HS2 endpoint.  Use KNOX or setup an additional HS2 in the management console that isn't Kerberized.  hms-mirror is built (default) with Hadoop 3, of which the libraries are NOT backwardly compatible.
- Standalone JDBC jar files for the LEGACY and NON-LEGACY clusters are available to the running host as specified in the 'configuration'.
- `hms-mirror` is run from an EdgeNode on CDP
  - The edgenode has network access to the Legacy HS2 endpoint
- No ACID tables (HDP)
- No VIEWs
- No Non-Native tables (Hive tables backed by HBase, JDBC, Kafka)
- The HiveServer2's on each cluster have enough concurrency to support the configured connections `transfer->concurrency`.  If not specified, the default is 4.

[Configuration Example](./cfg/basic_legacy_non-legacy.yaml)

## NOTES

`hms-mirror` runs in DRY-RUN mode by default.  Add `-e|--execute` to your command to actually run the process on the clusters.  Use `--accept` to avoid the verification questions (but don't deny their meaning).

All actions performed by `hms-mirror` are recorded in the *_execute.sql files.  Review them to understand the orchestration and process.

Review the report markdown files (html version also available) for details about the job.  Explanations regarding steps, issues, and failure reasons can be found there.

## Scenario Index

<!-- toc -->

- [One-time migration of SCHEMA's from LEFT to RIGHT.](#one-time-migration-of-schemas-from-left-to-right)
- [`DUMP` of clusters SCHEMA](#dump-of-clusters-schema)
- [Data Migration for Non-ACID tables using `SQL`](#data-migration-for-non-acid-tables-using-sql)
- [Data Migration for ACID tables using `SQL` or `HYBRID`](#data-migration-for-acid-tables-using-sql-or-hybrid)
- [Schema Migration of specific tables using RegEx](#schema-migration-of-specific-tables-using-regex)
- [Migrate VIEWS for a specific database](#migrate-views-for-a-specific-database)
- [Create schema in RIGHT cluster using the LEFT clusters data](#create-schema-in-right-cluster-using-the-left-clusters-data)
- [Migrate SCHEMA's and Data using `SQL`](#migrate-schemas-and-data-using-sql)
- [Migrate SCHEMA's and Data using `EXPORT_IMPORT`](#migrate-schemas-and-data-using-export_import)
- [Migrate SCHEMA's and Data using `HYBRID`](#migrate-schemas-and-data-using-hybrid)
- [Disaster Recovery (RIGHT Cluster is DR and READ-ONLY)](#disaster-recovery-right-cluster-is-dr-and-read-only)

<!-- tocstop -->

## One-time migration of SCHEMA's from LEFT to RIGHT.

This is done with the basic `SCHMEA_ONLY` data strategy (default) and will extract the schema's from the LEFT and replay them on the RIGHT cluster.  In this mode, NO DATA is moved.

### Command

`hms-mirror -db tpcds_bin_partitioned_orc_10 -o temp`

Examine the reports place in the relative `temp` directory specified with the `-o` option. A report set is generated to each database.

### Notes

Since no data is transferred in this scenario, the expectation is that the data is managed by another process, like `distcp`.  The output of `hms-mirror` will create some template distcp calls with the appropriate location details.  You can use this as a starting point to managed this transfer.

## `DUMP` of clusters SCHEMA

We want a simple extraction of a database schema.  An optional `-ds LEFT|RIGHT` option can be specified to target which configured cluster to use.  The default is `LEFT`.  Note: When you specify `RIGHT`, the DUMP output with still show up in the `LEFT` output report.

No translations are done in this scenario.  So if the DUMP is taken from a 'legacy' cluster, beware of the implications of 'replaying' it on a non-legacy cluster.

### Command

This will use the LEFT cluster configuration for the schema DUMP of `tpcds_bin_partititoned_orc_10`.
`hms-mirror -db tpcds_bin_partitioned_orc_10 -o temp -d DUMP`

This will use the RIGHT cluster configuration for the schema DUMP of `tpcds_bin_partititoned_orc_10`.
`hms-mirror -db tpcds_bin_partitioned_orc_10 -o temp -d DUMP -ds RIGHT`

## Data Migration for Non-ACID tables using `SQL`

This approach assumes the clusters are [linked](../README.md#linked).  The SQL data strategy uses the RIGHT's clusters view into the HDFS filesystem of the LEFT cluster to facilitate data movement.

### Command

`hms-mirror -d SQL -db tpcds_bin_partitioned_orc_10 -o temp`

## Data Migration for ACID tables using `SQL` or `HYBRID`

This approach assumes the clusters are [linked](../README.md#linked).  The SQL data strategy uses the RIGHT's clusters view into the HDFS filesystem of the LEFT cluster to facilitate data movement.

### Command

`hms-mirror -d SQL -db tpcds_bin_partitioned_orc_10 -o temp -ma`

Use `-mao` to migrate ONLY ACID tables.  `-ma` will migrate ACID and Non-ACID tables.

## Schema Migration of specific tables using RegEx

Using a RegEx pattern, filter the tables in the db to migrate.

### Command

`hms-mirror -db tpcds_bin_partitioned_orc_10 -tf call_*. -o temp`

`-tf|--table-filter` followed by a RegEx to filter tables.

## Migrate VIEWS for a specific database

View migration requires the underlying referenced tables exist BEFORE the 'view' can be created.  This isn't a requirement of `hms-mirror` rather a Hive requirement.  Therefore, you should migrate the tables first as shown above and in a followup process run the following.

### Command

`hms-mirror -db tpcds_bin_partitioned_orc_10 -v`

## Create schema in RIGHT cluster using the LEFT clusters data

This is a helpful scenario for 'testing' workflows on the RIGHT cluster.  The tables on the right cluster will NOT be configured with 'PURGE' to avoid the deletion of data on the LEFT cluster.  These tables should be considered READ-ONLY.  Test this against a sample dataset to THOROUGHLY understand the relationships here.  This is NOT intended for 'production' use and should be used only as a validation mechanism for the RIGHT cluster.

The clusters must be [linked](../README.md#linked).  Only legacy managed tables, external tables, and views can be linked.  ACID tables can NOT be linked.

### Command

`hms-mirror -d LINKED -db tpcds_bin_partitioned_orc_10 -o temp`

The tables created on the RIGHT cluster will use the data located on the LEFT cluster.  The 'database' will be created to match the database in the LEFT cluster.

WARNING:  If the LOCATION element is specified in the database definition AND you use `DROP DATABASE ... CASCADE` from the RIGHT cluster, YOU WILL DROP THE DATA ON THE LEFT CLUSTER even though the tables are NOT purgeable.  This is the DEFAULT behavior of hive 'DROP DATABASE'.  So BE CAREFUL!!!!

## Migrate SCHEMA's and Data using `SQL`

The clusters must be [linked](../README.md#linked).  In this scenario, we'll use the connected clusters and SQL to migrate data from the LEFT cluster to the RIGHT.

There are limits regarding partitioned tables.  For SQL migrations, the default is 500.  Meaning that tables with more than 500 partitions will NOT attempt this transfer.  This can be changed in the 'config' file by adding/changing `hybrid->sqlPartitionLimit`.  This was put in place as a general safeguard against attempts at tables with a partition count that may fail.  It doesn't mean they'll always fail, it's just a place holder.

### Command

`hms-mirror -d SQL -db tpcds_bin_partitioned_orc_10 -o temp`

To transfer ACID tables, add `-ma|-mao`.

### Notes

This is a one time transfer.  Incremental updates aren't supported with this configuration.  For incremental schema updates, see:

## Migrate SCHEMA's and Data using `EXPORT_IMPORT`

EXPORT/IMPORT is a basic Hive process used to package table schemas and data into a transferable unit that can be replayed on the new cluster.  For `hms-mirror` there is a defined prefix for a transfer directory in the configuration `transfer->exportBaseDirPrefix`.  If this isn't defined, the default is `/apps/hive/warehouse/export_`.

There are performance implications to using EXPORT_IMPORT with partitioned tables.  The IMPORT process is quite slow at loading partitions.  We've defined limits in the config (which can be changed) `hybrid->exportImportPartitionLimit`.  The default is 100.  If the number of partitions exceeds this value, we will NOT attempt the transfer and will note this in the output report.

The clusters must be [linked](../README.md#linked).  The before mentioned prefix directory on the LEFT cluster is accessed by the IMPORT process that runs on the RIGHT cluster.  If the namespace (and permissions) aren't correct, the IMPORT process will fail.

### Command

`hms-mirror -d EXPORT_IMPORT -db tpcds_bin_partitioned_orc_10 -o temp`

### Notes

EXPORT_IMPORT will NOT work for ACIDv1 -> ACIDv2 (Hive 1/2 to 3) conversions.  Use `SQL` or `HYBRID` instead.

## Migrate SCHEMA's and Data using `HYBRID`

The `HYBRID` data strategy is a combination of the `SQL` and `EXPORT_IMPORT` data strategies.  It uses basic rules to choose the more appropriate method for the table in question.

The clusters must be [linked](../README.md#linked).

The process will first consider using `EXPORT_IMPORT` unless:
- The table is ACIDv1 and you're migrating to ACIDv2 (Legacy to Non-Legacy Clusters)
- The number of partitions exceed the value set by: `hybrid->exportImportPartitionLimit`.  The default is 100.  When exceeded, the `SQL` method will be used.  The `SQL` method will fail is the partition count exceeds the value of `hybrid->sqlPartitionLimit`.  The default is 500.

### Command

`hms-mirror -d HYBRID -db tpcds_bin_partitioned_orc_10 -o temp`


## Disaster Recovery (RIGHT Cluster is DR and READ-ONLY)

The DR scenario will transfer schemas and subsequent runs will update the 'schema' if changes are made.  This process does NOT move data.  The process will generate a `distcp` work plan for you to get started.  You should modify that to use 'snapshot diffs' and managed the data migration through `distcp`.

You should first run the process in `DRY-RUN` mode to get the `distcp` plan.  The data must be transferred first!  This ensures that the database and table/partition directories are created BEFORE the schemas are replayed.  If the schemas are applied before the `distcp` with SNAPSHOT diffs, then `hive` will own the directories and the `distcp` with snapshots will fail.

WARNING: Do not attempt to `DROP DATABASE ... CASCADE` on the RIGHT cluster, this will modify the filesystem and cause the incremental `distcp` with snapshots to fail.

### Command

`hms-mirror -d SCHEMA_ONLY -db tpcds_bin_partitioned_orc_10 -ro -sync`

### Notes

This process will review the tables on the LEFT cluster with the RIGHT and either update the schema when it's changed (by dropping and recreating), add missing tables, or drop tables that don't exist anymore.

Tables that are migrated this way will NOT have the `PURGE` flag set on the RIGHT cluster.  This allows us to `DROP` a table without affecting the data for the `-sync` process.